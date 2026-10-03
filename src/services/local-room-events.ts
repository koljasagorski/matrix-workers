import type { Env, PDU } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { parseUserId } from '../utils/ids';
import { canonicalJson, verifySignature } from '../utils/crypto';
import { getEvent, getRoom, getRoomState, notifyUsersOfEvent, storeEvent } from './database';
import { checkEventAuth } from './event-auth';
import { eventReferenceId, FEDERATED_ROOM_VERSIONS, isObject, redactEvent, signEvent, wireEvent, type WireEvent } from './federation-events';
import { fetchRemoteServerKeys, getServerSigningKey, makeFederationRequest } from './federation-keys';
import { queueRoomEvent } from './federation-delivery';
import { readFederationJson } from './federation-http';
import { invalidateRoomCache } from './room-cache';
import { prepareRestrictedJoinContent } from './restricted-joins';

export interface LocalEventInput {
  roomId: string;
  sender: string;
  type: string;
  content: Record<string, unknown>;
  stateKey?: string;
  unsigned?: PDU['unsigned'];
  redacts?: string;
}

// Select exactly the auth state prescribed by the room version. Arbitrary room
// state (names, topics, etc.) must never be added to an auth chain.
export function selectAuthEvents(state: PDU[], input: LocalEventInput, version: string): string[] {
  if (input.type === 'm.room.create') return [];
  const member = input.type === 'm.room.member';
  const membership = input.content.membership;
  return state.filter(event =>
    (event.type === 'm.room.create' && version !== '12') ||
    event.type === 'm.room.power_levels' ||
    (event.type === 'm.room.member' && (event.state_key === input.sender ||
      (member && event.state_key === input.stateKey) ||
      (member && membership === 'join' && event.state_key === input.content.join_authorised_via_users_server))) ||
    (member && ['join', 'invite', 'knock'].includes(String(membership)) && event.type === 'm.room.join_rules') ||
    (member && membership === 'invite' && isObject(input.content.third_party_invite) &&
      isObject(input.content.third_party_invite.signed) && event.type === 'm.room.third_party_invite' &&
      event.state_key === input.content.third_party_invite.signed.token)
  ).map(event => event.event_id);
}

export async function buildLocalRoomEvent(env: Env, input: LocalEventInput, initialVersion?: string): Promise<{ event: PDU; version: string }> {
  if (!parseUserId(input.sender) || parseUserId(input.sender)?.serverName !== env.SERVER_NAME) throw Errors.forbidden('Invalid local sender');
  if (!isObject(input.content)) throw Errors.badJson('Event content must be an object');
  const room = initialVersion ? null : await getRoom(env.DB, input.roomId);
  if (!initialVersion && !room) throw Errors.notFound('Room not found');
  const version = initialVersion ?? room!.room_version;
  if (!FEDERATED_ROOM_VERSIONS.includes(version)) throw Errors.unsupportedRoomVersion();
  const state = initialVersion ? [] : await getRoomState(env.DB, input.roomId);
  if (input.type === 'm.room.member' && input.content.membership === 'join') {
    input = { ...input, content: await prepareRestrictedJoinContent(env, input.roomId, input.sender, input.content, state, version) };
  }
  if (input.type === 'm.room.create' && state.length) throw Errors.forbidden('Room creation state cannot be replaced');
  const latest = input.type === 'm.room.create' ? null : await env.DB.prepare(
    'SELECT event_id,depth FROM events WHERE room_id=? ORDER BY depth DESC,stream_ordering DESC LIMIT 1'
  ).bind(input.roomId).first<{ event_id: string; depth: number }>();
  return { version, event: await buildLocalRoomEventFromState(env, input, version, state, latest ?? undefined) };
}

// Build a bootstrap graph in memory so its complete, authorized state can be
// committed together. The caller supplies the actual state before this event.
export async function buildLocalRoomEventFromState(
  env: Env, input: LocalEventInput, version: string, state: PDU[],
  previous?: { event_id: string; depth: number },
): Promise<PDU> {
  if (parseUserId(input.sender)?.serverName !== env.SERVER_NAME) throw Errors.forbidden('Invalid local sender');
  if (!FEDERATED_ROOM_VERSIONS.includes(version)) throw Errors.unsupportedRoomVersion();
  if (!isObject(input.content)) throw Errors.badJson('Event content must be an object');
  if (state.some(event => event.room_id !== input.roomId)) throw Errors.invalidRoomState('Bootstrap state belongs to another room');
  const event: PDU = {
    event_id: '', room_id: input.roomId, sender: input.sender, type: input.type,
    content: input.content, ...(input.stateKey === undefined ? {} : { state_key: input.stateKey }),
    origin_server_ts: Date.now(), depth: Math.min((previous?.depth ?? 0) + 1, Number.MAX_SAFE_INTEGER),
    auth_events: selectAuthEvents(state, input, version), prev_events: previous ? [previous.event_id] : [],
    ...(input.redacts ? { redacts: input.redacts } : {}),
  };
  const authorization = checkEventAuth(event, state, version);
  if (!authorization.allowed) throw Errors.forbidden(authorization.error);
  const key = await getServerSigningKey(env.DB);
  if (!key) throw new Error('Server signing key unavailable');
  const signed = await signEvent(wireEvent(event, version), version, env.SERVER_NAME, key);
  if (new TextEncoder().encode(canonicalJson(signed)).length > 65536) throw Errors.tooLarge('Event exceeds 64 KiB');
  return { ...signed, room_id: input.roomId, event_id: await eventReferenceId(signed, version),
    ...(input.unsigned ? { unsigned: input.unsigned } : {}) } as PDU;
}

async function remoteJson(response: Response): Promise<Record<string, unknown>> {
  const body = await readFederationJson(response, 262144);
  if (!response.ok) {
    if (response.status === 403) throw Errors.forbidden('The remote server refused the membership change');
    if (response.status === 404) throw Errors.notFound('The remote server could not find the room or user');
    throw new MatrixApiError('M_UNKNOWN', `Remote membership request returned HTTP ${response.status}`, 502);
  }
  if (!isObject(body)) throw new Error('Invalid remote membership response');
  return body;
}

export async function countersignLocalRoomInvite(env: Env, event: PDU, version: string): Promise<PDU> {
  const destination = parseUserId(event.state_key ?? '')?.serverName;
  if (!destination) throw Errors.invalidParam('state_key');
  if (destination === env.SERVER_NAME) {
    if (!await env.DB.prepare('SELECT user_id FROM users WHERE user_id=? AND is_deactivated=0').bind(event.state_key!).first()) {
      throw Errors.notFound('Invited user does not exist');
    }
    return event;
  }
  const create = await env.DB.prepare(`SELECT e.content FROM room_state rs JOIN events e ON e.event_id=rs.event_id
    WHERE rs.room_id=? AND rs.event_type='m.room.create' AND rs.state_key=''`).bind(event.room_id).first<{ content: string }>();
  if (create && JSON.parse(create.content)['m.federate'] === false) throw Errors.forbidden('Room does not federate');
  const key = await getServerSigningKey(env.DB);
  if (!key) throw new Error('Server signing key unavailable');
  const state = await getRoomState(env.DB, event.room_id);
  const previewTypes = new Set(['m.room.create', 'm.room.name', 'm.room.avatar', 'm.room.canonical_alias', 'm.room.join_rules', 'm.room.encryption']);
  const inviteRoomState = state.filter(stateEvent => previewTypes.has(stateEvent.type) ||
    (stateEvent.type === 'm.room.member' && stateEvent.state_key === event.sender)).map(stateEvent => ({
    type: stateEvent.type, state_key: stateEvent.state_key, sender: stateEvent.sender, content: stateEvent.content,
  }));
  const body = await remoteJson(await makeFederationRequest('PUT', destination,
    `/_matrix/federation/v2/invite/${encodeURIComponent(event.room_id)}/${encodeURIComponent(event.event_id)}`,
    env.SERVER_NAME, key, env.CACHE, { room_version: version, event: wireEvent(event, version), invite_room_state: inviteRoomState },
    AbortSignal.timeout(25000)));
  if (!isObject(body.event) || !isObject(body.event.signatures)) throw new Error('Remote invitation has no countersignature');
  const returned = wireEvent(body.event as WireEvent, version);
  const sent = wireEvent(event, version);
  const signatures = returned.signatures;
  delete returned.signatures; delete returned.unsigned; delete sent.signatures; delete sent.unsigned;
  if (canonicalJson(returned) !== canonicalJson(sent)) throw new Error('Remote server changed the invitation');
  const signed = { ...wireEvent(event, version), signatures: { ...event.signatures, [destination]: signatures?.[destination] ?? {} } };
  const keys = await fetchRemoteServerKeys(destination, env.DB, env.CACHE);
  let valid = false;
  for (const remoteKey of keys) {
    if (remoteKey.key_id.startsWith('ed25519:') && (remoteKey.valid_until === null || remoteKey.valid_until >= event.origin_server_ts) &&
      await verifySignature(redactEvent(signed, version), destination, remoteKey.key_id, remoteKey.public_key)) { valid = true; break; }
  }
  if (!valid) throw new Error('Invalid remote invitation countersignature');
  return { ...event, signatures: signed.signatures };
}

export async function persistLocalRoomEvent(env: Env, event: PDU, version: string): Promise<void> {
  // Capture destinations before a kick/ban/leave removes the last resident on a
  // remote server, otherwise that server never receives its user's departure.
  const previousMembers = event.type === 'm.room.member'
    ? await env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id=? AND membership='join'")
      .bind(event.room_id).all<{ user_id: string }>() : null;
  const membership = event.type === 'm.room.member' ? [env.DB.prepare(`INSERT OR REPLACE INTO room_memberships
    (room_id,user_id,membership,event_id,display_name,avatar_url) VALUES (?,?,?,?,?,?)`)
    .bind(event.room_id, event.state_key!, String(event.content.membership), event.event_id,
      typeof event.content.displayname === 'string' ? event.content.displayname : null,
      typeof event.content.avatar_url === 'string' ? event.content.avatar_url : null)] : [];
  await storeEvent(env.DB, event, membership);
  if (event.state_key !== undefined) await invalidateRoomCache(env.CACHE, event.room_id)
    .catch(error => console.error('[rooms] Failed to invalidate room metadata:', error));
  await queueRoomEvent(env, event, version, previousMembers?.results.map(member => parseUserId(member.user_id)?.serverName)
    .filter((server): server is string => !!server) ?? []);
  await notifyUsersOfEvent(env, event.room_id, event.event_id, event.type);
  if (event.type === 'm.room.member' && event.state_key && event.content.membership !== 'join' &&
      parseUserId(event.state_key)?.serverName === env.SERVER_NAME) {
    await env.SYNC.get(env.SYNC.idFromName(event.state_key)).fetch(new Request('https://internal/notify', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
        room_id: event.room_id, event_id: event.event_id, type: event.type, timestamp: Date.now(),
      }),
    }));
  }
}

export async function sendLocalRoomEvent(env: Env, input: LocalEventInput): Promise<PDU> {
  const built = await buildLocalRoomEvent(env, input);
  const event = input.type === 'm.room.member' && input.content.membership === 'invite'
    ? await countersignLocalRoomInvite(env, built.event, built.version) : built.event;
  await persistLocalRoomEvent(env, event, built.version);
  return event;
}

// An invite received from another server contains only preview state. Reject it
// via a resident server rather than treating that preview as trusted room state.
export async function rejectRemoteInvite(env: Env, roomId: string, userId: string, servers: string[], reason?: string): Promise<void> {
  const room = await getRoom(env.DB, roomId);
  if (!room || !FEDERATED_ROOM_VERSIONS.includes(room.room_version)) throw Errors.unsupportedRoomVersion();
  const key = await getServerSigningKey(env.DB);
  if (!key) throw new Error('Server signing key unavailable');
  const membership = await env.DB.prepare('SELECT event_id FROM room_memberships WHERE room_id=? AND user_id=?')
    .bind(roomId, userId).first<{ event_id: string }>();
  const invite = membership ? await getEvent(env.DB, membership.event_id) : null;
  const inviter = invite ? parseUserId(invite.sender)?.serverName : undefined;
  const peers = [...new Set([inviter, ...servers])].filter((server): server is string => !!server && server !== env.SERVER_NAME).slice(0, 5);
  let failure: unknown = Errors.notFound('No resident server available to reject the invitation');
  const deadline = AbortSignal.timeout(25000);
  for (const server of peers) {
    try {
      const response = await remoteJson(await makeFederationRequest('GET', server,
        `/_matrix/federation/v1/make_leave/${encodeURIComponent(roomId)}/${encodeURIComponent(userId)}`,
        env.SERVER_NAME, key, env.CACHE, undefined, deadline));
      const raw = response.event;
      if ((response.room_version !== undefined && response.room_version !== room.room_version) || !isObject(raw) ||
          raw.room_id !== roomId || raw.sender !== userId || raw.state_key !== userId || raw.type !== 'm.room.member' ||
          !isObject(raw.content) || raw.content.membership !== 'leave' ||
          !Number.isSafeInteger(raw.depth) || Number(raw.depth) < 0 ||
          !Array.isArray(raw.auth_events) || raw.auth_events.length > 10 || !raw.auth_events.every(id => typeof id === 'string') ||
          !Array.isArray(raw.prev_events) || raw.prev_events.length > 20 || !raw.prev_events.every(id => typeof id === 'string')) {
        throw new Error('Invalid make_leave template');
      }
      const signed = await signEvent({ type: raw.type, sender: userId, room_id: roomId, state_key: userId,
        depth: Number(raw.depth), auth_events: raw.auth_events as string[], prev_events: raw.prev_events as string[],
        content: { membership: 'leave', ...(reason ? { reason } : {}) }, origin_server_ts: Date.now() },
        room.room_version, env.SERVER_NAME, key);
      const eventId = await eventReferenceId(signed, room.room_version);
      await remoteJson(await makeFederationRequest('PUT', server,
        `/_matrix/federation/v2/send_leave/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`,
        env.SERVER_NAME, key, env.CACHE, signed, deadline));
      await persistLocalRoomEvent(env, { ...signed, room_id: roomId, event_id: eventId } as PDU, room.room_version);
      return;
    } catch (error) { failure = error; }
    if (deadline.aborted) break;
  }
  throw failure;
}
