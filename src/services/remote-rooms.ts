import type { Env, PDU } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { parseRoomAlias, parseRoomId, parseUserId } from '../utils/ids';
import { canonicalJson } from '../utils/crypto';
import { getMembership, getRoomByAlias, notifyUsersOfEvent } from './database';
import { invalidateRoomCache } from './room-cache';
import { checkEventAuth } from './event-auth';
import { federationGet, makeFederationRequest, getServerSigningKey } from './federation-keys';
import { assertFederationServer, readFederationJson } from './federation-http';
import { eventReferenceId, eventVerifier, FEDERATED_ROOM_VERSIONS, isObject, signEvent, type WireEvent } from './federation-events';

export interface RoomLocation { room_id: string; servers: string[] }

function candidateServers(servers: unknown[], local: string): string[] {
  return [...new Set(servers.filter((value): value is string => {
    if (typeof value !== 'string' || value === local) return false;
    try { assertFederationServer(value); return true; } catch { return false; }
  }))].slice(0, 5);
}

async function peerJson(response: Response, limit?: number): Promise<Record<string, unknown>> {
  const body = await readFederationJson(response, limit);
  if (!response.ok) {
    if (isObject(body) && body.errcode === 'M_UNSUPPORTED_ROOM_VERSION') throw Errors.unsupportedRoomVersion();
    if (response.status === 403) throw Errors.forbidden('The remote server refused access to this room');
    if (response.status === 404) throw Errors.notFound('The remote server could not find this room');
    throw new MatrixApiError('M_UNKNOWN', `Remote server returned HTTP ${response.status}`, 502);
  }
  if (!isObject(body)) throw new Error('Invalid federation response');
  return body;
}

export async function resolveRoomAlias(env: Env, alias: string): Promise<RoomLocation> {
  const parsed = parseRoomAlias(alias);
  if (!parsed) throw Errors.invalidParam('room_alias');
  assertFederationServer(parsed.serverName);
  if (parsed.serverName === env.SERVER_NAME) {
    const room_id = await getRoomByAlias(env.DB, alias);
    if (!room_id) throw Errors.notFound('Room alias not found');
    return { room_id, servers: [env.SERVER_NAME] };
  }
  const body = await peerJson(await federationGet(parsed.serverName,
    `/_matrix/federation/v1/query/directory?room_alias=${encodeURIComponent(alias)}`,
    env.SERVER_NAME, env.DB, env.CACHE));
  if (typeof body.room_id !== 'string' || !/^![^\s/?#]+$/.test(body.room_id) || !Array.isArray(body.servers)) {
    throw new Error('Invalid remote room directory response');
  }
  const servers = candidateServers([...body.servers, parsed.serverName], env.SERVER_NAME);
  if (!servers.length) throw Errors.notFound('No reachable servers for this room');
  await env.CACHE.put(`federation:via:${body.room_id}`, JSON.stringify(servers), { expirationTtl: 300 });
  return { room_id: body.room_id, servers };
}

export async function locateRoom(env: Env, idOrAlias: string, via: string[] = []): Promise<RoomLocation> {
  if (idOrAlias.startsWith('#')) {
    const location = await resolveRoomAlias(env, idOrAlias);
    return { ...location, servers: candidateServers([...location.servers, ...via], env.SERVER_NAME) };
  }
  if (!/^![^\s/?#]+$/.test(idOrAlias)) throw Errors.invalidParam('room_id');
  const cached = await env.CACHE.get(`federation:via:${idOrAlias}`);
  const saved: unknown = cached ? JSON.parse(cached) : [];
  const resident = await env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id = ? AND membership = 'join' LIMIT 1000")
    .bind(idOrAlias).all<{ user_id: string }>();
  const inviters = await env.DB.prepare("SELECT e.sender FROM room_memberships rm JOIN events e ON e.event_id=rm.event_id WHERE rm.room_id=? AND rm.membership='invite'")
    .bind(idOrAlias).all<{sender:string}>();
  return { room_id: idOrAlias, servers: candidateServers([
    ...via, ...(Array.isArray(saved) ? saved : []), ...inviters.results.map(e => parseUserId(e.sender)?.serverName),
    ...resident.results.map(m => parseUserId(m.user_id)?.serverName), parseRoomId(idOrAlias)?.serverName,
  ], env.SERVER_NAME) };
}

export async function remoteRoomSummary(env: Env, location: RoomLocation): Promise<Record<string, unknown>> {
  let failure: unknown = Errors.notFound('Room not found; provide a via server');
  for (const server of location.servers) {
    try {
      const body = await peerJson(await federationGet(server,
        `/_matrix/federation/v1/hierarchy/${encodeURIComponent(location.room_id)}?suggested_only=true`,
        env.SERVER_NAME, env.DB, env.CACHE));
      if (!isObject(body.room) || body.room.room_id !== location.room_id) throw new Error('Invalid remote summary');
      // Only expose the public summary, never a remote membership claim about our user.
      const summary: Record<string, unknown> = { room_id: location.room_id, membership: 'leave' };
      for (const key of ['name', 'topic', 'avatar_url', 'canonical_alias', 'num_joined_members', 'join_rule',
        'world_readable', 'guest_can_join', 'room_type', 'room_version', 'encryption']) {
        if (body.room[key] !== undefined) summary[key] = body.room[key];
      }
      return summary;
    } catch (error) { failure = error; }
  }
  throw failure;
}

// Check auth dependencies against their own signed auth events, not the current state.
export function validateJoinGraph(events: PDU[], state: PDU[], join: PDU, version: string): PDU {
  const byId = new Map(events.map(event => [event.event_id, event]));
  byId.set(join.event_id, join);
  const createEvents = events.filter(e => e.type === 'm.room.create' && e.state_key === '');
  const creates = [...new Map(createEvents.map(e => [e.event_id, e])).values()];
  if (creates.length !== 1 || creates[0].content.room_version !== version) throw new Error('Missing or conflicting create event');
  const create = creates[0];
  const completed = new Set<string>();
  const visiting = new Set<string>();
  function visit(event: PDU, depth = 0) {
    if (completed.has(event.event_id)) return;
    if (depth > 1000 || visiting.has(event.event_id)) throw new Error('Invalid cyclic auth chain');
    visiting.add(event.event_id);
    const auth: PDU[] = [];
    const pairs = new Set<string>();
    for (const id of event.auth_events) {
      const dependency = byId.get(id);
      if (!dependency) throw new Error(`Incomplete auth chain: ${id}`);
      if (dependency.room_id !== join.room_id) throw new Error('Auth event from another room');
      const pair = `${dependency.type}\0${dependency.state_key}`;
      if (pairs.has(pair)) throw new Error('Duplicate auth event type and state key');
      pairs.add(pair);
      if (!['m.room.create', 'm.room.member', 'm.room.power_levels', 'm.room.join_rules', 'm.room.third_party_invite'].includes(dependency.type) ||
          (version === '12' && dependency.type === 'm.room.create')) throw new Error('Invalid auth event type');
      visit(dependency, depth + 1);
      auth.push(dependency);
    }
    if (version === '12' && event.type !== 'm.room.create') { visit(create, depth + 1); auth.push(create); }
    const result = checkEventAuth(event, auth, version);
    if (!result.allowed) throw new Error(`Event authorization failed: ${result.error}`);
    visiting.delete(event.event_id);
    completed.add(event.event_id);
  }
  for (const event of events) visit(event);
  visit(join);
  const pairs = new Set<string>();
  for (const event of state) {
    if (event.state_key === undefined) throw new Error('Non-state event in join state');
    const pair = `${event.type}\0${event.state_key}`;
    if (pairs.has(pair)) throw new Error('Duplicate current state');
    pairs.add(pair);
  }
  const currentAuth = checkEventAuth(join, state, version);
  if (!currentAuth.allowed) throw new Error(`Join rejected by room state: ${currentAuth.error}`);
  return create;
}

export async function persistRemoteJoin(env: Env, version: string, events: PDU[], state: PDU[], join: PDU, create: PDU) {
  const db = env.DB;
  const statements = [db.prepare(`INSERT INTO rooms(room_id,room_version,creator_id,is_public,created_at)
    VALUES (?,?,?,0,?) ON CONFLICT(room_id) DO UPDATE SET room_version=excluded.room_version,creator_id=excluded.creator_id`).bind(join.room_id, version, create.sender, Date.now())];
  for (const event of new Map([...events, join].map(e => [e.event_id, e])).values()) {
    statements.push(db.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,state_key,content,
      origin_server_ts,depth,auth_events,prev_events,hashes,signatures,redacts,stream_ordering)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,${event.event_id === join.event_id ? '(SELECT COALESCE(MAX(stream_ordering),0)+1 FROM events)' : 'NULL'})
      ON CONFLICT(event_id) DO NOTHING`).bind(event.event_id, join.room_id, event.sender, event.type,
        event.state_key ?? null, JSON.stringify(event.content), event.origin_server_ts, event.depth,
        JSON.stringify(event.auth_events), JSON.stringify(event.prev_events), JSON.stringify(event.hashes), JSON.stringify(event.signatures), event.redacts ?? null));
  }
  // Only the returned current state updates room_state; historic auth events cannot replace it.
  for (const event of [...state, join]) {
    statements.push(db.prepare(`INSERT INTO room_state(room_id,event_type,state_key,event_id) VALUES (?,?,?,?)
      ON CONFLICT(room_id,event_type,state_key) DO UPDATE SET event_id=excluded.event_id`)
      .bind(join.room_id, event.type, event.state_key!, event.event_id));
    if (event.type === 'm.room.member') {
      statements.push(db.prepare(`INSERT INTO room_memberships(room_id,user_id,membership,event_id,display_name,avatar_url)
        VALUES (?,?,?,?,?,?) ON CONFLICT(room_id,user_id) DO UPDATE SET membership=excluded.membership,
        event_id=excluded.event_id,display_name=excluded.display_name,avatar_url=excluded.avatar_url`)
        .bind(join.room_id, event.state_key!, event.content.membership, event.event_id,
          typeof event.content.displayname === 'string' ? event.content.displayname : null,
          typeof event.content.avatar_url === 'string' ? event.content.avatar_url : null));
    }
  }
  // D1 batch is transactional: a failed import must never expose a partial room to sync.
  await db.batch(statements);
  await invalidateRoomCache(env.CACHE, join.room_id);
}

export async function joinRemoteRoom(env: Env, location: RoomLocation, userId: string, reason?: string): Promise<string> {
  const roomId = location.room_id;
  if (parseUserId(userId)?.serverName !== env.SERVER_NAME) throw Errors.forbidden('Only local users can join');
  if ((await getMembership(env.DB, roomId, userId))?.membership === 'join') return roomId;
  if (!location.servers.length) throw Errors.notFound('No remote server supplied for this room; use its alias or a via parameter');
  const lockId = crypto.randomUUID();
  const lock = await env.DB.prepare(`INSERT INTO federation_join_locks(room_id,lock_id,expires_at) VALUES (?,?,?)
    ON CONFLICT(room_id) DO UPDATE SET lock_id=excluded.lock_id,expires_at=excluded.expires_at
    WHERE federation_join_locks.expires_at < ? RETURNING lock_id`)
    .bind(roomId, lockId, Date.now() + 600000, Date.now()).first<{ lock_id: string }>();
  if (!lock) throw Errors.limitExceeded('A join for this room is already in progress', 3000);
  try {
    const key = await getServerSigningKey(env.DB);
    if (!key) throw new Error('Server signing key unavailable');
    const profile = await env.DB.prepare('SELECT display_name,avatar_url FROM users WHERE user_id = ?')
      .bind(userId).first<{ display_name: string | null; avatar_url: string | null }>();
    const pendingKey = `federation:pending-join:${roomId}:${userId}`;
    const cached = await env.CACHE.get(pendingKey);
    const pending = cached ? JSON.parse(cached) as {version:string;signed:WireEvent;response:Record<string,unknown>;server:string} : null;
    let failure: unknown = Errors.notFound('No server could complete the join');
    for (const server of pending ? [pending.server] : location.servers) {
      let sent = false;
      try {
        let version: string, signed: WireEvent, response: Record<string, unknown>;
        if (pending) {
          ({ version, signed, response } = pending);
          sent = true;
        } else {
          const base = `${encodeURIComponent(roomId)}/${encodeURIComponent(userId)}`;
          const query = FEDERATED_ROOM_VERSIONS.map(v => `ver=${v}`).join('&');
          const template = await peerJson(await makeFederationRequest('GET', server,
            `/_matrix/federation/v1/make_join/${base}?${query}`, env.SERVER_NAME, key, env.CACHE));
          version = String(template.room_version ?? '1');
          if (!FEDERATED_ROOM_VERSIONS.includes(version)) throw Errors.unsupportedRoomVersion(`Remote joins support room versions ${FEDERATED_ROOM_VERSIONS.join(', ')}`);
          const raw = template.event;
          if (!isObject(raw) || raw.room_id !== roomId || raw.sender !== userId || raw.state_key !== userId ||
              raw.type !== 'm.room.member' || !isObject(raw.content) || raw.content.membership !== 'join' ||
              !Array.isArray(raw.auth_events) || !raw.auth_events.every(id => typeof id === 'string') || raw.auth_events.length > 10 ||
              !Array.isArray(raw.prev_events) || !raw.prev_events.every(id => typeof id === 'string') || raw.prev_events.length > 20 ||
              !Number.isSafeInteger(raw.depth) || Number(raw.depth) < 0) throw new Error('Invalid make_join template');
          const content = { ...raw.content };
          if (profile?.display_name) content.displayname = profile.display_name;
          if (profile?.avatar_url) content.avatar_url = profile.avatar_url;
          if (reason) content.reason = reason;
          signed = await signEvent({ ...raw, content, origin_server_ts: Date.now() } as WireEvent, version, env.SERVER_NAME, key);
          const joinId = await eventReferenceId(signed, version);
          // Once sent, do not retry through another resident with a different event.
          sent = true;
          response = await peerJson(await makeFederationRequest('PUT', server,
            `/_matrix/federation/v2/send_join/${encodeURIComponent(roomId)}/${encodeURIComponent(joinId)}?omit_members=false`,
            env.SERVER_NAME, key, env.CACHE, signed), 12 * 1024 * 1024);
          // Preserve the accepted handshake across key lookup/import failures; retry validation, not membership.
          await env.CACHE.put(pendingKey, JSON.stringify({version,signed,response,server}), {expirationTtl:3600});
        }
        const joinId = await eventReferenceId(signed, version);
        if (!Array.isArray(response.state) || !Array.isArray(response.auth_chain) || response.members_omitted === true ||
            response.state.length + response.auth_chain.length > 20000) throw new Error('Invalid or incomplete send_join response');
        const returned = response.event ?? signed;
        if (!isObject(returned) || await eventReferenceId(returned as WireEvent, version) !== joinId ||
            canonicalJson((returned as WireEvent).content) !== canonicalJson(signed.content)) throw new Error('Remote server changed the join event');
        const verify = eventVerifier(env);
        const rawEvents = [...response.auth_chain, ...response.state];
        const validated: PDU[] = [];
        // Bounded concurrency avoids overwhelming peers and Workers connection limits.
        for (let i = 0; i < rawEvents.length; i += 4) {
          validated.push(...await Promise.all(rawEvents.slice(i, i + 4).map(e => verify(e, version, roomId))));
        }
        const join = await verify(returned, version, roomId);
        const state = validated.slice(response.auth_chain.length);
        const create = validateJoinGraph(validated, state, join, version);
        await persistRemoteJoin(env, version, validated, state, join, create);
        await env.CACHE.delete(pendingKey);
        await notifyUsersOfEvent(env, roomId, joinId, 'm.room.member');
        return roomId;
      } catch (error) {
        failure = error;
        if (sent) throw error;
      }
    }
    throw failure;
  } finally {
    await env.DB.prepare('DELETE FROM federation_join_locks WHERE room_id = ? AND lock_id = ?').bind(roomId, lockId).run();
  }
}
