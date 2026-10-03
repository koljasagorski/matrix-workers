// Matrix room endpoints

import { Hono } from 'hono';
import type { AppEnv, Env, PDU } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { optionalAuth, requireAuth } from '../middleware/auth';
import { generateRoomId, formatRoomAlias, parseUserId } from '../utils/ids';
import { getDefaultRoomVersion } from '../services/room-versions';
import {
  createRoom,
  getRoom,
  storeEvent,
  getRoomState,
  getStateEvent,
  getRoomEvents,
  getMembership,
  getUserRooms,
  getRoomMembers,
  createRoomAlias,
  getRoomByAlias,
  deleteRoomAlias,
  getEvent,
  notifyUsersOfEvent,
} from '../services/database';
import { FEDERATED_ROOM_VERSIONS, isObject, redactEvent, wireEvent } from '../services/federation-events';
import { queueRoomEvent } from '../services/federation-delivery';
import { getTransaction, storeTransaction } from '../services/transactions';
import { buildLocalRoomEvent, persistLocalRoomEvent, rejectRemoteInvite, sendLocalRoomEvent } from '../services/local-room-events';
import { resolveRoomAlias, locateRoom, joinRemoteRoom, remoteRoomSummary } from '../services/remote-rooms';
import { getRemoteHistory, getHistoricalEvent } from '../services/room-history';
import { requireAliasDeleteAccess, requireLocalRoomAlias } from '../services/room-alias-access';

const app = new Hono<AppEnv>({ strict: false });
app.onError((error, c) => {
  if (error instanceof MatrixApiError) return error.toResponse();
  console.error('[rooms] Request failed:', error);
  return c.json({ errcode: 'M_UNKNOWN', error: 'Room request failed; please retry' }, 502);
});

// Validation for initial_state events
interface StateEventValidation {
  valid: boolean;
  error?: string;
}

function validateStateEvent(event: any, index: number): StateEventValidation {
  // Must be an object
  if (!event || typeof event !== 'object') {
    return { valid: false, error: `initial_state[${index}]: must be an object` };
  }

  // Must have a type property that is a non-empty string
  if (!event.type || typeof event.type !== 'string' || event.type.trim() === '') {
    return { valid: false, error: `initial_state[${index}]: missing or invalid 'type' property` };
  }

  // state_key must be a string if provided (can be empty string)
  if (event.state_key !== undefined && typeof event.state_key !== 'string') {
    return { valid: false, error: `initial_state[${index}]: 'state_key' must be a string` };
  }

  // content must be an object
  if (!event.content || typeof event.content !== 'object' || Array.isArray(event.content)) {
    return { valid: false, error: `initial_state[${index}]: missing or invalid 'content' property` };
  }

  // Disallow certain event types that are created automatically
  const disallowedTypes = ['m.room.create', 'm.room.member', 'm.room.power_levels'];
  if (disallowedTypes.includes(event.type)) {
    return { valid: false, error: `initial_state[${index}]: '${event.type}' cannot be set via initial_state` };
  }

  // Validate m.room.encryption content
  if (event.type === 'm.room.encryption') {
    if (!event.content.algorithm || typeof event.content.algorithm !== 'string') {
      return { valid: false, error: `initial_state[${index}]: m.room.encryption requires 'algorithm'` };
    }
    // Only m.megolm.v1.aes-sha2 is widely supported
    const supportedAlgorithms = ['m.megolm.v1.aes-sha2'];
    if (!supportedAlgorithms.includes(event.content.algorithm)) {
      return { valid: false, error: `initial_state[${index}]: unsupported algorithm '${event.content.algorithm}'` };
    }
  }

  return { valid: true };
}

// Initial room state uses the same authorization and signatures as later events.
async function createInitialRoomEvents(
  env: Env,
  createEvent: PDU,
  roomVersion: string,
  options: {
    name?: string;
    topic?: string;
    preset?: string;
    is_direct?: boolean;
    initial_state?: Array<{ type: string; state_key?: string; content: Record<string, unknown> }>;
    invite?: string[];
  }
): Promise<string> {
  const roomId = createEvent.room_id;
  const creatorId = createEvent.sender;
  await persistLocalRoomEvent(env, createEvent, roomVersion);
  async function createEventInRoom(type: string, content: Record<string, unknown>, stateKey = '') {
    return sendLocalRoomEvent(env, { roomId, sender: creatorId, type, content, stateKey });
  }
  const creator = await env.DB.prepare('SELECT display_name,avatar_url FROM users WHERE user_id=?')
    .bind(creatorId).first<{ display_name: string | null; avatar_url: string | null }>();
  await createEventInRoom('m.room.member', {
    membership: 'join',
    ...(creator?.display_name ? { displayname: creator.display_name } : {}),
    ...(creator?.avatar_url ? { avatar_url: creator.avatar_url } : {}),
  }, creatorId);
  const preset = options.preset || 'private_chat';
  const users: Record<string, number> = roomVersion === '12' ? {} : { [creatorId]: 100 };
  const creators = roomVersion === '12' ? [creatorId, ...(Array.isArray(createEvent.content.additional_creators) ? createEvent.content.additional_creators : [])] : [];
  if (preset === 'trusted_private_chat') for (const invitee of options.invite ?? []) {
    if (invitee !== creatorId && !creators.includes(invitee)) users[invitee] = 100;
  }
  await createEventInRoom('m.room.power_levels', {
    ban: 50, events: {
      'm.room.avatar': 50, 'm.room.canonical_alias': 50, 'm.room.encryption': 100,
      'm.room.history_visibility': 100, 'm.room.name': 50, 'm.room.power_levels': 100,
      'm.room.server_acl': 100, 'm.room.tombstone': 100, 'm.call.member': 0,
    }, events_default: 0, invite: preset === 'public_chat' ? 0 : 50, kick: 50,
    notifications: { room: 50 }, redact: 50, state_default: 50, users, users_default: 0,
  });
  await createEventInRoom('m.room.join_rules', { join_rule: preset === 'public_chat' ? 'public' : 'invite' });
  await createEventInRoom('m.room.history_visibility', { history_visibility: 'shared' });
  await createEventInRoom('m.room.guest_access', { guest_access: preset === 'public_chat' ? 'can_join' : 'forbidden' });
  if (options.name) await createEventInRoom('m.room.name', { name: options.name });
  if (options.topic) await createEventInRoom('m.room.topic', { topic: options.topic });
  for (const state of options.initial_state ?? []) await createEventInRoom(state.type, state.content, state.state_key ?? '');
  // A valid room can still be created if an optional initial invite is refused.
  // Explicit /invite requests propagate the refusal to the caller.
  for (const invitee of options.invite ?? []) {
    try {
      await createEventInRoom('m.room.member', { membership: 'invite', ...(options.is_direct ? { is_direct: true } : {}) }, invitee);
    } catch (error) { console.error(`[createRoom] Invitation to ${invitee} failed:`, error); }
  }
  return createEvent.event_id;
}

// POST /_matrix/client/v3/createRoom - Create a new room
app.post('/_matrix/client/v3/createRoom', requireAuth(), async (c) => {
  const userId = c.get('userId');

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }
  if (!isObject(body)) return Errors.badJson().toResponse();

  const {
    room_alias_local_part,
    name,
    topic,
    invite,
    room_version,
    initial_state,
    preset,
    is_direct,
    visibility,
  } = body as {
    room_alias_local_part?: string; name?: string; topic?: string; invite?: string[]; room_version?: string;
    initial_state?: Array<{ type: string; state_key?: string; content: Record<string, unknown> }>;
    preset?: string; is_direct?: boolean; visibility?: string;
  };
  const creationContent = body.creation_content ?? {};
  if (!isObject(creationContent)) return Errors.invalidParam('creation_content').toResponse();
  if (creationContent['m.federate'] !== undefined && typeof creationContent['m.federate'] !== 'boolean') {
    return Errors.invalidParam('creation_content').toResponse();
  }
  if (invite !== undefined && (!Array.isArray(invite) || invite.some(id => typeof id !== 'string' || !parseUserId(id)))) {
    return Errors.invalidParam('invite').toResponse();
  }
  if (preset !== undefined && !['private_chat', 'trusted_private_chat', 'public_chat'].includes(preset)) return Errors.invalidParam('preset').toResponse();
  if (body.is_direct !== undefined && typeof body.is_direct !== 'boolean') return Errors.invalidParam('is_direct').toResponse();
  if (body.visibility !== undefined && !['public', 'private'].includes(String(body.visibility))) return Errors.invalidParam('visibility').toResponse();
  for (const field of ['name', 'topic', 'room_alias_local_part']) {
    if (body[field] !== undefined && typeof body[field] !== 'string') return Errors.invalidParam(field).toResponse();
  }
  // Third-party invites are not implemented.
  void body.invite_3pid;

  // Validate room alias if provided
  if (room_alias_local_part) {
    const alias = formatRoomAlias(room_alias_local_part, c.env.SERVER_NAME);
    const existingRoom = await getRoomByAlias(c.env.DB, alias);
    if (existingRoom) {
      return Errors.roomInUse().toResponse();
    }
  }

  // Validate initial_state if provided
  if (initial_state !== undefined) {
    if (!Array.isArray(initial_state)) {
      return c.json({
        errcode: 'M_INVALID_PARAM',
        error: 'initial_state must be an array',
      }, 400);
    }

    // Check for duplicate encryption events
    const encryptionEvents = initial_state.filter((s: unknown) => isObject(s) && s.type === 'm.room.encryption');
    if (encryptionEvents.length > 1) {
      return c.json({
        errcode: 'M_INVALID_PARAM',
        error: 'Cannot specify multiple m.room.encryption events in initial_state',
      }, 400);
    }

    // Validate each state event
    for (let i = 0; i < initial_state.length; i++) {
      const validation = validateStateEvent(initial_state[i], i);
      if (!validation.valid) {
        return c.json({
          errcode: 'M_INVALID_PARAM',
          error: validation.error,
        }, 400);
      }
    }
  }

  // Validate room version
  const version = room_version || getDefaultRoomVersion();
  if (typeof version !== 'string' || !FEDERATED_ROOM_VERSIONS.includes(version)) {
    return Errors.unsupportedRoomVersion(`Room version '${version}' is not supported`).toResponse();
  }

  // Generate room ID
  let roomId = await generateRoomId(c.env.SERVER_NAME);
  const built = await buildLocalRoomEvent(c.env, { roomId, sender: userId, type: 'm.room.create', stateKey: '',
    content: { ...creationContent, room_version: version, ...(version === '10' ? { creator: userId } : {}) } }, version);
  if (version === '12') roomId = `!${built.event.event_id.slice(1)}`;
  built.event.room_id = roomId;

  console.log('[createRoom] Creating room:', roomId, 'for user:', userId);

  // Create room in database
  const isPublic = visibility === 'public';
  await createRoom(c.env.DB, roomId, version, userId, isPublic);
  console.log('[createRoom] Room record created in DB');

  // Create initial room events
  let createEventId: string | undefined;
  try {
    createEventId = await createInitialRoomEvents(c.env, built.event, version, {
      name,
      topic,
      preset,
      is_direct,
      initial_state,
      invite,
    });
    console.log('[createRoom] Initial room events created successfully');

    // Initialize m.fully_read marker for the room creator
    // This ensures the room doesn't show all messages as unread
    await c.env.DB.prepare(`
      INSERT INTO account_data (user_id, room_id, event_type, content)
      VALUES (?, ?, 'm.fully_read', ?)
      ON CONFLICT (user_id, room_id, event_type) DO UPDATE SET content = excluded.content
    `).bind(userId, roomId, JSON.stringify({ event_id: createEventId })).run();
    console.log('[createRoom] Initialized m.fully_read marker for creator');
  } catch (err) {
    console.error('[createRoom] Failed to create initial room events:', err);
    await c.env.DB.batch([
      c.env.DB.prepare('DELETE FROM room_state WHERE room_id=?').bind(roomId),
      c.env.DB.prepare('DELETE FROM events WHERE room_id=?').bind(roomId),
      c.env.DB.prepare('DELETE FROM rooms WHERE room_id=?').bind(roomId),
    ]);
    throw err;
  }

  // Create room alias if provided
  if (room_alias_local_part) {
    const alias = formatRoomAlias(room_alias_local_part, c.env.SERVER_NAME);
    await createRoomAlias(c.env.DB, alias, roomId, userId);
  }

  // Notify the creator's sync that the room was created
  await notifyUsersOfEvent(c.env, roomId, roomId, 'm.room.create');

  return c.json({
    room_id: roomId,
    room_alias: room_alias_local_part
      ? formatRoomAlias(room_alias_local_part, c.env.SERVER_NAME)
      : undefined,
  });
});

// GET /_matrix/client/v3/joined_rooms - List joined rooms
app.get('/_matrix/client/v3/joined_rooms', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const rooms = await getUserRooms(c.env.DB, userId, 'join');
  return c.json({ joined_rooms: rooms });
});

// POST /_matrix/client/v3/rooms/:roomId/join - Join a room
app.post('/_matrix/client/v3/rooms/:roomId/join', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');

  const room = await getRoom(c.env.DB, roomId);
  if (!room || (room.creator_id && !room.creator_id.endsWith(`:${c.env.SERVER_NAME}`) &&
      (await getMembership(c.env.DB, roomId, userId))?.membership !== 'join')) {
    const location = await locateRoom(c.env, roomId, [...(c.req.queries('via') ?? []), ...(c.req.queries('server_name') ?? [])]);
    const body = await c.req.json<{ reason?: string }>().catch(() => ({} as { reason?: string }));
    return c.json({ room_id: await joinRemoteRoom(c.env, location, userId, typeof body.reason === 'string' ? body.reason : undefined) });
  }

  // Check current membership
  const currentMembership = await getMembership(c.env.DB, roomId, userId);
  if (currentMembership?.membership === 'join') return c.json({ room_id: roomId });

  // Check join rules
  const joinRulesEvent = await getStateEvent(c.env.DB, roomId, 'm.room.join_rules');
  const joinRule = (joinRulesEvent?.content as any)?.join_rule || 'invite';

  // Determine if user can join
  let canJoin = false;
  if (['public', 'restricted', 'knock_restricted'].includes(joinRule)) {
    canJoin = true;
  } else if (currentMembership?.membership === 'invite') {
    canJoin = true;
  }

  if (!canJoin) {
    return Errors.forbidden('Cannot join room').toResponse();
  }

  const profile = await c.env.DB.prepare('SELECT display_name,avatar_url FROM users WHERE user_id=?')
    .bind(userId).first<{ display_name: string | null; avatar_url: string | null }>();
  await sendLocalRoomEvent(c.env, {
    roomId, sender: userId, type: 'm.room.member', stateKey: userId, content: {
      membership: 'join', ...(profile?.display_name ? { displayname: profile.display_name } : {}),
      ...(profile?.avatar_url ? { avatar_url: profile.avatar_url } : {}),
    },
  });

  return c.json({ room_id: roomId });
});

// POST /_matrix/client/v3/rooms/:roomId/leave - Leave or reject an invitation
app.post('/_matrix/client/v3/rooms/:roomId/leave', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (membership?.membership === 'leave') return c.json({});
  if (!membership || !['join', 'invite', 'knock'].includes(membership.membership)) {
    return Errors.forbidden('Not joined, invited, or knocking in this room').toResponse();
  }
  const body: unknown = await c.req.json().catch(() => ({}));
  if (!isObject(body) || (body.reason !== undefined && typeof body.reason !== 'string')) return Errors.badJson().toResponse();
  if (membership.membership !== 'join' && !await getStateEvent(c.env.DB, roomId, 'm.room.create')) {
    const location = await locateRoom(c.env, roomId);
    await rejectRemoteInvite(c.env, roomId, userId, location.servers, body.reason as string | undefined);
  } else {
    await sendLocalRoomEvent(c.env, { roomId, sender: userId, type: 'm.room.member', stateKey: userId,
      content: { membership: 'leave', ...(body.reason ? { reason: body.reason } : {}) } });
  }
  return c.json({});
});

// POST /_matrix/client/v3/rooms/:roomId/knock - Knock on a room (MSC2403)
app.post('/_matrix/client/v3/rooms/:roomId/knock', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  let body: { reason?: string };
  try {
    body = await c.req.json();
  } catch {
    body = {};
  }

  // Check if room exists
  const room = await getRoom(db, roomId);
  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Check current membership
  const currentMembership = await getMembership(db, roomId, userId);
  if (currentMembership?.membership === 'join') {
    return c.json({ room_id: roomId }); // Already joined
  }
  if (currentMembership?.membership === 'ban') {
    return Errors.forbidden('User is banned from this room').toResponse();
  }

  // Check join rules - knock only allowed if join_rule is 'knock' or 'knock_restricted'
  const joinRulesEvent = await getStateEvent(db, roomId, 'm.room.join_rules');
  const joinRule = (joinRulesEvent?.content as any)?.join_rule || 'invite';

  if (!['knock', 'knock_restricted'].includes(joinRule)) {
    return Errors.forbidden('Room does not allow knocking').toResponse();
  }

  await sendLocalRoomEvent(c.env, { roomId, sender: userId, type: 'm.room.member', stateKey: userId,
    content: { membership: 'knock', ...(body.reason ? { reason: body.reason } : {}) } });

  return c.json({ room_id: roomId });
});

// POST /_matrix/client/v3/knock/:roomIdOrAlias - Knock by ID or alias
app.post('/_matrix/client/v3/knock/:roomIdOrAlias', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomIdOrAlias = c.req.param('roomIdOrAlias')!;
  const db = c.env.DB;

  let body: { reason?: string; server_name?: string[] };
  try {
    body = await c.req.json();
  } catch {
    body = {};
  }

  let roomId = roomIdOrAlias;

  // If it's an alias, resolve it
  if (roomIdOrAlias.startsWith('#')) {
    const resolved = await getRoomByAlias(db, roomIdOrAlias);
    if (!resolved) {
      return Errors.notFound('Room alias not found').toResponse();
    }
    roomId = resolved;
  }

  // Check if room exists
  const room = await getRoom(db, roomId);
  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Check join rules
  const joinRulesEvent = await getStateEvent(db, roomId, 'm.room.join_rules');
  const joinRule = (joinRulesEvent?.content as any)?.join_rule || 'invite';

  if (!['knock', 'knock_restricted'].includes(joinRule)) {
    return Errors.forbidden('Room does not allow knocking').toResponse();
  }

  // Check current membership
  const currentMembership = await getMembership(db, roomId, userId);
  if (currentMembership?.membership === 'join') {
    return c.json({ room_id: roomId });
  }
  if (currentMembership?.membership === 'ban') {
    return Errors.forbidden('User is banned from this room').toResponse();
  }

  const event = await sendLocalRoomEvent(c.env, { roomId, sender: userId, type: 'm.room.member', stateKey: userId,
    content: { membership: 'knock', ...(body.reason ? { reason: body.reason } : {}) } });
  const eventId = event.event_id;

  await db.prepare(`
    INSERT OR REPLACE INTO room_knocks (room_id, user_id, reason, event_id, created_at)
    VALUES (?, ?, ?, ?, ?)
  `).bind(roomId, userId, body.reason || null, eventId, Date.now()).run();

  return c.json({ room_id: roomId });
});

// GET /_matrix/client/v3/rooms/:roomId/state - Get all current state
app.get('/_matrix/client/v3/rooms/:roomId/state', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const state = await getRoomState(c.env.DB, roomId);

  // Format events for client
  const clientEvents = state.map(e => ({
    type: e.type,
    state_key: e.state_key,
    content: e.content,
    sender: e.sender,
    origin_server_ts: e.origin_server_ts,
    event_id: e.event_id,
    room_id: e.room_id,
  }));

  return c.json(clientEvents);
});

// GET /_matrix/client/v3/rooms/:roomId/state/:eventType/:stateKey? - Get specific state
for (const path of ['/_matrix/client/v3/rooms/:roomId/state/:eventType/:stateKey?', '/_matrix/client/v3/rooms/:roomId/state/:eventType/']) app.get(path, requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId')!;
  const eventType = c.req.param('eventType')!;
  const stateKey = c.req.param('stateKey') ?? '';

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const event = await getStateEvent(c.env.DB, roomId, eventType, stateKey);
  if (!event) {
    return Errors.notFound('State event not found').toResponse();
  }

  return c.json(event.content);
});

// PUT /_matrix/client/v3/rooms/:roomId/state/:eventType/:stateKey? - Set state
for (const path of ['/_matrix/client/v3/rooms/:roomId/state/:eventType/:stateKey?', '/_matrix/client/v3/rooms/:roomId/state/:eventType/']) app.put(path, requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId')!;
  const eventType = c.req.param('eventType')!;
  const stateKey = c.req.param('stateKey') ?? '';
  let content: unknown;
  try { content = await c.req.json(); } catch { return Errors.badJson().toResponse(); }
  if (!isObject(content)) return Errors.badJson('State content must be an object').toResponse();
  const event = await sendLocalRoomEvent(c.env, { roomId, sender: userId, type: eventType, stateKey, content });
  return c.json({ event_id: event.event_id });
});

// GET /_matrix/client/v3/rooms/:roomId/members - Get room members
app.get('/_matrix/client/v3/rooms/:roomId/members', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const members = await getRoomMembers(c.env.DB, roomId);

  // Get full member events - OPTIMIZED: fetch in parallel instead of sequential
  const events = await Promise.all(
    members.map(member =>
      getStateEvent(c.env.DB, roomId, 'm.room.member', member.userId)
    )
  );

  const memberEvents = events
    .filter((event): event is NonNullable<typeof event> => event !== null && event !== undefined)
    .map(event => ({
      type: event.type,
      state_key: event.state_key,
      content: event.content,
      sender: event.sender,
      origin_server_ts: event.origin_server_ts,
      event_id: event.event_id,
      room_id: event.room_id,
    }));

  return c.json({ chunk: memberEvents });
});

// GET /_matrix/client/v3/rooms/:roomId/messages - Get room messages
app.get('/_matrix/client/v3/rooms/:roomId/messages', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const from = c.req.query('from');
  const dir = c.req.query('dir') || 'b';
  if (dir !== 'b' && dir !== 'f') return Errors.invalidParam('dir').toResponse();
  const requestedLimit = Number(c.req.query('limit') || '10');
  if (!Number.isSafeInteger(requestedLimit) || requestedLimit < 0) return Errors.invalidParam('limit').toResponse();
  const limit = Math.min(requestedLimit, 100);
  let events: PDU[] = [];
  let endToken: string | undefined;

  // Parse token - handle both 's123' format (from sliding-sync) and plain '123' format
  let fromToken: number | undefined;
  if (from?.startsWith('rh_')) {
    if (dir !== 'b') return Errors.invalidParam('dir', 'Historical cursors support backwards pagination').toResponse();
    if (limit > 0) {
      const page = await getRemoteHistory(c.env, roomId, userId, from, limit);
      events = page.events;
      endToken = page.end;
    }
  } else {
    if (from) {
      const tokenStr = from.replace(/^s?(-?\d+)(?:_td\d+)?(?:_dk\d+)?(?:_rr\d+)?$/, '$1');
      if (!/^-?\d+$/.test(tokenStr) || !Number.isSafeInteger(Number(tokenStr))) return Errors.invalidParam('from').toResponse();
      fromToken = Number(tokenStr);
    }
    const page = await getRoomEvents(c.env.DB, roomId, fromToken, limit, dir);
    events = page.events;
    if (events.length) endToken = `s${page.end}`;
    if (dir === 'b' && events.length < limit) {
      const oldest = events.at(-1) ?? (await getRoomEvents(c.env.DB, roomId, undefined, 1, 'f')).events[0];
      if (oldest) {
        const anchor = await getEvent(c.env.DB, oldest.event_id);
        if (anchor) {
          const history = await getRemoteHistory(c.env, roomId, userId, anchor, limit - events.length);
          events.push(...history.events);
          endToken = history.end;
        }
      }
    }
  }

  // Format events for client
  const clientEvents = events.map(e => ({
    type: e.type,
    state_key: e.state_key,
    content: e.content,
    sender: e.sender,
    origin_server_ts: e.origin_server_ts,
    event_id: e.event_id,
    room_id: e.room_id,
    unsigned: e.unsigned,
  }));

  // Empty filtered pages can still have a continuation. Only omit 'end' once
  // there is no more history; transient federation failures return an error.
  const response: { start: string; end?: string; chunk: typeof clientEvents } = {
    start: from || 's0',
    chunk: clientEvents,
  };

  if (endToken) response.end = endToken;

  return c.json(response);
});

// GET /_matrix/client/v3/rooms/:roomId/event/:eventId - Get specific event
app.get('/_matrix/client/v3/rooms/:roomId/event/:eventId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const event = await getEvent(c.env.DB, eventId) ?? await getHistoricalEvent(c.env, roomId, userId, eventId);
  if (!event || event.room_id !== roomId) {
    return Errors.notFound('Event not found').toResponse();
  }

  return c.json({
    type: event.type,
    state_key: event.state_key,
    content: event.content,
    sender: event.sender,
    origin_server_ts: event.origin_server_ts,
    event_id: event.event_id,
    room_id: event.room_id,
    unsigned: event.unsigned,
  });
});

// PUT /_matrix/client/v3/rooms/:roomId/send/:eventType/:txnId - Send message
app.put('/_matrix/client/v3/rooms/:roomId/send/:eventType/:txnId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const eventType = c.req.param('eventType');
  const txnId = c.req.param('txnId');
  const transactionKey = `room-send:${c.get('deviceId')}:${roomId}:${eventType}:${txnId}`;
  const previous = await getTransaction(c.env.DB, userId, transactionKey);
  if (previous?.eventId) {
    const stored = await getEvent(c.env.DB, previous.eventId);
    const room = await getRoom(c.env.DB, roomId);
    if (stored && room) await queueRoomEvent(c.env, stored, room.room_version);
    return c.json({ event_id: previous.eventId });
  }
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (membership?.membership !== 'join') return Errors.forbidden('Not a member of this room').toResponse();
  const room = await getRoom(c.env.DB, roomId);
  if (!room) return Errors.notFound().toResponse();
  let content: Record<string, unknown>;
  try { content = await c.req.json(); } catch { return Errors.badJson().toResponse(); }
  if (!content || typeof content !== 'object' || Array.isArray(content)) return Errors.badJson().toResponse();
  const { event } = await buildLocalRoomEvent(c.env, { roomId, sender: userId, type: eventType,
    content, unsigned: { transaction_id: txnId } });
  await storeEvent(c.env.DB, event);
  await storeTransaction(c.env.DB, userId, transactionKey, event.event_id);
  await queueRoomEvent(c.env, event, room.room_version);
  await notifyUsersOfEvent(c.env, roomId, event.event_id, eventType);
  if (eventType === 'm.room.message' || eventType === 'm.room.encrypted') {
    c.executionCtx.waitUntil(c.env.PUSH_NOTIFICATION_WORKFLOW.create({ params: {
      eventId:event.event_id,roomId,eventType,sender:userId,content,originServerTs:event.origin_server_ts,
    } }).catch(error => console.error('[rooms] Push notification failed:',error)));
  }
  return c.json({ event_id:event.event_id });
});

// Membership management is authorized against the same state as federated PDUs.
for (const action of ['invite', 'kick', 'ban', 'unban'] as const) {
  app.post(`/_matrix/client/v3/rooms/:roomId/${action}`, requireAuth(), async (c) => {
    const sender = c.get('userId');
    const roomId = c.req.param('roomId');
    let body: unknown;
    try { body = await c.req.json(); } catch { return Errors.badJson().toResponse(); }
    if (!isObject(body)) return Errors.badJson().toResponse();
    if (body.user_id === undefined) return Errors.missingParam('user_id').toResponse();
    if (typeof body.user_id !== 'string' || !parseUserId(body.user_id)) return Errors.invalidParam('user_id').toResponse();
    if (body.reason !== undefined && typeof body.reason !== 'string') return Errors.invalidParam('reason').toResponse();
    const current = await getMembership(c.env.DB, roomId, body.user_id);
    if (action === 'invite' && current?.membership === 'invite') {
      if ((await getMembership(c.env.DB, roomId, sender))?.membership !== 'join') return Errors.forbidden().toResponse();
      return c.json({});
    }
    if (action === 'kick' && !current) return Errors.forbidden('User is not in the room').toResponse();
    if (action === 'unban' && current?.membership !== 'ban') return Errors.forbidden('User is not banned').toResponse();
    await sendLocalRoomEvent(c.env, { roomId, sender, type: 'm.room.member', stateKey: body.user_id,
      content: { membership: action === 'invite' ? 'invite' : action === 'ban' ? 'ban' : 'leave',
        ...(body.reason ? { reason: body.reason } : {}) } });
    return c.json({});
  });
}

// POST /_matrix/client/v3/rooms/:roomId/forget - Forget a room
app.post('/_matrix/client/v3/rooms/:roomId/forget', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  // Check that user has left the room
  const membership = await getMembership(db, roomId, userId);
  if (membership && membership.membership === 'join') {
    return Errors.forbidden('Cannot forget room while still a member').toResponse();
  }

  // Remove membership record entirely
  await db.prepare(`
    DELETE FROM room_memberships WHERE room_id = ? AND user_id = ?
  `).bind(roomId, userId).run();

  return c.json({});
});

// PUT /_matrix/client/v3/rooms/:roomId/redact/:eventId/:txnId - Redact an event
app.put('/_matrix/client/v3/rooms/:roomId/redact/:eventId/:txnId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const targetEventId = c.req.param('eventId');
  const txnId = c.req.param('txnId');

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Get the target event
  const targetEvent = await getEvent(c.env.DB, targetEventId);
  if (!targetEvent || targetEvent.room_id !== roomId) {
    return Errors.notFound('Event not found').toResponse();
  }

  // Check power levels for redaction
  const powerLevelsEvent = await getStateEvent(c.env.DB, roomId, 'm.room.power_levels');
  const powerLevels = powerLevelsEvent?.content as any || {};
  const room = await getRoom(c.env.DB, roomId);
  if (!room) return Errors.notFound().toResponse();
  const create = await getStateEvent(c.env.DB, roomId, 'm.room.create');
  const creators = [create?.sender, ...(Array.isArray(create?.content.additional_creators) ? create.content.additional_creators : [])];
  const userPower = room.room_version === '12' && creators.includes(userId) ? Infinity : powerLevels.users?.[userId] ?? powerLevels.users_default ?? 0;
  const redactPower = powerLevels.redact ?? 50;

  // Users can redact their own messages, or need redact power level
  if (targetEvent.sender !== userId && userPower < redactPower) {
    return Errors.forbidden('Insufficient power level to redact').toResponse();
  }

  let body: any = {};
  try {
    body = await c.req.json();
  } catch {
    // Body is optional for redaction
  }

  if (!isObject(body) || (body.reason !== undefined && typeof body.reason !== 'string')) return Errors.badJson().toResponse();
  const transactionKey = `room-redact:${c.get('deviceId')}:${roomId}:${targetEventId}:${txnId}`;
  const previous = await getTransaction(c.env.DB, userId, transactionKey);
  if (previous?.eventId) {
    const stored = await getEvent(c.env.DB, previous.eventId);
    if (stored) await queueRoomEvent(c.env, stored, room.room_version);
    return c.json({ event_id: previous.eventId });
  }
  const { event } = await buildLocalRoomEvent(c.env, { roomId, sender: userId, type: 'm.room.redaction',
    content: { ...(room.room_version === '10' ? {} : { redacts: targetEventId }), ...(body.reason ? { reason: body.reason } : {}) },
    ...(room.room_version === '10' ? { redacts: targetEventId } : {}), unsigned: { transaction_id: txnId } });
  const redacted = redactEvent(wireEvent(targetEvent, room.room_version), room.room_version);
  const unsigned = { ...targetEvent.unsigned, redacted_because: {
    type: event.type, content: event.content, sender: event.sender, event_id: event.event_id,
    room_id: roomId, origin_server_ts: event.origin_server_ts, ...(event.redacts ? { redacts: event.redacts } : {}),
  } };
  await storeEvent(c.env.DB, event, [
    c.env.DB.prepare('UPDATE events SET content=?,unsigned=? WHERE event_id=? AND room_id=?')
      .bind(JSON.stringify(redacted.content), JSON.stringify(unsigned), targetEventId, roomId),
    c.env.DB.prepare(`INSERT INTO transaction_ids(user_id,txn_id,event_id) VALUES (?,?,?)
      ON CONFLICT(user_id,txn_id) DO UPDATE SET event_id=excluded.event_id`).bind(userId, transactionKey, event.event_id),
  ]);
  await queueRoomEvent(c.env, event, room.room_version);
  await notifyUsersOfEvent(c.env, roomId, event.event_id, 'm.room.redaction');
  return c.json({ event_id: event.event_id });
});

// GET /_matrix/client/v3/rooms/:roomId/context/:eventId - Get context around an event
// NOTE: This endpoint is used by Element X NSE (Notification Service Extension) to fetch
// event content for rich push notifications. If you see this endpoint being called
// shortly after a push notification is sent, that's the NSE working correctly.
app.get('/_matrix/client/v3/rooms/:roomId/context/:eventId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');
  const limit = Math.min(parseInt(c.req.query('limit') || '10'), 100);
  const userAgent = c.req.header('User-Agent');

  // NSE Detection logging - /context is a key endpoint for push notification content
  // NSE typically requests small limit (1-5) for single event context
  const isLikelyNSE = limit <= 5;
  console.log('[rooms/context] Request:', {
    userId,
    roomId,
    eventId,
    limit,
    userAgent: userAgent?.substring(0, 100),
    isLikelyNSE,
    timestamp: new Date().toISOString(),
  });

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    console.log('[rooms/context] DENIED - not a member:', { userId, roomId, eventId });
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Get the target event
  const targetEvent = await getEvent(c.env.DB, eventId);
  if (!targetEvent || targetEvent.room_id !== roomId) {
    console.log('[rooms/context] Event not found:', { eventId, roomId, eventRoomId: targetEvent?.room_id });
    return Errors.notFound('Event not found').toResponse();
  }

  console.log('[rooms/context] Found event:', {
    eventId,
    eventType: targetEvent.type,
    sender: targetEvent.sender,
    timestamp: targetEvent.origin_server_ts,
  });

  // Get events before and after
  const halfLimit = Math.floor(limit / 2);

  const eventsBefore = await c.env.DB.prepare(`
    SELECT * FROM events WHERE room_id = ? AND origin_server_ts < ?
    ORDER BY origin_server_ts DESC LIMIT ?
  `).bind(roomId, targetEvent.origin_server_ts, halfLimit).all();

  const eventsAfter = await c.env.DB.prepare(`
    SELECT * FROM events WHERE room_id = ? AND origin_server_ts > ?
    ORDER BY origin_server_ts ASC LIMIT ?
  `).bind(roomId, targetEvent.origin_server_ts, halfLimit).all();

  // Format events
  const formatEvent = (e: any) => ({
    type: e.event_type,
    state_key: e.state_key,
    content: JSON.parse(e.content || '{}'),
    sender: e.sender,
    origin_server_ts: e.origin_server_ts,
    event_id: e.event_id,
    room_id: e.room_id,
  });

  // Get current state
  const state = await getRoomState(c.env.DB, roomId);
  const stateEvents = state.map(e => ({
    type: e.type,
    state_key: e.state_key,
    content: e.content,
    sender: e.sender,
    origin_server_ts: e.origin_server_ts,
    event_id: e.event_id,
    room_id: e.room_id,
  }));

  return c.json({
    event: formatEvent(targetEvent),
    events_before: eventsBefore.results.reverse().map(formatEvent),
    events_after: eventsAfter.results.map(formatEvent),
    state: stateEvents,
    start: eventsBefore.results.length > 0 ? String(eventsBefore.results[0].origin_server_ts) : undefined,
    end: eventsAfter.results.length > 0 ? String(eventsAfter.results[eventsAfter.results.length - 1].origin_server_ts) : undefined,
  });
});

// GET /_matrix/client/v3/rooms/:roomId/joined_members - Get joined members with details
app.get('/_matrix/client/v3/rooms/:roomId/joined_members', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');

  // Check membership
  const membership = await getMembership(c.env.DB, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const members = await c.env.DB.prepare(`
    SELECT user_id, display_name, avatar_url
    FROM room_memberships
    WHERE room_id = ? AND membership = 'join'
  `).bind(roomId).all<{
    user_id: string;
    display_name: string | null;
    avatar_url: string | null;
  }>();

  const joined: Record<string, { display_name?: string; avatar_url?: string }> = {};
  for (const member of members.results) {
    joined[member.user_id] = {
      display_name: member.display_name || undefined,
      avatar_url: member.avatar_url || undefined,
    };
  }

  return c.json({ joined });
});

// GET /_matrix/client/v3/rooms/:roomId/aliases - Get room aliases
app.get('/_matrix/client/v3/rooms/:roomId/aliases', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  // Check membership
  const membership = await getMembership(db, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  const aliases = await db.prepare(`
    SELECT alias FROM room_aliases WHERE room_id = ?
  `).bind(roomId).all<{ alias: string }>();

  return c.json({
    aliases: aliases.results.map(a => a.alias),
  });
});

// POST /_matrix/client/v3/join/:roomIdOrAlias - Join room by ID or alias
app.post('/_matrix/client/v3/join/:roomIdOrAlias', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomIdOrAlias = c.req.param('roomIdOrAlias')!;
  const db = c.env.DB;

  const location = await locateRoom(c.env, roomIdOrAlias, [...(c.req.queries('via') ?? []), ...(c.req.queries('server_name') ?? [])]);
  const roomId = location.room_id;
  const room = await getRoom(db, roomId);
  if (!room || (room.creator_id && !room.creator_id.endsWith(`:${c.env.SERVER_NAME}`) &&
      (await getMembership(db, roomId, userId))?.membership !== 'join')) {
    const body = await c.req.json<{ reason?: string }>().catch(() => ({} as { reason?: string }));
    return c.json({ room_id: await joinRemoteRoom(c.env, location, userId, typeof body.reason === 'string' ? body.reason : undefined) });
  }

  // Check current membership
  const currentMembership = await getMembership(db, roomId, userId);
  if (currentMembership?.membership === 'join') return c.json({ room_id: roomId });

  // Check join rules
  const joinRulesEvent = await getStateEvent(db, roomId, 'm.room.join_rules');
  const joinRule = (joinRulesEvent?.content as any)?.join_rule || 'invite';

  // Determine if user can join
  let canJoin = false;
  if (['public', 'restricted', 'knock_restricted'].includes(joinRule)) {
    canJoin = true;
  } else if (currentMembership?.membership === 'invite') {
    canJoin = true;
  }

  if (!canJoin) {
    return Errors.forbidden('Cannot join room').toResponse();
  }

  const profile = await db.prepare('SELECT display_name,avatar_url FROM users WHERE user_id=?')
    .bind(userId).first<{ display_name: string | null; avatar_url: string | null }>();
  await sendLocalRoomEvent(c.env, { roomId, sender: userId, type: 'm.room.member', stateKey: userId,
    content: { membership: 'join', ...(profile?.display_name ? { displayname: profile.display_name } : {}),
      ...(profile?.avatar_url ? { avatar_url: profile.avatar_url } : {}) } });

  return c.json({ room_id: roomId });
});

// Room alias endpoints
// GET /_matrix/client/v3/directory/room/:roomAlias
app.get('/_matrix/client/v3/directory/room/:roomAlias', async (c) => {
  return c.json(await resolveRoomAlias(c.env, c.req.param('roomAlias')));
});

// PUT /_matrix/client/v3/directory/room/:roomAlias
app.put('/_matrix/client/v3/directory/room/:roomAlias', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const alias = c.req.param('roomAlias');
  requireLocalRoomAlias(c.env, alias);

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (!isObject(body)) return Errors.badJson().toResponse();
  const { room_id } = body;
  if (!room_id) {
    return Errors.missingParam('room_id').toResponse();
  }
  if (typeof room_id !== 'string') return Errors.invalidParam('room_id').toResponse();

  // Check if alias already exists
  const existing = await getRoomByAlias(c.env.DB, alias);
  if (existing) {
    return Errors.roomInUse().toResponse();
  }

  // Check if user has permission (is member of room)
  const membership = await getMembership(c.env.DB, room_id, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  await createRoomAlias(c.env.DB, alias, room_id, userId);
  return c.json({});
});

// DELETE /_matrix/client/v3/directory/room/:roomAlias
app.delete('/_matrix/client/v3/directory/room/:roomAlias', requireAuth(), async (c) => {
  const alias = c.req.param('roomAlias');
  await requireAliasDeleteAccess(c.env, alias, c.get('userId'));
  await deleteRoomAlias(c.env.DB, alias);
  return c.json({});
});

// ============================================
// Room Summary (MSC3266)
// ============================================

// GET /_matrix/client/v1/room_summary/:roomIdOrAlias - Get a summary of a room
// Allows previewing a room without joining it (if permitted by room settings)
for (const path of ['/_matrix/client/v1/room_summary/:roomIdOrAlias', '/_matrix/client/unstable/im.nheko.summary/summary/:roomIdOrAlias']) app.get(path, optionalAuth(), async (c) => {
  const roomIdOrAlias = c.req.param('roomIdOrAlias')!;
  const db = c.env.DB;

  const location = await locateRoom(c.env, roomIdOrAlias, [...(c.req.queries('via') ?? []), ...(c.req.queries('server_name') ?? [])]);
  const roomId = location.room_id;

  // Get room info
  const room = await db.prepare(
    `SELECT room_id, room_version, is_public FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string; room_version: string; is_public: number }>();

  if (!room) return c.json(await remoteRoomSummary(c.env, location));

  // Get room state events we need
  const stateEvents = await db.prepare(`
    SELECT e.event_type, e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type IN (
      'm.room.name', 'm.room.topic', 'm.room.avatar',
      'm.room.join_rules', 'm.room.canonical_alias', 'm.room.encryption',
      'm.room.history_visibility', 'm.room.guest_access'
    )
  `).bind(roomId).all<{ event_type: string; content: string }>();

  // Get member count
  const memberCount = await db.prepare(
    `SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'join'`
  ).bind(roomId).first<{ count: number }>();

  // Build response
  const response: Record<string, unknown> = {
    room_id: roomId,
    num_joined_members: memberCount?.count || 0,
    room_version: room.room_version,
  };

  // Extract state
  let joinRule = 'invite';
  let historyVisibility = 'shared';
  let worldReadable = false;
  let guestCanJoin = false;

  for (const event of stateEvents.results || []) {
    const content = JSON.parse(event.content);
    switch (event.event_type) {
      case 'm.room.name':
        response.name = content.name;
        break;
      case 'm.room.topic':
        response.topic = content.topic;
        break;
      case 'm.room.avatar':
        response.avatar_url = content.url;
        break;
      case 'm.room.join_rules':
        joinRule = content.join_rule;
        response.join_rule = joinRule;
        break;
      case 'm.room.canonical_alias':
        response.canonical_alias = content.alias;
        break;
      case 'm.room.encryption':
        response.encryption = content.algorithm;
        break;
      case 'm.room.history_visibility':
        historyVisibility = content.history_visibility;
        worldReadable = historyVisibility === 'world_readable';
        break;
      case 'm.room.guest_access':
        guestCanJoin = content.guest_access === 'can_join';
        break;
    }
  }

  response.world_readable = worldReadable;
  response.guest_can_join = guestCanJoin;

  // Check user membership if authenticated (optional auth)
  const userId = c.get('userId');
  if (userId) response.membership = (await getMembership(db, roomId, userId))?.membership ?? 'leave';

  // Check if room summary is allowed based on join rules
  // For non-public rooms, only show summary if user is a member or if world_readable
  if (!room.is_public && !worldReadable && !['join', 'invite'].includes(String(response.membership))) {
    // Don't reveal room existence for private rooms to non-members
    if (!['public', 'knock', 'knock_restricted'].includes(joinRule)) {
      return Errors.notFound('Room not found').toResponse();
    }
  }

  return c.json(response);
});

// ============================================
// Timestamp to Event (MSC3030)
// ============================================

// GET /_matrix/client/v3/rooms/:roomId/timestamp_to_event
// Finds the closest event to a given timestamp
app.get('/_matrix/client/v3/rooms/:roomId/timestamp_to_event', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  // Parse required parameters
  const tsParam = c.req.query('ts');
  const dirParam = c.req.query('dir');

  if (!tsParam) {
    return Errors.missingParam('ts').toResponse();
  }
  if (!dirParam) {
    return Errors.missingParam('dir').toResponse();
  }

  const ts = parseInt(tsParam, 10);
  if (isNaN(ts)) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'ts must be a valid integer timestamp in milliseconds',
    }, 400);
  }

  if (dirParam !== 'f' && dirParam !== 'b') {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: "dir must be 'f' (forward) or 'b' (backward)",
    }, 400);
  }

  // Check membership - user must be joined to the room
  const membership = await getMembership(db, roomId, userId);
  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Query for the closest event
  let event: { event_id: string; origin_server_ts: number } | null = null;

  if (dirParam === 'f') {
    // Forward: find the closest event at or after the timestamp
    event = await db.prepare(`
      SELECT event_id, origin_server_ts
      FROM events
      WHERE room_id = ? AND origin_server_ts >= ?
      ORDER BY origin_server_ts ASC
      LIMIT 1
    `).bind(roomId, ts).first<{ event_id: string; origin_server_ts: number }>();
  } else {
    // Backward: find the closest event at or before the timestamp
    event = await db.prepare(`
      SELECT event_id, origin_server_ts
      FROM events
      WHERE room_id = ? AND origin_server_ts <= ?
      ORDER BY origin_server_ts DESC
      LIMIT 1
    `).bind(roomId, ts).first<{ event_id: string; origin_server_ts: number }>();
  }

  if (!event) {
    return Errors.notFound('No event found for the given timestamp').toResponse();
  }

  return c.json({
    event_id: event.event_id,
    origin_server_ts: event.origin_server_ts,
  });
});

// ============================================
// Room Upgrade
// ============================================

// POST /_matrix/client/v3/rooms/:roomId/upgrade - Replace a room with a new version
app.post('/_matrix/client/v3/rooms/:roomId/upgrade', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const oldRoomId = c.req.param('roomId');
  let body: unknown;
  try { body = await c.req.json(); } catch { return Errors.badJson().toResponse(); }
  if (!isObject(body)) return Errors.badJson().toResponse();
  if (body.new_version === undefined) return Errors.missingParam('new_version').toResponse();
  if (typeof body.new_version !== 'string' || !FEDERATED_ROOM_VERSIONS.includes(body.new_version)) return Errors.unsupportedRoomVersion().toResponse();
  const oldRoom = await getRoom(c.env.DB, oldRoomId);
  if (!oldRoom) return Errors.notFound('Room not found').toResponse();
  const existing = await getStateEvent(c.env.DB, oldRoomId, 'm.room.tombstone');
  if (existing && typeof existing.content.replacement_room === 'string') {
    if ((await getMembership(c.env.DB, oldRoomId, userId))?.membership !== 'join') return Errors.forbidden().toResponse();
    return c.json({ replacement_room: existing.content.replacement_room });
  }
  const currentState = await getRoomState(c.env.DB, oldRoomId);
  const previous = await c.env.DB.prepare('SELECT event_id FROM events WHERE room_id=? ORDER BY depth DESC LIMIT 1')
    .bind(oldRoomId).first<{ event_id: string }>();
  let newRoomId = await generateRoomId(c.env.SERVER_NAME);
  const created = await buildLocalRoomEvent(c.env, { roomId: newRoomId, sender: userId, type: 'm.room.create', stateKey: '',
    content: { room_version: body.new_version, ...(body.new_version === '10' ? { creator: userId } : {}),
      predecessor: { room_id: oldRoomId, event_id: previous?.event_id ?? '' },
      ...(currentState.find(event => event.type === 'm.room.create')?.content['m.federate'] === false ? { 'm.federate': false } : {}),
    } }, body.new_version);
  if (body.new_version === '12') newRoomId = `!${created.event.event_id.slice(1)}`;
  created.event.room_id = newRoomId;
  // Check upgrade permission before creating any replacement room.
  await buildLocalRoomEvent(c.env, { roomId: oldRoomId, sender: userId, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'This room has been replaced', replacement_room: newRoomId } });
  await createRoom(c.env.DB, newRoomId, body.new_version, userId, !!oldRoom.is_public);
  const copyTypes = new Set(['m.room.join_rules', 'm.room.history_visibility', 'm.room.name', 'm.room.topic',
    'm.room.avatar', 'm.room.encryption', 'm.room.guest_access', 'm.room.server_acl']);
  await createInitialRoomEvents(c.env, created.event, body.new_version, {
    initial_state: currentState.filter(event => event.state_key === '' && copyTypes.has(event.type))
      .map(event => ({ type: event.type, state_key: '', content: event.content })),
  });
  const oldPower = currentState.find(event => event.type === 'm.room.power_levels');
  if (oldPower) {
    const users: Record<string, unknown> = isObject(oldPower.content.users) ? { ...oldPower.content.users } : {};
    if (body.new_version === '12') delete users[userId];
    await sendLocalRoomEvent(c.env, { roomId: newRoomId, sender: userId, type: 'm.room.power_levels', stateKey: '',
      content: { ...oldPower.content, users } });
  }
  await sendLocalRoomEvent(c.env, { roomId: oldRoomId, sender: userId, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'This room has been replaced', replacement_room: newRoomId } });
  // Permission to send a tombstone does not imply permission to change all power
  // levels. Restrict posting only when the ordinary event auth permits it.
  if (oldPower) {
    try {
      await sendLocalRoomEvent(c.env, { roomId: oldRoomId, sender: userId, type: 'm.room.power_levels', stateKey: '',
        content: { ...oldPower.content, events_default: Math.max(Number(oldPower.content.events_default ?? 0), 100),
          invite: Math.max(Number(oldPower.content.invite ?? 0), 100) } });
    } catch (error) {
      if (!(error instanceof MatrixApiError) || error.status !== 403) throw error;
    }
  }
  await c.env.DB.prepare('UPDATE room_aliases SET room_id=? WHERE room_id=?').bind(newRoomId, oldRoomId).run();
  return c.json({ replacement_room: newRoomId });
});

export default app;
