import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import rooms from '../src/api/rooms';
import classicSync from '../src/api/sync';
import slidingSync from '../src/api/sliding-sync';
import { hashToken } from '../src/utils/crypto';
import { sendLocalRoomEvent } from '../src/services/local-room-events';
import { storeAccountData } from '../src/services/account-data-stream';
import { testEnv } from './federation-helpers';

const alice = '@alice:local.example';
const bob = '@bob:local.example';
const headers = { Authorization: 'Bearer token', 'Content-Type': 'application/json' };
const paths = ['/_matrix/client/unstable/org.matrix.msc3575/sync', '/_matrix/client/v4/sync'];
let ctx: Awaited<ReturnType<typeof testEnv>>;
let states: Map<string, any>;
let waits: number;
beforeEach(async () => {
  ctx = await testEnv(); states = new Map(); waits = 0;
  vi.spyOn(console, 'log').mockImplementation(() => {});
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(bob, 'bob');
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)')
    .run('bob', await hashToken('bob-token'), bob, 'BOB');
  ctx.env.PUSH_NOTIFICATION_WORKFLOW = { create: async () => ({}) } as any;
  ctx.env.FEDERATION = { idFromName: (id: string) => id, get: () => ({ fetch: async () => Response.json({}) }) } as any;
  ctx.env.ROOMS = { idFromName: (id: string) => id, get: () => ({ fetch: async (request: Request) =>
    Response.json(new URL(request.url).pathname === '/typing' ? { user_ids: [] } : { receipts: {} }) }) } as any;
  ctx.env.USER_KEYS = { idFromName: (id: string) => id, get: () => ({ fetch: async () => Response.json({}) }) } as any;
  ctx.env.SYNC = { idFromName: (id: string) => id, get: () => ({ fetch: async (input: Request | string, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url);
    if (url.pathname === '/wait-for-events') { waits++; return Response.json({ hasEvents: false }); }
    if (url.pathname !== '/sliding-sync/state') return Response.json({});
    const key = url.searchParams.get('conn_id')!;
    if (request.method === 'PUT') { states.set(key, await request.json()); return Response.json({}); }
    return Response.json(states.get(key) ?? null);
  } }) } as any;
});
afterEach(() => { ctx.sqlite.close(); vi.restoreAllMocks(); });

async function create(token = 'token', content: Record<string, unknown> = {}) {
  const response = await rooms.request('/_matrix/client/v3/createRoom', {
    method: 'POST', headers: { ...headers, Authorization: `Bearer ${token}` },
    body: JSON.stringify({ room_version: '12', preset: 'public_chat', ...content }),
  }, ctx.env);
  expect(response.status).toBe(200); return (await response.json() as any).room_id as string;
}
async function classic(since?: string, timeout = 0, filter?: unknown) {
  const query = new URLSearchParams({ timeout: String(timeout) });
  if (since) query.set('since', since);
  if (filter) query.set('filter', JSON.stringify(filter));
  const response = await classicSync.request('/_matrix/client/v3/sync?' + query, { headers }, ctx.env);
  expect(response.status).toBe(200); return await response.json() as any;
}
async function sliding(path: string, body: unknown, pos?: string) {
  const response = await slidingSync.request(path + '?timeout=0' + (pos ? '&pos=' + encodeURIComponent(pos) : ''),
    { method: 'POST', headers, body: JSON.stringify(body) }, ctx.env);
  expect(response.status).toBe(200); return await response.json() as any;
}
async function replacement() {
  const previous = await create('token', { room_version: '10' });
  const current = await create('bob-token', { invite: [alice], creation_content: { predecessor: { room_id: previous } },
    initial_state: [{ type: 'm.room.encryption', state_key: '', content: { algorithm: 'm.megolm.v1.aes-sha2' } }] });
  await sendLocalRoomEvent(ctx.env, { roomId: previous, sender: alice, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'Replaced', replacement_room: current } });
  return { previous, current };
}
async function join(room: string) {
  const response = await rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/join`,
    { method: 'POST', headers, body: '{}' }, ctx.env);
  expect(response.status).toBe(200);
}

it('includes the full v12 creation, encryption and predecessor state after the bootstrap cursor was already consumed', async () => {
  const { previous, current } = await replacement();
  const before = await classic();
  await join(current);
  const joined = await classic(before.next_batch, 0, { room: { timeline: { limit: 1 } } });
  const state = joined.rooms.join[current].state.events;
  expect(state).toContainEqual(expect.objectContaining({ type: 'm.room.create', content: expect.objectContaining({
    room_version: '12', predecessor: { room_id: previous },
  }) }));
  expect(state).toContainEqual(expect.objectContaining({ type: 'm.room.encryption', content: { algorithm: 'm.megolm.v1.aes-sha2' } }));
  expect(state.some((event: any) => event.type === 'm.room.power_levels')).toBe(true);
});

it('repairs already cached room metadata without disabling long polling and respects state filters', async () => {
  const { previous, current } = await replacement(); await join(current);
  const initial = await classic();
  const recovered = await classic(initial.next_batch, 25000);
  expect(waits).toBe(1);
  expect(recovered.rooms.join[current].state.events.some((event: any) => event.type === 'm.room.create')).toBe(true);
  expect(recovered.rooms.join[current].state.events.some((event: any) => event.type === 'm.room.encryption')).toBe(true);
  expect(recovered.rooms.join[previous].state.events.some((event: any) => event.type === 'm.room.tombstone')).toBe(true);
  const filtered = await classic(recovered.next_batch, 0, { room: { state: { not_types: ['m.room.create'] } } });
  expect(filtered.rooms.join[current].state.events.some((event: any) => event.type === 'm.room.create')).toBe(false);
});

it.each(paths)('always delivers creation/encryption metadata when subscriptions omit it at %s', async path => {
  const { current } = await replacement(); await join(current);
  const response = await sliding(path, { conn_id: 'metadata', room_subscriptions: {
    [current]: { timeline_limit: 1, required_state: [['m.room.name', '']] },
  } });
  expect(response.rooms[current].required_state).toContainEqual(expect.objectContaining({ type: 'm.room.create',
    content: expect.objectContaining({ room_version: '12' }) }));
  expect(response.rooms[current].required_state.some((event: any) => event.type === 'm.room.encryption')).toBe(true);
});

it.each(paths)('replays missing metadata once for a pre-fix sliding connection at %s', async path => {
  const { current } = await replacement(); await join(current);
  const body = { conn_id: 'recovery', room_subscriptions: { [current]: { timeline_limit: 1, required_state: [] } } };
  const first = await sliding(path, body);
  for (const state of states.values()) delete state.roomStates[current].stateVersion;
  const recovered = await sliding(path, body, first.pos);
  expect(recovered.rooms[current]?.initial).toBe(true);
  expect(recovered.rooms[current].required_state.some((event: any) => event.type === 'm.room.create')).toBe(true);
  const next = await sliding(path, body, recovered.pos);
  expect(next.rooms[current]).toBeUndefined();
});

it.each(paths)('filters tombstones independently and uses private m.direct rather than names/member counts at %s', async path => {
  const old = await create(); const current = await create('token', { name: 'Named direct chat', invite: [bob] });
  const solo = await create();
  await sendLocalRoomEvent(ctx.env, { roomId: old, sender: alice, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'Replaced', replacement_room: current } });
  await storeAccountData(ctx.env.DB, alice, '', 'm.direct', { [bob]: [old, current] });
  // Another user's private DM classification must not affect Alice's list.
  await storeAccountData(ctx.env.DB, bob, '', 'm.direct', { [alice]: [solo] });
  const response = await sliding(path, { conn_id: 'direct', lists: {
    people: { range: [0, 20], filters: { is_dm: true, is_tombstoned: false } },
    rooms: { range: [0, 20], filters: { is_dm: false, is_tombstoned: false } },
    old: { range: [0, 20], filters: { is_tombstoned: true } },
  } });
  expect(response.lists.people.ops[0].room_ids).toEqual([current]);
  expect(response.lists.rooms.ops[0].room_ids).toEqual([solo]);
  expect(response.lists.old.ops[0].room_ids).toEqual([old]);
  expect(response.rooms[current].is_dm).toBe(true); expect(response.rooms[solo].is_dm).toBe(false);
  expect(response.rooms[current].heroes).toBeUndefined();
});

it.each(paths)('uses an invited peer as an unnamed joined direct-chat hero at %s', async path => {
  const room = await create('token', { invite: [bob] });
  await storeAccountData(ctx.env.DB, alice, '', 'm.direct', { [bob]: [room] });
  const response = await sliding(path, { room_subscriptions: { [room]: { timeline_limit: 1 } } });
  expect(response.rooms[room].heroes).toContainEqual(expect.objectContaining({ user_id: bob }));
});

it('includes invited heroes and member counts in classic sync so an owner-only joined room has a useful name', async () => {
  const room = await create('token', { invite: [bob] });
  const initial = await classic();
  expect(initial.rooms.join[room].summary).toEqual({ 'm.heroes': [bob], 'm.joined_member_count': 1, 'm.invited_member_count': 1 });
  const cached = await classic(initial.next_batch, 25000);
  expect(waits).toBe(1);
  expect(cached.rooms.join[room].state.events).toContainEqual(expect.objectContaining({
    type: 'm.room.member', state_key: bob, content: expect.objectContaining({ membership: 'invite' }),
  }));
});

it.each(['is_invite', 'is_invited'])('accepts true/false %s independently of the tombstone filter', async inviteFilter => {
  const joined = await create(); const invited = await create('bob-token', { invite: [alice] });
  const response = await sliding('/_matrix/client/v4/sync', { lists: {
    joined: { range: [0, 20], filters: { [inviteFilter]: false, is_tombstoned: false } },
    invited: { range: [0, 20], filters: { [inviteFilter]: true, is_tombstoned: false } },
  } });
  expect(response.lists.joined.ops[0].room_ids).toEqual([joined]);
  expect(response.lists.invited.ops[0].room_ids).toEqual([invited]);
});

it.each(paths)('refreshes cached DM classification after m.direct changes without new room events at %s', async path => {
  const room = await create();
  const body = { conn_id: 'dm-change', lists: { all: { range: [0, 20], timeline_limit: 1 } } };
  const initial = await sliding(path, body);
  expect(initial.rooms[room].is_dm).toBe(false);
  await storeAccountData(ctx.env.DB, alice, '', 'm.direct', { [bob]: [room] });
  const changed = await sliding(path, body, initial.pos);
  expect(changed.rooms[room].is_dm).toBe(true); expect(changed.rooms[room].timeline).toEqual([]);
  const unchanged = await sliding(path, body, changed.pos);
  expect(unchanged.rooms[room]).toBeUndefined();
});

it.each(paths)('publishes changed tombstone metadata even when the room timeline is disabled at %s', async path => {
  const old = await create(); const replacement = await create();
  const body = { conn_id: 'state-only', room_subscriptions: { [old]: { timeline_limit: 0, required_state: [] } } };
  const initial = await sliding(path, body);
  expect(initial.rooms[old].timeline ?? []).toEqual([]);
  await sendLocalRoomEvent(ctx.env, { roomId: old, sender: alice, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'Replaced', replacement_room: replacement } });
  const changed = await sliding(path, body, initial.pos);
  expect(changed.rooms[old].required_state).toContainEqual(expect.objectContaining({ type: 'm.room.tombstone',
    content: { body: 'Replaced', replacement_room: replacement } }));
  expect(changed.rooms[old].timeline ?? []).toEqual([]);
  const next = await sliding(path, body, changed.pos);
  expect(next.rooms[old]).toBeUndefined();
});

it.each(paths)('publishes redacted state content at the same event ID without requiring a timeline at %s', async path => {
  const room = await create('token', { name: 'Name to redact' });
  const body = { conn_id: 'redacted-state', room_subscriptions: { [room]: {
    timeline_limit: 0, required_state: [['m.room.name', '']],
  } } };
  const initial = await sliding(path, body);
  const name = initial.rooms[room].required_state.find((event: any) => event.type === 'm.room.name');
  const redaction = await rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/redact/${encodeURIComponent(name.event_id)}/metadata`,
    { method: 'PUT', headers, body: '{}' }, ctx.env);
  expect(redaction.status).toBe(200);
  const changed = await sliding(path, body, initial.pos);
  expect(changed.rooms[room].required_state).toContainEqual(expect.objectContaining({
    event_id: name.event_id, type: 'm.room.name', content: {},
  }));
  const next = await sliding(path, body, changed.pos);
  expect(next.rooms[room]).toBeUndefined();
});

it.each(paths)('publishes display-name changes once when timelines and requested state are empty at %s', async path => {
  const room = await create('token', { name: 'Old name' });
  const body = { conn_id: 'display-only', room_subscriptions: { [room]: { timeline_limit: 0, required_state: [] } } };
  const initial = await sliding(path, body);
  expect(initial.rooms[room].name).toBe('Old name');
  await sendLocalRoomEvent(ctx.env, { roomId: room, sender: alice, type: 'm.room.name', stateKey: '',
    content: { name: 'New name' } });
  const changed = await sliding(path, body, initial.pos);
  expect(changed.rooms[room].name).toBe('New name'); expect(changed.rooms[room].timeline ?? []).toEqual([]);
  const next = await sliding(path, body, changed.pos);
  expect(next.rooms[room]).toBeUndefined();
});

it.each(paths)('keeps left tombstoned rooms outside active sliding lists at %s', async path => {
  const old = await create(); const current = await create();
  await sendLocalRoomEvent(ctx.env, { roomId: old, sender: alice, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'Replaced', replacement_room: current } });
  const left = await rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(old)}/leave`,
    { method: 'POST', headers, body: '{}' }, ctx.env);
  expect(left.status).toBe(200);
  const response = await sliding(path, { lists: { old: { range: [0, 20], filters: { is_tombstoned: true } } } });
  expect(response.lists.old.count).toBe(0); expect(response.rooms[old]).toBeUndefined();
});
