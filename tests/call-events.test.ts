import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import calls from '../src/api/calls';
import voip from '../src/api/voip';
import serverNotices from '../src/api/server-notices';
import { getEvent, getEventsSince, getLatestStreamPosition, getStateEvent } from '../src/services/database';
import { eventVerifier } from '../src/services/federation-events';
import { sendLocalRoomEvent } from '../src/services/local-room-events';
import { testEnv } from './federation-helpers';

const roomId = '!calls:local.example';
const userId = '@alice:local.example';
const owner = '@owner:local.example';
const headers = { Authorization: 'Bearer token' };
const callPath = `/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/call`;
const rtcPath = `/_matrix/client/v1/rooms/${encodeURIComponent(roomId)}/call`;
let ctx: Awaited<ReturnType<typeof testEnv>>;
let callFetch: ReturnType<typeof vi.fn>;
let syncFetch: ReturnType<typeof vi.fn>;

beforeEach(async () => {
  ctx = await testEnv();
  ctx.sqlite.prepare('INSERT INTO rooms(room_id,room_version) VALUES (?,?)').run(roomId, '10');
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(owner, 'owner');
  syncFetch = vi.fn(async () => Response.json({}));
  callFetch = vi.fn(async (request: Request) => new URL(request.url).pathname === '/ws'
    ? new Response('WebSocket proxy reached', { status: 418 }) : Response.json({}));
  Object.assign(ctx.env, {
    SYNC: { idFromName: (id: string) => id, get: () => ({ fetch: syncFetch }) },
    CALLS_APP_ID: 'test-app', CALLS_APP_SECRET: 'test-secret',
    CALL_ROOMS: { idFromName: (id: string) => id, get: () => ({ fetch: callFetch }) },
  });
  const send = (type: string, content: Record<string, unknown>, stateKey = '') =>
    sendLocalRoomEvent(ctx.env, { roomId, sender: owner, type, content, stateKey });
  await send('m.room.create', { creator: owner, room_version: '10' });
  await send('m.room.member', { membership: 'join' }, owner);
  await send('m.room.power_levels', { users: { [owner]: 100 }, events: { 'm.call.state': 0, 'm.call.member': 0 } });
  await send('m.room.join_rules', { join_rule: 'public' });
  await sendLocalRoomEvent(ctx.env, { roomId, sender: userId, type: 'm.room.member', stateKey: userId, content: { membership: 'join' } });
  callFetch.mockClear(); syncFetch.mockClear();
});
afterEach(() => ctx.sqlite.close());

it('starts and ends a call with signed immutable events visible after each sync boundary', async () => {
  const since = await getLatestStreamPosition(ctx.env.DB);
  const started = await calls.request(`${callPath}/start`, { method: 'POST', headers }, ctx.env);
  expect(started.status).toBe(200);
  const { callId } = await started.json();
  const startEvent = (await getStateEvent(ctx.env.DB, roomId, 'm.call.state', ''))!;
  expect(startEvent.content).toMatchObject({ active: true, call_id: callId });
  await eventVerifier(ctx.env)(await getEvent(ctx.env.DB, startEvent.event_id), '10', roomId);
  expect((await getEventsSince(ctx.env.DB, roomId, since)).map(event => event.event_id)).toEqual([startEvent.event_id]);
  expect((await calls.request(callPath, { headers }, ctx.env)).status).toBe(200);

  const afterStart = await getLatestStreamPosition(ctx.env.DB);
  const ended = await calls.request(`${callPath}/end`, { method: 'POST', headers }, ctx.env);
  expect(ended.status).toBe(200);
  const endEvent = (await getStateEvent(ctx.env.DB, roomId, 'm.call.state', ''))!;
  expect(endEvent.event_id).not.toBe(startEvent.event_id);
  expect(endEvent.content).toMatchObject({ active: false, call_id: callId, ended_by: userId });
  expect((await getEvent(ctx.env.DB, startEvent.event_id))!.content.active).toBe(true);
  await eventVerifier(ctx.env)(await getEvent(ctx.env.DB, endEvent.event_id), '10', roomId);
  expect((await getEventsSince(ctx.env.DB, roomId, afterStart)).map(event => event.event_id)).toEqual([endEvent.event_id]);
  expect(syncFetch).toHaveBeenCalled();
});

it('denies a call state change before initializing the SFU when room power levels forbid it', async () => {
  await sendLocalRoomEvent(ctx.env, { roomId, sender: owner, type: 'm.room.power_levels', stateKey: '',
    content: { users: { [owner]: 100 }, events: { 'm.call.state': 50, 'm.call.member': 0 } } });
  expect((await calls.request(`${callPath}/start`, { method: 'POST', headers }, ctx.env)).status).toBe(403);
  expect(callFetch).not.toHaveBeenCalled();
  expect(await getStateEvent(ctx.env.DB, roomId, 'm.call.state', '')).toBeNull();
});

it('requires authentication and joined membership for call state, termination and the active websocket', async () => {
  const response = await calls.request(`${callPath}/start`, { method: 'POST', headers }, ctx.env);
  const { callId } = await response.json();
  const wsPath = `/calls/${callId}/ws`;
  expect((await calls.request(wsPath, {}, ctx.env)).status).toBe(401);
  expect((await calls.request(wsPath, { headers: { ...headers,
    'X-Matrix-Call-User': '@spoofed:local.example', 'X-Matrix-Call-Device': 'SPOOFED' } }, ctx.env)).status).toBe(418);
  const forwarded = callFetch.mock.calls.at(-1)![0] as Request;
  expect(forwarded.headers.get('X-Matrix-Call-User')).toBe(userId);
  expect(forwarded.headers.get('X-Matrix-Call-Device')).toBe('DEVICE');
  expect(forwarded.headers.get('Authorization')).toBeNull();
  ctx.sqlite.prepare("UPDATE room_memberships SET membership='leave' WHERE room_id=? AND user_id=?").run(roomId, userId);
  for (const path of [callPath, `${callPath}/end`, wsPath]) {
    const method = path.endsWith('/end') ? 'POST' : 'GET';
    expect((await calls.request(path, { method, headers }, ctx.env)).status).toBe(403);
  }
});

it('does not publish call state if the SFU initialization fails', async () => {
  callFetch.mockResolvedValueOnce(new Response('Unavailable', { status: 503 }));
  expect((await calls.request(`${callPath}/start`, { method: 'POST', headers }, ctx.env)).status).toBe(500);
  expect(await getStateEvent(ctx.env.DB, roomId, 'm.call.state', '')).toBeNull();
});

it('stores RTC join and leave as signed state events and notifies other clients on leave', async () => {
  const put = await voip.request(rtcPath, { method: 'PUT', headers, body: '{}' }, ctx.env);
  expect(put.status).toBe(200);
  const joined = (await getStateEvent(ctx.env.DB, roomId, 'm.call.member', userId))!;
  await eventVerifier(ctx.env)(await getEvent(ctx.env.DB, joined.event_id), '10', roomId);
  expect((await (await voip.request(rtcPath, { headers }, ctx.env)).json()).members).toMatchObject([{ user_id: userId, device_id: 'DEVICE' }]);
  const since = await getLatestStreamPosition(ctx.env.DB);
  syncFetch.mockClear();
  const deleted = await voip.request(rtcPath, { method: 'DELETE', headers }, ctx.env);
  expect(deleted.status).toBe(200);
  const left = (await getStateEvent(ctx.env.DB, roomId, 'm.call.member', userId))!;
  expect(left.content.memberships).toEqual([]);
  expect(left.event_id).not.toBe(joined.event_id);
  await eventVerifier(ctx.env)(await getEvent(ctx.env.DB, left.event_id), '10', roomId);
  expect((await getEventsSince(ctx.env.DB, roomId, since)).map(event => event.event_id)).toEqual([left.event_id]);
  expect(syncFetch).toHaveBeenCalled();
  expect((await voip.request(rtcPath, { headers }, ctx.env)).status).toBe(404);
});

it('rejects malformed RTC payloads and refuses leave requests from departed users', async () => {
  for (const body of ['null', '[]', '{"device_id":4}', '{"expires_ts":"later"}']) {
    expect((await voip.request(rtcPath, { method: 'PUT', headers, body }, ctx.env)).status).toBe(400);
  }
  ctx.sqlite.prepare("UPDATE room_memberships SET membership='leave' WHERE room_id=? AND user_id=?").run(roomId, userId);
  expect((await voip.request(rtcPath, { method: 'DELETE', headers }, ctx.env)).status).toBe(403);
});

it('creates a signed local notice room with correct membership ids and delivers subsequent notices on the sync stream', async () => {
  ctx.sqlite.prepare('UPDATE users SET admin=1 WHERE user_id=?').run(userId);
  const path = '/_synapse/admin/v1/send_server_notice';
  const first = await serverNotices.request(path, { method: 'POST', headers, body: JSON.stringify({
    user_id: userId, content: { body: 'First notice', msgtype: 'm.text' },
  }) }, ctx.env);
  expect(first.status).toBe(200);
  const { event_id: firstId } = await first.json();
  const firstEvent = (await getEvent(ctx.env.DB, firstId))!;
  const noticeRoom = firstEvent.room_id;
  const verifier = eventVerifier(ctx.env);
  const ids = ctx.sqlite.prepare('SELECT event_id FROM events WHERE room_id=? ORDER BY stream_ordering').all(noticeRoom);
  expect(ids).toHaveLength(8);
  for (const row of ids) await verifier(await getEvent(ctx.env.DB, String(row.event_id)), '10', noticeRoom);
  expect((await getStateEvent(ctx.env.DB, noticeRoom, 'm.room.create', ''))?.content['m.federate']).toBe(false);
  const memberships = ctx.sqlite.prepare('SELECT user_id,membership,event_id FROM room_memberships WHERE room_id=?').all(noticeRoom);
  for (const membership of memberships) {
    const event = (await getEvent(ctx.env.DB, String(membership.event_id)))!;
    expect(event.state_key).toBe(membership.user_id);
    expect(event.content.membership).toBe(membership.membership);
  }
  const since = await getLatestStreamPosition(ctx.env.DB);
  const second = await serverNotices.request(path, { method: 'POST', headers, body: JSON.stringify({
    user_id: userId, content: { body: 'Second notice' },
  }) }, ctx.env);
  expect(second.status).toBe(200);
  const secondId = (await second.json()).event_id;
  expect((await getEventsSince(ctx.env.DB, noticeRoom, since)).map(event => event.event_id)).toEqual([secondId]);
  expect((await getEvent(ctx.env.DB, secondId))?.content.body).toBe('Second notice');
});

it('restricts notices to administrators and active local users before creating any notice room', async () => {
  const path = '/_synapse/admin/v1/send_server_notice';
  const request = (target: string) => ({ method: 'POST', headers, body: JSON.stringify({ user_id: target, content: { body: 'Notice' } }) });
  expect((await serverNotices.request(path, request(userId), ctx.env)).status).toBe(403);
  ctx.sqlite.prepare('UPDATE users SET admin=1 WHERE user_id=?').run(userId);
  for (const target of ['@unknown:local.example', '@foreign:remote.example']) {
    expect((await serverNotices.request(path, request(target), ctx.env)).status).toBe(404);
  }
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM rooms').get()).toMatchObject({ n: 1 });
});
