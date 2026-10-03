import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import rooms from '../src/api/rooms';
import { getEvent, getRoomState, storeEvent } from '../src/services/database';
import { buildLocalRoomEvent } from '../src/services/local-room-events';
import { getSnapshotEventsByIds, getStateBeforeEvent, prepareVerifiedStateSnapshot } from '../src/services/event-state-snapshots';
import { eventReferenceId, eventVerifier, signEvent, wireEvent } from '../src/services/federation-events';
import { persistRemoteJoin, validateJoinGraph } from '../src/services/remote-rooms';
import type { PDU } from '../src/types';
import { roomFixture, testEnv } from './federation-helpers';

let ctx: Awaited<ReturnType<typeof testEnv>>;
let roomId: string;
const sender = '@alice:local.example';
beforeEach(async () => {
  ctx = await testEnv();
  const response = await rooms.request('/_matrix/client/v3/createRoom', { method: 'POST',
    headers: { Authorization: 'Bearer token' }, body: JSON.stringify({ room_version: '11', preset: 'public_chat' }) }, ctx.env);
  expect(response.status).toBe(200); roomId = (await response.json() as { room_id: string }).room_id;
});
afterEach(() => { ctx.sqlite.close(); vi.unstubAllGlobals(); });

async function build(type = 'm.room.message', content: Record<string, unknown> = { body: 'Test', msgtype: 'm.text' }, stateKey?: string, parents?: PDU[]) {
  let { event } = await buildLocalRoomEvent(ctx.env, { roomId, sender, type, content, stateKey });
  if (parents) {
    const signed = await signEvent({ ...wireEvent(event, '11'), prev_events: parents.map(parent => parent.event_id),
      depth: Math.max(...parents.map(parent => parent.depth)) + 1 }, '11', ctx.env.SERVER_NAME, ctx.localKey);
    event = { ...signed, room_id: roomId, event_id: await eventReferenceId(signed, '11') } as PDU;
  }
  return event;
}
async function add(type?: string, content?: Record<string, unknown>, stateKey?: string, parents?: PDU[]) {
  const event = await build(type, content, stateKey, parents); await storeEvent(ctx.env.DB, event); return event;
}
const topic = (state: PDU[]) => state.find(event => event.type === 'm.room.topic');

it('stores state before each event atomically and keeps old topics after later state changes', async () => {
  const old = await add('m.room.topic', { topic: 'Earlier' }, '');
  const message = await add();
  const newer = await add('m.room.topic', { topic: 'Later' }, '');
  expect(topic(await getStateBeforeEvent(ctx.env.DB, roomId, message.event_id))?.event_id).toBe(old.event_id);
  expect(topic(await getStateBeforeEvent(ctx.env.DB, roomId, newer.event_id))?.event_id).toBe(old.event_id);
  expect(topic(await getStateBeforeEvent(ctx.env.DB, roomId, old.event_id))).toBeUndefined();
  const live = ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM events WHERE room_id=?').get(roomId)!.n;
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM event_state_snapshots WHERE room_id=?').get(roomId)!.n).toBe(live);
  const rejected = await build('m.room.topic', { topic: 'Must roll back' }, '');
  ctx.sqlite.exec("CREATE TRIGGER fail_snapshot BEFORE INSERT ON event_state_snapshots BEGIN SELECT RAISE(ABORT,'snapshot failed'); END");
  await expect(storeEvent(ctx.env.DB, rejected)).rejects.toThrow('snapshot failed');
  expect(await getEvent(ctx.env.DB, rejected.event_id)).toBeNull();
  expect(topic(await getRoomState(ctx.env.DB, roomId))?.event_id).toBe(newer.event_id);
});

it('follows a causal predecessor even when a different branch was stored later', async () => {
  const base = await add();
  const a = await add('m.room.topic', { topic: 'Branch A' }, '', [base]);
  const b = await add('m.room.topic', { topic: 'Branch B' }, '', [base]);
  const childA = await add('m.room.message', { body: 'A', msgtype: 'm.text' }, undefined, [a]);
  expect(topic(await getStateBeforeEvent(ctx.env.DB, roomId, childA.event_id))?.event_id).toBe(a.event_id);
  expect(topic(await getRoomState(ctx.env.DB, roomId))?.event_id).toBe(b.event_id);
  const merge = await add('m.room.message', { body: 'Merge', msgtype: 'm.text' }, undefined, [a, b]);
  expect(ctx.sqlite.prepare('SELECT event_id FROM event_state_snapshots WHERE event_id=?').get(merge.event_id)).toBeUndefined();
  await expect(getStateBeforeEvent(ctx.env.DB, roomId, merge.event_id)).rejects.toMatchObject({ status: 404, message: expect.stringContaining('Exact historical state') });
});

it('reconstructs legacy linear history and merges only identical parent states', async () => {
  const old = await add('m.room.topic', { topic: 'Known' }, '');
  const a = await add('m.room.message', { body: 'A', msgtype: 'm.text' }, undefined, [old]);
  const b = await add('m.room.message', { body: 'B', msgtype: 'm.text' }, undefined, [old]);
  const merge = await add('m.room.message', { body: 'Merge', msgtype: 'm.text' }, undefined, [a, b]);
  ctx.sqlite.exec('DELETE FROM event_state_snapshots');
  expect(topic(await getStateBeforeEvent(ctx.env.DB, roomId, merge.event_id))?.event_id).toBe(old.event_id);
  const unknown = await build(); unknown.prev_events = ['$unavailable'];
  await storeEvent(ctx.env.DB, unknown);
  await expect(getStateBeforeEvent(ctx.env.DB, roomId, unknown.event_id)).rejects.toMatchObject({ status: 404 });
  await expect(getStateBeforeEvent(ctx.env.DB, '!another:local.example', merge.event_id)).rejects.toMatchObject({ status: 404 });
});

it('imports a verified pre-join baseline across a history gap and keeps it after later local changes', async () => {
  const fixture = await roomFixture('11');
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(fixture.keyResponse)));
  await ctx.env.CACHE.put('discovery:v2:remote.example', JSON.stringify({ host: fixture.remote, port: 443, tlsHostname: fixture.remote }));
  const verify = eventVerifier(ctx.env);
  const state = await Promise.all(fixture.events.map(event => verify(event, '11', fixture.roomId)));
  const signed = await signEvent({ ...fixture.template, prev_events: ['$history-not-fetched'] }, '11', ctx.env.SERVER_NAME, ctx.localKey);
  const joined = await verify(signed, '11', fixture.roomId);
  const create = validateJoinGraph(state, state, joined, '11');
  await persistRemoteJoin(ctx.env, '11', state, state, joined, create);
  const original = await getStateBeforeEvent(ctx.env.DB, fixture.roomId, joined.event_id);
  expect(original.map(event => event.event_id)).toEqual(state.map(event => event.event_id));
  expect(original.some(event => event.state_key === sender)).toBe(false);
  // A subsequent allowed local membership update advances the timeline but cannot alter this baseline.
  const later = await buildLocalRoomEvent(ctx.env, { roomId: fixture.roomId, sender, stateKey: sender,
    type: 'm.room.member', content: { membership: 'join', displayname: 'New profile' } });
  await storeEvent(ctx.env.DB, later.event);
  expect((await getStateBeforeEvent(ctx.env.DB, fixture.roomId, later.event.event_id)).some(event => event.event_id === joined.event_id)).toBe(true);
  expect(await getStateBeforeEvent(ctx.env.DB, fixture.roomId, joined.event_id)).toEqual(original);
});

it('archives signed historical state and auth without publishing its message in the global client timeline', async () => {
  const baseline = await getRoomState(ctx.env.DB, roomId);
  const oldPower = baseline.find(event => event.type === 'm.room.power_levels')!;
  const power = await build('m.room.power_levels', { ...oldPower.content, invite: 1 }, '');
  const nameTemplate = await build('m.room.topic', { topic: 'Archived only' }, '');
  const messageTemplate = await build();
  async function withPower(event: PDU) {
    const signed = await signEvent({ ...wireEvent(event, '11'), auth_events: event.auth_events.map(id => id === oldPower.event_id ? power.event_id : id) }, '11', ctx.env.SERVER_NAME, ctx.localKey);
    return { ...signed, room_id: roomId, event_id: await eventReferenceId(signed, '11') } as PDU;
  }
  const name = await withPower(nameTemplate); const message = await withPower(messageTemplate);
  const state = [...baseline.filter(event => event.type !== 'm.room.power_levels'), power, name];
  const auth = [...baseline, power];
  expect(() => validateJoinGraph([...auth, name], state, message, '11')).not.toThrow();
  await ctx.env.DB.batch(await prepareVerifiedStateSnapshot(ctx.env.DB, message, state, auth, '11'));
  expect(await getEvent(ctx.env.DB, message.event_id)).toBeNull();
  expect(await getEvent(ctx.env.DB, name.event_id)).toBeNull();
  expect(await getEvent(ctx.env.DB, power.event_id)).toBeNull();
  expect(topic(await getStateBeforeEvent(ctx.env.DB, roomId, message.event_id))?.event_id).toBe(name.event_id);
  expect((await getSnapshotEventsByIds(ctx.env.DB, roomId, [name.event_id, power.event_id])).map(event => event.event_id)).toEqual([name.event_id, power.event_id]);
});

it('retains historical state reference IDs while applying a later authorized redaction', async () => {
  const old = await add('m.room.topic', { topic: 'Private topic' }, ''); const message = await add();
  const response = await rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/redact/${encodeURIComponent(old.event_id)}/snapshot-redaction`, {
    method: 'PUT', headers: { Authorization: 'Bearer token' }, body: '{}' }, ctx.env);
  expect(response.status).toBe(200);
  const redacted = topic(await getStateBeforeEvent(ctx.env.DB, roomId, message.event_id))!;
  expect(redacted.event_id).toBe(old.event_id); expect(redacted.content).toEqual({});
  expect(redacted.hashes).toEqual(old.hashes); expect(redacted.signatures).toEqual(old.signatures);
  expect(redacted.unsigned?.redacted_because).toBeDefined();
});
