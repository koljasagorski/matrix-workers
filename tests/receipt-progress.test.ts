import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { publishReadReceipt, receiptPosition, recordReadReceipts, type ReadReceipt } from '../src/services/read-receipts';
import { countNotificationsWithRules } from '../src/services/push-rule-evaluator';
import { testEnv } from './federation-helpers';

vi.mock('cloudflare:workers', () => ({ DurableObject: class {
  ctx: any; env: any; constructor(ctx: any, env: any) { this.ctx = ctx; this.env = env; }
} }));
import { RoomDurableObject } from '../src/durable-objects/RoomDurableObject';

const room = '!progress:local.example';
const user = '@alice:local.example';
const remote = '@member:remote.example';
const future = Number.MAX_SAFE_INTEGER;
let ctx: Awaited<ReturnType<typeof testEnv>>;
let object: RoomDurableObject;
let state: DurableObjectState;
let values: Map<string, unknown>;
let sockets: any[];
let queued: any[];
let wakeCalls: number;

beforeEach(async () => {
  ctx = await testEnv(); values = new Map(); sockets = []; queued = []; wakeCalls = 0;
  ctx.sqlite.prepare('INSERT INTO rooms(room_id) VALUES (?)').run(room);
  for (const member of [user, remote]) ctx.sqlite.prepare(`
    INSERT INTO room_memberships(room_id,user_id,membership,event_id) VALUES (?,?,'join','$joined')`).run(room, member);
  for (const [id, depth, position] of [['$first', 1, 1], ['$second', 2, 2], ['$branch', 2, 3], ['$third', 3, 4]] as const) {
    ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
      VALUES (?,?,?,'m.room.message','{"body":"Message","msgtype":"m.text"}',1,?,'[]','[]',?)`).run(id, room, remote, depth, position);
  }
  state = { storage: {
    get: async (key: string) => values.get(key),
    put: async (key: string | Record<string, unknown>, value?: unknown) => {
      if (typeof key === 'string') values.set(key, value);
      else for (const [k, v] of Object.entries(key)) values.set(k, v);
    },
    list: async ({ prefix }: { prefix: string }) => new Map([...values].filter(([key]) => key.startsWith(prefix))),
  }, getWebSockets: () => sockets } as any;
  object = new RoomDurableObject(state, ctx.env);
  ctx.env.ROOMS = { idFromName: (id: string) => id, get: () => ({ fetch: (request: Request) => object.fetch(request) }) } as any;
  ctx.env.FEDERATION = { idFromName: (id: string) => id, get: () => ({ fetch: async (request: Request) => {
    queued.push(await request.json()); return Response.json({});
  } }) } as any;
  ctx.env.SYNC = { idFromName: (id: string) => id, get: () => ({ fetch: async () => {
    wakeCalls++; return Response.json({});
  } }) } as any;
});
afterEach(() => { ctx.sqlite.close(); vi.restoreAllMocks(); });

function receipt(eventId: string, ts: number, type: ReadReceipt['receipt_type'] = 'm.read', threadId?: string): ReadReceipt {
  return { user_id: user, event_id: eventId, receipt_type: type, ts, ...(threadId ? { thread_id: threadId } : {}) };
}
async function set(value: ReadReceipt) {
  const response = await object.fetch(new Request('https://room/receipt', {
    method: 'PUT', body: JSON.stringify({ ...value, room_id: room }),
  }));
  return await response.json() as { updated: boolean; receipt: ReadReceipt };
}
async function get() {
  return await (await object.fetch(new Request(`https://room/receipts?room_id=${encodeURIComponent(room)}`))).json();
}
function stored(type: ReadReceipt['receipt_type'] = 'm.read', threadId = '') {
  return ctx.sqlite.prepare('SELECT event_id,ts FROM receipts WHERE room_id=? AND user_id=? AND receipt_type=? AND thread_id=?')
    .get(room, user, type, threadId);
}
function archived(id: string, eventId: string, ts: number) {
  ctx.sqlite.prepare("INSERT INTO processed_edus(edu_id,edu_type,origin,processed_at,content) VALUES (?,'m.receipt','remote.example',1,?)")
    .run(id, JSON.stringify({ [room]: { 'm.read': { [remote]: { event_ids: [eventId], data: { ts } } } } }));
}

it.each(['m.read', 'm.read.private'] as const)('advances %s by event depth despite an old future timestamp and rejects a backward marker', async type => {
  await set(receipt('$first', future, type));
  expect(await set(receipt('$second', 100, type))).toEqual({ updated: true, receipt: receipt('$second', 100, type) });
  expect(stored(type)).toEqual({ event_id: '$second', ts: 100 });
  expect(await receiptPosition(ctx.env.DB)).toBe(2);
  expect(await set(receipt('$first', future, type))).toEqual({ updated: false, receipt: receipt('$second', 100, type) });
  expect(stored(type)).toEqual({ event_id: '$second', ts: 100 });
  expect(await receiptPosition(ctx.env.DB)).toBe(2);
});

it('applies the same progress rule atomically to the D1 copy and increments its cursor only for changes', async () => {
  await recordReadReceipts(ctx.env.DB, room, [receipt('$first', future), receipt('$third', 100), receipt('$second', future)]);
  expect(stored()).toEqual({ event_id: '$third', ts: 100 });
  expect(await receiptPosition(ctx.env.DB)).toBe(2);
  await recordReadReceipts(ctx.env.DB, room, [receipt('$third', 100), receipt('$third', 99)]);
  expect(await receiptPosition(ctx.env.DB)).toBe(2);
  await recordReadReceipts(ctx.env.DB, room, [receipt('$third', 101)]);
  expect(stored()).toEqual({ event_id: '$third', ts: 101 });
  expect(await receiptPosition(ctx.env.DB)).toBe(3);
});

it('keeps metadata monotonic for the same event and uses timestamps for equal-depth branches', async () => {
  await set(receipt('$second', 200));
  expect((await set(receipt('$second', 100))).updated).toBe(false);
  expect((await set(receipt('$second', 200))).updated).toBe(false);
  expect((await set(receipt('$branch', 199))).updated).toBe(false);
  expect((await set(receipt('$branch', 200))).updated).toBe(true);
  expect((await set(receipt('$branch', 201))).updated).toBe(true);
  expect(stored()).toEqual({ event_id: '$branch', ts: 201 });
  expect(await receiptPosition(ctx.env.DB)).toBe(3);
});

it('falls back to timestamps for unknown events and never borrows event depth from another room', async () => {
  ctx.sqlite.prepare("INSERT INTO rooms(room_id) VALUES ('!foreign:local.example')").run();
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events)
    VALUES ('$foreign','!foreign:local.example',?,'m.room.message','{}',1,100,'[]','[]')`).run(remote);
  await set(receipt('$first', 200));
  expect((await set(receipt('$foreign', 100))).updated).toBe(false);
  expect((await set(receipt('$unknown', 199))).updated).toBe(false);
  expect((await set(receipt('$unknown', 200))).updated).toBe(true);
  expect(stored()).toEqual({ event_id: '$unknown', ts: 200 });
  await recordReadReceipts(ctx.env.DB, room, [receipt('$first', 200, 'm.read.private'), receipt('$foreign', 100, 'm.read.private')]);
  expect(stored('m.read.private')).toEqual({ event_id: '$first', ts: 200 });
});

it('serializes concurrent receipt writes so a late old event cannot replace a newer event', async () => {
  await set(receipt('$first', 1));
  await Promise.all([set(receipt('$third', 100)), set(receipt('$second', future)), set(receipt('$first', future))]);
  expect((await get()).receipts).toEqual({ '$third': { 'm.read': { [user]: { ts: 100 } } } });
  expect(stored()).toEqual({ event_id: '$third', ts: 100 });
  expect(await receiptPosition(ctx.env.DB)).toBe(2);
});

it('mirrors existing private cache receipts once even when archived recovery was previously marked complete', async () => {
  values.set(`receipt:${user}:m.read.private:unthreaded`, receipt('$third', 100, 'm.read.private'));
  values.set('receipt-import:v1', true);
  expect((await countNotificationsWithRules(ctx.env.DB, user, room)).notification_count).toBe(4);
  await get();
  expect(stored('m.read.private')).toEqual({ event_id: '$third', ts: 100 });
  expect((await countNotificationsWithRules(ctx.env.DB, user, room)).notification_count).toBe(0);
  expect(values.get('receipt-mirror:v1')).toBe(true);
  expect(queued).toEqual([]);
  expect(await receiptPosition(ctx.env.DB)).toBe(1);
  await get(); object = new RoomDurableObject(state, ctx.env); await get();
  expect(await receiptPosition(ctx.env.DB)).toBe(1);
});

it('chooses the farther event when legacy and modern durable keys overlap, even if the legacy clock is in the future', async () => {
  values.set(`receipt:${user}:m.read`, { event_id: '$first', receipt_type: 'm.read', ts: future });
  values.set(`receipt:${user}:m.read:unthreaded`, receipt('$third', 100));
  values.set('receipt-import:v1', true);
  expect((await get()).receipts).toEqual({ '$third': { 'm.read': { [user]: { ts: 100 } } } });
  expect(stored()).toEqual({ event_id: '$third', ts: 100 });
  object = new RoomDurableObject(state, ctx.env);
  expect((await get()).receipts).toEqual({ '$third': { 'm.read': { [user]: { ts: 100 } } } });
});

it('retries a failed cache mirror and leaves a newer D1 marker intact', async () => {
  values.set(`receipt:${user}:m.read:unthreaded`, receipt('$first', future));
  values.set('receipt-import:v1', true);
  await recordReadReceipts(ctx.env.DB, room, [receipt('$third', 100)]);
  const batch = vi.spyOn(ctx.env.DB, 'batch').mockRejectedValueOnce(new Error('Temporary D1 failure'));
  await expect(get()).rejects.toThrow('Temporary D1 failure');
  expect(values.has('receipt-mirror:v1')).toBe(false);
  await get();
  expect(batch).toHaveBeenCalledTimes(2);
  expect(values.get('receipt-mirror:v1')).toBe(true);
  expect(stored()).toEqual({ event_id: '$third', ts: 100 });
  expect(await receiptPosition(ctx.env.DB)).toBe(1);
});

it('recovers archived forward progress despite a future cached timestamp and rejects archived backward progress', async () => {
  await set({ ...receipt('$second', future), user_id: remote });
  archived('forward', '$third', 100); archived('backward', '$first', future);
  expect((await get()).receipts).toEqual({ '$third': { 'm.read': { [remote]: { ts: 100 } } } });
  expect(ctx.sqlite.prepare('SELECT event_id,ts FROM receipts WHERE room_id=? AND user_id=?').get(room, remote))
    .toEqual({ event_id: '$third', ts: 100 });
  expect(queued).toEqual([]);
});

it('retries archived receipt recovery after a D1 failure without losing the relational copy', async () => {
  await get();
  values.delete('receipt-import:v1');
  object = new RoomDurableObject(state, ctx.env);
  archived('retry', '$third', 100);
  vi.spyOn(ctx.env.DB, 'batch').mockRejectedValueOnce(new Error('Temporary recovery failure'));
  await expect(get()).rejects.toThrow('Temporary recovery failure');
  expect(values.has('receipt-import:v1')).toBe(false);
  expect((await get()).receipts).toEqual({ '$third': { 'm.read': { [remote]: { ts: 100 } } } });
  expect(ctx.sqlite.prepare('SELECT event_id,ts FROM receipts WHERE room_id=? AND user_id=?').get(room, remote))
    .toEqual({ event_id: '$third', ts: 100 });
  expect(await receiptPosition(ctx.env.DB)).toBe(1);
});

it('publishes the effective forward marker for stale input while retaining delivery retries for duplicate receipts', async () => {
  await publishReadReceipt(ctx.env, room, receipt('$third', 100));
  await publishReadReceipt(ctx.env, room, receipt('$first', future));
  await publishReadReceipt(ctx.env, room, receipt('$third', 100));
  expect(queued).toHaveLength(3);
  for (const value of queued) expect(value.content[room]['m.read'][user]).toEqual({ event_ids: ['$third'], data: { ts: 100 } });
  expect(await receiptPosition(ctx.env.DB)).toBe(1);
  expect(wakeCalls).toBe(3);
});

it.each([true, false])('restores the room after hibernation for websocket read receipts (attachment=%s)', async attachmentHasRoom => {
  values.set('room-id', room);
  const sent: string[] = [];
  const ws = { deserializeAttachment: () => ({ id: 'socket', userId: user, deviceId: 'DEVICE', ...(attachmentHasRoom ? { roomId: room } : {}) }),
    send: (message: string) => sent.push(message) };
  sockets.push(ws); object = new RoomDurableObject(state, ctx.env);
  await object.webSocketMessage(ws as any, JSON.stringify({ type: 'read', event_id: '$third' }));
  expect(stored()).toMatchObject({ event_id: '$third' });
  expect(queued).toHaveLength(1); expect(wakeCalls).toBe(1);
  expect(queued[0].content[room]['m.read'][user].event_ids).toEqual(['$third']);
  expect(JSON.parse(sent[0])).toMatchObject({ type: 'receipt', room_id: room, event_id: '$third' });
});
