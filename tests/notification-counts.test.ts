import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import receipts from '../src/api/receipts';
import { countNotificationsWithRules } from '../src/services/push-rule-evaluator';
import { testEnv } from './federation-helpers';

vi.mock('cloudflare:workers', () => ({ DurableObject: class {
  ctx: any; env: any; constructor(ctx: any, env: any) { this.ctx = ctx; this.env = env; }
} }));
import { RoomDurableObject } from '../src/durable-objects/RoomDurableObject';

const roomId = '!notifications:local.example';
const userId = '@alice:local.example';
const other = '@other:remote.example';
let ctx: Awaited<ReturnType<typeof testEnv>>;
let queued: unknown[];
let log: ReturnType<typeof vi.spyOn>;

beforeEach(async () => {
  ctx = await testEnv(); queued = [];
  log = vi.spyOn(console, 'log').mockImplementation(() => {});
  ctx.sqlite.prepare('INSERT INTO rooms(room_id) VALUES (?)').run(roomId);
  for (const user of [userId, other]) ctx.sqlite.prepare(`
    INSERT INTO room_memberships(room_id,user_id,membership,event_id) VALUES (?,?,'join','$joined')`).run(roomId, user);
  const values = new Map<string, unknown>();
  const state = { storage: {
    get: async (key: string) => values.get(key),
    put: async (key: string | Record<string, unknown>, value?: unknown) => {
      if (typeof key === 'string') values.set(key, value);
      else for (const [k, v] of Object.entries(key)) values.set(k, v);
    },
    list: async ({ prefix }: { prefix: string }) => new Map([...values].filter(([key]) => key.startsWith(prefix))),
  }, getWebSockets: () => [] };
  const object = new RoomDurableObject(state as any, ctx.env);
  ctx.env.ROOMS = { idFromName: (id: string) => id, get: () => ({ fetch: (request: Request) => object.fetch(request) }) } as any;
  ctx.env.FEDERATION = { idFromName: (id: string) => id, get: () => ({ fetch: async (request: Request) => {
    queued.push(await request.json()); return Response.json({});
  } }) } as any;
});
afterEach(() => { ctx.sqlite.close(); log.mockRestore(); });

function message(id: string, position: number | null, content: Record<string, unknown> = { body: 'Message', msgtype: 'm.text' },
  type = 'm.room.message', sender = other, room = roomId) {
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
    VALUES (?,?,?,?,?,1,1,'[]','[]',?)`).run(id, room, sender, type, JSON.stringify(content), position);
}
async function send(type: string, eventId: string, threadId?: string) {
  const response = await receipts.request(`/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/receipt/${type}/${encodeURIComponent(eventId)}`, {
    method: 'POST', headers: { Authorization: 'Bearer token' }, body: JSON.stringify(threadId ? { thread_id: threadId } : {}),
  }, ctx.env);
  expect(response.status).toBe(200);
}
function counts() { return countNotificationsWithRules(ctx.env.DB, userId, roomId); }
function marker(eventId: string) {
  ctx.sqlite.prepare(`INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,?,'m.fully_read',?)
    ON CONFLICT(user_id,room_id,event_type) DO UPDATE SET content=excluded.content`).run(userId, roomId, JSON.stringify({ event_id: eventId }));
}

it('evaluates default message and encrypted push rules using their stored event type', async () => {
  message('$plain', 1);
  message('$encrypted', 2, { algorithm: 'm.megolm.v1.aes-sha2', ciphertext: 'ciphertext' }, 'm.room.encrypted');
  message('$self', 3, undefined, 'm.room.message', userId);
  message('$historical', null);
  expect(await counts()).toEqual({ notification_count: 2, highlight_count: 0 });
});

it('loads push rules once per count and observes rule changes on the next count', async () => {
  for (let i = 1; i <= 100; i++) message(`$message-${i}`, i);
  const prepare = vi.spyOn(ctx.env.DB, 'prepare');
  expect((await counts()).notification_count).toBe(100);
  const ruleReads = () => prepare.mock.calls.filter(([sql]) => /FROM push_rules/i.test(sql)).length;
  expect(ruleReads()).toBe(1);
  ctx.sqlite.prepare(`INSERT INTO push_rules(user_id,rule_id,kind,priority,enabled,actions)
    VALUES (?, '.m.rule.master', 'override', 0, 1, '["dont_notify"]')`).run(userId);
  expect((await counts()).notification_count).toBe(0);
  expect(ruleReads()).toBe(2);
});

it('clears notifications and highlights with a private receipt while preserving the fully-read anchor and keeping federation private', async () => {
  message('$anchor', 1);
  message('$mention', 2, { body: 'Hello alice', msgtype: 'm.text' });
  marker('$anchor');
  expect(await counts()).toEqual({ notification_count: 1, highlight_count: 1 });
  await send('m.read.private', '$mention');
  expect(await counts()).toEqual({ notification_count: 0, highlight_count: 0 });
  expect(queued).toEqual([]);
  expect(ctx.sqlite.prepare("SELECT content FROM account_data WHERE user_id=? AND room_id=? AND event_type='m.fully_read'")
    .get(userId, roomId)).toMatchObject({ content: JSON.stringify({ event_id: '$anchor' }) });
});

it('uses the farther public or private receipt without rewinding counts when the public receipt lags', async () => {
  message('$earlier', 1); message('$later', 2);
  await send('m.read.private', '$later');
  await send('m.read', '$earlier');
  expect(queued).toHaveLength(1);
  expect(await counts()).toEqual({ notification_count: 0, highlight_count: 0 });
});

it('keeps main and thread read positions separate while unthreaded receipts clear every timeline', async () => {
  message('$root', 1);
  const threaded = (root: string) => ({ body: 'Thread reply', msgtype: 'm.text', 'm.relates_to': { rel_type: 'm.thread', event_id: root } });
  message('$reply-a', 2, threaded('$root'));
  message('$main', 3);
  message('$reply-b', 4, threaded('$root'));
  message('$other-thread', 5, threaded('$other-root'));
  await send('m.read.private', '$main', 'main');
  expect((await counts()).notification_count).toBe(3);
  await send('m.read.private', '$reply-b', '$root');
  expect((await counts()).notification_count).toBe(1);
  await send('m.read.private', '$other-thread');
  expect((await counts()).notification_count).toBe(0);
});

it('filters already-read main events before the 500-message cap so later unread threads remain counted', async () => {
  for (let i = 1; i <= 510; i++) message(`$main-${i}`, i);
  message('$thread-after-main', 511, { body: 'Unread reply', msgtype: 'm.text', 'm.relates_to': { rel_type: 'm.thread', event_id: '$main-1' } });
  await send('m.read.private', '$main-510', 'main');
  expect((await counts()).notification_count).toBe(1);
});

it('ignores receipts and fully-read markers pointing into another room and receipts owned by another user', async () => {
  message('$unread', 1);
  const foreignRoom = '!foreign:local.example';
  ctx.sqlite.prepare('INSERT INTO rooms(room_id) VALUES (?)').run(foreignRoom);
  message('$foreign', 100, undefined, 'm.room.message', other, foreignRoom);
  marker('$foreign');
  ctx.sqlite.prepare(`INSERT INTO receipts(room_id,user_id,receipt_type,event_id,thread_id,ts)
    VALUES (?,?,'m.read.private',?,'',1)`).run(roomId, userId, '$foreign');
  ctx.sqlite.prepare(`INSERT INTO receipts(room_id,user_id,receipt_type,event_id,thread_id,ts)
    VALUES (?,?,'m.read.private',?,'',1)`).run(roomId, other, '$unread');
  expect((await counts()).notification_count).toBe(1);
});

it('preserves explicit zero as a boundary and tolerates malformed event JSON', async () => {
  message('$unread', 1);
  marker('$unread');
  expect((await counts()).notification_count).toBe(0);
  expect((await countNotificationsWithRules(ctx.env.DB, userId, roomId, 0)).notification_count).toBe(1);
  ctx.sqlite.prepare("UPDATE events SET event_type='m.room.encrypted',content=? WHERE event_id=?").run('malformed', '$unread');
  expect((await countNotificationsWithRules(ctx.env.DB, userId, roomId, 0)).notification_count).toBe(1);
});
