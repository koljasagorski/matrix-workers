import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import receipts, { getReceiptsForRoom } from '../src/api/receipts';
import federation from '../src/api/federation';
import sync from '../src/api/sync';
import slidingSync from '../src/api/sliding-sync';
import { receiveReadReceipts, receiptPosition } from '../src/services/read-receipts';
import { signFederationRequest } from '../src/services/federation-keys';
import { testEnv, roomFixture } from './federation-helpers';

vi.mock('cloudflare:workers', () => ({ DurableObject: class {
  ctx: any; env: any; constructor(ctx: any, env: any) { this.ctx = ctx; this.env = env; }
} }));
import { RoomDurableObject } from '../src/durable-objects/RoomDurableObject';
import { SyncDurableObject } from '../src/durable-objects/SyncDurableObject';
import { isServerAllowedInRoom } from '../src/services/server-acl';

let ctx: Awaited<ReturnType<typeof testEnv>>;
let fixture: Awaited<ReturnType<typeof roomFixture>>;
let object: RoomDurableObject;
let objectState: DurableObjectState;
let values: Map<string, unknown>;
let queued: any[];
let wakeCalls: number;
let waitCalls: number;
let onWait: (() => Promise<void>) | undefined;
let log: ReturnType<typeof vi.spyOn>;
const room = '!receipts:local.example';
const user = '@alice:local.example';
const remote = '@creator:remote.example';
const headers = { Authorization: 'Bearer token', 'Content-Type': 'application/json' };

beforeEach(async () => {
  ctx = await testEnv(); fixture = await roomFixture('10');
  queued = []; values = new Map(); wakeCalls = 0; waitCalls = 0; onWait = undefined;
  log = vi.spyOn(console, 'log').mockImplementation(() => {});
  ctx.sqlite.prepare("INSERT INTO rooms(room_id,room_version,creator_id) VALUES (?,'10',?)").run(room, user);
  for (const member of [user, remote, '@second:remote.example']) {
    ctx.sqlite.prepare("INSERT INTO room_memberships(room_id,user_id,membership,event_id) VALUES (?,?,'join','$join')").run(room, member);
  }
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
    VALUES ('$message',?,?,'m.room.message','{"body":"hello","msgtype":"m.text"}',1,1,'[]','[]',1)`).run(room, user);
  const storage = { get: async (key: string) => values.get(key),
    put: async (key: string | Record<string, unknown>, value?: unknown) => {
      if (typeof key === 'string') values.set(key, value);
      else for (const [k, v] of Object.entries(key)) values.set(k, v);
    }, list: async ({ prefix }: { prefix: string }) => new Map([...values].filter(([key]) => key.startsWith(prefix))) };
  objectState = { storage, getWebSockets: () => [] } as any;
  object = new RoomDurableObject(objectState, ctx.env);
  ctx.env.ROOMS = { idFromName: (s: string) => s, get: () => ({ fetch: (request: Request) => object.fetch(request) }) } as any;
  ctx.env.FEDERATION = { idFromName: (s: string) => s, get: () => ({ fetch: async (request: Request) => {
    queued.push(await request.json()); return Response.json({});
  } }) } as any;
  const states = new Map<string, unknown>();
  ctx.env.SYNC = { idFromName: (s: string) => s, get: () => ({ fetch: async (input: Request | URL | string, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url);
    if (url.pathname === '/notify-device') { wakeCalls++; return Response.json({ success: true }); }
    if (url.pathname === '/wait-for-events') { waitCalls++; await onWait?.(); return Response.json({ hasEvents: !!onWait }); }
    const id = url.searchParams.get('conn_id')!;
    if (request.method === 'PUT') { states.set(id, await request.json()); return Response.json({}); }
    return Response.json(states.get(id) ?? null);
  } }) } as any;
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(fixture.keyResponse)));
});
afterEach(() => { ctx.sqlite.close(); log.mockRestore(); vi.unstubAllGlobals(); });

function content(ts = 150, eventId = '$message', threadId?: string) {
  return { [room]: { 'm.read': { [remote]: { event_ids: [eventId], data: { ts, ...(threadId ? { thread_id: threadId } : {}) } } } } };
}
async function send(type = 'm.read', body: unknown = {}) {
  return receipts.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/receipt/${type}/%24message`, {
    method: 'POST', headers, body: JSON.stringify(body),
  }, ctx.env);
}
async function sliding(pos?: string) {
  return slidingSync.request('/_matrix/client/v4/sync?timeout=' + (pos ? '25000' : '0') + (pos ? `&pos=${pos}` : ''), {
    method: 'POST', headers, body: JSON.stringify({ conn_id: 'receipts', extensions: { receipts: { enabled: true } } }),
  }, ctx.env);
}

it('accepts an authenticated receipt EDU, preserves metadata and returns it in classic sync without echoing federation', async () => {
  const path = '/_matrix/federation/v1/send/receipt';
  const body = { pdus: [], edus: [{ edu_type: 'm.receipt', content: content(150, '$message', 'main') }] };
  const authorization = await signFederationRequest('PUT', path, fixture.remote, 'local.example', fixture.key, body);
  const response = await federation.request(path, { method: 'PUT', headers: { Authorization: authorization }, body: JSON.stringify(body) }, ctx.env);
  expect(response.status).toBe(200);
  expect(queued).toEqual([]); expect(wakeCalls).toBe(1);
  const synced = await (await sync.request('/_matrix/client/v3/sync', { headers }, ctx.env)).json();
  expect(synced.rooms.join[room].ephemeral.events).toContainEqual({ type: 'm.receipt', content: {
    '$message': { 'm.read': { [remote]: { ts: 150, thread_id: 'main' } } },
  } });
  expect(synced.next_batch).toBe('s1_td0_dk0_rr1');
});

it('federates public receipts once per destination, including thread metadata; keeps private receipts local', async () => {
  expect((await send('m.read', { thread_id: 'main' })).status).toBe(200);
  expect(queued).toHaveLength(1);
  expect(queued[0]).toMatchObject({ destination: 'remote.example', edu_type: 'm.receipt', content: {
    [room]: { 'm.read': { [user]: { event_ids: ['$message'], data: { ts: expect.any(Number), thread_id: 'main' } } } },
  } });
  expect((await send('m.read.private')).status).toBe(200);
  expect(queued).toHaveLength(1);
  const own = await getReceiptsForRoom(ctx.env, room, user);
  expect(own.content.$message['m.read.private'][user]).toBeDefined();
  const other = await getReceiptsForRoom(ctx.env, room, remote);
  expect(other.content.$message['m.read.private']).toBeUndefined();
});

it('publishes the public receipt from read_markers and keeps the private receipt private', async () => {
  const response = await receipts.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/read_markers`, {
    method: 'POST', headers, body: JSON.stringify({ 'm.fully_read': '$message', 'm.read': '$message', 'm.read.private': '$message' }),
  }, ctx.env);
  expect(response.status).toBe(200); expect(queued).toHaveLength(1); expect(wakeCalls).toBe(2);
});

it('ignores stale receipts and spoofed users, private EDUs and receipts from non-members', async () => {
  await receiveReadReceipts(ctx.env, 'remote.example', content(200, '$new'));
  await receiveReadReceipts(ctx.env, 'remote.example', content(100, '$old'));
  await receiveReadReceipts(ctx.env, 'evil.example', content(300, '$spoofed'));
  await receiveReadReceipts(ctx.env, 'remote.example', { [room]: { 'm.read.private': { [remote]: { event_ids: ['$private'], data: { ts: 300 } } },
    'm.read': { '@outsider:remote.example': { event_ids: ['$outsider'], data: { ts: 300 } } } } });
  expect((await getReceiptsForRoom(ctx.env, room, user)).content).toEqual({ '$new': { 'm.read': { [remote]: { ts: 200 } } } });
  expect(await receiptPosition(ctx.env.DB)).toBe(1);
});

it('recovers archived receipt EDUs once, persists them across object restarts, and preserves newer live receipts', async () => {
  for (const [id, origin, value] of [['one', 'remote.example', content(100, '$old')], ['two', 'remote.example', content(200, '$archived')],
    ['spoof', 'evil.example', content(999, '$spoof')]] as const) {
    ctx.sqlite.prepare("INSERT INTO processed_edus(edu_id,edu_type,origin,processed_at,content) VALUES (?,'m.receipt',?,?,?)")
      .run(id, origin, Date.now(), JSON.stringify(value));
  }
  await receiveReadReceipts(ctx.env, 'remote.example', content(300, '$live', 'main'));
  const first = await getReceiptsForRoom(ctx.env, room, user);
  expect(first.content.$archived['m.read'][remote].ts).toBe(200);
  expect(first.content.$live['m.read'][remote]).toEqual({ ts: 300, thread_id: 'main' });
  expect(first.content.$old).toBeUndefined(); expect(first.content.$spoof).toBeUndefined();
  ctx.sqlite.exec('DELETE FROM processed_edus');
  object = new RoomDurableObject(objectState, ctx.env);
  expect((await getReceiptsForRoom(ctx.env, room, user)).content).toEqual(first.content);
});

it('returns receipts arriving during a classic sync wait in the same response', async () => {
  onWait = async () => { await receiveReadReceipts(ctx.env, 'remote.example', content()); };
  const response = await sync.request('/_matrix/client/v3/sync?since=s1_td0_dk0&timeout=25000', { headers }, ctx.env);
  const data = await response.json();
  expect(waitCalls).toBe(1);
  expect(data.rooms.join[room].ephemeral.events[0].content.$message['m.read'][remote].ts).toBe(150);
  expect(data.next_batch).toBe('s1_td0_dk0_rr1');
});

it('honors the classic ephemeral filter after a receipt wakes long polling', async () => {
  onWait = async () => { await receiveReadReceipts(ctx.env, 'remote.example', content()); };
  const filter = encodeURIComponent(JSON.stringify({ room: { ephemeral: { not_types: ['m.receipt'] } } }));
  const response = await sync.request(`/_matrix/client/v3/sync?since=s1_td0_dk0&timeout=25000&filter=${filter}`, { headers }, ctx.env);
  const data = await response.json();
  expect(waitCalls).toBe(1);
  expect(data.rooms.join[room].ephemeral.events).toEqual([]);
  expect(data.next_batch).toBe('s1_td0_dk0_rr1');
});

it('rejects foreign and missing read-marker targets before any receipt or account-data write', async () => {
  ctx.sqlite.prepare("INSERT INTO rooms(room_id,room_version,creator_id) VALUES ('!other:local.example','10',?)").run(user);
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events)
    VALUES ('$foreign','!other:local.example',?,'m.room.message','{}',1,1,'[]','[]')`).run(user);
  for (const id of ['$foreign', '$missing']) {
    const direct = await receipts.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/receipt/m.fully_read/${id}`, {
      method: 'POST', headers, body: '{}',
    }, ctx.env);
    expect(direct.status).toBe(404);
    const combined = await receipts.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/read_markers`, {
      method: 'POST', headers, body: JSON.stringify({ 'm.fully_read': '$message', 'm.read': '$message', 'm.read.private': id }),
    }, ctx.env);
    expect(combined.status).toBe(404);
  }
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM account_data').get()).toEqual({ n: 0 });
  expect(queued).toEqual([]);
  expect(await receiptPosition(ctx.env.DB)).toBe(0);
});

it('returns receipts arriving during sliding sync in the same response and advances its independent receipt cursor', async () => {
  const first = await (await sliding()).json();
  onWait = async () => { await receiveReadReceipts(ctx.env, 'remote.example', content()); };
  const next = await (await sliding(first.pos)).json();
  expect(waitCalls).toBe(1);
  expect(next.extensions.receipts.rooms[room].content.$message['m.read'][remote].ts).toBe(150);
  expect(next.pos).toBe('1_dk0_rr1');
});

it('returns an already changed receipt immediately instead of waiting for unrelated room messages', async () => {
  const first = await (await sliding()).json();
  await receiveReadReceipts(ctx.env, 'remote.example', content());
  const next = await (await sliding(first.pos)).json();
  expect(next.extensions.receipts.rooms[room].content.$message).toBeDefined();
  expect(waitCalls).toBe(0);
});

it('closes the receipt race before wait registration', async () => {
  await receiveReadReceipts(ctx.env, 'remote.example', content());
  const syncObject = new SyncDurableObject({} as any, ctx.env);
  const start = Date.now();
  const response = await syncObject.fetch(new Request('https://internal/wait-for-events', {
    method: 'POST', body: JSON.stringify({ receiptsSince: 0, timeout: 25000 }),
  }));
  expect(await response.json()).toEqual({ hasEvents: true });
  expect(Date.now() - start).toBeLessThan(1000);
});

it('never broadcasts private receipts to another user over room WebSockets', async () => {
  const own = { deserializeAttachment: () => ({ userId: user }), send: vi.fn() };
  const other = { deserializeAttachment: () => ({ userId: remote }), send: vi.fn() };
  object = new RoomDurableObject({ ...objectState, getWebSockets: () => [own, other] } as any, ctx.env);
  expect((await send('m.read.private')).status).toBe(200);
  expect(own.send).toHaveBeenCalledTimes(1); expect(other.send).not.toHaveBeenCalled();
});

it('rejects malformed receipt bodies and does not disclose receipts to users outside the room', async () => {
  for (const value of [null, [], { thread_id: 1 }, { thread_id: '' }]) expect((await send('m.read', value)).status).toBe(400);
  const invalid = await receipts.request(`/_matrix/client/v3/rooms/${encodeURIComponent(room)}/read_markers`, {
    method: 'POST', headers, body: JSON.stringify({ 'm.read': {} }),
  }, ctx.env);
  expect(invalid.status).toBe(400);
  await receiveReadReceipts(ctx.env, 'remote.example', content());
  expect((await getReceiptsForRoom(ctx.env, room, '@outsider:local.example')).content).toEqual({});
});

it('applies case-insensitive room server ACLs to incoming receipts and ignores port numbers', async () => {
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,state_key,content,origin_server_ts,depth,auth_events,prev_events)
    VALUES ('$acl',?,?,'m.room.server_acl','',?,1,1,'[]','[]')`).run(room, user,
      JSON.stringify({ allow: ['*.example'], deny: ['REMOTE.example'], allow_ip_literals: false }));
  ctx.sqlite.prepare("INSERT INTO room_state(room_id,event_type,state_key,event_id) VALUES (?,'m.room.server_acl','','$acl')").run(room);
  expect(await isServerAllowedInRoom(ctx.env.DB, room, 'remote.example:8448')).toBe(false);
  expect(await isServerAllowedInRoom(ctx.env.DB, room, 'other.example:443')).toBe(true);
  expect(await isServerAllowedInRoom(ctx.env.DB, room, '[2001:db8::1]:443')).toBe(false);
  await receiveReadReceipts(ctx.env, 'remote.example', content());
  expect((await getReceiptsForRoom(ctx.env, room, user)).content).toEqual({});
});
