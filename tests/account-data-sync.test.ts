import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import accountData from '../src/api/account-data';
import tags from '../src/api/tags';
import sync from '../src/api/sync';
import slidingSync from '../src/api/sliding-sync';
import rooms from '../src/api/rooms';
import { hashToken } from '../src/utils/crypto';
import { accountDataPosition, publishAccountData, storeAccountData } from '../src/services/account-data-stream';
import { parseSyncPosition, syncPosition } from '../src/services/sync-positions';
import { testEnv } from './federation-helpers';

vi.mock('cloudflare:workers', () => ({ DurableObject: class {
  ctx: any; env: any; constructor(ctx: any, env: any) { this.ctx = ctx; this.env = env; }
} }));
import { RoomDurableObject } from '../src/durable-objects/RoomDurableObject';
import { SyncDurableObject } from '../src/durable-objects/SyncDurableObject';

const user = '@alice:local.example';
const room = '!account-data:local.example';
const headers = { Authorization: 'Bearer token', 'Content-Type': 'application/json' };
let ctx: Awaited<ReturnType<typeof testEnv>>;
let wakeCalls: number;
let waitCalls: number;
let onWait: (() => Promise<void>) | undefined;

beforeEach(async () => {
  ctx = await testEnv(); wakeCalls = 0; waitCalls = 0; onWait = undefined;
  vi.spyOn(console, 'log').mockImplementation(() => {});
  ctx.sqlite.prepare('INSERT INTO rooms(room_id) VALUES (?)').run(room);
  ctx.sqlite.prepare("INSERT INTO room_memberships(room_id,user_id,membership,event_id) VALUES (?,?,'join','$joined')").run(room, user);
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
    VALUES ('$old',?,?,'m.room.message','{"body":"Old message","msgtype":"m.text"}',1,1,'[]','[]',4000)`).run(room, user);
  const values = new Map<string, unknown>();
  const object = new RoomDurableObject({ storage: {
    get: async (key: string) => values.get(key),
    put: async (key: string | Record<string, unknown>, value?: unknown) => {
      if (typeof key === 'string') values.set(key, value);
      else for (const [k, v] of Object.entries(key)) values.set(k, v);
    }, list: async ({ prefix }: { prefix: string }) => new Map([...values].filter(([key]) => key.startsWith(prefix))),
  }, getWebSockets: () => [] } as any, ctx.env);
  ctx.env.ROOMS = { idFromName: (id: string) => id, get: () => ({ fetch: (request: Request) => object.fetch(request) }) } as any;
  ctx.env.USER_KEYS = { idFromName: (id: string) => id, get: () => ({ fetch: async () => Response.json({}) }) } as any;
  const states = new Map<string, unknown>();
  ctx.env.SYNC = { idFromName: (id: string) => id, get: () => ({ fetch: async (input: Request | string, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url);
    if (url.pathname === '/notify-device') { wakeCalls++; return Response.json({}); }
    if (url.pathname === '/wait-for-events') { waitCalls++; await onWait?.(); return Response.json({ hasEvents: !!onWait }); }
    const key = url.searchParams.get('conn_id')!;
    if (request.method === 'PUT') { states.set(key, await request.json()); return Response.json({}); }
    return Response.json(states.get(key) ?? null);
  } }) } as any;
});
afterEach(() => { ctx.sqlite.close(); vi.restoreAllMocks(); });

async function classic(since?: string, timeout = 0, filter?: unknown) {
  const query = new URLSearchParams({ timeout: String(timeout) });
  if (since) query.set('since', since);
  if (filter) query.set('filter', JSON.stringify(filter));
  const response = await sync.request('/_matrix/client/v3/sync?' + query, { headers }, ctx.env);
  expect(response.status).toBe(200); return await response.json();
}
async function sliding(pos?: string, timeout = 0) {
  const path = '/_matrix/client/v4/sync?timeout=' + timeout + (pos ? '&pos=' + encodeURIComponent(pos) : '');
  const response = await slidingSync.request(path, { method: 'POST', headers,
    body: JSON.stringify({ conn_id: 'account-data', extensions: { account_data: { enabled: true } } }),
  }, ctx.env);
  expect(response.status).toBe(200); return await response.json();
}
async function createRoom(token = 'token', invite?: string[]) {
  const response = await rooms.request('/_matrix/client/v3/createRoom', { method: 'POST',
    headers: { Authorization: `Bearer ${token}` }, body: JSON.stringify({ room_version: '12', ...(invite ? { invite } : {}) }),
  }, ctx.env);
  expect(response.status).toBe(200); return (await response.json() as { room_id: string }).room_id;
}

it('preserves legacy sync tokens and independently round-trips account-data positions', () => {
  expect(parseSyncPosition('s4000_td2_dk3_rr4')).toEqual({ events: 4000, toDevice: 2, keys: 3, receipts: 4, accountData: 0 });
  expect(parseSyncPosition('4000')).toEqual({ events: 4000, toDevice: 0, keys: 0, receipts: 0, accountData: 0 });
  expect(parseSyncPosition(syncPosition(4000, 2, 3, 4, 5))).toEqual({ events: 4000, toDevice: 2, keys: 3, receipts: 4, accountData: 5 });
  expect(syncPosition(4000, 0, 0)).toBe('s4000_td0_dk0');
});

it('returns m.direct and room tags even when the event cursor is thousands of positions ahead', async () => {
  const direct = { '@bob:local.example': [room] };
  const favourites = { tags: { 'm.favourite': { order: 0.2 } } };
  await publishAccountData(ctx.env, user, '', 'm.direct', direct);
  await publishAccountData(ctx.env, user, room, 'm.tag', favourites);
  const response = await classic('s4000_td0_dk0', 25000);
  expect(response.account_data.events).toContainEqual({ type: 'm.direct', content: direct });
  expect(response.rooms.join[room].account_data.events).toEqual([{ type: 'm.tag', content: favourites }]);
  expect(response.next_batch).toBe('s4000_td0_dk0_ad2');
  expect(waitCalls).toBe(0); expect(wakeCalls).toBe(2);
  const next = await classic(response.next_batch);
  expect(next.account_data.events).toEqual([]); expect(next.rooms.join[room].account_data.events).toEqual([]);
});

it('replays unlogged legacy account data once when upgrading an old token to the populated account-data stream', async () => {
  ctx.sqlite.prepare("INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,'','m.direct',?)")
    .run(user, JSON.stringify({ '@legacy:remote.example': [room] }));
  await storeAccountData(ctx.env.DB, user, '', 'com.example.new', { fresh: true });
  const first = await classic('s4000_td0_dk0');
  expect(first.account_data.events).toEqual(expect.arrayContaining([
    { type: 'm.direct', content: { '@legacy:remote.example': [room] } }, { type: 'com.example.new', content: { fresh: true } },
  ]));
  expect(first.next_batch).toBe('s4000_td0_dk0_ad1');
  expect((await classic(first.next_batch)).account_data.events).toEqual([]);
});

it('refreshes global and room account data arriving during the same classic long-poll response', async () => {
  onWait = async () => {
    await publishAccountData(ctx.env, user, '', 'm.direct', { '@bob:local.example': [room] });
    await publishAccountData(ctx.env, user, room, 'm.tag', { tags: { 'm.favourite': {} } });
  };
  const response = await classic('s4000_td0_dk0', 25000);
  expect(waitCalls).toBe(1);
  expect(response.account_data.events).toEqual([{ type: 'm.direct', content: { '@bob:local.example': [room] } }]);
  expect(response.rooms.join[room].account_data.events).toEqual([{ type: 'm.tag', content: { tags: { 'm.favourite': {} } } }]);
  expect(response.next_batch).toBe('s4000_td0_dk0_ad2');
});

it('honors account-data filters again after long-poll wakeup', async () => {
  onWait = async () => {
    await publishAccountData(ctx.env, user, '', 'm.direct', { '@bob:local.example': [room] });
    await publishAccountData(ctx.env, user, room, 'm.tag', { tags: { 'm.favourite': {} } });
  };
  const response = await classic('s4000_td0_dk0', 25000, {
    account_data: { not_types: ['m.direct'] }, room: { account_data: { not_types: ['m.tag'] } },
  });
  expect(response.account_data.events).toEqual([]); expect(response.rooms.join[room].account_data.events).toEqual([]);
  expect(response.next_batch).toBe('s4000_td0_dk0_ad2');
});

it('returns replacement-room tags on the next sync when joining and tagging happened during long polling', async () => {
  let replacement = '';
  onWait = async () => {
    replacement = await createRoom();
    await publishAccountData(ctx.env, user, replacement, 'm.tag', { tags: { 'm.favourite': {} } });
  };
  const waited = await classic('s4000_td0_dk0_ad0', 25000);
  expect(waitCalls).toBe(1);
  expect(waited.rooms.join[replacement]).toBeUndefined();
  expect(parseSyncPosition(waited.next_batch).events).toBe(4000);
  expect(parseSyncPosition(waited.next_batch).accountData).toBe(await accountDataPosition(ctx.env.DB));
  const next = await classic(waited.next_batch);
  expect(next.rooms.join[replacement].account_data.events).toContainEqual({ type: 'm.tag', content: { tags: { 'm.favourite': {} } } });
});

it('returns preferences recorded while invited even when their account-data cursor was already consumed before joining', async () => {
  const bob = '@bob:local.example';
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(bob, 'bob');
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)')
    .run('bob-token', await hashToken('bob-token'), bob, 'BOB');
  const invited = await createRoom('bob-token', [user]);
  await publishAccountData(ctx.env, user, invited, 'm.tag', { tags: { 'm.favourite': { order: 0.1 } } });
  const before = await classic('s4000_td0_dk0_ad0');
  expect(before.rooms.invite[invited]).toBeDefined(); expect(before.rooms.join[invited]).toBeUndefined();
  const join = await rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(invited)}/join`, { method: 'POST', headers, body: '{}' }, ctx.env);
  expect(join.status).toBe(200);
  const joined = await classic(before.next_batch);
  expect(joined.rooms.join[invited].account_data.events).toContainEqual({ type: 'm.tag', content: { tags: { 'm.favourite': { order: 0.1 } } } });
});

it('closes the account-data write race before wait registration and checks only the requesting user', async () => {
  await storeAccountData(ctx.env.DB, user, '', 'm.direct', { '@bob:local.example': [room] });
  const object = new SyncDurableObject({} as any, ctx.env);
  const start = Date.now();
  const response = await object.fetch(new Request('https://internal/wait-for-events', {
    method: 'POST', body: JSON.stringify({ userId: user, accountDataSince: 0, timeout: 25000 }),
  }));
  expect(await response.json()).toEqual({ hasEvents: true }); expect(Date.now() - start).toBeLessThan(1000);
  const other = await object.fetch(new Request('https://internal/wait-for-events', {
    method: 'POST', body: JSON.stringify({ userId: '@bob:local.example', accountDataSince: 0, timeout: 1 }),
  }));
  expect(await other.json()).toEqual({ hasEvents: false });
});

it('returns changed account data immediately in simplified sliding sync and refreshes data received while waiting', async () => {
  const initial = await sliding();
  await publishAccountData(ctx.env, user, room, 'm.tag', { tags: { 'm.favourite': {} } });
  const changed = await sliding(initial.pos, 25000);
  expect(changed.extensions.account_data.rooms[room]).toContainEqual({ type: 'm.tag', content: { tags: { 'm.favourite': {} } } });
  expect(changed.pos).toBe('4000_dk0_ad1'); expect(waitCalls).toBe(0);
  onWait = async () => { await publishAccountData(ctx.env, user, '', 'm.direct', { '@bob:local.example': [room] }); };
  const waited = await sliding(changed.pos, 25000);
  expect(waitCalls).toBe(1);
  expect(waited.extensions.account_data.global).toContainEqual({ type: 'm.direct', content: { '@bob:local.example': [room] } });
  expect(waited.pos).toBe('4000_dk0_ad2');
});

it('keeps simplified long polling active for another user\'s account-data writes and wakes for its own subsequent update', async () => {
  const initial = await sliding();
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run('@bob:local.example', 'bob');
  await storeAccountData(ctx.env.DB, '@bob:local.example', '', 'm.direct', { '@foreign:remote.example': [room] });
  onWait = async () => { await publishAccountData(ctx.env, user, '', 'm.direct', { '@own:remote.example': [room] }); };
  const response = await sliding(initial.pos, 25000);
  expect(waitCalls).toBe(1);
  expect(response.extensions.account_data.global).toEqual([{ type: 'm.direct', content: { '@own:remote.example': [room] } }]);
  expect(parseSyncPosition(response.pos).accountData).toBe(2);
});

it('publishes global/room account-data API writes and tag add/remove through one atomic stream', async () => {
  const globalPath = `/_matrix/client/v3/user/${encodeURIComponent(user)}/account_data/m.direct`;
  expect((await accountData.request(globalPath, { method: 'PUT', headers, body: JSON.stringify({ '@bob:local.example': [room] }) }, ctx.env)).status).toBe(200);
  const roomPath = `/_matrix/client/v3/user/${encodeURIComponent(user)}/rooms/${encodeURIComponent(room)}`;
  expect((await accountData.request(roomPath + '/account_data/com.example.setting', {
    method: 'PUT', headers, body: JSON.stringify({ preference: true }),
  }, ctx.env)).status).toBe(200);
  expect((await tags.request(roomPath + '/tags/m.favourite', { method: 'PUT', headers, body: '{"order":0.3}' }, ctx.env)).status).toBe(200);
  expect((await tags.request(roomPath + '/tags/m.favourite', { method: 'DELETE', headers }, ctx.env)).status).toBe(200);
  const response = await classic('s4000_td0_dk0');
  expect(response.account_data.events).toHaveLength(1);
  expect(response.rooms.join[room].account_data.events).toEqual(expect.arrayContaining([
    { type: 'm.tag', content: { tags: {} } }, { type: 'com.example.setting', content: { preference: true } },
  ]));
  expect(await accountDataPosition(ctx.env.DB)).toBe(4); expect(wakeCalls).toBe(4);
});

it('serializes concurrent writes into unique stream positions and retries notification delivery without duplicate data changes', async () => {
  await Promise.all(Array.from({ length: 12 }, (_, i) => storeAccountData(ctx.env.DB, user, '', `com.example.${i}`, { i })));
  const positions = ctx.sqlite.prepare('SELECT stream_position FROM account_data_changes ORDER BY stream_position').all();
  expect(positions.map(row => row.stream_position)).toEqual(Array.from({ length: 12 }, (_, i) => i + 1));
  await publishAccountData(ctx.env, user, '', 'com.example.0', { i: 0 });
  expect(await accountDataPosition(ctx.env.DB)).toBe(12); expect(wakeCalls).toBe(1);
});

it('honors compare-and-set existence when a row is deleted or created between a read and its write', async () => {
  const old = JSON.stringify({ old: true });
  ctx.sqlite.prepare("INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,'','com.example.cas',?)").run(user, old);
  ctx.sqlite.prepare("DELETE FROM account_data WHERE user_id=? AND event_type='com.example.cas'").run(user);
  expect(await storeAccountData(ctx.env.DB, user, '', 'com.example.cas', { replaced: true }, old)).toBe(false);
  expect(ctx.sqlite.prepare("SELECT content FROM account_data WHERE event_type='com.example.cas'").get()).toBeUndefined();
  ctx.sqlite.prepare("INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,'','com.example.cas',?)").run(user, old);
  expect(await storeAccountData(ctx.env.DB, user, '', 'com.example.cas', { replaced: true }, null)).toBe(false);
  expect(ctx.sqlite.prepare("SELECT content FROM account_data WHERE event_type='com.example.cas'").get()).toEqual({ content: old });
  expect(await accountDataPosition(ctx.env.DB)).toBe(0);
});

it('does not acknowledge data written after a cursor snapshot until a response can include it', async () => {
  const prepare = ctx.env.DB.prepare.bind(ctx.env.DB);
  let injected = false;
  vi.spyOn(ctx.env.DB, 'prepare').mockImplementation(sql => {
    const statement = prepare(sql);
    if (sql.includes("SELECT position FROM stream_positions WHERE stream_name='account_data'")) {
      const first = statement.first.bind(statement);
      statement.first = async () => {
        const snapshot = await first();
        if (!injected) { injected = true; await storeAccountData(ctx.env.DB, user, '', 'm.direct', { '@bob:local.example': [room] }); }
        return snapshot;
      };
    }
    return statement;
  });
  const first = await classic('s4000_td0_dk0');
  expect(first.account_data.events).toHaveLength(1);
  expect(parseSyncPosition(first.next_batch).accountData).toBe(0);
  const next = await classic(first.next_batch);
  expect(next.account_data.events).toHaveLength(1);
  expect(parseSyncPosition(next.next_batch).accountData).toBe(1);
});
