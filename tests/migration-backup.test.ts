import { beforeAll, beforeEach, afterEach, expect, it, vi } from 'vitest';
import { Hono } from 'hono';
import backup, { EXPORT_BINDINGS } from '../src/api/migration-backup';
import { migrationExport } from '../src/durable-objects/migration-export';
import { migrationFreeze } from '../src/middleware/migration-freeze';
import { testEnv } from './federation-helpers';
import { hashPassword } from '../src/utils/crypto';
import type { AppEnv } from '../src/types';
vi.mock('cloudflare:workers', () => ({ DurableObject: class { ctx: any; env: any; constructor(ctx: any, env: any) { this.ctx = ctx; this.env = env; } } }));
import { FederationDurableObject } from '../src/durable-objects/FederationDurableObject';
import { RoomDurableObject } from '../src/durable-objects/RoomDurableObject';

let context: Awaited<ReturnType<typeof testEnv>>;
let passwordHash: string;
const password = 'correct password';
const id = 'a'.repeat(64);
const headers = { Authorization: 'Bearer token', 'Content-Type': 'application/json' };
const auth = { type: 'm.login.password', password };
const send = (body: unknown, h = headers) => backup.request('/admin/api/migration/export', { method: 'POST', headers: h, body: JSON.stringify(body) }, context.env);
beforeAll(async () => { passwordHash = await hashPassword(password); });
beforeEach(async () => {
  context = await testEnv();
  context.sqlite.prepare('UPDATE users SET admin=1,password_hash=?').run(passwordHash);
  context.env.MIGRATION_EXPORT_OBJECTS = JSON.stringify(Object.fromEntries(EXPORT_BINDINGS.map(key => [key, [id]])));
});
afterEach(() => context.sqlite.close());

it('requires a valid administrator token and fresh password before object access', async () => {
  const get = vi.fn();
  context.env.USER_KEYS = { get } as any;
  expect((await send({ auth, namespace: 'USER_KEYS', object_id: id }, { Authorization: 'Bearer unknown', 'Content-Type': 'application/json' })).status).toBe(401);
  context.sqlite.prepare('UPDATE users SET admin=0').run();
  expect((await send({ auth, namespace: 'USER_KEYS', object_id: id })).status).toBe(403);
  context.sqlite.prepare('UPDATE users SET admin=1').run();
  for (const confirmation of [undefined, { type: 'm.login.password', session: 'completed' }, { type: 'm.login.password', password: 'wrong' }, { ...auth, user: '@other:local.example' }]) {
    expect([401, 403]).toContain((await send({ auth: confirmation, namespace: 'USER_KEYS', object_id: id })).status);
  }
  expect(get).not.toHaveBeenCalled();
});

it('fails closed for disabled, malformed or unapproved namespace/object inventories', async () => {
  const get = vi.fn();
  context.env.USER_KEYS = { get } as any;
  const valid = context.env.MIGRATION_EXPORT_OBJECTS;
  for (const value of [undefined, '{}', '{invalid', JSON.stringify({ ...JSON.parse(valid!), USER_KEYS: ['bad'] })]) {
    context.env.MIGRATION_EXPORT_OBJECTS = value;
    expect((await send({ auth, namespace: 'USER_KEYS', object_id: id })).status).toBe(403);
  }
  context.env.MIGRATION_EXPORT_OBJECTS = valid;
  expect((await send({ auth, namespace: 'USER_KEYS', object_id: 'b'.repeat(64) })).status).toBe(403);
  expect((await send({ auth, namespace: 'DB', object_id: id })).status).toBe(400);
  expect((await send({ auth, namespace: 'USER_KEYS', object_id: id, cursor: {}, limit: 100 })).status).toBe(400);
  expect(get).not.toHaveBeenCalled();
});

function stateFor(data: Map<string, unknown>) {
  const storage: any = {
    list: async ({ startAfter, limit }: { startAfter?: string; limit: number }) => new Map([...data].sort(([a], [b]) => a.localeCompare(b)).filter(([key]) => startAfter === undefined || key > startAfter).slice(0, limit)),
    getAlarm: async () => 1234,
    transaction: async (fn: (s: any) => unknown) => fn(storage),
    put: vi.fn(), delete: vi.fn(), setAlarm: vi.fn(), get: vi.fn(),
  };
  return { storage, id: { toString: () => id }, getWebSockets: () => [] };
}

it('exports all raw keys through bounded pages, including E2EE values absent from tracked key lists', async () => {
  const data = new Map<string, unknown>(Array.from({ length: 41 }, (_, i) => [`key${String(i).padStart(3, '0')}`, { index: i }]));
  data.set('account_data:m.secret_storage.orphan', { encrypted: 'preserve exactly' });
  const state = stateFor(data);
  context.env.USER_KEYS = { idFromString: (value: string) => { expect(value).toBe(id); return value; }, get: () => ({ fetch: (url: string) => migrationExport(new Request(url), state as any) }) } as any;
  let cursor: string | undefined;
  const restored = new Map();
  do {
    const response = await send({ auth, namespace: 'USER_KEYS', object_id: id, cursor, limit: 8 });
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    const page = await response.json() as any;
    expect(page.entries.length).toBeLessThanOrEqual(8);
    expect(page.alarm).toBe(1234);
    for (const [key, value] of page.entries) { expect(restored.has(key)).toBe(false); restored.set(key, value); }
    cursor = page.next_cursor ?? undefined;
  } while (cursor !== undefined);
  expect(restored).toEqual(data);
  expect(state.storage.put).not.toHaveBeenCalled();
  expect(state.storage.delete).not.toHaveBeenCalled();
});

it('refuses write methods and non-JSON structured values without a misleading lossy backup', async () => {
  const state = stateFor(new Map([['binary', new Uint8Array([1, 2])]]));
  expect((await migrationExport(new Request('http://internal/migration-export', { method: 'POST' }), state as any)).status).toBe(405);
  await expect(migrationExport(new Request('http://internal/migration-export'), state as any)).rejects.toThrow('typed storage');
});

it('holds federation alarm delivery without deleting queued encrypted messages while frozen', async () => {
  const state = stateFor(new Map([['edu:peer:message', { content: 'ciphertext' }]]));
  context.env.MIGRATION_FREEZE = '1';
  const object = new FederationDurableObject(state as any, context.env);
  await object.alarm();
  expect(state.storage.setAlarm).toHaveBeenCalledOnce();
  expect(state.storage.delete).not.toHaveBeenCalled();
  expect((await object.fetch(new Request('http://internal/queue', { method: 'POST', body: '{}' }))).status).toBe(503);
  expect((await object.fetch(new Request('http://internal/migration-export'))).status).toBe(200);
});

it('blocks preexisting receipt WebSocket writes during freeze', async () => {
  context.env.MIGRATION_FREEZE = '1';
  const state = stateFor(new Map());
  const room = new RoomDurableObject(state as any, context.env);
  const socket = { close: vi.fn(), deserializeAttachment: vi.fn() };
  await room.webSocketMessage(socket as any, JSON.stringify({ type: 'read', event_id: '$new' }));
  expect(socket.close).toHaveBeenCalledWith(1013, 'Migration maintenance');
  expect(socket.deserializeAttachment).not.toHaveBeenCalled();
});

it('freezes all public traffic except health and the independently authenticated backup route', async () => {
  const app = new Hono<AppEnv>();
  app.use('*', migrationFreeze);
  app.all('*', c => c.json({ ok: true }));
  context.env.MIGRATION_FREEZE = '1';
  for (const [path, method, status] of [['/health', 'GET', 200], ['/admin/api/migration/export', 'POST', 200], ['/health', 'POST', 503], ['/_matrix/client/v3/sync', 'GET', 503], ['/_matrix/federation/v1/send/transaction', 'PUT', 503]]) {
    const response = await app.request(path, { method }, context.env);
    expect(response.status).toBe(status);
    if (status === 503) expect(response.headers.get('Retry-After')).toBe('30');
  }
});
