import { afterEach, beforeEach, expect, it } from 'vitest';
import { mkdtemp, readFile, writeFile, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { exportCloudflare } from '../scripts/migration/export-cloudflare.mjs';
import { exportDurableObjects } from '../scripts/migration/export-durable-objects.mjs';
import { verifyMigration } from '../scripts/verify-migration.mjs';
import { encodeStoredValue } from '../src/durable-objects/migration-export';
import { testEnv } from './federation-helpers';
import { createHash } from 'node:crypto';
import { buildReplayPlan } from '../scripts/migration/prepare-federation-replay.mjs';

let directory: string;
const id = 'a'.repeat(64);
const account = '9a562df9759b0d525872d399986f10ef';
const classes = { ROOMS: 'RoomDurableObject', SYNC: 'SyncDurableObject', FEDERATION: 'FederationDurableObject', CALL_ROOMS: 'CallRoomDurableObject', ADMIN: 'AdminDurableObject', USER_KEYS: 'UserKeysDurableObject', PUSH: 'PushDurableObject', RATE_LIMIT: 'RateLimitDurableObject' };
const kv = ['SESSIONS', 'DEVICE_KEYS', 'CACHE', 'CROSS_SIGNING_KEYS', 'ACCOUNT_DATA', 'ONE_TIME_KEYS'];
const reply = (result: unknown, info?: unknown) => Response.json({ success: true, result, result_info: info });
const media = new Uint8Array([0, 255, 10, 99]);
const name = '@alice:local.example';
beforeEach(async () => { directory = await mkdtemp(join(tmpdir(), 'matrix-migration-test-')); });
afterEach(async () => { await rm(directory, { recursive: true, force: true }); });

function cloudflareFixture({ repeatCursor = false, sizeMismatch = false } = {}) {
  const namespaces = Object.entries(classes).map(([binding, classname]) => ({ id: binding, script: 'matrix-workers', class: classname, use_sqlite: true }));
  const bindings = [...Object.keys(classes).map(binding => ({ name: binding, type: 'durable_object_namespace', namespace_id: binding })),
    ...kv.map(binding => ({ name: binding, type: 'kv_namespace', namespace_id: binding })), { name: 'MEDIA', type: 'r2_bucket', bucket_name: 'matrix-workers-media' }];
  const refresh = { userId: name, deviceId: 'DEVICE', accessTokenId: 'saved-access', createdAt: 10 };
  const rows: Record<string, any[]> = {
    SESSIONS: [{ name: `refresh:${'A'.repeat(43)}`, expiration: 2000000000, raw: JSON.stringify(refresh) }],
    DEVICE_KEYS: [{ name: 'binary', raw: media }], CACHE: [{ name: `filter:${name}:old-filter`, raw: JSON.stringify({ room: { timeline: { limit: 20 } } }) }],
  };
  return async (input: string, options: any) => {
    expect(options.headers.Authorization).toBe('Bearer secret-cloudflare-token');
    const url = new URL(input);
    const path = url.pathname.replace(`/client/v4/accounts/${account}`, '');
    if (path.endsWith('/settings')) return reply({ bindings });
    if (path === '/workers/durable_objects/namespaces') return reply(url.searchParams.get('page') === '1' ? namespaces : []);
    if (path.endsWith('/objects') && path.includes('durable_objects')) {
      const binding = path.split('/').at(-2);
      if (binding !== 'USER_KEYS') return reply([]);
      return reply(!url.searchParams.has('cursor') || repeatCursor ? [{ id, hasStoredData: true }] : [], { cursor: id });
    }
    if (path.endsWith('/keys')) {
      const binding = path.split('/').at(-2)!;
      return reply((rows[binding] ?? []).map(({ raw, ...metadata }) => metadata));
    }
    if (path.includes('/values/')) {
      const binding = path.split('/')[4];
      const key = decodeURIComponent(path.slice(path.indexOf('/values/') + 8));
      return new Response(rows[binding].find(row => row.name === key).raw);
    }
    if (path.endsWith('/objects') && path.startsWith('/r2/')) return reply([{ key: 'nested/unsafe% key', etag: 'original', size: sizeMismatch ? 99 : media.length, http_metadata: { contentType: 'application/octet-stream' } }]);
    if (path.includes('/r2/') && path.includes('/objects/')) return new Response(media, { headers: { ETag: 'original' } });
    if (path === '/workflows') return reply([]);
    if (path.endsWith('/secrets')) return reply([{ name: 'SECRET_NAME', type: 'secret_text' }]);
    throw new Error('Unrecognized test path: ' + path);
  };
}

async function doFixture({ fail = false, orphan = false } = {}) {
  const credentials = join(directory, 'credentials.json');
  await writeFile(credentials, JSON.stringify({ userId: name, password: 'password' }));
  let cleanup = 0;
  const values = new Map<string, unknown>([['account_data:m.secret_storage.orphan', { encrypted: 'preserve exact' }], ['device_keys:DEVICE', { user_id: name, device_id: 'DEVICE', keys: {} }], ...Array.from({ length: 20 }, (_, i) => [`unused-${String(i).padStart(2, '0')}`, { optional: undefined }] as [string, unknown])]);
  const fetchImpl = async (input: string, options: any) => {
    const body = JSON.parse(options.body);
    if (input.endsWith('/login')) return Response.json({ user_id: name, device_id: body.device_id, access_token: 'secret-backup-token' });
    if (options.method === 'DELETE') { cleanup++; return Response.json({}); }
    expect(options.headers.Authorization).toBe('Bearer secret-backup-token');
    if (body.action === 'catalogue') return Response.json({ server_name: 'm.sgr.ski', users: orphan ? [] : [{ user_id: name, object_id: id }], namespaces: JSON.parse(await readFile(join(directory, 'approved-do-objects.json'), 'utf8')) });
    if (fail) return new Response(null, { status: 500 });
    const rows = [...values].filter(([key]) => body.cursor === undefined || key > body.cursor).slice(0, 17);
    const entries = rows.slice(0, 16);
    return Response.json({ format: 'matrix-workers-do-v2', encoding: 'structured-clone-v1', object_id: id, entries: entries.map(([key, value]) => [key, encodeStoredValue(value)]), next_cursor: rows.length > 16 ? entries.at(-1)![0] : null, alarm: null });
  };
  return { credentials, fetchImpl, cleanup: () => cleanup };
}

it('exports complete paginated inventories, binary KV/R2, expiry and filters without persisting bearer credentials', async () => {
  const result = await exportCloudflare({ directory, fetchImpl: cloudflareFixture(), token: 'secret-cloudflare-token' });
  expect(result.r2_objects).toBe(1);
  const refresh = JSON.parse(await readFile(join(directory, 'refresh-tokens.json'), 'utf8'));
  expect(refresh.tokens[0].expires_at_ms).toBe(2000000000000);
  const filters = JSON.parse(await readFile(join(directory, 'client-filters.json'), 'utf8'));
  expect(filters.filters[0]).toEqual({ user_id: name, filter_id: 'old-filter', filter: { room: { timeline: { limit: 20 } } } });
  const raw = JSON.parse(await readFile(join(directory, 'kv/DEVICE_KEYS.json'), 'utf8'));
  expect(Buffer.from(raw.keys[0].value_base64, 'base64')).toEqual(Buffer.from(media));
  const manifest = JSON.parse(await readFile(join(directory, 'cloudflare-export.json'), 'utf8'));
  for (const row of manifest.artifacts) expect((await readFile(join(directory, row.path))).includes(Buffer.from('secret-cloudflare-token'))).toBe(false);
  expect((await stat(join(directory, 'approved-do-objects.json'))).mode & 0o777).toBe(0o600);
});

it('aborts repeated cursors or corrupt media instead of claiming a complete export', async () => {
  await expect(exportCloudflare({ directory, inventoryOnly: true, fetchImpl: cloudflareFixture({ repeatCursor: true }), token: 'secret-cloudflare-token' })).rejects.toThrow('Repeated export cursor');
  await expect(exportCloudflare({ directory, fetchImpl: cloudflareFixture({ sizeMismatch: true }), token: 'secret-cloudflare-token' })).rejects.toThrow('Media size mismatch');
});

it('exports orphan E2EE keys through multiple pages and always removes the temporary device on failure', async () => {
  await exportCloudflare({ directory, inventoryOnly: true, fetchImpl: cloudflareFixture(), token: 'secret-cloudflare-token' });
  const fixture = await doFixture();
  await exportDurableObjects({ directory, ...fixture });
  const overlay = JSON.parse(await readFile(join(directory, 'user-keys.json'), 'utf8'));
  expect(overlay.users[name].account_data['m.secret_storage.orphan']).toEqual({ encrypted: 'preserve exact' });
  expect(fixture.cleanup()).toBe(1);
  const failing = await doFixture({ fail: true });
  await expect(exportDurableObjects({ directory, ...failing })).rejects.toThrow('HTTP 500');
  expect(failing.cleanup()).toBe(1);
  const orphan = await doFixture({ orphan: true });
  await expect(exportDurableObjects({ directory, ...orphan })).rejects.toThrow('orphan UserKeys');
  expect(orphan.cleanup()).toBe(1);
  expect(JSON.parse(await readFile(join(directory, 'do-state.json'), 'utf8')).complete).toBe(true);
});

it('verifies real backup checksums and fails closed for corruption or missing operational cutover proof', async () => {
  await exportCloudflare({ directory, fetchImpl: cloudflareFixture(), token: 'secret-cloudflare-token' });
  await exportDurableObjects({ directory, ...await doFixture() });
  const context = await testEnv();
  try { context.sqlite.prepare('VACUUM INTO ?').run(join(directory, 'source.sqlite')); } finally { context.sqlite.close(); }
  const result = await verifyMigration({ directory });
  expect(result.verified).toBe(true);
  expect(result.do_counts.USER_KEYS).toBe(1);
  await expect(verifyMigration({ directory, mode: 'cutover' })).rejects.toThrow();
  await writeFile(join(directory, 'kv/DEVICE_KEYS.json'), '{}');
  await expect(verifyMigration({ directory })).rejects.toThrow('checksum mismatch');
});

it('requires an exact final federation plan plus native ordering and durable-service proofs for cutover', async () => {
  await exportCloudflare({ directory, fetchImpl: cloudflareFixture(), token: 'secret-cloudflare-token' });
  await exportDurableObjects({ directory, ...await doFixture() });
  const context = await testEnv();
  const counts: Record<string, number> = {};
  try {
    for (const table of ['users', 'rooms', 'events']) counts[table] = context.sqlite.prepare(`SELECT count(*) AS count FROM ${table}`).get().count;
    context.sqlite.prepare('VACUUM INTO ?').run(join(directory, 'source.sqlite'));
  } finally { context.sqlite.close(); }
  const load = async (file: string) => JSON.parse(await readFile(join(directory, file), 'utf8'));
  const save = async (file: string, value: unknown) => writeFile(join(directory, file), JSON.stringify(value));
  const hash = (bytes: any) => createHash('sha256').update(bytes).digest('hex');
  const objectId = 'f'.repeat(64);
  const state = await load('do-state.json');
  const destination = 'peer.example';
  state.namespaces.FEDERATION[objectId] = { complete: true, encoding: 'structured-clone-v1', entries: [[`queue:${destination}:$old`, encodeStoredValue({ destination, event_id: '$old', created_at: 1, pdu: { type: 'm.room.encrypted', content: { ciphertext: 'keep exact' } } })]] };
  const inventory = await load('do-inventory.json');
  inventory.namespaces.FEDERATION.objects = [{ id: objectId, hasStoredData: true }];
  await save('do-state.json', state);
  await save('do-inventory.json', inventory);
  for (const file of ['cloudflare-export.json', 'durable-export.json']) {
    const manifest = await load(file);
    for (const row of manifest.artifacts) {
      const bytes = await readFile(join(directory, row.path));
      row.bytes = bytes.length; row.sha256 = hash(bytes);
    }
    await save(file, manifest);
  }
  const sourceHash = hash(await readFile(join(directory, 'source.sqlite')));
  const overlays = Object.fromEntries(await Promise.all([['user_keys', 'user-keys.json'], ['refresh_tokens', 'refresh-tokens.json'], ['client_filters', 'client-filters.json']].map(async ([field, file]) => [field, hash(await readFile(join(directory, file)))])));
  await save('user-import.json', { complete: true, synapse_version: '1.162.0', source_sha256: sourceHash, counts: { users: counts.users }, overlay_sha256: overlays });
  await save('room-import.json', { synapse_version: '1.162.0', source_sha256: sourceHash, rooms: counts.rooms, source_events: counts.events });
  const proof: Record<string, unknown> = { source_sha256: sourceHash, server_name: 'm.sgr.ski' };
  for (const field of ['consistent_frozen_snapshot', 'server_signing_keys_verified', 'e2ee_recovery_verified', 'sessions_verified', 'media_verified', 'federation_queue_reconciled', 'workflows_reconciled', 'rollback_backup_restored', 'all_expected_data_reconciled']) proof[field] = true;
  await save('cutover-proof.json', proof);
  const plan = buildReplayPlan(state, { sourceSha256: sourceHash, stateSha256: hash(await readFile(join(directory, 'do-state.json'))) });
  await save('federation-replay-plan.json', plan);
  await expect(verifyMigration({ directory, mode: 'cutover' })).rejects.toThrow('native ordering gate');
  proof.native_federation_replay_gate_verified = true; proof.federation_replay_service_verified = true;
  await save('cutover-proof.json', proof);
  expect((await verifyMigration({ directory, mode: 'cutover' })).verified).toBe(true);
  plan.transactions[0].transaction_id = 'rewritten';
  await save('federation-replay-plan.json', plan);
  await expect(verifyMigration({ directory, mode: 'cutover' })).rejects.toThrow('differs from');
});
