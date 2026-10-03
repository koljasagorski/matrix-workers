// Read-only preflight. It deliberately contains no deletion, DNS or deploy code.
import { readFile, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { resolve, join, sep } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { pathToFileURL } from 'node:url';
import { decodeStoredValue } from './migration/stored-values.mjs';
import { buildReplayPlan } from './migration/prepare-federation-replay.mjs';

const KV = ['SESSIONS', 'DEVICE_KEYS', 'CACHE', 'CROSS_SIGNING_KEYS', 'ACCOUNT_DATA', 'ONE_TIME_KEYS'];
const DO = ['ROOMS', 'SYNC', 'FEDERATION', 'CALL_ROOMS', 'ADMIN', 'USER_KEYS', 'PUSH', 'RATE_LIMIT'];
const hashFile = async file => { const hash = createHash('sha256'); for await (const chunk of createReadStream(file)) hash.update(chunk); return hash.digest('hex'); };
const json = async file => JSON.parse(await readFile(file, 'utf8'));

export async function verifyMigration({ directory, mode = 'backup', userManifest, roomManifest, proofFile } = {}) {
  if (!directory || !['backup', 'cutover', 'retire'].includes(mode)) throw new Error('Expected backup, cutover or retire verification mode');
  directory = resolve(directory);
  const required = (condition, message) => { if (!condition) throw new Error(message); };
  const artifact = async row => {
    required(typeof row.path === 'string' && /^[a-f0-9]{64}$/.test(row.sha256) && Number.isSafeInteger(row.bytes), 'Invalid backup artifact descriptor');
    const file = resolve(directory, row.path);
    required(file.startsWith(directory + sep), 'Backup artifact escapes the private export directory');
    const info = await stat(file);
    required(info.isFile() && info.size === row.bytes && await hashFile(file) === row.sha256, `Backup checksum mismatch: ${row.path}`);
  };
  const cf = await json(join(directory, 'cloudflare-export.json'));
  const durable = await json(join(directory, 'durable-export.json'));
  required(cf.complete === true && durable.complete === true, 'Cloudflare and Durable Object exports must both be complete');
  for (const row of [...cf.artifacts, ...durable.artifacts]) await artifact(row);
  const kvCounts = {};
  for (const name of KV) {
    const data = await json(join(directory, `kv/${name}.json`));
    required(data.complete === true && Array.isArray(data.keys), `Incomplete KV namespace ${name}`);
    required(new Set(data.keys.map(row => row.name)).size === data.keys.length, `Duplicate KV keys in ${name}`);
    for (const row of data.keys) required(typeof row.name === 'string' && typeof row.value_base64 === 'string', `Malformed KV snapshot ${name}`);
    kvCounts[name] = data.keys.length;
  }
  const media = await json(join(directory, 'r2-inventory.json'));
  required(media.complete === true && media.bucket === 'matrix-workers-media', 'Incomplete media inventory');
  required(new Set(media.objects.map(row => row.key)).size === media.objects.length, 'Duplicate media keys');
  for (const row of media.objects) { required(row.size === row.bytes, 'Media inventory size mismatch'); await artifact(row); }
  const inventory = await json(join(directory, 'do-inventory.json'));
  const state = await json(join(directory, 'do-state.json'));
  required(state.complete === true && inventory.worker === 'matrix-workers', 'Incomplete Durable Object inventory');
  const counts = {};
  let pendingPdus = 0; let pendingEdus = 0; let rejectedPdus = 0;
  for (const name of DO) {
    const list = inventory.namespaces[name];
    const objects = state.namespaces[name];
    required(list?.complete === true && objects, `Missing Durable Object namespace ${name}`);
    const expected = list.objects.map(row => row.id).sort();
    required(JSON.stringify(expected) === JSON.stringify(Object.keys(objects).sort()), `Exported object IDs differ from complete inventory: ${name}`);
    for (const object of Object.values(objects)) {
      required(object.complete === true && object.encoding === 'structured-clone-v1' && Array.isArray(object.entries), `Incomplete object export: ${name}`);
      const keys = new Set();
      for (const [key, encoded] of object.entries) {
        required(typeof key === 'string' && !keys.has(key), `Duplicate or malformed Durable Object key: ${name}`);
        keys.add(key); decodeStoredValue(encoded);
        if (name === 'FEDERATION') { if (key.startsWith('queue:')) pendingPdus++; if (key.startsWith('edu:')) pendingEdus++; if (key.startsWith('rejected:')) rejectedPdus++; }
      }
    }
    counts[name] = expected.length;
  }
  const overlay = await json(join(directory, 'user-keys.json'));
  const catalogue = await json(join(directory, 'do-catalogue.json'));
  required(catalogue.server_name === 'm.sgr.ski', 'Durable Object catalogue server identity mismatch');
  const userById = new Map(catalogue.users.map(user => [user.object_id, user.user_id]));
  for (const [id, object] of Object.entries(state.namespaces.USER_KEYS)) {
    const user = userById.get(id);
    required(user || !object.entries.length, 'Unmapped UserKeys object has data');
    if (!user) continue;
    required(overlay.users[user], 'Missing authoritative UserKeys overlay');
    for (const [key, encoded] of object.entries) {
      const value = decodeStoredValue(encoded);
      let mirrored;
      if (key.startsWith('account_data:')) mirrored = overlay.users[user].account_data[key.slice(13)];
      else if (key.startsWith('device_keys:')) mirrored = overlay.users[user].device_keys[key.slice(12)];
      else if (key === 'cross_signing_keys') mirrored = overlay.users[user].cross_signing;
      else if (key === 'signatures') mirrored = overlay.users[user].signatures;
      else continue;
      required(JSON.stringify(value) === JSON.stringify(mirrored), 'Authoritative UserKeys overlay differs from raw storage');
    }
  }
  await json(join(directory, 'refresh-tokens.json'));
  await json(join(directory, 'client-filters.json'));
  const source = join(directory, 'source.sqlite');
  const sourceHash = await hashFile(source);
  const db = new DatabaseSync(source, { readOnly: true });
  let sourceCounts;
  try {
    required(db.prepare('PRAGMA quick_check').get().quick_check === 'ok', 'Source SQLite integrity check failed');
    sourceCounts = Object.fromEntries(['users', 'devices', 'access_tokens', 'rooms', 'events', 'media', 'server_keys'].map(table => [table, db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count]));
    required(sourceCounts.server_keys > 0, 'Original Matrix signing keys are absent');
  } finally { db.close(); }
  const workflows = await json(join(directory, 'workflow-inventory.json'));
  const unfinished = workflows.workflows.flatMap(workflow => (workflow.instance_inventory ?? []).filter(instance => !['complete', 'terminated'].includes(instance.status)));
  if (mode !== 'backup') {
    const users = await json(userManifest ?? join(directory, 'user-import.json'));
    const rooms = await json(roomManifest ?? join(directory, 'room-import.json'));
    required(users.complete === true && users.source_sha256 === sourceHash && rooms.source_sha256 === sourceHash, 'Final import manifests must bind the same complete source snapshot');
    required(users.synapse_version === '1.162.0' && rooms.synapse_version === '1.162.0', 'Import manifests target an unreviewed Synapse version');
    required(users.counts?.users === sourceCounts.users && rooms.rooms === sourceCounts.rooms && rooms.source_events === sourceCounts.events, 'Import source counts mismatch');
    for (const [field, file] of [['user_keys', 'user-keys.json'], ['refresh_tokens', 'refresh-tokens.json'], ['client_filters', 'client-filters.json']]) {
      required(users.overlay_sha256?.[field] === await hashFile(join(directory, file)), `Imported ${field} overlay checksum mismatch or absent`);
    }
    const proof = await json(proofFile ?? join(directory, 'cutover-proof.json'));
    required(proof.source_sha256 === sourceHash && proof.server_name === 'm.sgr.ski', 'Operational proof belongs to another source/server');
    for (const key of ['consistent_frozen_snapshot', 'server_signing_keys_verified', 'e2ee_recovery_verified', 'sessions_verified', 'media_verified', 'federation_queue_reconciled', 'workflows_reconciled', 'rollback_backup_restored', 'all_expected_data_reconciled']) {
      required(proof[key] === true, `Missing verified operational prerequisite: ${key}`);
    }
    if (pendingPdus || pendingEdus) {
      const replay = await json(join(directory, 'federation-replay-plan.json'));
      const expected = buildReplayPlan(state, { sourceSha256: sourceHash, stateSha256: await hashFile(join(directory, 'do-state.json')) });
      required(JSON.stringify(replay) === JSON.stringify(expected), 'Federation replay plan differs from the final original queue snapshot');
      required(proof.native_federation_replay_gate_verified === true && proof.federation_replay_service_verified === true, 'Pending federation requires a verified native ordering gate and durable replay service');
    }
    if (rooms.legacy_rooms?.length || rooms.invalid_event_count || rooms.invalid_native_history?.length) required(proof.legacy_history_accessible === true, 'Legacy events require a verified accessible archive; native SQL import cannot preserve invalid reference IDs');
    if (mode === 'retire') for (const key of ['vps_post_cutover_checks_passed', 'offsite_backup_restore_passed', 'cloudflare_build_trigger_disabled', 'cloudflare_routes_detached', 'rollback_retention_completed']) required(proof[key] === true, `Cloudflare retirement blocked: ${key}`);
  }
  return { mode, verified: true, source_counts: sourceCounts, do_counts: counts, kv_counts: kvCounts, r2_objects: media.objects.length,
    pending_federation_pdus: pendingPdus, pending_federation_edus: pendingEdus, rejected_federation_pdus: rejectedPdus, unfinished_workflows: unfinished.length,
    consistency: mode === 'backup' ? 'Export integrity only; final freeze/import/recovery proof still required' : 'Operational prerequisites recorded and artifact integrity verified' };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const option = name => process.argv.find(arg => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  verifyMigration({ directory: option('directory') ?? '.local/vps-migration', mode: option('mode') ?? 'backup', userManifest: option('user-manifest'), roomManifest: option('room-manifest'), proofFile: option('proof') })
    .then(result => console.log(JSON.stringify(result))).catch(error => { console.error(error.message); process.exitCode = 1; });
}
