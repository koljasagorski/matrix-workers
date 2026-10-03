// Operator-only, read-only Cloudflare export. Credential values never reach logs.
import { readFile, writeFile, mkdir, chmod } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { homedir } from 'node:os';
import { resolve, join } from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { pathToFileURL } from 'node:url';

const WORKER = 'matrix-workers';
const ACCOUNT = '9a562df9759b0d525872d399986f10ef';
const CLASSES = { ROOMS: 'RoomDurableObject', SYNC: 'SyncDurableObject', FEDERATION: 'FederationDurableObject', CALL_ROOMS: 'CallRoomDurableObject', ADMIN: 'AdminDurableObject', USER_KEYS: 'UserKeysDurableObject', PUSH: 'PushDurableObject', RATE_LIMIT: 'RateLimitDurableObject' };
const KV = ['SESSIONS', 'DEVICE_KEYS', 'CACHE', 'CROSS_SIGNING_KEYS', 'ACCOUNT_DATA', 'ONE_TIME_KEYS'];
const sha = value => createHash('sha256').update(value).digest('hex');

async function credential() {
  if (process.env.CLOUDFLARE_API_TOKEN) return process.env.CLOUDFLARE_API_TOKEN;
  const candidates = [process.env.WRANGLER_AUTH_FILE, join(homedir(), '.wrangler/config/default.toml'), join(homedir(), 'Library/Preferences/.wrangler/config/default.toml'), join(process.env.XDG_CONFIG_HOME ?? join(homedir(), '.config'), '.wrangler/config/default.toml')].filter(Boolean);
  for (const file of candidates) {
    let text;
    try { text = await readFile(file, 'utf8'); } catch { continue; }
    const token = text.match(/^oauth_token\s*=\s*"([^"\r\n]+)"/m)?.[1];
    if (token) return token;
  }
  throw new Error('Cloudflare authentication unavailable; run wrangler whoami to refresh local OAuth first');
}

export async function exportCloudflare({ directory, inventoryOnly = false, fetchImpl = fetch, token } = {}) {
  if (!directory) throw new Error('Export directory is required');
  token ??= await credential();
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
  const artifacts = [];
  const save = async (file, value) => {
    const bytes = Buffer.from(JSON.stringify(value, null, 2) + '\n');
    await writeFile(join(directory, file), bytes, { mode: 0o600 });
    await chmod(join(directory, file), 0o600);
    artifacts.push({ path: file, bytes: bytes.length, sha256: sha(bytes) });
  };
  const request = async path => {
    const response = await fetchImpl(`https://api.cloudflare.com/client/v4/accounts/${ACCOUNT}${path}`, { headers: { Authorization: `Bearer ${token}` }, redirect: 'error', signal: AbortSignal.timeout(90000) });
    if (!response.ok) throw new Error(`Cloudflare export HTTP ${response.status} for ${path.split('?')[0]}`);
    return response;
  };
  const api = async path => {
    const body = await (await request(path)).json();
    if (body.success !== true) throw new Error(`Cloudflare export API failed for ${path.split('?')[0]}`);
    return body;
  };
  const pageAll = async (path, query = {}, mode = 'cursor') => {
    const all = []; const cursors = new Set(); let cursor; let page = 1;
    for (;;) {
      const params = new URLSearchParams({ ...query, ...(cursor ? { cursor } : {}), ...(mode === 'page' ? { page: String(page) } : {}) });
      const body = await api(`${path}?${params}`);
      if (!Array.isArray(body.result)) throw new Error(`Unexpected inventory shape for ${path}`);
      all.push(...body.result);
      if (mode === 'page') {
        if (body.result.length === 0 || (body.result_info?.total_pages && page >= body.result_info.total_pages)) break;
        page++; continue;
      }
      const next = body.result_info?.cursor ?? body.result_info?.cursors?.after;
      if (!body.result.length || !next) break;
      if (cursors.has(next)) throw new Error(`Repeated export cursor for ${path}`);
      cursors.add(next); cursor = next;
    }
    return all;
  };

  const settings = (await api(`/workers/scripts/${WORKER}/settings`)).result;
  const namespaces = await pageAll('/workers/durable_objects/namespaces', { per_page: '1000' }, 'page');
  const approved = {}; const doInventory = {};
  for (const [binding, className] of Object.entries(CLASSES)) {
    const configured = settings.bindings.find(b => b.name === binding && b.type === 'durable_object_namespace');
    const namespace = namespaces.find(n => n.id === configured?.namespace_id && n.script === WORKER && n.class === className);
    if (!namespace || !namespace.use_sqlite) throw new Error(`Unexpected durable object ownership for ${binding}`);
    const objects = await pageAll(`/workers/durable_objects/namespaces/${namespace.id}/objects`, { limit: '1000' });
    const ids = objects.map(o => o.id);
    if (new Set(ids).size !== ids.length || ids.some(id => !/^[a-f0-9]{64}$/.test(id))) throw new Error(`Invalid object inventory for ${binding}`);
    approved[binding] = ids;
    doInventory[binding] = { namespace_id: namespace.id, class_name: className, objects, complete: true };
  }
  await save('approved-do-objects.json', approved);
  await save('do-inventory.json', { captured_at: new Date().toISOString(), worker: WORKER, namespaces: doInventory });
  if (inventoryOnly) return { counts: Object.fromEntries(Object.entries(approved).map(([k, v]) => [k, v.length])) };

  await mkdir(join(directory, 'kv'), { recursive: true, mode: 0o700 });
  const kvCounts = {};
  for (const binding of KV) {
    const namespace = settings.bindings.find(b => b.name === binding && b.type === 'kv_namespace');
    if (!namespace) throw new Error(`Missing KV binding ${binding}`);
    const keys = await pageAll(`/storage/kv/namespaces/${namespace.namespace_id}/keys`, { limit: '1000' });
    const rows = [];
    for (const metadata of keys) {
      const response = await request(`/storage/kv/namespaces/${namespace.namespace_id}/values/${encodeURIComponent(metadata.name)}`);
      rows.push({ ...metadata, value_base64: Buffer.from(await response.arrayBuffer()).toString('base64') });
    }
    await save(`kv/${binding}.json`, { namespace_id: namespace.namespace_id, complete: true, keys: rows });
    kvCounts[binding] = rows.length;
    if (binding === 'SESSIONS') {
      const tokens = rows.filter(r => r.name.startsWith('refresh:')).map(r => {
        const value = JSON.parse(Buffer.from(r.value_base64, 'base64').toString('utf8'));
        if (!/^refresh:[A-Za-z0-9_-]{43}$/.test(r.name) || typeof value.userId !== 'string' || typeof value.deviceId !== 'string' || typeof value.accessTokenId !== 'string' || !Number.isSafeInteger(r.expiration)) throw new Error('Invalid refresh-token backup metadata');
        return { token_hash: r.name.slice(8), user_id: value.userId, device_id: value.deviceId, access_token_id: value.accessTokenId, expires_at_ms: r.expiration * 1000 };
      });
      await save('refresh-tokens.json', { tokens });
    }
    if (binding === 'CACHE') {
      const filters = rows.filter(r => r.name.startsWith('filter:')).map(r => {
        const separator = r.name.lastIndexOf(':');
        const user_id = r.name.slice(7, separator);
        const filter_id = r.name.slice(separator + 1);
        const filter = JSON.parse(Buffer.from(r.value_base64, 'base64').toString('utf8'));
        if (!user_id.startsWith('@') || !user_id.includes(':') || !filter_id || !filter || typeof filter !== 'object' || Array.isArray(filter)) throw new Error('Invalid saved client filter');
        return { user_id, filter_id, filter };
      });
      await save('client-filters.json', { filters });
    }
  }
  const media = settings.bindings.find(b => b.name === 'MEDIA' && b.type === 'r2_bucket');
  if (media?.bucket_name !== 'matrix-workers-media') throw new Error('Unexpected media bucket ownership');
  const objects = await pageAll(`/r2/buckets/${media.bucket_name}/objects`, { per_page: '1000' });
  await mkdir(join(directory, 'r2'), { recursive: true, mode: 0o700 });
  const mediaRows = [];
  for (const metadata of objects) {
    if (typeof metadata.key !== 'string' || metadata.key.split('/').some(part => part === '.' || part === '..')) throw new Error('Media key contains URL-normalized dot segments; export that key through a raw S3 client before completion');
    const file = `r2/${sha(metadata.key)}.bin`;
    const response = await request(`/r2/buckets/${media.bucket_name}/objects/${metadata.key.split('/').map(encodeURIComponent).join('/')}`);
    const hash = createHash('sha256'); let bytes = 0;
    const etag = response.headers.get('etag')?.replaceAll('"', '');
    if (etag && etag !== metadata.etag.replaceAll('"', '')) throw new Error('Media changed during export; repeat under maintenance');
    await pipeline(Readable.fromWeb(response.body), new Transform({ transform(chunk, _encoding, done) { bytes += chunk.length; hash.update(chunk); done(null, chunk); } }), createWriteStream(join(directory, file), { mode: 0o600 }));
    if (bytes !== metadata.size) throw new Error('Media size mismatch during backup');
    const artifact = { path: file, bytes, sha256: hash.digest('hex') };
    artifacts.push(artifact); mediaRows.push({ ...metadata, ...artifact });
  }
  await save('r2-inventory.json', { bucket: media.bucket_name, complete: true, objects: mediaRows });
  const workflows = (await pageAll('/workflows', { per_page: '100' }, 'page')).filter(w => w.script_name === WORKER);
  for (const workflow of workflows) workflow.instance_inventory = await pageAll(`/workflows/${encodeURIComponent(workflow.name)}/instances`, { per_page: '100' });
  await save('workflow-inventory.json', { workflows });
  const secrets = (await api(`/workers/scripts/${WORKER}/secrets`)).result.map(s => ({ name: s.name, type: s.type }));
  await save('resource-inventory.json', { captured_at: new Date().toISOString(), worker: WORKER, account_id: ACCOUNT,
    bindings: settings.bindings.filter(b => ['durable_object_namespace', 'kv_namespace', 'r2_bucket', 'd1', 'workflow'].includes(b.type)), secrets, do_counts: Object.fromEntries(Object.entries(approved).map(([k, v]) => [k, v.length])), kv_counts: kvCounts, r2_objects: mediaRows.length });
  await save('cloudflare-export.json', { captured_at: new Date().toISOString(), complete: true, consistency: 'requires independently verified maintenance freeze', artifacts });
  return { do_counts: Object.fromEntries(Object.entries(approved).map(([k, v]) => [k, v.length])), kv_counts: kvCounts, r2_objects: mediaRows.length };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const directory = resolve(process.argv.find(arg => arg.startsWith('--directory='))?.slice(12) ?? '.local/vps-migration');
  exportCloudflare({ directory, inventoryOnly: process.argv.includes('--inventory-only') }).then(result => console.log(JSON.stringify(result))).catch(error => { console.error(error.message); process.exitCode = 1; });
}
