import { readFile, writeFile, mkdir, chmod, unlink } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { decodeStoredValue } from './stored-values.mjs';

const digest = bytes => createHash('sha256').update(bytes).digest('hex');
export async function exportDurableObjects({ directory, credentials, sessionFile, prepareOnly = false, fetchImpl = fetch } = {}) {
  if (!directory || (!credentials && !sessionFile)) throw new Error('Private credentials or an existing export session are required');
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
  const save = async (file, value) => {
    const bytes = Buffer.from(JSON.stringify(value, null, 2) + '\n');
    await writeFile(join(directory, file), bytes, { mode: 0o600 });
    await chmod(join(directory, file), 0o600);
    return { path: file, bytes: bytes.length, sha256: digest(bytes) };
  };
  let session;
  if (sessionFile) session = JSON.parse(await readFile(sessionFile, 'utf8'));
  else {
    const secret = JSON.parse(await readFile(credentials, 'utf8'));
    if (typeof secret.userId !== 'string' || typeof secret.password !== 'string' || (secret.homeserver !== undefined && typeof secret.homeserver !== 'string')) throw new Error('Invalid private credentials file');
    const base = new URL(secret.homeserver ?? 'https://m.sgr.ski');
    if (base.protocol !== 'https:' || base.hostname !== 'm.sgr.ski' || base.pathname !== '/' || base.username || base.password) throw new Error('Backup login must target https://m.sgr.ski');
    const deviceId = `MIGRATION_BACKUP_${randomUUID().replaceAll('-', '')}`;
    const response = await fetchImpl(`${base.origin}/_matrix/client/v3/login`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(45000), headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'm.login.password', identifier: { type: 'm.id.user', user: secret.userId }, password: secret.password, device_id: deviceId, initial_device_display_name: 'Temporary migration backup' }) });
    if (!response.ok) throw new Error(`Backup login HTTP ${response.status}`);
    const login = await response.json();
    if (typeof login.access_token !== 'string' || login.user_id !== secret.userId || login.device_id !== deviceId) throw new Error('Unexpected backup login identity');
    session = { base: base.origin, userId: secret.userId, password: secret.password, accessToken: login.access_token, deviceId };
  }
  if (session.base !== 'https://m.sgr.ski' || typeof session.accessToken !== 'string' || typeof session.password !== 'string' || typeof session.deviceId !== 'string' || !session.deviceId.startsWith('MIGRATION_BACKUP_')) throw new Error('Invalid private backup session');
  let completed = false;
  let preserveSession = false;
  try {
    if (!sessionFile) await save('backup-session.json', session);
    if (prepareOnly) { preserveSession = true; return { session_prepared: true }; }
    const auth = { type: 'm.login.password', identifier: { type: 'm.id.user', user: session.userId }, password: session.password };
    const exportRequest = async body => {
      const response = await fetchImpl(`${session.base}/admin/api/migration/export`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(45000),
        headers: { Authorization: `Bearer ${session.accessToken}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, auth }) });
      if (!response.ok) throw new Error(`Durable Object export HTTP ${response.status} (${body.action ?? body.namespace ?? 'unknown'})`);
      return response.json();
    };
    const catalogue = await exportRequest({ action: 'catalogue' });
    if (catalogue.server_name !== 'm.sgr.ski' || !Array.isArray(catalogue.users) || !catalogue.namespaces) throw new Error('Invalid export catalogue');
    const expected = JSON.parse(await readFile(join(directory, 'approved-do-objects.json'), 'utf8'));
    for (const [binding, ids] of Object.entries(expected)) {
      if (JSON.stringify([...ids].sort()) !== JSON.stringify([...(catalogue.namespaces[binding] ?? [])].sort())) throw new Error('Deployed approval list does not match the complete Cloudflare inventory');
    }
    const objects = {};
    for (const [binding, ids] of Object.entries(expected)) {
      objects[binding] = {};
      for (const objectId of ids) {
        const entries = new Map(); const cursors = new Set(); let cursor; let alarm;
        for (;;) {
          const page = await exportRequest({ namespace: binding, object_id: objectId, cursor });
          if (page.object_id !== objectId || page.format !== 'matrix-workers-do-v2' || page.encoding !== 'structured-clone-v1' || !Array.isArray(page.entries) || page.entries.length > 16) throw new Error('Invalid Durable Object export page');
          for (const row of page.entries) {
            if (!Array.isArray(row) || row.length !== 2 || typeof row[0] !== 'string' || entries.has(row[0]) || (cursor !== undefined && row[0] <= cursor)) throw new Error('Duplicate or unordered Durable Object backup key');
            entries.set(row[0], row[1]);
          }
          alarm = page.alarm;
          if (page.next_cursor === null) break;
          if (typeof page.next_cursor !== 'string' || page.next_cursor !== page.entries.at(-1)?.[0] || cursors.has(page.next_cursor)) throw new Error('Invalid Durable Object continuation cursor');
          cursors.add(page.next_cursor); cursor = page.next_cursor;
        }
        objects[binding][objectId] = { entries: [...entries], encoding: 'structured-clone-v1', alarm, complete: true };
      }
    }
    const artifacts = [await save('do-state.json', { captured_at: new Date().toISOString(), complete: true, namespaces: objects })];
    const users = {};
    const names = new Map(catalogue.users.map(u => [u.object_id, u.user_id]));
    for (const [objectId, object] of Object.entries(objects.USER_KEYS)) {
      const userId = names.get(objectId);
      if (!userId && object.entries.length) throw new Error('An orphan UserKeys object has data: raw archive saved, native identity reconciliation required');
      if (!userId) continue;
      const account_data = {}; const device_keys = {}; let cross_signing = {}; let signatures = [];
      for (const [key, encoded] of object.entries) {
        const value = decodeStoredValue(encoded);
        if (key.startsWith('account_data:')) account_data[key.slice(13)] = value;
        else if (key.startsWith('device_keys:')) device_keys[key.slice(12)] = value;
        else if (key === 'cross_signing_keys') cross_signing = value;
        else if (key === 'signatures') signatures = value;
      }
      users[userId] = { account_data, device_keys, cross_signing, signatures };
    }
    artifacts.push(await save('user-keys.json', { users }));
    artifacts.push(await save('do-catalogue.json', catalogue));
    await save('durable-export.json', { captured_at: new Date().toISOString(), complete: true, artifacts,
      counts: Object.fromEntries(Object.entries(objects).map(([key, value]) => [key, Object.keys(value).length])) });
    completed = true;
    return { exported: true, users: Object.keys(users).length, object_count: Object.values(objects).reduce((n, value) => n + Object.keys(value).length, 0) };
  } finally {
    // A session explicitly reused during the final freeze remains available until
    // the operator reconciles its device after taking the consistent snapshot.
    if (!sessionFile && !preserveSession) {
      const response = await fetchImpl(`${session.base}/_matrix/client/v3/devices/${encodeURIComponent(session.deviceId)}`, { method: 'DELETE', redirect: 'error', signal: AbortSignal.timeout(45000),
        headers: { Authorization: `Bearer ${session.accessToken}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ auth: { type: 'm.login.password', password: session.password } }) });
      if (!response.ok) throw new Error(`Temporary backup device cleanup HTTP ${response.status}; export ${completed ? 'completed' : 'failed'}, private session retained`);
      await unlink(join(directory, 'backup-session.json'));
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const option = name => process.argv.find(arg => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  exportDurableObjects({ directory: resolve(option('directory') ?? '.local/vps-migration'), credentials: option('credentials'), sessionFile: option('session'), prepareOnly: process.argv.includes('--prepare-session') })
    .then(value => console.log(JSON.stringify(value))).catch(error => { console.error(error.message); process.exitCode = 1; });
}
