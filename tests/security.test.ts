// Regression tests for locally reproduced vulnerabilities; synthetic data only.
import { DatabaseSync } from 'node:sqlite';
import { readFileSync, readdirSync } from 'node:fs';
import { beforeEach, afterEach, expect, it, vi } from 'vitest';
import oauth from '../src/api/oauth';
import keys from '../src/api/keys';
import federation from '../src/api/federation';
import media from '../src/api/media';
import { hashPassword, hashToken } from '../src/utils/crypto';

function kv() {
  const data = new Map<string, string>();
  return {
    get: async (key: string, format?: string) => {
      const value = data.get(key) ?? null;
      return value && format === 'json' ? JSON.parse(value) : value;
    },
    put: async (key: string, value: string) => { data.set(key, value); },
    delete: async (key: string) => { data.delete(key); },
  };
}
const userId = '@audit:example.test';
const accessToken = 'synthetic-audit-access-token';
let db: DatabaseSync;
let env: any;
let logs: ReturnType<typeof vi.spyOn>;
let storedKeys: unknown;

beforeEach(async () => {
  logs = vi.spyOn(console, 'log').mockImplementation(() => {});
  db = new DatabaseSync(':memory:');
  for (const file of readdirSync('migrations').filter(f => f.endsWith('.sql')).sort()) {
    db.exec(readFileSync(`migrations/${file}`, 'utf8'));
  }
  db.prepare('INSERT INTO users (user_id, localpart, password_hash) VALUES (?, ?, ?)')
    .run(userId, 'audit', await hashPassword('synthetic-real-password'));
  db.prepare('INSERT INTO access_tokens (token_id, token_hash, user_id, device_id) VALUES (?, ?, ?, ?)')
    .run('current-session', await hashToken(accessToken), userId, 'AUDIT');
  const prepare = (sql: string, args: any[] = []) => ({
    bind: (...bound: any[]) => prepare(sql, bound),
    first: async () => db.prepare(sql).get(...args) ?? null,
    all: async () => ({ results: db.prepare(sql).all(...args), success: true }),
    run: async () => ({ success: true, meta: db.prepare(sql).run(...args) }),
  });
  storedKeys = undefined;
  env = {
    SERVER_NAME: 'example.test', DB: { prepare }, CACHE: kv(), SESSIONS: kv(),
    ACCOUNT_DATA: kv(), CROSS_SIGNING_KEYS: kv(),
    USER_KEYS: { idFromName: (s: string) => s, get: () => ({ fetch: async (r: Request) => {
      if (r.method === 'POST') storedKeys = await r.json();
      return Response.json({});
    } }) },
  };
});
afterEach(() => { db.close(); logs.mockRestore(); });

it('blocks unauthenticated federation v2 access to a private room state', async () => {
  db.exec(`INSERT INTO rooms(room_id,is_public) VALUES ('!audit:example.test',0);
    INSERT INTO events(event_id,room_id,sender,event_type,state_key,content,origin_server_ts,depth,auth_events,prev_events)
    VALUES ('$state','!audit:example.test','@audit:example.test','m.room.topic','','{"topic":"private audit canary"}',1,1,'[]','[]');
    INSERT INTO room_state(room_id,event_type,state_key,event_id) VALUES ('!audit:example.test','m.room.topic','','$state');`);
  const init = { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{}' };
  const v1 = await federation.request('/_matrix/federation/v1/send_join/!audit:example.test/$join', init, env);
  expect(v1.status).toBe(401);
  const v2 = await federation.request('/_matrix/federation/v2/send_join/!audit:example.test/$join', init, env);
  expect(v2.status).toBe(401);
  expect(await v2.text()).not.toContain('private audit canary');
});

it('rejects cross-signing replacement with a fabricated SSO code', async () => {
  db.prepare('INSERT INTO cross_signing_keys(user_id,key_type,key_id,key_data) VALUES (?, ?, ?, ?)')
    .run(userId, 'master', 'old-key', '{}');
  const upload = (auth?: unknown) => keys.request('/_matrix/client/v3/keys/device_signing/upload', {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
    body: JSON.stringify({ master_key: { user_id: userId, usage: ['master'], keys: { 'ed25519:audit': 'replacement' } }, auth }),
  }, env);
  const challenge = await upload();
  expect(challenge.status).toBe(401);
  const { session } = await challenge.json();
  const callback = await keys.request(`/_matrix/client/v3/auth/m.login.sso/callback?state=${session}&code=not-a-real-code`, undefined, env);
  expect(callback.status).toBe(403);
  expect(JSON.parse(await env.CACHE.get(`uia_session:${session}`)).completed_stages).not.toContain('m.login.sso');
  expect((await upload({ session, type: 'm.login.sso' })).status).toBe(401);
  expect(storedKeys).toBeUndefined();
});

it('does not log cleartext passwords on a key upload', async () => {
  db.prepare('INSERT INTO cross_signing_keys(user_id,key_type,key_id,key_data) VALUES (?, ?, ?, ?)')
    .run(userId, 'master', 'old-key', '{}');
  const canary = 'synthetic-secret-never-use-in-production';
  await keys.request('/_matrix/client/v3/keys/device_signing/upload', {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
    body: JSON.stringify({ auth: { type: 'm.login.password', password: canary } }),
  }, env);
  expect(logs.mock.calls.some(args => args.some(value => typeof value === 'string' && value.includes(canary)))).toBe(false);
});

it('rejects refresh after OAuth session revocation', async () => {
  await env.CACHE.put('oauth_client:audit-client', JSON.stringify({ client_id: 'audit-client', token_endpoint_auth_method: 'none' }));
  await env.SESSIONS.put('oauth_refresh:synthetic-refresh', JSON.stringify({
    token_id: 'current-session', access_token_hash: await hashToken(accessToken), client_id: 'audit-client', user_id: userId, device_id: 'AUDIT', scope: 'openid',
    expires_at: Date.now() + 60000,
  }));
  db.exec('DELETE FROM access_tokens');
  const response = await oauth.request('/oauth/token', {
    method: 'POST', body: new URLSearchParams({ grant_type: 'refresh_token', client_id: 'audit-client', refresh_token: 'synthetic-refresh' }),
  }, env);
  expect(response.status).toBe(400);
  expect(await response.json()).toMatchObject({ error: 'invalid_grant' });
  expect(db.prepare('SELECT * FROM access_tokens').all()).toHaveLength(0);
});

it('rejects a forged unsigned JWT during token introspection', async () => {
  const encode = (obj: unknown) => Buffer.from(JSON.stringify(obj)).toString('base64url');
  const token = `${encode({ alg: 'none' })}.${encode({ sub: userId, exp: Math.floor(Date.now() / 1000) + 600 })}.fake`;
  const response = await oauth.request('/oauth/introspect', { method: 'POST', body: new URLSearchParams({ token }) }, env);
  expect(await response.json()).toMatchObject({ active: false });
});

it('sandboxes uploaded HTML served on the admin origin', async () => {
  const html = '<!doctype html><script>window.auditCanary = true</script>';
  db.prepare('INSERT INTO media(media_id,user_id,content_type,content_length,filename) VALUES (?, ?, ?, ?, ?)')
    .run('audit-html', userId, 'text/html', html.length, 'audit.html');
  env.MEDIA = { get: async () => ({ body: new Response(html).body }) };
  const response = await media.request('/_matrix/media/v3/download/example.test/audit-html', undefined, env);
  expect(response.status).toBe(200);
  expect(response.headers.get('Content-Type')).toBe('text/html');
  expect(response.headers.get('Content-Disposition')).toContain('inline');
  expect(response.headers.get('Content-Security-Policy')).toContain('sandbox');
  expect(response.headers.get('X-Content-Type-Options')).toBe('nosniff');
  expect(await response.text()).toBe(html);
});
