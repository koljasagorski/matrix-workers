import { Hono } from 'hono';
import { afterEach, beforeEach, expect, it } from 'vitest';
import { requireAuth } from '../src/middleware/auth';
import login from '../src/api/login';
import { testEnv } from './federation-helpers';
import type { AppEnv } from '../src/types';

let fixture: Awaited<ReturnType<typeof testEnv>>;
const app = new Hono<AppEnv>();
app.get('/protected', requireAuth(), c => c.json({ user_id: c.get('userId'), device_id: c.get('deviceId') }));

beforeEach(async () => {
  fixture = await testEnv();
  fixture.sqlite.prepare(`INSERT INTO appservice_registrations(id,url,as_token,hs_token,sender_localpart,namespaces)
    VALUES(?,?,?,?,?,?)`).run('test-bridge', 'https://bridge.example', 'synthetic-bridge-token', 'synthetic-hs-token', 'bridgebot',
      JSON.stringify({ users: [{ exclusive: true, regex: '^@_bridge_.*' }], rooms: [], aliases: [] }));
});
afterEach(() => fixture.sqlite.close());

function request(userId?: string, token = 'synthetic-bridge-token') {
  const url = new URL('https://local.example/protected');
  if (userId !== undefined) url.searchParams.set('user_id', userId);
  return app.request(url, { headers: { Authorization: `Bearer ${token}` } }, fixture.env);
}

it.each(['@alice:local.example', '@_bridge_alice:remote.example', '@_bridge_without_domain', '@_bridge_alice:local.example.evil'])
('rejects application-service identity assertion for unauthorized user %s', async userId => {
  const response = await request(userId);
  expect(response.status).toBe(403);
  expect(await response.json()).toMatchObject({ errcode: 'M_FORBIDDEN' });
});

it.each(['_bridge_reserved', 'bridgebot'])
('does not expose a reserved application-service username as available to public registrants (%s)', async username => {
  const env = { ...fixture.env, ADMIN: {
    idFromName: () => 'global', get: () => ({ fetch: async () => Response.json({ registration_enabled: true }) }),
  } };
  const response = await login.request(`/_matrix/client/v3/register/available?username=${username}`, undefined, env);
  expect(response.status).toBe(400);
  expect(await response.json()).toMatchObject({ errcode: 'M_EXCLUSIVE' });
});

it('keeps non-exclusive namespaces available for public registrations', async () => {
  fixture.sqlite.prepare('UPDATE appservice_registrations SET namespaces = ?').run(JSON.stringify({
    users: [{ exclusive: false, regex: '^@_bridge_.*' }], rooms: [], aliases: [],
  }));
  const env = { ...fixture.env, ADMIN: {
    idFromName: () => 'global', get: () => ({ fetch: async () => Response.json({ registration_enabled: true }) }),
  } };
  const available = await login.request('/_matrix/client/v3/register/available?username=_bridge_normal', undefined, env);
  expect(available.status).toBe(200);
  const response = await login.request('/_matrix/client/v3/register', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: '_bridge_normal', password: 'synthetic-password', auth: { type: 'm.login.dummy' } }) }, env);
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({ user_id: '@_bridge_normal:local.example' });
});

it('does not create a guest identity in a globally reserved user namespace', async () => {
  fixture.sqlite.prepare('UPDATE appservice_registrations SET namespaces = ?').run(JSON.stringify({
    users: [{ exclusive: true, regex: '^@.*:local[.]example$' }], rooms: [], aliases: [],
  }));
  const env = { ...fixture.env, ADMIN: {
    idFromName: () => 'global', get: () => ({ fetch: async () => Response.json({ registration_enabled: true }) }),
  } };
  const response = await login.request('/_matrix/client/v3/register?kind=guest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }, env);
  expect(response.status).toBe(400);
  expect(await response.json()).toMatchObject({ errcode: 'M_EXCLUSIVE' });
  expect(fixture.sqlite.prepare('SELECT user_id FROM users WHERE is_guest = 1').all()).toEqual([]);
});

it.each([undefined, { type: 'm.login.dummy' }])
('prevents public registration in an exclusive application-service namespace before and after UIA (%s)', async auth => {
  const env = { ...fixture.env, ADMIN: {
    idFromName: () => 'global', get: () => ({ fetch: async () => Response.json({ registration_enabled: true }) }),
  } };
  const response = await login.request('/_matrix/client/v3/register', { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: '_bridge_reserved', password: 'synthetic-password', auth }) }, env);
  expect(response.status).toBe(400);
  expect(await response.json()).toMatchObject({ errcode: 'M_EXCLUSIVE' });
  expect(fixture.sqlite.prepare('SELECT user_id FROM users WHERE localpart = ?').get('_bridge_reserved')).toBeUndefined();
});

it.each([undefined, '@bridgebot:local.example', '@_bridge_bob:local.example'])
('permits the application-service sender and covered local identities (%s)', async userId => {
  const response = await request(userId);
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ user_id: userId ?? '@bridgebot:local.example', device_id: null });
});

it('keeps normal access-token identity independent of application-service query parameters', async () => {
  const response = await request('@_bridge_bob:local.example', 'token');
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ user_id: '@alice:local.example', device_id: 'DEVICE' });
});
