import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import { readdirSync, readFileSync } from 'node:fs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import login from '../src/api/login';
import { hashPassword, hashToken } from '../src/utils/crypto';

const userId = '@alice:example.test';
const password = 'synthetic-session-test-password';
const hour = 60 * 60 * 1000;
let db: DatabaseSync;
let now: number;
let bindings: ReturnType<typeof createBindings>;

function memoryKV() {
  const values = new Map<string, { value: string; expiresAt: number }>();
  return {
    values,
    get: async (key: string, format?: string) => {
      const record = values.get(key);
      if (!record || record.expiresAt <= Date.now()) return null;
      return format === 'json' ? JSON.parse(record.value) : record.value;
    },
    put: async (key: string, value: string, options?: { expirationTtl: number }) => {
      values.set(key, {
        value,
        expiresAt: options ? Date.now() + options.expirationTtl * 1000 : Infinity,
      });
    },
    delete: async (key: string) => { values.delete(key); },
  };
}

function createBindings() {
  const prepare = (sql: string, args: SQLInputValue[] = []) => ({
    bind: (...bound: SQLInputValue[]) => prepare(sql, bound),
    first: async () => db.prepare(sql).get(...args) ?? null,
    run: async () => ({ success: true, meta: db.prepare(sql).run(...args) }),
  });
  return {
    SERVER_NAME: 'example.test',
    DB: { prepare },
    SESSIONS: memoryKV(),
    ADMIN: {
      idFromName: () => 'global',
      get: () => ({ fetch: async () => Response.json({ registration_enabled: true }) }),
    },
  };
}

beforeEach(async () => {
  now = Date.now();
  vi.spyOn(Date, 'now').mockImplementation(() => now);
  db = new DatabaseSync(':memory:');
  for (const file of readdirSync('migrations').filter(f => f.endsWith('.sql')).sort()) {
    db.exec(readFileSync(`migrations/${file}`, 'utf8'));
  }
  db.prepare('INSERT INTO users (user_id, localpart, password_hash) VALUES (?, ?, ?)')
    .run(userId, 'alice', await hashPassword(password));
  bindings = createBindings();
});

afterEach(() => {
  db.close();
  vi.restoreAllMocks();
});

function post(path: string, body: unknown, accessToken?: string) {
  return login.request(path, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
    },
    body: JSON.stringify(body),
  }, bindings);
}

function passwordLogin(refreshToken?: boolean, deviceId = 'DESKTOP') {
  return post('/_matrix/client/v3/login', {
    type: 'm.login.password', identifier: { type: 'm.id.user', user: 'alice' },
    password, device_id: deviceId, refresh_token: refreshToken,
  });
}

function whoami(accessToken: string) {
  return login.request('/_matrix/client/v3/account/whoami', {
    headers: { Authorization: `Bearer ${accessToken}` },
  }, bindings);
}

describe('Matrix session refresh negotiation', () => {
  it.each([undefined, false])('keeps password sessions valid until logout without refresh support (%s)', async (refreshToken) => {
    const response = await passwordLogin(refreshToken);
    expect(response.status).toBe(200);
    const session = await response.json();
    expect(session).not.toHaveProperty('refresh_token');
    expect(session).not.toHaveProperty('expires_in_ms');
    expect(db.prepare('SELECT expires_at FROM access_tokens').get()).toMatchObject({ expires_at: null });
    expect(bindings.SESSIONS.values.size).toBe(0);

    // Reopening the app after sleep, or after the old refresh TTL, must work.
    now += 8 * 24 * hour;
    const restored = await whoami(session.access_token);
    expect(restored.status).toBe(200);
    expect(await restored.json()).toMatchObject({ user_id: userId, device_id: 'DESKTOP' });
    expect((await post('/_matrix/client/v3/logout', {}, session.access_token)).status).toBe(200);
    expect((await whoami(session.access_token)).status).toBe(401);
  });

  it('expires and refreshes sessions that explicitly request refresh support', async () => {
    const response = await passwordLogin(true);
    expect(response.status).toBe(200);
    const session = await response.json();
    expect(session.expires_in_ms).toBe(hour);
    expect(session.refresh_token).toEqual(expect.any(String));
    expect(db.prepare('SELECT expires_at FROM access_tokens').get()).toMatchObject({ expires_at: now + hour });

    now += hour;
    expect((await whoami(session.access_token)).status).toBe(401);
    const refreshed = await post('/_matrix/client/v3/refresh', { refresh_token: session.refresh_token });
    expect(refreshed.status).toBe(200);
    const replacement = await refreshed.json();
    expect(replacement.expires_in_ms).toBe(hour);
    expect(replacement.access_token).not.toBe(session.access_token);
    expect(replacement.refresh_token).not.toBe(session.refresh_token);
    expect((await whoami(replacement.access_token)).status).toBe(200);
    expect((await post('/_matrix/client/v3/refresh', { refresh_token: session.refresh_token })).status).toBe(401);
    expect((await post('/_matrix/client/v3/logout', {}, replacement.access_token)).status).toBe(200);
    expect((await post('/_matrix/client/v3/refresh', { refresh_token: replacement.refresh_token })).status).toBe(401);
  });

  it('keeps one-time-token logins valid without refresh support', async () => {
    const token = 'synthetic-one-time-login-token';
    await bindings.SESSIONS.put(`login_token:${await hashToken(token)}`, JSON.stringify({
      user_id: userId, expires_at: now + 120000,
    }), { expirationTtl: 120 });
    const response = await post('/_matrix/client/v3/login', { type: 'm.login.token', token });
    expect(response.status).toBe(200);
    const session = await response.json();
    expect(session).not.toHaveProperty('refresh_token');
    expect(session).not.toHaveProperty('expires_in_ms');
    now += 8 * 24 * hour;
    expect((await whoami(session.access_token)).status).toBe(200);
    expect((await post('/_matrix/client/v3/login', { type: 'm.login.token', token })).status).toBe(403);
  });

  it.each([undefined, false, true])('honors refresh support when registering (%s)', async (refreshToken) => {
    const response = await post('/_matrix/client/v3/register', {
      username: 'bob', password, auth: { type: 'm.login.dummy' }, refresh_token: refreshToken,
    });
    expect(response.status).toBe(200);
    const session = await response.json();
    if (refreshToken === true) {
      expect(session.expires_in_ms).toBe(hour);
      expect(session.refresh_token).toEqual(expect.any(String));
    } else {
      expect(session).not.toHaveProperty('expires_in_ms');
      expect(session).not.toHaveProperty('refresh_token');
    }
    now += hour;
    expect((await whoami(session.access_token)).status).toBe(refreshToken === true ? 401 : 200);
  });

  it('creates no session when registration inhibits login', async () => {
    const response = await post('/_matrix/client/v3/register', {
      username: 'bob', password, auth: { type: 'm.login.dummy' },
      refresh_token: true, inhibit_login: true,
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ user_id: '@bob:example.test', home_server: 'example.test' });
    expect(db.prepare('SELECT COUNT(*) AS count FROM access_tokens').get()).toMatchObject({ count: 0 });
    expect(bindings.SESSIONS.values.size).toBe(0);
  });

  it('rejects non-expiring access tokens when the account is deactivated', async () => {
    const response = await passwordLogin();
    expect(response.status).toBe(200);
    const session = await response.json();
    db.prepare('UPDATE users SET is_deactivated = 1 WHERE user_id = ?').run(userId);
    expect((await whoami(session.access_token)).status).toBe(401);
  });
});
