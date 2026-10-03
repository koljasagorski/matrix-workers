import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import oauth from '../src/api/oauth';
import { hashPassword } from '../src/utils/crypto';

function memoryKV() {
  const values = new Map<string, string>();
  return {
    get: async (key: string) => values.get(key) ?? null,
    put: async (key: string, value: string) => { values.set(key, value); },
    delete: async (key: string) => { values.delete(key); },
  };
}

const userId = '@alice:example.org';
const password = ' correct password with spaces ';
const redirectUri = 'io.element:/callback';
let db: DatabaseSync;
let bindings: ReturnType<typeof createBindings>;

function createBindings() {
  return {
    SERVER_NAME: 'example.org',
    DB: {
      prepare: (sql: string) => ({
        bind: (...args: string[]) => ({ first: async () => db.prepare(sql).get(...args) ?? null }),
      }),
    },
    CACHE: memoryKV(),
    SESSIONS: memoryKV(),
  };
}

beforeEach(async () => {
  db = new DatabaseSync(':memory:');
  db.exec(readFileSync('migrations/001_initial_schema.sql', 'utf8'));
  db.prepare('INSERT INTO users (user_id, localpart, password_hash, admin) VALUES (?, ?, ?, 1)')
    .run(userId, 'alice', await hashPassword(password));
  bindings = createBindings();
  await bindings.CACHE.put('oauth_client:element', JSON.stringify({
    client_id: 'element', client_name: 'Element X', redirect_uris: [redirectUri],
  }));
});
afterEach(() => db.close());

function requestIdFromPage(html: string) {
  const id = html.match(/name="auth_request_id" value="([^"]+)"/)?.[1];
  expect(id).toBeTruthy();
  return id!;
}

async function startLogin() {
  const query = new URLSearchParams({
    client_id: 'element', redirect_uri: redirectUri, response_type: 'code', state: 'client-state',
    code_challenge: 'pkce-challenge', code_challenge_method: 'S256',
  });
  const response = await oauth.request(`/oauth/authorize?${query}`, undefined, bindings);
  expect(response.status).toBe(200);
  return requestIdFromPage(await response.text());
}

function submitLogin(authRequestId: string, username: string, suppliedPassword = password) {
  return oauth.request('/oauth/authorize', {
    method: 'POST',
    body: new URLSearchParams({ username, password: suppliedPassword, auth_request_id: authRequestId }),
  }, bindings);
}

describe('OAuth browser login', () => {
  it.each(['alice', userId, ' alice ', ` ${userId} `])('accepts the local account as %s', async (username) => {
    const response = await submitLogin(await startLogin(), username);
    expect(response.status).toBe(302);
    const callback = new URL(response.headers.get('Location')!);
    expect(callback.origin + callback.pathname).toBe(new URL(redirectUri).origin + new URL(redirectUri).pathname);
    expect(callback.searchParams.get('state')).toBe('client-state');
    const code = callback.searchParams.get('code');
    expect(code).toBeTruthy();
    expect(JSON.parse((await bindings.SESSIONS.get(`oauth_code:${code}`))!)).toMatchObject({
      user_id: userId, client_id: 'element', redirect_uri: redirectUri,
      code_challenge: 'pkce-challenge', code_challenge_method: 'S256',
    });
  });

  it.each(['admin', '@alice:other.example', '@alice', ''])('rejects unknown or incomplete usernames: %s', async (username) => {
    const response = await submitLogin(await startLogin(), username);
    expect(response.status).toBe(200);
    expect(response.headers.has('Location')).toBe(false);
    expect(await response.text()).toMatch(/Invalid username or password|Missing username or password/);
  });

  it('rejects a wrong password and permits a subsequent retry with a full Matrix ID', async () => {
    const response = await submitLogin(await startLogin(), userId, 'wrong-password');
    expect(response.status).toBe(200);
    expect(response.headers.has('Location')).toBe(false);
    const html = await response.text();
    expect(html).toContain('Invalid username or password');
    const retry = await submitLogin(requestIdFromPage(html), userId);
    expect(retry.status).toBe(302);
  });

  it('rejects deactivated accounts', async () => {
    db.exec('UPDATE users SET is_deactivated = 1');
    const response = await submitLogin(await startLogin(), userId);
    expect(response.status).toBe(200);
    expect(response.headers.has('Location')).toBe(false);
    expect(await response.text()).toContain('Invalid username or password');
  });
});
