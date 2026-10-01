import { beforeEach, describe, expect, it, vi } from 'vitest';
import login from '../src/api/login';
import { getUserById, getPasswordHash, createAccessToken } from '../src/services/database';
import { hashPassword } from '../src/utils/crypto';

vi.mock('../src/services/database', () => ({
  createUser: vi.fn(), getUserByLocalpart: vi.fn(), getUserById: vi.fn(),
  getPasswordHash: vi.fn(), createDevice: vi.fn(), createAccessToken: vi.fn(),
  deleteAccessToken: vi.fn(), deleteAllUserTokens: vi.fn(), getUserByTokenHash: vi.fn(),
}));

function env(registrationEnabled = false) {
  return {
    SERVER_NAME: 'm.sgr.ski',
    DB: {},
    SESSIONS: { get: vi.fn(), put: vi.fn(), delete: vi.fn() },
    ADMIN: {
      idFromName: vi.fn(() => 'global'),
      get: vi.fn(() => ({ fetch: vi.fn(async () => Response.json({ registration_enabled: registrationEnabled })) })),
    },
  };
}
function post(path: string, body: unknown, bindings = env()) {
  return login.request(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, bindings);
}

beforeEach(() => vi.resetAllMocks());
describe('login security', () => {
  it('never advertises dummy authentication as a login flow', async () => {
    const response = await login.request('/_matrix/client/v3/login');
    expect((await response.json()).flows).not.toContainEqual({ type: 'm.login.dummy' });
  });
  it('rejects passwordless dummy login before accessing any account', async () => {
    const response = await post('/_matrix/client/v3/login', { type: 'm.login.dummy', identifier: { type: 'm.id.user', user: 'admin' } });
    expect(response.status).toBe(400);
    expect(getUserById).not.toHaveBeenCalled();
    expect(createAccessToken).not.toHaveBeenCalled();
  });
  it('rejects incorrect passwords', async () => {
    vi.mocked(getPasswordHash).mockResolvedValue(await hashPassword('correct-password'));
    const response = await post('/_matrix/client/v3/login', { type: 'm.login.password', identifier: { type: 'm.id.user', user: 'admin' }, password: 'wrong-password' });
    expect(response.status).toBe(403);
    expect(createAccessToken).not.toHaveBeenCalled();
  });
  it('allows password login and persists the advertised token expiry', async () => {
    vi.mocked(getPasswordHash).mockResolvedValue(await hashPassword('correct-password'));
    vi.mocked(getUserById).mockResolvedValue({ user_id: '@admin:m.sgr.ski', is_deactivated: false } as never);
    const start = Date.now();
    const response = await post('/_matrix/client/v3/login', { type: 'm.login.password', identifier: { type: 'm.id.user', user: 'admin' }, password: 'correct-password' });
    expect(response.status).toBe(200);
    const data = await response.json();
    expect(data.expires_in_ms).toBe(3600000);
    expect(vi.mocked(createAccessToken).mock.calls[0][5]).toBeGreaterThanOrEqual(start + data.expires_in_ms);
  });
  it.each(['', '?kind=guest'])('blocks registration when closed (%s)', async (suffix) => {
    const response = await post('/_matrix/client/v3/register' + suffix, { username: 'newuser', password: 'password', auth: { type: 'm.login.dummy' } });
    expect(response.status).toBe(403);
    expect((await response.json()).error).toBe('Registration is disabled');
  });
  it('blocks username availability when registration is closed', async () => {
    expect((await login.request('/_matrix/client/v3/register/available?username=admin', undefined, env())).status).toBe(403);
  });
  it('allows the registration challenge when an administrator enables registration', async () => {
    expect((await post('/_matrix/client/v3/register', {}, env(true))).status).toBe(401);
  });
  it('rejects refresh after logout or account deactivation', async () => {
    const bindings = env();
    bindings.SESSIONS.get.mockResolvedValue({ userId: '@admin:m.sgr.ski', deviceId: 'A', accessTokenId: 'revoked' });
    bindings.DB = { prepare: vi.fn(() => ({ bind: vi.fn(() => ({ first: vi.fn(async () => null) })) })) };
    expect((await post('/_matrix/client/v3/refresh', { refresh_token: 'old-refresh-token' }, bindings)).status).toBe(401);
    expect(createAccessToken).not.toHaveBeenCalled();
  });
});
