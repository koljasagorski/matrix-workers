// Matrix login/registration endpoints

import { Hono } from 'hono';
import type { AppEnv } from '../types';
import { Errors } from '../utils/errors';
import { hashPassword, verifyPassword, hashToken } from '../utils/crypto';
import {
  formatUserId,
  generateDeviceId,
  generateAccessToken,
  generateRefreshToken,
  generateOpaqueId,
  isValidLocalpart,
} from '../utils/ids';
import {
  createUser,
  getUserByLocalpart,
  getUserById,
  getPasswordHash,
  createDevice,
  createAccessToken,
  deleteAccessToken,
  deleteAllUserTokens,
} from '../services/database';
import { requireAuth, extractAccessToken } from '../middleware/auth';
import { getAppServices, isExclusiveAppServiceUser } from '../services/appservice';

const app = new Hono<AppEnv>();

const ACCESS_TOKEN_LIFETIME_MS = 60 * 60 * 1000;
const REFRESH_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60;

async function isReservedRegistrationUser(env: AppEnv['Bindings'], userId: string): Promise<boolean> {
  const appservices = await getAppServices(env.DB);
  return appservices.some(service => userId === formatUserId(service.sender_localpart, env.SERVER_NAME))
    || isExclusiveAppServiceUser(appservices, userId) !== null;
}

function exclusiveUsernameResponse(): Response {
  return Response.json({ errcode: 'M_EXCLUSIVE', error: 'Username is reserved by an application service' }, { status: 400 });
}

async function createSessionTokens(
  env: AppEnv['Bindings'],
  userId: string,
  deviceId: string | null,
  supportsRefresh: boolean
): Promise<{ access_token: string; refresh_token?: string; expires_in_ms?: number }> {
  const accessToken = await generateAccessToken();
  const tokenHash = await hashToken(accessToken);
  const tokenId = await generateOpaqueId(16);

  // Legacy clients such as Element Desktop cannot renew password-login tokens.
  // Only expire tokens when the client explicitly requests refresh support.
  await createAccessToken(
    env.DB, tokenId, tokenHash, userId, deviceId,
    supportsRefresh ? Date.now() + ACCESS_TOKEN_LIFETIME_MS : null
  );

  if (!supportsRefresh) {
    return { access_token: accessToken };
  }

  const refreshToken = await generateRefreshToken();
  const refreshTokenHash = await hashToken(refreshToken);
  await env.SESSIONS.put(
    `refresh:${refreshTokenHash}`,
    JSON.stringify({ userId, deviceId, accessTokenId: tokenId, createdAt: Date.now() }),
    { expirationTtl: REFRESH_TOKEN_TTL_SECONDS }
  );

  return {
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_in_ms: ACCESS_TOKEN_LIFETIME_MS,
  };
}

// GET /_matrix/client/v3/login - Get supported login flows
app.get('/_matrix/client/v3/login', (c) => {
  return c.json({
    flows: [
      {
        type: 'm.login.password',
      },
      {
        type: 'm.login.token',
      },
    ],
  });
});

// POST /_matrix/client/v3/login - Login
app.post('/_matrix/client/v3/login', async (c) => {
  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const {
    type, identifier, password, token, device_id, initial_device_display_name,
    refresh_token: refreshTokenRequested,
  } = body;

  let userId: string;

  if (type === 'm.login.token') {
    // Token-based login (for QR codes)
    if (!token) {
      return Errors.missingParam('token').toResponse();
    }

    // Look up the token in KV
    const tokenHash = await hashToken(token);
    const tokenData = await c.env.SESSIONS.get(`login_token:${tokenHash}`, 'json') as {
      user_id: string;
      expires_at: number;
    } | null;

    if (!tokenData) {
      return Errors.forbidden('Invalid or expired login token').toResponse();
    }

    // Check expiration
    if (Date.now() > tokenData.expires_at) {
      // Clean up expired token
      await c.env.SESSIONS.delete(`login_token:${tokenHash}`);
      return Errors.forbidden('Login token has expired').toResponse();
    }

    userId = tokenData.user_id;

    // Delete the token (one-time use)
    await c.env.SESSIONS.delete(`login_token:${tokenHash}`);

  } else if (type === 'm.login.password') {
    // Password-based login
    if (!identifier || !password) {
      return Errors.missingParam('identifier or password').toResponse();
    }

    // Parse identifier
    if (identifier.type === 'm.id.user') {
      // Can be full user ID or just localpart
      if (identifier.user.startsWith('@')) {
        userId = identifier.user;
      } else {
        userId = formatUserId(identifier.user, c.env.SERVER_NAME);
      }
    } else {
      return Errors.unrecognized('Unknown identifier type').toResponse();
    }

    // Get stored password hash
    const storedHash = await getPasswordHash(c.env.DB, userId);
    if (!storedHash) {
      return Errors.forbidden('Invalid username or password').toResponse();
    }

    // Verify password
    const valid = await verifyPassword(password, storedHash);
    if (!valid) {
      return Errors.forbidden('Invalid username or password').toResponse();
    }
  } else {
    return Errors.unrecognized('Unknown login type').toResponse();
  }

  // Check if user is deactivated
  const user = await getUserById(c.env.DB, userId);
  if (!user) {
    return Errors.forbidden('Invalid username or password').toResponse();
  }
  if (user.is_deactivated) {
    return Errors.userDeactivated().toResponse();
  }

  // Generate or use provided device ID
  const deviceId = device_id || await generateDeviceId();

  // Create device
  await createDevice(c.env.DB, userId, deviceId, initial_device_display_name);

  const tokens = await createSessionTokens(c.env, userId, deviceId, refreshTokenRequested === true);

  return c.json({
    user_id: userId,
    device_id: deviceId,
    home_server: c.env.SERVER_NAME,
    ...tokens,
  });
});

// POST /_matrix/client/v3/logout - Logout current session
app.post('/_matrix/client/v3/logout', requireAuth(), async (c) => {
  const token = extractAccessToken(c.req.raw);
  if (token) {
    const tokenHash = await hashToken(token);
    await deleteAccessToken(c.env.DB, tokenHash);
  }
  return c.json({});
});

// POST /_matrix/client/v3/logout/all - Logout all sessions
app.post('/_matrix/client/v3/logout/all', requireAuth(), async (c) => {
  const userId = c.get('userId');
  await deleteAllUserTokens(c.env.DB, userId);
  return c.json({});
});

// POST /_matrix/client/v3/refresh - Refresh access token
// Uses the refresh token to get a new access token + refresh token pair
// Implements token rotation (single-use refresh tokens)
app.post('/_matrix/client/v3/refresh', async (c) => {
  let body: { refresh_token?: string };
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { refresh_token: refreshToken } = body;

  if (!refreshToken) {
    return Errors.missingParam('refresh_token').toResponse();
  }

  // Hash the incoming refresh token
  const refreshTokenHash = await hashToken(refreshToken);

  // Look up in KV
  const tokenData = await c.env.SESSIONS.get(`refresh:${refreshTokenHash}`, 'json') as {
    userId: string;
    deviceId: string | null;
    accessTokenId: string;
    createdAt: number;
  } | null;

  if (!tokenData) {
    return Errors.unknownToken('Invalid or expired refresh token').toResponse();
  }

  const { userId, deviceId, accessTokenId } = tokenData;

  // Logout, device deletion and account deactivation must also revoke refresh.
  const session = await c.env.DB.prepare(
    `SELECT t.token_id FROM access_tokens t JOIN users u ON u.user_id = t.user_id
     WHERE t.token_id = ? AND t.user_id = ? AND u.is_deactivated = 0`
  ).bind(accessTokenId, userId).first();
  if (!session) {
    await c.env.SESSIONS.delete(`refresh:${refreshTokenHash}`);
    return Errors.unknownToken('Session has been revoked').toResponse();
  }

  // Delete old refresh token from KV (token rotation - single use)
  await c.env.SESSIONS.delete(`refresh:${refreshTokenHash}`);

  // Delete old access token from D1
  const consumed = await c.env.DB.prepare(
    `DELETE FROM access_tokens WHERE token_id = ? RETURNING token_id`
  ).bind(accessTokenId).first();
  if (!consumed) {
    return Errors.unknownToken('Refresh token has already been used').toResponse();
  }

  return c.json(await createSessionTokens(c.env, userId, deviceId, true));
});

// GET /_matrix/client/v3/register/available - Check if username is available
// Use the same durable configuration as the admin dashboard, including guests.
app.use('/_matrix/client/v3/register*', async (c, next) => {
  const admin = c.env.ADMIN.get(c.env.ADMIN.idFromName('global'));
  const response = await admin.fetch('https://internal/config');
  if (!response.ok) {
    return Errors.forbidden('Registration is unavailable').toResponse();
  }
  const config = await response.json() as { registration_enabled?: boolean };
  if (config.registration_enabled !== true) {
    return Errors.forbidden('Registration is disabled').toResponse();
  }
  return next();
});

app.get('/_matrix/client/v3/register/available', async (c) => {
  const username = c.req.query('username');

  if (!username) {
    return Errors.missingParam('username').toResponse();
  }

  if (!isValidLocalpart(username)) {
    return Errors.invalidUsername('Username contains invalid characters').toResponse();
  }

  if (await isReservedRegistrationUser(c.env, formatUserId(username, c.env.SERVER_NAME))) {
    return exclusiveUsernameResponse();
  }

  const existing = await getUserByLocalpart(c.env.DB, username);
  if (existing) {
    return Errors.userInUse().toResponse();
  }

  return c.json({ available: true });
});

// POST /_matrix/client/v3/register - Register new user
app.post('/_matrix/client/v3/register', async (c) => {
  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const {
    username,
    password,
    device_id,
    initial_device_display_name,
    inhibit_login,
    refresh_token: refreshTokenRequested,
    auth,
  } = body;

  // Check registration kind
  const kind = c.req.query('kind') || 'user';
  if (kind !== 'user' && kind !== 'guest') {
    return Errors.invalidParam('kind', 'Invalid registration kind').toResponse();
  }

  const isGuest = kind === 'guest';

  // Reservation checks must precede UIA and account creation. The AS sender is reserved
  // even when it is not explicitly included in the configured user namespaces.
  if (!isGuest && typeof username === 'string' && isValidLocalpart(username)
    && await isReservedRegistrationUser(c.env, formatUserId(username, c.env.SERVER_NAME))) {
    return exclusiveUsernameResponse();
  }

  // For non-guests, require username and password
  if (!isGuest) {
    // Simple auth - in production, implement UIA (User-Interactive Authentication)
    if (!auth || auth.type !== 'm.login.dummy') {
      // Return UIA requirements
      const sessionId = await generateOpaqueId(16);
      return c.json({
        flows: [{ stages: ['m.login.dummy'] }],
        params: {},
        session: sessionId,
      }, 401);
    }

    if (!username) {
      return Errors.missingParam('username').toResponse();
    }

    if (!isValidLocalpart(username)) {
      return Errors.invalidUsername('Username contains invalid characters').toResponse();
    }

    if (!password) {
      return Errors.missingParam('password').toResponse();
    }
  }

  // Generate localpart for guests
  const localpart = isGuest ? await generateOpaqueId(12) : username;
  const userId = formatUserId(localpart, c.env.SERVER_NAME);

  if (isGuest && await isReservedRegistrationUser(c.env, userId)) {
    return exclusiveUsernameResponse();
  }

  // Check if user already exists
  const existing = await getUserById(c.env.DB, userId);
  if (existing) {
    return Errors.userInUse().toResponse();
  }

  // Hash password (null for guests)
  const passwordHash = password ? await hashPassword(password) : null;

  // Create user
  await createUser(c.env.DB, userId, localpart, passwordHash, isGuest);

  // Response depends on inhibit_login
  if (inhibit_login) {
    return c.json({
      user_id: userId,
      home_server: c.env.SERVER_NAME,
    });
  }

  // Generate device and access token
  const deviceId = device_id || await generateDeviceId();
  await createDevice(c.env.DB, userId, deviceId, initial_device_display_name);

  const tokens = await createSessionTokens(c.env, userId, deviceId, refreshTokenRequested === true);

  return c.json({
    user_id: userId,
    device_id: deviceId,
    home_server: c.env.SERVER_NAME,
    ...tokens,
  });
});

// POST /_matrix/client/v1/login/get_token - Generate a login token for authenticated user
// Per Matrix spec: generates a short-lived login token for QR code login and similar flows
app.post('/_matrix/client/v1/login/get_token', requireAuth(), async (c) => {
  const userId = c.get('userId');

  // Generate a login token
  const tokenBytes = crypto.getRandomValues(new Uint8Array(32));
  const loginToken = btoa(String.fromCharCode(...tokenBytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');

  // Token is valid for 2 minutes (per Matrix spec recommendation)
  const expiresInMs = 2 * 60 * 1000;
  const expiresAt = Date.now() + expiresInMs;

  // Store the token in KV with TTL
  const tokenHash = await hashToken(loginToken);
  await c.env.SESSIONS.put(
    `login_token:${tokenHash}`,
    JSON.stringify({
      user_id: userId,
      expires_at: expiresAt,
    }),
    {
      expirationTtl: 120, // 2 minutes in seconds
    }
  );

  return c.json({
    login_token: loginToken,
    expires_in_ms: expiresInMs,
  });
});

// GET /_matrix/client/v3/account/whoami - Get current user info
app.get('/_matrix/client/v3/account/whoami', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const deviceId = c.get('deviceId');

  const user = await getUserById(c.env.DB, userId);
  if (!user) {
    return Errors.unknownToken().toResponse();
  }

  return c.json({
    user_id: userId,
    device_id: deviceId,
    is_guest: user.is_guest,
  });
});

export default app;
