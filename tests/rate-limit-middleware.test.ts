import { Hono } from 'hono';
import { expect, it, vi } from 'vitest';
import { rateLimitMiddleware } from '../src/middleware/rate-limit';
import type { AppEnv } from '../src/types';

function fixture() {
  const requests: { clientId: string; limit: number }[] = [];
  const idFromName = vi.fn((name: string) => name);
  const fetch = vi.fn(async (request: Request) => {
    requests.push(await request.json());
    return Response.json({ allowed: false, remaining: 0, retryAfterMs: 1500 });
  });
  const env = { RATE_LIMIT: { idFromName, get: () => ({ fetch }) } };
  const app = new Hono<AppEnv>();
  app.use('*', rateLimitMiddleware);
  app.all('*', c => c.json({ reached_handler: true }));
  return { app, env, requests, idFromName, fetch };
}

it.each([
  '/_matrix/client/v3/rooms/!room:local.example/send/sync.custom/txn',
  '/_matrix/client/v3/rooms/!room:local.example/state/sync.custom/',
])('does not allow arbitrary event types to bypass rate limiting through a /sync path substring (%s)', async path => {
  const { app, env, requests, fetch } = fixture();
  const response = await app.request(path, { method: 'PUT', headers: { 'CF-Connecting-IP': '192.0.2.10' } }, env);
  expect(response.status).toBe(429);
  expect(await response.json()).toMatchObject({ errcode: 'M_LIMIT_EXCEEDED', retry_after_ms: 1500 });
  expect(response.headers.get('Retry-After')).toBe('2');
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(requests[0].clientId).toBe('ip:192.0.2.10');
});

it.each([
  ['GET', '/_matrix/client/v3/sync'],
  ['POST', '/_matrix/client/v4/sync'],
  ['POST', '/_matrix/client/unstable/org.matrix.msc3575/sync'],
  ['POST', '/_matrix/client/unstable/org.matrix.simplified_msc3575/sync'],
])('preserves the existing long-poll sync exemption for %s %s', async (method, path) => {
  const { app, env, fetch } = fixture();
  expect((await app.request(path, { method }, env)).status).toBe(200);
  expect(fetch).not.toHaveBeenCalled();
});
