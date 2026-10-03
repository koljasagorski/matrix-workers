import { createMiddleware } from 'hono/factory';
import type { AppEnv } from '../types';

export function frozenResponse(): Response {
  return Response.json({ errcode: 'M_RESOURCE_LIMIT_EXCEEDED', error: 'Homeserver migration maintenance', retry_after_ms: 30000 },
    { status: 503, headers: { 'Retry-After': '30', 'Cache-Control': 'no-store' } });
}

export const migrationFreeze = createMiddleware<AppEnv>(async (c, next) => {
  if (c.env.MIGRATION_FREEZE !== '1') return next();
  const path = c.req.path;
  if ((c.req.method === 'GET' && path === '/health') || (c.req.method === 'POST' && path === '/admin/api/migration/export')) return next();
  return frozenResponse();
});
