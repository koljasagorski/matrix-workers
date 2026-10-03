import { Hono } from 'hono';
import type { AppEnv, Env } from '../types';
import { requireAuth } from '../middleware/auth';
import { getUserById } from '../services/database';
import { requirePasswordConfirmation } from '../services/password-auth';
import { isObject } from '../services/federation-events';
import { Errors } from '../utils/errors';

export const EXPORT_BINDINGS = ['ROOMS', 'SYNC', 'FEDERATION', 'CALL_ROOMS', 'ADMIN', 'USER_KEYS', 'PUSH', 'RATE_LIMIT'] as const;
type ExportBinding = typeof EXPORT_BINDINGS[number];
const app = new Hono<AppEnv>();

// The inventory is supplied by the operator from Cloudflare's existing-object
// enumeration. No namespace, object ID or URL supplied by a client is trusted.
function inventory(env: Env): Partial<Record<ExportBinding, string[]>> | null {
  if (!env.MIGRATION_EXPORT_OBJECTS) return null;
  let parsed: unknown;
  try { parsed = JSON.parse(env.MIGRATION_EXPORT_OBJECTS); } catch { return null; }
  if (!isObject(parsed) || Object.keys(parsed).some(key => !EXPORT_BINDINGS.includes(key as ExportBinding))) return null;
  for (const key of EXPORT_BINDINGS) {
    const ids = parsed[key];
    if (!Array.isArray(ids) || ids.length > 10000 || ids.some(id => typeof id !== 'string' || !/^[a-f0-9]{64}$/.test(id))) return null;
  }
  return parsed as Record<ExportBinding, string[]>;
}

app.post('/admin/api/migration/export', requireAuth(), async c => {
  c.header('Cache-Control', 'no-store');
  const userId = c.get('userId');
  const user = await getUserById(c.env.DB, userId);
  if (!user?.admin) return Errors.forbidden('Admin privileges required').toResponse();
  const text = await c.req.text();
  if (text.length > 16384) return Errors.badJson('Export request is too large').toResponse();
  let body: unknown;
  try { body = JSON.parse(text); } catch { return Errors.badJson('Invalid JSON').toResponse(); }
  if (!isObject(body)) return Errors.badJson('Invalid export request').toResponse();
  const denied = await requirePasswordConfirmation(c.env.DB, userId, body.auth);
  if (denied) return denied;
  const approved = inventory(c.env);
  if (!approved) return Errors.forbidden('Migration export is disabled').toResponse();

  if (body.action === 'catalogue') {
    const users = await c.env.DB.prepare('SELECT user_id FROM users ORDER BY user_id').all<{ user_id: string }>();
    return c.json({ server_name: c.env.SERVER_NAME, namespaces: approved,
      users: users.results.map(({ user_id }) => ({ user_id, object_id: c.env.USER_KEYS.idFromName(user_id).toString() })) });
  }
  if (typeof body.namespace !== 'string' || !EXPORT_BINDINGS.includes(body.namespace as ExportBinding) || typeof body.object_id !== 'string') {
    return Errors.badJson('Invalid namespace or object ID').toResponse();
  }
  const binding = body.namespace as ExportBinding;
  if (!approved[binding]?.includes(body.object_id)) return Errors.forbidden('Object is not in the approved existing-object inventory').toResponse();
  if ((body.cursor !== undefined && (typeof body.cursor !== 'string' || new TextEncoder().encode(body.cursor).length > 2048)) ||
      (body.limit !== undefined && (!Number.isInteger(body.limit) || (body.limit as number) < 1 || (body.limit as number) > 16))) {
    return Errors.badJson('Invalid export page').toResponse();
  }
  const namespace: DurableObjectNamespace = c.env[binding];
  const url = new URL('http://internal/migration-export');
  if (typeof body.cursor === 'string') url.searchParams.set('cursor', body.cursor);
  if (typeof body.limit === 'number') url.searchParams.set('limit', String(body.limit));
  const response = await namespace.get(namespace.idFromString(body.object_id)).fetch(url.toString());
  const headers = new Headers(response.headers);
  headers.set('Cache-Control', 'no-store');
  return new Response(response.body, { status: response.status, headers });
});

export default app;
