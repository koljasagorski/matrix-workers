// Room Tags API
// Implements: https://spec.matrix.org/v1.12/client-server-api/#room-tagging
//
// Room tags allow users to organize rooms into categories like favorites, low priority, etc.
// Tags are stored per-user and are private.

import { Hono } from 'hono';
import type { AppEnv } from '../types';
import { Errors } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { mergeAccountData, notifyAccountDataUser } from '../services/account-data-stream';
import { isObject } from '../services/federation-events';

const app = new Hono<AppEnv>();

// ============================================
// Endpoints
// ============================================

// GET /_matrix/client/v3/user/:userId/rooms/:roomId/tags - Get room tags
app.get('/_matrix/client/v3/user/:userId/rooms/:roomId/tags', requireAuth(), async (c) => {
  const requestingUserId = c.get('userId');
  const targetUserId = c.req.param('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  // Users can only get their own tags
  if (requestingUserId !== targetUserId) {
    return Errors.forbidden('Cannot access tags for other users').toResponse();
  }

  // Check membership (user should be a member to have tags)
  const membership = await db.prepare(`
    SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?
  `).bind(roomId, requestingUserId).first<{ membership: string }>();

  if (!membership) {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Get tags from account data
  const accountData = await db.prepare(`
    SELECT content FROM account_data
    WHERE user_id = ? AND room_id = ? AND event_type = 'm.tag'
  `).bind(requestingUserId, roomId).first<{ content: string }>();

  if (!accountData) {
    return c.json({ tags: {} });
  }

  try {
    const content = JSON.parse(accountData.content);
    return c.json({ tags: content.tags || {} });
  } catch {
    return c.json({ tags: {} });
  }
});

// PUT /_matrix/client/v3/user/:userId/rooms/:roomId/tags/:tag - Add a tag
app.put('/_matrix/client/v3/user/:userId/rooms/:roomId/tags/:tag', requireAuth(), async (c) => {
  const requestingUserId = c.get('userId');
  const targetUserId = c.req.param('userId');
  const roomId = c.req.param('roomId');
  const tag = c.req.param('tag');
  const db = c.env.DB;

  // Users can only set their own tags
  if (requestingUserId !== targetUserId) {
    return Errors.forbidden('Cannot set tags for other users').toResponse();
  }

  // Check membership
  const membership = await db.prepare(`
    SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?
  `).bind(roomId, requestingUserId).first<{ membership: string }>();

  if (!membership) {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Parse body for tag content (e.g., order)
  let tagContent: Record<string, any> = {};
  try {
    tagContent = await c.req.json();
  } catch {
    // Body is optional
  }

  await mergeAccountData(db, requestingUserId, roomId, 'm.tag', current => {
    let tags: Record<string, unknown> = {};
    try { const value = JSON.parse(current ?? '{}'); if (isObject(value) && isObject(value.tags)) tags = value.tags; } catch { /* start fresh */ }
    return { tags: { ...tags, [tag]: tagContent } };
  });
  await notifyAccountDataUser(c.env, requestingUserId);

  return c.json({});
});

// DELETE /_matrix/client/v3/user/:userId/rooms/:roomId/tags/:tag - Remove a tag
app.delete('/_matrix/client/v3/user/:userId/rooms/:roomId/tags/:tag', requireAuth(), async (c) => {
  const requestingUserId = c.get('userId');
  const targetUserId = c.req.param('userId');
  const roomId = c.req.param('roomId');
  const tag = c.req.param('tag');
  const db = c.env.DB;

  // Users can only delete their own tags
  if (requestingUserId !== targetUserId) {
    return Errors.forbidden('Cannot delete tags for other users').toResponse();
  }

  await mergeAccountData(db, requestingUserId, roomId, 'm.tag', current => {
    try {
      const value = JSON.parse(current ?? '{}');
      if (!isObject(value) || !isObject(value.tags)) return undefined;
      const tags = { ...value.tags }; delete tags[tag];
      return { tags };
    } catch { return undefined; }
  });
  await notifyAccountDataUser(c.env, requestingUserId);

  return c.json({});
});

export default app;
