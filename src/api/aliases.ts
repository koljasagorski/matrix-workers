// Room Aliases API
// Implements: https://spec.matrix.org/v1.12/client-server-api/#room-aliases
//
// Room aliases provide human-readable names for rooms (e.g., #general:server.org)

import { resolveRoomAlias } from '../services/remote-rooms';
import { Hono } from 'hono';
import type { AppEnv } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { requireAliasDeleteAccess, requireLocalRoomAlias, requireRoomDirectoryAccess } from '../services/room-alias-access';
import { createRoomAlias } from '../services/database';
import { isObject } from '../services/federation-events';

const app = new Hono<AppEnv>();
app.onError((error, c) => {
  if (error instanceof MatrixApiError) return error.toResponse();
  console.error('[aliases] Request failed:', error);
  return c.json({ errcode: 'M_UNKNOWN', error: 'Room alias request failed' }, 500);
});

// ============================================
// Endpoints
// ============================================

// GET /_matrix/client/v3/directory/room/:roomAlias - Resolve room alias
app.get('/_matrix/client/v3/directory/room/:roomAlias', async (c) => {
  return c.json(await resolveRoomAlias(c.env, c.req.param('roomAlias')));
});

// PUT /_matrix/client/v3/directory/room/:roomAlias - Create room alias
app.put('/_matrix/client/v3/directory/room/:roomAlias', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomAlias = c.req.param('roomAlias');
  requireLocalRoomAlias(c.env, roomAlias);
  const db = c.env.DB;

  let body: { room_id: string };
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }
  if (!isObject(body)) return Errors.badJson().toResponse();
  if (!body.room_id) {
    return Errors.missingParam('room_id').toResponse();
  }
  if (typeof body.room_id !== 'string') return Errors.invalidParam('room_id').toResponse();

  // Validate alias format
  if (!roomAlias.startsWith('#') || !roomAlias.includes(':')) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Invalid room alias format',
    }, 400);
  }

  // Check room exists
  const room = await db.prepare(`
    SELECT room_id FROM rooms WHERE room_id = ?
  `).bind(body.room_id).first();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Check user is member of room
  const membership = await db.prepare(`
    SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?
  `).bind(body.room_id, userId).first<{ membership: string }>();

  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Check alias doesn't already exist
  const existing = await db.prepare(`
    SELECT alias FROM room_aliases WHERE alias = ?
  `).bind(roomAlias).first();

  if (existing) {
    return c.json({
      errcode: 'M_ROOM_IN_USE',
      error: 'Room alias already exists',
    }, 409);
  }

  // Create alias
  await createRoomAlias(db, roomAlias, body.room_id, userId);

  return c.json({});
});

// DELETE /_matrix/client/v3/directory/room/:roomAlias - Delete room alias
app.delete('/_matrix/client/v3/directory/room/:roomAlias', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomAlias = c.req.param('roomAlias');
  const db = c.env.DB;
  await requireAliasDeleteAccess(c.env, roomAlias, userId);

  // Delete alias
  await db.prepare(`
    DELETE FROM room_aliases WHERE alias = ?
  `).bind(roomAlias).run();

  return c.json({});
});

// GET /_matrix/client/v3/directory/list/room/:roomId - Get room visibility
app.get('/_matrix/client/v3/directory/list/room/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  const room = await db.prepare(`
    SELECT is_public FROM rooms WHERE room_id = ?
  `).bind(roomId).first<{ is_public: number }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  return c.json({
    visibility: room.is_public ? 'public' : 'private',
  });
});

// PUT /_matrix/client/v3/directory/list/room/:roomId - Set room visibility
app.put('/_matrix/client/v3/directory/list/room/:roomId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  let body: { visibility: 'public' | 'private' };
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }
  if (!isObject(body)) return Errors.badJson().toResponse();

  if (!body.visibility || !['public', 'private'].includes(body.visibility)) {
    return Errors.missingParam('visibility').toResponse();
  }

  await requireRoomDirectoryAccess(c.env, roomId, userId);

  // Update visibility
  await db.prepare(`
    UPDATE rooms SET is_public = ? WHERE room_id = ?
  `).bind(body.visibility === 'public' ? 1 : 0, roomId).run();

  return c.json({});
});

export default app;
