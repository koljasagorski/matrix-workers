// Read Receipts API
// Implements: https://spec.matrix.org/v1.12/client-server-api/#receipts
//
// Read receipts track which events users have read.
// Types:
// - m.read: Public read receipt (visible to others) - stored in Room DO
// - m.read.private: Private read receipt (only visible to self) - stored in Room DO
// - m.fully_read: Read marker (stored in account data for unread counts)

import { Hono } from 'hono';
import type { AppEnv, Env } from '../types';
import { Errors } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { publishReadReceipt } from '../services/read-receipts';
import { isObject } from '../services/federation-events';

const app = new Hono<AppEnv>();

// ============================================
// Helper to get Room DO stub
// ============================================

function getRoomDO(env: Env, roomId: string) {
  const id = env.ROOMS.idFromName(roomId);
  return env.ROOMS.get(id);
}

// ============================================
// Endpoints
// ============================================

// POST /_matrix/client/v3/rooms/:roomId/receipt/:receiptType/:eventId - Send receipt
app.post('/_matrix/client/v3/rooms/:roomId/receipt/:receiptType/:eventId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const receiptType = c.req.param('receiptType');
  const eventId = c.req.param('eventId');
  const db = c.env.DB;

  console.log('[receipts] POST request from', userId, 'type:', receiptType, 'event:', eventId, 'room:', roomId);

  // Validate receipt type
  const validTypes = ['m.read', 'm.read.private', 'm.fully_read'];
  if (!validTypes.includes(receiptType)) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: `Invalid receipt type: ${receiptType}`,
    }, 400);
  }

  // Check membership
  const membership = await db.prepare(`
    SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?
  `).bind(roomId, userId).first<{ membership: string }>();

  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  if (!eventId.startsWith('$') || !await db.prepare('SELECT 1 FROM events WHERE event_id=? AND room_id=?')
    .bind(eventId, roomId).first()) {
    return Errors.notFound('Event not found in this room').toResponse();
  }

  // Parse optional body for thread_id
  let threadId: string | undefined;
  const text = await c.req.text();
  if (text) {
    let body: unknown;
    try { body = JSON.parse(text); } catch { return Errors.badJson().toResponse(); }
    if (!isObject(body)) return Errors.badJson().toResponse();
    if (body.thread_id !== undefined && (typeof body.thread_id !== 'string' || !body.thread_id)) {
      return Errors.invalidParam('thread_id').toResponse();
    }
    threadId = body.thread_id as string | undefined;
  }

  // m.fully_read is special - it's room account data, not an ephemeral receipt
  // Store it in account_data table so it's returned in account_data extension
  if (receiptType === 'm.fully_read') {
    await db.prepare(`
      INSERT INTO account_data (user_id, room_id, event_type, content)
      VALUES (?, ?, 'm.fully_read', ?)
      ON CONFLICT (user_id, room_id, event_type) DO UPDATE SET
        content = excluded.content
    `).bind(userId, roomId, JSON.stringify({ event_id: eventId })).run();
    console.log('[receipts] Stored m.fully_read in account_data for', userId, 'in room', roomId, 'event', eventId);
  } else {
    // Store m.read and m.read.private in Room Durable Object
    await publishReadReceipt(c.env, roomId, { user_id: userId, event_id: eventId,
      receipt_type: receiptType as 'm.read' | 'm.read.private', thread_id: threadId, ts: Date.now() });
    console.log('[receipts] Stored', receiptType, 'in Room DO for', userId, 'in room', roomId, 'event', eventId);

    // Also update m.fully_read when m.read is set - Element X uses m.fully_read for unread counts
    // This keeps the read marker in sync with the read receipt
    if (receiptType === 'm.read' && threadId === undefined) {
      await db.prepare(`
        INSERT INTO account_data (user_id, room_id, event_type, content)
        VALUES (?, ?, 'm.fully_read', ?)
        ON CONFLICT (user_id, room_id, event_type) DO UPDATE SET
          content = excluded.content
      `).bind(userId, roomId, JSON.stringify({ event_id: eventId })).run();
      console.log('[receipts] Also updated m.fully_read in account_data for', userId, 'in room', roomId);
    }
  }

  return c.json({});
});

// POST /_matrix/client/v3/rooms/:roomId/read_markers - Set read marker
app.post('/_matrix/client/v3/rooms/:roomId/read_markers', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = c.req.param('roomId');
  const db = c.env.DB;

  let body: {
    'm.fully_read'?: string;
    'm.read'?: string;
    'm.read.private'?: string;
  };
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }
  if (!isObject(body)) return Errors.badJson().toResponse();
  for (const key of ['m.read', 'm.read.private', 'm.fully_read'] as const) {
    if (body[key] !== undefined && (typeof body[key] !== 'string' || !body[key]!.startsWith('$'))) {
      return Errors.invalidParam(key).toResponse();
    }
  }

  // Check membership
  const membership = await db.prepare(`
    SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?
  `).bind(roomId, userId).first<{ membership: string }>();

  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Validate every target before making any changes to this combined request.
  const targets = ['m.read', 'm.read.private', 'm.fully_read'].flatMap(key => {
    const value = body[key as keyof typeof body];
    return value === undefined ? [] : [value];
  });
  for (const eventId of new Set(targets)) {
    if (!await db.prepare('SELECT 1 FROM events WHERE event_id=? AND room_id=?').bind(eventId, roomId).first()) {
      return Errors.notFound('Event not found in this room').toResponse();
    }
  }

  // Process m.fully_read (stored in account data for unread counts)
  if (body['m.fully_read']) {
    await db.prepare(`
      INSERT INTO account_data (user_id, room_id, event_type, content)
      VALUES (?, ?, 'm.fully_read', ?)
      ON CONFLICT (user_id, room_id, event_type) DO UPDATE SET
        content = excluded.content
    `).bind(userId, roomId, JSON.stringify({ event_id: body['m.fully_read'] })).run();
    console.log('[receipts] Stored m.fully_read in account_data for', userId, 'in room', roomId);
  }

  // Process m.read (stored in Room DO)
  if (body['m.read']) {
    await publishReadReceipt(c.env, roomId, { user_id: userId, event_id: body['m.read'], receipt_type: 'm.read', ts: Date.now() });

    // If m.fully_read wasn't explicitly provided, also update it to match m.read
    // This keeps unread counts in sync for clients that only send m.read
    if (!body['m.fully_read']) {
      await db.prepare(`
        INSERT INTO account_data (user_id, room_id, event_type, content)
        VALUES (?, ?, 'm.fully_read', ?)
        ON CONFLICT (user_id, room_id, event_type) DO UPDATE SET
          content = excluded.content
      `).bind(userId, roomId, JSON.stringify({ event_id: body['m.read'] })).run();
      console.log('[receipts] Auto-updated m.fully_read to match m.read for', userId, 'in room', roomId);
    }
  }

  // Process m.read.private (stored in Room DO)
  if (body['m.read.private']) {
    await publishReadReceipt(c.env, roomId, { user_id: userId, event_id: body['m.read.private'], receipt_type: 'm.read.private', ts: Date.now() });
  }

  return c.json({});
});

// ============================================
// Internal Helpers
// ============================================

// Get receipts for a room (for sync) - fetches from Room Durable Object
// requestingUserId is used to filter m.read.private receipts - they should only be visible to the owner
export async function getReceiptsForRoom(
  env: Env,
  roomId: string,
  requestingUserId: string
): Promise<{
  type: 'm.receipt';
  content: Record<string, Record<string, Record<string, { ts: number; thread_id?: string }>>>;
}> {
  const membership = await env.DB.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?')
    .bind(roomId, requestingUserId).first<{ membership: string }>();
  if (membership?.membership !== 'join') return { type: 'm.receipt', content: {} };
  const roomDO = getRoomDO(env, roomId);
  const response = await roomDO.fetch(new Request(`https://room/receipts?room_id=${encodeURIComponent(roomId)}`, {
    method: 'GET',
  }));

  const data = await response.json() as {
    receipts: Record<string, Record<string, Record<string, { ts: number; thread_id?: string }>>>;
  };

  // Filter private receipts - m.read.private should only be visible to the owner
  if (requestingUserId) {
    const filteredReceipts: typeof data.receipts = {};

    for (const [eventId, receiptTypes] of Object.entries(data.receipts)) {
      filteredReceipts[eventId] = {};

      for (const [receiptType, users] of Object.entries(receiptTypes)) {
        if (receiptType === 'm.read.private') {
          // Only include private receipt if it belongs to the requesting user
          if (users[requestingUserId]) {
            filteredReceipts[eventId][receiptType] = {
              [requestingUserId]: users[requestingUserId],
            };
          }
        } else {
          // Include all public receipts
          filteredReceipts[eventId][receiptType] = users;
        }
      }

      // Remove empty event entries
      if (Object.keys(filteredReceipts[eventId]).length === 0) {
        delete filteredReceipts[eventId];
      }
    }

    return {
      type: 'm.receipt',
      content: filteredReceipts,
    };
  }

  return {
    type: 'm.receipt',
    content: data.receipts,
  };
}

// Get receipts for multiple rooms (for sync)
// requestingUserId is used to filter m.read.private receipts
export async function getReceiptsForRooms(
  env: Env,
  roomIds: string[],
  requestingUserId: string
): Promise<Record<string, Record<string, Record<string, Record<string, { ts: number; thread_id?: string }>>>>> {
  if (roomIds.length === 0) return {};

  const results = await Promise.all(
    roomIds.map(async (roomId) => {
      try {
        const receipts = await getReceiptsForRoom(env, roomId, requestingUserId);
        return { roomId, content: receipts.content };
      } catch {
        return { roomId, content: {} };
      }
    })
  );

  const byRoom: Record<string, Record<string, Record<string, Record<string, { ts: number; thread_id?: string }>>>> = {};
  for (const { roomId, content } of results) {
    if (Object.keys(content).length > 0) {
      byRoom[roomId] = content;
    }
  }

  return byRoom;
}

export default app;
