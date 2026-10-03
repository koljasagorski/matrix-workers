// Matrix Calls API using Cloudflare Calls SFU
// Provides video/audio calling for Matrix rooms
//
// This is a custom implementation that uses Cloudflare's native SFU
// instead of LiveKit, providing a fully Cloudflare-based solution.

import { Hono } from 'hono';
import type { AppEnv } from '../types';
import { requireAuth } from '../middleware/auth';
import { Errors, MatrixApiError } from '../utils/errors';
import { isCallsConfigured } from '../services/cloudflare-calls';
import { generateOpaqueId } from '../utils/ids';
import { getMembership } from '../services/database';
import { buildLocalRoomEvent, persistLocalRoomEvent } from '../services/local-room-events';

const app = new Hono<AppEnv>();
app.onError((error) => {
  if (error instanceof MatrixApiError) return error.toResponse();
  console.error('[calls] Request failed:', error);
  return Errors.unknown().toResponse();
});

// ============================================
// Call Management API
// ============================================

// GET /_matrix/client/v3/rooms/:roomId/call - Get active call in room
app.get('/_matrix/client/v3/rooms/:roomId/call', requireAuth(), async (c) => {
  const roomId = decodeURIComponent(c.req.param('roomId'));
  const db = c.env.DB;
  if ((await getMembership(db, roomId, c.get('userId')))?.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Check if Calls is configured
  if (!isCallsConfigured(c.env)) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Video calling not configured',
    }, 500);
  }

  // Look for active call state in the room
  const callState = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.call.state'
  `).bind(roomId).first<{ content: string }>();

  if (!callState) {
    return c.json({
      active: false,
    });
  }

  try {
    const content = JSON.parse(callState.content);
    return c.json({
      active: content.active || false,
      callId: content.call_id,
      participants: content.participants || [],
      startedAt: content.started_at,
    });
  } catch {
    return c.json({ active: false });
  }
});

// POST /_matrix/client/v3/rooms/:roomId/call/start - Start a call
app.post('/_matrix/client/v3/rooms/:roomId/call/start', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = decodeURIComponent(c.req.param('roomId'));
  const db = c.env.DB;

  // Check if Calls is configured
  if (!isCallsConfigured(c.env)) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Video calling not configured',
    }, 500);
  }

  // Verify user is in the room
  const membership = await db.prepare(`
    SELECT membership FROM room_memberships
    WHERE room_id = ? AND user_id = ?
  `).bind(roomId, userId).first<{ membership: string }>();

  if (!membership || membership.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Check for existing active call
  const existingCall = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.call.state'
  `).bind(roomId).first<{ content: string }>();

  if (existingCall) {
    try {
      const content = JSON.parse(existingCall.content);
      if (content.active) {
        return c.json({
          errcode: 'M_CALL_ALREADY_ACTIVE',
          error: 'A call is already active in this room',
        }, 400);
      }
    } catch {
      // Ignore parse errors
    }
  }

  // Generate call ID
  const callId = await generateOpaqueId(16);

  // Get the CallRoom Durable Object
  if (!c.env.CALL_ROOMS) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Call rooms not configured',
    }, 500);
  }

  const callRoomId = c.env.CALL_ROOMS.idFromName(`${roomId}:${callId}`);
  const callRoom = c.env.CALL_ROOMS.get(callRoomId);

  const callStateContent = {
    active: true,
    call_id: callId,
    started_by: userId,
    started_at: Date.now(),
    participants: [],
  };
  // Authorize the room change before starting the SFU session.
  const built = await buildLocalRoomEvent(c.env, {
    roomId, sender: userId, type: 'm.call.state', stateKey: '', content: callStateContent,
  });
  const initialized = await callRoom.fetch(new Request('http://internal/init', {
    method: 'POST',
    body: JSON.stringify({
      roomId,
      callId,
    }),
  }));
  if (!initialized.ok) throw Errors.unknown('Could not initialize the call');
  await persistLocalRoomEvent(c.env, built.event, built.version);

  return c.json({
    callId,
    wsUrl: `wss://${c.env.SERVER_NAME}/calls/${callId}/ws`,
  });
});

// POST /_matrix/client/v3/rooms/:roomId/call/end - End a call
app.post('/_matrix/client/v3/rooms/:roomId/call/end', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const roomId = decodeURIComponent(c.req.param('roomId'));
  const db = c.env.DB;
  if ((await getMembership(db, roomId, userId))?.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }

  // Check if Calls is configured
  if (!isCallsConfigured(c.env)) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Video calling not configured',
    }, 500);
  }

  // Get current call state
  const callState = await db.prepare(`
    SELECT e.content, e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.call.state'
  `).bind(roomId).first<{ content: string; event_id: string }>();

  if (!callState) {
    return c.json({
      errcode: 'M_NOT_FOUND',
      error: 'No active call in this room',
    }, 404);
  }

  let callContent;
  try {
    callContent = JSON.parse(callState.content);
  } catch {
    return c.json({
      errcode: 'M_NOT_FOUND',
      error: 'No active call in this room',
    }, 404);
  }

  if (!callContent.active) {
    return c.json({
      errcode: 'M_NOT_FOUND',
      error: 'No active call in this room',
    }, 404);
  }

  const built = await buildLocalRoomEvent(c.env, {
    roomId, sender: userId, type: 'm.call.state', stateKey: '',
    content: { ...callContent, active: false, ended_at: Date.now(), ended_by: userId },
  });

  // End the call in the Durable Object
  if (c.env.CALL_ROOMS) {
    const callRoomId = c.env.CALL_ROOMS.idFromName(`${roomId}:${callContent.call_id}`);
    const callRoom = c.env.CALL_ROOMS.get(callRoomId);

    try {
      const ended = await callRoom.fetch(new Request('http://internal/end', {
        method: 'POST',
      }));
      if (!ended.ok) throw Errors.unknown('Could not end the call');
    } catch (error) {
      if (error instanceof MatrixApiError) throw error;
      throw Errors.unknown('Could not end the call');
    }
  }

  await persistLocalRoomEvent(c.env, built.event, built.version);

  return c.json({ success: true });
});

// ============================================
// WebSocket endpoint for call signaling
// ============================================

// GET /calls/:callId/ws - WebSocket connection for call signaling
app.get('/calls/:callId/ws', requireAuth(), async (c) => {
  const callId = c.req.param('callId');

  // Check if Calls is configured
  if (!isCallsConfigured(c.env)) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Video calling not configured',
    }, 500);
  }

  if (!c.env.CALL_ROOMS) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Call rooms not configured',
    }, 500);
  }

  const db = c.env.DB;
  const callEvent = await db.prepare(`
    SELECT e.room_id, e.content FROM room_state rs
    JOIN events e ON e.event_id = rs.event_id
    WHERE rs.event_type = 'm.call.state' AND rs.state_key = ''
      AND json_extract(e.content, '$.call_id') = ? AND json_extract(e.content, '$.active') = 1
  `).bind(callId).first<{ room_id: string; content: string }>();

  if (!callEvent) {
    return c.json({
      errcode: 'M_NOT_FOUND',
      error: 'Call not found',
    }, 404);
  }

  const roomId = callEvent.room_id;
  if ((await getMembership(db, roomId, c.get('userId')))?.membership !== 'join') {
    return Errors.forbidden('Not a member of this room').toResponse();
  }
  const deviceId = c.get('deviceId');
  if (!deviceId) return Errors.forbidden('A device-scoped access token is required for calls').toResponse();

  // Get the CallRoom Durable Object
  const callRoomId = c.env.CALL_ROOMS.idFromName(`${roomId}:${callId}`);
  const callRoom = c.env.CALL_ROOMS.get(callRoomId);

  // Proxy the WebSocket request to the Durable Object
  const internalHeaders = new Headers(c.req.raw.headers);
  internalHeaders.set('X-Matrix-Call-User', c.get('userId'));
  internalHeaders.set('X-Matrix-Call-Device', deviceId);
  internalHeaders.delete('Authorization');
  return callRoom.fetch(new Request(`http://internal/ws`, {
    headers: internalHeaders,
  }));
});

// ============================================
// TURN credentials endpoint
// ============================================

// GET /_matrix/client/v3/voip/turnServer - Get TURN server credentials
// This is already implemented in voip.ts, but we re-export for completeness

export default app;
