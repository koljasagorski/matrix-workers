// Server Notices API
// Implements server-generated notices to users
//
// Server notices are messages sent by the server to inform users about
// important events like terms of service updates, security alerts, etc.

import { Hono } from 'hono';
import type { AppEnv, Env } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { generateOpaqueId, parseUserId } from '../utils/ids';
import { sendLocalRoomEvent } from '../services/local-room-events';
import { createRoom } from '../services/database';
import { isObject } from '../services/federation-events';

const app = new Hono<AppEnv>();
app.onError((error) => {
  if (error instanceof MatrixApiError) return error.toResponse();
  console.error('[server-notices] Request failed:', error);
  return Errors.unknown().toResponse();
});

// Server notice room configuration
const SERVER_NOTICE_ROOM_TYPE = 'm.server_notice';
const SERVER_NOTICE_USER_LOCALPART = 'server';

// ============================================
// Internal Functions
// ============================================

// Get or create the server notice user
async function getServerNoticeUser(db: D1Database, serverName: string): Promise<string> {
  const userId = `@${SERVER_NOTICE_USER_LOCALPART}:${serverName}`;

  await db.prepare(`
    INSERT OR IGNORE INTO users (user_id, localpart, display_name, admin, is_guest, is_deactivated)
    VALUES (?, ?, 'Server Notices', 0, 0, 0)
  `).bind(userId, SERVER_NOTICE_USER_LOCALPART).run();

  return userId;
}

// Get or create a server notice room for a user
async function getOrCreateNoticeRoom(
  env: Env,
  targetUserId: string
): Promise<string> {
  const db = env.DB;
  const serverName = env.SERVER_NAME;
  // Check if user already has a server notice room
  const existing = await db.prepare(`
    SELECT rm.room_id FROM room_memberships rm
    JOIN room_state rs ON rm.room_id = rs.room_id
    JOIN events e ON rs.event_id = e.event_id
    WHERE rm.user_id = ?
      AND rs.event_type = 'm.room.create'
      AND json_extract(e.content, '$.type') = 'm.server_notice'
    LIMIT 1
  `).bind(targetUserId).first<{ room_id: string }>();

  if (existing) {
    return existing.room_id;
  }

  // Create a new server notice room
  const serverUserId = await getServerNoticeUser(db, serverName);
  const roomId = `!${await generateOpaqueId(18)}:${serverName}`;
  await createRoom(db, roomId, '10', serverUserId);

  // Create room events
  const events = [
    {
      type: 'm.room.create',
      state_key: '',
      content: {
        creator: serverUserId,
        room_version: '10',
        type: SERVER_NOTICE_ROOM_TYPE,
        'm.federate': false,
      },
    },
    {
      type: 'm.room.member',
      state_key: serverUserId,
      content: { membership: 'join', displayname: 'Server Notices' },
    },
    {
      type: 'm.room.name',
      state_key: '',
      content: {
        name: 'Server Notices',
      },
    },
    {
      type: 'm.room.join_rules',
      state_key: '',
      content: {
        join_rule: 'invite',
      },
    },
    {
      type: 'm.room.history_visibility',
      state_key: '',
      content: {
        history_visibility: 'joined',
      },
    },
    {
      type: 'm.room.power_levels',
      state_key: '',
      content: {
        users: {
          [serverUserId]: 100,
        },
        users_default: 0,
        events_default: 50,
        state_default: 50,
        ban: 50,
        kick: 50,
        redact: 50,
        invite: 100,
      },
    },
    {
      type: 'm.room.member',
      state_key: targetUserId,
      content: {
        membership: 'invite',
      },
    },
  ];

  for (const event of events) {
    await sendLocalRoomEvent(env, {
      roomId, sender: serverUserId, type: event.type, stateKey: event.state_key, content: event.content,
    });
  }

  return roomId;
}

// Send a server notice to a user
export async function sendServerNotice(
  env: Env,
  targetUserId: string,
  body: string,
  msgtype: string = 'm.text',
  adminContact?: string
): Promise<string> {
  if (parseUserId(targetUserId)?.serverName !== env.SERVER_NAME ||
      !await env.DB.prepare('SELECT user_id FROM users WHERE user_id=? AND is_deactivated=0').bind(targetUserId).first()) {
    throw Errors.notFound('Server notices require an active local user');
  }
  const roomId = await getOrCreateNoticeRoom(env, targetUserId);
  const serverUserId = await getServerNoticeUser(env.DB, env.SERVER_NAME);
  const content: Record<string, unknown> = {
    msgtype,
    body,
  };

  if (adminContact) {
    content.admin_contact = adminContact;
  }

  // Server notice specific content
  content['m.server_notice_type'] = 'm.server_notice.usage_limit_reached'; // or other types

  const event = await sendLocalRoomEvent(env, { roomId, sender: serverUserId, type: 'm.room.message', content });
  return event.event_id;
}

// ============================================
// Admin Endpoints for Server Notices
// ============================================

// POST /_synapse/admin/v1/send_server_notice - Send a server notice (Synapse-compatible)
app.post('/_synapse/admin/v1/send_server_notice', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const db = c.env.DB;

  // Check if user is admin
  const user = await db.prepare(`
    SELECT admin FROM users WHERE user_id = ?
  `).bind(userId).first<{ admin: number }>();

  if (!user || user.admin !== 1) {
    return Errors.forbidden('Admin access required').toResponse();
  }

  let body: {
    user_id: string;
    content: {
      msgtype: string;
      body: string;
      admin_contact?: string;
    };
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (!isObject(body) || typeof body.user_id !== 'string' || !isObject(body.content) ||
      typeof body.content.body !== 'string' || !body.content.body ||
      (body.content.msgtype !== undefined && typeof body.content.msgtype !== 'string') ||
      (body.content.admin_contact !== undefined && typeof body.content.admin_contact !== 'string')) {
    return Errors.missingParam('user_id or content.body').toResponse();
  }

  const eventId = await sendServerNotice(
    c.env,
    body.user_id,
    body.content.body,
    body.content.msgtype || 'm.text',
    body.content.admin_contact
  );

  return c.json({ event_id: eventId });
});

// POST /_matrix/client/v3/admin/send_server_notice - Alternative endpoint
app.post('/_matrix/client/v3/admin/send_server_notice', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const db = c.env.DB;

  // Check if user is admin
  const user = await db.prepare(`
    SELECT admin FROM users WHERE user_id = ?
  `).bind(userId).first<{ admin: number }>();

  if (!user || user.admin !== 1) {
    return Errors.forbidden('Admin access required').toResponse();
  }

  let body: {
    user_id: string;
    content: {
      msgtype: string;
      body: string;
    };
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (!isObject(body) || typeof body.user_id !== 'string' || !isObject(body.content) ||
      typeof body.content.body !== 'string' || !body.content.body ||
      (body.content.msgtype !== undefined && typeof body.content.msgtype !== 'string')) {
    return Errors.missingParam('user_id or content.body').toResponse();
  }

  const eventId = await sendServerNotice(
    c.env,
    body.user_id,
    body.content.body,
    body.content.msgtype || 'm.text'
  );

  return c.json({ event_id: eventId });
});

export default app;
