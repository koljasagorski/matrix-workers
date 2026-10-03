// Sliding Sync API (MSC3575 & MSC4186)
// Implements both the original sliding sync and simplified sliding sync

import { Hono, type Context } from 'hono';
import type { AppEnv, Env } from '../types';
import { initialRoomHistory } from '../services/sync-history';
import { Errors } from '../utils/errors';
import { getStoredInviteState } from '../services/remote-invites';
import { requireAuth } from '../middleware/auth';
import { getToDeviceMessages } from './to-device';
import { parseSyncPosition, deviceKeyPosition, changedDeviceUsers } from '../services/sync-positions';
import { getTypingForRooms } from './typing';
import { getReceiptsForRooms } from './receipts';
import { receiptPosition } from '../services/read-receipts';
import { accountDataPosition } from '../services/account-data-stream';
import { countNotificationsWithRules } from '../services/push-rule-evaluator';
// Room cache helper available for future optimizations
// import { getRoomMetadata, invalidateRoomCache, type RoomMetadata } from '../services/room-cache';

const app = new Hono<AppEnv>();

// Types for sliding sync

interface SlidingSyncRequest {
  // Connection tracking
  conn_id?: string;
  pos?: string;
  txn_id?: string;
  timeout?: number;

  // MSC3575 style
  delta_token?: string;

  // Room lists
  lists?: Record<string, SyncListConfig>;

  // Direct room subscriptions
  room_subscriptions?: Record<string, RoomSubscription>;
  unsubscribe_rooms?: string[];

  // Extensions
  extensions?: ExtensionsRequest;
}

interface SyncListConfig {
  ranges?: [number, number][];  // MSC3575
  range?: [number, number];     // MSC4186
  sort?: string[];
  required_state?: [string, string][];
  timeline_limit?: number;
  filters?: SlidingRoomFilter;
  bump_event_types?: string[];
}

interface RoomSubscription {
  required_state?: [string, string][];
  timeline_limit?: number;
  include_old_rooms?: {
    timeline_limit?: number;
    required_state?: [string, string][];
  };
}

interface SlidingRoomFilter {
  is_dm?: boolean;
  spaces?: string[];
  is_encrypted?: boolean;
  is_invite?: boolean;
  is_tombstoned?: boolean;
  room_types?: string[];
  not_room_types?: string[];
  room_name_like?: string;
  tags?: string[];
  not_tags?: string[];
}

interface ExtensionsRequest {
  to_device?: {
    enabled?: boolean;
    since?: string;
    limit?: number;
  };
  e2ee?: {
    enabled?: boolean;
  };
  account_data?: {
    enabled?: boolean;
    lists?: string[];
    rooms?: string[];
  };
  typing?: {
    enabled?: boolean;
    lists?: string[];
    rooms?: string[];
  };
  receipts?: {
    enabled?: boolean;
    lists?: string[];
    rooms?: string[];
  };
  presence?: {
    enabled?: boolean;
  };
}

interface SlidingSyncResponse {
  pos: string;
  txn_id?: string;
  lists: Record<string, SyncListResult>;
  rooms: Record<string, RoomResult>;
  extensions: ExtensionsResponse;
  delta_token?: string;
}

interface SyncListResult {
  count: number;
  ops?: RoomListOperation[];
}

interface RoomListOperation {
  op: 'SYNC' | 'DELETE' | 'INSERT' | 'INVALIDATE';
  range?: [number, number];
  index?: number;
  room_ids?: string[];
  room_id?: string;
}

interface RoomResult {
  name?: string;
  avatar?: string;
  topic?: string;
  canonical_alias?: string;
  heroes?: StrippedHero[];
  initial?: boolean;
  unstable_expanded_timeline?: boolean;
  expanded_timeline?: boolean;
  required_state?: any[];
  timeline?: any[];
  prev_batch?: string;
  limited?: boolean;
  joined_count?: number;
  invited_count?: number;
  notification_count?: number;
  highlight_count?: number;
  num_live?: number;
  timestamp?: number;
  bump_stamp?: number;
  is_dm?: boolean;
  invite_state?: any[];
  knock_state?: any[];
  membership?: string;  // MSC4186: explicit membership status ('join', 'invite', 'knock', 'leave', 'ban')
}

interface StrippedHero {
  user_id: string;
  displayname?: string;
  avatar_url?: string;
}

interface ExtensionsResponse {
  to_device?: {
    next_batch: string;
    events: any[];
  };
  e2ee?: {
    device_lists?: {
      changed: string[];
      left: string[];
    };
    device_one_time_keys_count?: Record<string, number>;
    device_unused_fallback_key_types?: string[];
  };
  account_data?: {
    global?: any[];
    rooms?: Record<string, any[]>;
  };
  typing?: {
    rooms?: Record<string, { type: string; content: { user_ids: string[] } }>;
  };
  receipts?: {
    rooms?: Record<string, any>;
  };
  presence?: {
    events?: any[];
  };
}

// Connection state stored in Durable Object (previously KV, migrated to avoid rate limits)
interface ConnectionState {
  userId: string;
  pos: number;  // Actual stream_ordering from database
  lastAccess: number;
  roomStates: Record<string, {
    lastStreamOrdering: number;  // Last stream_ordering sent for this room
    sentState: boolean;
    timelineLimit?: number;
    historyVersion?: number;
    requiredState?: [string, string][];
    membership?: string;
  }>;
  listStates: Record<string, {
    roomIds: string[];
    count: number;
  }>;
  // Track last-sent notification counts to detect changes even without new timeline events
  roomNotificationCounts?: Record<string, number>;
  // Track last-sent m.fully_read event IDs to detect when user marks as read
  roomFullyReadMarkers?: Record<string, string>;
  // Track if initial sync has been completed to prevent ephemeral spam on reconnects
  initialSyncComplete?: boolean;
  // Track rooms we've sent as "read" (notification_count = 0) to avoid resending
  roomSentAsRead?: Record<string, boolean>;
}

// Helper to get the current maximum stream ordering from the database
async function getCurrentStreamPosition(db: D1Database): Promise<number> {
  const result = await db.prepare(
    `SELECT MAX(stream_ordering) as max_pos FROM events`
  ).first<{ max_pos: number | null }>();
  return result?.max_pos ?? 0;
}

// Helper to get or create connection state using Durable Object (not KV - avoids rate limits)
async function getConnectionState(
  syncDO: DurableObjectNamespace,
  userId: string,
  connId: string
): Promise<ConnectionState | null> {
  // Use userId as the DO ID so each user has their own DO instance
  const doId = syncDO.idFromName(userId);
  const stub = syncDO.get(doId);

  try {
    const response = await stub.fetch(
      new URL(`http://internal/sliding-sync/state?conn_id=${encodeURIComponent(connId)}`),
      { method: 'GET' }
    );

    if (!response.ok) {
      // DO returned error (400 for bad params, 500 for internal error)
      // Throw so caller knows DO is unavailable vs state not found
      const errorText = await response.text().catch(() => 'unknown error');
      throw new Error(`DO fetch failed: ${response.status} - ${errorText}`);
    }

    // 200 with null body means state not found (this is expected for new connections)
    // 200 with state object means found
    const data = await response.json();
    return data as ConnectionState | null;
  } catch (error) {
    // Log but rethrow - caller should handle DO unavailability
    console.error('[sliding-sync] Failed to get connection state from DO:', error);
    throw error;
  }
}

async function saveConnectionState(
  syncDO: DurableObjectNamespace,
  userId: string,
  connId: string,
  state: ConnectionState
): Promise<void> {
  // Use userId as the DO ID so each user has their own DO instance
  const doId = syncDO.idFromName(userId);
  const stub = syncDO.get(doId);

  try {
    const response = await stub.fetch(
      new URL(`http://internal/sliding-sync/state?conn_id=${encodeURIComponent(connId)}`),
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(state),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`DO save failed: ${response.status} - ${errorText}`);
    }
  } catch (error) {
    // Log but don't throw - state can be rebuilt on next sync
    console.error('[sliding-sync] Failed to save connection state to DO:', error);
  }
}

// Get rooms for a user with optional filtering
// OPTIMIZED: Uses consolidated query with subqueries to avoid N+1 problem
async function getUserRooms(
  db: D1Database,
  userId: string,
  filters?: SlidingRoomFilter,
  sort?: string[]
): Promise<{ roomId: string; membership: string; lastActivity: number; name?: string; isDm: boolean }[]> {
  // Consolidated query with subqueries for room name and member count
  // This eliminates N+1 queries by fetching all data in a single query
  let query = `
    SELECT
      rm.room_id,
      rm.membership,
      COALESCE(
        (SELECT MAX(origin_server_ts) FROM events WHERE room_id = rm.room_id),
        r.created_at
      ) as last_activity,
      -- Subquery for room name (JSON_EXTRACT is SQLite function)
      (SELECT JSON_EXTRACT(e.content, '$.name')
       FROM room_state rs
       JOIN events e ON rs.event_id = e.event_id
       WHERE rs.room_id = rm.room_id AND rs.event_type = 'm.room.name'
       LIMIT 1
      ) as room_name,
      -- Subquery for member count (for DM detection)
      (SELECT COUNT(*)
       FROM room_memberships rm2
       WHERE rm2.room_id = rm.room_id AND rm2.membership = 'join'
      ) as member_count
    FROM room_memberships rm
    JOIN rooms r ON rm.room_id = r.room_id
    WHERE rm.user_id = ?
  `;
  const params: any[] = [userId];

  // Apply filters
  if (filters?.is_invite) {
    query += ` AND rm.membership = 'invite'`;
  } else if (filters?.is_tombstoned) {
    // Check for tombstone state
    query += ` AND EXISTS (SELECT 1 FROM room_state rs JOIN events e ON rs.event_id = e.event_id WHERE rs.room_id = rm.room_id AND rs.event_type = 'm.room.tombstone')`;
  } else {
    // By default, only return rooms the user has joined or been invited to
    query += ` AND rm.membership IN ('join', 'invite')`;
  }

  // Default sort: by recency
  const sortBy = sort || ['by_recency'];
  if (sortBy.includes('by_recency')) {
    query += ` ORDER BY last_activity DESC`;
  } else if (sortBy.includes('by_name')) {
    query += ` ORDER BY COALESCE(room_name, rm.room_id) ASC`;
  } else {
    query += ` ORDER BY last_activity DESC`;
  }

  const result = await db.prepare(query).bind(...params).all();

  const rooms: { roomId: string; membership: string; lastActivity: number; name?: string; isDm: boolean }[] = [];

  for (const row of result.results as any[]) {
    const name = row.room_name as string | null | undefined;
    const memberCount = row.member_count as number;

    // A DM is typically a room with 2 members and no explicit name
    const isDm = memberCount <= 2 && !name;

    // Apply filters in memory (already have all data)
    if (filters?.room_name_like && name) {
      if (!name.toLowerCase().includes(filters.room_name_like.toLowerCase())) {
        continue;
      }
    }

    if (filters?.is_dm !== undefined) {
      if (filters.is_dm && !isDm) continue;
      if (!filters.is_dm && isDm) continue;
    }

    rooms.push({
      roomId: row.room_id,
      membership: row.membership,
      lastActivity: row.last_activity,
      name: name || undefined,
      isDm,
    });
  }

  return rooms;
}

// Get room data for response
// OPTIMIZED: Uses DB.batch() to fetch all room metadata in a single network call
async function getRoomData(
  env: Env,
  roomId: string,
  userId: string,
  config: {
    requiredState?: [string, string][];
    timelineLimit?: number;
    initial?: boolean;
    sinceStreamOrdering?: number;  // Only return events after this stream position
    liveAfter?: number;  // Count only new events when expanding a timeline
  }
): Promise<RoomResult & { maxStreamOrdering?: number }> {
  const db = env.DB;
  const result: RoomResult & { maxStreamOrdering?: number } = {
    membership: 'join',  // MSC4186: explicitly indicate this is a joined room
  };

  // OPTIMIZATION: Batch all metadata queries into a single network call
  // This reduces 8+ sequential queries to 1 batched call
  const [
    roomResult,
    nameResult,
    avatarResult,
    topicResult,
    aliasResult,
    joinedCountResult,
    invitedCountResult,
    heroesResult,
  ] = await db.batch([
    // 1. Room info
    db.prepare(`SELECT room_id, created_at FROM rooms WHERE room_id = ?`).bind(roomId),
    // 2. Room name
    db.prepare(`
      SELECT e.content FROM room_state rs
      JOIN events e ON rs.event_id = e.event_id
      WHERE rs.room_id = ? AND rs.event_type = 'm.room.name'
    `).bind(roomId),
    // 3. Room avatar
    db.prepare(`
      SELECT e.content FROM room_state rs
      JOIN events e ON rs.event_id = e.event_id
      WHERE rs.room_id = ? AND rs.event_type = 'm.room.avatar'
    `).bind(roomId),
    // 4. Room topic
    db.prepare(`
      SELECT e.content FROM room_state rs
      JOIN events e ON rs.event_id = e.event_id
      WHERE rs.room_id = ? AND rs.event_type = 'm.room.topic'
    `).bind(roomId),
    // 5. Canonical alias
    db.prepare(`
      SELECT e.content FROM room_state rs
      JOIN events e ON rs.event_id = e.event_id
      WHERE rs.room_id = ? AND rs.event_type = 'm.room.canonical_alias'
    `).bind(roomId),
    // 6. Joined member count
    db.prepare(`SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'join'`).bind(roomId),
    // 7. Invited member count
    db.prepare(`SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'invite'`).bind(roomId),
    // 8. Heroes (other members for display)
    db.prepare(`
      SELECT user_id, display_name, avatar_url
      FROM room_memberships
      WHERE room_id = ? AND membership = 'join' AND user_id != ?
      LIMIT 5
    `).bind(roomId, userId),
  ]);

  // Check if room exists
  const room = roomResult.results[0] as { room_id: string; created_at: number } | undefined;
  if (!room) {
    return result;
  }

  // Process batched results
  result.initial = config.initial;

  // Member counts
  const joinedCount = (joinedCountResult.results[0] as { count: number } | undefined)?.count || 0;
  const invitedCount = (invitedCountResult.results[0] as { count: number } | undefined)?.count || 0;
  result.joined_count = joinedCount;
  result.invited_count = invitedCount;
  result.is_dm = joinedCount <= 2;

  // Room name
  const nameEvent = nameResult.results[0] as { content: string } | undefined;
  if (nameEvent) {
    try {
      result.name = JSON.parse(nameEvent.content).name;
    } catch { /* ignore */ }
  }

  // Room avatar
  const avatarEvent = avatarResult.results[0] as { content: string } | undefined;
  if (avatarEvent) {
    try {
      result.avatar = JSON.parse(avatarEvent.content).url;
    } catch { /* ignore */ }
  }

  // Room topic
  const topicEvent = topicResult.results[0] as { content: string } | undefined;
  if (topicEvent) {
    try {
      result.topic = JSON.parse(topicEvent.content).topic;
    } catch { /* ignore */ }
  }

  // Canonical alias
  const aliasEvent = aliasResult.results[0] as { content: string } | undefined;
  if (aliasEvent) {
    try {
      result.canonical_alias = JSON.parse(aliasEvent.content).alias;
    } catch { /* ignore */ }
  }

  // Heroes (only used when room has no name)
  if (!result.name) {
    result.heroes = (heroesResult.results as any[]).map(h => ({
      user_id: h.user_id,
      displayname: h.display_name,
      avatar_url: h.avatar_url,
    }));
  }

  // Get required state
  if (config.requiredState && config.requiredState.length > 0) {
    result.required_state = [];

    for (const [eventType, stateKey] of config.requiredState) {
      let stateQuery = `
        SELECT e.event_id, e.event_type, e.state_key, e.content, e.sender, e.origin_server_ts, e.unsigned
        FROM room_state rs
        JOIN events e ON rs.event_id = e.event_id
        WHERE rs.room_id = ?
      `;
      const stateParams: any[] = [roomId];

      if (eventType !== '*') {
        stateQuery += ` AND rs.event_type = ?`;
        stateParams.push(eventType);
      }

      if (stateKey !== '*' && stateKey !== '') {
        stateQuery += ` AND rs.state_key = ?`;
        // Handle $ME placeholder - replace with actual user ID
        const resolvedStateKey = stateKey === '$ME' ? userId : stateKey;
        stateParams.push(resolvedStateKey);
      } else if (stateKey === '') {
        stateQuery += ` AND rs.state_key = ''`;
      }

      const stateEvents = await db.prepare(stateQuery).bind(...stateParams).all();

      for (const event of stateEvents.results as any[]) {
        try {
          result.required_state.push({
            type: event.event_type,
            state_key: event.state_key,
            content: JSON.parse(event.content),
            sender: event.sender,
            origin_server_ts: event.origin_server_ts,
            event_id: event.event_id,
            unsigned: event.unsigned ? JSON.parse(event.unsigned) : undefined,
          });
        } catch { /* ignore parse errors */ }
      }
    }
  }

  // Get timeline
  if (config.timelineLimit && config.timelineLimit > 0) {
    let timelineQuery: string;
    let timelineParams: (string | number)[];
    const isIncremental = config.sinceStreamOrdering !== undefined;

    // For incremental sync (sinceStreamOrdering provided), only get new events
    // For initial sync, get the last N events
    // Fetch one extra event to determine if there are more events than the limit
    const fetchLimit = config.timelineLimit + 1;

    if (isIncremental) {
      // Incremental: get events since the last sync position
      timelineQuery = `
        SELECT event_id, event_type, state_key, content, sender, origin_server_ts, unsigned, depth, stream_ordering, prev_events
        FROM events
        WHERE room_id = ? AND stream_ordering > ?
        ORDER BY stream_ordering ASC
        LIMIT ?
      `;
      timelineParams = [roomId, config.sinceStreamOrdering!, fetchLimit];
    } else {
      // Initial: get the most recent events
      timelineQuery = `
        SELECT event_id, event_type, state_key, content, sender, origin_server_ts, unsigned, depth, stream_ordering, prev_events
        FROM events
        WHERE room_id = ? AND stream_ordering IS NOT NULL
        ORDER BY stream_ordering DESC
        LIMIT ?
      `;
      timelineParams = [roomId, fetchLimit];
    }

    const timelineEvents = await db.prepare(timelineQuery).bind(...timelineParams).all();

    // Check if there are more events than the limit
    const hasMoreEvents = timelineEvents.results.length > config.timelineLimit;

    // Only use up to timelineLimit events
    const eventsToUse = timelineEvents.results.slice(0, config.timelineLimit) as any[];

    // For initial sync, reverse to get chronological order
    const eventsToProcess = isIncremental ? eventsToUse : eventsToUse.reverse();

    result.timeline = eventsToProcess.map(event => {
      try {
        return {
          type: event.event_type,
          event_id: event.event_id,
          sender: event.sender,
          origin_server_ts: event.origin_server_ts,
          content: JSON.parse(event.content),
          state_key: event.state_key ?? undefined,
          unsigned: event.unsigned ? JSON.parse(event.unsigned) : undefined,
        };
      } catch {
        return {
          type: event.event_type,
          event_id: event.event_id,
          sender: event.sender,
          origin_server_ts: event.origin_server_ts,
          content: {},
          state_key: event.state_key ?? undefined,
        };
      }
    });

    // Track the max stream_ordering we're sending
    if (eventsToProcess.length > 0) {
      const maxEvent = eventsToProcess[eventsToProcess.length - 1];
      result.maxStreamOrdering = maxEvent.stream_ordering;
    }

    // Set num_live for incremental syncs (tells client how many new events)
    result.num_live = config.initial ? 0 : eventsToProcess.filter(event =>
      event.stream_ordering > (config.liveAfter ?? config.sinceStreamOrdering ?? 0)).length;

    // Get prev_batch for pagination (only useful for initial sync really)
    if (eventsToProcess.length > 0) {
      const oldestEvent = eventsToProcess[0];
      result.prev_batch = `s${oldestEvent.stream_ordering || oldestEvent.depth}`;
    }

    // limited: true means there are more events than what was returned
    // For incremental syncs: only true if there are actually more new events
    // For initial syncs: true if there are more historical events
    result.limited = hasMoreEvents;
    if (!isIncremental && !hasMoreEvents && eventsToProcess.length) {
      const history = await initialRoomHistory(env, roomId, userId, eventsToProcess[0].event_id,
        config.timelineLimit - result.timeline.length);
      result.timeline.unshift(...history.events.map(event => ({
        type: event.type, event_id: event.event_id, sender: event.sender,
        origin_server_ts: event.origin_server_ts, content: event.content,
        state_key: event.state_key, unsigned: event.unsigned,
      })));
      result.limited = history.limited;
      if (history.fetched) result.prev_batch = history.cursor;
    }
  }

  // Get notification and highlight counts using push rule evaluation
  const counts = await countNotificationsWithRules(db, userId, roomId);
  result.notification_count = counts.notification_count;
  result.highlight_count = counts.highlight_count;

  // Get last activity timestamp
  const lastEvent = await db.prepare(`
    SELECT MAX(origin_server_ts) as ts FROM events WHERE room_id = ?
  `).bind(roomId).first<{ ts: number }>();

  if (lastEvent?.ts) {
    result.bump_stamp = lastEvent.ts;
    result.timestamp = lastEvent.ts;
  }

  return result;
}

// Get stripped invite state for invited rooms
// Per Matrix spec, invited rooms only see limited "stripped state"
async function getInviteRoomData(
  db: D1Database,
  roomId: string,
  userId: string
): Promise<RoomResult> {
  const result: RoomResult = {
    initial: true,
    membership: 'invite',  // MSC4186: explicitly indicate this is an invited room
  };

  // Get stripped state events for invited users
  // These are the key events that help the user understand what they're invited to
  const strippedStateTypes = [
    'm.room.create',
    'm.room.name',
    'm.room.avatar',
    'm.room.topic',
    'm.room.canonical_alias',
    'm.room.encryption',
    'm.room.member',  // Only for inviter and invitee
  ];

  const inviteState: any[] = [];

  for (const eventType of strippedStateTypes) {
    let query = `
      SELECT e.event_type, e.state_key, e.content, e.sender
      FROM room_state rs
      JOIN events e ON rs.event_id = e.event_id
      WHERE rs.room_id = ? AND rs.event_type = ?
    `;
    const params: any[] = [roomId, eventType];

    // For member events, only include the invitee's own membership
    if (eventType === 'm.room.member') {
      query += ` AND rs.state_key = ?`;
      params.push(userId);
    }

    const events = await db.prepare(query).bind(...params).all();

    for (const event of events.results as any[]) {
      try {
        inviteState.push({
          type: event.event_type,
          state_key: event.state_key,
          content: JSON.parse(event.content),
          sender: event.sender,
        });
      } catch { /* ignore parse errors */ }
    }
  }

  const storedInvite = await getStoredInviteState(db, roomId, userId);
  if (storedInvite) inviteState.splice(0, inviteState.length, ...storedInvite);
  result.invite_state = inviteState;

  // Extract name from state if available
  const nameEvent = inviteState.find(e => e.type === 'm.room.name');
  if (nameEvent?.content?.name) {
    result.name = nameEvent.content.name;
  }

  // Extract avatar from state if available
  const avatarEvent = inviteState.find(e => e.type === 'm.room.avatar');
  if (avatarEvent?.content?.url) {
    result.avatar = avatarEvent.content.url;
  }

  // Get member counts
  const joinedCount = await db.prepare(`
    SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'join'
  `).bind(roomId).first<{ count: number }>();

  const invitedCount = await db.prepare(`
    SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'invite'
  `).bind(roomId).first<{ count: number }>();

  result.joined_count = joinedCount?.count || 0;
  result.invited_count = invitedCount?.count || 0;

  return result;
}

// MSC3575 Sliding Sync endpoint
app.post('/_matrix/client/unstable/org.matrix.msc3575/sync', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const db = c.env.DB;
  const syncDO = c.env.SYNC;  // Use Durable Object for connection state (not KV - avoids rate limits)
  const cache = c.env.CACHE;  // KV for presence lookups (read-only, no rate limit issues)

  let body: SlidingSyncRequest;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const connId = body.conn_id || 'default';
  // Note: timeout is parsed but not used yet (for future long-polling support)
  const _ = Math.min(body.timeout || 0, 30000); void _;

  // Get current stream position from database
  const currentStreamPos = await getCurrentStreamPosition(db);
  const currentKeys = await deviceKeyPosition(db);

  // Get or create connection state
  let connectionState: ConnectionState | null;
  try {
    connectionState = await getConnectionState(syncDO, userId, connId);
  } catch (error) {
    // DO unavailable - return error so client knows to retry
    console.error('[sliding-sync MSC3575] DO unavailable:', error);
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Sync service temporarily unavailable',
    }, 503);
  }

  // IMPORTANT: pos can be in query string OR body - check both
  const queryPos = c.req.query('pos');
  const posToken = queryPos || body.pos;
  const sincePos = posToken ? parseInt(posToken) : 0;
  // Note: isInitialSync is computed but not currently used (for future diagnostics)
  void (!posToken || !connectionState);

  // If client sends a pos but we don't have connection state, check if the pos
  // is a valid stream position (could be from before a deployment or KV expiry)
  if (posToken && !connectionState) {
    if (sincePos <= currentStreamPos) {
      // Valid position, create fresh connection state treating it as a reconnect
      console.log('[sliding-sync MSC3575] Reconnecting with valid pos', sincePos, 'current:', currentStreamPos);
      connectionState = {
        userId,
        pos: sincePos,
        lastAccess: Date.now(),
        roomStates: {},
        listStates: {},
      };
    } else {
      // Position is in the future - invalid
      return c.json({
        errcode: 'M_UNKNOWN_POS',
        error: 'Unknown position token',
      }, 400);
    }
  }

  if (!connectionState) {
    connectionState = {
      userId,
      pos: 0,
      lastAccess: Date.now(),
      roomStates: {},
      listStates: {},
    };
  }

  connectionState.pos = currentStreamPos;
  connectionState.lastAccess = Date.now();

  const response: SlidingSyncResponse = {
    pos: `${currentStreamPos}_dk${currentKeys}`,
    lists: {},
    rooms: {},
    extensions: {},
  };

  if (body.txn_id) {
    response.txn_id = body.txn_id;
  }

  // Process lists
  if (body.lists) {
    for (const [listKey, listConfig] of Object.entries(body.lists)) {
      const rooms = await getUserRooms(db, userId, listConfig.filters, listConfig.sort);

      // Determine range to return
      let startIndex = 0;
      let endIndex = rooms.length - 1;

      // MSC3575 uses ranges array
      if (listConfig.ranges && listConfig.ranges.length > 0) {
        startIndex = listConfig.ranges[0][0];
        endIndex = Math.min(listConfig.ranges[0][1], rooms.length - 1);
      }
      // MSC4186 uses single range
      else if (listConfig.range) {
        startIndex = listConfig.range[0];
        endIndex = Math.min(listConfig.range[1], rooms.length - 1);
      }

      const roomsInRange = rooms.slice(startIndex, endIndex + 1);
      const roomIds = roomsInRange.map(r => r.roomId);

      // Check if the list has changed since last sync
      const previousListState = connectionState.listStates[listKey];
      const listChanged = !previousListState ||
        previousListState.count !== rooms.length ||
        JSON.stringify(previousListState.roomIds) !== JSON.stringify(roomIds);

      // Only include ops if the list changed (or it's an initial sync)
      if (listChanged) {
        response.lists[listKey] = {
          count: rooms.length,
          ops: [{
            op: 'SYNC',
            range: [startIndex, endIndex],
            room_ids: roomIds,
          }],
        };
      } else {
        // List unchanged - just report count with no ops
        response.lists[listKey] = {
          count: rooms.length,
        };
      }

      // Get room data for rooms in range
      for (const roomInfo of roomsInRange) {
        const roomState = connectionState.roomStates[roomInfo.roomId];
        const isInitialRoom = !roomState?.sentState;
        const roomSincePos = isInitialRoom ? 0 : (roomState?.lastStreamOrdering || 0);

        // Handle invited rooms differently - they get invite_state not timeline
        // Always include invited room data (small payload) so client doesn't lose invites on reconnect
        if (roomInfo.membership === 'invite') {
          const roomData = await getInviteRoomData(db, roomInfo.roomId, userId);
          response.rooms[roomInfo.roomId] = roomData;
          connectionState.roomStates[roomInfo.roomId] = {
            sentState: true,
            lastStreamOrdering: roomSincePos,
          };
          continue;
        }

        // For joined rooms, get full room data
        const roomData = await getRoomData(c.env, roomInfo.roomId, userId, {
          requiredState: listConfig.required_state,
          timelineLimit: listConfig.timeline_limit || 10,
          initial: isInitialRoom,
          sinceStreamOrdering: isInitialRoom ? undefined : roomSincePos,
        });

        // Check if notification count changed (for marking rooms as read)
        const hasPrevCount = roomInfo.roomId in (connectionState.roomNotificationCounts || {});
        const prevNotificationCount = connectionState.roomNotificationCounts?.[roomInfo.roomId] ?? 0;
        const currentNotificationCount = roomData.notification_count ?? 0;
        const notificationCountChanged = hasPrevCount && currentNotificationCount !== prevNotificationCount;

        // Check if m.fully_read marker changed
        const fullyReadResult = await db.prepare(`
          SELECT content FROM account_data
          WHERE user_id = ? AND room_id = ? AND event_type = 'm.fully_read'
        `).bind(userId, roomInfo.roomId).first<{ content: string }>();
        let currentFullyRead = '';
        if (fullyReadResult) {
          try {
            currentFullyRead = JSON.parse(fullyReadResult.content).event_id || '';
          } catch { /* ignore */ }
        }
        const prevFullyRead = connectionState.roomFullyReadMarkers?.[roomInfo.roomId] ?? '';
        const fullyReadChanged = currentFullyRead !== prevFullyRead && currentFullyRead !== '';

        // Track if this is the first time we're sending this room as "read" (notification_count = 0)
        // This ensures Element X receives the room with 0 unread count at least once
        const firstTimeRead = currentNotificationCount === 0
          && !connectionState.roomSentAsRead?.[roomInfo.roomId];

        // Include room if it's initial, has new events, notification count changed, fully_read changed, OR first time read
        if (isInitialRoom || (roomData.timeline && roomData.timeline.length > 0) || notificationCountChanged || fullyReadChanged || firstTimeRead) {
          response.rooms[roomInfo.roomId] = roomData;

          // Update tracked state
          connectionState.roomNotificationCounts = connectionState.roomNotificationCounts || {};
          connectionState.roomNotificationCounts[roomInfo.roomId] = currentNotificationCount;
          connectionState.roomFullyReadMarkers = connectionState.roomFullyReadMarkers || {};
          connectionState.roomFullyReadMarkers[roomInfo.roomId] = currentFullyRead;

          // Track room read status - set when read, clear when unread
          connectionState.roomSentAsRead = connectionState.roomSentAsRead || {};
          if (currentNotificationCount === 0) {
            connectionState.roomSentAsRead[roomInfo.roomId] = true;
          } else {
            // Clear flag when there are unread messages so room will be included again when read
            delete connectionState.roomSentAsRead[roomInfo.roomId];
          }
        }

        // Mark as sent with stream ordering tracking
        const newStreamOrdering = roomData.maxStreamOrdering || roomSincePos;
        connectionState.roomStates[roomInfo.roomId] = {
          sentState: true,
          lastStreamOrdering: newStreamOrdering,
        };
      }

      // Save list state
      connectionState.listStates[listKey] = {
        roomIds,
        count: rooms.length,
      };
    }
  }

  // Process room subscriptions
  if (body.room_subscriptions) {
    for (const [roomId, subscription] of Object.entries(body.room_subscriptions)) {
      // Check if user has access to this room
      const membershipResult = await db.prepare(`
        SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?
      `).bind(roomId, userId).first<{ membership: string }>();

      if (!membershipResult || !['join', 'invite'].includes(membershipResult.membership)) {
        continue; // Skip rooms user isn't in
      }

      const roomState = connectionState.roomStates[roomId];
      const isInitialRoom = !roomState?.sentState;
      const roomSincePos = isInitialRoom ? 0 : (roomState?.lastStreamOrdering || 0);

      // Handle invited rooms differently - they get invite_state not timeline
      // Always include invited room data (small payload) so client doesn't lose invites on reconnect
      if (membershipResult.membership === 'invite') {
        const roomData = await getInviteRoomData(db, roomId, userId);
        response.rooms[roomId] = roomData;
        connectionState.roomStates[roomId] = {
          sentState: true,
          lastStreamOrdering: roomSincePos,
        };
        continue;
      }

      // For joined rooms, get full room data
      const roomData = await getRoomData(c.env, roomId, userId, {
        requiredState: subscription.required_state,
        timelineLimit: subscription.timeline_limit || 10,
        initial: isInitialRoom,
        sinceStreamOrdering: isInitialRoom ? undefined : roomSincePos,
      });

      // Check if notification count changed (for marking rooms as read)
      const hasPrevCount = roomId in (connectionState.roomNotificationCounts || {});
      const prevNotificationCount = connectionState.roomNotificationCounts?.[roomId] ?? 0;
      const currentNotificationCount = roomData.notification_count ?? 0;
      const notificationCountChanged = hasPrevCount && currentNotificationCount !== prevNotificationCount;

      // Check if m.fully_read marker changed
      const fullyReadResult = await db.prepare(`
        SELECT content FROM account_data
        WHERE user_id = ? AND room_id = ? AND event_type = 'm.fully_read'
      `).bind(userId, roomId).first<{ content: string }>();
      let currentFullyRead = '';
      if (fullyReadResult) {
        try {
          currentFullyRead = JSON.parse(fullyReadResult.content).event_id || '';
        } catch { /* ignore */ }
      }
      const prevFullyRead = connectionState.roomFullyReadMarkers?.[roomId] ?? '';
      const fullyReadChanged = currentFullyRead !== prevFullyRead && currentFullyRead !== '';

      // Track if this is the first time we're sending this room as "read" (notification_count = 0)
      const firstTimeRead = currentNotificationCount === 0
        && !connectionState.roomSentAsRead?.[roomId];

      // Include room if it's initial, has new events, notification count changed, fully_read changed, OR first time read
      if (isInitialRoom || (roomData.timeline && roomData.timeline.length > 0) || notificationCountChanged || fullyReadChanged || firstTimeRead) {
        response.rooms[roomId] = roomData;

        // Update tracked state
        connectionState.roomNotificationCounts = connectionState.roomNotificationCounts || {};
        connectionState.roomNotificationCounts[roomId] = currentNotificationCount;
        connectionState.roomFullyReadMarkers = connectionState.roomFullyReadMarkers || {};
        connectionState.roomFullyReadMarkers[roomId] = currentFullyRead;

        // Track room read status - set when read, clear when unread
        connectionState.roomSentAsRead = connectionState.roomSentAsRead || {};
        if (currentNotificationCount === 0) {
          connectionState.roomSentAsRead[roomId] = true;
        } else {
          // Clear flag when there are unread messages so room will be included again when read
          delete connectionState.roomSentAsRead[roomId];
        }
      }

      const newStreamOrdering = roomData.maxStreamOrdering || roomSincePos;
      connectionState.roomStates[roomId] = {
        sentState: true,
        lastStreamOrdering: newStreamOrdering,
      };
    }
  }

  // Handle unsubscriptions
  if (body.unsubscribe_rooms) {
    for (const roomId of body.unsubscribe_rooms) {
      delete connectionState.roomStates[roomId];
    }
  }

  // Process extensions
  if (body.extensions) {
    const extensionKeys = Object.keys(body.extensions);
    console.log('[sliding-sync] Extensions requested:', extensionKeys, 'by user:', userId);

    // To-device messages
    // to_device: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.to_device) {
      const limit = body.extensions.to_device.limit || 100;
      const deviceId = c.get('deviceId');

      // Get to-device messages from D1 database (properly stored per-device)
      const { getToDeviceMessages } = await import('./to-device');
      const { events, nextBatch } = await getToDeviceMessages(
        db,
        userId,
        deviceId || '',
        body.extensions.to_device.since,
        limit
      );

      response.extensions.to_device = {
        next_batch: nextBatch,
        events,
      };
    }

    // E2EE extension
    // e2ee: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.e2ee) {
      const deviceId = c.get('deviceId');

      // Get one-time key counts from database
      const keyCounts: Record<string, number> = {};
      if (deviceId) {
        const counts = await db.prepare(`
          SELECT algorithm, COUNT(*) as count
          FROM one_time_keys
          WHERE user_id = ? AND device_id = ? AND claimed = 0
          GROUP BY algorithm
        `).bind(userId, deviceId).all();
        for (const row of counts.results as { algorithm: string; count: number }[]) {
          keyCounts[row.algorithm] = row.count;
        }
      }

      // Get unused fallback key types
      const unusedFallbackTypes: string[] = [];
      if (deviceId) {
        const fallbackKeys = await db.prepare(`
          SELECT DISTINCT algorithm
          FROM fallback_keys
          WHERE user_id = ? AND device_id = ? AND used = 0
        `).bind(userId, deviceId).all();
        unusedFallbackTypes.push(...(fallbackKeys.results as { algorithm: string }[]).map(row => row.algorithm));
      }

      const keySince = parseSyncPosition(posToken).keys;
      const deviceListChanged = await changedDeviceUsers(db, userId, keySince, currentKeys);
      if (!posToken && !deviceListChanged.includes(userId)) deviceListChanged.push(userId);

      response.extensions.e2ee = {
        device_lists: {
          changed: deviceListChanged,
          left: [],
        },
        device_one_time_keys_count: keyCounts,
        device_unused_fallback_key_types: unusedFallbackTypes,
      };
    }

    // Account data extension
    // account_data: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.account_data) {
      // Get global account data from D1
      const globalData = await db.prepare(`
        SELECT event_type, content FROM account_data
        WHERE user_id = ? AND room_id = ''
      `).bind(userId).all();

      // Build map of D1 account data
      const globalAccountData: Record<string, any> = {};
      for (const d of globalData.results as any[]) {
        try {
          globalAccountData[d.event_type] = JSON.parse(d.content);
        } catch {
          // Malformed JSON in account_data - use empty object
          globalAccountData[d.event_type] = {};
          console.warn('[sliding-sync MSC3575] Failed to parse account data:', d.event_type);
        }
      }

      // CRITICAL: Get E2EE account data from Durable Object (strongly consistent)
      // This ensures SSSS data is immediately visible after being written
      const { getE2EEAccountDataFromDO } = await import('./account-data');
      try {
        const e2eeData = await getE2EEAccountDataFromDO(c.env, userId);
        // Merge E2EE data (Durable Object takes precedence for consistency)
        for (const [eventType, content] of Object.entries(e2eeData || {})) {
          globalAccountData[eventType] = content;
        }
      } catch (error) {
        console.error('[sliding-sync MSC3575] Failed to get E2EE account data from DO:', error);
        // Continue with D1 data only - DO unavailable
      }

      response.extensions.account_data = {
        global: Object.entries(globalAccountData).map(([type, content]) => ({
          type,
          content,
        })),
        rooms: {},
      };

      // Get room account data for specified rooms
      const roomsToCheck = body.extensions.account_data.rooms || Object.keys(response.rooms);
      for (const roomId of roomsToCheck) {
        const roomData = await db.prepare(`
          SELECT event_type, content FROM account_data
          WHERE user_id = ? AND room_id = ?
        `).bind(userId, roomId).all();

        if (roomData.results.length > 0) {
          response.extensions.account_data.rooms![roomId] = (roomData.results as any[]).map(d => {
            try {
              return { type: d.event_type, content: JSON.parse(d.content) };
            } catch {
              return { type: d.event_type, content: {} };
            }
          });
        }
      }
    }

    // Typing extension - uses Room Durable Objects
    // typing: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.typing) {
      const responseRoomIds = Object.keys(response.rooms);
      const subscribedRoomIds = body.room_subscriptions ? Object.keys(body.room_subscriptions) : [];
      const allRoomIds = [...new Set([...responseRoomIds, ...subscribedRoomIds])];

      if (allRoomIds.length > 0) {
        const typingByRoom = await getTypingForRooms(c.env, allRoomIds, userId);

        // Always include typing for all rooms so clients know when typing stops
        response.extensions.typing = { rooms: {} };
        for (const roomId of allRoomIds) {
          const userIds = typingByRoom[roomId] || [];
          response.extensions.typing.rooms![roomId] = {
            type: 'm.typing',
            content: { user_ids: userIds }
          };
        }
      }
    }

    // Receipts extension - uses Room Durable Objects
    // receipts: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.receipts) {
      // Collect room IDs from response AND room subscriptions
      // (room_subscriptions may include rooms not in response.rooms)
      let roomIdsToFetch = [
        ...Object.keys(response.rooms),
        ...(body.room_subscriptions ? Object.keys(body.room_subscriptions) : []),
      ];
      roomIdsToFetch = [...new Set(roomIdsToFetch)]; // dedupe

      // Pass userId to filter m.read.private receipts
      const receiptsByRoom = await getReceiptsForRooms(c.env, roomIdsToFetch, userId);

      response.extensions.receipts = { rooms: {} };
      for (const [roomId, content] of Object.entries(receiptsByRoom)) {
        response.extensions.receipts.rooms![roomId] = {
          type: 'm.receipt',
          content,
        };
      }
    }

    // Presence extension
    // presence: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.presence) {
      response.extensions.presence = { events: [] };

      // Get presence for users in the rooms
      const userIds = new Set<string>();
      for (const roomId of Object.keys(response.rooms)) {
        const members = await db.prepare(`
          SELECT user_id FROM room_memberships WHERE room_id = ? AND membership = 'join'
        `).bind(roomId).all();
        for (const member of members.results as any[]) {
          userIds.add(member.user_id);
        }
      }

      for (const uid of userIds) {
        if (uid === userId) continue;
        const presenceKey = `presence:${uid}`;
        const presence = await cache.get(presenceKey, 'json') as any;
        if (presence) {
          response.extensions.presence.events!.push({
            type: 'm.presence',
            sender: uid,
            content: presence,
          });
        }
      }
    }
  }

  // ALWAYS save connection state - using DO now (not KV), no rate limit concerns
  try {
    await saveConnectionState(syncDO, userId, connId, connectionState);
  } catch (error) {
    console.error('[sliding-sync MSC3575] Failed to save connection state:', error);
  }

  return c.json(response);
});

// Helper to detect if a request looks like it's from the iOS NSE (Notification Service Extension)
// NSE requests are typically:
// - Single room subscription (the room with the push notification)
// - Minimal or no extensions
// - User-agent may differ from main app
// - Made shortly after push notification delivery
function detectNSERequest(
  userAgent: string | undefined,
  body: SlidingSyncRequest
): { isLikelyNSE: boolean; indicators: string[] } {
  const indicators: string[] = [];

  // Check User-Agent patterns
  // Element X iOS NSE typically has "NSE" in User-Agent or different app name
  if (userAgent) {
    if (userAgent.includes('NSE') || userAgent.includes('NotificationService')) {
      indicators.push('user-agent-nse');
    }
    // Element X iOS main app pattern: "Element X iOS/..."
    // NSE might use different pattern
    if (!userAgent.includes('Element X iOS') && userAgent.includes('iOS')) {
      indicators.push('user-agent-different-ios');
    }
  }

  // Check request shape - NSE typically subscribes to single room
  const roomSubscriptions = body.room_subscriptions ? Object.keys(body.room_subscriptions) : [];
  const lists = body.lists ? Object.keys(body.lists) : [];

  if (roomSubscriptions.length === 1 && lists.length === 0) {
    indicators.push('single-room-subscription');
  }

  // Check for minimal extensions (NSE only needs room content)
  const extensionKeys = body.extensions ? Object.keys(body.extensions) : [];
  if (extensionKeys.length === 0) {
    indicators.push('no-extensions');
  } else if (extensionKeys.length <= 2 && !extensionKeys.includes('typing') && !extensionKeys.includes('presence')) {
    indicators.push('minimal-extensions');
  }

  // NSE typically requests small timeline
  if (body.room_subscriptions) {
    const subscriptions = Object.values(body.room_subscriptions);
    const allSmallTimeline = subscriptions.every(s => (s.timeline_limit || 10) <= 5);
    if (allSmallTimeline && subscriptions.length > 0) {
      indicators.push('small-timeline-limit');
    }
  }

  // Consider it likely NSE if we have 2+ indicators
  const isLikelyNSE = indicators.length >= 2;

  return { isLikelyNSE, indicators };
}

// MSC4186 Simplified Sliding Sync handler (shared between endpoints)
async function handleSimplifiedSlidingSync(c: Context<AppEnv>) {
  const userId = c.get('userId');
  const db = c.env.DB;
  const syncDO = c.env.SYNC;  // Use Durable Object for connection state (not KV - avoids rate limits)

  // Capture User-Agent for NSE detection
  const userAgent = c.req.header('User-Agent');

  let body: SlidingSyncRequest;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const connId = JSON.stringify([c.get('deviceId') ?? '', body.conn_id || 'default']);

  // NSE Detection - log potential NSE requests
  const nseDetection = detectNSERequest(userAgent, body);
  if (nseDetection.isLikelyNSE || nseDetection.indicators.length > 0) {
    console.log('[sliding-sync] POTENTIAL NSE REQUEST:', {
      userId,
      userAgent,
      isLikelyNSE: nseDetection.isLikelyNSE,
      indicators: nseDetection.indicators,
      roomSubscriptions: body.room_subscriptions ? Object.keys(body.room_subscriptions) : [],
      extensions: body.extensions ? Object.keys(body.extensions) : [],
      timestamp: new Date().toISOString(),
    });
  }

  // Parse timeout for long-polling (query string takes precedence, then body, default 0)
  const queryTimeout = c.req.query('timeout');
  const timeout = Math.min(
    queryTimeout ? parseInt(queryTimeout) : (body.timeout || 0),
    25000  // Cap at 25s to stay under Workers 30s limit
  );

  // Get current stream position from database
  const currentStreamPos = await getCurrentStreamPosition(db);
  let currentKeys = await deviceKeyPosition(db);
  let currentReceipts = await receiptPosition(db);
  let currentAccountData = body.extensions?.account_data ? await accountDataPosition(db) : 0;

  // Get or create connection state
  let connectionState: ConnectionState | null;
  try {
    connectionState = await getConnectionState(syncDO, userId, connId);
  } catch (error) {
    // DO unavailable - return error so client knows to retry
    console.error('[sliding-sync] DO unavailable:', error);
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Sync service temporarily unavailable',
    }, 503);
  }

  // IMPORTANT: pos can be in query string OR body - check both
  // Element X sends pos in query string, other clients may use body
  const queryPos = c.req.query('pos');
  const posToken = queryPos || body.pos;
  const sincePos = posToken ? parseInt(posToken) : 0;

  // Debug logging for connection state (includes user-agent for NSE debugging)
  console.log('[sliding-sync] Request:', {
    userId,
    connId,
    timeout,
    queryPos,
    bodyPos: body.pos,
    sincePos,
    currentStreamPos,
    hasConnectionState: !!connectionState,
    hasLists: !!body.lists && Object.keys(body.lists).length > 0,
    hasRoomSubscriptions: !!body.room_subscriptions && Object.keys(body.room_subscriptions).length > 0,
    hasExtensions: !!body.extensions,
    extensionKeys: body.extensions ? Object.keys(body.extensions) : [],
    userAgent: userAgent?.substring(0, 100), // Truncate for log readability
  });

  // If client sends a pos but we don't have connection state, check if the pos
  // is a valid stream position (could be from before a deployment or KV expiry)
  // If it's <= current position, treat as reconnect and rebuild state
  if (posToken && !connectionState) {
    if (sincePos <= currentStreamPos) {
      // Valid position, create fresh connection state treating it as a reconnect
      console.log('[sliding-sync] Reconnecting with valid pos', sincePos, 'current:', currentStreamPos);
      connectionState = {
        userId,
        pos: sincePos,
        lastAccess: Date.now(),
        roomStates: {},
        listStates: {},
      };
    } else {
      // Position is in the future - invalid
      return c.json({
        errcode: 'M_UNKNOWN_POS',
        error: 'Unknown position token',
      }, 400);
    }
  }

  const isInitialSync = !posToken;
  if (isInitialSync) connectionState = null;

  if (!connectionState) {
    connectionState = {
      userId,
      pos: 0,
      lastAccess: Date.now(),
      roomStates: {},
      listStates: {},
    };
  }

  // Track whether there are any actual changes to report
  let hasChanges = isInitialSync; // Initial sync always has "changes"

  // Don't update connection state position yet - only if we have changes
  connectionState.lastAccess = Date.now();

  const response: SlidingSyncResponse = {
    pos: posToken || String(currentStreamPos), // Start with input pos, update later if changes
    lists: {},
    rooms: {},
    extensions: {},
  };
  let receiptRoomIds: string[] = [];

  if (body.txn_id) {
    response.txn_id = body.txn_id;
  }

  // A room may match several lists and an explicit subscription. Merge the
  // requests first, then read/advance its cursor exactly once. Otherwise the
  // subscription overwrites the list's freshly read timeline with an empty one.
  const selected = new Map<string, { membership: string; timelineLimit: number; requiredState: [string, string][] }>();
  function selectRoom(roomId: string, membership: string, config: RoomSubscription) {
    const previous = selected.get(roomId);
    const requiredState = [...(previous?.requiredState ?? []), ...(config.required_state ?? [])];
    selected.set(roomId, {
      membership,
      timelineLimit: Math.max(previous?.timelineLimit ?? 0, Math.min(100, Math.max(0, config.timeline_limit ?? 10))),
      requiredState: [...new Map(requiredState.map(pair => [JSON.stringify(pair), pair])).values()],
    });
  }
  for (const [listKey, listConfig] of Object.entries(body.lists ?? {})) {
    const rooms = await getUserRooms(db, userId, listConfig.filters, listConfig.sort);
    const range = listConfig.range ?? listConfig.ranges?.[0] ?? [0, rooms.length - 1];
    const startIndex = Math.max(0, range[0]);
    const endIndex = Math.min(range[1], rooms.length - 1);
    const roomsInRange = rooms.slice(startIndex, endIndex + 1);
    const roomIds = roomsInRange.map(room => room.roomId);
    const previous = connectionState.listStates[listKey];
    const changed = !previous || previous.count !== rooms.length || JSON.stringify(previous.roomIds) !== JSON.stringify(roomIds);
    response.lists[listKey] = { count: rooms.length };
    if (changed) {
      hasChanges = true;
      if (endIndex >= startIndex) response.lists[listKey].ops = [{ op: 'SYNC', range: [startIndex, endIndex], room_ids: roomIds }];
    }
    for (const room of roomsInRange) selectRoom(room.roomId, room.membership, listConfig);
    connectionState.listStates[listKey] = { roomIds, count: rooms.length };
  }
  for (const [roomId, subscription] of Object.entries(body.room_subscriptions ?? {})) {
    const membership = await db.prepare('SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?')
      .bind(roomId, userId).first<{ membership: string }>();
    if (membership && ['join', 'invite'].includes(membership.membership)) selectRoom(roomId, membership.membership, subscription);
  }
  for (const [roomId, config] of selected) {
    const previous = connectionState.roomStates[roomId];
    const initial = !previous?.sentState || previous.membership === 'invite';
    const since = previous?.lastStreamOrdering ?? 0;
    if (config.membership === 'invite') {
      response.rooms[roomId] = await getInviteRoomData(db, roomId, userId); hasChanges = true;
      connectionState.roomStates[roomId] = { sentState: true, lastStreamOrdering: since, membership: 'invite' };
      continue;
    }
    const expanded = !initial && (config.timelineLimit > (previous.timelineLimit ?? 0) || previous.historyVersion !== 1);
    const stateChanged = JSON.stringify(config.requiredState) !== JSON.stringify(previous?.requiredState);
    const roomData = await getRoomData(c.env, roomId, userId, {
      requiredState: config.requiredState, timelineLimit: config.timelineLimit, initial,
      sinceStreamOrdering: initial || expanded ? undefined : since,
      liveAfter: since,
    });
    if (expanded) {
      roomData.unstable_expanded_timeline = true;
      roomData.expanded_timeline = true;
    }
    const count = roomData.notification_count ?? 0;
    const countChanged = connectionState.roomNotificationCounts?.[roomId] !== count;
    const marker = await db.prepare("SELECT content FROM account_data WHERE user_id = ? AND room_id = ? AND event_type = 'm.fully_read'")
      .bind(userId, roomId).first<{ content: string }>();
    const fullyRead = marker?.content ?? '';
    const readChanged = connectionState.roomFullyReadMarkers?.[roomId] !== fullyRead;
    if (initial || expanded || stateChanged || roomData.timeline?.length || countChanged || readChanged) {
      hasChanges = true;
      const { maxStreamOrdering: _, ...clientData } = roomData;
      response.rooms[roomId] = clientData;
    }
    (connectionState.roomNotificationCounts ??= {})[roomId] = count;
    (connectionState.roomFullyReadMarkers ??= {})[roomId] = fullyRead;
    connectionState.roomStates[roomId] = {
      sentState: true, lastStreamOrdering: roomData.maxStreamOrdering ?? since, membership: config.membership,
      timelineLimit: config.timelineLimit, requiredState: config.requiredState, historyVersion: 1,
    };
  }

  // Handle extensions
  // Note: Extensions are considered enabled if the key exists OR if enabled=true
  // Element X sends extensions without explicit enabled:true
  if (body.extensions) {
    // Log what extensions are requested (consider present = enabled for MSC4186 compatibility)
    const enabledExtensions = Object.keys(body.extensions).filter(k => {
      const ext = (body.extensions as any)[k];
      return ext !== undefined && ext !== null;
    });
    console.log('[sliding-sync] Extensions requested:', enabledExtensions);

    // to_device: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.to_device) {
      const deviceId = c.get('deviceId');
      const limit = body.extensions.to_device.limit || 100;

      // Get to-device messages from D1 database (properly stored per-device)
      const { getToDeviceMessages } = await import('./to-device');
      const { events, nextBatch } = await getToDeviceMessages(
        db,
        userId,
        deviceId || '',
        body.extensions.to_device.since,
        limit
      );

      response.extensions.to_device = {
        next_batch: nextBatch,
        events,
      };
    }

    // e2ee: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.e2ee) {
      const deviceId = c.get('deviceId');

      // Get one-time key counts from database
      const keyCounts: Record<string, number> = {};
      if (deviceId) {
        const counts = await db.prepare(`
          SELECT algorithm, COUNT(*) as count
          FROM one_time_keys
          WHERE user_id = ? AND device_id = ? AND claimed = 0
          GROUP BY algorithm
        `).bind(userId, deviceId).all();
        for (const row of counts.results as { algorithm: string; count: number }[]) {
          keyCounts[row.algorithm] = row.count;
        }
      }

      // Get unused fallback key types
      const unusedFallbackTypes: string[] = [];
      if (deviceId) {
        const fallbackKeys = await db.prepare(`
          SELECT DISTINCT algorithm
          FROM fallback_keys
          WHERE user_id = ? AND device_id = ? AND used = 0
        `).bind(userId, deviceId).all();
        unusedFallbackTypes.push(...(fallbackKeys.results as { algorithm: string }[]).map(row => row.algorithm));
      }

      const keySince = parseSyncPosition(posToken).keys;
      const deviceListChanged = await changedDeviceUsers(db, userId, keySince, currentKeys);
      if (!posToken && !deviceListChanged.includes(userId)) deviceListChanged.push(userId);

      response.extensions.e2ee = {
        device_lists: { changed: deviceListChanged, left: [] },
        device_one_time_keys_count: keyCounts,
        device_unused_fallback_key_types: unusedFallbackTypes,
      };
    }

    // account_data: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.account_data) {
      // Get global account data from D1
      const globalData = await db.prepare(`
        SELECT event_type, content FROM account_data
        WHERE user_id = ? AND room_id = ''
      `).bind(userId).all();

      // Build map of D1 account data
      const globalAccountData: Record<string, any> = {};
      for (const d of globalData.results as any[]) {
        try {
          globalAccountData[d.event_type] = JSON.parse(d.content);
        } catch {
          globalAccountData[d.event_type] = {};
        }
      }

      // CRITICAL: Get E2EE account data from Durable Object (strongly consistent)
      // This ensures SSSS data is immediately visible after being written
      const { getE2EEAccountDataFromDO } = await import('./account-data');
      try {
        const e2eeData = await getE2EEAccountDataFromDO(c.env, userId);
        // Merge E2EE data (Durable Object takes precedence for consistency)
        for (const [eventType, content] of Object.entries(e2eeData || {})) {
          globalAccountData[eventType] = content;
        }
      } catch (error) {
        console.error('[sliding-sync MSC4186] Failed to get E2EE account data from DO:', error);
        // Continue with D1 data only - DO unavailable
      }

      response.extensions.account_data = {
        global: Object.entries(globalAccountData).map(([type, content]) => ({
          type,
          content,
        })),
        rooms: {},
      };

      // Get room account data (including m.fully_read for unread counts)
      const userRooms = await db.prepare(`
        SELECT room_id FROM room_memberships WHERE user_id = ? AND membership = 'join'
      `).bind(userId).all();
      const userRoomIds = (userRooms.results as { room_id: string }[]).map(r => r.room_id);

      for (const roomId of userRoomIds) {
        const roomAccountData = await db.prepare(`
          SELECT event_type, content FROM account_data
          WHERE user_id = ? AND room_id = ?
        `).bind(userId, roomId).all();

        if (roomAccountData.results.length > 0) {
          response.extensions.account_data.rooms![roomId] = (roomAccountData.results as any[]).map(d => {
            try {
              return { type: d.event_type, content: JSON.parse(d.content) };
            } catch {
              return { type: d.event_type, content: {} };
            }
          });
        }
      }
    }

    // Handle typing extension - uses Room Durable Objects
    // Element X doesn't always request typing explicitly, so include it for rooms in response
    // typing: enabled if key exists (MSC4186 compatibility)
    const typingRequested = !!body.extensions.typing;
    const roomsInResponse = Object.keys(response.rooms);

    if (typingRequested || roomsInResponse.length > 0) {
      // Get room IDs from request - could be from rooms in response OR from explicit room subscriptions
      let responseRoomIds = roomsInResponse;
      const subscribedRoomIds = body.room_subscriptions ? Object.keys(body.room_subscriptions) : [];

      // Element X uses a separate sync connection for extensions with NO rooms
      if (typingRequested && responseRoomIds.length === 0 && subscribedRoomIds.length === 0) {
        const userRooms = await db.prepare(`
          SELECT room_id FROM room_memberships WHERE user_id = ? AND membership = 'join'
        `).bind(userId).all();
        responseRoomIds = (userRooms.results as { room_id: string }[]).map(r => r.room_id);
      }

      const allRoomIds = [...new Set([...responseRoomIds, ...subscribedRoomIds])];

      if (allRoomIds.length > 0) {
        const typingByRoom = await getTypingForRooms(c.env, allRoomIds, userId);

        // Always include typing extension with all rooms so clients know when typing stops
        response.extensions.typing = { rooms: {} };
        for (const roomId of allRoomIds) {
          const userIds = typingByRoom[roomId] || [];
          response.extensions.typing.rooms![roomId] = {
            type: 'm.typing',
            content: { user_ids: userIds }
          };
        }

        // Debug: log typing data being returned
        const typingRoomsWithUsers = Object.entries(response.extensions.typing.rooms!)
          .filter(([_, data]) => (data as any).content.user_ids.length > 0);
        if (typingRoomsWithUsers.length > 0) {
          console.log('[sliding-sync] Returning typing data:', JSON.stringify(typingRoomsWithUsers));
        }
      }
    }

    // receipts: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.receipts) {
      // Fetch receipts for rooms in response, subscriptions, or all user's rooms as fallback
      // This matches the typing extension pattern for Element X compatibility
      let roomIdsToFetch = Object.keys(response.rooms);
      const subscribedRoomIds = body.room_subscriptions ? Object.keys(body.room_subscriptions) : [];

      // If no rooms in response and no subscriptions, fetch for all user's joined rooms
      // Element X uses separate sync connections for extensions with no room context
      if (roomIdsToFetch.length === 0 && subscribedRoomIds.length === 0) {
        const userRooms = await db.prepare(`
          SELECT room_id FROM room_memberships WHERE user_id = ? AND membership = 'join'
        `).bind(userId).all();
        roomIdsToFetch = (userRooms.results as { room_id: string }[]).map(r => r.room_id);
      } else {
        roomIdsToFetch = [...new Set([...roomIdsToFetch, ...subscribedRoomIds])];
      }

      receiptRoomIds = roomIdsToFetch;
      if (roomIdsToFetch.length > 0) {
        // Pass userId to filter m.read.private receipts
        const receiptsByRoom = await getReceiptsForRooms(c.env, roomIdsToFetch, userId);

        response.extensions.receipts = { rooms: {} };
        for (const [roomId, content] of Object.entries(receiptsByRoom)) {
          response.extensions.receipts.rooms![roomId] = {
            type: 'm.receipt',
            content,
          };
        }

        // Debug: log receipts data being returned
        const roomsWithReceipts = Object.keys(receiptsByRoom).length;
        if (roomsWithReceipts > 0) {
          console.log('[sliding-sync] Returning receipts for', roomsWithReceipts, 'rooms');
        }
      } else {
        response.extensions.receipts = { rooms: {} };
      }
    }

    // presence: enabled if key exists (MSC4186) or enabled=true (MSC3575)
    if (body.extensions.presence) {
      response.extensions.presence = { events: [] };
    }
  }

  // Include ephemeral data on INITIAL sync only if extensions weren't requested
  // Running this on every sync causes spam - clients sync rapidly when they receive data
  // Element X typically requests extensions properly on incremental syncs
  // Use initialSyncComplete flag to track if we've already done initial sync for this connection,
  // since clients can reconnect with sincePos === 0 but we shouldn't spam ephemeral data again
  const needsEphemeralFallback = !connectionState.initialSyncComplete
    && (!body.extensions || Object.keys(body.extensions).length === 0);

  if (needsEphemeralFallback) {
    // Get all user's rooms for ephemeral data
    const userRoomsResult = await db.prepare(`
      SELECT room_id FROM room_memberships WHERE user_id = ? AND membership = 'join'
    `).bind(userId).all();
    const userRoomIds = (userRoomsResult.results as { room_id: string }[]).map(r => r.room_id);

    // Fallback: Include typing for all rooms - uses Room Durable Objects
    const typingByRoom = await getTypingForRooms(c.env, userRoomIds, userId);

    // Always include typing for all rooms so clients know when typing stops
    response.extensions.typing = { rooms: {} };
    for (const roomId of userRoomIds) {
      const userIds = typingByRoom[roomId] || [];
      response.extensions.typing.rooms![roomId] = {
        type: 'm.typing',
        content: { user_ids: userIds }
      };
    }

    // Fallback: Include receipts for all user's rooms - uses Room Durable Objects
    // Pass userId to filter m.read.private receipts
    receiptRoomIds = userRoomIds;
    const receiptsByRoom = await getReceiptsForRooms(c.env, userRoomIds, userId);

    response.extensions.receipts = { rooms: {} };
    for (const [roomId, content] of Object.entries(receiptsByRoom)) {
      response.extensions.receipts.rooms![roomId] = {
        type: 'm.receipt',
        content,
      };
    }

    // Fallback: Include room account_data (especially m.fully_read for unread counts)
    // Element X needs m.fully_read to calculate unread counts
    // IMPORTANT: Also ensure room is in response.rooms so Element X processes the account_data
    response.extensions.account_data = { global: [], rooms: {} };
    for (const roomId of userRoomIds) {
      const roomAccountData = await db.prepare(`
        SELECT event_type, content FROM account_data
        WHERE user_id = ? AND room_id = ?
      `).bind(userId, roomId).all();

      if (roomAccountData.results.length > 0) {
        response.extensions.account_data.rooms![roomId] = (roomAccountData.results as any[]).map(d => {
          try {
            return { type: d.event_type, content: JSON.parse(d.content) };
          } catch {
            return { type: d.event_type, content: {} };
          }
        });

      }
    }
  }

  // Always advance position to prevent client re-sync loops
  // Previously we only set pos when hasChanges, but this caused clients to receive
  // the same pos twice and immediately re-sync, thinking it was stale
  response.pos = `${currentStreamPos}_dk${currentKeys}${currentReceipts ? `_rr${currentReceipts}` : ''}${currentAccountData ? `_ad${currentAccountData}` : ''}`;
  connectionState.pos = currentStreamPos;

  // Mark initial sync as complete so ephemeral fallback doesn't run again on reconnects
  if (!connectionState.initialSyncComplete) {
    connectionState.initialSyncComplete = true;
  }

  // Debug logging for response
  console.log('[sliding-sync] Response:', { userId, responsePos: response.pos, hasChanges, timeout, willWait: !hasChanges && timeout > 0 });

  // ALWAYS save connection state - flags like initialSyncComplete must be persisted
  // We use Durable Objects now (not KV), so rate limits aren't a concern
  // Previously we only saved when hasChanges, but this caused initialSyncComplete
  // to not persist, resulting in ephemeral data spam on every request
  try {
    await saveConnectionState(syncDO, userId, connId, connectionState);
  } catch (error) {
    console.error('[sliding-sync] Failed to save connection state:', error);
    // Don't return error here - state can be rebuilt on next request
    // But client may experience duplicated ephemeral data
  }

  // Verification and encryption-key messages are changes even when no room
  // timeline changed. Never hold a ready device message for the poll timeout.
  const ownAccountDataChanged = response.extensions.account_data && currentAccountData > parseSyncPosition(posToken).accountData
    ? await db.prepare('SELECT 1 FROM account_data_changes WHERE user_id=? AND stream_position>? AND stream_position<=? LIMIT 1')
      .bind(userId, parseSyncPosition(posToken).accountData, currentAccountData).first() : null;
  hasChanges ||= (response.extensions.to_device?.events.length ?? 0) > 0 ||
    (response.extensions.e2ee?.device_lists?.changed.length ?? 0) > 0 ||
    (!!response.extensions.receipts && currentReceipts > parseSyncPosition(posToken).receipts) ||
    !!ownAccountDataChanged;

  // Long-polling: if no changes and timeout > 0, wait for events via Durable Object
  // The SyncDurableObject will wake us up when events arrive for this user
  // Per MSC3575/MSC4186, server should wait up to timeout ms for new events
  if (!hasChanges && timeout > 0) {
    console.log('[sliding-sync] No changes, waiting for events via DO, timeout:', timeout, 'ms');
    try {
      const doId = syncDO.idFromName(userId);
      const stub = syncDO.get(doId);
      const waitResponse = await stub.fetch(new Request('http://internal/wait-for-events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ timeout, userId, deviceId: body.extensions?.to_device ? c.get('deviceId') : undefined,
          toDeviceSince: body.extensions?.to_device?.since,
          receiptsSince: response.extensions.receipts ? currentReceipts : undefined,
          accountDataSince: response.extensions.account_data ? currentAccountData : undefined }),
      }));
      const waitResult = await waitResponse.json() as { hasEvents: boolean };

      if (waitResult.hasEvents) {
        console.log('[sliding-sync] Woken up early - events arrived');
        if (body.extensions?.to_device && c.get('deviceId')) {
          const result = await getToDeviceMessages(db, userId, c.get('deviceId')!, body.extensions.to_device.since,
            body.extensions.to_device.limit || 100);
          response.extensions.to_device = { events: result.events, next_batch: result.nextBatch };
        }
        if (body.extensions?.e2ee && response.extensions.e2ee) {
          currentKeys = await deviceKeyPosition(db);
          const changed = await changedDeviceUsers(db, userId, parseSyncPosition(posToken).keys, currentKeys);
          response.extensions.e2ee.device_lists = {changed, left:[]};
        }
        if (response.extensions.receipts) {
          currentReceipts = await receiptPosition(db);
          const receipts = await getReceiptsForRooms(c.env, receiptRoomIds, userId);
          response.extensions.receipts = { rooms: Object.fromEntries(Object.entries(receipts)
            .map(([roomId, content]) => [roomId, { type: 'm.receipt', content }])) };
        }
        if (response.extensions.account_data) {
          currentAccountData = await accountDataPosition(db);
          const { getGlobalAccountData, getRoomAccountData, getE2EEAccountDataFromDO } = await import('./account-data');
          const global = Object.fromEntries((await getGlobalAccountData(db, userId)).map(event => [event.type, event.content]));
          try { Object.assign(global, await getE2EEAccountDataFromDO(c.env, userId)); } catch { /* D1 remains available */ }
          const joined = await db.prepare("SELECT room_id FROM room_memberships WHERE user_id=? AND membership='join'")
            .bind(userId).all<{ room_id: string }>();
          const rooms: Record<string, { type: string; content: any }[]> = {};
          for (const { room_id: roomId } of joined.results) {
            const data = await getRoomAccountData(db, userId, roomId);
            if (data.length) rooms[roomId] = data;
          }
          response.extensions.account_data = { global: Object.entries(global).map(([type, content]) => ({ type, content })), rooms };
        }
        response.pos = `${currentStreamPos}_dk${currentKeys}${currentReceipts ? `_rr${currentReceipts}` : ''}${currentAccountData ? `_ad${currentAccountData}` : ''}`;
      } else {
        console.log('[sliding-sync] Wait timed out, no new events');
      }
    } catch (error) {
      console.error('[sliding-sync] Error waiting for events:', error);
      // Fall through and return current response on error
    }
  }

  return c.json(response);
}

// MSC4186 Simplified Sliding Sync - unstable endpoint (used by Element X)
app.post('/_matrix/client/unstable/org.matrix.simplified_msc3575/sync', requireAuth(), handleSimplifiedSlidingSync);

// MSC4186 Simplified Sliding Sync endpoint (v4)
app.post('/_matrix/client/v4/sync', requireAuth(), handleSimplifiedSlidingSync);


export default app;
