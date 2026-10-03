// Push Rule Evaluator Service
// Provides notification and highlight counting using push rule evaluation
// Extracted for use by sync and sliding-sync endpoints

import { evaluatePushRules, getUserPushRules } from '../api/push';

export { evaluatePushRules };

interface UnreadEvent {
  event_id: string;
  event_type: string;
  content: string;
  sender: string;
  room_id: string;
  state_key?: string;
}

/**
 * Count notifications and highlights for unread events in a room
 * using the user's push rules for accurate counting.
 */
export async function countNotificationsWithRules(
  db: D1Database,
  userId: string,
  roomId: string,
  sinceStreamOrdering?: number,
): Promise<{ notification_count: number; highlight_count: number }> {
  // The fully-read marker is a user-controlled anchor. Notification counts also
  // respect read receipts, including private ones, without moving that anchor.
  let readStreamOrdering = sinceStreamOrdering;

  if (readStreamOrdering === undefined) {
    const fullyReadMarker = await db.prepare(`
      SELECT content FROM account_data
      WHERE user_id = ? AND room_id = ? AND event_type = 'm.fully_read'
    `).bind(userId, roomId).first<{ content: string }>();

    if (fullyReadMarker) {
      try {
        const markerContent = JSON.parse(fullyReadMarker.content);
        const readEvent = await db.prepare(`
          SELECT stream_ordering FROM events WHERE event_id = ? AND room_id = ?
        `).bind(markerContent.event_id, roomId).first<{ stream_ordering: number | null }>();
        readStreamOrdering = readEvent?.stream_ordering ?? undefined;
      } catch { /* ignore */ }
    }
  }

  const unthreaded = await db.prepare(`SELECT MAX(e.stream_ordering) AS position
    FROM receipts r JOIN events e ON e.event_id=r.event_id AND e.room_id=r.room_id
    WHERE r.room_id=? AND r.user_id=? AND r.receipt_type IN ('m.read','m.read.private') AND r.thread_id=''`)
    .bind(roomId, userId).first<{ position: number | null }>();
  const readFloor = Math.max(readStreamOrdering ?? 0, unthreaded?.position ?? 0);

  // Apply the main/thread receipt boundary before LIMIT, otherwise 500 already
  // read main-timeline events can hide unread thread messages later in the room.
  // NULL stream positions are historical imports, rather than new notifications.
  const results = await db.prepare(`
    SELECT e.event_id, e.event_type, e.content, e.sender, e.room_id, e.state_key
    FROM events e
    WHERE e.room_id = ? AND e.sender != ? AND e.stream_ordering > ?
      AND e.event_type IN ('m.room.message', 'm.room.encrypted')
      AND e.stream_ordering > COALESCE((
        SELECT MAX(read_event.stream_ordering) FROM receipts r
        JOIN events read_event ON read_event.event_id=r.event_id AND read_event.room_id=r.room_id
        WHERE r.room_id=e.room_id AND r.user_id=? AND r.receipt_type IN ('m.read','m.read.private')
          AND r.thread_id=CASE WHEN json_valid(e.content) THEN
            CASE WHEN json_extract(e.content, '$."m.relates_to".rel_type')='m.thread'
              AND json_type(e.content, '$."m.relates_to".event_id')='text'
              THEN json_extract(e.content, '$."m.relates_to".event_id') ELSE 'main' END
            ELSE 'main' END
      ), 0)
    ORDER BY e.stream_ordering ASC
    LIMIT 500
  `).bind(roomId, userId, readFloor, userId).all<UnreadEvent>();
  const unreadEvents = results.results;

  if (unreadEvents.length === 0) {
    return { notification_count: 0, highlight_count: 0 };
  }

  // Get room member count for push rule evaluation
  const memberCount = await db.prepare(`
    SELECT COUNT(*) as count FROM room_memberships
    WHERE room_id = ? AND membership = 'join'
  `).bind(roomId).first<{ count: number }>();

  // Get user's display name for mention detection
  const user = await db.prepare(`
    SELECT display_name FROM users WHERE user_id = ?
  `).bind(userId).first<{ display_name: string | null }>();

  let notificationCount = 0;
  let highlightCount = 0;
  // One rule snapshot per count avoids a database query for every unread message.
  const rules = await getUserPushRules(db, userId);

  for (const event of unreadEvents) {
    let parsedContent: Record<string, unknown>;
    try {
      parsedContent = typeof event.content === 'string' ? JSON.parse(event.content) : event.content;
    } catch {
      parsedContent = {};
    }

    const result = await evaluatePushRules(
      db,
      userId,
      {
        type: event.event_type,
        content: parsedContent,
        sender: event.sender,
        room_id: event.room_id,
        state_key: event.state_key,
      },
      memberCount?.count || 1,
      user?.display_name || undefined,
      rules,
    );

    if (result.notify) {
      notificationCount++;
    }
    if (result.highlight) {
      highlightCount++;
    }
  }

  return { notification_count: notificationCount, highlight_count: highlightCount };
}
