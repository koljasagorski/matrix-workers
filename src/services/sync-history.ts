import type { Env } from '../types';
import { getEvent } from './database';
import { getRemoteHistory } from './room-history';

// A newly federated room can contain only our join in the live database.
// Include an authorized history page in its first timeline so clients which
// previously cached an empty history can discover the messages and a cursor.
export async function initialRoomHistory(env: Env, roomId: string, userId: string, oldestId: string, capacity: number) {
  const anchor = await getEvent(env.DB, oldestId);
  if (!anchor?.prev_events.length) return { events: [], limited: false };
  const known = await env.DB.prepare(`SELECT COUNT(*) AS count FROM events WHERE room_id = ? AND
    stream_ordering IS NOT NULL AND event_id IN (${anchor.prev_events.map(() => '?').join(',')})`)
    .bind(roomId, ...anchor.prev_events).first<{count:number}>();
  if ((known?.count ?? 0) === new Set(anchor.prev_events).size) return { events: [], limited: true };
  if (capacity <= 0) return { events: [], limited: true };
  try {
    const page = await getRemoteHistory(env, roomId, userId, anchor, Math.min(10, capacity));
    if (!page.events.length && !page.end) return { events: [], limited: true };
    return { events: page.events.slice().reverse(), limited: !!page.end, cursor: page.end, fetched: true };
  } catch {
    // A remote outage must neither block device verification nor mark a gap as
    // the beginning of the room. Pagination can retry with the local cursor.
    return { events: [], limited: true };
  }
}
