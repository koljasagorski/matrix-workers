import type { Env, PDU } from '../types';
import { getRoomState, getEvent, getUserById, storeEvent, updateMembership, notifyUsersOfEvent } from './database';
import { checkEventAuth } from './event-auth';
import { getServerSigningKey } from './federation-keys';
import { eventReferenceId, signEvent, wireEvent, FEDERATED_ROOM_VERSIONS } from './federation-events';
import { queueRoomEvent } from './federation-delivery';
import { generateEventId } from '../utils/ids';

// A Matrix profile is copied into each joined room's membership event. Updating
// only the users table leaves other members (and other servers) with the old photo.
export async function propagateProfile(env: Env, userId: string): Promise<void> {
  const user = await getUserById(env.DB, userId);
  if (!user) throw new Error('Profile user not found');
  const rooms = await env.DB.prepare(`SELECT r.room_id,r.room_version FROM room_memberships rm
    JOIN rooms r ON r.room_id=rm.room_id WHERE rm.user_id=? AND rm.membership='join'`)
    .bind(userId).all<{ room_id: string; room_version: string }>();
  for (const room of rooms.results) {
    const state = await getRoomState(env.DB, room.room_id);
    const previous = state.find(e => e.type === 'm.room.member' && e.state_key === userId);
    if (!previous || previous.content.membership !== 'join') continue;
    const unchanged = (previous.content.displayname ?? '') === (user.display_name ?? '') &&
      (previous.content.avatar_url ?? '') === (user.avatar_url ?? '');
    if (unchanged) {
      // Retrying a partially completed update must still deliver its durable event.
      const stored = await getEvent(env.DB, previous.event_id);
      if (stored) await queueRoomEvent(env, stored, room.room_version);
      continue;
    }
    const content: Record<string, unknown> = { ...previous.content, membership: 'join' };
    if (user.display_name) content.displayname = user.display_name; else delete content.displayname;
    if (user.avatar_url) content.avatar_url = user.avatar_url; else delete content.avatar_url;
    const latest = await env.DB.prepare(`SELECT event_id,depth FROM events WHERE room_id=? AND stream_ordering IS NOT NULL
      ORDER BY depth DESC,stream_ordering DESC LIMIT 1`).bind(room.room_id).first<{ event_id: string; depth: number }>();
    const auth = state.filter(e => (e.type === 'm.room.create' && room.room_version !== '12') ||
      e.type === 'm.room.power_levels' || e.type === 'm.room.join_rules' || (e.type === 'm.room.member' && e.state_key === userId));
    const event: PDU = { event_id: await generateEventId(env.SERVER_NAME), room_id: room.room_id, type: 'm.room.member',
      sender: userId, state_key: userId, content, origin_server_ts: Date.now(), depth: (latest?.depth ?? 0) + 1,
      auth_events: auth.map(e => e.event_id), prev_events: latest ? [latest.event_id] : [] };
    const allowed = checkEventAuth(event, state, room.room_version);
    if (!allowed.allowed) throw new Error(allowed.error);
    if (FEDERATED_ROOM_VERSIONS.includes(room.room_version)) {
      const key = await getServerSigningKey(env.DB);
      if (!key) throw new Error('Server signing key unavailable');
      const signed = await signEvent(wireEvent(event, room.room_version), room.room_version, env.SERVER_NAME, key);
      Object.assign(event, signed, { event_id: await eventReferenceId(signed, room.room_version) });
    }
    await storeEvent(env.DB, event);
    await updateMembership(env.DB, room.room_id, userId, 'join', event.event_id, user.display_name ?? undefined, user.avatar_url ?? undefined);
    await queueRoomEvent(env, event, room.room_version);
    await notifyUsersOfEvent(env, room.room_id, event.event_id, event.type);
  }
}
