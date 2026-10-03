import type { Env } from '../types';
import { Errors } from '../utils/errors';
import { parseUserId } from '../utils/ids';
import { signJson } from '../utils/crypto';
import { eventVerifier, isObject, redactEvent, wireEvent, FEDERATED_ROOM_VERSIONS } from './federation-events';
import { getServerSigningKey } from './federation-keys';

export async function receiveRemoteInvite(env: Env, origin: string, roomId: string, eventId: string, body: unknown) {
  if (!isObject(body) || typeof body.room_version !== 'string' || !FEDERATED_ROOM_VERSIONS.includes(body.room_version)) throw Errors.unsupportedRoomVersion();
  const raw = body.event;
  if (!isObject(raw) || raw.type !== 'm.room.member' || !isObject(raw.content) || raw.content.membership !== 'invite' ||
      typeof raw.state_key !== 'string' || parseUserId(raw.state_key)?.serverName !== env.SERVER_NAME ||
      parseUserId(String(raw.sender))?.serverName !== origin) throw Errors.forbidden('Invalid local invitation');
  const user = await env.DB.prepare('SELECT user_id FROM users WHERE user_id=?').bind(raw.state_key).first();
  if (!user) throw Errors.notFound('Invited user does not exist');
  const version = body.room_version;
  const event = await eventVerifier(env)(raw, version, roomId);
  if (event.event_id !== eventId) throw Errors.invalidParam('event_id');
  const stripped = Array.isArray(body.invite_room_state) ? body.invite_room_state : [];
  if (stripped.length > 256 || JSON.stringify(stripped).length > 131072) throw Errors.tooLarge('Invite state too large');
  const inviteState = stripped.filter(e => isObject(e) && typeof e.type === 'string' && typeof e.state_key === 'string' &&
    typeof e.sender === 'string' && isObject(e.content) && !(e.type === 'm.room.member' && e.state_key === event.state_key))
    .map(e => ({ type:e.type, state_key:e.state_key, sender:e.sender, content:e.content }));
  inviteState.push({ type:event.type, state_key:event.state_key!, sender:event.sender, content:event.content });
  const key = await getServerSigningKey(env.DB);
  if (!key) throw new Error('Server signing key unavailable');
  const signed = await signJson(redactEvent(wireEvent(event,version),version), env.SERVER_NAME,key.keyId,key.privateKeyJwk);
  const signedEvent = { ...wireEvent(event,version), signatures:signed.signatures };
  const membership = await env.DB.prepare('SELECT membership,event_id FROM room_memberships WHERE room_id=? AND user_id=?')
    .bind(roomId,event.state_key!).first<{ membership:string;event_id:string }>();
  if (membership?.membership !== 'join' && membership?.event_id !== eventId) {
    // Store stripped preview state only with the invitation, never as trusted
    // room state. A later join imports and verifies the full authorization chain.
    await env.DB.batch([
      env.DB.prepare(`INSERT INTO rooms(room_id,room_version,creator_id,is_public) VALUES (?,?,?,0) ON CONFLICT(room_id) DO NOTHING`).bind(roomId,version,event.sender),
      env.DB.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,state_key,content,origin_server_ts,unsigned,depth,auth_events,prev_events,hashes,signatures,stream_ordering)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,(SELECT COALESCE(MAX(stream_ordering),0)+1 FROM events)) ON CONFLICT(event_id) DO NOTHING`)
        .bind(eventId,roomId,event.sender,event.type,event.state_key!,JSON.stringify(event.content),event.origin_server_ts,
          JSON.stringify({invite_room_state:inviteState}),event.depth,JSON.stringify(event.auth_events),JSON.stringify(event.prev_events),JSON.stringify(event.hashes),JSON.stringify(signed.signatures)),
      env.DB.prepare(`INSERT INTO room_memberships(room_id,user_id,membership,event_id,display_name,avatar_url) VALUES (?,?,'invite',?,?,?)
        ON CONFLICT(room_id,user_id) DO UPDATE SET membership='invite',event_id=excluded.event_id,display_name=excluded.display_name,avatar_url=excluded.avatar_url`)
        .bind(roomId,event.state_key!,eventId,typeof event.content.displayname==='string'?event.content.displayname:null,typeof event.content.avatar_url==='string'?event.content.avatar_url:null),
    ]);
    // notifyUsersOfEvent only wakes joined members; the invitee needs a direct wakeup.
    await env.SYNC.get(env.SYNC.idFromName(event.state_key!)).fetch(new Request('https://internal/notify',{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({room_id:roomId,event_id:eventId,type:event.type,timestamp:Date.now()}),
    }));
  }
  return signedEvent;
}

export async function getStoredInviteState(db: D1Database, roomId: string, userId: string): Promise<any[] | null> {
  const row = await db.prepare(`SELECT e.unsigned FROM room_memberships rm JOIN events e ON rm.event_id=e.event_id
    WHERE rm.room_id=? AND rm.user_id=? AND rm.membership='invite'`).bind(roomId,userId).first<{unsigned:string|null}>();
  if (!row?.unsigned) return null;
  const state = JSON.parse(row.unsigned).invite_room_state;
  return Array.isArray(state) ? state : null;
}
