import type { Env, PDU } from '../types';
import { Errors } from '../utils/errors';
import { parseUserId } from '../utils/ids';
import { checkEventAuth } from './event-auth';
import { isObject } from './federation-events';

export async function hasRestrictedJoinAccess(db: D1Database, userId: string, rules: Record<string, unknown>): Promise<boolean> {
  const allowedRooms = Array.isArray(rules.allow) ? [...new Set(rules.allow.filter(rule => isObject(rule) &&
    rule.type === 'm.room_membership' && typeof rule.room_id === 'string').map(rule => String(rule.room_id)))] : [];
  // Keep each query within D1's SQL parameter limit, including the user ID.
  for (let offset = 0; offset < allowedRooms.length; offset += 90) {
    const batch = allowedRooms.slice(offset, offset + 90);
    const member = await db.prepare(`SELECT room_id FROM room_memberships WHERE user_id=? AND membership='join'
      AND room_id IN (${batch.map(() => '?').join(',')}) LIMIT 1`).bind(userId, ...batch).first();
    if (member) return true;
  }
  return false;
}

// This check belongs at the joining/resident server boundary. The event auth
// rules check an authorizer's power; they cannot prove membership in other rooms.
export async function prepareRestrictedJoinContent(
  env: Env, roomId: string, userId: string, content: Record<string, unknown>, state: PDU[], version: string,
): Promise<Record<string, unknown>> {
  const current = state.find(event => event.type === 'm.room.member' && event.state_key === userId)?.content.membership;
  if (current === 'ban') throw Errors.forbidden('Banned users cannot join');
  const rules = state.find(event => event.type === 'm.room.join_rules' && event.state_key === '')?.content;
  const clean = { ...content };
  // Existing membership or an ordinary invitation already authorizes this join.
  // Omit a client-supplied authorizer so its server's unnecessary signature is
  // never claimed by a local profile/rejoin event.
  if (current === 'join' || current === 'invite' || !rules || !['restricted', 'knock_restricted'].includes(String(rules.join_rule))) {
    delete clean.join_authorised_via_users_server;
    return clean;
  }
  if (!await hasRestrictedJoinAccess(env.DB, userId, rules)) throw Errors.forbidden('User is not joined to a room allowed by the restricted join rule');
  const requested = clean.join_authorised_via_users_server;
  if (requested !== undefined && (typeof requested !== 'string' || parseUserId(requested)?.serverName !== env.SERVER_NAME)) {
    throw Errors.forbidden('Restricted join authorizer must belong to this server');
  }
  const candidates = state.filter(event => event.type === 'm.room.member' && event.content.membership === 'join' &&
    typeof event.state_key === 'string' && parseUserId(event.state_key)?.serverName === env.SERVER_NAME &&
    (requested === undefined || event.state_key === requested));
  for (const candidate of candidates) {
    const permission: PDU = { event_id: '$restricted-join-permission', room_id: roomId, sender: candidate.state_key!,
      type: 'm.room.member', state_key: userId, content: { membership: 'invite' }, origin_server_ts: Date.now(),
      depth: 0, auth_events: [], prev_events: [] };
    if (checkEventAuth(permission, state, version).allowed) return { ...clean, join_authorised_via_users_server: candidate.state_key! };
  }
  throw Errors.forbidden('No local room member can authorize this restricted join');
}
