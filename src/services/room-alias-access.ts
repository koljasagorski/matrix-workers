import type { Env, PDU, RoomPowerLevelsContent } from '../types';
import { Errors } from '../utils/errors';
import { parseRoomAlias } from '../utils/ids';
import { getMembership, getRoom, getRoomState, getUserById } from './database';
import { checkEventAuth } from './event-auth';

export function requireLocalRoomAlias(env: Env, alias: string): void {
  const parsed = parseRoomAlias(alias);
  if (!parsed || parsed.serverName !== env.SERVER_NAME) throw Errors.invalidParam('room_alias', 'Room alias must belong to this server');
}

export async function requireAliasDeleteAccess(env: Env, alias: string, userId: string): Promise<void> {
  requireLocalRoomAlias(env, alias);
  const record = await env.DB.prepare('SELECT room_id,creator_id FROM room_aliases WHERE alias=?')
    .bind(alias).first<{ room_id: string; creator_id: string | null }>();
  if (!record) throw Errors.notFound('Room alias not found');
  if (record.creator_id === userId || (await getUserById(env.DB, userId))?.admin) return;
  if ((await getMembership(env.DB, record.room_id, userId))?.membership !== 'join') throw Errors.forbidden('Not a member of this room');
  const room = await getRoom(env.DB, record.room_id);
  const state = await getRoomState(env.DB, record.room_id);
  const event: PDU = { event_id: '$alias-permission', room_id: record.room_id, sender: userId,
    type: 'm.room.canonical_alias', state_key: '', content: {}, origin_server_ts: Date.now(),
    depth: 0, auth_events: [], prev_events: [] };
  if (!room || !checkEventAuth(event, state, room.room_version).allowed) throw Errors.forbidden('Insufficient power level to delete alias');
}

export async function requireRoomDirectoryAccess(env: Env, roomId: string, userId: string): Promise<void> {
  const [room, state, membership] = await Promise.all([
    getRoom(env.DB, roomId), getRoomState(env.DB, roomId), getMembership(env.DB, roomId, userId),
  ]);
  if (!room) throw Errors.notFound('Room not found');
  if (membership?.membership !== 'join') throw Errors.forbidden('Not a member of this room');
  const create = state.find(event => event.type === 'm.room.create');
  const power = state.find(event => event.type === 'm.room.power_levels');
  const levels = power?.content as RoomPowerLevelsContent | undefined;
  const creators = [create?.sender, ...(Array.isArray(create?.content.additional_creators) ? create.content.additional_creators : [])];
  const userPower = room.room_version === '12' && creators.includes(userId) ? Infinity : levels ?
    levels.users?.[userId] ?? levels.users_default ?? 0 : create?.sender === userId ? 100 : 0;
  if (!(userPower >= (levels?.state_default ?? 50))) throw Errors.forbidden('Insufficient power level to change room visibility');
}
