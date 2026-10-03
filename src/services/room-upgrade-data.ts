import type { Env } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { parseRoomAlias, parseUserId } from '../utils/ids';
import { getUserPushRules } from '../api/push';
import { getRoom, getRoomState, getUserById, notifyUsersOfEvent } from './database';
import { mergeAccountData, notifyAccountDataUser } from './account-data-stream';
import { isObject } from './federation-events';
import { queueRoomEvent } from './federation-delivery';
import { sendLocalRoomEvent } from './local-room-events';
import { requireAliasDeleteAccess } from './room-alias-access';

interface UpgradeDataInput {
  oldRoomId: string;
  newRoomId: string;
  actorUserId: string;
}

interface UpgradeDataResult {
  aliases: string[];
  skippedAliases: string[];
  usersUpdated: number;
}

function objectContent(content: string | undefined): Record<string, unknown> | undefined {
  if (content === undefined) return undefined;
  try { const value = JSON.parse(content); return isObject(value) ? value : undefined; } catch { return undefined; }
}

// Invoked after the signed replacement graph, invitations and old tombstone are
// persisted. It only moves this server's aliases and this server's user settings;
// membership changes and the original room's history remain with the caller.
export async function migrateRoomUpgradeData(env: Env, input: UpgradeDataInput): Promise<UpgradeDataResult> {
  const { oldRoomId, newRoomId, actorUserId } = input;
  if (oldRoomId === newRoomId || parseUserId(actorUserId)?.serverName !== env.SERVER_NAME) throw Errors.forbidden('Invalid upgrade actor or replacement');
  const [actor, oldRoom, newRoom, oldState, newState] = await Promise.all([
    getUserById(env.DB, actorUserId), getRoom(env.DB, oldRoomId), getRoom(env.DB, newRoomId),
    getRoomState(env.DB, oldRoomId), getRoomState(env.DB, newRoomId),
  ]);
  if (!oldRoom || !newRoom) throw Errors.notFound('Upgrade room not found');
  const tombstone = oldState.find(event => event.type === 'm.room.tombstone' && event.state_key === '');
  const create = newState.find(event => event.type === 'm.room.create' && event.state_key === '');
  if (!actor || actor.is_deactivated || tombstone?.sender !== actorUserId || tombstone.content.replacement_room !== newRoomId ||
      create?.sender !== actorUserId || !isObject(create.content.predecessor) || create.content.predecessor.room_id !== oldRoomId) {
    throw Errors.forbidden('Replacement room is not the authorized upgrade');
  }

  const aliases = await env.DB.prepare('SELECT alias,creator_id FROM room_aliases WHERE room_id=?')
    .bind(oldRoomId).all<{ alias: string; creator_id: string | null }>();
  const allowed: Array<{ alias: string; creator_id: string | null }> = [];
  const skippedAliases: string[] = [];
  for (const alias of aliases.results) {
    if (parseRoomAlias(alias.alias)?.serverName !== env.SERVER_NAME) { skippedAliases.push(alias.alias); continue; }
    try { await requireAliasDeleteAccess(env, alias.alias, actorUserId); allowed.push(alias); }
    catch (error) {
      if (!(error instanceof MatrixApiError) || ![403, 404].includes(error.status)) throw error;
      skippedAliases.push(alias.alias);
    }
  }
  if (allowed.length) await env.DB.batch(allowed.map(alias => env.DB.prepare(
    'UPDATE room_aliases SET room_id=? WHERE alias=? AND room_id=? AND creator_id IS ?'
  ).bind(newRoomId, alias.alias, oldRoomId, alias.creator_id)));

  // Include aliases already moved by a previous attempt so canonical-alias event
  // delivery can be retried independently of the relational alias transaction.
  const moved = await env.DB.prepare('SELECT alias FROM room_aliases WHERE room_id=?')
    .bind(newRoomId).all<{ alias: string }>();
  const movedLocal = new Set(moved.results.map(row => row.alias).filter(alias => parseRoomAlias(alias)?.serverName === env.SERVER_NAME));
  const oldCanonical = oldState.find(event => event.type === 'm.room.canonical_alias' && event.state_key === '')?.content;
  const canonical: Record<string, unknown> = {};
  if (typeof oldCanonical?.alias === 'string' && movedLocal.has(oldCanonical.alias)) canonical.alias = oldCanonical.alias;
  if (Array.isArray(oldCanonical?.alt_aliases)) {
    const alternatives = [...new Set(oldCanonical.alt_aliases.filter((alias): alias is string => typeof alias === 'string' && movedLocal.has(alias)))];
    if (alternatives.length) canonical.alt_aliases = alternatives;
  }
  const existingCanonical = newState.find(event => event.type === 'm.room.canonical_alias' && event.state_key === '');
  if (Object.keys(canonical).length && !existingCanonical) {
    await sendLocalRoomEvent(env, { roomId: newRoomId, sender: actorUserId, type: 'm.room.canonical_alias', stateKey: '', content: canonical });
  } else if (existingCanonical?.sender === actorUserId && JSON.stringify(existingCanonical.content) === JSON.stringify(canonical)) {
    // A previous attempt may have stored the signed event before delivery failed.
    await queueRoomEvent(env, existingCanonical, newRoom.room_version);
    await notifyUsersOfEvent(env, newRoomId, existingCanonical.event_id, existingCanonical.type);
  }

  const users = await env.DB.prepare(`SELECT u.user_id FROM users u
    JOIN room_memberships previous ON previous.user_id=u.user_id AND previous.room_id=? AND previous.membership IN ('join','invite')
    JOIN room_memberships replacement ON replacement.user_id=u.user_id AND replacement.room_id=? AND replacement.membership IN ('join','invite')
    WHERE u.is_deactivated=0`).bind(oldRoomId, newRoomId).all<{ user_id: string }>();
  let usersUpdated = 0;
  for (const { user_id: userId } of users.results) {
    if (parseUserId(userId)?.serverName !== env.SERVER_NAME) continue;
    let updated = false;
    const tags = await env.DB.prepare("SELECT content FROM account_data WHERE user_id=? AND room_id=? AND event_type='m.tag'")
      .bind(userId, oldRoomId).first<{ content: string }>();
    const sourceTags = objectContent(tags?.content);
    if (sourceTags && isObject(sourceTags.tags)) updated = await mergeAccountData(env.DB, userId, newRoomId, 'm.tag', current => {
      const existing = objectContent(current);
      if (current !== undefined && (!existing || !isObject(existing.tags))) return undefined;
      return { ...sourceTags, ...existing, tags: { ...sourceTags.tags as Record<string, unknown>, ...existing?.tags as Record<string, unknown> | undefined } };
    }) || updated;

    updated = await mergeAccountData(env.DB, userId, '', 'm.direct', current => {
      const direct = objectContent(current);
      if (!direct) return undefined;
      return Object.fromEntries(Object.entries(direct).map(([peer, rooms]) => [peer,
        Array.isArray(rooms) && rooms.every(id => typeof id === 'string') && rooms.includes(oldRoomId) && !rooms.includes(newRoomId)
          ? [...rooms, newRoomId] : rooms]));
    }) || updated;

    await env.DB.prepare(`INSERT INTO push_rules(user_id,rule_id,kind,priority,conditions,actions,enabled)
      SELECT user_id,?,'room',priority,conditions,actions,enabled FROM push_rules WHERE user_id=? AND kind='room' AND rule_id=?
      ON CONFLICT(user_id,kind,rule_id) DO NOTHING`).bind(newRoomId, userId, oldRoomId).run();
    const roomRule = await env.DB.prepare("SELECT 1 FROM push_rules WHERE user_id=? AND kind='room' AND rule_id=?")
      .bind(userId, oldRoomId).first();
    if (roomRule) {
      updated = await mergeAccountData(env.DB, userId, '', 'm.push_rules', () => getUserPushRules(env.DB, userId)) || updated;
    }
    if (updated) usersUpdated++;
    // Also wake after an earlier attempt committed data but delivery failed.
    await notifyAccountDataUser(env, userId);
  }
  return { aliases: allowed.map(alias => alias.alias).filter(alias => movedLocal.has(alias)), skippedAliases, usersUpdated };
}

export async function completeRoomUpgradeMappings(env: Env, oldRoomId: string, newRoomId: string, actorUserId: string): Promise<UpgradeDataResult> {
  return migrateRoomUpgradeData(env, { oldRoomId, newRoomId, actorUserId });
}
