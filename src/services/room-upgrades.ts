import type { Env, PDU } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { generateRoomId, parseUserId } from '../utils/ids';
import { getEvent, getRoom, getRoomState, getStateEvent, getUserById, notifyUsersOfEvent,
  prepareStoreEventStatements } from './database';
import { checkEventAuth } from './event-auth';
import { FEDERATED_ROOM_VERSIONS, isObject } from './federation-events';
import { queueRoomEvent } from './federation-delivery';
import { buildLocalRoomEvent, buildLocalRoomEventFromState, countersignLocalRoomInvite } from './local-room-events';
import { migrateRoomUpgradeData } from './room-upgrade-data';
import { invalidateRoomCache } from './room-cache';

const LEASE_MS = 120000;
interface UpgradeMember { userId: string; content: Record<string, unknown>; eventId?: string }
interface UpgradePlan { bootstrap: PDU[]; members: UpgradeMember[]; tombstone: PDU; restriction?: PDU; isPublic: boolean }
interface UpgradeJob {
  old_room_id: string; new_version: string; actor_user_id: string; additional_creators: string;
  replacement_room_id: string | null; plan_json: string | null; phase: string; member_cursor: number;
  pending_event_json: string | null; lease_token: string | null; lease_until: number;
}
interface UpgradeOptions { oldRoomId: string; actorUserId: string; newVersion: string; additionalCreators?: unknown }

function membershipStatement(env: Env, event: PDU): D1PreparedStatement {
  return env.DB.prepare(`INSERT OR REPLACE INTO room_memberships
    (room_id,user_id,membership,event_id,display_name,avatar_url) VALUES(?,?,?,?,?,?)`)
    .bind(event.room_id, event.state_key!, String(event.content.membership), event.event_id,
      typeof event.content.displayname === 'string' ? event.content.displayname : null,
      typeof event.content.avatar_url === 'string' ? event.content.avatar_url : null);
}

// The NOT NULL constraint makes a lost/expired lease abort the entire D1 batch,
// including any event or room publication which follows this guard.
function guard(env: Env, oldRoomId: string, token: string): D1PreparedStatement {
  const now = Date.now();
  return env.DB.prepare(`UPDATE room_upgrades SET lease_until=
    CASE WHEN lease_token=? AND lease_until>? THEN ? ELSE NULL END, updated_at=? WHERE old_room_id=?`)
    .bind(token, now, now + LEASE_MS, now, oldRoomId);
}
function phase(env: Env, oldRoomId: string, value: string): D1PreparedStatement {
  return env.DB.prepare('UPDATE room_upgrades SET phase=?,updated_at=? WHERE old_room_id=?').bind(value, Date.now(), oldRoomId);
}
async function renew(env: Env, job: UpgradeJob, token: string): Promise<void> {
  await env.DB.batch([guard(env, job.old_room_id, token)]);
}

async function isRedactedTombstone(env: Env, event: PDU, version: string): Promise<boolean> {
  if (event.content.replacement_room !== undefined) return false;
  return !!await env.DB.prepare(`SELECT event_id FROM events WHERE room_id=? AND event_type='m.room.redaction'
    AND ${version === '10' ? 'redacts' : "json_extract(content,'$.redacts')"}=? LIMIT 1`)
    .bind(event.room_id, event.event_id).first();
}

async function publish(env: Env, event: PDU, version: string): Promise<void> {
  await queueRoomEvent(env, event, version);
  await notifyUsersOfEvent(env, event.room_id, event.event_id, event.type);
  if (event.type === 'm.room.member' && event.content.membership !== 'join' &&
      parseUserId(event.state_key ?? '')?.serverName === env.SERVER_NAME) {
    const response = await env.SYNC.get(env.SYNC.idFromName(event.state_key!)).fetch(new Request('https://internal/notify', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
        room_id: event.room_id, event_id: event.event_id, type: event.type, timestamp: Date.now(),
      }),
    }));
    if (!response.ok) throw new Error('Could not notify upgraded-room invite recipient');
  }
}

async function planUpgrade(env: Env, options: UpgradeOptions, state: PDU[], additionalCreators: string[]): Promise<UpgradePlan> {
  const oldRoom = (await getRoom(env.DB, options.oldRoomId))!;
  const oldCreate = state.find(event => event.type === 'm.room.create');
  const oldPower = state.find(event => event.type === 'm.room.power_levels');
  let roomId = await generateRoomId(env.SERVER_NAME);
  const tombstoneInput = (replacement: string) => ({ roomId: options.oldRoomId, sender: options.actorUserId,
    type: 'm.room.tombstone', stateKey: '', content: { body: 'This room has been replaced', replacement_room: replacement } });
  // v12 omits predecessor.event_id to avoid the create/tombstone hash cycle.
  // Earlier versions can reference the exact, prebuilt tombstone instead.
  let tombstone = options.newVersion === '12' ? undefined : (await buildLocalRoomEvent(env, tombstoneInput(roomId))).event;
  const bootstrap: PDU[] = [];
  const current: PDU[] = [];
  const add = async (type: string, content: Record<string, unknown>, stateKey = '') => {
    const event = await buildLocalRoomEventFromState(env, { roomId, sender: options.actorUserId, type, content, stateKey },
      options.newVersion, current, bootstrap.at(-1));
    bootstrap.push(event);
    const index = current.findIndex(previous => previous.type === type && previous.state_key === stateKey);
    if (index === -1) current.push(event); else current[index] = event;
    return event;
  };
  const create = await add('m.room.create', { room_version: options.newVersion,
    ...(options.newVersion === '10' ? { creator: options.actorUserId } : {}),
    predecessor: { room_id: options.oldRoomId, ...(tombstone ? { event_id: tombstone.event_id } : {}) },
    ...(oldCreate?.content.type !== undefined ? { type: oldCreate.content.type } : {}),
    ...(oldCreate?.content['m.federate'] === false ? { 'm.federate': false } : {}),
    ...(additionalCreators.length ? { additional_creators: additionalCreators } : {}),
  });
  if (options.newVersion === '12') { roomId = `!${create.event_id.slice(1)}`; create.room_id = roomId; }
  const profile = await getUserById(env.DB, options.actorUserId);
  await add('m.room.member', { membership: 'join', ...(profile?.display_name ? { displayname: profile.display_name } : {}),
    ...(profile?.avatar_url ? { avatar_url: profile.avatar_url } : {}) }, options.actorUserId);
  const omitted = new Set(['m.room.create', 'm.room.member', 'm.room.power_levels', 'm.room.tombstone',
    'm.room.canonical_alias', 'm.room.aliases', 'm.room.third_party_invite', 'm.call.member', 'org.matrix.msc3401.call.member']);
  for (const previous of state) {
    if (previous.state_key === undefined || omitted.has(previous.type) ||
        (previous.state_key.startsWith('@') && previous.state_key !== options.actorUserId)) continue;
    await add(previous.type, previous.content, previous.state_key);
  }
  if (!current.some(event => event.type === 'm.room.join_rules')) await add('m.room.join_rules', { join_rule: 'invite' });
  if (!current.some(event => event.type === 'm.room.history_visibility')) await add('m.room.history_visibility', { history_visibility: 'shared' });
  // Bans are real authorized events, rather than copied membership rows. Create
  // them before the final power levels so an old ban cannot be accidentally lost.
  for (const member of state.filter(event => event.type === 'm.room.member' && event.content.membership === 'ban')) {
    if (member.state_key && member.state_key !== options.actorUserId) await add('m.room.member', member.content, member.state_key);
  }
  const users: Record<string, unknown> = isObject(oldPower?.content.users) ? { ...oldPower.content.users } : {};
  if (options.newVersion === '12') for (const creator of [options.actorUserId, ...additionalCreators]) delete users[creator];
  else users[options.actorUserId] = Math.max(typeof users[options.actorUserId] === 'number' ? users[options.actorUserId] as number : 0, 100);
  await add('m.room.power_levels', oldPower ? { ...oldPower.content, users } : {
    users, events: { 'm.room.power_levels': 100, 'm.room.tombstone': 100, 'm.room.encryption': 100, 'm.call.member': 0 },
    events_default: 0, state_default: 50, users_default: 0, invite: 50, kick: 50, ban: 50, redact: 50,
  });
  tombstone ??= (await buildLocalRoomEvent(env, tombstoneInput(roomId))).event;
  const members: UpgradeMember[] = [];
  for (const member of state.filter(event => event.type === 'm.room.member' && ['join', 'invite'].includes(String(event.content.membership)))) {
    const userId = member.state_key;
    const parsed = parseUserId(userId ?? '');
    if (!userId || userId === options.actorUserId || !parsed) continue;
    if (parsed.serverName === env.SERVER_NAME && (await getUserById(env.DB, userId))?.is_deactivated !== false) continue;
    const content = { membership: 'invite', ...Object.fromEntries(['displayname', 'avatar_url', 'is_direct']
      .filter(key => member.content[key] !== undefined).map(key => [key, member.content[key]])) };
    // Check all invite permissions before exposing the replacement room.
    const probe = { ...bootstrap.at(-1)!, type: 'm.room.member', state_key: userId, content };
    const auth = checkEventAuth(probe, current, options.newVersion);
    if (!auth.allowed) throw Errors.forbidden(auth.error);
    members.push({ userId, content });
  }
  const plan = { bootstrap, members, tombstone, isPublic: oldRoom.is_public };
  if (new TextEncoder().encode(JSON.stringify(plan)).length > 1800000) throw Errors.tooLarge('Room upgrade plan exceeds storage limit');
  return plan;
}

export async function upgradeRoom(env: Env, options: UpgradeOptions): Promise<string> {
  if (!FEDERATED_ROOM_VERSIONS.includes(options.newVersion)) throw Errors.unsupportedRoomVersion();
  const additional = options.additionalCreators ?? [];
  if (!Array.isArray(additional) || (additional.length && options.newVersion !== '12') || additional.length > 20 ||
      !additional.every(userId => typeof userId === 'string' && parseUserId(userId)) || new Set(additional).size !== additional.length ||
      additional.includes(options.actorUserId)) throw Errors.invalidParam('additional_creators');
  const additionalCreators = additional as string[];
  const oldRoom = await getRoom(env.DB, options.oldRoomId);
  if (!oldRoom) throw Errors.notFound('Room not found');
  const state = await getRoomState(env.DB, options.oldRoomId);
  const auth = checkEventAuth({ event_id: '$upgrade-permission', room_id: options.oldRoomId, sender: options.actorUserId,
    type: 'm.room.tombstone', state_key: '', content: {}, origin_server_ts: Date.now(), depth: 1, auth_events: [], prev_events: [] }, state, oldRoom.room_version);
  if (!auth.allowed || parseUserId(options.actorUserId)?.serverName !== env.SERVER_NAME) throw Errors.forbidden(auth.error);
  let job = await env.DB.prepare('SELECT * FROM room_upgrades WHERE old_room_id=?').bind(options.oldRoomId).first<UpgradeJob>();
  if (!job) {
    const tombstone = state.find(event => event.type === 'm.room.tombstone');
    if (tombstone && !await isRedactedTombstone(env, tombstone, oldRoom.room_version)) {
      const replacement = typeof tombstone.content.replacement_room === 'string' ? await getRoom(env.DB, tombstone.content.replacement_room) : null;
      const create = replacement ? await getStateEvent(env.DB, replacement.room_id, 'm.room.create') : null;
      if (!replacement || !isObject(create?.content.predecessor) || create.content.predecessor.room_id !== options.oldRoomId) {
        throw Errors.invalidRoomState('Existing tombstone has no valid replacement');
      }
      if (replacement.room_version !== options.newVersion) throw Errors.invalidParam('new_version', 'Room already upgraded to another version');
      return replacement.room_id;
    }
    if (oldRoom.room_version === options.newVersion) {
      throw Errors.invalidParam('new_version', 'Room already uses this version');
    }
    await env.DB.prepare(`INSERT INTO room_upgrades(old_room_id,new_version,actor_user_id,additional_creators,created_at,updated_at)
      VALUES(?,?,?,?,?,?) ON CONFLICT(old_room_id) DO NOTHING`).bind(options.oldRoomId, options.newVersion, options.actorUserId,
      JSON.stringify(additionalCreators), Date.now(), Date.now()).run();
    job = await env.DB.prepare('SELECT * FROM room_upgrades WHERE old_room_id=?').bind(options.oldRoomId).first<UpgradeJob>();
  }
  if (!job) throw new Error('Could not reserve room upgrade');
  if (job.new_version !== options.newVersion || job.additional_creators !== JSON.stringify(additionalCreators)) {
    throw Errors.invalidParam('new_version', 'Room upgrade already reserved with different options');
  }
  if (job.phase === 'complete') return job.replacement_room_id!;
  if (job.actor_user_id !== options.actorUserId) throw Errors.forbidden('Only the original upgrader can resume this upgrade');
  const token = crypto.randomUUID();
  const claimed = await env.DB.prepare(`UPDATE room_upgrades SET lease_token=?,lease_until=?,updated_at=?
    WHERE old_room_id=? AND (lease_token IS NULL OR lease_until<=?) RETURNING *`)
    .bind(token, Date.now() + LEASE_MS, Date.now(), options.oldRoomId, Date.now()).first<UpgradeJob>();
  if (!claimed) throw Errors.limitExceeded('Room upgrade is already running; retry shortly', 1000);
  job = claimed;
  try {
    let plan: UpgradePlan;
    if (!job.plan_json) {
      plan = await planUpgrade(env, options, state, additionalCreators);
      const roomId = plan.bootstrap[0].room_id;
      await env.DB.batch([guard(env, options.oldRoomId, token), env.DB.prepare(`UPDATE room_upgrades
        SET plan_json=?,replacement_room_id=?,phase='planned',updated_at=? WHERE old_room_id=?`)
        .bind(JSON.stringify(plan), roomId, Date.now(), options.oldRoomId)]);
      job.phase = 'planned'; job.replacement_room_id = roomId;
    } else plan = JSON.parse(job.plan_json) as UpgradePlan;
    const roomId = job.replacement_room_id!;
    if (job.phase === 'planned') {
      const statements = [guard(env, options.oldRoomId, token), env.DB.prepare(`INSERT INTO rooms(room_id,room_version,is_public,creator_id,created_at)
        VALUES(?,?,?,?,?)`).bind(roomId, options.newVersion, plan.isPublic ? 1 : 0, options.actorUserId, Date.now())];
      const before: PDU[] = [];
      for (const event of plan.bootstrap) {
        statements.push(...await prepareStoreEventStatements(env.DB, event,
          event.type === 'm.room.member' ? [membershipStatement(env, event)] : [], before));
        const index = before.findIndex(previous => previous.type === event.type && previous.state_key === event.state_key);
        if (index === -1) before.push(event); else before[index] = event;
      }
      statements.push(phase(env, options.oldRoomId, 'bootstrapped'));
      await env.DB.batch(statements);
      job.phase = 'bootstrapped';
    }
    // Publication is safe to replay: federation queues deduplicate event IDs.
    // Replaying also repairs failures after a stored member cursor was advanced.
    for (const event of plan.bootstrap) {
      await renew(env, job, token);
      await publish(env, event, options.newVersion);
    }
    for (const member of plan.members) if (member.eventId) {
      await renew(env, job, token);
      const event = await getEvent(env.DB, member.eventId);
      if (!event) throw new Error('Saved upgrade invitation is missing');
      await publish(env, event, options.newVersion);
    }
    if (job.phase === 'bootstrapped') {
      for (; job.member_cursor < plan.members.length; job.member_cursor++) {
        await renew(env, job, token);
        const member = plan.members[job.member_cursor];
        const current = await getStateEvent(env.DB, roomId, 'm.room.member', member.userId);
        if (current && ['join', 'invite'].includes(String(current.content.membership))) {
          member.eventId = current.event_id;
          await env.DB.batch([guard(env, options.oldRoomId, token), env.DB.prepare(`UPDATE room_upgrades
            SET member_cursor=?,pending_event_json=NULL,plan_json=? WHERE old_room_id=?`)
            .bind(job.member_cursor + 1, JSON.stringify(plan), options.oldRoomId)]);
          job.pending_event_json = null;
          await publish(env, current, options.newVersion);
          continue;
        }
        let pending: PDU;
        if (job.pending_event_json) pending = JSON.parse(job.pending_event_json) as PDU;
        else {
          pending = (await buildLocalRoomEvent(env, { roomId, sender: options.actorUserId, type: 'm.room.member',
            stateKey: member.userId, content: member.content })).event;
          await env.DB.batch([guard(env, options.oldRoomId, token), env.DB.prepare('UPDATE room_upgrades SET pending_event_json=? WHERE old_room_id=?')
            .bind(JSON.stringify(pending), options.oldRoomId)]);
          job.pending_event_json = JSON.stringify(pending);
        }
        // A saved network retry must still be permitted if a member was banned
        // or the replacement's power levels changed while the job was idle.
        const authorization = checkEventAuth(pending, await getRoomState(env.DB, roomId), options.newVersion);
        if (!authorization.allowed) throw Errors.forbidden(authorization.error);
        const event = await countersignLocalRoomInvite(env, pending, options.newVersion);
        member.eventId = event.event_id;
        await env.DB.batch([guard(env, options.oldRoomId, token), ...await prepareStoreEventStatements(env.DB, event,
          [membershipStatement(env, event), env.DB.prepare(`UPDATE room_upgrades
            SET member_cursor=?,pending_event_json=NULL,plan_json=? WHERE old_room_id=?`)
            .bind(job.member_cursor + 1, JSON.stringify(plan), options.oldRoomId)])]);
        job.pending_event_json = null;
        await publish(env, event, options.newVersion);
      }
      await env.DB.batch([guard(env, options.oldRoomId, token), phase(env, options.oldRoomId, 'members_done')]);
      job.phase = 'members_done';
    }
    if (job.phase === 'members_done') {
      const current = await getStateEvent(env.DB, options.oldRoomId, 'm.room.tombstone');
      if (current && current.event_id !== plan.tombstone.event_id && !await isRedactedTombstone(env, current, oldRoom.room_version)) {
        throw Errors.invalidRoomState('Room received another tombstone during upgrade');
      }
      await env.DB.batch([guard(env, options.oldRoomId, token),
        ...await prepareStoreEventStatements(env.DB, plan.tombstone), phase(env, options.oldRoomId, 'tombstoned')]);
      job.phase = 'tombstoned';
    }
    await renew(env, job, token);
    await publish(env, plan.tombstone, oldRoom.room_version);
    if (job.phase === 'tombstoned') {
      const power = await getStateEvent(env.DB, options.oldRoomId, 'm.room.power_levels');
      if (power) {
        try {
          const built = await buildLocalRoomEvent(env, { roomId: options.oldRoomId, sender: options.actorUserId,
            type: 'm.room.power_levels', stateKey: '', content: { ...power.content,
              events_default: Math.max(Number(power.content.events_default ?? 0), 100),
              invite: Math.max(Number(power.content.invite ?? 0), 100) } });
          plan.restriction = built.event;
          await env.DB.batch([guard(env, options.oldRoomId, token), ...await prepareStoreEventStatements(env.DB, built.event),
            env.DB.prepare('UPDATE room_upgrades SET plan_json=? WHERE old_room_id=?').bind(JSON.stringify(plan), options.oldRoomId),
            phase(env, options.oldRoomId, 'restricted')]);
        } catch (error) {
          if (!(error instanceof MatrixApiError) || error.status !== 403) throw error;
          await env.DB.batch([guard(env, options.oldRoomId, token), phase(env, options.oldRoomId, 'restricted')]);
        }
      } else await env.DB.batch([guard(env, options.oldRoomId, token), phase(env, options.oldRoomId, 'restricted')]);
      job.phase = 'restricted';
    }
    if (plan.restriction) {
      await renew(env, job, token);
      await publish(env, plan.restriction, oldRoom.room_version);
    }
    await renew(env, job, token);
    await migrateRoomUpgradeData(env, { oldRoomId: options.oldRoomId, newRoomId: roomId, actorUserId: options.actorUserId });
    await env.DB.batch([guard(env, options.oldRoomId, token), phase(env, options.oldRoomId, 'complete')]);
    await invalidateRoomCache(env.CACHE, options.oldRoomId);
    await invalidateRoomCache(env.CACHE, roomId);
    return roomId;
  } finally {
    await env.DB.prepare('UPDATE room_upgrades SET lease_token=NULL,lease_until=0 WHERE old_room_id=? AND lease_token=?')
      .bind(options.oldRoomId, token).run();
  }
}
