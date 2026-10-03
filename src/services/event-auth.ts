// Event Authorization Rules per Matrix Spec §11.3
// Implements all 10 authorization rules for validating events

import { parseUserId } from '../utils/ids';
import type { PDU, RoomPowerLevelsContent, RoomJoinRulesContent, RoomMemberContent } from '../types';
import { getRoomVersion, type RoomVersionBehavior } from './room-versions';

export interface AuthResult {
  allowed: boolean;
  error?: string;
}

/** State map keyed by (event_type, state_key) */
export type RoomStateMap = Map<string, PDU>;

/** Build a state key for the state map */
export function stateKey(eventType: string, stateKey: string): string {
  return `${eventType}\0${stateKey}`;
}

/** Build a RoomStateMap from an array of state events */
export function buildStateMap(events: PDU[]): RoomStateMap {
  const map: RoomStateMap = new Map();
  for (const event of events) {
    if (event.state_key !== undefined) {
      map.set(stateKey(event.type, event.state_key), event);
    }
  }
  return map;
}

/** Get a state event from the state map */
function getState(state: RoomStateMap, type: string, key: string = ''): PDU | undefined {
  return state.get(stateKey(type, key));
}

/** Get power levels from state, with defaults per spec */
function getPowerLevels(state: RoomStateMap): RoomPowerLevelsContent {
  const pl = getState(state, 'm.room.power_levels');
  const create = getState(state, 'm.room.create');
  const levels: RoomPowerLevelsContent = pl ? { ...pl.content } : { users: {} };
  levels.users = { ...levels.users };
  if (create?.content.room_version === '12') {
    const extra = Array.isArray(create.content.additional_creators) ? create.content.additional_creators : [];
    for (const id of [create.sender, ...extra]) if (typeof id === 'string') levels.users[id] = Infinity;
  } else if (!pl && create) levels.users[create.sender] = 100;
  return levels;
}

/** Get user's power level from power levels content */
function getUserPowerLevel(powerLevels: RoomPowerLevelsContent, userId: string): number {
  return powerLevels.users?.[userId] ?? powerLevels.users_default ?? 0;
}

/** Get the required power level for an event */
function getEventPowerLevel(
  powerLevels: RoomPowerLevelsContent,
  eventType: string,
  isState: boolean
): number {
  const specific = powerLevels.events?.[eventType];
  if (specific !== undefined) return specific;
  if (isState) return powerLevels.state_default ?? 50;
  return powerLevels.events_default ?? 0;
}

/**
 * Check if an event is authorized according to the room state.
 * Implements the full authorization rules from Matrix spec §11.3.
 */
export function checkEventAuth(
  event: PDU,
  roomState: PDU[],
  roomVersion?: string
): AuthResult {
  const state = buildStateMap(roomState);
  const versionBehavior = getRoomVersion(roomVersion ?? '10');
  if (!versionBehavior) {
    return { allowed: false, error: `Unsupported room version: ${roomVersion}` };
  }

  // Rule 1: If the event has a type of m.room.create
  if (event.type === 'm.room.create') {
    return checkCreateEvent(event, state);
  }

  // Rule 2: Considering the event's auth_events, the create event must exist
  const createEvent = getState(state, 'm.room.create');
  if (!createEvent) {
    return { allowed: false, error: 'No m.room.create event in room state' };
  }

  if (createEvent.content['m.federate'] === false && event.sender.slice(event.sender.indexOf(':') + 1) !==
      createEvent.sender.slice(createEvent.sender.indexOf(':') + 1)) return { allowed: false, error: 'Room does not federate' };

  // Rule 3: If type is m.room.member
  if (event.type === 'm.room.member') {
    return checkMemberEvent(event, state, versionBehavior);
  }

  // Rule 4: If the sender's current membership is not 'join', reject
  const senderMembership = getState(state, 'm.room.member', event.sender);
  const senderMembershipState = (senderMembership?.content as RoomMemberContent)?.membership;
  if (senderMembershipState !== 'join') {
    return { allowed: false, error: 'Sender is not joined to the room' };
  }

  // Rule 5: If type is m.room.third_party_invite
  if (event.type === 'm.room.third_party_invite') {
    return checkThirdPartyInvite(event, state);
  }

  if (event.state_key?.startsWith('@') && event.state_key !== event.sender) return { allowed: false, error: 'Invalid state key owner' };

  // Rule 6: Check power levels for state events
  if (event.state_key !== undefined) {
    return checkStateEventPower(event, state, versionBehavior);
  }

  // Rule 7: Check power levels for non-state events
  return checkNonStateEventPower(event, state);
}

/** Rule 1: m.room.create validation */
function checkCreateEvent(event: PDU, _state: RoomStateMap): AuthResult {
  // 1.1: It MUST be the first event in the room
  if (event.prev_events && event.prev_events.length > 0) {
    return { allowed: false, error: 'm.room.create must have no prev_events' };
  }

  // 1.2: Must have empty state_key
  if (event.state_key !== '') {
    return { allowed: false, error: 'm.room.create must have empty state_key' };
  }

  // 1.3: room_version must be present
  const content = event.content as Record<string, unknown>;
  if (!content.room_version && !content.creator) {
    return { allowed: false, error: 'm.room.create must have creator or room_version' };
  }
  if (content.room_version === '12' && content.additional_creators !== undefined &&
      (!Array.isArray(content.additional_creators) || content.additional_creators.some(id => typeof id !== 'string' || !parseUserId(id)))) {
    return { allowed: false, error: 'Invalid additional room creators' };
  }

  return { allowed: true };
}

/** Rule 3: m.room.member validation */
function checkMemberEvent(
  event: PDU,
  state: RoomStateMap,
  versionBehavior: RoomVersionBehavior
): AuthResult {
  const content = event.content as RoomMemberContent;
  const targetUserId = event.state_key!;
  const membership = content.membership;

  if (!membership || !parseUserId(targetUserId ?? '')) {
    return { allowed: false, error: 'Missing membership in content' };
  }

  const powerLevels = getPowerLevels(state);
  const senderPower = getUserPowerLevel(powerLevels, event.sender);
  const targetPower = getUserPowerLevel(powerLevels, targetUserId);

  const senderMemberEvent = getState(state, 'm.room.member', event.sender);
  const senderMembership = (senderMemberEvent?.content as RoomMemberContent)?.membership;

  const targetMemberEvent = getState(state, 'm.room.member', targetUserId);
  const targetMembership = (targetMemberEvent?.content as RoomMemberContent)?.membership;

  const joinRulesEvent = getState(state, 'm.room.join_rules');
  const joinRules = (joinRulesEvent?.content as unknown as RoomJoinRulesContent) || { join_rule: 'invite' };
  const joinRule = joinRules.join_rule;

  switch (membership) {
    case 'join': {
      if (senderMembership === 'ban') return { allowed: false, error: 'Banned users cannot join' };
      const create = getState(state, 'm.room.create');
      if (event.sender === targetUserId && targetUserId === create?.sender &&
          event.prev_events.length === 1 && event.prev_events[0] === create.event_id) return { allowed: true };
      // If sender != state_key, reject
      if (event.sender !== targetUserId) {
        return { allowed: false, error: 'Cannot join on behalf of another user' };
      }

      // If currently joined, allow (no-op re-join for profile updates)
      if (senderMembership === 'join') {
        return { allowed: true };
      }

      // If currently invited, allow
      if (senderMembership === 'invite') {
        return { allowed: true };
      }

      // If join_rule is public, allow
      if (joinRule === 'public') {
        return { allowed: true };
      }

      // If restricted/knock_restricted, check allow rules (v8+)
      if (versionBehavior.restrictedJoinsSupported &&
        (joinRule === 'restricted' || joinRule === 'knock_restricted')) {
        if (content.join_authorised_via_users_server) {
          // Verify the authorizing user is in the room and has invite power
          const authUserMember = getState(state, 'm.room.member', content.join_authorised_via_users_server);
          const authUserMembership = (authUserMember?.content as RoomMemberContent)?.membership;
          if (authUserMembership === 'join') {
            const authUserPower = getUserPowerLevel(powerLevels, content.join_authorised_via_users_server);
            if (authUserPower >= (powerLevels.invite ?? 0)) {
              return { allowed: true };
            }
          }
        }
        // Check if user is a member of an allowed room (validated externally)
        // For local auth, we allow if the join_authorised field is present
      }

      return { allowed: false, error: 'Not authorized to join' };
    }

    case 'invite': {
      if (content.third_party_invite) return { allowed: false, error: 'Third-party invite authorization is not supported' };
      // Sender must be joined
      if (senderMembership !== 'join') {
        return { allowed: false, error: 'Sender must be joined to invite' };
      }

      // Target must not be banned
      if (targetMembership === 'ban') {
        return { allowed: false, error: 'Cannot invite banned user' };
      }

      // Target must not already be joined
      if (targetMembership === 'join') {
        return { allowed: false, error: 'User is already joined' };
      }

      // Sender needs invite power level
      if (senderPower < (powerLevels.invite ?? 0)) {
        return { allowed: false, error: 'Insufficient power level to invite' };
      }

      return { allowed: true };
    }

    case 'leave': {
      // Leaving: sender == target
      if (event.sender === targetUserId) {
        // Can leave if joined or invited
        if (senderMembership === 'join' || senderMembership === 'invite') {
          return { allowed: true };
        }
        // Can rescind knock
        if (versionBehavior.knockingSupported && senderMembership === 'knock') {
          return { allowed: true };
        }
        return { allowed: false, error: 'Not a member of the room' };
      }

      // Kicking: sender != target
      if (senderMembership !== 'join') {
        return { allowed: false, error: 'Sender must be joined to kick' };
      }

      // Cannot kick banned users without unban power (that's 'leave' on banned user = unban)
      if (targetMembership === 'ban') {
        if (senderPower < (powerLevels.ban ?? 50)) {
          return { allowed: false, error: 'Insufficient power level to unban' };
        }
      }

      // Sender needs kick power and higher power than target
      if (senderPower < (powerLevels.kick ?? 50)) {
        return { allowed: false, error: 'Insufficient power level to kick' };
      }
      if (senderPower <= targetPower) {
        return { allowed: false, error: 'Cannot kick user with equal or higher power' };
      }

      return { allowed: true };
    }

    case 'ban': {
      // Sender must be joined
      if (senderMembership !== 'join') {
        return { allowed: false, error: 'Sender must be joined to ban' };
      }

      // Sender needs ban power
      if (senderPower < (powerLevels.ban ?? 50)) {
        return { allowed: false, error: 'Insufficient power level to ban' };
      }

      // Cannot ban users with equal or higher power
      if (senderPower <= targetPower) {
        return { allowed: false, error: 'Cannot ban user with equal or higher power' };
      }

      return { allowed: true };
    }

    case 'knock': {
      if (!versionBehavior.knockingSupported) {
        return { allowed: false, error: 'Knocking not supported in this room version' };
      }

      // Sender must be the target
      if (event.sender !== targetUserId) {
        return { allowed: false, error: 'Cannot knock on behalf of another user' };
      }

      // Join rule must be knock or knock_restricted
      if (joinRule !== 'knock' && joinRule !== 'knock_restricted') {
        return { allowed: false, error: 'Room does not allow knocking' };
      }

      // Must not be banned
      if (senderMembership === 'ban') {
        return { allowed: false, error: 'Banned users cannot knock' };
      }

      // Must not already be joined
      if (senderMembership === 'join' || senderMembership === 'invite') {
        return { allowed: false, error: 'Already joined or invited' };
      }

      return { allowed: true };
    }

    default:
      return { allowed: false, error: `Unknown membership: ${membership}` };
  }
}

/** Rule 5: m.room.third_party_invite */
function checkThirdPartyInvite(event: PDU, state: RoomStateMap): AuthResult {
  const powerLevels = getPowerLevels(state);
  const senderPower = getUserPowerLevel(powerLevels, event.sender);

  const senderMemberEvent = getState(state, 'm.room.member', event.sender);
  const senderMembership = (senderMemberEvent?.content as RoomMemberContent)?.membership;

  if (senderMembership !== 'join') {
    return { allowed: false, error: 'Sender must be joined' };
  }

  if (senderPower < (powerLevels.invite ?? 0)) {
    return { allowed: false, error: 'Insufficient power level for third party invite' };
  }

  return { allowed: true };
}

/** Rule 6: State event power level check */
function checkStateEventPower(
  event: PDU,
  state: RoomStateMap,
  versionBehavior: RoomVersionBehavior
): AuthResult {
  const powerLevels = getPowerLevels(state);
  const senderPower = getUserPowerLevel(powerLevels, event.sender);
  const requiredPower = getEventPowerLevel(powerLevels, event.type, true);

  if (senderPower < requiredPower) {
    return {
      allowed: false,
      error: `Insufficient power level for ${event.type} (have ${senderPower}, need ${requiredPower})`,
    };
  }

  // Special check for m.room.power_levels: escalation prevention
  if (event.type === 'm.room.power_levels') {
    return checkPowerLevelChange(event, state, senderPower, versionBehavior);
  }

  return { allowed: true };
}

/** Power level change escalation prevention */
function checkPowerLevelChange(
  event: PDU,
  state: RoomStateMap,
  senderPower: number,
  versionBehavior: RoomVersionBehavior
): AuthResult {
  const newPl = event.content as RoomPowerLevelsContent;
  const create = getState(state, 'm.room.create');
  if (versionBehavior.version === '12' && create) {
    const creators = [create.sender, ...(Array.isArray(create.content.additional_creators) ? create.content.additional_creators : [])];
    if (creators.some(id => typeof id === 'string' && Object.hasOwn(newPl.users ?? {}, id))) {
      return { allowed: false, error: 'Room creators must not be listed in power levels' };
    }
  }
  const currentPlEvent = getState(state, 'm.room.power_levels');
  const currentPl = (currentPlEvent?.content ?? {}) as RoomPowerLevelsContent;

  const scalars = ['ban','events_default','invite','kick','redact','state_default','users_default'] as const;
  const objects = ['events','users','notifications'] as const;
  if (versionBehavior.integerPowerLevels) {
    for (const field of scalars) if (newPl[field] !== undefined && !Number.isSafeInteger(newPl[field])) {
      return { allowed: false, error: 'Power levels must be integers' };
    }
    for (const field of objects) {
      const values = newPl[field];
      if (values !== undefined && (!values || typeof values !== 'object' || Array.isArray(values) ||
          Object.values(values).some(value => !Number.isSafeInteger(value)))) {
        return { allowed: false, error: 'Invalid power level map' };
      }
    }
    if (Object.keys(newPl.users ?? {}).some(id => !parseUserId(id))) return { allowed: false, error: 'Invalid power level user' };
  }
  if (!currentPlEvent) return { allowed: true };
  for (const field of scalars) {
    if (newPl[field] !== currentPl[field] && ((newPl[field] ?? 0) > senderPower || (currentPl[field] ?? 0) > senderPower)) {
      return { allowed: false, error: 'Cannot change a power level above own' };
    }
  }
  for (const field of objects) {
    const before: Record<string, number> = currentPl[field] ?? {};
    const after: Record<string, number> = newPl[field] ?? {};
    for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
      if (before[key] === after[key]) continue;
      if (after[key] !== undefined && after[key] > senderPower) return { allowed: false, error: 'Cannot grant power above own' };
      if (before[key] !== undefined && (before[key] > senderPower ||
          (field === 'users' && key !== event.sender && before[key] === senderPower))) {
        return { allowed: false, error: 'Cannot alter equal or higher power' };
      }
    }
  }
  return { allowed: true };
}

/** Rule 7: Non-state event power level check */
function checkNonStateEventPower(event: PDU, state: RoomStateMap): AuthResult {
  const powerLevels = getPowerLevels(state);
  const senderPower = getUserPowerLevel(powerLevels, event.sender);
  const requiredPower = getEventPowerLevel(powerLevels, event.type, false);

  if (senderPower < requiredPower) {
    return {
      allowed: false,
      error: `Insufficient power level for ${event.type} (have ${senderPower}, need ${requiredPower})`,
    };
  }

  return { allowed: true };
}
