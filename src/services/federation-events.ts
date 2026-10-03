// Matrix wire events for room versions 10–12. Never sign the client-facing event_id.
import type { Env, PDU } from '../types';
import { canonicalJson, signJson, verifySignature } from '../utils/crypto';
import { base64UrlDecode, base64UrlEncode, parseUserId } from '../utils/ids';
import { fetchRemoteServerKeys, fetchHistoricalServerKeys, type SigningKey } from './federation-keys';

export const FEDERATED_ROOM_VERSIONS = ['10', '11', '12'];
export type WireEvent = Record<string, unknown> & {
  type: string; sender: string; content: Record<string, unknown>; depth: number;
  origin_server_ts: number; auth_events: string[]; prev_events: string[];
  room_id?: string; state_key?: string;
  hashes?: { sha256: string }; signatures?: Record<string, Record<string, string>>;
};

export function wireEvent(event: WireEvent | PDU, version: string): WireEvent {
  const result = JSON.parse(JSON.stringify(event)) as WireEvent;
  delete result.event_id;
  if (version === '12' && result.type === 'm.room.create') delete result.room_id;
  return result;
}

export function redactEvent(event: WireEvent, version: string): WireEvent {
  const keys = ['type', 'room_id', 'sender', 'state_key', 'hashes', 'signatures',
    'depth', 'prev_events', 'auth_events', 'origin_server_ts'];
  if (version === '10') keys.push('origin', 'prev_state', 'membership');
  const result: Record<string, unknown> = {};
  for (const key of keys) if (event[key] !== undefined) result[key] = event[key];
  const content: Record<string, unknown> = {};
  const allowed: Record<string, string[]> = {
    'm.room.member': ['membership', 'join_authorised_via_users_server'],
    'm.room.create': ['creator'],
    'm.room.join_rules': ['join_rule', 'allow'],
    // The invite level is protected from redaction starting with room version 11.
    'm.room.power_levels': ['ban', 'events', 'events_default', 'kick', 'redact', 'state_default', 'users', 'users_default',
      ...(version === '10' ? [] : ['invite'])],
    'm.room.history_visibility': ['history_visibility'],
    'm.room.redaction': version === '10' ? [] : ['redacts'],
  };
  for (const key of allowed[event.type] ?? []) {
    if (event.content[key] !== undefined) content[key] = event.content[key];
  }
  if (version !== '10' && event.type === 'm.room.member') {
    const invite = event.content.third_party_invite;
    if (isObject(invite) && invite.signed !== undefined) content.third_party_invite = { signed: invite.signed };
  }
  result.content = version !== '10' && event.type === 'm.room.create' ? event.content : content;
  return result as WireEvent;
}

export function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

async function digest(value: unknown) {
  return new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(canonicalJson(value))));
}
export async function eventReferenceId(event: WireEvent, version: string): Promise<string> {
  const redacted = redactEvent(wireEvent(event, version), version);
  delete redacted.signatures;
  delete redacted.unsigned;
  return `$${base64UrlEncode(await digest(redacted))}`;
}
export async function eventContentHash(event: WireEvent): Promise<string> {
  const copy = { ...event };
  delete copy.unsigned;
  delete copy.signatures;
  delete copy.hashes;
  delete copy.event_id;
  return btoa(String.fromCharCode(...await digest(copy))).replace(/=+$/, '');
}
export async function signEvent(event: WireEvent, version: string, server: string, key: SigningKey): Promise<WireEvent> {
  const result = wireEvent(event, version);
  delete result.signatures;
  delete result.unsigned;
  result.hashes = { sha256: await eventContentHash(result) };
  const signed = await signJson(redactEvent(result, version), server, key.keyId, key.privateKeyJwk);
  result.signatures = signed.signatures as WireEvent['signatures'];
  return result;
}

// Each join reuses key lookups; it never treats an arbitrary cosigner as the sender.
export function eventVerifier(env: Env) {
  const keys = new Map<string, ReturnType<typeof fetchRemoteServerKeys>>();
  const historical = new Map<string, ReturnType<typeof fetchHistoricalServerKeys>>();
  return async (raw: unknown, version: string, roomId: string): Promise<PDU> => {
    if (!FEDERATED_ROOM_VERSIONS.includes(version) || !isObject(raw) ||
        typeof raw.type !== 'string' || typeof raw.sender !== 'string' || !parseUserId(raw.sender) ||
        !isObject(raw.content) || !Number.isSafeInteger(raw.depth) || Number(raw.depth) < 0 ||
        !Number.isSafeInteger(raw.origin_server_ts) ||
        (raw.state_key !== undefined && typeof raw.state_key !== 'string') ||
        !Array.isArray(raw.auth_events) || raw.auth_events.length > 10 || !raw.auth_events.every(e => typeof e === 'string') ||
        !Array.isArray(raw.prev_events) || raw.prev_events.length > 20 || !raw.prev_events.every(e => typeof e === 'string') ||
        !isObject(raw.hashes) || typeof raw.hashes.sha256 !== 'string' || !isObject(raw.signatures)) {
      throw new Error('Malformed federation event');
    }
    const event = wireEvent(raw as WireEvent, version);
    if (new TextEncoder().encode(canonicalJson(raw)).length > 65536) throw new Error('Federation event exceeds 64 KiB');
    if (event.type === 'm.room.create' && version === '12') {
      if (raw.room_id !== undefined || roomId !== `!${(await eventReferenceId(event, version)).slice(1)}`) {
        throw new Error('Create event does not match room ID');
      }
    } else if (event.room_id !== roomId) throw new Error('Event belongs to another room');
    const eventId = await eventReferenceId(event, version);
    if (raw.event_id !== undefined && raw.event_id !== eventId) throw new Error('Event ID mismatch');
    const redacted = redactEvent(event, version);
    const servers = [parseUserId(event.sender)!.serverName];
    if (event.type === 'm.room.member' && event.content.membership === 'join' && event.content.join_authorised_via_users_server) {
      const authorizer = parseUserId(String(event.content.join_authorised_via_users_server));
      if (!authorizer) throw new Error('Invalid restricted join authorizer');
      servers.push(authorizer.serverName);
    }
    for (const server of new Set(servers)) {
      if (!keys.has(server)) {
        const load = server === env.SERVER_NAME
          ? env.DB.prepare('SELECT key_id,public_key,valid_until FROM server_keys').all<{key_id:string;public_key:string;valid_until:number|null}>()
            .then(result => result.results.map(key => ({ server_name:server, key_id:key.key_id,public_key:key.public_key,
              valid_from:0,valid_until:key.valid_until,fetched_at:Date.now(),verified:true })))
          : fetchRemoteServerKeys(server, env.DB, env.CACHE);
        keys.set(server, load);
      }
      const candidates = [...await keys.get(server)!];
      if (server !== env.SERVER_NAME) {
        for (const id of Object.keys(event.signatures?.[server] ?? {}).slice(0, 8)) {
          if (!candidates.some(key => key.key_id === id && (key.valid_until === null || key.valid_until >= event.origin_server_ts))) {
            const key = `${server}:${id}`;
            if (!historical.has(key)) historical.set(key, fetchHistoricalServerKeys(server,id,env.DB,env.CACHE));
            candidates.push(...await historical.get(key)!);
          }
        }
      }
      let valid = false;
      for (const key of candidates) {
        if (!key.key_id.startsWith('ed25519:') || !event.signatures?.[server]?.[key.key_id] ||
            (key.valid_until !== null && key.valid_until < event.origin_server_ts)) continue;
        if (await verifySignature(redacted, server, key.key_id, key.public_key)) { valid = true; break; }
      }
      if (!valid) throw new Error(`Invalid event signature from ${server}`);
    }
    // A mismatching content hash is a redacted event, never trusted full content.
    const actual = base64UrlEncode(base64UrlDecode(await eventContentHash(event)));
    const expected = base64UrlEncode(base64UrlDecode(event.hashes!.sha256));
    const accepted = actual === expected ? event : redacted;
    return { ...accepted, event_id: eventId, room_id: roomId } as PDU;
  };
}
