import type { Env, PDU } from '../types';
import { Errors, MatrixApiError } from '../utils/errors';
import { hashToken } from '../utils/crypto';
import { parseUserId } from '../utils/ids';
import { getRoom } from './database';
import { eventVerifier, FEDERATED_ROOM_VERSIONS, isObject } from './federation-events';
import { readFederationJson } from './federation-http';
import { getServerSigningKey, makeFederationRequest } from './federation-keys';
import { locateRoom, validateJoinGraph } from './remote-rooms';

// History is fetched on demand, separately from the live stream. In particular,
// old memberships must never replace current memberships or trigger notifications.
const TTL = 7 * 86400;
const PAGE_SIZE = 20;
interface Cursor { roomId: string; userId: string; frontier: PDU[]; seen: string[] }
export interface HistoryPage { events: PDU[]; end?: string }

function cursorKey(token: string) { return `room-history:cursor:${token}`; }
async function eventKey(roomId: string, userId: string, eventId: string) {
  return `room-history:event:${await hashToken(JSON.stringify([roomId, userId, eventId]))}`;
}

export async function getHistoricalEvent(env: Env, roomId: string, userId: string, eventId: string): Promise<PDU | null> {
  return env.CACHE.get(await eventKey(roomId, userId, eventId), 'json');
}

export async function getRemoteHistory(
  env: Env, roomId: string, userId: string, anchor: PDU | string, limit: number,
): Promise<HistoryPage> {
  let cursor: Cursor;
  if (typeof anchor === 'string') {
    if (!/^rh_[0-9a-f-]{36}$/.test(anchor)) throw Errors.invalidParam('from');
    const saved = await env.CACHE.get<Cursor>(cursorKey(anchor), 'json');
    if (!saved || saved.roomId !== roomId || saved.userId !== userId) {
      throw Errors.invalidParam('from', 'History position expired or belongs to another room or user');
    }
    cursor = saved;
  } else {
    if (anchor.room_id !== roomId) throw Errors.invalidParam('from');
    cursor = { roomId, userId, frontier: [anchor], seen: [anchor.event_id] };
  }
  if (!cursor.frontier.some(event => event.prev_events.length)) return { events: [] };
  const room = await getRoom(env.DB, roomId);
  if (!room || !FEDERATED_ROOM_VERSIONS.includes(room.room_version)) return { events: [] };
  const count = Math.max(1, Math.min(PAGE_SIZE, limit));
  const pageKey = `room-history:page:${await hashToken(JSON.stringify([roomId, userId, cursor, count]))}`;
  const cached = await env.CACHE.get<HistoryPage>(pageKey, 'json');
  if (cached) return cached;
  const location = await locateRoom(env, roomId, room.creator_id ? [parseUserId(room.creator_id)?.serverName ?? ''] : []);
  if (!location.servers.length) return { events: [] };
  const key = await getServerSigningKey(env.DB);
  if (!key) throw new Error('Server signing key unavailable');
  const signal = AbortSignal.timeout(25000);
  const verify = eventVerifier(env);
  // Repeated state/auth events are verified once per request, including concurrent
  // snapshots. The complete wire representation is the key (not an untrusted ID).
  const verified = new Map<string, Promise<PDU>>();
  const verifyOnce = (raw: unknown) => {
    const key = JSON.stringify(raw);
    let result = verified.get(key);
    if (!result) { result = verify(raw, room.room_version, roomId); verified.set(key, result); }
    return result;
  };
  async function verifyAll(raw: unknown[]): Promise<PDU[]> {
    const result: PDU[] = [];
    for (let i = 0; i < raw.length; i += 4) {
      signal.throwIfAborted();
      result.push(...await Promise.all(raw.slice(i, i + 4).map(verifyOnce)));
    }
    return result;
  }
  let failure: unknown;
  for (const server of location.servers.slice(0, 2)) {
    try {
      async function request(path: string, maxBytes = 2 * 1024 * 1024) {
        signal.throwIfAborted();
        const response = await makeFederationRequest('GET', server, path, env.SERVER_NAME, key!, env.CACHE, undefined, signal);
        const body = await readFederationJson(response, maxBytes);
        if (!response.ok || !isObject(body)) throw new Error(`History peer returned HTTP ${response.status}`);
        return body;
      }
      const query = new URLSearchParams({ limit: String(Math.min(100, count + cursor.frontier.length)) });
      for (const event of cursor.frontier) query.append('v', event.event_id);
      const response = await request(`/_matrix/federation/v1/backfill/${encodeURIComponent(roomId)}?${query}`);
      if (!Array.isArray(response.pdus) || response.pdus.length > 100) throw new Error('Invalid history response');
      const pdus = await verifyAll(response.pdus);
      const byId = new Map(pdus.map(event => [event.event_id, event]));
      const seen = new Set(cursor.seen);
      for (const event of cursor.frontier) seen.add(event.event_id);
      const reachable = new Map<string, PDU>();
      const pending = [...cursor.frontier];
      while (pending.length) {
        const child = pending.pop()!;
        for (const id of child.prev_events) {
          const parent = byId.get(id);
          if (!parent || seen.has(id) || reachable.has(id)) continue;
          if (parent.depth >= child.depth) throw new Error('Invalid history event depth');
          reachable.set(id, parent);
          pending.push(parent);
        }
      }
      const selected = [...reachable.values()].sort((a, b) => b.depth - a.depth || b.event_id.localeCompare(a.event_id)).slice(0, count);
      if (!selected.length) return { events: [] };

      // Visibility is evaluated at each event, not using today's setting. State
      // snapshots also supply the full auth chain for validating historical PDUs.
      // Bound concurrency and page size to keep this within Workers limits.
      const visible: PDU[] = [];
      for (let offset = 0; offset < selected.length; offset += 4) {
        const results = await Promise.all(selected.slice(offset, offset + 4).map(async event => {
          const snapshot = await request(`/_matrix/federation/v1/state/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(event.event_id)}`);
          if (!Array.isArray(snapshot.pdus) || !Array.isArray(snapshot.auth_chain) ||
              snapshot.pdus.length + snapshot.auth_chain.length > 5000) throw new Error('Invalid historical state');
          const auth = await verifyAll(snapshot.auth_chain);
          const state = await verifyAll(snapshot.pdus);
          validateJoinGraph([...auth, ...state, event], state, event, room.room_version);
          const history = state.find(e => e.type === 'm.room.history_visibility' && e.state_key === '');
          const visibility = history?.content.history_visibility ?? 'shared';
          const member = state.find(e => e.type === 'm.room.member' && e.state_key === userId);
          const membership = member?.content.membership;
          const visible = visibility === 'shared' || visibility === 'world_readable' || membership === 'join' ||
            (visibility === 'invited' && membership === 'invite');
          // A visibility/membership transition is itself visible under the less
          // restrictive of its before/after states.
          const transition = (event.type === 'm.room.member' && event.state_key === userId &&
            (event.content.membership === 'join' || (visibility === 'invited' && event.content.membership === 'invite'))) ||
            (event.type === 'm.room.history_visibility' && event.state_key === '' &&
              ['shared', 'world_readable'].includes(String(event.content.history_visibility)));
          return visible || transition ? event : null;
        }));
        visible.push(...results.filter((e): e is PDU => e !== null));
      }
      for (const event of selected) seen.add(event.event_id);
      // Keep all branches with an unvisited predecessor; never jump across a DAG
      // gap merely because a peer returned an event with a smaller depth.
      const frontier = [...cursor.frontier, ...selected].filter(e => e.prev_events.some(id => !seen.has(id)));
      if (frontier.length > 32) throw new Error('History graph has too many unresolved branches');
      const page: HistoryPage = { events: visible };
      if (frontier.length) {
        page.end = `rh_${crypto.randomUUID()}`;
        await env.CACHE.put(cursorKey(page.end), JSON.stringify({ roomId, userId, frontier, seen: [...seen].slice(-2000) } satisfies Cursor), { expirationTtl: TTL });
      }
      // User-scoped cache also supports opening a just-paginated event directly.
      await Promise.all(visible.map(async event => env.CACHE.put(await eventKey(roomId, userId, event.event_id), JSON.stringify(event), { expirationTtl: TTL })));
      await env.CACHE.put(pageKey, JSON.stringify(page), { expirationTtl: 3600 });
      return page;
    } catch (error) { failure = error; }
  }
  console.error('[history] Backfill failed:', failure instanceof Error ? failure.message : 'Remote history unavailable');
  // A temporary peer failure must not tell the client it reached the beginning.
  throw new MatrixApiError('M_UNKNOWN', 'Room history is temporarily unavailable; please retry', 502);
}
