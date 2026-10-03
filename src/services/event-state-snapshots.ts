import type { PDU } from '../types';
import { getEvent, getEventsByIds } from './database';
import { eventContentHash, wireEvent } from './federation-events';
import { Errors } from '../utils/errors';

// A tuple retains the state key alongside its reference ID, avoiding loading an
// entire room's PDUs whenever a new timeline event copies its parent's state.
type StateEntry = [type: string, stateKey: string, eventId: string];
type StateMap = Map<string, StateEntry>;
const MAX_RECONSTRUCTION_EVENTS = 256;
const MAX_STATE_EVENTS = 10000;

class UnavailableState extends Error {}
const stateKey = (type: string, key: string) => JSON.stringify([type, key]);
function stateMap(events: PDU[], roomId: string): StateMap {
  const result: StateMap = new Map();
  for (const event of events) {
    if (event.room_id !== roomId || event.state_key === undefined) throw Errors.invalidParam('state', 'Snapshot contains an invalid state event');
    const key = stateKey(event.type, event.state_key);
    if (result.has(key)) throw Errors.invalidParam('state', 'Snapshot contains duplicate state keys');
    result.set(key, [event.type, event.state_key, event.event_id]);
  }
  if (result.size > MAX_STATE_EVENTS) throw Errors.tooLarge('Historical state exceeds the snapshot limit');
  return result;
}
function snapshotStatement(db: D1Database, eventId: string, roomId: string, state: StateMap): D1PreparedStatement {
  return db.prepare(`INSERT INTO event_state_snapshots(event_id,room_id,state_before,created_at) VALUES(?,?,?,?)
    ON CONFLICT(event_id) DO NOTHING`).bind(eventId, roomId, JSON.stringify([...state.values()]), Date.now());
}

async function archivedEvent(db: D1Database, eventId: string, roomId: string): Promise<PDU | null> {
  const event = await getEvent(db, eventId);
  if (event) return event.room_id === roomId ? event : null;
  const archived = await db.prepare('SELECT event_json FROM event_state_archive WHERE event_id=? AND room_id=?')
    .bind(eventId, roomId).first<{ event_json: string }>();
  return archived ? JSON.parse(archived.event_json) as PDU : null;
}
function reconstruction(db: D1Database, roomId: string) {
  let traversed = 0;
  const pending = new Set<string>();
  const memo = new Map<string, StateMap>();
  async function before(event: PDU): Promise<StateMap> {
    if (event.room_id !== roomId) throw new UnavailableState('Event belongs to another room');
    if (memo.has(event.event_id)) return new Map(memo.get(event.event_id)!);
    if (pending.has(event.event_id)) throw new UnavailableState('Event graph contains a cycle');
    if (++traversed > MAX_RECONSTRUCTION_EVENTS) throw new UnavailableState('Historical reconstruction limit reached');
    const stored = await db.prepare('SELECT state_before FROM event_state_snapshots WHERE event_id=? AND room_id=?')
      .bind(event.event_id, roomId).first<{ state_before: string }>();
    if (stored) {
      const entries = JSON.parse(stored.state_before) as StateEntry[];
      const result = new Map(entries.map(entry => [stateKey(entry[0], entry[1]), entry]));
      memo.set(event.event_id, result); return new Map(result);
    }
    if (!event.prev_events.length) {
      if (event.type !== 'm.room.create' || event.state_key !== '') throw new UnavailableState('Historical predecessors are unavailable');
      memo.set(event.event_id, new Map()); return new Map();
    }
    pending.add(event.event_id);
    try {
      const parents: StateMap[] = [];
      for (const id of [...new Set(event.prev_events)]) {
        const parent = await archivedEvent(db, id, roomId);
        if (!parent || parent.depth >= event.depth) throw new UnavailableState('Historical predecessor is unavailable or invalid');
        const state = await before(parent);
        if (parent.state_key !== undefined) state.set(stateKey(parent.type, parent.state_key), [parent.type, parent.state_key, parent.event_id]);
        parents.push(state);
      }
      const first = parents[0];
      // The existing v2 resolver does not implement the complete auth-chain-diff
      // and mainline algorithms. Only identical parent states are provably exact.
      if (parents.some(parent => parent.size !== first.size || [...parent].some(([key, entry]) => first.get(key)?.[2] !== entry[2]))) {
        throw new UnavailableState('Conflicting historical branches require a verified state snapshot');
      }
      memo.set(event.event_id, first); return new Map(first);
    } finally { pending.delete(event.event_id); }
  }
  return before;
}

// This statement belongs in the same D1 batch as event and membership publication.
// Missing or conflicting history must not be replaced by today's room state.
export async function prepareEventStateSnapshot(db: D1Database, event: PDU): Promise<D1PreparedStatement | null> {
  try { return snapshotStatement(db, event.event_id, event.room_id, await reconstruction(db, event.room_id)(event)); }
  catch (error) { if (error instanceof UnavailableState) return null; throw error; }
}

// Only call after verifying every peer PDU and the complete authorization graph.
export async function prepareVerifiedStateSnapshot(
  db: D1Database, event: PDU, state: PDU[], auth: PDU[], version: string
): Promise<D1PreparedStatement[]> {
  const statements: D1PreparedStatement[] = [];
  for (const archived of new Map([...state, ...auth].map(pdu => [pdu.event_id, pdu])).values()) {
    if (archived.room_id !== event.room_id) throw Errors.invalidParam('state', 'Snapshot event belongs to another room');
    const actual = archived.hashes ? await eventContentHash(wireEvent(archived, version)) : null;
    const expected = archived.hashes?.sha256;
    const normalize = (hash: string) => hash.replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
    const redacted = actual !== null && expected !== undefined && normalize(actual) !== normalize(expected) ? 1 : 0;
    statements.push(db.prepare(`INSERT INTO event_state_archive(event_id,room_id,event_json,redacted) VALUES(?,?,?,?)
      ON CONFLICT(event_id) DO UPDATE SET event_json=excluded.event_json,redacted=excluded.redacted
      WHERE event_state_archive.room_id=excluded.room_id AND event_state_archive.redacted<=excluded.redacted`)
      .bind(archived.event_id, event.room_id, JSON.stringify(archived), redacted));
  }
  statements.push(snapshotStatement(db, event.event_id, event.room_id, stateMap(state, event.room_id)));
  return statements;
}

export async function getStateBeforeEvent(db: D1Database, roomId: string, eventId: string): Promise<PDU[]> {
  const event = await archivedEvent(db, eventId, roomId);
  // Backfill snapshot metadata can exist without publishing the historical message
  // globally. Its archived state still provides the exact requested federation state.
  const saved = await db.prepare('SELECT state_before FROM event_state_snapshots WHERE event_id=? AND room_id=?')
    .bind(eventId, roomId).first<{ state_before: string }>();
  let state: StateMap;
  if (saved) {
    const entries = JSON.parse(saved.state_before) as StateEntry[];
    state = new Map(entries.map(entry => [stateKey(entry[0], entry[1]), entry]));
  } else {
    if (!event) throw Errors.notFound('Event or historical state snapshot not found in room');
    try { state = await reconstruction(db, roomId)(event); }
    catch (error) {
      if (error instanceof UnavailableState) throw Errors.notFound('Exact historical state is unavailable for this event');
      throw error;
    }
    await db.batch([snapshotStatement(db, eventId, roomId, state)]);
  }
  const ids = [...state.values()].map(entry => entry[2]);
  const events = await getSnapshotEventsByIds(db, roomId, ids);
  if (events.length !== ids.length) throw Errors.notFound('Historical state snapshot is incomplete');
  return events;
}

// Historical peer state/auth PDUs are archived separately from the client timeline.
export async function getSnapshotEventsByIds(db: D1Database, roomId: string, ids: string[]): Promise<PDU[]> {
  const result: PDU[] = [];
  // The room ID is another bound parameter; D1 allows at most 100.
  for (let offset = 0; offset < ids.length; offset += 99) {
    const batch = ids.slice(offset, offset + 99);
    const live = new Map((await getEventsByIds(db, batch)).filter(pdu => pdu.room_id === roomId).map(pdu => [pdu.event_id, pdu]));
    const placeholders = batch.map(() => '?').join(',');
    const archived = await db.prepare(`SELECT event_id,event_json FROM event_state_archive WHERE room_id=? AND event_id IN (${placeholders})`)
      .bind(roomId, ...batch).all<{ event_id: string; event_json: string }>();
    const archive = new Map(archived.results.map(row => [row.event_id, JSON.parse(row.event_json) as PDU]));
    for (const id of batch) {
      const current = live.get(id);
      // A later authorized redaction also applies when serving older snapshots.
      const pdu = current?.unsigned?.redacted_because ? current : archive.get(id) ?? current;
      if (pdu) result.push(pdu);
    }
  }
  return result;
}
