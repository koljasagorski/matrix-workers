import { afterEach, beforeEach, expect, it } from 'vitest';
import type { PDU } from '../src/types';
import { getEvent, getEventsSince, getLatestStreamPosition, getRoomEvents, getStateEvent, storeEvent } from '../src/services/database';
import { testEnv } from './federation-helpers';

const roomId = '!storage:local.example';
let ctx: Awaited<ReturnType<typeof testEnv>>;

beforeEach(async () => {
  ctx = await testEnv();
  ctx.sqlite.prepare('INSERT INTO rooms(room_id) VALUES (?)').run(roomId);
});
afterEach(() => ctx.sqlite.close());

function message(id: string): PDU {
  return {
    event_id: id, room_id: roomId, type: 'm.room.message', sender: '@alice:local.example',
    content: { msgtype: 'm.text', body: id }, origin_server_ts: 1, depth: 1,
    auth_events: [], prev_events: [],
  };
}

it('gives concurrent writes distinct positions above imported events and never skips a pagination boundary', async () => {
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
    VALUES (?,?,'@alice:local.example','m.room.message','{}',1,1,'[]','[]',?)`)
    .run('$imported', roomId, 42);
  // Historical/auth-chain imports do not belong to the live sync stream.
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events)
    VALUES ('$history',?,'@alice:local.example','m.room.message','{}',1,9999,'[]','[]')`).run(roomId);

  const ids = Array.from({ length: 12 }, (_, i) => `$concurrent-${i}`);
  const positions = await Promise.all(ids.map(id => storeEvent(ctx.env.DB, message(id))));
  expect(positions).toEqual(Array.from({ length: 12 }, (_, i) => 43 + i));
  expect(await getLatestStreamPosition(ctx.env.DB)).toBe(54);
  expect((await getEventsSince(ctx.env.DB, roomId, 42)).map(event => event.event_id)).toEqual(ids);

  const paginated: string[] = [];
  let cursor = 42;
  for (let i = 0; i < ids.length; i++) {
    const page = await getRoomEvents(ctx.env.DB, roomId, cursor, 1, 'f');
    expect(page.events).toHaveLength(1);
    paginated.push(page.events[0].event_id);
    expect(page.end).toBe(cursor + 1);
    cursor = page.end;
  }
  expect(paginated).toEqual(ids);
});

it('delivers the second concurrent event after a client acknowledges the first sync position', async () => {
  const [first, second] = await Promise.all([
    storeEvent(ctx.env.DB, message('$first')),
    storeEvent(ctx.env.DB, message('$second')),
  ]);
  expect((await getEventsSince(ctx.env.DB, roomId, 0, 100, first)).map(event => event.event_id)).toEqual(['$first']);
  expect((await getEventsSince(ctx.env.DB, roomId, first, 100, second)).map(event => event.event_id)).toEqual(['$second']);
});

it('rolls back event publication when updating current state fails and accepts a subsequent batch', async () => {
  const oldState: PDU = { ...message('$old-state'), type: 'm.room.name', state_key: '', content: { name: 'Original' } };
  expect(await storeEvent(ctx.env.DB, oldState)).toBe(1);
  ctx.sqlite.exec(`CREATE TRIGGER reject_state BEFORE INSERT ON room_state
    WHEN NEW.event_id = '$rejected-state' BEGIN SELECT RAISE(ABORT, 'state update rejected'); END;`);

  const rejected: PDU = { ...oldState, event_id: '$rejected-state', content: { name: 'Rejected' } };
  await expect(storeEvent(ctx.env.DB, rejected)).rejects.toThrow('state update rejected');
  expect(await getEvent(ctx.env.DB, rejected.event_id)).toBeNull();
  expect(await getLatestStreamPosition(ctx.env.DB)).toBe(1);
  expect((await getStateEvent(ctx.env.DB, roomId, 'm.room.name', ''))?.event_id).toBe(oldState.event_id);
  expect(await getEventsSince(ctx.env.DB, roomId, 1)).toEqual([]);

  const accepted = { ...oldState, event_id: '$accepted-state', content: { name: 'Accepted' } };
  expect(await storeEvent(ctx.env.DB, accepted)).toBe(2);
  expect((await getStateEvent(ctx.env.DB, roomId, 'm.room.name', ''))?.content).toEqual({ name: 'Accepted' });
});

it('does not replace state or allocate a new position when an event id is already stored', async () => {
  const original: PDU = { ...message('$same-event'), type: 'm.room.name', state_key: '', content: { name: 'Original' } };
  await storeEvent(ctx.env.DB, original);
  await expect(storeEvent(ctx.env.DB, { ...original, content: { name: 'Overwrite' } })).rejects.toThrow();
  expect(await getLatestStreamPosition(ctx.env.DB)).toBe(1);
  expect((await getStateEvent(ctx.env.DB, roomId, 'm.room.name', ''))?.content).toEqual({ name: 'Original' });
});
