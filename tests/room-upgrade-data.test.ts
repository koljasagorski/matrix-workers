import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import rooms from '../src/api/rooms';
import { getUserPushRules } from '../src/api/push';
import { getRoomState, storeEvent, updateMembership } from '../src/services/database';
import { checkEventAuth } from '../src/services/event-auth';
import { storeAccountData } from '../src/services/account-data-stream';
import { migrateRoomUpgradeData } from '../src/services/room-upgrade-data';
import { sendLocalRoomEvent } from '../src/services/local-room-events';
import { eventReferenceId, redactEvent, signEvent, wireEvent } from '../src/services/federation-events';
import { generateSigningKeyPair, verifySignature } from '../src/utils/crypto';
import type { PDU } from '../src/types';
import { testEnv } from './federation-helpers';

const alice = '@alice:local.example';
const bob = '@bob:local.example';
let ctx: Awaited<ReturnType<typeof testEnv>>;
let oldRoom: string;
let newRoom: string;
let wakes: string[];
let baselinePosition: number;

beforeEach(async () => {
  ctx = await testEnv(); wakes = [];
  vi.spyOn(console, 'log').mockImplementation(() => {});
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(bob, 'bob');
  ctx.env.PUSH_NOTIFICATION_WORKFLOW = { create: async () => ({}) } as any;
  ctx.env.FEDERATION = { idFromName: (id: string) => id, get: () => ({ fetch: async () => Response.json({}) }) } as any;
  ctx.env.SYNC = { idFromName: (id: string) => id, get: (id: string) => ({ fetch: async () => {
    wakes.push(id); return Response.json({});
  } }) } as any;
  [oldRoom, newRoom] = await linkedRooms('10'); wakes = [];
  baselinePosition = (ctx.sqlite.prepare("SELECT position FROM stream_positions WHERE stream_name='account_data'").get() as { position: number }).position;
});
afterEach(() => { ctx.sqlite.close(); vi.restoreAllMocks(); });

async function create(version: string, predecessor?: string) {
  const response = await rooms.request('/_matrix/client/v3/createRoom', {
    method: 'POST', headers: { Authorization: 'Bearer token' }, body: JSON.stringify({ room_version: version,
      preset: 'public_chat', ...(predecessor ? { creation_content: { predecessor: { room_id: predecessor } } } : {}) }),
  }, ctx.env);
  expect(response.status).toBe(200);
  return (await response.json() as { room_id: string }).room_id;
}
async function linkedRooms(version: string) {
  const previous = await create(version);
  const replacement = await create('12', previous);
  await sendLocalRoomEvent(ctx.env, { roomId: previous, sender: alice, type: 'm.room.tombstone', stateKey: '',
    content: { body: 'Replaced', replacement_room: replacement } });
  return [previous, replacement];
}
function account(user: string, room: string, type: string, content: unknown) {
  ctx.sqlite.prepare(`INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,?,?,?)
    ON CONFLICT(user_id,room_id,event_type) DO UPDATE SET content=excluded.content`).run(user, room, type, JSON.stringify(content));
}
function accountContent(user: string, room: string, type: string) {
  const row = ctx.sqlite.prepare('SELECT content FROM account_data WHERE user_id=? AND room_id=? AND event_type=?')
    .get(user, room, type) as { content: string } | undefined;
  return row ? JSON.parse(row.content) : undefined;
}
function position() {
  const row = ctx.sqlite.prepare("SELECT position FROM stream_positions WHERE stream_name='account_data'").get() as { position: number };
  return { position: row.position - baselinePosition };
}
function migrate() { return migrateRoomUpgradeData(ctx.env, { oldRoomId: oldRoom, newRoomId: newRoom, actorUserId: alice }); }
function alias(name: string, creator: string | null = alice) {
  ctx.sqlite.prepare('INSERT INTO room_aliases(alias,room_id,creator_id,created_at) VALUES (?,?,?,123)').run(name, oldRoom, creator);
}
function pushRule(room: string, actions: string[], priority = 37, enabled = 0) {
  ctx.sqlite.prepare(`INSERT INTO push_rules(user_id,rule_id,kind,priority,conditions,actions,enabled)
    VALUES (?,?,'room',?,NULL,?,?)`).run(alice, room, priority, JSON.stringify(actions), enabled);
}

it('copies tags into the replacement, preserves new preferences and keeps old read markers and history in the original room', async () => {
  const source = { tags: { 'm.favourite': { order: 0.2 }, 'u.work': { order: 0.4 } } };
  account(alice, oldRoom, 'm.tag', source);
  account(alice, newRoom, 'm.tag', { tags: { 'm.favourite': { order: 0.7 }, 'u.new': {} } });
  account(alice, oldRoom, 'm.fully_read', { event_id: '$old-marker' });
  account(alice, oldRoom, 'com.example.event-position', { event_id: '$old-position' });
  const newMarker = accountContent(alice, newRoom, 'm.fully_read');
  const history = ctx.sqlite.prepare('SELECT * FROM events WHERE room_id=? ORDER BY event_id').all(oldRoom);
  const membership = ctx.sqlite.prepare('SELECT * FROM room_memberships ORDER BY room_id,user_id').all();
  expect(await migrate()).toMatchObject({ usersUpdated: 1 });
  expect(accountContent(alice, newRoom, 'm.tag')).toEqual({ tags: {
    'm.favourite': { order: 0.7 }, 'u.work': { order: 0.4 }, 'u.new': {},
  } });
  expect(accountContent(alice, oldRoom, 'm.tag')).toEqual(source);
  expect(accountContent(alice, oldRoom, 'm.fully_read')).toEqual({ event_id: '$old-marker' });
  expect(accountContent(alice, newRoom, 'm.fully_read')).toEqual(newMarker);
  expect(accountContent(alice, newRoom, 'com.example.event-position')).toBeUndefined();
  expect(ctx.sqlite.prepare('SELECT * FROM events WHERE room_id=? ORDER BY event_id').all(oldRoom)).toEqual(history);
  expect(ctx.sqlite.prepare('SELECT * FROM room_memberships ORDER BY room_id,user_id').all()).toEqual(membership);
  expect(position()).toEqual({ position: 1 });
  expect(ctx.sqlite.prepare('SELECT user_id,room_id,event_type,stream_position FROM account_data_changes WHERE stream_position>?').all(baselinePosition))
    .toEqual([{ user_id: alice, room_id: newRoom, event_type: 'm.tag', stream_position: baselinePosition + 1 }]);
});

it('appends the replacement to every matching direct chat without losing old rooms or adding duplicate entries on retries', async () => {
  const direct = { [bob]: [oldRoom, '!unrelated:local.example'], '@peer:remote.example': [oldRoom, newRoom], '@another:remote.example': ['!other:remote.example'] };
  account(alice, '', 'm.direct', direct);
  account(alice, oldRoom, 'm.tag', { tags: { 'm.favourite': { order: 0.1 } } });
  await migrate();
  expect(accountContent(alice, '', 'm.direct')).toEqual({ ...direct, [bob]: [oldRoom, '!unrelated:local.example', newRoom] });
  expect(accountContent(alice, newRoom, 'm.tag')).toEqual({ tags: { 'm.favourite': { order: 0.1 } } });
  expect(position()).toEqual({ position: 2 });
  expect(await migrate()).toMatchObject({ usersUpdated: 0 });
  expect(position()).toEqual({ position: 2 });
  expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM account_data_changes WHERE stream_position>?').get(baselinePosition)).toEqual({ n: 2 });
  expect(wakes).toEqual([alice, alice]);
});

it('copies room notification rules with priority/actions/enabled intact and publishes the resulting push account data', async () => {
  pushRule(oldRoom, ['dont_notify']);
  await migrate();
  for (const room of [oldRoom, newRoom]) expect(ctx.sqlite.prepare('SELECT kind,priority,conditions,actions,enabled FROM push_rules WHERE user_id=? AND rule_id=?')
    .get(alice, room)).toEqual({ kind: 'room', priority: 37, conditions: null, actions: '["dont_notify"]', enabled: 0 });
  const rules = await getUserPushRules(ctx.env.DB, alice);
  expect(rules.global.room).toEqual(expect.arrayContaining([
    expect.objectContaining({ rule_id: oldRoom, actions: ['dont_notify'], enabled: false }),
    expect.objectContaining({ rule_id: newRoom, actions: ['dont_notify'], enabled: false }),
  ]));
  expect(accountContent(alice, '', 'm.push_rules')).toEqual(rules);
  expect(position()).toEqual({ position: 1 });
  await migrate(); expect(position()).toEqual({ position: 1 });
});

it('preserves an already configured notification rule in the replacement room', async () => {
  pushRule(oldRoom, ['dont_notify']); pushRule(newRoom, ['notify'], 9, 1);
  await migrate();
  expect(ctx.sqlite.prepare('SELECT priority,actions,enabled FROM push_rules WHERE user_id=? AND rule_id=?').get(alice, newRoom))
    .toEqual({ priority: 9, actions: '["notify"]', enabled: 1 });
});

it('builds the push-rule snapshot after its CAS baseline so concurrent notification edits are preserved', async () => {
  pushRule(oldRoom, ['dont_notify']);
  const prepare = ctx.env.DB.prepare.bind(ctx.env.DB);
  let injected = false;
  vi.spyOn(ctx.env.DB, 'prepare').mockImplementation(sql => {
    const statement = prepare(sql);
    if (sql.startsWith('SELECT content FROM account_data WHERE user_id=? AND room_id=? AND event_type=?')) {
      const bind = statement.bind.bind(statement);
      statement.bind = (...args: any[]) => {
        const bound = bind(...args);
        if (args[2] === 'm.push_rules') {
          const first = bound.first.bind(bound);
          bound.first = async () => {
            if (!injected) {
              injected = true;
              ctx.sqlite.prepare("UPDATE push_rules SET actions='[\"notify\"]',enabled=1 WHERE user_id=? AND kind='room' AND rule_id=?").run(alice, newRoom);
              await storeAccountData(ctx.env.DB, alice, '', 'm.push_rules', await getUserPushRules(ctx.env.DB, alice));
            }
            return first();
          };
        }
        return bound;
      };
    }
    return statement;
  });
  await migrate();
  expect(injected).toBe(true);
  expect(accountContent(alice, '', 'm.push_rules').global.room).toContainEqual(expect.objectContaining({ rule_id: newRoom, actions: ['notify'], enabled: true }));
  expect(position()).toEqual({ position: 1 });
});

it('atomically retargets local aliases, preserves their owners and signs only local replacement canonical aliases', async () => {
  for (const value of ['#primary:local.example', '#alternate:local.example', '#foreign:remote.example']) alias(value, bob);
  await sendLocalRoomEvent(ctx.env, { roomId: oldRoom, sender: alice, type: 'm.room.canonical_alias', stateKey: '', content: {
    alias: '#primary:local.example', alt_aliases: ['#alternate:local.example', '#alternate:local.example', '#foreign:remote.example'],
  } });
  const sourceCanonical = (await getRoomState(ctx.env.DB, oldRoom)).find(event => event.type === 'm.room.canonical_alias');
  const result = await migrate();
  expect(result.aliases.sort()).toEqual(['#alternate:local.example', '#primary:local.example']);
  expect(result.skippedAliases).toEqual(['#foreign:remote.example']);
  for (const value of result.aliases) expect(ctx.sqlite.prepare('SELECT room_id,creator_id,created_at FROM room_aliases WHERE alias=?').get(value))
    .toEqual({ room_id: newRoom, creator_id: bob, created_at: 123 });
  expect(ctx.sqlite.prepare('SELECT room_id FROM room_aliases WHERE alias=?').get('#foreign:remote.example')).toEqual({ room_id: oldRoom });
  const canonical = (await getRoomState(ctx.env.DB, newRoom)).find(event => event.type === 'm.room.canonical_alias')!;
  expect(canonical.content).toEqual({ alias: '#primary:local.example', alt_aliases: ['#alternate:local.example'] });
  expect(await verifySignature(redactEvent(wireEvent(canonical, '12'), '12'), 'local.example', ctx.localKey.keyId, ctx.localKey.publicKey)).toBe(true);
  expect((await getRoomState(ctx.env.DB, oldRoom)).find(event => event.type === 'm.room.canonical_alias')).toEqual(sourceCanonical);
  await migrate();
  expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM events WHERE room_id=? AND event_type='m.room.canonical_alias'").get(newRoom))
    .toEqual({ n: 1 });
});

it.each(['10', '12'])('uses room-version %s alias authorization instead of granting every legacy creator infinite power', async version => {
  [oldRoom, newRoom] = await linkedRooms(version);
  alias(`#permission-${version}:local.example`, bob);
  await sendLocalRoomEvent(ctx.env, { roomId: oldRoom, sender: alice, type: 'm.room.power_levels', stateKey: '', content: {
    users: { [bob]: 100, ...(version === '10' ? { [alice]: 0 } : {}) }, state_default: 50,
    events: { 'm.room.canonical_alias': version === '12' ? 101 : 50 },
  } });
  await migrate();
  expect(ctx.sqlite.prepare('SELECT room_id,creator_id FROM room_aliases WHERE alias=?').get(`#permission-${version}:local.example`))
    .toEqual({ room_id: version === '12' ? newRoom : oldRoom, creator_id: bob });
});

it('retries canonical-alias publication after a post-persist delivery failure without creating another event', async () => {
  alias('#retry:local.example');
  await sendLocalRoomEvent(ctx.env, { roomId: oldRoom, sender: alice, type: 'm.room.canonical_alias', stateKey: '',
    content: { alias: '#retry:local.example' } });
  // The remote user signs their own join. Migration never impersonates it.
  const remote = '@member:remote.example';
  const key = await generateSigningKeyPair();
  const state = await getRoomState(ctx.env.DB, newRoom);
  const latest = state.reduce((a, b) => a.depth > b.depth ? a : b);
  const signed = await signEvent({ room_id: newRoom, type: 'm.room.member', sender: remote, state_key: remote,
    content: { membership: 'join' }, depth: latest.depth + 1, origin_server_ts: Date.now(), prev_events: [latest.event_id],
    auth_events: state.filter(event => ['m.room.power_levels', 'm.room.join_rules'].includes(event.type)).map(event => event.event_id),
  }, '12', 'remote.example', key);
  const join = { ...signed, room_id: newRoom, event_id: await eventReferenceId(signed, '12') } as PDU;
  expect(checkEventAuth(join, state, '12').allowed).toBe(true);
  await storeEvent(ctx.env.DB, join); await updateMembership(ctx.env.DB, newRoom, remote, 'join', join.event_id);
  vi.spyOn(ctx.env.FEDERATION, 'get').mockImplementationOnce(() => ({ fetch: async () => new Response('Temporary queue failure', { status: 503 }) }) as any);
  await expect(migrate()).rejects.toThrow('Could not queue federation event');
  const event = (await getRoomState(ctx.env.DB, newRoom)).find(event => event.type === 'm.room.canonical_alias')!;
  expect(event).toBeDefined();
  const delivered: string[] = [];
  vi.spyOn(ctx.env.FEDERATION, 'get').mockImplementation(() => ({ fetch: async (request: Request) => {
    delivered.push((await request.json() as { event_id: string }).event_id);
    return Response.json({});
  } }) as any);
  await migrate();
  expect(delivered).toContain(event.event_id);
  expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM events WHERE room_id=? AND event_type='m.room.canonical_alias'").get(newRoom)).toEqual({ n: 1 });
});

it('keeps malformed account data untouched and does not copy event-bound or unrelated room settings', async () => {
  account(alice, oldRoom, 'm.tag', { tags: { 'm.favourite': {} } });
  account(alice, newRoom, 'm.tag', { tags: 'invalid-existing-tags' });
  ctx.sqlite.prepare("INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,'','m.direct','not-json')").run(alice);
  account(alice, oldRoom, 'm.fully_read', { event_id: '$old-event' });
  const newMarker = accountContent(alice, newRoom, 'm.fully_read');
  await migrate();
  expect(accountContent(alice, newRoom, 'm.tag')).toEqual({ tags: 'invalid-existing-tags' });
  expect(ctx.sqlite.prepare("SELECT content FROM account_data WHERE user_id=? AND room_id='' AND event_type='m.direct'").get(alice))
    .toEqual({ content: 'not-json' });
  expect(accountContent(alice, newRoom, 'm.fully_read')).toEqual(newMarker);
  expect(position()).toEqual({ position: 0 });
});

it('rejects a different actor or an unrelated replacement before modifying any settings or aliases', async () => {
  alias('#private:local.example'); account(alice, oldRoom, 'm.tag', { tags: { 'm.favourite': {} } });
  const unrelated = await create('12');
  const before = position();
  for (const input of [{ oldRoomId: oldRoom, newRoomId: newRoom, actorUserId: bob },
    { oldRoomId: oldRoom, newRoomId: unrelated, actorUserId: alice },
    { oldRoomId: oldRoom, newRoomId: oldRoom, actorUserId: alice }]) {
    await expect(migrateRoomUpgradeData(ctx.env, input)).rejects.toMatchObject({ status: 403 });
  }
  expect(ctx.sqlite.prepare('SELECT room_id FROM room_aliases WHERE alias=?').get('#private:local.example')).toEqual({ room_id: oldRoom });
  expect(accountContent(alice, newRoom, 'm.tag')).toBeUndefined();
  expect(position()).toEqual(before);
});

it('merges a concurrent direct-chat write without losing the client change or publishing a duplicate stream entry', async () => {
  account(alice, '', 'm.direct', { [bob]: [oldRoom] });
  const originalBatch = ctx.env.DB.batch.bind(ctx.env.DB);
  vi.spyOn(ctx.env.DB, 'batch').mockImplementationOnce(async statements => {
    account(alice, '', 'm.direct', { [bob]: [oldRoom], '@concurrent:remote.example': ['!concurrent:remote.example'] });
    return originalBatch(statements);
  });
  await migrate();
  expect(accountContent(alice, '', 'm.direct')).toEqual({ [bob]: [oldRoom, newRoom], '@concurrent:remote.example': ['!concurrent:remote.example'] });
  expect(position()).toEqual({ position: 1 });
  expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM account_data_changes WHERE stream_position>?').get(baselinePosition)).toEqual({ n: 1 });
});

it('rolls back settings, cursor and sync log together on a failed write and completes on a later retry', async () => {
  account(alice, oldRoom, 'm.tag', { tags: { 'm.favourite': {} } });
  ctx.sqlite.exec("CREATE TRIGGER reject_upgrade_data BEFORE INSERT ON account_data_changes BEGIN SELECT RAISE(ABORT,'Temporary change log failure'); END;");
  await expect(migrate()).rejects.toThrow('Temporary change log failure');
  expect(accountContent(alice, newRoom, 'm.tag')).toBeUndefined();
  expect(position()).toEqual({ position: 0 });
  expect(wakes).toEqual([]);
  ctx.sqlite.exec('DROP TRIGGER reject_upgrade_data');
  await migrate();
  expect(accountContent(alice, newRoom, 'm.tag')).toEqual({ tags: { 'm.favourite': {} } });
  expect(position()).toEqual({ position: 1 });
});
