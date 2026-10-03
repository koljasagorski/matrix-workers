import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Hono } from 'hono';
import type { AppEnv } from '../src/types';
import rooms from '../src/api/rooms';
import aliases from '../src/api/aliases';
import { hashToken } from '../src/utils/crypto';
import { testEnv } from './federation-helpers';

const alice = '@alice:local.example';
const bob = '@bob:local.example';
let ctx: Awaited<ReturnType<typeof testEnv>>;
let mounted: Hono<AppEnv>;
beforeEach(async () => {
  ctx = await testEnv();
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(bob, 'bob');
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)')
    .run('bob', await hashToken('bob-token'), bob, 'BOB');
  mounted = new Hono<AppEnv>().route('/', rooms).route('/', aliases);
});
afterEach(() => { ctx.sqlite.close(); vi.restoreAllMocks(); });
async function request(path: string, method = 'GET', body?: unknown, token?: string) {
  return mounted.request(path, { method, headers: token ? { Authorization: `Bearer ${token}` } : {},
    body: body === undefined ? undefined : JSON.stringify(body) }, ctx.env);
}
async function create(version = '12', extra: Record<string, unknown> = {}) {
  const response = await request('/_matrix/client/v3/createRoom', 'POST', { room_version: version, name: 'Secret name', topic: 'Secret topic', ...extra }, 'token');
  expect(response.status).toBe(200);
  return (await response.json() as { room_id: string }).room_id;
}
const roomPath = (id: string, suffix: string) => `/_matrix/client/v3/rooms/${encodeURIComponent(id)}/${suffix}`;
const aliasPath = (alias: string) => `/_matrix/client/v3/directory/room/${encodeURIComponent(alias)}`;

describe('private room summary access', () => {
  it.each(['/_matrix/client/v1/room_summary/', '/_matrix/client/unstable/im.nheko.summary/summary/'])('hides private state from anonymous and authenticated outsiders via %s', async prefix => {
    const roomId = await create();
    const path = prefix + encodeURIComponent(roomId);
    expect((await request(path)).status).toBe(404);
    expect((await request(path, 'GET', undefined, 'bob-token')).status).toBe(404);
    const member = await request(path, 'GET', undefined, 'token');
    expect(member.status).toBe(200); expect(await member.json()).toMatchObject({ name: 'Secret name', topic: 'Secret topic', membership: 'join' });
    expect((await request(roomPath(roomId, 'invite'), 'POST', { user_id: bob }, 'token')).status).toBe(200);
    expect((await request(path, 'GET', undefined, 'bob-token')).status).toBe(200);
  });

  it('does not use an expired token or a deactivated account to reveal a private room', async () => {
    const roomId = await create(); const path = '/_matrix/client/v1/room_summary/' + encodeURIComponent(roomId);
    ctx.sqlite.prepare('UPDATE access_tokens SET expires_at=? WHERE user_id=?').run(Date.now() - 1, alice);
    expect((await request(path, 'GET', undefined, 'token')).status).toBe(404);
    ctx.sqlite.prepare('UPDATE access_tokens SET expires_at=NULL WHERE user_id=?').run(alice);
    ctx.sqlite.prepare('UPDATE users SET is_deactivated=1 WHERE user_id=?').run(alice);
    expect((await request(path, 'GET', undefined, 'token')).status).toBe(404);
  });

  it('keeps public and world-readable previews available without joining', async () => {
    const publicRoom = await create('10', { preset: 'public_chat' });
    expect((await request('/_matrix/client/v1/room_summary/' + encodeURIComponent(publicRoom))).status).toBe(200);
    const readable = await create('12', { initial_state: [{ type: 'm.room.history_visibility', content: { history_visibility: 'world_readable' } }] });
    expect((await request('/_matrix/client/v1/room_summary/' + encodeURIComponent(readable), 'GET', undefined, 'bob-token')).status).toBe(200);
  });
});

describe('local alias permissions through mounted routes', () => {
  it('blocks unrelated users and ordinary members from deleting another user alias', async () => {
    const roomId = await create('10', { preset: 'public_chat' });
    const alias = '#owned:local.example';
    expect((await request(aliasPath(alias), 'PUT', { room_id: roomId }, 'token')).status).toBe(200);
    expect((await request(aliasPath(alias), 'DELETE', undefined, 'bob-token')).status).toBe(403);
    expect((await request(roomPath(roomId, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
    expect((await request(aliasPath(alias), 'DELETE', undefined, 'bob-token')).status).toBe(403);
    expect(ctx.sqlite.prepare('SELECT alias FROM room_aliases WHERE alias=?').get(alias)).toMatchObject({ alias });
  });

  it('lets the alias creator delete their alias after leaving the room', async () => {
    const roomId = await create(); const alias = '#former-member:local.example';
    expect((await request(aliasPath(alias), 'PUT', { room_id: roomId }, 'token')).status).toBe(200);
    expect((await request(roomPath(roomId, 'leave'), 'POST', {}, 'token')).status).toBe(200);
    expect((await request(aliasPath(alias), 'DELETE', undefined, 'token')).status).toBe(200);
  });

  it('recognizes infinite v12 creator power when deleting an alias created by another member', async () => {
    const roomId = await create('12', { preset: 'public_chat' }); const alias = '#members-alias:local.example';
    expect((await request(roomPath(roomId, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
    expect((await request(aliasPath(alias), 'PUT', { room_id: roomId }, 'bob-token')).status).toBe(200);
    expect((await request(aliasPath(alias), 'DELETE', undefined, 'token')).status).toBe(200);
  });

  it('only creates local aliases and decodes percent signs exactly once', async () => {
    const roomId = await create();
    expect((await request(aliasPath('#foreign:other.example'), 'PUT', { room_id: roomId }, 'token')).status).toBe(400);
    const alias = '#100%:local.example';
    expect((await request(aliasPath(alias), 'PUT', { room_id: roomId }, 'token')).status).toBe(200);
    expect(ctx.sqlite.prepare('SELECT alias FROM room_aliases WHERE alias=?').get(alias)).toMatchObject({ alias });
    expect((await request(aliasPath(alias), 'DELETE', undefined, 'token')).status).toBe(200);
  });

  it('applies the same permissions and valid schema in the aliases sub-app directly', async () => {
    const roomId = await create(); const alias = '#direct:local.example';
    const path = aliasPath(alias);
    expect((await aliases.request(path, { method: 'PUT', headers: { Authorization: 'Bearer token' }, body: JSON.stringify({ room_id: roomId }) }, ctx.env)).status).toBe(200);
    expect((await aliases.request(path, { method: 'DELETE', headers: { Authorization: 'Bearer bob-token' } }, ctx.env)).status).toBe(403);
  });

  it('allows v12 creators to publish a room while blocking ordinary members', async () => {
    const roomId = await create('12', { preset: 'public_chat' });
    const path = '/_matrix/client/v3/directory/list/room/' + encodeURIComponent(roomId);
    expect((await request(roomPath(roomId, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
    expect((await request(path, 'PUT', { visibility: 'public' }, 'bob-token')).status).toBe(403);
    expect((await request(path, 'PUT', { visibility: 'public' }, 'token')).status).toBe(200);
    expect(await (await request(path)).json()).toEqual({ visibility: 'public' });
    expect((await request(path, 'PUT', { visibility: 'private' }, 'token')).status).toBe(200);
  });

  it('honors an explicit zero user power when the default user power is higher', async () => {
    const roomId = await create('10', { preset: 'public_chat' });
    expect((await request(roomPath(roomId, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
    expect((await request(roomPath(roomId, 'state/m.room.power_levels'), 'PUT', { users: { [alice]: 100, [bob]: 0 }, users_default: 50, state_default: 50 }, 'token')).status).toBe(200);
    const path = '/_matrix/client/v3/directory/list/room/' + encodeURIComponent(roomId);
    expect((await request(path, 'PUT', { visibility: 'public' }, 'bob-token')).status).toBe(403);
  });
});

it('removes partially initialized rooms on an initial-state failure and returns the actual failure', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
  const response = await request('/_matrix/client/v3/createRoom', 'POST', { room_version: '12', initial_state: [
    { type: 'com.example.personal-state', state_key: bob, content: { value: 'Cannot write for another person' } },
  ] }, 'token');
  expect(response.status).toBe(403);
  for (const table of ['rooms', 'room_state', 'events', 'room_memberships', 'account_data', 'room_aliases']) {
    expect(ctx.sqlite.prepare(`SELECT count(*) AS n FROM ${table}`).get()).toMatchObject({ n: 0 });
  }
});

it('does not allow an ineligible local user to join a restricted room by naming its creator as authorizer', async () => {
  const restricted = await create();
  const rule = await request(roomPath(restricted, 'state/m.room.join_rules'), 'PUT', {
    join_rule: 'restricted', allow: [{ type: 'm.room_membership', room_id: '!unrelated:local.example' }],
  }, 'token');
  expect(rule.status).toBe(200);
  const response = await request(roomPath(restricted, `state/m.room.member/${encodeURIComponent(bob)}`), 'PUT', {
    membership: 'join', join_authorised_via_users_server: alice,
  }, 'bob-token');
  expect(response.status).toBe(403);
  expect(ctx.sqlite.prepare('SELECT * FROM room_memberships WHERE room_id=? AND user_id=?').get(restricted, bob)).toBeUndefined();
});

it.each(['rooms', 'alias'])('authorizes an eligible local restricted join through the %s join endpoint', async endpoint => {
  const allowed = await create('12', { preset: 'public_chat' });
  expect((await request(roomPath(allowed, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
  const restricted = await create();
  expect((await request(roomPath(restricted, 'state/m.room.join_rules'), 'PUT', {
    join_rule: 'restricted', allow: [{ type: 'm.room_membership', room_id: allowed }],
  }, 'token')).status).toBe(200);
  const path = endpoint === 'rooms' ? roomPath(restricted, 'join') : '/_matrix/client/v3/join/' + encodeURIComponent(restricted);
  expect((await request(path, 'POST', {}, 'bob-token')).status).toBe(200);
  const event = ctx.sqlite.prepare(`SELECT e.content,e.auth_events FROM room_memberships m JOIN events e ON e.event_id=m.event_id
    WHERE m.room_id=? AND m.user_id=?`).get(restricted, bob) as { content: string; auth_events: string };
  expect(JSON.parse(event.content)).toMatchObject({ membership: 'join', join_authorised_via_users_server: alice });
  const count = ctx.sqlite.prepare('SELECT count(*) AS n FROM events WHERE room_id=?').get(restricted);
  expect((await request(path, 'POST', {}, 'bob-token')).status).toBe(200);
  expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events WHERE room_id=?').get(restricted)).toEqual(count);
});

it('rejects eligible user attempts to impersonate a remote join authorizer', async () => {
  const allowed = await create('12', { preset: 'public_chat' });
  expect((await request(roomPath(allowed, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
  const restricted = await create();
  expect((await request(roomPath(restricted, 'state/m.room.join_rules'), 'PUT', {
    join_rule: 'knock_restricted', allow: [{ type: 'm.room_membership', room_id: allowed }],
  }, 'token')).status).toBe(200);
  expect((await request(roomPath(restricted, `state/m.room.member/${encodeURIComponent(bob)}`), 'PUT', {
    membership: 'join', join_authorised_via_users_server: '@moderator:other.example',
  }, 'bob-token')).status).toBe(403);
});
