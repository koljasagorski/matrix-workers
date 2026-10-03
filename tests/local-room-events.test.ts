import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createHash, createPublicKey, verify } from 'node:crypto';
import { Hono } from 'hono';
import rooms from '../src/api/rooms';
import type { AppEnv, Env, PDU } from '../src/types';
import { canonicalJson, generateSigningKeyPair, hashToken, signJson } from '../src/utils/crypto';
import { getEvent, getRoomState, storeEvent, updateMembership } from '../src/services/database';
import { eventReferenceId, signEvent, wireEvent, type WireEvent } from '../src/services/federation-events';
import { checkEventAuth } from '../src/services/event-auth';
import { receiveRemoteInvite } from '../src/services/remote-invites';
import { roomFixture, testEnv } from './federation-helpers';

const alice = '@alice:local.example';
const bob = '@bob:local.example';
let ctx: Awaited<ReturnType<typeof testEnv>>;
let queued: { destination: string; pdu: WireEvent; event_id: string }[];
let notified: string[];

// A peer independently derives the redacted signing payload and reference ID.
function peerSigningEvent(event: WireEvent, version: string): Record<string, unknown> {
  const keys = ['type', 'room_id', 'sender', 'state_key', 'hashes', 'depth', 'prev_events', 'auth_events', 'origin_server_ts'];
  const result: Record<string, unknown> = {};
  for (const key of keys) if (event[key] !== undefined) result[key] = event[key];
  const allowed: Record<string, string[]> = {
    'm.room.create': ['creator'], 'm.room.member': ['membership', 'join_authorised_via_users_server'],
    'm.room.power_levels': ['ban', 'events', 'events_default', 'kick', 'redact', 'state_default', 'users', 'users_default',
      ...(version === '10' ? [] : ['invite'])],
    'm.room.join_rules': ['join_rule', 'allow'], 'm.room.history_visibility': ['history_visibility'],
    'm.room.redaction': version === '10' ? [] : ['redacts'],
  };
  result.content = version !== '10' && event.type === 'm.room.create' ? event.content :
    Object.fromEntries((allowed[event.type] ?? []).filter(key => event.content[key] !== undefined).map(key => [key, event.content[key]]));
  return result;
}
function assertSigned(event: PDU, version: string) {
  const wire = wireEvent(event, version);
  const signaturePayload = canonicalJson(peerSigningEvent(wire, version));
  const publicJwk = { ...ctx.localKey.privateKeyJwk }; delete publicJwk.d;
  expect(verify(null, Buffer.from(signaturePayload), createPublicKey({ key: publicJwk, format: 'jwk' }),
    Buffer.from(wire.signatures!['local.example'][ctx.localKey.keyId], 'base64'))).toBe(true);
  expect(event.event_id).toBe(`$${createHash('sha256').update(signaturePayload).digest('base64url')}`);
  delete wire.hashes; delete wire.signatures; delete wire.unsigned;
  expect(event.hashes?.sha256).toBe(createHash('sha256').update(canonicalJson(wire)).digest('base64').replace(/=+$/, ''));
}
async function request(path: string, method = 'POST', body: unknown = {}, token = 'token') {
  return rooms.request(path, { method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: method === 'GET' ? undefined : JSON.stringify(body) }, ctx.env,
    { waitUntil: () => {}, passThroughOnException: () => {} } as ExecutionContext);
}
const roomPath = (roomId: string, suffix: string) => `/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/${suffix}`;
async function create(version = '10', options: Record<string, unknown> = {}) {
  const response = await request('/_matrix/client/v3/createRoom', 'POST', { room_version: version, preset: 'public_chat', ...options });
  const body = await response.json() as { room_id: string };
  expect(response.status, JSON.stringify(body)).toBe(200);
  return body.room_id;
}
async function registerBob() {
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(bob, 'bob');
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)')
    .run('bob-token', await hashToken('bob-token'), bob, 'BOBDEVICE');
}
async function addRemoteResident(roomId: string, version: string) {
  const key = await generateSigningKeyPair();
  const user = '@member:remote.example';
  const state = await getRoomState(ctx.env.DB, roomId);
  const latest = ctx.sqlite.prepare('SELECT event_id,depth FROM events WHERE room_id=? ORDER BY depth DESC LIMIT 1')
    .get(roomId) as { event_id: string; depth: number };
  const signed = await signEvent({ room_id: roomId, type: 'm.room.member', sender: user, state_key: user,
    content: { membership: 'join' }, depth: latest.depth + 1, origin_server_ts: Date.now(), prev_events: [latest.event_id],
    auth_events: state.filter(event => event.type === 'm.room.power_levels' || event.type === 'm.room.join_rules' ||
      (version !== '12' && event.type === 'm.room.create')).map(event => event.event_id),
  }, version, 'remote.example', key);
  const id = await eventReferenceId(signed, version);
  await storeEvent(ctx.env.DB, { ...signed, event_id: id, room_id: roomId } as PDU);
  await updateMembership(ctx.env.DB, roomId, user, 'join', id);
  return user;
}

beforeEach(async () => {
  ctx = await testEnv(); queued = []; notified = [];
  ctx.env.PUSH_NOTIFICATION_WORKFLOW = { create: async () => ({}) } as unknown as Env['PUSH_NOTIFICATION_WORKFLOW'];
  ctx.env.FEDERATION = { idFromName: (name: string) => name, get: (destination: string) => ({ fetch: async (input: Request) => {
    queued.push({ ...await input.json() as object, destination } as typeof queued[number]); return Response.json({});
  } }) } as unknown as Env['FEDERATION'];
  ctx.env.SYNC = { idFromName: (name: string) => name, get: (user: string) => ({ fetch: async () => {
    notified.push(user); return Response.json({});
  } }) } as unknown as Env['SYNC'];
});
afterEach(() => { ctx.sqlite.close(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

describe('authorized signed local room events', () => {
  it.each(['10', '11', '12'])('creates a valid signed event graph for version %s', async version => {
    const roomId = await create(version, { initial_state: [{ type: 'm.room.encryption', content: { algorithm: 'm.megolm.v1.aes-sha2' } }] });
    const ids = ctx.sqlite.prepare('SELECT event_id FROM events WHERE room_id=? ORDER BY depth').all(roomId) as { event_id: string }[];
    const prior: PDU[] = [];
    for (const row of ids) {
      const event = (await getEvent(ctx.env.DB, row.event_id))!;
      assertSigned(event, version);
      const auth = event.auth_events.map(id => prior.find(previous => previous.event_id === id)!);
      expect(auth.every(Boolean)).toBe(true);
      if (version === '12' && event.type !== 'm.room.create') {
        expect(auth.some(previous => previous.type === 'm.room.create')).toBe(false);
        auth.push(prior[0]);
      }
      expect(checkEventAuth(event, auth, version)).toMatchObject({ allowed: true });
      prior.push(event);
    }
    if (version === '12') {
      expect(roomId).toBe(`!${prior[0].event_id.slice(1)}`);
      expect(wireEvent(prior[0], version).room_id).toBeUndefined();
      expect(prior.find(event => event.type === 'm.room.power_levels')!.content.users).toEqual({});
    }
  });

  it.each(['10', '11', '12'])('blocks an ordinary member from granting themselves power in version %s', async version => {
    const roomId = await create(version); await registerBob();
    expect((await request(roomPath(roomId, 'join'), 'POST', {}, 'bob-token')).status).toBe(200);
    const old = ctx.sqlite.prepare('SELECT event_id FROM room_state WHERE room_id=? AND event_type=?')
      .get(roomId, 'm.room.power_levels');
    const response = await request(roomPath(roomId, 'state/m.room.power_levels/'), 'PUT', { users: { [alice]: 100, [bob]: 100 } }, 'bob-token');
    expect(response.status).toBe(403);
    expect(await response.json()).toMatchObject({ errcode: 'M_FORBIDDEN' });
    expect(ctx.sqlite.prepare('SELECT event_id FROM room_state WHERE room_id=? AND event_type=?').get(roomId, 'm.room.power_levels')).toEqual(old);
    expect((await request(roomPath(roomId, 'state/m.room.name'), 'PUT', { name: 'Unauthorized' }, 'bob-token')).status).toBe(403);
  });

  it.each(['10', '11', '12'])('signs and federates authorized state with an empty trailing state key in version %s', async version => {
    const roomId = await create(version); await addRemoteResident(roomId, version); queued = [];
    const response = await request(roomPath(roomId, 'state/m.room.name/'), 'PUT', { name: 'Visible on every server' });
    expect(response.status).toBe(200);
    const { event_id } = await response.json() as { event_id: string };
    const event = (await getEvent(ctx.env.DB, event_id))!;
    assertSigned(event, version);
    expect(queued).toHaveLength(1);
    expect(queued[0]).toMatchObject({ destination: 'remote.example', event_id, pdu: { type: 'm.room.name', content: event.content } });
    expect(queued[0].pdu.event_id).toBeUndefined();
    for (const suffix of ['state/m.room.name', 'state/m.room.name/']) {
      const read = await request(roomPath(roomId, suffix), 'GET');
      expect(read.status).toBe(200); expect(await read.json()).toEqual(event.content);
    }
  });

  it.each(['10', '11', '12'])('delivers a departure to the final remote member server in version %s', async version => {
    const roomId = await create(version); const user = await addRemoteResident(roomId, version); queued = [];
    expect((await request(roomPath(roomId, 'kick'), 'POST', { user_id: user, reason: 'Testing departures' })).status).toBe(200);
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(roomId, user)).toMatchObject({ membership: 'leave' });
    expect(queued).toHaveLength(1); expect(queued[0].destination).toBe('remote.example');
    const event = (await getEvent(ctx.env.DB, queued[0].event_id))!; assertSigned(event, version);
    expect(event.content).toEqual({ membership: 'leave', reason: 'Testing departures' });
  });

  it('rejects a local invite, directly wakes the invitee, and allows repeated leave', async () => {
    await registerBob(); const roomId = await create('12', { preset: 'private_chat' });
    expect((await request(roomPath(roomId, 'invite'), 'POST', { user_id: bob })).status).toBe(200);
    notified = [];
    expect((await request(roomPath(roomId, 'leave'), 'POST', {}, 'bob-token')).status).toBe(200);
    expect(notified).toContain(bob);
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(roomId, bob)).toMatchObject({ membership: 'leave' });
    expect((await request(roomPath(roomId, 'leave'), 'POST', {}, 'bob-token')).status).toBe(200);
  });

  it('rejects replacing the creation state and malformed state bodies without storing an event', async () => {
    const roomId = await create();
    const before = ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get();
    expect((await request(roomPath(roomId, 'state/m.room.create'), 'PUT', { creator: alice, room_version: '10' })).status).toBe(403);
    for (const content of [null, [], 'name']) expect((await request(roomPath(roomId, 'state/m.room.name/'), 'PUT', content)).status).toBe(400);
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get()).toEqual(before);
  });

  it('supports empty state keys when the room routes are mounted under the production router', async () => {
    const roomId = await create();
    const mounted = new Hono<AppEnv>().route('/', rooms);
    const path = roomPath(roomId, 'state/m.room.name/');
    const write = await mounted.request(path, { method: 'PUT', headers: { Authorization: 'Bearer token' }, body: JSON.stringify({ name: 'Mounted state' }) }, ctx.env);
    expect(write.status).toBe(200);
    const read = await mounted.request(path, { headers: { Authorization: 'Bearer token' } }, ctx.env);
    expect(read.status).toBe(200); expect(await read.json()).toEqual({ name: 'Mounted state' });
  });

  it('rolls back the redaction event and transaction if applying the target redaction fails', async () => {
    const roomId = await create();
    const sent = await request(roomPath(roomId, 'send/m.room.message/rollback-message'), 'PUT', { body: 'Keep this until redacted', msgtype: 'm.text' });
    const { event_id } = await sent.json() as { event_id: string };
    ctx.sqlite.exec("CREATE TRIGGER reject_redaction BEFORE UPDATE OF content ON events BEGIN SELECT RAISE(ABORT,'Simulated database failure'); END;");
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const path = roomPath(roomId, `redact/${encodeURIComponent(event_id)}/rollback-redaction`);
    expect((await request(path, 'PUT')).status).toBe(502);
    expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM events WHERE event_type='m.room.redaction'").get()).toMatchObject({ n: 0 });
    expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM transaction_ids WHERE txn_id LIKE 'room-redact:%'").get()).toMatchObject({ n: 0 });
    expect(await getEvent(ctx.env.DB, event_id)).toMatchObject({ content: { body: 'Keep this until redacted' } });
    ctx.sqlite.exec('DROP TRIGGER reject_redaction');
    expect((await request(path, 'PUT')).status).toBe(200);
    expect(await getEvent(ctx.env.DB, event_id)).toMatchObject({ content: {} });
  });

  it('rejects unsupported versions and malformed additional v12 creators before creating a room', async () => {
    expect((await request('/_matrix/client/v3/createRoom', 'POST', { room_version: '9' })).status).toBe(400);
    expect((await request('/_matrix/client/v3/createRoom', 'POST', { room_version: '12', creation_content: { additional_creators: ['invalid'] } })).status).toBe(403);
    expect(ctx.sqlite.prepare('SELECT * FROM rooms').all()).toEqual([]);
  });

  it('blocks remote invitation and delivery for a room with federation disabled', async () => {
    const roomId = await create('10', { creation_content: { 'm.federate': false } });
    const fetchMock = vi.fn(); vi.stubGlobal('fetch', fetchMock);
    expect((await request(roomPath(roomId, 'invite'), 'POST', { user_id: '@guest:remote.example' })).status).toBe(403);
    expect(fetchMock).not.toHaveBeenCalled();
    await addRemoteResident(roomId, '10'); queued = [];
    expect((await request(roomPath(roomId, 'state/m.room.name'), 'PUT', { name: 'Local only' })).status).toBe(200);
    expect(queued).toEqual([]);
  });

  it('accepts a sync token with receipt progress for backwards room pagination', async () => {
    const roomId = await create();
    const response = await request(`${roomPath(roomId, 'messages')}?from=s100_td0_dk0_rr1&dir=b&limit=1`, 'GET');
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ start: 's100_td0_dk0_rr1', chunk: expect.any(Array) });
  });

  it.each(['10', '11', '12'])('uses signed replayable redactions and idempotent transactions in version %s', async version => {
    const roomId = await create(version); await addRemoteResident(roomId, version);
    const { event_id: messageId } = await (await request(roomPath(roomId, 'send/m.room.message/message-txn'), 'PUT', { body: 'temporary', msgtype: 'm.text' })).json() as { event_id: string };
    queued = [];
    const first = await (await request(roomPath(roomId, `redact/${encodeURIComponent(messageId)}/redact-txn`), 'PUT', { reason: 'Cleanup' })).json() as { event_id: string };
    const second = await (await request(roomPath(roomId, `redact/${encodeURIComponent(messageId)}/redact-txn`), 'PUT', { reason: 'Cleanup' })).json();
    expect(second).toEqual(first);
    const event = (await getEvent(ctx.env.DB, first.event_id))!;
    assertSigned(event, version);
    if (version === '10') {
      expect(event.redacts).toBe(messageId); expect(queued[1].pdu.redacts).toBe(messageId);
    } else {
      expect(event.content.redacts).toBe(messageId); expect(queued[0].pdu.content.redacts).toBe(messageId);
    }
    expect(await getEvent(ctx.env.DB, messageId)).toMatchObject({ content: {}, unsigned: { redacted_because: { event_id: first.event_id } } });
  });
});

describe('remote membership handshakes', () => {
  it('gets and verifies a remote invitation countersignature before storing success', async () => {
    const roomId = await create();
    const remote = 'remote.example'; const key = await generateSigningKeyPair();
    const keyResponse = await signJson({ server_name: remote, valid_until_ts: Date.now() + 86400000,
      verify_keys: { [key.keyId]: { key: key.publicKey } } }, remote, key.keyId, key.privateKeyJwk);
    await ctx.env.CACHE.put(`discovery:${remote}`, JSON.stringify({ host: remote, port: 443, tlsHostname: remote }));
    let inviteCount = 0;
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.pathname === '/_matrix/key/v2/server') return Response.json(keyResponse);
      if (url.pathname.includes('/invite/')) {
        inviteCount++;
        const body = JSON.parse(String(init?.body)) as { event: WireEvent; invite_room_state: unknown[] };
        expect(body.invite_room_state).toEqual(expect.arrayContaining([expect.objectContaining({ type: 'm.room.create' })]));
        const signed = await signJson({ ...peerSigningEvent(body.event, '10'), signatures: body.event.signatures }, remote, key.keyId, key.privateKeyJwk);
        return Response.json({ event: { ...body.event, signatures: signed.signatures } });
      }
      throw new Error(`Unexpected request ${url}`);
    }));
    const response = await request(roomPath(roomId, 'invite'), 'POST', { user_id: '@guest:remote.example' });
    expect(response.status).toBe(200); expect(inviteCount).toBe(1);
    const membership = ctx.sqlite.prepare('SELECT membership,event_id FROM room_memberships WHERE room_id=? AND user_id=?')
      .get(roomId, '@guest:remote.example') as { membership: string; event_id: string };
    expect(membership.membership).toBe('invite');
    const event = (await getEvent(ctx.env.DB, membership.event_id))!;
    assertSigned(event, '10'); expect(event.signatures?.[remote]?.[key.keyId]).toBeTruthy();
  });

  it('does not persist an invitation which the destination server refuses', async () => {
    const roomId = await create();
    await ctx.env.CACHE.put('discovery:remote.example', JSON.stringify({ host: 'remote.example', port: 443, tlsHostname: 'remote.example' }));
    vi.stubGlobal('fetch', vi.fn(async () => Response.json({ errcode: 'M_FORBIDDEN', error: 'No invitation permitted' }, { status: 403 })));
    const response = await request(roomPath(roomId, 'invite'), 'POST', { user_id: '@guest:remote.example' });
    expect(response.status).toBe(403);
    expect(ctx.sqlite.prepare('SELECT * FROM room_memberships WHERE room_id=? AND user_id=?').get(roomId, '@guest:remote.example')).toBeUndefined();
  });

  it('rejects a stripped remote invite via make_leave/send_leave without accepting it', async () => {
    const fixture = await roomFixture('12');
    const invite = await signEvent({ ...fixture.template, sender: '@creator:remote.example', state_key: alice,
      content: { membership: 'invite' } }, '12', fixture.remote, fixture.key);
    const inviteId = await eventReferenceId(invite, '12');
    await ctx.env.CACHE.put('discovery:remote.example', JSON.stringify({ host: 'remote.example', port: 443, tlsHostname: 'remote.example' }));
    let leave: WireEvent | undefined;
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.pathname === '/_matrix/key/v2/server') return Response.json(fixture.keyResponse);
      if (url.pathname.includes('/make_leave/')) return Response.json({ room_version: '12', event: {
        ...fixture.template, content: { membership: 'leave' }, prev_events: [inviteId], auth_events: [inviteId],
      } });
      if (url.pathname.includes('/send_leave/')) { leave = JSON.parse(String(init?.body)); return Response.json({}); }
      throw new Error(`Unexpected request ${url}`);
    }));
    await receiveRemoteInvite(ctx.env, fixture.remote, fixture.roomId, inviteId, { room_version: '12', event: invite, invite_room_state: [] });
    expect(await getRoomState(ctx.env.DB, fixture.roomId)).toEqual([]);
    const response = await request(roomPath(fixture.roomId, 'leave'), 'POST', { reason: 'Declined' });
    expect(response.status).toBe(200);
    expect(leave?.content).toEqual({ membership: 'leave', reason: 'Declined' });
    const event = (await getEvent(ctx.env.DB, await eventReferenceId(leave!, '12')))!;
    assertSigned(event, '12');
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(fixture.roomId, alice)).toMatchObject({ membership: 'leave' });
  });
});
