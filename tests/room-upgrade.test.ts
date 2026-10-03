import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createHash, createPublicKey, verify } from 'node:crypto';
import { Hono } from 'hono';
import rooms from '../src/api/rooms';
import type { AppEnv, Env, PDU } from '../src/types';
import { canonicalJson, generateSigningKeyPair, hashToken, signJson } from '../src/utils/crypto';
import { getEvent, getRoomState, storeEvent, updateMembership } from '../src/services/database';
import { checkEventAuth } from '../src/services/event-auth';
import { sendLocalRoomEvent } from '../src/services/local-room-events';
import type { WireEvent } from '../src/services/federation-events';
import { testEnv } from './federation-helpers';

const alice = '@alice:local.example';
const bob = '@bob:local.example';
const remoteUser = '@peer:remote.example';
let ctx: Awaited<ReturnType<typeof testEnv>>;
let queued: { event_id: string; destination: string; pdu: WireEvent }[];
const path = (id: string, suffix = 'upgrade') => `/_matrix/client/v3/rooms/${encodeURIComponent(id)}/${suffix}`;
const mounted = new Hono<AppEnv>().route('/', rooms);
async function request(url: string, body: unknown, token = 'token', method = 'POST') {
  return mounted.request(url, { method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body) }, ctx.env, { waitUntil: () => {}, passThroughOnException: () => {} } as ExecutionContext);
}
const upgrade = (id: string, version = '12', options: Record<string, unknown> = {}, token = 'token') =>
  request(path(id), { new_version: version, ...options }, token);
async function create(options: Record<string, unknown> = {}) {
  const response = await request('/_matrix/client/v3/createRoom', { room_version: '10', ...options });
  expect(response.status).toBe(200);
  return (await response.json() as { room_id: string }).room_id;
}
async function replacement(response: Response) {
  const body = await response.json() as { replacement_room: string };
  expect(response.status, JSON.stringify(body)).toBe(200);
  return body.replacement_room;
}
function job(id: string) { return ctx.sqlite.prepare('SELECT * FROM room_upgrades WHERE old_room_id=?').get(id) as Record<string, any>; }
async function registerBob() {
  ctx.sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run(bob, 'bob');
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)')
    .run('bob-token', await hashToken('bob-token'), bob, 'BOB');
}

// Independently reconstruct the signature/reference payload used by a peer.
function signingPayload(event: WireEvent, version: string) {
  const result: Record<string, unknown> = {};
  for (const key of ['type', 'room_id', 'sender', 'state_key', 'hashes', 'depth', 'prev_events', 'auth_events', 'origin_server_ts']) {
    if (event[key] !== undefined) result[key] = event[key];
  }
  const retained: Record<string, string[]> = {
    'm.room.create': ['creator'], 'm.room.member': ['membership', 'join_authorised_via_users_server'],
    'm.room.power_levels': ['ban', 'events', 'events_default', 'kick', 'redact', 'state_default', 'users', 'users_default',
      ...(version === '10' ? [] : ['invite'])], 'm.room.join_rules': ['join_rule', 'allow'],
    'm.room.history_visibility': ['history_visibility'],
  };
  result.content = version !== '10' && event.type === 'm.room.create' ? event.content :
    Object.fromEntries((retained[event.type] ?? []).filter(key => event.content[key] !== undefined).map(key => [key, event.content[key]]));
  return result;
}
function assertSigned(event: PDU, version: string) {
  const wire = JSON.parse(JSON.stringify(event)) as WireEvent;
  delete wire.event_id; delete wire.unsigned;
  if (version === '12' && event.type === 'm.room.create') delete wire.room_id;
  const payload = canonicalJson(signingPayload(wire, version));
  const publicJwk = { ...ctx.localKey.privateKeyJwk }; delete publicJwk.d;
  expect(verify(null, Buffer.from(payload), createPublicKey({ key: publicJwk, format: 'jwk' }),
    Buffer.from(event.signatures!['local.example'][ctx.localKey.keyId], 'base64'))).toBe(true);
  expect(event.event_id).toBe(`$${createHash('sha256').update(payload).digest('base64url')}`);
  delete wire.hashes; delete wire.signatures;
  expect(event.hashes?.sha256).toBe(createHash('sha256').update(canonicalJson(wire)).digest('base64').replace(/=+$/, ''));
}
async function assertGraph(id: string, version: string) {
  const ids = ctx.sqlite.prepare('SELECT event_id FROM events WHERE room_id=? ORDER BY depth,stream_ordering').all(id) as { event_id: string }[];
  const state: PDU[] = [];
  for (const { event_id } of ids) {
    const event = (await getEvent(ctx.env.DB, event_id))!;
    assertSigned(event, version);
    expect(event.prev_events).toEqual(state.length ? [ids[ids.findIndex(row => row.event_id === event_id) - 1].event_id] : []);
    const authState = event.auth_events.map(auth => state.find(previous => previous.event_id === auth)!);
    expect(authState.every(Boolean)).toBe(true);
    if (version === '12' && event.type !== 'm.room.create') {
      expect(authState.some(previous => previous.type === 'm.room.create')).toBe(false);
      authState.push(state.find(previous => previous.type === 'm.room.create')!);
    }
    expect(checkEventAuth(event, authState, version)).toMatchObject({ allowed: true });
    const index = state.findIndex(previous => previous.type === event.type && previous.state_key === event.state_key);
    if (index === -1) state.push(event); else state[index] = event;
  }
  return state;
}

async function legacyRoom(remoteMembership?: 'invite' | 'join') {
  const id = '!unsigned:local.example';
  ctx.sqlite.prepare('INSERT INTO rooms(room_id,room_version,creator_id) VALUES (?,\'10\',?)').run(id, alice);
  const previous: PDU[] = [];
  async function add(type: string, content: Record<string, unknown>, stateKey?: string) {
    const event: PDU = { event_id: `$legacy-${previous.length}`, room_id: id, sender: alice, type, content,
      ...(stateKey !== undefined ? { state_key: stateKey } : {}), origin_server_ts: Date.now(), depth: previous.length + 1,
      auth_events: previous.filter(event => ['m.room.create', 'm.room.power_levels'].includes(event.type) ||
        (event.type === 'm.room.member' && event.state_key === alice)).map(event => event.event_id),
      prev_events: previous.length ? [previous.at(-1)!.event_id] : [] };
    await storeEvent(ctx.env.DB, event);
    if (type === 'm.room.member') await updateMembership(ctx.env.DB, id, stateKey!, String(content.membership), event.event_id);
    previous.push(event);
  }
  await add('m.room.create', { creator: alice, room_version: '10', type: 'm.space' }, '');
  await add('m.room.member', { membership: 'join', displayname: 'Alice' }, alice);
  await add('m.room.power_levels', { users: { [alice]: 100 }, events: { 'm.room.tombstone': 100 }, invite: 50 }, '');
  await add('m.room.join_rules', { join_rule: 'invite' }, '');
  await add('m.room.encryption', { algorithm: 'm.megolm.v1.aes-sha2', rotation_period_msgs: 20 }, '');
  await add('m.room.name', { name: 'Legacy encrypted room' }, '');
  await add('m.space.child', { via: ['local.example'] }, '!child:local.example');
  await add('m.room.encrypted', { algorithm: 'm.megolm.v1.aes-sha2', ciphertext: 'original-ciphertext', session_id: 'old-session' });
  if (remoteMembership) await add('m.room.member', { membership: remoteMembership, is_direct: true }, remoteUser);
  return id;
}
async function remotePeer(respond?: (event: WireEvent) => Promise<Response | undefined>) {
  const key = await generateSigningKeyPair();
  await ctx.env.CACHE.put('discovery:v2:remote.example', JSON.stringify({ host: 'remote.example', port: 443, tlsHostname: 'remote.example' }));
  ctx.sqlite.prepare(`INSERT INTO remote_server_keys(server_name,key_id,public_key,valid_from,valid_until,fetched_at,verified)
    VALUES('remote.example',?,?,?,?,?,1)`).run(key.keyId, key.publicKey, Date.now() - 1000, Date.now() + 86400000, Date.now());
  const invites: { event: WireEvent; path: string; state: unknown[] }[] = [];
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    if (!url.pathname.includes('/_matrix/federation/v2/invite/')) throw new Error(`Unexpected request ${url}`);
    const body = JSON.parse(String(init?.body)) as { event: WireEvent; room_version: string; invite_room_state: unknown[] };
    invites.push({ event: body.event, path: url.pathname, state: body.invite_room_state });
    const response = await respond?.(body.event);
    if (response) return response;
    const signed = await signJson({ ...signingPayload(body.event, body.room_version), signatures: body.event.signatures },
      'remote.example', key.keyId, key.privateKeyJwk);
    return Response.json({ event: { ...body.event, signatures: signed.signatures } });
  }));
  return { key, invites };
}

beforeEach(async () => {
  ctx = await testEnv(); queued = [];
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'log').mockImplementation(() => {});
  ctx.env.PUSH_NOTIFICATION_WORKFLOW = { create: async () => ({}) } as unknown as Env['PUSH_NOTIFICATION_WORKFLOW'];
  ctx.env.FEDERATION = { idFromName: (id: string) => id, get: (destination: string) => ({ fetch: async (input: Request) => {
    queued.push({ ...await input.json() as object, destination } as typeof queued[number]); return Response.json({});
  } }) } as unknown as Env['FEDERATION'];
});
afterEach(() => { ctx.sqlite.close(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe('resumable signed room upgrades', () => {
  it('migrates unsigned encrypted history to a signed v12 space without modifying old IDs, ciphertext or historical events', async () => {
    const old = await legacyRoom();
    const history = ctx.sqlite.prepare('SELECT * FROM events WHERE room_id=? ORDER BY event_id').all(old);
    const next = await replacement(await upgrade(old));
    const state = await assertGraph(next, '12');
    const create = state.find(event => event.type === 'm.room.create')!;
    expect(next).toBe(`!${create.event_id.slice(1)}`);
    expect(create.content).toMatchObject({ type: 'm.space', predecessor: { room_id: old } });
    expect((create.content.predecessor as object)).not.toHaveProperty('event_id');
    expect(state.find(event => event.type === 'm.room.encryption')?.content).toEqual({ algorithm: 'm.megolm.v1.aes-sha2', rotation_period_msgs: 20 });
    expect(state.find(event => event.type === 'm.space.child')?.state_key).toBe('!child:local.example');
    expect(state.find(event => event.type === 'm.room.power_levels')?.content.users).toEqual({});
    expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM events WHERE room_id=? AND event_type='m.room.encrypted'").get(next)).toEqual({ n: 0 });
    expect(ctx.sqlite.prepare("SELECT * FROM events WHERE room_id=? AND event_id LIKE '$legacy-%' ORDER BY event_id").all(old)).toEqual(history);
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(old, alice)).toEqual({ membership: 'join' });
    expect((await getRoomState(ctx.env.DB, old)).find(event => event.type === 'm.room.tombstone')?.content.replacement_room).toBe(next);
    expect(job(old)).toMatchObject({ phase: 'complete', lease_token: null });
    const before = ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get();
    expect(await replacement(await upgrade(old))).toBe(next);
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get()).toEqual(before);
    expect((await upgrade(old, '11')).status).toBe(400);
  });

  it.each(['10', '11'])('links predecessor to the exact signed tombstone when replacing with v%s', async version => {
    const oldVersion = version === '10' ? '11' : '10';
    const old = await create({ room_version: oldVersion });
    const next = await replacement(await upgrade(old, version));
    await assertGraph(next, version);
    const tombstone = (await getRoomState(ctx.env.DB, old)).find(event => event.type === 'm.room.tombstone')!;
    const created = (await getRoomState(ctx.env.DB, next)).find(event => event.type === 'm.room.create')!;
    expect(created.content.predecessor).toEqual({ room_id: old, event_id: tombstone.event_id });
    assertSigned(tombstone, oldVersion);
  });

  it.each(['10', '11', '12'])('rejects new same-version v%s upgrades without reserving or cloning rooms', async version => {
    const old = await create({ room_version: version });
    const before = ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get();
    for (let attempt = 0; attempt < 2; attempt++) {
      const response = await upgrade(old, version);
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ errcode: 'M_INVALID_PARAM' });
    }
    expect(job(old)).toBeUndefined();
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual({ n: 1 });
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get()).toEqual(before);
    expect((await getRoomState(ctx.env.DB, old)).some(event => event.type === 'm.room.tombstone')).toBe(false);
  });

  it('resumes existing same-version reservations and preserves completed and historical replacement retries', async () => {
    const old = await create();
    ctx.sqlite.prepare(`INSERT INTO room_upgrades(old_room_id,new_version,actor_user_id,additional_creators,created_at,updated_at)
      VALUES(?, '10', ?, '[]', ?, ?)`).run(old, alice, Date.now(), Date.now());
    const next = await replacement(await upgrade(old, '10'));
    expect(next).not.toBe(old);
    expect(job(old)).toMatchObject({ phase: 'complete', replacement_room_id: next });
    await assertGraph(next, '10');
    const before = ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get();
    expect(await replacement(await upgrade(old, '10'))).toBe(next);
    ctx.sqlite.prepare('DELETE FROM room_upgrades WHERE old_room_id=?').run(old);
    expect(await replacement(await upgrade(old, '10'))).toBe(next);
    expect(job(old)).toBeUndefined();
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual({ n: 2 });
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get()).toEqual(before);
  });

  it.each(['10', '11', '12'])('allows a new upgrade after a stored matching v%s redaction removes the old tombstone', async version => {
    const old = await create({ room_version: version, preset: 'public_chat' });
    const tombstone = await sendLocalRoomEvent(ctx.env, { roomId: old, sender: alice, type: 'm.room.tombstone', stateKey: '',
      content: { body: 'Accidental upgrade', replacement_room: '!abandoned:local.example' } });
    const redaction = await request(path(old, `redact/${encodeURIComponent(tombstone.event_id)}/undo-upgrade`), {}, 'token', 'PUT');
    expect(redaction.status).toBe(200);
    expect((await getEvent(ctx.env.DB, tombstone.event_id))?.content).toEqual({});
    const targetVersion = version === '12' ? '11' : '12';
    await registerBob();
    expect((await request(path(old, 'join'), {}, 'bob-token')).status).toBe(200);
    expect((await upgrade(old, targetVersion, {}, 'bob-token')).status).toBe(403);
    expect(job(old)).toBeUndefined();
    const next = await replacement(await upgrade(old, targetVersion));
    await assertGraph(next, targetVersion);
    expect((await getRoomState(ctx.env.DB, old)).find(event => event.type === 'm.room.tombstone')?.content.replacement_room).toBe(next);
  });

  it.each(['missing', 'wrong target', 'wrong room'])('does not trust a fake redacted tombstone annotation with %s redaction evidence', async evidence => {
    const old = await create();
    const tombstone = await sendLocalRoomEvent(ctx.env, { roomId: old, sender: alice, type: 'm.room.tombstone', stateKey: '',
      content: { body: 'Invalid replacement', replacement_room: '!missing:local.example' } });
    let redactionId = '$missing-redaction';
    if (evidence !== 'missing') {
      const redactionRoom = evidence === 'wrong room' ? await create() : old;
      const event = await sendLocalRoomEvent(ctx.env, { roomId: redactionRoom, sender: alice, type: 'm.room.redaction',
        content: {}, redacts: evidence === 'wrong room' ? tombstone.event_id : '$unrelated-event' });
      redactionId = event.event_id;
    }
    ctx.sqlite.prepare('UPDATE events SET content=?,unsigned=? WHERE event_id=?').run('{}',
      JSON.stringify({ redacted_because: { type: 'm.room.redaction', event_id: redactionId } }), tombstone.event_id);
    const before = ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get();
    expect((await upgrade(old)).status).toBe(400);
    expect(job(old)).toBeUndefined();
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual(before);
  });

  it('invites other local members through authorized events and preserves bans without forging their joins', async () => {
    await registerBob(); const old = await create({ preset: 'public_chat' });
    expect((await request(path(old, 'join'), {}, 'bob-token')).status).toBe(200);
    const banned = '@blocked:remote.example';
    await sendLocalRoomEvent(ctx.env, { roomId: old, sender: alice, type: 'm.room.member', stateKey: banned, content: { membership: 'ban', reason: 'Preserve ban' } });
    const next = await replacement(await upgrade(old, '12', { additional_creators: [bob] }));
    const state = await assertGraph(next, '12');
    expect(state.find(event => event.type === 'm.room.create')?.content.additional_creators).toEqual([bob]);
    expect(state.find(event => event.type === 'm.room.power_levels')?.content.users).toEqual({});
    expect(state.find(event => event.state_key === bob && event.type === 'm.room.member')).toMatchObject({ sender: alice, content: { membership: 'invite' } });
    expect(state.find(event => event.state_key === banned && event.type === 'm.room.member')?.content).toEqual({ membership: 'ban', reason: 'Preserve ban' });
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(old, bob)).toEqual({ membership: 'join' });
  });

  it('checks actual tombstone permission before reservation and again on idempotent requests', async () => {
    await registerBob(); const old = await create({ preset: 'public_chat' });
    expect((await request(path(old, 'join'), {}, 'bob-token')).status).toBe(200);
    expect((await upgrade(old, '12', {}, 'bob-token')).status).toBe(403);
    expect((await upgrade(old, '10', {}, 'bob-token')).status).toBe(403);
    expect(job(old)).toBeUndefined();
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual({ n: 1 });
    const next = await replacement(await upgrade(old));
    expect((await upgrade(old, '12', {}, 'bob-token')).status).toBe(403);
    expect(job(old).replacement_room_id).toBe(next);
    for (const additional_creators of [['invalid'], [alice], [bob, bob], 'invalid']) {
      expect((await upgrade(old, '12', { additional_creators })).status).toBe(400);
    }
  });

  it('rolls back the entire replacement bootstrap on storage failure and retries the same signed plan', async () => {
    const old = await legacyRoom();
    ctx.sqlite.exec("CREATE TRIGGER reject_bootstrap BEFORE INSERT ON events WHEN NEW.room_id!='!unsigned:local.example' AND NEW.event_type='m.room.encryption' BEGIN SELECT RAISE(ABORT,'Temporary bootstrap failure'); END;");
    expect((await upgrade(old)).status).toBe(502);
    const planned = job(old);
    expect(planned).toMatchObject({ phase: 'planned', lease_token: null });
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual({ n: 1 });
    for (const table of ['events', 'room_state', 'room_memberships', 'event_state_snapshots']) {
      expect(ctx.sqlite.prepare(`SELECT count(*) AS n FROM ${table} WHERE room_id=?`).get(planned.replacement_room_id)).toEqual({ n: 0 });
    }
    ctx.sqlite.exec('DROP TRIGGER reject_bootstrap');
    expect(await replacement(await upgrade(old))).toBe(planned.replacement_room_id);
    await assertGraph(planned.replacement_room_id, '12');
  });

  it('persists and reuses a refused remote invite, then completes only after a verified countersignature', async () => {
    const old = await legacyRoom('invite'); let refused = true;
    const peer = await remotePeer(async () => refused ? Response.json({ errcode: 'M_FORBIDDEN', error: 'Try later' }, { status: 403 }) : undefined);
    expect((await upgrade(old)).status).toBe(403);
    const pending = job(old);
    expect(pending).toMatchObject({ phase: 'bootstrapped', member_cursor: 0, lease_token: null });
    expect(JSON.parse(pending.pending_event_json).event_id).toBeTruthy();
    expect((await getRoomState(ctx.env.DB, old)).some(event => event.type === 'm.room.tombstone')).toBe(false);
    expect(ctx.sqlite.prepare('SELECT * FROM room_memberships WHERE room_id=? AND user_id=?').get(pending.replacement_room_id, remoteUser)).toBeUndefined();
    refused = false;
    const next = await replacement(await upgrade(old));
    expect(next).toBe(pending.replacement_room_id);
    expect(peer.invites).toHaveLength(2);
    expect(peer.invites[0].path).toBe(peer.invites[1].path);
    expect(peer.invites[0].event).toEqual(peer.invites[1].event);
    expect(peer.invites[1].state).toEqual(expect.arrayContaining([expect.objectContaining({ type: 'm.room.encryption' })]));
    const invite = (await getRoomState(ctx.env.DB, next)).find(event => event.type === 'm.room.member' && event.state_key === remoteUser)!;
    expect(invite).toMatchObject({ sender: alice, content: { membership: 'invite', is_direct: true } });
    expect(invite.signatures?.['remote.example']?.[peer.key.keyId]).toBeTruthy();
    await assertGraph(next, '12');
    expect(job(old).phase).toBe('complete');
  });

  it('serializes concurrent requests and returns the same completed replacement on retry', async () => {
    const old = await legacyRoom('invite');
    let release!: () => void; const gate = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void; const networkEntered = new Promise<void>(resolve => { entered = resolve; });
    const peer = await remotePeer(async () => { entered(); await gate; return undefined; });
    const first = upgrade(old);
    await networkEntered;
    const second = await upgrade(old);
    expect(second.status).toBe(429);
    expect(await second.json()).toMatchObject({ retry_after_ms: 1000 });
    release(); const next = await replacement(await first);
    expect(await replacement(await upgrade(old))).toBe(next);
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual({ n: 2 });
    expect(peer.invites).toHaveLength(1);
  });

  it('keeps an invite with a changed remote event pending instead of accepting a fake handshake success', async () => {
    const old = await legacyRoom('invite'); let tampered = true;
    const peer = await remotePeer(async event => tampered ? Response.json({ event: {
      ...event, content: { ...event.content, displayname: 'Changed remotely' },
    } }) : undefined);
    expect((await upgrade(old)).status).toBe(502);
    const saved = job(old);
    expect(saved).toMatchObject({ phase: 'bootstrapped', member_cursor: 0 });
    expect(ctx.sqlite.prepare('SELECT * FROM room_memberships WHERE room_id=? AND user_id=?').get(saved.replacement_room_id, remoteUser)).toBeUndefined();
    tampered = false;
    expect(await replacement(await upgrade(old))).toBe(saved.replacement_room_id);
    expect(peer.invites[0].path).toBe(peer.invites[1].path);
  });

  it('reauthorizes saved pending invites after a recipient was banned while the upgrade was idle', async () => {
    const old = await legacyRoom('invite'); let refused = true;
    const peer = await remotePeer(async () => refused ? Response.json({ errcode: 'M_FORBIDDEN' }, { status: 403 }) : undefined);
    expect((await upgrade(old)).status).toBe(403);
    const saved = job(old);
    await sendLocalRoomEvent(ctx.env, { roomId: saved.replacement_room_id, sender: alice, type: 'm.room.member',
      stateKey: remoteUser, content: { membership: 'ban' } });
    refused = false;
    expect((await upgrade(old)).status).toBe(403);
    expect(peer.invites).toHaveLength(1);
    expect(job(old).phase).toBe('bootstrapped');
    expect((await getRoomState(ctx.env.DB, old)).some(event => event.type === 'm.room.tombstone')).toBe(false);
  });

  it('resumes after invitations but before the old tombstone without duplicate rooms or invitations', async () => {
    const old = await legacyRoom('invite'); const peer = await remotePeer();
    ctx.sqlite.exec("CREATE TRIGGER reject_tombstone BEFORE INSERT ON events WHEN NEW.event_type='m.room.tombstone' BEGIN SELECT RAISE(ABORT,'Temporary tombstone failure'); END;");
    expect((await upgrade(old)).status).toBe(502);
    const saved = job(old); expect(saved.phase).toBe('members_done');
    const events = ctx.sqlite.prepare('SELECT * FROM events WHERE room_id=? ORDER BY event_id').all(saved.replacement_room_id);
    ctx.sqlite.exec('DROP TRIGGER reject_tombstone');
    expect(await replacement(await upgrade(old))).toBe(saved.replacement_room_id);
    expect(peer.invites).toHaveLength(1);
    expect(ctx.sqlite.prepare('SELECT * FROM events WHERE room_id=? ORDER BY event_id').all(saved.replacement_room_id)).toEqual(events);
    expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM events WHERE room_id=? AND event_type='m.room.tombstone'").get(old)).toEqual({ n: 1 });
  });

  it('replays durable tombstone publication after queue failure without storing another tombstone', async () => {
    const old = await legacyRoom('join'); await remotePeer(); let failed = false;
    const original = ctx.env.FEDERATION.get;
    ctx.env.FEDERATION.get = ((id: DurableObjectId) => ({ fetch: async (input: Request) => {
      const body = await input.clone().json() as { pdu: WireEvent };
      if (!failed && body.pdu.type === 'm.room.tombstone') { failed = true; return new Response('Queue failed', { status: 503 }); }
      return original(id).fetch(input);
    } })) as typeof ctx.env.FEDERATION.get;
    expect((await upgrade(old)).status).toBe(502);
    const saved = job(old); expect(saved.phase).toBe('tombstoned');
    expect(await replacement(await upgrade(old))).toBe(saved.replacement_room_id);
    expect(queued.some(event => event.pdu.type === 'm.room.tombstone' && event.destination === 'remote.example')).toBe(true);
    expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM events WHERE room_id=? AND event_type='m.room.tombstone'").get(old)).toEqual({ n: 1 });
  });

  it('guards all bootstrap mutations against a stolen lease, then resumes after lease expiration', async () => {
    const old = await legacyRoom(); const original = ctx.env.DB.batch.bind(ctx.env.DB); let batches = 0;
    vi.spyOn(ctx.env.DB, 'batch').mockImplementation(async statements => {
      batches++;
      if (batches === 2) ctx.sqlite.prepare('UPDATE room_upgrades SET lease_token=?,lease_until=? WHERE old_room_id=?')
        .run('new-owner', Date.now() + 60000, old);
      return original(statements);
    });
    expect((await upgrade(old)).status).toBe(502);
    const saved = job(old); expect(saved).toMatchObject({ phase: 'planned', lease_token: 'new-owner' });
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM rooms').get()).toEqual({ n: 1 });
    expect((await upgrade(old)).status).toBe(429);
    ctx.sqlite.prepare('UPDATE room_upgrades SET lease_until=0 WHERE old_room_id=?').run(old);
    expect(await replacement(await upgrade(old))).toBe(saved.replacement_room_id);
  });

  it('resumes a mapping failure after tombstone publication without duplicating old or new events', async () => {
    const old = await legacyRoom();
    ctx.sqlite.prepare("INSERT INTO account_data(user_id,room_id,event_type,content) VALUES (?,?,'m.tag',?)")
      .run(alice, old, JSON.stringify({ tags: { 'm.favourite': {} } }));
    ctx.sqlite.exec("CREATE TRIGGER reject_mapping BEFORE INSERT ON account_data_changes BEGIN SELECT RAISE(ABORT,'Temporary mapping failure'); END;");
    expect((await upgrade(old)).status).toBe(502);
    const saved = job(old); expect(saved.phase).toBe('restricted');
    const events = ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get();
    ctx.sqlite.exec('DROP TRIGGER reject_mapping');
    expect(await replacement(await upgrade(old))).toBe(saved.replacement_room_id);
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events').get()).toEqual(events);
    expect(ctx.sqlite.prepare("SELECT content FROM account_data WHERE user_id=? AND room_id=? AND event_type='m.tag'").get(alice, saved.replacement_room_id))
      .toEqual({ content: '{"tags":{"m.favourite":{}}}' });
    expect(job(old).phase).toBe('complete');
  });
});
