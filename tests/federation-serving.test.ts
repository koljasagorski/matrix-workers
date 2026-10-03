import { beforeEach, afterEach, expect, it, vi } from 'vitest';
import { createHash, createPublicKey, verify } from 'node:crypto';
import federation from '../src/api/federation';
import rooms from '../src/api/rooms';
import { signFederationRequest } from '../src/services/federation-keys';
import { canonicalJson, generateSigningKeyPair, signJson } from '../src/utils/crypto';
import { eventReferenceId, signEvent, wireEvent, type WireEvent } from '../src/services/federation-events';
import { getRoomState, getEvent, storeEvent } from '../src/services/database';
import { buildLocalRoomEvent, selectAuthEvents } from '../src/services/local-room-events';
import { prepareVerifiedStateSnapshot } from '../src/services/event-state-snapshots';
import { validateJoinGraph } from '../src/services/remote-rooms';
import type { Env, PDU } from '../src/types';
import { testEnv } from './federation-helpers';

let ctx: Awaited<ReturnType<typeof testEnv>>;
let remoteKey: Awaited<ReturnType<typeof generateSigningKeyPair>>;
let queued: { pdu: WireEvent; event_id: string }[];
const remote = 'remote.example';
const remoteUser = '@peer:remote.example';

beforeEach(async () => {
  ctx = await testEnv(); remoteKey = await generateSigningKeyPair(); queued = [];
  ctx.env.FEDERATION = { idFromName: (name: string) => name, get: () => ({ fetch: async (input: Request) => {
    queued.push(await input.json() as typeof queued[number]); return Response.json({});
  } }) } as unknown as Env['FEDERATION'];
  ctx.env.PUSH_NOTIFICATION_WORKFLOW = { create: async () => ({}) } as unknown as Env['PUSH_NOTIFICATION_WORKFLOW'];
  await ctx.env.CACHE.put(`discovery:${remote}`, JSON.stringify({ host: remote, port: 443, tlsHostname: remote }));
  const keyResponse = await signJson({ server_name: remote, valid_until_ts: Date.now() + 86400000,
    verify_keys: { [remoteKey.keyId]: { key: remoteKey.publicKey } } }, remote, remoteKey.keyId, remoteKey.privateKeyJwk);
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(keyResponse)));
});
afterEach(() => { ctx.sqlite.close(); vi.unstubAllGlobals(); });

// Independently derive redacted payloads; do not use the server's redaction/hash implementation.
function peerSigningJson(event: WireEvent, version: string): Record<string, unknown> {
  const retained = ['type', 'room_id', 'sender', 'state_key', 'hashes', 'depth', 'prev_events', 'auth_events', 'origin_server_ts'];
  if (version === '10') retained.push('origin', 'prev_state', 'membership');
  const json: Record<string, unknown> = {};
  for (const key of retained) if (event[key] !== undefined) json[key] = event[key];
  const fields: Record<string, string[]> = {
    'm.room.create': ['creator'], 'm.room.member': ['membership', 'join_authorised_via_users_server'],
    'm.room.power_levels': ['ban', 'events', 'events_default', 'kick', 'redact', 'state_default', 'users', 'users_default', ...(version === '10' ? [] : ['invite'])],
    'm.room.join_rules': ['join_rule', 'allow'], 'm.room.history_visibility': ['history_visibility'],
    'm.room.redaction': version === '10' ? [] : ['redacts'],
  };
  json.content = version !== '10' && event.type === 'm.room.create' ? event.content :
    Object.fromEntries((fields[event.type] ?? []).filter(key => event.content[key] !== undefined).map(key => [key, event.content[key]]));
  return json;
}
function verifyPeerEvent(event: WireEvent, version: string): PDU {
  expect(event.event_id).toBeUndefined();
  if (event.type === 'm.room.create' && version === '12') expect(event.room_id).toBeUndefined();
  const key = event.sender.endsWith(':local.example') ? ctx.localKey : remoteKey;
  const server = event.sender.endsWith(':local.example') ? 'local.example' : remote;
  const publicJwk = { ...key.privateKeyJwk }; delete publicJwk.d;
  const signedJson = canonicalJson(peerSigningJson(event, version));
  expect(verify(null, Buffer.from(signedJson), createPublicKey({ key: publicJwk, format: 'jwk' }),
    Buffer.from(event.signatures![server][key.keyId], 'base64'))).toBe(true);
  const content = { ...event }; delete content.hashes; delete content.signatures; delete content.unsigned;
  expect(event.hashes?.sha256).toBe(createHash('sha256').update(canonicalJson(content)).digest('base64').replace(/=+$/, ''));
  const id = `$${createHash('sha256').update(signedJson).digest('base64url')}`;
  return { ...event, event_id: id, room_id: event.room_id ?? `!${id.slice(1)}` } as PDU;
}
async function federatedRequest(path: string, method = 'GET', body?: unknown) {
  const authorization = await signFederationRequest(method, path, remote, 'local.example', remoteKey, body);
  return federation.request(path, { method, headers: { Authorization: authorization, 'Content-Type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }) }, ctx.env);
}
async function localRequest(roomId: string, suffix: string, content: unknown) {
  return rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/${suffix}`, { method: 'PUT',
    headers: { Authorization: 'Bearer token', 'Content-Type': 'application/json' }, body: JSON.stringify(content) }, ctx.env,
    { waitUntil: () => {}, passThroughOnException: () => {} } as ExecutionContext);
}
async function create(version: string) {
  const response = await rooms.request('/_matrix/client/v3/createRoom', { method: 'POST', headers: { Authorization: 'Bearer token' },
    body: JSON.stringify({ room_version: version, preset: 'public_chat', name: 'Federation regression room' }) }, ctx.env);
  expect(response.status).toBe(200); return (await response.json() as { room_id: string }).room_id;
}
async function join(roomId: string, version: string, apiVersion = 'v2') {
  const make = await federatedRequest(`/_matrix/federation/v1/make_join/${encodeURIComponent(roomId)}/${encodeURIComponent(remoteUser)}?ver=${version}`);
  expect(make.status, await make.clone().text()).toBe(200);
  const template = (await make.json() as { event: WireEvent }).event;
  const createEvent = (await getRoomState(ctx.env.DB, roomId)).find(event => event.type === 'm.room.create')!;
  expect(template.auth_events.includes(createEvent.event_id)).toBe(version !== '12');
  const signed = await signEvent(template, version, remote, remoteKey);
  const eventId = await eventReferenceId(signed, version);
  const path = `/_matrix/federation/${apiVersion}/send_join/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`;
  const response = await federatedRequest(path, 'PUT', signed);
  expect(response.status, await response.clone().text()).toBe(200);
  const json = await response.json() as any;
  const result = apiVersion === 'v1' ? json[1] : json;
  const state = result.state.map((event: WireEvent) => verifyPeerEvent(event, version));
  const auth = result.auth_chain.map((event: WireEvent) => verifyPeerEvent(event, version));
  const event = verifyPeerEvent(result.event, version);
  expect(state.some((event: PDU) => event.state_key === remoteUser)).toBe(false);
  expect(() => validateJoinGraph([...state, ...auth], state, event, version)).not.toThrow();
  expect((await federatedRequest(path, 'PUT', signed)).status).toBe(200);
  return { event, result, state, auth, path, signed };
}

it.each(['10', '11', '12'].flatMap(version => ['v1', 'v2'].map(apiVersion => ({ version, apiVersion }))))
('serves an independently verifiable pre-join graph for version $version through $apiVersion', async ({ version, apiVersion }) => {
  const roomId = await create(version);
  // Replace auth state so the transitive chain contains historical power-level events.
  const oldPower = (await getRoomState(ctx.env.DB, roomId)).find(event => event.type === 'm.room.power_levels')!;
  expect((await localRequest(roomId, 'state/m.room.power_levels/', { ...oldPower.content, invite: 10 })).status).toBe(200);
  const joined = await join(roomId, version, apiVersion);
  expect(joined.auth.some((event: PDU) => event.event_id === oldPower.event_id)).toBe(true);
  const idsResponse = await federatedRequest(`/_matrix/federation/v1/state_ids/${encodeURIComponent(roomId)}`);
  expect(idsResponse.status).toBe(200);
  expect((await idsResponse.json() as { auth_chain_ids: string[] }).auth_chain_ids).toContain(oldPower.event_id);
  expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(roomId, remoteUser)).toMatchObject({ membership: 'join' });
  expect(queued.some(entry => entry.event_id === joined.event.event_id)).toBe(true);
});

it.each(['10', '11', '12'])('keeps the version-specific redaction target in federation event retrieval for version %s', async version => {
  const roomId = await create(version); await join(roomId, version);
  const message = await (await localRequest(roomId, 'send/m.room.message/redaction-source', { msgtype: 'm.text', body: 'Redaction test' })).json() as { event_id: string };
  const redaction = await (await localRequest(roomId, `redact/${encodeURIComponent(message.event_id)}/redaction-txn`, { reason: 'Cleanup' })).json() as { event_id: string };
  const response = await federatedRequest(`/_matrix/federation/v1/event/${encodeURIComponent(redaction.event_id)}`);
  expect(response.status).toBe(200);
  const wire = (await response.json() as { pdus: WireEvent[] }).pdus[0];
  expect(verifyPeerEvent(wire, version).event_id).toBe(redaction.event_id);
  if (version === '10') expect(wire.redacts).toBe(message.event_id);
  else expect(wire.content.redacts).toBe(message.event_id);
});

it.each(['10', '11', '12'])('keeps signatures and reference IDs in state, event_auth, event, backfill and missing-event responses for version %s', async version => {
  const roomId = await create(version); const joined = await join(roomId, version);
  const latest = await (await localRequest(roomId, 'send/m.room.message/serving-test', { msgtype: 'm.text', body: 'Signed timeline' })).json() as { event_id: string };
  const eventPath = `/_matrix/federation/v1/event/${encodeURIComponent(latest.event_id)}`;
  const single = await federatedRequest(eventPath);
  expect(single.status).toBe(200);
  expect(verifyPeerEvent((await single.json() as { pdus: WireEvent[] }).pdus[0], version).event_id).toBe(latest.event_id);
  const stateResponse = await federatedRequest(`/_matrix/federation/v1/state/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(latest.event_id)}`);
  expect(stateResponse.status, await stateResponse.clone().text()).toBe(200);
  const state = await stateResponse.json() as { pdus: WireEvent[]; auth_chain: WireEvent[] };
  for (const event of [...state.pdus, ...state.auth_chain]) verifyPeerEvent(event, version);
  const authResponse = await federatedRequest(`/_matrix/federation/v1/event_auth/${encodeURIComponent(roomId)}/${encodeURIComponent(latest.event_id)}`);
  expect(authResponse.status).toBe(200);
  for (const event of (await authResponse.json() as { auth_chain: WireEvent[] }).auth_chain) verifyPeerEvent(event, version);
  const historyResponse = await federatedRequest(`/_matrix/federation/v1/backfill/${encodeURIComponent(roomId)}?v=${encodeURIComponent(latest.event_id)}&limit=100`);
  expect(historyResponse.status).toBe(200);
  const history = (await historyResponse.json() as { pdus: WireEvent[] }).pdus.map(event => verifyPeerEvent(event, version));
  expect(history.some(event => event.type === 'm.room.create')).toBe(true);
  expect(history.some(event => event.event_id === latest.event_id)).toBe(false);
  const missingResponse = await federatedRequest(`/_matrix/federation/v1/get_missing_events/${encodeURIComponent(roomId)}`, 'POST', {
    earliest_events: [joined.state[0].event_id], latest_events: [latest.event_id], limit: 100, min_depth: 0,
  });
  expect(missingResponse.status).toBe(200);
  const missing = (await missingResponse.json() as { events: WireEvent[] }).events.map(event => verifyPeerEvent(event, version));
  expect(missing.some(event => event.event_id === latest.event_id || event.event_id === joined.state[0].event_id)).toBe(false);
  expect(missing.some(event => event.event_id === joined.event.event_id)).toBe(true);
});

it.each(['10', '11', '12'])('persists and federates a signed leave exactly once in version %s', async version => {
  const roomId = await create(version); await join(roomId, version); queued = [];
  const make = await federatedRequest(`/_matrix/federation/v1/make_leave/${encodeURIComponent(roomId)}/${encodeURIComponent(remoteUser)}`);
  expect(make.status).toBe(200);
  const template = (await make.json() as { event: WireEvent }).event;
  const createEvent = (await getRoomState(ctx.env.DB, roomId)).find(event => event.type === 'm.room.create')!;
  expect(template.auth_events.includes(createEvent.event_id)).toBe(version !== '12');
  const event = await signEvent(template, version, remote, remoteKey);
  const eventId = await eventReferenceId(event, version);
  const path = `/_matrix/federation/v2/send_leave/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`;
  for (let i = 0; i < 2; i++) expect((await federatedRequest(path, 'PUT', event)).status).toBe(200);
  expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?').get(roomId, remoteUser)).toMatchObject({ membership: 'leave' });
  expect((await getEvent(ctx.env.DB, eventId))?.content.membership).toBe('leave');
  expect(queued.filter(entry => entry.event_id === eventId)).toHaveLength(1);
});

it('does not acknowledge an unsigned departure or serve state for an event from another room', async () => {
  const roomId = await create('12'); await join(roomId, '12');
  const before = ctx.sqlite.prepare('SELECT COUNT(*) AS count FROM events').get();
  const make = await federatedRequest(`/_matrix/federation/v1/make_leave/${encodeURIComponent(roomId)}/${encodeURIComponent(remoteUser)}`);
  const template = (await make.json() as { event: WireEvent }).event;
  const unsigned = await federatedRequest(`/_matrix/federation/v2/send_leave/${encodeURIComponent(roomId)}/%24unsigned`, 'PUT', template);
  expect(unsigned.status).toBe(403);
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS count FROM events').get()).toEqual(before);
  const other = await create('12'); const otherCreate = (await getRoomState(ctx.env.DB, other))[0].event_id;
  const state = await federatedRequest(`/_matrix/federation/v1/state/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(otherCreate)}`);
  expect(state.status).toBe(404);
});

it('serves event-time state and the original pre-join response after subsequent state changes and cache expiry', async () => {
  const roomId = await create('11'); const joined = await join(roomId, '11');
  const oldTopic = await (await localRequest(roomId, 'state/m.room.topic/', { topic: 'Earlier topic' })).json() as { event_id: string };
  const message = await (await localRequest(roomId, 'send/m.room.message/event-time', { msgtype: 'm.text', body: 'Between topics' })).json() as { event_id: string };
  const nextTopic = await (await localRequest(roomId, 'state/m.room.topic/', { topic: 'Later topic' })).json() as { event_id: string };
  expect((await localRequest(roomId, 'state/m.room.name/', { name: 'Later room name' })).status).toBe(200);
  for (const eventId of [message.event_id, nextTopic.event_id]) {
    const response = await federatedRequest(`/_matrix/federation/v1/state/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(eventId)}`);
    expect(response.status).toBe(200);
    const state = (await response.json() as { pdus: WireEvent[] }).pdus.map(event => verifyPeerEvent(event, '11'));
    expect(state.find(event => event.type === 'm.room.topic')?.event_id).toBe(oldTopic.event_id);
    expect(state.find(event => event.type === 'm.room.name')?.content.name).toBe('Federation regression room');
    const ids = await federatedRequest(`/_matrix/federation/v1/state_ids/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(eventId)}`);
    expect(await ids.json()).toMatchObject({ pdu_ids: expect.arrayContaining([oldTopic.event_id]) });
  }
  await ctx.env.CACHE.delete(`federation:send-join-response:${roomId}:${joined.event.event_id}`);
  const retried = await federatedRequest(joined.path, 'PUT', joined.signed);
  expect(retried.status, await retried.clone().text()).toBe(200);
  const sorted = (state: WireEvent[]) => [...state].sort((a, b) => `${a.type}\0${a.state_key}`.localeCompare(`${b.type}\0${b.state_key}`));
  expect(sorted((await retried.json() as { state: WireEvent[] }).state)).toEqual(sorted(joined.result.state));
});

it('returns a clear missing-state error for an event with an unknown predecessor', async () => {
  const roomId = await create('11'); await join(roomId, '11');
  const { event } = await buildLocalRoomEvent(ctx.env, { roomId, sender: '@alice:local.example', type: 'm.room.message', content: { msgtype: 'm.text', body: 'Unknown history' } });
  event.prev_events = ['$missing-predecessor']; await storeEvent(ctx.env.DB, event);
  for (const endpoint of ['state', 'state_ids']) {
    const response = await federatedRequest(`/_matrix/federation/v1/${endpoint}/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(event.event_id)}`);
    expect(response.status).toBe(404);
    expect(await response.json()).toMatchObject({ errcode: 'M_NOT_FOUND', error: expect.stringContaining('Exact historical state') });
  }
});

it('serves archived peer state and its complete auth chain without publishing the historical message globally', async () => {
  const roomId = await create('11'); await join(roomId, '11');
  const baseline = await getRoomState(ctx.env.DB, roomId);
  const previousPower = baseline.find(event => event.type === 'm.room.power_levels')!;
  const power = (await buildLocalRoomEvent(ctx.env, { roomId, sender: '@alice:local.example', type: 'm.room.power_levels', stateKey: '', content: { ...previousPower.content, invite: 2 } })).event;
  async function archived(type: string, content: Record<string, unknown>, stateKey?: string) {
    const built = await buildLocalRoomEvent(ctx.env, { roomId, sender: '@alice:local.example', type, content, stateKey });
    const signed = await signEvent({ ...wireEvent(built.event, '11'), auth_events: built.event.auth_events.map(id => id === previousPower.event_id ? power.event_id : id) }, '11', ctx.env.SERVER_NAME, ctx.localKey);
    return { ...signed, room_id: roomId, event_id: await eventReferenceId(signed, '11') } as PDU;
  }
  const topic = await archived('m.room.topic', { topic: 'Historical archive' }, '');
  const message = await archived('m.room.message', { msgtype: 'm.text', body: 'Peer backfill only' });
  const state = [...baseline.filter(event => event.type !== 'm.room.power_levels'), power, topic];
  const auth = [...baseline, power];
  expect(() => validateJoinGraph([...auth, topic], state, message, '11')).not.toThrow();
  await ctx.env.DB.batch(await prepareVerifiedStateSnapshot(ctx.env.DB, message, state, auth, '11'));
  expect(await getEvent(ctx.env.DB, message.event_id)).toBeNull(); expect(await getEvent(ctx.env.DB, power.event_id)).toBeNull();
  const response = await federatedRequest(`/_matrix/federation/v1/state/${encodeURIComponent(roomId)}?event_id=${encodeURIComponent(message.event_id)}`);
  expect(response.status, await response.clone().text()).toBe(200);
  const result = await response.json() as { pdus: WireEvent[]; auth_chain: WireEvent[] };
  expect(result.pdus.map(event => verifyPeerEvent(event, '11')).find(event => event.type === 'm.room.topic')?.event_id).toBe(topic.event_id);
  expect(result.auth_chain.map(event => verifyPeerEvent(event, '11').event_id)).toContain(power.event_id);
});

it.each(['10', '11', '12'])('authorizes and co-signs an eligible restricted peer join without changing its signed payload in version %s', async version => {
  const allowedRoom = await create(version); await join(allowedRoom, version);
  const roomId = await create(version);
  expect((await localRequest(roomId, 'state/m.room.join_rules/', { join_rule: 'restricted', allow: [{ type: 'm.room_membership', room_id: allowedRoom }] })).status).toBe(200);
  const joined = await join(roomId, version);
  expect(joined.signed.content.join_authorised_via_users_server).toBe('@alice:local.example');
  expect(joined.signed.signatures?.[ctx.env.SERVER_NAME]).toBeUndefined();
  expect(joined.result.event.hashes).toEqual(joined.signed.hashes);
  expect(joined.result.event.signatures[remote]).toEqual(joined.signed.signatures?.[remote]);
  const publicJwk = { ...ctx.localKey.privateKeyJwk }; delete publicJwk.d;
  expect(verify(null, Buffer.from(canonicalJson(peerSigningJson(joined.result.event, version))), createPublicKey({ key: publicJwk, format: 'jwk' }),
    Buffer.from(joined.result.event.signatures[ctx.env.SERVER_NAME][ctx.localKey.keyId], 'base64'))).toBe(true);
  // An accepted join remains idempotent after the allowed-room membership changes.
  ctx.sqlite.prepare("UPDATE room_memberships SET membership='leave' WHERE room_id=? AND user_id=?").run(allowedRoom, remoteUser);
  await ctx.env.CACHE.delete(`federation:send-join-response:${roomId}:${joined.event.event_id}`);
  expect((await federatedRequest(joined.path, 'PUT', joined.signed)).status).toBe(200);
});

it('rejects a restricted peer who forges a local authorizer without membership in an allowed room', async () => {
  const allowedRoom = await create('11'); const roomId = await create('11');
  expect((await localRequest(roomId, 'state/m.room.join_rules/', { join_rule: 'restricted', allow: [{ type: 'm.room_membership', room_id: allowedRoom }] })).status).toBe(200);
  const make = await federatedRequest(`/_matrix/federation/v1/make_join/${encodeURIComponent(roomId)}/${encodeURIComponent(remoteUser)}?ver=11`);
  expect(make.status).toBe(403);
  const state = await getRoomState(ctx.env.DB, roomId);
  const latest = ctx.sqlite.prepare('SELECT event_id,depth FROM events WHERE room_id=? ORDER BY depth DESC,stream_ordering DESC LIMIT 1').get(roomId)!;
  const content = { membership: 'join', join_authorised_via_users_server: '@alice:local.example' };
  const signed = await signEvent({ room_id: roomId, sender: remoteUser, state_key: remoteUser, type: 'm.room.member', content,
    depth: Number(latest.depth) + 1, origin_server_ts: Date.now(), prev_events: [String(latest.event_id)],
    auth_events: selectAuthEvents(state, { roomId, sender: remoteUser, stateKey: remoteUser, type: 'm.room.member', content }, '11') }, '11', remote, remoteKey);
  const eventId = await eventReferenceId(signed, '11');
  const response = await federatedRequest(`/_matrix/federation/v2/send_join/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`, 'PUT', signed);
  expect(response.status).toBe(403); expect(await getEvent(ctx.env.DB, eventId)).toBeNull();
});
