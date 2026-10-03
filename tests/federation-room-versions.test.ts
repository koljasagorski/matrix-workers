import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHash, createPrivateKey, createPublicKey, sign, verify } from 'node:crypto';
import rooms from '../src/api/rooms';
import { eventReferenceId, eventVerifier, signEvent, type WireEvent } from '../src/services/federation-events';
import { canonicalJson, generateSigningKeyPair, signJson } from '../src/utils/crypto';
import { testEnv } from './federation-helpers';

// Independent peer implementation: these tests must not generate peer signatures
// or auth references through the same redaction code that verifies them.
function signingJson(event: WireEvent, version: string) {
  const { type, sender, state_key, room_id, depth, auth_events, prev_events, origin_server_ts, hashes } = event;
  let content: Record<string, unknown> = {};
  if (type === 'm.room.create') content = version === '10' ? { creator: event.content.creator } : event.content;
  if (type === 'm.room.member') content = { membership: event.content.membership };
  if (type === 'm.room.join_rules') content = { join_rule: event.content.join_rule };
  if (type === 'm.room.power_levels') {
    content = { users: event.content.users };
    if (version !== '10') content.invite = event.content.invite;
  }
  return { type, sender, state_key, ...(room_id !== undefined ? { room_id } : {}), depth,
    auth_events, prev_events, origin_server_ts, hashes, content };
}
function peerReferenceId(event: WireEvent, version: string) {
  return `$${createHash('sha256').update(canonicalJson(signingJson(event, version))).digest('base64url')}`;
}

let ctx: Awaited<ReturnType<typeof testEnv>> | undefined;
afterEach(() => { ctx?.sqlite.close(); ctx = undefined; vi.unstubAllGlobals(); });

describe('federated joins with an independently signed invite power level', () => {
  it.each(['10', '11', '12'].flatMap(version => [false, true].map(cached => ({ version, cached }))))(
    'imports an invited encrypted room in version $version (cached: $cached)', async ({ version, cached }) => {
    ctx = await testEnv();
    const env = ctx.env;
    const remote = 'remote.example';
    const creator = `@creator:${remote}`;
    const localUser = '@alice:local.example';
    const key = await generateSigningKeyPair();
    const privateKey = createPrivateKey({ key: key.privateKeyJwk, format: 'jwk' });
    const keyResponse = await signJson({ server_name: remote, valid_until_ts: Date.now() + 86400000,
      verify_keys: { [key.keyId]: { key: key.publicKey } } }, remote, key.keyId, key.privateKeyJwk);
    let roomId = '!invited:remote.example';
    const state: WireEvent[] = [];
    function add(type: string, content: Record<string, unknown>, state_key = '', auth_events: string[] = []) {
      const event: WireEvent = { type, sender: creator, state_key, content, depth: state.length + 1,
        ...(version === '12' && type === 'm.room.create' ? {} : { room_id: roomId }),
        origin_server_ts: Date.now(), auth_events,
        prev_events: state.length ? [peerReferenceId(state.at(-1)!, version)] : [] };
      event.hashes = { sha256: createHash('sha256').update(canonicalJson(event)).digest('base64').replace(/=+$/, '') };
      event.signatures = { [remote]: { [key.keyId]: sign(null,
        Buffer.from(canonicalJson(signingJson(event, version))), privateKey).toString('base64').replace(/=+$/, '') } };
      state.push(event);
      return peerReferenceId(event, version);
    }
    const createId = add('m.room.create', { room_version: version, ...(version === '10' ? { creator } : {}) });
    if (version === '12') roomId = `!${createId.slice(1)}`;
    const createAuth = version === '12' ? [] : [createId];
    const creatorId = add('m.room.member', { membership: 'join' }, creator, createAuth);
    const powerId = add('m.room.power_levels', { users: version === '12' ? {} : { [creator]: 100 }, invite: 0 }, '', [...createAuth, creatorId]);
    const rulesId = add('m.room.join_rules', { join_rule: 'invite' }, '', [...createAuth, creatorId, powerId]);
    const inviteId = add('m.room.member', { membership: 'invite' }, localUser, [...createAuth, creatorId, powerId]);
    add('m.room.encryption', { algorithm: 'm.megolm.v1.aes-sha2' }, '', [...createAuth, creatorId, powerId]);
    const template: WireEvent = { type: 'm.room.member', room_id: roomId, sender: localUser, state_key: localUser,
      content: { membership: 'join' }, depth: state.length + 1, origin_server_ts: Date.now(),
      auth_events: [...createAuth, powerId, rulesId, inviteId], prev_events: [peerReferenceId(state.at(-1)!, version)] };
    let sendCount = 0;
    let makeCount = 0;
    await env.CACHE.put(`discovery:${remote}`, JSON.stringify({ host: remote, port: 443, tlsHostname: remote }));
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input : input.url);
      if (url.pathname === '/_matrix/key/v2/server') return Response.json(keyResponse);
      if (url.pathname.includes('/make_join/')) {
        makeCount++;
        return Response.json({ room_version: version, event: template });
      }
      if (url.pathname.includes('/send_join/')) {
        sendCount++;
        const event: WireEvent = JSON.parse(String(init?.body));
        const publicJwk = { ...ctx!.localKey.privateKeyJwk }; delete publicJwk.d;
        expect(verify(null, Buffer.from(canonicalJson(signingJson(event, version))),
          createPublicKey({ key: publicJwk, format: 'jwk' }),
          Buffer.from(event.signatures!['local.example'][ctx!.localKey.keyId], 'base64'))).toBe(true);
        expect(decodeURIComponent(url.pathname.split('/').at(-1)!)).toBe(peerReferenceId(event, version));
        return Response.json({ origin: remote, state, auth_chain: state.slice(0, 4), event });
      }
      throw new Error(`Unexpected federation request ${url}`);
    }));
    const power = state.find(event => event.type === 'm.room.power_levels')!;
    expect(await eventReferenceId(power, version)).toBe(powerId);
    expect((await eventVerifier(env)(power, version, roomId)).content.invite).toBe(0);

    const path = `/_matrix/client/v3/join/${encodeURIComponent(roomId)}?via=${remote}`;
    const pendingKey = `federation:pending-join:${roomId}:${localUser}`;
    if (cached) {
      // Retry an accepted handshake without sending a second join.
      const signed = await signEvent(template, version, 'local.example', ctx.localKey);
      await env.CACHE.put(pendingKey, JSON.stringify({ version, signed, server: remote,
        response: { origin: remote, state, auth_chain: state.slice(0, 4), event: signed } }));
    }
    const response = await rooms.request(path, {
      method: 'POST', headers: { Authorization: 'Bearer token' }, body: '{}',
    }, env);
    expect(await response.json()).toEqual({ room_id: roomId });
    expect(response.status).toBe(200);
    expect(makeCount).toBe(cached ? 0 : 1);
    expect(sendCount).toBe(cached ? 0 : 1);
    expect(await env.CACHE.get(pendingKey)).toBeNull();
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE room_id=? AND user_id=?')
      .get(roomId, localUser)).toMatchObject({ membership: 'join' });
    expect(ctx.sqlite.prepare("SELECT content FROM events WHERE event_id=?").get(powerId))
      .toMatchObject({ content: JSON.stringify(power.content) });
    expect(ctx.sqlite.prepare("SELECT count(*) AS n FROM room_state WHERE room_id=?").get(roomId)).toMatchObject({ n: 6 });
    expect(ctx.sqlite.prepare('SELECT * FROM federation_join_locks').all()).toEqual([]);
  });
});
