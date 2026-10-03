import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { CallRoomDurableObject } from '../src/durable-objects/call-room';
import { createSession, addTracks, closeTracks } from '../src/services/cloudflare-calls';
import { testEnv } from './federation-helpers';

vi.mock('../src/services/cloudflare-calls', () => ({
  createSession: vi.fn(), addTracks: vi.fn(), renegotiate: vi.fn(), closeTracks: vi.fn(),
}));

const roomId = '!secure-call:local.example';
const userId = '@alice:local.example';
const join = JSON.stringify({ type: 'join', userId, deviceId: 'DEVICE' });
let ctx: Awaited<ReturnType<typeof testEnv>>;
let stored: Map<string, unknown>;
let sockets: FakeSocket[];
let durable: CallRoomDurableObject;

class FakeSocket {
  attachment: unknown = { userId, deviceId: 'DEVICE' };
  messages: Record<string, any>[] = [];
  closed = false;
  serializeAttachment(value: unknown) { this.attachment = structuredClone(value); }
  deserializeAttachment() { return structuredClone(this.attachment); }
  send(value: string) { this.messages.push(JSON.parse(value)); }
  close() { this.closed = true; }
}

async function restore() {
  let initialized: Promise<unknown>;
  const state = {
    storage: {
      get: async (key: string) => stored.get(key),
      put: async (key: string | Record<string, unknown>, value?: unknown) => {
        if (typeof key === 'string') stored.set(key, value);
        else for (const [name, entry] of Object.entries(key)) stored.set(name, entry);
      },
      deleteAll: async () => stored.clear(),
    },
    blockConcurrencyWhile: (callback: () => Promise<unknown>) => { initialized = callback(); return initialized; },
    getWebSockets: () => sockets.filter(socket => !socket.closed),
    acceptWebSocket: (socket: FakeSocket) => { sockets.push(socket); },
    setWebSocketAutoResponse: vi.fn(),
  };
  const object = new CallRoomDurableObject(state as any, ctx.env);
  await initialized!;
  return object;
}

function socket() { const value = new FakeSocket(); sockets.push(value); return value; }
function signal(value: FakeSocket, message: string) { return durable.webSocketMessage(value as any, message); }
async function participants() { return (await (await durable.fetch(new Request('https://internal/state'))).json()).participants; }

beforeEach(async () => {
  ctx = await testEnv();
  ctx.sqlite.prepare('INSERT INTO rooms(room_id) VALUES (?)').run(roomId);
  ctx.sqlite.prepare("INSERT INTO room_memberships(room_id,user_id,membership,event_id) VALUES (?,?,'join','$joined')").run(roomId, userId);
  stored = new Map([['callId', 'test-call'], ['matrixRoomId', roomId]]);
  sockets = [];
  vi.stubGlobal('WebSocketRequestResponsePair', class {});
  vi.mocked(createSession).mockReset().mockResolvedValue({ sessionId: 'sfu-session' });
  vi.mocked(addTracks).mockReset().mockResolvedValue({
    sessionDescription: { type: 'answer', sdp: 'answer-sdp' },
    tracks: [{ mid: '0', trackName: 'camera', location: 'local' }], requiresImmediateRenegotiation: false,
  } as any);
  vi.mocked(closeTracks).mockReset().mockResolvedValue({} as any);
  durable = await restore();
});
afterEach(() => { ctx.sqlite.close(); vi.unstubAllGlobals(); });

it('refuses websocket upgrades without the trusted identity supplied by the authenticated API', async () => {
  const response = await durable.fetch(new Request('https://internal/ws', { headers: { Upgrade: 'websocket' } }));
  expect(response.status).toBe(401);
  expect(sockets).toEqual([]);
});

it('rejects spoofed user or device identities before allocating an SFU session', async () => {
  const value = socket();
  for (const identity of [{ userId: '@victim:local.example', deviceId: 'DEVICE' }, { userId, deviceId: 'VICTIM' }]) {
    await signal(value, JSON.stringify({ type: 'join', ...identity }));
    expect(value.messages.at(-1)).toMatchObject({ type: 'error', code: 'FORBIDDEN' });
  }
  expect(createSession).not.toHaveBeenCalled();
  expect(await participants()).toEqual([]);
});

it('preserves an authenticated join and rejects a duplicate join for that device', async () => {
  const value = socket();
  await signal(value, join);
  expect(value.messages.at(-1)).toMatchObject({ type: 'welcome', callId: 'test-call' });
  expect(value.attachment).toMatchObject({ userId, deviceId: 'DEVICE', participant: { sessionId: 'sfu-session' } });
  await signal(value, join);
  expect(value.messages.at(-1)).toMatchObject({ type: 'error', code: 'ALREADY_JOINED' });
  expect(createSession).toHaveBeenCalledTimes(1);
});

it('reserves the device before asynchronous SFU creation so simultaneous joins allocate one session', async () => {
  let release!: (value: { sessionId: string }) => void;
  const pending = new Promise<{ sessionId: string }>(resolve => { release = resolve; });
  vi.mocked(createSession).mockReturnValueOnce(pending);
  const value = socket();
  const first = signal(value, join);
  await vi.waitFor(() => expect(createSession).toHaveBeenCalledTimes(1));
  await signal(value, join);
  expect(value.messages.at(-1)).toMatchObject({ code: 'ALREADY_JOINED' });
  release({ sessionId: 'one-session' });
  await first;
  expect(createSession).toHaveBeenCalledTimes(1);
  expect(await participants()).toHaveLength(1);
});

it('restores participant sessions, published tracks and later mute changes across hibernation', async () => {
  const value = socket();
  await signal(value, join);
  await signal(value, JSON.stringify({ type: 'offer', sdp: 'offer-sdp', trackName: 'camera', kind: 'video' }));
  durable = await restore();
  expect(await participants()).toMatchObject([{ oderId: userId, deviceId: 'DEVICE', sessionId: 'sfu-session',
    tracks: [{ trackName: 'camera', kind: 'video', enabled: true }] }]);
  await signal(value, JSON.stringify({ type: 'mute', trackName: 'camera', muted: true }));
  durable = await restore();
  expect((await participants())[0].tracks[0].enabled).toBe(false);
  expect(createSession).toHaveBeenCalledTimes(1);
});

it('closes a departed room member and removes their call attachment before another SFU operation', async () => {
  const value = socket();
  await signal(value, join);
  ctx.sqlite.prepare("UPDATE room_memberships SET membership='leave' WHERE room_id=? AND user_id=?").run(roomId, userId);
  await signal(value, JSON.stringify({ type: 'offer', sdp: 'offer-sdp', trackName: 'camera', kind: 'video' }));
  expect(value.messages.at(-1)).toMatchObject({ code: 'FORBIDDEN' });
  expect(value.closed).toBe(true);
  expect(addTracks).not.toHaveBeenCalled();
  expect(await participants()).toEqual([]);
  durable = await restore();
  expect(await participants()).toEqual([]);
});

it('ends the in-memory session as well as persisted state and refuses later joins', async () => {
  const value = socket();
  await signal(value, join);
  expect((await durable.fetch(new Request('https://internal/end', { method: 'POST' }))).status).toBe(200);
  expect(stored.size).toBe(0);
  await signal(value, join);
  expect(value.messages.at(-1)).toMatchObject({ code: 'UNAUTHORIZED' });
  expect(createSession).toHaveBeenCalledTimes(1);
  expect(await participants()).toEqual([]);
});

it('rejects non-object signaling JSON and reconnects sockets that lack a trusted attachment after eviction', async () => {
  const value = socket();
  for (const body of ['null', '[]', '"join"']) {
    await signal(value, body);
    expect(value.messages.at(-1)).toMatchObject({ code: 'INVALID_MESSAGE' });
  }
  value.attachment = null;
  durable = await restore();
  expect(value.closed).toBe(true);
  expect(createSession).not.toHaveBeenCalled();
});
