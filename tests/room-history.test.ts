import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import rooms from '../src/api/rooms';
import sync from '../src/api/sync';
import slidingSync from '../src/api/sliding-sync';
import { getRoomEvents } from '../src/services/database';
import { eventReferenceId, eventVerifier, signEvent, type WireEvent } from '../src/services/federation-events';
import { persistRemoteJoin } from '../src/services/remote-rooms';
import { roomFixture, testEnv } from './federation-helpers';

let ctx: Awaited<ReturnType<typeof testEnv>>;
let fixture: Awaited<ReturnType<typeof roomFixture>>;
let wire: Map<string, WireEvent>;
let snapshots: Map<string, WireEvent[]>;
let oldMessages: string[];
let base: string;
let peerFailure = false;
let tamper = false;
const headers = { Authorization: 'Bearer token' };

beforeEach(async () => {
  ctx = await testEnv(); fixture = await roomFixture(); wire = new Map(); snapshots = new Map(); oldMessages = [];
  peerFailure = false; tamper = false;
  base = `/_matrix/client/v3/rooms/${encodeURIComponent(fixture.roomId)}`;
  await ctx.env.CACHE.put('discovery:remote.example', JSON.stringify({host:'remote.example',port:443,tlsHostname:'remote.example'}));
  vi.stubGlobal('fetch', vi.fn(async (input: string | URL, init?: RequestInit) => {
    const url = new URL(input);
    if (url.pathname === '/_matrix/key/v2/server') return Response.json(fixture.keyResponse);
    expect(new Headers(init?.headers).get('Authorization')).toMatch(/^X-Matrix /);
    if (peerFailure) return Response.json({error:'Temporary failure'}, {status:503});
    if (url.pathname.includes('/backfill/')) {
      const queue = [...url.searchParams.getAll('v')];
      const visited = new Set<string>();
      while (queue.length && visited.size < Number(url.searchParams.get('limit'))) {
        const id = queue.shift()!;
        if (visited.has(id) || !wire.has(id)) continue;
        visited.add(id); queue.push(...wire.get(id)!.prev_events);
      }
      const pdus = [...visited].map(id => wire.get(id)!);
      if (tamper && pdus[1]) pdus[1] = { ...pdus[1], signatures: { [fixture.remote]: { [fixture.key.keyId]: 'invalid' } } };
      return Response.json({pdus});
    }
    if (url.pathname.includes('/state/')) {
      const state = snapshots.get(url.searchParams.get('event_id')!)!;
      return Response.json({pdus:state,auth_chain:fixture.events});
    }
    throw new Error(`Unexpected fetch ${url}`);
  }));
});
afterEach(() => { ctx.sqlite.close(); vi.unstubAllGlobals(); });

async function seed(visibility = 'shared') {
  let state: WireEvent[] = [];
  for (const event of fixture.events) {
    const id = await eventReferenceId(event, '12');
    snapshots.set(id, [...state]); wire.set(id, event); state.push(event);
  }
  const auth = await Promise.all([fixture.events[1], fixture.events[2]].map(e => eventReferenceId(e, '12')));
  const history = await signEvent({type:'m.room.history_visibility',state_key:'',room_id:fixture.roomId,sender:'@creator:remote.example',
    content:{history_visibility:visibility},auth_events:auth,prev_events:[await eventReferenceId(fixture.events.at(-1)!, '12')],depth:6,origin_server_ts:6},
  '12', fixture.remote, fixture.key);
  const historyId = await eventReferenceId(history, '12');
  wire.set(historyId, history); snapshots.set(historyId, [...state]); state.push(history);
  fixture.events.push(history);
  let previous = historyId;
  for (let i = 0; i < 4; i++) {
    const event = await signEvent({type:'m.room.encrypted',room_id:fixture.roomId,sender:'@creator:remote.example',
      content:{algorithm:'m.megolm.v1.aes-sha2',ciphertext:`old-ciphertext-${i}`},auth_events:auth,prev_events:[previous],depth:7+i,origin_server_ts:7+i},
    '12',fixture.remote,fixture.key);
    const id = await eventReferenceId(event,'12');
    wire.set(id,event); snapshots.set(id,[...state]); oldMessages.push(id); previous=id;
  }
  const signedJoin = await signEvent({...fixture.template,depth:11,prev_events:[previous]},'12',ctx.env.SERVER_NAME,ctx.localKey);
  const joinId = await eventReferenceId(signedJoin,'12'); wire.set(joinId,signedJoin);
  const verify = eventVerifier(ctx.env);
  const verified = await Promise.all(fixture.events.map(e=>verify(e,'12',fixture.roomId)));
  const join = await verify(signedJoin,'12',fixture.roomId);
  await persistRemoteJoin(ctx.env,'12',verified,verified,join,verified[0]);
}

it('paginates older encrypted messages across the join boundary without changing live state', async () => {
  await seed();
  const before = ctx.sqlite.prepare('SELECT * FROM room_state ORDER BY event_id').all();
  const first = await rooms.request(`${base}/messages?dir=b&from=s1&limit=2`,{headers},ctx.env);
  expect(first.status).toBe(200);
  const page1 = await first.json();
  expect(page1.chunk.map((e: {event_id:string})=>e.event_id)).toEqual(oldMessages.slice(2).reverse());
  expect(page1.end).toMatch(/^rh_/);
  const second = await rooms.request(`${base}/messages?dir=b&from=${page1.end}&limit=2`,{headers},ctx.env);
  expect(second.status).toBe(200);
  expect((await second.json()).chunk.map((e: {event_id:string})=>e.event_id)).toEqual(oldMessages.slice(0,2).reverse());
  expect(ctx.sqlite.prepare('SELECT * FROM room_state ORDER BY event_id').all()).toEqual(before);
  expect(ctx.sqlite.prepare('SELECT * FROM events WHERE stream_ordering IS NOT NULL').all()).toHaveLength(1);
  expect((await getRoomEvents(ctx.env.DB,fixture.roomId)).events).toHaveLength(1);
  const event = await rooms.request(`${base}/event/${encodeURIComponent(oldMessages[3])}`,{headers},ctx.env);
  expect(event.status).toBe(200);
  expect((await event.json()).content.ciphertext).toBe('old-ciphertext-3');
  // Cached replays return the same next position without fetching the peer again.
  const calls = vi.mocked(fetch).mock.calls.length;
  expect(await (await rooms.request(`${base}/messages?dir=b&from=s1&limit=2`,{headers},ctx.env)).json()).toEqual(page1);
  expect(vi.mocked(fetch).mock.calls.length).toBe(calls);
});

it('does not expose pre-join messages whose historical visibility was joined', async () => {
  await seed('joined');
  // A later change to shared must not make previously restricted events visible.
  ctx.sqlite.prepare("UPDATE events SET content='{\"history_visibility\":\"shared\"}' WHERE event_type='m.room.history_visibility'").run();
  const response = await rooms.request(`${base}/messages?dir=b&from=s1&limit=2`,{headers},ctx.env);
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({chunk:[],end:expect.stringMatching(/^rh_/)});
  expect((await rooms.request(`${base}/event/${encodeURIComponent(oldMessages[3])}`,{headers},ctx.env)).status).toBe(404);
});

it('binds historical cursors to the room and user and checks membership before cache access', async () => {
  await seed();
  const {end} = await (await rooms.request(`${base}/messages?dir=b&from=s1&limit=1`,{headers},ctx.env)).json();
  const cursor = JSON.parse((await ctx.env.CACHE.get(`room-history:cursor:${end}`))!);
  await ctx.env.CACHE.put(`room-history:cursor:${end}`,JSON.stringify({...cursor,userId:'@different:local.example'}));
  expect((await rooms.request(`${base}/messages?dir=b&from=${end}`,{headers},ctx.env)).status).toBe(400);
  ctx.sqlite.exec("UPDATE room_memberships SET membership='leave' WHERE user_id='@alice:local.example'");
  expect((await rooms.request(`${base}/messages?dir=b&from=${end}`,{headers},ctx.env)).status).toBe(403);
});

it.each(['peer failure','invalid signature'])('returns a retryable error rather than ending history on %s', async kind => {
  await seed(); peerFailure = kind === 'peer failure'; tamper = kind === 'invalid signature';
  const log = vi.spyOn(console,'error').mockImplementation(()=>{});
  const response = await rooms.request(`${base}/messages?dir=b&from=s1&limit=2`,{headers},ctx.env);
  expect(response.status).toBe(502);
  expect((await response.json()).end).toBeUndefined();
  log.mockRestore();
});

it('treats zero as an actual stream boundary and rejects malformed pagination', async () => {
  await seed();
  expect((await getRoomEvents(ctx.env.DB,fixture.roomId,0,10,'b')).events).toEqual([]);
  for (const query of ['limit=-1','limit=NaN','from=s1junk','dir=sideways']) {
    expect((await rooms.request(`${base}/messages?${query}`,{headers},ctx.env)).status).toBe(400);
  }
});

for (const client of ['classic', 'sliding']) it(`${client} includes authorized pre-join history in the first timeline and preserves current state`,async()=>{
  await seed();
  ctx.env.ROOMS={idFromName:(s:string)=>s,get:()=>({fetch:async()=>Response.json({receipts:{},user_ids:[]})})} as any;
  const before=ctx.sqlite.prepare('SELECT * FROM room_state ORDER BY event_id').all();
  const response=client==='classic'
    ? await sync.request('/_matrix/client/v3/sync',{headers},ctx.env)
    : await slidingSync.request('/_matrix/client/unstable/org.matrix.simplified_msc3575/sync',{method:'POST',headers,
      body:JSON.stringify({room_subscriptions:{[fixture.roomId]:{timeline_limit:10}}})},ctx.env);
  expect(response.status).toBe(200);
  const body=await response.json();
  const timeline=client==='classic'?body.rooms.join[fixture.roomId].timeline.events:body.rooms[fixture.roomId].timeline;
  expect(timeline.filter((e:any)=>e.type==='m.room.encrypted').map((e:any)=>e.event_id)).toEqual(oldMessages);
  expect(ctx.sqlite.prepare('SELECT * FROM room_state ORDER BY event_id').all()).toEqual(before);
  expect(ctx.sqlite.prepare('SELECT * FROM events WHERE stream_ordering IS NOT NULL').all()).toHaveLength(1);
});
it('accepts a composite sync position as a room pagination cursor',async()=>{
  await seed();
  const response=await rooms.request(`${base}/messages?dir=b&from=s1_td40_dk5&limit=2`,{headers},ctx.env);
  expect(response.status).toBe(200);
  expect((await response.json()).chunk.map((e:any)=>e.event_id)).toEqual(oldMessages.slice(2).reverse());
});
