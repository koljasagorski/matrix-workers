import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import slidingSync from '../src/api/sliding-sync';
import { testEnv } from './federation-helpers';
import { hashToken } from '../src/utils/crypto';

let ctx: Awaited<ReturnType<typeof testEnv>>;
const room = '!room:local.example';
const user = '@alice:local.example';
const config = { required_state: [['m.room.name', '']], timeline_limit: 1 };
const states = new Map<string, unknown>();
beforeEach(async () => {
  ctx = await testEnv(); states.clear();
  ctx.env.SYNC = { idFromName: (s: string) => s, get: () => ({ fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input)); const key = url.searchParams.get('conn_id')!;
    if (init?.method === 'PUT') { states.set(key, JSON.parse(String(init.body))); return Response.json({}); }
    return Response.json(states.get(key) ?? null);
  } }) } as any;
  ctx.sqlite.prepare("INSERT INTO rooms (room_id,room_version,creator_id) VALUES (?,'12',?)").run(room,user);
  ctx.sqlite.prepare("INSERT INTO room_memberships (room_id,user_id,membership,event_id) VALUES (?,?,'join','$join')").run(room,user);
  event('$one',1); event('$two',2);
  // Imported authentication history must not turn into the live timeline.
  event('$historic', null);
});
afterEach(() => ctx.sqlite.close());
function event(id: string, pos: number | null) {
  ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,event_type,sender,content,origin_server_ts,depth,stream_ordering,auth_events,prev_events) VALUES (?,?,'m.room.encrypted',?,'{"algorithm":"m.megolm.v1.aes-sha2","ciphertext":"encrypted"}',?,? ,?,'[]','[]')`).run(id,room,user,Date.now(),pos??0,pos);
}
async function sync(body: unknown, pos?: string, token='token') {
  const r = await slidingSync.request('/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0'+(pos?'&pos='+encodeURIComponent(pos):''), {
    method:'POST', headers:{Authorization:'Bearer '+token}, body:JSON.stringify(body),
  },ctx.env);
  expect(r.status).toBe(200); return await r.json() as any;
}
const both = {conn_id:'room-list',lists:{all:config},room_subscriptions:{[room]:{...config,timeline_limit:20}},extensions:{typing:{enabled:false}}};
it('combines lists and subscriptions before consuming initial and new room events', async () => {
  const first = await sync(both);
  expect(first.rooms[room].timeline.map((e:any)=>e.event_id)).toEqual(['$one','$two']);
  event('$three',3);
  const next = await sync(both,first.pos);
  expect(next.rooms[room].timeline.map((e:any)=>e.event_id)).toEqual(['$three']);
  expect(next.rooms[room].num_live).toBe(1);
});
it('expands the timeline when a room is opened after a one-event room-list preview', async () => {
  const first=await sync({ ...both,room_subscriptions:{} });
  expect(first.rooms[room].timeline.map((e:any)=>e.event_id)).toEqual(['$two']);
  const opened=await sync(both,first.pos);
  expect(opened.rooms[room].timeline.map((e:any)=>e.event_id)).toEqual(['$one','$two']);
  expect(opened.rooms[room].unstable_expanded_timeline).toBe(true);
  expect(opened.rooms[room].num_live).toBe(0);
});
it('starts fresh without pos and isolates identical connection names across devices', async () => {
  const first=await sync(both);
  expect((await sync(both)).rooms[room].initial).toBe(true);
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)').run('second',await hashToken('second-token'),user,'SECOND');
  expect((await sync(both,undefined,'second-token')).rooms[room].initial).toBe(true);
  expect(states.size).toBe(2);
  expect(first.rooms[room].timeline).toHaveLength(2);
});

it('marks an incomplete initial timeline as limited so clients can request remote history', async () => {
  expect((await sync(both)).rooms[room].limited).toBe(false);
  ctx.sqlite.prepare("UPDATE events SET prev_events='[\"$missing-remote-event\"]' WHERE event_id='$one'").run();
  const initial = await sync(both);
  expect(initial.rooms[room].limited).toBe(true);
  expect(initial.rooms[room].prev_batch).toBe('s1');
  const next = await sync(both, initial.pos);
  expect(next.rooms[room]?.timeline ?? []).toEqual([]);
});

it.each([
  '/_matrix/client/unstable/org.matrix.msc3575/sync',
  '/_matrix/client/unstable/org.matrix.simplified_msc3575/sync',
  '/_matrix/client/v4/sync',
])('protects timelines, receipts, and typing for unauthorized subscriptions at %s', async path => {
  const memberships = ['leave', 'ban', 'knock', 'invite', undefined];
  const hiddenRooms = memberships.map((_, index) => `!hidden${index}:local.example`);
  for (let i = 0; i < hiddenRooms.length; i++) {
    const id = hiddenRooms[i];
    ctx.sqlite.prepare("INSERT INTO rooms(room_id,room_version,creator_id) VALUES (?,'12',?)").run(id, user);
    if (memberships[i]) ctx.sqlite.prepare('INSERT INTO room_memberships(room_id,user_id,membership,event_id) VALUES (?,?,?,?)')
      .run(id, user, memberships[i], '$membership');
    ctx.sqlite.prepare(`INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
      VALUES (?, ?, ?, 'm.room.encrypted', '{"ciphertext":"private"}', 1, 1, '[]', '[]', ?)`)
      .run(`$hidden${i}`, id, user, 10 + i);
  }
  const roomFetch = vi.fn(async (request: Request) => {
    return new URL(request.url).pathname === '/typing'
      ? Response.json({ user_ids: ['@secret:elsewhere.example'] })
      : Response.json({ receipts: { '$secret': { 'm.read': { '@secret:elsewhere.example': { ts: 100 } } } } });
  });
  ctx.env.ROOMS = { idFromName: (s: string) => s, get: () => ({ fetch: roomFetch }) } as any;
  const response = await slidingSync.request(path + '?timeout=0', {
    method: 'POST', headers: { Authorization: 'Bearer token' }, body: JSON.stringify({
      conn_id: 'privacy', room_subscriptions: Object.fromEntries(hiddenRooms.map(id => [id, config])),
      extensions: { typing: { enabled: true }, receipts: { enabled: true } },
    }),
  }, ctx.env);
  expect(response.status).toBe(200);
  const data = await response.json() as any;
  for (const id of hiddenRooms) {
    expect(data.rooms[id]?.timeline ?? []).toEqual([]);
    expect(data.extensions.receipts?.rooms[id]).toBeUndefined();
    expect(data.extensions.typing?.rooms[id]?.content.user_ids ?? []).toEqual([]);
  }
  expect(roomFetch).not.toHaveBeenCalled();
});
