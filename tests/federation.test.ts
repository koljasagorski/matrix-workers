import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPublicKey, verify } from 'node:crypto';
import federation from '../src/api/federation';
import keys from '../src/api/keys';
import toDevice from '../src/api/to-device';
import { signFederationRequest } from '../src/services/federation-keys';
import rooms from '../src/api/rooms';
import { resolveRoomAlias, joinRemoteRoom } from '../src/services/remote-rooms';
import { eventReferenceId, eventVerifier, wireEvent, signEvent } from '../src/services/federation-events';
import { canonicalJson, signJson } from '../src/utils/crypto';
import { roomFixture, testEnv } from './federation-helpers';
let ctx: Awaited<ReturnType<typeof testEnv>>;
let fixture: Awaited<ReturnType<typeof roomFixture>>;
let sentJoins: Record<string, any>[];

beforeEach(async () => {
  ctx = await testEnv(); fixture = await roomFixture(); sentJoins = [];
  await ctx.env.CACHE.put('discovery:v2:remote.example', JSON.stringify({host:'delegated.example',port:443,tlsHostname:'delegated.example'}));
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input : input.url);
    if (url.pathname === '/_matrix/key/v2/server') {
      if (url.hostname === 'delegated.example') return Response.json(fixture.keyResponse);
      return Response.json(await signJson({ server_name:'local.example',valid_until_ts: Date.now()+86400000,
        verify_keys:{[ctx.localKey.keyId]:{key:ctx.localKey.publicKey}} }, 'local.example',ctx.localKey.keyId,ctx.localKey.privateKeyJwk));
    }
    if (url.pathname.endsWith('/.well-known/matrix/server')) return Response.json({ 'm.server': `${url.hostname}:443` });
    const header = new Headers(init?.headers).get('Authorization');
    expect(header).toMatch(/^X-Matrix /);
    const sig = header!.match(/sig="([^"]+)"/)![1];
    const signed: Record<string, unknown> = { method:init?.method,uri:url.pathname+url.search,origin:'local.example',destination:'remote.example' };
    if (init?.body) signed.content = JSON.parse(String(init.body));
    const publicJwk = { ...ctx.localKey.privateKeyJwk }; delete publicJwk.d;
    expect(verify(null,Buffer.from(canonicalJson(signed)),createPublicKey({key:publicJwk,format:'jwk'}),Buffer.from(sig,'base64'))).toBe(true);
    if (url.pathname.endsWith('/user/keys/query')) return Response.json({ device_keys: {
      '@creator:remote.example': { REMOTE: { user_id:'@creator:remote.example',device_id:'REMOTE' } },
      '@alice:local.example': { INJECTED: {} },
    }, master_keys: { '@creator:remote.example': { user_id:'@creator:remote.example',usage:['master'] } } });
    if (url.pathname.endsWith('/user/keys/claim')) return Response.json({ one_time_keys: {
      '@creator:remote.example': { REMOTE: { 'signed_curve25519:key': {key:'signed-remote-key'} } },
    } });
    if (url.pathname.endsWith('/query/directory')) return Response.json({room_id:fixture.roomId,servers:[fixture.remote]});
    if (url.pathname.includes('/hierarchy/')) return Response.json({ room: {room_id:fixture.roomId,name:'Hackerhütte',join_rule:'public',num_joined_members:159} });
    if (url.pathname.includes('/make_join/')) {
      expect(url.searchParams.getAll('ver')).toContain('12');
      return Response.json({room_version:fixture.version,event:fixture.template});
    }
    if (url.pathname.includes('/send_join/')) {
      const event = JSON.parse(String(init?.body)); sentJoins.push(event);
      expect(event.event_id).toBeUndefined();
      expect(decodeURIComponent(url.pathname.split('/').at(-1)!)).toBe(await eventReferenceId(event,fixture.version));
      await eventVerifier(ctx.env)(event,fixture.version,fixture.roomId);
      return Response.json({state:fixture.events,auth_chain:fixture.events,origin:fixture.remote,event});
    }
    throw new Error(`Unexpected fetch ${url}`);
  }));
});
afterEach(() => { ctx.sqlite.close(); vi.unstubAllGlobals(); });

describe('remote room discovery and joining', () => {
  it('resolves a remote alias using signed federation and discovery without creating a local alias', async () => {
    const response = await rooms.request('/_matrix/client/v3/directory/room/%23hackerhuette%3Aremote.example',{},ctx.env);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({room_id:fixture.roomId,servers:[fixture.remote]});
    expect(ctx.sqlite.prepare('SELECT * FROM room_aliases').all()).toEqual([]);
  });
  it.each(['/_matrix/client/v1/room_summary/', '/_matrix/client/unstable/im.nheko.summary/summary/'])('previews remote rooms through %s', async path => {
    const response = await rooms.request(`${path}${encodeURIComponent(fixture.roomId)}?via=remote.example`,{},ctx.env);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({name:'Hackerhütte',membership:'leave',num_joined_members:159});
  });
  it.each(['10','11','12'])('joins room version %s and persists verified state and membership before success', async version => {
    fixture = await roomFixture(version);
    const response = await rooms.request('/_matrix/client/v3/join/%23room%3Aremote.example',{
      method:'POST',headers:{Authorization:'Bearer token'},body:'{}',
    },ctx.env);
    expect(await response.clone().json()).toEqual({room_id:fixture.roomId});
    expect(response.status).toBe(200);
    expect(sentJoins).toHaveLength(1);
    expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships WHERE user_id=?').get('@alice:local.example')).toMatchObject({membership:'join'});
    expect(ctx.sqlite.prepare('SELECT room_version FROM rooms').get()).toMatchObject({room_version:version});
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM room_state').get()).toMatchObject({n:6});
    expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events WHERE stream_ordering IS NOT NULL').get()).toMatchObject({n:1});
    expect(ctx.sqlite.prepare('SELECT * FROM federation_join_locks').all()).toEqual([]);
  });
  it('joins an opaque room ID with server_name hints and is idempotent', async () => {
    const path=`/_matrix/client/v3/rooms/${encodeURIComponent(fixture.roomId)}/join?server_name=remote.example`;
    for (let i=0;i<2;i++) expect((await rooms.request(path,{method:'POST',headers:{Authorization:'Bearer token'},body:'{}'},ctx.env)).status).toBe(200);
    expect(sentJoins).toHaveLength(1);
  });
  it('does not send or persist a tampered make_join template', async () => {
    fixture.template.sender='@victim:local.example';
    await expect(joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example')).rejects.toThrow('Invalid make_join');
    expect(sentJoins).toHaveLength(0); expect(ctx.sqlite.prepare('SELECT * FROM rooms').all()).toEqual([]);
  });
  it('rejects a bad event signature and leaves no partial room', async () => {
    fixture.events[2].signatures![fixture.remote][fixture.key.keyId]='invalid';
    await expect(joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example')).rejects.toThrow('signature');
    expect(ctx.sqlite.prepare('SELECT * FROM rooms').all()).toEqual([]);
  });
  it('blocks private and malformed alias servers before fetching', async () => {
    await expect(resolveRoomAlias(ctx.env,'#room:127.0.0.1')).rejects.toThrow();
    await expect(resolveRoomAlias(ctx.env,'#room:evil.example/path')).rejects.toThrow();
    expect(fetch).not.toHaveBeenCalled();
  });
  it('does not allow two concurrent imports', async () => {
    ctx.sqlite.prepare('INSERT INTO federation_join_locks VALUES (?,?,?)').run(fixture.roomId,'existing',Date.now()+60000);
    await expect(joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example')).rejects.toMatchObject({status:429});
    expect(sentJoins).toHaveLength(0);
  });
  it('redacts untrusted content with a valid signature but mismatching content hash', async () => {
    const event = {...fixture.events.at(-1)!, content:{name:'tampered'}};
    const verified = await eventVerifier(ctx.env)(event,'12',fixture.roomId);
    expect(verified.content).toEqual({});
  });
  it('requires the sender signature, not an unrelated cosigner', async () => {
    const event = await signEvent({...fixture.events.at(-1)!,sender:'@outsider:other.example'},'12',fixture.remote,fixture.key);
    await expect(eventVerifier(ctx.env)(event,'12',fixture.roomId)).rejects.toThrow();
  });
  it('keeps the v12 create room_id out of wire events', async () => {
    const verified = await eventVerifier(ctx.env)(fixture.events[0],'12',fixture.roomId);
    expect(wireEvent(verified,'12').room_id).toBeUndefined();
  });
});

async function incoming(body: unknown, path = '/_matrix/federation/v1/send/test') {
  const authorization = await signFederationRequest('PUT',path,fixture.remote,'local.example',fixture.key,body);
  return federation.request(path,{method:'PUT',headers:{Authorization:authorization,'Content-Type':'application/json'},body:JSON.stringify(body)},ctx.env);
}

it('receives signed wire events without event_id and exposes them to sync exactly once', async () => {
  await joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example');
  const event = await signEvent({type:'m.room.encrypted',sender:'@creator:remote.example',room_id:fixture.roomId,
    content:{algorithm:'m.megolm.v1.aes-sha2',ciphertext:'ciphertext'},depth:7,origin_server_ts:Date.now(),
    auth_events:[await eventReferenceId(fixture.events[1],'12'),await eventReferenceId(fixture.events[2],'12')],
    prev_events:[await eventReferenceId(sentJoins[0] as any,'12')]},'12',fixture.remote,fixture.key);
  const id=await eventReferenceId(event,'12');
  for (let i=0;i<2;i++) {
    const response=await incoming({pdus:[event],edus:[]});
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({pdus:{[id]:{}}});
  }
  expect(ctx.sqlite.prepare('SELECT stream_ordering FROM events WHERE event_id=?').get(id)).toMatchObject({stream_ordering:2});
});
it('does not accept an unsigned event even from the authenticated sender server', async () => {
  await joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example');
  const response=await incoming({pdus:[{...fixture.template,sender:'@creator:remote.example'}]});
  expect(JSON.stringify(await response.json())).toContain('Malformed federation event');
  expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events WHERE stream_ordering IS NOT NULL').get()).toMatchObject({n:1});
});
it('requires a valid membership event before disclosing state through send_join', async () => {
  await joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example');
  const path=`/_matrix/federation/v2/send_join/${encodeURIComponent(fixture.roomId)}/%24fake`;
  const response=await incoming({},path);
  expect(response.status).toBe(403);
  expect(await response.text()).not.toContain('Remote room');
});
it('forwards remote device key queries and claims without accepting keys for another server', async () => {
  const query=await keys.request('/_matrix/client/v3/keys/query',{method:'POST',headers:{Authorization:'Bearer token'},
    body:JSON.stringify({device_keys:{'@creator:remote.example':[]}})},ctx.env);
  const result=await query.json();
  expect(result.failures).toEqual({});
  expect(result.device_keys['@creator:remote.example'].REMOTE.device_id).toBe('REMOTE');
  expect(result.device_keys['@alice:local.example']).toBeUndefined();
  const claim=await keys.request('/_matrix/client/v3/keys/claim',{method:'POST',headers:{Authorization:'Bearer token'},
    body:JSON.stringify({one_time_keys:{'@creator:remote.example':{REMOTE:'signed_curve25519'}}})},ctx.env);
  expect((await claim.json()).one_time_keys['@creator:remote.example'].REMOTE['signed_curve25519:key'].key).toBe('signed-remote-key');
});
it('routes encrypted device messages to the remote server and delivers inbound messages to local devices', async () => {
  const queued: any[]=[];
  ctx.env.FEDERATION={idFromName:(s:string)=>s,get:()=>({fetch:async(r:Request)=>{queued.push(await r.json());return Response.json({});}})} as any;
  const response=await toDevice.request('/_matrix/client/v3/sendToDevice/m.room.encrypted/txn',{method:'PUT',headers:{Authorization:'Bearer token'},
    body:JSON.stringify({messages:{'@creator:remote.example':{REMOTE:{ciphertext:'outbound'}}}})},ctx.env);
  expect(response.status).toBe(200);
  expect(queued[0]).toMatchObject({destination:fixture.remote,edu_type:'m.direct_to_device',content:{sender:'@alice:local.example'}});
  ctx.sqlite.prepare('INSERT INTO devices(user_id,device_id) VALUES (?,?)').run('@alice:local.example','DEVICE');
  const inbound={pdus:[],edus:[{edu_type:'m.direct_to_device',content:{sender:'@creator:remote.example',type:'m.room.encrypted',message_id:'remote-message',messages:{'@alice:local.example':{DEVICE:{ciphertext:'inbound'}}}}}]};
  for(let i=0;i<2;i++) expect((await incoming(inbound,`/_matrix/federation/v1/send/device${i}`)).status).toBe(200);
  expect(ctx.sqlite.prepare('SELECT content FROM to_device_messages').all()).toEqual([{content:'{"ciphertext":"inbound"}'}]);
});
it('signs and queues outgoing encrypted room messages once per transaction', async () => {
  await joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example');
  const queued:any[]=[];
  ctx.env.FEDERATION={idFromName:(s:string)=>s,get:()=>({fetch:async(r:Request)=>{queued.push(await r.json());return Response.json({});}})} as any;
  ctx.env.PUSH_NOTIFICATION_WORKFLOW={create:async()=>({})} as any;
  const execution={waitUntil:()=>{},passThroughOnException:()=>{}} as any;
  let eventId:string|undefined;
  for (let i=0;i<2;i++) {
    const response=await rooms.request(`/_matrix/client/v3/rooms/${encodeURIComponent(fixture.roomId)}/send/m.room.encrypted/txn`,
      {method:'PUT',headers:{Authorization:'Bearer token'},body:JSON.stringify({algorithm:'m.megolm.v1.aes-sha2',ciphertext:'outbound'})},ctx.env,execution);
    expect(response.status).toBe(200);
    const body=await response.json();
    if (eventId) expect(body.event_id).toBe(eventId);
    eventId=body.event_id;
  }
  expect(queued[0].pdu.event_id).toBeUndefined();
  expect((await eventVerifier(ctx.env)(queued[0].pdu,'12',fixture.roomId)).event_id).toBe(eventId);
  expect(ctx.sqlite.prepare('SELECT count(*) AS n FROM events WHERE stream_ordering IS NOT NULL').get()).toMatchObject({n:2});
});

it('propagates profile changes as signed membership events and safely retries them',async()=>{
  const {default:profile}=await import('../src/api/profile');
  await joinRemoteRoom(ctx.env,{room_id:fixture.roomId,servers:[fixture.remote]},'@alice:local.example');
  const queued:any[]=[];
  ctx.env.FEDERATION={idFromName:(s:string)=>s,get:()=>({fetch:async(r:Request)=>{queued.push(await r.json());return Response.json({});}})} as any;
  for(const [field,value] of [['avatar_url','mxc://local.example/photo'],['displayname','Alice'],['avatar_url',null]]) {
    const r=await profile.request('/_matrix/client/v3/profile/%40alice%3Alocal.example/'+field,{method:'PUT',headers:{Authorization:'Bearer token'},body:JSON.stringify({[field!]:value})},ctx.env);
    expect(r.status).toBe(200);
    const event=await eventVerifier(ctx.env)(queued.at(-1).pdu,'12',fixture.roomId);
    expect(event.type).toBe('m.room.member');expect(event.content.membership).toBe('join');
    expect(event.content[field!]).toBe(value??undefined);
  }
  const before=ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM events').get();
  await profile.request('/_matrix/client/v3/profile/%40alice%3Alocal.example/displayname',{method:'PUT',headers:{Authorization:'Bearer token'},body:'{"displayname":"Alice"}'},ctx.env);
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM events').get()).toEqual(before);
  await expect(eventVerifier(ctx.env)(queued.at(-1).pdu,'12',fixture.roomId)).resolves.toHaveProperty('type','m.room.member');
});

it('persists a signed incoming invitation and exposes stripped state to Element X without joining',async()=>{
  const {default:slidingSync}=await import('../src/api/sliding-sync');
  const {locateRoom}=await import('../src/services/remote-rooms');
  const invite=await signEvent({...fixture.template,type:'m.room.member',sender:'@creator:remote.example',state_key:'@alice:local.example',content:{membership:'invite'}},'12',fixture.remote,fixture.key);
  const id=await eventReferenceId(invite,'12');
  const path=`/_matrix/federation/v2/invite/${encodeURIComponent(fixture.roomId)}/${encodeURIComponent(id)}`;
  const body={room_version:'12',event:invite,invite_room_state:[{type:'m.room.name',state_key:'',sender:invite.sender,content:{name:'Members only'}}]};
  const authorization=await signFederationRequest('PUT',path,fixture.remote,'local.example',fixture.key,body);
  for(let i=0;i<2;i++){
    const r=await federation.request(path,{method:'PUT',headers:{Authorization:authorization},body:JSON.stringify(body)},ctx.env);
    expect(r.status).toBe(200);
    const signed=(await r.json()).event;
    expect(signed.signatures).toHaveProperty('local.example');
    expect(signed.signatures).toHaveProperty('remote.example');
    expect(await eventReferenceId(signed,'12')).toBe(id);
  }
  expect(ctx.sqlite.prepare('SELECT membership FROM room_memberships').get()).toMatchObject({membership:'invite'});
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM events').get()).toMatchObject({n:1});
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM room_state').get()).toMatchObject({n:0});
  expect((await locateRoom(ctx.env,fixture.roomId)).servers).toContain('remote.example');
  ctx.env.SYNC={idFromName:(s:string)=>s,get:()=>({fetch:async()=>Response.json(null)})} as any;
  const sync=await slidingSync.request('/_matrix/client/unstable/org.matrix.simplified_msc3575/sync',{method:'POST',headers:{Authorization:'Bearer token'},body:JSON.stringify({lists:{invites:{filters:{is_invite:true},timeline_limit:0}},extensions:{typing:{enabled:false}}})},ctx.env);
  expect(sync.status).toBe(200);
  const room=(await sync.json()).rooms[fixture.roomId];
  expect(room).toMatchObject({membership:'invite',name:'Members only'});
  expect(room.invite_state).toContainEqual(expect.objectContaining({type:'m.room.member',state_key:'@alice:local.example',content:{membership:'invite'}}));
  expect(room.timeline).toBeUndefined();
});
it('rejects forged invitation events even on signed federation requests',async()=>{
  const path=`/_matrix/federation/v2/invite/${encodeURIComponent(fixture.roomId)}/%24fake`;
  const body={room_version:'12',event:{...fixture.template,content:{membership:'invite'},sender:'@creator:remote.example'}};
  const authorization=await signFederationRequest('PUT',path,fixture.remote,'local.example',fixture.key,body);
  const r=await federation.request(path,{method:'PUT',headers:{Authorization:authorization},body:JSON.stringify(body)},ctx.env);
  expect(r.status).toBeGreaterThanOrEqual(400);
  expect(ctx.sqlite.prepare('SELECT COUNT(*) AS n FROM room_memberships').get()).toMatchObject({n:0});
});
