import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import toDevice, {getToDeviceMessages} from '../src/api/to-device';
import slidingSync from '../src/api/sliding-sync';
import sync from '../src/api/sync';
import keys from '../src/api/keys';
import {storeDeviceMessage} from '../src/services/device-messages';
import {hashToken} from '../src/utils/crypto';
import {testEnv} from './federation-helpers';

let ctx: Awaited<ReturnType<typeof testEnv>>;
let waitCalls: number;
let wakeCalls: number;
let onWait: (() => Promise<void>) | undefined;
let log: ReturnType<typeof vi.spyOn>;
const user='@alice:local.example';
const path='/_matrix/client/unstable/org.matrix.simplified_msc3575/sync';
const headers={Authorization:'Bearer token','Content-Type':'application/json'};

beforeEach(async()=>{
  ctx=await testEnv();waitCalls=0;wakeCalls=0;onWait=undefined;
  log=vi.spyOn(console,'log').mockImplementation(()=>{});
  for(const device of ['DEVICE','DESKTOP'])ctx.sqlite.prepare('INSERT INTO devices(user_id,device_id) VALUES (?,?)').run(user,device);
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)').run('desktop',await hashToken('desktop-token'),user,'DESKTOP');
  const states=new Map<string,unknown>();
  ctx.env.SYNC={idFromName:(s:string)=>s,get:()=>({fetch:async(input:Request|URL,init?:RequestInit)=>{
    const request=input instanceof Request?input:new Request(input,init);
    const url=new URL(request.url);
    if(url.pathname==='/notify-device'){wakeCalls++;return Response.json({success:true});}
    if(url.pathname==='/wait-for-events'){
      waitCalls++;await onWait?.();return Response.json({hasEvents:!!onWait});
    }
    const id=url.searchParams.get('conn_id')!;
    if(request.method==='PUT'){states.set(id,await request.json());return Response.json({});}
    return Response.json(states.get(id)??null);
  }})} as any;
});
afterEach(()=>{ctx.sqlite.close();log.mockRestore();});

async function message(id='request',device='DEVICE') {
  await storeDeviceMessage(ctx.env.DB,{userId:user,deviceId:device,sender:user,type:'m.key.verification.request',
    content:{from_device:'DESKTOP',transaction_id:id,methods:['m.sas.v1'],timestamp:Date.now()},id});
}
async function sliding(pos?:string, since='0'){
  return slidingSync.request(path+`?timeout=${pos?'30000':'0'}`+(pos?`&pos=${pos}`:''),{method:'POST',headers,
    body:JSON.stringify({conn_id:'verification',extensions:{to_device:{enabled:true,since}}})},ctx.env);
}

it('returns waiting verification requests immediately without a long-poll timeout',async()=>{
  const first=await(await sliding()).json();
  await message();
  const response=await sliding(first.pos);
  expect(response.status).toBe(200);
  expect((await response.json()).extensions.to_device.events[0].type).toBe('m.key.verification.request');
  expect(waitCalls).toBe(0);
});

it('delivers messages arriving during a sliding-sync wait in the same response',async()=>{
  const first=await(await sliding()).json();
  onWait=async()=>{await message();};
  const response=await sliding(first.pos);
  expect(waitCalls).toBe(1);
  expect((await response.json()).extensions.to_device.events).toHaveLength(1);
});

it('delivers messages arriving during a classic-sync wait with a separate acknowledgement cursor',async()=>{
  // Unrelated room activity advances only the room stream.
  ctx.sqlite.exec(`INSERT INTO rooms(room_id) VALUES ('!unrelated:local.example');
    INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
    VALUES ('$unrelated','!unrelated:local.example','@alice:local.example','m.room.message','{}',1,1,'[]','[]',1)`);
  onWait=async()=>{await message();};
  const response=await sync.request('/_matrix/client/v3/sync?since=s1_td0&timeout=30000',{headers},ctx.env);
  expect(response.status).toBe(200);
  const body=await response.json();
  expect(waitCalls).toBe(1);
  expect(body.to_device.events).toHaveLength(1);
  expect(body.next_batch).toBe('s1_td1_dk0');
});

it('does not skip a message inserted just after an empty queue snapshot',async()=>{
  const prepare=ctx.env.DB.prepare.bind(ctx.env.DB);
  let injected=false;
  const raceDb={prepare:(sql:string)=>{
    const statement=prepare(sql);
    if(!sql.includes('SELECT id, sender_user_id'))return statement;
    const bind=statement.bind.bind(statement);
    statement.bind=(...args:unknown[])=>{
      const bound=bind(...args);const all=bound.all.bind(bound);
      bound.all=async()=>{const result=await all();if(!injected){injected=true;await message();}return result;};
      return bound;
    };
    return statement;
  }} as D1Database;
  const empty=await getToDeviceMessages(raceDb,user,'DEVICE','0');
  expect(empty).toEqual({events:[],nextBatch:'0'});
  expect((await getToDeviceMessages(ctx.env.DB,user,'DEVICE',empty.nextBatch)).events).toHaveLength(1);
});

it('replays unacknowledged messages and acknowledges only the requesting device and returned page',async()=>{
  await message('one');await message('two');await message('other','DESKTOP');
  const page=await getToDeviceMessages(ctx.env.DB,user,'DEVICE','0',1);
  expect((await getToDeviceMessages(ctx.env.DB,user,'DEVICE','0',1)).events).toEqual(page.events);
  const next=await getToDeviceMessages(ctx.env.DB,user,'DEVICE',page.nextBatch,1);
  expect(next.events[0].content.transaction_id).toBe('two');
  expect((await getToDeviceMessages(ctx.env.DB,user,'DESKTOP','0')).events).toHaveLength(1);
});

it('isolates transaction IDs by sending device and event type and wakes the recipient',async()=>{
  async function send(token:string,type:string){return toDevice.request(`/_matrix/client/v3/sendToDevice/${type}/same-id`,{
    method:'PUT',headers:{...headers,Authorization:`Bearer ${token}`},body:JSON.stringify({messages:{[user]:{DEVICE:{transaction_id:'verification'}}}}),
  },ctx.env);}
  expect((await send('token','m.key.verification.request')).status).toBe(200);
  expect((await send('token','m.key.verification.request')).status).toBe(200);
  expect((await send('desktop-token','m.key.verification.request')).status).toBe(200);
  expect((await send('desktop-token','m.key.verification.ready')).status).toBe(200);
  expect(ctx.sqlite.prepare('SELECT * FROM to_device_messages').all()).toHaveLength(3);
  expect(wakeCalls).toBe(3);
});

it('refreshes verification signatures using the key stream even when room positions are much higher',async()=>{
  ctx.sqlite.exec(`INSERT INTO rooms(room_id) VALUES ('!unrelated:local.example');
    INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events,stream_ordering)
    VALUES ('$unrelated','!unrelated:local.example','@alice:local.example','m.room.message','{}',1,1,'[]','[]',100)`);
  const initial=await(await sync.request('/_matrix/client/v3/sync',{headers},ctx.env)).json();
  const initialSliding=await(await slidingSync.request(path,{method:'POST',headers,body:JSON.stringify({extensions:{e2ee:{enabled:true}}})},ctx.env)).json();
  ctx.sqlite.exec("UPDATE stream_positions SET position=1 WHERE stream_name='device_keys'");
  ctx.sqlite.prepare("INSERT INTO device_key_changes(user_id,device_id,change_type,stream_position) VALUES (?,'DESKTOP','update',1)").run(user);
  const classic=await(await sync.request(`/_matrix/client/v3/sync?since=${initial.next_batch}&timeout=30000`,{headers},ctx.env)).json();
  expect(classic.device_lists).toEqual({changed:[user],left:[]});
  expect(classic.next_batch).toBe('s100_td0_dk1');
  const slide=await(await slidingSync.request(path+`?pos=${initialSliding.pos}&timeout=30000`,{method:'POST',headers,body:JSON.stringify({extensions:{e2ee:{enabled:true}}})},ctx.env)).json();
  expect(slide.extensions.e2ee.device_lists).toEqual({changed:[user],left:[]});
  expect(slide.pos).toBe('100_dk1');
  expect(waitCalls).toBe(0);
  const repeat=await(await sync.request(`/_matrix/client/v3/sync?since=${classic.next_batch}`,{headers},ctx.env)).json();
  expect(repeat.device_lists?.changed??[]).toEqual([]);
});

it('reports a deleted own device as a changed user, including accounts with no joined rooms',async()=>{
  ctx.sqlite.prepare("INSERT INTO device_key_changes(user_id,device_id,change_type,stream_position) VALUES (?,'DESKTOP','delete',2)").run(user);
  const response=await keys.request('/_matrix/client/v3/keys/changes?from=s100_td30_dk1&to=s101_td40_dk2',{headers},ctx.env);
  expect(await response.json()).toEqual({changed:[user],left:[]});
});
it('returns uploaded signatures on the cross-signing master key as well as device keys',async()=>{
  const master={user_id:user,usage:['master'],keys:{'ed25519:master-public':'master-public'}};
  ctx.env.USER_KEYS={idFromName:(s:string)=>s,get:()=>({fetch:async(req:Request)=>
    Response.json(new URL(req.url).pathname==='/cross-signing/get'?{master}:{})})} as any;
  ctx.sqlite.prepare('INSERT INTO cross_signing_signatures(user_id,key_id,signer_user_id,signer_key_id,signature) VALUES (?,?,?,?,?)')
    .run(user,'master-public',user,'ed25519:DESKTOP','saved-signature');
  const response=await keys.request('/_matrix/client/v3/keys/query',{method:'POST',headers,body:JSON.stringify({device_keys:{[user]:[]}})},ctx.env);
  expect(response.status).toBe(200);
  expect((await response.json()).master_keys[user].signatures[user]['ed25519:DESKTOP']).toBe('saved-signature');
});
