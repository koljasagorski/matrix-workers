import { afterEach, beforeEach, beforeAll, expect, it, vi } from 'vitest';
import account from '../src/api/account';
import devices from '../src/api/devices';
import { testEnv, memoryKV } from './federation-helpers';
import { hashPassword, hashToken } from '../src/utils/crypto';
vi.mock('cloudflare:workers', () => ({DurableObject:class {
  ctx:any;env:any;constructor(ctx:any,env:any){this.ctx=ctx;this.env=env;}
}}));
import { UserKeysDurableObject } from '../src/durable-objects/UserKeysDurableObject';
let ctx:Awaited<ReturnType<typeof testEnv>>;
let storage:Map<string,unknown>;
let passwordHash:string;
const user='@alice:local.example';
const headers={Authorization:'Bearer token','Content-Type':'application/json'};
const password='correct test password';
beforeAll(async()=>{passwordHash=await hashPassword(password);});
beforeEach(async()=>{
  ctx=await testEnv();storage=new Map();
  const kv:any={get:async(k:string)=>storage.get(k),put:async(k:string,v:unknown)=>{storage.set(k,v);},
    delete:async(k:string)=>storage.delete(k),transaction:async(fn:any)=>fn(kv)};
  const object=new UserKeysDurableObject({storage:kv} as any,ctx.env);
  ctx.env.USER_KEYS={idFromName:(s:string)=>s,get:()=>({fetch:(r:Request)=>object.fetch(r)})} as any;
  ctx.env.DEVICE_KEYS=memoryKV() as any;
  ctx.sqlite.prepare('UPDATE users SET password_hash=? WHERE user_id=?').run(passwordHash,user);
  for(const id of ['DEVICE','VICTIM']) {
    ctx.sqlite.prepare('INSERT INTO devices(user_id,device_id) VALUES (?,?)').run(user,id);
    storage.set(`device_keys:${id}`,{user_id:user,device_id:id,keys:{[`ed25519:${id}`]:'public'}});
    await ctx.env.DEVICE_KEYS.put(`device:${user}:${id}`,'public key');
  }
  storage.set('device_ids',['DEVICE','VICTIM']);
  ctx.sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)').run('victim',await hashToken('victim-token'),user,'VICTIM');
  ctx.sqlite.prepare("INSERT INTO one_time_keys(user_id,device_id,algorithm,key_id,key_data) VALUES (?,'VICTIM','signed_curve25519','one','{}')").run(user);
  ctx.sqlite.prepare("INSERT INTO fallback_keys(user_id,device_id,algorithm,key_id,key_data) VALUES (?,'VICTIM','signed_curve25519','fallback','{}')").run(user);
});
afterEach(()=>ctx.sqlite.close());
const endpoints=[
  {name:'deactivation',app:account,path:'/_matrix/client/v3/account/deactivate',method:'POST'},
  {name:'single device',app:devices,path:'/_matrix/client/v3/devices/VICTIM',method:'DELETE'},
  {name:'multiple devices',app:devices,path:'/_matrix/client/v3/delete_devices',method:'POST'},
];
const invalidAuth=[undefined,{},'password',false,[],{type:'m.login.dummy'},{type:'m.login.password'},
  ...[null,false,0,{},[], '', 'wrong'].map(password=>({type:'m.login.password',password})),
  {type:'m.login.password',password,identifier:{type:'m.id.user',user:'@bob:local.example'}},
  {type:'m.login.password',session:'pretend-completed-session'}];
for(const endpoint of endpoints) {
  it.each(invalidAuth.map((auth,index)=>({auth,index})))(`${endpoint.name} rejects invalid confirmation $index without changing account, sessions or keys`,async({auth})=>{
    const before=ctx.sqlite.prepare('SELECT * FROM access_tokens').all();
    const response=await endpoint.app.request(endpoint.path,{method:endpoint.method,headers,body:JSON.stringify({auth,devices:['VICTIM']})},ctx.env);
    expect([401,403]).toContain(response.status);
    expect(ctx.sqlite.prepare('SELECT * FROM access_tokens').all()).toEqual(before);
    expect(ctx.sqlite.prepare('SELECT is_deactivated FROM users WHERE user_id=?').get(user)?.is_deactivated).toBe(0);
    expect(ctx.sqlite.prepare('SELECT * FROM devices').all()).toHaveLength(2);
    expect(storage.has('device_keys:VICTIM')).toBe(true);
    expect(await ctx.env.DEVICE_KEYS.get(`device:${user}:VICTIM`)).not.toBeNull();
  });
  it(`${endpoint.name} fails closed when the account has no password`,async()=>{
    ctx.sqlite.prepare('UPDATE users SET password_hash=NULL WHERE user_id=?').run(user);
    const response=await endpoint.app.request(endpoint.path,{method:endpoint.method,headers,body:JSON.stringify({auth:{type:'m.login.password',password},devices:['VICTIM']})},ctx.env);
    expect(response.status).toBe(403);
    expect(ctx.sqlite.prepare('SELECT * FROM access_tokens').all()).toHaveLength(2);
  });
}
for(const endpoint of endpoints.slice(1)) it(`${endpoint.name} deletes only the selected device and its real key stores after correct confirmation`,async()=>{
  ctx.sqlite.prepare("INSERT INTO users(user_id,localpart) VALUES ('@bob:local.example','bob')").run();
  ctx.sqlite.prepare("INSERT INTO devices(user_id,device_id) VALUES ('@bob:local.example','VICTIM')").run();
  const response=await endpoint.app.request(endpoint.path,{method:endpoint.method,headers,body:JSON.stringify({auth:{type:'m.login.password',password},devices:['VICTIM']})},ctx.env);
  expect(response.status).toBe(200);
  expect(ctx.sqlite.prepare('SELECT device_id FROM devices WHERE user_id=?').all(user)).toEqual([{device_id:'DEVICE'}]);
  expect(ctx.sqlite.prepare("SELECT * FROM devices WHERE user_id='@bob:local.example'").all()).toHaveLength(1);
  expect(ctx.sqlite.prepare('SELECT device_id FROM access_tokens').all()).toEqual([{device_id:'DEVICE'}]);
  expect(storage.has('device_keys:VICTIM')).toBe(false);
  expect(storage.get('device_ids')).toEqual(['DEVICE']);
  expect(await ctx.env.DEVICE_KEYS.get(`device:${user}:VICTIM`)).toBeNull();
  expect(ctx.sqlite.prepare('SELECT * FROM one_time_keys').all()).toEqual([]);
  expect(ctx.sqlite.prepare('SELECT * FROM fallback_keys').all()).toEqual([]);
  expect(ctx.sqlite.prepare('SELECT change_type FROM device_key_changes').get()?.change_type).toBe('delete');
});
it('deactivates the authenticated synthetic account and revokes all its tokens after correct confirmation',async()=>{
  const response=await account.request('/_matrix/client/v3/account/deactivate',{method:'POST',headers,body:JSON.stringify({auth:{type:'m.login.password',password}})},ctx.env);
  expect(response.status).toBe(200);
  expect(ctx.sqlite.prepare('SELECT is_deactivated FROM users WHERE user_id=?').get(user)?.is_deactivated).toBe(1);
  expect(ctx.sqlite.prepare('SELECT * FROM access_tokens').all()).toEqual([]);
});
