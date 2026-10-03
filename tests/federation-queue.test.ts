import { afterEach, expect, it, vi } from 'vitest';
import { testEnv } from './federation-helpers';
vi.mock('cloudflare:workers', () => ({ DurableObject: class {
  ctx: any; env: any; constructor(ctx: any, env: any) { this.ctx=ctx;this.env=env; }
} }));
import { FederationDurableObject } from '../src/durable-objects/FederationDurableObject';
afterEach(()=>vi.unstubAllGlobals());
it('persists queued messages, signs delivery, and retries a failed destination with the same transaction ID', async()=>{
  const {env,sqlite}=await testEnv();
  const values=new Map<string,unknown>(); let alarm:number|null=null;
  const storage={get:async(k:string)=>values.get(k),put:async(k:string,v:unknown)=>{values.set(k,v);},
    delete:async(k:string)=>values.delete(k),list:async({prefix}:{prefix:string})=>new Map([...values].filter(([k])=>k.startsWith(prefix))),
    setAlarm:async(n:number)=>{alarm=n;},getAlarm:async()=>alarm};
  await env.CACHE.put('discovery:remote.example',JSON.stringify({host:'remote.example',port:443,tlsHostname:'remote.example'}));
  const sent: {url:string;body:string}[]=[];
  vi.stubGlobal('fetch',vi.fn(async(input:string,init:RequestInit)=>{
    expect(new Headers(init.headers).get('Authorization')).toMatch(/^X-Matrix /);
    expect(init.redirect).toBe('manual');
    sent.push({url:input,body:String(init.body)});
    return sent.length===1 ? new Response('{}',{status:503}) : Response.json({pdus:{}});
  }));
  try {
    const object=new FederationDurableObject({storage} as any,env);
    expect((await object.fetch(new Request('https://internal/send-edu',{method:'POST',body:JSON.stringify({destination:'remote.example',edu_type:'m.direct_to_device',content:{message_id:'message'}})}))).status).toBe(200);
    expect(fetch).not.toHaveBeenCalled(); expect(alarm).not.toBeNull();
    await object.alarm();
    expect([...values.keys()].some(k=>k.startsWith('edu:'))).toBe(true);
    const target=values.get('server:remote.example') as {nextRetry:number}; target.nextRetry=Date.now()-1;
    await object.alarm();
    expect(sent).toHaveLength(2); expect(sent[0]).toEqual(sent[1]);
    expect([...values.keys()].some(k=>k.startsWith('edu:'))).toBe(false);
  } finally {sqlite.close();}
});
