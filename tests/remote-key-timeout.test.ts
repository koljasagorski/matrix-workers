import { afterEach, expect, it, vi } from 'vitest';
import { testEnv } from './federation-helpers';
import { queryRemoteKeys } from '../src/services/remote-keys';
function hangs(signal: AbortSignal) {
  return new Promise<Response>((_,reject)=>{
    if(signal.aborted)reject(signal.reason);
    else signal.addEventListener('abort',()=>reject(signal.reason),{once:true});
  });
}
afterEach(()=>vi.unstubAllGlobals());
it.each([false,true])('bounds a %s key lookup while retaining successful servers',async claim=>{
  const {env,sqlite}=await testEnv();
  for (const server of ['slow.example','fast.example']) await env.CACHE.put('discovery:'+server,JSON.stringify({host:server,port:443,tlsHostname:server}));
  vi.stubGlobal('fetch',vi.fn(async(input,init)=>String(input).includes('slow.example') ? hangs(init.signal) :
    Response.json({[claim?'one_time_keys':'device_keys']:{'@bob:fast.example':{DEVICE:{key:'public'}}}})));
  try {
    const start=Date.now();
    const result=await queryRemoteKeys(env,{'@alice:slow.example':[], '@bob:fast.example':[]},claim,60);
    expect(Date.now()-start).toBeLessThan(1000);
    expect(result.failures).toHaveProperty('slow.example');
    expect(result[claim?'one_time_keys':'device_keys']).toHaveProperty('@bob:fast.example');
  } finally {sqlite.close();}
});
it('includes server discovery in the timeout budget',async()=>{
  const {env,sqlite}=await testEnv();
  vi.stubGlobal('fetch',vi.fn(async(_,init)=>hangs(init.signal)));
  try {
    const result=await queryRemoteKeys(env,{'@alice:uncached.example':[]},false,40);
    expect(result.failures).toHaveProperty('uncached.example');
    expect(fetch).toHaveBeenCalledTimes(1);
  }finally{sqlite.close();}
});
