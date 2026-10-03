import {expect,it,vi} from 'vitest';
import {testEnv} from './federation-helpers';
import {storeDeviceMessage} from '../src/services/device-messages';
vi.mock('cloudflare:workers',()=>({DurableObject:class{ctx:any;env:any;constructor(ctx:any,env:any){this.ctx=ctx;this.env=env;}}}));
import {SyncDurableObject} from '../src/durable-objects/SyncDurableObject';
it('closes the race between an empty Worker queue read and registering its wait',async()=>{
  const {env,sqlite}=await testEnv();
  try {
    await storeDeviceMessage(env.DB,{userId:'@alice:local.example',deviceId:'DEVICE',sender:'@alice:local.example',id:'test',type:'m.key.verification.request',content:{}});
    const object=new SyncDurableObject({} as any,env);
    const start=Date.now();
    const response=await object.fetch(new Request('https://internal/wait-for-events',{method:'POST',body:JSON.stringify({userId:'@alice:local.example',deviceId:'DEVICE',toDeviceSince:'0',timeout:25000})}));
    expect(await response.json()).toEqual({hasEvents:true});
    expect(Date.now()-start).toBeLessThan(1000);
    expect((object as any).waitingResolvers).toHaveLength(0);
  }finally{sqlite.close();}
});
it('wakes every waiting device without storing a fake room event',async()=>{
  const {env,sqlite}=await testEnv();
  try {
    const object=new SyncDurableObject({} as any,env);
    const wait=()=>object.fetch(new Request('https://internal/wait-for-events',{method:'POST',body:JSON.stringify({timeout:25000})}));
    const first=wait(),second=wait();
    await vi.waitFor(()=>expect((object as any).waitingResolvers).toHaveLength(2));
    await object.fetch(new Request('https://internal/notify-device',{method:'POST'}));
    expect(await(await first).json()).toEqual({hasEvents:true});
    expect(await(await second).json()).toEqual({hasEvents:true});
    expect((object as any).pendingEvents).toEqual([]);
    expect((object as any).waitingResolvers).toEqual([]);
  }finally{sqlite.close();}
});
