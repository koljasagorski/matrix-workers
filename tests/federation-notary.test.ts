import { expect, it, vi, afterEach } from 'vitest';
import { generateSigningKeyPair, signJson } from '../src/utils/crypto';
import { fetchHistoricalServerKeys } from '../src/services/federation-keys';
import { testEnv } from './federation-helpers';
afterEach(()=>vi.unstubAllGlobals());
it('requires the trusted notary signature for retired server keys',async()=>{
  const {env,sqlite}=await testEnv();
  const notary=await generateSigningKeyPair();
  const remote=await generateSigningKeyPair();
  await env.CACHE.put('federation:keys:matrix.org',JSON.stringify([{server_name:'matrix.org',key_id:notary.keyId,
    public_key:notary.publicKey,valid_from:0,valid_until:Date.now()+86400000,fetched_at:Date.now(),verified:true}]));
  let document=await signJson({server_name:'retired.example',valid_until_ts:Date.now()+86400000,
    verify_keys:{[remote.keyId]:{key:remote.publicKey}}},'matrix.org',notary.keyId,notary.privateKeyJwk);
  vi.stubGlobal('fetch',vi.fn(async(url:string,init:RequestInit)=>{
    expect(url).toBe(`https://matrix.org/_matrix/key/v2/query/retired.example/${encodeURIComponent(remote.keyId)}`);
    expect(init.redirect).toBe('manual');
    return Response.json({server_keys:[document]});
  }));
  try {
    expect(await fetchHistoricalServerKeys('retired.example',remote.keyId,env.DB,env.CACHE)).toMatchObject([{public_key:remote.publicKey,verified:true}]);
    document={...document,signatures:{}};
    await expect(fetchHistoricalServerKeys('retired.example',remote.keyId,env.DB,env.CACHE)).rejects.toThrow('No trusted historical keys');
  } finally {sqlite.close();}
});
