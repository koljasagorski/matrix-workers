import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import media from '../src/api/media';
import { decodeFederationMedia } from '../src/services/remote-media';
import { testEnv } from './federation-helpers';
let ctx: Awaited<ReturnType<typeof testEnv>>;
const binary = new Uint8Array([137,80,78,71,13,10,26,10,0,255,128,34]);
function multipart(data = binary, headers = 'Content-Type: image/png', chunkSize = 7) {
  const bytes=Buffer.concat([Buffer.from('--matrix-media\r\nContent-Type: application/json\r\n\r\n{}\r\n--matrix-media\r\n'+headers+'\r\n\r\n'),data,Buffer.from('\r\n--matrix-media--\r\n')]);
  let at=0;
  return new Response(new ReadableStream({pull(controller) {
    if(at>=bytes.length){controller.close();return;}
    controller.enqueue(bytes.subarray(at,at+chunkSize)); at+=chunkSize;
  }}),{headers:{'Content-Type':'multipart/mixed; boundary="matrix-media"'}});
}
beforeEach(async()=>{ctx=await testEnv();await ctx.env.CACHE.put('discovery:remote.example',JSON.stringify({host:'delegated.example',port:443,tlsHostname:'delegated.example'}));});
afterEach(()=>{ctx.sqlite.close();vi.unstubAllGlobals();});
it.each([1,7,1024])('streams binary MIME intact across chunks of %i bytes',async size=>{
  const response=await decodeFederationMedia(multipart(binary,undefined,size));
  expect(response.headers.get('Content-Type')).toBe('image/png');
  expect(new Uint8Array(await response.arrayBuffer())).toEqual(binary);
});
it.each(['download','thumbnail'])('proxies authenticated %s with a server signature, never the user token',async action=>{
  vi.stubGlobal('fetch',vi.fn(async(input,init)=>{
    const url=new URL(String(input));
    expect(url.hostname).toBe('delegated.example');
    expect(url.pathname).toBe('/_matrix/federation/v1/media/'+action+'/avatar');
    expect(new Headers(init.headers).get('Authorization')).toMatch(/^X-Matrix /);
    expect(JSON.stringify(init)).not.toContain('Bearer token');
    if(action==='thumbnail')expect(url.searchParams.get('width')).toBe('96');
    return multipart();
  }));
  const r=await media.request('/_matrix/client/v1/media/'+action+'/remote.example/avatar?width=96&height=96',{headers:{Authorization:'Bearer token'}},ctx.env);
  expect(r.status).toBe(200);expect(r.headers.get('Content-Security-Policy')).toContain('sandbox');
  expect(new Uint8Array(await r.arrayBuffer())).toEqual(binary);
});
it('requires authentication and honors allow_remote=false on legacy URLs',async()=>{
  vi.stubGlobal('fetch',vi.fn());
  expect((await media.request('/_matrix/client/v1/media/download/remote.example/avatar',{},ctx.env)).status).toBe(401);
  expect((await media.request('/_matrix/media/v3/download/remote.example/avatar',{},ctx.env)).status).toBe(401);
  expect((await media.request('/_matrix/media/v3/download/remote.example/avatar?allow_remote=false',{headers:{Authorization:'Bearer token'}},ctx.env)).status).toBe(404);
  expect(fetch).not.toHaveBeenCalled();
});
it('blocks private destinations and unsafe CDN redirects',async()=>{
  vi.stubGlobal('fetch',vi.fn());
  const r=await media.request('/_matrix/client/v1/media/download/127.0.0.1/avatar',{headers:{Authorization:'Bearer token'}},ctx.env);
  expect(r.status).toBe(502);expect(fetch).not.toHaveBeenCalled();
  await expect(decodeFederationMedia(multipart(new Uint8Array(),'Location: http://169.254.169.254/credentials'))).rejects.toThrow();
  expect(fetch).not.toHaveBeenCalled();
});
it('follows a validated CDN location without forwarding authorization',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(_,init)=>{expect(init.headers).toBeUndefined();expect(init.redirect).toBe('manual');return new Response(binary,{headers:{'Content-Type':'image/png'}});}));
  const r=await decodeFederationMedia(multipart(new Uint8Array(),'Location: https://cdn.example/image'));
  expect(new Uint8Array(await r.arrayBuffer())).toEqual(binary);
});
it('rejects a truncated multipart body',async()=>{
  const source=multipart();const bytes=new Uint8Array(await source.arrayBuffer());
  const r=await decodeFederationMedia(new Response(bytes.slice(0,-24),{headers:source.headers}));
  await expect(r.arrayBuffer()).rejects.toThrow('Truncated');
});
it('falls back only for M_UNRECOGNIZED, with allow_remote=false',async()=>{
  const fetcher=vi.fn(async(input)=>{
    if(String(input).includes('/federation/'))return Response.json({errcode:'M_UNRECOGNIZED'},{status:404});
    expect(new URL(String(input)).searchParams.get('allow_remote')).toBe('false');
    return new Response(binary,{headers:{'Content-Type':'image/png'}});
  });vi.stubGlobal('fetch',fetcher);
  const r=await media.request('/_matrix/client/v1/media/download/remote.example/avatar',{headers:{Authorization:'Bearer token'}},ctx.env);
  expect(new Uint8Array(await r.arrayBuffer())).toEqual(binary);expect(fetcher).toHaveBeenCalledTimes(2);
  fetcher.mockImplementation(async()=>Response.json({errcode:'M_NOT_FOUND'},{status:404}));fetcher.mockClear();
  expect((await media.request('/_matrix/client/v1/media/download/remote.example/absent',{headers:{Authorization:'Bearer token'}},ctx.env)).status).toBe(404);
  expect(fetcher).toHaveBeenCalledTimes(1);
});

it('encodes local media in the federation MIME format consumed by other servers',async()=>{
  const {federationMediaResponse}=await import('../src/services/federation-media');
  const encoded=federationMediaResponse(new Response(binary).body!,'image/png','profile.png');
  expect(encoded.headers.get('Content-Type')).toMatch(/^multipart\/mixed; boundary=/);
  const decoded=await decodeFederationMedia(encoded);
  expect(new Uint8Array(await decoded.arrayBuffer())).toEqual(binary);
  expect(decoded.headers.get('Content-Disposition')).toContain('profile.png');
});
