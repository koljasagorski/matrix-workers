import { afterEach, expect, it, vi } from 'vitest';
import { discoverServer, buildServerUrl } from '../src/services/server-discovery';
afterEach(() => vi.unstubAllGlobals());
const dns = () => Response.json({ Status: 0 });

it('follows bounded validated well-known redirects and discovers the delegated443 endpoint', async () => {
  const urls: string[] = [];
  vi.stubGlobal('fetch', vi.fn(async (input: string, options: RequestInit) => {
    urls.push(input); expect(options.redirect).toBe('manual');
    if (urls.length === 1) return new Response(null, { status: 301, headers: { Location: 'https://www.example.org/.well-known/matrix/server' } });
    if (urls.length === 2) return new Response(null, { status: 307, headers: { Location: '/matrix-discovery.json' } });
    return Response.json({ 'm.server': 'matrix.example.org:443' });
  }));
  expect(buildServerUrl(await discoverServer('example.org'))).toBe('https://matrix.example.org');
  expect(urls).toEqual(['https://example.org/.well-known/matrix/server', 'https://www.example.org/.well-known/matrix/server', 'https://www.example.org/matrix-discovery.json']);
});

it.each(['https://127.0.0.1/x', 'https://2130706433/x', 'https://0x7f000001/x', 'https://10.0.0.2/x', 'https://169.254.169.254/x', 'https://[::1]/x', 'https://[::ffff:127.0.0.1]/x', 'https://[::ffff:7f00:1]/x', 'https://localhost/x', 'https://localhost./x', 'https://sub.localhost./x', 'https://service.internal/x', 'https://sub.metadata.google.internal/x', 'https://user:password@public.example/x', 'http://public.example/x'])('never fetches an unsafe redirect %s', async location => {
  const urls: string[] = [];
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    urls.push(input);
    if (input === 'https://example.org/.well-known/matrix/server') return new Response(null, { status: 302, headers: { Location: location } });
    expect(input.startsWith('https://cloudflare-dns.com/')).toBe(true);
    return dns();
  }));
  expect(buildServerUrl(await discoverServer('example.org'))).toBe('https://example.org:8448');
  expect(urls).toHaveLength(3); // Original well-known and two public DNS queries.
});

it('terminates loops and redirect chains without unbounded requests', async () => {
  let requests = 0;
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    if (input.startsWith('https://cloudflare-dns.com/')) return dns();
    requests++;
    return new Response(null, { status: 302, headers: { Location: 'https://example.org/.well-known/matrix/server' } });
  }));
  await discoverServer('example.org'); expect(requests).toBe(1);
  requests = 0;
  vi.stubGlobal('fetch', vi.fn(async (input: string) => {
    if (input.startsWith('https://cloudflare-dns.com/')) return dns();
    requests++;
    return new Response(null, { status: 302, headers: { Location: `https://public.example/${requests}` } });
  }));
  await discoverServer('example.org'); expect(requests).toBe(6);
});

it('refreshes the known bad cached default endpoint using the new cache generation', async () => {
  const put = vi.fn(); const get = vi.fn(async key => key === 'discovery:mozilla.org' ? JSON.stringify({ host: 'mozilla.org', port: 8448 }) : null);
  vi.stubGlobal('fetch', vi.fn(async () => Response.json({ 'm.server': 'mozilla.modular.im:443' })));
  expect(buildServerUrl(await discoverServer('mozilla.org', { get, put } as any))).toBe('https://mozilla.modular.im');
  expect(get).toHaveBeenCalledWith('discovery:v2:mozilla.org');
  expect(put).toHaveBeenCalledWith('discovery:v2:mozilla.org', expect.any(String), { expirationTtl: 3600 });
});
