import { Buffer } from 'node:buffer';
import type { Env } from '../types';
import { federationGet } from './federation-keys';
import { assertFederationServer, assertFederationUrl, readFederationJson } from './federation-http';
import { Errors } from '../utils/errors';

const MAX_MEDIA = 50 * 1024 * 1024;
const MAX_HEADERS = 64 * 1024;

// Read only the small MIME headers/metadata into memory. The media itself is
// streamed, including when a MIME delimiter spans network chunks.
class MultipartReader {
  private pending = Buffer.alloc(0);
  private done = false;
  constructor(private reader: ReadableStreamDefaultReader<Uint8Array>) {}
  async more() {
    const next = await this.reader.read();
    this.done = next.done;
    if (next.value) this.pending = Buffer.concat([this.pending, next.value]);
  }
  async until(delimiter: Buffer, limit = MAX_HEADERS): Promise<Buffer> {
    while (true) {
      const at = this.pending.indexOf(delimiter);
      if (at >= 0) {
        if (at > limit) throw new Error('Media metadata too large');
        const value = this.pending.subarray(0, at);
        this.pending = this.pending.subarray(at + delimiter.length);
        return value;
      }
      if (this.done || this.pending.length > limit + delimiter.length) throw new Error('Malformed media multipart response');
      await this.more();
    }
  }
  cancel() { return this.reader.cancel().catch(() => {}); }
  stream(boundary: string): ReadableStream<Uint8Array> {
    const delimiter = Buffer.from(`\r\n--${boundary}`);
    let total = 0;
    return new ReadableStream<Uint8Array>({
      pull: async controller => {
        try {
          while (true) {
            const at = this.pending.indexOf(delimiter);
            if (at >= 0) {
              while (this.pending.length < at + delimiter.length + 2 && !this.done) await this.more();
              if (this.pending.subarray(at + delimiter.length, at + delimiter.length + 2).toString() !== '--') {
                throw new Error('Expected exactly two media parts');
              }
              total += at;
              if (total > MAX_MEDIA) throw new Error('Remote media too large');
              if (at) controller.enqueue(this.pending.subarray(0, at));
              controller.close(); await this.cancel(); return;
            }
            if (this.done) throw new Error('Truncated media response');
            const safeBytes = this.pending.length - delimiter.length;
            if (safeBytes > 0) {
              total += safeBytes;
              if (total > MAX_MEDIA) throw new Error('Remote media too large');
              controller.enqueue(this.pending.subarray(0, safeBytes));
              this.pending = Buffer.from(this.pending.subarray(safeBytes));
              return;
            }
            await this.more();
          }
        } catch (error) { await this.cancel(); controller.error(error); }
      },
      cancel: () => this.cancel(),
    });
  }
}
function parseHeaders(bytes: Buffer): Headers {
  const headers = new Headers();
  for (const line of bytes.toString('utf8').split('\r\n')) {
    const colon = line.indexOf(':');
    if (colon < 1) throw new Error('Malformed media headers');
    headers.append(line.slice(0, colon), line.slice(colon + 1).trim());
  }
  const encoding = headers.get('Content-Transfer-Encoding');
  if (encoding && !['binary', '8bit'].includes(encoding.toLowerCase())) throw new Error('Unsupported media transfer encoding');
  return headers;
}
function mediaResponse(body: ReadableStream<Uint8Array>, source: Headers, filename?: string): Response {
  const headers = new Headers({
    'Content-Type': source.get('Content-Type') || 'application/octet-stream',
    'Cache-Control': 'private, max-age=86400',
    'X-Content-Type-Options': 'nosniff',
    'Content-Security-Policy': "sandbox; default-src 'none'; script-src 'none'; object-src 'none'",
  });
  if (filename) headers.set('Content-Disposition', `inline; filename*=UTF-8''${encodeURIComponent(filename).replace(/'/g, '%27')}`);
  else if (source.has('Content-Disposition')) headers.set('Content-Disposition', source.get('Content-Disposition')!);
  return new Response(body, { headers });
}
function boundedBody(response: Response): ReadableStream<Uint8Array> {
  if (!response.body || Number(response.headers.get('Content-Length')) > MAX_MEDIA) {
    void response.body?.cancel(); throw new Error('Remote media too large or empty');
  }
  let size = 0;
  return response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      size += chunk.byteLength;
      if (size > MAX_MEDIA) throw new Error('Remote media too large');
      controller.enqueue(chunk);
    },
  }));
}
async function fetchLocation(location: string): Promise<Response> {
  for (let redirects = 0; redirects <= 3; redirects++) {
    assertFederationUrl(location);
    // Never forward either the user's token or a federation signature to a CDN.
    const response = await fetch(location, { redirect: 'manual', signal: AbortSignal.timeout(20000) });
    if (response.status >= 300 && response.status < 400 && response.headers.has('Location')) {
      await response.body?.cancel();
      location = new URL(response.headers.get('Location')!, location).href;
      continue;
    }
    if (!response.ok) { await response.body?.cancel(); throw new Error('Media location unavailable'); }
    return response;
  }
  throw new Error('Too many media redirects');
}
export async function decodeFederationMedia(response: Response, filename?: string): Promise<Response> {
  const type = response.headers.get('Content-Type') ?? '';
  const boundary = /^multipart\/mixed\s*;/i.test(type)
    ? /(?:^|;)\s*boundary=(?:"([^"\r\n]+)"|([^;\s]+))/i.exec(type) : null;
  const value = boundary?.[1] ?? boundary?.[2];
  if (!value || value.length > 70 || !response.body) { await response.body?.cancel(); throw new Error('Invalid federation media format'); }
  const reader = new MultipartReader(response.body.getReader());
  try {
    await reader.until(Buffer.from(`--${value}\r\n`));
    const metadataHeaders = parseHeaders(await reader.until(Buffer.from('\r\n\r\n')));
    if (!metadataHeaders.get('Content-Type')?.startsWith('application/json')) throw new Error('Invalid media metadata type');
    const metadata = JSON.parse((await reader.until(Buffer.from(`\r\n--${value}\r\n`))).toString());
    if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) throw new Error('Invalid media metadata');
    const headers = parseHeaders(await reader.until(Buffer.from('\r\n\r\n')));
    if (headers.has('Location')) {
      await reader.cancel();
      const located = await fetchLocation(headers.get('Location')!);
      return mediaResponse(boundedBody(located), located.headers, filename);
    }
    if (Number(headers.get('Content-Length')) > MAX_MEDIA) throw new Error('Remote media too large');
    return mediaResponse(reader.stream(value), headers, filename);
  } catch (error) { await reader.cancel(); throw error; }
}

export async function remoteMedia(env: Env, server: string, mediaId: string, options: {
  thumbnail?: { width: number; height: number; method: string; animated?: boolean };
  filename?: string;
} = {}): Promise<Response> {
  try {
    assertFederationServer(server);
    if (!mediaId || mediaId.length > 255 || /[\x00-\x20/\\]/.test(mediaId)) return Errors.invalidParam('mediaId').toResponse();
    const action = options.thumbnail ? 'thumbnail' : 'download';
    const query = new URLSearchParams({ timeout_ms: '20000' });
    if (options.thumbnail) {
      const { width, height, method, animated } = options.thumbnail;
      if (![width,height].every(n => Number.isInteger(n) && n > 0 && n <= 1920) || !['crop','scale'].includes(method)) return Errors.invalidParam('thumbnail').toResponse();
      query.set('width', String(width)); query.set('height', String(height)); query.set('method', method);
      query.set('animated', String(animated ?? false));
    }
    let response = await federationGet(server, `/_matrix/federation/v1/media/${action}/${encodeURIComponent(mediaId)}?${query}`,
      env.SERVER_NAME, env.DB, env.CACHE);
    if (response.ok) return await decodeFederationMedia(response, options.filename);
    const error = await readFederationJson(response, MAX_HEADERS) as { errcode?: string };
    if (response.status === 404 && error.errcode === 'M_UNRECOGNIZED') {
      query.set('allow_remote','false');
      response = await federationGet(server, `/_matrix/media/v3/${action}/${encodeURIComponent(server)}/${encodeURIComponent(mediaId)}?${query}`,
        env.SERVER_NAME, env.DB, env.CACHE);
      if (response.ok) return mediaResponse(boundedBody(response), response.headers, options.filename);
      await response.body?.cancel();
    }
    if (response.status === 404) return Errors.notFound('Remote media not found').toResponse();
    return Response.json({ errcode: 'M_UNKNOWN', error: 'Remote media unavailable' }, { status: 502 });
  } catch (error) {
    console.warn('[media] Remote media fetch failed:', error instanceof Error ? error.message : 'Unknown error');
    return Response.json({ errcode: 'M_UNKNOWN', error: 'Unable to retrieve remote media' }, { status: 502 });
  }
}
