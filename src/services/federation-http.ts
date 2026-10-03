import { isValidServerName } from '../utils/ids';
import { validateUrl } from '../utils/url-validator';

export function assertFederationServer(server: string): void {
  if (!isValidServerName(server) || !validateUrl(`https://${server}/`).valid) {
    throw new Error('Invalid or private federation server');
  }
}
export function assertFederationUrl(url: string): void {
  const parsed = new URL(url);
  if (parsed.protocol !== 'https:' || parsed.username || parsed.password || !validateUrl(url).valid) {
    throw new Error('Invalid or private federation URL');
  }
}
// Bound untrusted responses even when a peer omits or lies about Content-Length.
export async function readFederationJson(response: Response, limit = 2 * 1024 * 1024): Promise<unknown> {
  if (Number(response.headers.get('Content-Length')) > limit) {
    await response.body?.cancel();
    throw new Error('Federation response too large');
  }
  if (!response.body) throw new Error('Empty federation response');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let size = 0, text = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) throw new Error('Federation response too large');
      text += decoder.decode(value, { stream: true });
    }
    return JSON.parse(text + decoder.decode());
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  } finally { reader.releaseLock(); }
}
