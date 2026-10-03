import type { Env } from '../types';
import { parseUserId } from '../utils/ids';
import { federationPost } from './federation-keys';
import { readFederationJson } from './federation-http';
import { isObject } from './federation-events';

// Each destination can only answer for its requested users. Clients verify the key signatures.
export async function queryRemoteKeys(env: Env, requested: Record<string, unknown>, claim: boolean, timeout?: number) {
  const groups = new Map<string, Record<string, unknown>>();
  for (const [user, devices] of Object.entries(requested)) {
    const server = parseUserId(user)?.serverName;
    if (!server || server === env.SERVER_NAME) continue;
    if (!groups.has(server)) groups.set(server, {});
    groups.get(server)![user] = devices;
  }
  const device_keys: Record<string, unknown> = {}, master_keys: Record<string, unknown> = {},
    self_signing_keys: Record<string, unknown> = {}, one_time_keys: Record<string, unknown> = {}, failures: Record<string, unknown> = {};
  const entries = [...groups.entries()];
  // One deadline for the whole request, including discovery and response bodies.
  // A slow/offline homeserver must not make Element X cancel the key exchange.
  const budget = typeof timeout === 'number' && Number.isFinite(timeout) ? Math.min(10000, Math.max(1, timeout)) : 10000;
  const deadline = new AbortController();
  const timer = setTimeout(() => deadline.abort(new Error('Key query timed out')), budget);
  let next = 0;
  try {
    await Promise.all(Array.from({ length: Math.min(6, entries.length) }, async () => {
      while (next < entries.length) {
        const [server, users] = entries[next++];
        try {
          deadline.signal.throwIfAborted();
          const signal = AbortSignal.any([deadline.signal, AbortSignal.timeout(Math.min(5000, budget))]);
          const response = await federationPost(server, `/_matrix/federation/v1/user/keys/${claim ? 'claim' : 'query'}`,
            { [claim ? 'one_time_keys' : 'device_keys']: users }, env.SERVER_NAME, env.DB, env.CACHE, signal);
          const body = await readFederationJson(response, 4 * 1024 * 1024);
          signal.throwIfAborted();
          if (!response.ok || !isObject(body)) throw new Error('Remote keys unavailable');
          for (const user of Object.keys(users)) {
            for (const [name, target] of Object.entries({ device_keys, master_keys, self_signing_keys, one_time_keys })) {
              const source = body[name];
              if (isObject(source) && isObject(source[user])) target[user] = source[user];
            }
          }
        } catch { failures[server] = { errcode: 'M_UNAVAILABLE', error: 'Remote key server unavailable' }; }
      }
    }));
  } finally { clearTimeout(timer); }
  return { device_keys, master_keys, self_signing_keys, one_time_keys, failures };
}
