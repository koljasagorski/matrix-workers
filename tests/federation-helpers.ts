import { DatabaseSync } from 'node:sqlite';
import { readdirSync, readFileSync } from 'node:fs';
import { generateSigningKeyPair, signJson, hashToken } from '../src/utils/crypto';
import { signEvent, eventReferenceId, type WireEvent } from '../src/services/federation-events';
import type { Env } from '../src/types';

export function memoryKV() {
  const data = new Map<string, string>();
  return { get: async (key: string, format?: string) => {
    const value = data.get(key) ?? null;
    return format === 'json' && value ? JSON.parse(value) : value;
  }, put: async (key: string, value: string) => { data.set(key, value); }, delete: async (key: string) => { data.delete(key); } };
}
export async function testEnv() {
  const sqlite = new DatabaseSync(':memory:');
  let batchQueue: Promise<unknown> = Promise.resolve();
  for (const f of readdirSync('migrations').filter(f => f.endsWith('.sql')).sort()) sqlite.exec(readFileSync(`migrations/${f}`, 'utf8'));
  function prepare(sql: string, args: (string | number | null)[] = []): any {
    return { bind: (...bound: (string | number | null)[]) => prepare(sql, bound),
      first: async () => sqlite.prepare(sql).get(...args) ?? null,
      all: async () => ({ results: sqlite.prepare(sql).all(...args), success: true }),
      run: async () => {
        const statement = sqlite.prepare(sql);
        return statement.columns().length
          ? { success: true, results: statement.all(...args), meta: { changes: 0 } }
          : { success: true, results: [], meta: statement.run(...args) };
      },
    };
  }
  const env = {
    SERVER_NAME: 'local.example', DB: { prepare, batch: async (statements: { run(): Promise<unknown> }[]) => {
      // Real D1 batches are serialized transactions, including across requests.
      const result = batchQueue.then(async () => {
        sqlite.exec('BEGIN');
        try { const results = []; for (const s of statements) results.push(await s.run()); sqlite.exec('COMMIT'); return results; }
        catch (e) { sqlite.exec('ROLLBACK'); throw e; }
      });
      batchQueue = result.catch(() => {});
      return result;
    } }, CACHE: memoryKV(), SESSIONS: memoryKV(),
    SYNC: { idFromName: (s: string) => s, get: () => ({ fetch: async () => Response.json({}) }) },
  } as unknown as Env;
  const localKey = await generateSigningKeyPair();
  sqlite.prepare("INSERT INTO server_keys(key_id,public_key,private_key_jwk,private_key,key_version,valid_from,is_current) VALUES (?,?,?,'jwk',2,?,1)")
    .run(localKey.keyId, localKey.publicKey, JSON.stringify(localKey.privateKeyJwk), Date.now());
  sqlite.prepare('INSERT INTO users(user_id,localpart) VALUES (?,?)').run('@alice:local.example','alice');
  sqlite.prepare('INSERT INTO access_tokens(token_id,token_hash,user_id,device_id) VALUES (?,?,?,?)')
    .run('test', await hashToken('token'), '@alice:local.example','DEVICE');
  return { env, sqlite, localKey };
}
export async function roomFixture(version = '12') {
  const remote = 'remote.example';
  const user = `@creator:${remote}`;
  const key = await generateSigningKeyPair();
  const keyResponse = await signJson({ server_name: remote, valid_until_ts: Date.now() + 86400000,
    verify_keys: { [key.keyId]: { key: key.publicKey } }, old_verify_keys: {} }, remote, key.keyId, key.privateKeyJwk);
  let roomId = '!room:remote.example';
  const events: WireEvent[] = [];
  const id = (e: WireEvent) => eventReferenceId(e, version);
  async function add(type: string, content: Record<string, unknown>, state_key = '', auth_events: string[] = []) {
    const event = await signEvent({ type, room_id: roomId, sender: user, state_key, content, depth: events.length + 1,
      origin_server_ts: Date.now(), auth_events, prev_events: events.length ? [await id(events.at(-1)!)] : [] }, version, remote, key);
    events.push(event); return event;
  }
  const create = await add('m.room.create', { room_version: version, ...(version === '10' ? { creator: user } : {}) });
  if (version === '12') roomId = `!${(await id(create)).slice(1)}`;
  const createAuth = version === '12' ? [] : [await id(create)];
  const creator = await add('m.room.member', { membership: 'join' }, user, createAuth);
  const power = await add('m.room.power_levels', { users: version === '12' ? {} : { [user]: 100 } }, '', [...createAuth, await id(creator)]);
  const rules = await add('m.room.join_rules', { join_rule: 'public' }, '', [...createAuth, await id(creator), await id(power)]);
  await add('m.room.name', { name: 'Remote room' }, '', [...createAuth, await id(creator), await id(power)]);
  const template: WireEvent = { type: 'm.room.member', room_id: roomId, sender: '@alice:local.example', state_key: '@alice:local.example',
    content: { membership: 'join' }, auth_events: [...createAuth, await id(power), await id(rules)],
    prev_events: [await id(events.at(-1)!)], depth: events.length + 1, origin_server_ts: Date.now() };
  return { roomId, remote, key, keyResponse, events, template, version };
}
