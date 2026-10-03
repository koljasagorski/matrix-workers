import { readFile, writeFile, chmod } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { decodeStoredValue } from './stored-values.mjs';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

export function buildReplayPlan(state, { origin = 'm.sgr.ski', sourceSha256, stateSha256 } = {}) {
  if (origin !== 'm.sgr.ski' || state.complete !== true) throw new Error('Expected complete original server queue snapshot');
  const transactions = [];
  for (const [objectId, object] of Object.entries(state.namespaces.FEDERATION)) {
    if (!object.complete || object.encoding !== 'structured-clone-v1') throw new Error('Incomplete federation object');
    const storage = new Map(object.entries.map(([key, encoded]) => [key, decodeStoredValue(encoded)]));
    const remaining = new Map([...storage].filter(([key]) => key.startsWith('queue:') || key.startsWith('edu:')));
    const destinations = new Set([...remaining.values()].map(value => value.destination));
    for (const [key, value] of storage) if (key.startsWith('transaction:')) {
      if (!destinations.has(value?.destination) || key !== `transaction:${value.destination}`) throw new Error('Persisted transaction has no matching queue destination');
    }
    for (const destination of destinations) {
      if (typeof destination !== 'string' || /[\s"/@?#\\]/.test(destination) || destination === origin) throw new Error('Invalid original federation destination');
      let sequence = 0;
      let pending = storage.get(`transaction:${destination}`);
      while ([...remaining.values()].some(value => value.destination === destination)) {
        if (!pending) {
          const sorted = prefix => [...remaining].filter(([key, value]) => key.startsWith(prefix) && value.destination === destination)
            .sort(([aKey, a], [bKey, b]) => a.created_at - b.created_at || aKey.localeCompare(bKey, 'en-US'));
          const events = sorted('queue:').slice(0, 50), edus = sorted('edu:').slice(0, 100);
          pending = { destination, eventKeys: events.map(([key]) => key), eduKeys: edus.map(([key]) => key),
            transactionId: createHash('sha256').update(JSON.stringify([events.map(([, value]) => value.event_id), edus.map(([key]) => key)])).digest('base64url'),
            originServerTs: events[0]?.[1].created_at ?? edus[0][1].created_at };
        }
        if (pending.destination !== destination || !/^[A-Za-z0-9_-]+$/.test(pending.transactionId) || !Number.isSafeInteger(pending.originServerTs) || pending.originServerTs < 0 || !Array.isArray(pending.eventKeys) || !Array.isArray(pending.eduKeys) || pending.eventKeys.length > 50 || pending.eduKeys.length > 100) throw new Error('Invalid persisted transaction metadata');
        const events = pending.eventKeys.map(key => remaining.get(key));
        const edus = pending.eduKeys.map(key => remaining.get(key));
        const keys = [...pending.eventKeys, ...pending.eduKeys];
        if (!keys.length || new Set(keys).size !== keys.length || pending.eventKeys.some(key => !key.startsWith('queue:')) || pending.eduKeys.some(key => !key.startsWith('edu:')) || events.some(value => !value?.pdu || typeof value.event_id !== 'string') || edus.some(value => typeof value?.edu_type !== 'string' || !value.content) || [...events, ...edus].some(value => !value || value.destination !== destination)) throw new Error('Persisted transaction references missing or mismatched queue data');
        const body = { origin, origin_server_ts: pending.originServerTs, pdus: events.map(value => value.pdu), edus: edus.map(value => ({ edu_type: value.edu_type, content: value.content })) };
        const bodyJson = JSON.stringify(body);
        transactions.push({ destination, transaction_id: pending.transactionId, sequence: sequence++, object_id: objectId,
          body_json: bodyJson, body_sha256: sha(bodyJson), event_ids: events.map(value => value.event_id), source_keys: keys });
        for (const key of keys) remaining.delete(key);
        pending = undefined;
      }
    }
  }
  const identities = transactions.map(row => `${row.destination}:${row.transaction_id}`);
  if (new Set(identities).size !== identities.length) throw new Error('Duplicate federation transaction identities');
  return { format: 'matrix-workers-federation-replay-v1', origin, source_sha256: sourceSha256, do_state_sha256: stateSha256, transactions };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const directory = resolve(process.argv.find(arg => arg.startsWith('--directory='))?.slice(12) ?? '.local/vps-migration');
  try {
    const bytes = await readFile(join(directory, 'do-state.json'));
    const plan = buildReplayPlan(JSON.parse(bytes), { sourceSha256: sha(await readFile(join(directory, 'source.sqlite'))), stateSha256: sha(bytes) });
    const file = join(directory, 'federation-replay-plan.json');
    await writeFile(file, JSON.stringify(plan, null, 2) + '\n', { mode: 0o600 }); await chmod(file, 0o600);
    console.log(JSON.stringify({ transactions: plan.transactions.length, pdus: plan.transactions.reduce((n, row) => n + JSON.parse(row.body_json).pdus.length, 0), edus: plan.transactions.reduce((n, row) => n + JSON.parse(row.body_json).edus.length, 0) }));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
