import { expect, it } from 'vitest';
import { createHash } from 'node:crypto';
import { buildReplayPlan } from '../scripts/migration/prepare-federation-replay.mjs';
import { encodeStoredValue } from '../src/durable-objects/migration-export';

function snapshot(rows: [string, unknown][]) {
  return { complete: true, namespaces: { FEDERATION: { object: { complete: true, encoding: 'structured-clone-v1', entries: rows.map(([key, value]) => [key, encodeStoredValue(value)]) } } } };
}
const destination = 'remote.example';
const event = (id: number) => ({ destination, event_id: `$original-${id}`, created_at: 100 + id,
  pdu: { type: 'm.room.encrypted', content: { ciphertext: `unaltered-${id}`, optional: undefined }, signatures: { 'm.sgr.ski': { 'ed25519:original': 'old-signature' } } } });
const opts = { sourceSha256: 'a'.repeat(64), stateSha256: 'b'.repeat(64) };

it('preserves an in-flight transaction body, signatures and ID before later batches exceeding both limits', () => {
  const rows: [string, unknown][] = Array.from({ length: 62 }, (_, i) => [`queue:${destination}:$original-${i}`, event(i)]);
  rows.push(...Array.from({ length: 105 }, (_, i) => [`edu:${destination}:${i}`, { destination, edu_type: 'm.device_list_update', content: { stream_id: i }, created_at: i }] as [string, unknown]));
  const pending = { destination, transactionId: 'original_transaction_ID', originServerTs: 1,
    eventKeys: [rows[1][0], rows[0][0]], eduKeys: ['edu:remote.example:2', 'edu:remote.example:0'] };
  rows.push([`transaction:${destination}`, pending]);
  const plan = buildReplayPlan(snapshot(rows), opts);
  expect(plan.transactions).toHaveLength(3);
  const first = plan.transactions[0];
  expect(first.transaction_id).toBe(pending.transactionId);
  expect(first.body_json).toBe(JSON.stringify({ origin: 'm.sgr.ski', origin_server_ts: 1, pdus: [event(1).pdu, event(0).pdu], edus: [{ edu_type: 'm.device_list_update', content: { stream_id: 2 } }, { edu_type: 'm.device_list_update', content: { stream_id: 0 } }] }));
  expect(first.body_sha256).toBe(createHash('sha256').update(first.body_json).digest('hex'));
  expect(plan.transactions.map(row => JSON.parse(row.body_json).pdus.length)).toEqual([2, 50, 10]);
  expect(plan.transactions.map(row => JSON.parse(row.body_json).edus.length)).toEqual([2, 100, 3]);
  expect(plan.transactions.flatMap(row => row.event_ids)).toHaveLength(62);
  expect(new Set(plan.transactions.flatMap(row => row.source_keys)).size).toBe(167);
  expect(buildReplayPlan(snapshot(rows), opts)).toEqual(plan);
});

it('fails for missing queue references, dangling persisted transactions, partial snapshots and cross-destination references', () => {
  const pending = { destination, transactionId: 'original', originServerTs: 1, eventKeys: ['queue:remote.example:missing'], eduKeys: [] };
  expect(() => buildReplayPlan(snapshot([[`transaction:${destination}`, pending]]), opts)).toThrow('no matching queue');
  expect(() => buildReplayPlan(snapshot([[`queue:${destination}:$original-0`, event(0)], [`transaction:${destination}`, pending]]), opts)).toThrow('missing or mismatched');
  expect(() => buildReplayPlan({ ...snapshot([]), complete: false }, opts)).toThrow('complete');
  expect(() => buildReplayPlan(snapshot([[`queue:${destination}:$original-0`, { ...event(0), destination: 'another.example' }], [`transaction:${destination}`, { ...pending, eventKeys: [`queue:${destination}:$original-0`] }]]), opts)).toThrow('no matching queue');
});
