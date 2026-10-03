'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { checkProductionHealth } = require('./check-production-health.cjs');
const key = { server_name: 'm.sgr.ski', valid_until_ts: Date.now() + 3600000,
  verify_keys: { 'ed25519:current': { key: Buffer.alloc(32, 1).toString('base64') } },
  signatures: { 'm.sgr.ski': { 'ed25519:current': Buffer.alloc(64, 1).toString('base64') } } };
const bodies = { '/health': { status: 'ok' }, '/_matrix/client/versions': { versions: ['v1.1', 'v1.11'] }, '/_matrix/key/v2/server': key };

test('performs exactly three bounded public GETs without credentials or redirects', async () => {
  const paths = [];
  await checkProductionHealth({ log: () => {}, fetchImpl: async (url, init) => {
    const endpoint = new URL(url); paths.push(endpoint.pathname);
    assert.equal(endpoint.origin, 'https://m.sgr.ski'); assert.equal(init.method, 'GET'); assert.equal(init.redirect, 'error');
    assert.ok(init.signal instanceof AbortSignal); assert.deepEqual(init.headers, { Accept: 'application/json' });
    assert.equal(init.body, undefined); assert.equal(init.credentials, undefined);
    return Response.json(bodies[endpoint.pathname]);
  } });
  assert.deepEqual(paths.sort(), Object.keys(bodies).sort());
});
for (const [name, path, value, status] of [
  ['unhealthy service', '/health', { status: 'failed' }, 200],
  ['empty Matrix versions', '/_matrix/client/versions', { versions: [] }, 200],
  ['wrong Matrix key identity', '/_matrix/key/v2/server', { ...key, server_name: 'other.example' }, 200],
  ['expired Matrix key', '/_matrix/key/v2/server', { ...key, valid_until_ts: 1 }, 200],
  ['missing Matrix signatures', '/_matrix/key/v2/server', { ...key, signatures: {} }, 200],
  ['HTTP failure', '/health', { status: 'ok' }, 503],
]) test(`detects ${name} without printing response data`, async () => {
  await assert.rejects(checkProductionHealth({ log: () => {}, fetchImpl: async url => {
    const endpoint = new URL(url).pathname;
    return Response.json(endpoint === path ? value : bodies[endpoint], { status: endpoint === path ? status : 200 });
  } }), error => error instanceof AggregateError && error.errors.some(failure => failure.message.startsWith(path)));
});
test('detects network timeouts while completing the other independent checks', async () => {
  let requests = 0;
  await assert.rejects(checkProductionHealth({ log: () => {}, fetchImpl: async url => {
    requests++; const path = new URL(url).pathname;
    if (path === '/health') throw new DOMException('Timed out', 'TimeoutError');
    return Response.json(bodies[path]);
  } }), AggregateError);
  assert.equal(requests, 3);
});
