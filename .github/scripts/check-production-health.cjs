'use strict';

const BASE_URL = 'https://m.sgr.ski';
const SERVER_NAME = 'm.sgr.ski';
const TIMEOUT_MS = 45000;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const CHECKS = [
  { path: '/health', valid: body => object(body) && body.status === 'ok' },
  { path: '/_matrix/client/versions', valid: body => object(body) && Array.isArray(body.versions) &&
    body.versions.length > 0 && body.versions.every(version => typeof version === 'string' && version.length > 0) },
  { path: '/_matrix/key/v2/server', valid: body => object(body) && body.server_name === SERVER_NAME &&
    Number.isSafeInteger(body.valid_until_ts) && body.valid_until_ts > Date.now() && object(body.verify_keys) &&
    Object.entries(body.verify_keys).some(([id, value]) => id.startsWith('ed25519:') && object(value) && typeof value.key === 'string' &&
      Buffer.from(value.key, 'base64').length === 32 && object(body.signatures?.[SERVER_NAME]) &&
      typeof body.signatures[SERVER_NAME][id] === 'string' && Buffer.from(body.signatures[SERVER_NAME][id], 'base64').length === 64) },
];

async function checkProductionHealth({ fetchImpl = fetch, log = console.log } = {}) {
  // Independent public GETs only. No account token, cookies, client writes, or
  // redirects to another host; failures expose the endpoint, never its payload.
  const results = await Promise.allSettled(CHECKS.map(async check => {
    const response = await fetchImpl(`${BASE_URL}${check.path}`, { method: 'GET', redirect: 'error',
      signal: AbortSignal.timeout(TIMEOUT_MS), headers: { Accept: 'application/json' } });
    if (response.status !== 200) throw new Error(`${check.path}: HTTP ${response.status}`);
    let body;
    try { body = await response.json(); } catch { throw new Error(`${check.path}: response is not JSON`); }
    if (!check.valid(body)) throw new Error(`${check.path}: unexpected response contents`);
    log(`Healthy: ${check.path}`);
  }));
  const failed = results.filter(result => result.status === 'rejected');
  if (failed.length) throw new AggregateError(failed.map(result => result.reason), `${failed.length} public production checks failed`);
}

if (require.main === module) checkProductionHealth().catch(error => {
  for (const failure of error.errors ?? [error]) console.error(failure.message);
  process.exitCode = 1;
});
module.exports = { checkProductionHealth };
