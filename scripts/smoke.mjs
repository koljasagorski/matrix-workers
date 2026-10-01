import assert from 'node:assert/strict';
const base = (process.argv[2] || 'http://localhost:8787').replace(/\/$/, '');
const server = process.argv[3] || 'm.sgr.ski';
async function get(path) {
  const response = await fetch(base + path);
  assert.equal(response.status, 200, path);
  return response.json();
}
assert.equal((await get('/health')).status, 'ok');
assert.equal((await get('/.well-known/matrix/client'))['m.homeserver'].base_url, `https://${server}`);
assert.equal((await get('/.well-known/matrix/server'))['m.server'], `${server}:443`);
assert.ok((await get('/_matrix/client/versions')).versions.length > 0);
assert.ok(!(await get('/_matrix/client/v3/login')).flows.some(flow => flow.type === 'm.login.dummy'));
assert.equal((await fetch(base + '/_matrix/client/v3/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).status, 403);
assert.equal((await fetch(base + '/admin')).status, 200);
assert.equal((await get('/_matrix/key/v2/server')).server_name, server);
console.log(`Smoke checks passed for ${base}`);
