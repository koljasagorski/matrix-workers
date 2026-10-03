'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { compatibleVersion, compatibleNpmUpdate, actionChanges, mergeTestedUpdate, mergePendingUpdates, REQUIRED_CHECKS } = require('./dependabot-automerge.cjs');

const manifest = value => ({ name: 'matrix-worker', version: '0.1.0', scripts: { check: 'npm test' }, dependencies: { hono: value } });
const lock = value => ({ name: 'matrix-worker', version: '0.1.0', lockfileVersion: 3, packages: {
  '': { name: 'matrix-worker', version: '0.1.0', dependencies: { hono: `^${value}` } },
  'node_modules/hono': { version: value, resolved: `https://registry.npmjs.org/hono/-/hono-${value}.tgz`, integrity: 'sha512-fixture' },
} });

test('accepts stable minor and patch updates but rejects majors, prereleases, downgrades, and 0.x minor changes', () => {
  for (const [before, after] of [['^4.13.1', '^4.14.0'], ['~4.13.1', '~4.13.2'], ['0.4.1', '0.4.2']]) assert.equal(compatibleVersion(before, after), true);
  for (const [before, after] of [['^4.13.1', '^5.0.0'], ['0.4.1', '0.5.0'], ['4.13.1', '4.12.9'], ['4.13.1', '4.13.2-beta.1'], ['4.13.1', 'github:owner/repo']]) assert.equal(compatibleVersion(before, after), false);
});

test('rejects lifecycle-script changes, package additions, non-registry sources, and major transitive updates', () => {
  const before = manifest('^4.13.1'); const after = manifest('^4.14.0'); const oldLock = lock('4.13.1'); const newLock = lock('4.14.0');
  assert.equal(compatibleNpmUpdate(before, after, oldLock, newLock), true);
  assert.equal(compatibleNpmUpdate(before, { ...after, scripts: { preinstall: 'malicious command' } }, oldLock, newLock), false);
  assert.equal(compatibleNpmUpdate(before, { ...after, dependencies: { ...after.dependencies, extra: '^1.0.0' } }, oldLock, newLock), false);
  const source = structuredClone(newLock); source.packages['node_modules/hono'].resolved = 'https://untrusted.example/package.tgz';
  assert.equal(compatibleNpmUpdate(before, after, oldLock, source), false);
  const oldTransitive = structuredClone(oldLock); const newTransitive = structuredClone(newLock);
  oldTransitive.packages['node_modules/transitive'] = { version: '1.0.0' };
  newTransitive.packages['node_modules/transitive'] = { version: '2.0.0', resolved: 'https://registry.npmjs.org/transitive/-/transitive-2.0.0.tgz', integrity: 'sha512-fixture' };
  assert.equal(compatibleNpmUpdate(before, after, oldTransitive, newTransitive), false);
});

const oldAction = `      - uses: actions/checkout@${'a'.repeat(40)} # v7.0.1\n        with:\n          persist-credentials: false\n`;
const newAction = `      - uses: actions/checkout@${'b'.repeat(40)} # v7.0.2\n        with:\n          persist-credentials: false\n`;
test('allows only same-major SHA-only official Actions updates with identical workflow structure', () => {
  assert.deepEqual(actionChanges(oldAction, newAction), [{ repository: 'actions/checkout', sha: 'b'.repeat(40), major: 7 }]);
  assert.equal(actionChanges(oldAction, newAction.replace('false', 'true')), null);
  assert.equal(actionChanges(oldAction, newAction.replace('v7.0.2', 'v8.0.0')), null);
  assert.equal(actionChanges(oldAction, newAction.replace('actions/checkout', 'unknown/checkout')), null);
  assert.equal(actionChanges(oldAction, `${newAction}      - run: malicious command\n`), null);
});

function fixture(options = {}) {
  const repo = { owner: 'owner', repo: 'repo' }; const fullName = 'owner/repo';
  const head = 'b'.repeat(40); const base = 'a'.repeat(40);
  const pr = { number: 4, state: 'open', draft: false, user: { login: 'dependabot[bot]', id: 49699333, type: 'Bot' },
    base: { ref: 'main', sha: base, repo: { full_name: fullName } },
    head: { ref: 'dependabot/npm_and_yarn/hono-4.14.0', sha: head, repo: { full_name: fullName } }, mergeable_state: 'clean' };
  if (options.adjustPr) options.adjustPr(pr);
  if (options.actions) pr.head.ref = 'dependabot/github_actions/actions-update';
  const context = { repo, payload: { workflow_run: { id: 3, event: 'pull_request', conclusion: 'success',
    head_repository: { full_name: fullName }, head_branch: pr.head.ref, head_sha: head } } };
  const merged = []; const messages = []; let reads = 0;
  const contents = { [base]: { 'package.json': manifest('^4.13.1'), 'package-lock.json': lock('4.13.1') },
    [head]: { 'package.json': manifest(options.major ? '^5.0.0' : '^4.14.0'), 'package-lock.json': lock(options.major ? '5.0.0' : '4.14.0') } };
  contents[base]['.github/workflows/security.yml'] = oldAction;
  contents[head]['.github/workflows/security.yml'] = options.changedAction ?? newAction;
  const rest = { pulls: { list: () => {}, listFiles: () => {}, get: async () => {
    reads++; const copy = structuredClone(pr); if (options.race && reads > 1) copy.head.sha = 'c'.repeat(40); return { data: copy };
  }, merge: async data => { merged.push(data); return { data: { merged: true } }; } }, actions: { listJobsForWorkflowRun: () => {},
    listWorkflowRuns: async () => ({ data: { workflow_runs: [{ ...context.payload.workflow_run, status: options.pending ? 'in_progress' : 'completed' }] } }) },
  repos: { listTags: async () => ({ data: options.noTag ? [] : [{ name: 'v7.0.2', commit: { sha: head } }] }),
    getContent: async ({ path, ref }) => {
      const data = contents[ref][path];
      return { data: { type: 'file', encoding: 'base64', content: Buffer.from(typeof data === 'string' ? data : JSON.stringify(data)).toString('base64') } };
    } } };
  const github = { rest, graphql: async () => ({ repository: { ref: { branchProtectionRule: options.protected === false ? null : {
    requiresStatusChecks: true, requiresStrictStatusChecks: true, requiredStatusCheckContexts: REQUIRED_CHECKS,
  } } } }), paginate: async method => {
    if (method === rest.pulls.list) return [pr];
    if (method === rest.pulls.listFiles) return options.files ?? (options.actions ? [{ filename: '.github/workflows/security.yml', status: 'modified' }] : [{ filename: 'package.json', status: 'modified' }, { filename: 'package-lock.json', status: 'modified' }]);
    if (method === rest.actions.listJobsForWorkflowRun) return REQUIRED_CHECKS.map(name => ({ name, conclusion: name === options.failed ? 'failure' : 'success' }));
    throw new Error('Unexpected API request');
  } };
  return { github, context, core: { info: value => messages.push(value) }, merged, messages, head };
}

test('merges only the verified exact bot head after all protected CI jobs pass', async () => {
  const f = fixture(); await mergeTestedUpdate(f);
  assert.deepEqual(f.merged, [{ owner: 'owner', repo: 'repo', pull_number: 4, sha: f.head, merge_method: 'squash' }]);
});
test('the trusted scheduler discovers and conditionally merges a completed bot PR', async () => {
  const f = fixture(); await mergePendingUpdates(f); assert.equal(f.merged.length, 1);
});
test('the trusted scheduler leaves pending CI open', async () => {
  const f = fixture({ pending: true }); await mergePendingUpdates(f); assert.equal(f.merged.length, 0);
});
test('merges a same-major action pin only when the exact SHA has an official version tag', async () => {
  const f = fixture({ actions: true }); await mergeTestedUpdate(f); assert.equal(f.merged.length, 1);
  const untagged = fixture({ actions: true, noTag: true }); await mergeTestedUpdate(untagged); assert.equal(untagged.merged.length, 0);
  const changed = fixture({ actions: true, changedAction: newAction.replace('false', 'true') }); await mergeTestedUpdate(changed); assert.equal(changed.merged.length, 0);
});
for (const [name, options] of [
  ['spoofed bot account', { adjustPr: pr => { pr.user.id = 1; } }],
  ['forked bot branch', { adjustPr: pr => { pr.head.repo.full_name = 'fork/repo'; } }],
  ['untested new commit', { adjustPr: pr => { pr.head.sha = 'c'.repeat(40); } }],
  ['missing protection', { protected: false }],
  ['failed required CI job', { failed: 'check (24)' }],
  ['major update', { major: true }],
  ['code changes mixed into dependency PR', { files: [{ filename: 'src/index.ts', status: 'modified' }] }],
  ['file renames', { files: [{ filename: 'package.json', status: 'renamed' }] }],
  ['head changes during verification', { race: true }],
]) test(`does not merge ${name}`, async () => {
  const f = fixture(options); await mergeTestedUpdate(f); assert.equal(f.merged.length, 0);
});
