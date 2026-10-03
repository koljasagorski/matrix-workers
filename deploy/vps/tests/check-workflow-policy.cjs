const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const workflow = fs.readFileSync(path.resolve(__dirname, '../../../.github/workflows/vps-deploy.yml'), 'utf8');
const script = workflow.split('          script: |\n')[1].split('      - name: Submit only')[0]
  .split('\n').map(line => line.replace(/^ {12}/, '')).join('\n');
const evaluate = new (Object.getPrototypeOf(async function () {}).constructor)('github', 'context', 'core', script);
const sha = 'a'.repeat(40);

async function check(options = {}) {
  const run = { id: 5, path: '.github/workflows/security.yml', event: 'push', head_branch: 'main', head_sha: sha,
    status: 'completed', conclusion: 'success', head_repository: { full_name: 'owner/matrix' }, ...options.run };
  const output = {};
  const github = { rest: {
    repos: { getBranch: async () => ({ data: { commit: { sha } } }) },
    actions: { getWorkflowRun: async () => ({ data: run }),
      listWorkflowRuns: async () => ({ data: { workflow_runs: [run] } }), listJobsForWorkflowRun: () => {} }
  }, paginate: async () => ['check (22)', 'check (24)', 'codeql', 'migration-check'].map(name => ({ name,
    conclusion: name === options.failed ? 'failure' : 'success' })) };
  await evaluate(github, { repo: { owner: 'owner', repo: 'matrix' }, eventName: options.dispatch ? 'workflow_dispatch' : 'workflow_run',
    payload: { workflow_run: { id: 5 } } }, { setOutput: (name, value) => { output[name] = value; } });
  return output;
}

test('verified current main push and explicit main dispatch select the exact SHA', async () => {
  assert.deepEqual(await check(), { sha });
  assert.deepEqual(await check({ dispatch: true }), { sha });
});

for (const [description, run] of [
  ['PR CI', { event: 'pull_request' }], ['fork', { head_repository: { full_name: 'other/matrix' } }],
  ['other workflow', { path: '.github/workflows/other.yml' }], ['stale main', { head_sha: 'b'.repeat(40) }],
  ['feature branch', { head_branch: 'feature' }], ['unfinished run', { status: 'in_progress' }],
  ['failed run', { conclusion: 'failure' }],
]) {
  test(`rejects ${description} before exposing a deployment SHA`, async () => {
    await assert.rejects(check({ run }), /no verified successful push CI/);
  });
}

test('full native migration check is required even when the overall run claims success', async () => {
  await assert.rejects(check({ failed: 'migration-check' }), /Required CI job failed/);
});

test('privileged workflow uses no checkout or PR code and keeps host checking mandatory', () => {
  assert.doesNotMatch(workflow, /uses: actions\/checkout|pull_request_target|StrictHostKeyChecking=no/);
  assert.match(workflow, /github\.event_name == 'workflow_dispatch' && github\.ref == 'refs\/heads\/main'/);
  assert.match(workflow, /StrictHostKeyChecking=yes/);
  assert.match(workflow, /matrix-deploy@nbg\.patchletter\.com/);
  assert.doesNotMatch(workflow, /matrix-deploy@nbg\.patchletter\.com\s+[^\s]/);
});
