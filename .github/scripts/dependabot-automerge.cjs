'use strict';

const { isDeepStrictEqual } = require('node:util');
const REQUIRED_CHECKS = ['check (22)', 'check (24)', 'codeql', 'dependency-review'];
const DEPENDABOT_ID = 49699333;
const DEPENDENCY_FIELDS = ['dependencies', 'devDependencies', 'optionalDependencies', 'peerDependencies'];

function version(value) {
  const match = typeof value === 'string' && value.match(/^[~^=]?(\d+)\.(\d+)\.(\d+)$/);
  return match ? match.slice(1).map(Number) : null;
}
function compatibleVersion(before, after) {
  const a = version(before); const b = version(after);
  return !!a && !!b && a[0] === b[0] && (a[0] !== 0 || a[1] === b[1]) &&
    (b[1] > a[1] || (b[1] === a[1] && b[2] >= a[2]));
}
function withoutDependencies(manifest) {
  const result = { ...manifest };
  for (const field of DEPENDENCY_FIELDS) delete result[field];
  return result;
}

// Parse PR files as data; never run dependency code with the merge token.
function compatibleNpmUpdate(before, after, oldLock, newLock) {
  if (!isDeepStrictEqual(withoutDependencies(before), withoutDependencies(after)) ||
      !isDeepStrictEqual(withoutDependencies(oldLock.packages?.[''] ?? {}), withoutDependencies(newLock.packages?.[''] ?? {})) ||
      oldLock.lockfileVersion !== 3 || newLock.lockfileVersion !== 3 ||
      oldLock.name !== newLock.name || oldLock.version !== newLock.version) return false;
  for (const field of DEPENDENCY_FIELDS) {
    const oldDeps = before[field] ?? {}; const newDeps = after[field] ?? {};
    if (!isDeepStrictEqual(Object.keys(oldDeps).sort(), Object.keys(newDeps).sort()) ||
        !isDeepStrictEqual(newDeps, newLock.packages[''][field] ?? {})) return false;
    for (const name of Object.keys(oldDeps)) {
      if (!compatibleVersion(oldDeps[name], newDeps[name])) return false;
    }
  }
  for (const [path, pkg] of Object.entries(newLock.packages ?? {})) {
    if (!path) continue;
    // Fail closed for git/file sources, malformed packages, and major transitive changes.
    if (!pkg.resolved?.startsWith('https://registry.npmjs.org/') || !pkg.integrity?.startsWith('sha512-') || !version(pkg.version)) return false;
    const old = oldLock.packages?.[path];
    if (old && !compatibleVersion(old.version, pkg.version)) return false;
  }
  return true;
}

function actionChanges(before, after) {
  const a = before.split('\n'); const b = after.split('\n');
  if (a.length !== b.length) return null;
  const changes = [];
  const action = /^(\s*-?\s*uses:\s+)((?:actions|github)\/[A-Za-z0-9_.-]+)@([a-f0-9]{40})(\s+#\s+v(\d+)(?:\.\d+){0,2})\s*$/;
  for (let index = 0; index < a.length; index++) {
    if (a[index] === b[index]) continue;
    const old = a[index].match(action); const next = b[index].match(action);
    if (!old || !next || old[1] !== next[1] || old[2] !== next[2] || old[5] !== next[5] || old[3] === next[3]) return null;
    changes.push({ repository: next[2], sha: next[3], major: Number(next[5]) });
  }
  return changes.length ? changes : null;
}

async function mergeTestedUpdate({ github, context, core }) {
  const run = context.payload.workflow_run;
  const repo = context.repo;
  const fullName = `${repo.owner}/${repo.repo}`;
  const skip = reason => core.info(`Update remains open: ${reason}`);
  if (run.event !== 'pull_request' || run.conclusion !== 'success' || run.head_repository?.full_name !== fullName) return skip('not a successful same-repository PR run');
  const candidates = await github.paginate(github.rest.pulls.list, { ...repo, state: 'open', base: 'main', head: `${repo.owner}:${run.head_branch}`, per_page: 100 });
  if (candidates.length !== 1) return skip('no unique open PR for the tested branch');
  const { data: pr } = await github.rest.pulls.get({ ...repo, pull_number: candidates[0].number });
  if (pr.user?.login !== 'dependabot[bot]' || pr.user.id !== DEPENDABOT_ID || pr.user.type !== 'Bot' || pr.draft ||
      pr.base.ref !== 'main' || pr.base.repo.full_name !== fullName || pr.head.repo?.full_name !== fullName ||
      pr.head.sha !== run.head_sha || !/^dependabot\/(npm_and_yarn|github_actions)\//.test(pr.head.ref) || pr.mergeable_state !== 'clean') {
    return skip('PR identity, tested commit, base, or mergeability changed');
  }
  const result = await github.graphql(`query($owner:String!,$name:String!){repository(owner:$owner,name:$name){ref(qualifiedName:"refs/heads/main"){branchProtectionRule{requiresStatusChecks requiresStrictStatusChecks requiredStatusCheckContexts}}}}`, { owner: repo.owner, name: repo.repo });
  const protection = result.repository?.ref?.branchProtectionRule;
  if (!protection?.requiresStatusChecks || !protection.requiresStrictStatusChecks ||
      !REQUIRED_CHECKS.every(name => protection.requiredStatusCheckContexts.includes(name))) return skip('required strict branch protection is missing');
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, { ...repo, run_id: run.id, filter: 'latest', per_page: 100 });
  if (!REQUIRED_CHECKS.every(name => jobs.some(job => job.name === name && job.conclusion === 'success'))) return skip('full CI and migration checks are not all green');
  const files = await github.paginate(github.rest.pulls.listFiles, { ...repo, pull_number: pr.number, per_page: 100 });
  if (!files.length || files.length > 20 || files.some(file => file.status !== 'modified')) return skip('unexpected file addition, removal, or rename');
  const read = async (path, ref) => {
    const { data } = await github.rest.repos.getContent({ ...repo, path, ref });
    if (Array.isArray(data) || data.type !== 'file' || data.encoding !== 'base64' || !data.content) throw new Error('Invalid update file response');
    return Buffer.from(data.content, 'base64').toString('utf8');
  };
  if (pr.head.ref.startsWith('dependabot/npm_and_yarn/')) {
    if (files.some(file => !['package.json', 'package-lock.json'].includes(file.filename))) return skip('npm PR changes files outside dependency manifests');
    const documents = await Promise.all(['package.json', 'package-lock.json'].flatMap(path => [read(path, pr.base.sha), read(path, pr.head.sha)]));
    const [before, after, oldLock, newLock] = documents.map(document => JSON.parse(document));
    if (!compatibleNpmUpdate(before, after, oldLock, newLock)) return skip('major, prerelease, source, or non-dependency change needs review');
  } else {
    const changes = [];
    for (const file of files) {
      if (!/^\.github\/workflows\/[^/]+\.ya?ml$/.test(file.filename)) return skip('Actions PR changes files outside workflows');
      const update = actionChanges(await read(file.filename, pr.base.sha), await read(file.filename, pr.head.sha));
      if (!update) return skip('workflow changes more than same-major official action pins');
      changes.push(...update);
    }
    for (const update of new Map(changes.map(change => [`${change.repository}@${change.sha}`, change])).values()) {
      const [owner, name] = update.repository.split('/');
      const { data: tags } = await github.rest.repos.listTags({ owner, repo: name, per_page: 100 });
      if (!tags.some(tag => tag.commit.sha === update.sha && new RegExp(`^v${update.major}\\.\\d+\\.\\d+$`).test(tag.name))) return skip('action pin is not a same-major official version tag');
    }
  }
  // Re-read immediately before the conditional merge. GitHub also enforces branch
  // protection and atomically rejects a changed PR head through the sha parameter.
  const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pr.number });
  if (current.head.sha !== pr.head.sha || current.base.sha !== pr.base.sha || current.state !== 'open' || current.mergeable_state !== 'clean') return skip('PR changed during verification');
  const { data: merged } = await github.rest.pulls.merge({ ...repo, pull_number: pr.number, sha: pr.head.sha, merge_method: 'squash' });
  if (!merged.merged) throw new Error(`GitHub refused the protected merge: ${merged.message}`);
  core.info(`Merged tested Dependabot PR #${pr.number}. Cloudflare Workers Builds handles the production rollout.`);
}

async function mergePendingUpdates({ github, context, core }) {
  // Scheduled/default-branch execution avoids privileged pull_request_target or
  // workflow_run triggers. Only API metadata from completed PR CI is consumed.
  const pulls = await github.paginate(github.rest.pulls.list, { ...context.repo, state: 'open', base: 'main', per_page: 100 });
  for (const pr of pulls.filter(pr => pr.user?.login === 'dependabot[bot]' && pr.user.id === DEPENDABOT_ID && pr.user.type === 'Bot').slice(0, 20)) {
    const { data } = await github.rest.actions.listWorkflowRuns({ ...context.repo, workflow_id: 'security.yml', event: 'pull_request',
      head_sha: pr.head.sha, per_page: 10 });
    const run = data.workflow_runs[0];
    if (!run || run.status !== 'completed' || run.conclusion !== 'success') {
      core.info(`Update #${pr.number} is waiting for successful CI.`); continue;
    }
    await mergeTestedUpdate({ github, context: { ...context, payload: { workflow_run: run } }, core });
  }
}

module.exports = { REQUIRED_CHECKS, compatibleVersion, compatibleNpmUpdate, actionChanges, mergeTestedUpdate, mergePendingUpdates };
