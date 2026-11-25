import { appendFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dispatchRoute, dispatchTarget, prepare, statusPlan, validateDispatch, assert } from './config.mjs';
import { cleanupPlan, fetchSourceFiles, projectFromSource } from './source.mjs';
import { build } from './build.mjs';
import { deletePreview, deploy, request, isCurrent, previewCommentBody, upsertPreviewComment } from './deploy.mjs';
import { exportSecrets, loadSecrets } from './secrets.mjs';

function output(name, value) {
  assert(!/[\r\n]/.test(String(value)), 'Workflow outputs must be single-line values');
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}

function logTarget(plan) {
  assert(/^Lascade-Co\/[A-Za-z0-9_.-]+$/.test(plan.repo) && ['staging', 'production', 'pr'].includes(plan.target), 'Invalid deployment label');
  assert(plan.target !== 'pr' || (Number.isInteger(plan.pr_number) && plan.pr_number >= 1 && plan.pr_number <= 9999999), 'Invalid deployment label');
  console.log(`Vinext deployment: ${plan.repo} · ${plan.target === 'pr' ? `pr #${plan.pr_number}` : plan.target}`);
}

const workerKey = project => createHash('sha256').update(`${project.account_id}/${project.worker_name}`).digest('hex');

const env = process.env;
// GitHub REST calls with the job's scoped app token; `options` carries method and body.
const github = (path, options) => request(`https://api.github.com${path}`, env.GH_TOKEN, options);
try {
  const command = process.argv[2];
  if (command === 'preflight') {
    const source = validateDispatch(JSON.parse(env.PAYLOAD_JSON));
    output('owner', source.owner);
    output('repo_name', source.repo_name);
  } else if (command === 'prepare') {
    const payload = JSON.parse(env.PAYLOAD_JSON);
    const { project_slug } = validateDispatch(payload);
    assert(dispatchRoute(payload) === 'deploy', 'A closed pull request only deletes its Preview');
    const source = await fetchSourceFiles(payload, github);
    // Keyed on the target, not the branch: a PR whose head is main is still a PR Preview.
    const infisical_env = dispatchTarget(payload) === 'production' ? 'prod' : 'staging';
    // Public build values are embedded in browser assets; do not mask them before
    // GitHub records the plan output, where matching masks suppress job outputs.
    const values = await loadSecrets({ infisical_project_slug: project_slug, infisical_env, infisical_path: '/' }, { maskPublic: false });
    const plan = prepare(payload, projectFromSource(payload, source, values));
    logTarget(plan);
    output('plan', JSON.stringify(plan));
    output('owner', plan.owner);
    output('repo_name', plan.repo_name);
    output('repositories', [plan.repo_name, ...plan.project.submodule_repos].join(','));
    output('worker_key', workerKey(plan.project));
  } else if (command === 'cleanup-resolve' || command === 'cleanup') {
    // A closed PR: no build. The Worker comes from the PR's last head commit.
    const payload = JSON.parse(env.PAYLOAD_JSON);
    assert(dispatchRoute(payload) === 'cleanup', 'Preview cleanup runs only for closed pull requests');
    const plan = cleanupPlan(payload, await fetchSourceFiles(payload, github));
    logTarget(plan);
    if (command === 'cleanup-resolve') {
      output('wrangler_version', plan.project.wrangler_version);
      output('worker_key', workerKey(plan.project));
    } else {
      // Reopened since this event: the reopen dispatch owns the Preview now.
      const pull = await github(`/repos/${plan.repo}/pulls/${plan.pr_number}`);
      if (pull?.state === 'open') {
        output('state', 'reopened');
        console.log('The pull request was reopened; its Preview is kept.');
      } else {
        assert(env.CLOUDFLARE_API_TOKEN && env.GH_TOKEN, 'Missing deployment credentials');
        const cloudflare = async path => (await request(`https://api.cloudflare.com/client/v4${path}`, env.CLOUDFLARE_API_TOKEN)).result;
        const result = await deletePreview(plan, { wranglerBin: env.WRANGLER_BIN, cloudflare });
        output('state', result.state);
        const body = previewCommentBody(plan, { result: result.state === 'deleted' ? 'removed' : 'absent', runUrl: env.RUN_URL });
        // Update the existing comment only; a PR that never had a Preview gets none.
        await upsertPreviewComment(plan, body, github, { create: false });
        if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, `### Vinext Preview ${result.state}\n\n${plan.repo} · pr #${plan.pr_number}\n`);
      }
    }
  } else {
    const plan = env.PLAN_JSON ? JSON.parse(env.PLAN_JSON) : ['status', 'comment'].includes(command) ? statusPlan(JSON.parse(env.PAYLOAD_JSON)) : (() => { throw new Error('Missing deployment plan'); })();
    if (command === 'verify-source') {
      assert(await isCurrent(plan, github), plan.target === 'pr' ? 'Requested source commit is not the current PR head' : 'Requested source commit is not the current branch HEAD');
    } else if (command === 'secrets') {
      await exportSecrets(plan, { file: env.INFISICAL_ENV_FILE, kind: env.SECRET_KIND });
    } else if (command === 'build') {
      logTarget(plan);
      assert(await isCurrent(plan, github), 'Requested source commit has been superseded');
      build(plan, { appRoot: env.APP_ROOT, envFile: env.INFISICAL_ENV_FILE, bundleRoot: env.BUNDLE_ROOT });
    } else if (command === 'deploy') {
      logTarget(plan);
      // Each workflow deploy step names the targets it may run; a PR plan can
      // never run in the production step, nor a production plan in the PR step.
      assert(typeof env.DEPLOY_TARGETS === 'string' && env.DEPLOY_TARGETS.split(',').includes(plan.target), `Target ${plan.target} is not allowed in this deploy step`);
      assert(env.CLOUDFLARE_API_TOKEN && env.GH_TOKEN, 'Missing deployment credentials');
      const cloudflare = async path => (await request(`https://api.cloudflare.com/client/v4${path}`, env.CLOUDFLARE_API_TOKEN)).result;
      const result = await deploy(plan, { bundleRoot: env.BUNDLE_ROOT, envFile: env.INFISICAL_ENV_FILE, wranglerBin: env.WRANGLER_BIN, github, cloudflare });
      for (const [key, value] of Object.entries(result)) output(key, value);
      if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, `### Vinext ${result.state}\n\n${plan.repo} · ${plan.target === 'pr' ? `pr #${plan.pr_number}` : plan.target}\n\n${result.state === 'superseded' ? 'A newer source commit superseded this request.' : plan.target === 'pr' ? `Wrangler deployed the ${result.preview_name} PR Preview at ${result.preview_url} and the production deployment stayed unchanged.` : plan.target === 'staging' && plan.project.preview_mode === 'native' ? `Wrangler deployed the ${result.preview_name} Worker Preview and the production deployment stayed unchanged.` : plan.target === 'staging' ? 'Cloudflare confirmed the preview version and dev alias.' : 'Cloudflare confirmed the active deployment.'}\n`);
    } else if (command === 'status') {
      assert(['pending', 'success', 'failure', 'error'].includes(env.STATUS_STATE), 'Invalid commit status');
      const body = { state: env.STATUS_STATE, context: plan.target === 'pr' ? 'vinext/preview' : `vinext/${plan.target}`, target_url: env.STATUS_URL, description: env.DEPLOY_STATE === 'superseded' ? 'Superseded by a newer commit' : `Central Vinext deployment: ${env.STATUS_STATE}` };
      await request(`https://api.github.com/repos/${plan.repo}/statuses/${plan.sha}`, env.GH_TOKEN, { method: 'POST', body: JSON.stringify(body) });
    } else if (command === 'comment') {
      assert(plan.target === 'pr', 'Only PR Previews have a PR comment');
      const result = env.DEPLOY_STATE === 'superseded' ? 'superseded' : env.STATUS_STATE === 'success' ? 'deployed' : 'failed';
      const body = previewCommentBody(plan, { result, previewUrl: env.PREVIEW_URL, runUrl: env.RUN_URL });
      // A run whose commit is no longer the PR head never overwrites the
      // comment of a newer run; it may still create the first one.
      // COMMENT_MODE=update-only: callers outside the per-Worker deploy queue
      // (the prepare job) never create the comment, so two runs cannot both POST.
      assert(env.COMMENT_MODE === undefined || env.COMMENT_MODE === '' || env.COMMENT_MODE === 'update-only', 'Invalid COMMENT_MODE');
      const outcome = await upsertPreviewComment(plan, body, github, { overwrite: await isCurrent(plan, github), create: env.COMMENT_MODE !== 'update-only' });
      console.log(`PR comment ${outcome}: ${result}`);
    } else throw new Error('Expected preflight, prepare, verify-source, secrets, build, deploy, status, comment, cleanup-resolve, or cleanup');
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
