import { appendFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { prepare, statusPlan, validateDispatch, assert } from './config.mjs';
import { fetchSourceFiles, projectFromSource } from './source.mjs';
import { build } from './build.mjs';
import { deploy, request, isCurrent } from './deploy.mjs';
import { exportSecrets, loadSecrets } from './secrets.mjs';

function output(name, value) {
  assert(!/[\r\n]/.test(String(value)), 'Workflow outputs must be single-line values');
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}

function logTarget(plan) {
  assert(/^Lascade-Co\/[A-Za-z0-9_.-]+$/.test(plan.repo) && ['staging', 'production'].includes(plan.target), 'Invalid deployment label');
  console.log(`Vinext deployment: ${plan.repo} · ${plan.target}`);
}

const env = process.env;
try {
  const command = process.argv[2];
  if (command === 'preflight') {
    const source = validateDispatch(JSON.parse(env.PAYLOAD_JSON));
    output('owner', source.owner);
    output('repo_name', source.repo_name);
  } else if (command === 'prepare') {
    const payload = JSON.parse(env.PAYLOAD_JSON);
    validateDispatch(payload);
    const github = path => request(`https://api.github.com${path}`, env.GH_TOKEN);
    const source = await fetchSourceFiles(payload, github);
    const infisical_env = payload.branch === 'main' ? 'prod' : 'staging';
    // Public build values are embedded in browser assets; do not mask them before
    // GitHub records the plan output, where matching masks suppress job outputs.
    const values = await loadSecrets({ infisical_project_slug: payload.project_slug, infisical_env, infisical_path: '/' }, { maskPublic: false });
    const plan = prepare(payload, projectFromSource(payload, source, values));
    logTarget(plan);
    output('plan', JSON.stringify(plan));
    output('owner', plan.owner);
    output('repo_name', plan.repo_name);
    output('repositories', [plan.repo_name, ...plan.project.submodule_repos].join(','));
    output('worker_key', createHash('sha256').update(`${plan.project.account_id}/${plan.project.worker_name}`).digest('hex'));
  } else {
    const plan = env.PLAN_JSON ? JSON.parse(env.PLAN_JSON) : command === 'status' ? statusPlan(JSON.parse(env.PAYLOAD_JSON)) : (() => { throw new Error('Missing deployment plan'); })();
    const github = path => request(`https://api.github.com${path}`, env.GH_TOKEN);
    if (command === 'verify-source') {
      assert(await isCurrent(plan, github), 'Requested source commit is not the current branch HEAD');
    } else if (command === 'secrets') {
      await exportSecrets(plan, { file: env.INFISICAL_ENV_FILE, kind: env.SECRET_KIND });
    } else if (command === 'build') {
      logTarget(plan);
      assert(await isCurrent(plan, github), 'Requested source commit has been superseded');
      build(plan, { appRoot: env.APP_ROOT, envFile: env.INFISICAL_ENV_FILE, bundleRoot: env.BUNDLE_ROOT });
    } else if (command === 'deploy') {
      logTarget(plan);
      assert(env.CLOUDFLARE_API_TOKEN && env.GH_TOKEN, 'Missing deployment credentials');
      const cloudflare = async path => (await request(`https://api.cloudflare.com/client/v4${path}`, env.CLOUDFLARE_API_TOKEN)).result;
      const result = await deploy(plan, { bundleRoot: env.BUNDLE_ROOT, envFile: env.INFISICAL_ENV_FILE, wranglerBin: env.WRANGLER_BIN, github, cloudflare });
      for (const [key, value] of Object.entries(result)) output(key, value);
      if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, `### Vinext ${result.state}\n\n${plan.repo} · ${plan.target}\n\n${result.state === 'superseded' ? 'A newer source commit superseded this request.' : plan.target === 'staging' && plan.project.preview_mode === 'native' ? `Wrangler deployed the ${result.preview_name} Worker Preview and the production deployment stayed unchanged.` : plan.target === 'staging' ? 'Cloudflare confirmed the preview version and dev alias.' : 'Cloudflare confirmed the active deployment.'}\n`);
    } else if (command === 'status') {
      assert(['pending', 'success', 'failure', 'error'].includes(env.STATUS_STATE), 'Invalid commit status');
      const body = { state: env.STATUS_STATE, context: `vinext/${plan.target}`, target_url: env.STATUS_URL, description: env.DEPLOY_STATE === 'superseded' ? 'Superseded by a newer commit' : `Central Vinext deployment: ${env.STATUS_STATE}` };
      await request(`https://api.github.com/repos/${plan.repo}/statuses/${plan.sha}`, env.GH_TOKEN, { method: 'POST', body: JSON.stringify(body) });
    } else throw new Error('Expected preflight, prepare, verify-source, secrets, build, deploy, or status');
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
