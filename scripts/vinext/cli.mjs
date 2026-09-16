import { appendFileSync, readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { prepare, assert } from './config.mjs';
import { build } from './build.mjs';
import { deploy, request, isCurrent } from './deploy.mjs';
import { exportSecrets } from './secrets.mjs';

function output(name, value) {
  assert(!/[\r\n]/.test(String(value)), 'Workflow outputs must be single-line values');
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}

const env = process.env;
try {
  const command = process.argv[2];
  if (command === 'prepare') {
    const plan = prepare(JSON.parse(env.PAYLOAD_JSON), JSON.parse(readFileSync(env.REGISTRY_PATH, 'utf8')));
    output('plan', JSON.stringify(plan));
    output('owner', plan.owner);
    output('repo_name', plan.repo_name);
    output('repositories', [plan.repo_name, ...plan.project.submodule_repos].join(','));
    // The account ID may also be stored as a repository secret. Keep it out of
    // cross-job outputs so GitHub's secret masker cannot drop the lock key.
    output('worker_key', createHash('sha256').update(`${plan.project.account_id}/${plan.project.worker_name}`).digest('hex'));
  } else {
    const plan = JSON.parse(env.PLAN_JSON);
    const github = path => request(`https://api.github.com${path}`, env.GH_TOKEN);
    if (command === 'verify-source') {
      assert(await isCurrent(plan, github), 'Requested source commit is not the current branch HEAD');
    } else if (command === 'secrets') {
      await exportSecrets(plan, { file: env.INFISICAL_ENV_FILE, kind: env.SECRET_KIND });
    } else if (command === 'build') {
      assert(await isCurrent(plan, github), 'Requested source commit has been superseded');
      build(plan, { appRoot: env.APP_ROOT, envFile: env.INFISICAL_ENV_FILE, bundleRoot: env.BUNDLE_ROOT });
    } else if (command === 'deploy') {
      assert(env.CLOUDFLARE_API_TOKEN && env.GH_TOKEN, 'Missing deployment credentials');
      const cloudflare = async path => (await request(`https://api.cloudflare.com/client/v4${path}`, env.CLOUDFLARE_API_TOKEN)).result;
      const result = await deploy(plan, { bundleRoot: env.BUNDLE_ROOT, envFile: env.INFISICAL_ENV_FILE, wranglerBin: env.WRANGLER_BIN, github, cloudflare });
      for (const [key, value] of Object.entries(result)) output(key, value);
      if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, `### Vinext ${result.state}\n\n${plan.repo} · ${plan.branch} · \`${plan.sha}\`\n\n${result.deployment_url || 'A newer source commit superseded this request.'}\n\nVersion: ${result.version_id || 'unchanged'}\n`);
    } else if (command === 'status') {
      assert(['pending', 'success', 'failure', 'error'].includes(env.STATUS_STATE), 'Invalid commit status');
      const body = { state: env.STATUS_STATE, context: `vinext/${plan.target}`, target_url: env.STATUS_URL, description: env.DEPLOY_STATE === 'superseded' ? 'Superseded by a newer commit' : `Central Vinext deployment: ${env.STATUS_STATE}` };
      await request(`https://api.github.com/repos/${plan.repo}/statuses/${plan.sha}`, env.GH_TOKEN, { method: 'POST', body: JSON.stringify(body) });
    } else throw new Error('Expected prepare, verify-source, secrets, build, deploy, or status');
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
