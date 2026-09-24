import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { assert } from './config.mjs';
import { readSecrets, selectValues, validateBundle, run } from './build.mjs';

export async function request(url, token, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json', 'Content-Type': 'application/json', ...options.headers },
    signal: AbortSignal.timeout(30000),
  });
  assert(response.ok, `API request failed (${response.status}) at ${new URL(url).pathname}`);
  const body = await response.json();
  assert(body.success !== false, `Cloudflare rejected request at ${new URL(url).pathname}`);
  return body;
}

export async function isCurrent(plan, github) {
  const branch = await github(`/repos/${plan.repo}/commits/${plan.branch}`);
  return branch.sha === plan.sha;
}

export function assertSecretBindings(bindings, names) {
  assert(Array.isArray(bindings), 'Missing remote version bindings');
  const secrets = bindings.filter(b => b.type === 'secret_text' || b.type === 'secret_key');
  assert(secrets.every(b => names.includes(b.name)), 'Unexpected remote secrets would be inherited; review their removal before deploying');
}

export function newest(versions, alias) {
  assert(Array.isArray(versions) && versions.every(v => typeof v.id === 'string' && Number.isFinite(Date.parse(v.metadata?.created_on))), 'Invalid version metadata');
  return versions.filter(v => alias === undefined || v.annotations?.['workers/alias'] === alias)
    .sort((a, b) => Date.parse(b.metadata.created_on) - Date.parse(a.metadata.created_on))[0];
}

export async function listVersions(cloudflare, workerPath) {
  const versions = [];
  const seen = new Set();
  for (let page = 1; page <= 1000; page++) {
    const { items } = await cloudflare(`${workerPath}/versions?per_page=100&page=${page}`);
    assert(Array.isArray(items), 'Invalid version list');
    if (!items.length) return versions;
    for (const item of items) {
      assert(typeof item.id === 'string' && !seen.has(item.id), 'Version pagination repeated an ID; retry after concurrent uploads finish');
      seen.add(item.id);
      versions.push(item);
    }
  }
  throw new Error('Version history exceeded the pagination limit');
}

export function deploymentArgs(plan, configPath, secretsPath, tag) {
  return [
    ...(plan.target === 'staging' ? plan.project.preview_mode === 'native' ? ['preview', '--name', plan.project.preview_name] : ['versions', 'upload', '--preview-alias', 'dev'] : ['deploy']),
    '--config', configPath, '--secrets-file', secretsPath, '--tag', tag,
    '--message', `${plan.repo}@${plan.sha} (${plan.branch})`,
    ...(plan.target === 'staging' && plan.project.preview_mode === 'native' ? ['--json', '--ignore-base-config'] : []),
  ];
}

async function activeOrUndefined(cloudflare, workerPath) {
  try {
    const result = await cloudflare(`${workerPath}/deployments`);
    assert(Array.isArray(result.deployments), 'Invalid production deployment list');
    return result.deployments[0];
  } catch (error) {
    if (/^API request failed \(404\)/.test(error.message)) return undefined;
    throw error;
  }
}

async function deployNative(plan, { configPath, secrets, wranglerBin, env, execute, github, cloudflare, pause }) {
  const workerPath = `/accounts/${plan.project.account_id}/workers/scripts/${plan.project.worker_name}`;
  const before = await activeOrUndefined(cloudflare, workerPath);
  if (plan.target === 'production' && before) {
    const versions = await listVersions(cloudflare, workerPath);
    const latest = newest(versions);
    assert(latest, 'Existing Worker has no version history');
    for (const id of new Set([latest.id, ...before.versions.map(version => version.version_id)])) {
      const version = await cloudflare(`${workerPath}/versions/${id}`);
      assertSecretBindings(version.resources?.bindings, plan.project.runtime_secrets);
    }
  }
  const directory = mkdtempSync(join(tmpdir(), 'vinext-secrets-'));
  try {
    const secretsPath = join(directory, 'secrets.json');
    writeFileSync(secretsPath, JSON.stringify(secrets), { mode: 0o600, flag: 'wx' });
    const tag = `${plan.branch}-${randomUUID()}`;
    const childEnv = { ...env, CLOUDFLARE_ACCOUNT_ID: plan.project.account_id, WRANGLER_SEND_METRICS: 'false', CI: 'true' };
    for (const key of Object.keys(childEnv)) if (key === 'CLOUDFLARE_ENV' || key === 'GH_TOKEN' || key === 'GITHUB_TOKEN' || key.startsWith('INFISICAL_') || plan.project.runtime_secrets.includes(key)) delete childEnv[key];
    if (!await isCurrent(plan, github)) return { state: 'superseded' };
    const output = execute(process.execPath, [wranglerBin, ...deploymentArgs(plan, configPath, secretsPath, tag)], {
      cwd: dirname(configPath), env: childEnv,
      ...(plan.target === 'staging' ? { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] } : {}),
    });
    const after = await activeOrUndefined(cloudflare, workerPath);
    if (plan.target === 'staging') {
      let result;
      try { result = JSON.parse(output); }
      catch { throw new Error('Wrangler returned invalid Preview deployment JSON'); }
      assert(result.preview?.name === plan.project.preview_name && typeof result.preview.id === 'string' && typeof result.deployment?.id === 'string', 'Wrangler returned a different Preview deployment');
      assert(Array.isArray(result.preview.urls) && result.preview.urls.length > 0, 'Wrangler returned no Preview URL');
      const bindings = result.deployment.env;
      assert(bindings && typeof bindings === 'object' && !Array.isArray(bindings), 'Wrangler returned no Preview bindings');
      const secrets = Object.entries(bindings).filter(([, binding]) => binding?.type === 'secret_text' || binding?.type === 'secret_key');
      assert(secrets.every(([name, binding]) => binding.type === 'secret_text' && plan.project.runtime_secrets.includes(name)), 'Preview deployment contains an unexpected secret binding');
      assert(plan.project.runtime_secrets.every(name => bindings[name]?.type === 'secret_text'), 'Preview deployment is missing a required secret binding');
      assert(isDeepStrictEqual(after, before), 'Native Preview deployment unexpectedly changed active production');
      return { state: 'deployed', preview_name: result.preview.name, preview_id: result.preview.id, deployment_id: result.deployment.id };
    }
    assert(after?.versions?.length === 1 && after.versions[0].percentage === 100, 'Production version is not receiving 100% traffic');
    let uploaded;
    for (let attempt = 0; attempt < 6; attempt++) {
      uploaded = (await listVersions(cloudflare, workerPath)).find(version => version.annotations?.['workers/tag'] === tag);
      if (uploaded) break;
      await pause(2000);
    }
    assert(uploaded?.id === after.versions[0].version_id, 'Production active version differs from the uploaded source');
    const version = await cloudflare(`${workerPath}/versions/${after.versions[0].version_id}`);
    assertSecretBindings(version.resources?.bindings, plan.project.runtime_secrets);
    return { state: 'deployed', version_id: after.versions[0].version_id };
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

export async function deploy(plan, { bundleRoot, envFile, wranglerBin, env = process.env, execute = run, github, cloudflare, pause = ms => new Promise(r => setTimeout(r, ms)) }) {
  const values = readSecrets(envFile);
  const secrets = selectValues(values, plan.project.runtime_secrets);
  const release = JSON.parse(readFileSync(join(bundleRoot, 'vinext-release.json'), 'utf8'));
  assert(isDeepStrictEqual(release, { repo: plan.repo, sha: plan.sha, target: plan.target, worker: plan.project.worker_name }), 'Artifact release does not match dispatch');
  const { config, configPath } = validateBundle(bundleRoot, plan);
  // Re-sanitize on the clean runner, not just in the application build job.
  writeFileSync(configPath, JSON.stringify(config));
  if (!await isCurrent(plan, github)) return { state: 'superseded' };
  if (plan.project.preview_mode === 'native') return deployNative(plan, { configPath, secrets, wranglerBin, env, execute, github, cloudflare, pause });
  const workerPath = `/accounts/${plan.project.account_id}/workers/scripts/${plan.project.worker_name}`;
  const versionsForWorker = () => listVersions(cloudflare, workerPath);
  const active = async () => {
    const result = await cloudflare(`${workerPath}/deployments`);
    assert(Array.isArray(result.deployments) && result.deployments[0]?.versions?.length, 'Initialize the Worker once before enabling central deployments');
    return result.deployments[0];
  };
  const before = await active();
  const versions = await versionsForWorker();
  const latest = newest(versions);
  assert(latest, 'Initialize the Worker once before enabling central deployments');
  const priorPreview = newest(versions, 'dev');
  for (const id of new Set([latest.id, ...before.versions.map(v => v.version_id)])) {
    const version = await cloudflare(`${workerPath}/versions/${id}`);
    assertSecretBindings(version.resources?.bindings, plan.project.runtime_secrets);
  }
  const directory = mkdtempSync(join(tmpdir(), 'vinext-secrets-'));
  try {
    const secretsPath = join(directory, 'secrets.json');
    writeFileSync(secretsPath, JSON.stringify(secrets), { mode: 0o600, flag: 'wx' });
    const tag = `${plan.branch}-${randomUUID()}`;
    const childEnv = { ...env, CLOUDFLARE_ACCOUNT_ID: plan.project.account_id, WRANGLER_SEND_METRICS: 'false', CI: 'true' };
    for (const key of Object.keys(childEnv)) if (key === 'CLOUDFLARE_ENV' || key === 'GH_TOKEN' || key === 'GITHUB_TOKEN' || key.startsWith('INFISICAL_') || plan.project.runtime_secrets.includes(key)) delete childEnv[key];
    if (!await isCurrent(plan, github)) return { state: 'superseded' };
    execute(process.execPath, [wranglerBin, ...deploymentArgs(plan, configPath, secretsPath, tag)], { cwd: dirname(configPath), env: childEnv });
    let uploaded;
    let afterVersions;
    for (let attempt = 0; attempt < 6; attempt++) {
      afterVersions = await versionsForWorker();
      uploaded = afterVersions.find(v => v.annotations?.['workers/tag'] === tag);
      if (uploaded) break;
      await pause(2000);
    }
    assert(uploaded?.id, 'Upload completed but version metadata could not be verified');
    const after = await active();
    if (plan.target === 'staging') {
      assert(isDeepStrictEqual(after, before), 'Staging upload unexpectedly changed active production');
      assert(newest(afterVersions, 'dev')?.id === uploaded.id, 'Dev alias did not point to the new version');
    } else {
      assert(after.versions.length === 1 && after.versions[0].version_id === uploaded.id && after.versions[0].percentage === 100, 'Production version is not receiving 100% traffic');
      assert(newest(afterVersions, 'dev')?.id === priorPreview?.id, 'Production deploy changed the dev preview alias');
    }
    return { state: 'deployed', version_id: uploaded.id };
  } finally { rmSync(directory, { recursive: true, force: true }); }
}
