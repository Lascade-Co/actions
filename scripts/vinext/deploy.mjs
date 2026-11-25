import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { assert } from './config.mjs';
import { readSecrets, selectValues, validateBundle, validateD1Migrations, run } from './build.mjs';
import { assertBaseWorker } from './source.mjs';

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
  if (plan.target === 'pr') {
    // A newer push to the PR, closing it, or converting it to a draft all
    // supersede this request. A head outside this repository is never current.
    assert(Number.isInteger(plan.pr_number) && plan.pr_number >= 1 && plan.pr_number <= 9999999, 'Invalid pull request number');
    const pull = await github(`/repos/${plan.repo}/pulls/${plan.pr_number}`);
    return pull?.state === 'open' && pull.draft === false && pull.head?.sha === plan.sha && pull.head?.repo?.full_name === plan.repo;
  }
  const branch = await github(`/repos/${plan.repo}/commits/${plan.branch}`);
  return branch.sha === plan.sha;
}

function cloudflareEnvironment(plan, env) {
  const childEnv = { ...env, CLOUDFLARE_ACCOUNT_ID: plan.project.account_id, WRANGLER_SEND_METRICS: 'false', CI: 'true' };
  for (const key of Object.keys(childEnv)) if (key === 'CLOUDFLARE_ENV' || key === 'GH_TOKEN' || key === 'GITHUB_TOKEN' || key.startsWith('INFISICAL_') || plan.project.runtime_secrets.includes(key)) delete childEnv[key];
  return childEnv;
}

export function applyD1Migrations(plan, { bundleRoot, wranglerBin, env, execute }) {
  const databases = validateD1Migrations(bundleRoot, plan);
  // Defence in depth: prepare already refuses PR plans with configured migrations.
  assert(plan.target !== 'pr' || databases.length === 0, 'PR Previews cannot apply D1 migrations');
  if (!databases.length) return false;
  const directory = mkdtempSync(join(tmpdir(), 'vinext-d1-migrations-'));
  try {
    const config = {
      name: plan.project.worker_name,
      account_id: plan.project.account_id,
      d1_databases: databases.map((database, index) => {
        const migrationDirectory = `migrations-${index + 1}`;
        cpSync(join(bundleRoot, database.directory), join(directory, migrationDirectory), { recursive: true, dereference: false });
        return {
          binding: database.binding,
          database_name: database.database_name,
          database_id: database.database_id,
          ...(database.migrations_table === undefined ? {} : { migrations_table: database.migrations_table }),
          migrations_dir: migrationDirectory,
        };
      }),
    };
    const configPath = join(directory, 'wrangler.json');
    writeFileSync(configPath, JSON.stringify(config), { mode: 0o600, flag: 'wx' });
    const childEnv = cloudflareEnvironment(plan, env);
    for (const database of config.d1_databases) {
      execute(process.execPath, [wranglerBin, 'd1', 'migrations', 'apply', database.binding, '--remote', '--config', configPath], { cwd: directory, env: childEnv });
    }
    return true;
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

export function assertSecretBindings(bindings, names) {
  assert(Array.isArray(bindings), 'Missing remote version bindings');
  const secrets = bindings.filter(b => b.type === 'secret_text' || b.type === 'secret_key');
  assert(secrets.every(b => names.includes(b.name)), 'Unexpected remote secrets would be inherited; review their removal before deploying');
}

export function assertRequiredSecretBindings(bindings, names) {
  assertSecretBindings(bindings, names);
  assert(names.every(name => bindings.some(binding => binding.name === name && binding.type === 'secret_text')), 'Deployed Worker is missing a required runtime secret');
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

// A PR Preview plan is always a native Preview named exactly pr-<number>.
export function assertPullRequestPlan(plan) {
  assert(plan?.target === 'pr' && Number.isInteger(plan.pr_number) && plan.pr_number >= 1 && plan.pr_number <= 9999999 &&
    plan.project?.preview_mode === 'native' && plan.project.preview_name === `pr-${plan.pr_number}`, 'Invalid PR Preview plan');
}

// Explicit per target: only `production` ever runs `wrangler deploy`.
export function deploymentArgs(plan, configPath, secretsPath, tag) {
  let command, label = plan.branch, nativePreview = false;
  if (plan.target === 'production') {
    command = ['deploy'];
  } else if (plan.target === 'staging') {
    nativePreview = plan.project.preview_mode === 'native';
    command = nativePreview ? ['preview', '--name', plan.project.preview_name] : ['versions', 'upload', '--preview-alias', 'dev'];
  } else if (plan.target === 'pr') {
    assertPullRequestPlan(plan);
    nativePreview = true;
    command = ['preview', '--name', plan.project.preview_name];
    // The head branch is caller-chosen text; the PR number identifies it.
    label = `pr #${plan.pr_number}`;
  } else {
    throw new Error('Unsupported deployment target');
  }
  return [
    ...command,
    '--config', configPath, '--secrets-file', secretsPath, '--tag', tag,
    '--message', `${plan.repo}@${plan.sha} (${label})`,
    ...(nativePreview ? ['--json', '--ignore-base-config'] : []),
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
  // Staging (stg) and PR Previews never change the active production deployment.
  const nativePreview = plan.target === 'staging' || plan.target === 'pr';
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
    // A PR head branch may be long or contain `/`; the PR number is a safe tag.
    const tag = `${plan.target === 'pr' ? `pr-${plan.pr_number}` : plan.branch}-${randomUUID()}`;
    const childEnv = cloudflareEnvironment(plan, env);
    if (!await isCurrent(plan, github)) return { state: 'superseded' };
    execute(process.execPath, [wranglerBin, ...deploymentArgs(plan, configPath, secretsPath, tag)], {
      cwd: dirname(configPath), env: childEnv,
      ...(nativePreview ? { stdio: ['ignore', 'ignore', 'inherit'] } : {}),
    });
    const after = await activeOrUndefined(cloudflare, workerPath);
    if (nativePreview) {
      const previewPath = `/accounts/${plan.project.account_id}/workers/workers/${plan.project.worker_name}/previews/${encodeURIComponent(plan.project.preview_name)}`;
      let preview, deployment;
      for (let attempt = 0; attempt < 6; attempt++) {
        try {
          preview = await cloudflare(previewPath);
          deployment = await cloudflare(`${previewPath}/deployments/latest`);
        } catch (error) {
          if (!/^API request failed \(404\)/.test(error.message)) throw error;
        }
        if (deployment?.annotations?.['workers/tag'] === tag) break;
        if (attempt < 5) await pause(2000);
      }
      assert(preview?.name === plan.project.preview_name && typeof preview.id === 'string' && typeof deployment?.id === 'string', 'Cloudflare returned a different Preview deployment');
      assert(Array.isArray(preview.urls) && preview.urls.length > 0, 'Cloudflare returned no Preview URL');
      assert(deployment.annotations?.['workers/tag'] === tag, 'Latest Preview deployment differs from the uploaded source');
      const bindings = deployment.env ?? {};
      assert(bindings && typeof bindings === 'object' && !Array.isArray(bindings), 'Cloudflare returned no Preview bindings');
      const secrets = Object.entries(bindings).filter(([, binding]) => binding?.type === 'secret_text' || binding?.type === 'secret_key');
      assert(secrets.every(([name, binding]) => binding.type === 'secret_text' && plan.project.runtime_secrets.includes(name)), 'Preview deployment contains an unexpected secret binding');
      assert(plan.project.runtime_secrets.every(name => bindings[name]?.type === 'secret_text'), 'Preview deployment is missing a required secret binding');
      assert(isDeepStrictEqual(after, before), 'Native Preview deployment unexpectedly changed active production');
      if (plan.target !== 'pr') return { state: 'deployed', preview_name: preview.name, preview_id: preview.id, deployment_id: deployment.id };
      // Display only: an unexpected URL shape is dropped, never a failed deploy.
      return { state: 'deployed', preview_name: preview.name, preview_id: preview.id, deployment_id: deployment.id, preview_url: displayPreviewUrl(preview.urls[0]) };
    }
    assert(plan.target === 'production', 'Unsupported deployment target');
    assert(after?.versions?.length === 1 && after.versions[0].percentage === 100, 'Production version is not receiving 100% traffic');
    let uploaded;
    for (let attempt = 0; attempt < 6; attempt++) {
      uploaded = (await listVersions(cloudflare, workerPath)).find(version => version.annotations?.['workers/tag'] === tag);
      if (uploaded) break;
      await pause(2000);
    }
    assert(uploaded?.id === after.versions[0].version_id, 'Production active version differs from the uploaded source');
    const version = await cloudflare(`${workerPath}/versions/${after.versions[0].version_id}`);
    assertRequiredSecretBindings(version.resources?.bindings, plan.project.runtime_secrets);
    return { state: 'deployed', version_id: after.versions[0].version_id };
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

export async function deploy(plan, { bundleRoot, envFile, wranglerBin, env = process.env, execute = run, github, cloudflare, pause = ms => new Promise(r => setTimeout(r, ms)) }) {
  if (plan.target === 'pr') {
    assertPullRequestPlan(plan);
    await assertBaseWorker(plan, github);
  }
  const values = readSecrets(envFile);
  const secrets = selectValues(values, plan.project.runtime_secrets);
  const release = JSON.parse(readFileSync(join(bundleRoot, 'vinext-release.json'), 'utf8'));
  assert(isDeepStrictEqual(release, { repo: plan.repo, sha: plan.sha, target: plan.target, worker: plan.project.worker_name }), 'Artifact release does not match dispatch');
  const { config, configPath } = validateBundle(bundleRoot, plan);
  validateD1Migrations(bundleRoot, plan);
  // Re-sanitize on the clean runner, not just in the application build job.
  writeFileSync(configPath, JSON.stringify(config));
  if (!await isCurrent(plan, github)) return { state: 'superseded' };
  // An unmerged PR's SQL never runs; prepare already refuses PR plans with migrations.
  const migrated = plan.target === 'pr' ? false : applyD1Migrations(plan, { bundleRoot, wranglerBin, env, execute });
  if (migrated && !await isCurrent(plan, github)) return { state: 'superseded' };
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
    const childEnv = cloudflareEnvironment(plan, env);
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
      const version = await cloudflare(`${workerPath}/versions/${uploaded.id}`);
      assertRequiredSecretBindings(version.resources?.bindings, plan.project.runtime_secrets);
    }
    return { state: 'deployed', version_id: uploaded.id };
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

// A plain https origin (optionally with a path) that is safe to print in Markdown.
function safePreviewUrl(value) {
  return typeof value === 'string' && value.length <= 300 && /^https:\/\/[a-z0-9.-]+(?:\/[A-Za-z0-9._~\/-]*)?$/.test(value);
}

// Cloudflare's first Preview URL as a plain https URL, or '' if it is unusable.
function displayPreviewUrl(value) {
  if (typeof value !== 'string') return '';
  const url = /^[a-z][a-z0-9+.-]*:/i.test(value) ? value : `https://${value}`;
  return safePreviewUrl(url) ? url : '';
}

const notFound = error => /^API request failed \(404\)/.test(error.message);

// Deletes a closed PR's Preview. A Preview that does not exist (the PR was
// always a draft, or it was already deleted) is a successful no-op. Cloudflare's
// API decides existence: wrangler's failure output is not machine-readable.
export async function deletePreview(plan, { wranglerBin, env = process.env, execute = run, cloudflare, pause = ms => new Promise(r => setTimeout(r, ms)) }) {
  assertPullRequestPlan(plan);
  const name = plan.project.preview_name;
  const previewPath = `/accounts/${plan.project.account_id}/workers/workers/${plan.project.worker_name}/previews/${encodeURIComponent(name)}`;
  const exists = async () => {
    try { await cloudflare(previewPath); return true; }
    catch (error) { if (notFound(error)) return false; throw error; }
  };
  if (!await exists()) return { state: 'absent', preview_name: name };
  // No bundle on this route: run from an empty directory so no stray Wrangler
  // configuration applies, and name the Worker explicitly.
  const directory = mkdtempSync(join(tmpdir(), 'vinext-preview-delete-'));
  try {
    const childEnv = cloudflareEnvironment(plan, env);
    delete childEnv.WRANGLER_CI_OVERRIDE_NAME;
    try {
      execute(process.execPath, [wranglerBin, 'preview', 'delete', '--name', name, '--worker-name', plan.project.worker_name, '--skip-confirmation'], { cwd: directory, env: childEnv });
    } catch (error) {
      if (await exists()) throw error;
      return { state: 'absent', preview_name: name };
    }
    for (let attempt = 0; attempt < 6; attempt++) {
      if (!await exists()) return { state: 'deleted', preview_name: name };
      if (attempt < 5) await pause(2000);
    }
    throw new Error('Cloudflare still lists the deleted Preview');
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

export const previewCommentMarker = '<!-- vinext-preview -->';

const commentResults = {
  deployed: 'Deployed',
  failed: 'Failed',
  superseded: 'Superseded',
  removed: 'Removed',
  absent: 'Closed',
};

// The single PR comment. Every interpolated value is validated first, so the
// body cannot carry Markdown or HTML from the payload.
export function previewCommentBody(plan, { result, previewUrl, runUrl }) {
  assert(plan?.target === 'pr' && Number.isInteger(plan.pr_number) && plan.pr_number >= 1 && plan.pr_number <= 9999999, 'Invalid PR Preview comment plan');
  assert(/^[a-f0-9]{40}$/.test(plan.sha), 'Invalid commit SHA');
  assert(Object.hasOwn(commentResults, result), 'Invalid comment result');
  assert(typeof runUrl === 'string' && /^https:\/\/github\.com\/Lascade-Co\/[A-Za-z0-9_.-]+\/actions\/runs\/\d+(?:\/attempts\/\d+)?$/.test(runUrl), 'Invalid run URL');
  const name = `pr-${plan.pr_number}`;
  const commit = `\`${plan.sha.slice(0, 7)}\` (${plan.sha})`;
  const detail = {
    deployed: `${safePreviewUrl(previewUrl) ? `**Preview:** ${previewUrl}` : `Cloudflare did not report a usable Preview URL; see the [run log](${runUrl}).`}\n\nStaff only (Cloudflare Access). Built from commit ${commit} with staging values.`,
    failed: `Commit ${commit} did not deploy. See the [run log](${runUrl}).`,
    superseded: `Commit ${commit} was superseded by a newer commit before it deployed.`,
    removed: `The pull request closed, so the \`${name}\` Preview was deleted. Last commit: ${commit}.`,
    absent: `The pull request closed and no Preview \`${name}\` was left to delete. Last commit: ${commit}.`,
  }[result];
  return `${previewCommentMarker}\n### PR preview \`${name}\`: ${commentResults[result]}\n\n${detail}\n\n[Run](${runUrl})\n`;
}

async function findPreviewComment(plan, github, appSlug) {
  for (let page = 1; page <= 100; page++) {
    const comments = await github(`/repos/${plan.repo}/issues/${plan.pr_number}/comments?per_page=100&page=${page}`);
    assert(Array.isArray(comments), 'Invalid comment list');
    // Only this app's bot account owns the comment; people and other apps may
    // quote the marker. `[` cannot appear in a user login, so this is exact.
    const found = comments.find(comment => comment?.user?.type === 'Bot' && comment.user.login === `${appSlug}[bot]` && typeof comment.body === 'string' && comment.body.startsWith(previewCommentMarker));
    if (found) return found;
    if (!comments.length) return undefined;
  }
  throw new Error('Comment history exceeded the pagination limit');
}

// Creates or edits the single marked PR comment. `overwrite: false` leaves an
// existing comment alone (a newer run owns it); `create: false` never adds one.
export async function upsertPreviewComment(plan, body, github, { appSlug, overwrite = true, create = true } = {}) {
  assert(plan?.target === 'pr' && Number.isInteger(plan.pr_number) && /^Lascade-Co\/[A-Za-z0-9_.-]+$/.test(plan.repo), 'Invalid PR Preview comment plan');
  assert(typeof appSlug === 'string' && /^[a-z0-9][a-z0-9-]{0,99}$/.test(appSlug), 'Invalid GitHub app slug');
  const existing = await findPreviewComment(plan, github, appSlug);
  if (existing) {
    if (!overwrite) return 'skipped';
    assert(Number.isSafeInteger(existing.id), 'Invalid comment id');
    await github(`/repos/${plan.repo}/issues/comments/${existing.id}`, { method: 'PATCH', body: JSON.stringify({ body }) });
    return 'updated';
  }
  if (!create) return 'skipped';
  await github(`/repos/${plan.repo}/issues/${plan.pr_number}/comments`, { method: 'POST', body: JSON.stringify({ body }) });
  return 'created';
}
