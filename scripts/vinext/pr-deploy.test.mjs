import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { prepare, validateDispatch, dispatchRoute } from './config.mjs';
import { projectFromSource, cleanupPlan } from './source.mjs';
import { deploy, deploymentArgs, deletePreview, previewCommentBody, upsertPreviewComment, previewCommentMarker } from './deploy.mjs';
import { stageD1Migrations } from './build.mjs';
import { createHash } from 'node:crypto';

const base = {
  repo: 'Lascade-Co/example-web', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
  project_slug: 'example-web',
};
const prPayload = { ...base, branch: 'feat/pr-previews', pr_number: 42, pr_action: 'synchronize' };
const runtimeValues = { APP_SECRET: 'selected' };

function nativeSource() {
  return {
    pkg: { packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.164.0' }, scripts: { 'check:deploy': 'true', 'build:vinext': 'true' } },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32), images: { binding: 'IMAGES' },
      previews: { images: { binding: 'IMAGES' } },
      env: { staging: { name: 'example-web', images: { binding: 'IMAGES' } }, production: { name: 'example-web', images: { binding: 'IMAGES' } } },
    },
    submodule_repos: [],
  };
}
const planFor = (payload, values = runtimeValues) => prepare(payload, projectFromSource(payload, nativeSource(), values));

function nativeBundle(root, plan) {
  const bundleRoot = join(root, 'bundle');
  mkdirSync(join(bundleRoot, 'server'), { recursive: true });
  mkdirSync(join(bundleRoot, 'client'));
  writeFileSync(join(bundleRoot, 'server', 'index.mjs'), 'export default { fetch() { return new Response("ok") } };');
  writeFileSync(join(bundleRoot, 'server', 'wrangler.json'), JSON.stringify({
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: 'staging',
    main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
    workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' }, previews: nativeSource().wrangler.previews,
  }));
  writeFileSync(join(bundleRoot, 'vinext-release.json'), JSON.stringify({ repo: plan.repo, sha: plan.sha, target: plan.target, worker: 'example-web' }));
  const envFile = join(root, 'infisical.json');
  writeFileSync(envFile, JSON.stringify(runtimeValues));
  return { bundleRoot, envFile };
}

const baseSha = 'd'.repeat(40);
const openPull = { state: 'open', draft: false, head: { sha: base.sha, repo: { full_name: base.repo } }, base: { ref: 'main', sha: baseSha, repo: { full_name: base.repo } } };
const onBase = (pull, ref) => ({ ...pull, base: { ...pull.base, ref } });
const APP = 'lascade-ci';
const ourBot = { type: 'Bot', login: `${APP}[bot]` };
const githubFile = value => { const text = JSON.stringify(value); return { type: 'file', encoding: 'base64', content: Buffer.from(text).toString('base64'), size: Buffer.byteLength(text) }; };

// GitHub for a pr deploy: the PR (head and base) and the base branch's wrangler.jsonc.
function prGithub({ pull = openPull, baseWrangler = nativeSource().wrangler } = {}) {
  const paths = [];
  const github = async path => {
    paths.push(path);
    if (path === '/repos/Lascade-Co/example-web/pulls/42') return pull;
    if (path === `/repos/Lascade-Co/example-web/contents/wrangler.jsonc?ref=${baseSha}`) return githubFile(baseWrangler);
    throw new Error(`Unexpected GitHub path: ${path}`);
  };
  github.paths = paths;
  return github;
}

// Mocks Cloudflare for a native Preview named `name`. `changeProduction` makes
// the active production deployment differ after wrangler runs.
function previewCloudflare(name, state, { changeProduction = false, urls = [`https://${name}-example-web.example.workers.dev`, 'https://other.example.workers.dev'] } = {}) {
  const production = { versions: [{ version_id: 'production-version', percentage: 100 }] };
  let calls = 0;
  return async path => {
    if (path.endsWith(`/previews/${name}/deployments/latest`)) return { id: 'deployment-id', annotations: { 'workers/tag': state.tag }, env: { APP_SECRET: { type: 'secret_text' }, IMAGES: { type: 'images' } } };
    if (path.endsWith(`/previews/${name}`)) return { id: 'preview-id', name, urls };
    if (path.endsWith('/deployments')) return { deployments: [changeProduction && ++calls > 1 ? { versions: [{ version_id: 'pr-version', percentage: 100 }] } : production] };
    throw new Error(`Unexpected Cloudflare API path: ${path}`);
  };
}

test('deploymentArgs is explicit per target and a pr plan can never deploy production', () => {
  const common = ['--config', '/config', '--secrets-file', '/secrets', '--tag', 'tag'];
  const plan = (target, project, extra = {}) => ({ repo: base.repo, sha: base.sha, branch: extra.branch ?? 'main', target, project, ...extra });
  const message = label => ['--message', `${base.repo}@${base.sha} (${label})`];
  const cases = [
    [plan('production', { preview_mode: 'native', preview_name: 'stg' }), ['deploy', ...common, ...message('main')]],
    [plan('production', { preview_mode: 'alias' }), ['deploy', ...common, ...message('main')]],
    [plan('production', {}), ['deploy', ...common, ...message('main')]],
    [plan('staging', { preview_mode: 'native', preview_name: 'stg' }, { branch: 'dev' }), ['preview', '--name', 'stg', ...common, ...message('dev'), '--json', '--ignore-base-config']],
    [plan('staging', { preview_mode: 'alias' }, { branch: 'dev' }), ['versions', 'upload', '--preview-alias', 'dev', ...common, ...message('dev')]],
    [plan('staging', {}, { branch: 'dev' }), ['versions', 'upload', '--preview-alias', 'dev', ...common, ...message('dev')]],
    [plan('pr', { preview_mode: 'native', preview_name: 'pr-42' }, { branch: 'feat/x y', pr_number: 42 }), ['preview', '--name', 'pr-42', ...common, ...message('pr #42'), '--json', '--ignore-base-config']],
  ];
  for (const [input, expected] of cases) assert.deepEqual(deploymentArgs(input, '/config', '/secrets', 'tag'), expected, `${input.target} ${input.project.preview_mode}`);

  for (const target of ['preview', 'prod', 'PR', 'dev', 'main', '', undefined, null]) {
    assert.throws(() => deploymentArgs(plan(target, { preview_mode: 'native', preview_name: 'stg' }), '/config', '/secrets', 'tag'), /Unsupported deployment target/, String(target));
  }
  // Every malformed pr plan throws instead of falling through to `deploy`.
  for (const [name, forged] of [
    ['alias', plan('pr', { preview_mode: 'alias' }, { pr_number: 42 })],
    ['no mode', plan('pr', { preview_name: 'pr-42' }, { pr_number: 42 })],
    ['stg name', plan('pr', { preview_mode: 'native', preview_name: 'stg' }, { pr_number: 42 })],
    ['other pr', plan('pr', { preview_mode: 'native', preview_name: 'pr-41' }, { pr_number: 42 })],
    ['no number', plan('pr', { preview_mode: 'native', preview_name: 'pr-42' })],
    ['string number', plan('pr', { preview_mode: 'native', preview_name: 'pr-42' }, { pr_number: '42' })],
    ['zero', plan('pr', { preview_mode: 'native', preview_name: 'pr-0' }, { pr_number: 0 })],
  ]) {
    let args;
    assert.throws(() => { args = deploymentArgs(forged, '/config', '/secrets', 'tag'); }, /PR Preview/, name);
    assert.equal(args, undefined, name);
  }
});

test('pr deploy runs wrangler preview with name pr-<N> and keeps production unchanged', async () => {
  const plan = planFor(prPayload);
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-deploy-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan);
    const state = {};
    const calls = [];
    const execute = (_command, args, options) => {
      calls.push(args);
      state.tag = args[args.indexOf('--tag') + 1];
      assert.deepEqual(args.slice(1, 4), ['preview', '--name', 'pr-42']);
      assert(args.includes('--json') && args.includes('--ignore-base-config'));
      assert.equal(args[args.indexOf('--message') + 1], `${plan.repo}@${plan.sha} (pr #42)`);
      assert.match(state.tag, /^pr-42-[0-9a-f-]{36}$/);
      assert.deepEqual(JSON.parse(readFileSync(args[args.indexOf('--secrets-file') + 1], 'utf8')), runtimeValues);
      assert.equal(options.env.CLOUDFLARE_ENV, undefined);
      assert.deepEqual(options.stdio, ['ignore', 'ignore', 'inherit']);
    };
    const github = prGithub();
    const result = await deploy(plan, { bundleRoot, envFile, wranglerBin: '/wrangler.js', env: { CLOUDFLARE_ENV: 'production' }, github, cloudflare: previewCloudflare('pr-42', state), execute, pause: async () => {} });
    assert.equal(result.state, 'deployed');
    // Exactly one wrangler call: the Preview upload. No D1 migration, no deploy.
    assert.equal(calls.length, 1);

    // readSecrets consumes the secrets file, so each deploy gets a fresh bundle.
    const fresh = name => { mkdirSync(join(root, name)); return nativeBundle(join(root, name), plan); };

    // The production-unchanged guard also runs for pr.
    const changed = {};
    await assert.rejects(() => deploy(plan, {
      ...fresh('changed'), wranglerBin: '/wrangler.js', env: {}, github, pause: async () => {},
      cloudflare: previewCloudflare('pr-42', changed, { changeProduction: true }),
      execute: (_command, args) => { changed.tag = args[args.indexOf('--tag') + 1]; },
    }), /unexpectedly changed active production/);

    // A superseded PR head never reaches wrangler.
    let executed = false;
    const superseded = await deploy(plan, {
      ...fresh('superseded'), wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
      github: prGithub({ pull: { ...openPull, head: { ...openPull.head, sha: 'c'.repeat(40) } } }),
      cloudflare: previewCloudflare('pr-42', {}), execute: () => { executed = true; },
    });
    assert.deepEqual(superseded, { state: 'superseded' });
    assert.equal(executed, false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('pr deploy returns the preview url', async () => {
  const plan = planFor(prPayload);
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-url-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan);
    const state = {};
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
      github: prGithub(), cloudflare: previewCloudflare('pr-42', state),
      execute: (_command, args) => { state.tag = args[args.indexOf('--tag') + 1]; },
    });
    assert.deepEqual(result, { state: 'deployed', preview_name: 'pr-42', preview_id: 'preview-id', deployment_id: 'deployment-id', preview_url: 'https://pr-42-example-web.example.workers.dev' });

    // The URL is display-only: an unexpected shape never fails a finished deploy.
    for (const [urls, expected] of [
      [['pr-42-example-web.example.workers.dev'], 'https://pr-42-example-web.example.workers.dev'],
      [['https://pr-42-example-web.example.workers.dev/'], 'https://pr-42-example-web.example.workers.dev/'],
      [['javascript:alert(1)'], ''],
      [['https://x.dev/) [y](https://evil'], ''],
      [[42], ''],
    ]) {
      const dir = join(root, String(Math.random()).slice(2));
      mkdirSync(dir);
      const shaped = {};
      const shapedResult = await deploy(plan, {
        ...nativeBundle(dir, plan), wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
        github: prGithub(), cloudflare: previewCloudflare('pr-42', shaped, { urls }),
        execute: (_command, args) => { shaped.tag = args[args.indexOf('--tag') + 1]; },
      });
      assert.equal(shapedResult.state, 'deployed', String(urls[0]));
      assert.equal(shapedResult.preview_url, expected, String(urls[0]));
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('a forged pr plan in alias mode never runs wrangler', async () => {
  const plan = planFor(prPayload);
  const forged = { ...plan, project: { ...plan.project, preview_mode: 'alias', preview_name: undefined } };
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-alias-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan);
    let executed = false;
    await assert.rejects(() => deploy(forged, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
      github: async () => openPull, cloudflare: async () => { throw new Error('Cloudflare must not be called'); },
      execute: () => { executed = true; },
    }), /PR Preview/);
    assert.equal(executed, false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('a pr deploy whose head targets a different Worker or account than its base is refused', async () => {
  const plan = planFor(prPayload);
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-base-test-'));
  try {
    let executed = false;
    for (const [name, github] of [
      ['renamed Worker', prGithub({ baseWrangler: { ...nativeSource().wrangler, name: 'base-web' } })],
      ['other account', prGithub({ baseWrangler: { ...nativeSource().wrangler, account_id: 'e'.repeat(32) } })],
      ['base in another repo', prGithub({ pull: { ...openPull, base: { ...openPull.base, repo: { full_name: 'someone/example-web' } } } })],
      ['no base sha', prGithub({ pull: { ...openPull, base: { ref: 'main', repo: openPull.base.repo } } })],
      // A writer could push a branch with another Worker and retarget the PR to it.
      ['feature base', prGithub({ pull: onBase(openPull, 'feature/x') })],
      ['no base ref', prGithub({ pull: onBase(openPull, undefined) })],
    ]) {
      mkdirSync(join(root, name));
      await assert.rejects(() => deploy(plan, {
        ...nativeBundle(join(root, name), plan), wranglerBin: '/wrangler.js', env: {}, pause: async () => {}, github,
        cloudflare: async () => { throw new Error('Cloudflare must not be called'); }, execute: () => { executed = true; },
      }), /base/, name);
    }
    assert.equal(executed, false);
    const featureDir = join(root, 'feature-message');
    mkdirSync(featureDir);
    await assert.rejects(() => deploy(plan, {
      ...nativeBundle(featureDir, plan), wranglerBin: '/wrangler.js', env: {}, pause: async () => {}, github: prGithub({ pull: onBase(openPull, 'feature/x') }),
      cloudflare: async () => { throw new Error('Cloudflare must not be called'); }, execute: () => { executed = true; },
    }), /^Error: PR previews require a base branch of main or dev$/);

    // main and dev bases deploy.
    for (const ref of ['main', 'dev']) {
      const dir = join(root, `ok-${ref}`);
      mkdirSync(dir);
      const state = {};
      const result = await deploy(plan, {
        ...nativeBundle(dir, plan), wranglerBin: '/wrangler.js', env: {}, pause: async () => {}, github: prGithub({ pull: onBase(openPull, ref) }),
        cloudflare: previewCloudflare('pr-42', state), execute: (_command, args) => { state.tag = args[args.indexOf('--tag') + 1]; },
      });
      assert.equal(result.state, 'deployed', ref);
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

const migratedDatabase = { binding: 'CORE_DB', database_name: 'example-web-preview', database_id: '22222222-2222-4222-8222-222222222222', migrations_dir: 'migrations' };

test('a pr deploy with a D1 migration bundle runs only the Preview upload', async () => {
  // A forged pr plan that carries staging migrations, over a bundle that holds them.
  const source = nativeSource();
  source.wrangler.previews.d1_databases = [migratedDatabase];
  source.wrangler.env.staging.d1_databases = [migratedDatabase];
  const devPlan = prepare({ ...base, branch: 'dev' }, projectFromSource({ ...base, branch: 'dev' }, source, runtimeValues));
  const plan = { ...devPlan, branch: prPayload.branch, target: 'pr', pr_number: 42, project: { ...devPlan.project, preview_name: 'pr-42' } };
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-d1-deploy-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan);
    const { migrations_dir: _directory, ...unmigrated } = migratedDatabase;
    const configPath = join(bundleRoot, 'server', 'wrangler.json');
    writeFileSync(configPath, JSON.stringify({ ...JSON.parse(readFileSync(configPath, 'utf8')), d1_databases: [unmigrated], previews: { images: { binding: 'IMAGES' }, d1_databases: [unmigrated] } }));
    const appRoot = join(root, 'app');
    mkdirSync(join(appRoot, 'migrations'), { recursive: true });
    writeFileSync(join(appRoot, 'migrations', '0001_init.sql'), 'CREATE TABLE example (id INTEGER);');
    stageD1Migrations(appRoot, bundleRoot, devPlan);
    const state = {}, calls = [];
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {}, pause: async () => {}, github: prGithub({ baseWrangler: source.wrangler }),
      cloudflare: previewCloudflare('pr-42', state),
      execute: (_command, args) => { calls.push(args.slice(1)); state.tag = args[args.indexOf('--tag') + 1]; },
    });
    assert.equal(result.state, 'deployed');
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].slice(0, 3), ['preview', '--name', 'pr-42']);
    assert.equal(calls.flat().includes('d1'), false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

const closedPayload = { ...prPayload, pr_action: 'closed' };

test('pr_action is accepted only on pr dispatches and routes closed PRs to cleanup', () => {
  for (const pr_action of ['opened', 'synchronize', 'reopened', 'ready_for_review', 'workflow_dispatch']) {
    assert.equal(dispatchRoute({ ...prPayload, pr_action }), 'deploy', pr_action);
  }
  assert.equal(dispatchRoute(closedPayload), 'cleanup');
  const { pr_action: _action, ...noAction } = prPayload;
  assert.equal(dispatchRoute(noAction), 'deploy');
  assert.equal(dispatchRoute({ ...base, branch: 'main' }), 'deploy');
  assert.equal(dispatchRoute({ ...base, branch: 'dev' }), 'deploy');
  for (const pr_action of ['edited', 'labeled', 'CLOSED', '', null, 1]) {
    assert.throws(() => validateDispatch({ ...prPayload, pr_action }), /pr_action/, String(pr_action));
  }
  // A push dispatch can never carry a PR action.
  assert.throws(() => validateDispatch({ ...base, branch: 'main', pr_action: 'closed' }), /pr_action/);
  // A closed PR never plans a deploy.
  assert.throws(() => planFor(closedPayload), /closed/);
});

test('cleanup plan resolves the Worker from source and only for closed PRs', () => {
  const plan = cleanupPlan(closedPayload, nativeSource());
  assert.deepEqual(plan, {
    repo: base.repo, branch: 'feat/pr-previews', sha: base.sha, owner: 'Lascade-Co', repo_name: 'example-web', target: 'pr', pr_number: 42,
    project: { account_id: 'b'.repeat(32), worker_name: 'example-web', wrangler_version: '4.164.0', preview_mode: 'native', preview_name: 'pr-42', runtime_secrets: [] },
  });
  // No previews block at the last head: still resolvable, the delete probe decides.
  const noPreviews = nativeSource();
  delete noPreviews.wrangler.previews;
  assert.equal(cleanupPlan(closedPayload, noPreviews).project.preview_name, 'pr-42');
  assert.throws(() => cleanupPlan(prPayload, nativeSource()), /closed/);
  assert.throws(() => cleanupPlan({ ...base, branch: 'main' }, nativeSource()), /closed/);
  const badAccount = nativeSource(); badAccount.wrangler.account_id = 'x';
  assert.throws(() => cleanupPlan(closedPayload, badAccount), /account and Worker/);
  const badWorker = nativeSource(); badWorker.wrangler.name = '../x';
  assert.throws(() => cleanupPlan(closedPayload, badWorker), /account and Worker/);
  const unpinned = nativeSource(); unpinned.pkg.devDependencies.wrangler = '^4.164.0';
  assert.throws(() => cleanupPlan(closedPayload, unpinned), /Wrangler/);
  // `wrangler preview delete` needs the same floor as native Previews.
  for (const version of ['4.134.9', '3.200.0']) {
    const old = nativeSource(); old.pkg.devDependencies.wrangler = version;
    assert.throws(() => cleanupPlan(closedPayload, old), /4\.135\.0/, version);
  }
  for (const version of ['4.135.0', '5.0.0']) {
    const recent = nativeSource(); recent.pkg.devDependencies.wrangler = version;
    assert.equal(cleanupPlan(closedPayload, recent).project.wrangler_version, version);
  }
});

test('deletePreview treats a missing preview as absent', async () => {
  const plan = cleanupPlan(closedPayload, nativeSource());
  const previewPath = `/accounts/${'b'.repeat(32)}/workers/workers/example-web/previews/pr-42`;
  const notFound = () => { throw new Error(`API request failed (404) at ${previewPath}`); };

  // Never deployed (for example a draft PR): no wrangler call, success.
  let executed = 0;
  const absent = await deletePreview(plan, { wranglerBin: '/wrangler.js', env: {}, execute: () => { executed++; }, cloudflare: async path => { assert.equal(path, previewPath); notFound(); }, pause: async () => {} });
  assert.deepEqual(absent, { state: 'absent', preview_name: 'pr-42' });
  assert.equal(executed, 0);

  // Present: wrangler deletes it by name from an empty directory, then Cloudflare confirms.
  let exists = true, cwd;
  const deleted = await deletePreview(plan, {
    wranglerBin: '/wrangler.js', env: { CLOUDFLARE_ENV: 'production', WRANGLER_CI_OVERRIDE_NAME: 'other-worker', GH_TOKEN: 'x' }, pause: async () => {},
    cloudflare: async () => exists ? { id: 'preview-id', name: 'pr-42' } : notFound(),
    execute: (command, args, options) => {
      assert.equal(command, process.execPath);
      assert.deepEqual(args, ['/wrangler.js', 'preview', 'delete', '--name', 'pr-42', '--worker-name', 'example-web', '--skip-confirmation']);
      assert.equal(options.env.CLOUDFLARE_ENV, undefined);
      assert.equal(options.env.WRANGLER_CI_OVERRIDE_NAME, undefined);
      assert.equal(options.env.GH_TOKEN, undefined);
      assert.equal(options.env.CLOUDFLARE_ACCOUNT_ID, 'b'.repeat(32));
      cwd = options.cwd;
      assert.deepEqual(readdirSync(cwd), []);
      exists = false;
    },
  });
  assert.deepEqual(deleted, { state: 'deleted', preview_name: 'pr-42' });
  assert.equal(existsSync(cwd), false);

  // Wrangler fails because it was deleted concurrently: still absent.
  exists = true;
  const raced = await deletePreview(plan, {
    wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
    cloudflare: async () => exists ? { id: 'preview-id', name: 'pr-42' } : notFound(),
    execute: () => { exists = false; throw new Error('wrangler failed (1)'); },
  });
  assert.deepEqual(raced, { state: 'absent', preview_name: 'pr-42' });

  // Wrangler fails and the Preview is still there: the run fails.
  await assert.rejects(() => deletePreview(plan, {
    wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
    cloudflare: async () => ({ id: 'preview-id', name: 'pr-42' }),
    execute: () => { throw new Error('wrangler failed (1)'); },
  }), /wrangler failed/);

  // Wrangler succeeds but Cloudflare keeps listing it.
  await assert.rejects(() => deletePreview(plan, {
    wranglerBin: '/wrangler.js', env: {}, pause: async () => {},
    cloudflare: async () => ({ id: 'preview-id', name: 'pr-42' }), execute: () => {},
  }), /still lists/);

  // Other Cloudflare errors are not "absent".
  await assert.rejects(() => deletePreview(plan, {
    wranglerBin: '/wrangler.js', env: {}, pause: async () => {}, execute: () => { executed++; },
    cloudflare: async () => { throw new Error(`API request failed (403) at ${previewPath}`); },
  }), /403/);
  assert.equal(executed, 0);

  // Only pr plans can be deleted.
  for (const forged of [{ ...plan, target: 'staging' }, { ...plan, project: { ...plan.project, preview_name: 'stg' } }]) {
    await assert.rejects(() => deletePreview(forged, { wranglerBin: '/wrangler.js', env: {}, execute: () => { executed++; }, cloudflare: async () => ({}) }), /PR Preview/);
  }
  assert.equal(executed, 0);
});

// An in-memory GitHub issue comment API. `perPage` makes pagination observable.
function commentApi(initial = [], perPage = 2) {
  const comments = initial.map((comment, index) => ({ id: index + 1, ...comment }));
  const requests = [];
  const github = async (path, options = {}) => {
    const method = options.method ?? 'GET';
    requests.push(`${method} ${path.replace(/\?.*$/, '')}`);
    const list = /^\/repos\/Lascade-Co\/example-web\/issues\/42\/comments(?:\?per_page=100&page=(\d+))?$/.exec(path);
    if (method === 'GET' && list) {
      const page = Number(list[1]);
      return comments.slice((page - 1) * perPage, page * perPage);
    }
    if (method === 'POST' && list) {
      const comment = { id: comments.length + 1, user: ourBot, body: JSON.parse(options.body).body };
      comments.push(comment);
      return comment;
    }
    const patch = /^\/repos\/Lascade-Co\/example-web\/issues\/comments\/(\d+)$/.exec(path);
    if (method === 'PATCH' && patch) {
      const comment = comments.find(item => item.id === Number(patch[1]));
      comment.body = JSON.parse(options.body).body;
      return comment;
    }
    throw new Error(`Unexpected GitHub request: ${method} ${path}`);
  };
  return { github, comments, requests };
}

test('comment upserts a single marked comment', async () => {
  const plan = { repo: base.repo, sha: base.sha, target: 'pr', pr_number: 42 };
  const runUrl = 'https://github.com/Lascade-Co/actions/actions/runs/9';
  const api = commentApi([
    { user: { type: 'User' }, body: 'looks good' },
    // A person quoting the marker does not own the Preview comment.
    { user: { type: 'User' }, body: `${previewCommentMarker}\nquoted` },
  ]);
  const deployed = previewCommentBody(plan, { result: 'deployed', previewUrl: 'https://pr-42-example-web.example.workers.dev', runUrl });
  assert(deployed.startsWith(previewCommentMarker));
  assert.match(deployed, /https:\/\/pr-42-example-web\.example\.workers\.dev/);
  assert.match(deployed, new RegExp(base.sha));
  assert.match(deployed, /deployed/i);
  assert.equal(await upsertPreviewComment(plan, deployed, api.github, { appSlug: APP }), 'created');
  const failed = previewCommentBody(plan, { result: 'failed', runUrl });
  assert.equal(await upsertPreviewComment(plan, failed, api.github, { appSlug: APP }), 'updated');
  // A third comment from others pushes ours to page 2 of the listing; still found.
  api.comments.splice(1, 0, { id: 99, user: { type: 'User' }, body: 'another' });
  assert.equal(await upsertPreviewComment(plan, deployed, api.github, { appSlug: APP }), 'updated');
  assert.equal(api.requests.filter(request => request.startsWith('POST')).length, 1);
  assert.equal(api.requests.filter(request => request.startsWith('PATCH')).length, 2);
  const ours = api.comments.filter(comment => comment.user.type === 'Bot');
  assert.equal(ours.length, 1);
  assert.equal(ours[0].body, deployed);
  assert.equal(api.comments[2].body, `${previewCommentMarker}\nquoted`);

  // A stale run never overwrites a newer run's comment, but may create the first one.
  assert.equal(await upsertPreviewComment(plan, failed, api.github, { appSlug: APP, overwrite: false }), 'skipped');
  assert.equal(ours[0].body, deployed);
  const fresh = commentApi();
  assert.equal(await upsertPreviewComment(plan, previewCommentBody(plan, { result: 'superseded', runUrl }), fresh.github, { appSlug: APP, overwrite: false }), 'created');

  // Another app's comment carrying the marker is not ours: a new one is created.
  const otherBot = commentApi([{ user: { type: 'Bot', login: 'other-app[bot]' }, performed_via_github_app: { slug: 'other-app' }, body: `${previewCommentMarker}\nnot ours` }]);
  assert.equal(await upsertPreviewComment(plan, deployed, otherBot.github, { appSlug: APP }), 'created');
  assert.equal(otherBot.comments[0].body, `${previewCommentMarker}\nnot ours`);
  assert.equal(otherBot.comments.length, 2);
  // Without a valid app identity nothing is matched or written.
  for (const appSlug of [undefined, '', 'Bad Slug', 'x[bot]']) {
    await assert.rejects(() => upsertPreviewComment(plan, deployed, otherBot.github, { appSlug }), /app slug/, String(appSlug));
  }
  assert.equal(otherBot.comments.length, 2);

  for (const result of ['superseded', 'removed', 'absent']) assert.match(previewCommentBody(plan, { result, runUrl }), new RegExp(base.sha.slice(0, 7)));
  assert.throws(() => previewCommentBody(plan, { result: 'unknown', runUrl }), /comment result/);
  // An unusable URL is left out of the comment, never printed.
  for (const previewUrl of ['javascript:alert(1)', 'https://x.dev/) [y](https://evil', '', undefined]) {
    const body = previewCommentBody(plan, { result: 'deployed', previewUrl, runUrl });
    assert.match(body, /did not report a usable Preview URL/, String(previewUrl));
    if (previewUrl) assert.equal(body.includes(previewUrl), false, previewUrl);
  }
  assert.throws(() => previewCommentBody(plan, { result: 'failed', runUrl: 'https://evil.example/run' }), /run URL/);
  assert.throws(() => previewCommentBody({ ...plan, target: 'staging' }, { result: 'failed', runUrl }), /PR Preview/);
});

// Runs cli.mjs in-process with the given env and a mocked fetch. Returns the
// exit code, error lines, outputs and the fetch requests.
async function runCli(command, env, respond) {
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-cli-'));
  const keys = ['PAYLOAD_JSON', 'PLAN_JSON', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY', 'GH_TOKEN', 'STATUS_STATE', 'STATUS_URL', 'DEPLOY_STATE', 'DEPLOY_TARGETS', 'CLOUDFLARE_API_TOKEN', 'COMMENT_RESULT', 'COMMENT_MODE', 'APP_SLUG', 'PREVIEW_URL', 'RUN_URL', 'WRANGLER_BIN'];
  const saved = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  const originalFetch = globalThis.fetch, originalCommand = process.argv[2], originalExitCode = process.exitCode, originalError = console.error, originalLog = console.log;
  const errors = [], requests = [];
  try {
    for (const key of keys) delete process.env[key];
    Object.assign(process.env, { GITHUB_OUTPUT: join(root, 'outputs') }, env);
    process.argv[2] = command;
    process.exitCode = undefined;
    console.error = line => errors.push(line);
    console.log = () => {};
    globalThis.fetch = async (url, options = {}) => {
      requests.push({ method: options.method ?? 'GET', url: String(url), body: options.body && JSON.parse(options.body) });
      const value = await respond(new URL(url), options);
      return value instanceof Response ? value : new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });
    };
    await import(`./cli.mjs?pr-deploy=${Date.now()}-${Math.random()}`);
    const outputs = existsSync(process.env.GITHUB_OUTPUT) ? readFileSync(process.env.GITHUB_OUTPUT, 'utf8') : '';
    return { exitCode: process.exitCode, errors, requests, outputs };
  } finally {
    globalThis.fetch = originalFetch;
    console.error = originalError;
    console.log = originalLog;
    process.argv[2] = originalCommand;
    process.exitCode = originalExitCode;
    for (const key of keys) if (saved[key] === undefined) delete process.env[key]; else process.env[key] = saved[key];
    rmSync(root, { recursive: true, force: true });
  }
}

test('status context is vinext/preview for pr', async () => {
  const contexts = {};
  for (const [name, payload] of [['pr', prPayload], ['dev', { ...base, branch: 'dev' }], ['main', { ...base, branch: 'main' }]]) {
    const { exitCode, requests } = await runCli('status', { PAYLOAD_JSON: JSON.stringify(payload), GH_TOKEN: 't', STATUS_STATE: 'success', STATUS_URL: 'https://github.com/Lascade-Co/actions/actions/runs/9' }, async () => ({}));
    assert.equal(exitCode, undefined, name);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, `https://api.github.com/repos/Lascade-Co/example-web/statuses/${base.sha}`);
    contexts[name] = requests[0].body.context;
  }
  assert.deepEqual(contexts, { pr: 'vinext/preview', dev: 'vinext/staging', main: 'vinext/production' });
});

test('CLI deploy refuses a plan outside the step DEPLOY_TARGETS', async () => {
  const plan = planFor(prPayload);
  for (const [targets, planJson] of [['production,staging', plan], ['pr', planFor({ ...base, branch: 'main' })], [undefined, plan]]) {
    const { exitCode, errors, requests } = await runCli('deploy', { PLAN_JSON: JSON.stringify(planJson), GH_TOKEN: 't', CLOUDFLARE_API_TOKEN: 'c', ...(targets ? { DEPLOY_TARGETS: targets } : {}) }, async () => { throw new Error('no requests'); });
    assert.equal(exitCode, 1, String(targets));
    assert.match(errors.join('\n'), /not allowed in this deploy step/);
    assert.equal(requests.length, 0);
  }
});

test('CLI comment reports the deploy result and skips when superseded by a newer head', async () => {
  const plan = planFor(prPayload);
  const runUrl = 'https://github.com/Lascade-Co/actions/actions/runs/9';
  const respond = pull => async (url, options = {}) => {
    if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return pull;
    if (url.pathname === '/repos/Lascade-Co/example-web/issues/42/comments' && (options.method ?? 'GET') === 'GET') return url.searchParams.get('page') === '1' ? [{ id: 5, user: ourBot, body: `${previewCommentMarker}\nold` }] : [];
    if (url.pathname === '/repos/Lascade-Co/example-web/issues/comments/5' && options.method === 'PATCH') return {};
    throw new Error(`Unexpected request ${options.method} ${url}`);
  };
  const env = { APP_SLUG: APP, PLAN_JSON: JSON.stringify(plan), GH_TOKEN: 't', STATUS_STATE: 'success', DEPLOY_STATE: 'deployed', PREVIEW_URL: 'https://pr-42-example-web.example.workers.dev', RUN_URL: runUrl };
  const current = await runCli('comment', env, respond(openPull));
  assert.equal(current.exitCode, undefined, current.errors.join('\n'));
  const patch = current.requests.find(request => request.method === 'PATCH');
  assert.match(patch.body.body, /pr-42-example-web/);
  assert.match(patch.body.body, /deployed/i);

  const stale = await runCli('comment', { ...env, STATUS_STATE: 'failure', DEPLOY_STATE: '' }, respond({ ...openPull, head: { ...openPull.head, sha: 'c'.repeat(40) } }));
  assert.equal(stale.exitCode, undefined, stale.errors.join('\n'));
  assert.equal(stale.requests.filter(request => request.method !== 'GET').length, 0);

  // Without a plan (prepare failed) the payload still identifies the PR.
  const early = await runCli('comment', { APP_SLUG: APP, PAYLOAD_JSON: JSON.stringify(prPayload), GH_TOKEN: 't', STATUS_STATE: 'failure', RUN_URL: runUrl }, respond(openPull));
  assert.equal(early.exitCode, undefined, early.errors.join('\n'));
  assert.match(early.requests.find(request => request.method === 'PATCH').body.body, /failed/i);

  // The prepare job's failure comment is update-only: it is outside the deploy
  // queue, so only queued runs may create the comment.
  const updateOnly = await runCli('comment', { APP_SLUG: APP, PAYLOAD_JSON: JSON.stringify(prPayload), GH_TOKEN: 't', STATUS_STATE: 'failure', RUN_URL: runUrl, COMMENT_MODE: 'update-only' }, async (url, options = {}) => {
    if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return openPull;
    if (url.pathname === '/repos/Lascade-Co/example-web/issues/42/comments' && (options.method ?? 'GET') === 'GET') return [];
    throw new Error(`Unexpected request ${options.method} ${url}`);
  });
  assert.equal(updateOnly.exitCode, undefined, updateOnly.errors.join('\n'));
  assert.equal(updateOnly.requests.filter(request => request.method !== 'GET').length, 0);
  const badMode = await runCli('comment', { APP_SLUG: APP, PAYLOAD_JSON: JSON.stringify(prPayload), GH_TOKEN: 't', STATUS_STATE: 'failure', RUN_URL: runUrl, COMMENT_MODE: 'create' }, async () => { throw new Error('no requests'); });
  assert.equal(badMode.exitCode, 1);

  // Never for dev or main.
  const push = await runCli('comment', { PAYLOAD_JSON: JSON.stringify({ ...base, branch: 'main' }), GH_TOKEN: 't', STATUS_STATE: 'success', RUN_URL: runUrl }, respond(openPull));
  assert.equal(push.exitCode, 1);
  assert.equal(push.requests.length, 0);
});

// Answers the GitHub source-file requests that fetchSourceFiles makes at `sha`.
function sourceFiles(url, sha = baseSha, wrangler = nativeSource().wrangler) {
  const fixture = nativeSource();
  if (url.pathname.endsWith('/contents/package.json') && url.searchParams.get('ref') === sha) return githubFile(fixture.pkg);
  if (url.pathname.endsWith('/contents/wrangler.jsonc') && url.searchParams.get('ref') === sha) return githubFile(wrangler);
  if (url.pathname.endsWith(`/git/commits/${sha}`)) return { tree: { sha: 'c'.repeat(40) } };
  if (url.pathname.endsWith(`/git/trees/${'c'.repeat(40)}`)) return { tree: [], truncated: false };
  return undefined;
}

const closedPull = { ...openPull, state: 'closed' };
// A closed PR whose base deploys `baseWrangler`. Cloudflare reports no Preview.
const cleanupResponder = ({ pull = closedPull, baseWrangler, comments = [] } = {}) => async (url, options = {}) => {
  const file = sourceFiles(url, baseSha, baseWrangler);
  if (file) return file;
  if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return pull;
  if (url.hostname === 'api.cloudflare.com' && /\/workers\/workers\/[a-z0-9-]+\/previews\/pr-42$/.test(url.pathname)) return new Response(JSON.stringify({ success: false }), { status: 404 });
  if (url.pathname === '/repos/Lascade-Co/example-web/issues/42/comments' && (options.method ?? 'GET') === 'GET') return url.searchParams.get('page') === '1' ? comments : [];
  if (url.pathname === '/repos/Lascade-Co/example-web/issues/comments/5' && options.method === 'PATCH') return {};
  throw new Error(`Unexpected request ${options.method} ${url}`);
};
const runUrl = 'https://github.com/Lascade-Co/actions/actions/runs/9';
const cleanupEnv = { PAYLOAD_JSON: JSON.stringify(closedPayload), GH_TOKEN: 't', CLOUDFLARE_API_TOKEN: 'c', RUN_URL: runUrl, WRANGLER_BIN: '/wrangler.js', APP_SLUG: APP };
const workerKey = (account, worker) => createHash('sha256').update(`${account}/${worker}`).digest('hex');

test('CLI cleanup-resolve outputs the base Worker key only for closed PRs', async () => {
  const resolved = await runCli('cleanup-resolve', { PAYLOAD_JSON: JSON.stringify(closedPayload), GH_TOKEN: 't' }, cleanupResponder());
  assert.equal(resolved.exitCode, undefined, resolved.errors.join('\n'));
  assert.match(resolved.outputs, new RegExp(`^worker_key=${workerKey('b'.repeat(32), 'example-web')}$`, 'm'));
  assert.match(resolved.outputs, /^wrangler_version=4\.164\.0$/m);
  // Files are read at the base commit only, never at the PR head.
  assert.equal(resolved.requests.some(request => request.url.includes(`ref=${base.sha}`)), false);
  const open = await runCli('cleanup-resolve', { PAYLOAD_JSON: JSON.stringify(prPayload), GH_TOKEN: 't' }, cleanupResponder());
  assert.equal(open.exitCode, 1);
  assert.equal(open.requests.length, 0);
});

test('CLI cleanup is bound to the closed PR head and deletes from the base Worker', async () => {
  // A dispatch whose sha is not the closed PR's head, or whose head is a fork, is refused.
  for (const [name, pull] of [
    ['other sha', { ...closedPull, head: { ...closedPull.head, sha: 'c'.repeat(40) } }],
    ['fork head', { ...closedPull, head: { ...closedPull.head, repo: { full_name: 'someone/example-web' } } }],
    ['deleted fork', { ...closedPull, head: { ...closedPull.head, repo: null } }],
  ]) {
    for (const command of ['cleanup-resolve', 'cleanup']) {
      const refused = await runCli(command, cleanupEnv, cleanupResponder({ pull }));
      assert.equal(refused.exitCode, 1, `${name} ${command}`);
      assert.match(refused.errors.join('\n'), /closed pull request head/, `${name} ${command}`);
      assert.equal(refused.requests.some(request => request.url.startsWith('https://api.cloudflare.com') || request.method !== 'GET'), false, `${name} ${command}`);
    }
  }

  // Only main and dev bases: a feature base is refused before any file or Cloudflare request.
  for (const command of ['cleanup-resolve', 'cleanup']) {
    const feature = await runCli(command, cleanupEnv, cleanupResponder({ pull: onBase(closedPull, 'feature/x') }));
    assert.equal(feature.exitCode, 1, command);
    assert.match(feature.errors.join('\n'), /PR previews require a base branch of main or dev/, command);
    assert.equal(feature.requests.some(request => request.url.includes('/contents/') || request.url.startsWith('https://api.cloudflare.com')), false, command);
  }
  for (const ref of ['main', 'dev']) {
    const accepted = await runCli('cleanup', cleanupEnv, cleanupResponder({ pull: onBase(closedPull, ref) }));
    assert.equal(accepted.exitCode, undefined, `${ref}: ${accepted.errors.join('\n')}`);
    assert.match(accepted.outputs, /^state=absent$/m);
  }

  // The head renamed the Worker: cleanup still targets the base branch's Worker.
  const renamed = await runCli('cleanup', cleanupEnv, async (url, options) => {
    const head = sourceFiles(url, base.sha, { ...nativeSource().wrangler, name: 'renamed-web' });
    if (head) return head;
    return cleanupResponder()(url, options);
  });
  assert.equal(renamed.exitCode, undefined, renamed.errors.join('\n'));
  assert.match(renamed.outputs, /^state=absent$/m);
  const probes = renamed.requests.filter(request => request.url.startsWith('https://api.cloudflare.com'));
  assert.equal(probes.length, 1);
  assert.match(probes[0].url, new RegExp(`/accounts/${'b'.repeat(32)}/workers/workers/example-web/previews/pr-42$`));
});

test('CLI cleanup skips a reopened PR and refuses non-closed payloads', async () => {
  // Reopened (even after new pushes, onto any base, with a broken base): a quiet
  // no-op before any base file is read. The reopen dispatch owns the Preview.
  for (const pull of [
    { ...openPull, head: { ...openPull.head, sha: 'c'.repeat(40) } },
    { ...onBase(openPull, 'feature/x'), base: { ref: 'feature/x' } },
  ]) {
    for (const command of ['cleanup-resolve', 'cleanup']) {
      const reopened = await runCli(command, cleanupEnv, async url => {
        if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return pull;
        throw new Error(`Unexpected request ${url}`);
      });
      assert.equal(reopened.exitCode, undefined, `${command}: ${reopened.errors.join('\n')}`);
      assert.match(reopened.outputs, /^state=reopened$/m, command);
      assert.doesNotMatch(reopened.outputs, /worker_key|wrangler_version/, command);
      assert.equal(reopened.requests.length, 1, command);
    }
  }

  // An invalid APP_SLUG fails cleanup before any request, so nothing is deleted.
  for (const APP_SLUG of ['', 'Bad Slug']) {
    const badSlug = await runCli('cleanup', { ...cleanupEnv, APP_SLUG }, async () => { throw new Error('no requests'); });
    assert.equal(badSlug.exitCode, 1, APP_SLUG);
    assert.match(badSlug.errors.join('\n'), /app slug/, APP_SLUG);
    assert.equal(badSlug.requests.length, 0, APP_SLUG);
  }
  const { APP_SLUG: _slug, ...noSlug } = cleanupEnv;
  const missingSlug = await runCli('cleanup', noSlug, async () => { throw new Error('no requests'); });
  assert.equal(missingSlug.exitCode, 1);
  assert.equal(missingSlug.requests.length, 0);

  // Closed and never deployed: absent is success. Our existing comment is
  // updated; none is created, and another app's marked comment is not touched.
  for (const [comments, expectedComment] of [
    [[{ id: 5, user: ourBot, body: `${previewCommentMarker}\nold` }], /no Preview/i],
    [[], undefined],
    [[{ id: 6, user: { type: 'Bot', login: 'other-app[bot]' }, body: `${previewCommentMarker}\nold` }], undefined],
  ]) {
    const closed = await runCli('cleanup', cleanupEnv, cleanupResponder({ comments }));
    assert.equal(closed.exitCode, undefined, closed.errors.join('\n'));
    assert.match(closed.outputs, /^state=absent$/m);
    const writes = closed.requests.filter(request => request.method !== 'GET');
    if (expectedComment) assert.match(writes.find(request => request.method === 'PATCH').body.body, expectedComment);
    else assert.equal(writes.length, 0);
  }

  const forged = await runCli('cleanup', { ...cleanupEnv, PAYLOAD_JSON: JSON.stringify(prPayload) }, async () => { throw new Error('no requests'); });
  assert.equal(forged.exitCode, 1);
  assert.equal(forged.requests.length, 0);
});

test('CLI verify-source refuses a PR head that targets a different Worker than base', async () => {
  const plan = planFor(prPayload);
  const respond = baseWrangler => async url => {
    const file = sourceFiles(url, baseSha, baseWrangler);
    if (file) return file;
    if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return openPull;
    throw new Error(`Unexpected request ${url}`);
  };
  const same = await runCli('verify-source', { PLAN_JSON: JSON.stringify(plan), GH_TOKEN: 't' }, respond(nativeSource().wrangler));
  assert.equal(same.exitCode, undefined, same.errors.join('\n'));
  for (const baseWrangler of [{ ...nativeSource().wrangler, name: 'base-web' }, { ...nativeSource().wrangler, account_id: 'e'.repeat(32) }]) {
    const other = await runCli('verify-source', { PLAN_JSON: JSON.stringify(plan), GH_TOKEN: 't' }, respond(baseWrangler));
    assert.equal(other.exitCode, 1);
    assert.match(other.errors.join('\n'), /base branch/);
  }
  // Only main and dev bases.
  const featureBase = await runCli('verify-source', { PLAN_JSON: JSON.stringify(plan), GH_TOKEN: 't' }, async url => {
    if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return onBase(openPull, 'feature/x');
    throw new Error(`Unexpected request ${url}`);
  });
  assert.equal(featureBase.exitCode, 1);
  assert.match(featureBase.errors.join('\n'), /PR previews require a base branch of main or dev/);
  for (const ref of ['main', 'dev']) {
    const accepted = await runCli('verify-source', { PLAN_JSON: JSON.stringify(plan), GH_TOKEN: 't' }, async url => {
      if (url.pathname === '/repos/Lascade-Co/example-web/pulls/42') return onBase(openPull, ref);
      return respond(nativeSource().wrangler)(url);
    });
    assert.equal(accepted.exitCode, undefined, `${ref}: ${accepted.errors.join('\n')}`);
  }

  // dev and main are unchanged: no PR or base lookup.
  const dev = await runCli('verify-source', { PLAN_JSON: JSON.stringify(planFor({ ...base, branch: 'dev' })), GH_TOKEN: 't' }, async url => {
    assert.equal(url.pathname, '/repos/Lascade-Co/example-web/commits/dev');
    return { sha: base.sha };
  });
  assert.equal(dev.exitCode, undefined, dev.errors.join('\n'));
  assert.equal(dev.requests.length, 1);
});
