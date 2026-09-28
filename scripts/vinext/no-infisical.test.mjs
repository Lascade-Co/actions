import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { prepare, validateDispatch } from './config.mjs';
import { projectFromSource } from './source.mjs';
import { exportSecrets, loadSecrets } from './secrets.mjs';
import { deploy } from './deploy.mjs';
import { build } from './build.mjs';

const payload = {
  repo: 'Lascade-Co/example-web', branch: 'main', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
};
function source() {
  const production = { binding: 'APP_KV', id: '1'.repeat(32) };
  const staging = { binding: 'APP_KV', id: '2'.repeat(32) };
  return {
    pkg: { packageManager: 'npm@11.6.0', devDependencies: { wrangler: '4.135.0' }, scripts: { 'check:deploy': 'true', 'build:vinext': 'true' } },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32), kv_namespaces: [production],
      previews: { kv_namespaces: [staging] },
      env: { production: { name: 'example-web', kv_namespaces: [production] }, staging: { name: 'example-web', kv_namespaces: [staging] } },
    },
    submodule_repos: [],
  };
}
const neverFetch = async () => assert.fail('Infisical network access must be skipped');
const unreadableEnv = new Proxy({}, { get() { assert.fail('Infisical configuration must not be read'); } });
const planFor = (dispatch = payload, values = {}) => prepare(dispatch, projectFromSource(dispatch, source(), values));

test('dispatch omission and empty project slugs normalize to no-environment plans', () => {
  for (const dispatch of [payload, { ...payload, project_slug: '' }]) {
    assert.equal(validateDispatch(dispatch).project_slug, '');
    const plan = planFor(dispatch);
    assert.equal(plan.infisical_project_slug, '');
    assert.deepEqual(plan.project.build_variables, []);
    assert.deepEqual(plan.project.runtime_secrets, []);
    assert.equal(plan.project.worker_name, 'example-web');
    assert.equal(plan.project.preview_mode, 'native');
  }
});

test('optional slug validation rejects nonstrings and every unsafe character, including trailing newlines', () => {
  for (const project_slug of [null, 0, false, {}, [], ' ', ' app', 'app ', 'app\n', 'app\r\n', 'app/name', 'app.name', 'applé']) {
    assert.throws(() => validateDispatch({ ...payload, project_slug }), /valid Infisical project slug/);
  }
  for (const project_slug of ['example-web', 'Project_123']) assert.equal(validateDispatch({ ...payload, project_slug }).project_slug, project_slug);
  assert.throws(() => validateDispatch({ ...payload, branch: 'other' }), /Only dev and main/);
  assert.throws(() => validateDispatch({ ...payload, repo: 'Other/example-web' }), /Only Lascade-Co/);
});

test('no-environment loading returns empty values before inspecting credentials or fetching', async () => {
  for (const infisical_project_slug of [undefined, '']) assert.deepEqual(await loadSecrets({ infisical_project_slug }, { env: unreadableEnv, fetcher: neverFetch }), {});
  await assert.rejects(() => loadSecrets({ infisical_project_slug: null }, { env: unreadableEnv, fetcher: neverFetch }), /valid Infisical project slug/);
});

test('no-environment preparation cannot conceal build variables or runtime secrets', () => {
  for (const values of [{ NEXT_PUBLIC_API_URL: 'https://example.com' }, { APP_SECRET: 'required' }]) assert.throws(() => planFor(payload, values), /requires no build variables or runtime secrets/);
  const configured = planFor({ ...payload, project_slug: 'example-web' }, { NEXT_PUBLIC_API_URL: 'https://example.com', APP_SECRET: 'required' });
  assert.deepEqual(configured.project.build_variables, ['NEXT_PUBLIC_API_URL']);
  assert.deepEqual(configured.project.runtime_secrets, ['APP_SECRET']);
  const invalid = source();
  invalid.wrangler.env.production.name = 'another-worker';
  assert.throws(() => projectFromSource(payload, invalid, {}), /same Worker and account/);
});

test('empty build and runtime snapshots export without Infisical credentials', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-no-infisical-export-'));
  try {
    for (const kind of ['build', 'runtime']) {
      const file = join(root, `${kind}.json`);
      await exportSecrets(planFor(), { file, kind, env: unreadableEnv, fetcher: neverFetch });
      assert.equal(readFileSync(file, 'utf8'), '{}');
      assert.equal(statSync(file).mode & 0o777, 0o600);
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('empty-slug export rejects required values in either list, even when exporting the other kind', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-no-infisical-required-'));
  try {
    for (const kind of ['build', 'runtime']) for (const list of ['build_variables', 'runtime_secrets']) {
      const plan = planFor();
      plan.project[list] = ['REQUIRED'];
      const file = join(root, `${kind}-${list}.json`);
      writeFileSync(file, '{}');
      await assert.rejects(() => exportSecrets(plan, { file, kind, env: unreadableEnv, fetcher: neverFetch }), /requires no build variables or runtime secrets/);
      assert.throws(() => statSync(file), { code: 'ENOENT' });
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('existing nonempty slugs retain Universal Auth and the selected project/environment lookup', async () => {
  const calls = [];
  const values = await loadSecrets({ infisical_project_slug: 'example-web', infisical_env: 'prod', infisical_path: '/' }, {
    env: { INFISICAL_DOMAIN: 'https://infisical.example', INFISICAL_CLIENT_ID: 'mock-id', INFISICAL_CLIENT_SECRET: 'mock-secret' },
    fetcher: async (url, options) => {
      calls.push({ url, options });
      return { ok: true, json: async () => url.pathname.endsWith('/login') ? { accessToken: 'mock-token' } : { secrets: [{ secretKey: 'APP_SECRET', secretValue: 'selected' }] } };
    },
  });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url.pathname, '/api/v1/auth/universal-auth/login');
  assert.equal(calls[1].url.searchParams.get('workspaceSlug'), 'example-web');
  assert.equal(calls[1].url.searchParams.get('environment'), 'prod');
  assert.equal(values.APP_SECRET, 'selected');
});

test('CLI prepare reads exact source files without contacting Infisical for an omitted slug', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-no-infisical-cli-'));
  const keys = ['PAYLOAD_JSON', 'GITHUB_OUTPUT', 'GH_TOKEN', 'INFISICAL_DOMAIN'];
  const saved = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  const originalFetch = globalThis.fetch, originalCommand = process.argv[2], originalExitCode = process.exitCode;
  const fixture = source(), paths = [];
  const githubFile = value => { const text = JSON.stringify(value); return { type: 'file', encoding: 'base64', content: Buffer.from(text).toString('base64'), size: Buffer.byteLength(text) }; };
  try {
    process.env.PAYLOAD_JSON = JSON.stringify(payload);
    process.env.GITHUB_OUTPUT = join(root, 'outputs');
    process.env.GH_TOKEN = 'mock-github-token';
    process.env.INFISICAL_DOMAIN = 'invalid-if-read';
    process.argv[2] = 'prepare';
    globalThis.fetch = async urlValue => {
      const url = new URL(urlValue);
      assert.equal(url.hostname, 'api.github.com');
      paths.push(url.pathname);
      let value;
      if (url.pathname.endsWith('/contents/package.json')) { assert.equal(url.searchParams.get('ref'), payload.sha); value = githubFile(fixture.pkg); }
      else if (url.pathname.endsWith('/contents/wrangler.jsonc')) { assert.equal(url.searchParams.get('ref'), payload.sha); value = githubFile(fixture.wrangler); }
      else if (url.pathname.endsWith(`/git/commits/${payload.sha}`)) value = { tree: { sha: 'c'.repeat(40) } };
      else if (url.pathname.endsWith(`/git/trees/${'c'.repeat(40)}`)) value = { tree: [], truncated: false };
      else assert.fail(`Unexpected source URL: ${url.pathname}`);
      return new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });
    };
    await import(`./cli.mjs?no-infisical=${Date.now()}`);
    assert.equal(process.exitCode, originalExitCode);
    const output = readFileSync(process.env.GITHUB_OUTPUT, 'utf8').split('\n').find(line => line.startsWith('plan='));
    const plan = JSON.parse(output.slice(5));
    assert.equal(plan.infisical_project_slug, '');
    assert.deepEqual(plan.project.runtime_secrets, []);
    assert.deepEqual(plan.project.build_variables, []);
    assert.equal(paths.length, 4);
  } finally {
    globalThis.fetch = originalFetch;
    process.argv[2] = originalCommand;
    process.exitCode = originalExitCode;
    for (const key of keys) if (saved[key] === undefined) delete process.env[key]; else process.env[key] = saved[key];
    rmSync(root, { recursive: true, force: true });
  }
});

test('no-environment production deploy still validates the bundle and publishes an empty secrets snapshot', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-no-infisical-deploy-'));
  const plan = planFor(), fixture = source();
  try {
    const bundleRoot = join(root, 'bundle');
    mkdirSync(join(bundleRoot, 'server'), { recursive: true });
    mkdirSync(join(bundleRoot, 'client'));
    writeFileSync(join(bundleRoot, 'server/index.mjs'), 'export default { fetch() { return new Response("ok"); } };');
    writeFileSync(join(bundleRoot, 'server/wrangler.json'), JSON.stringify({
      name: plan.project.worker_name, account_id: plan.project.account_id, targetEnvironment: 'production',
      main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
      workers_dev: true, preview_urls: true, kv_namespaces: fixture.wrangler.kv_namespaces, previews: fixture.wrangler.previews,
    }));
    writeFileSync(join(bundleRoot, 'vinext-release.json'), JSON.stringify({ repo: plan.repo, sha: plan.sha, target: plan.target, worker: plan.project.worker_name }));
    const envFile = join(root, 'runtime.json');
    await exportSecrets(plan, { file: envFile, kind: 'runtime', env: unreadableEnv, fetcher: neverFetch });
    let tag, executed = false;
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/mock-wrangler.js',
      env: { INFISICAL_CLIENT_ID: 'must-not-reach-wrangler', GH_TOKEN: 'must-not-reach-wrangler' },
      github: async () => ({ sha: plan.sha }),
      cloudflare: async apiPath => {
        if (apiPath.endsWith('/deployments')) return { deployments: executed ? [{ versions: [{ version_id: 'version-id', percentage: 100 }] }] : [] };
        if (apiPath.includes('/versions?')) return { items: apiPath.endsWith('page=1') ? [{ id: 'version-id', metadata: { created_on: '2026-01-01T00:00:00Z' }, annotations: { 'workers/tag': tag } }] : [] };
        if (apiPath.endsWith('/versions/version-id')) return { resources: { bindings: [{ name: 'APP_KV', type: 'kv_namespace' }] } };
        assert.fail(`Unexpected deployment URL: ${apiPath}`);
      },
      execute: (_command, args, options) => {
        executed = true;
        assert.equal(args[1], 'deploy');
        tag = args[args.indexOf('--tag') + 1];
        assert.deepEqual(JSON.parse(readFileSync(args[args.indexOf('--secrets-file') + 1], 'utf8')), {});
        assert.equal(options.env.GH_TOKEN, undefined);
        assert.equal(options.env.INFISICAL_CLIENT_ID, undefined);
      },
    });
    assert.deepEqual(result, { state: 'deployed', version_id: 'version-id' });
  } finally { rmSync(root, { recursive: true, force: true }); }
});


function buildFixture(root, plan) {
  const fixture = source(), appRoot = join(root, 'app'), bundleRoot = join(root, 'bundle'), envFile = join(root, 'build.json');
  mkdirSync(appRoot);
  writeFileSync(join(appRoot, 'package.json'), JSON.stringify(fixture.pkg));
  writeFileSync(join(appRoot, 'package-lock.json'), '{}');
  writeFileSync(join(appRoot, '.env.local'), 'IGNORED=value');
  writeFileSync(join(appRoot, '.dev.vars'), 'IGNORED=value');
  const generatedRoot = join(appRoot, 'dist');
  function emitBundle(overrides = {}) {
    mkdirSync(join(generatedRoot, 'server'), { recursive: true });
    mkdirSync(join(generatedRoot, 'client'));
    writeFileSync(join(generatedRoot, 'server/index.mjs'), 'export default { fetch() { return new Response("ok"); } };');
    writeFileSync(join(generatedRoot, 'client/index.html'), '<!doctype html><title>Public app</title>');
    writeFileSync(join(generatedRoot, 'server/.env.generated'), 'IGNORED=value');
    writeFileSync(join(generatedRoot, 'server/wrangler.json'), JSON.stringify({
      name: plan.project.worker_name, account_id: plan.project.account_id, targetEnvironment: plan.target,
      main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
      workers_dev: true, preview_urls: true, kv_namespaces: fixture.wrangler.kv_namespaces, previews: fixture.wrangler.previews,
      ...overrides,
    }));
  }
  return { appRoot, bundleRoot, envFile, generatedRoot, emitBundle };
}

test('no-environment build consumes an empty export and still installs, checks, builds, validates, and isolates credentials', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-no-infisical-build-'));
  const plan = planFor(), fixture = buildFixture(root, plan), calls = [];
  try {
    await exportSecrets(plan, { file: fixture.envFile, kind: 'build', env: unreadableEnv, fetcher: neverFetch });
    assert.equal(readFileSync(fixture.envFile, 'utf8'), '{}');
    const credentialNames = ['INFISICAL_DOMAIN', 'INFISICAL_CLIENT_ID', 'INFISICAL_CLIENT_SECRET', 'CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_API_KEY', 'GH_TOKEN', 'GITHUB_TOKEN'];
    build(plan, {
      ...fixture,
      env: { ...Object.fromEntries(credentialNames.map(key => [key, 'must-not-reach-build'])), SAFE_VALUE: 'allowed', CI: 'false', CLOUDFLARE_ENV: 'staging' },
      execute: (command, args, options) => {
        calls.push([command, args]);
        assert.equal(options.cwd, fixture.appRoot);
        assert.equal(options.env.CI, 'true');
        assert.equal(options.env.CLOUDFLARE_ENV, 'production');
        assert.equal(options.env.SAFE_VALUE, 'allowed');
        for (const name of credentialNames) assert.equal(options.env[name], undefined, name);
        for (const file of [fixture.envFile, join(fixture.appRoot, '.env.local'), join(fixture.appRoot, '.dev.vars')]) assert.throws(() => statSync(file), { code: 'ENOENT' });
        if (args[0] === 'run' && args[1] === 'build:vinext') fixture.emitBundle();
      },
    });
    assert.deepEqual(calls, [['npm', ['ci']], ['npm', ['run', 'check:deploy']], ['npm', ['run', 'build:vinext']]]);
    assert.equal(readFileSync(join(fixture.bundleRoot, 'server/index.mjs'), 'utf8'), readFileSync(join(fixture.generatedRoot, 'server/index.mjs'), 'utf8'));
    assert.equal(readFileSync(join(fixture.bundleRoot, 'client/index.html'), 'utf8'), '<!doctype html><title>Public app</title>');
    const built = JSON.parse(readFileSync(join(fixture.bundleRoot, 'server/wrangler.json'), 'utf8'));
    assert.equal(built.name, plan.project.worker_name);
    assert.equal(built.account_id, plan.project.account_id);
    assert.equal(built.targetEnvironment, undefined);
    assert.deepEqual(built.secrets, { required: [] });
    assert.deepEqual(built.kv_namespaces, source().wrangler.kv_namespaces);
    assert.deepEqual(built.previews, source().wrangler.previews);
    assert.throws(() => statSync(join(fixture.bundleRoot, 'server/.env.generated')), { code: 'ENOENT' });
    assert.deepEqual(JSON.parse(readFileSync(join(fixture.bundleRoot, 'vinext-release.json'), 'utf8')), { repo: plan.repo, sha: plan.sha, target: plan.target, worker: plan.project.worker_name });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('no-environment builds still reject invalid source locks, generated destinations, and forbidden artifacts', async () => {
  const cases = [
    { name: 'missing frozen lock', message: /Missing frozen dependency lockfile/, expectedCalls: 0, prepare: fixture => rmSync(join(fixture.appRoot, 'package-lock.json')) },
    { name: 'different generated Worker', message: /Built account or Worker differs/, expectedCalls: 3, overrides: { name: 'another-worker' } },
    { name: 'dependencies in bundle', message: /Bundle includes environment files or dependencies/, expectedCalls: 3, afterBuild: fixture => mkdirSync(join(fixture.generatedRoot, 'node_modules')) },
  ];
  for (const scenario of cases) {
    const root = mkdtempSync(join(tmpdir(), 'vinext-no-infisical-build-invalid-'));
    const plan = planFor(), fixture = buildFixture(root, plan);
    let executed = 0;
    try {
      await exportSecrets(plan, { file: fixture.envFile, kind: 'build', env: unreadableEnv, fetcher: neverFetch });
      scenario.prepare?.(fixture);
      assert.throws(() => build(plan, {
        ...fixture, env: {}, execute: (_command, args) => {
          executed++;
          if (args[0] === 'run' && args[1] === 'build:vinext') { fixture.emitBundle(scenario.overrides); scenario.afterBuild?.(fixture); }
        },
      }), scenario.message, scenario.name);
      assert.equal(executed, scenario.expectedCalls, scenario.name);
      assert.throws(() => statSync(fixture.envFile), { code: 'ENOENT' });
      assert.throws(() => statSync(fixture.bundleRoot), { code: 'ENOENT' }, 'an invalid build must not produce a deployment artifact');
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
});

test('configured nonempty slugs require both Universal Auth credentials even when no values are declared', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-infisical-auth-required-'));
  const plan = planFor({ ...payload, project_slug: 'example-web' });
  try {
    for (const credentials of [{}, { INFISICAL_CLIENT_ID: 'mock-id' }, { INFISICAL_CLIENT_SECRET: 'mock-secret' }]) {
      const env = { INFISICAL_DOMAIN: 'https://infisical.example', ...credentials };
      await assert.rejects(() => loadSecrets(plan, { env, fetcher: neverFetch }), /Missing Infisical Universal Auth credentials/);
      const file = join(root, 'build.json');
      await assert.rejects(() => exportSecrets(plan, { file, kind: 'build', env, fetcher: neverFetch }), /Missing Infisical Universal Auth credentials/);
      assert.throws(() => statSync(file), { code: 'ENOENT' }, 'missing credentials must not silently export an empty snapshot');
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});
