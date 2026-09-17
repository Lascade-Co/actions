import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { validatePayload, validateConfig, validateBasePath, verifyBaseDirectory, verifyStaticDirectory } from './control.mjs';

const execFileAsync = promisify(execFile);
const controlPath = fileURLToPath(new URL('./control.mjs', import.meta.url));
const sha = 'a'.repeat(40);
const payload = { repo: 'Lascade-Co/store-reviews', branch: 'main', sha, base_path: 'dashboard' };
const config = {
  name: 'store-reviews-dashboard', account_id: 'c'.repeat(32),
  pages_build_output_dir: './dist', compatibility_date: '2026-09-17',
};

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'pages-control-test-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  return directory;
}

async function staticFixture(t) {
  const directory = await fixture(t);
  await writeFile(join(directory, 'index.html'), '<!doctype html><div id="root"></div>');
  return directory;
}

test('only an organization repository, main branch, and exact lowercase SHA are accepted', () => {
  const plan = validatePayload(payload);
  assert.equal(plan.sha, sha);
  assert.equal(plan.repo, payload.repo);
  assert.equal(plan.base_path, 'dashboard');
  const other = validatePayload({ ...payload, repo: 'Lascade-Co/another-site', base_path: '.' });
  assert.equal(other.repo, 'Lascade-Co/another-site');
  for (const candidate of [
    null, undefined, {}, [], 'payload',
    { ...payload, repo: 'attacker/store-reviews' },
    { ...payload, repo: 'Lascade-Co/.' },
    { ...payload, repo: 'Lascade-Co/..' },
    { ...payload, repo: 'Lascade-Co/store-reviews\n' },
    { ...payload, branch: 'refs/heads/main' },
    { ...payload, branch: 'main; echo injected' },
    { ...payload, branch: 'preview' },
    { ...payload, sha: 'A'.repeat(40) },
    { ...payload, sha: 'a'.repeat(39) },
    { ...payload, sha: 'a'.repeat(41) },
    { ...payload, sha: 'g'.repeat(40) },
    { ...payload, sha: `${sha}\n` },
    { ...payload, sha: `${sha}\nmalicious` },
    { ...payload, sha: 123 },
    { ...payload, sha: null },
  ]) assert.throws(() => validatePayload(candidate));
});

test('source configuration determines the Pages destination', () => {
  const plan = validateConfig(config);
  assert.equal(plan.account_id, config.account_id);
  assert.equal(plan.project_name, config.name);
  assert.match(plan.target_key, /^pages-[a-z0-9-]+$/);
  assert.equal(validateConfig(config).target_key, plan.target_key);
  assert.notEqual(validateConfig({ ...config, name: 'another-site' }).target_key, plan.target_key);
  assert.notEqual(validateConfig({ ...config, account_id: 'd'.repeat(32) }).target_key, plan.target_key);
  assert.deepEqual(validateConfig({ ...config, $schema: 'node_modules/wrangler/config-schema.json' }), plan);
});

test('invalid targets, output paths, dates, and runtime bindings fail configuration validation', () => {
  for (const candidate of [null, {}, [],
    { ...config, name: '-bad-project' },
    { ...config, name: 'Bad-Project' },
    { ...config, name: 'project\n' },
    { ...config, account_id: 'c'.repeat(31) },
    { ...config, account_id: `${config.account_id}\n` },
    { ...config, pages_build_output_dir: '../dist' },
    { ...config, pages_build_output_dir: '/dist' },
    { ...config, pages_build_output_dir: './dist\n' },
    { ...config, compatibility_date: 'invalid' },
    { ...config, compatibility_date: '2026-02-30' },
    { ...config, compatibility_date: '2026-13-01' },
    { ...config, compatibility_date: '2026-09-17\n' },
    { ...config, main: 'worker.js' },
    { ...config, vars: { SECRET: 'secret' } },
    { ...config, r2_buckets: [] },
  ]) assert.throws(() => validateConfig(candidate));
});

test('base paths accept root and relative directories but reject traversal and shell syntax', () => {
  for (const path of ['.', 'dashboard', 'apps/customer-dashboard', 'site_v2.1']) {
    assert.equal(validateBasePath(path), path);
  }
  for (const path of ['', '..', '../dashboard', 'dashboard/..', './dashboard', '/dashboard',
    'dashboard/', 'apps//dashboard', 'apps/./dashboard', 'apps\\dashboard', 'dashboard\n',
    'dashboard;echo injected', '$(whoami)', null, 42]) {
    assert.throws(() => validateBasePath(path));
  }
  const { base_path, ...withoutBase } = payload;
  assert.equal(validatePayload(withoutBase).base_path, '.');
});

test('optional Infisical settings have safe defaults and reject unsafe values', () => {
  const defaults = validatePayload(payload);
  assert.equal(defaults.project_slug, '');
  assert.equal(defaults.infisical_path, '/');
  const configured = validatePayload({ ...payload, project_slug: 'store-reviews-ab12', infisical_path: '/dashboard/build' });
  assert.equal(configured.project_slug, 'store-reviews-ab12');
  assert.equal(configured.infisical_path, '/dashboard/build');
  for (const project_slug of ['project\n', '$(whoami)', 'project;echo bad', '../project']) {
    assert.throws(() => validatePayload({ ...payload, project_slug }));
  }
  for (const infisical_path of ['relative', '/../outside', '/a/../b', '/a\\b', '/a\n', '/$(whoami)']) {
    assert.throws(() => validatePayload({ ...payload, project_slug: 'store-reviews-ab12', infisical_path }));
  }
});

test('base directories must exist as real directories without symlink components', async t => {
  const directory = await fixture(t);
  await mkdir(join(directory, 'apps', 'dashboard'), { recursive: true });
  await verifyBaseDirectory(directory, '.');
  await verifyBaseDirectory(directory, 'apps/dashboard');
  await writeFile(join(directory, 'file'), 'not a directory');
  await assert.rejects(verifyBaseDirectory(directory, 'file'));
  await assert.rejects(verifyBaseDirectory(directory, 'missing'));
  await assert.rejects(verifyBaseDirectory(directory, '../outside'));
  await symlink(join(directory, 'apps'), join(directory, 'linked'));
  await assert.rejects(verifyBaseDirectory(directory, 'linked/dashboard'));
  await symlink(join(directory, 'apps', 'dashboard'), join(directory, 'dashboard-link'));
  await assert.rejects(verifyBaseDirectory(directory, 'dashboard-link'));
});

test('nested static assets and Pages configuration files are accepted', async t => {
  const directory = await staticFixture(t);
  await mkdir(join(directory, 'assets', 'fonts'), { recursive: true });
  await writeFile(join(directory, 'assets', 'app.js'), 'console.log("static app");');
  await writeFile(join(directory, 'assets', 'fonts', 'font.woff2'), new Uint8Array([1, 2, 3]));
  await writeFile(join(directory, '_headers'), '/*\n  X-Content-Type-Options: nosniff\n');
  await writeFile(join(directory, '_redirects'), '/old /new 301\n');
  await verifyStaticDirectory(directory);
});

test('static output requires a directory and a nonempty regular index.html', async t => {
  const directory = await fixture(t);
  await assert.rejects(verifyStaticDirectory(directory), /ENOENT/);
  const index = join(directory, 'index.html');
  await writeFile(index, '');
  await assert.rejects(verifyStaticDirectory(directory), /Missing nonempty index.html/);
  await assert.rejects(verifyStaticDirectory(index), /Expected a static output directory/);
  await rm(index);
  await mkdir(index);
  await assert.rejects(verifyStaticDirectory(directory), /Missing nonempty index.html/);
});

test('symlinked output, index, and nested assets are rejected', async t => {
  const directory = await staticFixture(t);
  const links = await fixture(t);
  await symlink(directory, join(links, 'output'));
  await assert.rejects(verifyStaticDirectory(join(links, 'output')), /Expected a static output directory/);
  await symlink(join(directory, 'index.html'), join(links, 'index.html'));
  await assert.rejects(verifyStaticDirectory(links), /Missing nonempty index.html/);
  await mkdir(join(directory, 'assets'));
  await symlink(join(directory, 'index.html'), join(directory, 'assets', 'linked.html'));
  await assert.rejects(verifyStaticDirectory(directory), /Symlinks are forbidden/);
});

test('runtime code, dependencies, repository metadata, and environment secrets are rejected recursively', async t => {
  for (const name of ['_worker.js', 'functions', 'node_modules', '.git', '.env', '.env.production', 'wrangler.json', 'wrangler.jsonc', 'wrangler.toml']) {
    await t.test(name, async t => {
      const directory = await staticFixture(t);
      const nested = join(directory, 'assets');
      await mkdir(nested);
      const forbidden = join(nested, name);
      if (['functions', 'node_modules', '.git'].includes(name)) await mkdir(forbidden);
      else await writeFile(forbidden, 'forbidden artifact');
      await assert.rejects(verifyStaticDirectory(directory), /Runtime or secret artifact is forbidden/);
    });
  }
});

// The preload replaces fetch before the CLI starts; no test can contact GitHub.
const fetchMock = `
  import assert from 'node:assert/strict';
  globalThis.fetch = async (url, options) => {
    assert.equal(options.headers.Authorization, 'Bearer test-token');
    const fixture = JSON.parse(process.env.TEST_RESPONSE);
    if (fixture.forbidFetch) throw new Error('Unexpected fetch');
    if (url.includes('/contents/')) {
      const source = JSON.parse(process.env.PAYLOAD_JSON);
      const prefix = source.base_path === '.' || !source.base_path ? '' : source.base_path + '/';
      assert.equal(url, 'https://api.github.com/repos/' + source.repo + '/contents/' + prefix + 'wrangler.json?ref=' + source.sha);
      assert.equal(options.headers.Accept, 'application/vnd.github.raw+json');
      const status = fixture.configStatus ?? 200;
      return { ok: status === 200, status, json: async () => JSON.parse(process.env.TEST_CONFIG) };
    }
    assert.equal(url, 'https://api.github.com/repos/Lascade-Co/store-reviews/git/ref/heads/main');
    assert.equal(options.headers.Accept, 'application/vnd.github+json');
    return { ok: fixture.status === 200, status: fixture.status, json: async () => fixture.body };
  };
`;

async function runHead(t, response, source = payload) {
  const directory = await fixture(t);
  const outputPath = join(directory, 'github-output');
  let result;
  try {
    result = { code: 0, ...await execFileAsync(process.execPath, [
      '--import', `data:text/javascript,${encodeURIComponent(fetchMock)}`, controlPath, 'head',
    ], {
      timeout: 10000,
      env: { ...process.env, GH_TOKEN: 'test-token', GITHUB_OUTPUT: outputPath,
        PAYLOAD_JSON: JSON.stringify(source), TEST_RESPONSE: JSON.stringify(response),
        TEST_CONFIG: JSON.stringify(response.config ?? config) },
    }) };
  } catch (error) {
    result = { code: error.code, stdout: error.stdout, stderr: error.stderr };
  }
  result.output = await readFile(outputPath, 'utf8').catch(error => {
    if (error.code === 'ENOENT') return '';
    throw error;
  });
  return result;
}

test('current main HEAD enables deployment', async t => {
  const result = await runHead(t, { status: 200, body: { object: { sha } } });
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.output, new RegExp(`^sha=${sha}$`, 'm'));
  assert.match(result.output, /^current=true$/m);
  assert.match(result.output, /^base_path=dashboard$/m);
  assert.match(result.output, /^project_name=store-reviews-dashboard$/m);
  assert.match(result.stdout, /Deploying current main commit/);
});

test('superseded commits skip deployment successfully', async t => {
  const result = await runHead(t, { status: 200, body: { object: { sha: 'b'.repeat(40) } } });
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.output, new RegExp(`^sha=${sha}$`, 'm'));
  assert.match(result.output, /^current=false$/m);
  assert.match(result.stdout, /Skipping superseded commit/);
});

test('root base path loads wrangler.json from the exact source commit', async t => {
  const result = await runHead(t, { status: 200, body: { object: { sha } } }, { ...payload, base_path: '.' });
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.output, /^base_path=\.$/m);
});

test('missing or invalid source configuration fails before approving deployment', async t => {
  for (const response of [
    { configStatus: 404 },
    { config: { ...config, main: 'worker.js' } },
  ]) {
    const result = await runHead(t, response);
    assert.equal(result.code, 1);
    assert.doesNotMatch(result.output, /current=true/);
  }
});

test('HEAD lookup errors and malformed responses fail closed', async t => {
  for (const response of [
    { status: 403, body: {} },
    { status: 404, body: {} },
    { status: 200, body: {} },
    { status: 200, body: { object: { sha: 'main' } } },
    { status: 200, body: { object: { sha: `${sha}\n` } } },
  ]) {
    const result = await runHead(t, response);
    assert.equal(result.code, 1);
    assert.doesNotMatch(result.output, /current=/);
    assert.match(result.stderr, /Unable to verify main HEAD|Invalid main HEAD response/);
  }
});

test('invalid dispatch fails before requesting HEAD or writing outputs', async t => {
  const result = await runHead(t, { forbidFetch: true }, { ...payload, repo: 'attacker/repository' });
  assert.equal(result.code, 1);
  assert.equal(result.output, '');
  assert.match(result.stderr, /Lascade-Co|repository|Repository/);
  assert.doesNotMatch(result.stderr, /Unexpected fetch/);
});
