import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { prepare, sanitizeConfig, statusPlan, validateDispatch } from './config.mjs';
import { fetchSourceFiles, projectFromSource } from './source.mjs';
import { build, readSecrets, selectValues, validateBundle } from './build.mjs';
import { deploy, deploymentArgs, listVersions } from './deploy.mjs';

const project = {
  infisical_project_slug: 'fixture-web',
  account_id: '1'.repeat(32), worker_name: 'example-web', node_version: '24',
  package_manager: 'pnpm', wrangler_version: '4.131.1', working_directory: '.',
  bundle_directory: 'dist', generated_config: 'dist/server/wrangler.json',
  build_script: 'build:vinext', check_scripts: ['typecheck'], submodule_repos: [],
  build_variables: ['NEXT_PUBLIC_API_URL'], required_build_variables: ['NEXT_PUBLIC_API_URL'],
  runtime_secrets: ['APP_SECRET'],
  environments: {
    staging: { infisical_env: 'staging', infisical_path: '/', bindings: {} },
    production: { infisical_env: 'prod', infisical_path: '/', bindings: {} },
  },
};
const sha = 'a'.repeat(40);
const payload = { repo: 'Lascade-Co/example', branch: 'dev', sha, project_slug: 'fixture-web', source_run_url: 'https://github.com/Lascade-Co/example/actions/runs/123' };
function plan(branch = 'dev', mutate = () => {}) {
  const copy = structuredClone(project);
  mutate(copy);
  return prepare({ ...payload, branch }, copy);
}
function temp(t) {
  const directory = mkdtempSync(join(tmpdir(), 'vinext-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}
function config(p) {
  return { name: p.project.worker_name, account_id: p.project.account_id, targetEnvironment: p.target,
    compatibility_date: '2026-09-15', no_bundle: true, main: 'index.js', assets: { directory: '../client' },
    workers_dev: true, preview_urls: true, ...structuredClone(p.project.environments[p.target].bindings) };
}
function bundle(directory, p, mutate = () => {}) {
  mkdirSync(join(directory, 'server'), { recursive: true });
  mkdirSync(join(directory, 'client'), { recursive: true });
  writeFileSync(join(directory, 'server/index.js'), 'export default {}');
  writeFileSync(join(directory, 'client/index.html'), '<h1>fixture</h1>');
  const c = config(p); mutate(c);
  writeFileSync(join(directory, 'server/wrangler.json'), JSON.stringify(c));
  writeFileSync(join(directory, 'vinext-release.json'), JSON.stringify({ repo: p.repo, sha: p.sha, target: p.target, worker: p.project.worker_name }));
}
function envFile(directory, content = JSON.stringify({ APP_SECRET: 'fixture-runtime-value' })) {
  const path = join(directory, 'export.env'); writeFileSync(path, content); return path;
}

test('final status can be reported from validated dispatch if later Infisical resolution fails', () => {
  assert.deepEqual(statusPlan(payload), { repo: payload.repo, sha, target: 'staging' });
  assert.equal(statusPlan({ ...payload, branch: 'main' }).target, 'production');
  assert.throws(() => statusPlan({ ...payload, branch: 'feature' }));
});

test('source-owned project config selects environments and destinations', () => {
  const staging = plan(), production = plan('main');
  assert.equal(staging.target, 'staging'); assert.equal(staging.infisical_env, 'staging');
  assert.equal(production.target, 'production'); assert.equal(production.infisical_env, 'prod');
  assert.equal(staging.infisical_project_slug, 'fixture-web');
  assert.equal(production.infisical_project_slug, 'fixture-web');
  assert.deepEqual(deploymentArgs(staging, 'config', 'secrets', 'tag').slice(0, 4), ['versions', 'upload', '--preview-alias', 'dev']);
  assert.equal(deploymentArgs(production, 'config', 'secrets', 'tag')[0], 'deploy');
});

test('production-only source config accepts main and rejects dev', () => {
  const onlyProduction = structuredClone(project);
  delete onlyProduction.environments.staging;
  assert.equal(prepare({ ...payload, branch: 'main' }, onlyProduction).target, 'production');
  assert.throws(() => prepare(payload, onlyProduction), /No staging environment/);
});

test('source-owned app-data KV can be retained without a framework cache', () => {
  const p = plan('main', copy => {
    delete copy.environments.staging;
    copy.environments.production.bindings.kv_namespaces = [{ binding: 'CORE_KV', id: '2'.repeat(32) }];
  });
  assert.deepEqual(p.project.environments.production.bindings.kv_namespaces.map(b => b.binding), ['CORE_KV']);
  sanitizeConfig(config(p), p);
});

test('malformed dispatch requests fail before fetching source config', () => {
  for (const change of [{ repo: 'outsider/example' }, { branch: 'feature' }, { sha: 'main' }, { sha: 'A'.repeat(40) }, { source_run_url: 'https://github.com/evil/repo/actions/runs/123' }]) {
    assert.throws(() => validateDispatch({ ...payload, ...change }));
  }
});

test('package and Wrangler are fetched at the pinned source commit', async () => {
  const requests = [];
  const source = await fetchSourceFiles(payload, async path => {
    requests.push(path);
    const raw = Buffer.from(JSON.stringify(path.includes('package.json') ? { packageManager: 'pnpm@10.0.0' } : { name: 'example-web' }));
    return { type: 'file', encoding: 'base64', size: raw.length, content: raw.toString('base64') };
  });
  assert.equal(source.pkg.packageManager, 'pnpm@10.0.0');
  assert.equal(source.wrangler.name, 'example-web');
  assert.deepEqual(requests.sort(), ['package.json', 'wrangler.jsonc'].map(name => `/repos/${payload.repo}/contents/${name}?ref=${sha}`).sort());
  await assert.rejects(fetchSourceFiles(payload, async () => ({ type: 'symlink', encoding: 'base64', size: 2, content: 'e30=' })), /Missing or oversized/);
});

test('source files and Infisical metadata derive a deployable project', () => {
  const pkg = { packageManager: 'pnpm@10.15.0', scripts: { 'check:deploy': 'echo check', 'build:vinext': 'echo build' }, devDependencies: { wrangler: '4.131.1' } };
  const wrangler = { name: 'example-web', account_id: '1'.repeat(32), env: { staging: { name: 'example-web' }, production: { name: 'example-web', kv_namespaces: [{ binding: 'CORE_KV', id: '2'.repeat(32) }] } } };
  const values = { VINEXT_SOURCE_REPOSITORY: payload.repo, VINEXT_WORKER_NAME: 'example-web', VINEXT_ACCOUNT_ID: '1'.repeat(32), VINEXT_SUBMODULE_REPOS: '', VINEXT_RUNTIME_SECRETS: 'APP_SECRET', VINEXT_REQUIRED_BUILD_VARIABLES: 'NEXT_PUBLIC_API_URL', NEXT_PUBLIC_API_URL: 'https://api.example.test', APP_SECRET: 'fixture' };
  const derived = projectFromSource(payload, { pkg, wrangler }, values);
  assert.deepEqual(derived.runtime_secrets, ['APP_SECRET']);
  const withoutSubmodules = { ...values };
  delete withoutSubmodules.VINEXT_SUBMODULE_REPOS;
  assert.deepEqual(projectFromSource(payload, { pkg, wrangler }, withoutSubmodules).submodule_repos, []);
  assert.deepEqual(projectFromSource(payload, { pkg, wrangler }, { ...values, VINEXT_SUBMODULE_REPOS: 'allowed-helper' }).submodule_repos, ['allowed-helper']);
  assert.throws(() => projectFromSource(payload, { pkg, wrangler }, { ...values, VINEXT_SUBMODULE_REPOS: 'other/org' }), /submodule allowlist/);
  assert.deepEqual(derived.build_variables, ['NEXT_PUBLIC_API_URL']);
  assert.equal(derived.environments.production.bindings.kv_namespaces[0].binding, 'CORE_KV');
  assert.equal(prepare(payload, derived).project.worker_name, 'example-web');
  assert.throws(() => projectFromSource(payload, { pkg: { ...pkg, devDependencies: { wrangler: '^4.131.1' } }, wrangler }, values), /exact Wrangler/);
  assert.throws(() => projectFromSource(payload, { pkg, wrangler }, { ...values, VINEXT_RUNTIME_SECRETS: 'MISSING' }), /Missing declared runtime/);
});

test('source-derived production-only Worker deploys main and rejects dev', () => {
  const pkg = { packageManager: 'pnpm@10.15.0', scripts: { 'check:deploy': 'echo check', 'build:vinext': 'echo build' }, devDependencies: { wrangler: '4.131.1' } };
  const wrangler = { name: 'example-web', account_id: '1'.repeat(32), env: { production: { name: 'example-web', kv_namespaces: [{ binding: 'CORE_KV', id: '2'.repeat(32) }] } } };
  const values = { VINEXT_SOURCE_REPOSITORY: payload.repo, VINEXT_WORKER_NAME: 'example-web', VINEXT_ACCOUNT_ID: '1'.repeat(32), VINEXT_SUBMODULE_REPOS: '', VINEXT_RUNTIME_SECRETS: 'APP_SECRET', VINEXT_REQUIRED_BUILD_VARIABLES: 'NEXT_PUBLIC_API_URL', NEXT_PUBLIC_API_URL: 'https://api.example.test', APP_SECRET: 'fixture' };
  const main = { ...payload, branch: 'main' };
  const source = projectFromSource(main, { pkg, wrangler }, values);
  const prepared = prepare(main, source);
  assert.equal(prepared.target, 'production');
  assert.deepEqual(source.environments.production.bindings.kv_namespaces.map(binding => binding.binding), ['CORE_KV']);
  assert.throws(() => projectFromSource(payload, { pkg, wrangler }, values), /no staging environment/);
});

test('source-derived project rejects invalid Infisical metadata and missing required values', () => {
  const pkg = { packageManager: 'pnpm@10.15.0', scripts: { 'check:deploy': 'echo check', 'build:vinext': 'echo build' }, devDependencies: { wrangler: '4.131.1' } };
  const wrangler = { name: 'example-web', account_id: '1'.repeat(32), env: { production: { name: 'example-web' } } };
  const main = { ...payload, branch: 'main' };
  const values = { VINEXT_SOURCE_REPOSITORY: payload.repo, VINEXT_WORKER_NAME: 'example-web', VINEXT_ACCOUNT_ID: '1'.repeat(32), VINEXT_SUBMODULE_REPOS: '', VINEXT_RUNTIME_SECRETS: 'APP_SECRET', VINEXT_REQUIRED_BUILD_VARIABLES: 'NEXT_PUBLIC_API_URL', NEXT_PUBLIC_API_URL: 'https://api.example.test', APP_SECRET: 'fixture' };
  for (const metadata of ['APP_SECRET,APP_SECRET', 'APP_SECRET,,OTHER', 'APP_SECRET;OTHER', 'NEXT_PUBLIC_EXPOSED']) {
    assert.throws(() => projectFromSource(main, { pkg, wrangler }, { ...values, VINEXT_RUNTIME_SECRETS: metadata }));
  }
  for (const required of ['NEXT_PUBLIC_MISSING', 'APP_SECRET', 'NEXT_PUBLIC_API_URL,NEXT_PUBLIC_API_URL']) {
    assert.throws(() => projectFromSource(main, { pkg, wrangler }, { ...values, VINEXT_REQUIRED_BUILD_VARIABLES: required }));
  }
  assert.throws(() => projectFromSource(main, { pkg, wrangler }, { ...values, NEXT_PUBLIC_API_URL: ' ' }), /Missing required public/);
  assert.throws(() => projectFromSource(main, { pkg, wrangler: { ...wrangler, env: { production: { name: 'other-web' } } } }, values), /same Worker and account/);
});

test('Infisical authorization metadata binds the source repository and Worker destination', () => {
  const pkg = { packageManager: 'pnpm@10.15.0', scripts: { 'check:deploy': 'echo check', 'build:vinext': 'echo build' }, devDependencies: { wrangler: '4.131.1' } };
  const wrangler = { name: 'example-web', account_id: '1'.repeat(32), env: { production: { name: 'example-web' } } };
  const values = { VINEXT_SOURCE_REPOSITORY: payload.repo, VINEXT_WORKER_NAME: wrangler.name, VINEXT_ACCOUNT_ID: wrangler.account_id, VINEXT_SUBMODULE_REPOS: '', VINEXT_RUNTIME_SECRETS: '', VINEXT_REQUIRED_BUILD_VARIABLES: '' };
  const main = { ...payload, branch: 'main' };
  assert.equal(projectFromSource(main, { pkg, wrangler }, values).worker_name, wrangler.name);
  for (const [key, invalid] of [['VINEXT_SOURCE_REPOSITORY', 'Lascade-Co/other'], ['VINEXT_WORKER_NAME', 'other-worker'], ['VINEXT_ACCOUNT_ID', '0'.repeat(32)]]) {
    assert.throws(() => projectFromSource(main, { pkg, wrangler }, { ...values, [key]: invalid }), /does not authorize/);
    assert.throws(() => projectFromSource(main, { pkg, wrangler }, { ...values, [key]: undefined }), /does not authorize/);
  }
});

test('caller requires a nonempty valid Infisical slug', () => {
  for (const invalid of ['', null, 42, ' ', ' slug', 'slug ', 'path/slug', 'slug\n']) {
    assert.throws(() => prepare({ ...payload, project_slug: invalid }, structuredClone(project)), /Infisical project slug/);
  }
});

test('source config rejects reserved, duplicate, overlapping variables and escaped paths', () => {
  for (const mutate of [
    p => p.build_variables.push('NODE_OPTIONS'), p => p.build_variables.push('INFISICAL_TOKEN'),
    p => p.build_variables.push('PATH'), p => p.runtime_secrets.push('CLOUDFLARE_API_TOKEN'),
    p => p.runtime_secrets.push('NEXT_PUBLIC_SECRET'), p => p.build_variables.push('APP_SECRET'),
    p => p.runtime_secrets.push('APP_SECRET'), p => p.required_build_variables.push('UNDECLARED'),
    p => p.working_directory = '../outside', p => p.generated_config = '../outside/config.json',
    p => p.generated_config = 'elsewhere/config.json', p => p.bundle_directory = '/tmp/bundle',
    p => {
      const shared = [{ binding: 'CORE_KV', id: '0'.repeat(32) }];
      p.environments.staging.bindings.kv_namespaces = shared;
      p.environments.production.bindings.kv_namespaces = structuredClone(shared);
    },
  ]) assert.throws(() => plan('dev', mutate));
});

test('generated config rejects wrong destination, bindings, target and executable hooks', () => {
  const p = plan();
  for (const mutate of [
    c => c.account_id = '0'.repeat(32), c => c.name = 'different-worker',
    c => c.targetEnvironment = 'production', c => c.kv_namespaces = [{ binding: 'VINEXT_KV_CACHE', id: '0'.repeat(32) }],
    c => c.build = { command: 'echo unsafe' }, c => c.no_bundle = false,
    c => c.vars = { APP_SECRET: 'fixture' },
  ]) { const c = config(p); mutate(c); assert.throws(() => sanitizeConfig(c, p)); }
  const c = config(p); c.routes = ['unexpected.example/*']; c.unused = 'ignored';
  assert.equal(sanitizeConfig(c, p).routes, undefined);
  assert.equal(sanitizeConfig(c, p).unused, undefined);
});

test('Infisical JSON export preserves inner quotes and multiline values and removes source', t => {
  const path = envFile(temp(t), JSON.stringify({ TEXT: "we're ready", MULTILINE: "first\nsecond", EMPTY: "" }));
  assert.deepEqual(readSecrets(path), { TEXT: "we're ready", MULTILINE: 'first\nsecond', EMPTY: '' });
  assert.equal(existsSync(path), false);
});

test('malformed JSON or non-string values fail closed and remove source', t => {
  const directory = temp(t);
  for (const data of ["not-json", "null", "[]", '{"KEY":42}', '{"KEY":null}', '{"KEY":{}}']) {
    const path = envFile(directory, data);
    assert.throws(() => readSecrets(path));
    assert.equal(existsSync(path), false);
  }
});

test('runtime selection requires every secret and omits undeclared values', () => {
  assert.throws(() => selectValues({ FIRST: 'one' }, ['FIRST', 'SECOND']), /Missing Infisical value: SECOND/);
  assert.throws(() => selectValues({ FIRST: '  ' }, ['FIRST']), /Missing Infisical/);
  assert.deepEqual(selectValues({ FIRST: 'one', EXTRA: 'two' }, ['FIRST']), { FIRST: 'one' });
});

test('bundle rejects environment files, symlinks and references outside artifact', t => {
  const root = temp(t), p = plan();
  bundle(root, p); validateBundle(root, p);
  writeFileSync(join(root, 'client/.env.production'), 'SECRET=fixture');
  assert.throws(() => validateBundle(root, p), /environment files/);
  rmSync(join(root, 'client/.env.production'));
  symlinkSync(join(root, 'server/index.js'), join(root, 'client/link.js'));
  assert.throws(() => validateBundle(root, p), /symlinks/);
  rmSync(join(root, 'client/link.js'));
  bundle(root, p, c => c.main = '../../outside.js');
  assert.throws(() => validateBundle(root, p), /escapes/);
});

test('build uses selected public values, removes dotenv overrides, and strips deployment credentials', t => {
  const root = temp(t), p = plan(), output = join(root, 'artifact');
  const values = Object.fromEntries(p.project.required_build_variables.map(name => [name, `fixture-${name}`]));
  const source = envFile(root, JSON.stringify({ ...values, APP_SECRET: 'private-runtime', UNDECLARED: 'omit' }));
  writeFileSync(join(root, 'package.json'), JSON.stringify({ packageManager: 'pnpm@11.27.0', scripts: Object.fromEntries([...p.project.check_scripts, p.project.build_script].map(name => [name, 'echo fixture'])) }));
  writeFileSync(join(root, 'pnpm-lock.yaml'), 'lockfileVersion: 9');
  writeFileSync(join(root, '.env.production'), 'OVERRIDE=bad');
  bundle(join(root, 'dist'), p);
  writeFileSync(join(root, 'dist/server/.dev.vars'), 'GENERATED=fixture');
  writeFileSync(join(root, 'dist/client/.env.production'), 'GENERATED=fixture');
  const calls = [];
  build(p, { appRoot: root, envFile: source, bundleRoot: output,
    env: { PATH: '/fixture-bin', GITHUB_TOKEN: 'private-github', GH_TOKEN: 'private-gh', CLOUDFLARE_API_TOKEN: 'private-cloudflare', INFISICAL_TOKEN: 'private-infisical', APP_SECRET: 'private-runtime' },
    execute: (command, args, options) => { calls.push({ command, args, options }); assert.equal(existsSync(join(root, '.env.production')), false); },
  });
  assert.deepEqual(calls[0].args, ['install', '--frozen-lockfile']);
  assert.deepEqual(calls.at(-1).args, ['run', p.project.build_script]);
  for (const { options } of calls) {
    assert.equal(options.env.CLOUDFLARE_ENV, 'staging');
    assert.equal(options.env.NEXT_PUBLIC_API_URL, values.NEXT_PUBLIC_API_URL);
    for (const name of ['GITHUB_TOKEN', 'GH_TOKEN', 'CLOUDFLARE_API_TOKEN', 'INFISICAL_TOKEN', 'APP_SECRET', 'UNDECLARED']) assert.equal(options.env[name], undefined);
  }
  assert.equal(existsSync(source), false);
  assert.equal(JSON.parse(readFileSync(join(output, 'vinext-release.json'))).sha, sha);
  assert.equal(existsSync(join(output, 'server/.dev.vars')), false);
  assert.equal(existsSync(join(output, 'client/.env.production')), false);
  assert.equal(existsSync(join(root, 'dist/server/.dev.vars')), true);
  assert.equal(JSON.parse(readFileSync(join(output, 'server/wrangler.json'))).no_bundle, true);
  validateBundle(output, p);
});

function deploymentFixture(t, { branch = 'dev', staleAt = 0, extraSecret = false, executeError = false, changedProduction = false, changedAlias = false, badRelease = false, paginatedAlias = false, initialLookupStatus = null } = {}) {
  const directory = temp(t), p = plan(branch), root = join(directory, 'bundle'); bundle(root, p);
  if (badRelease) writeFileSync(join(root, 'vinext-release.json'), JSON.stringify({ repo: p.repo, sha: 'b'.repeat(40), target: p.target, worker: p.project.worker_name }));
  const source = envFile(directory);
  let uploaded = false, githubCalls = 0, cloudflareCalls = 0, secretPath;
  let uploadedVersion;
  const prod = { id: 'prod-version', metadata: { created_on: '2026-01-01T00:00:00Z' }, annotations: {} };
  const preview = { id: 'preview-version', metadata: { created_on: '2026-01-02T00:00:00Z' }, annotations: { 'workers/alias': 'dev' } };
  const before = { id: 'deployment-before', versions: [{ version_id: prod.id, percentage: 100 }] };
  const options = {
    bundleRoot: root, envFile: source, wranglerBin: '/fixture/wrangler/bin.js',
    env: { CLOUDFLARE_API_TOKEN: 'fixture-token', CLOUDFLARE_ENV: 'wrong', GITHUB_TOKEN: 'github-secret', GH_TOKEN: 'gh-secret', INFISICAL_TOKEN: 'infisical-secret', APP_SECRET: 'inherited-secret' },
    github: async () => { githubCalls++; return { sha: staleAt === githubCalls ? 'b'.repeat(40) : sha }; },
    cloudflare: async path => {
      cloudflareCalls++;
      if (path.endsWith('/deployments')) {
        if (!uploaded && initialLookupStatus) throw new Error(`API request failed (${initialLookupStatus}) at ${path}`);
        return { deployments: [uploaded && (branch === 'main' || changedProduction) ? { id: 'deployment-after', versions: [{ version_id: uploadedVersion.id, percentage: 100 }] } : before] };
      }
      if (path.includes('/versions?')) {
        const page = Number(new URL(path, 'https://api.example.test').searchParams.get('page'));
        if (paginatedAlias) {
          return { items: uploaded ? (page === 1 ? [uploadedVersion] : page === 2 ? [preview, prod] : []) : (page === 1 ? [preview] : page === 2 ? [prod] : []) };
        }
        return { items: page === 1 ? [prod, preview, ...(uploaded ? [uploadedVersion] : [])] : [] };
      }
      if (/\/versions\/[^/]+$/.test(path)) return { resources: { bindings: [{ type: 'secret_text', name: extraSecret ? 'UNEXPECTED_SECRET' : 'APP_SECRET' }] } };
      throw new Error(`Unexpected mock API path: ${path}`);
    },
    execute: (command, args, { env }) => {
      assert.equal(command, process.execPath);
      secretPath = args[args.indexOf('--secrets-file') + 1];
      assert.equal(statSync(secretPath).mode & 0o777, 0o600);
      assert.deepEqual(JSON.parse(readFileSync(secretPath)), { APP_SECRET: 'fixture-runtime-value' });
      for (const name of ['CLOUDFLARE_ENV', 'GH_TOKEN', 'GITHUB_TOKEN', 'INFISICAL_TOKEN', 'APP_SECRET']) assert.equal(env[name], undefined);
      assert.equal(env.CLOUDFLARE_API_TOKEN, 'fixture-token');
      assert.equal(env.CLOUDFLARE_ACCOUNT_ID, p.project.account_id);
      if (executeError) throw new Error('mock upload failed');
      uploaded = true;
      uploadedVersion = { id: 'new-version', metadata: { created_on: '2026-01-03T00:00:00Z' }, annotations: { 'workers/tag': args[args.indexOf('--tag') + 1], ...(branch === 'dev' || changedAlias ? { 'workers/alias': 'dev' } : {}) } };
    },
    pause: async () => {},
  };
  return { p, options, state: () => ({ uploaded, githubCalls, cloudflareCalls, secretPath, source }) };
}

for (const branch of ['dev', 'main']) test(`${branch} deploy validates version state, isolates credentials, and removes runtime secret file`, async t => {
  const f = deploymentFixture(t, { branch });
  const result = await deploy(f.p, f.options);
  assert.deepEqual(result, { state: 'deployed', version_id: 'new-version' });
  assert.equal(f.state().githubCalls, 2);
  assert.equal(existsSync(f.state().secretPath), false); assert.equal(existsSync(f.state().source), false);
});

for (const staleAt of [1, 2]) test(`outdated SHA at check ${staleAt} skips upload`, async t => {
  const f = deploymentFixture(t, { staleAt });
  assert.deepEqual(await deploy(f.p, f.options), { state: 'superseded' });
  assert.equal(f.state().uploaded, false);
  if (staleAt === 1) assert.equal(f.state().cloudflareCalls, 0);
});

for (const branch of ['dev', 'main']) test(`${branch} deploy rejects authorization failure before upload`, async t => {
  const f = deploymentFixture(t, { branch, initialLookupStatus: 403 });
  await assert.rejects(deploy(f.p, f.options), /API request failed \(403\)/);
  assert.equal(f.state().uploaded, false);
  assert.equal(f.state().githubCalls, 1);
});

for (const branch of ['dev', 'main']) test(`${branch} deploy rejects a missing Worker before upload`, async t => {
  const f = deploymentFixture(t, { branch, initialLookupStatus: 404 });
  await assert.rejects(deploy(f.p, f.options), /API request failed \(404\)/);
  assert.equal(f.state().uploaded, false);
  assert.equal(f.state().githubCalls, 1);
});

test('upload failure removes restrictive temporary secrets file', async t => {
  const f = deploymentFixture(t, { executeError: true });
  await assert.rejects(deploy(f.p, f.options), /mock upload failed/);
  assert.equal(existsSync(f.state().secretPath), false);
});

test('unexpected inherited secrets prevent upload', async t => {
  const f = deploymentFixture(t, { extraSecret: true });
  await assert.rejects(deploy(f.p, f.options), /Unexpected remote secrets/);
  assert.equal(f.state().uploaded, false);
});

test('artifact identity mismatch prevents any remote interaction', async t => {
  const f = deploymentFixture(t, { badRelease: true });
  await assert.rejects(deploy(f.p, f.options), /Artifact release does not match/);
  assert.equal(f.state().githubCalls, 0); assert.equal(f.state().cloudflareCalls, 0); assert.equal(f.state().uploaded, false);
});

test('missing runtime secret prevents deployment', async t => {
  const f = deploymentFixture(t); writeFileSync(f.options.envFile, JSON.stringify({ OTHER: 'irrelevant' }));
  await assert.rejects(deploy(f.p, f.options), /Missing Infisical value: APP_SECRET/);
  assert.equal(f.state().githubCalls, 0); assert.equal(f.state().uploaded, false);
});

test('staging upload detects changed active production', async t => {
  const f = deploymentFixture(t, { changedProduction: true });
  await assert.rejects(deploy(f.p, f.options), /Staging upload unexpectedly changed/);
  assert.equal(existsSync(f.state().secretPath), false);
});

test('production deploy detects changed preview alias', async t => {
  const f = deploymentFixture(t, { branch: 'main', changedAlias: true });
  await assert.rejects(deploy(f.p, f.options), /Production deploy changed the dev preview alias/);
});

test('deploy revalidates artifact config even after a successful build boundary', async t => {
  const f = deploymentFixture(t);
  const path = join(f.options.bundleRoot, 'server/wrangler.json');
  const c = JSON.parse(readFileSync(path)); c.account_id = '0'.repeat(32);
  writeFileSync(path, JSON.stringify(c));
  await assert.rejects(deploy(f.p, f.options), /Built account or Worker differs/);
  assert.equal(f.state().githubCalls, 0); assert.equal(f.state().uploaded, false);
});

test('version listing follows pages until empty without dropping older preview versions', async () => {
  const calls = [];
  const records = [{ id: 'newest' }, { id: 'older-preview' }, { id: 'oldest' }];
  const result = await listVersions(async path => {
    calls.push(path);
    const query = new URL(path, 'https://api.example.test').searchParams;
    assert.equal(query.get('per_page'), '100');
    const page = Number(query.get('page'));
    return { items: page <= records.length ? [records[page - 1]] : [] };
  }, '/accounts/fixture/workers/scripts/fixture');
  assert.deepEqual(result, records);
  assert.equal(calls.length, 4);
});

test('version listing rejects duplicate IDs across pages rather than looping or hiding inconsistency', async () => {
  let calls = 0;
  await assert.rejects(listVersions(async () => { calls++; return { items: [{ id: 'same-version' }] }; }, '/accounts/fixture/workers/scripts/fixture'), /repeated an ID/i);
  assert.equal(calls, 2);
});

test('production deploy preserves old preview alias when it moves from page one to page two', async t => {
  const f = deploymentFixture(t, { branch: 'main', paginatedAlias: true });
  assert.equal((await deploy(f.p, f.options)).state, 'deployed');
  assert.equal(f.state().uploaded, true);
});
