import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { prepare, sanitizeConfig } from './config.mjs';
import { build, readSecrets, selectValues, validateBundle } from './build.mjs';
import { deploy, deploymentArgs, listVersions, smoke } from './deploy.mjs';

const registry = JSON.parse(readFileSync(new URL('../../data/vinext-projects.json', import.meta.url), 'utf8'));
const sha = 'a'.repeat(40);
const payload = { repo: 'Lascade-Co/roamjoy', branch: 'dev', sha, project_slug: 'roam-joy-web', source_run_url: 'https://github.com/Lascade-Co/roamjoy/actions/runs/123' };
function plan(branch = 'dev', mutate = () => {}) {
  const copy = structuredClone(registry);
  mutate(copy[payload.repo]);
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
function envFile(directory, content = JSON.stringify({ LAABHAM_SECRET_KEY: 'fixture-runtime-value' })) {
  const path = join(directory, 'export.env'); writeFileSync(path, content); return path;
}

test('registered branches select separate environment, bindings, and destinations', () => {
  const staging = plan(), production = plan('main');
  assert.equal(staging.target, 'staging'); assert.equal(staging.infisical_env, 'staging');
  assert.equal(production.target, 'production'); assert.equal(production.infisical_env, 'prod');
  assert.equal(staging.infisical_project_slug, 'roam-joy-web');
  assert.equal(production.infisical_project_slug, 'roam-joy-web');
  assert.notEqual(staging.project.environments.staging.bindings.kv_namespaces[0].id, production.project.environments.production.bindings.kv_namespaces[0].id);
  assert.deepEqual(deploymentArgs(staging, 'config', 'secrets', 'tag').slice(0, 4), ['versions', 'upload', '--preview-alias', 'dev']);
  assert.equal(deploymentArgs(production, 'config', 'secrets', 'tag')[0], 'deploy');
});

test('unregistered or malformed dispatch requests fail closed', () => {
  for (const change of [{ repo: 'Lascade-Co/unknown' }, { repo: 'outsider/roamjoy' }, { branch: 'feature' }, { sha: 'main' }, { sha: 'A'.repeat(40) }, { source_run_url: 'https://github.com/evil/repo/actions/runs/123' }]) {
    assert.throws(() => prepare({ ...payload, ...change }, registry));
  }
});

test('caller must supply a nonempty valid project slug', () => {
  const { project_slug, ...withoutSlug } = payload;
  assert.throws(() => prepare(withoutSlug, registry), /project_slug/);
  for (const invalid of ['', null, 42, ' ', ' slug', 'slug ', 'path/slug', 'slug\n', 'slug\nnext']) {
    assert.throws(() => prepare({ ...payload, project_slug: invalid }, registry), /project_slug/);
  }
});

test('caller project slug wins over legacy registry values and cannot be omitted', () => {
  const legacy = structuredClone(registry);
  legacy[payload.repo].infisical_project_slug = 'old-central-project';
  legacy[payload.repo].infisical_project_variable = 'OLD_CENTRAL_VARIABLE';
  assert.equal(prepare(payload, legacy).infisical_project_slug, 'roam-joy-web');
  const { project_slug, ...withoutSlug } = payload;
  assert.throws(() => prepare(withoutSlug, legacy), /project_slug/);
});

test('registry rejects reserved, duplicate, overlapping variables and escaped paths', () => {
  for (const mutate of [
    p => p.build_variables.push('NODE_OPTIONS'), p => p.build_variables.push('INFISICAL_TOKEN'),
    p => p.build_variables.push('PATH'), p => p.runtime_secrets.push('CLOUDFLARE_API_TOKEN'),
    p => p.runtime_secrets.push('NEXT_PUBLIC_SECRET'), p => p.build_variables.push('LAABHAM_SECRET_KEY'),
    p => p.runtime_secrets.push('LAABHAM_SECRET_KEY'), p => p.required_build_variables.push('UNDECLARED'),
    p => p.working_directory = '../outside', p => p.generated_config = '../outside/config.json',
    p => p.generated_config = 'elsewhere/config.json', p => p.bundle_directory = '/tmp/bundle',
    p => p.environments.staging.bindings.kv_namespaces = structuredClone(p.environments.production.bindings.kv_namespaces),
  ]) assert.throws(() => plan('dev', mutate));
});

test('generated config rejects wrong destination, cache, target and executable hooks', () => {
  const p = plan();
  for (const mutate of [
    c => c.account_id = '0'.repeat(32), c => c.name = 'different-worker',
    c => c.targetEnvironment = 'production', c => c.kv_namespaces[0].id = '0'.repeat(32),
    c => c.build = { command: 'echo unsafe' }, c => c.no_bundle = false,
    c => c.vars = { LAABHAM_SECRET_KEY: 'fixture' },
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
  const source = envFile(root, JSON.stringify({ ...values, LAABHAM_SECRET_KEY: 'private-runtime', UNDECLARED: 'omit' }));
  writeFileSync(join(root, 'package.json'), JSON.stringify({ packageManager: 'pnpm@11.27.0', scripts: Object.fromEntries([...p.project.check_scripts, p.project.build_script].map(name => [name, 'echo fixture'])) }));
  writeFileSync(join(root, 'pnpm-lock.yaml'), 'lockfileVersion: 9');
  writeFileSync(join(root, '.env.production'), 'OVERRIDE=bad');
  bundle(join(root, 'dist'), p);
  writeFileSync(join(root, 'dist/server/.dev.vars'), 'GENERATED=fixture');
  writeFileSync(join(root, 'dist/client/.env.production'), 'GENERATED=fixture');
  const calls = [];
  build(p, { appRoot: root, envFile: source, bundleRoot: output,
    env: { PATH: '/fixture-bin', GITHUB_TOKEN: 'private-github', GH_TOKEN: 'private-gh', CLOUDFLARE_API_TOKEN: 'private-cloudflare', INFISICAL_TOKEN: 'private-infisical', LAABHAM_SECRET_KEY: 'private-runtime' },
    execute: (command, args, options) => { calls.push({ command, args, options }); assert.equal(existsSync(join(root, '.env.production')), false); },
  });
  assert.deepEqual(calls[0].args, ['install', '--frozen-lockfile']);
  assert.deepEqual(calls.at(-1).args, ['run', p.project.build_script]);
  for (const { options } of calls) {
    assert.equal(options.env.CLOUDFLARE_ENV, 'staging');
    assert.equal(options.env.NEXT_PUBLIC_API_URL, values.NEXT_PUBLIC_API_URL);
    for (const name of ['GITHUB_TOKEN', 'GH_TOKEN', 'CLOUDFLARE_API_TOKEN', 'INFISICAL_TOKEN', 'LAABHAM_SECRET_KEY', 'UNDECLARED']) assert.equal(options.env[name], undefined);
  }
  assert.equal(existsSync(source), false);
  assert.equal(JSON.parse(readFileSync(join(output, 'vinext-release.json'))).sha, sha);
  assert.equal(existsSync(join(output, 'server/.dev.vars')), false);
  assert.equal(existsSync(join(output, 'client/.env.production')), false);
  assert.equal(existsSync(join(root, 'dist/server/.dev.vars')), true);
  assert.equal(JSON.parse(readFileSync(join(output, 'server/wrangler.json'))).no_bundle, true);
  validateBundle(output, p);
});

function deploymentFixture(t, { branch = 'dev', staleAt = 0, extraSecret = false, executeError = false, changedProduction = false, changedAlias = false, badRelease = false, paginatedAlias = false } = {}) {
  const directory = temp(t), p = plan(branch), root = join(directory, 'bundle'); bundle(root, p);
  if (badRelease) writeFileSync(join(root, 'vinext-release.json'), JSON.stringify({ repo: p.repo, sha: 'b'.repeat(40), target: p.target, worker: p.project.worker_name }));
  const source = envFile(directory);
  let uploaded = false, githubCalls = 0, cloudflareCalls = 0, smokeCalls = 0, secretPath;
  let uploadedVersion;
  const prod = { id: 'prod-version', metadata: { created_on: '2026-01-01T00:00:00Z' }, annotations: {} };
  const preview = { id: 'preview-version', metadata: { created_on: '2026-01-02T00:00:00Z' }, annotations: { 'workers/alias': 'dev' } };
  const before = { id: 'deployment-before', versions: [{ version_id: prod.id, percentage: 100 }] };
  const options = {
    bundleRoot: root, envFile: source, wranglerBin: '/fixture/wrangler/bin.js',
    env: { CLOUDFLARE_API_TOKEN: 'fixture-token', CLOUDFLARE_ENV: 'wrong', GITHUB_TOKEN: 'github-secret', GH_TOKEN: 'gh-secret', INFISICAL_TOKEN: 'infisical-secret', LAABHAM_SECRET_KEY: 'inherited-secret' },
    github: async () => { githubCalls++; return { sha: staleAt === githubCalls ? 'b'.repeat(40) : sha }; },
    cloudflare: async path => {
      cloudflareCalls++;
      if (path.endsWith('/deployments')) return { deployments: [uploaded && (branch === 'main' || changedProduction) ? { id: 'deployment-after', versions: [{ version_id: uploadedVersion.id, percentage: 100 }] } : before] };
      if (path.includes('/versions?')) {
        const page = Number(new URL(path, 'https://api.example.test').searchParams.get('page'));
        if (paginatedAlias) {
          return { items: uploaded ? (page === 1 ? [uploadedVersion] : page === 2 ? [preview, prod] : []) : (page === 1 ? [preview] : page === 2 ? [prod] : []) };
        }
        return { items: page === 1 ? [prod, preview, ...(uploaded ? [uploadedVersion] : [])] : [] };
      }
      if (/\/versions\/[^/]+$/.test(path)) return { resources: { bindings: [{ type: 'secret_text', name: extraSecret ? 'UNEXPECTED_SECRET' : 'LAABHAM_SECRET_KEY' }] } };
      throw new Error(`Unexpected mock API path: ${path}`);
    },
    execute: (command, args, { env }) => {
      assert.equal(command, process.execPath);
      secretPath = args[args.indexOf('--secrets-file') + 1];
      assert.equal(statSync(secretPath).mode & 0o777, 0o600);
      assert.deepEqual(JSON.parse(readFileSync(secretPath)), { LAABHAM_SECRET_KEY: 'fixture-runtime-value' });
      for (const name of ['CLOUDFLARE_ENV', 'GH_TOKEN', 'GITHUB_TOKEN', 'INFISICAL_TOKEN', 'LAABHAM_SECRET_KEY']) assert.equal(env[name], undefined);
      assert.equal(env.CLOUDFLARE_API_TOKEN, 'fixture-token');
      assert.equal(env.CLOUDFLARE_ACCOUNT_ID, p.project.account_id);
      if (executeError) throw new Error('mock upload failed');
      uploaded = true;
      uploadedVersion = { id: 'new-version', metadata: { created_on: '2026-01-03T00:00:00Z' }, annotations: { 'workers/tag': args[args.indexOf('--tag') + 1], ...(branch === 'dev' || changedAlias ? { 'workers/alias': 'dev' } : {}) } };
    },
    smokeCheck: async () => { smokeCalls++; }, pause: async () => {},
  };
  return { p, options, state: () => ({ uploaded, githubCalls, cloudflareCalls, smokeCalls, secretPath, source }) };
}

for (const branch of ['dev', 'main']) test(`${branch} deploy validates version state, isolates credentials, and removes runtime secret file`, async t => {
  const f = deploymentFixture(t, { branch });
  const result = await deploy(f.p, f.options);
  assert.deepEqual(result, { state: 'deployed', version_id: 'new-version', deployment_url: f.p.smoke_url });
  assert.equal(f.state().githubCalls, 2); assert.equal(f.state().smokeCalls, 1);
  assert.equal(existsSync(f.state().secretPath), false); assert.equal(existsSync(f.state().source), false);
});

for (const staleAt of [1, 2]) test(`outdated SHA at check ${staleAt} skips upload`, async t => {
  const f = deploymentFixture(t, { staleAt });
  assert.deepEqual(await deploy(f.p, f.options), { state: 'superseded' });
  assert.equal(f.state().uploaded, false); assert.equal(f.state().smokeCalls, 0);
  if (staleAt === 1) assert.equal(f.state().cloudflareCalls, 0);
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
  await assert.rejects(deploy(f.p, f.options), /Missing Infisical value: LAABHAM_SECRET_KEY/);
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

test('smoke checks reject failed HTTP and wrong indexing policy for both targets', async () => {
  await assert.rejects(smoke(plan(), async () => new Response('', { status: 503 })), /HTTP 503/);
  await assert.rejects(smoke(plan(), async () => new Response('')), /wrong indexing policy/);
  await assert.rejects(smoke(plan('main'), async () => new Response('', { headers: { 'x-robots-tag': 'noindex' } })), /wrong indexing policy/);
  await smoke(plan(), async () => new Response('', { headers: { 'x-robots-tag': 'noindex, nofollow' } }));
  await smoke(plan('main'), async () => new Response(''));
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
  assert.equal(f.state().smokeCalls, 1);
});
