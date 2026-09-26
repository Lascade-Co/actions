import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { projectFromSource } from './source.mjs';
import { prepare, sanitizeConfig } from './config.mjs';
import { validateBundle } from './build.mjs';

const payload = {
  repo: 'Lascade-Co/example-web', branch: 'main', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
  project_slug: 'example-web',
};
const database = {
  binding: 'CORE_DB', database_name: 'example-web',
  database_id: '11111111-1111-4111-8111-111111111111',
  preview_database_id: '22222222-2222-4222-8222-222222222222',
  migrations_dir: 'migrations',
};

function fixture() {
  const source = {
    pkg: {
      packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.135.0' },
      scripts: { 'check:deploy': 'true', 'build:vinext': 'true' },
    },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32),
      env: { production: { name: 'example-web', d1_databases: [database] } },
    },
    submodule_repos: [],
  };
  const plan = prepare(payload, projectFromSource(payload, source, {}));
  const config = {
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: 'production',
    main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' },
    no_bundle: true, workers_dev: true, preview_urls: true,
    d1_databases: [{ ...database, migrations_dir: '../../migrations' }],
  };
  return { plan, config };
}

test('D1 accepts Vinext migration path rebasing but omits the local path from deployment', () => {
  const { plan, config } = fixture();
  const result = sanitizeConfig(config, plan);
  const { migrations_dir: _directory, ...runtimeDatabase } = database;
  assert.deepEqual(result.d1_databases, [runtimeDatabase]);
  assert.equal(Object.hasOwn(result.d1_databases[0], 'migrations_dir'), false);
  assert.equal(config.d1_databases[0].migrations_dir, '../../migrations');
  assert.equal(plan.project.environments.production.bindings.d1_databases[0].migrations_dir, 'migrations');
  assert.deepEqual(sanitizeConfig(result, plan), result);
});

test('D1 deployment-only bundle passes validation twice without migration files', () => {
  const { plan, config } = fixture();
  const root = mkdtempSync(join(tmpdir(), 'vinext-d1-second-pass-'));
  try {
    mkdirSync(join(root, 'server'));
    mkdirSync(join(root, 'client'));
    writeFileSync(join(root, 'server', 'index.mjs'), 'export default { fetch() { return new Response("ok") } };');
    writeFileSync(join(root, 'vinext-release.json'), JSON.stringify({
      repo: payload.repo, sha: payload.sha, target: 'production', worker: 'example-web',
    }));
    const path = join(root, 'server', 'wrangler.json');
    writeFileSync(path, JSON.stringify(config));
    const first = validateBundle(root, plan).config;
    writeFileSync(path, JSON.stringify(first));
    const second = validateBundle(root, plan).config;
    assert.deepEqual(second, first);
    assert.equal(Object.hasOwn(second.d1_databases[0], 'migrations_dir'), false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('D1 still rejects changes to destination, identity, and unknown binding options', () => {
  const { plan, config } = fixture();
  for (const [key, value] of [
    ['database_id', '33333333-3333-4333-8333-333333333333'],
    ['preview_database_id', '44444444-4444-4444-8444-444444444444'],
    ['database_name', 'another-database'],
    ['binding', 'ANOTHER_DB'],
    ['unexpected_option', true],
  ]) {
    const changed = { ...config, d1_databases: [{ ...config.d1_databases[0], [key]: value }] };
    assert.throws(() => sanitizeConfig(changed, plan), /Unexpected production resource binding: d1_databases/, key);
  }
  assert.throws(() => sanitizeConfig({ ...config, d1_databases: [] }, plan), /Unexpected production resource binding: d1_databases/);
});

test('native Preview D1 omits local migration paths in base and Preview configuration', () => {
  const source = {
    pkg: {
      packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.135.0' },
      scripts: { 'check:deploy': 'true', 'build:vinext': 'true' },
    },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32),
      d1_databases: [database],
      previews: { d1_databases: [{ ...database, database_name: 'example-web-preview' }] },
      env: {
        staging: { name: 'example-web', d1_databases: [database] },
        production: { name: 'example-web', d1_databases: [database] },
      },
    },
    submodule_repos: [],
  };
  const plan = prepare(payload, projectFromSource(payload, source, {}));
  const { config } = fixture();
  config.previews = { d1_databases: [{ ...source.wrangler.previews.d1_databases[0], migrations_dir: '../../migrations' }] };
  const first = sanitizeConfig(config, plan);
  assert.equal(Object.hasOwn(first.d1_databases[0], 'migrations_dir'), false);
  assert.equal(Object.hasOwn(first.previews.d1_databases[0], 'migrations_dir'), false);
  assert.equal(first.previews.d1_databases[0].database_name, 'example-web-preview');
  assert.deepEqual(sanitizeConfig(first, plan), first);
  for (const [key, value] of [
    ['database_id', '33333333-3333-4333-8333-333333333333'],
    ['preview_database_id', '44444444-4444-4444-8444-444444444444'],
    ['database_name', 'another-preview-database'],
    ['binding', 'ANOTHER_DB'],
    ['unexpected_option', true],
  ]) {
    const changed = {
      ...first,
      previews: { d1_databases: [{ ...first.previews.d1_databases[0], [key]: value }] },
    };
    assert.throws(() => sanitizeConfig(changed, plan), /Generated Preview configuration differs from source/, key);
  }
});
