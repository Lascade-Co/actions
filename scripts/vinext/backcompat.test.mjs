import test from 'node:test';
import assert from 'node:assert/strict';
import { prepare, sanitizeConfig } from './config.mjs';
import { projectFromSource } from './source.mjs';

const account = 'b'.repeat(32);
const privateApi = { binding: 'PRIVATE_API', service_id: '11111111-1111-4111-8111-111111111111' };

function bindings(target, native) {
  return {
    d1_databases: [{ binding: 'DB', database_name: `example-${target}`, database_id: target === 'production' ? '11111111-1111-4111-8111-111111111111' : '22222222-2222-4222-8222-222222222222', migrations_dir: 'migrations' }],
    kv_namespaces: [{ binding: 'DATA', id: target === 'production' ? '1'.repeat(32) : '2'.repeat(32) }],
    images: { binding: 'IMAGES' },
    ...(native && target === 'staging' ? { vpc_services: [privateApi] } : {}),
  };
}

function deploymentBindings(value) {
  const copy = structuredClone(value);
  for (const database of copy.d1_databases) delete database.migrations_dir;
  return copy;
}

for (const native of [false, true]) for (const branch of ['main', 'dev']) {
  test(`${native ? 'native 4.135' : 'legacy 4.69'} ${branch} callers ignore generated empty cache defaults without changing existing bindings`, () => {
    const target = branch === 'main' ? 'production' : 'staging';
    const production = bindings('production', native), staging = bindings('staging', native);
    const source = {
      pkg: {
        packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: native ? '4.135.0' : '4.69.0' },
        scripts: { 'check:deploy': 'true', 'build:vinext': 'true' },
      },
      wrangler: {
        name: 'example-web', account_id: account,
        ...(native ? { ...production, previews: staging } : {}),
        env: { production: { name: 'example-web', ...production }, staging: { name: 'example-web', ...staging } },
      },
      submodule_repos: [],
    };
    const dispatch = {
      repo: 'Lascade-Co/example-web', branch, sha: 'a'.repeat(40), project_slug: 'example-web',
      source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
    };
    const project = projectFromSource(dispatch, source, {
      NEXT_PUBLIC_API_URL: 'https://example.invalid', APP_SECRET: 'fixture-only', VINEXT_METADATA: 'fixture-only', EMPTY: ' ',
    });
    const plan = prepare(dispatch, project);
    assert.equal(project.preview_mode, native ? 'native' : 'alias');
    assert.deepEqual(project.build_variables, ['NEXT_PUBLIC_API_URL']);
    assert.deepEqual(project.runtime_secrets, ['APP_SECRET', 'VINEXT_METADATA']);
    const generated = {
      name: 'example-web', account_id: account, targetEnvironment: target,
      main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' },
      no_bundle: true, workers_dev: true, preview_urls: true,
      ...structuredClone(target === 'production' ? production : staging),
      ...(native ? { previews: structuredClone(staging) } : {}),
      exports: {}, ratelimits: [],
    };
    const unchanged = structuredClone({ source, generated });
    const result = sanitizeConfig(generated, plan);
    for (const key of ['cache', 'exports', 'ratelimits']) assert.equal(Object.hasOwn(result, key), false, key);
    const expected = deploymentBindings(native ? production : target === 'production' ? production : staging);
    for (const key of ['d1_databases', 'kv_namespaces', 'images', 'vpc_services']) assert.deepEqual(result[key], expected[key], key);
    if (native) assert.deepEqual(result.previews, deploymentBindings(staging));
    else assert.equal(Object.hasOwn(result, 'previews'), false);
    assert.deepEqual(result.secrets, { required: ['APP_SECRET', 'VINEXT_METADATA'] });
    assert.equal(Object.hasOwn(result, 'vars'), false);
    assert.deepEqual(sanitizeConfig(result, plan), result);
    assert.deepEqual({ source, generated }, unchanged);
  });
}
