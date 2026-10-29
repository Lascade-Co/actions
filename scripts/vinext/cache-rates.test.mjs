import test from 'node:test';
import assert from 'node:assert/strict';
import { prepare, sanitizeConfig } from './config.mjs';
import { projectFromSource } from './source.mjs';

const payload = {
  repo: 'Lascade-Co/example-web', branch: 'dev', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1', project_slug: 'example-web',
};
const productionRate = { name: 'REQUEST_LIMIT', namespace_id: '1001', simple: { limit: 60, period: 60 } };
const stagingRate = { name: 'REQUEST_LIMIT', namespace_id: '1002', simple: { limit: 30, period: 60 } };
const workerExports = { default: { type: 'worker', cache: { enabled: false } }, PublicContent: { type: 'worker', cache: { enabled: true } } };
function fixture(branch = 'dev') {
  const source = {
    pkg: { packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.154.0' }, scripts: { 'check:deploy': 'true', 'build:vinext': 'true' } },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32), cache: { enabled: false }, exports: structuredClone(workerExports), ratelimits: [structuredClone(productionRate)],
      previews: { cache: { enabled: true }, ratelimits: [structuredClone(stagingRate)] },
      env: {
        staging: { name: 'example-web', cache: { enabled: true }, ratelimits: [structuredClone(stagingRate)] },
        production: { name: 'example-web', ratelimits: [structuredClone(productionRate)] },
      },
    }, submodule_repos: [],
  };
  const dispatch = { ...payload, branch };
  const project = projectFromSource(dispatch, source, {});
  const plan = prepare(dispatch, project);
  const config = {
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: plan.target, main: 'index.mjs',
    assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true, workers_dev: true, preview_urls: true,
    cache: { enabled: branch === 'dev' }, exports: structuredClone(workerExports),
    ratelimits: [structuredClone(branch === 'dev' ? stagingRate : productionRate)], previews: structuredClone(source.wrangler.previews),
  };
  return { source, dispatch, project, plan, config };
}
const replan = (source, dispatch = payload, values = {}) => prepare(dispatch, projectFromSource(dispatch, source, values));

test('native build and repeated validation preserve source cache, shared exports and isolated rate identities', () => {
  for (const branch of ['dev', 'main']) {
    const { plan, config } = fixture(branch);
    const first = sanitizeConfig(config, plan);
    assert.deepEqual(first.cache, { enabled: false });
    assert.deepEqual(first.exports, workerExports);
    assert.deepEqual(first.previews.cache, { enabled: true });
    assert.equal(first.previews.exports, undefined);
    assert.deepEqual(first.ratelimits, [productionRate]);
    assert.deepEqual(first.previews.ratelimits, [stagingRate]);
    assert.deepEqual(sanitizeConfig(first, plan), first);
  }
});

test('cache and exports inherit through Wrangler environments without inheriting Preview cache', () => {
  const { source } = fixture();
  const plan = replan(source);
  assert.deepEqual(plan.project.environments.production.deployment_behavior, { cache: { enabled: false }, exports: workerExports });
  assert.deepEqual(plan.project.environments.staging.deployment_behavior.exports, workerExports);
  delete source.wrangler.previews.cache;
  assert.throws(() => replan(source), /staging cache must match previews.cache/);
});

test('native production cache and every environment export map must match the production base', () => {
  for (const target of ['staging', 'production']) {
    const { source } = fixture();
    source.wrangler.env[target].exports = { default: { type: 'worker', cache: { enabled: true } } };
    assert.throws(() => replan(source), /exports must match/);
  }
  const { source } = fixture();
  source.wrangler.env.production.cache = { enabled: true };
  assert.throws(() => replan(source), /production cache must match/);
  const preview = fixture().source;
  preview.wrangler.previews.exports = structuredClone(workerExports);
  assert.throws(() => replan(preview), /Unsupported native Preview configuration/);
});

test('source-owned cross-version cache is validated and retained without adding it by default', () => {
  const { source, config } = fixture();
  source.wrangler.cache.cross_version_cache = false;
  source.wrangler.env.staging.cache.cross_version_cache = true;
  source.wrangler.previews.cache.cross_version_cache = true;
  config.cache.cross_version_cache = true;
  config.previews.cache.cross_version_cache = true;
  const result = sanitizeConfig(config, replan(source));
  assert.deepEqual(result.cache, { enabled: false, cross_version_cache: false });
  assert.deepEqual(result.previews.cache, { enabled: true, cross_version_cache: true });
  assert.deepEqual(sanitizeConfig(result, replan(source)), result);
  const plain = fixture();
  assert.equal(sanitizeConfig(plain.config, plain.plan).cache.cross_version_cache, undefined);
});

test('generated cache and entrypoint changes or omission fail before and after sanitization', () => {
  const { plan, config } = fixture();
  for (const change of [
    value => { value.cache.enabled = false; },
    value => { delete value.cache; },
    value => { value.exports.default.cache.enabled = true; },
    value => { delete value.exports.PublicContent; },
    value => { value.exports.Other = { type: 'worker' }; },
    value => { value.previews.cache.enabled = false; },
  ]) {
    const changed = structuredClone(config);
    change(changed);
    assert.throws(() => sanitizeConfig(changed, plan), /Worker behavior|Preview configuration/);
  }
  const clean = sanitizeConfig(config, plan);
  clean.exports.default.cache.enabled = true;
  assert.throws(() => sanitizeConfig(clean, plan), /Worker behavior: exports/);
});

test('Worker behavior rejects malformed fields, cache modes and lifecycle exports at every source level', () => {
  const malformedCache = [null, [], {}, { enabled: 'false' }, { enabled: false, unknown: true }, { enabled: false, cross_version_cache: 1 }];
  for (const value of malformedCache) for (const location of ['base', 'staging', 'production', 'preview']) {
    const { source } = fixture();
    const target = location === 'base' ? source.wrangler : location === 'preview' ? source.wrangler.previews : source.wrangler.env[location];
    target.cache = value;
    assert.throws(() => replan(source), /Unsupported Worker cache configuration/);
  }
  for (const exports of [null, [], { Bad: null }, { Bad: { type: 'durable-object', state: 'created' } }, { Bad: { type: 'workflow' } }, { Bad: { type: 'worker', state: 'created' } }, { Bad: { type: 'worker', cache: { enabled: true, cross_version_cache: true } } }]) {
    const { source } = fixture();
    source.wrangler.exports = exports;
    assert.throws(() => replan(source), /Worker exports|Worker entrypoint|cache configuration/);
  }
});

test('Worker export names and count are bounded and only supported Wrangler versions opt in', () => {
  for (const name of ['bad-name', 'default\n', 'A'.repeat(129), '']) {
    const { source } = fixture();
    source.wrangler.exports = { [name]: { type: 'worker' } };
    assert.throws(() => replan(source), /Invalid Worker export name/);
  }
  const { source } = fixture();
  source.wrangler.exports = Object.fromEntries(Array.from({ length: 129 }, (_, i) => [`Entry${i}`, { type: 'worker' }]));
  assert.throws(() => replan(source), /at most 128 entrypoints/);
  const legacy = fixture().source;
  delete legacy.wrangler.previews;
  legacy.pkg.devDependencies.wrangler = '4.106.0';
  assert.throws(() => replan(legacy), /requires Wrangler 4.107.0/);
});

test('old callers accept Wrangler empty exports normalization without adding cache or exports', () => {
  const { source, config } = fixture();
  delete source.wrangler.cache;
  delete source.wrangler.exports;
  delete source.wrangler.previews.cache;
  delete source.wrangler.env.staging.cache;
  delete config.cache;
  config.exports = {};
  delete config.previews.cache;
  const plan = replan(source);
  const clean = sanitizeConfig(config, plan);
  assert.equal(clean.cache, undefined);
  assert.equal(clean.exports, undefined);
  assert.deepEqual(sanitizeConfig(clean, plan), clean);
  config.cache = { enabled: true };
  assert.throws(() => sanitizeConfig(config, plan), /Worker behavior: cache/);
});

test('rate binding identity and policy changes are rejected at every validated boundary', () => {
  const { plan, config } = fixture();
  for (const [key, value] of [['name', 'OTHER'], ['namespace_id', '9000'], ['simple', { limit: 999, period: 10 }], ['unknown', true]]) {
    const changed = structuredClone(config);
    changed.ratelimits[0][key] = value;
    assert.throws(() => sanitizeConfig(changed, plan), /resource binding: ratelimits/);
    const preview = structuredClone(config);
    preview.previews.ratelimits[0][key] = value;
    assert.throws(() => sanitizeConfig(preview, plan), /Preview configuration/);
  }
});

test('rate binding name fields cannot collide with runtime secrets', () => {
  const { source } = fixture();
  assert.throws(() => replan(source, payload, { REQUEST_LIMIT: 'private' }), /collides with a Worker resource binding/);
});

test('source rate declarations reject malformed names, namespace IDs, policies and duplicate bindings', () => {
  for (const value of [null, {}, [productionRate, productionRate], [{ ...productionRate, name: 'BAD-NAME' }], [{ ...productionRate, namespace_id: 'abc' }], [{ ...productionRate, simple: { limit: 0, period: 60 } }], [{ ...productionRate, simple: { limit: 1, period: 30 } }], [{ ...productionRate, extra: true }]]) {
    for (const location of ['base', 'staging', 'production', 'preview']) {
      const { source } = fixture();
      const target = location === 'base' ? source.wrangler : location === 'preview' ? source.wrangler.previews : source.wrangler.env[location];
      target.ratelimits = value;
      assert.throws(() => replan(source), /ratelimits|rate limit/);
    }
  }
});
