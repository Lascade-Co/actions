import test from 'node:test';
import assert from 'node:assert/strict';
import { projectFromSource } from './source.mjs';
import { prepare, sanitizeConfig } from './config.mjs';

const payload = {
  repo: 'Lascade-Co/example-web', branch: 'dev', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
  project_slug: 'example-web',
};
const service = {
  binding: 'PRIVATE_API', service_id: '11111111-1111-4111-8111-111111111111',
};

function fixture(branch = 'dev', values = {}) {
  const source = {
    pkg: {
      packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.135.0' },
      scripts: { 'check:deploy': 'true', 'build:vinext': 'true' },
    },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32),
      previews: { vpc_services: [service] },
      env: {
        staging: { name: 'example-web', vpc_services: [service] },
        production: { name: 'example-web' },
      },
    },
    submodule_repos: [],
  };
  const dispatch = { ...payload, branch };
  const project = projectFromSource(dispatch, source, values);
  const config = {
    name: 'example-web', account_id: 'b'.repeat(32),
    targetEnvironment: branch === 'dev' ? 'staging' : 'production',
    main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' },
    no_bundle: true, workers_dev: true, preview_urls: true,
    ...(branch === 'dev' ? { vpc_services: [service] } : {}),
    previews: source.wrangler.previews,
  };
  return { source, dispatch, project, config };
}

test('source extracts VPC Service bindings only for the configured staging target', () => {
  const { source, project } = fixture();
  assert.deepEqual(project.environments.staging.bindings.vpc_services, [service]);
  assert.equal(Object.hasOwn(project.environments.production.bindings, 'vpc_services'), false);
  assert.deepEqual(project.preview_config, source.wrangler.previews);
});

test('native VPC Service configuration survives repeated staging and production sanitization without leaking into production', () => {
  for (const branch of ['dev', 'main']) {
    const { dispatch, project, config } = fixture(branch);
    const plan = prepare(dispatch, project);
    const first = sanitizeConfig(config, plan);
    assert.equal(Object.hasOwn(first, 'vpc_services'), false, branch);
    assert.deepEqual(first.previews.vpc_services, [service], branch);
    assert.deepEqual(sanitizeConfig(first, plan), first, branch);
  }
});

test('VPC Service target identity drift is rejected', () => {
  const { dispatch, project, config } = fixture();
  const plan = prepare(dispatch, project);
  assert.throws(() => sanitizeConfig({
    ...config,
    vpc_services: [{ ...service, service_id: '22222222-2222-4222-8222-222222222222' }],
  }, plan), /Unexpected staging resource binding: vpc_services/);
  const sanitized = sanitizeConfig(config, plan);
  assert.throws(() => sanitizeConfig({ ...sanitized, vpc_services: [service] }, plan),
    /Unexpected production resource binding: vpc_services/);
});

test('VPC Service Preview identity drift is rejected for both deployment targets', () => {
  for (const branch of ['dev', 'main']) {
    const { dispatch, project, config } = fixture(branch);
    const plan = prepare(dispatch, project);
    for (const built of [config, sanitizeConfig(config, plan)]) {
      assert.throws(() => sanitizeConfig({
        ...built,
        previews: { vpc_services: [{ ...service, service_id: '22222222-2222-4222-8222-222222222222' }] },
      }, plan), /Generated Preview configuration differs from source/, branch);
    }
  }
});

test('runtime secrets cannot collide with a staging VPC Service binding', () => {
  for (const branch of ['dev', 'main']) {
    const { dispatch, project } = fixture(branch, { PRIVATE_API: 'secret-value' });
    assert.throws(() => prepare(dispatch, project), /Runtime secret name collides with a Worker resource binding/, branch);
  }
});
