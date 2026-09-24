import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { projectFromSource } from './source.mjs';
import { prepare, sanitizeConfig } from './config.mjs';
import { deploy, deploymentArgs } from './deploy.mjs';

const payload = {
  repo: 'Lascade-Co/example-web', branch: 'dev', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
  project_slug: 'example-web',
};

function nativeSource() {
  return {
    pkg: {
      packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.135.0' },
      scripts: { 'check:deploy': 'true', 'build:vinext': 'true' },
    },
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32),
      images: { binding: 'IMAGES' }, secrets: { required: ['APP_SECRET'] },
      previews: { images: { binding: 'IMAGES' }, secrets: { required: ['APP_SECRET'] } },
      env: {
        staging: { name: 'example-web', images: { binding: 'IMAGES' } },
        production: { name: 'example-web', images: { binding: 'IMAGES' } },
      },
    },
    submodule_repos: [],
  };
}

function nativeBundle(root, plan, source) {
  const bundleRoot = join(root, 'bundle');
  mkdirSync(join(bundleRoot, 'server'), { recursive: true });
  mkdirSync(join(bundleRoot, 'client'));
  writeFileSync(join(bundleRoot, 'server', 'index.mjs'), 'export default { fetch() { return new Response("ok") } };');
  writeFileSync(join(bundleRoot, 'server', 'wrangler.json'), JSON.stringify({
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: plan.target,
    main: 'index.mjs', assets: { directory: '../client' }, no_bundle: true,
    workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' },
    previews: source.wrangler.previews,
  }));
  writeFileSync(join(bundleRoot, 'vinext-release.json'), JSON.stringify({
    repo: payload.repo, sha: payload.sha, target: plan.target, worker: 'example-web',
  }));
  const envFile = join(root, 'infisical.json');
  writeFileSync(envFile, JSON.stringify({ APP_SECRET: 'selected' }));
  return { bundleRoot, envFile };
}

test('native Preview only uploads declared runtime secret names', () => {
  const source = nativeSource();
  const project = projectFromSource(payload, source, {
    NEXT_PUBLIC_API_URL: 'https://staging.example.com', APP_SECRET: 'selected',
    UNRELATED_API_KEY: 'must-not-upload',
  });
  const plan = prepare(payload, project);
  assert.deepEqual(project.runtime_secrets, ['APP_SECRET']);
  assert.equal(project.preview_name, 'stg');
  const built = {
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: 'staging',
    main: 'index.mjs', assets: { directory: '../client' }, no_bundle: true,
    workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' },
    previews: source.wrangler.previews,
  };
  assert.deepEqual(sanitizeConfig(built, plan).previews, source.wrangler.previews);
  assert.deepEqual(sanitizeConfig(built, plan).secrets.required, ['APP_SECRET']);
  assert.throws(() => sanitizeConfig({ ...built, previews: undefined }, plan), /Generated Preview configuration differs from source/);
  assert.throws(() => sanitizeConfig({ ...built, vars: { API_URL: 'plain' } }, plan), /must not declare plaintext vars/);
});

test('native source rejects plaintext vars at every Wrangler level', () => {
  for (const location of ['top', 'previews', 'staging', 'production']) {
    const source = nativeSource();
    if (location === 'top') source.wrangler.vars = { API_URL: 'plain' };
    else if (location === 'previews') source.wrangler.previews.vars = { API_URL: 'plain' };
    else source.wrangler.env[location].vars = { API_URL: 'plain' };
    assert.throws(() => projectFromSource(payload, source, { APP_SECRET: 'selected' }), /must not declare plaintext vars/, location);
  }
});

test('native Preview deploy passes Infisical secret file atomically and preserves production', async () => {
  const source = nativeSource();
  const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-preview-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source);
    const production = { versions: [{ version_id: 'production-version', percentage: 100 }] };
    let executed = false;
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        assert.match(path, /\/deployments$/);
        return { deployments: [production] };
      },
      execute: (_command, args, options) => {
        executed = true;
        assert.deepEqual(args.slice(1, 4), ['preview', '--name', 'stg']);
        assert(args.includes('--secrets-file') && args.includes('--json') && args.includes('--ignore-base-config'));
        assert.deepEqual(JSON.parse(readFileSync(args[args.indexOf('--secrets-file') + 1], 'utf8')), { APP_SECRET: 'selected' });
        assert.equal(options.env.CLOUDFLARE_ENV, undefined);
        return JSON.stringify({ preview: { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] }, deployment: { id: 'deployment-id', env: { APP_SECRET: { type: 'secret_text' }, IMAGES: { type: 'images' } } } });
      },
    });
    assert(executed);
    assert.deepEqual(result, { state: 'deployed', preview_name: 'stg', preview_id: 'preview-id', deployment_id: 'deployment-id' });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('native Preview rejects unexpected remote secret bindings', async () => {
  const source = nativeSource();
  const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-preview-secrets-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source);
    await assert.rejects(() => deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async () => ({ deployments: [] }),
      execute: () => JSON.stringify({
        preview: { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] },
        deployment: { id: 'deployment-id', env: { APP_SECRET: { type: 'secret_text' }, OLD_SECRET: { type: 'secret_text' } } },
      }),
    }), /unexpected secret binding/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('first native Preview works without an active production deployment', async () => {
  for (const missingResponse of ['404', 'empty']) {
    const source = nativeSource();
    const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected' }));
    const root = mkdtempSync(join(tmpdir(), 'vinext-first-preview-test-'));
    try {
      const { bundleRoot, envFile } = nativeBundle(root, plan, source);
      const result = await deploy(plan, {
        bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
        github: async () => ({ sha: payload.sha }),
        cloudflare: async () => {
          if (missingResponse === '404') throw new Error('API request failed (404) at /deployments');
          return { deployments: [] };
        },
        execute: () => JSON.stringify({
          preview: { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] },
          deployment: { id: 'deployment-id', env: { APP_SECRET: { type: 'secret_text' } } },
        }),
      });
      assert.equal(result.state, 'deployed', missingResponse);
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
});

test('first native production deploy works after a Preview with no active version', async () => {
  const source = nativeSource();
  const productionPayload = { ...payload, branch: 'main' };
  const plan = prepare(productionPayload, projectFromSource(productionPayload, source, { APP_SECRET: 'selected' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-first-production-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source);
    let deploymentCalls = 0;
    let uploadedTag;
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/deployments')) return { deployments: ++deploymentCalls === 1 ? [] : [{ versions: [{ version_id: 'new-version', percentage: 100 }] }] };
        if (path.includes('/versions?')) return { items: /[?&]page=1$/.test(path) ? [{ id: 'new-version', metadata: { created_on: '2026-09-24T00:00:00Z' }, annotations: { 'workers/tag': uploadedTag } }] : [] };
        if (path.endsWith('/versions/new-version')) return { resources: { bindings: [{ type: 'secret_text', name: 'APP_SECRET' }] } };
        throw new Error('Unexpected Cloudflare API call');
      },
      execute: (_command, args) => {
        assert.equal(args[1], 'deploy');
        uploadedTag = args[args.indexOf('--tag') + 1];
      },
    });
    assert.deepEqual(result, { state: 'deployed', version_id: 'new-version' });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('native production rejects unexpected existing secret bindings before deploy', async () => {
  const source = nativeSource();
  const productionPayload = { ...payload, branch: 'main' };
  const plan = prepare(productionPayload, projectFromSource(productionPayload, source, { APP_SECRET: 'selected' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-production-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source);
    let executed = false;
    await assert.rejects(() => deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/deployments')) return { deployments: [{ versions: [{ version_id: 'active', percentage: 100 }] }] };
        if (path.includes('/versions?')) return { items: /[?&]page=1$/.test(path) ? [{ id: 'latest', metadata: { created_on: '2026-09-24T00:00:00Z' } }] : [] };
        if (path.endsWith('/versions/latest') || path.endsWith('/versions/active')) return { resources: { bindings: [{ type: 'secret_text', name: 'OLD_SECRET' }] } };
        throw new Error('Unexpected Cloudflare API call');
      },
      execute: () => { executed = true; },
    }), /Unexpected remote secrets would be inherited/);
    assert.equal(executed, false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('legacy staging still uploads the dev alias', () => {
  const args = deploymentArgs({ repo: payload.repo, sha: payload.sha, branch: 'dev', target: 'staging', project: { preview_mode: 'alias' } }, '/config', '/secrets', 'tag');
  assert.deepEqual(args.slice(0, 4), ['versions', 'upload', '--preview-alias', 'dev']);
  assert(!args.includes('--json'));
});
