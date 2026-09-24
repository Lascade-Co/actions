import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { projectFromSource } from './source.mjs';
import { prepare, sanitizeConfig } from './config.mjs';
import { deploy, deploymentArgs } from './deploy.mjs';
import { exportSecrets } from './secrets.mjs';
import { validateBundle } from './build.mjs';

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
      images: { binding: 'IMAGES' },
      previews: { images: { binding: 'IMAGES' } },
      env: {
        staging: { name: 'example-web', images: { binding: 'IMAGES' } },
        production: { name: 'example-web', images: { binding: 'IMAGES' } },
      },
    },
    submodule_repos: [],
  };
}

function nativeBundle(root, plan, source, values = { APP_SECRET: 'selected' }) {
  const bundleRoot = join(root, 'bundle');
  mkdirSync(join(bundleRoot, 'server'), { recursive: true });
  mkdirSync(join(bundleRoot, 'client'));
  writeFileSync(join(bundleRoot, 'server', 'index.mjs'), 'export default { fetch() { return new Response("ok") } };');
  writeFileSync(join(bundleRoot, 'server', 'wrangler.json'), JSON.stringify({
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: plan.target,
    main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
    workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' },
    previews: source.wrangler.previews,
  }));
  writeFileSync(join(bundleRoot, 'vinext-release.json'), JSON.stringify({
    repo: payload.repo, sha: payload.sha, target: plan.target, worker: 'example-web',
  }));
  const envFile = join(root, 'infisical.json');
  writeFileSync(envFile, JSON.stringify(values));
  return { bundleRoot, envFile };
}

test('native Preview derives every private runtime secret from Infisical', () => {
  const source = nativeSource();
  const project = projectFromSource(payload, source, {
    NEXT_PUBLIC_API_URL: 'https://staging.example.com', APP_SECRET: 'selected',
    UNRELATED_API_KEY: 'also-upload', VINEXT_RUNTIME_SECRETS: 'also-upload',
  });
  const plan = prepare(payload, project);
  assert.deepEqual(project.runtime_secrets, ['APP_SECRET', 'UNRELATED_API_KEY', 'VINEXT_RUNTIME_SECRETS']);
  assert.equal(project.preview_name, 'stg');
  const built = {
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: 'staging',
    main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
    workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' },
    previews: source.wrangler.previews,
  };
  assert.deepEqual(sanitizeConfig(built, plan).previews, source.wrangler.previews);
  assert.deepEqual(sanitizeConfig(built, plan).secrets.required, project.runtime_secrets);
  assert.throws(() => sanitizeConfig({ ...built, previews: undefined }, plan), /Generated Preview configuration differs from source/);
  assert.throws(() => sanitizeConfig({ ...built, vars: { API_URL: 'plain' } }, plan), /must not declare plaintext vars/);
  assert.throws(() => projectFromSource(payload, { ...source, wrangler: { ...source.wrangler, previews: { ...source.wrangler.previews, secrets: { required: ['APP_SECRET'] } } } }, { APP_SECRET: 'selected' }), /Unsupported native Preview configuration/);
});

test('sanitized native Preview bundle passes validation a second time', () => {
  const source = nativeSource();
  const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected', VINEXT_METADATA: 'private-value' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-second-pass-test-'));
  try {
    const { bundleRoot } = nativeBundle(root, plan, source);
    const configPath = join(bundleRoot, 'server', 'wrangler.json');
    const first = validateBundle(bundleRoot, plan).config;
    writeFileSync(configPath, JSON.stringify(first));
    const second = validateBundle(bundleRoot, plan).config;
    assert.deepEqual(second, first);
    assert.deepEqual(second.previews, source.wrangler.previews);
    assert.throws(() => sanitizeConfig({ ...first, previews: { ...first.previews, vars: { EXTRA: 'plain' } } }, plan), /Generated Preview configuration differs from source/);
  } finally { rmSync(root, { recursive: true, force: true }); }
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

test('runtime secrets cannot shadow production, Preview, or asset bindings', () => {
  for (const [name, configure] of [
    ['IMAGES', () => {}],
    ['ASSETS', () => {}],
    ['PREVIEW_KV', source => { source.wrangler.previews.kv_namespaces = [{ binding: 'PREVIEW_KV', id: 'preview-kv' }]; }],
    ['PROD_R2', source => { source.wrangler.r2_buckets = [{ binding: 'PROD_R2', bucket_name: 'prod-bucket' }]; }],
  ]) {
    const source = nativeSource();
    configure(source);
    const project = projectFromSource(payload, source, { [name]: 'secret-value' });
    assert.throws(() => prepare(payload, project), /collides with a Worker resource binding/, name);
  }
});

test('runtime export fails if Infisical adds a private key after preparation', async () => {
  const source = nativeSource();
  const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-infisical-drift-test-'));
  try {
    const file = join(root, 'runtime.json');
    const fetcher = async url => ({
      ok: true,
      json: async () => url.pathname.endsWith('/login')
        ? { accessToken: 'dummy-token' }
        : { secrets: [{ secretKey: 'APP_SECRET', secretValue: 'selected' }, { secretKey: 'VINEXT_METADATA', secretValue: 'new-value' }] },
    });
    await assert.rejects(() => exportSecrets(plan, {
      file, kind: 'runtime',
      env: { INFISICAL_DOMAIN: 'https://infisical.example', INFISICAL_CLIENT_ID: 'id', INFISICAL_CLIENT_SECRET: 'secret' },
      fetcher,
    }), /variable names changed since deployment preparation/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('native Preview deploy passes Infisical secret file atomically and preserves production', async () => {
  const source = nativeSource();
  const runtimeValues = { APP_SECRET: 'selected', VINEXT_METADATA: 'private-value' };
  const plan = prepare(payload, projectFromSource(payload, source, runtimeValues));
  const root = mkdtempSync(join(tmpdir(), 'vinext-preview-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source, runtimeValues);
    const production = { versions: [{ version_id: 'production-version', percentage: 100 }] };
    let executed = false;
    let uploadedTag;
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/previews/stg/deployments/latest')) return { id: 'deployment-id', annotations: { 'workers/tag': uploadedTag }, env: { APP_SECRET: { type: 'secret_text' }, VINEXT_METADATA: { type: 'secret_text' }, IMAGES: { type: 'images' } } };
        if (path.endsWith('/previews/stg')) return { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] };
        if (path.endsWith('/deployments')) return { deployments: [production] };
        throw new Error(`Unexpected Cloudflare API path: ${path}`);
      },
      execute: (_command, args, options) => {
        executed = true;
        uploadedTag = args[args.indexOf('--tag') + 1];
        assert.deepEqual(args.slice(1, 4), ['preview', '--name', 'stg']);
        assert(args.includes('--secrets-file') && args.includes('--json') && args.includes('--ignore-base-config'));
        assert.deepEqual(JSON.parse(readFileSync(args[args.indexOf('--secrets-file') + 1], 'utf8')), runtimeValues);
        assert.equal(options.env.CLOUDFLARE_ENV, undefined);
        assert.deepEqual(options.stdio, ['ignore', 'ignore', 'inherit']);
        return '🌀 Building list of assets...\n🌀 Starting asset upload...\n{ "preview": "mixed with progress output" }';
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
    let uploadedTag;
    await assert.rejects(() => deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/previews/stg/deployments/latest')) return { id: 'deployment-id', annotations: { 'workers/tag': uploadedTag }, env: { APP_SECRET: { type: 'secret_text' }, OLD_SECRET: { type: 'secret_text' } } };
        if (path.endsWith('/previews/stg')) return { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] };
        if (path.endsWith('/deployments')) return { deployments: [] };
        throw new Error(`Unexpected Cloudflare API path: ${path}`);
      },
      execute: (_command, args) => { uploadedTag = args[args.indexOf('--tag') + 1]; },
    }), /unexpected secret binding/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('native Preview does not credit an unrelated latest deployment', async () => {
  const source = nativeSource();
  const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected' }));
  const root = mkdtempSync(join(tmpdir(), 'vinext-preview-tag-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source);
    await assert.rejects(() => deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      pause: async () => {},
      cloudflare: async path => {
        if (path.endsWith('/previews/stg/deployments/latest')) return { id: 'deployment-id', annotations: { 'workers/tag': 'another-upload' }, env: { APP_SECRET: { type: 'secret_text' } } };
        if (path.endsWith('/previews/stg')) return { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] };
        if (path.endsWith('/deployments')) return { deployments: [] };
        throw new Error(`Unexpected Cloudflare API path: ${path}`);
      },
      execute: () => {},
    }), /Latest Preview deployment differs from the uploaded source/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('native Preview accepts no runtime secrets when Cloudflare omits env', async () => {
  const source = nativeSource();
  const plan = prepare(payload, projectFromSource(payload, source, {}));
  const root = mkdtempSync(join(tmpdir(), 'vinext-preview-no-secrets-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source, {});
    let uploadedTag;
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/previews/stg/deployments/latest')) return { id: 'deployment-id', annotations: { 'workers/tag': uploadedTag } };
        if (path.endsWith('/previews/stg')) return { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] };
        if (path.endsWith('/deployments')) return { deployments: [] };
        throw new Error(`Unexpected Cloudflare API path: ${path}`);
      },
      execute: (_command, args) => { uploadedTag = args[args.indexOf('--tag') + 1]; },
    });
    assert.equal(result.state, 'deployed');
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('first native Preview works without an active production deployment', async () => {
  for (const missingResponse of ['404', 'empty']) {
    const source = nativeSource();
    const plan = prepare(payload, projectFromSource(payload, source, { APP_SECRET: 'selected' }));
    const root = mkdtempSync(join(tmpdir(), 'vinext-first-preview-test-'));
    try {
      const { bundleRoot, envFile } = nativeBundle(root, plan, source);
      let uploadedTag;
      let latestCalls = 0;
      const result = await deploy(plan, {
        bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
        github: async () => ({ sha: payload.sha }),
        pause: async () => {},
        cloudflare: async path => {
          if (path.endsWith('/previews/stg/deployments/latest')) {
            if (missingResponse === '404' && ++latestCalls === 1) throw new Error('API request failed (404) at /deployments/latest');
            return { id: 'deployment-id', annotations: { 'workers/tag': uploadedTag }, env: { APP_SECRET: { type: 'secret_text' } } };
          }
          if (path.endsWith('/previews/stg')) return { id: 'preview-id', name: 'stg', urls: ['https://stg.example.com'] };
          if (path.endsWith('/deployments')) {
            if (missingResponse === '404') throw new Error('API request failed (404) at /deployments');
            return { deployments: [] };
          }
          throw new Error(`Unexpected Cloudflare API path: ${path}`);
        },
        execute: (_command, args) => { uploadedTag = args[args.indexOf('--tag') + 1]; },
      });
      assert.equal(result.state, 'deployed', missingResponse);
      if (missingResponse === '404') assert.equal(latestCalls, 2);
    } finally { rmSync(root, { recursive: true, force: true }); }
  }
});

test('first native production deploy works after a Preview with no active version', async () => {
  const source = nativeSource();
  const productionPayload = { ...payload, branch: 'main' };
  const runtimeValues = { APP_SECRET: 'selected', VINEXT_METADATA: 'private-value' };
  const plan = prepare(productionPayload, projectFromSource(productionPayload, source, runtimeValues));
  const root = mkdtempSync(join(tmpdir(), 'vinext-first-production-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source, runtimeValues);
    let deploymentCalls = 0;
    let uploadedTag;
    const result = await deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/deployments')) return { deployments: ++deploymentCalls === 1 ? [] : [{ versions: [{ version_id: 'new-version', percentage: 100 }] }] };
        if (path.includes('/versions?')) return { items: /[?&]page=1$/.test(path) ? [{ id: 'new-version', metadata: { created_on: '2026-09-24T00:00:00Z' }, annotations: { 'workers/tag': uploadedTag } }] : [] };
        if (path.endsWith('/versions/new-version')) return { resources: { bindings: [{ type: 'secret_text', name: 'APP_SECRET' }, { type: 'secret_text', name: 'VINEXT_METADATA' }] } };
        throw new Error('Unexpected Cloudflare API call');
      },
      execute: (_command, args) => {
        assert.equal(args[1], 'deploy');
        assert.deepEqual(JSON.parse(readFileSync(args[args.indexOf('--secrets-file') + 1], 'utf8')), runtimeValues);
        uploadedTag = args[args.indexOf('--tag') + 1];
      },
    });
    assert.deepEqual(result, { state: 'deployed', version_id: 'new-version' });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('native production rejects an active version missing a runtime secret', async () => {
  const source = nativeSource();
  const productionPayload = { ...payload, branch: 'main' };
  const runtimeValues = { APP_SECRET: 'selected', VINEXT_METADATA: 'private-value' };
  const plan = prepare(productionPayload, projectFromSource(productionPayload, source, runtimeValues));
  const root = mkdtempSync(join(tmpdir(), 'vinext-missing-production-secret-test-'));
  try {
    const { bundleRoot, envFile } = nativeBundle(root, plan, source, runtimeValues);
    let deploymentCalls = 0;
    let uploadedTag;
    await assert.rejects(() => deploy(plan, {
      bundleRoot, envFile, wranglerBin: '/wrangler.js', env: {},
      github: async () => ({ sha: payload.sha }),
      cloudflare: async path => {
        if (path.endsWith('/deployments')) return { deployments: ++deploymentCalls === 1 ? [] : [{ versions: [{ version_id: 'new-version', percentage: 100 }] }] };
        if (path.includes('/versions?')) return { items: /[?&]page=1$/.test(path) ? [{ id: 'new-version', metadata: { created_on: '2026-09-24T00:00:00Z' }, annotations: { 'workers/tag': uploadedTag } }] : [] };
        if (path.endsWith('/versions/new-version')) return { resources: { bindings: [{ type: 'secret_text', name: 'APP_SECRET' }] } };
        throw new Error('Unexpected Cloudflare API call');
      },
      execute: (_command, args) => { uploadedTag = args[args.indexOf('--tag') + 1]; },
    }), /missing a required runtime secret/);
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
