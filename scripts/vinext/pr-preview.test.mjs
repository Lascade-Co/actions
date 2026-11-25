import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { prepare, sanitizeConfig, statusPlan, validateDispatch } from './config.mjs';
import { projectFromSource } from './source.mjs';
import { isCurrent } from './deploy.mjs';
import { build, validateBundle } from './build.mjs';

const base = {
  repo: 'Lascade-Co/example-web', sha: 'a'.repeat(40),
  source_run_url: 'https://github.com/Lascade-Co/example-web/actions/runs/1',
  project_slug: 'example-web',
};
const prPayload = { ...base, branch: 'feat/pr-previews', pr_number: 42 };
const values = { NEXT_PUBLIC_API_URL: 'https://x', APP_SECRET: 's' };

function pkg() {
  return { packageManager: 'pnpm@10.0.0', devDependencies: { wrangler: '4.135.0' }, scripts: { 'check:deploy': 'true', 'build:vinext': 'true' } };
}
function nativeSource() {
  return {
    pkg: pkg(),
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32), images: { binding: 'IMAGES' },
      previews: { images: { binding: 'IMAGES' } },
      env: { staging: { name: 'example-web', images: { binding: 'IMAGES' } }, production: { name: 'example-web', images: { binding: 'IMAGES' } } },
    },
    submodule_repos: [],
  };
}
function aliasSource() {
  return {
    pkg: pkg(),
    wrangler: {
      name: 'example-web', account_id: 'b'.repeat(32),
      env: { staging: { name: 'example-web', kv_namespaces: [{ binding: 'KV', id: 'stage' }] }, production: { name: 'example-web', kv_namespaces: [{ binding: 'KV', id: 'prod' }] } },
    },
    submodule_repos: [],
  };
}
const planFor = (payload, source = nativeSource()) => prepare(payload, projectFromSource(payload, source, values));
const json = value => JSON.parse(JSON.stringify(value));

test('pr dispatch requires a valid pr_number and branch', () => {
  const accepted = validateDispatch(prPayload);
  assert.equal(accepted.pr_number, 42);
  assert.equal(accepted.repo_name, 'example-web');
  for (const pr_number of [1, 9999999]) assert.equal(validateDispatch({ ...prPayload, pr_number }).pr_number, pr_number);
  // A PR's head may be any branch, including dev or main; pr_number decides the target.
  for (const branch of ['dev', 'main', 'feature', 'a/b/c', 'v1.2_x-y']) assert.equal(validateDispatch({ ...prPayload, branch }).pr_number, 42);
  for (const pr_number of [0, 10000000, -1, 1.5, '42', null, Number.NaN, true]) {
    assert.throws(() => validateDispatch({ ...prPayload, pr_number }), /pr_number/, String(pr_number));
  }
  for (const branch of ['../x', 'a/../b', 'a..b', '/x', 'x/', 'x//y', '-x', '.x', 'a/.x', 'x.', 'x.lock', 'a b', 'a~b', 'a:b', '', 'x'.repeat(201), undefined, 7]) {
    assert.throws(() => validateDispatch({ ...prPayload, branch }), /pull request head branch/, String(branch));
  }
  assert.equal(validateDispatch({ ...prPayload, branch: 'x'.repeat(200) }).pr_number, 42);
  const { pr_number, ...withoutNumber } = prPayload;
  assert.throws(() => validateDispatch(withoutNumber), /Only dev and main/);
  assert.throws(() => validateDispatch({ ...prPayload, sha: 'main' }), /40-character commit SHA/);
});

test('pr target plans a native preview named pr-<N> from staging values', () => {
  const project = projectFromSource(prPayload, nativeSource(), values);
  assert.equal(project.preview_mode, 'native');
  assert.equal(project.preview_name, 'pr-42');
  const plan = prepare(prPayload, project);
  assert.equal(plan.target, 'pr');
  assert.equal(plan.pr_number, 42);
  assert.equal(plan.branch, 'feat/pr-previews');
  assert.equal(plan.sha, prPayload.sha);
  assert.equal(plan.project.preview_name, 'pr-42');
  assert.equal(plan.infisical_env, 'staging');
  assert.equal(plan.infisical_path, '/');
  assert.deepEqual(plan.project.build_variables, ['NEXT_PUBLIC_API_URL']);
  assert.deepEqual(plan.project.runtime_secrets, ['APP_SECRET']);
  assert.deepEqual(statusPlan(prPayload), { repo: prPayload.repo, sha: prPayload.sha, target: 'pr', pr_number: 42 });

  // dev -> main PRs and a PR whose head is main still plan a PR Preview.
  for (const branch of ['dev', 'main']) {
    const headPlan = planFor({ ...prPayload, branch });
    assert.equal(headPlan.target, 'pr', branch);
    assert.equal(headPlan.project.preview_name, 'pr-42', branch);
    assert.equal(headPlan.infisical_env, 'staging', branch);
    assert.equal(statusPlan({ ...prPayload, branch }).target, 'pr', branch);
  }

  // The name is exactly pr-<N>; stg stays reserved for staging.
  for (const preview_name of ['stg', 'pr-6', 'pr-042', 'pr-42 ', 'PR-42', undefined]) {
    assert.throws(() => prepare(prPayload, { ...project, preview_name }), /pr-<number>/, String(preview_name));
  }
  assert.throws(() => prepare({ ...base, branch: 'dev' }, { ...project, preview_name: 'pr-42' }), /stg name/);
  assert.throws(() => prepare({ ...base, branch: 'main' }, { ...project, preview_name: 'pr-42' }), /stg name/);
  // prepare refuses a non-native project even if one is constructed by hand.
  assert.throws(() => prepare(prPayload, { ...project, preview_mode: 'alias', preview_name: undefined, preview_config: undefined }), /native/);
  assert.throws(() => prepare(prPayload, { ...project, preview_mode: undefined, preview_name: undefined, preview_config: undefined }), /native/);
});

test('pr plan validates a staging-built native bundle and never production', () => {
  const source = nativeSource();
  const plan = planFor(prPayload, source);
  const built = {
    name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: 'staging',
    main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
    workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' }, previews: source.wrangler.previews,
  };
  const sanitized = sanitizeConfig(built, plan);
  assert.deepEqual(sanitized.previews, source.wrangler.previews);
  assert.deepEqual(sanitized.secrets, { required: ['APP_SECRET'] });
  for (const targetEnvironment of ['production', 'pr']) {
    assert.throws(() => sanitizeConfig({ ...built, targetEnvironment }, plan), /Wrong build target/, targetEnvironment);
  }
});

test('pr build runs the staging Cloudflare environment and labels the release pr', () => {
  const source = nativeSource();
  const plan = planFor(prPayload, source);
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-build-test-'));
  try {
    const appRoot = join(root, 'app');
    mkdirSync(join(appRoot, 'dist', 'server'), { recursive: true });
    mkdirSync(join(appRoot, 'dist', 'client'));
    writeFileSync(join(appRoot, 'package.json'), JSON.stringify(source.pkg));
    writeFileSync(join(appRoot, 'pnpm-lock.yaml'), 'lockfileVersion: 9.0\n');
    writeFileSync(join(appRoot, 'dist', 'server', 'index.mjs'), 'export default { fetch() { return new Response("ok") } };');
    writeFileSync(join(appRoot, 'dist', 'server', 'wrangler.json'), JSON.stringify({
      name: 'example-web', account_id: 'b'.repeat(32), targetEnvironment: 'staging',
      main: 'index.mjs', assets: { directory: '../client', binding: 'ASSETS' }, no_bundle: true,
      workers_dev: true, preview_urls: true, images: { binding: 'IMAGES' }, previews: source.wrangler.previews,
    }));
    const envFile = join(root, 'build.json');
    writeFileSync(envFile, JSON.stringify({ NEXT_PUBLIC_API_URL: 'https://x' }));
    const bundleRoot = join(root, 'bundle');
    const environments = [];
    build(plan, { appRoot, envFile, bundleRoot, env: {}, execute: (_command, _args, options) => environments.push(options.env.CLOUDFLARE_ENV) });
    assert.ok(environments.length > 0);
    assert.ok(environments.every(name => name === 'staging'));
    assert.deepEqual(JSON.parse(readFileSync(join(bundleRoot, 'vinext-release.json'), 'utf8')), { repo: plan.repo, sha: plan.sha, target: 'pr', worker: 'example-web' });
    assert.deepEqual(validateBundle(bundleRoot, plan).config.previews, source.wrangler.previews);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('pr target refuses a source without a previews block', () => {
  assert.throws(() => projectFromSource(prPayload, aliasSource(), values), /previews block/);
  const noPreviews = nativeSource();
  delete noPreviews.wrangler.previews;
  assert.throws(() => projectFromSource(prPayload, noPreviews, values), /previews block/);
  const noStaging = nativeSource();
  delete noStaging.wrangler.env.staging;
  assert.throws(() => projectFromSource(prPayload, noStaging, values), /staging/);
  // The same alias source still plans dev and main as before.
  assert.equal(planFor({ ...base, branch: 'dev' }, aliasSource()).target, 'staging');
  assert.equal(planFor({ ...base, branch: 'main' }, aliasSource()).target, 'production');
});

test('isCurrent for pr compares the PR head sha and requires open non-draft', async () => {
  const plan = planFor(prPayload);
  const open = { state: 'open', draft: false, head: { sha: plan.sha, ref: plan.branch, repo: { full_name: plan.repo } } };
  const paths = [];
  const github = pull => async path => { paths.push(path); return pull; };
  assert.equal(await isCurrent(plan, github(open)), true);
  assert.deepEqual(paths, ['/repos/Lascade-Co/example-web/pulls/42']);
  for (const [name, pull] of [
    ['superseded push', { ...open, head: { ...open.head, sha: 'c'.repeat(40) } }],
    ['closed', { ...open, state: 'closed' }],
    ['draft', { ...open, draft: true }],
    ['draft missing', { state: 'open', head: open.head }],
    ['fork head', { ...open, head: { ...open.head, repo: { full_name: 'someone/example-web' } } }],
    ['deleted fork', { ...open, head: { ...open.head, repo: null } }],
  ]) assert.equal(await isCurrent(plan, github(pull)), false, name);

  // dev and main keep comparing against the branch head.
  const branchPaths = [];
  const branchGithub = async path => { branchPaths.push(path); return { sha: base.sha }; };
  assert.equal(await isCurrent(planFor({ ...base, branch: 'dev' }), branchGithub), true);
  assert.equal(await isCurrent(planFor({ ...base, branch: 'main' }), branchGithub), true);
  assert.deepEqual(branchPaths, ['/repos/Lascade-Co/example-web/commits/dev', '/repos/Lascade-Co/example-web/commits/main']);
});

test('CLI prepare loads Infisical staging for a PR whose head is main', async () => {
  const root = mkdtempSync(join(tmpdir(), 'vinext-pr-cli-'));
  const keys = ['PAYLOAD_JSON', 'GITHUB_OUTPUT', 'GH_TOKEN', 'INFISICAL_DOMAIN', 'INFISICAL_CLIENT_ID', 'INFISICAL_CLIENT_SECRET', 'GITHUB_ACTIONS'];
  const saved = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  const originalFetch = globalThis.fetch, originalCommand = process.argv[2], originalExitCode = process.exitCode, originalLog = console.log;
  const fixture = nativeSource(), environments = [], logs = [];
  const payload = { ...prPayload, branch: 'main', pr_number: 7 };
  const githubFile = value => { const text = JSON.stringify(value); return { type: 'file', encoding: 'base64', content: Buffer.from(text).toString('base64'), size: Buffer.byteLength(text) }; };
  try {
    process.env.PAYLOAD_JSON = JSON.stringify(payload);
    process.env.GITHUB_OUTPUT = join(root, 'outputs');
    process.env.GH_TOKEN = 'mock-github-token';
    process.env.INFISICAL_DOMAIN = 'https://infisical.example';
    process.env.INFISICAL_CLIENT_ID = 'mock-id';
    process.env.INFISICAL_CLIENT_SECRET = 'mock-secret';
    delete process.env.GITHUB_ACTIONS;
    process.argv[2] = 'prepare';
    console.log = line => logs.push(line);
    globalThis.fetch = async urlValue => {
      const url = new URL(urlValue);
      let value;
      if (url.hostname === 'infisical.example') {
        if (url.pathname.endsWith('/login')) value = { accessToken: 'mock-token' };
        else { environments.push(url.searchParams.get('environment')); value = { secrets: [{ secretKey: 'APP_SECRET', secretValue: 's' }] }; }
      } else if (url.pathname.endsWith('/contents/package.json')) value = githubFile(fixture.pkg);
      else if (url.pathname.endsWith('/contents/wrangler.jsonc')) value = githubFile(fixture.wrangler);
      else if (url.pathname.endsWith(`/git/commits/${payload.sha}`)) value = { tree: { sha: 'c'.repeat(40) } };
      else if (url.pathname.endsWith(`/git/trees/${'c'.repeat(40)}`)) value = { tree: [], truncated: false };
      else assert.fail(`Unexpected URL: ${url}`);
      return new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });
    };
    await import(`./cli.mjs?pr-preview=${Date.now()}`);
    assert.equal(process.exitCode, originalExitCode);
    assert.deepEqual(environments, ['staging']);
    const output = readFileSync(process.env.GITHUB_OUTPUT, 'utf8').split('\n').find(line => line.startsWith('plan='));
    const plan = JSON.parse(output.slice(5));
    assert.equal(plan.target, 'pr');
    assert.equal(plan.pr_number, 7);
    assert.equal(plan.project.preview_name, 'pr-7');
    assert.equal(plan.infisical_env, 'staging');
    assert.ok(logs.includes('Vinext deployment: Lascade-Co/example-web · pr #7'), logs.join('\n'));
  } finally {
    globalThis.fetch = originalFetch;
    console.log = originalLog;
    process.argv[2] = originalCommand;
    process.exitCode = originalExitCode;
    for (const key of keys) if (saved[key] === undefined) delete process.env[key]; else process.env[key] = saved[key];
    rmSync(root, { recursive: true, force: true });
  }
});

// Captured from origin/main e0e63be before the pr target existed.
const baseline = {
  "native-dev": {
    "dispatch": {
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project_slug": "example-web"
    },
    "status": {
      "repo": "Lascade-Co/example-web",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "target": "staging"
    },
    "plan": {
      "repo": "Lascade-Co/example-web",
      "branch": "dev",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "source_run_url": "https://github.com/Lascade-Co/example-web/actions/runs/1",
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project": {
        "account_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "worker_name": "example-web",
        "node_version": "24",
        "asset_binding": "ASSETS",
        "package_manager": "pnpm",
        "wrangler_version": "4.135.0",
        "working_directory": ".",
        "bundle_directory": "dist",
        "generated_config": "dist/server/wrangler.json",
        "build_script": "build:vinext",
        "check_scripts": [
          "check:deploy"
        ],
        "submodule_repos": [],
        "build_variables": [
          "NEXT_PUBLIC_API_URL"
        ],
        "runtime_secrets": [
          "APP_SECRET"
        ],
        "preview_mode": "native",
        "preview_name": "stg",
        "preview_config": {
          "images": {
            "binding": "IMAGES"
          }
        },
        "environments": {
          "staging": {
            "infisical_env": "staging",
            "infisical_path": "/",
            "bindings": {
              "images": {
                "binding": "IMAGES"
              }
            },
            "deployment_behavior": {}
          },
          "production": {
            "infisical_env": "prod",
            "infisical_path": "/",
            "bindings": {
              "images": {
                "binding": "IMAGES"
              }
            },
            "deployment_behavior": {}
          }
        }
      },
      "target": "staging",
      "infisical_project_slug": "example-web",
      "infisical_env": "staging",
      "infisical_path": "/"
    }
  },
  "native-main": {
    "dispatch": {
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project_slug": "example-web"
    },
    "status": {
      "repo": "Lascade-Co/example-web",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "target": "production"
    },
    "plan": {
      "repo": "Lascade-Co/example-web",
      "branch": "main",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "source_run_url": "https://github.com/Lascade-Co/example-web/actions/runs/1",
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project": {
        "account_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "worker_name": "example-web",
        "node_version": "24",
        "asset_binding": "ASSETS",
        "package_manager": "pnpm",
        "wrangler_version": "4.135.0",
        "working_directory": ".",
        "bundle_directory": "dist",
        "generated_config": "dist/server/wrangler.json",
        "build_script": "build:vinext",
        "check_scripts": [
          "check:deploy"
        ],
        "submodule_repos": [],
        "build_variables": [
          "NEXT_PUBLIC_API_URL"
        ],
        "runtime_secrets": [
          "APP_SECRET"
        ],
        "preview_mode": "native",
        "preview_name": "stg",
        "preview_config": {
          "images": {
            "binding": "IMAGES"
          }
        },
        "environments": {
          "staging": {
            "infisical_env": "staging",
            "infisical_path": "/",
            "bindings": {
              "images": {
                "binding": "IMAGES"
              }
            },
            "deployment_behavior": {}
          },
          "production": {
            "infisical_env": "prod",
            "infisical_path": "/",
            "bindings": {
              "images": {
                "binding": "IMAGES"
              }
            },
            "deployment_behavior": {}
          }
        }
      },
      "target": "production",
      "infisical_project_slug": "example-web",
      "infisical_env": "prod",
      "infisical_path": "/"
    }
  },
  "alias-dev": {
    "dispatch": {
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project_slug": "example-web"
    },
    "status": {
      "repo": "Lascade-Co/example-web",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "target": "staging"
    },
    "plan": {
      "repo": "Lascade-Co/example-web",
      "branch": "dev",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "source_run_url": "https://github.com/Lascade-Co/example-web/actions/runs/1",
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project": {
        "account_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "worker_name": "example-web",
        "node_version": "24",
        "asset_binding": "ASSETS",
        "package_manager": "pnpm",
        "wrangler_version": "4.135.0",
        "working_directory": ".",
        "bundle_directory": "dist",
        "generated_config": "dist/server/wrangler.json",
        "build_script": "build:vinext",
        "check_scripts": [
          "check:deploy"
        ],
        "submodule_repos": [],
        "build_variables": [
          "NEXT_PUBLIC_API_URL"
        ],
        "runtime_secrets": [
          "APP_SECRET"
        ],
        "preview_mode": "alias",
        "environments": {
          "staging": {
            "infisical_env": "staging",
            "infisical_path": "/",
            "bindings": {
              "kv_namespaces": [
                {
                  "binding": "KV",
                  "id": "stage"
                }
              ]
            },
            "deployment_behavior": {}
          },
          "production": {
            "infisical_env": "prod",
            "infisical_path": "/",
            "bindings": {
              "kv_namespaces": [
                {
                  "binding": "KV",
                  "id": "prod"
                }
              ]
            },
            "deployment_behavior": {}
          }
        }
      },
      "target": "staging",
      "infisical_project_slug": "example-web",
      "infisical_env": "staging",
      "infisical_path": "/"
    }
  },
  "alias-main": {
    "dispatch": {
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project_slug": "example-web"
    },
    "status": {
      "repo": "Lascade-Co/example-web",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "target": "production"
    },
    "plan": {
      "repo": "Lascade-Co/example-web",
      "branch": "main",
      "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "source_run_url": "https://github.com/Lascade-Co/example-web/actions/runs/1",
      "owner": "Lascade-Co",
      "repo_name": "example-web",
      "project": {
        "account_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "worker_name": "example-web",
        "node_version": "24",
        "asset_binding": "ASSETS",
        "package_manager": "pnpm",
        "wrangler_version": "4.135.0",
        "working_directory": ".",
        "bundle_directory": "dist",
        "generated_config": "dist/server/wrangler.json",
        "build_script": "build:vinext",
        "check_scripts": [
          "check:deploy"
        ],
        "submodule_repos": [],
        "build_variables": [
          "NEXT_PUBLIC_API_URL"
        ],
        "runtime_secrets": [
          "APP_SECRET"
        ],
        "preview_mode": "alias",
        "environments": {
          "staging": {
            "infisical_env": "staging",
            "infisical_path": "/",
            "bindings": {
              "kv_namespaces": [
                {
                  "binding": "KV",
                  "id": "stage"
                }
              ]
            },
            "deployment_behavior": {}
          },
          "production": {
            "infisical_env": "prod",
            "infisical_path": "/",
            "bindings": {
              "kv_namespaces": [
                {
                  "binding": "KV",
                  "id": "prod"
                }
              ]
            },
            "deployment_behavior": {}
          }
        }
      },
      "target": "production",
      "infisical_project_slug": "example-web",
      "infisical_env": "prod",
      "infisical_path": "/"
    }
  }
};

test('existing dev/main dispatches are unchanged', () => {
  for (const [kind, source] of [['native', nativeSource], ['alias', aliasSource]]) {
    for (const branch of ['dev', 'main']) {
      const payload = { ...base, branch };
      const expected = baseline[`${kind}-${branch}`];
      const plan = prepare(payload, projectFromSource(payload, source(), values));
      assert.deepEqual(json(validateDispatch(payload)), expected.dispatch, `${kind}-${branch} dispatch`);
      assert.deepEqual(json(statusPlan(payload)), expected.status, `${kind}-${branch} status`);
      assert.deepEqual(json(plan), expected.plan, `${kind}-${branch} plan`);
      assert.equal(Object.hasOwn(plan, 'pr_number'), false);
      assert.equal(Object.hasOwn(validateDispatch(payload), 'pr_number'), false);
    }
  }
});
