import { isDeepStrictEqual } from 'node:util';
import { isAbsolute, relative, resolve, sep } from 'node:path';

export function assert(condition, message) {
  if (!condition) throw new Error(message);
}

export function inside(root, path) {
  assert(typeof path === 'string' && path.length > 0 && !isAbsolute(path), 'Expected a relative path');
  const result = resolve(root, path);
  const rel = relative(resolve(root), result);
  assert(rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel), 'Path escapes its root');
  return result;
}

const variable = /^[A-Z][A-Z0-9_]*$/;
const reserved = /^(?:NODE_|NPM_|PNPM_|YARN_|GITHUB_|GH_|ACTIONS_|RUNNER_|CLOUDFLARE_|WRANGLER_|INFISICAL_|CI$|PATH$|HOME$|BASH_ENV$|ENV$)/;
const script = /^[a-zA-Z0-9][a-zA-Z0-9:_-]*$/;
export const bindingKeys = ['kv_namespaces', 'r2_buckets', 'd1_databases', 'services', 'durable_objects', 'images', 'ai', 'version_metadata', 'analytics_engine_datasets', 'hyperdrive', 'vectorize', 'queues', 'workflows', 'secrets_store_secrets'];

export function validateDispatch(payload) {
  assert(payload && /^Lascade-Co\/[A-Za-z0-9_.-]+$/.test(payload.repo), 'Only Lascade-Co repositories are supported');
  assert(['dev', 'main'].includes(payload.branch), 'Only dev and main can deploy');
  assert(/^[a-f0-9]{40}$/.test(payload.sha), 'An exact 40-character commit SHA is required');
  assert(typeof payload.source_run_url === 'string' && payload.source_run_url.startsWith(`https://github.com/${payload.repo}/actions/runs/`) && /^\d+$/.test(payload.source_run_url.split('/').at(-1)), 'Invalid source run URL');
  assert(typeof payload.project_slug === 'string' && /^[a-zA-Z0-9_-]+$/.test(payload.project_slug), 'Caller must supply a valid Infisical project slug');
  const [owner, repo_name] = payload.repo.split('/');
  return { owner, repo_name };
}

export function statusPlan(payload) {
  validateDispatch(payload);
  return { repo: payload.repo, sha: payload.sha, target: payload.branch === 'main' ? 'production' : 'staging' };
}

export function prepare(payload, project) {
  const { owner, repo_name } = validateDispatch(payload);
  assert(project && /^[a-f0-9]{32}$/.test(project.account_id) && /^[a-z0-9][a-z0-9-]{0,62}$/.test(project.worker_name), 'Project must register a valid account and Worker');
  assert(/^[0-9]+(?:\.[0-9]+){0,2}$/.test(project.node_version) && Number(project.node_version.split('.')[0]) >= 22, 'Vinext requires Node 22 or newer');
  assert(/^[0-9]+\.[0-9]+\.[0-9]+$/.test(project.wrangler_version), 'Pin an exact Wrangler version');
  assert(['alias', 'native'].includes(project.preview_mode ?? 'alias'), 'Unsupported Preview mode');
  if (project.preview_mode === 'native') {
    const [major, minor] = project.wrangler_version.split('.').map(Number);
    assert(major > 4 || (major === 4 && minor >= 135), 'Native Previews require Wrangler 4.135.0 or later');
    assert(project.preview_name === 'stg', 'Dev Preview must use the stg name');
    assert(project.preview_config && typeof project.preview_config === 'object' && !Array.isArray(project.preview_config), 'Missing source Preview configuration');
  }
  assert(['pnpm', 'npm', 'yarn'].includes(project.package_manager), 'Unsupported package manager');
  for (const key of ['working_directory', 'generated_config', 'bundle_directory']) inside('/project', project[key]);
  inside(resolve('/project', project.bundle_directory), relative(resolve('/project', project.bundle_directory), resolve('/project', project.generated_config)));
  assert(script.test(project.build_script) && Array.isArray(project.check_scripts) && project.check_scripts.every(v => script.test(v)), 'Expected package script names');
  assert(Array.isArray(project.submodule_repos) && project.submodule_repos.every(v => /^[A-Za-z0-9_.-]+$/.test(v)), 'Invalid submodule repository');
  for (const key of ['build_variables', 'runtime_secrets']) {
    assert(Array.isArray(project[key]) && new Set(project[key]).size === project[key].length && project[key].every(v => variable.test(v) && !reserved.test(v)), `Invalid ${key}`);
  }
  assert(project.runtime_secrets.every(v => !project.build_variables.includes(v) && !v.startsWith('NEXT_PUBLIC_')), 'Runtime secrets must not overlap public/build variables');
  const target = payload.branch === 'dev' ? 'staging' : 'production';
  assert(project.environments?.production, 'Missing production environment');
  assert(project.environments[target], `No ${target} environment is configured`);
  for (const name of ['staging', 'production']) {
    const env = project.environments[name];
    if (!env) continue;
    assert(/^[a-zA-Z0-9_-]+$/.test(env.infisical_env), 'Missing Infisical environment mapping');
    assert(typeof env.infisical_path === 'string' && /^\/[a-zA-Z0-9_/-]*$/.test(env.infisical_path) && !env.infisical_path.includes('//'), 'Invalid Infisical path');
    assert(env.bindings && typeof env.bindings === 'object' && !Array.isArray(env.bindings) && Object.keys(env.bindings).every(k => bindingKeys.includes(k)), 'Unsupported resource binding configuration');
  }
  const stageKv = project.environments.staging?.bindings.kv_namespaces ?? [];
  const prodKv = project.environments.production.bindings.kv_namespaces ?? [];
  assert(stageKv.every(a => !prodKv.some(b => a.id === b.id)), 'Staging and production must use distinct KV namespaces');
  const slug = payload.project_slug;
  const environment = project.environments[target];
  return { repo: payload.repo, branch: payload.branch, sha: payload.sha, source_run_url: payload.source_run_url, owner, repo_name, project, target, infisical_project_slug: slug, infisical_env: environment.infisical_env, infisical_path: environment.infisical_path };
}

const empty = value => value == null || (Array.isArray(value) ? value.length === 0 : typeof value === 'object' && Object.values(value).every(empty));

// Only deployment data crosses the build/deploy boundary. Never run build hooks
// or use local files referenced by a caller-controlled Wrangler configuration.
export function sanitizeConfig(config, plan) {
  const p = plan.project;
  assert(config.name === p.worker_name && config.account_id === p.account_id, 'Built account or Worker differs from registered destination');
  assert(config.targetEnvironment === undefined || config.targetEnvironment === plan.target, 'Wrong build target');
  for (const key of ['build', 'alias', 'env', 'migrations', 'unsafe', 'containers']) assert(empty(config[key]), `Unsupported generated config: ${key}`);
  assert(config.no_bundle === true, 'Vinext must emit a prebuilt no_bundle Worker');
  assert(config.workers_dev === true && config.preview_urls === true, 'Enable workers_dev and preview_urls');
  assert(typeof config.main === 'string' && /\.(m?js)$/.test(config.main), 'Missing built Worker entry');
  assert(config.assets && typeof config.assets.directory === 'string', 'Missing built assets');
  for (const key of p.runtime_secrets) assert(!Object.hasOwn(config.vars ?? {}, key), 'Runtime secret found in plaintext vars');
  const expected = p.environments[plan.target].bindings;
  for (const key of bindingKeys) {
    assert(isDeepStrictEqual(config[key], expected[key]) || (empty(config[key]) && empty(expected[key])), `Unexpected ${plan.target} resource binding: ${key}`);
  }
  const result = {};
  for (const key of ['name', 'account_id', 'compatibility_date', 'compatibility_flags', 'main', 'assets', 'rules', 'vars', 'workers_dev', 'preview_urls', 'observability', 'limits', 'placement', 'jsx_factory', 'jsx_fragment', ...bindingKeys]) {
    if (config[key] !== undefined) result[key] = config[key];
  }
  result.no_bundle = true;
  if (p.preview_mode === 'native') {
    assert(config.vars === undefined || (config.vars && typeof config.vars === 'object' && !Array.isArray(config.vars) && Object.keys(config.vars).length === 0), 'Generated native Preview config must not declare plaintext vars');
    assert(isDeepStrictEqual(config.previews, p.preview_config), 'Generated Preview configuration differs from source');
    for (const key of bindingKeys) {
      if (p.environments.production.bindings[key] === undefined) delete result[key];
      else result[key] = p.environments.production.bindings[key];
    }
    result.previews = p.preview_config;
  } else {
    assert(empty(config.previews), 'Legacy alias build must not configure native Previews');
  }
  // Domains/routes are managed outside this runner; omit instead of clearing.
  result.secrets = { required: p.runtime_secrets };
  return result;
}
