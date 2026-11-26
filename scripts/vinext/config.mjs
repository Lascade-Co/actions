import { isDeepStrictEqual } from 'node:util';
import { isAbsolute, relative, resolve, sep } from 'node:path';

export function assert(condition, message) {
  if (!condition) throw new Error(message);
}

// Ignore unset Infisical entries without altering meaningful secret whitespace.
export function nonemptyValues(values) {
  return Object.fromEntries(Object.entries(values).filter(([, value]) => {
    assert(typeof value === 'string', 'Infisical values must be strings');
    return value.trim().length > 0;
  }));
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
export const bindingKeys = ['kv_namespaces', 'r2_buckets', 'd1_databases', 'services', 'vpc_services', 'durable_objects', 'images', 'ai', 'version_metadata', 'analytics_engine_datasets', 'hyperdrive', 'vectorize', 'queues', 'workflows', 'secrets_store_secrets', 'ratelimits'];
export const behaviorKeys = ['cache', 'exports'];

export function validateRateLimits(value) {
  if (value === undefined) return;
  assert(Array.isArray(value) && value.length <= 32, 'Source ratelimits must be an array of at most 32 bindings');
  const names = new Set();
  for (const limit of value) {
    assert(limit && typeof limit === 'object' && !Array.isArray(limit) &&
      Object.keys(limit).every(key => ['name', 'namespace_id', 'simple'].includes(key)) &&
      typeof limit.name === 'string' && limit.name.length <= 128 && /^[A-Za-z_][A-Za-z0-9_]*$(?![\s\S])/.test(limit.name) && !names.has(limit.name), 'Invalid or duplicate rate limit binding');
    names.add(limit.name);
    assert(typeof limit.namespace_id === 'string' && /^(?:0|[1-9][0-9]{0,19})$(?![\s\S])/.test(limit.namespace_id), 'Invalid rate limit namespace_id');
    const simple = limit.simple;
    assert(simple && typeof simple === 'object' && !Array.isArray(simple) &&
      Object.keys(simple).every(key => ['limit', 'period'].includes(key)) &&
      Number.isSafeInteger(simple.limit) && simple.limit > 0 && [10, 60].includes(simple.period), 'Invalid rate limit simple configuration');
  }
}

const plainObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);

function validateCache(value, entrypoint = false) {
  if (value === undefined) return;
  const keys = entrypoint ? ['enabled'] : ['enabled', 'cross_version_cache'];
  assert(plainObject(value) && typeof value.enabled === 'boolean' && Object.keys(value).every(key => keys.includes(key)) &&
    (value.cross_version_cache === undefined || typeof value.cross_version_cache === 'boolean'), 'Unsupported Worker cache configuration');
}

export function validateWorkerBehavior(value) {
  assert(plainObject(value) && Object.keys(value).every(key => behaviorKeys.includes(key)), 'Unsupported Worker deployment behavior');
  validateCache(value.cache);
  if (value.exports === undefined) return;
  assert(plainObject(value.exports) && Object.keys(value.exports).length <= 128, 'Worker exports must be a map of at most 128 entrypoints');
  for (const [name, entrypoint] of Object.entries(value.exports)) {
    assert(/^[A-Za-z_$][A-Za-z0-9_$]{0,127}$(?![\s\S])/.test(name), 'Invalid Worker export name');
    assert(plainObject(entrypoint) && entrypoint.type === 'worker' && Object.keys(entrypoint).every(key => ['type', 'cache'].includes(key)), 'Only Worker entrypoint exports with cache configuration are supported');
    validateCache(entrypoint.cache, true);
  }
}

export const sameBehaviorValue = (key, left, right) => isDeepStrictEqual(left, right) ||
  (key === 'exports' && Object.keys(left ?? {}).length === 0 && Object.keys(right ?? {}).length === 0);

function resourceBindingNames(bindings) {
  const names = new Set();
  const visit = value => {
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value)) return value.forEach(visit);
    if (typeof value.binding === 'string') names.add(value.binding);
    for (const child of Object.values(value)) visit(child);
  };
  for (const key of bindingKeys) visit(bindings[key]);
  for (const item of bindings.durable_objects?.bindings ?? []) if (typeof item.name === 'string') names.add(item.name);
  for (const item of bindings.ratelimits ?? []) if (typeof item.name === 'string') names.add(item.name);
  return names;
}

export function normalizeProjectSlug(value) {
  const slug = value === undefined ? '' : value;
  assert(typeof slug === 'string' && !/[^a-zA-Z0-9_-]/.test(slug), 'Expected an optional, valid Infisical project slug');
  return slug;
}

export function assertNoInfisicalValues(project, slug) {
  if (slug) return;
  assert(project && Array.isArray(project.build_variables) && Array.isArray(project.runtime_secrets) &&
    project.build_variables.length === 0 && project.runtime_secrets.length === 0,
  'Omitting project_slug requires no build variables or runtime secrets');
}

// A PR head branch: the brief's character set, plus Git's ref rules for
// `..`, empty or dot-leading components, `.lock`, and a leading `-`.
function validPullRequestBranch(branch) {
  return typeof branch === 'string' && /^[A-Za-z0-9._/-]{1,200}$/.test(branch) && !branch.includes('..') &&
    !branch.startsWith('-') && !branch.endsWith('.') &&
    branch.split('/').every(part => part.length > 0 && !part.startsWith('.') && !part.endsWith('.lock'));
}

const pullRequestActions = ['opened', 'synchronize', 'reopened', 'ready_for_review', 'closed', 'workflow_dispatch'];

export function validateDispatch(payload) {
  assert(payload && /^Lascade-Co\/[A-Za-z0-9_.-]+$/.test(payload.repo), 'Only Lascade-Co repositories are supported');
  // A present pr_number always means a PR Preview. It never falls back to a
  // dev or main deploy, even when the PR head branch is dev or main.
  const pullRequest = Object.hasOwn(payload, 'pr_number');
  if (pullRequest) {
    assert(Number.isInteger(payload.pr_number) && payload.pr_number >= 1 && payload.pr_number <= 9999999, 'pr_number must be an integer from 1 to 9999999');
    assert(validPullRequestBranch(payload.branch), 'Invalid pull request head branch');
  } else {
    assert(['dev', 'main'].includes(payload.branch), 'Only dev and main can deploy');
  }
  // pr_action exists only on PR dispatches; `closed` routes to Preview cleanup.
  if (Object.hasOwn(payload, 'pr_action')) assert(pullRequest && pullRequestActions.includes(payload.pr_action), 'Invalid pr_action');
  assert(/^[a-f0-9]{40}$/.test(payload.sha), 'An exact 40-character commit SHA is required');
  assert(typeof payload.source_run_url === 'string' && payload.source_run_url.startsWith(`https://github.com/${payload.repo}/actions/runs/`) && /^\d+$/.test(payload.source_run_url.split('/').at(-1)), 'Invalid source run URL');
  const project_slug = normalizeProjectSlug(payload.project_slug);
  const [owner, repo_name] = payload.repo.split('/');
  return { owner, repo_name, project_slug, ...(pullRequest ? { pr_number: payload.pr_number } : {}) };
}

export function dispatchTarget(payload) {
  const { pr_number } = validateDispatch(payload);
  return pr_number !== undefined ? 'pr' : payload.branch === 'main' ? 'production' : 'staging';
}

// A closed PR only deletes its Preview; every other dispatch builds and deploys.
export function dispatchRoute(payload) {
  const { pr_number } = validateDispatch(payload);
  return pr_number !== undefined && payload.pr_action === 'closed' ? 'cleanup' : 'deploy';
}

// The Wrangler/Infisical environment a target is built from. A PR Preview is
// built exactly like staging; only its Preview name and freshness check differ.
export const sourceEnvironment = target => target === 'pr' ? 'staging' : target;

export function statusPlan(payload) {
  const { pr_number } = validateDispatch(payload);
  const target = dispatchTarget(payload);
  return { repo: payload.repo, sha: payload.sha, target, ...(target === 'pr' ? { pr_number } : {}) };
}

export function prepare(payload, project) {
  const { owner, repo_name, project_slug: slug, pr_number } = validateDispatch(payload);
  const target = dispatchTarget(payload);
  assert(dispatchRoute(payload) === 'deploy', 'A closed pull request only deletes its Preview');
  assert(project && /^[a-f0-9]{32}$/.test(project.account_id) && /^[a-z0-9][a-z0-9-]{0,62}$/.test(project.worker_name), 'Project must register a valid account and Worker');
  assert(/^[0-9]+(?:\.[0-9]+){0,2}$/.test(project.node_version) && Number(project.node_version.split('.')[0]) >= 22, 'Vinext requires Node 22 or newer');
  assert(/^[0-9]+\.[0-9]+\.[0-9]+$/.test(project.wrangler_version), 'Pin an exact Wrangler version');
  assert(['alias', 'native'].includes(project.preview_mode ?? 'alias'), 'Unsupported Preview mode');
  assert(target !== 'pr' || project.preview_mode === 'native', 'PR Previews require a native previews block in the source Wrangler configuration');
  if (project.preview_mode === 'native') {
    const [major, minor] = project.wrangler_version.split('.').map(Number);
    assert(major > 4 || (major === 4 && minor >= 135), 'Native Previews require Wrangler 4.135.0 or later');
    if (target === 'pr') assert(project.preview_name === `pr-${pr_number}`, 'PR Preview must use the pr-<number> name');
    else assert(project.preview_name === 'stg', 'Dev Preview must use the stg name');
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
  const assetBinding = project.asset_binding ?? 'ASSETS';
  assert(typeof assetBinding === 'string' && variable.test(assetBinding), 'Source must declare a valid asset binding');
  const environmentName = sourceEnvironment(target);
  assert(project.environments?.production, 'Missing production environment');
  assert(project.environments[environmentName], `No ${environmentName} environment is configured`);
  for (const name of ['staging', 'production']) {
    const env = project.environments[name];
    if (!env) continue;
    assert(/^[a-zA-Z0-9_-]+$/.test(env.infisical_env), 'Missing Infisical environment mapping');
    assert(typeof env.infisical_path === 'string' && /^\/[a-zA-Z0-9_/-]*$/.test(env.infisical_path) && !env.infisical_path.includes('//'), 'Invalid Infisical path');
    assert(env.bindings && typeof env.bindings === 'object' && !Array.isArray(env.bindings) && Object.keys(env.bindings).every(k => bindingKeys.includes(k)), 'Unsupported resource binding configuration');
    validateRateLimits(env.bindings.ratelimits);
    validateWorkerBehavior(env.deployment_behavior ?? {});
  }
  const stageKv = project.environments.staging?.bindings.kv_namespaces ?? [];
  const prodKv = project.environments.production.bindings.kv_namespaces ?? [];
  assert(stageKv.every(a => !prodKv.some(b => a.id === b.id)), 'Staging and production must use distinct KV namespaces');
  const resourceNames = new Set([assetBinding]);
  for (const environment of Object.values(project.environments)) for (const name of resourceBindingNames(environment.bindings)) resourceNames.add(name);
  assert(project.runtime_secrets.every(name => !resourceNames.has(name)), 'Runtime secret name collides with a Worker resource binding');
  assertNoInfisicalValues(project, slug);
  const environment = project.environments[environmentName];
  // An unmerged PR's SQL must never run against a shared (or production) D1 database.
  if (target === 'pr') {
    const databases = environment.bindings.d1_databases;
    assert(databases === undefined || (Array.isArray(databases) && databases.every(database => database?.migrations_dir === undefined)), 'PR Previews cannot apply D1 migrations');
  }
  return { repo: payload.repo, branch: payload.branch, sha: payload.sha, source_run_url: payload.source_run_url, owner, repo_name, project, target, ...(target === 'pr' ? { pr_number } : {}), infisical_project_slug: slug, infisical_env: environment.infisical_env, infisical_path: environment.infisical_path };
}

const empty = value => value == null || (Array.isArray(value) ? value.length === 0 : typeof value === 'object' && Object.values(value).every(empty));

// Vinext rebases migration paths relative to its generated config. They are
// local migration-tool metadata, never part of a deployment-only bundle.
function deploymentBindings(bindings) {
  if (!Array.isArray(bindings.d1_databases)) return bindings;
  return { ...bindings, d1_databases: bindings.d1_databases.map(database => {
    const result = { ...database };
    delete result.migrations_dir;
    return result;
  }) };
}

// Only deployment data crosses the build/deploy boundary. Never run build hooks
// or use local files referenced by a caller-controlled Wrangler configuration.
export function sanitizeConfig(config, plan) {
  const p = plan.project;
  assert(config.name === p.worker_name && config.account_id === p.account_id, 'Built account or Worker differs from registered destination');
  assert(config.targetEnvironment === undefined || config.targetEnvironment === sourceEnvironment(plan.target), 'Wrong build target');
  for (const key of ['build', 'alias', 'env', 'migrations', 'unsafe', 'containers']) assert(empty(config[key]), `Unsupported generated config: ${key}`);
  assert(config.no_bundle === true, 'Vinext must emit a prebuilt no_bundle Worker');
  // workers_dev may be off once a Worker serves a real route (e.g. after a domain cutover);
  // Previews only need preview_urls, which stays mandatory.
  assert(typeof config.workers_dev === 'boolean' && config.preview_urls === true, 'Set workers_dev explicitly and enable preview_urls');
  assert(typeof config.main === 'string' && /\.(m?js)$/.test(config.main), 'Missing built Worker entry');
  assert(config.assets && typeof config.assets.directory === 'string', 'Missing built assets');
  if (p.preview_mode === 'native') assert(config.assets.binding === (p.asset_binding ?? 'ASSETS') && !p.runtime_secrets.includes(config.assets.binding), 'Built asset binding differs from source or collides with a runtime secret');
  for (const key of p.runtime_secrets) assert(!Object.hasOwn(config.vars ?? {}, key), 'Runtime secret found in plaintext vars');
  // Native Preview bundles are validated again after sanitization. Their
  // top-level bindings have intentionally been restored to production while
  // the staging bindings remain under `previews`.
  const bindingTarget = p.preview_mode === 'native' && config.targetEnvironment === undefined ? 'production' : sourceEnvironment(plan.target);
  const expected = deploymentBindings(p.environments[bindingTarget].bindings);
  const generated = deploymentBindings(config);
  for (const key of bindingKeys) {
    assert(isDeepStrictEqual(generated[key], expected[key]) || (empty(generated[key]) && empty(expected[key])), `Unexpected ${bindingTarget} resource binding: ${key}`);
  }
  const expectedBehavior = p.environments[bindingTarget].deployment_behavior ?? {};
  const generatedBehavior = Object.fromEntries(behaviorKeys.filter(key => config[key] !== undefined).map(key => [key, config[key]]));
  validateWorkerBehavior(generatedBehavior);
  for (const key of behaviorKeys) assert(sameBehaviorValue(key, config[key], expectedBehavior[key]), `Unexpected ${bindingTarget} Worker behavior: ${key}`);
  const result = {};
  for (const key of ['name', 'account_id', 'compatibility_date', 'compatibility_flags', 'main', 'assets', 'rules', 'vars', 'workers_dev', 'preview_urls', 'observability', 'limits', 'placement', 'jsx_factory', 'jsx_fragment', ...bindingKeys]) {
    if (generated[key] !== undefined) result[key] = generated[key];
  }
  // Wrangler normalizes absent rate bindings to []; keep existing bundles unchanged.
  if (expected.ratelimits === undefined && empty(generated.ratelimits)) delete result.ratelimits;
  result.no_bundle = true;
  Object.assign(result, structuredClone(expectedBehavior));
  if (p.preview_mode === 'native') {
    assert(config.vars === undefined || (config.vars && typeof config.vars === 'object' && !Array.isArray(config.vars) && Object.keys(config.vars).length === 0), 'Generated native Preview config must not declare plaintext vars');
    assert(isDeepStrictEqual(config.previews && deploymentBindings(config.previews), deploymentBindings(p.preview_config)), 'Generated Preview configuration differs from source');
    const production = deploymentBindings(p.environments.production.bindings);
    for (const key of bindingKeys) {
      if (production[key] === undefined) delete result[key];
      else result[key] = production[key];
    }
    result.previews = deploymentBindings(p.preview_config);
    const productionBehavior = p.environments.production.deployment_behavior ?? {};
    for (const key of behaviorKeys) {
      if (productionBehavior[key] === undefined) delete result[key];
      else result[key] = structuredClone(productionBehavior[key]);
    }
  } else {
    assert(empty(config.previews), 'Legacy alias build must not configure native Previews');
  }
  // Domains/routes are managed outside this runner; omit instead of clearing.
  result.secrets = { required: p.runtime_secrets };
  return result;
}
