import { assert, bindingKeys, validateDispatch } from './config.mjs';

async function fileAtCommit(payload, github, name) {
  const file = await github(`/repos/${payload.repo}/contents/${name}?ref=${payload.sha}`);
  assert(file?.type === 'file' && file.encoding === 'base64' && typeof file.content === 'string' && file.size > 0 && file.size <= 100000, `Missing or oversized source ${name}`);
  const raw = Buffer.from(file.content.replaceAll('\n', ''), 'base64');
  assert(raw.length === file.size, `Source ${name} size mismatch`);
  return raw.toString('utf8');
}

// These checked-in files are read at the exact commit in the dispatch. They
// contain the Worker destination and package tooling; no deploy manifest exists.
export async function fetchSourceFiles(payload, github) {
  validateDispatch(payload);
  const [pkgText, wranglerText] = await Promise.all([
    fileAtCommit(payload, github, 'package.json'),
    fileAtCommit(payload, github, 'wrangler.jsonc'),
  ]);
  // The source Wrangler file must use the JSON subset of JSONC. All supported
  // apps already do; rejecting comments avoids a hand-written lossy parser.
  try { return { pkg: JSON.parse(pkgText), wrangler: JSON.parse(wranglerText) }; }
  catch { throw new Error('Source package.json or wrangler.jsonc is not valid JSON'); }
}

const variable = /^[A-Z][A-Z0-9_]*$/;
const names = value => {
  assert(typeof value === 'string', 'Missing Infisical Vinext secret-name metadata');
  const result = value ? value.split(',').map(x => x.trim()) : [];
  assert(result.every(x => variable.test(x)) && new Set(result).size === result.length, 'Invalid Infisical Vinext secret-name metadata');
  return result;
};

export function projectFromSource(payload, { pkg, wrangler }, values) {
  validateDispatch(payload);
  assert(pkg && typeof pkg === 'object' && wrangler && typeof wrangler === 'object', 'Missing source package or Wrangler configuration');
  const manager = /^((?:pnpm|npm|yarn))@[0-9]+\.[0-9]+\.[0-9]+(?:\+sha[0-9]+\.[a-f0-9]+)?$/.exec(pkg.packageManager ?? '')?.[1];
  assert(manager, 'Pin packageManager in source package.json');
  const wranglerVersion = pkg.devDependencies?.wrangler ?? pkg.dependencies?.wrangler;
  assert(typeof wranglerVersion === 'string' && /^[0-9]+\.[0-9]+\.[0-9]+$/.test(wranglerVersion), 'Pin an exact Wrangler version in package.json');
  assert(typeof pkg.scripts?.['check:deploy'] === 'string' && typeof pkg.scripts?.['build:vinext'] === 'string', 'Provide check:deploy and build:vinext scripts');
  assert(values.VINEXT_SOURCE_REPOSITORY === payload.repo, 'Infisical project does not authorize this source repository');
  const submodulesRaw = values.VINEXT_SUBMODULE_REPOS ?? '';
  assert(typeof submodulesRaw === 'string', 'Invalid Infisical submodule allowlist');
  const submodules = submodulesRaw ? submodulesRaw.split(',').map(x => x.trim()) : [];
  assert(submodules.every(x => /^[A-Za-z0-9_.-]+$/.test(x)) && new Set(submodules).size === submodules.length, 'Invalid Infisical submodule allowlist');
  const runtime = names(values.VINEXT_RUNTIME_SECRETS);
  const required = names(values.VINEXT_REQUIRED_BUILD_VARIABLES);
  assert(runtime.every(k => !k.startsWith('NEXT_PUBLIC_') && Object.hasOwn(values, k)), 'Missing declared runtime secret in Infisical');
  const build = Object.keys(values).filter(k => k.startsWith('NEXT_PUBLIC_') && variable.test(k));
  assert(required.every(k => k.startsWith('NEXT_PUBLIC_') && build.includes(k) && values[k].trim()), 'Missing required public build value in Infisical');
  const topName = wrangler.name, topAccount = wrangler.account_id;
  assert(typeof topName === 'string' && typeof topAccount === 'string', 'Source Wrangler must declare Worker and account');
  assert(values.VINEXT_WORKER_NAME === topName && values.VINEXT_ACCOUNT_ID === topAccount, 'Infisical project does not authorize this Worker destination');
  const environments = {};
  for (const [target, infisical_env] of [['staging', 'staging'], ['production', 'prod']]) {
    const source = wrangler.env?.[target];
    if (!source) continue;
    assert(source.name === topName && (source.account_id ?? topAccount) === topAccount, 'All source environments must target the same Worker and account');
    const bindings = Object.fromEntries(bindingKeys.filter(key => source[key] !== undefined).map(key => [key, source[key]]));
    environments[target] = { infisical_env, infisical_path: '/', bindings };
  }
  const target = payload.branch === 'main' ? 'production' : 'staging';
  assert(environments[target], `Source Wrangler has no ${target} environment`);
  return {
    account_id: topAccount, worker_name: topName, node_version: '24',
    package_manager: manager, wrangler_version: wranglerVersion,
    working_directory: '.', bundle_directory: 'dist', generated_config: 'dist/server/wrangler.json',
    build_script: 'build:vinext', check_scripts: ['check:deploy'], submodule_repos: submodules,
    build_variables: build, required_build_variables: required, runtime_secrets: runtime,
    environments,
  };
}
