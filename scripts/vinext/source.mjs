import { assert, bindingKeys, validateDispatch } from './config.mjs';

const noPlainVars = source => source.vars === undefined || (source.vars && typeof source.vars === 'object' && !Array.isArray(source.vars) && Object.keys(source.vars).length === 0);

async function fileAtCommit(payload, github, name) {
  const file = await github(`/repos/${payload.repo}/contents/${name}?ref=${payload.sha}`);
  assert(file?.type === 'file' && file.encoding === 'base64' && typeof file.content === 'string' && file.size > 0 && file.size <= 100000, `Missing or oversized source ${name}`);
  const raw = Buffer.from(file.content.replaceAll('\n', ''), 'base64');
  assert(raw.length === file.size, `Source ${name} size mismatch`);
  return raw.toString('utf8');
}

function submoduleEntries(text) {
  const entries = [];
  let current;
  for (const line of text.split(/\r?\n/)) {
    const value = line.trim();
    if (!value || value.startsWith('#') || value.startsWith(';')) continue;
    if (value.startsWith('[')) {
      assert(/^\[submodule "[^"\r\n]+"\]$/.test(value), 'Invalid source .gitmodules section');
      if (current) entries.push(current);
      current = {};
      continue;
    }
    assert(current, 'Invalid source .gitmodules entry');
    const setting = /^([A-Za-z][A-Za-z0-9]*)\s*=\s*(\S+)\s*$/.exec(value);
    assert(setting, 'Invalid source .gitmodules setting');
    if (setting[1] === 'path' || setting[1] === 'url') {
      assert(current[setting[1]] === undefined, 'Duplicate source .gitmodules setting');
      current[setting[1]] = setting[2];
    }
  }
  if (current) entries.push(current);
  return entries.map(({ path, url }) => {
    assert(typeof path === 'string' && /^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/.test(path) && !path.split('/').includes('..'), 'Invalid source submodule path');
    const match = /^(?:git@github\.com:|https:\/\/github\.com\/)Lascade-Co\/([A-Za-z0-9_.-]+?)(?:\.git)?$/.exec(url ?? '');
    assert(match && match[1] !== '.' && match[1] !== '..', 'Submodules must use same-organization GitHub URLs');
    return { path, repo: match[1] };
  });
}

async function assertGitlink(payload, github, rootTree, path) {
  let tree = rootTree;
  const parts = path.split('/');
  for (const [index, part] of parts.entries()) {
    const entry = tree.tree.find(item => item.path === part);
    assert(entry, 'Source .gitmodules path is not a pinned submodule');
    if (index === parts.length - 1) {
      assert(entry.mode === '160000' && entry.type === 'commit', 'Source .gitmodules path is not a pinned submodule');
    } else {
      assert(entry.type === 'tree' && /^[a-f0-9]{40}$/.test(entry.sha), 'Invalid source submodule parent');
      tree = await github(`/repos/${payload.repo}/git/trees/${entry.sha}`);
      assert(Array.isArray(tree?.tree) && tree.truncated !== true, 'Invalid source submodule parent tree');
    }
  }
}

// These checked-in files are read at the exact commit in the dispatch. They
// contain the Worker destination and package tooling; no deploy manifest exists.
export async function fetchSourceFiles(payload, github) {
  validateDispatch(payload);
  const [pkgText, wranglerText, commit] = await Promise.all([
    fileAtCommit(payload, github, 'package.json'),
    fileAtCommit(payload, github, 'wrangler.jsonc'),
    github(`/repos/${payload.repo}/git/commits/${payload.sha}`),
  ]);
  assert(/^[a-f0-9]{40}$/.test(commit?.tree?.sha ?? ''), 'Invalid source commit tree');
  const rootTree = await github(`/repos/${payload.repo}/git/trees/${commit.tree.sha}`);
  assert(Array.isArray(rootTree?.tree) && rootTree.truncated !== true, 'Invalid source tree');
  const gitmodules = rootTree.tree.find(entry => entry.path === '.gitmodules');
  assert(!gitmodules || (gitmodules.type === 'blob' && gitmodules.mode === '100644'), 'Invalid source .gitmodules');
  const entries = gitmodules ? submoduleEntries(await fileAtCommit(payload, github, '.gitmodules')) : [];
  await Promise.all(entries.map(entry => assertGitlink(payload, github, rootTree, entry.path)));
  const submodule_repos = [...new Set(entries.map(entry => entry.repo))];
  // The source Wrangler file must use the JSON subset of JSONC. All supported
  // apps already do; rejecting comments avoids a hand-written lossy parser.
  try { return { pkg: JSON.parse(pkgText), wrangler: JSON.parse(wranglerText), submodule_repos }; }
  catch { throw new Error('Source package.json or wrangler.jsonc is not valid JSON'); }
}

export function projectFromSource(payload, { pkg, wrangler, submodule_repos }, values) {
  validateDispatch(payload);
  assert(pkg && typeof pkg === 'object' && wrangler && typeof wrangler === 'object', 'Missing source package or Wrangler configuration');
  const manager = /^((?:pnpm|npm|yarn))@[0-9]+\.[0-9]+\.[0-9]+(?:\+sha[0-9]+\.[a-f0-9]+)?$/.exec(pkg.packageManager ?? '')?.[1];
  assert(manager, 'Pin packageManager in source package.json');
  const wranglerVersion = pkg.devDependencies?.wrangler ?? pkg.dependencies?.wrangler;
  assert(typeof wranglerVersion === 'string' && /^[0-9]+\.[0-9]+\.[0-9]+$/.test(wranglerVersion), 'Pin an exact Wrangler version in package.json');
  assert(typeof pkg.scripts?.['check:deploy'] === 'string' && typeof pkg.scripts?.['build:vinext'] === 'string', 'Provide check:deploy and build:vinext scripts');
  assert(Array.isArray(submodule_repos) && submodule_repos.every(x => /^[A-Za-z0-9_.-]+$/.test(x)), 'Invalid source submodule repositories');
  const keys = Object.keys(values);
  const build = keys.filter(k => k.startsWith('NEXT_PUBLIC_'));
  let runtime = keys.filter(k => !k.startsWith('NEXT_PUBLIC_') && !k.startsWith('VINEXT_'));
  assert(build.every(k => values[k].trim()), 'Empty NEXT_PUBLIC_ value in Infisical');
  const topName = wrangler.name, topAccount = wrangler.account_id;
  assert(typeof topName === 'string' && typeof topAccount === 'string', 'Source Wrangler must declare Worker and account');
  const nativePreview = wrangler.previews !== undefined;
  if (nativePreview) {
    assert(wrangler.previews && typeof wrangler.previews === 'object' && !Array.isArray(wrangler.previews), 'Source previews must be an object');
    assert(noPlainVars(wrangler) && noPlainVars(wrangler.previews), 'Native Preview source must not declare plaintext vars; use Infisical');
    const [major, minor] = wranglerVersion.split('.').map(Number);
    assert(major > 4 || (major === 4 && minor >= 135), 'Native Previews require Wrangler 4.135.0 or later');
    const required = wrangler.secrets?.required;
    assert(Array.isArray(required) && required.length > 0 && new Set(required).size === required.length && required.every(key => typeof key === 'string' && /^[A-Z][A-Z0-9_]*$/.test(key) && !key.startsWith('NEXT_PUBLIC_') && !key.startsWith('VINEXT_')), 'Native Previews require source-declared runtime secret names');
    assert(JSON.stringify(wrangler.previews.secrets?.required) === JSON.stringify(required), 'Production and Preview must declare the same required runtime secret names');
    for (const key of required) assert(typeof values[key] === 'string' && values[key].trim(), `Missing Infisical value: ${key}`);
    runtime = required;
  }
  const environments = {};
  for (const [target, infisical_env] of [['staging', 'staging'], ['production', 'prod']]) {
    const source = wrangler.env?.[target];
    if (!source) continue;
    if (nativePreview) assert(noPlainVars(source), 'Native Preview source environments must not declare plaintext vars; use Infisical');
    assert(source.name === topName && (source.account_id ?? topAccount) === topAccount, 'All source environments must target the same Worker and account');
    const bindings = Object.fromEntries(bindingKeys.filter(key => source[key] !== undefined).map(key => [key, source[key]]));
    environments[target] = { infisical_env, infisical_path: '/', bindings };
  }
  const target = payload.branch === 'main' ? 'production' : 'staging';
  assert(environments[target], `Source Wrangler has no ${target} environment`);
  if (nativePreview) {
    assert(environments.staging && environments.production, 'Native Previews require staging and production source environments for Vinext builds');
    const previewBindings = Object.fromEntries(bindingKeys.filter(key => wrangler.previews[key] !== undefined).map(key => [key, wrangler.previews[key]]));
    const productionBindings = Object.fromEntries(bindingKeys.filter(key => wrangler[key] !== undefined).map(key => [key, wrangler[key]]));
    assert(Object.keys(wrangler.previews).every(key => bindingKeys.includes(key) || ['vars', 'secrets', 'compatibility_date', 'compatibility_flags', 'observability', 'limits', 'placement'].includes(key)), 'Unsupported native Preview configuration');
    environments.staging.bindings = previewBindings;
    environments.production.bindings = productionBindings;
  }
  return {
    account_id: topAccount, worker_name: topName, node_version: '24',
    package_manager: manager, wrangler_version: wranglerVersion,
    working_directory: '.', bundle_directory: 'dist', generated_config: 'dist/server/wrangler.json',
    build_script: 'build:vinext', check_scripts: ['check:deploy'], submodule_repos,
    build_variables: build, runtime_secrets: runtime,
    preview_mode: nativePreview ? 'native' : 'alias', preview_name: nativePreview ? 'stg' : undefined,
    preview_config: nativePreview ? wrangler.previews : undefined,
    environments,
  };
}
