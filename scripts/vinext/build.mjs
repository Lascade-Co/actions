import { cpSync, existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { assert, inside, sanitizeConfig } from './config.mjs';

export function readSecrets(file) {
  try {
    let values;
    try { values = JSON.parse(readFileSync(file, 'utf8')); }
    catch { throw new Error('Invalid Infisical JSON export'); }
    assert(values && typeof values === 'object' && !Array.isArray(values) && Object.values(values).every(v => typeof v === 'string'), 'Infisical export must contain string values');
    if (process.env.GITHUB_ACTIONS) for (const value of Object.values(values)) {
      if (value) console.log(`::add-mask::${value.replaceAll('%', '%25').replaceAll('\r', '%0D').replaceAll('\n', '%0A')}`);
    }
    return values;
  } finally { rmSync(file, { force: true }); }
}

export function selectValues(values, names, required = names) {
  for (const key of required) assert(typeof values[key] === 'string' && values[key].trim(), `Missing Infisical value: ${key}`);
  return Object.fromEntries(names.filter(k => Object.hasOwn(values, k)).map(k => [k, values[k]]));
}

export function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: 'inherit', ...options });
  if (result.error) throw result.error;
  assert(result.status === 0, `${command} failed (${result.status ?? result.signal})`);
  return result.stdout;
}

const environmentFile = name => /^(?:\.env(?:\.|$)|\.dev\.vars(?:\.|$))/.test(name);

export function inspectTree(root, ignoreEnvironmentFiles = false) {
  assert(!lstatSync(root).isSymbolicLink(), 'Bundle must not contain symlinks');
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    assert(!entry.isSymbolicLink(), 'Bundle must not contain symlinks');
    if (ignoreEnvironmentFiles && entry.isFile() && environmentFile(entry.name)) continue;
    assert(!environmentFile(entry.name) && !['.git', 'node_modules'].includes(entry.name), 'Bundle includes environment files or dependencies');
    const path = join(root, entry.name);
    if (entry.isDirectory()) inspectTree(path, ignoreEnvironmentFiles);
    else assert(entry.isFile(), 'Bundle must contain only ordinary files');
  }
}

export function validateBundle(root, plan, ignoreEnvironmentFiles = false) {
  inspectTree(root, ignoreEnvironmentFiles);
  const configRel = relative(plan.project.bundle_directory, plan.project.generated_config);
  const configPath = inside(root, configRel);
  const config = sanitizeConfig(JSON.parse(readFileSync(configPath, 'utf8')), plan);
  const entry = inside(root, relative(root, resolve(dirname(configPath), config.main)));
  const assets = inside(root, relative(root, resolve(dirname(configPath), config.assets.directory)));
  assert(lstatSync(entry).isFile() && lstatSync(assets).isDirectory(), 'Worker entry or assets missing');
  return { config, configPath };
}

export function build(plan, { appRoot, envFile, bundleRoot, execute = run, env = process.env }) {
  const p = plan.project;
  const values = readSecrets(envFile);
  const buildValues = selectValues(values, p.build_variables, p.required_build_variables);
  const cwd = inside(appRoot, p.working_directory);
  const pkg = JSON.parse(readFileSync(join(cwd, 'package.json'), 'utf8'));
  assert(new RegExp(`^${p.package_manager}@[0-9]+\\.[0-9]+\\.[0-9]+(?:\\+sha[0-9]+\\.[a-f0-9]+)?$`).test(pkg.packageManager), 'Set packageManager to a pinned version matching central configuration');
  const locks = { pnpm: 'pnpm-lock.yaml', npm: 'package-lock.json', yarn: 'yarn.lock' };
  assert(existsSync(join(cwd, locks[p.package_manager])), 'Missing frozen dependency lockfile');
  for (const [manager, lock] of Object.entries(locks)) assert(manager === p.package_manager || !existsSync(join(cwd, lock)), 'Conflicting package manager lockfiles');
  for (const name of [...p.check_scripts, p.build_script]) assert(typeof pkg.scripts?.[name] === 'string', `Missing package script: ${name}`);
  // This is a fresh CI checkout. Do not let committed dotenv files select a
  // different backend or override the explicitly selected Infisical target.
  for (const name of readdirSync(cwd)) if (/^(?:\.env(?:\.|$)|\.dev\.vars(?:\.|$))/.test(name)) rmSync(join(cwd, name), { force: true });
  const childEnv = { ...env, ...buildValues, CI: 'true', CLOUDFLARE_ENV: plan.target };
  for (const key of Object.keys(childEnv)) {
    if (/^(?:CLOUDFLARE_API_|INFISICAL_|GH_TOKEN$|GITHUB_TOKEN$)/.test(key) || p.runtime_secrets.includes(key)) delete childEnv[key];
  }
  const options = { cwd, env: childEnv };
  const install = p.package_manager === 'npm' ? ['ci'] : p.package_manager === 'yarn' ? ['install', Number(pkg.packageManager.split('@')[1].split('.')[0]) === 1 ? '--frozen-lockfile' : '--immutable'] : ['install', '--frozen-lockfile'];
  execute(p.package_manager, install, options);
  for (const name of [...p.check_scripts, p.build_script]) execute(p.package_manager, ['run', name], options);
  const source = inside(cwd, p.bundle_directory);
  const { config, configPath } = validateBundle(source, plan, true);
  mkdirSync(bundleRoot, { recursive: true });
  cpSync(source, bundleRoot, { recursive: true, dereference: false, filter: path => !environmentFile(basename(path)) });
  writeFileSync(inside(bundleRoot, relative(source, configPath)), JSON.stringify(config));
  writeFileSync(join(bundleRoot, 'vinext-release.json'), JSON.stringify({ repo: plan.repo, sha: plan.sha, target: plan.target, worker: p.worker_name }));
  validateBundle(bundleRoot, plan);
}
