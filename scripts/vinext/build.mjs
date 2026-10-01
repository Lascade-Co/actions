import { cpSync, existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
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
const migrationRootName = 'vinext-d1-migrations';
const migrationManifestName = 'vinext-d1-migrations.json';
const migrationFileName = /^[A-Za-z0-9][A-Za-z0-9_.-]*\.sql$/;

function targetMigrationDatabases(plan) {
  const databases = plan.project.environments[plan.target].bindings.d1_databases ?? [];
  assert(Array.isArray(databases), 'D1 bindings must be an array');
  const configured = databases.filter(database => database.migrations_dir !== undefined);
  if (!configured.length) return [];
  const bindings = new Set(), identities = new Set();
  for (const database of databases) {
    assert(database && typeof database === 'object' && !Array.isArray(database), 'Invalid D1 migration binding');
    assert(/^[A-Za-z_][A-Za-z0-9_]*$/.test(database.binding ?? ''), 'Invalid D1 migration binding name');
    assert(typeof database.database_name === 'string' && database.database_name.length > 0, 'Missing D1 migration database name');
    assert(/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(database.database_id ?? ''), 'Missing D1 migration database ID');
    assert(!bindings.has(database.binding), 'Duplicate D1 migration binding');
    assert(!identities.has(database.database_id), 'A migrated D1 database may only be bound once per target');
    bindings.add(database.binding);
    identities.add(database.database_id);
  }
  for (const database of configured) {
    assert(typeof database.migrations_dir === 'string' && database.migrations_dir.length > 0, 'Invalid D1 migrations_dir');
    assert(database.migrations_pattern === undefined, 'D1 migrations_pattern is not supported; use top-level SQL files');
    assert(database.migrations_table === undefined || (typeof database.migrations_table === 'string' && database.migrations_table.length > 0 && database.migrations_table.length <= 128), 'Invalid D1 migrations_table');
  }
  return configured.toSorted((a, b) => a.binding.localeCompare(b.binding));
}

const digest = contents => createHash('sha256').update(contents).digest('hex');

export function stageD1Migrations(cwd, bundleRoot, plan) {
  const databases = targetMigrationDatabases(plan);
  if (!databases.length) return [];
  const migrationRoot = join(bundleRoot, migrationRootName);
  const manifestPath = join(bundleRoot, migrationManifestName);
  assert(!existsSync(migrationRoot) && !existsSync(manifestPath), 'Bundle collides with reserved D1 migration paths');
  mkdirSync(migrationRoot);
  const manifest = { version: 1, databases: [] };
  let totalSize = 0;
  for (const database of databases) {
    const source = inside(realpathSync(cwd), database.migrations_dir);
    const sourceStat = lstatSync(source);
    assert(sourceStat.isDirectory() && !sourceStat.isSymbolicLink(), 'D1 migrations_dir must be an ordinary directory');
    assert(realpathSync(source) === source, 'D1 migrations_dir must not traverse symlinks');
    const entries = readdirSync(source, { withFileTypes: true }).toSorted((a, b) => a.name.localeCompare(b.name));
    assert(entries.length > 0 && entries.length <= 100, 'D1 migrations_dir must contain 1-100 SQL files');
    const directory = `${migrationRootName}/${database.binding}`;
    const target = inside(bundleRoot, directory);
    mkdirSync(target);
    const files = [];
    for (const entry of entries) {
      assert(entry.isFile() && !entry.isSymbolicLink() && migrationFileName.test(entry.name), 'D1 migrations_dir may contain only top-level .sql files');
      const contents = readFileSync(join(source, entry.name));
      assert(contents.length > 0 && contents.length <= 1024 * 1024, 'D1 migration file must be 1 byte to 1 MiB');
      totalSize += contents.length;
      assert(totalSize <= 10 * 1024 * 1024, 'D1 migration files exceed 10 MiB');
      writeFileSync(join(target, entry.name), contents, { flag: 'wx' });
      files.push({ name: entry.name, size: contents.length, sha256: digest(contents) });
    }
    manifest.databases.push({
      binding: database.binding,
      database_name: database.database_name,
      database_id: database.database_id,
      ...(database.migrations_table === undefined ? {} : { migrations_table: database.migrations_table }),
      directory,
      files,
    });
  }
  writeFileSync(manifestPath, JSON.stringify(manifest), { flag: 'wx' });
  return manifest.databases;
}

export function validateD1Migrations(bundleRoot, plan) {
  const expected = targetMigrationDatabases(plan);
  const manifestPath = join(bundleRoot, migrationManifestName);
  const migrationRoot = join(bundleRoot, migrationRootName);
  if (!expected.length) {
    assert(!existsSync(manifestPath) && !existsSync(migrationRoot), 'Unexpected D1 migration artifact');
    return [];
  }
  assert(lstatSync(manifestPath).isFile() && !lstatSync(manifestPath).isSymbolicLink(), 'Missing D1 migration manifest');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  assert(manifest?.version === 1 && Array.isArray(manifest.databases) && manifest.databases.length === expected.length, 'Invalid D1 migration manifest');
  let totalSize = 0;
  for (const [index, database] of manifest.databases.entries()) {
    const source = expected[index];
    assert(database.binding === source.binding && database.database_name === source.database_name && database.database_id === source.database_id && database.migrations_table === source.migrations_table, 'D1 migration manifest differs from source configuration');
    assert(database.directory === `${migrationRootName}/${source.binding}` && Array.isArray(database.files) && database.files.length > 0 && database.files.length <= 100, 'Invalid D1 migration directory manifest');
    const directory = inside(bundleRoot, database.directory);
    const entries = readdirSync(directory, { withFileTypes: true }).toSorted((a, b) => a.name.localeCompare(b.name));
    assert(entries.length === database.files.length, 'D1 migration artifact file count differs from manifest');
    for (const [fileIndex, file] of database.files.entries()) {
      const entry = entries[fileIndex];
      assert(entry.isFile() && !entry.isSymbolicLink() && entry.name === file.name && migrationFileName.test(file.name), 'Invalid D1 migration artifact file');
      const contents = readFileSync(join(directory, file.name));
      assert(contents.length > 0 && contents.length <= 1024 * 1024, 'D1 migration artifact file must be 1 byte to 1 MiB');
      totalSize += contents.length;
      assert(totalSize <= 10 * 1024 * 1024, 'D1 migration artifact files exceed 10 MiB');
      assert(contents.length === file.size && digest(contents) === file.sha256, 'D1 migration artifact hash mismatch');
    }
  }
  return manifest.databases;
}

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
  const buildValues = selectValues(values, p.build_variables);
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
  stageD1Migrations(cwd, bundleRoot, plan);
  validateD1Migrations(bundleRoot, plan);
  validateBundle(bundleRoot, plan);
}
