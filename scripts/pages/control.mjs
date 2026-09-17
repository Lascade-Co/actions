import { lstat, readdir, appendFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { createHash } from 'node:crypto';

export function validateBasePath(basePath = '.') {
  if (typeof basePath !== 'string' || (basePath !== '.' &&
      !basePath.split('/').every(segment => /^[A-Za-z0-9_.-]+$(?![\s\S])/.test(segment) && segment !== '.' && segment !== '..'))) {
    throw new Error('base_path must be a safe relative directory, or . for the repository root.');
  }
  return basePath;
}

export function validatePayload(payload) {
  if (typeof payload?.repo !== 'string' || !/^Lascade-Co\/[A-Za-z0-9_.-]+$(?![\s\S])/.test(payload.repo) ||
      payload.branch !== 'main' || typeof payload.sha !== 'string' || !/^[0-9a-f]{40}$(?![\s\S])/.test(payload.sha)) {
    throw new Error('Expected an organization repository and an exact main commit.');
  }
  const [owner, repo_name] = payload.repo.split('/');
  if (repo_name === '.' || repo_name === '..') throw new Error('Invalid source repository.');
  const project_slug = payload.project_slug ?? '';
  const infisical_path = payload.infisical_path ?? '/';
  if (typeof project_slug !== 'string' || !/^[A-Za-z0-9_-]*$(?![\s\S])/.test(project_slug) ||
      typeof infisical_path !== 'string' || !infisical_path.startsWith('/') ||
      (infisical_path !== '/' && infisical_path.slice(1).split('/').some(part => !/^[A-Za-z0-9_.-]+$(?![\s\S])/.test(part) || part === '.' || part === '..'))) {
    throw new Error('Invalid Infisical project slug or secret path.');
  }
  return { repo: payload.repo, owner, repo_name, sha: payload.sha, base_path: validateBasePath(payload.base_path), project_slug, infisical_path };
}

export function validateConfig(config) {
  const allowed = ['$schema', 'name', 'account_id', 'pages_build_output_dir', 'compatibility_date'];
  if (!config || typeof config !== 'object' || Array.isArray(config) ||
      Object.keys(config).some(key => !allowed.includes(key))) throw new Error('Expected a static Pages config without runtime bindings.');
  if (typeof config.account_id !== 'string' || !/^[0-9a-f]{32}$(?![\s\S])/.test(config.account_id) ||
      typeof config.name !== 'string' || !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$(?![\s\S])/.test(config.name)) {
    throw new Error('Invalid Pages account or project name.');
  }
  if (config.pages_build_output_dir !== './dist' || typeof config.compatibility_date !== 'string' ||
      !/^\d{4}-\d{2}-\d{2}$(?![\s\S])/.test(config.compatibility_date) ||
      !Number.isFinite(Date.parse(config.compatibility_date)) ||
      new Date(config.compatibility_date).toISOString().slice(0, 10) !== config.compatibility_date) {
    throw new Error('Expected pages_build_output_dir ./dist and a valid compatibility_date.');
  }
  return { account_id: config.account_id, project_name: config.name,
    target_key: `pages-${createHash('sha256').update(`${config.account_id}/${config.name}`).digest('hex')}` };
}

export async function verifyBaseDirectory(root, basePath) {
  validateBasePath(basePath);
  let path = resolve(root);
  for (const segment of ['', ...(basePath === '.' ? [] : basePath.split('/'))]) {
    path = join(path, segment);
    const stat = await lstat(path);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('Build directory must contain no symlinks.');
  }
  return path;
}

export async function verifyStaticDirectory(directory) {
  const root = await lstat(directory);
  if (!root.isDirectory() || root.isSymbolicLink()) throw new Error('Expected a static output directory.');
  const index = await lstat(join(directory, 'index.html'));
  if (!index.isFile() || index.isSymbolicLink() || index.size === 0) throw new Error('Missing nonempty index.html.');
  async function visit(path) {
    for (const entry of await readdir(path, { withFileTypes: true })) {
      const full = join(path, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`Symlinks are forbidden: ${full}`);
      if (['_worker.js', 'functions', 'node_modules', '.git', '.env', 'wrangler.json', 'wrangler.jsonc', 'wrangler.toml'].includes(entry.name) || entry.name.startsWith('.env.')) {
        throw new Error(`Runtime or secret artifact is forbidden: ${full}`);
      }
      if (entry.isDirectory()) await visit(full);
      else if (!entry.isFile()) throw new Error(`Non-file artifact is forbidden: ${full}`);
    }
  }
  await visit(directory);
}

async function output(name, value) {
  if (process.env.GITHUB_OUTPUT) await appendFile(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}

async function main() {
  const command = process.argv[2];
  if (command === 'static') return verifyStaticDirectory(resolve(process.argv[3]));
  if (command === 'base') return verifyBaseDirectory(process.argv[3], process.argv[4]);
  const source = validatePayload(JSON.parse(process.env.PAYLOAD_JSON));
  if (command === 'validate') {
    for (const [key, value] of Object.entries(source)) await output(key, value);
    return;
  }
  if (command !== 'head') throw new Error(`Unknown command: ${command}`);
  const headers = { Accept: 'application/vnd.github+json', 'User-Agent': 'central-pages-deploy',
    ...(process.env.GH_TOKEN ? { Authorization: `Bearer ${process.env.GH_TOKEN}` } : {}) };
  const configPath = source.base_path === '.' ? 'wrangler.json' : `${source.base_path}/wrangler.json`;
  const configResponse = await fetch(`https://api.github.com/repos/${source.repo}/contents/${configPath}?ref=${source.sha}`, {
    headers: { ...headers, Accept: 'application/vnd.github.raw+json' },
  });
  if (!configResponse.ok) throw new Error(`Unable to load source wrangler.json: HTTP ${configResponse.status}`);
  const plan = { ...source, ...validateConfig(await configResponse.json()) };
  for (const [key, value] of Object.entries(plan)) await output(key, value);
  const { sha, repo } = plan;
  const response = await fetch(`https://api.github.com/repos/${repo}/git/ref/heads/main`, { headers });
  if (!response.ok) throw new Error(`Unable to verify main HEAD: HTTP ${response.status}`);
  const head = (await response.json()).object?.sha;
  if (typeof head !== 'string' || !/^[0-9a-f]{40}$(?![\s\S])/.test(head)) throw new Error('Invalid main HEAD response.');
  const current = head === sha;
  await output('current', String(current));
  console.log(current ? `Deploying current main commit ${sha}` : `Skipping superseded commit ${sha}; main is ${head}`);
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(new URL(import.meta.url).pathname)) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
