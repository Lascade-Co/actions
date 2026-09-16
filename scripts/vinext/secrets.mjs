import { writeFileSync, rmSync } from 'node:fs';
import { assert } from './config.mjs';
import { selectValues } from './build.mjs';

// Same Universal Auth and secret-list contract as Infisical/secrets-action.
// Its file exporter interpolates unescaped quotes into dotenv. JSON preserves
// multiline/quoted values and lets us expose only the keys needed by this job.
export async function exportSecrets(plan, { file, kind, env = process.env, fetcher = fetch }) {
  assert(['build', 'runtime'].includes(kind), 'Invalid secret export kind');
  const domain = new URL(env.INFISICAL_DOMAIN);
  assert(domain.protocol === 'https:' && !domain.username && !domain.password, 'Infisical must use HTTPS');
  assert(env.INFISICAL_CLIENT_ID && env.INFISICAL_CLIENT_SECRET, 'Missing Infisical Universal Auth credentials');
  const call = async (path, options) => {
    const response = await fetcher(new URL(path, domain), { ...options, redirect: 'error', signal: AbortSignal.timeout(30000) });
    assert(response.ok, `Infisical ${path.split('?')[0]} failed (HTTP ${response.status})`);
    // Do not include response bodies or SDK errors that may contain credentials.
    try { return await response.json(); } catch { throw new Error('Infisical returned invalid JSON'); }
  };
  rmSync(file, { force: true });
  const login = await call('/api/v1/auth/universal-auth/login', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ clientId: env.INFISICAL_CLIENT_ID, clientSecret: env.INFISICAL_CLIENT_SECRET }).toString(),
  });
  assert(typeof login.accessToken === 'string' && login.accessToken, 'Infisical login returned no token');
  const query = new URLSearchParams({ workspaceSlug: plan.infisical_project_slug, environment: plan.infisical_env, secretPath: plan.infisical_path, include_imports: 'true', recursive: 'false', expandSecretReferences: 'true' });
  const result = await call(`/api/v3/secrets/raw?${query}`, { headers: { Authorization: `Bearer ${login.accessToken}` } });
  assert(Array.isArray(result.secrets) && (result.imports === undefined || Array.isArray(result.imports)), 'Invalid Infisical secret list');
  const values = Object.create(null);
  for (const list of [result.secrets, ...(result.imports ?? []).toReversed().map(i => i.secrets)]) {
    assert(Array.isArray(list), 'Invalid Infisical import');
    for (const secret of list) {
      assert(typeof secret.secretKey === 'string' && typeof secret.secretValue === 'string', 'Invalid Infisical secret');
      if (env.GITHUB_ACTIONS && secret.secretValue) console.log(`::add-mask::${secret.secretValue.replaceAll('%', '%25').replaceAll('\r', '%0D').replaceAll('\n', '%0A')}`);
      if (!Object.hasOwn(values, secret.secretKey)) values[secret.secretKey] = secret.secretValue;
    }
  }
  const p = plan.project;
  const selected = kind === 'build' ? selectValues(values, p.build_variables, p.required_build_variables) : selectValues(values, p.runtime_secrets);
  writeFileSync(file, JSON.stringify(selected), { mode: 0o600, flag: 'wx' });
}
