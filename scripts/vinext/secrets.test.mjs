import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { exportSecrets, loadSecrets } from './secrets.mjs';

const plan = {
  infisical_project_slug: 'test-project', infisical_env: 'staging', infisical_path: '/web',
  project: { build_variables: ['NEXT_PUBLIC_API_URL', 'OPTIONAL'], required_build_variables: ['NEXT_PUBLIC_API_URL'], runtime_secrets: ['SERVER_SECRET', 'OTHER_SECRET'] },
};
const env = { INFISICAL_DOMAIN: 'https://infisical.example.test', INFISICAL_CLIENT_ID: 'fixture-id', INFISICAL_CLIENT_SECRET: 'fixture-auth' };
const entry = (secretKey, secretValue) => ({ secretKey, secretValue });
function destination(t) {
  const directory = mkdtempSync(join(tmpdir(), 'vinext-secrets-test-'));
  t.after(() => rmSync(directory, { force: true, recursive: true }));
  return join(directory, 'selected.json');
}
function mock(result) {
  const calls = [];
  return { calls, fetcher: async (url, options) => {
    calls.push({ url, options });
    return new Response(JSON.stringify(calls.length === 1 ? { accessToken: 'fixture-session' } : result));
  } };
}

test('build export fetches explicit project/environment and exposes only selected build keys', async t => {
  const file = destination(t);
  const api = mock({ secrets: [entry('NEXT_PUBLIC_API_URL', 'https://stage.example.test'), entry('SERVER_SECRET', 'private'), entry('UNDECLARED', 'omit')] });
  await exportSecrets(plan, { file, kind: 'build', env, fetcher: api.fetcher });
  assert.deepEqual(JSON.parse(readFileSync(file)), { NEXT_PUBLIC_API_URL: 'https://stage.example.test' });
  assert.equal(statSync(file).mode & 0o777, 0o600);
  assert.equal(api.calls[0].url.pathname, '/api/v1/auth/universal-auth/login');
  assert.equal(api.calls[0].options.method, 'POST');
  assert.equal(new URLSearchParams(api.calls[0].options.body).get('clientSecret'), 'fixture-auth');
  assert.equal(api.calls[1].url.pathname, '/api/v3/secrets/raw');
  for (const [key, expected] of Object.entries({ workspaceSlug: 'test-project', environment: 'staging', secretPath: '/web', include_imports: 'true', recursive: 'false', expandSecretReferences: 'true' })) assert.equal(api.calls[1].url.searchParams.get(key), expected);
  assert.equal(api.calls[1].options.headers.Authorization, 'Bearer fixture-session');
  for (const call of api.calls) assert.equal(call.options.redirect, 'error');
});

test('runtime export preserves multiline quotes and direct values take precedence over imports', async t => {
  const file = destination(t), value = "first line\nwe're ready\r\n%value";
  const api = mock({ secrets: [entry('SERVER_SECRET', value), entry('NEXT_PUBLIC_API_URL', 'public')], imports: [
    { secrets: [entry('SERVER_SECRET', 'ignore import'), entry('OTHER_SECRET', 'older')] },
    { secrets: [entry('OTHER_SECRET', 'later')] },
  ] });
  await exportSecrets(plan, { file, kind: 'runtime', env, fetcher: api.fetcher });
  assert.deepEqual(JSON.parse(readFileSync(file)), { SERVER_SECRET: value, OTHER_SECRET: 'later' });
});

test('missing full runtime secret set leaves no export or previous stale file', async t => {
  const file = destination(t); writeFileSync(file, 'previous-secrets');
  const api = mock({ secrets: [entry('SERVER_SECRET', 'one')] });
  await assert.rejects(exportSecrets(plan, { file, kind: 'runtime', env, fetcher: api.fetcher }), /Missing Infisical value: OTHER_SECRET/);
  assert.equal(existsSync(file), false);
});

test('malformed secret responses fail without writing an artifact', async t => {
  const file = destination(t);
  for (const result of [{}, { secrets: {} }, { secrets: [entry('SERVER_SECRET', 123)] }, { secrets: [], imports: [{}] }]) {
    const api = mock(result);
    await assert.rejects(exportSecrets(plan, { file, kind: 'runtime', env, fetcher: api.fetcher }), /Invalid Infisical/);
    assert.equal(existsSync(file), false);
  }
});

test('Infisical failures do not include potentially secret response bodies', async t => {
  const file = destination(t);
  await assert.rejects(exportSecrets(plan, { file, kind: 'build', env, fetcher: async () => new Response('sensitive-response-fixture', { status: 401 }) }), error => {
    assert.match(error.message, /HTTP 401/); assert.doesNotMatch(error.message, /sensitive-response-fixture/); return true;
  });
  await assert.rejects(exportSecrets(plan, { file, kind: 'build', env, fetcher: async () => new Response('sensitive-invalid-json') }), error => {
    assert.match(error.message, /invalid JSON/); assert.doesNotMatch(error.message, /sensitive-invalid-json/); return true;
  });
});

test('insecure origin, missing credentials and unknown export kind fail before network access', async t => {
  const file = destination(t); let calls = 0;
  const fetcher = async () => { calls++; throw new Error('Network must not run'); };
  for (const options of [
    { kind: 'build', env: { ...env, INFISICAL_DOMAIN: 'http://example.test' } },
    { kind: 'build', env: { ...env, INFISICAL_DOMAIN: 'https://user:pass@example.test' } },
    { kind: 'build', env: { ...env, INFISICAL_CLIENT_SECRET: '' } },
    { kind: 'unknown', env },
  ]) await assert.rejects(exportSecrets(plan, { file, fetcher, ...options }));
  assert.equal(calls, 0);
});

test('public runner masks imported values while leaving only nonsecret plan metadata unmasked', async t => {
  const file = destination(t);
  const output = [];
  const previous = console.log;
  console.log = line => output.push(line);
  t.after(() => { console.log = previous; });
  const api = mock({ secrets: [
    entry('NEXT_PUBLIC_API_URL', 'https://public.example.test'),
    entry('SERVER_SECRET', 'private-value'),
    entry('VINEXT_RUNTIME_SECRETS', 'SERVER_SECRET'),
    entry('VINEXT_SOURCE_REPOSITORY', 'Lascade-Co/example'),
  ] });
  await exportSecrets(plan, { file, kind: 'build', env: { ...env, GITHUB_ACTIONS: 'true' }, fetcher: api.fetcher });
  assert(output.includes('::add-mask::https://public.example.test'));
  assert(output.includes('::add-mask::private-value'));
  assert(!output.some(line => line.includes('Lascade-Co/example') || line === '::add-mask::SERVER_SECRET'));
});

test('plan resolution masks private values without masking public values that can collide with job outputs', async t => {
  const output = [];
  const previous = console.log;
  console.log = line => output.push(line);
  t.after(() => { console.log = previous; });
  const api = mock({ secrets: [
    entry('NEXT_PUBLIC_APP_CODE', 'example'),
    entry('SERVER_SECRET', 'private-value'),
    entry('VINEXT_SOURCE_REPOSITORY', 'Lascade-Co/example'),
  ] });
  await loadSecrets(plan, { env: { ...env, GITHUB_ACTIONS: 'true' }, fetcher: api.fetcher, maskPublic: false });
  assert(output.includes('::add-mask::private-value'));
  assert(!output.includes('::add-mask::example'));
});
