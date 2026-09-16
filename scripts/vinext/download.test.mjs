import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const workflow = readFileSync(new URL('../../.github/workflows/vinext-deploy-runner.yml', import.meta.url), 'utf8');
const blocks = [...workflow.matchAll(/^        run: \|\n((?:          .*\n)+)/gm)]
  .filter(match => match[1].includes('https://raw.githubusercontent.com/'))
  .map(match => ({ shell: match[1].replace(/^          /gm, ''), prefix: workflow.slice(Math.max(0, match.index - 200), match.index) }));
const sha = 'a'.repeat(40);
const files = ['scripts/vinext/cli.mjs', 'scripts/vinext/config.mjs', 'scripts/vinext/build.mjs', 'scripts/vinext/deploy.mjs', 'scripts/vinext/secrets.mjs', 'data/vinext-projects.json'];
const origin = 'https://raw.githubusercontent.com';

// Exercise the workflow's shell verbatim except the fixture origin and bounded,
// shorter network/retry times. No replacement curl, shell, or download script.
async function fixture(t, respond) {
  const root = mkdtempSync(join(tmpdir(), 'vinext-download-test-'));
  const requests = new Map();
  const server = createServer((request, response) => {
    const count = (requests.get(request.url) ?? 0) + 1;
    requests.set(request.url, count);
    respond(request, response, count);
  });
  const children = new Set();
  t.after(async () => {
    for (const child of children) child.kill('SIGKILL');
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    rmSync(root, { recursive: true, force: true });
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const localOrigin = `http://127.0.0.1:${server.address().port}`;
  const shell = blocks[0].shell.replaceAll(origin, localOrigin)
    .replace('--retry-delay 2', '--retry-delay 0')
    .replace('--retry-max-time 60', '--retry-max-time 2')
    .replace('--connect-timeout 10', '--connect-timeout 1')
    .replace('--max-time 20', '--max-time 1');
  return {
    root, requests,
    run: () => new Promise((resolve, reject) => {
      const child = spawn('/bin/bash', ['-c', shell], {
        cwd: root,
        env: { PATH: process.env.PATH ?? '/usr/bin:/bin', CONTROL_SHA: sha, NO_PROXY: '127.0.0.1' },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.add(child);
      let stderr = '';
      child.stdout.resume();
      child.stderr.on('data', chunk => { stderr += chunk.toString(); });
      const timer = setTimeout(() => { child.kill('SIGKILL'); }, 12000);
      child.once('error', error => { clearTimeout(timer); children.delete(child); reject(error); });
      child.once('close', (code, signal) => {
        clearTimeout(timer); children.delete(child);
        if (signal) reject(new Error(`Fixture download terminated: ${signal}; ${stderr}`));
        else resolve({ code, stderr });
      });
    }),
  };
}
function responseBody(path) { return `complete fixture for ${path}\n`; }
function success(request, response) {
  response.writeHead(200, { 'Content-Type': 'text/plain' });
  response.end(responseBody(request.url));
}
function requestPath(file) { return `/Lascade-Co/actions/${sha}/${file}`; }

// This test guards every duplicated bootstrap, including jobs that are not
// reached by the live fixture. A mutable branch must never replace CONTROL_SHA.
test('all three bootstrap blocks match and use a SHA-pinned URL with bounded secure retries', () => {
  assert.equal(blocks.length, 3);
  for (const { shell, prefix } of blocks) {
    assert.equal(shell, blocks[0].shell);
    assert.match(prefix, /CONTROL_SHA: \$\{\{ github\.sha \}\}/);
    assert.match(shell, /https:\/\/raw\.githubusercontent\.com\/Lascade-Co\/actions\/\$CONTROL_SHA\/\$file/);
    assert.match(shell, /set -euo pipefail/);
    assert.match(shell, /--fail\b/);
    assert.match(shell, /--retry 5\b/);
    assert.match(shell, /--retry-all-errors\b/);
    assert.match(shell, /--retry-delay 2\b/);
    assert.match(shell, /--retry-max-time 60\b/);
    assert.match(shell, /--connect-timeout 10\b/);
    assert.match(shell, /--max-time 20\b/);
    assert.doesNotMatch(shell, /(?:^|\s)(?:--insecure|-k)(?:\s|$)/);
    assert.match(shell, /-o "control\/\$file\.tmp"/);
    assert.ok(shell.includes('test -s "control/$file.tmp"'));
    assert.ok(shell.includes('mv "control/$file.tmp" "control/$file"'));
    assert.ok(shell.indexOf('test -s "control/$file.tmp"') < shell.indexOf('mv "control/$file.tmp" "control/$file"'));
  }
});

test('connection resets are retried and all six complete nonempty files are published', { timeout: 20000 }, async t => {
  const f = await fixture(t, (request, response, count) => {
    if (request.url === requestPath(files[0]) && count === 1) request.socket.destroy();
    else success(request, response);
  });
  const result = await f.run();
  assert.equal(result.code, 0, result.stderr);
  assert.ok(f.requests.get(requestPath(files[0])) >= 2);
  assert.equal(f.requests.size, files.length);
  for (const file of files) {
    assert.equal(readFileSync(join(f.root, 'control', file), 'utf8'), responseBody(requestPath(file)));
    assert.equal(existsSync(join(f.root, 'control', `${file}.tmp`)), false);
  }
});

test('exhausted connection resets never publish the failed destination', { timeout: 20000 }, async t => {
  const f = await fixture(t, request => request.socket.destroy());
  const result = await f.run();
  assert.notEqual(result.code, 0);
  assert.ok(f.requests.get(requestPath(files[0])) > 1);
  for (const file of files) assert.equal(existsSync(join(f.root, 'control', file)), false);
});

test('partial responses cannot replace the final destination or continue to later files', { timeout: 20000 }, async t => {
  const failed = files[1];
  const f = await fixture(t, (request, response) => {
    if (request.url !== requestPath(failed)) return success(request, response);
    response.writeHead(200, { 'Content-Length': 1000 });
    response.write('partial fixture bytes');
    const timer = setTimeout(() => response.destroy(), 10);
    response.once('close', () => clearTimeout(timer));
  });
  const result = await f.run();
  assert.notEqual(result.code, 0);
  assert.equal(readFileSync(join(f.root, 'control', files[0]), 'utf8'), responseBody(requestPath(files[0])));
  assert.ok(f.requests.get(requestPath(failed)) > 1);
  for (const file of files.slice(1)) assert.equal(existsSync(join(f.root, 'control', file)), false);
  assert.equal(f.requests.has(requestPath(files[2])), false);
});

test('an empty HTTP success fails the nonempty check before publication', { timeout: 20000 }, async t => {
  const f = await fixture(t, (_request, response) => { response.writeHead(200); response.end(); });
  const result = await f.run();
  assert.notEqual(result.code, 0);
  assert.equal(f.requests.get(requestPath(files[0])), 1);
  for (const file of files) assert.equal(existsSync(join(f.root, 'control', file)), false);
});
