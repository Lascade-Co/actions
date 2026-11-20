"""Tests for the runner sealing toolkit (install.sh, seal-bash, seal-pack, seal-unpack).

Each test runs install.sh against a fake runner in tmp_path. install.sh downloads
and verifies the real age v1.3.2 release; a `curl` shim on PATH replays the
tarball from a per-session cache under the pytest tmp dir after the first
download. The tests generate a throwaway age key and point SEAL_PUBKEY_FILE at it.
The caller-repo key path (the `actions/seal` composite) is served by a local
http.server standing in for the GitHub contents API; the shim rewrites the API host.
"""
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tarfile
import threading
import time
import zlib

import pytest
import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
INSTALL = HERE / 'install.sh'
SEAL_ACTION = ROOT / 'actions' / 'seal' / 'action.yml'
UPLOAD_ACTION = ROOT / 'actions' / 'seal-upload' / 'action.yml'
TEST_KNOBS = ('SEAL_SOURCE_DIR', 'SEAL_PUBKEY_FILE', 'SEAL_NO_SUDO', 'SEAL_AGE_SHA256_OVERRIDE')
REAL_CURL = shutil.which('curl')
# Example recipient from the age README; only used to bootstrap age-keygen.
BOOTSTRAP_RECIPIENT = 'age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p'
VIEWER = 'https://ci.lascade.com/?repo=actions&run=42&attempt=1'


@pytest.fixture(scope='session')
def shim_dir(tmp_path_factory):
    """A PATH dir whose `curl` serves the age release tarball from a session cache.

    Raw control files of this repo are served from $SHIM_SERVE_DIR when a test sets
    it; GitHub API calls go to the local stub at $SHIM_API_BASE (real curl, same
    arguments and stdin); every requested URL is appended to $SHIM_LOG when set.
    $SHIM_ARGV_LOG, when set, gets every call's argv and environment.
    Anything else fails. A `sudo` stub makes sure no test can link into /usr/local/bin.
    """
    d = tmp_path_factory.mktemp('shim')
    cache = tmp_path_factory.mktemp('age-cache')
    (d / 'curl').write_text(f'''#!/bin/bash
set -eu
out= url= prev=
for a in "$@"; do
  [ "$prev" = -o ] && out=$a
  case "$a" in https://*) url=$a ;; esac
  prev=$a
done
if [ -z "$url" ] || [ -z "$out" ]; then echo "test curl shim: unexpected call" >&2; exit 22; fi
[ -z "${{SHIM_LOG:-}}" ] || echo "$url" >>"$SHIM_LOG"
[ -z "${{SHIM_ARGV_LOG:-}}" ] || {{ printf '%s\n' "$*"; env; }} >>"$SHIM_ARGV_LOG"
case "$url" in
  https://github.com/FiloSottile/age/releases/download/*)
    c="{cache}/${{url##*/}}"
    if [ -s "$c" ]; then cp "$c" "$out"; else "{REAL_CURL}" "$@"; cp "$out" "$c"; fi ;;
  https://raw.githubusercontent.com/Lascade-Co/actions/*/scripts/seal/*)
    f="${{SHIM_SERVE_DIR:-/nonexistent}}/${{url##*/}}"
    [ -e "$f" ] || exit 22
    cp "$f" "$out" ;;
  https://api.github.com/*)
    [ -n "${{SHIM_API_BASE:-}}" ] || exit 22
    args=()
    for a in "$@"; do
      case "$a" in https://api.github.com/*) args+=("$SHIM_API_BASE/${{a#https://api.github.com/}}") ;; *) args+=("$a") ;; esac
    done
    exec "{REAL_CURL}" "${{args[@]}}" ;;
  *) exit 22 ;;
esac
''')
    (d / 'curl').chmod(0o755)
    (d / 'sudo').write_text('#!/bin/sh\n[ -z "${SHIM_ARGV_LOG:-}" ] || { echo "spy:sudo $*"; env; } >>"$SHIM_ARGV_LOG"\n'
                            'echo "test sudo shim: refused" >&2\nexit 1\n')
    (d / 'sudo').chmod(0o755)
    return d


def base_env(root, shim_dir, pubkey_file):
    rt = root / 'runner_temp'
    rt.mkdir(parents=True)
    for f in ('github_env', 'github_path', 'step_summary'):
        (root / f).touch()
    env = {k: os.environ[k] for k in ('HOME', 'LANG', 'TMPDIR', 'USER') if k in os.environ}
    env.update(
        PATH=f'{shim_dir}:{os.environ["PATH"]}',
        RUNNER_TEMP=str(rt),
        GITHUB_ENV=str(root / 'github_env'),
        GITHUB_PATH=str(root / 'github_path'),
        GITHUB_STEP_SUMMARY=str(root / 'step_summary'),
        GITHUB_JOB='build',
        GITHUB_RUN_ATTEMPT='1',
        GITHUB_RUN_ID='42',
        GITHUB_REPOSITORY='Lascade-Co/actions',
        RUNNER_NAME='test-runner 1',
        RUNNER_OS='macOS',
        SEAL_SOURCE_DIR=str(HERE),
        SEAL_NO_SUDO='1',
        SEAL_PUBKEY_FILE=str(pubkey_file),
    )
    return env


def run_install(env, **extra):
    return subprocess.run(['bash', str(INSTALL)], env={**env, **extra}, capture_output=True,
                          text=True, stdin=subprocess.DEVNULL, start_new_session=True, timeout=120)


@pytest.fixture(scope='session')
def keypair(tmp_path_factory, shim_dir):
    """Throwaway age identity generated with an installed age-keygen."""
    root = tmp_path_factory.mktemp('bootstrap')
    (root / 'bootstrap.pub').write_text(BOOTSTRAP_RECIPIENT + '\n')
    env = base_env(root, shim_dir, root / 'bootstrap.pub')
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    keygen = Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'age-keygen'
    ident = root / 'key.txt'
    subprocess.run([keygen, '-o', ident], check=True, capture_output=True)
    pub = subprocess.run([keygen, '-y', ident], check=True, capture_output=True, text=True).stdout
    (root / 'key.pub').write_text(pub)
    secret = next(l for l in ident.read_text().splitlines() if l.startswith('AGE-SECRET-KEY-'))
    return {'identity': ident, 'pub': root / 'key.pub', 'secret': secret}


class Runner:
    def __init__(self, root, env, keypair):
        self.root = root
        self.env = env
        self.keypair = keypair
        self.rt = Path(env['RUNNER_TEMP'])
        self.seal = self.rt / 'seal'
        self.bin = self.seal / 'bin'
        self.out = self.seal / 'out'
        self.n = 0

    @property
    def name(self):
        m = re.search(r'^SEAL_LOG_ARTIFACT=(.*)$', (self.root / 'github_env').read_text(), re.M)
        return m.group(1)

    def script(self, body):
        self.n += 1
        p = self.root / f'step{self.n}.sh'
        p.write_text(body + '\n')
        return p

    def step(self, body, mode='seal-bash', timeout=60):
        return subprocess.run([self.bin / mode, self.script(body)], env=self.env, capture_output=True,
                              text=True, stdin=subprocess.DEVNULL, start_new_session=True,
                              timeout=timeout)

    def decrypt(self, path):
        return subprocess.run([self.bin / 'age', '-d', '-i', self.keypair['identity'], path],
                              check=True, capture_output=True).stdout

    def tool(self, name, *args, cwd=None, env=None):
        return subprocess.run([self.bin / name, *map(str, args)], env=env or self.env, cwd=cwd,
                              capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60)

    def unpack_env(self):
        return {**self.env, 'REPO_AGE_KEY': self.keypair['secret']}


@pytest.fixture
def runner(tmp_path, shim_dir, keypair):
    env = base_env(tmp_path, shim_dir, keypair['pub'])
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    return Runner(tmp_path, env, keypair)


# --- install.sh -------------------------------------------------------------

def test_install_exports_artifact_name(runner):
    lines = (runner.root / 'github_env').read_text().splitlines()
    assert [l for l in lines if l.startswith('SEAL_LOG_ARTIFACT=')] == lines
    assert len(lines) == 1
    assert re.fullmatch(r'SEAL_LOG_ARTIFACT=sealed-log-build-1-[0-9a-f]{8}', lines[0])
    meta = json.loads(runner.decrypt(runner.out / 'meta.json.age'))
    assert meta['job'] == 'build'
    assert meta['attempt'] == '1'
    assert meta['runner_name'] == 'test-runner 1'
    assert meta['os'] == 'macOS'
    assert re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', meta['started_at'])
    assert (runner.root / 'github_path').read_text() == f'{runner.bin}\n'
    for tool in ('age', 'age-keygen', 'seal-bash', 'seal-bash-e', 'seal-pack', 'seal-unpack'):
        assert os.access(runner.bin / tool, os.X_OK), tool
    assert (runner.seal / 'repo-key.pub').read_text() == runner.keypair['pub'].read_text()


SHA = '0123456789abcdef0123456789abcdef01234567'


def served_env(tmp_path, shim_dir, keypair):
    """An env for the real (non-test-knob) path: tools and key fetched at SEAL_REF."""
    serve = tmp_path / 'served'
    serve.mkdir()
    for t in ('seal-bash', 'seal-pack', 'seal-unpack'):
        shutil.copy(HERE / t, serve / t)
    shutil.copy(keypair['pub'], serve / 'repo-key.pub')
    env = base_env(tmp_path, shim_dir, keypair['pub'])
    del env['SEAL_SOURCE_DIR'], env['SEAL_PUBKEY_FILE']
    env.update(SEAL_REF=SHA, SHIM_SERVE_DIR=str(serve), SHIM_LOG=str(tmp_path / 'urls.log'))
    return env, serve


def test_install_fetches_control_files_at_ref(tmp_path, shim_dir, keypair):
    env, _ = served_env(tmp_path, shim_dir, keypair)
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    raw = f'https://raw.githubusercontent.com/Lascade-Co/actions/{SHA}/scripts/seal'
    urls = (tmp_path / 'urls.log').read_text().splitlines()
    assert urls[0].startswith('https://github.com/FiloSottile/age/releases/download/v1.3.2/age-v1.3.2-')
    assert sorted(urls[1:]) == [f'{raw}/{t}' for t in ('repo-key.pub', 'seal-bash', 'seal-pack', 'seal-unpack')]
    runner = Runner(tmp_path, env, keypair)
    s = runner.step('echo fetched-ok')
    assert s.returncode == 0
    assert runner.decrypt(runner.out / '1.log.age') == b'fetched-ok\n'


def test_install_requires_full_sha_ref(tmp_path, shim_dir, keypair):
    env, _ = served_env(tmp_path, shim_dir, keypair)
    r = run_install(env, SEAL_REF='main')
    assert r.returncode != 0
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'seal-bash').exists()
    assert (tmp_path / 'github_env').read_text() == ''


def test_install_rejects_empty_download(tmp_path, shim_dir, keypair):
    env, serve = served_env(tmp_path, shim_dir, keypair)
    (serve / 'seal-pack').write_text('')
    r = run_install(env)
    assert r.returncode != 0
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'seal-bash').exists()
    assert (tmp_path / 'github_env').read_text() == ''


@pytest.mark.parametrize('knob, value', [
    ('SEAL_SOURCE_DIR', str(HERE)), ('SEAL_PUBKEY_FILE', '/dev/null'),
    ('SEAL_NO_SUDO', '1'), ('SEAL_AGE_SHA256_OVERRIDE', '0' * 64)])
def test_install_refuses_test_knobs_on_actions(tmp_path, shim_dir, keypair, knob, value):
    env, _ = served_env(tmp_path, shim_dir, keypair)
    env.pop('SEAL_NO_SUDO')
    r = run_install(env, GITHUB_ACTIONS='true', **{knob: value})
    assert r.returncode != 0
    assert knob in r.stderr
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'seal-bash').exists()
    assert (tmp_path / 'github_env').read_text() == ''


def test_install_refuses_second_run(runner):
    r = run_install(runner.env)
    assert r.returncode != 0
    assert 'already installed' in r.stderr
    assert len((runner.root / 'github_env').read_text().splitlines()) == 1
    assert (runner.root / 'github_path').read_text() == f'{runner.bin}\n'


def test_install_prints_viewer_url_once(tmp_path, shim_dir, keypair):
    r = run_install(base_env(tmp_path, shim_dir, keypair['pub']))
    assert r.returncode == 0, r.stderr
    assert r.stdout.count(VIEWER) == 1
    assert not re.search(r'🔒 sealed sealed-log-', r.stdout)


def test_install_rejects_bad_checksum(tmp_path, shim_dir, keypair):
    env = base_env(tmp_path, shim_dir, keypair['pub'])
    r = run_install(env, SEAL_AGE_SHA256_OVERRIDE='0' * 64)
    assert r.returncode != 0
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'seal-bash').exists()
    assert (tmp_path / 'github_env').read_text() == ''
    assert (tmp_path / 'github_path').read_text() == ''


def test_install_rejects_unknown_platform(tmp_path, shim_dir, keypair):
    fake = tmp_path / 'fakeuname'
    fake.mkdir()
    (fake / 'uname').write_text('#!/bin/sh\ncase "$1" in -s) echo Linux ;; *) echo riscv64 ;; esac\n')
    (fake / 'uname').chmod(0o755)
    env = base_env(tmp_path, shim_dir, keypair['pub'])
    r = run_install(env, PATH=f'{fake}:{env["PATH"]}')
    assert r.returncode != 0
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'seal-bash').exists()


def test_install_rejects_bad_pubkey(tmp_path, shim_dir):
    bad = tmp_path / 'bad.pub'
    bad.write_text('not-a-key\n')
    env = base_env(tmp_path, shim_dir, bad)
    r = run_install(env)
    assert r.returncode != 0
    assert (tmp_path / 'github_env').read_text() == ''


# --- caller-repo key (actions/seal composite) ---------------------------------

CALLER = 'Lascade-Co/connector'
TOKEN = 'ghs_TESTTOKEN0123456789abcdefSECRET'
KEY_PATH = f'/repos/{CALLER}/contents/.github/seal/repo-key.pub?ref={SHA}'


@pytest.fixture
def key_api():
    """A stand-in for the GitHub contents API. `routes` maps request path to (status, body)."""
    state = {'routes': {}, 'requests': []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state['requests'].append({'path': self.path, 'headers': dict(self.headers)})
            status, body = state['routes'].get(self.path, (404, b'{"message":"Not Found","status":"404"}'))
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    state['base'] = f'http://127.0.0.1:{srv.server_address[1]}'
    try:
        yield state
    finally:
        srv.shutdown()
        srv.server_close()


def caller_env(tmp_path, shim_dir, key_api):
    """The composite action's install: tools from SEAL_TOOLKIT_DIR, key from the caller repo."""
    env = base_env(tmp_path, shim_dir, '/nonexistent')
    del env['SEAL_SOURCE_DIR'], env['SEAL_PUBKEY_FILE']
    env.update(SEAL_TOOLKIT_DIR=str(HERE), SEAL_KEY_REPO=CALLER, SEAL_KEY_REF=SHA, SEAL_TOKEN=TOKEN,
               GITHUB_REPOSITORY=CALLER, SHIM_API_BASE=key_api['base'], SHIM_LOG=str(tmp_path / 'urls.log'))
    return env


def logged_urls(tmp_path):
    log = tmp_path / 'urls.log'
    return log.read_text().splitlines() if log.exists() else []


def run_install_in(cwd, env, **extra):
    """install.sh from a directory with no checkout in it."""
    cwd.mkdir(exist_ok=True)
    return subprocess.run(['bash', str(INSTALL)], env={**env, **extra}, cwd=cwd, capture_output=True,
                          text=True, stdin=subprocess.DEVNULL, start_new_session=True, timeout=120)


def assert_not_installed(tmp_path, env):
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'bin' / 'seal-bash').exists()
    assert not (Path(env['RUNNER_TEMP']) / 'seal' / 'repo-key.pub').exists()
    assert (tmp_path / 'github_env').read_text() == ''
    assert (tmp_path / 'github_path').read_text() == ''


def test_marker_includes_repo(tmp_path, shim_dir, keypair):
    env = base_env(tmp_path, shim_dir, keypair['pub'])
    env['GITHUB_REPOSITORY'] = CALLER
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    url = 'https://ci.lascade.com/?repo=connector&run=42&attempt=1'
    assert r.stdout.count(url) == 1
    runner = Runner(tmp_path, env, keypair)
    s = runner.step('echo hi')
    assert s.returncode == 0
    assert s.stdout.splitlines() == [f'🔒 sealed {runner.name}#1 {url}']


def test_install_from_toolkit_dir_without_curl(tmp_path, shim_dir, keypair, key_api):
    """Tools are copied from SEAL_TOOLKIT_DIR (the action's checkout): no raw download, no SEAL_REF."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    env = caller_env(tmp_path, shim_dir, key_api)
    assert 'SEAL_REF' not in env
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    urls = logged_urls(tmp_path)
    assert urls[0].startswith('https://github.com/FiloSottile/age/releases/download/v1.3.2/age-v1.3.2-')
    assert urls[1:] == [f'https://api.github.com{KEY_PATH}']
    runner = Runner(tmp_path, env, keypair)
    for t in ('seal-bash', 'seal-pack', 'seal-unpack'):
        assert (runner.bin / t).read_bytes() == (HERE / t).read_bytes(), t
    assert (runner.bin / 'seal-bash-e').read_bytes() == (HERE / 'seal-bash').read_bytes()
    s = runner.step('echo toolkit-ok')
    assert s.returncode == 0
    assert runner.decrypt(runner.out / '1.log.age') == b'toolkit-ok\n'


def test_key_from_caller_repo(tmp_path, shim_dir, keypair, key_api):
    """The key comes from the caller repo's contents API at the ref; no checkout is needed."""
    assert keypair['pub'].read_text() != (HERE / 'repo-key.pub').read_text()
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    env = caller_env(tmp_path, shim_dir, key_api)
    r = run_install_in(tmp_path / 'no-checkout', env)
    assert r.returncode == 0, r.stderr
    seal = Path(env['RUNNER_TEMP']) / 'seal'
    assert (seal / 'repo-key.pub').read_text() == keypair['pub'].read_text()
    assert [q['path'] for q in key_api['requests']] == [KEY_PATH]
    h = {k.lower(): v for k, v in key_api['requests'][0]['headers'].items()}
    assert h['authorization'] == f'Bearer {TOKEN}'
    assert h['accept'] == 'application/vnd.github.raw'
    assert h['x-github-api-version'] == '2022-11-28'
    assert TOKEN not in r.stdout + r.stderr
    assert 'https://ci.lascade.com/?repo=connector&run=42&attempt=1' in r.stdout
    runner = Runner(tmp_path, env, keypair)
    s = runner.step('echo caller-key-ok')
    assert s.returncode == 0
    assert runner.decrypt(runner.out / '1.log.age') == b'caller-key-ok\n'


@pytest.mark.parametrize('body', [
    b'not-a-key\n',
    b'',
    b'AGE-SECRET-KEY-1QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ\n',
    (BOOTSTRAP_RECIPIENT + '\n' + BOOTSTRAP_RECIPIENT + '\n').encode(),
    (BOOTSTRAP_RECIPIENT + '\r\r\n').encode(),
], ids=['garbage', 'empty', 'secret', 'two-lines', 'double-cr'])
def test_bad_caller_key_fails_before_run_steps(tmp_path, shim_dir, key_api, body):
    """Runs as on GitHub (no test knobs): SEAL_TOOLKIT_DIR is a production setting, the bad key fails the step."""
    key_api['routes'][KEY_PATH] = (200, body)
    env = caller_env(tmp_path, shim_dir, key_api)
    env.pop('SEAL_NO_SUDO')
    r = run_install_in(tmp_path / 'no-checkout', env, GITHUB_ACTIONS='true')
    assert r.returncode != 0
    assert 'test-only' not in r.stderr
    assert '.github/seal/repo-key.pub' in r.stderr and CALLER in r.stderr
    assert TOKEN not in r.stdout + r.stderr
    assert_not_installed(tmp_path, env)
    assert not [u for u in logged_urls(tmp_path) if 'raw.githubusercontent.com' in u]
    script = tmp_path / 's.sh'
    script.write_text('echo CANARY-BADKEY\n')
    s = subprocess.run(['bash', HERE / 'seal-bash', script], env=env, capture_output=True, text=True,
                       stdin=subprocess.DEVNULL)
    assert s.returncode != 0 and 'CANARY-BADKEY' not in s.stdout + s.stderr


def test_missing_caller_key_names_the_file(tmp_path, shim_dir, key_api):
    """A 404 names the file and repo, is not retried, and never falls back to this repo's key."""
    env = caller_env(tmp_path, shim_dir, key_api)
    env.pop('SEAL_NO_SUDO')
    r = run_install_in(tmp_path / 'no-checkout', env, GITHUB_ACTIONS='true')
    assert r.returncode != 0
    assert '.github/seal/repo-key.pub' in r.stderr and CALLER in r.stderr
    assert 'test-only' not in r.stderr
    assert TOKEN not in r.stdout + r.stderr
    assert [q['path'] for q in key_api['requests']] == [KEY_PATH]
    assert not [u for u in logged_urls(tmp_path) if 'raw.githubusercontent.com' in u]
    assert_not_installed(tmp_path, env)


@pytest.mark.parametrize('status', [401, 403])
def test_unreadable_caller_key_mentions_permission(tmp_path, shim_dir, key_api, status):
    key_api['routes'][KEY_PATH] = (status, b'{"message":"Resource not accessible by integration"}')
    env = caller_env(tmp_path, shim_dir, key_api)
    r = run_install(env)
    assert r.returncode != 0
    assert '.github/seal/repo-key.pub' in r.stderr and 'contents: read' in r.stderr
    assert_not_installed(tmp_path, env)


@pytest.mark.parametrize('var, value', [
    ('SEAL_KEY_REPO', ''), ('SEAL_KEY_REPO', 'connector'), ('SEAL_KEY_REPO', 'Lascade-Co/..'),
    ('SEAL_KEY_REPO', 'Lascade-Co/x?ref=main#'), ('SEAL_KEY_REF', 'main'), ('SEAL_KEY_REF', ''),
    ('SEAL_TOKEN', '')])
def test_caller_key_settings_validated(tmp_path, shim_dir, keypair, key_api, var, value):
    """Bad settings fail before any request; SEAL_TOOLKIT_DIR without SEAL_KEY_REPO never uses this repo's key."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    env = caller_env(tmp_path, shim_dir, key_api)
    env[var] = value
    r = run_install(env)
    assert r.returncode != 0
    assert key_api['requests'] == []
    assert not [u for u in logged_urls(tmp_path) if 'raw.githubusercontent.com' in u]
    assert_not_installed(tmp_path, env)


def test_crlf_caller_key_is_accepted(tmp_path, shim_dir, keypair, key_api):
    """A key file committed with Windows line endings: one trailing CR is stripped."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_text().strip().encode() + b'\r\n')
    env = caller_env(tmp_path, shim_dir, key_api)
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    assert (Path(env['RUNNER_TEMP']) / 'seal' / 'repo-key.pub').read_text() == keypair['pub'].read_text()


def test_rate_limited_key_fetch_says_so(tmp_path, shim_dir, key_api):
    key_api['routes'][KEY_PATH] = (403, b'{"message":"API rate limit exceeded for installation ID 1."}')
    env = caller_env(tmp_path, shim_dir, key_api)
    r = run_install(env)
    assert r.returncode != 0
    assert 'rate limit' in r.stderr and '.github/seal/repo-key.pub' in r.stderr
    assert 'contents: read' not in r.stderr
    assert_not_installed(tmp_path, env)


SPIED = ('tar', 'python3', 'uname', 'sha256sum', 'shasum', 'od', 'date')


def spy_dir(tmp_path):
    """PATH wrappers that log each call's argv and environment to $SHIM_ARGV_LOG, then run the real tool."""
    d = tmp_path / 'spy'
    d.mkdir()
    for t in SPIED:
        real = shutil.which(t)
        if real:
            (d / t).write_text(f'#!/bin/bash\n{{ echo "spy:{t} $*"; env; }} >>"$SHIM_ARGV_LOG"\nexec "{real}" "$@"\n')
            (d / t).chmod(0o755)
    return d


@pytest.mark.parametrize('ambient', [None, 'ambient-decoy'], ids=['plain', 'ambient-seal_token'])
def test_token_never_in_child_argv_env_or_output(tmp_path, shim_dir, keypair, key_api, ambient):
    """The token reaches curl only on stdin: never in any child's argv or environment, nor in output,
    even when the caller's env exports a variable named like install.sh's internal one."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    env = caller_env(tmp_path, shim_dir, key_api)
    env.pop('SEAL_NO_SUDO')  # reach the sudo stub too (it refuses; nothing is linked)
    if ambient:
        env['seal_token'] = ambient
    env['PATH'] = f'{spy_dir(tmp_path)}:{env["PATH"]}'
    argv_log = tmp_path / 'argv.log'
    r = run_install(env, SHIM_ARGV_LOG=str(argv_log))
    assert r.returncode == 0, r.stderr
    calls = argv_log.read_text()
    assert 'api.github.com' in calls and 'FiloSottile/age' in calls
    for t in ('tar', 'python3', 'uname', 'sudo'):
        assert f'spy:{t} ' in calls, t
    api_call = next(l for l in calls.splitlines() if 'https://api.github.com/' in l)
    assert not re.search(r'(^| )(-[a-zA-Z]*L|--location)\b', api_call), api_call  # no redirects with the token
    assert TOKEN not in calls
    assert 'SEAL_TOKEN' not in calls and 'seal_token' not in calls
    assert TOKEN not in r.stdout + r.stderr
    assert TOKEN not in (tmp_path / 'github_env').read_text()
    h = {k.lower(): v for k, v in key_api['requests'][0]['headers'].items()}
    assert h['authorization'] == f'Bearer {TOKEN}'


COMPOSITE_VARS = ('SEAL_TOOLKIT_DIR', 'SEAL_KEY_REPO', 'SEAL_KEY_REF', 'SEAL_TOKEN')


@pytest.mark.parametrize('value', ['x', ''], ids=['set', 'set-empty'])
@pytest.mark.parametrize('var', COMPOSITE_VARS)
def test_verbatim_setup_refuses_composite_settings(tmp_path, shim_dir, keypair, key_api, var, value):
    """With SEAL_REF (this repo's setup step), an ambient SEAL_KEY_*/SEAL_TOKEN/SEAL_TOOLKIT_DIR can't redirect the key."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    env, _ = served_env(tmp_path, shim_dir, keypair)
    env['SHIM_API_BASE'] = key_api['base']
    attack = {'SEAL_TOOLKIT_DIR': str(HERE), 'SEAL_KEY_REPO': CALLER, 'SEAL_KEY_REF': SHA, 'SEAL_TOKEN': TOKEN}
    env.update(attack)
    env[var] = value
    r = run_install(env)
    assert r.returncode != 0
    assert 'SEAL_REF' in r.stderr
    assert key_api['requests'] == []
    assert logged_urls(tmp_path) == []
    assert TOKEN not in r.stdout + r.stderr
    assert_not_installed(tmp_path, env)
    # The same env without the stray setting installs this repo's key from SEAL_REF.
    for v in COMPOSITE_VARS:
        env.pop(v)
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    assert key_api['requests'] == []


@pytest.mark.parametrize('var', COMPOSITE_VARS)
def test_composite_path_requires_every_setting(tmp_path, shim_dir, keypair, key_api, var):
    """Without SEAL_REF (the seal action), all four settings are required; nothing is fetched without them."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    env = caller_env(tmp_path, shim_dir, key_api)
    del env[var]
    r = run_install(env)
    assert r.returncode != 0
    assert var in r.stderr
    assert key_api['requests'] == []
    assert logged_urls(tmp_path) == []
    assert_not_installed(tmp_path, env)


@pytest.mark.parametrize('repo', ['', None], ids=['empty', 'unset'])
def test_marker_omits_empty_repo(tmp_path, shim_dir, keypair, repo):
    env = base_env(tmp_path, shim_dir, keypair['pub'])
    if repo is None:
        del env['GITHUB_REPOSITORY']
    else:
        env['GITHUB_REPOSITORY'] = repo
    r = run_install(env)
    assert r.returncode == 0, r.stderr
    url = 'https://ci.lascade.com/?run=42&attempt=1'
    assert r.stdout.count(url) == 1 and 'repo=' not in r.stdout
    runner = Runner(tmp_path, env, keypair)
    s = runner.step('echo hi')
    assert s.returncode == 0
    assert s.stdout.splitlines() == [f'🔒 sealed {runner.name}#1 {url}']


# --- composite actions ---------------------------------------------------------

def load_action(path):
    return yaml.safe_load(path.read_text())


def test_seal_action_is_composite():
    a = load_action(SEAL_ACTION)
    assert a['runs']['using'] == 'composite'
    assert {k: v['default'] for k, v in a['inputs'].items()} == {
        'token': '${{ github.token }}', 'repository': '${{ github.repository }}', 'ref': '${{ github.sha }}'}
    assert all(not v.get('required', False) for v in a['inputs'].values())
    (step,) = a['runs']['steps']
    assert set(step) <= {'name', 'shell', 'env', 'run'}
    assert step['shell'] == 'bash'
    assert step['env'] == {'SEAL_TOKEN': '${{ inputs.token }}', 'SEAL_KEY_REPO': '${{ inputs.repository }}',
                           'SEAL_KEY_REF': '${{ inputs.ref }}'}
    assert '${{' not in step['run']
    assert 'SEAL_TOOLKIT_DIR="$GITHUB_ACTION_PATH/../../scripts/seal"' in step['run']
    assert 'bash "$GITHUB_ACTION_PATH/../../scripts/seal/install.sh"' in step['run']
    for knob in (*TEST_KNOBS, 'SEAL_REF='):
        assert knob not in step['run'], knob


def test_seal_upload_action_is_composite():
    a = load_action(UPLOAD_ACTION)
    assert not a.get('inputs')
    assert a['runs']['using'] == 'composite'
    (step,) = a['runs']['steps']
    assert set(step) <= {'name', 'if', 'uses', 'with'}
    assert step.get('if', 'always()') == 'always()'
    assert step['uses'] == 'actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9'  # v7.0.2
    assert step['with'] == {
        'name': '${{ env.SEAL_LOG_ARTIFACT }}',
        'path': '${{ runner.temp }}/seal/out/',
        'retention-days': 7,
        'if-no-files-found': 'ignore',
    }


def test_seal_action_step_installs(tmp_path, shim_dir, keypair, key_api):
    """Run the composite's step as the runner would: its env with the default inputs, `shell: bash`."""
    key_api['routes'][KEY_PATH] = (200, keypair['pub'].read_bytes())
    step = load_action(SEAL_ACTION)['runs']['steps'][0]
    inputs = {'token': TOKEN, 'repository': CALLER, 'ref': SHA}
    env = caller_env(tmp_path, shim_dir, key_api)
    for k in ('SEAL_TOOLKIT_DIR', 'SEAL_KEY_REPO', 'SEAL_KEY_REF', 'SEAL_TOKEN'):
        del env[k]
    for k, v in step['env'].items():
        env[k] = re.sub(r'\$\{\{ inputs\.(\w+) \}\}', lambda m: inputs[m.group(1)], v)
    env['GITHUB_ACTION_PATH'] = str(SEAL_ACTION.parent)
    script = tmp_path / 'composite-step.sh'
    script.write_text(step['run'])
    cwd = tmp_path / 'no-checkout'
    cwd.mkdir()
    r = subprocess.run(['bash', '--noprofile', '--norc', '-eo', 'pipefail', script], env=env, cwd=cwd,
                       capture_output=True, text=True, stdin=subprocess.DEVNULL, start_new_session=True,
                       timeout=120)
    assert r.returncode == 0, r.stderr
    assert [q['path'] for q in key_api['requests']] == [KEY_PATH]
    assert not [u for u in logged_urls(tmp_path) if 'raw.githubusercontent.com' in u]
    runner = Runner(tmp_path, env, keypair)
    s = runner.step('echo composite-ok')
    assert s.stdout.splitlines() == [
        f'🔒 sealed {runner.name}#1 https://ci.lascade.com/?repo=connector&run=42&attempt=1']
    assert runner.decrypt(runner.out / '1.log.age') == b'composite-ok\n'


# --- seal-bash ----------------------------------------------------------------

def test_stdout_shows_only_marker_and_masks(runner):
    r = runner.step('echo CANARY-123; echo "::add-mask::m1"; echo "::error::CANARY-456" >&2')
    assert r.returncode == 0
    assert r.stdout.splitlines() == ['::add-mask::m1', f'🔒 sealed {runner.name}#1 {VIEWER}']
    assert r.stderr == ''
    log = runner.decrypt(runner.out / '1.log.age').decode()
    assert 'CANARY-123' in log and '::error::CANARY-456' in log
    assert not (runner.out / '1.summary.md.age').exists()


def test_masks_survive_binary_output(runner):
    r = runner.step(r'printf "bin\0ary\n"; echo "::add-mask::m2"; echo "x ::add-mask::not-a-command"')
    assert r.returncode == 0
    assert r.stdout.splitlines() == ['::add-mask::m2', f'🔒 sealed {runner.name}#1 {VIEWER}']


def test_indented_masks_are_forwarded(runner):
    """The runner honors ::add-mask:: after leading whitespace (seen on hosted runners), so forward it."""
    r = runner.step(r'echo "  ::add-mask::spaced"; printf "\t::add-mask::tabbed\n"; echo "  CANARY-789"')
    assert r.returncode == 0
    assert r.stdout.splitlines() == ['::add-mask::spaced', '::add-mask::tabbed', f'🔒 sealed {runner.name}#1 {VIEWER}']


def test_shell_modes_match_github(runner):
    r = runner.step('false | true; echo ok', mode='seal-bash-e')
    assert r.returncode == 0
    assert runner.decrypt(runner.out / '1.log.age') == b'ok\n'
    r = runner.step('false | true; echo ok', mode='seal-bash')
    assert r.returncode == 1
    assert runner.decrypt(runner.out / '2.log.age') == b''


def test_background_process_does_not_block(runner):
    pidfile = runner.root / 'bg.pid'
    start = time.monotonic()
    try:
        r = runner.step(f'sleep 300 & echo $! > {pidfile}; echo started', timeout=30)
        assert time.monotonic() - start < 5
        assert r.returncode == 0
        assert runner.decrypt(runner.out / '1.log.age') == b'started\n'
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_summary_is_sealed(runner):
    r = runner.step('echo "# r" >> "$GITHUB_STEP_SUMMARY"')
    assert r.returncode == 0
    assert runner.decrypt(runner.out / '1.summary.md.age') == b'# r\n'
    assert (runner.root / 'step_summary').read_text() == ''
    assert not list((runner.seal / 'log').iterdir())


def test_other_file_commands_still_work(runner):
    out = runner.root / 'github_output'
    out.touch()
    runner.env['GITHUB_OUTPUT'] = str(out)
    r = runner.step('echo "k=v" >> "$GITHUB_OUTPUT"')
    assert r.returncode == 0
    assert out.read_text() == 'k=v\n'


def test_exit_code_propagates(runner):
    r = runner.step('exit 3')
    assert r.returncode == 3
    r = runner.step('false; echo after')
    assert r.returncode == 1
    assert b'after' not in runner.decrypt(runner.out / '2.log.age')
    assert r.stdout.splitlines() == [f'🔒 sealed {runner.name}#2 {VIEWER}']


def test_segment_sealed_on_failure(runner):
    r = runner.step('echo before; exit 3')
    assert r.returncode == 3
    assert (runner.out / '1.log.age').exists()
    assert runner.decrypt(runner.out / '1.log.age') == b'before\n'
    assert not (runner.seal / 'log' / '1.log').exists()


def test_failed_encryption_fails_closed(runner):
    (runner.seal / 'repo-key.pub').write_text('garbage\n')
    r = runner.step('echo CANARY-ENC; echo "::add-mask::m3"')
    assert r.returncode != 0
    assert r.stdout.splitlines() == ['::add-mask::m3', f'🔒 seal failed for {runner.name}#1']
    assert 'CANARY-ENC' not in r.stdout + r.stderr
    assert not list((runner.seal / 'log').iterdir())
    assert not (runner.out / '1.log.age').exists()


def test_killed_step_is_sealed(runner):
    """A step process killed outright (e.g. by the OOM killer) still seals, with no shell notice."""
    r = runner.step('echo before; kill -KILL $$')
    assert r.returncode == 137
    assert r.stderr == ''
    assert r.stdout.splitlines() == [f'🔒 sealed {runner.name}#1 {VIEWER}']
    assert runner.decrypt(runner.out / '1.log.age') == b'before\n'


@pytest.mark.parametrize('sig, code', [(signal.SIGINT, 130), (signal.SIGTERM, 143)])
def test_segment_sealed_on_cancel(runner, sig, code):
    """The runner cancels a step by signalling the shell process; the segment is still sealed."""
    pidfile = runner.root / 'child.pid'
    log = runner.seal / 'log' / '1.log'
    p = subprocess.Popen([runner.bin / 'seal-bash', runner.script(
        f"echo before; bash -c 'echo $$ > {pidfile}; exec sleep 30'; echo after")],
        env=runner.env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True)
    try:
        deadline = time.monotonic() + 10
        while not (pidfile.exists() and pidfile.read_text().strip()):
            assert time.monotonic() < deadline, 'step did not start'
            time.sleep(0.05)
        start = time.monotonic()
        p.send_signal(sig)
        stdout, stderr = p.communicate(timeout=15)
        assert time.monotonic() - start < 7
        assert p.returncode == code
        assert stdout.splitlines() == [f'🔒 sealed {runner.name}#1 {VIEWER}']
        assert stderr == ''
        assert runner.decrypt(runner.out / '1.log.age') == b'before\n'
        assert not log.exists()
        bg = int(pidfile.read_text())
        time.sleep(0.2)
        with pytest.raises(ProcessLookupError):
            os.kill(bg, 0)
    finally:
        if p.poll() is None:
            p.kill()
            p.wait()
        if pidfile.exists() and pidfile.read_text().strip():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def pids_gone(pids, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        alive = []
        for pid in pids:
            try:
                os.kill(pid, 0)
                alive.append(pid)
            except ProcessLookupError:
                pass
        if not alive:
            return True
        time.sleep(0.05)
    return False


def test_cancel_kills_signal_ignoring_step(runner):
    """INT is forwarded and waited on; TERM ends a step that ignores both, with nothing left behind."""
    pidfile = runner.root / 'stubborn.pid'
    p = subprocess.Popen([runner.bin / 'seal-bash', runner.script(
        f"trap '' INT TERM; echo $$ > {pidfile}; sleep 300 & echo $! >> {pidfile}; echo running; wait")],
        env=runner.env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True)
    pids = []
    try:
        deadline = time.monotonic() + 10
        while len(pids) < 2:
            assert time.monotonic() < deadline, 'step did not start'
            time.sleep(0.05)
            pids = [int(x) for x in pidfile.read_text().split()] if pidfile.exists() else []
        p.send_signal(signal.SIGINT)
        time.sleep(1)
        assert p.poll() is None, 'seal-bash must keep waiting after SIGINT'
        start = time.monotonic()
        p.send_signal(signal.SIGTERM)
        stdout, stderr = p.communicate(timeout=10)
        assert time.monotonic() - start < 3
        assert p.returncode == 143
        assert stdout.splitlines() == [f'🔒 sealed {runner.name}#1 {VIEWER}']
        assert stderr == ''
        assert runner.decrypt(runner.out / '1.log.age') == b'running\n'
        assert not (runner.seal / 'log' / '1.log').exists()
        assert pids_gone(pids), 'a step process survived cancellation'
    finally:
        if p.poll() is None:
            p.kill()
            p.wait()
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_seq_increments(runner):
    a = runner.step('echo one')
    b = runner.step('echo two', mode='seal-bash-e')
    assert a.stdout.splitlines() == [f'🔒 sealed {runner.name}#1 {VIEWER}']
    assert b.stdout.splitlines() == [f'🔒 sealed {runner.name}#2 {VIEWER}']
    assert runner.decrypt(runner.out / '1.log.age') == b'one\n'
    assert runner.decrypt(runner.out / '2.log.age') == b'two\n'


def test_large_output_is_complete(runner):
    start = time.monotonic()
    r = runner.step('seq 1 200000')
    assert time.monotonic() - start < 20
    assert r.returncode == 0
    lines = runner.decrypt(runner.out / '1.log.age').decode().splitlines()
    assert lines == [str(i) for i in range(1, 200001)]


def test_seal_bash_fails_closed_without_install(tmp_path):
    script = tmp_path / 's.sh'
    script.write_text('echo CANARY-789\n')
    env = {'PATH': os.environ['PATH'], 'RUNNER_TEMP': str(tmp_path / 'empty')}
    r = subprocess.run(['bash', HERE / 'seal-bash', script], env=env, capture_output=True,
                       text=True, stdin=subprocess.DEVNULL)
    assert r.returncode != 0
    assert 'CANARY-789' not in r.stdout + r.stderr


# --- seal-pack / seal-unpack ------------------------------------------------------

@pytest.fixture
def tree(runner):
    work = runner.root / 'work'
    (work / 'a' / 'b').mkdir(parents=True)
    (work / 'a' / 'x.txt').write_text('hello x\n')
    (work / 'a' / 'b' / 'y.bin').write_bytes(os.urandom(1024 * 1024))
    (work / 'a' / 'l').symlink_to('x.txt')
    return work


def pack_and_unpack(runner, work, *args):
    r = runner.tool('seal-pack', *args, cwd=work)
    assert r.returncode == 0, r.stderr
    name = [str(a) for a in args if not str(a).startswith('--')][0]
    art = runner.seal / 'art' / name
    assert r.stdout == f'{art}\n'
    assert [p.name for p in art.iterdir()] == [f'{name}.tgz.age']
    dest = runner.root / f'unpacked-{name}'
    u = runner.tool('seal-unpack', art, dest, env=runner.unpack_env())
    assert u.returncode == 0, u.stderr
    assert not list(art.glob('*.age'))
    return dest


def listing(d):
    return sorted(str(p.relative_to(d)) for p in d.rglob('*'))


def test_pack_single_file(runner, tree):
    dest = pack_and_unpack(runner, tree, 'one', 'a/x.txt')
    assert listing(dest) == ['x.txt']
    assert (dest / 'x.txt').read_text() == 'hello x\n'


def test_pack_single_dir(runner, tree):
    dest = pack_and_unpack(runner, tree, 'dir', 'a/b')
    assert listing(dest) == ['y.bin']
    assert (dest / 'y.bin').read_bytes() == (tree / 'a' / 'b' / 'y.bin').read_bytes()


def test_pack_unpack_roundtrip(runner, tree):
    dest = pack_and_unpack(runner, tree, 'art', 'a/x.txt', 'a/b')
    assert listing(dest) == ['b', 'b/y.bin', 'x.txt']
    assert (dest / 'x.txt').read_bytes() == (tree / 'a' / 'x.txt').read_bytes()
    assert (dest / 'b' / 'y.bin').read_bytes() == (tree / 'a' / 'b' / 'y.bin').read_bytes()
    whole = pack_and_unpack(runner, tree, 'whole', 'a')
    assert listing(whole) == ['b', 'b/y.bin', 'l', 'x.txt']
    assert (whole / 'l').is_symlink() and os.readlink(whole / 'l') == 'x.txt'


def members(runner, name):
    tgz = runner.decrypt(runner.seal / 'art' / name / f'{name}.tgz.age')
    with tarfile.open(fileobj=io.BytesIO(tgz)) as t:
        return [os.path.normpath(n) for n in t.getnames()]


def test_pack_expands_quoted_glob(runner, tree):
    """The root is the glob's search path (`a`), as upload-artifact computes it."""
    dest = pack_and_unpack(runner, tree, 'glob', 'a/**/*.bin')
    assert listing(dest) == ['b', 'b/y.bin']


def test_pack_nested_matches_archived_once(runner, tree):
    r = runner.tool('seal-pack', 'nest', 'a/**/*', cwd=tree)
    assert r.returncode == 0, r.stderr
    names = members(runner, 'nest')
    assert len(names) == len(set(names)), names
    assert sorted(names) == ['b/y.bin', 'l', 'x.txt']


def test_pack_root_is_search_path_not_matches(runner):
    work = runner.root / 'work'
    for d in ('android', 'ios'):
        (work / 'source/build/ci/release' / d).mkdir(parents=True)
        (work / 'source/build/ci/release' / d / f'{d}.json').write_text('{}')
    r = runner.tool('seal-pack', 'diag', 'source/build/**/*.json', cwd=work)
    assert r.returncode == 0, r.stderr
    assert sorted(members(runner, 'diag')) == ['ci/release/android/android.json', 'ci/release/ios/ios.json']
    r = runner.tool('seal-pack', 'one', 'source/build/**/android.json', cwd=work)
    assert r.returncode == 0, r.stderr
    assert members(runner, 'one') == ['ci/release/android/android.json']


def test_pack_missing_literal_keeps_root(runner):
    work = runner.root / 'work'
    (work / 'build/app/outputs/apk').mkdir(parents=True)
    (work / 'build/app/outputs/apk/app.apk').write_text('apk')
    r = runner.tool('seal-pack', 'rel', 'releasenotes.txt', 'build/app/outputs/**', cwd=work)
    assert r.returncode == 0, r.stderr
    assert members(runner, 'rel') == ['build/app/outputs/apk/app.apk']


def test_pack_excludes_hidden_by_default(runner):
    work = runner.root / 'work'
    (work / 'd' / '.git').mkdir(parents=True)
    (work / 'd' / '.git' / 'config').write_text('[core]')
    (work / 'd' / '.env').write_text('X=1')
    (work / 'd' / 'keep.txt').write_text('keep')
    r = runner.tool('seal-pack', 'nohid', 'd', cwd=work)
    assert r.returncode == 0, r.stderr
    assert members(runner, 'nohid') == ['keep.txt']
    r = runner.tool('seal-pack', 'nohidglob', 'd/**', cwd=work)
    assert r.returncode == 0, r.stderr
    assert members(runner, 'nohidglob') == ['keep.txt']
    r = runner.tool('seal-pack', '--include-hidden', 'hid', 'd', cwd=work)
    assert r.returncode == 0, r.stderr
    assert sorted(members(runner, 'hid')) == ['.env', '.git/config', 'keep.txt']
    r = runner.tool('seal-pack', '--allow-empty', 'onlyhid', 'd/.env', cwd=work)
    assert r.returncode == 0, r.stderr
    assert not list((runner.seal / 'art' / 'onlyhid').iterdir())


@pytest.fixture
def gnu_tar_shim(runner):
    """A `tar` that reports itself as GNU tar, logs GNU-only flags and exits $TAR_SHIM_RC."""
    real = shutil.which('tar')
    d = runner.root / 'gnutar'
    d.mkdir()
    (d / 'tar').write_text(f'''#!/bin/bash
[ "$1" = --version ] && {{ echo "tar (GNU tar) 1.35"; exit 0; }}
args=()
for a in "$@"; do
  case "$a" in --warning=*|--verbatim-files-from|--no-unquote) echo "$a" >>"{d}/flags" ;; *) args+=("$a") ;; esac
done
"{real}" "${{args[@]}}" || exit $?
exit "${{TAR_SHIM_RC:-0}}"
''')
    (d / 'tar').chmod(0o755)
    return d


def test_pack_gnu_tar_file_changed_is_success(runner, tree, gnu_tar_shim):
    env = {**runner.env, 'PATH': f'{gnu_tar_shim}:{runner.env["PATH"]}', 'TAR_SHIM_RC': '1'}
    r = runner.tool('seal-pack', 'changed', 'a/x.txt', cwd=tree, env=env)
    assert r.returncode == 0, r.stderr
    assert '--warning=no-file-changed' in (gnu_tar_shim / 'flags').read_text().split()
    assert members(runner, 'changed') == ['x.txt']
    env['TAR_SHIM_RC'] = '2'
    r = runner.tool('seal-pack', 'broken', 'a/x.txt', cwd=tree, env=env)
    assert r.returncode != 0
    assert not list((runner.seal / 'art' / 'broken').iterdir())


def test_pack_bsd_tar_exit_1_fails(runner, tree, gnu_tar_shim):
    shim = gnu_tar_shim / 'tar'
    shim.write_text(shim.read_text().replace('tar (GNU tar) 1.35', 'bsdtar 3.5.3'))
    env = {**runner.env, 'PATH': f'{gnu_tar_shim}:{runner.env["PATH"]}', 'TAR_SHIM_RC': '1'}
    r = runner.tool('seal-pack', 'bsd', 'a/x.txt', cwd=tree, env=env)
    assert r.returncode != 0
    assert not (gnu_tar_shim / 'flags').exists()


def test_pack_is_one_clean_gzip_stream(runner, tree):
    """No bytes after the gzip member: bsdtar -z pads with zeros, which browsers' DecompressionStream rejects."""
    r = runner.tool('seal-pack', 'clean', 'a/x.txt', cwd=tree)
    assert r.returncode == 0, r.stderr
    tgz = runner.decrypt(runner.seal / 'art' / 'clean' / 'clean.tgz.age')
    d = zlib.decompressobj(31)
    d.decompress(tgz)
    assert d.eof and d.unused_data == b''


def test_pack_absolute_path(runner, tree):
    dest = pack_and_unpack(runner, tree, 'abs', tree / 'a' / 'x.txt')
    assert listing(dest) == ['x.txt']


@pytest.mark.skipif(not shutil.which('xattr'), reason='needs the macOS xattr tool')
def test_pack_omits_xattrs(runner, tree):
    """bsdtar archives xattrs by default; GNU tar on Linux consumers would warn on them."""
    subprocess.run(['xattr', '-w', 'com.example.seal', 'v', tree / 'a' / 'x.txt'], check=True)
    r = runner.tool('seal-pack', 'xa', 'a/x.txt', cwd=tree)
    assert r.returncode == 0, r.stderr
    tgz = runner.decrypt(runner.seal / 'art' / 'xa' / 'xa.tgz.age')
    assert b'xattr' not in gzip.decompress(tgz)


def test_pack_no_match(runner, tree):
    r = runner.tool('seal-pack', 'none', 'nope/*', 'missing.txt', cwd=tree)
    assert r.returncode == 1
    assert not list(runner.seal.glob('art/*/*.age'))
    r = runner.tool('seal-pack', '--allow-empty', 'none', 'nope/*', cwd=tree)
    assert r.returncode == 0
    art = runner.seal / 'art' / 'none'
    assert r.stdout == f'{art}\n'
    assert art.is_dir() and not list(art.iterdir())


def test_pack_rejects_unsafe_name(runner, tree):
    for name in ('../x', 'a/b', '..', ''):
        r = runner.tool('seal-pack', name, 'a/x.txt', cwd=tree)
        assert r.returncode != 0, name
    assert not (runner.seal / 'x').exists()


def test_unpack_without_key_fails(runner, tree):
    r = runner.tool('seal-pack', 'k', 'a/x.txt', cwd=tree)
    assert r.returncode == 0, r.stderr
    art = runner.seal / 'art' / 'k'
    dest = runner.root / 'dest'
    u = runner.tool('seal-unpack', art, dest)
    assert u.returncode != 0
    assert not dest.exists() or not list(dest.iterdir())
    assert (art / 'k.tgz.age').exists()


def test_unpack_wrong_key_keeps_ciphertext(runner, tree):
    r = runner.tool('seal-pack', 'w', 'a/x.txt', cwd=tree)
    assert r.returncode == 0, r.stderr
    art = runner.seal / 'art' / 'w'
    other = runner.root / 'other.txt'
    subprocess.run([runner.bin / 'age-keygen', '-o', other], check=True, capture_output=True)
    wrong = next(l for l in other.read_text().splitlines() if l.startswith('AGE-SECRET-KEY-'))
    u = runner.tool('seal-unpack', art, env={**runner.env, 'REPO_AGE_KEY': wrong})
    assert u.returncode != 0
    assert (art / 'w.tgz.age').exists()
    assert wrong not in u.stdout + u.stderr


def test_unpack_no_files(runner):
    empty = runner.root / 'emptydir'
    empty.mkdir()
    u = runner.tool('seal-unpack', empty, env=runner.unpack_env())
    assert u.returncode == 1


def test_unpack_defaults_dest_to_dir(runner, tree):
    r = runner.tool('seal-pack', 'here', 'a/x.txt', cwd=tree)
    art = runner.seal / 'art' / 'here'
    u = runner.tool('seal-unpack', art, env=runner.unpack_env())
    assert u.returncode == 0, u.stderr
    assert listing(art) == ['x.txt']
