"""Tests for the runner sealing toolkit (install.sh, seal-bash, seal-pack, seal-unpack).

Each test runs install.sh against a fake runner in tmp_path. install.sh downloads
and verifies the real age v1.3.2 release; a `curl` shim on PATH replays the
tarball from a per-session cache under the pytest tmp dir after the first
download. The tests generate a throwaway age key and point SEAL_PUBKEY_FILE at it.
"""
import gzip
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tarfile
import time

import pytest

HERE = Path(__file__).resolve().parent
INSTALL = HERE / 'install.sh'
REAL_CURL = shutil.which('curl')
# Example recipient from the age README; only used to bootstrap age-keygen.
BOOTSTRAP_RECIPIENT = 'age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p'
VIEWER = 'https://ci.lascade.com/?run=42&attempt=1'


@pytest.fixture(scope='session')
def shim_dir(tmp_path_factory):
    """A PATH dir whose `curl` serves the age release tarball from a session cache.

    Raw control files of this repo are served from $SHIM_SERVE_DIR when a test sets
    it; every requested URL is appended to $SHIM_LOG when set. Anything else fails.
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
case "$url" in
  https://github.com/FiloSottile/age/releases/download/*)
    c="{cache}/${{url##*/}}"
    if [ -s "$c" ]; then cp "$c" "$out"; else "{REAL_CURL}" "$@"; cp "$out" "$c"; fi ;;
  https://raw.githubusercontent.com/Lascade-Co/actions/*/scripts/seal/*)
    f="${{SHIM_SERVE_DIR:-/nonexistent}}/${{url##*/}}"
    [ -e "$f" ] || exit 22
    cp "$f" "$out" ;;
  *) exit 22 ;;
esac
''')
    (d / 'curl').chmod(0o755)
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
    r = runner.step(r'printf "bin\0ary\n"; echo "::add-mask::m2"; echo "  ::add-mask::indented"')
    assert r.returncode == 0
    assert r.stdout.splitlines() == ['::add-mask::m2', f'🔒 sealed {runner.name}#1 {VIEWER}']


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
