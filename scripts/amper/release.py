#!/usr/bin/env python3
"""Trusted release orchestration for Kotlin Toolchain (Amper) apps; see docs/amper-ci.md.

Run from the central checkout (`control/scripts/amper/release.py`): it loads its siblings by path, because
scripts/kmp and scripts/amper both have a control.py. Binaries go straight to a draft GitHub release in the
private source repo; the release job verifies that draft, pushes the version commit and tag, then publishes it.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

HERE = Path(__file__).resolve().parent


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


apps = _load('amper_apps', HERE.parent / 'kmp' / 'apps.py')
kmp_control = _load('kmp_control', HERE.parent / 'kmp' / 'control.py')
kmp_release = _load('kmp_release', HERE.parent / 'kmp' / 'release.py')
amper_control = _load('amper_control', HERE / 'control.py')

BOT_SUBJECT = kmp_release.BOT_SUBJECT
SEMVER = re.compile(r'([0-9]+)\.([0-9]+)\.([0-9]+)')
REQUIRED = ('.aab', '.apk', '.ipa', 'mapping.txt', 'dSYMs.zip')
OPTIONAL = ('native-debug-symbols.zip',)
# A version commit may only change these build settings (set-version's lines), never IDs or signing.
VERSION_LINE = re.compile(r'^[+-]\s*(versionCode:|versionName:|MARKETING_VERSION = |CURRENT_PROJECT_VERSION = )')
CHUNK = 1 << 20
MIN_TOKEN = 16


def validate(payload, central_repo):
    if central_repo.lower() != 'lascade-co/actions':
        raise ValueError('Unexpected orchestration repository')
    repo = payload.get('repo', '')
    if not isinstance(repo, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise ValueError('Invalid source repository')
    try:
        app = apps.load_app(repo)
    except SystemExit as error:
        raise ValueError(str(error)) from None
    # Prod signing keys come from this Infisical project, so it must be the one registered for the repo.
    if not app.get('project_slug') or payload.get('project_slug') != app['project_slug']:
        raise ValueError('project_slug does not match the app registry')
    if payload.get('branch') != 'main':
        raise ValueError('Release requires main')
    sha = payload.get('sha', '')
    if not isinstance(sha, str) or not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('Release requires an immutable source SHA')
    owner, name = repo.split('/')
    return dict(repo=repo, owner=owner, name=name, branch='main', sha=sha, project_slug=app['project_slug'])


def resolve(payload, central_repo, get=kmp_control.api):
    values = validate(payload, central_repo)
    head = get(f'repos/{values["repo"]}/commits/main')
    subject = head['commit']['message'].splitlines()[0]
    # A manual dispatch on the bot's own bump must not create another release.
    if BOT_SUBJECT.fullmatch(subject) and ((head.get('author') or {}).get('type') == 'Bot' or
                                          head['commit'].get('author', {}).get('name') == 'github-actions[bot]'):
        return dict(values, skip='true')
    if head['sha'] != values['sha']:
        raise ValueError('Source main advanced; refusing to release a stale dispatch')
    return dict(values, skip='false')


def next_version(version, build, labels):
    found = SEMVER.fullmatch(version)
    if not found or not re.fullmatch('[1-9][0-9]*', str(build)):
        raise ValueError('Source version must be X.Y.Z with a positive build number')
    major, minor, patch = map(int, found.groups())
    kind = 'major' if 'major' in labels else 'minor' if 'minor' in labels else 'patch'
    version = {'major': f'{major + 1}.0.0', 'minor': f'{major}.{minor + 1}.0', 'patch': f'{major}.{minor}.{patch + 1}'}[kind]
    return version, str(int(build) + 1), kind


def merged_labels(repo, sha, get=kmp_control.api):
    pulls = get(f'repos/{repo}/commits/{sha}/pulls')
    return sorted({label['name'] for pull in pulls if pull.get('merged_at') and pull['base']['ref'] == 'main'
                   for label in pull.get('labels', [])})


def bump(source, repo, sha, get=kmp_control.api):
    yml = amper_control.module_yaml(source, amper_control.manifest(source))
    current = (amper_control.yq_get('.settings.android.versionName', yml), amper_control.yq_get('.settings.android.versionCode', yml))
    version, build, kind = next_version(*current, merged_labels(repo, sha, get))
    return dict(version=version, build_number=build, tag='v' + version, bump=kind,
                prior_version=current[0], prior_build_number=current[1])


def release_assets(folder):
    paths = [p for p in Path(folder).rglob('*') if p.is_file()]
    selected = []
    for suffix in REQUIRED + OPTIONAL:
        matches = [p for p in paths if p.name.endswith(suffix)]
        if suffix in OPTIONAL and not matches:
            continue
        if len(matches) != 1 or not matches[0].stat().st_size:
            raise ValueError(f'Expected one nonempty release artifact ending in {suffix}')
        selected.append(matches[0])
    extra = sorted(p.name for p in set(paths) - set(selected))
    if extra:
        raise ValueError(f'Unexpected files among the release assets: {extra}')
    return selected


def occurrences(archive, needles):
    """Count needle hits across every zip entry, streamed, including hits split across read chunks."""
    count, overlap = 0, max(map(len, needles)) - 1
    with zipfile.ZipFile(archive) as z:
        for info in z.infolist():
            with z.open(info) as stream:
                tail = b''
                while chunk := stream.read(CHUNK):
                    window = tail + chunk
                    # Hits wholly inside the carried tail were counted in the previous window.
                    count += sum(window.count(n) - tail.count(n) for n in needles)
                    tail = window[-overlap:] if overlap else b''
    return count


def check_token(secrets_file, binaries):
    """Fail if the staging App Check debug token is in any binary, as UTF-8 or UTF-16. Never prints it."""
    try:
        token = kmp_control.parse_secret_file(Path(secrets_file).read_text()).get('APPCHECK_DEBUG_TOKEN', '')
    finally:
        Path(secrets_file).unlink(missing_ok=True)
    if len(token) < MIN_TOKEN:
        raise ValueError('The staging APPCHECK_DEBUG_TOKEN is missing or too short to search for')
    needles = [token.encode(), token.encode('utf-16-le'), token.encode('utf-16-be')]
    counts = {}
    for path in map(Path, binaries):
        counts[path.name] = occurrences(path, needles)
        print(f'{path.name}: {counts[path.name]} App Check debug token occurrences')
    leaked = [name for name, count in counts.items() if count]
    if leaked:
        raise ValueError(f'The App Check debug token is inside {leaked}; refusing to release')
    return counts


def mapping_id(resource):
    found = re.search(r'name="com\.google\.firebase\.crashlytics\.mapping_file_id"[^>]*>([0-9a-f]{16,})<',
                      Path(resource).read_text())
    if not found:
        raise ValueError(f'No Crashlytics mapping_file_id in {resource}')
    return found.group(1)


def check_mapping_id(resource, binaries):
    """The generated ID must be packaged, or Crashlytics cannot match crashes to the uploaded mapping."""
    value = mapping_id(resource)
    needles = [value.encode(), value.encode('utf-16-le')]
    missing = [Path(p).name for p in binaries if not occurrences(p, needles)]
    if missing:
        raise ValueError(f'The Crashlytics mapping_file_id was not packaged into {missing}')
    print(f'mapping_file_id {value} is packaged in {len(binaries)} binaries')


API = 'https://api.github.com/'
UPLOADS = 'https://uploads.github.com/'
SAFE_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]*')


def http(method, url, body=None, headers=None, out=None):
    """GitHub REST call with $GH_TOKEN. Raises urllib HTTPError on any 4xx/5xx; streams to `out` when given."""
    request = urllib.request.Request(url, data=body, method=method, headers={
        'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28', **(headers or {})})
    # Unredirected, so asset downloads do not send the token on to the storage redirect.
    request.add_unredirected_header('Authorization', 'Bearer ' + os.environ['GH_TOKEN'])
    with urllib.request.urlopen(request, timeout=900) as response:
        if out is not None:
            shutil.copyfileobj(response, out)
            return None
        data = response.read()
        return json.loads(data) if data else None


def api_json(method, path, payload):
    return http(method, API + path, json.dumps(payload).encode(), {'Content-Type': 'application/json'})


def not_found(error):
    return isinstance(error, urllib.error.HTTPError) and error.code == 404


def check_target(repo, tag):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo) or not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag):
        raise ValueError('Invalid release target')


def published_release(repo, tag):
    """The published release for tag, or None on 404; drafts never appear here. Other errors propagate."""
    try:
        return http('GET', f'{API}repos/{repo}/releases/tags/{tag}')
    except urllib.error.HTTPError as error:
        if not_found(error):
            return None
        raise


def find_draft(repo, tag, pages=10):
    drafts = []
    for page in range(1, pages + 1):
        batch = http('GET', f'{API}repos/{repo}/releases?per_page=100&page={page}')
        drafts += [r for r in batch if r.get('draft') and r.get('tag_name') == tag]
        if len(batch) < 100:
            break
    if len(drafts) > 1:
        raise ValueError(f'{len(drafts)} draft releases for {tag}; delete the extras')
    return drafts[0] if drafts else None


def draft(repo, tag, sha, notes_path):
    """Create the draft once (prepare), before the build jobs upload to it by ID. Its tag is pushed later."""
    check_target(repo, tag)
    if published_release(repo, tag):
        raise ValueError(f'{tag} is already published; refusing to change it')
    fields = dict(tag_name=tag, target_commitish=sha, name=f'Release {tag}', body=Path(notes_path).read_text(),
                  draft=True, prerelease=False)
    existing = find_draft(repo, tag)
    if existing:
        result = api_json('PATCH', f'repos/{repo}/releases/{existing["id"]}', fields)
    else:
        result = api_json('POST', f'repos/{repo}/releases', fields)
    if not result.get('draft') or result.get('tag_name') != tag:
        raise ValueError('GitHub did not return the expected draft release')
    return dict(release_id=str(result['id']))


def get_draft(repo, release_id, tag):
    check_target(repo, tag)
    if not re.fullmatch('[1-9][0-9]*', str(release_id)):
        raise ValueError('Invalid release ID')
    found = http('GET', f'{API}repos/{repo}/releases/{release_id}')
    if not found.get('draft') or found.get('tag_name') != tag:
        raise ValueError(f'Release {release_id} is not the draft for {tag}; refusing to change it')
    return found


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        while chunk := stream.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def upload(repo, release_id, tag, files):
    """Upload (replacing same-named assets) to the draft by ID; returns name:sha256 pairs for the job outputs."""
    found = get_draft(repo, release_id, tag)
    sums = []
    for path in map(Path, files):
        if not SAFE_NAME.fullmatch(path.name):
            raise ValueError(f'Unsafe asset name {path.name!r}')
        for asset in found.get('assets', []):
            if asset['name'] == path.name:
                http('DELETE', f'{API}repos/{repo}/releases/assets/{asset["id"]}')
        with path.open('rb') as body:
            http('POST', f'{UPLOADS}repos/{repo}/releases/{release_id}/assets?name={urllib.parse.quote(path.name)}', body,
                 {'Content-Type': 'application/octet-stream', 'Content-Length': str(path.stat().st_size)})
        sums.append(f'{path.name}:{sha256(path)}')
        print(sums[-1])
    return dict(checksums=' '.join(sums))


def parse_checksums(text):
    pairs = dict(item.split(':', 1) for item in text.split())
    if any(not SAFE_NAME.fullmatch(n) or not re.fullmatch('[0-9a-f]{64}', h) for n, h in pairs.items()):
        raise ValueError('Malformed expected checksums')
    return pairs


def download(repo, release_id, tag, dest, expected, notes_out):
    """Download every draft asset by ID and require exactly the expected names with matching SHA-256."""
    found = get_draft(repo, release_id, tag)
    want = parse_checksums(expected)
    names = sorted(asset['name'] for asset in found.get('assets', []))
    if names != sorted(want):
        raise ValueError(f'Draft assets {names} differ from the uploaded set {sorted(want)}')
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    for asset in found['assets']:
        target = dest / asset['name']
        with target.open('wb') as out:
            http('GET', f'{API}repos/{repo}/releases/assets/{asset["id"]}', headers={'Accept': 'application/octet-stream'}, out=out)
        if sha256(target) != want[asset['name']]:
            raise ValueError(f'{asset["name"]} does not match the SHA-256 its build job uploaded')
    Path(notes_out).write_text(found.get('body') or '')


def publish(repo, release_id, tag, folder, notes_path):
    """Last step: publishing fires the source's publish-kmp, so every asset is already on the draft."""
    kmp_release.check_notes(notes_path)
    release_assets(folder)
    get_draft(repo, release_id, tag)
    try:
        http('GET', f'{API}repos/{repo}/git/ref/tags/{tag}')
    except urllib.error.HTTPError as error:
        if not_found(error):
            raise ValueError(f'The tag {tag} is not pushed; refusing to publish') from None
        raise
    api_json('PATCH', f'repos/{repo}/releases/{release_id}',
             dict(tag_name=tag, body=Path(notes_path).read_text(), draft=False, prerelease=False))


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def push_version(source_sha, version, number):
    """Commit the set-version edit and tag it in one atomic push leased on the pinned main SHA."""
    if not re.fullmatch('[0-9a-f]{40}', source_sha) or git('rev-parse', 'HEAD') != source_sha:
        raise ValueError('Version commit must start at the pinned source SHA')
    if not SEMVER.fullmatch(version) or not re.fullmatch('[1-9][0-9]*', number):
        raise ValueError('Invalid release version')
    m = amper_control.manifest('.')
    allowed = set(m['version_files']) | {m['ios_project'] + '/project.pbxproj'}
    changed = set(git('diff', '--name-only').splitlines())
    if git('diff', '--cached', '--name-only') or not changed or not changed <= allowed or not set(m['version_files']) <= changed:
        raise ValueError(f'Only the version files {sorted(allowed)} may change; found {sorted(changed)}')
    lines = [line for line in git('diff', '-U0', '--no-color').splitlines()
             if line[:1] in '+-' and not line.startswith(('+++', '---'))]
    if any(not VERSION_LINE.match(line) for line in lines):
        raise ValueError('The version commit may change only versionCode, versionName, MARKETING_VERSION '
                         'and CURRENT_PROJECT_VERSION')
    subject = f'chore(release): v{version} ({number}) [skip ci]'
    refs = dict(line.split()[::-1] for line in git('ls-remote', 'origin', 'refs/heads/main', f'refs/tags/v{version}',
                                                    f'refs/tags/v{version}^{{}}').splitlines())
    remote_main = refs.get('refs/heads/main')
    remote_tag = refs.get(f'refs/tags/v{version}^{{}}', refs.get(f'refs/tags/v{version}'))
    if remote_main != source_sha or remote_tag:
        # Resume only the exact transaction already pushed; never adopt a different release or contributor commit.
        if remote_main and remote_tag == remote_main:
            subprocess.run(['git', 'fetch', '--no-tags', 'origin', remote_main], check=True)
            if (git('show', '-s', '--format=%P', remote_main) == source_sha and
                    git('show', '-s', '--format=%s', remote_main) == subject and
                    git('show', '-s', '--format=%an', remote_main) == 'github-actions[bot]' and
                    set(git('diff', '--name-only', source_sha, remote_main).splitlines()) == changed and
                    all(subprocess.check_output(['git', 'show', f'{remote_main}:{path}']) == Path(path).read_bytes()
                        for path in changed)):
                return dict(release_sha=remote_main)
        raise ValueError('Source main or release tag changed; refusing an unrelated release retry')
    for args in [('config', 'user.name', 'github-actions[bot]'),
                 ('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'),
                 ('add', '--', *sorted(changed)), ('commit', '-m', subject), ('tag', '-a', f'v{version}', '-m', f'v{version}')]:
        subprocess.run(['git', *args], check=True)
    subprocess.run(['git', 'push', '--atomic', f'--force-with-lease=refs/heads/main:{source_sha}',
                    'origin', 'HEAD:refs/heads/main', f'refs/tags/v{version}'], check=True)
    return dict(release_sha=git('rev-parse', 'HEAD'))


def chat_id(secrets_file):
    try:
        value = kmp_control.parse_secret_file(Path(secrets_file).read_text()).get('TELEGRAM_CHAT_ID', '')
    finally:
        Path(secrets_file).unlink(missing_ok=True)
    if not re.fullmatch(r'-?[0-9]+|@[A-Za-z0-9_]+', value):
        raise ValueError('TELEGRAM_CHAT_ID is missing or malformed')
    return dict(chat_id=value)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for name, options in {
        'validate': (), 'resolve': (), 'push-version': (),
        'bump': ('source', 'repo', 'sha'),
        'notes': ('template', 'prompt'),
        'check-notes': ('notes',),
        'draft': ('repo', 'tag', 'sha', 'notes'),
        'upload': ('repo', 'release_id', 'tag'),
        'download': ('repo', 'release_id', 'tag', 'dest', 'expected', 'notes_out'),
        'verify-assets': ('assets', 'notes'),
        'check-token': ('secrets_file',),
        'check-mapping-id': ('resource_file',),
        'publish': ('repo', 'release_id', 'tag', 'assets', 'notes'),
        'chat-id': ('secrets_file',),
    }.items():
        item = sub.add_parser(name)
        for option in options:
            item.add_argument('--' + option.replace('_', '-'), required=True)
        if name in ('upload', 'check-token', 'check-mapping-id'):
            item.add_argument('files', nargs='+')
    args = parser.parse_args(argv)
    event = lambda: json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())['client_payload']
    out = kmp_control.output
    if args.command == 'validate':
        out(validate(event(), os.environ['GITHUB_REPOSITORY']))
    elif args.command == 'resolve':
        out(resolve(event(), os.environ['GITHUB_REPOSITORY']))
    elif args.command == 'bump':
        out(bump(args.source, args.repo, args.sha))
    elif args.command == 'notes':
        out(kmp_release.notes(args.template, args.prompt))
    elif args.command == 'check-notes':
        kmp_release.check_notes(args.notes)
    elif args.command == 'draft':
        out(draft(args.repo, args.tag, args.sha, args.notes))
    elif args.command == 'upload':
        out(upload(args.repo, args.release_id, args.tag, args.files))
    elif args.command == 'download':
        download(args.repo, args.release_id, args.tag, args.dest, args.expected, args.notes_out)
    elif args.command == 'verify-assets':
        kmp_release.check_notes(args.notes)
        for path in release_assets(args.assets):
            print(f'{path.name}: {path.stat().st_size} bytes')
    elif args.command == 'check-token':
        check_token(args.secrets_file, args.files)
    elif args.command == 'check-mapping-id':
        check_mapping_id(args.resource_file, args.files)
    elif args.command == 'push-version':
        out(push_version(os.environ['SOURCE_SHA'], os.environ['VERSION'], os.environ['BUILD_NUMBER']))
    elif args.command == 'publish':
        publish(args.repo, args.release_id, args.tag, args.assets, args.notes)
    elif args.command == 'chat-id':
        out(chat_id(args.secrets_file))


if __name__ == '__main__':
    sys.exit(main())
