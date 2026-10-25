#!/usr/bin/env python3
"""Trusted KMP release orchestration; source scripts own version and build policy."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import control

BOT_SUBJECT = re.compile(r'^chore\(release\): v\d+\.\d+\.\d+ \([1-9]\d*\) \[skip ci\]$')
SOURCE_REPO = 'Lascade-Co/travel-animator-shared'


def validate(payload, central_repo):
    if central_repo.lower() != 'lascade-co/actions':
        raise ValueError('Unexpected orchestration repository')
    if payload.get('repo', '').lower() != SOURCE_REPO.lower():
        raise ValueError('Only the approved KMP source repository may release')
    if payload.get('branch') != 'main' or payload.get('project_slug') != 'travelanimator':
        raise ValueError('Release requires main and the travelanimator project')
    sha = payload.get('sha', '')
    if not isinstance(sha, str) or not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('Release requires an immutable source SHA')
    owner, name = SOURCE_REPO.split('/')
    return dict(repo=SOURCE_REPO, owner=owner, name=name, branch='main', sha=sha,
                project_slug='travelanimator')


def resolve(payload, central_repo, get=control.api):
    values = validate(payload, central_repo)
    head = get(f'repos/{values["repo"]}/commits/main')
    subject = head['commit']['message'].splitlines()[0]
    # A manual dispatch on the bot head must not create another patch release.
    if BOT_SUBJECT.fullmatch(subject) and ((head.get('author') or {}).get('type') == 'Bot' or
                                          head['commit'].get('author', {}).get('name') == 'github-actions[bot]'):
        values['skip'] = 'true'
        return values
    if head['sha'] != values['sha']:
        raise ValueError('Source main advanced; refusing to release a stale dispatch')
    values['skip'] = 'false'
    return values


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


# The Claude request must stay small: a first release once diffed the whole repository history
# (47 MB with binary content) and the API rejected it with HTTP 400.
MAX_PROMPT_COMMITS = 300
MAX_PROMPT_DIFF_CHARS = 150_000


def release_base(tags):
    """Last release tag; else the previous main state (first parent of the merge); else the root."""
    if tags:
        return git('rev-list', '-1', tags[0])
    parent = subprocess.run(['git', 'rev-parse', '--verify', '--quiet', 'HEAD^1'], capture_output=True, text=True)
    return parent.stdout.strip() if parent.returncode == 0 else git('rev-list', '--max-parents=0', 'HEAD').splitlines()[0]


def notes(prompt_template, prompt_path):
    tags = git('tag', '--merged', 'HEAD', '--sort=-version:refname').splitlines()
    tags = [tag for tag in tags if re.fullmatch(r'v?[0-9]+\.[0-9]+\.[0-9]+', tag)]
    base = release_base(tags)
    updated = bool(tags and Path('releasenotes.txt').is_file() and
                   git('log', f'{base}..HEAD', '--format=%H', '--', 'releasenotes.txt'))
    if not updated:
        commits = git('log', '--no-merges', f'--max-count={MAX_PROMPT_COMMITS}', '--format=- %s', f'{base}..HEAD')
        # Binary changes are summarized by git; NULs from oddly encoded text files are dropped.
        diff = git('diff', '--no-color', '--no-ext-diff', base, 'HEAD').replace('\x00', '')
        if len(diff) > MAX_PROMPT_DIFF_CHARS:
            diff = diff[:MAX_PROMPT_DIFF_CHARS] + '\n[diff truncated; rely on the commit list for the rest]'
        Path(prompt_path).write_text(Path(prompt_template).read_text() + '\n\n## Commits\n\n' + commits +
                                     '\n\n```diff\n' + diff + '\n```\n')
    return dict(base=base, outdated=str(not updated).lower())


def check_notes(path):
    text = Path(path).read_text().strip()
    if not text:
        raise ValueError('Release notes must not be empty')
    if len(text.encode('utf-16-le')) // 2 > 500:
        raise ValueError('Release notes exceed the Play 500 character limit; shorten without truncating')
    return text


def install_inputs(platform):
    targets = ([('KEYSTORE_BASE64', 'androidApp/keystore.jks'),
                ('SERVICE_JSON_BASE64', 'androidApp/google-services.json')]
               if platform == 'android' else
               [('SERVICE_PLIST_BASE64', 'iosApp/iosApp/Config/Release/GoogleService-Info.plist')])
    for key, filename in targets:
        value = os.environ.get(key, '')
        if not value:
            raise ValueError('Missing production build input ' + key)
        content = base64.b64decode(''.join(value.split()), validate=True)
        if not content:
            raise ValueError('Empty production build input ' + key)
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(0o600)


def metadata():
    # Read dependency identities only; version arithmetic is exclusively source-owned.
    kotlin = re.search(r'^kotlin\s*=\s*"([0-9A-Za-z.+-]+)"', Path('gradle/libs.versions.toml').read_text(), re.M)
    angle = re.search(r'val angleRevision = "([0-9a-f]{40})"', Path('shared/build.gradle.kts').read_text())
    if not kotlin or not angle:
        raise ValueError('Missing Kotlin version or ANGLE revision')
    return dict(kotlin_version=kotlin[1], angle_revision=angle[1])


def push_version(source_sha, version, number):
    if not re.fullmatch('[0-9a-f]{40}', source_sha):
        raise ValueError('Invalid source SHA')
    if git('rev-parse', 'HEAD') != source_sha:
        raise ValueError('Version commit must start at the pinned source SHA')
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', version) or not re.fullmatch('[1-9][0-9]*', number):
        raise ValueError('Invalid release version')
    changed = git('diff', '--name-only').splitlines()
    staged = git('diff', '--cached', '--name-only')
    if changed != ['gradle.properties'] or staged:
        raise ValueError('Only the source-owned gradle.properties bump may be committed')
    subject = f'chore(release): v{version} ({number}) [skip ci]'
    refs = dict(line.split()[::-1] for line in git('ls-remote', 'origin', 'refs/heads/main',
                f'refs/tags/v{version}', f'refs/tags/v{version}^{{}}').splitlines())
    remote_main = refs.get('refs/heads/main')
    remote_tag = refs.get(f'refs/tags/v{version}^{{}}', refs.get(f'refs/tags/v{version}'))
    if remote_main != source_sha or remote_tag:
        # Retry only the exact already-pushed transaction, never a different release
        # or contributor commit. Read objects into FETCH_HEAD without moving a branch.
        if remote_main and remote_tag == remote_main:
            subprocess.run(['git', 'fetch', '--no-tags', 'origin', remote_main], check=True)
            if (git('show', '-s', '--format=%P', remote_main) == source_sha and
                git('show', '-s', '--format=%s', remote_main) == subject and
                git('show', '-s', '--format=%an', remote_main) == 'github-actions[bot]' and
                git('diff', '--name-only', source_sha, remote_main) == 'gradle.properties' and
                git('show', f'{remote_main}:gradle.properties') == Path('gradle.properties').read_text().strip()):
                return dict(release_sha=remote_main)
        raise ValueError('Source main or release tag changed; refusing an unrelated release retry')
    # Never stage ignored signing files or generated release notes.
    for args in [('config', 'user.name', 'github-actions[bot]'),
                 ('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'),
                 ('add', '--', 'gradle.properties'),
                 ('commit', '-m', f'chore(release): v{version} ({number}) [skip ci]'),
                 ('tag', '-a', f'v{version}', '-m', f'v{version}')]:
        subprocess.run(['git', *args], check=True)
    # Atomic push means neither main nor the tag can be published on its own. The
    # source-SHA lease rejects concurrent app commits without overwriting them.
    subprocess.run(['git', 'push', '--atomic', f'--force-with-lease=refs/heads/main:{source_sha}',
                    'origin', 'HEAD:refs/heads/main', f'refs/tags/v{version}'], check=True)
    return dict(release_sha=git('rev-parse', 'HEAD'))


ARTIFACT_SUFFIXES = ('.aab', '.apk', '.ipa', 'native-debug-symbols.zip', 'mapping.txt', 'dSYMs.zip')


def release_assets(folder):
    paths = [p for p in Path(folder).rglob('*') if p.is_file()]
    selected = []
    for suffix in ARTIFACT_SUFFIXES:
        matches = [p for p in paths if p.name.endswith(suffix)]
        if len(matches) != 1 or not matches[0].stat().st_size:
            raise ValueError(f'Expected one nonempty release artifact ending in {suffix}')
        selected.append(matches[0])
    return selected


def publish(repo, tag, folder, notes_path):
    if repo != SOURCE_REPO or not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag):
        raise ValueError('Invalid release target')
    check_notes(notes_path)
    assets = release_assets(folder)
    # Publishing is the final step: release:published consumers see all six assets.
    # A rerun can repair a draft left by a failed asset upload, but never mutate a
    # release that has already been published.
    existing = subprocess.run(['gh', 'release', 'view', tag, '--repo', repo, '--json', 'isDraft'],
                              text=True, capture_output=True)
    if existing.returncode == 0:
        if not json.loads(existing.stdout)['isDraft']:
            raise ValueError('Release is already published; refusing to replace assets')
    else:
        subprocess.run(['gh', 'release', 'create', tag, '--repo', repo, '--verify-tag', '--draft',
                        '--title', f'Release {tag}', '--notes-file', notes_path], check=True)
    subprocess.run(['gh', 'release', 'upload', tag, '--repo', repo, '--clobber', *map(str, assets)], check=True)
    subprocess.run(['gh', 'release', 'edit', tag, '--repo', repo, '--notes-file', notes_path,
                    '--draft=false', '--prerelease=false'], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('validate', 'resolve', 'metadata', 'notes', 'check-notes', 'push-version', 'publish', 'verify-assets', 'android-inputs', 'ios-inputs'):
        item = sub.add_parser(command)
        if command == 'notes':
            item.add_argument('--template', required=True)
            item.add_argument('--prompt', required=True)
        elif command == 'check-notes': item.add_argument('path')
        elif command == 'verify-assets':
            item.add_argument('--assets', required=True)
            item.add_argument('--notes', required=True)
        elif command == 'publish':
            for option in ('repo', 'tag', 'assets', 'notes'): item.add_argument('--' + option, required=True)
    args = parser.parse_args()
    if args.command in ('validate', 'resolve'):
        payload = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())['client_payload']
        control.output((validate if args.command == 'validate' else resolve)(payload, os.environ['GITHUB_REPOSITORY']))
    elif args.command in ('android-inputs', 'ios-inputs'): install_inputs(args.command.split('-')[0])
    elif args.command == 'metadata': control.output(metadata())
    elif args.command == 'notes': control.output(notes(args.template, args.prompt))
    elif args.command == 'check-notes': check_notes(args.path)
    elif args.command == 'verify-assets':
        check_notes(args.notes)
        release_assets(args.assets)
    elif args.command == 'push-version':
        control.output(push_version(os.environ['SOURCE_SHA'], os.environ['VERSION'], os.environ['BUILD_NUMBER']))
    elif args.command == 'publish': publish(args.repo, args.tag, args.assets, args.notes)


if __name__ == '__main__':
    main()
