#!/usr/bin/env python3
"""Trusted orchestration helpers for the central KMP debug workflow (no build policy)."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.parse
import urllib.request


def api(path):
    request = urllib.request.Request('https://api.github.com/' + path, headers={
        'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
        'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def output(values):
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        for key, value in values.items():
            value = str(value)
            if '\n' in value or '\r' in value:
                raise ValueError('Unexpected multiline output: ' + key)
            stream.write(f'{key}={value}\n')


def validate_payload(payload, central_repo):
    repo = payload.get('repo', '')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise ValueError('Invalid source repository')
    owner, name = repo.split('/')
    if owner.lower() != central_repo.split('/')[0].lower():
        raise ValueError('Source must belong to the central repository organization')
    project = payload.get('project_slug', '')
    if not isinstance(project, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', project):
        raise ValueError('A nonempty Infisical project_slug is required')
    pr = str(payload.get('pr') or '')
    if pr and not re.fullmatch('[1-9][0-9]*', pr):
        raise ValueError('Invalid PR number')
    branch = payload.get('branch', '')
    if not pr and (not isinstance(branch, str) or not branch or '\n' in branch or '\r' in branch):
        raise ValueError('A branch is required for manual dispatch')
    return dict(owner=owner, name=name, repo=repo, pr=pr, branch=branch, project_slug=project)


def resolve(payload, central_repo, get=api):
    values = validate_payload(payload, central_repo)
    repo, pr = values['repo'], values['pr']
    if pr:
        pull = get(f'repos/{repo}/pulls/{pr}')
        if pull['head']['repo'] is None or pull['head']['repo']['full_name'].lower() != repo.lower():
            raise ValueError('Fork PRs cannot enter the secret-bearing build pipeline')
        if pull['state'] != 'open':
            raise ValueError('PR must be open')
        values.update(branch=pull['head']['ref'], sha=pull['head']['sha'], title=pull.get('title', 'Debug APK'))
    else:
        commit = get(f'repos/{repo}/commits/' + urllib.parse.quote(values['branch'], safe=''))
        values['sha'] = commit['sha']
    if not re.fullmatch('[0-9a-f]{40}', values['sha']):
        raise ValueError('Invalid resolved commit SHA')
    values['title'] = ' '.join(str(values.get('title', payload.get('title') or 'Manual debug build')).splitlines())
    values['pr_url'] = f'https://github.com/{repo}/pull/{pr}' if pr else ''
    values['tag'] = 'debug-pr-' + pr if pr else 'debug-run-' + os.environ['GITHUB_RUN_NUMBER']
    return values


def parse_secret_file(text):
    """Read secrets-action's KEY='verbatim value' format; never execute it as shell."""
    pattern = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)='(.*?)'(?:\n(?=[A-Za-z_][A-Za-z0-9_]*=')|\Z)", re.S)
    values, position = {}, 0
    for match in pattern.finditer(text):
        if match.start() != position:
            raise ValueError('Malformed Infisical export')
        values[match[1]] = match[2]
        position = match.end()
    if position != len(text):
        raise ValueError('Malformed Infisical export')
    return values


def java_value(value):
    return ''.join({'\\': '\\\\', '\n': '\\n', '\r': '\\r', '\t': '\\t', '\f': '\\f'}.get(c, '\\ ' if c == ' ' and i == 0 else c)
                   for i, c in enumerate(value))


def local_properties(source, destination, sdk):
    values = {'PLAY_LICENSE_KEY': '', 'CHATWOOT_BASE_URL': '', 'CHATWOOT_WEBSITE_TOKEN': ''}
    values.update(parse_secret_file(Path(source).read_text()))
    for name in ('ZOHO_APP_KEY', 'ZOHO_ACCESS_KEY', 'sdk.dir'):
        values.pop(name, None)
    if not sdk:
        raise ValueError('ANDROID_HOME or ANDROID_SDK_ROOT is required')
    values['sdk.dir'] = sdk
    target = Path(destination)
    target.write_text(''.join(f'{key}={java_value(value)}\n' for key, value in values.items()))
    target.chmod(0o600)
    Path(source).unlink()


def android_inputs():
    for name, target in [('KEYSTORE_BASE64', 'androidApp/keystore-debug.jks'),
                         ('SERVICE_JSON_BASE64', 'androidApp/google-services.json')]:
        value = os.environ.get(name, '')
        if not value:
            raise ValueError('Missing build input ' + name)
        data = base64.b64decode(''.join(value.split()), validate=True)
        if not data:
            raise ValueError('Empty build input ' + name)
        Path(target).write_bytes(data)
        Path(target).chmod(0o600)


def lint_state(path):
    result = json.loads(Path(path).read_text())
    # Lint-only jobs omit unit fields: the parallel unit-tests job gates publishing separately.
    unit = result.get('unit_test_exit_code', 0)
    lint = result['lint_exit_code']
    if not isinstance(unit, int) or not isinstance(lint, int):
        raise ValueError('Invalid lint result')
    subject = subprocess.check_output(['git', 'log', '-1', '--pretty=%s'], text=True).strip()
    return {'units_passed': str(unit == 0).lower(), 'lint_failed': str(lint != 0).lower(),
            'autofix_allowed': str(unit == 0 and lint != 0 and not re.match(r'^lint: Lint fix by (Claude|Codex)', subject)).lower()}


def metadata(suffix):
    values = dict(re.findall(r'^(VERSION(?:_CODE)?)=(.*)$', Path('gradle.properties').read_text(), re.M))
    code, version = values['VERSION_CODE'].strip(), values['VERSION'].strip()
    if not re.fullmatch('[1-9][0-9]*', code) or not re.fullmatch('[1-9][0-9]*', suffix):
        raise ValueError('Invalid build number')
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', version):
        raise ValueError('Invalid marketing version')
    kotlin = re.search(r'^kotlin\s*=\s*"([0-9A-Za-z.+-]+)"', Path('gradle/libs.versions.toml').read_text(), re.M)
    angle = re.search(r'val angleRevision = "([0-9a-f]{40})"', Path('shared/build.gradle.kts').read_text())
    if not kotlin or not angle:
        raise ValueError('Missing source-owned Kotlin version or ANGLE revision')
    return {'version': version, 'build_number': code + '.' + suffix,
            'kotlin_version': kotlin[1], 'angle_revision': angle[1]}


def push_fix(branch, sha):
    """Only tracked source changes; never sweep ignored signing inputs into an AI commit."""
    report = Path('claude-report.md')
    if not report.exists() or report.read_text().splitlines()[:1] != ['Status: SUCCESS']:
        output({'pushed': 'false'})
        return
    changes = subprocess.check_output(['git', 'diff', '--name-only', '-z'], text=True).split('\0')
    changes = [p for p in changes if p and p != 'claude-report.md']
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], text=True).split('\0')
    changes.extend(p for p in untracked if p.startswith(('androidApp/src/', 'shared/src/')))
    if not changes:
        output({'pushed': 'false'})
        return
    subprocess.run(['git', 'config', 'user.name', 'octopoocha'], check=True)
    subprocess.run(['git', 'config', 'user.email', 'octopoocha@lascade.com'], check=True)
    subprocess.run(['git', 'add', '--', *changes], check=True)
    subprocess.run(['git', 'commit', '-m', 'lint: Lint fix by Claude\n\nCentral KMP CI lint auto-fix.'], check=True)
    # An atomic lease prevents overwriting a contributor push while Claude worked.
    subprocess.run(['git', 'push', '--force-with-lease=refs/heads/' + branch + ':' + sha,
                    'origin', 'HEAD:refs/heads/' + branch], check=True)
    output({'pushed': 'true'})


ALTOOL_ERROR = re.compile(r'ERROR: \[[^\]]*\] (.+)')


def altool_errors(output, limit=2):
    """Apple's own failure reasons from altool output, shortened, without the generic exit line."""
    messages = []
    for line in output.splitlines():
        match = ALTOOL_ERROR.search(line)
        if match and not match[1].startswith('ExitFailure'):
            text = ' '.join(match[1].split())
            messages.append(text if len(text) <= 300 else text[:299] + '…')
    return list(dict.fromkeys(messages))[:limit]


def workflow_escape(text):
    return text.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')


def upload_ipa(path):
    key_id = os.environ['APPSTORE_API_KEY_ID']
    issuer = os.environ['APPSTORE_ISSUER_ID']
    if not re.fullmatch('[A-Za-z0-9]+', key_id):
        raise ValueError('Invalid App Store key ID')
    folder = Path(os.environ['RUNNER_TEMP']) / 'kmp-appstore-keys'
    folder.mkdir(mode=0o700, exist_ok=True)
    key = folder / f'AuthKey_{key_id}.p8'
    try:
        key.write_text(os.environ['APPSTORE_API_PRIVATE_KEY'])
        key.chmod(0o600)
        env = dict(os.environ, API_PRIVATE_KEYS_DIR=str(folder))
        result = subprocess.run(['xcrun', 'altool', '--upload-package', path,
                                 '--api-key', key_id, '--api-issuer', issuer], env=env,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(result.stdout, end='')
        if result.returncode:
            # Point the run page and Telegram at Apple's reason instead of only the exit code.
            for message in altool_errors(result.stdout):
                print(f'::error title=App Store upload::{workflow_escape(message)}')
            raise subprocess.CalledProcessError(result.returncode, 'altool --upload-package')
    finally:
        key.unlink(missing_ok=True)
        folder.rmdir()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ['validate', 'resolve', 'android-inputs', 'metadata', 'lint-state', 'push-fix', 'upload-ipa', 'local-properties']:
        child = sub.add_parser(command)
        if command == 'local-properties':
            child.add_argument('--input', required=True)
            child.add_argument('--output', required=True)
        elif command == 'metadata': child.add_argument('--suffix', required=True)
        elif command == 'lint-state': child.add_argument('path')
        elif command == 'upload-ipa': child.add_argument('path')
    args = parser.parse_args()
    if args.command in ('validate', 'resolve'):
        payload = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())['client_payload']
        values = (resolve if args.command == 'resolve' else validate_payload)(payload, os.environ['GITHUB_REPOSITORY'])
        output(values)
    elif args.command == 'local-properties':
        local_properties(args.input, args.output, os.environ.get('ANDROID_HOME') or os.environ.get('ANDROID_SDK_ROOT'))
    elif args.command == 'android-inputs': android_inputs()
    elif args.command == 'metadata': output(metadata(args.suffix))
    elif args.command == 'lint-state': output(lint_state(args.path))
    elif args.command == 'push-fix': push_fix(os.environ['HEAD_BRANCH'], os.environ['SOURCE_SHA'])
    elif args.command == 'upload-ipa': upload_ipa(args.path)


if __name__ == '__main__':
    main()
