#!/usr/bin/env python3
"""Helpers for the central native iOS TestFlight workflow (ios-build-debug.yml).

metadata     (cwd: source checkout) Detect the Xcode project and scheme, read the project's
             marketing version, compute the build number and normalise the dispatch flags.
profiles     List the provisioning-profile variables present in the environment, space separated,
             for the apple-signing composite.
signing      Read those profiles, export <PREFIX>_PROFILE_NAME/UUID/BUNDLE_ID/TEAM_ID to GITHUB_ENV
             (consumed by scripts/ios/fix_ios_signing.py) and write ExportOptions.plist.

Native apps keep their build in the Xcode project, so this runner (not the app) owns signing.
No secret value is ever printed.
"""
import argparse
import base64
import os
from pathlib import Path
import plistlib
import re
import subprocess

# Variable name in Infisical /Build -> prefix exported for fix_ios_signing.py.
PROFILES = [
    ('IOS_PROVISION_PROFILE_BASE64', 'APP'),
    ('IOS_NSE_PROVISION_PROFILE_BASE64', 'NSE'),
    ('IOS_WIDGET_PROVISION_PROFILE_BASE64', 'WIDGET'),
    ('IOS_WATCH_PROVISION_PROFILE_BASE64', 'WATCH'),
    ('IOS_SHARE_EXT_PROVISION_PROFILE_BASE64', 'SHARE'),
]
VERSION = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+')


def write(path_env, values):
    with open(os.environ[path_env], 'a') as stream:
        for key, value in values.items():
            value = str(value)
            if '\n' in value or '\r' in value:
                raise ValueError('Unexpected multiline value: ' + key)
            stream.write(f'{key}={value}\n')


def parse_version(text):
    return tuple(int(part) for part in text.split('.'))


def project_version(pbxproj_text):
    """Highest X.Y.Z MARKETING_VERSION in the project; test targets often stay at 1.0."""
    found = re.findall(r'MARKETING_VERSION = "?([0-9][0-9.]*)"?;', pbxproj_text)
    full = [v for v in found if VERSION.fullmatch(v)]
    if not full:
        raise ValueError('No X.Y.Z MARKETING_VERSION in the Xcode project')
    return max(full, key=parse_version)


def build_number(run_number, pr=''):
    if not re.fullmatch('[1-9][0-9]*', run_number):
        raise ValueError('Invalid run number')
    # ADR 0008: 1000 + run_number clears every train floor; the PR rides along as a second component.
    number = str(1000 + int(run_number))
    return number + '.' + pr if pr else number


def flags(fetch, version, debug, force=''):
    manual = version.strip() if version and version.strip() != 'auto' else ''
    if manual and not VERSION.fullmatch(manual):
        raise ValueError("version must be 'auto' or X.Y.Z")
    force_update = 'true' if str(force).strip().lower() == 'true' else 'false'
    if manual and force_update == 'true':
        raise ValueError("force_update picks the version itself; send version 'auto' with it")
    return {
        'force_update': force_update,
        'fetch_secrets': 'true' if fetch == 'true' else 'false',
        'manual_version': manual,
        # Default on (ADR 0009). Send "false" for a build that may later be submitted for review.
        'debug_flag': 'false' if debug == 'false' else 'true',
    }


def metadata(env):
    projects = sorted(p for p in Path('.').glob('*.xcodeproj') if p.is_dir())
    if not projects:
        raise ValueError('No .xcodeproj at the repository root')
    project = projects[0]
    scheme = project.stem
    values = {
        'project': project.name,
        'scheme': scheme,
        'project_version': project_version((project / 'project.pbxproj').read_text()),
        'build_number': build_number(env['RUN_NUMBER'], env.get('PR', '')),
        'xcode_version': Path('.xcode-version').read_text().strip() if Path('.xcode-version').is_file() else 'latest-stable',
    }
    values.update(flags(env.get('FETCH_SECRETS', ''), env.get('VERSION_INPUT', ''), env.get('DEBUG_FLAG', ''),
                        env.get('FORCE_UPDATE', '')))
    return values


def present_profiles(env):
    names = [name for name, _ in PROFILES if env.get(name, '').strip()]
    if 'IOS_PROVISION_PROFILE_BASE64' not in names:
        raise ValueError('IOS_PROVISION_PROFILE_BASE64 is required in Infisical /Build')
    return ' '.join(names)


def decode_profile(encoded, scratch):
    scratch.write_bytes(base64.b64decode(''.join(encoded.split()), validate=True))
    scratch.chmod(0o600)
    try:
        data = subprocess.run(['security', 'cms', '-D', '-i', str(scratch)], check=True, capture_output=True).stdout
    finally:
        scratch.unlink(missing_ok=True)
    return profile_identity(plistlib.loads(data))


def profile_identity(profile):
    team = (profile.get('TeamIdentifier') or [''])[0]
    app_id = profile.get('Entitlements', {}).get('application-identifier', '')
    bundle = app_id[len(team) + 1:] if team and app_id.startswith(team + '.') else ''
    return {'name': profile.get('Name', ''), 'uuid': profile.get('UUID', ''), 'team': team,
            'bundle': '' if '*' in bundle else bundle}


def export_options(team, mapping):
    return {'method': 'app-store-connect', 'teamID': team, 'signingStyle': 'manual',
            'uploadSymbols': True, 'provisioningProfiles': dict(mapping)}


def signing(env, temp):
    exported, mapping, app = {}, {}, None
    for name, prefix in PROFILES:
        if not env.get(name, '').strip():
            continue
        info = decode_profile(env[name], Path(temp) / 'profile.mobileprovision')
        exported[f'{prefix}_PROFILE_NAME'] = info['name']
        exported[f'{prefix}_PROFILE_UUID'] = info['uuid']
        if info['team']:
            exported[f'{prefix}_PROFILE_TEAM_ID'] = info['team']
        if info['bundle']:
            exported[f'{prefix}_PROFILE_BUNDLE_ID'] = info['bundle']
        if prefix == 'APP':
            app = info
        elif info['bundle']:
            mapping[info['bundle']] = info['uuid']
    team = env.get('IOS_TEAM_ID') or app['team']
    bundle = env.get('IOS_BUNDLE_ID') or app['bundle']
    if not team or not bundle:
        raise ValueError('No team/bundle in Infisical and none derivable from the app profile')
    mapping = {bundle: app['uuid'], **mapping}
    Path('ExportOptions.plist').write_bytes(plistlib.dumps(export_options(team, mapping)))
    write('GITHUB_ENV', exported)
    print(f'Team {team}, bundle {bundle}; profiles: ' + ', '.join(sorted(mapping)))
    return {'team': team, 'bundle': bundle}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['metadata', 'profiles', 'signing'])
    args = parser.parse_args()
    if args.command == 'metadata':
        write('GITHUB_OUTPUT', metadata(os.environ))
    elif args.command == 'profiles':
        write('GITHUB_OUTPUT', {'list': present_profiles(os.environ)})
    else:
        write('GITHUB_OUTPUT', signing(os.environ, os.environ['RUNNER_TEMP']))


if __name__ == '__main__':
    main()
