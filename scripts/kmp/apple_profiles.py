#!/usr/bin/env python3
"""Install named environment profiles and remove them after the build."""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess
import tempfile


def profile_metadata(data):
    profile = plistlib.loads(data)
    uuid = profile.get('UUID', '')
    if not re.fullmatch(r'[A-Za-z0-9-]+', uuid):
        raise ValueError('Invalid profile UUID')
    if profile.get('ExpirationDate', datetime.datetime.min) <= datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None):
        raise ValueError('Provisioning profile has expired')
    if profile.get('ProvisionedDevices') or profile.get('ProvisionsAllDevices'):
        raise ValueError('App Store distribution profile required')
    identifier = profile.get('Entitlements', {}).get('application-identifier', '')
    if not identifier or '*' in identifier or profile.get('Entitlements', {}).get('get-task-allow', False):
        raise ValueError('An explicit App Store distribution profile is required')
    return uuid, profile.get('Name', '')


def state_path():
    return Path(os.environ['RUNNER_TEMP']) / 'central-apple-profiles'


def cleanup():
    state = state_path()
    manifest = state / 'installed.json'
    if manifest.exists():
        for entry in json.loads(manifest.read_text()):
            destination = Path(entry['destination'])
            destination.unlink(missing_ok=True)
            if entry['backup']:
                shutil.copy2(state / entry['backup'], destination)
        shutil.rmtree(state)


def install():
    variables = os.environ['PROFILE_VARIABLES'].split()
    if not variables or any(not re.fullmatch(r'[A-Z][A-Z0-9_]*', var) for var in variables):
        raise ValueError('Profiles must list environment variable names')
    state = state_path()
    state.mkdir(mode=0o700)
    manifest = state / 'installed.json'
    installed = []
    manifest.write_text('[]')
    destination_dir = Path.home() / 'Library/Developer/Xcode/UserData/Provisioning Profiles'
    destination_dir.mkdir(parents=True, exist_ok=True)
    try:
        uuids = set()
        for variable in variables:
            encoded = os.environ.get(variable, '')
            if not encoded:
                raise ValueError(f'Required profile variable {variable} is empty')
            data = base64.b64decode(''.join(encoded.split()), validate=True)
            source = state / 'profile.mobileprovision'
            source.write_bytes(data)
            source.chmod(0o600)
            decoded = subprocess.run(['security', 'cms', '-D', '-i', str(source)],
                                     check=True, capture_output=True).stdout
            uuid, name = profile_metadata(decoded)
            if uuid in uuids:
                raise ValueError('Each selected profile must have a distinct UUID')
            uuids.add(uuid)
            destination = destination_dir / f'{uuid}.mobileprovision'
            backup = f'backup-{len(installed)}' if destination.exists() else ''
            if backup:
                shutil.copy2(destination, state / backup)
            installed.append({'destination': str(destination), 'backup': backup})
            manifest.write_text(json.dumps(installed))
            shutil.copy2(source, destination)
            source.unlink()
            print(f'Installed profile {name} ({uuid})')
    except BaseException:
        cleanup()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['install', 'cleanup'])
    args = parser.parse_args()
    if args.mode == 'install':
        install()
    else:
        cleanup()


if __name__ == '__main__':
    main()
