#!/usr/bin/env python3
"""Validate a publish-kmp dispatch/release and write safe workflow outputs and shared notes."""
import argparse
import json
from pathlib import Path
import re


def validate(payload, release, central_repository):
    repo = payload.get('repo', '')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo) or repo.split('/')[0].lower() != central_repository.split('/')[0].lower():
        raise ValueError('Release repository must belong to the central organization')
    if not str(payload.get('release_id', '')).isdigit():
        raise ValueError('A numeric release_id is required')
    if repo != 'Lascade-Co/travel-animator-shared' or payload.get('package_name') != 'com.travelanimator.routemap':
        raise ValueError('Only the approved KMP source repository/package may publish')
    if str(release.get('id')) != str(payload['release_id']) or release.get('draft') or release.get('prerelease'):
        raise ValueError('Expected the requested published production release')
    match = re.fullmatch(r'v(\d+\.\d+\.\d+)', release.get('tag_name', ''))
    if not match:
        raise ValueError('Release tag must be vX.Y.Z')
    notes = (release.get('body') or '').strip()
    if not notes or len(notes.encode('utf-16-le')) // 2 > 500:
        raise ValueError('Release notes must contain 1–500 UTF-16 code units; shorten without truncating')
    return {'owner': repo.split('/')[0], 'name': repo.split('/')[1], 'version': match[1],
            'tag': release['tag_name'], 'gh_url': f'https://github.com/{repo}/releases/tag/{release["tag_name"]}'}, notes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event', required=True)
    parser.add_argument('--release', required=True)
    parser.add_argument('--central-repository', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--notes', required=True)
    args = parser.parse_args()
    payload = json.loads(Path(args.event).read_text())['client_payload']
    values, notes = validate(payload, json.loads(Path(args.release).read_text()), args.central_repository)
    Path(args.notes).write_text(notes + '\n')
    with Path(args.output).open('a') as stream:
        for key, value in values.items():
            stream.write(f'{key}={value}\n')


if __name__ == '__main__':
    main()
