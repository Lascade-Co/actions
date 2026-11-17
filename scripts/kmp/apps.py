#!/usr/bin/env python3
"""App registry for the store publishers (config/apps.json, keyed by source repository).

`load_app(repo)` is the one place that decides which source repos may publish and what their store
identifiers and Play rollout are. The bundle ID and package name come from the registry, never from a
dispatch payload. As a CLI it validates `--repo` (and `--package-name`, when given) and writes the entry as
workflow outputs.

`--allow-unregistered` exists only for the legacy `publish-playstore` event, which other apps still send
without registering: it keeps the original Play rollout (BD, 0.99) for them instead of failing.
"""
import argparse
import json
from pathlib import Path

REGISTRY = Path(__file__).resolve().parents[2] / 'config' / 'apps.json'
LEGACY_ROLLOUT = {'country': 'BD', 'fraction': 0.99}


def load_app(repo):
    apps = json.loads(REGISTRY.read_text())
    if repo not in apps:
        raise SystemExit(f'{repo} is not registered in config/apps.json')
    return apps[repo]


def outputs(app):
    return {'bundle_id': app['bundle_id'], 'package_name': app['package_name'],
            'metadata_repo': app['metadata_repo'] or '',
            'rollout_country': app['rollout']['country'], 'rollout_fraction': app['rollout']['fraction'],
            # Play native-debug-symbols.zip; only apps that build none (Kotlin Toolchain) set false.
            'native_symbols': 'false' if app.get('native_symbols') is False else 'true'}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--package-name', help='payload package name; must equal the registry value')
    parser.add_argument('--output', required=True, help='$GITHUB_OUTPUT')
    parser.add_argument('--allow-unregistered', action='store_true')
    args = parser.parse_args()
    try:
        app = load_app(args.repo)
    except SystemExit:
        if not args.allow_unregistered:
            raise
        values = {'rollout_country': LEGACY_ROLLOUT['country'], 'rollout_fraction': LEGACY_ROLLOUT['fraction'],
                  'native_symbols': 'true'}
    else:
        if args.package_name is not None and args.package_name != app['package_name']:
            raise SystemExit(f'package_name {args.package_name} does not match the registry for {args.repo}')
        values = outputs(app)
    with Path(args.output).open('a') as stream:
        for key, value in values.items():
            stream.write(f'{key}={value}\n')


if __name__ == '__main__':
    main()
