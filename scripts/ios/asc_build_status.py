#!/usr/bin/env python3
"""Read-only App Store Connect report: recent builds of one app with processing and TestFlight state.

Usage: asc_build_status.py --bundle-id BUNDLE [--limit N]
Env: APPSTORE_API_KEY_ID, APPSTORE_ISSUER_ID, APPSTORE_API_PRIVATE_KEY (never printed).
Only GET requests are made.
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import jwt  # PyJWT with cryptography (ES256)

API = 'https://api.appstoreconnect.apple.com/v1'


def token():
    now = int(time.time())
    return jwt.encode({'iss': os.environ['APPSTORE_ISSUER_ID'], 'iat': now, 'exp': now + 600,
                       'aud': 'appstoreconnect-v1'}, os.environ['APPSTORE_API_PRIVATE_KEY'],
                      algorithm='ES256', headers={'kid': os.environ['APPSTORE_API_KEY_ID']})


def get(path, params, bearer):
    url = f'{API}/{path}?{urllib.parse.urlencode(params)}'
    request = urllib.request.Request(url, headers={'Authorization': f'Bearer {bearer}'})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--bundle-id', required=True)
    cli.add_argument('--limit', type=int, default=8)
    args = cli.parse_args()
    bearer = token()
    apps = get('apps', {'filter[bundleId]': args.bundle_id}, bearer)['data']
    if not apps:
        print(f'No App Store Connect app has bundle ID {args.bundle_id}')
        return 1
    app = apps[0]
    print(f"App: {app['attributes']['name']} ({app['attributes']['bundleId']}, id {app['id']})")
    groups = get(f"apps/{app['id']}/betaGroups", {'limit': 50}, bearer)['data']
    for group in groups:
        attrs = group['attributes']
        print(f"Beta group: {attrs['name']} internal={attrs.get('isInternalGroup')} "
              f"allBuilds={attrs.get('hasAccessToAllBuilds')}")
    builds = get('builds', {'filter[app]': app['id'], 'sort': '-uploadedDate', 'limit': args.limit,
                            'include': 'buildBetaDetail,preReleaseVersion,betaGroups'}, bearer)
    included = {(item['type'], item['id']): item for item in builds.get('included', [])}
    for build in builds['data']:
        attrs, rel = build['attributes'], build['relationships']
        detail = included.get(('buildBetaDetails', (rel.get('buildBetaDetail', {}).get('data') or {}).get('id')), {})
        version = included.get(('preReleaseVersions', (rel.get('preReleaseVersion', {}).get('data') or {}).get('id')), {})
        group_names = [included.get(('betaGroups', g['id']), {}).get('attributes', {}).get('name', g['id'])
                       for g in (rel.get('betaGroups', {}).get('data') or [])]
        d = detail.get('attributes', {})
        print(f"{version.get('attributes', {}).get('version', '?')} ({attrs['version']}) uploaded {attrs['uploadedDate']} "
              f"processing={attrs['processingState']} expired={attrs.get('expired')} "
              f"encryptionAnswered={attrs.get('usesNonExemptEncryption') is not None} "
              f"internalState={d.get('internalBuildState')} externalState={d.get('externalBuildState')} "
              f"groups={group_names}")
    if not builds['data']:
        print('No processed builds; checking build uploads for processing errors')
    try:
        uploads = get(f"apps/{app['id']}/buildUploads", {'limit': args.limit, 'sort': '-uploadedDate'}, bearer)
        for upload in uploads.get('data', []):
            a = upload['attributes']
            state = a.get('state') or {}
            print(f"Upload {a.get('cfBundleShortVersionString')} ({a.get('cfBundleVersion')}) uploaded {a.get('uploadedDate')} "
                  f"state={state.get('state')}")
            for issue in (state.get('errors') or []) + (state.get('warnings') or []):
                print(f"  {issue.get('code')}: {issue.get('description') or issue.get('message')}")
    except urllib.error.HTTPError as error:
        print(f'buildUploads unavailable (HTTP {error.code}): {error.read().decode(errors="replace")[:300]}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
