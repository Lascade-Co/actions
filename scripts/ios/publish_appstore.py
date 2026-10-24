#!/usr/bin/env python3
"""Upload a release IPA and submit its exact version/build to App Review.

Only main() uses credentials/network. Publisher accepts a fake client and clock for offline tests.
API contracts: https://developer.apple.com/documentation/appstoreconnectapi
Uses reviewSubmissions/reviewSubmissionItems (not the deprecated version-submission endpoint).
Apple's phased-release resource always represents its standard seven-day schedule.
Apple carries metadata into a new version automatically; app-level age rating is left intact:
https://developer.apple.com/help/app-store-connect/update-your-app/create-a-new-version/
The prerelease checklist still requires a production metadata check before this is enabled.
"""
import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

API = 'https://api.appstoreconnect.apple.com/v1/'
LOCALIZATION_FIELDS = ('description', 'keywords', 'marketingUrl', 'promotionalText', 'supportUrl')
REVIEW_FIELDS = ('contactEmail', 'contactFirstName', 'contactLastName', 'contactPhone',
                 'demoAccountName', 'demoAccountPassword', 'demoAccountRequired', 'notes')
SUBMITTED_STATES = {'WAITING_FOR_REVIEW', 'IN_REVIEW', 'PENDING_APPLE_RELEASE',
                    'PENDING_DEVELOPER_RELEASE', 'READY_FOR_SALE', 'PROCESSING_FOR_APP_STORE',
                    'READY_FOR_DISTRIBUTION', 'PROCESSING_FOR_DISTRIBUTION', 'ACCEPTED'}


def version_state(version):
    # API 3.3 replaced appStoreState; keep the fallback for old responses/fixtures.
    attrs = version['attributes']
    return attrs.get('appVersionState') or attrs.get('appStoreState')


class ApiError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)


class Client:
    def __init__(self, opener=urllib.request.urlopen):
        self.opener = opener

    def request(self, method, path, params=None, data=None):
        # Refresh on every request: processing can take longer than a JWT's lifetime.
        import jwt
        missing = [key for key in ('APPSTORE_API_KEY_ID', 'APPSTORE_ISSUER_ID', 'APPSTORE_API_PRIVATE_KEY')
                   if not os.environ.get(key)]
        if missing:
            raise ValueError('Missing GitHub secrets: ' + ', '.join(missing))
        now = int(time.time())
        token = jwt.encode({'iss': os.environ['APPSTORE_ISSUER_ID'], 'iat': now,
                            'exp': now + 600, 'aud': 'appstoreconnect-v1'},
                           os.environ['APPSTORE_API_PRIVATE_KEY'], algorithm='ES256',
                           headers={'kid': os.environ['APPSTORE_API_KEY_ID']})
        if '://' in path and not path.startswith(API):
            raise ValueError('App Store pagination must stay on the App Store API')
        url = path if path.startswith(API) else API + path
        if params:
            url += '?' + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, method=method, headers={
            'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
            data=json.dumps(data).encode() if data is not None else None)
        try:
            with self.opener(request, timeout=60) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as error:
            # Apple's error code/detail identifies missing metadata and submission blockers.
            try:
                errors = json.loads(error.read()).get('errors', [])
                message = '; '.join(f'{e.get("code", "API_ERROR")}: {e.get("detail") or e.get("title", "")}'
                                    for e in errors)
            except (ValueError, TypeError):
                message = 'Unreadable App Store Connect error'
            finally:
                error.close()
            raise ApiError(error.code, f'App Store Connect HTTP {error.code}: {message}') from None

    def list(self, path, params=None):
        result = []
        while path:
            page = self.request('GET', path, params=params)
            result.extend(page.get('data', []))
            path, params = page.get('links', {}).get('next'), None
        return result

    def related(self, path):
        try:
            return self.request('GET', path).get('data')
        except ApiError as error:
            if error.status == 404:
                return None
            raise


def resource(kind, attributes=None, relationships=None, identifier=None):
    item = {'type': kind}
    if attributes is not None:
        item['attributes'] = attributes
    if relationships:
        item['relationships'] = {key: {'data': {'type': value[0], 'id': value[1]}}
                                 for key, value in relationships.items()}
    if identifier:
        item['id'] = identifier
    return {'data': item}


def ipa_identity(path):
    with zipfile.ZipFile(path) as archive:
        names = [n for n in archive.namelist() if re.fullmatch(r'Payload/[^/]+\.app/Info\.plist', n)]
        if len(names) != 1:
            raise ValueError('IPA must contain exactly one main app Info.plist')
        info = plistlib.loads(archive.read(names[0]))
    return tuple(str(info[k]) for k in ('CFBundleIdentifier', 'CFBundleShortVersionString', 'CFBundleVersion'))


class Publisher:
    def __init__(self, client, sleep=time.sleep, monotonic=time.monotonic):
        self.client, self.sleep, self.monotonic = client, sleep, monotonic

    def find_build(self, app, version, number):
        builds = self.client.list('builds', {'filter[app]': app, 'filter[version]': number,
            'filter[preReleaseVersion.version]': version, 'filter[preReleaseVersion.platform]': 'IOS',
            'limit': 200})
        if len(builds) > 1:
            raise ValueError('More than one App Store build matched the exact version/build')
        return builds[0] if builds else None

    def wait_build(self, app, version, number, timeout):
        deadline = self.monotonic() + timeout
        while True:
            build = self.find_build(app, version, number)
            state = build['attributes'].get('processingState') if build else None
            if state == 'VALID':
                if build['attributes'].get('expired'):
                    raise ValueError('The exact App Store build is expired')
                return build
            if state in ('FAILED', 'INVALID'):
                raise ValueError(f'Apple rejected processing for {version} ({number}): {state}')
            if self.monotonic() >= deadline:
                raise TimeoutError(f'Timed out waiting for Apple to process {version} ({number})')
            print(f'Waiting for {version} ({number}): {state or "not visible yet"}', flush=True)
            self.sleep(min(30, max(0, deadline - self.monotonic())))

    def carry_metadata(self, version, previous, notes):
        """Preserve Apple's cloned metadata; fill absent fields/locales from previous version."""
        client = self.client
        current = client.list(f'appStoreVersions/{version}/appStoreVersionLocalizations', {'limit': 200})
        old = client.list(f'appStoreVersions/{previous}/appStoreVersionLocalizations', {'limit': 200}) if previous else []
        by_locale = {loc['attributes']['locale']: loc for loc in current}
        for loc in old:
            attrs = loc['attributes']
            if attrs['locale'] not in by_locale:
                copied = {key: attrs[key] for key in (*LOCALIZATION_FIELDS, 'locale') if attrs.get(key) is not None}
                copied['whatsNew'] = notes
                created = client.request('POST', 'appStoreVersionLocalizations', data=resource(
                    'appStoreVersionLocalizations', copied, {'appStoreVersion': ('appStoreVersions', version)}))['data']
                by_locale[attrs['locale']] = created
        if not by_locale:
            raise ValueError('No version localizations; complete the App Store metadata first')
        old_by_locale = {loc['attributes']['locale']: loc['attributes'] for loc in old}
        for locale, loc in by_locale.items():
            # Never overwrite existing version metadata, except the requested What's New.
            attrs = {'whatsNew': notes}
            attrs.update({key: old_by_locale[locale][key] for key in LOCALIZATION_FIELDS
                          if locale in old_by_locale and not loc['attributes'].get(key)
                          and old_by_locale[locale].get(key) is not None})
            client.request('PATCH', f'appStoreVersionLocalizations/{loc["id"]}',
                           data=resource('appStoreVersionLocalizations', attrs, identifier=loc['id']))
        review = client.related(f'appStoreVersions/{version}/appStoreReviewDetail')
        if previous:
            old_review = client.related(f'appStoreVersions/{previous}/appStoreReviewDetail')
            if old_review:
                attrs = {key: old_review['attributes'][key] for key in REVIEW_FIELDS
                         if old_review['attributes'].get(key) is not None
                         and (not review or review['attributes'].get(key) in (None, ''))}
                if review and attrs:
                    client.request('PATCH', f'appStoreReviewDetails/{review["id"]}', data=resource(
                        'appStoreReviewDetails', attrs, identifier=review['id']))
                elif not review:
                    client.request('POST', 'appStoreReviewDetails', data=resource('appStoreReviewDetails', attrs,
                        {'appStoreVersion': ('appStoreVersions', version)}))

    def publish(self, app, version, build, notes):
        client = self.client
        versions = client.list(f'apps/{app}/appStoreVersions', {'filter[platform]': 'IOS', 'limit': 200})
        matches = [v for v in versions if v['attributes']['versionString'] == version]
        if len(matches) > 1:
            raise ValueError('Multiple App Store versions matched')
        previous = next((v for v in sorted(versions, key=lambda v: v['attributes'].get('createdDate', ''), reverse=True)
                         if v['attributes']['versionString'] != version and
                         version_state(v) in ('READY_FOR_SALE', 'REPLACED_WITH_NEW_VERSION',
                                                                  'READY_FOR_DISTRIBUTION')), None)
        if matches:
            target = matches[0]
            if version_state(target) in SUBMITTED_STATES:
                linked = client.related(f'appStoreVersions/{target["id"]}/build')
                if not linked or linked['id'] != build['id']:
                    raise ValueError('Submitted version is attached to a different build')
                return target['id']
        else:
            attrs = {'platform': 'IOS', 'versionString': version, 'releaseType': 'AFTER_APPROVAL'}
            if previous and previous['attributes'].get('copyright'):
                attrs['copyright'] = previous['attributes']['copyright']
            target = client.request('POST', 'appStoreVersions', data=resource('appStoreVersions', attrs,
                {'app': ('apps', app)}))['data']
        identifier = target['id']
        client.request('PATCH', f'appStoreVersions/{identifier}', data=resource('appStoreVersions',
                       {'releaseType': 'AFTER_APPROVAL'}, identifier=identifier))
        self.carry_metadata(identifier, previous['id'] if previous else None, notes)
        client.request('PATCH', f'appStoreVersions/{identifier}/relationships/build',
                       data={'data': {'type': 'builds', 'id': build['id']}})
        phased = client.related(f'appStoreVersions/{identifier}/appStoreVersionPhasedRelease')
        if not phased:
            client.request('POST', 'appStoreVersionPhasedReleases', data=resource('appStoreVersionPhasedReleases',
                {'phasedReleaseState': 'INACTIVE'}, {'appStoreVersion': ('appStoreVersions', identifier)}))
        elif phased['attributes']['phasedReleaseState'] != 'INACTIVE':
            raise ValueError('Existing phased release is already active/completed; inspect it before resubmitting')
        # Resume an existing READY_FOR_REVIEW submission after an interrupted run.
        submissions = client.list(f'apps/{app}/reviewSubmissions', {'limit': 200})
        submission = None
        attached = False
        for candidate in submissions:
            items = client.list(f'reviewSubmissions/{candidate["id"]}/items', {'limit': 200, 'include': 'appStoreVersion'})
            if any((i.get('relationships', {}).get('appStoreVersion', {}).get('data') or {}).get('id') == identifier for i in items):
                submission, attached = candidate, True
                break
        if submission and submission['attributes'].get('state') != 'READY_FOR_REVIEW':
            raise ValueError('Existing review submission is not editable; inspect App Store Connect')
        if not submission:
            submission = client.request('POST', 'reviewSubmissions', data=resource('reviewSubmissions',
                {'platform': 'IOS'}, {'app': ('apps', app)}))['data']
        if not attached:
            client.request('POST', 'reviewSubmissionItems', data=resource('reviewSubmissionItems', relationships={
                'reviewSubmission': ('reviewSubmissions', submission['id']),
                'appStoreVersion': ('appStoreVersions', identifier)}))
        client.request('PATCH', f'reviewSubmissions/{submission["id"]}', data=resource('reviewSubmissions',
            {'submitted': True}, identifier=submission['id']))
        return identifier


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ipa', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--build-number')
    parser.add_argument('--bundle-id', default='com.travelanimator.routemap')
    parser.add_argument('--notes-file', required=True)
    parser.add_argument('--already-uploaded', action='store_true')
    parser.add_argument('--processing-timeout', type=int, default=5400)
    args = parser.parse_args(argv)
    bundle, version, number = ipa_identity(args.ipa)
    if (bundle, version) != (args.bundle_id, args.version) or (args.build_number and args.build_number != number):
        raise ValueError('IPA identity does not match the requested release')
    notes = Path(args.notes_file).read_text().strip()
    if not notes or len(notes.encode('utf-16-le')) // 2 > 500:
        raise ValueError('Shared release notes must contain 1–500 UTF-16 code units; never truncate')
    client = Client()
    apps = client.list('apps', {'filter[bundleId]': bundle})
    if len(apps) != 1:
        raise ValueError('Expected exactly one App Store Connect app matching the bundle ID')
    app = apps[0]['id']
    publisher = Publisher(client)
    if not args.already_uploaded and not publisher.find_build(app, version, number):
        subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] / 'kmp/control.py'),
                        'upload-ipa', args.ipa], check=True)
    build = publisher.wait_build(app, version, number, args.processing_timeout)
    identifier = publisher.publish(app, version, build, notes)
    print(f'Submitted App Store version {version} ({number}); version ID {identifier}; seven-day phased release after approval')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ApiError, ValueError, OSError, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        message = str(exc).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
        print(f'::error title=App Store submission::{message}', file=sys.stderr)
        raise SystemExit(1)
