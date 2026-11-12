#!/usr/bin/env python3
"""Upload a release IPA and submit its exact version/build to App Review (or, with --no-submit, prepare it).

Only main() uses credentials/network. Publisher accepts a fake client and clock for offline tests.
API contracts: https://developer.apple.com/documentation/appstoreconnectapi
Uses reviewSubmissions/reviewSubmissionItems (not the deprecated version-submission endpoint).
Apple's phased-release resource always represents its standard seven-day schedule.
Apple carries metadata into a new version automatically; app-level age rating is left intact:
https://developer.apple.com/help/app-store-connect/update-your-app/create-a-new-version/
The prerelease checklist still requires a production metadata check before this is enabled.

Release type: a version this script creates requests AFTER_APPROVAL. An existing editable version keeps
whatever release type it already has (for example MANUAL set in App Store Connect); it is never patched.
--no-submit does everything up to and including the phased-release setup, then stops before any
reviewSubmission/reviewSubmissionItem is created or changed, so privacy labels and review sign-in can be
entered in App Store Connect and the version submitted there.
What's New: the shared notes are validated as before (1-500 UTF-16 units), then only the App Store copy has
the characters App Store Connect rejects (emoji and other pictographs, with their variation selectors, ZWJ
and one following space) removed; nothing is truncated. Google Play receives the shared notes unchanged.
"""
import argparse
from pathlib import Path
import subprocess
import sys
import time
import zipfile

from asc_client import (API, ApiError, Client, find_app, find_build, ipa_identity, resource,  # noqa: F401 (re-exported)
                        strip_rejected)

LOCALIZATION_FIELDS = ('description', 'keywords', 'marketingUrl', 'promotionalText', 'supportUrl')
REVIEW_FIELDS = ('contactEmail', 'contactFirstName', 'contactLastName', 'contactPhone',
                 'demoAccountName', 'demoAccountPassword', 'demoAccountRequired', 'notes')
SUBMITTED_STATES = {'WAITING_FOR_REVIEW', 'IN_REVIEW', 'PENDING_APPLE_RELEASE',
                    'PENDING_DEVELOPER_RELEASE', 'READY_FOR_SALE', 'PROCESSING_FOR_APP_STORE',
                    'READY_FOR_DISTRIBUTION', 'PROCESSING_FOR_DISTRIBUTION', 'ACCEPTED'}
# App Store Connect's limit for "What's New in This Version" (characters).
WHATS_NEW_LIMIT = 4000
REVIEW_CAPACITY_ERROR = 'STATE_ERROR.MAX_IN_REVIEW_SUBMISSIONS_PER_PLATFORM_LIMIT_REACHED'
IN_FLIGHT_REVIEW_STATES = {'WAITING_FOR_REVIEW', 'IN_REVIEW', 'UNRESOLVED_ISSUES'}
REVIEW_ITEM_RELATIONSHIPS = ('appStoreVersion', 'appEvent', 'appCustomProductPageVersion', 'appStoreVersionExperiment')
EDITABLE_VERSION_STATES = {'PREPARE_FOR_SUBMISSION', 'DEVELOPER_REJECTED', 'REJECTED', 'METADATA_REJECTED', 'INVALID_BINARY'}
REPLACEABLE_VERSION_STATES = EDITABLE_VERSION_STATES | {'WAITING_FOR_REVIEW', 'IN_REVIEW'}


def appstore_whats_new(notes):
    """The App Store copy of the shared notes without the characters Apple rejects; never truncated."""
    lines = [line.rstrip() for line in strip_rejected(notes).split('\n')]
    text = '\n'.join(lines).strip()
    if not text or len(text) > WHATS_NEW_LIMIT:
        raise ValueError(f"App Store What's New must contain 1-{WHATS_NEW_LIMIT} characters after removing "
                         'emoji/pictographs; edit the release notes instead of truncating')
    return text


def version_state(version):
    # API 3.3 replaced appStoreState; keep the fallback for old responses/fixtures.
    attrs = version['attributes']
    return attrs.get('appVersionState') or attrs.get('appStoreState')


def matching_version(versions, version):
    matches = [v for v in versions if v['attributes']['versionString'] == version]
    if len(matches) > 1:
        raise ValueError('Multiple App Store versions matched')
    return matches[0] if matches else None


def version_number(version):
    parts = version.split('.')
    if not 1 <= len(parts) <= 3 or not all(part.isascii() and part.isdigit() for part in parts):
        raise ValueError(f'Cannot compare App Store version {version!r}')
    return tuple(map(int, parts)) + (0,) * (3 - len(parts))


def supplemental_review_items(items):
    carry = []
    for item in items:
        linked = [(key, value.get('data')) for key, value in item.get('relationships', {}).items()
                  if key in REVIEW_ITEM_RELATIONSHIPS and value.get('data')]
        if len(linked) != 1:
            raise ValueError(f'Cannot identify review item {item["id"]}; inspect it before replacing its submission')
        if linked[0][0] != 'appStoreVersion' and item.get('attributes', {}).get('state') not in ('ACCEPTED', 'APPROVED'):
            carry.append(linked[0])
    return carry


class Publisher:
    def __init__(self, client, sleep=time.sleep, monotonic=time.monotonic):
        self.client, self.sleep, self.monotonic = client, sleep, monotonic
        # After publish(): 'submitted', 'prepared' (--no-submit) or the state of an already submitted version.
        self.outcome = None

    def find_build(self, app, version, number):
        return find_build(self.client, app, version, number)

    def review_items(self, identifier):
        return self.client.list(f'reviewSubmissions/{identifier}/items',
                                {'limit': 200, 'include': ','.join(REVIEW_ITEM_RELATIONSHIPS)})

    def carry_review_items(self, submission, carry):
        if not carry:
            return
        current = self.review_items(submission['id'])
        existing = {(key, value['data']['id']) for item in current
                    for key, value in item.get('relationships', {}).items() if value.get('data')}
        for key, linked in carry:
            if (key, linked['id']) not in existing:
                self.client.request('POST', 'reviewSubmissionItems', data=resource('reviewSubmissionItems',
                    relationships={'reviewSubmission': ('reviewSubmissions', submission['id']),
                                   key: (linked['type'], linked['id'])}))
                existing.add((key, linked['id']))
                print(f'Carried {key} {linked["id"]} into review {submission["id"]}', flush=True)

    def cancel_review(self, submission, timeout=300):
        path = f'reviewSubmissions/{submission["id"]}'
        if submission['attributes']['state'] != 'CANCELING':
            self.client.request('PATCH', path, data=resource('reviewSubmissions', {'canceled': True},
                                                           identifier=submission['id']))
        deadline = self.monotonic() + timeout
        while True:
            current = self.client.request('GET', path)['data']
            if current['attributes'].get('state') == 'COMPLETE':
                return
            if self.monotonic() >= deadline:
                raise TimeoutError(f'Timed out waiting for cancellation of iOS review {submission["id"]}; retry to resume')
            self.sleep(min(5, max(0, deadline - self.monotonic())))

    def replace_blocking_version(self, app, versions, version, timeout=300):
        """Withdraw the older version's own review, then reuse its metadata/screenshots for the new release."""
        candidates = [v for v in versions if version_state(v) in REPLACEABLE_VERSION_STATES]
        if len(candidates) != 1 or any(version_number(v['attributes']['versionString']) >= version_number(version)
                                       for v in versions):
            return None
        target = candidates[0]
        identifier, old_version = target['id'], target['attributes']['versionString']
        submissions = self.client.list(f'apps/{app}/reviewSubmissions', {'limit': 200})
        matching = []
        for submission in submissions:
            if (submission['attributes'].get('platform') != 'IOS' or
                    submission['attributes'].get('state') not in IN_FLIGHT_REVIEW_STATES | {'CANCELING'}):
                continue
            items = self.review_items(submission['id'])
            if any((i.get('relationships', {}).get('appStoreVersion', {}).get('data') or {}).get('id') == identifier
                   for i in items):
                supplemental_review_items(items)  # Validate every item before withdrawing its submission.
                matching.append(submission)
        if len(matching) > 1 or (version_state(target) not in EDITABLE_VERSION_STATES and not matching):
            return None
        for submission in matching:
            print(f'Replacing App Store version {old_version} with {version}; withdrawing its iOS review '
                  f'{submission["id"]}', flush=True)
            self.cancel_review(submission, timeout)
        deadline = self.monotonic() + timeout
        while True:
            target = self.client.request('GET', f'appStoreVersions/{identifier}')['data']
            if target['attributes']['versionString'] != old_version:
                raise ValueError('The blocking App Store version changed during replacement; retry to recheck it')
            if version_state(target) in EDITABLE_VERSION_STATES:
                break
            if self.monotonic() >= deadline:
                raise TimeoutError(f'Timed out waiting for App Store version {old_version} to become editable; retry to resume')
            self.sleep(min(5, max(0, deadline - self.monotonic())))
        target = self.client.request('PATCH', f'appStoreVersions/{identifier}', data=resource('appStoreVersions',
            {'versionString': version}, identifier=identifier))['data']
        print(f'Reused App Store version {identifier}: {old_version} -> {version}; preserved metadata and release type', flush=True)
        return target

    def submit_review(self, app, submission, timeout=300):
        """Make room for this release by withdrawing one older iOS review only on Apple's capacity error."""
        client = self.client
        path = f'reviewSubmissions/{submission["id"]}'
        data = resource('reviewSubmissions', {'submitted': True}, identifier=submission['id'])
        try:
            client.request('PATCH', path, data=data)
            return
        except ApiError as error:
            if error.status != 409 or REVIEW_CAPACITY_ERROR not in str(error):
                raise
        older = [candidate for candidate in client.list(f'apps/{app}/reviewSubmissions', {'limit': 200})
                 if candidate['id'] != submission['id'] and candidate['attributes'].get('platform') == 'IOS'
                 and candidate['attributes'].get('state') in IN_FLIGHT_REVIEW_STATES | {'CANCELING'}
                 and candidate['attributes'].get('submittedDate')]
        if not older:
            raise ValueError('Apple review capacity is full, but no previous in-flight iOS review was found')
        canceling = [candidate for candidate in older if candidate['attributes']['state'] == 'CANCELING']
        previous = min(canceling or older, key=lambda candidate: candidate['attributes']['submittedDate'])
        previous_path = f'reviewSubmissions/{previous["id"]}'
        carry = supplemental_review_items(self.review_items(previous['id']))
        print(f'Apple review capacity is full; replacing previous iOS review {previous["id"]} '
              f'({previous["attributes"]["state"]}, submitted {previous["attributes"]["submittedDate"]}) '
              f'with {submission["id"]}', flush=True)
        if previous['attributes']['state'] != 'CANCELING':
            client.request('PATCH', previous_path, data=resource('reviewSubmissions', {'canceled': True},
                                                              identifier=previous['id']))
        deadline = self.monotonic() + timeout
        carried = False
        while True:
            previous = client.request('GET', previous_path)['data']
            if previous['attributes'].get('state') == 'COMPLETE':
                if carry and not carried:
                    self.carry_review_items(submission, carry)
                    carried = True
                try:
                    client.request('PATCH', path, data=data)
                    return
                except ApiError as error:
                    if error.status != 409 or REVIEW_CAPACITY_ERROR not in str(error):
                        raise
            if self.monotonic() >= deadline:
                raise TimeoutError('Timed out waiting for the previous iOS review to release its slot; '
                                   f'retry to resume submission {submission["id"]}')
            self.sleep(min(5, max(0, deadline - self.monotonic())))

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

    def publish(self, app, version, build, notes, submit=True):
        client = self.client
        versions = client.list(f'apps/{app}/appStoreVersions', {'filter[platform]': 'IOS', 'limit': 200})
        target = matching_version(versions, version)
        previous = next((v for v in sorted(versions, key=lambda v: v['attributes'].get('createdDate', ''), reverse=True)
                         if v['attributes']['versionString'] != version and
                         version_state(v) in ('READY_FOR_SALE', 'REPLACED_WITH_NEW_VERSION',
                                                                  'READY_FOR_DISTRIBUTION')), None)
        if not target:
            # Only a version created here gets the default automatic release after approval.
            attrs = {'platform': 'IOS', 'versionString': version, 'releaseType': 'AFTER_APPROVAL'}
            if previous and previous['attributes'].get('copyright'):
                attrs['copyright'] = previous['attributes']['copyright']
            try:
                target = client.request('POST', 'appStoreVersions', data=resource('appStoreVersions', attrs,
                    {'app': ('apps', app)}))['data']
            except ApiError as error:
                if error.status != 409:
                    raise
                # Another publisher or App Store Connect may have created the version since our read.
                versions = client.list(f'apps/{app}/appStoreVersions', {'filter[platform]': 'IOS', 'limit': 200})
                target = matching_version(versions, version)
                if not target and submit and 'You cannot create a new version of the App in the current state' in str(error):
                    target = self.replace_blocking_version(app, versions, version)
                if not target:
                    states = ', '.join(f'{v["attributes"]["versionString"]}: {version_state(v)}' for v in versions[:8])
                    raise ValueError(f'{error}; existing iOS versions: {states}; resolve the blocking '
                                     'version in App Store Connect before retrying') from error
                print(f'Version creation conflicted; resuming existing App Store version {version} '
                      f'({target["id"]}): {version_state(target)}', flush=True)
        if version_state(target) in SUBMITTED_STATES:
            linked = client.related(f'appStoreVersions/{target["id"]}/build')
            if not linked or linked['id'] != build['id']:
                raise ValueError('Submitted version is attached to a different build')
            self.outcome = version_state(target)
            return target['id']
        # Existing editable version: keep its release type (e.g. MANUAL chosen in App Store Connect).
        identifier = target['id']
        self.carry_metadata(identifier, previous['id'] if previous else None, notes)
        client.request('PATCH', f'appStoreVersions/{identifier}/relationships/build',
                       data={'data': {'type': 'builds', 'id': build['id']}})
        phased = client.related(f'appStoreVersions/{identifier}/appStoreVersionPhasedRelease')
        if not phased:
            client.request('POST', 'appStoreVersionPhasedReleases', data=resource('appStoreVersionPhasedReleases',
                {'phasedReleaseState': 'INACTIVE'}, {'appStoreVersion': ('appStoreVersions', identifier)}))
        elif phased['attributes']['phasedReleaseState'] != 'INACTIVE':
            raise ValueError('Existing phased release is already active/completed; inspect it before resubmitting')
        if not submit:
            self.outcome = 'prepared'
            return identifier
        # Resume an existing READY_FOR_REVIEW submission after an interrupted run.
        submissions = client.list(f'apps/{app}/reviewSubmissions', {'limit': 200})
        submission = None
        attached = False
        carry = []
        for candidate in submissions:
            items = self.review_items(candidate['id'])
            if any((i.get('relationships', {}).get('appStoreVersion', {}).get('data') or {}).get('id') == identifier for i in items):
                if candidate['attributes'].get('state') == 'COMPLETE':
                    # The version resource is reused after withdrawal. Its old submission is historical;
                    # recover ancillary items from it even if the previous run stopped after cancellation.
                    carry.extend(supplemental_review_items(items))
                    continue
                if submission:
                    raise ValueError('Multiple unfinished review submissions matched this App Store version')
                submission, attached = candidate, True
        if submission and submission['attributes'].get('state') != 'READY_FOR_REVIEW':
            raise ValueError('Existing review submission is not editable; inspect App Store Connect')
        if not submission:
            submission = client.request('POST', 'reviewSubmissions', data=resource('reviewSubmissions',
                {'platform': 'IOS'}, {'app': ('apps', app)}))['data']
        if not attached:
            client.request('POST', 'reviewSubmissionItems', data=resource('reviewSubmissionItems', relationships={
                'reviewSubmission': ('reviewSubmissions', submission['id']),
                'appStoreVersion': ('appStoreVersions', identifier)}))
        self.carry_review_items(submission, carry)
        self.submit_review(app, submission)
        self.outcome = 'submitted'
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
    parser.add_argument('--no-submit', dest='submit', action='store_false',
                        help='Prepare the version (build, metadata, phased release) but do not submit it for review')
    args = parser.parse_args(argv)
    bundle, version, number = ipa_identity(args.ipa)
    if (bundle, version) != (args.bundle_id, args.version) or (args.build_number and args.build_number != number):
        raise ValueError('IPA identity does not match the requested release')
    notes = Path(args.notes_file).read_text().strip()
    if not notes or len(notes.encode('utf-16-le')) // 2 > 500:
        raise ValueError('Shared release notes must contain 1–500 UTF-16 code units; never truncate')
    whats_new = appstore_whats_new(notes)
    client = Client()
    app = find_app(client, bundle)
    publisher = Publisher(client)
    if args.already_uploaded:
        print(f'Reusing already uploaded build {version} ({number}); waiting for Apple processing', flush=True)
    elif publisher.find_build(app, version, number):
        print(f'Reusing existing build {version} ({number}); skipping IPA upload', flush=True)
    else:
        subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] / 'kmp/control.py'),
                        'upload-ipa', args.ipa], check=True)
    build = publisher.wait_build(app, version, number, args.processing_timeout)
    identifier = publisher.publish(app, version, build, whats_new, submit=args.submit)
    if publisher.outcome == 'prepared':
        print(f'App Store version {version} ({number}) prepared, not submitted for review; version ID {identifier}; '
              'complete it and submit it in App Store Connect')
    elif publisher.outcome == 'submitted':
        print(f'Submitted App Store version {version} ({number}); version ID {identifier}; '
              'seven-day phased release after approval')
    else:
        print(f'App Store version {version} ({number}) is already {publisher.outcome} with this build; '
              f'version ID {identifier}; nothing changed')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ApiError, ValueError, OSError, TimeoutError, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        message = str(exc).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
        print(f'::error title=App Store submission::{message}', file=sys.stderr)
        raise SystemExit(1)
