"""Offline App Store API contract tests. No credentials, Apple calls or IPA upload."""
import copy
import io
import json
from pathlib import Path
import plistlib
import tempfile
import types
import unittest
from unittest.mock import patch
import urllib.error
import zipfile

import publish_appstore as asc
import publish_metadata


def item(kind, identifier, **attrs):
    return {'type': kind, 'id': identifier, 'attributes': attrs}


class FakeClient:
    """Apple order: version creation clones metadata before a caller can attach/submit its build."""
    def __init__(self, target=True):
        self.calls = []
        self.versions = [item('appStoreVersions', 'old', versionString='5.0.8',
                              appStoreState='READY_FOR_SALE', createdDate='2026-10-01', copyright='2026 Lascade')]
        if target:
            self.versions.append(item('appStoreVersions', 'new', versionString='5.0.9',
                                      appStoreState='PREPARE_FOR_SUBMISSION', createdDate='2026-10-24'))
        self.locales = {'new': [item('appStoreVersionLocalizations', 'en-new', locale='en-US',
                                     description='Current description')],
                        'old': [item('appStoreVersionLocalizations', 'en-old', locale='en-US',
                                     description='Old description', supportUrl='https://example.com/help'),
                                item('appStoreVersionLocalizations', 'fr-old', locale='fr-FR',
                                     description='Description française', keywords='voyage')]}
        self.reviews = {'old': item('appStoreReviewDetails', 'old-review', contactEmail='review@example.com',
                                   contactFirstName='Reviewer', demoAccountRequired=True, notes='Sign in instructions')}
        self.phased = None
        self.submissions = []
        self.submission_items = {}
        self.builds = [item('builds', 'exact-build', version='509', processingState='VALID', expired=False)]

    def list(self, path, params=None):
        self.calls.append(('LIST', path, params))
        if path == 'apps/app/appStoreVersions':
            return copy.deepcopy(self.versions)
        if path == 'apps':
            return [{'id': 'app'}]
        if path.endswith('/appStoreVersionLocalizations'):
            return copy.deepcopy(self.locales[path.split('/')[1]])
        if path == 'apps/app/reviewSubmissions':
            return copy.deepcopy(self.submissions)
        if path.endswith('/items'):
            assert params['include'] == 'appStoreVersion'
            return copy.deepcopy(self.submission_items[path.split('/')[1]])
        if path == 'builds':
            assert params['filter[app]'] == 'app'
            assert params['filter[version]'] == '509'
            assert params['filter[preReleaseVersion.version]'] == '5.0.9'
            assert params['filter[preReleaseVersion.platform]'] == 'IOS'
            return copy.deepcopy(self.builds)
        raise AssertionError(path)

    def related(self, path):
        self.calls.append(('RELATED', path, None))
        if path.endswith('/appStoreReviewDetail'):
            return copy.deepcopy(self.reviews.get(path.split('/')[1]))
        if path.endswith('/appStoreVersionPhasedRelease'):
            return copy.deepcopy(self.phased)
        if path.endswith('/build'):
            return copy.deepcopy(self.builds[0])
        raise AssertionError(path)

    def request(self, method, path, params=None, data=None):
        self.calls.append((method, path, copy.deepcopy(data)))
        if method == 'POST':
            result = copy.deepcopy(data['data'])
            result['id'] = {'appStoreVersions': 'new', 'reviewSubmissions': 'review',
                            'appStoreVersionLocalizations': 'fr-new'}.get(path, 'created')
            return {'data': result}
        return {'data': copy.deepcopy(data.get('data'))}


class PublisherTests(unittest.TestCase):
    def test_create_version_attach_build_notes_every_locale_and_submit_after_phased_setup(self):
        client = FakeClient(target=False)
        self.assertEqual(asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Shared English notes.'), 'new')
        created = next(c[2]['data'] for c in client.calls if c[:2] == ('POST', 'appStoreVersions'))
        self.assertEqual(created['attributes'], {'platform': 'IOS', 'versionString': '5.0.9',
            'releaseType': 'AFTER_APPROVAL', 'copyright': '2026 Lascade'})
        self.assertEqual(created['relationships']['app']['data']['id'], 'app')
        patched = [c[2]['data'] for c in client.calls if c[0] == 'PATCH' and c[1].startswith('appStoreVersionLocalizations/')]
        self.assertEqual(len(patched), 2)
        self.assertTrue(all(c['attributes']['whatsNew'] == 'Shared English notes.' for c in patched))
        en = next(c for c in patched if c['id'] == 'en-new')
        self.assertNotIn('description', en['attributes'])  # Preserve current metadata.
        self.assertEqual(en['attributes']['supportUrl'], 'https://example.com/help')
        fr = next(c[2]['data']['attributes'] for c in client.calls if c[:2] == ('POST', 'appStoreVersionLocalizations'))
        self.assertEqual(fr['description'], 'Description française')
        build_index = next(i for i, c in enumerate(client.calls) if c[1] == 'appStoreVersions/new/relationships/build')
        self.assertEqual(client.calls[build_index][2], {'data': {'type': 'builds', 'id': 'exact-build'}})
        phased_index = next(i for i, c in enumerate(client.calls) if c[:2] == ('POST', 'appStoreVersionPhasedReleases'))
        self.assertEqual(client.calls[phased_index][2]['data']['attributes'], {'phasedReleaseState': 'INACTIVE'})
        submit_index = next(i for i, c in enumerate(client.calls) if c[:2] == ('PATCH', 'reviewSubmissions/review'))
        self.assertLess(build_index, submit_index)
        self.assertLess(phased_index, submit_index)
        self.assertEqual(client.calls[submit_index][2]['data']['attributes'], {'submitted': True})
        review_item = next(c[2]['data'] for c in client.calls if c[:2] == ('POST', 'reviewSubmissionItems'))
        self.assertEqual(review_item['relationships']['appStoreVersion']['data']['id'], 'new')

    def test_partial_review_metadata_fills_missing_fields_without_overwriting_boolean_or_current_contact(self):
        client = FakeClient()
        client.reviews['new'] = item('appStoreReviewDetails', 'new-review', contactEmail='current@example.com',
                                      demoAccountRequired=False, contactFirstName='')
        asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Notes.')
        attrs = next(c[2]['data']['attributes'] for c in client.calls if c[:2] == ('PATCH', 'appStoreReviewDetails/new-review'))
        self.assertEqual(attrs, {'contactFirstName': 'Reviewer', 'notes': 'Sign in instructions'})

    def test_retry_reuses_attached_draft_submission_without_duplicate_mutations(self):
        client = FakeClient()
        client.submissions = [item('reviewSubmissions', 'draft', state='READY_FOR_REVIEW')]
        client.submission_items['draft'] = [{'relationships': {'appStoreVersion': {'data': {'id': 'new'}}}}]
        asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Notes.')
        self.assertFalse(any(c[:2] in [('POST', 'reviewSubmissions'), ('POST', 'reviewSubmissionItems')] for c in client.calls))
        self.assertTrue(any(c[:2] == ('PATCH', 'reviewSubmissions/draft') for c in client.calls))

    def test_submitted_exact_build_is_noop(self):
        client = FakeClient()
        client.versions[1]['attributes']['appStoreState'] = 'WAITING_FOR_REVIEW'
        self.assertEqual(asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Notes.'), 'new')
        self.assertFalse(any(c[0] in ('POST', 'PATCH') for c in client.calls))

    def test_modern_app_version_state_takes_precedence_over_deprecated_state(self):
        client = FakeClient()
        client.versions[1]['attributes']['appVersionState'] = 'READY_FOR_DISTRIBUTION'
        self.assertEqual(asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Notes.'), 'new')
        self.assertFalse(any(c[0] in ('POST', 'PATCH') for c in client.calls))

    def test_submitted_version_with_wrong_build_fails(self):
        client = FakeClient()
        client.versions[1]['attributes']['appStoreState'] = 'IN_REVIEW'
        with self.assertRaisesRegex(ValueError, 'different build'):
            asc.Publisher(client).publish('app', '5.0.9', {'id': 'wrong'}, 'Notes.')

    def test_active_phased_release_fails_before_review_submission(self):
        client = FakeClient()
        client.phased = item('appStoreVersionPhasedReleases', 'phase', phasedReleaseState='ACTIVE')
        with self.assertRaisesRegex(ValueError, 'already active'):
            asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Notes.')
        self.assertFalse(any(c[:2] == ('POST', 'reviewSubmissions') for c in client.calls))

    def test_wait_processing_then_valid_and_refresh_exact_identity(self):
        client = FakeClient()
        client.builds[0]['attributes']['processingState'] = 'PROCESSING'
        clock = [0]
        def sleep(seconds):
            clock[0] += seconds
            client.builds[0]['attributes']['processingState'] = 'VALID'
        found = asc.Publisher(client, sleep, lambda: clock[0]).wait_build('app', '5.0.9', '509', 60)
        self.assertEqual(found['id'], 'exact-build')
        self.assertEqual(clock[0], 30)

    def test_invalid_expired_and_timeout_never_submit(self):
        for state in ('INVALID', 'FAILED'):
            client = FakeClient()
            client.builds[0]['attributes']['processingState'] = state
            with self.assertRaisesRegex(ValueError, state):
                asc.Publisher(client).wait_build('app', '5.0.9', '509', 0)
        client = FakeClient()
        client.builds[0]['attributes']['expired'] = True
        with self.assertRaisesRegex(ValueError, 'expired'):
            asc.Publisher(client).wait_build('app', '5.0.9', '509', 0)
        client.builds = []
        with self.assertRaises(TimeoutError):
            asc.Publisher(client).wait_build('app', '5.0.9', '509', 0)

    def test_no_locales_is_clear_metadata_blocker(self):
        client = FakeClient()
        client.locales = {'new': [], 'old': []}
        with self.assertRaisesRegex(ValueError, 'metadata'):
            asc.Publisher(client).publish('app', '5.0.9', client.builds[0], 'Notes.')


class IdentityTests(unittest.TestCase):
    def run_main(self, client, extra=(), upload=None):
        with tempfile.TemporaryDirectory() as folder:
            notes = Path(folder) / 'notes.txt'
            notes.write_text('Shared English notes.')
            with patch.object(asc, 'Client', return_value=client), patch.object(asc, 'ipa_identity',
                    return_value=('com.travelanimator.routemap', '5.0.9', '509')), patch.object(
                    asc.subprocess, 'run', side_effect=upload) as run:
                result = asc.main(['--ipa', 'release.ipa', '--version', '5.0.9', '--notes-file', str(notes), *extra])
                return result, run.call_args_list

    def test_existing_build_skips_altool_on_retry(self):
        result, calls = self.run_main(FakeClient())
        self.assertEqual(result, 0)
        self.assertEqual(calls, [])

    def test_absent_build_uploads_with_existing_control_command_before_processing(self):
        client = FakeClient()
        build = client.builds.pop()
        def upload(command, check):
            self.assertEqual(command[-2:], ['upload-ipa', 'release.ipa'])
            self.assertTrue(command[1].endswith('scripts/kmp/control.py'))
            self.assertTrue(check)
            client.builds.append(build)
        result, calls = self.run_main(client, upload=upload)
        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 1)

    def test_already_uploaded_flag_and_build_number_validation(self):
        result, calls = self.run_main(FakeClient(), ['--already-uploaded', '--build-number', '509'])
        self.assertEqual(result, 0)
        self.assertEqual(calls, [])
        with self.assertRaisesRegex(ValueError, 'identity'):
            self.run_main(FakeClient(), ['--build-number', '510'])

    def test_already_uploaded_flag_skips_upload_even_before_build_is_visible(self):
        client = FakeClient()
        build = client.builds.pop()
        with patch.object(asc.Publisher, 'wait_build', return_value=build) as wait:
            result, calls = self.run_main(client, ['--already-uploaded'])
        self.assertEqual(result, 0)
        self.assertEqual(calls, [])
        wait.assert_called_once_with('app', '5.0.9', '509', 5400)

    def test_ipa_reads_only_main_app_info_not_extension(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'release.ipa'
            with zipfile.ZipFile(path, 'w') as archive:
                archive.writestr('Payload/App.app/Info.plist', plistlib.dumps({
                    'CFBundleIdentifier': 'com.travelanimator.routemap', 'CFBundleShortVersionString': '5.0.9',
                    'CFBundleVersion': '509'}))
                archive.writestr('Payload/App.app/PlugIns/Share.appex/Info.plist', b'not the main plist')
            self.assertEqual(asc.ipa_identity(path), ('com.travelanimator.routemap', '5.0.9', '509'))

    def test_missing_or_multiple_main_apps_fail(self):
        with tempfile.TemporaryDirectory() as folder:
            for count in (0, 2):
                path = Path(folder) / f'{count}.ipa'
                with zipfile.ZipFile(path, 'w') as archive:
                    for n in range(count):
                        archive.writestr(f'Payload/App{n}.app/Info.plist', b'bad')
                with self.assertRaisesRegex(ValueError, 'exactly one'):
                    asc.ipa_identity(path)


class MetadataTests(unittest.TestCase):
    def test_published_release_shared_notes_and_safe_outputs(self):
        outputs, notes = publish_metadata.validate({'repo': 'Lascade-Co/travel-animator-shared', 'release_id': 42, 'package_name': 'com.travelanimator.routemap'},
            {'id': 42, 'tag_name': 'v5.0.9', 'body': ' English notes. '}, 'Lascade-Co/actions')
        self.assertEqual(outputs['version'], '5.0.9')
        self.assertEqual(notes, 'English notes.')

    def test_bad_releases_and_overlong_notes_rejected_without_truncation(self):
        payload = {'repo': 'Lascade-Co/travel-animator-shared', 'release_id': 42, 'package_name': 'com.travelanimator.routemap'}
        release = {'id': 42, 'tag_name': 'v5.0.9', 'body': 'Notes.'}
        for extra in ({'draft': True}, {'prerelease': True}, {'body': '🚀' * 251}, {'body': ''},
                      {'tag_name': 'v5.0.9\ncommand'}, {'id': 43}):
            with self.assertRaises(ValueError):
                publish_metadata.validate(payload, release | extra, 'Lascade-Co/actions')
        with self.assertRaises(ValueError):
            publish_metadata.validate(payload | {'repo': 'attacker/app'}, release, 'Lascade-Co/actions')
        with self.assertRaises(ValueError):
            publish_metadata.validate(payload | {'repo': 'Lascade-Co/other'}, release, 'Lascade-Co/actions')
        with self.assertRaises(ValueError):
            publish_metadata.validate(payload | {'package_name': 'com.attacker.app'}, release, 'Lascade-Co/actions')


class ClientTests(unittest.TestCase):
    def test_list_follows_apple_pagination(self):
        client = asc.Client()
        with patch.object(client, 'request', side_effect=[{'data': [1], 'links': {'next': asc.API + 'apps?page=2'}},
                                                         {'data': [2]}]) as request:
            self.assertEqual(client.list('apps', {'limit': 1}), [1, 2])
            self.assertEqual(request.call_args_list[1].args, ('GET', asc.API + 'apps?page=2'))

    def test_api_error_reports_apple_reason(self):
        error = urllib.error.HTTPError(asc.API, 409, 'Conflict', {}, io.BytesIO(json.dumps({
            'errors': [{'code': 'ENTITY_ERROR.ATTRIBUTE.REQUIRED', 'detail': 'Review contact missing'}]}).encode()))
        fake_jwt = types.SimpleNamespace(encode=lambda *a, **kw: 'token')
        with patch.dict('sys.modules', {'jwt': fake_jwt}), patch.dict('os.environ', {
            'APPSTORE_ISSUER_ID': 'issuer', 'APPSTORE_API_PRIVATE_KEY': 'private', 'APPSTORE_API_KEY_ID': 'key'}):
            client = asc.Client(opener=lambda *a, **kw: (_ for _ in ()).throw(error))
            with self.assertRaisesRegex(asc.ApiError, 'Review contact missing'):
                client.request('POST', 'reviewSubmissions', data={})


if __name__ == '__main__':
    unittest.main()
