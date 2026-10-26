"""Offline What to Test notes tests: composition and App Store Connect HTTP fakes. No credentials."""
import contextlib
import io
import json
import os
from pathlib import Path
import plistlib
import tempfile
import types
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse
import zipfile

import asc_client
import testflight_notes as notes

SHA = 'abcdef0123456789abcdef0123456789abcdef01'
PULL = {'number': 42, 'title': 'Fix globe zoom', 'html_url': 'https://github.com/Lascade-Co/app/pull/42',
        'body': 'Zoom no longer jumps.\r\n\r\nTest the globe.'}
EMOJI, KEYCAP, CHECK, HEART, VS16 = chr(0x1F916), chr(0x20E3), chr(0x2705), chr(0x2764), chr(0xFE0F)


class ComposeTests(unittest.TestCase):
    def test_pr_header_then_description(self):
        self.assertEqual(notes.compose('fix/globe', SHA, pull=PULL),
                         'PR #42 \u00b7 Fix globe zoom\nBranch fix/globe @ abcdef0\n'
                         'https://github.com/Lascade-Co/app/pull/42\n\nZoom no longer jumps.\n\nTest the globe.')

    def test_manual_run_uses_branch_sha_and_head_subject(self):
        commit = {'commit': {'message': 'Tune export bitrate\n\nLonger explanation that is not shown.'}}
        self.assertEqual(notes.compose('dev', SHA, commit=commit), 'Branch dev @ abcdef0\n\nTune export bitrate')

    def test_empty_description_and_template_comments(self):
        for body in (None, '', '<!-- Describe the change -->\n\n'):
            with self.subTest(body=body):
                self.assertTrue(notes.compose('b', SHA, pull=dict(PULL, body=body)).endswith('\n\n(No PR description.)'))
        text = notes.compose('b', SHA, pull=dict(PULL, body='Keep\n<!-- hidden\nmultiline -->\nthis'))
        self.assertTrue(text.endswith('\n\nKeep\n\nthis'))

    def test_long_description_is_truncated_cleanly_within_limit(self):
        body = '\n'.join(f'Line {n}: ' + 'word ' * 12 for n in range(400))
        text = notes.compose('b', SHA, pull=dict(PULL, body=body))
        self.assertLessEqual(len(text), notes.LIMIT)
        self.assertGreater(len(text), notes.LIMIT - 500)
        self.assertTrue(text.startswith('PR #42 \u00b7 Fix globe zoom\n'))
        self.assertTrue(text.endswith('word\n\n\u2026 (truncated; full description on the PR)'))

    def test_unbroken_text_and_exact_limit(self):
        header_length = len(notes.compose('b', SHA, pull=dict(PULL, body='x'))) - 1
        exact = 'y' * (notes.LIMIT - header_length)
        self.assertEqual(len(notes.compose('b', SHA, pull=dict(PULL, body=exact))), notes.LIMIT)
        text = notes.compose('b', SHA, pull=dict(PULL, body=exact + 'y'))
        self.assertEqual(len(text), notes.LIMIT)
        self.assertTrue(text.endswith('y' + notes.PR_MARKER))

    def test_unicode_kept_and_rejected_symbols_removed(self):
        body = (f'Caf\u00e9 \u5730\u56f3 \u0645\u0631\u062d\u0628\u0627 \u05e9\u05dc\u05d5\u05dd \u2192 \u2122 '
                f'done{CHECK} love{HEART}{VS16} 1{VS16}{KEYCAP} {EMOJI} Generated\u0007')
        text = notes.compose('b', SHA, pull=dict(PULL, title=f'{EMOJI} Ship it', body=body))
        self.assertIn('PR #42 \u00b7 Ship it\n', text)
        self.assertTrue(text.endswith('\n\nCaf\u00e9 \u5730\u56f3 \u0645\u0631\u062d\u0628\u0627 \u05e9\u05dc\u05d5\u05dd '
                                      '\u2192 \u2122 done love 1 Generated'))
        self.assertIsNone(notes.REJECTED.search(text))
        self.assertTrue(all(ord(c) <= 0xFFFF for c in text))

    def test_removed_leading_symbol_leaves_no_indent(self):
        body = f'Does X.\n\n{EMOJI} Generated with Claude Code\n  - nested {CHECK} item'
        self.assertTrue(notes.compose('b', SHA, pull=dict(PULL, body=body)).endswith(
            '\n\nDoes X.\n\nGenerated with Claude Code\n  - nested item'))

    def test_truncation_never_strands_a_combining_mark(self):
        prefix_length = len(notes.compose('b', SHA, pull=dict(PULL, body='x'))) - 1
        room = notes.LIMIT - prefix_length - len(notes.PR_MARKER)
        body = 'e\u0301' * notes.LIMIT  # 'e' + combining acute; no word boundaries
        text = notes.compose('b', SHA, pull=dict(PULL, body=body))
        kept = text[prefix_length:-len(notes.PR_MARKER)]
        self.assertLessEqual(len(kept), room)
        self.assertTrue(kept.endswith('e\u0301'))

    def test_untrusted_text_stays_data(self):
        body = '::error::spoof\n$(touch /tmp/x) ${{ secrets.X }}'
        self.assertTrue(notes.compose('b', SHA, pull=dict(PULL, body=body)).endswith('\n\n' + body))

    def test_invalid_sha_rejected(self):
        with self.assertRaises(ValueError):
            notes.compose('b', 'abc', pull=PULL)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FakeAppStore:
    """Apple's order: the uploaded build appears after processing starts; writes may be refused meanwhile."""
    def __init__(self, hidden_polls=0, write_failures=(), localizations=(), apps=1):
        self.requests, self.hidden_polls, self.write_failures = [], hidden_polls, list(write_failures)
        self.localizations, self.apps = list(localizations), apps

    def __call__(self, request, timeout):
        url = urllib.parse.urlsplit(request.full_url)
        path, query = url.path.removeprefix('/v1/'), dict(urllib.parse.parse_qsl(url.query))
        body = json.loads(request.data) if request.data else None
        self.requests.append((request.get_method(), path, query, body, request.get_header('Authorization')))
        if request.get_method() == 'GET' and path == 'apps':
            assert query == {'filter[bundleId]': 'com.example.app.debug'}
            return self.reply({'data': [{'type': 'apps', 'id': f'app{n}'} for n in range(self.apps)]})
        if request.get_method() == 'GET' and path == 'builds':
            assert query == {'filter[app]': 'app0', 'filter[version]': '410.57', 'filter[preReleaseVersion.version]': '5.1.0',
                             'filter[preReleaseVersion.platform]': 'IOS', 'limit': '200'}
            if self.hidden_polls:
                self.hidden_polls -= 1
                return self.reply({'data': []})
            return self.reply({'data': [{'type': 'builds', 'id': 'build1', 'attributes': {'version': '410.57'}}]})
        if request.get_method() == 'GET' and path == 'builds/build1/betaBuildLocalizations':
            return self.reply({'data': self.localizations})
        if request.get_method() in ('POST', 'PATCH') and path.startswith('betaBuildLocalizations'):
            if self.write_failures:
                failure = self.write_failures.pop(0)
                if isinstance(failure, Exception):
                    raise failure
                status, errors = failure
                raise urllib.error.HTTPError(request.full_url, status, 'error', {},
                                             io.BytesIO(json.dumps({'errors': errors}).encode()))
            return self.reply({'data': dict(body['data'], id=body['data'].get('id', 'loc1'))})
        raise AssertionError((request.get_method(), path))

    @staticmethod
    def reply(payload):
        return Response(json.dumps(payload).encode())


class Clock:
    def __init__(self):
        self.now, self.sleeps = 1000.0, []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


CREDENTIALS = {'APPSTORE_ISSUER_ID': 'issuer-id', 'APPSTORE_API_PRIVATE_KEY': 'private-key-value',
               'APPSTORE_API_KEY_ID': 'KEY123'}
PROCESSING = (409, [{'code': 'STATE_ERROR', 'detail': 'The build is still processing.'}])


class PublishTests(unittest.TestCase):
    def setUp(self):
        fake_jwt = types.SimpleNamespace(encode=lambda claims, key, algorithm, headers: 'signed-jwt-value')
        self.enterContext(patch.dict('sys.modules', {'jwt': fake_jwt}))
        self.enterContext(patch.dict('os.environ', CREDENTIALS))
        self.clock, self.log = Clock(), io.StringIO()

    def publish(self, store, timeout=1800):
        notes_client = notes.Notes(asc_client.Client(opener=store), self.clock.sleep, self.clock.monotonic,
                                   lambda *args, **kw: print(*args, file=self.log))
        return notes_client.publish('com.example.app.debug', '5.1.0', '410.57', 'Secret-free notes', timeout)

    def writes(self, store):
        return [r for r in store.requests if r[0] in ('POST', 'PATCH')]

    def test_polls_until_build_appears_then_creates_en_us(self):
        store = FakeAppStore(hidden_polls=2)
        self.assertEqual(self.publish(store), 'created')
        self.assertEqual(self.clock.sleeps, [15, 30])
        self.assertEqual(self.writes(store), [('POST', 'betaBuildLocalizations', {}, {'data': {
            'type': 'betaBuildLocalizations', 'attributes': {'locale': 'en-US', 'whatsNew': 'Secret-free notes'},
            'relationships': {'build': {'data': {'type': 'builds', 'id': 'build1'}}}}}, 'Bearer signed-jwt-value')])
        self.assertEqual([r[1] for r in store.requests].count('apps'), 1)

    def test_existing_en_us_localization_is_patched(self):
        store = FakeAppStore(localizations=[
            {'type': 'betaBuildLocalizations', 'id': 'fr', 'attributes': {'locale': 'fr-FR'}},
            {'type': 'betaBuildLocalizations', 'id': 'en', 'attributes': {'locale': 'en-US', 'whatsNew': 'old'}}])
        self.assertEqual(self.publish(store), 'updated')
        self.assertEqual(self.writes(store)[0][:4], ('PATCH', 'betaBuildLocalizations/en', {}, {'data': {
            'type': 'betaBuildLocalizations', 'attributes': {'whatsNew': 'Secret-free notes'}, 'id': 'en'}}))
        self.assertEqual(self.clock.sleeps, [])

    def test_refused_writes_retry_with_backoff_and_relist(self):
        store = FakeAppStore(write_failures=[PROCESSING, (500, []), urllib.error.URLError('reset'), PROCESSING])
        self.assertEqual(self.publish(store), 'created')
        self.assertEqual(self.clock.sleeps, [15, 30, 60, 120])
        self.assertEqual(len(self.writes(store)), 5)
        self.assertEqual([r[1] for r in store.requests].count('builds'), 1)
        self.assertEqual([r[1] for r in store.requests].count('builds/build1/betaBuildLocalizations'), 5)

    def test_times_out_within_bound(self):
        store = FakeAppStore(write_failures=[PROCESSING] * 100)
        with self.assertRaisesRegex(TimeoutError, r'5\.1\.0 \(410\.57\).*still processing'):
            self.publish(store, timeout=1800)
        self.assertEqual(sum(self.clock.sleeps), 1800)
        self.assertLessEqual(max(self.clock.sleeps), 120)

    def test_never_visible_build_times_out(self):
        store = FakeAppStore(hidden_polls=10**6)
        with self.assertRaisesRegex(TimeoutError, 'not visible yet'):
            self.publish(store, timeout=600)
        self.assertFalse(self.writes(store))

    def test_final_errors_fail_without_retry(self):
        invalid = (409, [{'code': 'ENTITY_ERROR.ATTRIBUTE.INVALID',
                          'detail': "Text for whatsNew contains invalid characters:'[x]'."}])
        for failure in (invalid, (403, [{'code': 'FORBIDDEN_ERROR', 'detail': 'No access'}]), (400, [])):
            with self.subTest(status=failure[0]):
                self.clock.sleeps.clear()
                with self.assertRaises(asc_client.ApiError):
                    self.publish(FakeAppStore(write_failures=[failure]))
                self.assertEqual(self.clock.sleeps, [])

    def test_ambiguous_app_fails(self):
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            self.publish(FakeAppStore(apps=2))

    def test_jwt_key_and_notes_never_logged(self):
        store = FakeAppStore(hidden_polls=1, write_failures=[PROCESSING])
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.publish(store)
        logged = self.log.getvalue() + stdout.getvalue()
        self.assertIn('Created en-US What to Test notes (17 characters) for 5.1.0 (410.57)', logged)
        for secret in ('signed-jwt-value', 'private-key-value', 'Secret-free notes'):
            self.assertNotIn(secret, logged)


class CommandTests(unittest.TestCase):
    def test_identity_outputs_ipa_bundle_version_and_build(self):
        with tempfile.TemporaryDirectory() as directory:
            ipa, out = Path(directory) / 'app.ipa', Path(directory) / 'out'
            with zipfile.ZipFile(ipa, 'w') as archive:
                archive.writestr('Payload/App.app/Info.plist', plistlib.dumps({
                    'CFBundleIdentifier': 'com.example.app.debug', 'CFBundleShortVersionString': '5.1.0',
                    'CFBundleVersion': '410.57'}))
            with patch.dict('os.environ', GITHUB_OUTPUT=str(out)):
                notes.main(['identity', str(ipa)])
            self.assertEqual(out.read_text(), 'bundle_id=com.example.app.debug\nversion=5.1.0\nbuild_number=410.57\n')

    def test_compose_reads_github_json_files(self):
        with tempfile.TemporaryDirectory() as directory:
            pull, out = Path(directory) / 'pull.json', Path(directory) / 'notes.txt'
            pull.write_text(json.dumps(PULL))
            with contextlib.redirect_stdout(io.StringIO()):
                notes.main(['compose', '--branch', 'fix/globe', '--sha', SHA, '--pull', str(pull), '--output', str(out)])
            self.assertEqual(out.read_text(), notes.compose('fix/globe', SHA, pull=PULL))

    def test_publish_rejects_empty_or_oversized_notes_before_network(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'notes.txt'
            for text in (' \n', 'x' * (notes.LIMIT + 1)):
                path.write_text(text)
                with self.subTest(length=len(text)), patch.object(notes, 'Client', side_effect=AssertionError), \
                        self.assertRaises(ValueError):
                    notes.main(['publish', '--bundle-id', 'b', '--version', '1.0.0', '--build-number', '1',
                                '--notes-file', str(path)])


if __name__ == '__main__':
    unittest.main()
