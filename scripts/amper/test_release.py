"""Offline gates for the Amper release: dispatch trust, bump policy, assets, token absence and the version commit."""
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import zipfile

import yaml

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('amper_release', HERE / 'release.py')
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)

REPO = 'Lascade-Co/TreasureHunch'
PBX = 'iosApp/module.xcodeproj/project.pbxproj'
needs_yq = unittest.skipUnless(shutil.which('yq'), 'yq not installed')


@contextmanager
def in_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True, stderr=subprocess.DEVNULL).strip()


class Dispatch(unittest.TestCase):
    payload = dict(repo=REPO, branch='main', sha='a' * 40, project_slug='treasurehunch')

    def test_registered_repo_with_its_project_validates(self):
        values = release.validate(self.payload, 'Lascade-Co/actions')
        self.assertEqual((values['owner'], values['name'], values['project_slug']), ('Lascade-Co', 'TreasureHunch', 'treasurehunch'))

    def test_invalid_scope_never_reaches_the_api(self):
        for change in ({'repo': 'Lascade-Co/unregistered'}, {'repo': 'attacker/TreasureHunch'}, {'branch': 'dev'},
                       {'sha': 'a' * 39}, {'project_slug': 'travelanimator'}, {'project_slug': ''}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                release.resolve(dict(self.payload, **change), 'Lascade-Co/actions', lambda path: self.fail('API called'))
        with self.assertRaises(ValueError):
            release.validate(self.payload, 'someone/actions')

    def test_pins_main_and_rejects_stale_dispatch(self):
        calls = []
        head = {'sha': 'a' * 40, 'commit': {'message': 'Merge pull request #3'}}
        self.assertEqual(release.resolve(self.payload, 'Lascade-Co/actions', lambda p: calls.append(p) or head)['skip'], 'false')
        self.assertEqual(calls, [f'repos/{REPO}/commits/main'])
        with self.assertRaisesRegex(ValueError, 'stale'):
            release.resolve(self.payload, 'Lascade-Co/actions', lambda p: dict(head, sha='b' * 40))

    def test_bot_bump_head_skips(self):
        head = {'sha': 'b' * 40, 'author': {'type': 'Bot'}, 'commit': {'message': 'chore(release): v0.1.1 (2) [skip ci]'}}
        self.assertEqual(release.resolve(self.payload, 'Lascade-Co/actions', lambda p: head)['skip'], 'true')


class Concurrency(unittest.TestCase):
    def group(self, repo):
        workflow = yaml.safe_load((HERE.parents[1] / '.github/workflows/amper-release.yml').read_text())
        expression = workflow['concurrency']['group']
        self.assertIs(workflow['concurrency']['cancel-in-progress'], False)
        return re.sub(r'\$\{\{\s*github\.event\.client_payload\.repo\s*\}\}', repo, expression)

    def test_two_repos_get_different_keys(self):
        self.assertEqual(self.group(REPO), 'amper-release-Lascade-Co/TreasureHunch')
        self.assertNotEqual(self.group(REPO), self.group('Lascade-Co/OtherApp'))


class WorkflowShape(unittest.TestCase):
    def setUp(self):
        self.jobs = yaml.safe_load((HERE.parents[1] / '.github/workflows/amper-release.yml').read_text())['jobs']

    def test_release_restores_only_release_only_bundles(self):
        # Release-only names; amper-debug may restore them but never publishes any bundle.
        names = [step['with']['name'] for job in self.jobs.values() for step in job['steps']
                 if step.get('uses') == './control/actions/gh-bundle-cache']
        self.assertTrue(names)
        for name in names:
            self.assertRegex(name, r"-release'\) \}\}$")

    def test_every_bundle_publish_waits_for_a_successful_cache_secret_scan(self):
        for name in ('android', 'ios-build'):
            steps = self.jobs[name]['steps']
            names = [step.get('name', '') for step in steps]
            scan = next(i for i, step in enumerate(steps) if step.get('id') == 'cache-scan')
            self.assertIn('check-cache', steps[scan]['run'])
            self.assertNotIn('continue-on-error', steps[scan])
            # staging is read as a file: an env export would overwrite the prod KEYSTORE_* the scan searches for
            staging = names.index('Read /Build from Infisical staging for the cache secret scan')
            self.assertEqual(steps[staging]['with']['export-type'], 'file')
            self.assertLess(staging, scan)
            publishes = [i for i, step in enumerate(steps) if step.get('with', {}).get('mode') == 'publish']
            self.assertTrue(publishes)
            for i in publishes:
                with self.subTest(job=name, step=names[i]):
                    self.assertGreater(i, scan)
                    self.assertIn("steps.cache-scan.outcome == 'success'", steps[i]['if'])

    def test_release_apk_is_signed_before_it_is_located(self):
        names = [step.get('name', '') for step in self.jobs['android']['steps']]
        sign = names.index('Sign the release APK with the upload keystore')
        self.assertLess(names.index('Build the release APK'), sign)
        self.assertLess(sign, names.index('Locate, check and name the AAB, APK and R8 mapping'))
        self.assertIn('sign-apk', self.jobs['android']['steps'][sign]['run'])

    def test_firebase_cli_installs_before_any_prod_secret(self):
        steps = [step.get('name', '') for step in self.jobs['android']['steps']]
        install = steps.index('Install the Firebase CLI for Crashlytics mapping files')
        self.assertLess(install, steps.index('Read /Build from Infisical prod'))
        self.assertIn('--ignore-scripts', self.jobs['android']['steps'][install]['run'])



BUILD_COMMAND = re.compile(r'(^|\s)(\./kotlin|xcodebuild) ', re.M)  # invocations, not xcodebuild.log
TOKEN_USE = re.compile(r'steps\.([\w-]+)\.outputs\.token')


def assert_fresh_tokens_after_build(case, workflow, jobs):
    """Every App token a step uses after the job's last build step was minted after that build: the token lasts
    one hour, and the iOS jobs run up to 90 minutes."""
    loaded = yaml.safe_load((HERE.parents[1] / '.github/workflows' / workflow).read_text())['jobs']
    for name in jobs:
        steps = loaded[name]['steps']
        last_build = max(i for i, s in enumerate(steps) if BUILD_COMMAND.search(str(s.get('run', ''))))
        minted = {s['id']: i for i, s in enumerate(steps)
                  if str(s.get('uses', '')).startswith('actions/create-github-app-token')}
        uses = [(i, token) for i, s in enumerate(steps) if i > last_build for token in TOKEN_USE.findall(yaml.safe_dump(s))]
        case.assertTrue(uses, f'{workflow} {name}: no source-repo step after the build')
        for i, token in uses:
            with case.subTest(workflow=workflow, job=name, step=steps[i].get('name')):
                case.assertIn(token, minted)
                case.assertGreater(minted[token], last_build)
                case.assertLess(minted[token], i)


class FreshTokens(unittest.TestCase):
    def test_release_uploads_use_tokens_minted_after_the_build(self):
        assert_fresh_tokens_after_build(self, 'amper-release.yml', ('android', 'ios-build'))

    def test_debug_uploads_use_tokens_minted_after_the_build(self):
        assert_fresh_tokens_after_build(self, 'amper-debug.yml', ('android', 'ios-build'))

class DebugWorkflowShape(unittest.TestCase):
    """amper-debug runs PR code (the source's kotlin wrapper, Xcode's Build Kotlin phase): it may not write GHCR,
    and the org-wide App Store Connect key stays out of every job that runs it."""
    def setUp(self):
        self.workflow = yaml.safe_load((HERE.parents[1] / '.github/workflows/amper-debug.yml').read_text())
        self.jobs = self.workflow['jobs']

    def test_no_job_has_packages_write(self):
        top = self.workflow.get('permissions', {})
        self.assertNotIn(top.get('packages'), ('write',))
        for name, job in self.jobs.items():
            effective = job.get('permissions', top)
            with self.subTest(job=name):
                self.assertIsInstance(effective, dict)  # never read-all / write-all
                self.assertNotEqual(effective.get('packages'), 'write')
        for name in ('android', 'ios-build'):
            self.assertEqual(self.jobs[name]['permissions']['packages'], 'read')

    def test_restores_only_release_bundles_and_publishes_none(self):
        steps = [(name, step) for name, job in self.jobs.items() for step in job.get('steps', [])
                 if step.get('uses') == './control/actions/gh-bundle-cache']
        self.assertTrue(steps)
        for name, step in steps:
            with self.subTest(job=name, step=step['name']):
                self.assertEqual(step['with']['mode'], 'restore')
                self.assertRegex(step['with']['name'], r"-release'\) \}\}$")

    def test_app_store_key_only_in_jobs_that_run_no_source_code(self):
        for name, job in self.jobs.items():
            text = yaml.safe_dump(job)
            runs_source = any(step.get('with', {}).get('path') == 'source' for step in job.get('steps', []))
            with self.subTest(job=name):
                if 'APPSTORE_' in text:
                    self.assertFalse(runs_source)
        publish = self.jobs['ios-publish']
        self.assertEqual(publish['needs'], ['prepare', 'android', 'ios-build'])
        self.assertIn('APPSTORE_API_PRIVATE_KEY', yaml.safe_dump(publish))
        self.assertNotIn('working-directory', yaml.safe_dump(publish.get('defaults', {})))
        checkouts = [s for s in publish['steps'] if str(s.get('uses', '')).startswith('actions/checkout')]
        self.assertEqual([s['with']['repository'] for s in checkouts], ['${{ github.repository }}'])
        self.assertNotIn('APPSTORE_', yaml.safe_dump(self.jobs['ios-build']))
        self.assertNotIn('actions', self.jobs['ios-build']['permissions'])

    def test_testflight_consumers_read_ios_publish(self):
        self.assertIn('ios-publish', self.jobs['testflight-notes']['needs'])
        self.assertIn('ios-publish', self.jobs['notify']['needs'])
        text = (HERE.parents[1] / '.github/workflows/amper-debug.yml').read_text()
        self.assertNotIn("needs['ios-build'].outputs.uploaded", text)

    def test_debug_apk_is_signed_and_built_from_the_manifest_module(self):
        steps = self.jobs['android']['steps']
        names = [step.get('name', '') for step in steps]
        build = steps[names.index('Build the release APK')]
        self.assertIn('-m "$MODULE"', build['run'])
        self.assertNotIn('androidApp', build['run'])
        self.assertLess(names.index('Build the release APK'), names.index('Sign the APK with the debug keystore'))
        self.assertLess(names.index('Sign the APK with the debug keystore'), names.index('Locate and name the APK'))


class Bump(unittest.TestCase):
    def test_patch_by_default_and_build_always_plus_one(self):
        self.assertEqual(release.next_version('0.1.9', '41', []), ('0.1.10', '42', 'patch'))

    def test_minor_and_major_labels(self):
        self.assertEqual(release.next_version('1.4.2', '7', ['minor']), ('1.5.0', '8', 'minor'))
        self.assertEqual(release.next_version('1.4.2', '7', ['minor', 'major']), ('2.0.0', '8', 'major'))

    def test_rejects_non_semver_source_version(self):
        with self.assertRaises(ValueError):
            release.next_version('1.0', '1', [])

    def test_labels_come_from_prs_merged_into_main(self):
        pulls = [{'merged_at': '2026-11-17', 'base': {'ref': 'main'}, 'labels': [{'name': 'minor'}]},
                 {'merged_at': None, 'base': {'ref': 'main'}, 'labels': [{'name': 'major'}]},
                 {'merged_at': '2026-11-17', 'base': {'ref': 'dev'}, 'labels': [{'name': 'major'}]}]
        calls = []
        self.assertEqual(release.merged_labels(REPO, 'a' * 40, lambda p: calls.append(p) or pulls), ['minor'])
        self.assertEqual(calls, [f'repos/{REPO}/commits/{"a" * 40}/pulls'])


class Assets(unittest.TestCase):
    NAMES = ('TreasureHunch-0.1.1-2.aab', 'TreasureHunch-0.1.1-2.apk', 'TreasureHunch-0.1.1-2.ipa',
             'TreasureHunch-0.1.1-2-mapping.txt', 'TreasureHunch-0.1.1-2-dSYMs.zip')

    def folder(self, names=NAMES):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        for name in names:
            (Path(temp.name) / name).write_bytes(b'asset')
        return Path(temp.name)

    def test_five_assets_without_native_symbols(self):
        self.assertEqual(len(release.release_assets(self.folder())), 5)

    def test_native_symbols_are_optional_but_unique(self):
        folder = self.folder(self.NAMES + ('native-debug-symbols.zip',))
        self.assertEqual(len(release.release_assets(folder)), 6)
        (folder / 'other-native-debug-symbols.zip').write_bytes(b'x')
        with self.assertRaisesRegex(ValueError, 'native-debug-symbols'):
            release.release_assets(folder)

    def test_missing_empty_duplicate_or_unexpected_assets_fail(self):
        with self.assertRaisesRegex(ValueError, 'ipa'):
            release.release_assets(self.folder(self.NAMES[:2] + self.NAMES[3:]))
        folder = self.folder()
        (folder / self.NAMES[3]).write_bytes(b'')
        with self.assertRaisesRegex(ValueError, 'mapping'):
            release.release_assets(folder)
        with self.assertRaisesRegex(ValueError, 'apk'):
            release.release_assets(self.folder(self.NAMES + ('old.apk',)))
        with self.assertRaisesRegex(ValueError, 'Unexpected'):
            release.release_assets(self.folder(self.NAMES + ('xcodebuild.log',)))


class TokenAbsence(unittest.TestCase):
    TOKEN = '0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0'

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)
        self.secrets = self.dir / '.staging.env'
        self.secrets.write_text(f"KEY_ALIAS='debug'\nAPPCHECK_DEBUG_TOKEN='{self.TOKEN}'")

    def archive(self, name, payload):
        path = self.dir / name
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
            z.writestr('base/dex/classes.dex', b'\x00dex' + payload + b'\x00')
            z.writestr('Payload/app.app/Info.plist', b'plist')
        return path

    def test_clean_binaries_pass_and_secrets_file_is_deleted(self):
        clean = [self.archive('a.aab', b'nothing'), self.archive('a.ipa', b'')]
        self.assertEqual(release.check_token(self.secrets, clean), {'a.aab': 0, 'a.ipa': 0})
        self.assertFalse(self.secrets.exists())

    def test_utf8_token_in_dex_fails_without_printing_it(self):
        path = self.archive('a.aab', self.TOKEN.encode())
        with self.assertRaises(ValueError) as caught:
            release.check_token(self.secrets, [path])
        self.assertNotIn(self.TOKEN, str(caught.exception))
        self.assertFalse(self.secrets.exists())

    def test_utf16_token_in_native_binary_fails(self):
        # Kotlin/Native keeps string literals as UTF-16, which a plain ASCII grep misses.
        path = self.archive('a.ipa', self.TOKEN.encode('utf-16-le'))
        with self.assertRaises(ValueError):
            release.check_token(self.secrets, [path])

    def test_token_split_across_read_chunks_is_found(self):
        with patch.object(release, 'CHUNK', 7):
            path = self.archive('a.aab', b'x' * 5 + self.TOKEN.encode())
            with self.assertRaises(ValueError):
                release.check_token(self.secrets, [path])

    def test_missing_or_short_token_fails_instead_of_passing_vacuously(self):
        for body in ("KEY_ALIAS='debug'", "APPCHECK_DEBUG_TOKEN=''", "APPCHECK_DEBUG_TOKEN='abc'"):
            self.secrets.write_text(body)
            with self.subTest(body=body), self.assertRaises(ValueError):
                release.check_token(self.secrets, [self.archive('a.aab', b'')])


class MappingId(unittest.TestCase):
    def test_generated_id_must_be_packaged(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            resource = folder / 'crashlytics_mapping_file_id.xml'
            resource.write_text('<resources><string name="com.google.firebase.crashlytics.mapping_file_id" '
                                'translatable="false">56772fb712cd417291efd9ddece17081</string></resources>')
            with zipfile.ZipFile(folder / 'ok.aab', 'w') as z:
                z.writestr('base/resources.pb', b'..56772fb712cd417291efd9ddece17081..')
            with zipfile.ZipFile(folder / 'bad.aab', 'w') as z:
                z.writestr('base/resources.pb', b'..no id..')
            self.assertEqual(release.mapping_id(resource), '56772fb712cd417291efd9ddece17081')
            release.check_mapping_id(resource, [folder / 'ok.aab'])
            with self.assertRaisesRegex(ValueError, 'bad.aab'):
                release.check_mapping_id(resource, [folder / 'ok.aab', folder / 'bad.aab'])


class FakeGitHub:
    """Routes release.http calls; records them; 404s anything unknown."""
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def __call__(self, method, url, body=None, headers=None, out=None):
        self.calls.append((method, url, body))
        key = (method, url.split('?')[0] if method != 'GET' or 'releases?' not in url else url)
        if key not in self.routes:
            raise urllib.error.HTTPError(url, 404, 'Not Found', {}, None)
        value = self.routes[key]
        if isinstance(value, Exception):
            raise value
        if out is not None:
            out.write(value)
            return None
        return value(body) if callable(value) else value


API = 'https://api.github.com/repos/' + REPO
DRAFT = {'id': 77, 'tag_name': 'v0.1.1', 'draft': True, 'assets': []}


class GitHubRelease(unittest.TestCase):
    def fake(self, routes):
        fake = FakeGitHub(routes)
        patcher = patch.object(release, 'http', fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        return fake

    def temp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        return Path(temp.name)

    def notes(self):
        path = self.temp() / 'notes.txt'
        path.write_text('Fixed hunts.')
        return path

    def test_draft_is_created_once_and_its_id_returned(self):
        fake = self.fake({('GET', API + '/releases?per_page=100&page=1'): [],
                          ('POST', API + '/releases'): lambda body: dict(DRAFT)})
        self.assertEqual(release.draft(REPO, 'v0.1.1', 'a' * 40, self.notes()), {'release_id': '77'})
        created = json.loads(fake.calls[-1][2])
        self.assertEqual((created['tag_name'], created['target_commitish'], created['draft']), ('v0.1.1', 'a' * 40, True))
        self.assertEqual(created['body'], 'Fixed hunts.')

    def test_existing_draft_is_reused_and_published_release_refused(self):
        fake = self.fake({('GET', API + '/releases?per_page=100&page=1'): [DRAFT, dict(DRAFT, id=5, tag_name='v0.1.0')],
                          ('GET', API + '/releases?per_page=100&page=2'): [],
                          ('PATCH', API + '/releases/77'): lambda body: DRAFT})
        self.assertEqual(release.draft(REPO, 'v0.1.1', 'a' * 40, self.notes())['release_id'], '77')
        self.assertEqual(fake.calls[-1][0], 'PATCH')
        self.fake({('GET', API + '/releases/tags/v0.1.1'): dict(DRAFT, draft=False)})
        with self.assertRaisesRegex(ValueError, 'already published'):
            release.draft(REPO, 'v0.1.1', 'a' * 40, self.notes())

    def test_lookup_errors_other_than_404_fail_instead_of_meaning_no_draft(self):
        boom = urllib.error.HTTPError('u', 502, 'Bad Gateway', {}, None)
        fake = self.fake({('GET', API + '/releases/tags/v0.1.1'): boom})
        with self.assertRaises(urllib.error.HTTPError):
            release.draft(REPO, 'v0.1.1', 'a' * 40, self.notes())
        self.assertNotIn('POST', [c[0] for c in fake.calls])
        self.fake({('GET', API + '/releases/77'): boom})
        with self.assertRaises(urllib.error.HTTPError):
            release.upload(REPO, '77', 'v0.1.1', [])

    def test_upload_replaces_same_named_asset_on_the_draft_and_reports_sha256(self):
        folder = self.temp()
        aab = folder / 'TreasureHunch-0.1.1-2.aab'
        aab.write_bytes(b'aab-bytes')
        fake = self.fake({('GET', API + '/releases/77'): dict(DRAFT, assets=[{'id': 9, 'name': aab.name}]),
                          ('DELETE', API + '/releases/assets/9'): None,
                          ('POST', f'https://uploads.github.com/repos/{REPO}/releases/77/assets'): {'id': 10}})
        sums = release.upload(REPO, '77', 'v0.1.1', [aab])
        self.assertEqual(sums, {'checksums': f'{aab.name}:{hashlib.sha256(b"aab-bytes").hexdigest()}'})
        method, url, _ = fake.calls[-1]
        self.assertEqual((method, url), ('POST', f'https://uploads.github.com/repos/{REPO}/releases/77/assets?name={aab.name}'))
        self.assertIn(('DELETE', API + '/releases/assets/9', None), fake.calls)

    def test_upload_refuses_a_published_release_or_another_tag(self):
        for found in (dict(DRAFT, draft=False), dict(DRAFT, tag_name='v9.9.9')):
            self.fake({('GET', API + '/releases/77'): found})
            with self.subTest(found=found), self.assertRaises(ValueError):
                release.upload(REPO, '77', 'v0.1.1', [])

    def test_download_verifies_every_sha256_and_the_asset_set(self):
        assets = [{'id': 1, 'name': 'a.aab'}, {'id': 2, 'name': 'a.ipa'}]
        routes = {('GET', API + '/releases/77'): dict(DRAFT, assets=assets, body='Notes.'),
                  ('GET', API + '/releases/assets/1'): b'aab', ('GET', API + '/releases/assets/2'): b'ipa'}
        sha = lambda data: hashlib.sha256(data).hexdigest()
        good = f'a.aab:{sha(b"aab")} a.ipa:{sha(b"ipa")}'
        self.fake(routes)
        dest = self.temp() / 'assets'
        release.download(REPO, '77', 'v0.1.1', dest, good, dest.parent / 'notes.txt')
        self.assertEqual((dest / 'a.ipa').read_bytes(), b'ipa')
        self.assertEqual((dest.parent / 'notes.txt').read_text(), 'Notes.')
        for expected in (f'a.aab:{sha(b"aab")} a.ipa:{sha(b"tampered")}', f'a.aab:{sha(b"aab")}'):
            with self.subTest(expected=expected), self.assertRaises(ValueError):
                release.download(REPO, '77', 'v0.1.1', self.temp() / 'x', expected, self.temp() / 'n.txt')

    def test_publish_patches_the_draft_by_id_only_after_the_tag_exists(self):
        folder = self.temp()
        for name in Assets.NAMES:
            (folder / name).write_bytes(b'asset')
        notes = self.notes()
        routes = {('GET', API + '/releases/77'): DRAFT, ('PATCH', API + '/releases/77'): lambda body: DRAFT}
        fake = self.fake(routes)
        with self.assertRaisesRegex(ValueError, 'tag'):
            release.publish(REPO, '77', 'v0.1.1', folder, notes)
        self.assertNotIn('PATCH', [c[0] for c in fake.calls])
        routes[('GET', API + '/git/ref/tags/v0.1.1')] = {'ref': 'refs/tags/v0.1.1'}
        fake = self.fake(routes)
        release.publish(REPO, '77', 'v0.1.1', folder, notes)
        method, url, body = fake.calls[-1]
        self.assertEqual((method, url), ('PATCH', API + '/releases/77'))
        self.assertEqual({k: json.loads(body)[k] for k in ('draft', 'prerelease', 'tag_name')},
                         {'draft': False, 'prerelease': False, 'tag_name': 'v0.1.1'})



PARK = 'ci-testflight-debug-pr-12'
PARKED = {'id': 88, 'tag_name': PARK, 'draft': True, 'assets': []}


class TestFlightHandOff(unittest.TestCase):
    """amper-debug: ios-build parks the staging IPA; ios-publish (no PR code, holds the ASC key) takes it."""
    fake, temp = GitHubRelease.fake, GitHubRelease.temp

    def ipa(self, data=b'ipa-bytes'):
        path = self.temp() / 'TreasureHunch-0.1.0-5-staging.ipa'
        path.write_bytes(data)
        return path

    def test_park_replaces_this_prs_draft_and_reports_id_and_digest(self):
        ipa = self.ipa()
        stale = dict(PARKED, id=60)
        fake = self.fake({('GET', API + '/releases?per_page=100&page=1'): [stale, DRAFT],
                          ('DELETE', API + '/releases/60'): None,
                          ('POST', API + '/releases'): lambda body: dict(PARKED),
                          ('GET', API + '/releases/88'): PARKED,
                          ('POST', f'https://uploads.github.com/repos/{REPO}/releases/88/assets'): {'id': 1}})
        self.assertEqual(release.park(REPO, PARK, 'a' * 40, ipa),
                         {'release_id': '88', 'sha256': hashlib.sha256(b'ipa-bytes').hexdigest()})
        self.assertIn(('DELETE', API + '/releases/60', None), fake.calls)
        self.assertNotIn(('DELETE', API + '/releases/77', None), fake.calls)  # the version draft is untouched
        created = json.loads(next(c[2] for c in fake.calls if c[:2] == ('POST', API + '/releases')))
        self.assertEqual((created['tag_name'], created['draft'], created['prerelease']), (PARK, True, False))

    def test_park_tags_never_collide_with_release_or_debug_prerelease_tags(self):
        for tag in ('v0.1.0', 'debug-pr-12', 'ci-testflight-v0.1.0', 'ci-testflight-debug-pr-0'):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.park(REPO, tag, 'a' * 40, self.ipa())
        with self.assertRaises(ValueError):
            release.upload(REPO, '88', PARK, [])  # the release path keeps its vX.Y.Z-only check

    def test_unpark_requires_one_ipa_with_the_reported_digest(self):
        good = hashlib.sha256(b'ipa-bytes').hexdigest()
        routes = {('GET', API + '/releases/88'): dict(PARKED, assets=[{'id': 3, 'name': 'a.ipa'}]),
                  ('GET', API + '/releases/assets/3'): b'ipa-bytes'}
        self.fake(routes)
        dest = self.temp()
        self.assertEqual(release.unpark(REPO, '88', PARK, good, dest), {'ipa': str(dest / 'a.ipa')})
        self.assertEqual((dest / 'a.ipa').read_bytes(), b'ipa-bytes')
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            release.unpark(REPO, '88', PARK, hashlib.sha256(b'other').hexdigest(), self.temp())
        for assets in ([], [{'id': 3, 'name': 'a.ipa'}, {'id': 4, 'name': 'b.ipa'}], [{'id': 3, 'name': 'a.apk'}]):
            self.fake({**routes, ('GET', API + '/releases/88'): dict(PARKED, assets=assets)})
            with self.subTest(assets=assets), self.assertRaisesRegex(ValueError, 'exactly one IPA'):
                release.unpark(REPO, '88', PARK, good, self.temp())

    def test_discard_deletes_only_a_draft_under_the_expected_parking_tag(self):
        fake = self.fake({('GET', API + '/releases/88'): PARKED, ('DELETE', API + '/releases/88'): None})
        release.discard(REPO, '88', PARK)
        self.assertEqual(fake.calls[-1][:2], ('DELETE', API + '/releases/88'))
        # The ID comes from a job that ran PR code: a published release or another tag is never deleted.
        for found in (dict(PARKED, draft=False), dict(PARKED, tag_name='ci-testflight-debug-pr-13'), dict(DRAFT, id=88)):
            fake = self.fake({('GET', API + '/releases/88'): found, ('DELETE', API + '/releases/88'): None})
            with self.subTest(found=found), self.assertRaises(ValueError):
                release.discard(REPO, '88', PARK)
            self.assertNotIn('DELETE', [c[0] for c in fake.calls])
        with self.assertRaises(ValueError):
            release.discard(REPO, '88', 'v0.1.1')


class CacheSecrets(unittest.TestCase):
    """amper-release scans each dependency cache for build secrets before it becomes a public GHCR bundle."""
    TOKEN = '0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0'
    PASSWORD = 'prod-store-pass'

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)
        self.secrets = self.dir / '.staging.env'
        self.secrets.write_text(f"KEY_ALIAS='debug'\nAPPCHECK_DEBUG_TOKEN='{self.TOKEN}'")
        self.cache = self.dir / 'cache'
        (self.cache / 'deep/er').mkdir(parents=True)
        (self.cache / 'deep/er/lib.jar').write_bytes(b'PK\x03\x04 third-party bytes')
        # Toolchain caches hold directory links and dangling links; grep must not treat them as errors.
        (self.cache / 'linked').symlink_to('deep', target_is_directory=True)
        (self.cache / 'dangling').symlink_to('nowhere')
        env = patch.dict(os.environ, {'KEYSTORE_PASSWORD': self.PASSWORD})
        env.start()
        self.addCleanup(env.stop)

    def test_clean_cache_passes_missing_paths_are_skipped_and_the_secrets_file_is_deleted(self):
        release.check_cache(self.secrets, [self.cache, self.dir / 'absent'])
        self.assertFalse(self.secrets.exists())

    def test_either_secret_anywhere_in_the_tree_fails_without_printing_it(self):
        for secret in (self.TOKEN, self.PASSWORD):
            with self.subTest(secret=secret):
                self.setUp()
                (self.cache / 'deep/er/leak.bin').write_bytes(b'\x00\x01' + secret.encode() + b'\x00')
                with self.assertRaisesRegex(ValueError, 'inside the cache paths') as caught:
                    release.check_cache(self.secrets, [self.dir / 'absent', self.cache])
                self.assertNotIn(secret, str(caught.exception))
                self.assertFalse(self.secrets.exists())

    def test_missing_or_short_secrets_fail_instead_of_passing_vacuously(self):
        self.secrets.write_text("APPCHECK_DEBUG_TOKEN='abc'")
        with self.assertRaisesRegex(ValueError, 'APPCHECK_DEBUG_TOKEN'):
            release.check_cache(self.secrets, [self.cache])
        for password in ('', '12345'):
            self.secrets.write_text(f"APPCHECK_DEBUG_TOKEN='{self.TOKEN}'")
            with self.subTest(password=password), patch.dict(os.environ, {'KEYSTORE_PASSWORD': password}), \
                    self.assertRaisesRegex(ValueError, 'KEYSTORE_PASSWORD'):
                release.check_cache(self.secrets, [self.cache])

@needs_yq
class VersionCommit(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name)
        self.remote, self.source = self.base / 'remote', self.base / 'source'
        git(self.base, 'init', '--bare', str(self.remote))
        shutil.copytree(HERE / 'fixtures', self.source, ignore=shutil.ignore_patterns('*.log'))
        for args in (('init', '-b', 'main'), ('config', 'user.name', 'Test'), ('config', 'user.email', 't@example.com'),
                     ('remote', 'add', 'origin', str(self.remote)), ('add', '.'), ('commit', '-m', 'base'),
                     ('push', 'origin', 'main')):
            git(self.source, *args)
        self.sha = git(self.source, 'rev-parse', 'HEAD')
        release.amper_control.main(['set-version', '--source', str(self.source), '--version', '0.1.1', '--build', '2'])

    def push(self):
        with in_directory(self.source):
            return release.push_version(self.sha, '0.1.1', '2')

    def test_module_yaml_and_pbxproj_bump_is_accepted_and_atomic(self):
        result = self.push()
        self.assertEqual(git(self.remote, 'rev-parse', 'main'), result['release_sha'])
        self.assertEqual(git(self.remote, 'rev-parse', 'v0.1.1^{}'), result['release_sha'])
        self.assertEqual(git(self.remote, 'log', '-1', '--pretty=%s', 'main'), 'chore(release): v0.1.1 (2) [skip ci]')
        self.assertEqual(set(git(self.remote, 'diff', '--name-only', self.sha, 'main').splitlines()),
                         {'androidApp/module.yaml', PBX})

    def test_source_file_change_is_rejected(self):
        path = self.source / 'shared/src/x.kt'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('val x = 1\n')
        git(self.source, 'add', '-N', 'shared/src/x.kt')
        with self.assertRaisesRegex(ValueError, 'version'):
            self.push()
        self.assertEqual(git(self.remote, 'rev-parse', 'main'), self.sha)
        self.assertEqual(git(self.remote, 'tag'), '')

    def test_non_version_edit_in_an_allowed_file_is_rejected(self):
        # set-variant also edits these files (IDs, signing); none of that may reach main.
        pbx = self.source / PBX
        pbx.write_text(pbx.read_text().replace('PRODUCT_BUNDLE_IDENTIFIER = com.lascade.hunch.debug',
                                               'PRODUCT_BUNDLE_IDENTIFIER = com.lascade.hunch', 1))
        with self.assertRaisesRegex(ValueError, 'version'):
            self.push()

    def test_retry_resumes_only_the_exact_pushed_bump(self):
        first = self.push()
        git(self.source, 'checkout', '--detach', self.sha)
        release.amper_control.main(['set-version', '--source', str(self.source), '--version', '0.1.1', '--build', '2'])
        self.assertEqual(self.push(), first)

    def test_concurrent_main_commit_rejects_push_and_tag(self):
        other = self.base / 'other'
        git(self.base, 'clone', '--branch', 'main', str(self.remote), str(other))
        git(other, 'config', 'user.name', 'O')
        git(other, 'config', 'user.email', 'o@example.com')
        (other / 'new').write_text('work')
        git(other, 'add', '.')
        git(other, 'commit', '-m', 'new work')
        git(other, 'push', 'origin', 'main')
        with self.assertRaisesRegex(ValueError, 'changed'):
            self.push()
        self.assertEqual(git(self.remote, 'tag'), '')


if __name__ == '__main__':
    unittest.main()
