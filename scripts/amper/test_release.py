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
        # PR (amper-debug) builds publish the plain names; a release must never restore those.
        names = [step['with']['name'] for job in self.jobs.values() for step in job['steps']
                 if step.get('uses') == './control/actions/gh-bundle-cache']
        self.assertTrue(names)
        for name in names:
            self.assertRegex(name, r"-release'\) \}\}$")

    def test_firebase_cli_installs_before_any_prod_secret(self):
        steps = [step.get('name', '') for step in self.jobs['android']['steps']]
        install = steps.index('Install the Firebase CLI for Crashlytics mapping files')
        self.assertLess(install, steps.index('Read /Build from Infisical prod'))
        self.assertIn('--ignore-scripts', self.jobs['android']['steps'][install]['run'])


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
