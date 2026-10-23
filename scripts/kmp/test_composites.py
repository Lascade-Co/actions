"""Offline behavioral checks for reusable dependency, signing and notify steps."""
import base64
import contextlib
import datetime
import io
import json
import os
from pathlib import Path
import plistlib
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import apple_profiles
import bundle_cache
import telegram_notify


class BundleTests(unittest.TestCase):
    def test_rejects_destructive_or_overlapping_cache_selection(self):
        for selected in ['/', str(Path.home()), str(Path.cwd()), '/tmp/cache\n/tmp/cache/nested']:
            with patch.dict(os.environ, {'CACHE_PATHS':selected}):
                with self.assertRaises(ValueError):
                    bundle_cache.paths_from_env()

    def test_roundtrip_two_paths_relocates_to_new_home(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / 'wrapper'
            first.mkdir()
            (first / 'gradle.zip').write_text('dependency')
            archive = root / 'bundle.tar'
            bundle_cache.pack([first, root / 'missing'], archive)
            dest = root / 'new-wrapper'
            dest.mkdir()
            (dest / 'stale').write_text('stale')
            bundle_cache.unpack(archive, [dest, root / 'new-missing'])
            self.assertEqual((dest / 'gradle.zip').read_text(), 'dependency')
            self.assertFalse((dest / 'stale').exists())
            self.assertFalse((root / 'new-missing').exists())

    def test_reject_archive_traversal_before_any_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ['../escape', '/escape', 'path-0/../../escape', 'path-3/file']:
                archive = root / 'bundle.tar'
                with tarfile.open(archive, 'w') as stream:
                    stream.addfile(tarfile.TarInfo(name))
                with self.assertRaises(ValueError):
                    bundle_cache.unpack(archive, [root / 'cache'])
                self.assertFalse((root / 'cache').exists())

    def test_native_parent_relative_symlinks_and_chains_roundtrip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / 'native'
            plugin = cache / 'dependencies/toolchain/lib/bfd-plugins'
            target = cache / 'dependencies/toolchain/libexec/gcc/liblto_plugin.so'
            plugin.mkdir(parents=True)
            target.parent.mkdir(parents=True)
            target.write_bytes(b'native plugin')
            link = plugin / 'liblto_plugin.so'
            link.symlink_to('../../libexec/gcc/liblto_plugin.so')
            (plugin / 'alias.so').symlink_to('liblto_plugin.so')
            archive = root / 'bundle.tar'
            bundle_cache.pack([cache], archive)
            restored = root / 'restored'
            bundle_cache.unpack(archive, [restored])
            restored_plugin = restored / 'dependencies/toolchain/lib/bfd-plugins'
            self.assertEqual((restored_plugin / 'liblto_plugin.so').read_bytes(), b'native plugin')
            self.assertEqual((restored_plugin / 'alias.so').read_bytes(), b'native plugin')
            self.assertTrue((restored_plugin / 'liblto_plugin.so').is_symlink())
            self.assertEqual(os.readlink(restored_plugin / 'liblto_plugin.so'), '../../libexec/gcc/liblto_plugin.so')

    def test_reject_cross_root_absolute_cyclic_and_ancestor_symlink_escapes(self):
        attacks = [
            [('path-0/link', '../path-1/target')],
            [('path-0/link', '/tmp/outside')],
            [('path-0/link', 'other'), ('path-0/other', 'link')],
            [('path-0/real/up', '..'), ('path-0/link', 'real/up/../../outside')],
            [('path-0', '.')],
        ]
        for attack in attacks:
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                archive = root / 'bundle.tar'
                with tarfile.open(archive, 'w') as stream:
                    for name, target in attack:
                        member = tarfile.TarInfo(name)
                        member.type = tarfile.SYMTYPE
                        member.linkname = target
                        stream.addfile(member)
                    # Writes through an alias must also be rejected before extraction.
                    member = tarfile.TarInfo('path-0/link/payload')
                    stream.addfile(member)
                with self.assertRaises(ValueError):
                    bundle_cache.unpack(archive, [root / 'cache', root / 'other-cache'])
                self.assertFalse((root / 'cache').exists())
                self.assertFalse((root / 'other-cache').exists())

    def test_hard_links_cannot_cross_selected_roots(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / 'bundle.tar'
            with tarfile.open(archive, 'w') as stream:
                member = tarfile.TarInfo('path-1/target')
                stream.addfile(member)
                member = tarfile.TarInfo('path-0/hard-link')
                member.type = tarfile.LNKTYPE
                member.linkname = 'path-1/target'
                stream.addfile(member)
            with self.assertRaises(ValueError):
                bundle_cache.unpack(archive, [root / 'cache', root / 'other-cache'])

    def test_reject_escape_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / 'bundle.tar'
            with tarfile.open(archive, 'w') as stream:
                member = tarfile.TarInfo('path-0/link')
                member.type = tarfile.SYMTYPE
                member.linkname = '../../outside'
                stream.addfile(member)
            with self.assertRaises(ValueError):
                bundle_cache.unpack(archive, [root / 'cache'])

    def test_restore_replaces_existing_destination_symlink_without_following(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'
            source.mkdir()
            (source / 'fresh').write_text('fresh')
            external = root / 'external'
            external.mkdir()
            (external / 'original').write_text('preserved')
            destination = root / 'destination'
            destination.symlink_to(external)
            bundle_cache.pack([source], root / 'bundle.tar')
            bundle_cache.unpack(root / 'bundle.tar', [destination])
            self.assertFalse(destination.is_symlink())
            self.assertEqual((external / 'original').read_text(), 'preserved')
            self.assertFalse((external / 'fresh').exists())

    def test_exact_miss_falls_back_and_reports_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / 'dependency'
            cache.mkdir()
            (cache / 'cached').write_text('hit')
            source_tar = root / 'cached.tar'
            bundle_cache.pack([cache], source_tar)
            import shutil
            shutil.rmtree(cache)
            def oras(*args, **kwargs):
                if args[0] == 'pull':
                    if args[1].endswith(':exact'):
                        raise bundle_cache.subprocess.CalledProcessError(1, ['oras'])
                    shutil.copyfile(source_tar, Path(args[3]) / 'cache.tar')
            env = {'CACHE_NAME':'example-Linux', 'CACHE_KEY':'exact', 'CACHE_FALLBACK_KEY':'latest',
                   'CACHE_PATHS':str(cache), 'GITHUB_REPOSITORY':'Lascade-Co/actions',
                   'GITHUB_ACTOR':'ci', 'GH_TOKEN':'not-a-real-token', 'GITHUB_OUTPUT':str(root/'output')}
            with patch.dict(os.environ, env), patch('sys.argv', ['cache', 'restore']), patch.object(bundle_cache, 'run_oras', side_effect=oras):
                bundle_cache.main()
            self.assertIn('hit=false', (root/'output').read_text())
            self.assertIn('restored-key=latest', (root/'output').read_text())
            self.assertEqual((cache/'cached').read_text(), 'hit')


class ProfileTests(unittest.TestCase):
    def profile(self, **kwargs):
        return plistlib.dumps({'UUID':'A1-123', 'Name':'TA Staging App',
          'ExpirationDate':datetime.datetime(2099,1,1),
          'Entitlements':{'application-identifier':'TEAM.com.example.debug'}, **kwargs})

    def test_only_unexpired_explicit_app_store_profiles(self):
        self.assertEqual(apple_profiles.profile_metadata(self.profile()), ('A1-123','TA Staging App'))
        for values in [dict(ProvisionedDevices=['device']), dict(ProvisionsAllDevices=True),
                       dict(ExpirationDate=datetime.datetime(2000,1,1)), dict(UUID='../escape'),
                       dict(Entitlements={'application-identifier':'TEAM.*'}),
                       dict(Entitlements={'application-identifier':'TEAM.app','get-task-allow':True})]:
            with self.assertRaises(ValueError):
                apple_profiles.profile_metadata(self.profile(**values))

    def test_install_then_cleanup_restores_existing_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            home = root/'home'
            profile_dir = home/'Library/Developer/Xcode/UserData/Provisioning Profiles'
            profile_dir.mkdir(parents=True)
            existing = profile_dir/'A1-123.mobileprovision'
            existing.write_text('original')
            env = {'RUNNER_TEMP':str(root), 'PROFILE_VARIABLES':'APP_PROFILE',
                   'APP_PROFILE':base64.b64encode(b'profile bytes').decode()}
            with patch.dict(os.environ, env), patch.object(apple_profiles.Path, 'home', return_value=home), patch.object(apple_profiles.subprocess, 'run') as security:
                security.return_value.stdout = self.profile()
                apple_profiles.install()
                self.assertEqual(existing.read_bytes(), b'profile bytes')
                apple_profiles.cleanup()
                self.assertEqual(existing.read_text(), 'original')
                self.assertFalse(apple_profiles.state_path().exists())

    def test_partial_failure_rolls_back_profiles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {'RUNNER_TEMP':str(root), 'PROFILE_VARIABLES':'APP_PROFILE MISSING_PROFILE',
                   'APP_PROFILE':base64.b64encode(b'profile bytes').decode()}
            with patch.dict(os.environ, env), patch.object(apple_profiles.Path, 'home', return_value=root/'home'), patch.object(apple_profiles.subprocess, 'run') as security:
                security.return_value.stdout = self.profile()
                with self.assertRaises(ValueError):
                    apple_profiles.install()
                self.assertFalse(apple_profiles.state_path().exists())
                self.assertEqual(list((root/'home').rglob('*.mobileprovision')), [])


class TelegramTests(unittest.TestCase):
    def test_payload_preserves_plain_text_and_fails_on_api_rejection(self):
        def opener(request, timeout):
            payload = json.loads(request.data)
            self.assertEqual(payload['text'], '<literal> & [text]')
            self.assertNotIn('parse_mode', payload)
            return contextlib.closing(io.BytesIO(b'{"ok": false,"error_code":400}'))
        with self.assertRaisesRegex(RuntimeError, '400'):
            telegram_notify.send('private-token', '-123', '<literal> & [text]', opener)

    def test_transport_error_never_discloses_token(self):
        def opener(request, timeout):
            raise telegram_notify.urllib.error.URLError(request.full_url)
        with self.assertRaises(RuntimeError) as error:
            telegram_notify.send('private-token', '-123', 'hello', opener)
        self.assertNotIn('private-token', str(error.exception))


if __name__ == '__main__':
    unittest.main()
