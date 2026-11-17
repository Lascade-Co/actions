import base64
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).parent
FIX = HERE / 'fixtures'
spec = importlib.util.spec_from_file_location('amper_control', HERE / 'control.py')
control = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control)

KT = 'shared/src/com/lascade/hunch/platform/BuildSecrets.kt'
PBX = 'iosApp/module.xcodeproj/project.pbxproj'
ENT = 'iosApp/src/iosApp.entitlements'


def run(*argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        control.main([str(a) for a in argv])
    return dict(line.split('=', 1) for line in out.getvalue().splitlines() if '=' in line)


needs_yq = unittest.skipUnless(shutil.which('yq'), 'yq not installed')


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.src = Path(self.tmp) / 'src'
        shutil.copytree(FIX, self.src, ignore=shutil.ignore_patterns('*.log'))

    def read(self, rel):
        return (self.src / rel).read_text()


@needs_yq
class MetadataTests(Base):
    def test_reads_version_build_and_toolchain(self):
        out = run('metadata', '--source', self.src)
        self.assertEqual(out, {'version': '0.1.0', 'build_number': '1', 'toolchain': '0.14.0'})

    def test_suffix_becomes_build_number(self):
        self.assertEqual(run('metadata', '--source', self.src, '--suffix', 77)['build_number'], '77')


@needs_yq
class SetVersionTests(Base):
    def test_patches_module_yaml_and_every_pbxproj_configuration(self):
        run('set-version', '--source', self.src, '--version', '1.2.3', '--build', 45)
        yml = self.read('androidApp/module.yaml')
        self.assertIn('versionName: "1.2.3"', yml)
        self.assertIn('versionCode: 45', yml)
        self.assertIn('# Signs the release AAB', yml)  # comments survive
        pbx = self.read(PBX)
        self.assertEqual(re.findall(r'MARKETING_VERSION = ([^;]+);', pbx), ['1.2.3', '1.2.3'])
        self.assertEqual(re.findall(r'CURRENT_PROJECT_VERSION = ([^;]+);', pbx), ['45', '45'])

    def test_rejects_unsafe_version(self):
        with self.assertRaises(SystemExit):
            run('set-version', '--source', self.src, '--version', '1.0"; rm', '--build', 1)


@needs_yq
class SetVariantTests(Base):
    def test_debug_writes_token_from_env_and_debug_ids(self):
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'debug')
        self.assertIn('APPCHECK_DEBUG_TOKEN = "tok-123"', self.read(KT))
        self.assertIn('applicationId: com.lascade.hunch.debug', self.read('androidApp/module.yaml'))
        self.assertEqual(set(re.findall(r'PRODUCT_BUNDLE_IDENTIFIER = ([^;]+);', self.read(PBX))),
                         {'com.lascade.hunch.debug'})

    def test_release_removes_token_and_swaps_ids_plist_entitlement(self):
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'release')
        kt = self.read(KT)
        self.assertNotIn('tok-123', kt)
        self.assertTrue('APPCHECK' not in kt or '""' in kt)
        self.assertIn('package com.lascade.hunch.platform', kt)
        self.assertRegex(self.read('androidApp/module.yaml'), r'applicationId: com\.lascade\.hunch\s*\n')
        self.assertEqual(set(re.findall(r'PRODUCT_BUNDLE_IDENTIFIER = ([^;]+);', self.read(PBX))),
                         {'com.lascade.hunch'})
        self.assertEqual(self.read('iosApp/src/GoogleService-Info.plist'), 'release-plist\n')
        ent = self.read(ENT)
        self.assertIn('<string>production</string>', ent)
        self.assertNotIn('development', ent)

    def test_release_signing_goes_on_target_build_settings(self):
        run('set-variant', '--source', self.src, '--variant', 'release', '--team', 'ABCDE12345', '--profile', 'Hunch AppStore')
        pbx = self.read(PBX)
        self.assertEqual(pbx.count('CODE_SIGN_STYLE = Manual;'), 2)
        self.assertEqual(pbx.count('DEVELOPMENT_TEAM = ABCDE12345;'), 2)
        self.assertEqual(pbx.count('PROVISIONING_PROFILE_SPECIFIER = "Hunch AppStore";'), 2)
        self.assertEqual(pbx.count('CODE_SIGN_IDENTITY = "Apple Distribution";'), 2)
        self.assertNotIn('Automatic', pbx)

    def test_debug_signing_goes_on_target_build_settings(self):
        # The debug variant is signed for TestFlight, so it takes the same manual signing as release.
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'debug', '--team', 'ABCDE12345', '--profile', 'Hunch Staging')
        pbx = self.read(PBX)
        self.assertEqual(pbx.count('CODE_SIGN_STYLE = Manual;'), 2)
        self.assertEqual(pbx.count('DEVELOPMENT_TEAM = ABCDE12345;'), 2)
        self.assertEqual(pbx.count('PROVISIONING_PROFILE_SPECIFIER = "Hunch Staging";'), 2)
        self.assertIn('APPCHECK_DEBUG_TOKEN = "tok-123"', self.read(KT))

    def test_debug_requires_token(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('APPCHECK_DEBUG_TOKEN', None)
            with self.assertRaises(SystemExit):
                run('set-variant', '--source', self.src, '--variant', 'debug')

    def test_missing_entitlements_is_skipped_with_warning(self):
        (self.src / ENT).unlink()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            run('set-variant', '--source', self.src, '--variant', 'release')
        self.assertIn('warning', err.getvalue().lower())


@needs_yq
class AppTargetOnlyTests(Base):
    def setUp(self):
        super().setUp()
        shutil.copy(FIX / 'pbxproj-multitarget', self.src / PBX)

    def test_ids_versions_and_signing_leave_test_target_untouched(self):
        run('set-version', '--source', self.src, '--version', '1.2.3', '--build', 45)
        run('set-variant', '--source', self.src, '--variant', 'release', '--team', 'ABCDE12345', '--profile', 'P')
        pbx = self.read(PBX)
        self.assertEqual(re.findall(r'PRODUCT_BUNDLE_IDENTIFIER = ([^;]+);', pbx).count('com.lascade.hunch'), 2)
        self.assertEqual(re.findall(r'PRODUCT_BUNDLE_IDENTIFIER = ([^;]+);', pbx).count('com.lascade.hunch.tests'), 2)
        self.assertEqual(re.findall(r'MARKETING_VERSION = ([^;]+);', pbx).count('9.9'), 2)
        self.assertEqual(re.findall(r'CURRENT_PROJECT_VERSION = ([^;]+);', pbx).count('9'), 2)
        self.assertEqual(pbx.count('CODE_SIGN_STYLE = Manual;'), 2)
        self.assertEqual(pbx.count('CODE_SIGN_STYLE = Automatic;'), 2)  # test target only
        self.assertEqual(pbx.count('PROVISIONING_PROFILE_SPECIFIER'), 2)

    def test_missing_signing_keys_are_added(self):
        pbx = self.src / PBX
        text = re.sub(r'^\t+(CODE_SIGN_STYLE|CODE_SIGN_IDENTITY|DEVELOPMENT_TEAM) = [^;]*;\n', '', pbx.read_text(), flags=re.M)
        pbx.write_text(text)
        run('set-variant', '--source', self.src, '--variant', 'release', '--team', 'ABCDE12345', '--profile', 'P')
        out = self.read(PBX)
        self.assertEqual(out.count('CODE_SIGN_STYLE = Manual;'), 2)
        self.assertEqual(out.count('DEVELOPMENT_TEAM = ABCDE12345;'), 2)
        self.assertEqual(out.count('CODE_SIGN_IDENTITY = "Apple Distribution";'), 2)

    def test_no_application_target_fails(self):
        pbx = self.src / PBX
        pbx.write_text(pbx.read_text().replace('product-type.application', 'product-type.tool'))
        with self.assertRaises(SystemExit):
            run('set-version', '--source', self.src, '--version', '1.0.0', '--build', 1)


class AndroidInputsTests(Base):
    def test_writes_keystore_and_properties(self):
        env = {'KEYSTORE_BASE64': base64.b64encode(b'jks-bytes').decode(), 'KEYSTORE_PASSWORD': 'sp',
               'KEY_ALIAS': 'al', 'KEY_PASSWORD': 'kp'}
        with patch.dict(os.environ, env):
            run('android-inputs', '--source', self.src)
        self.assertEqual((self.src / 'androidApp/ci.jks').read_bytes(), b'jks-bytes')
        for name in ('ci.jks', 'keystore.properties'):
            self.assertEqual((self.src / 'androidApp' / name).stat().st_mode & 0o777, 0o600)
        props = self.read('androidApp/keystore.properties')
        for line in ['storeFile=ci.jks', 'storePassword=sp', 'keyAlias=al', 'keyPassword=kp']:
            self.assertIn(line, props.splitlines())


class FindArtifactTests(Base):
    def put(self, rel):
        p = self.src / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('x')
        return p

    def test_finds_single_aab_and_mapping(self):
        aab = self.put('build/tasks/_androidApp_bundleAndroid/gradle-project-release.aab')
        mp = self.put('build/tasks/_androidApp_bundleAndroid/gradle-project/build/outputs/mapping/release/mapping.txt')
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'aab')['path'], str(aab))
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'mapping')['path'], str(mp))

    def test_ignores_gradle_intermediate_copy(self):
        # The toolchain leaves a second copy of the APK inside its generated Gradle project.
        apk = self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project-release.apk')
        self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project/build/_androidApp/outputs/apk/release/gradle-project-release.apk')
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'apk')['path'], str(apk))

    def test_prefers_release_variant_apk(self):
        self.put('build/tasks/_androidApp_buildAndroidDebug/gradle-project-debug.apk')
        apk = self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project-release.apk')
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'apk')['path'], str(apk))

    def test_fails_on_zero_or_many(self):
        with self.assertRaises(SystemExit):
            run('find-artifact', '--source', self.src, '--kind', 'apk')
        self.put('build/tasks/a/app-debug.apk')
        self.put('build/tasks/b/app-debug.apk')
        with self.assertRaises(SystemExit):
            run('find-artifact', '--source', self.src, '--kind', 'apk')


class GcGuardTests(Base):
    def guard(self, log, gc):
        d = Path(self.tmp) / 'gc'
        d.mkdir(exist_ok=True)
        shutil.copy(FIX / gc, d / 'gc-1.log')
        return run('kn-gc-guard', '--log', FIX / log, '--gc-dir', d, '--max-full-gc', 20)

    def test_fails_with_21_full_gcs(self):
        with self.assertRaises(SystemExit):
            self.guard('build-ok.log', 'gc-fail.log')

    def test_passes_with_3_full_gcs(self):
        self.assertEqual(self.guard('build-ok.log', 'gc-pass.log')['max_full_gc'], '3')

    def test_fails_without_picked_up_options(self):
        with self.assertRaises(SystemExit):
            self.guard('build-nopickup.log', 'gc-pass.log')


class ExportOptionsTests(Base):
    def test_writes_plist(self):
        import plistlib
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp)
        run('export-options', '--team', 'T1', '--profile', 'Prof', '--bundle-id', 'com.x.y')
        files = list(Path(self.tmp).glob('ExportOptions*.plist'))
        self.assertEqual(len(files), 1)
        d = plistlib.loads(files[0].read_bytes())
        self.assertEqual(d['method'], 'app-store-connect')
        self.assertEqual(d['signingStyle'], 'manual')
        self.assertEqual(d['teamID'], 'T1')
        self.assertEqual(d['provisioningProfiles'], {'com.x.y': 'Prof'})
        self.assertIs(d['stripSwiftSymbols'], True)
        self.assertIs(d['compileBitcode'], False)


if __name__ == '__main__':
    unittest.main()
