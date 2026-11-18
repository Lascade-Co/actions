import base64
import contextlib
import importlib.util
import io
import json
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

    def test_changes_only_the_two_version_lines(self):
        # The release job commits this file to main, so formatting, blank lines and comments must survive.
        before = self.read('androidApp/module.yaml').splitlines()
        run('set-version', '--source', self.src, '--version', '1.2.3', '--build', 45)
        after = self.read('androidApp/module.yaml').splitlines()
        self.assertEqual(len(before), len(after))
        changed = [(b, a) for b, a in zip(before, after) if b != a]
        self.assertEqual(changed, [('    versionCode: 1', '    versionCode: 45'),
                                   ('    versionName: "0.1.0"', '    versionName: "1.2.3"')])

    def test_fails_without_exactly_one_version_line(self):
        yml = self.src / 'androidApp/module.yaml'
        yml.write_text(yml.read_text().replace('    versionCode: 1\n', ''))
        with self.assertRaises(SystemExit):
            run('set-version', '--source', self.src, '--version', '1.2.3', '--build', 45)


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

    def _with_aps(self):
        import plistlib
        path = self.src / ENT
        data = plistlib.loads(path.read_bytes())
        data['aps-environment'] = 'development'
        path.write_bytes(plistlib.dumps(data))

    def test_signed_debug_uses_production_push_but_keeps_app_attest(self):
        self._with_aps()
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'debug', '--team', 'ABCDE12345', '--profile', 'Hunch Staging')
        import plistlib
        data = plistlib.loads((self.src / ENT).read_bytes())
        self.assertEqual(data['aps-environment'], 'production')
        self.assertEqual(data['com.apple.developer.devicecheck.appattest-environment'], 'development')

    def test_signed_release_uses_production_push_and_app_attest(self):
        self._with_aps()
        run('set-variant', '--source', self.src, '--variant', 'release', '--team', 'ABCDE12345', '--profile', 'Hunch AppStore')
        import plistlib
        data = plistlib.loads((self.src / ENT).read_bytes())
        self.assertEqual(data['aps-environment'], 'production')
        self.assertEqual(data['com.apple.developer.devicecheck.appattest-environment'], 'production')

    def test_unsigned_debug_leaves_entitlements_alone(self):
        self._with_aps()
        before = (self.src / ENT).read_bytes()
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'debug')
        self.assertEqual((self.src / ENT).read_bytes(), before)

    def test_meta_token_and_app_id_written_for_both_platforms(self):
        import plistlib
        env = {'APPCHECK_DEBUG_TOKEN': 'tok-123', 'META_CLIENT_TOKEN': 'abc123def', 'META_APP_ID': '42'}
        with patch.dict(os.environ, env):
            run('set-variant', '--source', self.src, '--variant', 'release')
        res = self.read('androidApp/res/values/meta.xml')
        self.assertIn('<string name="meta_client_token" translatable="false">abc123def</string>', res)
        self.assertIn('<string name="meta_app_id" translatable="false">42</string>', res)
        data = plistlib.loads((self.src / 'iosApp/src/Info.plist').read_bytes())
        self.assertEqual((data['FacebookClientToken'], data['FacebookAppID']), ('abc123def', '42'))

    def test_meta_left_off_without_token(self):
        before = self.read('androidApp/res/values/meta.xml'), self.read('iosApp/src/Info.plist')
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            os.environ.pop('META_CLIENT_TOKEN', None)
            run('set-variant', '--source', self.src, '--variant', 'debug')
        self.assertEqual((self.read('androidApp/res/values/meta.xml'), self.read('iosApp/src/Info.plist')), before)

    def test_meta_rejects_unsafe_token(self):
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123', 'META_CLIENT_TOKEN': 'a</string>'}):
            with self.assertRaises(SystemExit):
                run('set-variant', '--source', self.src, '--variant', 'debug')

    def test_variant_icon_and_files(self):
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'release')
        self.assertEqual(self.read('androidApp/res/drawable/ic_launcher_foreground.xml'), 'release-icon\n')
        pbx = self.read(PBX)
        self.assertEqual(pbx.count('ASSETCATALOG_COMPILER_APPICON_NAME = AppIcon;'), 2)

    def test_debug_variant_keeps_default_files_and_uses_debug_icon(self):
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            run('set-variant', '--source', self.src, '--variant', 'debug')
        self.assertEqual(self.read('androidApp/res/drawable/ic_launcher_foreground.xml'), 'debug-icon\n')
        self.assertEqual(self.read(PBX).count('ASSETCATALOG_COMPILER_APPICON_NAME = AppIconDebug;'), 2)

    def test_variant_files_cannot_escape_source(self):
        import json
        path = self.src / '.ci/amper.json'
        data = json.loads(path.read_text())
        data['variant_files'] = {'release': {'../outside.txt': 'androidApp/variants/release/ic_launcher_foreground.xml'}}
        path.write_text(json.dumps(data))
        with patch.dict(os.environ, {'APPCHECK_DEBUG_TOKEN': 'tok-123'}):
            with self.assertRaises(SystemExit):
                run('set-variant', '--source', self.src, '--variant', 'release')

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

    def test_line_wrapped_keystore_base64_decodes(self):
        # `base64` and Infisical pastes wrap at 76 columns; the profile decode already strips whitespace.
        keystore = bytes(range(256)) * 2
        wrapped = base64.encodebytes(keystore).decode().replace('\n', '\r\n', 3)
        self.assertGreater(wrapped.count('\n'), 5)
        env = {'KEYSTORE_BASE64': ' ' + wrapped, 'KEYSTORE_PASSWORD': 'sp', 'KEY_ALIAS': 'al', 'KEY_PASSWORD': 'kp'}
        with patch.dict(os.environ, env):
            run('android-inputs', '--source', self.src)
        self.assertEqual((self.src / 'androidApp/ci.jks').read_bytes(), keystore)


FAKE_TOOL = '''#!/usr/bin/env python3
import json, os, shutil, sys
from pathlib import Path
log = Path(__file__).with_name('calls.jsonl')
env = {k: os.environ.get(k) for k in ('KEYSTORE_PASSWORD', 'KEY_PASSWORD')}
with log.open('a') as f:
    f.write(json.dumps({'tool': Path(__file__).name, 'argv': sys.argv[1:], 'env': env}) + '\\n')
args = sys.argv[1:]
if Path(__file__).name == 'zipalign':
    shutil.copyfile(args[-2], args[-1])
elif args[0] == 'sign':
    shutil.copyfile(args[-1], args[args.index('--out') + 1])
else:
    print('Signer #1 certificate SHA-1 digest: 474127fe')
'''


class SignApkTests(Base):
    SECRETS = {'KEYSTORE_PASSWORD': 'store-secret-1', 'KEY_ALIAS': 'ci', 'KEY_PASSWORD': 'key-secret-2'}

    def setUp(self):
        super().setUp()
        self.tools = Path(self.tmp) / 'build-tools' / '36.0.0'
        self.tools.mkdir(parents=True)
        for name in ('zipalign', 'apksigner'):
            (self.tools / name).write_text(FAKE_TOOL)
            (self.tools / name).chmod(0o755)
        (self.src / 'androidApp/ci.jks').write_bytes(b'jks')
        self.unsigned = self.src / 'build/tasks/_androidApp_buildAndroidRelease/gradle-project-release-unsigned.apk'
        self.unsigned.parent.mkdir(parents=True)
        self.unsigned.write_bytes(b'PK-apk')

    def calls(self):
        return [json.loads(line) for line in (self.tools / 'calls.jsonl').read_text().splitlines()]

    def test_aligns_signs_and_verifies_with_passwords_only_in_the_environment(self):
        with patch.dict(os.environ, self.SECRETS):
            out = run('sign-apk', '--source', self.src, '--build-tools', self.tools)
        signed = self.src / 'build/signed/gradle-project-release-signed.apk'
        self.assertEqual(out['path'], str(signed))
        self.assertEqual(signed.read_bytes(), b'PK-apk')
        calls = self.calls()
        self.assertEqual([(c['tool'], c['argv'][0]) for c in calls], [('zipalign', '-f'), ('apksigner', 'sign'), ('apksigner', 'verify')])
        self.assertEqual(calls[0]['argv'][:3], ['-f', '-p', '4'])
        sign = calls[1]['argv']
        self.assertEqual(sign[sign.index('--ks') + 1], str(self.src / 'androidApp/ci.jks'))
        self.assertEqual(sign[sign.index('--ks-pass') + 1], 'env:KEYSTORE_PASSWORD')
        self.assertEqual(sign[sign.index('--key-pass') + 1], 'env:KEY_PASSWORD')
        self.assertEqual(sign[sign.index('--ks-key-alias') + 1], 'ci')
        self.assertEqual(calls[1]['env'], {'KEYSTORE_PASSWORD': 'store-secret-1', 'KEY_PASSWORD': 'key-secret-2'})
        self.assertEqual(calls[2]['argv'], ['verify', '--print-certs', str(signed)])
        for call in calls:
            self.assertFalse({'store-secret-1', 'key-secret-2'} & set(call['argv']))
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'apk')['path'], str(signed))

    def test_picks_the_newest_build_tools_from_android_home(self):
        older = Path(self.tmp) / 'build-tools' / '9.0.0'
        older.mkdir()
        (older / 'zipalign').write_text('')
        (older / 'apksigner').write_text('')
        with patch.dict(os.environ, {'ANDROID_HOME': self.tmp}):
            self.assertEqual(control.build_tools(), self.tools)

    def test_fails_without_keystore_or_passwords(self):
        with patch.dict(os.environ, dict(self.SECRETS, KEY_PASSWORD='')), self.assertRaises(SystemExit):
            run('sign-apk', '--source', self.src, '--build-tools', self.tools)
        (self.src / 'androidApp/ci.jks').unlink()
        with patch.dict(os.environ, self.SECRETS), self.assertRaises(SystemExit):
            run('sign-apk', '--source', self.src, '--build-tools', self.tools)
        self.assertFalse((self.tools / 'calls.jsonl').exists())


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
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'unsigned-apk')['path'], str(apk))

    def test_prefers_release_variant_apk(self):
        self.put('build/tasks/_androidApp_buildAndroidDebug/gradle-project-debug.apk')
        apk = self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project-release-unsigned.apk')
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'unsigned-apk')['path'], str(apk))

    def test_apk_is_only_the_signed_output(self):
        # `kotlin build` leaves its APK unsigned; only sign-apk's output under build/signed counts as the APK.
        self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project-release-unsigned.apk')
        with self.assertRaises(SystemExit):
            run('find-artifact', '--source', self.src, '--kind', 'apk')
        apk = self.put('build/signed/gradle-project-release-signed.apk')
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'apk')['path'], str(apk))

    def test_mapping_prefers_the_bundle_task_when_both_builds_ran(self):
        # `kotlin package` (AAB) and `kotlin build -v release` (APK) each run R8; the AAB's mapping is the one Play uses.
        mp = self.put('build/tasks/_androidApp_bundleAndroid/gradle-project/build/_androidApp/outputs/mapping/release/mapping.txt')
        self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project/build/_androidApp/outputs/mapping/release/mapping.txt')
        self.assertEqual(run('find-artifact', '--source', self.src, '--kind', 'mapping')['path'], str(mp))

    def test_mapping_fails_when_the_two_r8_runs_differ(self):
        self.put('build/tasks/_androidApp_bundleAndroid/gradle-project/build/_androidApp/outputs/mapping/release/mapping.txt')
        other = self.put('build/tasks/_androidApp_buildAndroidRelease/gradle-project/build/_androidApp/outputs/mapping/release/mapping.txt')
        other.write_text('different')
        with self.assertRaises(SystemExit):
            run('find-artifact', '--source', self.src, '--kind', 'mapping')

    def test_fails_on_zero_or_many(self):
        with self.assertRaises(SystemExit):
            run('find-artifact', '--source', self.src, '--kind', 'apk')
        self.put('build/tasks/a/app-debug.apk')
        self.put('build/tasks/b/app-debug.apk')
        with self.assertRaises(SystemExit):
            run('find-artifact', '--source', self.src, '--kind', 'unsigned-apk')


class FirebaseAppIdTests(Base):
    RELEASE_ID = '1:100000000001:android:0123456789abcdef012345'

    def services(self, clients):
        (self.src / 'androidApp/google-services.json').write_text(json.dumps({'client': [
            {'client_info': {'mobilesdk_app_id': app, 'android_client_info': {'package_name': pkg}}}
            for app, pkg in clients]}))

    def set_manifest(self, **extra):
        path = self.src / '.ci/amper.json'
        path.write_text(json.dumps({**json.loads(path.read_text()), **extra}))

    def test_derives_release_client_from_google_services(self):
        self.services([('1:100000000001:android:fedcba9876543210fedcba', 'com.lascade.hunch.debug'),
                       (self.RELEASE_ID, 'com.lascade.hunch')])
        self.assertEqual(run('firebase-app-id', '--source', self.src), {'app_id': self.RELEASE_ID})

    def test_manifest_value_wins(self):
        self.services([('1:1:android:aaaa', 'com.lascade.hunch')])
        self.set_manifest(firebase_android_app_id=self.RELEASE_ID)
        self.assertEqual(run('firebase-app-id', '--source', self.src), {'app_id': self.RELEASE_ID})

    def test_rejects_malformed_app_id(self):
        self.set_manifest(firebase_android_app_id='1:2:android:x"; rm -rf /')
        with self.assertRaises(SystemExit):
            run('firebase-app-id', '--source', self.src)

    def test_fails_when_nothing_names_the_release_app(self):
        self.services([('1:1:android:aaaa', 'com.lascade.hunch.debug')])
        with self.assertRaises(SystemExit):
            run('firebase-app-id', '--source', self.src)

    def test_explicit_opt_out_skips_the_mapping_upload(self):
        self.set_manifest(crashlytics_mapping=False)
        self.assertEqual(run('firebase-app-id', '--source', self.src), {'app_id': ''})


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
