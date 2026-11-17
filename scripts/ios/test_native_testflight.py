import os
import plistlib
import tempfile
import unittest
from pathlib import Path

import native_testflight as nt


class NativeTestflightTest(unittest.TestCase):
    def test_project_version_ignores_test_targets(self):
        text = 'MARKETING_VERSION = 1.0;\nMARKETING_VERSION = 2.5.0;\nMARKETING_VERSION = "2.4.9";\n'
        self.assertEqual(nt.project_version(text), '2.5.0')

    def test_project_version_requires_full_version(self):
        with self.assertRaises(ValueError):
            nt.project_version('MARKETING_VERSION = 1.0;')

    def test_build_number(self):
        self.assertEqual(nt.build_number('61'), '1061')
        self.assertEqual(nt.build_number('61', '12'), '1061.12')

    def test_flags(self):
        self.assertEqual(nt.flags('true', 'auto', ''),
                         {'force_update': 'false', 'fetch_secrets': 'true', 'manual_version': '', 'debug_flag': 'true'})
        self.assertEqual(nt.flags('', '2.6.0', 'false'),
                         {'force_update': 'false', 'fetch_secrets': 'false', 'manual_version': '2.6.0', 'debug_flag': 'false'})
        with self.assertRaises(ValueError):
            nt.flags('', '2.6', '')
        self.assertEqual(nt.flags('', 'auto', '', 'true')['force_update'], 'true')
        with self.assertRaises(ValueError):
            nt.flags('', '3.0.0', '', 'true')

    def test_present_profiles(self):
        env = {'IOS_PROVISION_PROFILE_BASE64': 'x', 'IOS_WIDGET_PROVISION_PROFILE_BASE64': 'y'}
        self.assertEqual(nt.present_profiles(env),
                         'IOS_PROVISION_PROFILE_BASE64 IOS_WIDGET_PROVISION_PROFILE_BASE64')
        with self.assertRaises(ValueError):
            nt.present_profiles({'IOS_WIDGET_PROVISION_PROFILE_BASE64': 'y'})

    def test_profile_identity(self):
        profile = {'Name': 'Rent80 AppStore CI', 'UUID': 'abc', 'TeamIdentifier': ['756N4HCWVK'],
                   'Entitlements': {'application-identifier': '756N4HCWVK.com.rent80.rentalcars'}}
        self.assertEqual(nt.profile_identity(profile),
                         {'name': 'Rent80 AppStore CI', 'uuid': 'abc', 'team': '756N4HCWVK',
                          'bundle': 'com.rent80.rentalcars'})
        profile['Entitlements']['application-identifier'] = '756N4HCWVK.*'
        self.assertEqual(nt.profile_identity(profile)['bundle'], '')

    def test_export_options(self):
        options = nt.export_options('T', {'a.b': 'u1'})
        self.assertEqual(plistlib.loads(plistlib.dumps(options))['provisioningProfiles'], {'a.b': 'u1'})
        self.assertEqual(options['method'], 'app-store-connect')

    def test_metadata(self):
        with tempfile.TemporaryDirectory() as folder:
            cwd = os.getcwd()
            try:
                os.chdir(folder)
                Path('Rent80-v2.xcodeproj').mkdir()
                Path('Rent80-v2.xcodeproj/project.pbxproj').write_text('MARKETING_VERSION = 2.5.0;')
                values = nt.metadata({'RUN_NUMBER': '5', 'FETCH_SECRETS': 'true', 'VERSION_INPUT': 'auto'})
            finally:
                os.chdir(cwd)
        self.assertEqual(values['scheme'], 'Rent80-v2')
        self.assertEqual(values['project_version'], '2.5.0')
        self.assertEqual(values['build_number'], '1005')
        self.assertEqual(values['xcode_version'], 'latest-stable')


if __name__ == '__main__':
    unittest.main()
