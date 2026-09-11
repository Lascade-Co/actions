import io
import os
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

import next_version


class NextVersionTest(unittest.TestCase):
    def run_main(self, *, current="3.9.6", prerelease_versions=(), store_versions=()):
        calls = []

        def fake_get(path, _token):
            calls.append(path)
            if path.startswith("/v1/apps?"):
                return {"data": [{"id": "app-1"}]}
            if "/appStoreVersions" in path:
                return {
                    "data": [
                        {"attributes": {"versionString": version}}
                        for version in store_versions
                    ]
                }
            if "/preReleaseVersions" in path:
                return {
                    "data": [
                        {"attributes": {"version": version}}
                        for version in prerelease_versions
                    ]
                }
            raise AssertionError(f"unexpected App Store Connect path: {path}")

        env = {
            "CURRENT_VERSION": current,
            "APPSTORE_API_KEY_ID": "key-id",
            "APPSTORE_ISSUER_ID": "issuer-id",
            "APPSTORE_API_PRIVATE_KEY": "private-key",
            "IOS_BUNDLE_ID": "com.example.app",
        }
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.dict(os.environ, env, clear=True),
            patch.object(next_version, "make_token", return_value="token"),
            patch.object(next_version, "get", side_effect=fake_get),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            next_version.main()

        return stdout.getvalue().strip(), stderr.getvalue(), calls

    def test_reuses_highest_testflight_marketing_version(self):
        version, warning, calls = self.run_main(
            current="3.9.6",
            prerelease_versions=("3.9.8", "3.10.0", "3.9.9"),
            store_versions=("4.0.0",),
        )

        self.assertEqual("3.10.0", version)
        self.assertIn("latest TestFlight marketing version is 3.10.0", warning)
        self.assertFalse(any("/appStoreVersions" in path for path in calls))
        self.assertTrue(any("filter[platform]=IOS" in path for path in calls))

    def test_project_version_opens_the_next_train_after_a_release(self):
        version, warning, _ = self.run_main(
            current="4.1.0",
            prerelease_versions=("3.9.9",),
        )

        self.assertEqual("4.1.0", version)
        self.assertIn("project marketing version 4.1.0 is newer", warning)

    def test_falls_back_to_project_version_when_testflight_has_no_train(self):
        version, warning, _ = self.run_main(current="3.9.6")

        self.assertEqual("3.9.6", version)
        self.assertIn("no TestFlight marketing versions", warning)

    def test_missing_credentials_falls_back_without_calling_api(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.dict(os.environ, {"CURRENT_VERSION": "3.9.6"}, clear=True),
            patch.object(next_version, "get") as get,
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            next_version.main()

        self.assertEqual("3.9.6", stdout.getvalue().strip())
        self.assertIn("credentials or bundle id missing", stderr.getvalue())
        get.assert_not_called()

    def test_lookup_failure_falls_back_to_project_version(self):
        env = {
            "CURRENT_VERSION": "3.9.6",
            "APPSTORE_API_KEY_ID": "key-id",
            "APPSTORE_ISSUER_ID": "issuer-id",
            "APPSTORE_API_PRIVATE_KEY": "private-key",
            "IOS_BUNDLE_ID": "com.example.app",
        }
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.dict(os.environ, env, clear=True),
            patch.object(next_version, "make_token", return_value="token"),
            patch.object(
                next_version,
                "get",
                side_effect=urllib.error.URLError("offline"),
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            next_version.main()

        self.assertEqual("3.9.6", stdout.getvalue().strip())
        self.assertIn("lookup failed", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
