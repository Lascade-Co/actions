import io
import os
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

import next_version


def parse_output(text):
    """The script speaks GITHUB_OUTPUT key=value lines, not a bare version."""
    out = {}
    for line in text.strip().splitlines():
        key, _, value = line.partition("=")
        out[key] = value
    return out


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
                        {
                            "attributes": {
                                "versionString": version,
                                "appVersionState": "READY_FOR_DISTRIBUTION",
                            }
                        }
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

        return parse_output(stdout.getvalue()), stderr.getvalue(), calls

    # --- the open-train case: grouping builds under one marketing version ---

    def test_reuses_highest_open_train(self):
        out, warning, calls = self.run_main(
            current="3.9.6",
            prerelease_versions=("3.9.8", "3.10.0", "3.9.9"),
            store_versions=(),
        )

        self.assertEqual("3.10.0", out["version"])
        self.assertEqual("true", out["verified"])
        self.assertIn("3.10.0", warning)
        # The lookup commit 7b2e432 deleted. Its absence is the whole bug.
        self.assertTrue(any("/appStoreVersions" in path for path in calls))

    def test_both_lookups_are_restricted_to_ios(self):
        _, _, calls = self.run_main(prerelease_versions=("3.9.6",))

        for fragment in ("/preReleaseVersions", "/appStoreVersions"):
            path = next(p for p in calls if fragment in p)
            self.assertIn("filter[platform]=IOS", path, f"{fragment} not filtered to iOS")

    def test_open_train_above_a_closed_one_is_reused(self):
        out, _, _ = self.run_main(
            current="4.0.1",
            prerelease_versions=("4.0.1", "4.0.2"),
            store_versions=("4.0.1",),
        )

        self.assertEqual("4.0.2", out["version"])

    # --- the closed-train case: what this change exists to fix ---

    def test_closed_latest_train_takes_the_next_patch(self):
        out, warning, _ = self.run_main(
            current="4.0.1",
            prerelease_versions=("4.0.1",),
            store_versions=("4.0.1",),
        )

        self.assertEqual("4.0.2", out["version"])
        self.assertEqual("true", out["verified"])
        self.assertIn("closed", warning)

    def test_stale_branch_below_the_highest_closed_train(self):
        # A PR branched before the hand bump: its project version and the newest
        # train both sit under a train that has already shipped.
        out, _, _ = self.run_main(
            current="3.9.3",
            prerelease_versions=("4.0.0",),
            store_versions=("4.0.1",),
        )

        # Stepping above the candidate would land on 4.0.1, which is closed.
        self.assertEqual("4.0.2", out["version"])

    def test_patch_step_does_not_wrap_at_nine(self):
        out, _, _ = self.run_main(
            current="4.0.9",
            prerelease_versions=("4.0.9",),
            store_versions=("4.0.9",),
        )

        self.assertEqual("4.0.10", out["version"])

    def test_closed_train_with_no_testflight_train_at_all(self):
        out, _, _ = self.run_main(
            current="3.9.6",
            prerelease_versions=(),
            store_versions=("4.0.0",),
        )

        self.assertEqual("4.0.1", out["version"])

    # --- the project version keeps the power to open a train ---

    def test_project_version_opens_the_next_train(self):
        out, warning, _ = self.run_main(
            current="4.1.0",
            prerelease_versions=("3.9.9",),
            store_versions=(),
        )

        self.assertEqual("4.1.0", out["version"])
        self.assertIn("4.1.0", warning)

    def test_project_version_above_a_closed_train_wins_over_the_patch_step(self):
        out, _, _ = self.run_main(
            current="4.1.0",
            prerelease_versions=("4.0.1",),
            store_versions=("4.0.1",),
        )

        self.assertEqual("4.1.0", out["version"])

    def test_falls_back_to_project_version_when_testflight_has_no_train(self):
        out, warning, _ = self.run_main(current="3.9.6")

        self.assertEqual("3.9.6", out["version"])
        # Nothing failed: an empty TestFlight is a real answer, not a fallback.
        self.assertEqual("true", out["verified"])
        self.assertIn("no TestFlight marketing versions", warning)

    # --- fail-soft, but never silently ---

    def test_missing_credentials_falls_back_unverified(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.dict(os.environ, {"CURRENT_VERSION": "3.9.6"}, clear=True),
            patch.object(next_version, "get") as get,
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            next_version.main()

        out = parse_output(stdout.getvalue())
        self.assertEqual("3.9.6", out["version"])
        self.assertEqual("false", out["verified"])
        self.assertIn("credentials or bundle id missing", stderr.getvalue())
        get.assert_not_called()

    def test_lookup_failure_falls_back_unverified(self):
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

        out = parse_output(stdout.getvalue())
        self.assertEqual("3.9.6", out["version"])
        self.assertEqual("false", out["verified"])
        self.assertIn("lookup failed", stderr.getvalue())

    def test_unknown_app_falls_back_unverified(self):
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
            patch.object(next_version, "get", return_value={"data": []}),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            next_version.main()

        out = parse_output(stdout.getvalue())
        self.assertEqual("3.9.6", out["version"])
        self.assertEqual("false", out["verified"])
        self.assertIn("no app found", stderr.getvalue())

    def test_missing_current_version_is_fatal(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            next_version.main()

        self.assertEqual(1, raised.exception.code)


if __name__ == "__main__":
    unittest.main()
