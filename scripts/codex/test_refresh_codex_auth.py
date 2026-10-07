import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/codex/refresh_codex_auth.py"


class RefreshCodexAuthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()

        self.exec_count = self.root / "exec-count"
        self.login_started = self.root / "login-started"
        self.login_complete = self.root / "login-complete"
        self.codex_env = self.root / "codex-env"
        self.curl_args = self.root / "curl-args"
        self.app_server_started = self.root / "app-server-started"
        self.app_request = self.root / "app-request"

        self._write_executable(
            "codex",
            r"""
            #!/usr/bin/env bash
            set -euo pipefail
            printf '%s\n' "${TELEGRAM_BOT_TOKEN-unset}" >> "$FAKE_CODEX_ENV"

            case "${1:-}" in
              app-server)
                touch "$FAKE_APP_SERVER_STARTED"
                while IFS= read -r line; do
                  case "$line" in
                    *'"id": 1,'*) echo '{"id":1,"result":{}}' ;;
                    *'"id": 2,'*)
                      echo "$line" > "$FAKE_APP_REQUEST"
                      if [ "$FAKE_APP_MODE" = "reused" ]; then
                        echo "ERROR codex_login::auth::manager: Failed to refresh token: Your access token could not be refreshed because your refresh token was already used. Please log out and sign in again." >&2
                        echo '{"id":2,"error":{"code":-32000,"message":"refresh token was already used"}}'
                      else
                        echo '{"id":2,"result":{"account":null}}'
                      fi
                      ;;
                  esac
                done
                ;;
              exec)
                count=0
                if [ -f "$FAKE_EXEC_COUNT" ]; then read -r count < "$FAKE_EXEC_COUNT"; fi
                count=$((count + 1))
                printf '%s\n' "$count" > "$FAKE_EXEC_COUNT"
                if [ "$FAKE_MODE" = "success" ] || [ "$count" -gt 1 ]; then
                  echo refreshed
                  exit 0
                fi
                if [ "$FAKE_MODE" = "other_error" ]; then
                  echo "ERROR: unrelated authentication failure"
                  exit 7
                fi
                if [ "$FAKE_MODE" = "refresh_revoked" ]; then
                  echo 'ERROR: {"code":"refresh_token_invalidated"}'
                  echo "ERROR: Your access token could not be refreshed because your refresh token was revoked. Please log out and sign in again."
                  exit 1
                fi
                echo "ERROR: Your access token could not be refreshed because your refresh token was already used. Please log out and sign in again."
                exit 1
                ;;
              login)
                test "${2:-}" = "--device-auth"
                touch "$FAKE_LOGIN_STARTED"
                if [ "$FAKE_LOGIN_MODE" = "unavailable" ]; then
                  echo "device code login is not enabled for this Codex server"
                  exit 1
                fi
                echo "Open this link in your browser and sign in to your account"
                printf '\033[34mhttps://auth.openai.com/codex/device\033[0m\n'
                echo "Enter this one-time code (expires in 15 minutes)"
                printf '\033[34mABCD-1234\033[0m\n'
                while [ ! -f "$FAKE_LOGIN_COMPLETE" ]; do sleep 0.01; done
                echo "Successfully logged in"
                ;;
              *)
                echo "unexpected codex command: $*" >&2
                exit 64
                ;;
            esac
            """,
        )
        self._write_executable(
            "curl",
            r"""
            #!/usr/bin/env bash
            set -euo pipefail
            printf '%s\n' "$@" > "$FAKE_CURL_ARGS"
            if [ "$FAKE_CURL_MODE" = "failure" ]; then
              echo "request to bot${TELEGRAM_BOT_TOKEN}/sendMessage failed" >&2
              exit 22
            fi
            touch "$FAKE_LOGIN_COMPLETE"
            printf '%s' '{"ok":true,"result":{}}'
            """,
        )

        self.env = os.environ.copy()
        self.env.update(
            {
                "PATH": f"{self.bin}{os.pathsep}{self.env['PATH']}",
                "TELEGRAM_BOT_TOKEN": "123:telegram-secret",
                "TELEGRAM_CHAT_ID": "1575855120",
                "FAKE_MODE": "refresh_reused",
                "FAKE_LOGIN_MODE": "success",
                "FAKE_CURL_MODE": "success",
                "FAKE_APP_MODE": "ok",
                "FAKE_APP_SERVER_STARTED": str(self.app_server_started),
                "FAKE_APP_REQUEST": str(self.app_request),
                "FAKE_EXEC_COUNT": str(self.exec_count),
                "FAKE_LOGIN_STARTED": str(self.login_started),
                "FAKE_LOGIN_COMPLETE": str(self.login_complete),
                "FAKE_CODEX_ENV": str(self.codex_env),
                "FAKE_CURL_ARGS": str(self.curl_args),
                "GITHUB_SERVER_URL": "https://github.com",
                "GITHUB_REPOSITORY": "Lascade-Co/actions",
                "GITHUB_RUN_ID": "12345",
            }
        )

    def _write_executable(self, name: str, source: str) -> None:
        path = self.bin / name
        path.write_text(textwrap.dedent(source).lstrip())
        path.chmod(0o755)

    def run_script(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT)],
            env=self.env,
            text=True,
            capture_output=True,
            timeout=5,
        )

    def test_reused_refresh_token_starts_device_login_and_notifies_telegram(self) -> None:
        result = self.run_script()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("2", self.exec_count.read_text().strip())
        self.assertTrue(self.login_started.exists())

        curl_args = self.curl_args.read_text()
        self.assertIn(
            "https://api.telegram.org/bot123:telegram-secret/sendMessage", curl_args
        )
        self.assertIn("chat_id=1575855120", curl_args)
        self.assertIn("https://auth.openai.com/codex/device", curl_args)
        self.assertIn("ABCD-1234", curl_args)
        self.assertIn("https://github.com/Lascade-Co/actions/actions/runs/12345", curl_args)

        output = result.stdout + result.stderr
        self.assertNotIn("ABCD-1234", output)
        self.assertNotIn("telegram-secret", output)
        self.assertIn("one-time code sent via Telegram", output)
        codex_environments = self.codex_env.read_text().splitlines()
        self.assertEqual(["unset"] * 4, codex_environments)

    def test_forces_a_token_refresh_before_the_exec_check(self) -> None:
        self.env["FAKE_MODE"] = "success"

        result = self.run_script()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertTrue(self.app_server_started.exists())
        request = self.app_request.read_text()
        self.assertIn('"method": "account/read"', request)
        self.assertIn('"refreshToken": true', request)
        self.assertIn("Forced refresh requested.", result.stdout)

    def test_dead_refresh_token_found_by_forced_refresh_starts_login_even_if_exec_passes(self) -> None:
        self.env["FAKE_MODE"] = "success"
        self.env["FAKE_APP_MODE"] = "reused"

        result = self.run_script()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertTrue(self.login_started.exists())
        self.assertTrue(self.curl_args.exists())
        self.assertIn("Forced refresh failed: the refresh token is no longer valid", result.stdout)

    def test_missing_app_server_falls_back_to_exec(self) -> None:
        (self.bin / "codex").write_text(
            "#!/usr/bin/env bash\n"
            'if [ "$1" = app-server ]; then exit 64; fi\n'
            "echo refreshed\n"
        )

        result = self.run_script()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertFalse(self.login_started.exists())

    def test_unrelated_failure_does_not_start_login_or_notify(self) -> None:
        self.env["FAKE_MODE"] = "other_error"

        result = self.run_script()

        self.assertEqual(7, result.returncode, result.stdout + result.stderr)
        self.assertEqual("1", self.exec_count.read_text().strip())
        self.assertFalse(self.login_started.exists())
        self.assertFalse(self.curl_args.exists())

    def test_revoked_refresh_token_starts_device_login(self) -> None:
        self.env["FAKE_MODE"] = "refresh_revoked"

        result = self.run_script()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("2", self.exec_count.read_text().strip())
        self.assertTrue(self.login_started.exists())
        self.assertTrue(self.curl_args.exists())

    def test_successful_refresh_does_not_start_login_or_notify(self) -> None:
        self.env["FAKE_MODE"] = "success"

        result = self.run_script()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("1", self.exec_count.read_text().strip())
        self.assertFalse(self.login_started.exists())
        self.assertFalse(self.curl_args.exists())

    def test_device_login_unavailable_fails_without_notifying(self) -> None:
        self.env["FAKE_LOGIN_MODE"] = "unavailable"

        result = self.run_script()

        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertTrue(self.login_started.exists())
        self.assertFalse(self.curl_args.exists())
        self.assertIn("Codex device login failed with exit code 1", result.stderr)

    def test_telegram_failure_stops_login_and_redacts_token(self) -> None:
        self.env["FAKE_CURL_MODE"] = "failure"

        result = self.run_script()

        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertTrue(self.login_started.exists())
        self.assertFalse(self.login_complete.exists())
        output = result.stdout + result.stderr
        self.assertIn("Telegram notification failed", output)
        self.assertIn("[REDACTED]", output)
        self.assertNotIn("telegram-secret", output)
        self.assertNotIn("ABCD-1234", output)

    def test_workflow_runs_helper_with_the_requested_telegram_chat(self) -> None:
        workflow = (ROOT / ".github/workflows/refresh-codex-auth.yml").read_text()

        self.assertIn("timeout-minutes: 35", workflow)
        self.assertIn("timeout-minutes: 22", workflow)
        self.assertIn(
            "https://raw.githubusercontent.com/Lascade-Co/actions/main/"
            "scripts/codex/refresh_codex_auth.py",
            workflow,
        )
        self.assertIn(
            "TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}", workflow
        )
        self.assertIn("TELEGRAM_CHAT_ID: '1575855120'", workflow)
        self.assertIn("publish_codex_auth.sh", workflow)

    def test_workflow_rotates_daily_and_publishes_before_it_verifies(self) -> None:
        workflow = (ROOT / ".github/workflows/refresh-codex-auth.yml").read_text()

        self.assertIn('cron: "0 3 * * *"', workflow)
        self.assertIn("@openai/codex@0.160.1", workflow)
        steps = [
            workflow.index("name: Install Infisical CLI"),
            workflow.index("name: Download the publish helper"),
            workflow.index("name: Refresh Codex token"),
            workflow.index("name: Push refreshed auth.json to Infisical"),
            workflow.index("name: Verify the token was rotated and is fresh"),
            workflow.index("name: Alert on Telegram"),
        ]
        self.assertEqual(sorted(steps), steps)
        # Publish and verify must run even when the refresh step failed.
        publish = workflow[steps[3] : steps[4]]
        self.assertIn("if: always()", publish)
        self.assertIn("if: always() && (failure() || cancelled())", workflow[steps[5] :])

    def test_hung_exec_is_bounded_so_a_rotated_token_can_still_be_published(self) -> None:
        import importlib.util

        (self.bin / "codex").write_text(
            "#!/usr/bin/env bash\n"
            'if [ "$1" = app-server ]; then exit 64; fi\n'
            "sleep 5\n"
        )
        spec = importlib.util.spec_from_file_location("refresh_under_test", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.EXEC_TIMEOUT_SECONDS = 1
        old_path = os.environ["PATH"]
        os.environ["PATH"] = self.env["PATH"]
        try:
            result = module.run_codex_exec()
        finally:
            os.environ["PATH"] = old_path
        self.assertEqual(124, result.returncode)


def _jwt(exp: float) -> str:
    import base64
    import json

    def part(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part({'exp': exp})}.sig"


class PublishCodexAuthTest(unittest.TestCase):
    PUBLISH = ROOT / "scripts/codex/publish_codex_auth.sh"

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        (self.root / "bin").mkdir()
        self.auth = self.root / "auth.json"
        self.before = self.root / "before"
        self.store = self.root / "store"
        self.counter = self.root / "login-count"
        self.auth.write_text('{"last_refresh": "2026-10-07T03:00:00Z"}')
        self.before.write_text("2026-10-06T03:00:00Z")
        fake = self.root / "bin" / "infisical"
        fake.write_text(textwrap.dedent(
            r"""
            #!/usr/bin/env bash
            case "$1 $2" in
              "login "*|"login")
                n=0; [ -f "$FAKE_COUNTER" ] && read -r n < "$FAKE_COUNTER"
                n=$((n + 1)); echo "$n" > "$FAKE_COUNTER"
                [ "$n" -le "$FAKE_LOGIN_FAILURES" ] && exit 1
                echo session-token ;;
              "secrets set") printf '%s' "${3#CODEX_AUTH_JSON_BASE_64=}" > "$FAKE_STORE" ;;
              "secrets get")
                if [ "$FAKE_READBACK" = mismatch ]; then printf 'stale'; else cat "$FAKE_STORE"; fi ;;
            esac
            """).lstrip())
        fake.chmod(0o755)
        self.env = os.environ.copy()
        self.env.update({
            "PATH": f"{self.root / 'bin'}{os.pathsep}{self.env['PATH']}",
            "CODEX_AUTH_FILE": str(self.auth),
            "BEFORE_REFRESH_FILE": str(self.before),
            "PUBLISH_RETRY_DELAY": "0",
            "INFISICAL_CLIENT_ID": "id", "INFISICAL_CLIENT_SECRET": "secret",
            "INFISICAL_DOMAIN": "https://example.invalid", "PROJECT_ID": "p",
            "FAKE_STORE": str(self.store), "FAKE_COUNTER": str(self.counter),
            "FAKE_LOGIN_FAILURES": "0", "FAKE_READBACK": "ok",
        })

    def publish(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["bash", str(self.PUBLISH)], env=self.env, text=True,
                              capture_output=True, timeout=20)

    def test_rotated_token_is_published_and_read_back(self) -> None:
        result = self.publish()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("read it back", result.stdout)
        import base64
        self.assertEqual(self.auth.read_bytes(), base64.b64decode(self.store.read_text()))

    def test_unrotated_token_publishes_nothing(self) -> None:
        self.before.write_text("2026-10-07T03:00:00Z")
        result = self.publish()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("nothing to publish", result.stdout)
        self.assertFalse(self.store.exists())

    def test_transient_infisical_failures_are_retried(self) -> None:
        self.env["FAKE_LOGIN_FAILURES"] = "2"
        result = self.publish()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("3", self.counter.read_text().strip())

    def test_exhausted_retries_fail_loudly(self) -> None:
        self.env["FAKE_LOGIN_FAILURES"] = "9"
        result = self.publish()
        self.assertEqual(1, result.returncode)
        self.assertIn("Could not publish the rotated auth.json", result.stdout)

    def test_readback_mismatch_is_not_success(self) -> None:
        self.env["FAKE_READBACK"] = "mismatch"
        result = self.publish()
        self.assertEqual(1, result.returncode)
        self.assertEqual("3", self.counter.read_text().strip())


class VerifyStepTest(unittest.TestCase):
    """Runs the workflow's own verify script against crafted auth.json files."""

    def run_verify(self, *, before: str, after: str, days: float, event: str) -> subprocess.CompletedProcess[str]:
        import json
        import time

        workflow = (ROOT / ".github/workflows/refresh-codex-auth.yml").read_text()
        body = workflow.split("<<'PY'\n", 1)[1].split("\n          PY", 1)[0]
        code = textwrap.dedent(body)
        with tempfile.TemporaryDirectory() as home:
            (Path(home) / ".codex").mkdir()
            (Path(home) / ".codex/auth.json").write_text(json.dumps({
                "last_refresh": after,
                "tokens": {"access_token": _jwt(time.time() + days * 86400)},
            }))
            return subprocess.run(
                ["python3", "-c", code, before],
                env={**os.environ, "HOME": home, "GITHUB_EVENT_NAME": event},
                text=True, capture_output=True, timeout=20,
            )

    def test_healthy_rotation_passes(self) -> None:
        import datetime
        yesterday = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.123456789Z")
        result = self.run_verify(before=yesterday, after="2026-10-07T03:00:00Z", days=10, event="schedule")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_unrotated_token_fails(self) -> None:
        result = self.run_verify(before="2026-10-07T03:00:00Z", after="2026-10-07T03:00:00Z", days=9, event="workflow_dispatch")
        self.assertEqual(1, result.returncode)
        self.assertIn("the token was not rotated", result.stdout)

    def test_short_lived_token_fails(self) -> None:
        result = self.run_verify(before="2026-10-06T03:00:00Z", after="2026-10-07T03:00:00Z", days=2, event="workflow_dispatch")
        self.assertEqual(1, result.returncode)
        self.assertIn("under 5 days", result.stdout)

    def test_stale_restored_secret_fails_scheduled_runs_only(self) -> None:
        stale = "2026-09-21T14:39:05.169009704Z"
        scheduled = self.run_verify(before=stale, after="2026-10-07T03:00:00Z", days=10, event="schedule")
        self.assertEqual(1, scheduled.returncode)
        self.assertIn("Infisical to GitHub sync", scheduled.stdout)
        manual = self.run_verify(before=stale, after="2026-10-07T03:00:00Z", days=10, event="workflow_dispatch")
        self.assertEqual(0, manual.returncode, manual.stdout)


if __name__ == "__main__":
    unittest.main()
