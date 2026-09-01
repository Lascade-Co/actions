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

        self._write_executable(
            "codex",
            r"""
            #!/usr/bin/env bash
            set -euo pipefail
            printf '%s\n' "${TELEGRAM_BOT_TOKEN-unset}" >> "$FAKE_CODEX_ENV"

            case "${1:-}" in
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

    def _run(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(SCRIPT)],
            env=self.env,
            text=True,
            capture_output=True,
            timeout=5,
        )

    def test_reused_refresh_token_starts_device_login_and_notifies_telegram(self) -> None:
        result = self._run()

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
        self.assertEqual(["unset", "unset", "unset"], codex_environments)

    def test_unrelated_failure_does_not_start_login_or_notify(self) -> None:
        self.env["FAKE_MODE"] = "other_error"

        result = self._run()

        self.assertEqual(7, result.returncode, result.stdout + result.stderr)
        self.assertEqual("1", self.exec_count.read_text().strip())
        self.assertFalse(self.login_started.exists())
        self.assertFalse(self.curl_args.exists())

    def test_successful_refresh_does_not_start_login_or_notify(self) -> None:
        self.env["FAKE_MODE"] = "success"

        result = self._run()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("1", self.exec_count.read_text().strip())
        self.assertFalse(self.login_started.exists())
        self.assertFalse(self.curl_args.exists())

    def test_device_login_unavailable_fails_without_notifying(self) -> None:
        self.env["FAKE_LOGIN_MODE"] = "unavailable"

        result = self._run()

        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertTrue(self.login_started.exists())
        self.assertFalse(self.curl_args.exists())
        self.assertIn("Codex device login failed with exit code 1", result.stderr)

    def test_telegram_failure_stops_login_and_redacts_token(self) -> None:
        self.env["FAKE_CURL_MODE"] = "failure"

        result = self._run()

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

        self.assertIn("timeout-minutes: 25", workflow)
        self.assertIn(
            "https://raw.githubusercontent.com/Lascade-Co/actions/main/"
            "scripts/codex/refresh_codex_auth.py",
            workflow,
        )
        self.assertIn(
            "TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}", workflow
        )
        self.assertIn("TELEGRAM_CHAT_ID: '1575855120'", workflow)


if __name__ == "__main__":
    unittest.main()
