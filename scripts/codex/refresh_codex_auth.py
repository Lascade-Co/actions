#!/usr/bin/env python3
"""Refresh Codex auth, falling back to a Telegram-assisted device login."""

from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
from collections.abc import Mapping


DEVICE_LOGIN_REQUIRED_MARKERS = (
    "refresh token was already used",
    "refresh token was revoked",
    "refresh token has expired",
    "refresh_token_invalidated",
    "refresh_token_reused",
    "refresh_token_expired",
    "token_revoked",
    "log out and sign in again",
)
APP_SERVER_TIMEOUT_SECONDS = 60
EXEC_TIMEOUT_SECONDS = 90
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
VERIFICATION_URL = re.compile(r"https://[^\s]+")
DEVICE_CODE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)+$")
CODEX_EXEC = (
    "codex",
    "exec",
    "--skip-git-repo-check",
    "--sandbox",
    "read-only",
    "Reply with exactly one word: refreshed",
)


class RefreshError(RuntimeError):
    """A safe-to-print refresh failure."""


def codex_environment(source: Mapping[str, str] | None = None) -> dict[str, str]:
    """Keep notification credentials out of every Codex subprocess."""
    environment = dict(source or os.environ)
    environment.pop("TELEGRAM_BOT_TOKEN", None)
    return environment


def requires_device_login(output: str) -> bool:
    normalized = output.lower()
    return any(marker in normalized for marker in DEVICE_LOGIN_REQUIRED_MARKERS)


def run_codex_exec() -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            CODEX_EXEC,
            env=codex_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=EXEC_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        # A hang here must not outlast the job: the workflow publishes any rotated token afterwards.
        output = error.stdout if isinstance(error.stdout, str) else ""
        print(output, end="" if output.endswith("\n") or not output else "\n")
        print("Codex exec timed out.")
        return subprocess.CompletedProcess(CODEX_EXEC, 124, stdout=output)
    except OSError as error:
        raise RefreshError(f"Could not start Codex: {error}") from error

    print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    return result


def force_refresh() -> str:
    """Rotate the stored token now and return Codex's stderr for failure triage.

    Codex only refreshes on its own once the access token is within five minutes of
    expiring, so a plain `codex exec` leaves a healthy token alone. Consumers would
    then meet the expiry together and burn the shared refresh token. `account/read`
    with `refreshToken` runs the normal refresh flow and persists the result.
    Never raises: the caller falls back to `codex exec`, which reports real failures.
    Only fixed status words are printed; backend text goes to the caller for triage alone.
    """
    with tempfile.TemporaryFile("w+") as errors:
        try:
            process = subprocess.Popen(
                ("codex", "app-server"),
                env=codex_environment(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=errors,
                text=True,
                bufsize=1,
            )
        except OSError as error:
            print(f"Forced refresh unavailable: {error}")
            return ""

        responses: queue.Queue[dict | None] = queue.Queue()

        def read_responses() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                if isinstance(message, dict) and "id" in message:
                    responses.put(message)
            responses.put(None)  # stdout closed: the server exited

        threading.Thread(target=read_responses, daemon=True).start()

        def request(request_id: int, method: str, params: dict) -> dict | None:
            assert process.stdin is not None
            try:
                process.stdin.write(
                    json.dumps({"id": request_id, "method": method, "params": params}) + "\n"
                )
                process.stdin.flush()
                while True:
                    message = responses.get(timeout=APP_SERVER_TIMEOUT_SECONDS)
                    if message is None:
                        return None
                    if message["id"] == request_id:
                        return message
            except (OSError, queue.Empty):
                return None

        rpc_error = ""
        try:
            if request(1, "initialize", {"clientInfo": {"name": "refresh-codex-auth", "version": "1"}}) is None:
                print("Forced refresh failed: app-server did not initialize")
            else:
                assert process.stdin is not None
                process.stdin.write(json.dumps({"method": "initialized"}) + "\n")
                process.stdin.flush()
                answer = request(2, "account/read", {"refreshToken": True})
                if answer is None:
                    print("Forced refresh failed: no answer from app-server")
                elif "error" in answer:
                    rpc_error = json.dumps(answer["error"])
                    dead = requires_device_login(rpc_error)
                    print("Forced refresh failed:", "the refresh token is no longer valid" if dead else "see exec check")
                else:
                    print("Forced refresh requested.")
        except OSError as error:
            print(f"Forced refresh failed: {error}")
        finally:
            stop_process(process)
        errors.seek(0)
        return errors.read() + rpc_error


def workflow_run_url() -> str | None:
    server = os.environ.get("GITHUB_SERVER_URL")
    repository = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if server and repository and run_id:
        return f"{server}/{repository}/actions/runs/{run_id}"
    return None


def send_device_code(token: str, chat_id: str, url: str, code: str) -> None:
    lines = [
        "Codex sign-in required",
        "",
        "The stored Codex session could not be refreshed, so this Actions run started a device login.",
        f"Open: {url}",
        f"Code: {code}",
        "Expires in 15 minutes.",
        "",
        "Only continue if you recognize this Lascade-Co/actions workflow run.",
    ]
    if run_url := workflow_run_url():
        lines.extend(("", f"Workflow: {run_url}"))
    message = "\n".join(lines)

    try:
        result = subprocess.run(
            (
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--retry",
                "3",
                "--max-time",
                "20",
                "--request",
                "POST",
                f"https://api.telegram.org/bot{token}/sendMessage",
                "--data-urlencode",
                f"chat_id={chat_id}",
                "--data-urlencode",
                f"text={message}",
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except OSError as error:
        raise RefreshError(f"Could not start Telegram notification: {error}") from error

    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().replace(token, "[REDACTED]")
        raise RefreshError(
            f"Telegram notification failed (curl exit {result.returncode}): {detail}"
        )

    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RefreshError("Telegram returned an invalid response") from error
    if response.get("ok") is not True:
        description = str(response.get("description", "unknown error")).replace(
            token, "[REDACTED]"
        )
        raise RefreshError(f"Telegram rejected the device-code message: {description}")


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def run_device_login(token: str, chat_id: str) -> None:
    try:
        process = subprocess.Popen(
            ("codex", "login", "--device-auth"),
            env=codex_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError as error:
        raise RefreshError(f"Could not start Codex device login: {error}") from error

    verification_url: str | None = None
    user_code: str | None = None
    awaiting_url = False
    awaiting_code = False
    notified = False

    try:
        assert process.stdout is not None
        for raw_line in process.stdout:
            line = ANSI_ESCAPE.sub("", raw_line).rstrip("\r\n")
            if "Open this link in your browser" in line:
                awaiting_url = True
            elif awaiting_url and (match := VERIFICATION_URL.search(line)):
                verification_url = match.group(0)
                awaiting_url = False
            if "Enter this one-time code" in line:
                awaiting_code = True
            elif awaiting_code and DEVICE_CODE.fullmatch(line.strip()):
                user_code = line.strip()
                awaiting_code = False

            if verification_url and user_code and not notified:
                send_device_code(token, chat_id, verification_url, user_code)
                notified = True

            safe_line = line.replace(
                user_code or "\0", "[one-time code sent via Telegram]"
            )
            print(safe_line, flush=True)

        returncode = process.wait()
    except BaseException:
        stop_process(process)
        raise

    if returncode != 0:
        raise RefreshError(f"Codex device login failed with exit code {returncode}")
    if not notified:
        raise RefreshError(
            "Codex device login ended before a verification code could be sent"
        )


def main() -> int:
    forced_output = force_refresh()
    initial = run_codex_exec()
    # A refresh token the forced refresh already found dead needs a login now, even
    # while the access token still works; waiting only moves the failure to the consumers.
    if initial.returncode == 0 and not requires_device_login(forced_output):
        return 0
    if not requires_device_login(initial.stdout + forced_output):
        return initial.returncode or 1

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "1575855120")
    if not token:
        raise RefreshError(
            "TELEGRAM_BOT_TOKEN is required when Codex needs device login"
        )

    print("Stored Codex session requires a new login; starting device login.")
    run_device_login(token, chat_id)
    print("Device login completed; verifying the new Codex session.")

    verified = run_codex_exec()
    if verified.returncode != 0:
        raise RefreshError(
            f"Codex still failed after device login (exit {verified.returncode})"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RefreshError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
