#!/usr/bin/env python3
"""Refresh Codex auth, falling back to a Telegram-assisted device login."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping


REFRESH_TOKEN_REUSED = "refresh token was already used"
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


def run_codex_exec() -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            CODEX_EXEC,
            env=codex_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except OSError as error:
        raise RefreshError(f"Could not start Codex: {error}") from error

    print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    return result


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
        "The stored refresh token was already used, so this Actions run started a device login.",
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
    initial = run_codex_exec()
    if initial.returncode == 0:
        return 0
    if REFRESH_TOKEN_REUSED not in initial.stdout.lower():
        return initial.returncode or 1

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "1575855120")
    if not token:
        raise RefreshError(
            "TELEGRAM_BOT_TOKEN is required when Codex needs device login"
        )

    print("Stored Codex refresh token was already used; starting device login.")
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
