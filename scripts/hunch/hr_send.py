#!/usr/bin/env python3
"""Freeze one retention email and send the same bytes on every Resend retry."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from zoneinfo import ZoneInfo

import requests


UTC = dt.timezone.utc
PT = ZoneInfo("America/Los_Angeles")
FROM = "Lascade Games <noreply@metrics.lascade.com>"
TO = "cherian@lascade.com"
SEND_URL = "https://api.resend.com/emails"


def utc_now():
    return dt.datetime.now(UTC)


def aware(value):
    value = dt.datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-required")
    return value.astimezone(UTC)


def safe_suffix(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(value)):
        raise ValueError("invalid-run-id")
    return str(value)


def manual_test(event, recovery=False):
    if event not in ("schedule", "workflow_dispatch"):
        raise ValueError("invalid-event")
    return event == "workflow_dispatch" and not recovery


def send_key(date, event, run_id, recovery=False):
    suffix = f"-manual-{safe_suffix(run_id)}" if manual_test(event, recovery) else ""
    return f"hunch-retention-{date}{suffix}"


def recipient(event, recovery=False, to=None):
    to = to or TO
    if not re.fullmatch(r"[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+", to):
        raise ValueError("invalid-recipient")
    if not manual_test(event, recovery) and to != TO:
        raise ValueError("recipient-override-forbidden")
    return to


def scheduled_time(date, now):
    """Make the scheduling choice once, before freezing the payload bytes."""
    now = aware(now)
    cutoff = dt.datetime.combine(date, dt.time(5, 53), PT).astimezone(UTC)
    if now <= cutoff:
        return dt.datetime.combine(date, dt.time(5, 55), PT).astimezone(UTC)
    return None


def validate_frozen(path, to):
    body = json.loads(path.read_bytes())
    if (not isinstance(body, dict) or body.get("to") != [to] or body.get("from") != FROM
            or any(field in body for field in ("cc", "bcc"))
            or any(not isinstance(body.get(field), str) for field in ("html", "text", "subject"))):
        raise ValueError("invalid-payload")
    if "scheduled_at" in body:
        aware(body["scheduled_at"])
    return path


def freeze_payload(out, date, run_id, event, recovery=False, to=None, now=utc_now):
    """Atomically publish a complete payload; never replace a frozen payload."""
    to = recipient(event, recovery, to)
    out = Path(out)
    path = out / f"payload-{date}-{safe_suffix(run_id)}.json"
    if path.exists():
        return validate_frozen(path, to)
    email = json.loads((out / "email.json").read_text())
    body = {"from": FROM, "to": [to],
            **{field: email[field] for field in ("subject", "html", "text")}}
    if any(not isinstance(body[field], str) for field in ("subject", "html", "text")):
        raise ValueError("invalid-email")
    if manual_test(event, recovery) and not re.match(r"^\[Test [A-Za-z0-9_-]+\] ", body["subject"]):
        tag = safe_suffix(os.environ.get("HR_TEST_NUMBER") or run_id)
        body["subject"] = f"[Test {tag}] {body['subject']}"
    scheduled = scheduled_time(date, now())
    if scheduled is not None:
        body["scheduled_at"] = scheduled.isoformat()
    content = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=out, prefix=".payload-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return validate_frozen(path, to)


def send_payload(path, key, api_key, clock=utc_now, sleep=time.sleep, post=requests.post):
    """Only HTTP 200 means acceptance; retry transient errors with frozen bytes."""
    content = Path(path).read_bytes()
    body = json.loads(content)
    latest_start = (aware(body["scheduled_at"]) - dt.timedelta(minutes=1)
                    if "scheduled_at" in body else None)
    headers = {"Authorization": f"Bearer {api_key}", "Idempotency-Key": key,
               "Content-Type": "application/json"}
    for attempt in range(1, 5):
        if latest_start is not None and aware(clock()) > latest_start:
            print("schedule-expired", file=sys.stderr)
            return False
        response = None
        try:
            response = post(SEND_URL, headers=headers, data=content, timeout=(10, 50))
            status = response.status_code
        except requests.RequestException:
            status = 0
        if status == 200:
            print("accepted")
            return True
        if status == 409:
            try:
                concurrent = response.json().get("name") == "concurrent_idempotent_requests"
            except (ValueError, AttributeError):
                concurrent = False
            if not concurrent:
                print("conflict", file=sys.stderr)
                return False
            delay = 10
        elif status == 0 or status == 429 or 500 <= status <= 599:
            delay = 15 * attempt
        else:
            print("send-failed", file=sys.stderr)
            return False
        if attempt < 4:
            if latest_start is not None and aware(clock()) + dt.timedelta(seconds=delay) > latest_start:
                print("schedule-expired", file=sys.stderr)
                return False
            print("retrying")
            sleep(delay)
    print("send-failed", file=sys.stderr)
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-date", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--event", choices=("schedule", "workflow_dispatch"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out", type=Path, default=Path("out"))
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--to")
    args = parser.parse_args(argv)
    if not os.environ.get("RESEND_API_KEY"):
        print("resend-key-missing", file=sys.stderr)
        return 2
    try:
        key = send_key(args.report_date, args.event, args.run_id, args.recovery)
        path = freeze_payload(args.out, args.report_date, args.run_id, args.event,
                              recovery=args.recovery, to=args.to)
        return 0 if send_payload(path, key, os.environ["RESEND_API_KEY"]) else 1
    except Exception:
        print("send-failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
