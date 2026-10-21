#!/usr/bin/env python3
"""Freeze one edition's email payload and submit identical bytes to Resend.

Only HTTP status and generic outcomes are logged. Acceptance is HTTP 200;
delivery after acceptance is not monitored here.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys
import tempfile
import time

import requests

from ip_editions import EDITIONS, get_edition, safe_suffix

UTC = dt.timezone.utc
FROM = "Lascade Product <noreply@metrics.lascade.com>"
SEND_URL = "https://api.resend.com/emails"


def utc_now():
    return dt.datetime.now(UTC)


def aware(value):
    value = dt.datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp requires an explicit timezone")
    return value.astimezone(UTC)


def validate_timing(date, edition, timing):
    if timing.get("edition") != edition.name or timing.get("report_date") != str(date):
        raise ValueError("build timing does not match this edition and report date")
    started = aware(timing["started_at"])
    expected = edition.at(date, 5, 48)
    deadline = aware(timing["deadline"]) if timing.get("deadline") is not None else None
    if deadline != (expected if started < expected else None):
        raise ValueError("build timing has an inconsistent deadline")
    return deadline


def scheduled_time(date, edition, timing, now, event="schedule", schedule_in_min=0):
    """Decide once at payload freeze; a later retry cannot change this choice."""
    if event not in ("schedule", "workflow_dispatch"):
        raise ValueError("unsupported send event")
    if not isinstance(schedule_in_min, int) or schedule_in_min < 0 or schedule_in_min == 1:
        raise ValueError("test scheduling requires zero or at least two whole minutes")
    now = aware(now)
    if schedule_in_min:
        if event != "workflow_dispatch" or edition.name != "pt":
            raise ValueError("test scheduling is only available for manual PT tests")
        return now + dt.timedelta(minutes=schedule_in_min)
    deadline = validate_timing(date, edition, timing)
    if deadline is not None and now <= edition.at(date, 5, 53):
        return edition.at(date, 5, 55)
    return None


def validate_frozen(path, edition):
    body = json.loads(path.read_bytes())
    if (body.get("to") != [edition.recipient] or body.get("from") != FROM
            or any(field in body for field in ("cc", "bcc"))
            or any(not isinstance(body.get(field), str) for field in ("html", "text", "subject"))):
        raise ValueError("frozen payload does not match this edition")
    if "scheduled_at" in body:
        aware(body["scheduled_at"])
    return path


def freeze_payload(out, edition, date, run_id, event, schedule_in_min=0, test_tag=None, now=utc_now):
    """Publish a complete payload exclusively; an existing payload is never changed."""
    edition = get_edition(edition)
    out = Path(out)
    path = out / edition.payload_name(date, run_id)
    timing = json.loads((out / "build-timing.json").read_text())
    validate_timing(date, edition, timing)
    if path.exists():
        return validate_frozen(path, edition)

    body = {"from": FROM, "to": [edition.recipient],
            "html": (out / "email-A.html").read_text(),
            "text": (out / "email-A.txt").read_text(),
            "subject": (out / "subject-A.txt").read_text().rstrip("\n")}
    if test_tag is not None:
        body["subject"] = f"[Test {safe_suffix(test_tag)}] {body['subject']}"
    # Read T1 only after all source files have been loaded.
    scheduled = scheduled_time(date, edition, timing, now(), event, schedule_in_min)
    if scheduled is not None:
        body["scheduled_at"] = scheduled.isoformat()
    content = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    # Linking a complete temporary file is atomic and refuses to replace a winner.
    # Interrupted writes never leave a partial payload under the final filename.
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
    return validate_frozen(path, edition)


def send_payload(path, key, api_key, clock=utc_now, sleep=time.sleep, post=requests.post):
    """Submit at most four attempts, preserving payload bytes and send key."""
    content = Path(path).read_bytes()
    payload = json.loads(content)
    latest_start = (aware(payload["scheduled_at"]) - dt.timedelta(minutes=1)
                    if "scheduled_at" in payload else None)
    headers = {"Authorization": f"Bearer {api_key}", "Idempotency-Key": key,
               "Content-Type": "application/json"}
    for attempt in range(1, 5):
        if latest_start is not None and aware(clock()) > latest_start:
            print("scheduled send attempt window closed", file=sys.stderr)
            return False
        response = None
        try:
            response = post(SEND_URL, headers=headers, data=content, timeout=(10, 50))
            status = response.status_code
        except requests.RequestException:
            status = 0
        print(f"attempt {attempt}: HTTP {status:03d}")
        if status == 200:
            print("accepted")
            return True
        if status == 409:
            try:
                concurrent = response.json().get("name") == "concurrent_idempotent_requests"
            except (ValueError, AttributeError):
                concurrent = False
            if not concurrent:
                print("conflict on the send key", file=sys.stderr)
                return False
            delay = 10
        elif status == 0 or status == 429 or 500 <= status <= 599:
            delay = 15 * attempt
        else:
            return False
        if attempt < 4:
            if latest_start is not None and aware(clock()) + dt.timedelta(seconds=delay) > latest_start:
                print("scheduled send attempt window closed", file=sys.stderr)
                return False
            sleep(delay)
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--edition", choices=EDITIONS, default="pt")
    parser.add_argument("--report-date", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--event", choices=("schedule", "workflow_dispatch"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out", type=Path, default=Path("out"))
    parser.add_argument("--schedule-in-min", type=int, default=0)
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--force-resend", action="store_true")
    parser.add_argument("--test-tag")
    args = parser.parse_args(argv)
    manual = args.event == "workflow_dispatch"
    plain_test = manual and not (args.recovery or args.force_resend)
    if manual and args.edition == "ist" and plain_test:
        print("IST tests must use the build-only dry run", file=sys.stderr)
        return 2
    if args.schedule_in_min < 0 or args.schedule_in_min == 1 or (args.schedule_in_min and not (plain_test and args.edition == "pt")):
        print("test scheduling requires a manual PT test and at least two minutes", file=sys.stderr)
        return 2
    if not os.environ.get("RESEND_API_KEY"):
        print("Resend API key is missing", file=sys.stderr)
        return 2
    try:
        edition = get_edition(args.edition)
        key = edition.send_key(args.report_date, args.event, args.run_id)
        tag = (args.test_tag or args.run_id) if plain_test else None
        path = freeze_payload(args.out, args.edition, args.report_date, args.run_id, args.event,
                              schedule_in_min=args.schedule_in_min, test_tag=tag)
        return 0 if send_payload(path, key, os.environ["RESEND_API_KEY"]) else 1
    except Exception:
        # Payloads, query results and API errors may contain private figures.
        print("email send failed; payload and response details withheld", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
