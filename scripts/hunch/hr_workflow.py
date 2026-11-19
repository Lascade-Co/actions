"""Workflow settings and delivery-history checks without public metric logs."""
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "catchup"))
import catchup_gate as G

PT = ZoneInfo("America/Los_Angeles")
TO = "cherian@lascade.com"


def delivery_mode(event, recovery=False):
    if event not in ("schedule", "workflow_dispatch"):
        raise ValueError("invalid-event")
    return "real" if event == "schedule" or recovery else "test"


def delivery_step(date, mode):
    return f"{'Test send for ' if mode == 'test' else 'Deliver for '}{date}"


def settings(notification=False):
    event = os.environ.get("EVENT", "schedule")
    mode = delivery_mode(event, os.environ.get("RECOVERY") == "true")
    supplied = os.environ.get("REPORT_DATE") or (os.environ.get("INPUT_DATE") if event != "schedule" else "")
    date = None
    if supplied:
        try:
            date = dt.date.fromisoformat(supplied)
        except ValueError:
            if not notification:
                raise
    if date is None and event == "schedule":
        date = G.plan(G.now_utc(), tz=PT, target=(5, 5))[0]
    elif date is None:
        local = G.now_utc().astimezone(PT)
        date = local.date() if local.time() >= dt.time(5) else local.date() - dt.timedelta(days=1)
    to = (os.environ.get("INPUT_TO") or TO) if not notification else TO
    if (not re.fullmatch(r"[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+", to)
            or (mode != "test" and to != TO)):
        raise ValueError("invalid-recipient")
    return date, mode, to


def emit(**values):
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as stream:
            stream.write("".join(f"{key}={value}\n" for key, value in values.items()))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("settings", "dedupe", "failure-message"))
    args = parser.parse_args(argv)
    try:
        date, mode, to = settings(notification=args.command == "failure-message")
        if args.command == "failure-message":
            # The workflow captures this text for Telegram; it is never logged.
            print(f"TreasureHunch retention for {date} failed. "
                  f"Run: {os.environ.get('RUN_URL', '')}")
        elif args.command == "dedupe":
            accepted = mode == "real" and G.already_sent(
                date, G.now_utc(), workflow="daily-hunch-retention.yml", sent_step="Deliver for ")
            emit(sent=str(accepted))
            print("dedupe-ok")
        else:
            emit(report_date=date, mode=mode, recipient=to,
                 send_step=delivery_step(date, mode))
            print("settings-ok")
        return 0
    except G.DedupeUnavailable:
        if args.command == "dedupe" and os.environ.get("EVENT", "schedule") == "schedule":
            # A scheduled retry retains the same key even if history is unavailable.
            emit(sent="False")
            print("history-unavailable", file=sys.stderr)
            return 0
        print("history-unavailable", file=sys.stderr)
        return 3
    except (ValueError, KeyError):
        print("settings-invalid", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
