"""Figure-free workflow settings, dedupe and failure notification text."""
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "catchup"))
import catchup_gate as G
from ip_editions import get_edition, resolve_daily_schedule


def delivery_mode(event, recovery=False, force=False):
    if event == "schedule":
        return "real"
    return "forced" if force else "real" if recovery else "test"


def delivery_step(edition, date, mode):
    if mode == "forced":
        return f"Forced send for {date}"
    if mode == "test":
        return f"Test send for {date}"
    return get_edition(edition).delivery_step(date)


def settings(notification=False):
    event = os.environ.get("EVENT", "schedule")
    name = (resolve_daily_schedule(os.environ["SCHEDULE"]) if event == "schedule"
            else os.environ.get("INPUT_EDITION") or "pt")
    edition = get_edition(name)
    mode = delivery_mode(event, os.environ.get("RECOVERY") == "true", os.environ.get("FORCE_RESEND") == "true")
    supplied = os.environ.get("REPORT_DATE") or (os.environ.get("INPUT_DATE") if event != "schedule" else "")
    date = None
    if supplied:
        try:
            date = dt.date.fromisoformat(supplied)
        except ValueError:
            if not notification:
                raise
    if date is None and event == "schedule":
        date = G.plan(G.now_utc(), tz=edition.zone, target=(5, 5))[0]
    elif date is None:
        local = G.now_utc().astimezone(edition.zone)
        date = local.date() if local.time() >= dt.time(5) else local.date() - dt.timedelta(days=1)
    offset = int(os.environ.get("SCHEDULE_IN_MIN") or "0") if event == "workflow_dispatch" and not notification else 0
    return edition, date, mode, offset


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=("settings", "dedupe", "failure-message"))
    args = ap.parse_args(argv)
    try:
        edition, date, mode, offset = settings(notification=args.command == "failure-message")
        if args.command == "failure-message":
            print(f"Install→paid {edition.label} edition for {date} failed. "
                  f"Run workflow → edition={edition.name}, report_date={date}, recovery ✓. "
                  f"Workflow: {os.environ.get('WF_URL', '')} Failed run: {os.environ.get('RUN_URL', '')}")
        elif args.command == "dedupe":
            sent = mode == "real" and G.already_sent(date, G.now_utc(), workflow="daily-install-paid.yml",
                                                     sent_step=edition.sent_step)
            G.emit(sent=str(sent))
        else:
            if offset and (mode != "test" or edition.name != "pt" or offset < 2):
                raise ValueError("invalid test scheduling inputs")
            if edition.name == "ist" and mode == "test" and os.environ.get("DRY_RUN") != "true":
                raise ValueError("IST tests must be dry runs")
            G.emit(edition=edition.name, timezone=edition.timezone, sent_step=edition.sent_step,
                   report_date=date, mode=mode, schedule_in_min=offset,
                   send_step=delivery_step(edition.name, date, mode))
        return 0
    except G.DedupeUnavailable:
        print("delivery history unavailable; refusing to send", file=sys.stderr)
        return 3
    except (ValueError, KeyError):
        print("invalid install-paid workflow settings", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
