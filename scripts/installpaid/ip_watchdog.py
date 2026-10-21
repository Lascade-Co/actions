#!/usr/bin/env python3
"""Warn Cherian when Resend has not accepted an edition's daily email.

The prep phase acts at 05:30–06:00 local; deliver acts at 06:15–12:00.
Each phase has its own dated successful-step marker. A failed Telegram request
leaves the next check free to alert, and a prep warning cannot hide a missed send.
History starts at UTC midnight two days before the report date. A run created
before that window is invisible even when rerun today, so it can cause another DM.
Incomplete scans are unknown and preserve the alert-on-unknown behavior.

Env: GH_TOKEN. NOW (ISO, UTC) overrides the clock for tests.
"""
import argparse
import datetime as dt
import os
import sys

import catchup_gate as G
from ip_editions import EDITIONS, get_edition

WINDOWS = {"prep": (dt.time(5, 30), dt.time(6)), "deliver": (dt.time(6, 15), dt.time(12))}


def alerted_today(today, fetch=G.gh_json, *, edition="pt", phase="deliver"):
    setting = get_edition(edition)
    repo = os.environ.get("GITHUB_REPOSITORY", "Lascade-Co/actions")
    wanted = setting.alert_step(today, phase)
    since = dt.datetime.combine(today - dt.timedelta(days=2), dt.time(), dt.timezone.utc)
    # One budget spans run pages and every trusted run's attempt/job pages.
    fetch = G.budgeted_fetch(fetch)
    runs = G.paginated(fetch, f"repos/{repo}/actions/workflows/install-paid-watchdog.yml/runs"
                       f"?created=>={since:%Y-%m-%dT%H:%M:%SZ}", "workflow_runs")
    for run in runs:
        if not G.trusted_run(run, repo):
            continue
        # Include earlier attempts of this run and successful steps in failed jobs.
        for attempt in range(1, int(run.get("run_attempt", 1)) + 1):
            jobs = G.paginated(fetch, f"repos/{repo}/actions/runs/{run['id']}/attempts/{attempt}/jobs", "jobs")
            for job in jobs:
                if any(step.get("name") == wanted and step.get("conclusion") == "success"
                       for step in job.get("steps") or []):
                    return True
    return False


def decide(now, edition="pt", phase="deliver"):
    setting = get_edition(edition)
    start, end = WINDOWS[phase]
    local = now.astimezone(setting.zone)
    today = local.date()
    if not (start <= local.time() < end):
        return False, today, "outside the watch window"
    try:
        if G.already_sent(today, now, workflow="daily-install-paid.yml", sent_step=setting.sent_step):
            return False, today, f"{setting.label} edition accepted for {today}"
    except G.DedupeUnavailable:
        print("delivery history unavailable; checking alert history", file=sys.stderr)
    try:
        if alerted_today(today, edition=edition, phase=phase):
            return False, today, "this phase already alerted today"
    except Exception:  # A duplicate DM is preferable to a silent miss; never echo API bodies.
        print("alert history check failed; proceeding with alert", file=sys.stderr)
    status = "at risk" if phase == "prep" else "missed"
    return True, today, f"{setting.label} edition for {today}: {status}; Resend acceptance is unconfirmed"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--edition", choices=EDITIONS, default="pt")
    ap.add_argument("--phase", choices=WINDOWS, default="deliver")
    args = ap.parse_args(argv)
    setting = get_edition(args.edition)
    alert, today, why = decide(G.now_utc(), edition=args.edition, phase=args.phase)
    status = "is at risk" if args.phase == "prep" else "was missed"
    message = (f"Install→paid {setting.label} edition for {today} {status}: Resend acceptance is unconfirmed. "
               f"Run workflow → edition={setting.name}, report_date={today}, recovery ✓")
    print(why)
    G.emit(alert=str(alert).lower(), date=today, edition=setting.name, phase=args.phase,
           alert_step=setting.alert_step(today, args.phase), message=message)
    return 0


if __name__ == "__main__":
    sys.exit(main())
