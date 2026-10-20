#!/usr/bin/env python3
"""Timing gate for daily workflows: hold the run until a target time, then release.

GitHub's cron scheduler fires late, so the workflow is scheduled several times the
evening before and each run waits here until the target. Two chained jobs are used
because a single job is capped at 6h.

  phase 1: compute report_date, dedupe, sleep min(wait, MAX_SLEEP), emit remaining
  phase 2: sleep remaining, dedupe again

Outputs (GITHUB_OUTPUT): proceed, report_date, remaining (seconds).
Env NOW (ISO, UTC) overrides the clock for tests.

The target timezone and time are configurable via CLI args (defaults preserve the
original 05:45 America/Los_Angeles behavior for catchup and ad-spend):
  --target-tz      timezone name (default: America/Los_Angeles)
  --target-hour    hour in that timezone (default: 5)
  --target-minute  minute (default: 45)
  --cutoff-hour    hour in target-tz on D-1 after which a completed send belongs
                   to report D (default: 16)
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Defaults — match the original 05:45 Pacific behavior
_DEFAULT_TZ = "America/Los_Angeles"
_DEFAULT_HOUR = 5
_DEFAULT_MINUTE = 45
_DEFAULT_CUTOFF = 16

MAX_SLEEP = 340 * 60           # per-job cap is 6h; leave headroom
MAX_WAIT = 2 * MAX_SLEEP       # two chained sleep jobs


def now_utc():
    v = os.environ.get("NOW")
    return datetime.fromisoformat(v).astimezone(timezone.utc) if v else datetime.now(timezone.utc)


def target_for(report_date, tz, hhmm):
    return datetime(report_date.year, report_date.month, report_date.day,
                    *hhmm, tzinfo=tz)


def plan(now, tz, hhmm, cutoff_hour):
    """Return (report_date, wait_seconds, proceed)."""
    local = now.astimezone(tz)
    report_date = (local + timedelta(hours=24 - cutoff_hour)).date()
    wait = max(0.0, (target_for(report_date, tz, hhmm) - now).total_seconds())
    return report_date, wait, wait <= MAX_WAIT


def phase2_wait(report_date, now, tz, hhmm):
    """Seconds phase 2 should sleep, from the absolute target rather than the
    `remaining` phase 1 computed: gate2 can queue behind gate for an hour."""
    return min(MAX_SLEEP, max(0.0, (target_for(report_date, tz, hhmm) - now).total_seconds()))


def sent_cutoff(report_date, tz, cutoff_hour):
    d = report_date - timedelta(days=1)
    return datetime(d.year, d.month, d.day, cutoff_hour, tzinfo=tz)


def gh_json(path):
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def already_sent(report_date, now, fetch=gh_json, workflow="daily-catchup.yml",
                 tz=None, cutoff_hour=_DEFAULT_CUTOFF):
    """True if a scheduled run's `email` job succeeded for this report_date. Fails open."""
    if tz is None:
        tz = ZoneInfo(_DEFAULT_TZ)
    repo = os.environ.get("GITHUB_REPOSITORY", "Lascade-Co/actions")
    current = os.environ.get("GITHUB_RUN_ID", "")
    cutoff = sent_cutoff(report_date, tz, cutoff_hour).astimezone(timezone.utc)
    since = (now - timedelta(hours=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        runs = fetch(f"repos/{repo}/actions/workflows/{workflow}/runs"
                     f"?event=schedule&per_page=50&created=>={since}")["workflow_runs"]
        for r in runs:
            if str(r["id"]) == current:
                continue
            jobs = fetch(f"repos/{repo}/actions/runs/{r['id']}/jobs?per_page=100")["jobs"]
            for j in jobs:
                if j["name"] == "email" and j["conclusion"] == "success" and j.get("completed_at"):
                    done = datetime.fromisoformat(j["completed_at"].replace("Z", "+00:00"))
                    if done >= cutoff:
                        return True
    except Exception as e:  # fail open: Resend Idempotency-Key is the backstop
        print(f"dedupe check failed, proceeding: {e}", file=sys.stderr)
    return False


def emit(**kv):
    line = "".join(f"{k}={v}\n" for k, v in kv.items())
    print(line, end="")
    p = os.environ.get("GITHUB_OUTPUT")
    if p:
        with open(p, "a") as f:
            f.write(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, choices=[1, 2], required=True)
    ap.add_argument("--event", default="schedule")
    ap.add_argument("--report-date")
    ap.add_argument("--remaining", type=float, default=0)  # unused; kept so callers keep working
    ap.add_argument("--workflow", default="daily-catchup.yml")
    ap.add_argument("--target-tz", default=_DEFAULT_TZ)
    ap.add_argument("--target-hour", type=int, default=_DEFAULT_HOUR)
    ap.add_argument("--target-minute", type=int, default=_DEFAULT_MINUTE)
    ap.add_argument("--cutoff-hour", type=int, default=_DEFAULT_CUTOFF)
    a = ap.parse_args()
    manual = a.event == "workflow_dispatch"
    now = now_utc()

    tz = ZoneInfo(a.target_tz)
    hhmm = (a.target_hour, a.target_minute)

    if a.phase == 1:
        report_date, wait, ok = plan(now, tz, hhmm, a.cutoff_hour)
        if manual:
            wait, ok = 0.0, True
        if not ok:
            print(f"wait {wait/60:.0f}m exceeds cap; a later cron handles it")
            return emit(proceed="false", report_date=report_date, remaining=0)
        if not manual and already_sent(report_date, now, workflow=a.workflow,
                                       tz=tz, cutoff_hour=a.cutoff_hour):
            print("already sent for", report_date)
            return emit(proceed="false", report_date=report_date, remaining=0)
        nap = min(wait, MAX_SLEEP)
        print(f"report_date={report_date} wait={wait/60:.0f}m sleeping {nap/60:.0f}m")
        time.sleep(nap)
        return emit(proceed="true", report_date=report_date, remaining=int(wait - nap))

    from datetime import date
    report_date = date.fromisoformat(a.report_date)
    if not manual:
        time.sleep(phase2_wait(report_date, now_utc(), tz, hhmm))
    if not manual and already_sent(report_date, now_utc(), workflow=a.workflow,
                                   tz=tz, cutoff_hour=a.cutoff_hour):
        print("already sent for", report_date)
        return emit(proceed="false", report_date=report_date)
    emit(proceed="true", report_date=report_date)


if __name__ == "__main__":
    main()
