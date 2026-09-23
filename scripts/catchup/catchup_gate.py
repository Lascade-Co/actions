#!/usr/bin/env python3
"""Timing gate for daily-catchup: hold the run until 05:45 Pacific, then release.

GitHub's cron scheduler fires late, so the workflow is scheduled several times the
evening before and each run waits here until the target. Two chained jobs are used
because a single job is capped at 6h.

  phase 1: compute report_date, dedupe, sleep min(wait, MAX_SLEEP), emit remaining
  phase 2: sleep remaining, dedupe again

Outputs (GITHUB_OUTPUT): proceed, report_date, remaining (seconds).
Env NOW (ISO, UTC) overrides the clock for tests.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PT = ZoneInfo("America/Los_Angeles")
TARGET_HHMM = (5, 45)          # 15 min lead for the pipeline
MAX_SLEEP = 340 * 60           # per-job cap is 6h; leave headroom
MAX_WAIT = 2 * MAX_SLEEP       # two chained sleep jobs
SENT_CUTOFF_HOUR = 16          # a run finishing after 16:00 PT day D-1 belongs to report D


def now_utc():
    v = os.environ.get("NOW")
    return datetime.fromisoformat(v).astimezone(timezone.utc) if v else datetime.now(timezone.utc)


def target_for(report_date):
    return datetime(report_date.year, report_date.month, report_date.day,
                    *TARGET_HHMM, tzinfo=PT)


def plan(now):
    """Return (report_date, wait_seconds, proceed)."""
    local = now.astimezone(PT)
    report_date = (local + timedelta(hours=8)).date()
    wait = max(0.0, (target_for(report_date) - now).total_seconds())
    return report_date, wait, wait <= MAX_WAIT


def phase2_wait(report_date, now):
    """Seconds phase 2 should sleep, from the absolute target rather than the
    `remaining` phase 1 computed: gate2 can queue behind gate for an hour."""
    return min(MAX_SLEEP, max(0.0, (target_for(report_date) - now).total_seconds()))


def sent_cutoff(report_date):
    d = report_date - timedelta(days=1)
    return datetime(d.year, d.month, d.day, SENT_CUTOFF_HOUR, tzinfo=PT)


def gh_json(path):
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def already_sent(report_date, now, fetch=gh_json, workflow="daily-catchup.yml"):
    """True if a scheduled run's `email` job succeeded for this report_date. Fails open."""
    repo = os.environ.get("GITHUB_REPOSITORY", "Lascade-Co/actions")
    current = os.environ.get("GITHUB_RUN_ID", "")
    cutoff = sent_cutoff(report_date).astimezone(timezone.utc)
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
    a = ap.parse_args()
    manual = a.event == "workflow_dispatch"
    now = now_utc()

    if a.phase == 1:
        report_date, wait, ok = plan(now)
        if manual:
            wait, ok = 0.0, True
        if not ok:
            print(f"wait {wait/60:.0f}m exceeds cap; a later cron handles it")
            return emit(proceed="false", report_date=report_date, remaining=0)
        if not manual and already_sent(report_date, now, workflow=a.workflow):
            print("already sent for", report_date)
            return emit(proceed="false", report_date=report_date, remaining=0)
        nap = min(wait, MAX_SLEEP)
        print(f"report_date={report_date} wait={wait/60:.0f}m sleeping {nap/60:.0f}m")
        time.sleep(nap)
        return emit(proceed="true", report_date=report_date, remaining=int(wait - nap))

    from datetime import date
    report_date = date.fromisoformat(a.report_date)
    if not manual:
        time.sleep(phase2_wait(report_date, now_utc()))
    if not manual and already_sent(report_date, now_utc(), workflow=a.workflow):
        print("already sent for", report_date)
        return emit(proceed="false", report_date=report_date)
    emit(proceed="true", report_date=report_date)


if __name__ == "__main__":
    main()
