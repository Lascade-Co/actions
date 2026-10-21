#!/usr/bin/env python3
"""Timing gate: hold until the local target, defaulting to 05:45 Pacific.

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
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

PT = ZoneInfo("America/Los_Angeles")
TARGET_HHMM = (5, 45)          # 15 min lead for the pipeline
MAX_SLEEP = 340 * 60           # per-job cap is 6h; leave headroom
MAX_WAIT = 2 * MAX_SLEEP       # two chained sleep jobs
SENT_CUTOFF_HOUR = 16          # a run finishing after 16:00 PT day D-1 belongs to report D


class DedupeUnavailable(RuntimeError):
    """Delivery history could not be verified; a fresh send key is unsafe."""


def now_utc():
    v = os.environ.get("NOW")
    return datetime.fromisoformat(v).astimezone(timezone.utc) if v else datetime.now(timezone.utc)


def parse_zone(value):
    try:
        return ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise argparse.ArgumentTypeError("timezone must be a valid IANA name") from exc


def parse_target(value):
    try:
        if (len(value) != 5 or value[2] != ":"
                or not value[:2].isascii() or not value[3:].isascii()
                or not value[:2].isdigit() or not value[3:].isdigit()):
            raise ValueError
        hour, minute = map(int, value.split(":"))
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError
        return hour, minute
    except ValueError as exc:
        raise argparse.ArgumentTypeError("target must be HH:MM in 24-hour time") from exc


def target_for(report_date, tz=PT, target=TARGET_HHMM):
    tz = parse_zone(tz) if isinstance(tz, str) else tz
    target = parse_target(target) if isinstance(target, str) else target
    return datetime(report_date.year, report_date.month, report_date.day,
                    *target, tzinfo=tz)


def plan(now, tz=PT, target=TARGET_HHMM):
    """Return (report_date, wait_seconds, proceed)."""
    tz = parse_zone(tz) if isinstance(tz, str) else tz
    local = now.astimezone(tz)
    report_date = (local + timedelta(hours=8)).date()
    wait = max(0.0, (target_for(report_date, tz, target).astimezone(timezone.utc)
                     - now.astimezone(timezone.utc)).total_seconds())
    return report_date, wait, wait <= MAX_WAIT


def phase2_wait(report_date, now, tz=PT, target=TARGET_HHMM):
    """Seconds phase 2 should sleep, from the absolute target rather than the
    `remaining` phase 1 computed: gate2 can queue behind gate for an hour."""
    return min(MAX_SLEEP, max(0.0, (target_for(report_date, tz, target).astimezone(timezone.utc)
                                   - now.astimezone(timezone.utc)).total_seconds()))


def sent_cutoff(report_date):
    d = report_date - timedelta(days=1)
    return datetime(d.year, d.month, d.day, SENT_CUTOFF_HOUR, tzinfo=PT)


def gh_json(path):
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def paginated(fetch, path, key):
    """Yield each API page without limiting dedupe to the newest runs or jobs."""
    page = 1
    while True:
        result = fetch(f"{path}?per_page=100&page={page}")
        rows = result[key]
        yield from rows
        if len(rows) < 100 or page * 100 >= result.get("total_count", float("inf")):
            return
        page += 1


def trusted_run(run, repo):
    """Only main's cron and maintainer dispatch runs can record a delivery."""
    return (run.get("event") in ("schedule", "workflow_dispatch")
            and run.get("head_branch") == "main"
            and (run.get("head_repository") or {}).get("full_name") == repo)


def sent_step_succeeded(report_date, fetch, repo, workflow, sent_step):
    """A successful delivery step is acceptance, even if another step failed.

    Query the complete retained run history so an older report's manual recovery
    also sees previous acceptance. Include earlier attempts of the current run.
    """
    runs = paginated(fetch, f"repos/{repo}/actions/workflows/{workflow}/runs", "workflow_runs")
    for run in runs:
        if not trusted_run(run, repo):
            continue
        for attempt in range(1, int(run.get("run_attempt", 1)) + 1):
            path = f"repos/{repo}/actions/runs/{run['id']}/attempts/{attempt}/jobs"
            for job in paginated(fetch, path, "jobs"):
                if job.get("name") == "email" and any(
                        step.get("name") == f"{sent_step}{report_date}" and step.get("conclusion") == "success"
                        for step in job.get("steps") or []):
                    return True
    return False


def already_sent(report_date, now, fetch=gh_json, workflow="daily-catchup.yml", sent_step=None):
    """Dedupe by accepted dated step, or the legacy scheduled-job cutoff.

    Dated-step checks fail closed because recovery uses a fresh send key.
    The legacy scheduled-job check retains its fail-open behavior.
    """
    repo = os.environ.get("GITHUB_REPOSITORY", "Lascade-Co/actions")
    if sent_step is not None:
        try:
            return sent_step_succeeded(report_date, fetch, repo, workflow, sent_step)
        except Exception:
            raise DedupeUnavailable("delivery history unavailable") from None

    # Legacy catchup/ad-spend behavior: latest jobs, schedule only, 16:00 PT cutoff.
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
    ap.add_argument("--tz", type=parse_zone, default=PT, help="IANA timezone (default America/Los_Angeles)")
    ap.add_argument("--target", type=parse_target, default=TARGET_HHMM, help="local HH:MM target (default 05:45)")
    ap.add_argument("--workflow", default="daily-catchup.yml")
    ap.add_argument("--sent-step", default=None, help="dedupe on a successful step named <prefix><report_date>")
    a = ap.parse_args()
    manual = a.event == "workflow_dispatch"
    now = now_utc()

    if a.phase == 1:
        report_date, wait, ok = plan(now, a.tz, a.target)
        if manual:
            wait, ok = 0.0, True
        if not ok:
            print(f"wait {wait/60:.0f}m exceeds cap; a later cron handles it")
            return emit(proceed="false", report_date=report_date, remaining=0)
        if not manual and already_sent(report_date, now, workflow=a.workflow, sent_step=a.sent_step):
            print("already sent for", report_date)
            return emit(proceed="false", report_date=report_date, remaining=0)
        nap = min(wait, MAX_SLEEP)
        print(f"report_date={report_date} wait={wait/60:.0f}m sleeping {nap/60:.0f}m")
        time.sleep(nap)
        return emit(proceed="true", report_date=report_date, remaining=int(wait - nap))

    from datetime import date
    report_date = date.fromisoformat(a.report_date)
    if not manual:
        time.sleep(phase2_wait(report_date, now_utc(), a.tz, a.target))
    if not manual and already_sent(report_date, now_utc(), workflow=a.workflow, sent_step=a.sent_step):
        print("already sent for", report_date)
        return emit(proceed="false", report_date=report_date)
    emit(proceed="true", report_date=report_date)


if __name__ == "__main__":
    main()
