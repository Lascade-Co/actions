#!/usr/bin/env python3
"""Watchdog for the daily install->paid email: decide whether to DM Cherian because no
scheduled run has sent today's email by 06:15 PT.

Acts only between 06:15 and 12:00 PT. Prints alert=true|false and date=<today> to
GITHUB_OUTPUT; the workflow's "Alert for <date>" step sends the message. A day counts
as alerted only once such a step has succeeded, so a failed send or a broken run
leaves the next hourly run free to try again.

Env: GH_TOKEN. NOW (ISO, UTC) overrides the clock for tests.
"""
import datetime as dt
import os
import sys

import catchup_gate as G

START, END = dt.time(6, 15), dt.time(12, 0)
SENT_STEP, ALERT_STEP = "Send for ", "Alert for "


def alerted_today(today, fetch=G.gh_json):
    repo = os.environ.get("GITHUB_REPOSITORY", "Lascade-Co/actions")
    midnight = dt.datetime.combine(today, dt.time(0), G.PT).astimezone(dt.timezone.utc)
    runs = fetch(f"repos/{repo}/actions/workflows/install-paid-watchdog.yml/runs"
                 f"?per_page=30&created=>={midnight:%Y-%m-%dT%H:%M:%SZ}")["workflow_runs"]
    # Every attempt of every run, this one included: a rerun of a run that already
    # alerted must stay quiet. The running attempt has no finished alert step yet.
    for r in runs:
        for j in fetch(f"repos/{repo}/actions/runs/{r['id']}/jobs?filter=all&per_page=50")["jobs"]:
            if any(s.get("name") == f"{ALERT_STEP}{today}" and s.get("conclusion") == "success"
                   for s in j.get("steps") or []):
                return True
    return False


def decide(now):
    local = now.astimezone(G.PT)
    today = local.date()
    if not (START <= local.time() < END):
        return False, today, "outside the watch window"
    if G.already_sent(today, now, workflow="daily-install-paid.yml", sent_step=SENT_STEP):
        return False, today, f"email sent for {today}"
    try:
        if alerted_today(today):
            return False, today, "already alerted today"
    except Exception as e:  # fail towards alerting: a duplicate DM beats a silent miss
        print(f"alert history check failed: {e}", file=sys.stderr)
    return True, today, f"no email for {today} by {local:%H:%M} PT"


def main():
    alert, today, why = decide(G.now_utc())
    print(why)
    G.emit(alert=str(alert).lower(), date=today)
    return 0


if __name__ == "__main__":
    sys.exit(main())
