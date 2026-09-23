import os
import sys
from datetime import datetime, timezone, date, timedelta
sys.path.insert(0, os.path.dirname(__file__))
import catchup_gate as g


def U(s):
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def check(now, rd, wait_min, ok=True):
    d, w, p = g.plan(U(now))
    assert (str(d), round(w / 60), p) == (rd, wait_min, ok), (now, d, w / 60, p)


def test_plan():
    # PDT (UTC-7): crons 03:17/06:17/09:17 UTC = 20:17/23:17/02:17 local
    check("2026-09-24T03:17:00+00:00", "2026-09-24", 568)   # 20:17 -> 05:45 next = 9h28
    check("2026-09-24T06:17:00+00:00", "2026-09-24", 388)   # 23:17 -> 6h28
    check("2026-09-24T09:17:00+00:00", "2026-09-24", 208)   # 02:17 -> 3h28
    # PST (UTC-8), 2026-01-15
    check("2026-01-15T03:17:00+00:00", "2026-01-15", 628)   # 19:17 Jan14 -> 10h28
    check("2026-01-15T06:17:00+00:00", "2026-01-15", 448)
    check("2026-01-15T09:17:00+00:00", "2026-01-15", 268)
    # midnight rollover
    check("2026-09-24T06:59:00+00:00", "2026-09-24", 346)   # 23:59
    check("2026-09-24T07:01:00+00:00", "2026-09-24", 344)   # 00:01
    # exact / +-1min
    check("2026-09-24T12:45:00+00:00", "2026-09-24", 0)
    check("2026-09-24T12:44:00+00:00", "2026-09-24", 1)
    check("2026-09-24T12:46:00+00:00", "2026-09-24", 0)
    # +8h edge: 15:59 local -> today; 16:01 -> tomorrow (wait ~13h44 > cap -> declines)
    check("2026-09-24T22:59:00+00:00", "2026-09-24", 0)
    check("2026-09-24T23:01:00+00:00", "2026-09-25", 824, False)
    # late arrival 09:00 local proceeds immediately
    check("2026-09-24T16:00:00+00:00", "2026-09-24", 0)
    # DST: spring forward 2026-03-08 (02:00 -> 03:00); target 05:45 PDT = 12:45 UTC
    check("2026-03-08T06:17:00+00:00", "2026-03-08", 388)   # 22:17 PST -> 05:45 PDT
    check("2026-03-08T09:17:00+00:00", "2026-03-08", 208)   # 01:17 PST -> 05:45 PDT: 3h28 real
    # fall back 2026-11-01 (02:00 PDT -> 01:00 PST); target 05:45 PST = 13:45 UTC
    check("2026-11-01T06:17:00+00:00", "2026-11-01", 448)   # 23:17 PDT Oct31 -> 7h28
    check("2026-11-01T09:17:00+00:00", "2026-11-01", 268)   # 01:17 PST (2nd) -> 4h28


def fetcher(runs, jobs_by_run):
    def f(path):
        if "/jobs" in path:
            rid = int(path.split("/runs/")[1].split("/")[0])
            return {"jobs": jobs_by_run[rid]}
        return {"workflow_runs": runs}
    return f


def test_dedupe():
    now = U("2026-09-24T09:17:00+00:00")
    rd = date(2026, 9, 24)
    os.environ["GITHUB_RUN_ID"] = "1"
    ok = {"name": "email", "conclusion": "success", "completed_at": "2026-09-24T12:50:00Z"}
    assert g.already_sent(rd, now, fetcher([{"id": 2}], {2: [ok]})) is True
    # yesterday's email (before 16:00 PT cutoff) must not count
    old = dict(ok, completed_at="2026-09-23T12:50:00Z")
    assert g.already_sent(rd, now, fetcher([{"id": 2}], {2: [old]})) is False
    # failed email, current run excluded, no runs
    bad = dict(ok, conclusion="failure")
    assert g.already_sent(rd, now, fetcher([{"id": 2}], {2: [bad]})) is False
    assert g.already_sent(rd, now, fetcher([{"id": 1}], {1: [ok]})) is False
    assert g.already_sent(rd, now, fetcher([], {})) is False

    def boom(_):
        raise RuntimeError("api")
    assert g.already_sent(rd, now, boom) is False  # fail open


def test_phase2_wait_is_absolute():
    # gate2 queued 60 min after gate finished: it must not sleep the stale `remaining`
    rd = date(2026, 9, 24)
    target = datetime(2026, 9, 24, 5, 45, tzinfo=g.PT).astimezone(timezone.utc)
    assert g.phase2_wait(rd, target - timedelta(minutes=30)) == 30 * 60
    assert g.phase2_wait(rd, target + timedelta(minutes=60)) == 0
    far = target - timedelta(hours=10)
    assert g.phase2_wait(rd, far) == g.MAX_SLEEP


def test_workflow_routes_dedupe():
    seen = []

    def f(path):
        seen.append(path)
        return {"workflow_runs": []}
    now = U("2026-09-24T09:17:00+00:00")
    g.already_sent(date(2026, 9, 24), now, f, workflow="daily-ad-spend.yml")
    assert "workflows/daily-ad-spend.yml/runs" in seen[0]
    seen.clear()
    g.already_sent(date(2026, 9, 24), now, f)
    assert "workflows/daily-catchup.yml/runs" in seen[0]


if __name__ == "__main__":
    test_plan(); test_dedupe(); test_phase2_wait_is_absolute(); test_workflow_routes_dedupe(); print("ok")
