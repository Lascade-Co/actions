import os
import sys
import traceback
from datetime import datetime, timezone, date, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
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


@pytest.mark.parametrize("hour,wait_minutes", [(13, 628), (16, 448), (19, 268)])
def test_ist_crons_wait_until_next_local_report_day(hour, wait_minutes):
    now = U(f"2026-10-20T{hour}:07:00+00:00")
    rd, wait, proceed = g.plan(now, tz="Asia/Kolkata", target="05:05")
    assert (rd, wait, proceed) == (date(2026, 10, 21), wait_minutes * 60, True)
    assert g.target_for(rd, tz="Asia/Kolkata", target="05:05").astimezone(timezone.utc) == U(
        "2026-10-20T23:35:00+00:00")
    assert wait <= g.MAX_WAIT


@pytest.mark.parametrize("report_day,now,target,wait_minutes", [
    ("2026-03-08", "2026-03-08T09:17:00+00:00", "2026-03-08T12:05:00+00:00", 168),
    ("2026-11-01", "2026-11-01T08:17:00+00:00", "2026-11-01T13:05:00+00:00", 288),
])
def test_pt_installpaid_target_across_dst(report_day, now, target, wait_minutes):
    rd = date.fromisoformat(report_day)
    assert g.target_for(rd, tz=g.PT, target=(5, 5)).astimezone(timezone.utc) == U(target)
    assert g.plan(U(now), tz=g.PT, target=(5, 5)) == (rd, wait_minutes * 60, True)
    assert g.phase2_wait(rd, U(now), tz=g.PT, target=(5, 5)) == wait_minutes * 60
    # A caller with an already-local clock must still get elapsed, not wall-clock, time.
    assert g.plan(U(now).astimezone(g.PT), tz=g.PT, target=(5, 5)) == (rd, wait_minutes * 60, True)


def test_phase2_ist_target_is_absolute_and_clamped():
    rd = date(2026, 10, 21)
    assert g.phase2_wait(rd, U("2026-10-20T23:05:00+00:00"), "Asia/Kolkata", "05:05") == 1800
    assert g.phase2_wait(rd, U("2026-10-20T23:40:00+00:00"), "Asia/Kolkata", "05:05") == 0
    assert g.phase2_wait(rd, U("2026-10-20T13:07:00+00:00"), "Asia/Kolkata", "05:05") == g.MAX_SLEEP


def _trusted_run(run_id, **changes):
    return dict(id=run_id, event="schedule", head_branch="main", run_attempt=1,
                head_repository={"full_name": "Lascade-Co/actions"}, **changes)


def _sent_job(step="Deliver for 2026-10-21", **changes):
    return dict(name="email", conclusion="success", completed_at="2026-10-21T12:20:00Z",
                steps=[{"name": step, "conclusion": "success"}], **changes)


def _attempt_fetcher(runs, jobs_by_attempt):
    def f(path):
        if "/jobs" in path:
            run_id = int(path.split("/runs/")[1].split("/")[0])
            # The default jobs endpoint returns only the latest attempt.
            attempt = int(path.split("/attempts/")[1].split("/")[0]) if "/attempts/" in path else "latest"
            return {"jobs": jobs_by_attempt.get((run_id, attempt), [])}
        return {"workflow_runs": runs}
    return f


def test_dated_dedupe_counts_successful_step_despite_job_failure(monkeypatch):
    monkeypatch.setenv("GITHUB_REPOSITORY", "Lascade-Co/actions")
    rd, now = date(2026, 10, 21), U("2026-10-21T14:00:00+00:00")
    job = dict(_sent_job(), conclusion="failure", completed_at=None)
    fetch = _attempt_fetcher([_trusted_run(2)], {(2, 1): [job], (2, "latest"): [job]})
    assert g.already_sent(rd, now, fetch, workflow="daily-install-paid.yml", sent_step="Deliver for ")


def test_dated_dedupe_checks_earlier_attempt_of_current_run(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "2")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "3")
    rd, now = date(2026, 10, 21), U("2026-10-21T14:00:00+00:00")
    runs = [dict(_trusted_run(2), run_attempt=3)]
    fetch = _attempt_fetcher(runs, {(2, 1): [_sent_job()], (2, 2): [], (2, 3): []})
    assert g.already_sent(rd, now, fetch, workflow="daily-install-paid.yml", sent_step="Deliver for ")


def test_dated_dedupe_paginates_runs_and_attempt_jobs(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    rd, now = date(2026, 10, 21), U("2026-10-21T14:00:00+00:00")

    def fetch(path):
        query = parse_qs(urlsplit(path).query)
        page = int(query.get("page", [1])[0])
        per_page = int(query["per_page"][0])
        if "/jobs" in path:
            assert "/runs/800/attempts/2/jobs" in path or "/runs/800/attempts/1/jobs" in path
            if "/attempts/1/" in path:
                return {"jobs": []}
            jobs = ([{"name": "other", "conclusion": "success"}] * per_page) if page == 1 else [_sent_job()]
            return {"jobs": jobs, "total_count": per_page + 1}
        runs = ([dict(_trusted_run(99), head_branch="untrusted")] * per_page) if page == 1 else [dict(_trusted_run(800), run_attempt=2)]
        return {"workflow_runs": runs, "total_count": per_page + 1}

    assert g.already_sent(rd, now, fetch, workflow="daily-install-paid.yml", sent_step="Deliver for ")


def test_dated_dedupe_created_window_is_anchored_to_report_date(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    rd, now = date(2026, 10, 21), U("2026-11-21T14:00:00+00:00")
    seen = []

    def fetch(path):
        seen.append(path)
        if "/jobs" in path:
            return {"jobs": [_sent_job()]}
        if parse_qs(urlsplit(path).query).get("created") != [">=2026-10-18T00:00:00Z"]:
            return {"workflow_runs": []}
        return {"workflow_runs": [_trusted_run(2)]}

    assert g.already_sent(rd, now, fetch, workflow="daily-install-paid.yml", sent_step="Deliver for ")
    query = parse_qs(urlsplit(seen[0]).query)
    assert query == {"created": [">=2026-10-18T00:00:00Z"], "per_page": ["100"], "page": ["1"]}


@pytest.mark.parametrize("kind", ["runs", "jobs"])
def test_dated_dedupe_rejects_endless_run_or_job_pages_after_five(kind):
    seen = []

    def fetch(path):
        seen.append(path)
        # Bound the pre-fix test itself if production pagination never stops.
        if len(seen) > 7:
            raise RuntimeError("synthetic pagination guard")
        if "/jobs" in path:
            return {"jobs": [{"name": "other", "conclusion": "success"}] * 100}
        if kind == "runs":
            return {"workflow_runs": [dict(_trusted_run(2), event="push")] * 100}
        return {"workflow_runs": [_trusted_run(2)]}

    with pytest.raises(g.DedupeUnavailable):
        g.already_sent(date(2026, 10, 21), U("2026-10-21T14:00:00+00:00"), fetch,
                       workflow="daily-install-paid.yml", sent_step="Deliver for ")
    assert len(seen) == (5 if kind == "runs" else 6)


@pytest.mark.parametrize("acceptance", [False, True])
def test_dated_dedupe_global_budget_covers_trusted_run_attempts(acceptance):
    seen = []

    def fetch(path):
        seen.append(path)
        if "/jobs" in path:
            if acceptance and "/runs/33/attempts/3/jobs" in path:
                return {"jobs": [_sent_job()]}
            return {"jobs": []}
        return {"workflow_runs": [dict(_trusted_run(i), run_attempt=3) for i in range(1, 51)]}

    args = (date(2026, 10, 21), U("2026-10-21T14:00:00+00:00"), fetch)
    if acceptance:
        assert g.already_sent(*args, workflow="daily-install-paid.yml", sent_step="Deliver for ")
    else:
        with pytest.raises(g.DedupeUnavailable):
            g.already_sent(*args, workflow="daily-install-paid.yml", sent_step="Deliver for ")
    assert len(seen) == 100


def test_dated_dedupe_success_on_fifth_job_page_is_within_limits():
    seen = []

    def fetch(path):
        seen.append(path)
        if "/jobs" not in path:
            return {"workflow_runs": [_trusted_run(2)]}
        page = int(parse_qs(urlsplit(path).query)["page"][0])
        if page == 5:
            return {"jobs": [_sent_job()]}
        return {"jobs": [{"name": "other", "conclusion": "success"}] * 100}

    assert g.already_sent(date(2026, 10, 21), U("2026-10-21T14:00:00+00:00"), fetch,
                          workflow="daily-install-paid.yml", sent_step="Deliver for ")
    assert len(seen) == 6


def test_pagination_rejects_a_short_page_when_total_shows_missing_history():
    with pytest.raises(g.DedupeUnavailable):
        list(g.paginated(lambda _: {"jobs": [], "total_count": 1}, "synthetic/jobs", "jobs"))


def test_pagination_accepts_a_complete_full_fifth_page():
    seen = []

    def fetch(path):
        seen.append(path)
        return {"jobs": [{"name": "other"}] * 100, "total_count": 500}

    assert len(list(g.paginated(fetch, "synthetic/jobs", "jobs"))) == 500
    assert len(seen) == 5


@pytest.mark.parametrize("change", [
    {"event": "pull_request"}, {"head_branch": "feature"},
    {"head_repository": {"full_name": "fork/actions"}}, {"head_repository": None},
])
def test_dated_dedupe_rejects_untrusted_runs(monkeypatch, change):
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    run = dict(_trusted_run(2), **change)
    fetch = _attempt_fetcher([run], {(2, 1): [_sent_job()], (2, "latest"): [_sent_job()]})
    assert not g.already_sent(date(2026, 10, 21), U("2026-10-21T14:00:00+00:00"), fetch,
                              workflow="daily-install-paid.yml", sent_step="Deliver for ")


@pytest.mark.parametrize("prefix,other_step", [
    ("Deliver for ", "Deliver IST for 2026-10-21"),
    ("Deliver IST for ", "Deliver for 2026-10-21"),
    ("Deliver for ", "Forced send for 2026-10-21"),
    ("Deliver IST for ", "Forced send for 2026-10-21"),
    ("Deliver for ", "Deliver for 2026-10-20"),
])
def test_dated_dedupe_isolates_edition_date_and_forced_steps(monkeypatch, prefix, other_step):
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    fetch = _attempt_fetcher([_trusted_run(2)], {(2, 1): [_sent_job(other_step)]})
    assert not g.already_sent(date(2026, 10, 21), U("2026-10-21T14:00:00+00:00"), fetch,
                              workflow="daily-install-paid.yml", sent_step=prefix)


def test_legacy_dedupe_keeps_schedule_query_latest_jobs_and_16_hour_cutoff(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    rd, now = date(2026, 10, 21), U("2026-10-21T14:00:00+00:00")
    before_cutoff = dict(_sent_job(), completed_at="2026-10-20T22:59:59Z")
    at_cutoff = dict(_sent_job(), completed_at="2026-10-20T23:00:00Z")
    seen = []

    def fetch(path):
        seen.append(path)
        if "/jobs" in path:
            return {"jobs": [current_job]}
        return {"workflow_runs": [{"id": 2}]}

    for current_job, expected in ((before_cutoff, False), (at_cutoff, True)):
        assert g.already_sent(rd, now, fetch) is expected
    assert all("event=schedule&per_page=50&created=>=" in p for p in seen if "/jobs" not in p)
    assert all("/runs/2/jobs?per_page=100" in p and "/attempts/" not in p for p in seen if "/jobs" in p)


def test_cli_routes_zone_and_target_and_preserves_remaining(monkeypatch, capsys):
    monkeypatch.setenv("NOW", "2026-10-20T23:20:00+00:00")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(sys, "argv", ["catchup_gate.py", "--phase", "2", "--report-date", "2026-10-21",
                                     "--remaining", "999999", "--tz", "Asia/Kolkata", "--target", "05:05"])
    sleeps = []
    monkeypatch.setattr(g.time, "sleep", sleeps.append)
    monkeypatch.setattr(g, "already_sent", lambda *a, **k: False)
    g.main()
    assert sleeps == [900]
    assert "proceed=true\nreport_date=2026-10-21\n" in capsys.readouterr().out


def test_cli_legacy_defaults_and_manual_remaining(monkeypatch, capsys):
    monkeypatch.setenv("NOW", "2026-10-21T12:20:00+00:00")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(sys, "argv", ["catchup_gate.py", "--phase", "2", "--report-date", "2026-10-21",
                                     "--remaining", "999999"])
    sleeps = []
    monkeypatch.setattr(g.time, "sleep", sleeps.append)
    monkeypatch.setattr(g, "already_sent", lambda *a, **k: False)
    g.main()
    assert sleeps == [1500]
    sleeps.clear()
    monkeypatch.setattr(sys, "argv", sys.argv + ["--event", "workflow_dispatch"])
    g.main()
    assert sleeps == []
    assert capsys.readouterr().out.count("proceed=true") == 2


def test_cli_phase1_uses_ist_report_date_and_sleep_cap(monkeypatch, capsys):
    monkeypatch.setenv("NOW", "2026-10-20T13:07:00+00:00")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(sys, "argv", ["catchup_gate.py", "--phase", "1", "--tz", "Asia/Kolkata", "--target", "05:05"])
    sleeps = []
    monkeypatch.setattr(g.time, "sleep", sleeps.append)
    monkeypatch.setattr(g, "already_sent", lambda *a, **k: False)
    g.main()
    assert sleeps == [20400]
    assert "proceed=true\nreport_date=2026-10-21\nremaining=17280\n" in capsys.readouterr().out


@pytest.mark.parametrize("argument,value", [
    ("--tz", "Unknown/Timezone"), ("--target", "25:05"), ("--target", "05:60"),
    ("--target", "5:05"), ("--target", " 5:05"), ("--target", "aa:bb"),
])
def test_cli_rejects_invalid_timezone_or_target(monkeypatch, argument, value):
    monkeypatch.setattr(sys, "argv", ["catchup_gate.py", "--phase", "1", argument, value])
    monkeypatch.setenv("NOW", "2026-10-21T12:05:00+00:00")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(g.time, "sleep", lambda _: None)
    monkeypatch.setattr(g, "already_sent", lambda *a, **k: False)
    with pytest.raises(SystemExit) as exc:
        g.main()
    assert exc.value.code == 2


@pytest.mark.parametrize("fail_at", ["runs", "jobs"])
def test_dated_dedupe_failure_stops_sending_without_logging_api_body(capsys, fail_at):
    def fetch(path):
        if fail_at == "jobs" and "/jobs" not in path:
            return {"workflow_runs": [_trusted_run(2)]}
        raise RuntimeError("private response body")

    with pytest.raises(RuntimeError) as exc:
        g.already_sent(date(2026, 10, 21), U("2026-10-21T14:00:00+00:00"), fetch,
                       workflow="daily-install-paid.yml", sent_step="Deliver for ")
    assert type(exc.value) is g.DedupeUnavailable
    assert str(exc.value) == "delivery history unavailable"
    captured = capsys.readouterr()
    trace = "".join(traceback.format_exception(exc.value))
    assert "private response body" not in captured.out + captured.err + trace


@pytest.mark.parametrize("phase", ["1", "2"])
def test_cli_schedule_continues_on_shared_key_when_dated_dedupe_is_unavailable(monkeypatch, capsys, phase):
    def unavailable(*args, **kwargs):
        raise RuntimeError("private response body")

    monkeypatch.setenv("NOW", "2026-10-21T14:00:00+00:00")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(sys, "argv", ["catchup_gate.py", "--phase", phase, "--report-date", "2026-10-21",
                                     "--workflow", "daily-install-paid.yml", "--sent-step", "Deliver for "])
    monkeypatch.setattr(g, "sent_step_succeeded", unavailable)
    monkeypatch.setattr(g.time, "sleep", lambda _: None)
    g.main()
    captured = capsys.readouterr()
    assert "proceed=true" in captured.out
    assert captured.err == "delivery history unavailable; proceeding on scheduled key\n"
    assert "private response body" not in captured.out + captured.err


@pytest.mark.parametrize("phase", ["1", "2"])
def test_cli_non_schedule_propagates_dated_dedupe_failure(monkeypatch, capsys, phase):
    def unavailable(*args, **kwargs):
        raise RuntimeError("private response body")

    monkeypatch.setenv("NOW", "2026-10-21T14:00:00+00:00")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(sys, "argv", ["catchup_gate.py", "--phase", phase, "--report-date", "2026-10-21",
                                     "--event", "repository_dispatch",
                                     "--workflow", "daily-install-paid.yml", "--sent-step", "Deliver for "])
    monkeypatch.setattr(g, "sent_step_succeeded", unavailable)
    monkeypatch.setattr(g.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError) as exc:
        g.main()
    assert type(exc.value) is g.DedupeUnavailable
    captured = capsys.readouterr()
    assert "proceed=true" not in captured.out
    assert "private response body" not in captured.out + captured.err


if __name__ == "__main__":
    test_plan(); test_dedupe(); test_phase2_wait_is_absolute(); test_workflow_routes_dedupe(); print("ok")
