"""Offline watchdog behavior; fixtures contain timestamps and delivery state only."""
import datetime as dt
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "catchup"))
import ip_watchdog as W


def utc(value):
    return dt.datetime.fromisoformat(value).replace(tzinfo=dt.timezone.utc)


def trusted_run(run_id=7, **kw):
    return {"id": run_id, "run_attempt": 1, "event": "schedule", "head_branch": "main",
            "head_repository": {"full_name": "Lascade-Co/actions"}, **kw}


def history(runs, steps):
    """A GitHub response with separate job results for each attempt."""
    def fetch(path):
        if "/jobs" in path:
            match = re.search(r"/runs/(\d+)/attempts/(\d+)/jobs", path)
            assert match, "each run attempt must be inspected explicitly"
            return {"jobs": [{"name": "watch", "conclusion": "failure",
                              "steps": steps.get(tuple(map(int, match.groups())), [])}]}
        return {"workflow_runs": runs}
    return fetch


def step(name, conclusion="success"):
    return {"name": name, "conclusion": conclusion}


class TestWatchdogEditions:
    @pytest.mark.parametrize("edition,phase,stamp,want", [
        ("pt", "prep", "2026-07-01T12:29:59", False),
        ("pt", "prep", "2026-07-01T12:30:00", True),
        ("pt", "prep", "2026-07-01T12:59:59", True),
        ("pt", "prep", "2026-07-01T13:00:00", False),
        ("pt", "deliver", "2026-07-01T13:14:59", False),
        ("pt", "deliver", "2026-07-01T13:15:00", True),
        ("pt", "deliver", "2026-07-01T18:59:59", True),
        ("pt", "deliver", "2026-07-01T19:00:00", False),
        ("ist", "prep", "2026-07-01T00:00:00", True),
        ("ist", "prep", "2026-07-01T00:30:00", False),
        ("ist", "deliver", "2026-07-01T00:44:59", False),
        ("ist", "deliver", "2026-07-01T00:45:00", True),
        ("ist", "deliver", "2026-07-01T06:29:59", True),
        ("ist", "deliver", "2026-07-01T06:30:00", False),
    ])
    def test_local_window_edges(self, monkeypatch, edition, phase, stamp, want):
        monkeypatch.setattr(W.G, "already_sent", lambda *a, **k: False)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: False)
        assert W.decide(utc(stamp), edition=edition, phase=phase)[0] is want

    @pytest.mark.parametrize("season", ["2026-07-01", "2026-12-01"])
    def test_every_cron_in_both_dst_seasons(self, monkeypatch, season):
        from ip_editions import resolve_watchdog_schedule
        monkeypatch.setattr(W.G, "already_sent", lambda *a, **k: False)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: False)
        summer = season == "2026-07-01"
        cron_cases = [
            ("30 12,13 * * *", "pt", "prep", [(12, 30, summer), (13, 30, not summer)]),
            ("15 13-19 * * *", "pt", "deliver", [(13, 15, summer), (14, 15, True),
             (15, 15, True), (16, 15, True), (17, 15, True), (18, 15, True), (19, 15, not summer)]),
            ("0 0 * * *", "ist", "prep", [(0, 0, True)]),
            ("45 0-5 * * *", "ist", "deliver", [(0, 45, True), (1, 45, True),
             (2, 45, True), (3, 45, True), (4, 45, True), (5, 45, True)]),
        ]
        for cron, edition, phase, cases in cron_cases:
            assert resolve_watchdog_schedule(cron) == (edition, phase)
            for hour, minute, want in cases:
                stamp = utc(f"{season}T{hour:02}:{minute:02}:00")
                assert W.decide(stamp, edition=edition, phase=phase)[0] is want, (cron, stamp)

    @pytest.mark.parametrize("edition,phase,stamp,expected_step", [
        ("pt", "prep", "2026-07-01T12:30:00", "Deliver for "),
        ("pt", "deliver", "2026-07-01T13:15:00", "Deliver for "),
        ("ist", "prep", "2026-07-01T00:00:00", "Deliver IST for "),
        ("ist", "deliver", "2026-07-01T00:45:00", "Deliver IST for "),
    ])
    def test_successful_delivery_suppresses_its_edition(self, monkeypatch, edition, phase, stamp, expected_step):
        def sent(today, now, **kw):
            return kw["workflow"] == "daily-install-paid.yml" and kw["sent_step"] == expected_step
        monkeypatch.setattr(W.G, "already_sent", sent)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: False)
        assert W.decide(utc(stamp), edition=edition, phase=phase)[0] is False

    @pytest.mark.parametrize("edition,phase,alert_name", [
        ("pt", "prep", "Prep alert PT for 2026-07-01"),
        ("pt", "deliver", "Alert PT for 2026-07-01"),
        ("ist", "prep", "Prep alert IST for 2026-07-01"),
        ("ist", "deliver", "Alert IST for 2026-07-01"),
    ])
    def test_alert_dedupe_is_edition_and_phase_specific(self, edition, phase, alert_name):
        today = dt.date(2026, 7, 1)
        for existing in ("Prep alert PT for 2026-07-01", "Alert PT for 2026-07-01",
                         "Prep alert IST for 2026-07-01", "Alert IST for 2026-07-01"):
            fetch = history([trusted_run()], {(7, 1): [step(existing)]})
            assert W.alerted_today(today, fetch, edition=edition, phase=phase) is (existing == alert_name)

    def test_all_attempts_include_current_run_and_ignore_job_failure(self, monkeypatch):
        monkeypatch.setenv("GITHUB_RUN_ID", "7")
        today = dt.date(2026, 7, 1)
        fetch = history([trusted_run(run_attempt=3)], {(7, 1): [step("Alert IST for 2026-07-01")]})
        assert W.alerted_today(today, fetch, edition="ist", phase="deliver")

    @pytest.mark.parametrize("bad", [
        {"event": "pull_request"}, {"event": "push"}, {"head_branch": "experiment"},
        {"head_repository": {"full_name": "elsewhere/actions"}}, {"head_repository": None},
    ])
    def test_untrusted_run_cannot_suppress_alert(self, bad):
        fetch = history([trusted_run(**bad)], {(7, 1): [step("Alert IST for 2026-07-01")]})
        assert not W.alerted_today(dt.date(2026, 7, 1), fetch, edition="ist", phase="deliver")

    def test_failed_or_other_date_alert_does_not_suppress(self):
        fetch = history([trusted_run()], {(7, 1): [step("Alert IST for 2026-07-01", "failure"),
                                                  step("Alert IST for 2026-06-30")]})
        assert not W.alerted_today(dt.date(2026, 7, 1), fetch, edition="ist", phase="deliver")

    def test_history_failure_alerts_without_logging_exception_body(self, monkeypatch, capsys):
        monkeypatch.setattr(W.G, "already_sent", lambda *a, **k: False)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("private-response")))
        assert W.decide(utc("2026-07-01T00:00:00"), edition="ist", phase="prep")[0]
        assert "private-response" not in capsys.readouterr().err

    @pytest.mark.parametrize("edition,phase,stamp,alert_name,word", [
        ("pt", "prep", "2026-07-01T12:30:00", "Prep alert PT for 2026-07-01", "at risk"),
        ("pt", "deliver", "2026-07-01T13:15:00", "Alert PT for 2026-07-01", "missed"),
        ("ist", "prep", "2026-07-01T00:00:00", "Prep alert IST for 2026-07-01", "at risk"),
        ("ist", "deliver", "2026-07-01T00:45:00", "Alert IST for 2026-07-01", "missed"),
    ])
    def test_cli_outputs_recovery_instructions(self, monkeypatch, tmp_path, edition, phase, stamp, alert_name, word):
        monkeypatch.setenv("NOW", stamp + "+00:00")
        output = tmp_path / "outputs"
        monkeypatch.setenv("GITHUB_OUTPUT", str(output))
        monkeypatch.setattr(W.G, "already_sent", lambda *a, **k: False)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: False)
        assert W.main(["--edition", edition, "--phase", phase]) == 0
        values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        assert values["alert"] == "true"
        assert values["alert_step"] == alert_name
        assert values["date"] == "2026-07-01"
        assert word in values["message"]
        assert f"{edition.upper()} edition" in values["message"]
        assert f"Run workflow → edition={edition}, report_date=2026-07-01, recovery ✓" in values["message"]

    @pytest.mark.parametrize("event,schedule,chosen,phase,want", [
        ("schedule", "30 12,13 * * *", "ist", "deliver", ("pt", "prep")),
        ("schedule", "15 13-19 * * *", "ist", "prep", ("pt", "deliver")),
        ("schedule", "0 0 * * *", "pt", "deliver", ("ist", "prep")),
        ("schedule", "45 0-5 * * *", "pt", "prep", ("ist", "deliver")),
        ("workflow_dispatch", "", "pt", "prep", ("pt", "prep")),
        ("workflow_dispatch", "", "ist", "deliver", ("ist", "deliver")),
    ])
    def test_workflow_selects_the_same_edition_and_phase(self, tmp_path, event, schedule, chosen, phase, want):
        import yaml
        root = HERE.parents[1]
        workflow = yaml.load((root / ".github/workflows/install-paid-watchdog.yml").read_text(), Loader=yaml.BaseLoader)
        selector = next((s for s in workflow["jobs"]["watch"]["steps"] if s.get("id") == "edition"), None)
        assert selector is not None, "workflow must resolve its edition through the shared settings"
        output = tmp_path / "outputs"
        env = dict(os.environ, EVENT_NAME=event, SCHEDULE=schedule, INPUT_EDITION=chosen,
                   INPUT_PHASE=phase, GITHUB_OUTPUT=str(output),
                   PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
        result = subprocess.run(["bash", "-e", "-c", selector["run"]], cwd=root, env=env,
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        assert (values["edition"], values["phase"]) == want
        check = next(s for s in workflow["jobs"]["watch"]["steps"] if s.get("id") == "check")
        # Exercise the actual build-to-check boundary with an offline GitHub API.
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        gh = fake_bin / "gh"
        gh.write_text("#!/bin/sh\nprintf '{\"workflow_runs\":[]}\\n'\n")
        gh.chmod(0o755)
        contexts = {"github.token": "synthetic", **{f"steps.edition.outputs.{k}": v for k, v in values.items()}}
        for key, expression in check["env"].items():
            match = re.fullmatch(r"\$\{\{\s*(.*?)\s*\}\}", expression)
            env[key] = contexts.get(match[1], "") if match else expression
        stamp = {("pt", "prep"): "2026-07-01T12:30:00+00:00",
                 ("pt", "deliver"): "2026-07-01T13:15:00+00:00",
                 ("ist", "prep"): "2026-07-01T00:00:00+00:00",
                 ("ist", "deliver"): "2026-07-01T00:45:00+00:00"}[want]
        env.update(NOW=stamp, PATH=str(fake_bin) + os.pathsep + env["PATH"])
        result = subprocess.run(["bash", "-e", "-c", check["run"]], cwd=root, env=env,
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        checked = dict(line.split("=", 1) for line in output.read_text().splitlines())
        assert (checked["edition"], checked["phase"]) == want
        assert checked["alert"] == "true"

    def test_alert_from_an_old_run_rerun_today_is_found(self):
        today = dt.date(2026, 7, 1)
        def fetch(path):
            if "/jobs" in path:
                return {"jobs": [{"steps": [step("Prep alert IST for 2026-07-01")]}]}
            # GitHub filters by original creation time, not the rerun's start.
            runs = [] if "created=" in path else [trusted_run(created_at="2026-01-01T00:00:00Z", run_attempt=2)]
            return {"workflow_runs": runs}
        assert W.alerted_today(today, fetch, edition="ist", phase="prep")

    def test_alert_in_later_run_and_job_pages_is_found(self):
        today = dt.date(2026, 7, 1)
        def fetch(path):
            if "/jobs" in path:
                if "page=2" in path:
                    return {"jobs": [{"steps": [step("Alert PT for 2026-07-01")]}]}
                return {"jobs": [{"steps": []}] * 100}
            if "page=2" in path:
                return {"workflow_runs": [trusted_run()]}
            return {"workflow_runs": [trusted_run(event="pull_request")] * 100}
        assert W.alerted_today(today, fetch, edition="pt", phase="deliver")

    def test_unavailable_delivery_history_still_warns_without_leaking_errors(self, monkeypatch, capsys):
        def unavailable(*args, **kwargs):
            raise W.G.DedupeUnavailable("synthetic private response")
        monkeypatch.setattr(W.G, "already_sent", unavailable)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: False)
        assert W.decide(utc("2026-07-01T00:00:00"), edition="ist", phase="prep")[0]
        assert "private" not in capsys.readouterr().err

    def test_unavailable_delivery_history_still_dedupes_its_alert_phase(self, monkeypatch):
        def unavailable(*args, **kwargs):
            raise W.G.DedupeUnavailable("history unavailable")
        monkeypatch.setattr(W.G, "already_sent", unavailable)
        monkeypatch.setattr(W, "alerted_today", lambda *a, **k: True)
        assert not W.decide(utc("2026-07-01T00:00:00"), edition="ist", phase="prep")[0]
