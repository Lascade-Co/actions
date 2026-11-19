"""Build integration, safe logs, and the Pacific deadline boundary."""
import datetime as dt
from contextlib import contextmanager
import json
import re

import pytest

from preview_fixture import REPORT_DATE, preview_inputs

UTC = dt.timezone.utc


@pytest.mark.parametrize("date,started,expected", [
    ("2026-11-19", "2026-11-19T13:05:00+00:00", "2026-11-19T13:48:00+00:00"),
    ("2026-07-19", "2026-07-19T12:05:00+00:00", "2026-07-19T12:48:00+00:00"),
    ("2026-11-19", "2026-11-19T13:48:00+00:00", None),
    ("2026-11-19", "2026-11-19T14:00:00+00:00", None),
    ("2026-11-18", "2026-11-19T13:05:00+00:00", None),
])
def test_build_deadline_matches_pacific_schedule(date, started, expected):
    from hr_main import build_deadline

    deadline = build_deadline(dt.date.fromisoformat(date), dt.datetime.fromisoformat(started))
    assert (deadline.isoformat() if deadline else None) == expected


def test_main_builds_real_fixture_json_without_printing_figures(monkeypatch, tmp_path, capsys):
    import hr_main as M

    monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "fixture-key")
    monkeypatch.delenv("CLAUDE_API_KEY", raising=False)
    monkeypatch.setenv("HR_TEST_NUMBER", "42")
    monkeypatch.setattr(M, "utc_now", lambda: dt.datetime(2026, 11, 19, 14, tzinfo=UTC))
    monkeypatch.setattr(M.F, "fetch_all", lambda report_date, deadline=None: preview_inputs())
    assert M.main(["--report-date", str(REPORT_DATE), "--out", str(tmp_path), "--dry-run"]) == 0
    email = json.loads((tmp_path / "email.json").read_text())
    assert set(email) == {"subject", "html", "text"}
    assert email["subject"].startswith("[Test 42] TreasureHunch")
    assert "8.6%" in email["html"]
    assert "Summary unavailable today." in email["html"]
    captured = capsys.readouterr()
    assert captured.out == "summary: fallback:auth\nrendered\n"
    assert captured.err == ""
    assert not re.search(r"\d", captured.out)


def test_main_missing_posthog_key_fails_safely(monkeypatch, tmp_path, capsys):
    import hr_main as M

    monkeypatch.delenv("POSTHOG_PERSONAL_API_KEY", raising=False)
    assert M.main(["--report-date", "2026-11-19", "--out", str(tmp_path)]) != 0
    captured = capsys.readouterr()
    assert captured.out + captured.err == "posthog-key-missing\n"
    assert not (tmp_path / "email.json").exists()


def test_main_does_not_expose_unexpected_fetch_errors(monkeypatch, tmp_path, capsys):
    import hr_main as M

    monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "fixture-key")
    monkeypatch.setattr(M, "utc_now", lambda: dt.datetime(2026, 11, 19, 14, tzinfo=UTC))

    def fail(*args, **kwargs):
        raise RuntimeError("private query body: 675 players")

    monkeypatch.setattr(M.F, "fetch_all", fail)
    assert M.main(["--report-date", "2026-11-19", "--out", str(tmp_path)]) != 0
    captured = capsys.readouterr()
    assert captured.out + captured.err == "build-failed\n"
    assert not (tmp_path / "email.json").exists()


def test_main_rejects_a_report_before_the_pacific_cutoff(monkeypatch, tmp_path, capsys):
    import hr_main as M

    monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "fixture-key")
    monkeypatch.setattr(M, "utc_now", lambda: dt.datetime(2026, 11, 19, 12, 59, tzinfo=UTC))
    assert M.main(["--report-date", "2026-11-19", "--out", str(tmp_path)]) != 0
    captured = capsys.readouterr()
    assert captured.out + captured.err == "report-not-ready\n"


def test_main_stops_after_fetch_crosses_the_build_deadline(monkeypatch, tmp_path, capsys):
    import hr_main as M

    now = [dt.datetime(2026, 11, 19, 13, 5, tzinfo=UTC)]
    monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "fixture-key")
    monkeypatch.setattr(M, "utc_now", lambda: now[0])

    def fetch(*args, **kwargs):
        now[0] = dt.datetime(2026, 11, 19, 13, 48, tzinfo=UTC)
        return preview_inputs()

    monkeypatch.setattr(M.F, "fetch_all", fetch)
    assert M.main(["--report-date", "2026-11-19", "--out", str(tmp_path)]) != 0
    captured = capsys.readouterr()
    assert captured.out + captured.err == "deadline\n"
    assert not (tmp_path / "email.json").exists()


def test_stalled_summary_leaves_time_to_render_fallback(monkeypatch, tmp_path, capsys):
    """An API stream reaching its alarm must not consume the build deadline."""
    import hr_main as M

    now = [dt.datetime(2026, 11, 19, 13, 5, tzinfo=UTC)]
    active_deadline = [None]
    monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "fixture-key")
    monkeypatch.delenv("HR_TEST_NUMBER", raising=False)
    monkeypatch.setattr(M, "utc_now", lambda: now[0])
    monkeypatch.setattr(M.F, "fetch_all", lambda *args, **kwargs: preview_inputs())

    @contextmanager
    def deadline_guard(deadline):
        active_deadline[0] = deadline
        yield
        active_deadline[0] = None

    def stalled_api(*args, **kwargs):
        now[0] = active_deadline[0] + dt.timedelta(microseconds=1)
        raise M.C.ClaudeError("request-failed:FetchError")

    monkeypatch.setattr(M.F, "absolute_deadline", deadline_guard)
    monkeypatch.setattr(M.C, "generate_text", stalled_api)
    assert M.main(["--report-date", "2026-11-19", "--out", str(tmp_path)]) == 0
    email = json.loads((tmp_path / "email.json").read_text())
    assert "Summary unavailable today." in email["html"]
    assert "8.6%" in email["html"]
    captured = capsys.readouterr()
    assert captured.out == "summary: fallback:deadline\nrendered\n"
    assert captured.err == ""
