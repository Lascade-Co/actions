"""Edition and delivery contracts; all payloads and query results are synthetic."""
import datetime as dt
import json
from pathlib import Path

import pytest

UTC = dt.timezone.utc


def instant(value):
    return dt.datetime.fromisoformat(value).replace(tzinfo=UTC)


class TestEditionDelivery:
    def test_settings_isolate_recipients_steps_and_files(self):
        from ip_editions import get_edition, resolve_daily_schedule
        day = dt.date(2030, 1, 12)
        pt, ist = get_edition("pt"), get_edition("ist")
        assert pt.recipient == "cherian@lascade.com"
        assert ist.recipient == "fasna@lascade.com"
        assert pt.delivery_step(day) == "Deliver for 2030-01-12"
        assert ist.delivery_step(day) == "Deliver IST for 2030-01-12"
        assert pt.send_key(day, "schedule", "sample") == "install-paid-pt-2030-01-12-scheduled"
        assert ist.send_key(day, "workflow_dispatch", "sample") == "install-paid-ist-2030-01-12-manual-sample"
        for event in ("schedule", "workflow_dispatch"):
            assert pt.send_key(day, event, "sample") != ist.send_key(day, event, "sample")
            assert pt.concurrency_group(event) != ist.concurrency_group(event)
        assert pt.payload_name(day, "sample") != ist.payload_name(day, "sample")
        assert pt.cache_name(instant("2030-01-12T12:00")) != ist.cache_name(instant("2030-01-12T12:00"))
        for hour in (3, 6, 9):
            assert resolve_daily_schedule(f"41 {hour} * * *") == "pt"
        for hour in (13, 16, 19):
            assert resolve_daily_schedule(f"7 {hour} * * *") == "ist"
        with pytest.raises(ValueError):
            get_edition("external@example.invalid")

    @pytest.mark.parametrize("edition,day,cutoff", [
        ("pt", "2026-03-08", "2026-03-08T12:00"),
        ("pt", "2026-11-01", "2026-11-01T13:00"),
        ("ist", "2030-01-12", "2030-01-11T23:30"),
    ])
    def test_cutoff_and_ready_date(self, edition, day, cutoff):
        import ip_main as main
        date = dt.date.fromisoformat(day)
        ready = instant(cutoff)
        assert main.cutoff_for(date, edition) == ready.replace(tzinfo=None)
        assert main.latest_ready_date(ready, edition) == date
        assert main.latest_ready_date(ready - dt.timedelta(seconds=1), edition) == date - dt.timedelta(days=1)

    @pytest.mark.parametrize("started,frozen,want", [
        ("05:05:00", "05:52:59", "05:55:00"),
        ("05:47:59", "05:53:00", "05:55:00"),
        ("05:05:00", "05:53:01", None),
        ("05:48:00", "05:49:00", None),
        ("06:20:00", "06:21:00", None),
    ])
    @pytest.mark.parametrize("edition", ["pt", "ist"])
    def test_timing_table_uses_build_start_and_freeze_time(self, edition, started, frozen, want):
        from ip_editions import get_edition
        from ip_main import build_deadline
        from ip_send import scheduled_time
        e, date = get_edition(edition), dt.date(2030, 1, 12)
        local = lambda t: dt.datetime.fromisoformat(f"2030-01-12T{t}").replace(tzinfo=e.zone).astimezone(UTC)
        deadline = build_deadline(date, edition, local(started))
        timing = {"edition": edition, "report_date": str(date), "started_at": local(started).isoformat(),
                  "deadline": deadline.isoformat() if deadline else None}
        got = scheduled_time(date, e, timing, local(frozen))
        assert got == (local(want) if want else None)

    @pytest.mark.parametrize("edition,day,want", [
        ("pt", "2026-03-09", ("Sat 7 Mar", "1–7 Mar", "PT")),
        ("pt", "2026-11-02", ("Sat 31 Oct", "25–31 Oct", "PT")),
        ("ist", "2030-01-12", ("Wed 9 Jan", "3–9 Jan", "IST")),
    ])
    def test_week_window_and_cutoff_label_across_dst(self, edition, day, want, tmp_path):
        import ip_main as main
        import ip_model as model
        import ip_render_email as render
        date = dt.date.fromisoformat(day)
        benchmark = {key: {platform: {"counts": dict.fromkeys(model.COUNTS, 0)}
                           for platform in ("ios", "android")} for key in ("mar26", "aug25")}
        m = model.build_model({"ios": [], "android": []}, benchmark, date, main.cutoff_for(date, edition), "synthetic", "synthetic")
        m["edition"] = edition
        subject, html, text, _ = render.render(m, "A", tmp_path)
        last, week, label = want
        assert subject.startswith(f"TA install→paid · {last}:")
        assert f"latest full day {last} · last 7 days {week}" in html and f"data to 05:00 {label}" in text

    def test_payload_is_immutable_and_edition_bound(self, tmp_path):
        import ip_send as send
        for name, value in (("email-A.html", "<p>synthetic</p>"), ("email-A.txt", "synthetic"),
                            ("subject-A.txt", "synthetic subject")):
            (tmp_path / name).write_text(value)
        (tmp_path / "build-timing.json").write_text(json.dumps({
            "edition": "pt", "report_date": "2030-01-12", "started_at": "2030-01-12T13:05:00+00:00",
            "deadline": "2030-01-12T13:48:00+00:00"}))
        args = (tmp_path, "pt", dt.date(2030, 1, 12), "synthetic", "schedule")
        payload = send.freeze_payload(*args, now=lambda: instant("2030-01-12T13:52"))
        frozen = payload.read_bytes()
        body = json.loads(frozen)
        assert body["to"] == ["cherian@lascade.com"]
        assert body["scheduled_at"] == "2030-01-12T13:55:00+00:00"
        assert payload.name == "payload-pt-2030-01-12-synthetic.json"
        (tmp_path / "subject-A.txt").write_text("changed source")
        assert send.freeze_payload(*args, now=lambda: instant("2030-01-12T14:00")).read_bytes() == frozen
        with pytest.raises(ValueError):
            send.freeze_payload(tmp_path, "ist", dt.date(2030, 1, 12), "synthetic", "schedule")

    def test_sender_retries_identical_bytes_and_enforces_start_deadline(self, tmp_path):
        import ip_send as send
        payload = tmp_path / "payload.json"
        content = {"subject": "synthetic private content", "scheduled_at": "2030-01-12T13:55:00+00:00"}
        payload.write_text(json.dumps(content))
        now = [instant("2030-01-12T13:53:50")]
        attempts = []
        def post(*args, **kwargs):
            attempts.append(kwargs["data"])
            return type("Response", (), {"status_code": 503})()
        def sleep(seconds):
            now[0] += dt.timedelta(seconds=seconds)
        assert not send.send_payload(payload, "synthetic", "synthetic", clock=lambda: now[0], sleep=sleep, post=post)
        assert len(attempts) == 1
        assert attempts[0] == payload.read_bytes()

    def test_concurrent_conflict_retries_but_other_conflicts_fail(self, tmp_path, capsys):
        import ip_send as send
        payload = tmp_path / "payload.json"
        payload.write_text('{"subject":"private synthetic content"}')
        class Response:
            def __init__(self, status, name):
                self.status_code, self.name = status, name
            def json(self):
                return {"name": self.name, "private": "must never log this"}
        responses = [Response(409, "concurrent_idempotent_requests"), Response(200, "accepted")]
        sent = []
        def post(*args, **kw):
            sent.append(kw["data"])
            return responses.pop(0)
        assert send.send_payload(payload, "synthetic", "synthetic", post=post, sleep=lambda _: None)
        assert sent == [payload.read_bytes(), payload.read_bytes()]
        assert not send.send_payload(payload, "synthetic", "synthetic", post=lambda *a, **k: Response(409, "invalid_idempotent_request"))
        logged = capsys.readouterr()
        assert "private" not in logged.out + logged.err

    def test_test_schedule_is_manual_pt_only(self):
        from ip_editions import get_edition
        from ip_send import scheduled_time
        now = instant("2030-01-12T20:00")
        assert scheduled_time(dt.date(2030, 1, 12), get_edition("pt"), {}, now,
                              event="workflow_dispatch", schedule_in_min=3) == instant("2030-01-12T20:03")
        for event, edition in (("schedule", "pt"), ("workflow_dispatch", "ist")):
            with pytest.raises(ValueError):
                scheduled_time(dt.date(2030, 1, 12), get_edition(edition), {}, now,
                               event=event, schedule_in_min=3)

    def test_absolute_deadline_caps_poll_wait_and_network_timeout(self, monkeypatch):
        import ip_fetch as fetch
        now = [instant("2030-01-12T13:47:57")]
        deadline = instant("2030-01-12T13:48:00")
        calls = []
        monkeypatch.setattr(fetch, "utcnow", lambda: now[0], raising=False)
        def post(*args, **kwargs):
            calls.append(kwargs["timeout"])
            return type("Response", (), {"status_code": 202, "json": lambda self: {"query_status": {"id": "synthetic"}}})()
        def sleep(seconds):
            now[0] += dt.timedelta(seconds=seconds)
        monkeypatch.setattr(fetch.requests, "post", post)
        monkeypatch.setattr(fetch.requests, "get", lambda *a, **kw: pytest.fail("poll started after deadline"))
        monkeypatch.setattr(fetch.time, "sleep", sleep)
        with pytest.raises(fetch.Unavailable, match="deadline"):
            fetch.make_runner("synthetic", deadline=deadline, poll_s=4)("synthetic query")
        assert calls == [3]
        assert now[0] <= deadline

    def test_expired_deadline_stops_chunk_loop(self, monkeypatch):
        import ip_fetch as fetch
        cutoff = dt.datetime(2030, 1, 12)
        monkeypatch.setattr(fetch, "utcnow", lambda: instant("2030-01-12T13:48"), raising=False)
        with pytest.raises(fetch.Unavailable, match="deadline"):
            fetch.fetch_cohort(lambda _: pytest.fail("expired chunk submitted"), cutoff - dt.timedelta(days=2),
                               cutoff - dt.timedelta(days=1), cutoff, ["192.0.2.0/24"],
                               deadline=instant("2030-01-12T13:48"))

    def test_transport_failure_never_echoes_exception_body(self, monkeypatch, capsys):
        import ip_fetch as fetch
        monkeypatch.setattr(fetch.time, "sleep", lambda _: None)
        def post(*a, **kw):
            raise fetch.requests.RequestException("synthetic private response body")
        monkeypatch.setattr(fetch.requests, "post", post)
        with pytest.raises(fetch.Unavailable) as caught:
            fetch.make_runner("synthetic", retries=1)("synthetic query")
        assert "private" not in str(caught.value)

    @pytest.mark.parametrize("edition", ["pt", "ist"])
    def test_workflow_modes_keep_force_and_test_out_of_dedupe(self, edition):
        from ip_workflow import delivery_mode, delivery_step
        day = dt.date(2030, 1, 12)
        assert delivery_mode("schedule", recovery=True, force=True) == "real"
        assert delivery_mode("workflow_dispatch", recovery=True, force=False) == "real"
        assert delivery_mode("workflow_dispatch", recovery=True, force=True) == "forced"
        assert delivery_mode("workflow_dispatch", recovery=False, force=False) == "test"
        assert delivery_step(edition, day, "forced") == "Forced send for 2030-01-12"
        assert delivery_step(edition, day, "test") == "Test send for 2030-01-12"
        assert delivery_step(edition, day, "real") == ("Deliver for 2030-01-12" if edition == "pt" else "Deliver IST for 2030-01-12")

    def test_workflow_settings_resolve_edition_from_cron_and_ignore_manual_flags(self, monkeypatch, capsys):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "schedule")
        monkeypatch.setenv("SCHEDULE", "7 13 * * *")
        monkeypatch.setenv("INPUT_EDITION", "pt")
        monkeypatch.setenv("FORCE_RESEND", "true")
        monkeypatch.setenv("SCHEDULE_IN_MIN", "3")
        monkeypatch.setenv("NOW", "2030-01-11T13:07:00+00:00")
        assert workflow.main(["settings"]) == 0
        logged = capsys.readouterr().out
        assert "edition=ist\n" in logged and "timezone=Asia/Kolkata\n" in logged
        assert "report_date=2030-01-12\n" in logged
        assert "mode=real\n" in logged and "schedule_in_min=0\n" in logged

    def test_workflow_manual_latest_ready_date_is_local(self, monkeypatch, capsys):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "workflow_dispatch")
        monkeypatch.setenv("INPUT_EDITION", "ist")
        monkeypatch.setenv("DRY_RUN", "true")
        monkeypatch.setenv("NOW", "2030-01-11T23:29:00+00:00")
        assert workflow.main(["settings"]) == 0
        assert "report_date=2030-01-11\n" in capsys.readouterr().out

    def test_workflow_failure_message_includes_edition_date_and_recovery(self, monkeypatch, capsys):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "workflow_dispatch")
        monkeypatch.setenv("INPUT_EDITION", "ist")
        monkeypatch.setenv("REPORT_DATE", "2030-01-12")
        monkeypatch.setenv("RUN_URL", "https://example.invalid/run")
        monkeypatch.setenv("WF_URL", "https://example.invalid/workflow")
        assert workflow.main(["failure-message"]) == 0
        text = capsys.readouterr().out
        assert "IST edition for 2030-01-12" in text
        assert "edition=ist, report_date=2030-01-12, recovery ✓" in text

    @pytest.mark.parametrize("bad_input,value", [("SCHEDULE_IN_MIN", "1.5"), ("INPUT_DATE", "invalid")])
    def test_failure_notification_survives_bad_manual_inputs(self, bad_input, value, monkeypatch, capsys):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "workflow_dispatch")
        monkeypatch.setenv("INPUT_EDITION", "pt")
        monkeypatch.setenv("NOW", "2030-01-12T14:00:00+00:00")
        monkeypatch.setenv(bad_input, value)
        assert workflow.main(["failure-message"]) == 0
        assert "PT edition for 2030-01-12" in capsys.readouterr().out

    def test_real_cache_boundary_cannot_reuse_another_edition(self, tmp_path, monkeypatch):
        import ip_fetch as fetch
        import ip_main as main
        monkeypatch.setattr(main, "CACHE", tmp_path)
        monkeypatch.setattr(fetch, "load_cidrs", lambda: (["192.0.2.0/24"], "synthetic"))
        monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "synthetic")
        calls = []
        def runner(key, **kwargs):
            calls.append(kwargs["deadline"])
            return lambda sql: []
        monkeypatch.setattr(fetch, "make_runner", runner)
        date, cutoff = dt.date(2030, 1, 12), dt.datetime(2030, 1, 12, 12)
        deadline = instant("2030-01-12T13:48")
        for edition in ("pt", "ist", "pt", "ist"):
            assert main.fetch_window(date, cutoff, True, edition, deadline) == {"ios": [], "android": []}
        assert calls == [deadline, deadline]
        assert sorted(path.name for path in tmp_path.iterdir()) == [
            "window-ist-2030-01-12T12:00:00Z.local.json", "window-pt-2030-01-12T12:00:00Z.local.json"]

    @pytest.mark.parametrize("started,want_deadline", [
        ("2030-01-12T13:05", "2030-01-12T13:48:00+00:00"),
        ("2030-01-12T13:48", None),
        ("2030-01-12T14:20", None),
    ])
    def test_builder_records_actual_step_start_and_passes_deadline(self, monkeypatch, tmp_path, started, want_deadline):
        import ip_fetch as fetch
        import ip_main as main
        import ip_model as model
        now = instant(started)
        class Clock(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)
        counts = dict.fromkeys(model.COUNTS, 0)
        counts["n"] = 1
        benchmark = {key: {platform: {"counts": counts} for platform in ("ios", "android")}
                     for key in ("mar26", "aug25")}
        cidrs, stamp = fetch.load_cidrs()
        benchmark["definition"] = fetch.definition_hash(cidrs, stamp, model.RULES)
        monkeypatch.setattr(main.dt, "datetime", Clock)
        monkeypatch.setattr(main, "load_bench", lambda: benchmark)
        fetched = []
        def window(*args):
            fetched.append(args)
            return {"ios": [], "android": []}
        monkeypatch.setattr(main, "fetch_window", window)
        assert main.main(["--edition", "pt", "--report-date", "2030-01-12", "--out", str(tmp_path)]) == 0
        timing = json.loads((tmp_path / "build-timing.json").read_text())
        assert timing["started_at"] == now.isoformat()
        assert timing["deadline"] == want_deadline
        assert fetched[0][3] == "pt"
        assert fetched[0][4] == (dt.datetime.fromisoformat(want_deadline) if want_deadline else None)

    @pytest.mark.parametrize("edition", ["pt", "ist"])
    @pytest.mark.parametrize("flag", ["RECOVERY"])   # forced sends skip history (see below)
    def test_manual_dedupe_uncertainty_fails_closed(self, monkeypatch, capsys, edition, flag):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "workflow_dispatch")
        monkeypatch.setenv("INPUT_EDITION", edition)
        monkeypatch.setenv("RECOVERY", "false")
        monkeypatch.setenv("FORCE_RESEND", "false")
        monkeypatch.setenv(flag, "true")
        monkeypatch.setenv("REPORT_DATE", "2030-01-12")
        def unavailable(*args, **kwargs):
            raise workflow.G.DedupeUnavailable("private response must not be logged")
        monkeypatch.setattr(workflow.G, "already_sent", unavailable)
        assert workflow.main(["dedupe"]) == 3
        out = capsys.readouterr()
        assert "sent=False" not in out.out and "private" not in out.err

    @pytest.mark.parametrize("edition,schedule", [("pt", "41 3 * * *"), ("ist", "7 13 * * *")])
    def test_scheduled_dedupe_uncertainty_proceeds_on_the_scheduled_key(self, monkeypatch, capsys, edition, schedule):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "schedule")
        monkeypatch.setenv("SCHEDULE", schedule)
        monkeypatch.setenv("REPORT_DATE", "2030-01-12")
        def unavailable(*args, **kwargs):
            raise workflow.G.DedupeUnavailable("private response must not be logged")
        monkeypatch.setattr(workflow.G, "already_sent", unavailable)
        assert workflow.main(["dedupe"]) == 0
        out = capsys.readouterr()
        assert out.out == "sent=False\n"
        assert out.err.strip() == "delivery history unavailable; proceeding on scheduled key"

    @pytest.mark.parametrize("edition", ["pt", "ist"])
    @pytest.mark.parametrize("history", ["accepted", "unavailable"])
    def test_forced_send_skips_history_entirely(self, monkeypatch, capsys, edition, history):
        import ip_workflow as workflow
        monkeypatch.setenv("EVENT", "workflow_dispatch")
        monkeypatch.setenv("INPUT_EDITION", edition)
        monkeypatch.setenv("FORCE_RESEND", "true")
        monkeypatch.setenv("REPORT_DATE", "2030-01-12")
        checked = []
        def lookup(*args, **kwargs):
            checked.append(kwargs["sent_step"])
            if history == "unavailable":
                raise workflow.G.DedupeUnavailable("private response must not be logged")
            return True
        monkeypatch.setattr(workflow.G, "already_sent", lookup)
        assert workflow.main(["dedupe"]) == 0
        assert checked == []
        assert capsys.readouterr().out == "sent=False\n"

    def test_one_minute_test_is_rejected_before_build_or_payload(self, monkeypatch, tmp_path, capsys):
        import ip_workflow as workflow
        import ip_send as sender
        monkeypatch.setenv("EVENT", "workflow_dispatch")
        monkeypatch.setenv("INPUT_EDITION", "pt")
        monkeypatch.setenv("SCHEDULE_IN_MIN", "1")
        assert workflow.main(["settings"]) == 2
        monkeypatch.setenv("RESEND_API_KEY", "synthetic")
        assert sender.main(["--edition", "pt", "--event", "workflow_dispatch", "--run-id", "synthetic",
                            "--report-date", "2030-01-12", "--out", str(tmp_path), "--schedule-in-min", "1"]) == 2
        assert not list(tmp_path.iterdir())
