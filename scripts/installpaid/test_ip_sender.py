"""Sender safety checks with synthetic private content and no external requests."""
import datetime as dt
import json

import pytest
import requests


def utc(value):
    return dt.datetime.fromisoformat(value).replace(tzinfo=dt.timezone.utc)


def sources(out, edition="pt", day="2030-01-12", started="2030-01-12T13:05:00+00:00",
            deadline="2030-01-12T13:48:00+00:00"):
    for name, value in (("email-A.html", "<p>private synthetic body</p>"),
                        ("email-A.txt", "private synthetic text"),
                        ("subject-A.txt", "private synthetic subject\n")):
        (out / name).write_text(value)
    (out / "build-timing.json").write_text(json.dumps({
        "edition": edition, "report_date": day, "started_at": started, "deadline": deadline}))


class TestSender:
    @pytest.mark.parametrize("status,accepted", [(200, True), (201, False), (400, False), (403, False)])
    def test_acceptance_requires_http_200(self, tmp_path, capsys, status, accepted):
        import ip_send as send
        payload = tmp_path / "payload.json"
        payload.write_text('{"subject":"private synthetic subject"}')

        class Response:
            status_code = status
            text = "private synthetic response"

        assert send.send_payload(payload, "synthetic", "synthetic", post=lambda *a, **k: Response()) is accepted
        output = capsys.readouterr()
        assert "private" not in output.out + output.err

    def test_network_errors_retry_four_times_without_body_logging(self, tmp_path, capsys):
        import ip_send as send
        payload = tmp_path / "payload.json"
        payload.write_text('{"subject":"private synthetic subject"}')
        attempted, sleeps = [], []

        def post(*args, **kwargs):
            attempted.append(kwargs["data"])
            raise requests.Timeout("private synthetic transport detail")

        assert not send.send_payload(payload, "synthetic", "synthetic", post=post, sleep=sleeps.append)
        assert attempted == [payload.read_bytes()] * 4
        assert sleeps == [15, 30, 45]
        output = capsys.readouterr()
        assert "private" not in output.out + output.err

    @pytest.mark.parametrize("offset,attempts", [(0, 1), (1, 0)])
    def test_scheduled_attempt_start_boundary_is_inclusive(self, tmp_path, offset, attempts):
        import ip_send as send
        payload = tmp_path / "payload.json"
        payload.write_text('{"scheduled_at":"2030-01-12T13:55:00+00:00"}')
        calls = []

        def post(*args, **kwargs):
            calls.append(kwargs["data"])
            return type("Response", (), {"status_code": 200})()

        now = utc("2030-01-12T13:54:00") + dt.timedelta(microseconds=offset)
        assert send.send_payload(payload, "synthetic", "synthetic", clock=lambda: now, post=post) is bool(attempts)
        assert len(calls) == attempts
        assert json.loads(payload.read_text())["scheduled_at"] == "2030-01-12T13:55:00+00:00"

    def test_payload_freeze_uses_clock_after_source_reads(self, tmp_path, monkeypatch):
        import ip_send as send
        sources(tmp_path)
        original = type(tmp_path).read_text
        now = [utc("2030-01-12T13:52:00")]

        def read(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path.name == "subject-A.txt":
                now[0] = utc("2030-01-12T13:53:01")
            return result

        monkeypatch.setattr(type(tmp_path), "read_text", read)
        path = send.freeze_payload(tmp_path, "pt", dt.date(2030, 1, 12), "synthetic", "schedule", now=lambda: now[0])
        assert "scheduled_at" not in json.loads(path.read_text())

    def test_frozen_recipient_mismatch_is_rejected(self, tmp_path):
        import ip_send as send
        sources(tmp_path)
        payload = tmp_path / "payload-pt-2030-01-12-synthetic.json"
        payload.write_text('{"to":["external@example.invalid"],"subject":"synthetic"}')
        with pytest.raises(ValueError):
            send.freeze_payload(tmp_path, "pt", dt.date(2030, 1, 12), "synthetic", "schedule")

    @pytest.mark.parametrize("change", [
        {"edition": "ist"}, {"report_date": "2030-01-11"},
        {"started_at": "2030-01-12T13:05:00"}, {"deadline": "2030-01-12T13:50:00+00:00"},
        {"started_at": "2030-01-12T13:49:00+00:00"},
    ])
    def test_payload_rejects_inconsistent_build_timing(self, tmp_path, change):
        import ip_send as send
        sources(tmp_path)
        timing = json.loads((tmp_path / "build-timing.json").read_text())
        (tmp_path / "build-timing.json").write_text(json.dumps({**timing, **change}))
        with pytest.raises(ValueError):
            send.freeze_payload(tmp_path, "pt", dt.date(2030, 1, 12), "synthetic", "schedule")
        assert not list(tmp_path.glob("payload-*.json"))

    @pytest.mark.parametrize("extra", [[], ["--test-tag", "synthetic"], ["--schedule-in-min", "3"]])
    def test_cli_forbids_ist_test_send(self, tmp_path, monkeypatch, extra):
        import ip_send as send
        monkeypatch.setenv("RESEND_API_KEY", "synthetic")
        calls = []
        monkeypatch.setattr(send, "send_payload", lambda *a, **k: calls.append(a) or True)
        assert send.main(["--edition", "ist", "--report-date", "2030-01-12", "--event", "workflow_dispatch",
                          "--run-id", "synthetic", "--out", str(tmp_path), *extra]) != 0
        assert calls == []

    @pytest.mark.parametrize("flag", ["--recovery", "--force-resend"])
    def test_cli_permits_ist_recovery_and_force_with_edition_key(self, tmp_path, monkeypatch, flag):
        import ip_send as send
        sources(tmp_path, "ist", started="2030-01-12T02:00:00+00:00", deadline=None)
        monkeypatch.setenv("RESEND_API_KEY", "synthetic")
        calls = []
        monkeypatch.setattr(send, "send_payload", lambda *a, **k: calls.append(a) or True)
        assert send.main(["--edition", "ist", "--report-date", "2030-01-12", "--event", "workflow_dispatch",
                          "--run-id", "synthetic", "--out", str(tmp_path), flag]) == 0
        path, key, _ = calls[0]
        assert key == "install-paid-ist-2030-01-12-manual-synthetic"
        payload = json.loads(path.read_text())
        assert payload["to"] == ["fasna@lascade.com"]
        assert not payload["subject"].startswith("[Test")

    def test_cli_tags_plain_manual_pt_subject_without_changing_source(self, tmp_path, monkeypatch):
        import ip_send as send
        sources(tmp_path, started="2030-01-12T15:00:00+00:00", deadline=None)
        monkeypatch.setenv("RESEND_API_KEY", "synthetic")
        calls = []
        monkeypatch.setattr(send, "send_payload", lambda *a, **k: calls.append(a) or True)
        assert send.main(["--edition", "pt", "--report-date", "2030-01-12", "--event", "workflow_dispatch",
                          "--run-id", "synthetic", "--out", str(tmp_path), "--test-tag", "example"]) == 0
        path, key, _ = calls[0]
        assert key == "install-paid-pt-2030-01-12-manual-synthetic"
        assert json.loads(path.read_text())["subject"] == "[Test example] private synthetic subject"
        assert (tmp_path / "subject-A.txt").read_text() == "private synthetic subject\n"

    def test_cli_ignores_force_on_cron(self, tmp_path, monkeypatch):
        import ip_send as send
        sources(tmp_path, started="2030-01-12T15:00:00+00:00", deadline=None)
        monkeypatch.setenv("RESEND_API_KEY", "synthetic")
        calls = []
        monkeypatch.setattr(send, "send_payload", lambda *a, **k: calls.append(a) or True)
        assert send.main(["--edition", "pt", "--report-date", "2030-01-12", "--event", "schedule",
                          "--run-id", "synthetic", "--out", str(tmp_path), "--force-resend"]) == 0
        assert calls[0][1] == "install-paid-pt-2030-01-12-scheduled"

    @pytest.mark.parametrize("flag", ["--recovery", "--force-resend"])
    def test_cli_test_schedule_cannot_schedule_recovery_or_force(self, tmp_path, monkeypatch, flag):
        import ip_send as send
        monkeypatch.setenv("RESEND_API_KEY", "synthetic")
        calls = []
        monkeypatch.setattr(send, "send_payload", lambda *a, **k: calls.append(a) or True)
        assert send.main(["--edition", "pt", "--report-date", "2030-01-12", "--event", "workflow_dispatch",
                          "--run-id", "synthetic", "--out", str(tmp_path), "--schedule-in-min", "3", flag]) != 0
        assert calls == []
