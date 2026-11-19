"""Delivery contracts, with the network replaced at its HTTP boundary."""
import datetime as dt
import importlib
import json
from pathlib import Path

import pytest
import requests


UTC = dt.timezone.utc
REPORT_DATE = dt.date(2026, 11, 19)


def sender():
    return importlib.import_module("hr_send")


def workflow():
    return importlib.import_module("hr_workflow")


def write_email(path, subject="TreasureHunch retention"):
    (path / "email.json").write_text(json.dumps({
        "subject": subject, "html": "<p>private-metric-value</p>",
        "text": "private-metric-value",
    }))


@pytest.mark.parametrize("event,recovery,want", [
    ("schedule", False, "hunch-retention-2026-11-19"),
    ("workflow_dispatch", False, "hunch-retention-2026-11-19-manual-12345"),
    ("workflow_dispatch", True, "hunch-retention-2026-11-19"),
])
def test_send_keys_separate_tests_from_real_delivery(event, recovery, want):
    assert sender().send_key(REPORT_DATE, event, "12345", recovery) == want


@pytest.mark.parametrize("event,recovery", [("schedule", False), ("workflow_dispatch", True)])
def test_real_sends_reject_an_alternate_recipient(event, recovery):
    with pytest.raises(ValueError):
        sender().recipient(event, recovery, "someone@example.com")


def test_manual_test_can_override_the_recipient():
    assert sender().recipient("workflow_dispatch", False, "someone@example.com") == "someone@example.com"


@pytest.mark.parametrize("now,want", [
    ("2026-11-19T13:52:59+00:00", "2026-11-19T13:55:00+00:00"),
    ("2026-11-19T13:53:00+00:00", "2026-11-19T13:55:00+00:00"),
    ("2026-11-19T13:53:01+00:00", None),
])
def test_send_schedule_switches_to_immediate_after_0553(now, want):
    value = sender().scheduled_time(REPORT_DATE, dt.datetime.fromisoformat(now))
    assert (value.isoformat() if value else None) == want


def test_freeze_preserves_payload_bytes_and_schedule_on_retry(tmp_path):
    s = sender()
    write_email(tmp_path)
    path = s.freeze_payload(tmp_path, REPORT_DATE, "12345", "schedule",
                            now=lambda: dt.datetime(2026, 11, 19, 13, 52, tzinfo=UTC))
    frozen = path.read_bytes()
    body = json.loads(frozen)
    assert body["from"] == "Lascade Games <noreply@metrics.lascade.com>"
    assert body["to"] == [sender().TO]
    assert body["scheduled_at"] == "2026-11-19T13:55:00+00:00"
    write_email(tmp_path, subject="Changed")
    again = s.freeze_payload(tmp_path, REPORT_DATE, "12345", "schedule",
                             now=lambda: dt.datetime(2026, 11, 19, 14, 0, tzinfo=UTC))
    assert again.read_bytes() == frozen


@pytest.mark.parametrize("subject,want", [
    ("TreasureHunch retention", "[Test 7] TreasureHunch retention"),
    ("[Test 7] TreasureHunch retention", "[Test 7] TreasureHunch retention"),
])
def test_manual_payload_always_has_one_test_prefix(tmp_path, monkeypatch, subject, want):
    write_email(tmp_path, subject=subject)
    monkeypatch.setenv("HR_TEST_NUMBER", "7")
    path = sender().freeze_payload(tmp_path, REPORT_DATE, "12345", "workflow_dispatch",
                                   to="someone@example.com",
                                   now=lambda: dt.datetime(2026, 11, 19, 14, 0, tzinfo=UTC))
    body = json.loads(path.read_bytes())
    assert body["subject"] == want
    assert body["to"] == ["someone@example.com"]
    assert "scheduled_at" not in body


def test_frozen_payload_cannot_silently_change_recipients(tmp_path):
    write_email(tmp_path)
    sender().freeze_payload(tmp_path, REPORT_DATE, "12345", "workflow_dispatch",
                            to="someone@example.com")
    with pytest.raises(ValueError):
        sender().freeze_payload(tmp_path, REPORT_DATE, "12345", "workflow_dispatch",
                                to="another@example.com")


class Response:
    def __init__(self, status, body):
        self.status_code = status
        self.body = body

    def json(self):
        return self.body


def payload(tmp_path, scheduled=False):
    path = tmp_path / "payload.json"
    body = {"subject": "private-metric-value", "html": "private-metric-value"}
    if scheduled:
        body["scheduled_at"] = "2026-11-19T13:55:00+00:00"
    path.write_text(json.dumps(body))
    return path


def test_concurrent_409_retries_identical_bytes_and_key(tmp_path, capsys):
    path = payload(tmp_path)
    sent = []
    responses = iter([
        Response(409, {"name": "concurrent_idempotent_requests", "message": "private-metric-value"}),
        Response(200, {"id": "private-id"}),
    ])
    delays = []

    def post(url, **kwargs):
        sent.append((url, kwargs["data"], kwargs["headers"]["Idempotency-Key"]))
        return next(responses)

    assert sender().send_payload(path, "frozen-key", "secret", post=post, sleep=delays.append)
    assert sent == [("https://api.resend.com/emails", path.read_bytes(), "frozen-key")] * 2
    assert delays == [10]
    captured = capsys.readouterr()
    assert "private" not in captured.out + captured.err
    assert all(len(line.split()) == 1 for line in (captured.out + captured.err).splitlines())


@pytest.mark.parametrize("status,body", [
    (409, {"name": "invalid_idempotent_request", "message": "private-metric-value"}),
    (201, {"id": "private-id"}),
    (202, {"id": "private-id"}),
])
def test_only_200_is_accepted_and_key_reuse_conflict_is_terminal(tmp_path, status, body):
    calls = []

    def post(url, **kwargs):
        calls.append(url)
        return Response(status, body)

    assert not sender().send_payload(payload(tmp_path), "key", "secret", post=post,
                                     sleep=lambda seconds: pytest.fail("unexpected-retry"))
    assert len(calls) == 1


def test_network_failure_retries_without_changing_payload(tmp_path):
    path = payload(tmp_path)
    received = []
    delays = []

    def post(url, **kwargs):
        received.append(kwargs["data"])
        if len(received) == 1:
            raise requests.Timeout("private-network-body")
        return Response(200, {"id": "accepted"})

    assert sender().send_payload(path, "key", "secret", post=post, sleep=delays.append)
    assert received == [path.read_bytes(), path.read_bytes()]
    assert delays == [15]


def test_scheduled_retry_stops_before_submission_window_closes(tmp_path):
    assert not sender().send_payload(
        payload(tmp_path, scheduled=True), "key", "secret",
        clock=lambda: dt.datetime(2026, 11, 19, 13, 54, 1, tzinfo=UTC),
        post=lambda *a, **kw: pytest.fail("late-scheduled-send"),
    )


@pytest.mark.parametrize("event,recovery,want", [
    ("schedule", False, "Deliver for 2026-11-19"),
    ("workflow_dispatch", True, "Deliver for 2026-11-19"),
    ("workflow_dispatch", False, "Test send for 2026-11-19"),
])
def test_delivery_step_records_only_real_sends(event, recovery, want):
    w = workflow()
    assert w.delivery_step(REPORT_DATE, w.delivery_mode(event, recovery)) == want


@pytest.mark.parametrize("now,want", [
    ("2026-11-19T12:59:00+00:00", dt.date(2026, 11, 18)),
    ("2026-11-19T13:00:00+00:00", dt.date(2026, 11, 19)),
])
def test_manual_blank_date_uses_newest_pacific_date_past_0500(monkeypatch, now, want):
    monkeypatch.setenv("EVENT", "workflow_dispatch")
    monkeypatch.setenv("NOW", now)
    monkeypatch.delenv("REPORT_DATE", raising=False)
    monkeypatch.delenv("INPUT_DATE", raising=False)
    monkeypatch.delenv("RECOVERY", raising=False)
    date, mode, to = workflow().settings()
    assert date == want
    assert mode == "test"
    assert to == workflow().TO


def test_settings_write_outputs_without_logging_dates(tmp_path, monkeypatch, capsys):
    output = tmp_path / "github-output"
    monkeypatch.setenv("EVENT", "workflow_dispatch")
    monkeypatch.setenv("INPUT_DATE", "2026-11-19")
    monkeypatch.setenv("RECOVERY", "true")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert workflow().main(["settings"]) == 0
    outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert outputs["report_date"] == "2026-11-19"
    assert outputs["send_step"] == "Deliver for 2026-11-19"
    assert capsys.readouterr().out == "settings-ok\n"


def test_dedupe_uses_bare_delivery_prefix_and_skips_already_sent(tmp_path, monkeypatch):
    w = workflow()
    monkeypatch.setenv("EVENT", "workflow_dispatch")
    monkeypatch.setenv("INPUT_DATE", "2026-11-19")
    monkeypatch.setenv("RECOVERY", "true")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))
    history = []

    def already_sent(date, now, **kwargs):
        history.append((date, kwargs))
        return True

    monkeypatch.setattr(w.G, "already_sent", already_sent)
    assert w.main(["dedupe"]) == 0
    assert history == [(REPORT_DATE, {"workflow": "daily-hunch-retention.yml", "sent_step": "Deliver for "})]
    assert (tmp_path / "outputs").read_text() == "sent=True\n"


def test_history_unavailable_refuses_recovery(tmp_path, monkeypatch, capsys):
    w = workflow()
    monkeypatch.setenv("EVENT", "workflow_dispatch")
    monkeypatch.setenv("INPUT_DATE", "2026-11-19")
    monkeypatch.setenv("RECOVERY", "true")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))

    def unavailable(*a, **kw):
        raise w.G.DedupeUnavailable("private-body")

    monkeypatch.setattr(w.G, "already_sent", unavailable)
    assert w.main(["dedupe"]) == 3
    assert not (tmp_path / "outputs").exists()
    assert capsys.readouterr().err == "history-unavailable\n"
