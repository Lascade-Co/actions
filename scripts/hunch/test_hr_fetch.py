"""PostHog boundary tests use synthetic data and never contact the service."""
from __future__ import annotations

import datetime as dt
import signal
import time

import pytest
import requests

import hr_fetch as fetch


class Response:
    def __init__(self, status, body):
        self.status_code = status
        self.body = body

    def json(self):
        return self.body


def test_missing_key_fails_with_safe_reason_before_http(monkeypatch, capsys):
    monkeypatch.delenv("POSTHOG_PERSONAL_API_KEY", raising=False)
    monkeypatch.setattr(requests, "post", lambda *a, **k: pytest.fail("unexpected HTTP"))
    with pytest.raises(fetch.FetchError) as exc:
        fetch.fetch_all(dt.date(2026, 11, 19))
    assert exc.value.reason == "posthog-key-missing"
    assert str(exc.value) == "posthog-key-missing"
    assert capsys.readouterr() == ("", "")


def test_fetch_emits_utc_queries_and_maps_all_four_results(monkeypatch, capsys):
    monkeypatch.setenv("POSTHOG_PERSONAL_API_KEY", "synthetic-key")
    submitted = []
    results = iter([
        {"columns": ["d0", "n", "d1", "d3", "d7any", "d7", "days3", "paid", "returned", "returned_daily"],
         "results": [["2026-11-08", 40, 6, 8, 9, 2, 3, 15, 10, 8]]},
        {"columns": ["day", "people", "players", "hunts", "finished", "won", "daily_players"],
         "results": [["2026-11-18", 60, 30, 50, 40, 36, 20]]},
        {"columns": ["event"], "results": [["new_daily_event"]]},
        {"columns": ["day", "platform", "version"],
         "results": [["2026-11-16", "web", "0.2.0"], ["2026-11-18", "ios", "1.0.0"]]},
    ])

    def post(url, *, json, headers, timeout):
        assert url == "https://us.posthog.com/api/projects/701052/query/"
        assert headers == {"Authorization": "Bearer synthetic-key"}
        assert json["refresh"] == "force_async"
        assert json["query"]["kind"] == "HogQLQuery"
        assert 0 < timeout <= 60
        submitted.append(json["query"]["query"])
        return Response(200, next(results))

    monkeypatch.setattr(requests, "post", post)
    actual = fetch.fetch_all(dt.date(2026, 11, 19))
    assert actual == {
        "groups": [{"d0": "2026-11-08", "n": 40, "d1": 6, "d3": 8,
                    "d7any": 9, "d7": 2, "days3": 3, "paid": 15,
                    "returned": 10, "returned_daily": 8}],
        "days": [{"day": "2026-11-18", "people": 60, "players": 30,
                  "hunts": 50, "finished": 40, "won": 36, "daily_players": 20}],
        "new_events": ["new_daily_event"],
        "new_versions": [{"day": "2026-11-16", "platform": "web", "version": "0.2.0"},
                         {"day": "2026-11-18", "platform": "ios", "version": "1.0.0"}],
    }
    assert len(submitted) == 4
    for sql in submitted:
        assert "toDate(toTimeZone(" in sql
        assert "toDate(timestamp, 'UTC')" not in sql
        assert "timestamp < toDateTime('2026-11-19 00:00:00', 'UTC')" in sql
        assert "LIMIT " in sql
    groups, days, events, versions = submitted
    assert "timestamp >= toDateTime('2026-10-31 00:00:00', 'UTC')" in groups
    assert "groupUniqArray(toDate(toTimeZone(timestamp, 'UTC')))" in groups
    assert "argMin(ifNull(toString(properties.utm_medium), ''), timestamp)" in groups
    assert "has(days, d0 + 1)" in groups
    assert "x > d0 and x <= d0 + 3" in groups
    assert "x > d0 and x <= d0 + 7" in groups
    assert "has(days, d0 + 7)" in groups
    assert "length(days) >= 3" in groups
    assert "person_id in (select person_id from dp)" in groups
    assert "timestamp >= toDateTime('2026-11-11 00:00:00', 'UTC')" in days
    assert "uniqExact(person_id) AS people" in days
    assert "properties.outcome = 'won'" in days
    assert "properties.mode = 'daily'" in days
    assert "timestamp >= toDateTime('2026-10-29 00:00:00', 'UTC')" in events
    assert "= toDate('2026-11-18')" in events
    assert "properties.app_version" in versions and "properties.platform" in versions
    assert ">= toDate('2026-11-08')" in versions
    assert capsys.readouterr() == ("", "")


def test_async_pending_responses_are_polled_until_complete(monkeypatch):
    monkeypatch.setattr(requests, "post", lambda *a, **k: Response(202, {"query_status": {"id": "synthetic"}}))
    responses = iter([
        Response(202, {"query_status": {"id": "synthetic", "complete": False}}),
        Response(200, {"query_status": {"complete": True, "error": False,
                                      "results": {"columns": ["n"], "results": [[40]]}}}),
    ])

    def get(url, **kwargs):
        assert url == "https://us.posthog.com/api/projects/701052/query/synthetic/"
        return next(responses)

    monkeypatch.setattr(requests, "get", get)
    assert fetch.make_runner("synthetic", poll_s=0)("synthetic query") == [{"n": 40}]


def test_transient_submit_failure_is_retried_without_private_exception(monkeypatch, capsys):
    responses = iter([
        Response(504, {"private": "data"}),
        Response(200, {"columns": ["n"], "results": [[40]]}),
    ])
    monkeypatch.setattr(requests, "post", lambda *a, **k: next(responses))
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    assert fetch.make_runner("synthetic", retries=2)("synthetic query") == [{"n": 40}]
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("status,body,reason", [
    (401, {"detail": "private response"}, "posthog-key-invalid"),
    (403, {"detail": "API key missing required scope 'query:read'"}, "posthog-scope-missing"),
    (403, {"detail": "You don't have access to the project."}, "posthog-project-denied"),
    (403, {"detail": "private response", "code": "permission_denied"}, "posthog-rejected-403-permission_denied"),
    (403, ["private"], "posthog-rejected-403"),
    (200, {"query_status": {"complete": True, "error": "private query"}}, "posthog-query-error"),
    (200, {"columns": ["n"], "results": [[40]], "hasMore": True}, "posthog-truncated"),
    (200, {"columns": ["n", "paid"], "results": [[40]]}, "posthog-invalid-response"),
    (200, ["private", "invalid"], "posthog-invalid-response"),
])
def test_api_errors_never_expose_response_bodies(monkeypatch, capsys, status, body, reason):
    monkeypatch.setattr(requests, "post", lambda *a, **k: Response(status, body))
    with pytest.raises(fetch.FetchError) as exc:
        fetch.make_runner("synthetic", retries=1, poll_s=0)("synthetic query")
    assert exc.value.reason == reason
    assert str(exc.value) == reason
    assert capsys.readouterr() == ("", "")


def test_network_failure_has_only_sanitized_reason(monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise requests.RequestException("private-body-key-person")

    monkeypatch.setattr(requests, "post", fail)
    with pytest.raises(fetch.FetchError) as exc:
        fetch.make_runner("synthetic", retries=1)("synthetic query")
    assert exc.value.reason == "posthog-unavailable"
    assert str(exc.value) == "posthog-unavailable"
    assert capsys.readouterr() == ("", "")


def test_poll_timeout_stops_waiting(monkeypatch):
    monkeypatch.setattr(requests, "post", lambda *a, **k: Response(202, {"query_status": {"id": "synthetic"}}))
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("poll exceeded its budget"))
    with pytest.raises(fetch.FetchError) as exc:
        fetch.make_runner("synthetic", retries=1, poll_s=0, max_wait_s=0)("synthetic query")
    assert exc.value.reason == "posthog-unavailable"


def test_elapsed_deadline_prevents_any_http(monkeypatch):
    monkeypatch.setattr(requests, "post", lambda *a, **k: pytest.fail("unexpected HTTP"))
    deadline = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
    with pytest.raises(fetch.FetchError) as exc:
        fetch.make_runner("synthetic", deadline=deadline)("synthetic query")
    assert exc.value.reason == "build-deadline"


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="Unix deadline enforcement")
def test_absolute_deadline_interrupts_blocking_http_and_restores_alarm(monkeypatch):
    def blocked(*args, **kwargs):
        time.sleep(2)
        return Response(200, {"columns": [], "results": []})

    monkeypatch.setattr(requests, "post", blocked)
    previous = signal.getsignal(signal.SIGALRM)
    start = time.monotonic()
    deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=0.15)
    with pytest.raises(fetch.FetchError) as exc:
        fetch.make_runner("synthetic", deadline=deadline)("synthetic query")
    assert exc.value.reason == "build-deadline"
    assert time.monotonic() - start < 0.75
    assert signal.getsignal(signal.SIGALRM) == previous
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0
