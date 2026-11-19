"""UTC-day PostHog facts for the TreasureHunch retention email.

The query endpoint is always asynchronous. Errors contain only safe reason words;
neither query text, response bodies nor figures are logged by this module.
"""
from __future__ import annotations

from contextlib import contextmanager
import datetime as dt
import os
import re
import signal
import threading
import time

import requests

HOST = "https://us.posthog.com"
PROJECT = "701052"
QUERY_LIMIT = 50000


class FetchError(Exception):
    """A safe reason word that the CLI may log without exposing private data."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def query_sql(report_date: dt.date) -> dict[str, str]:
    """The report's last full UTC day is the day before its Pacific send date."""
    last = report_date - dt.timedelta(days=1)
    start = last - dt.timedelta(days=7)
    cutoff = f"toDateTime('{report_date.isoformat()} 00:00:00', 'UTC')"
    return {
        "groups": f"""
WITH p AS (
    SELECT person_id,
           groupUniqArray(toDate(toTimeZone(timestamp, 'UTC'))) AS days,
           min(toDate(toTimeZone(timestamp, 'UTC'))) AS d0,
           argMin(ifNull(toString(properties.utm_medium), ''), timestamp) AS med
    FROM events
    WHERE event = 'hunt_started'
      AND timestamp >= toDateTime('2026-10-31 00:00:00', 'UTC')
      AND timestamp < {cutoff}
    GROUP BY person_id
), dp AS (
    SELECT DISTINCT person_id
    FROM events
    WHERE event = 'hunt_completed' AND properties.mode = 'daily'
      AND timestamp < {cutoff}
)
SELECT d0, count() AS n,
       countIf(has(days, d0 + 1)) AS d1,
       countIf(arrayExists(x -> x > d0 and x <= d0 + 3, days)) AS d3,
       countIf(arrayExists(x -> x > d0 and x <= d0 + 7, days)) AS d7any,
       countIf(has(days, d0 + 7)) AS d7,
       countIf(length(days) >= 3) AS days3,
       countIf(med = 'paid') AS paid,
       countIf(arrayExists(x -> x > d0, days)) AS returned,
       countIf(arrayExists(x -> x > d0, days)
               and person_id in (select person_id from dp)) AS returned_daily
FROM p
GROUP BY d0
ORDER BY d0
LIMIT {QUERY_LIMIT}
""".strip(),
        "days": f"""
SELECT toDate(toTimeZone(timestamp, 'UTC')) AS day,
       uniqExact(person_id) AS people,
       uniqExactIf(person_id, event = 'hunt_started') AS players,
       countIf(event = 'hunt_started') AS hunts,
       countIf(event = 'hunt_completed') AS finished,
       countIf(event = 'hunt_completed' AND properties.outcome = 'won') AS won,
       uniqExactIf(person_id, event = 'hunt_completed' AND properties.mode = 'daily') AS daily_players
FROM events
WHERE timestamp >= toDateTime('{start.isoformat()} 00:00:00', 'UTC')
  AND timestamp < {cutoff}
GROUP BY day
ORDER BY day
LIMIT {QUERY_LIMIT}
""".strip(),
        "new_events": f"""
SELECT event
FROM events
WHERE timestamp >= toDateTime('2026-10-29 00:00:00', 'UTC')
  AND timestamp < {cutoff}
GROUP BY event
HAVING toDate(toTimeZone(min(timestamp), 'UTC')) = toDate('{last.isoformat()}')
ORDER BY event
LIMIT {QUERY_LIMIT}
""".strip(),
        "new_versions": f"""
SELECT toDate(toTimeZone(min(timestamp), 'UTC')) AS day,
       ifNull(toString(properties.platform), '') AS platform,
       ifNull(toString(properties.app_version), '') AS version
FROM events
WHERE timestamp >= toDateTime('2026-10-29 00:00:00', 'UTC')
  AND timestamp < {cutoff}
  AND ifNull(toString(properties.app_version), '') != ''
GROUP BY platform, version
HAVING day >= toDate('2026-11-08')
ORDER BY day, platform, version
LIMIT {QUERY_LIMIT}
""".strip(),
    }


def check_deadline(deadline: dt.datetime | None) -> float:
    if deadline is None:
        return float("inf")
    remaining = (deadline - dt.datetime.now(dt.timezone.utc)).total_seconds()
    if remaining <= 0:
        raise FetchError("build-deadline")
    return remaining


@contextmanager
def absolute_deadline(deadline: dt.datetime | None):
    """Reuse install-paid's Unix alarm guard to stop even trickling HTTP reads.

    Requests' timeout bounds inactivity rather than the entire response. This CLI
    owns one alarm; another active alarm or a worker thread must fail closed.
    """
    if deadline is None:
        yield
        return
    check_deadline(deadline)
    if (threading.current_thread() is not threading.main_thread()
            or not all(hasattr(signal, name) for name in (
                "SIGALRM", "ITIMER_REAL", "setitimer", "getitimer", "pthread_sigmask", "SIG_BLOCK"))):
        raise FetchError("deadline-unavailable")
    previous_handler = signal.getsignal(signal.SIGALRM)
    if (previous_handler is None or signal.getitimer(signal.ITIMER_REAL)[0] > 0
            or signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, set())):
        raise FetchError("deadline-unavailable")

    def expired(_signum, _frame):
        raise FetchError("build-deadline") from None

    try:
        signal.signal(signal.SIGALRM, expired)
    except (OSError, ValueError):
        raise FetchError("deadline-unavailable") from None
    try:
        try:
            signal.setitimer(signal.ITIMER_REAL, check_deadline(deadline))
        except (OSError, ValueError):
            raise FetchError("deadline-unavailable") from None
        yield
    finally:
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        finally:
            signal.signal(signal.SIGALRM, previous_handler)


def rejected_reason(response) -> str:
    """Map a PostHog refusal to one status word; the body itself is never shown."""
    try:
        body = response.json()
        detail = str(body.get("detail", "")).lower()
        code = re.sub(r"[^a-z_]", "", str(body.get("code", "")).lower())[:40]
    except Exception:
        detail, code = "", ""
    if response.status_code == 401:
        return "posthog-key-invalid"
    if response.status_code == 403 and "scope" in detail:
        return "posthog-scope-missing"
    if response.status_code == 403 and any(w in detail for w in ("project", "team", "organization")):
        return "posthog-project-denied"
    return f"posthog-rejected-{response.status_code}" + (f"-{code}" if code else "")


def make_runner(key: str, retries: int = 4, poll_s: float = 4,
                max_wait_s: float = 900, deadline: dt.datetime | None = None):
    """Return run(sql) -> row dictionaries, with bounded retries and async polls."""
    url = f"{HOST}/api/projects/{PROJECT}/query/"
    headers = {"Authorization": f"Bearer {key}"}

    def pause(seconds):
        time.sleep(min(seconds, check_deadline(deadline)))
        check_deadline(deadline)

    def response_data(response):
        if response.status_code >= 500 or response.status_code == 429:
            raise requests.RequestException()
        if response.status_code not in (200, 202):
            raise FetchError(rejected_reason(response))
        body = response.json()
        if not isinstance(body, dict):
            raise FetchError("posthog-invalid-response")
        status = body.get("query_status") or {}
        if not isinstance(status, dict):
            raise FetchError("posthog-invalid-response")
        if status.get("error"):
            raise FetchError("posthog-query-error")
        result = body if response.status_code == 200 and "results" in body else None
        if status.get("complete") and result is None:
            result = status.get("results")
            if not isinstance(result, dict):
                raise FetchError("posthog-invalid-response")
        return status, result

    def once(sql):
        response = requests.post(
            url, json={"query": {"kind": "HogQLQuery", "query": sql}, "refresh": "force_async"},
            headers=headers, timeout=min(60, check_deadline(deadline)),
        )
        check_deadline(deadline)
        status, result = response_data(response)
        query_id = status.get("id")
        started = time.monotonic()
        while result is None:
            if not query_id or time.monotonic() - started >= max_wait_s:
                raise requests.RequestException()
            pause(poll_s)
            response = requests.get(
                f"{url}{query_id}/", headers=headers,
                timeout=min(60, check_deadline(deadline)),
            )
            check_deadline(deadline)
            status, result = response_data(response)
        columns, values = result["columns"], result["results"]
        if (not isinstance(columns, list) or not all(isinstance(c, str) for c in columns)
                or not isinstance(values, list)
                or any(not isinstance(row, list) or len(row) != len(columns) for row in values)):
            raise FetchError("posthog-invalid-response")
        if len(values) >= QUERY_LIMIT or result.get("hasMore"):
            raise FetchError("posthog-truncated")
        return [dict(zip(columns, row)) for row in values]

    def run(sql):
        with absolute_deadline(deadline):
            for attempt in range(retries):
                check_deadline(deadline)
                try:
                    return once(sql)
                except requests.RequestException:
                    pass  # Exceptions can contain a private URL or response body.
                except (ValueError, KeyError, TypeError):
                    raise FetchError("posthog-invalid-response") from None
                if attempt + 1 < retries:
                    pause(min(60, 5 * 2 ** attempt))
            raise FetchError("posthog-unavailable") from None

    return run


def fetch_all(report_date: dt.date, deadline: dt.datetime | None = None) -> dict:
    """Fetch player groups, eight engagement days and first-seen events/versions."""
    key = os.environ.get("POSTHOG_PERSONAL_API_KEY", "").strip()
    if not key:
        raise FetchError("posthog-key-missing")
    run = make_runner(key, deadline=deadline)
    data = {name: run(sql) for name, sql in query_sql(report_date).items()}
    try:
        data["new_events"] = [row["event"] for row in data["new_events"]]
    except (KeyError, TypeError):
        raise FetchError("posthog-invalid-response") from None
    check_deadline(deadline)
    return data
