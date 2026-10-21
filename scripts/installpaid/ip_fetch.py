"""PostHog HogQL for the daily TravelAnimator install->paid email.

One person = one row in the inner query: first `Application Installed` in a 180-day
lookback (a reinstall never restarts the clock), the build and country of that first
install (every attribute is ifNull-wrapped inside argMinIf: argMinIf skips NULL values,
so a NULL on the first install would otherwise borrow a later reinstall's value), and every funnel event inside the first 24 hours. Intersections (export after
the first preview, paid among previewers) are worked out per person before anything is
counted, so the conditional rates are real, not ratios of marginal counts.

Logs from this module never carry figures: the repository is public.
"""
from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
import json
import os
import signal
import threading
import time
from pathlib import Path

import requests

HOST = os.environ.get("POSTHOG_HOST", "https://us.posthog.com").rstrip("/")
PROJECT = os.environ.get("POSTHOG_PROJECT_ID", "107752")
LOOKBACK_DAYS = 180
CHUNK_DAYS = 14           # whole-month cohorts time out (HTTP 504) as one query
QUERY_LIMIT = 50000       # the HogQL API silently returns 100 rows without an explicit LIMIT
H = 24                    # every rate in the email is reach within 24 hours of install
PLAT_OS = {"ios": "('iOS','iPadOS')", "android": "('Android')"}
CIDR_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "installpaid_google_ipranges.json"

PAID = ("event = 'subscription_started' AND properties.environment = 'Production' "
        "AND toFloat(properties.price_usd) > 0")
STEPS = [
    ("pw", "event = 'paywall_viewed'"),
    ("pv", "event = 'clicked_preview'"),
    # export_completed, not clicked_export: clicked_export never fires on Android 5.x
    ("ex", "event = 'export_completed'"),
    ("paid", PAID),                                   # server: Adapty via PostHog
    ("pc", "event = 'paywall_purchase_completed'"),   # client: in-app purchase event
]
COUNTS = ("n", "pw", "pv", "ex", "exr", "paid", "pc", "paid_pv", "pc_pv")


class Unavailable(Exception):
    """PostHog could not answer. The message is safe to log (no figures)."""


def load_cidrs(path=None):
    """Google's published ranges (gstatic ipranges/goog.json), pinned in the repo.
    Returns (cidrs, creation_time)."""
    g = json.loads(Path(path or os.environ.get("INSTALLPAID_CIDRS", CIDR_PATH)).read_text())
    return [p.get("ipv4Prefix") or p.get("ipv6Prefix") for p in g["prefixes"]], g.get("creationTime", "")


def _ts(x: dt.datetime) -> str:
    return x.strftime("%Y-%m-%d %H:%M:%S")


def _utc(x: dt.datetime) -> str:
    return f"toDateTime('{_ts(x)}', 'UTC')"


def google_ip_sql(cidrs, expr="ip"):
    e = f"if({expr} = '', '0.0.0.0', {expr})"
    return "(" + " OR ".join(f"isIPAddressInRange({e}, '{c}')" for c in cidrs) + ")"


def cohort_sql(start: dt.datetime, end: dt.datetime, cutoff: dt.datetime, plat: str, cidrs) -> str:
    """Installs with first install in [start, end), all naive UTC. Every install must
    be >= 24h old at `cutoff` (the caller guarantees end <= cutoff - 24h); events are
    scanned to min(end + 24h, cutoff)."""
    if end > cutoff - dt.timedelta(hours=H):
        raise ValueError("cohort end must be at least 24h before the cutoff")
    look = start - dt.timedelta(days=LOOKBACK_DAYS)
    scan_end = min(end + dt.timedelta(hours=H), cutoff)
    s = _utc(start)
    arrays = ",\n".join(
        f"    groupArrayIf(timestamp, {cond} AND timestamp >= {s}) AS a_{k}" for k, cond in STEPS)
    ev_or = " OR ".join(f"({cond})" for _, cond in STEPS)
    win = f"x >= t0 AND x < t0 + toIntervalHour({H})"
    return f"""
SELECT v, cc, gip, emu, miss, d,
       count() AS n,
       countIf(pw1) AS pw, countIf(pv1) AS pv, countIf(ex1) AS ex, countIf(exr1) AS exr,
       countIf(paid1) AS paid, countIf(pc1) AS pc,
       countIf(paid1 AND pv1) AS paid_pv, countIf(pc1 AND pv1) AS pc_pv
FROM (
 SELECT v, cc, gip, emu, miss, d, pw1, pv1, paid1, pc1, exr1,
        pv1 AND arrayExists(x -> x >= fp AND x < t0 + toIntervalHour({H}), a_ex) AS ex1
 FROM (
  SELECT ifNull(toString(v0), '') AS v, ifNull(toString(cc0), '') AS cc,
         {google_ip_sql(cidrs)} AS gip,
         startsWith(ifNull(toString(model), ''), 'sdk_') AS emu,
         (ip = '' OR ifNull(toString(model), '') = '') AS miss,
         toDate(toTimeZone(t0, 'America/Los_Angeles')) AS d,
         t0, a_ex,
         arrayExists(x -> {win}, a_pw) AS pw1,
         arrayExists(x -> {win}, a_pv) AS pv1,
         arrayExists(x -> {win}, a_ex) AS exr1,
         arrayExists(x -> {win}, a_paid) AS paid1,
         arrayExists(x -> {win}, a_pc) AS pc1,
         arrayMin(arrayFilter(x -> {win}, a_pv)) AS fp
  FROM (
   SELECT person_id,
     minIf(timestamp, event = 'Application Installed') AS t0,
     argMinIf(ifNull(toString(properties.$app_version), ''), timestamp, event = 'Application Installed') AS v0,
     argMinIf(ifNull(toString(properties.$geoip_country_code), ''), timestamp, event = 'Application Installed') AS cc0,
     argMinIf(ifNull(toString(properties.$os), ''), timestamp, event = 'Application Installed') AS os,
     argMinIf(ifNull(toString(properties.$device_model), ''), timestamp, event = 'Application Installed') AS model,
     argMinIf(ifNull(toString(properties.$ip), ''), timestamp, event = 'Application Installed') AS ip,
{arrays}
   FROM events
   WHERE ((event = 'Application Installed' AND timestamp >= {_utc(look)})
          OR (({ev_or}) AND timestamp >= {s}))
     AND timestamp < {_utc(scan_end)}
   GROUP BY person_id
  )
  WHERE t0 >= {s} AND t0 < {_utc(end)} AND os IN {PLAT_OS[plat]}
 )
)
GROUP BY v, cc, gip, emu, miss, d
LIMIT {QUERY_LIMIT}
""".strip()


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def check_deadline(deadline):
    if deadline is None:
        return float("inf")
    remaining = (deadline - utcnow()).total_seconds()
    if remaining <= 0:
        raise Unavailable("absolute build deadline reached")
    return remaining


@contextmanager
def absolute_deadline(deadline):
    """Interrupt blocking HTTP reads at the shared deadline, including trickles.

    Requests' timeout limits inactivity between reads, so it cannot enforce the
    overall budget. This CLI owns one Unix alarm while a query runs. A caller
    with another active alarm, or outside the main thread, must fail closed.
    """
    if deadline is None:
        yield
        return
    check_deadline(deadline)
    if (threading.current_thread() is not threading.main_thread()
            or not all(hasattr(signal, name) for name in (
                "SIGALRM", "ITIMER_REAL", "setitimer", "getitimer", "pthread_sigmask", "SIG_BLOCK"))):
        raise Unavailable("absolute build deadline cannot be enforced")
    previous_handler = signal.getsignal(signal.SIGALRM)
    if (previous_handler is None or signal.getitimer(signal.ITIMER_REAL)[0] > 0
            or signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, set())):
        raise Unavailable("absolute build deadline cannot be enforced")

    def expired(_signum, _frame):
        raise Unavailable("absolute build deadline reached") from None

    try:
        signal.signal(signal.SIGALRM, expired)
    except (OSError, ValueError):
        raise Unavailable("absolute build deadline cannot be enforced") from None
    try:
        try:
            signal.setitimer(signal.ITIMER_REAL, check_deadline(deadline))
        except (OSError, ValueError):
            raise Unavailable("absolute build deadline cannot be enforced") from None
        yield
    finally:
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        finally:
            signal.signal(signal.SIGALRM, previous_handler)


def make_runner(key: str, retries: int = 4, poll_s: int = 4, max_wait_s: int = 900, deadline=None):
    """run(sql) -> [row dicts]. Async query + polling: the synchronous endpoint's gateway
    returns HTTP 504 after ~16 s on these cohorts (seen 20 Oct 2026), while the same query
    finishes in seconds asynchronously. Retries submit/poll failures; raises Unavailable."""
    url = f"{HOST}/api/projects/{PROJECT}/query/"
    hdr = {"Authorization": f"Bearer {key}"}

    def pause(seconds):
        time.sleep(min(seconds, check_deadline(deadline)))
        check_deadline(deadline)

    def once(sql: str):
        r = requests.post(url, json={"query": {"kind": "HogQLQuery", "query": sql}, "refresh": "force_async"},
                          headers=hdr, timeout=min(60, check_deadline(deadline)))
        check_deadline(deadline)
        if r.status_code >= 500 or r.status_code == 429:
            raise requests.RequestException(f"HTTP {r.status_code}")      # retried
        if r.status_code not in (200, 202):
            raise Unavailable(f"PostHog query rejected (HTTP {r.status_code})")
        st = r.json().get("query_status") or {}
        res = r.json() if r.status_code == 200 and "results" in r.json() else None
        t0 = time.monotonic()
        while res is None:
            if not st.get("id"):
                raise requests.RequestException("no query id")
            if time.monotonic() - t0 > max_wait_s:
                raise requests.RequestException("poll timeout")
            pause(poll_s)
            p = requests.get(f"{url}{st['id']}/", headers=hdr, timeout=min(60, check_deadline(deadline)))
            check_deadline(deadline)
            if p.status_code not in (200, 202):     # 202 = still running
                raise requests.RequestException(f"poll HTTP {p.status_code}")
            st = p.json().get("query_status") or {}
            if st.get("complete"):
                if st.get("error"):
                    raise Unavailable("PostHog query error")
                res = st.get("results") or {}
        rows = [dict(zip(res["columns"], x)) for x in res.get("results") or []]
        if len(rows) >= QUERY_LIMIT or res.get("hasMore"):
            raise Unavailable("query hit the row limit; result would be truncated")
        return rows

    def attempts(sql: str):
        for i in range(retries):
            check_deadline(deadline)
            try:
                return once(sql)
            except requests.RequestException:
                pass  # exception text may contain a private response body or URL
            except (ValueError, KeyError, TypeError):
                raise Unavailable("PostHog returned an invalid response") from None
            if i + 1 < retries:
                pause(min(60, 5 * 2 ** i))
        raise Unavailable("PostHog query failed after retries")

    def run(sql: str):
        with absolute_deadline(deadline):
            return attempts(sql)

    return run


def definition_hash(cidrs, cidr_date, extra=""):
    """Fingerprint of the query text, horizon, lookback and CIDR list (plus `extra`,
    the model's filter rules, for aggregated benchmarks). Cached data whose fingerprint
    differs from the current code must not be reused."""
    import hashlib
    probe = "".join(cohort_sql(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2), dt.datetime(2000, 1, 5), p, cidrs)
                    for p in sorted(PLAT_OS))
    return hashlib.sha256(f"{probe}|{H}|{LOOKBACK_DAYS}|{cidr_date}|{extra}".encode()).hexdigest()[:16]


def chunks(start: dt.datetime, end: dt.datetime, days: int = CHUNK_DAYS):
    a = start
    while a < end:
        b = min(a + dt.timedelta(days=days), end)
        yield a, b
        a = b


def fetch_cohort(run, start, end, cutoff, cidrs, log=print, deadline=None):
    """{plat: [rows]} for installs in [start, end), queried in <= 14-day chunks."""
    out = {}
    for plat in PLAT_OS:
        rows = []
        for a, b in chunks(start, end):
            check_deadline(deadline)
            rows += run(cohort_sql(a, b, cutoff, plat, cidrs))
            check_deadline(deadline)
            log(f"fetched {plat} {a:%Y-%m-%d}..{b:%Y-%m-%d}")
        out[plat] = rows
    return out
