"""Live check of the cohort query's per-person logic against an independently written
join-based query, one build per track, field by field (every count, split by country and by the test /
missing-data flags). There is no ClickHouse offline, so this is the
executable test of the SQL semantics (first install wins, 24 h window, export after the
first preview, paid regardless of preview order). Prints MATCH / MISMATCH only: the
repository is public.

    POSTHOG_PERSONAL_API_KEY=... python3 ip_crosscheck.py --report-date 2026-10-20 [--builds ios:4.1.1,android:4.0.155]
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

import ip_fetch as F
import ip_main


def _t(x):
    return f"toDateTime('{x:%Y-%m-%d %H:%M:%S}', 'UTC')"


KEYS = ("cc", "gip", "emu", "miss", "d")
FIELDS = F.COUNTS


def independent_sql(plat, v, start, end, cutoff, cidrs):
    """Same definitions, different construction: a join instead of per-person arrays.
    Timestamps stay DateTime64, so sub-second event order is respected."""
    win = "ev.timestamp >= inst.t0 AND ev.timestamp < inst.t0 + interval 24 hour"
    none = "toDateTime('1971-01-01 00:00:00', 'UTC')"
    return f"""
WITH inst AS (
  SELECT person_id, min(timestamp) AS t0,
         argMin(ifNull(toString(properties.$app_version), ''), timestamp) AS v,
         argMin(ifNull(toString(properties.$os), ''), timestamp) AS os,
         argMin(ifNull(toString(properties.$geoip_country_code), ''), timestamp) AS cc,
         argMin(ifNull(toString(properties.$device_model), ''), timestamp) AS model,
         argMin(ifNull(toString(properties.$ip), ''), timestamp) AS ip
  FROM events WHERE event = 'Application Installed'
    AND timestamp >= {_t(start - dt.timedelta(days=F.LOOKBACK_DAYS))} AND timestamp < {_t(end)}
  GROUP BY person_id
  HAVING t0 >= {_t(start)} AND v = '{v}' AND os IN {F.PLAT_OS[plat]}
),
ev AS (
  SELECT person_id, event, timestamp FROM events
  WHERE event IN ('subscription_started', 'clicked_preview', 'export_completed', 'paywall_viewed',
                  'paywall_purchase_completed')
    AND timestamp >= {_t(start)} AND timestamp < {_t(cutoff)}
    AND (event != 'subscription_started' OR (properties.environment = 'Production' AND toFloat(properties.price_usd) > 0))
)
SELECT cc, {F.google_ip_sql(cidrs)} AS gip, startsWith(model, 'sdk_') AS emu, (ip = '' OR model = '') AS miss,
       toDate(toTimeZone(t0, 'America/Los_Angeles')) AS d,
       count() AS n, countIf(w > 0) AS pw, countIf(fp > {none}) AS pv, countIf(fp > {none} AND le >= fp) AS ex,
       countIf(e > 0) AS exr,
       countIf(p > 0) AS paid, countIf(c > 0) AS pc,
       countIf(p > 0 AND fp > {none}) AS paid_pv, countIf(c > 0 AND fp > {none}) AS pc_pv
FROM (
  SELECT inst.person_id AS pid, any(inst.t0) AS t0, any(inst.cc) AS cc, any(inst.model) AS model, any(inst.ip) AS ip,
    countIf(ev.event = 'export_completed' AND {win}) AS e,
    countIf(ev.event = 'paywall_viewed' AND {win}) AS w,
    countIf(ev.event = 'subscription_started' AND {win}) AS p,
    countIf(ev.event = 'paywall_purchase_completed' AND {win}) AS c,
    minIf(ev.timestamp, ev.event = 'clicked_preview' AND {win}) AS fp,
    maxIf(ev.timestamp, ev.event = 'export_completed' AND {win}) AS le
  FROM inst LEFT JOIN ev ON ev.person_id = inst.person_id
  GROUP BY pid
)
GROUP BY cc, gip, emu, miss, d
LIMIT 50000"""


def by_key(rows):
    out = {}
    for r in rows:
        k = tuple(str(r[x]).lower() for x in KEYS)
        acc = out.setdefault(k, dict.fromkeys(FIELDS, 0))
        for f in FIELDS:
            acc[f] += int(r[f])
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-date", required=True)
    ap.add_argument("--builds", default="ios:4.1.1,android:4.0.155,android:5.0.8")
    a = ap.parse_args(argv)
    cutoff = ip_main.cutoff_for(dt.date.fromisoformat(a.report_date))
    start, end = cutoff - dt.timedelta(days=60), cutoff - dt.timedelta(hours=F.H)
    run = F.make_runner(os.environ["POSTHOG_PERSONAL_API_KEY"])
    cidrs, _ = F.load_cidrs()
    pipe = F.fetch_cohort(run, start, end, cutoff, cidrs, log=lambda *_: None)
    bad = 0
    for spec in a.builds.split(","):
        plat, v = spec.split(":")
        mine = by_key([r for r in pipe[plat] if r["v"] == v])
        # Same 14-day chunks as production: each chunk has its own 180-day lookback.
        ind = by_key([r for a, b in F.chunks(start, end)
                      for r in run(independent_sql(plat, v, a, b, cutoff, cidrs))])
        ok = mine == ind and bool(mine)
        bad += not ok
        print(f"{plat} {v}: {'MATCH' if ok else 'MISMATCH'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
