"""Live check of the cohort query's per-person logic against an independently written
join-based query, one build per track, field by field (every count, split by country and by the test /
missing-data flags). There is no ClickHouse offline, so this is the
executable test of the SQL semantics (first install wins, 24 h window, export after the
first preview, paid regardless of preview order). Prints MATCH / MISMATCH only: the
repository is public.

    POSTHOG_PERSONAL_API_KEY=... python3 ip_crosscheck.py --report-date 2026-10-20 [--builds ios:4.1.1,android:4.0.155]

Alert reconciliation (all eligible builds, calendar days in Asia/Muscat):
    python3 ip_crosscheck.py --muscat-days 2024-02-03 2024-02-04 2024-02-05 \
        --spine-start 2024-01-01 --output .cache/muscat.local.json
    # Replace --output with --compare .cache/alert.local.json to print MATCH/MISMATCH.

--spine-start is the earliest day of the WHOLE alert spine, not merely the first
day being checked. Its preceding six days anchor the fixed 180-day install scan.
JSON schema: {"spine_start": "YYYY-MM-DD", "rows": [{"date": "YYYY-MM-DD",
"platform": "ios|android", "eligible": integer, "paid": integer}, ...]}.
Every requested day needs both platform rows, including zero counts. Output files
must be untracked and gitignored within the current repository. Counts and API
response bodies are never printed by this mode.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import ip_fetch as F
import ip_main


def _t(x):
    return f"toDateTime('{x:%Y-%m-%d %H:%M:%S}', 'UTC')"


KEYS = ("cc", "gip", "emu", "miss", "d")
FIELDS = F.COUNTS
MUSCAT = ZoneInfo("Asia/Muscat")


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


def validate_muscat_days(days, spine_start, now=None):
    """Require matured dates inside the whole query window, including its lead-in."""
    days = sorted(set(days))
    if not days or len(days) * 2 >= F.QUERY_LIMIT:
        raise ValueError("invalid reconciliation day range")
    if days[0] < spine_start - dt.timedelta(days=6):
        raise ValueError("reconciliation day precedes the whole query window")
    latest = (now or dt.datetime.now(dt.timezone.utc)).astimezone(MUSCAT).date() - dt.timedelta(days=2)
    if days[-1] > latest:
        raise ValueError("reconciliation days must be mature in Asia/Muscat")
    return days


def muscat_days_sql(days, spine_start, cidrs):
    """Independent join-based B1 counts with one fixed first-install lookback.

    Sub-second timestamps survive the join. Paid events are reduced to a boolean
    per person before aggregation; missing source days remain zero spine rows.
    """
    days = validate_muscat_days(days, spine_start)

    def midnight(day):
        return dt.datetime.combine(day, dt.time(), MUSCAT).astimezone(dt.timezone.utc).replace(tzinfo=None)

    start, end = midnight(days[0]), midnight(days[-1] + dt.timedelta(days=1))
    lookback = midnight(spine_start - dt.timedelta(days=6 + F.LOOKBACK_DAYS))
    spine = "\n    UNION ALL ".join(f"SELECT toDate('{day.isoformat()}') AS d" for day in days)
    google = F.google_ip_sql(cidrs) if cidrs else "false"
    return f"""
WITH spine AS (
    {spine}
), platforms AS (
    SELECT 'ios' AS platform UNION ALL SELECT 'android' AS platform
), first_installs AS (
    SELECT person_id, min(timestamp) AS t0,
        argMin(ifNull(toString(properties.$app_version), ''), timestamp) AS v,
        argMin(ifNull(toString(properties.$os), ''), timestamp) AS os,
        argMin(ifNull(toString(properties.$device_model), ''), timestamp) AS model,
        argMin(ifNull(toString(properties.$ip), ''), timestamp) AS ip
    FROM events
    WHERE event = 'Application Installed'
      AND timestamp >= {_t(lookback)} AND timestamp < {_t(end)}
    GROUP BY person_id
), inst AS (
    SELECT person_id, t0, if(os = 'Android', 'android', 'ios') AS platform
    FROM first_installs
    WHERE t0 >= {_t(start)} AND t0 < {_t(end)}
      AND os IN ('iOS', 'iPadOS', 'Android')
      AND match(v, '^[0-9]+([.][0-9]+)+$')
      AND (os != 'Android' OR startsWith(v, '4.0.') OR startsWith(v, '5.'))
      AND NOT {google}
      AND NOT startsWith(model, 'sdk_')
), paid_events AS (
    SELECT person_id, timestamp, event
    FROM events
    WHERE event = 'subscription_started'
      AND properties.environment = 'Production' AND toFloat(properties.price_usd) > 0
      AND timestamp >= {_t(start)} AND timestamp < {_t(end + dt.timedelta(hours=24))}
), people AS (
    SELECT inst.person_id AS pid, inst.t0 AS t0, inst.platform AS platform,
        countIf(paid_events.event = 'subscription_started'
            AND paid_events.timestamp >= inst.t0
            AND paid_events.timestamp < inst.t0 + interval 24 hour) > 0 AS has_paid
    FROM inst LEFT JOIN paid_events ON paid_events.person_id = inst.person_id
    GROUP BY inst.person_id, inst.t0, inst.platform
), daily AS (
    SELECT toDate(toTimeZone(t0, 'Asia/Muscat')) AS d, platform,
        count() AS eligible, countIf(has_paid) AS paid
    FROM people GROUP BY d, platform
)
SELECT spine.d AS date, platforms.platform AS platform,
    ifNull(daily.eligible, 0) AS eligible, ifNull(daily.paid, 0) AS paid
FROM spine CROSS JOIN platforms
LEFT JOIN daily ON daily.d = spine.d AND daily.platform = platforms.platform
ORDER BY date, platform
LIMIT {len(days) * 2}
""".strip()


def local_output_path(value):
    """Fail closed before fetching when a destination could enter the public repo."""
    root_result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    if root_result.returncode:
        raise ValueError("output requires a git repository")
    root = Path(root_result.stdout.strip()).resolve()
    path = Path(value).resolve()
    try:
        relative = str(path.relative_to(root))
    except ValueError:
        raise ValueError("output must remain inside the current repository") from None
    tracked = subprocess.run(["git", "-C", str(root), "ls-files", "--error-unmatch", "--", relative],
                             capture_output=True)
    ignored = subprocess.run(["git", "-C", str(root), "check-ignore", "--quiet", "--", relative],
                             capture_output=True)
    if tracked.returncode == 0 or ignored.returncode != 0 or path.is_dir():
        raise ValueError("output must be an untracked gitignored file")
    return path


def daily_counts(rows, days):
    """Validate a complete day/platform grid and canonicalize it for comparison."""
    expected = {(day.isoformat(), platform) for day in days for platform in ("ios", "android")}
    found = {}
    for row in rows:
        key = (str(row["date"]), row["platform"])
        eligible, paid = row["eligible"], row["paid"]
        if (key not in expected or key in found or type(eligible) is not int or type(paid) is not int
                or not 0 <= paid <= eligible):
            raise ValueError("invalid daily reconciliation rows")
        found[key] = {"date": key[0], "platform": key[1], "eligible": eligible, "paid": paid}
    if found.keys() != expected:
        raise ValueError("incomplete daily reconciliation rows")
    return [found[key] for key in sorted(found)]


def muscat_main(args):
    """Never include exception text: API/parser errors can contain private figures."""
    try:
        if not args.spine_start or bool(args.output) == bool(args.compare):
            raise ValueError("Muscat mode requires an anchor and one output mode")
        days = [dt.date.fromisoformat(day) for day in args.muscat_days]
        spine_start = dt.date.fromisoformat(args.spine_start)
        days = validate_muscat_days(days, spine_start)
        output = local_output_path(args.output) if args.output else None
        cidrs, _ = F.load_cidrs()
        run = F.make_runner(os.environ["POSTHOG_PERSONAL_API_KEY"])
        rows = run(muscat_days_sql(days, spine_start, cidrs))
        if args.compare:
            try:
                comparison = json.loads(Path(args.compare).read_text())
                matches = (comparison["spine_start"] == spine_start.isoformat()
                           and daily_counts(rows, days) == daily_counts(comparison["rows"], days))
            except (ValueError, KeyError, TypeError, OSError):
                matches = False
            print("MATCH" if matches else "MISMATCH")
            return 0 if matches else 1
        result = {"spine_start": spine_start.isoformat(), "rows": daily_counts(rows, days)}
        output.parent.mkdir(parents=True, exist_ok=True)
        # Restrict the local file as well as protecting it from accidental commits.
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(result, handle, indent=2)
            handle.write("\n")
        return 0
    except Exception:
        print("Muscat reconciliation failed; no figures logged.", file=sys.stderr)
        return 2


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-date")
    ap.add_argument("--builds", default="ios:4.1.1,android:4.0.155,android:5.0.8")
    ap.add_argument("--muscat-days", nargs="+", metavar="YYYY-MM-DD")
    ap.add_argument("--spine-start", help="Earliest day in the whole alert spine (before selecting check days)")
    output_mode = ap.add_mutually_exclusive_group()
    output_mode.add_argument("--output", help="Untracked gitignored JSON output; stdout remains empty")
    output_mode.add_argument("--compare", help="JSON with spine_start and daily rows; stdout is only MATCH/MISMATCH")
    a = ap.parse_args(argv)
    if a.muscat_days:
        if a.report_date:
            ap.error("--report-date and --muscat-days are separate modes")
        return muscat_main(a)
    if not a.report_date or a.spine_start or a.output or a.compare:
        ap.error("choose --report-date or --muscat-days with --spine-start and --output/--compare")
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
