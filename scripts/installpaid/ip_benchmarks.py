"""One-time fetch of the Aug 2025 and Mar 2026 one-day benchmarks for the install->paid
email, with exactly the daily query and filters.

Writes scripts/installpaid/.cache/benchmarks.local.json (gitignored: the repository is
public). For CI, base64 that file into the Infisical secret INSTALLPAID_BENCHMARKS_JSON_B64.
Raw rows go to .cache/bench_raw.local.json for reconciliation; never commit either.

    POSTHOG_PERSONAL_API_KEY=... python3 ip_benchmarks.py
    python3 ip_benchmarks.py --from-raw     # re-aggregate cached rows, no queries
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

import ip_fetch as F
import ip_model as M

MONTHS = {"aug25": (dt.datetime(2025, 8, 1), dt.datetime(2025, 9, 1)),
          "mar26": (dt.datetime(2026, 3, 1), dt.datetime(2026, 4, 1))}
CACHE = Path(__file__).resolve().parent / ".cache"


def main():
    cidrs, cidr_date = F.load_cidrs()
    qdef = F.definition_hash(cidrs, cidr_date)
    if "--from-raw" in sys.argv:
        cached = json.loads((CACHE / "bench_raw.local.json").read_text())
        if cached.get("definition") != qdef:
            print("cached rows were fetched with a different query; refetch without --from-raw", file=sys.stderr)
            return 2
        raw, fetched_at = cached["rows"], cached["fetched_at"]
    else:
        run = F.make_runner(os.environ["POSTHOG_PERSONAL_API_KEY"])
        raw = {k: F.fetch_cohort(run, a, b, b + dt.timedelta(days=2), cidrs) for k, (a, b) in MONTHS.items()}
        fetched_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = {"definition": F.definition_hash(cidrs, cidr_date, M.RULES), "query_definition": qdef,
           "cidr_date": cidr_date, "fetched_at": fetched_at}
    for k in MONTHS:
        out[k] = M.bench_aggregate(raw[k])
    CACHE.mkdir(exist_ok=True)
    (CACHE / "bench_raw.local.json").write_text(
        json.dumps({"definition": qdef, "fetched_at": fetched_at, "rows": raw}, default=str))
    (CACHE / "benchmarks.local.json").write_text(json.dumps(out, indent=1, sort_keys=True))
    print("benchmarks written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
