"""Daily TravelAnimator install->paid email: fetch -> model -> render.

    python3 ip_main.py [--report-date 2026-10-20] --out out [--variant A|B|AB] [--cache]

Data cutoff: 05:00 America/Los_Angeles on the report date. Installs in
[cutoff - 60 days, cutoff - 24 h], so every install has a complete first day.
Benchmarks: INSTALLPAID_BENCHMARKS_JSON_B64 (CI) or .cache/benchmarks.local.json (local).
--cache reuses .cache/window-<date>.local.json if present (local iteration only).
Prints no figures: the repository is public.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import ip_fetch as F
import ip_model as M


PT = ZoneInfo("America/Los_Angeles")
HERE = Path(__file__).resolve().parent
CACHE = HERE / ".cache"


def cutoff_for(report_date: dt.date) -> dt.datetime:
    """05:00 PT on the report date, as naive UTC (DST-aware)."""
    return dt.datetime.combine(report_date, dt.time(5), PT).astimezone(dt.timezone.utc).replace(tzinfo=None)


def latest_ready_date(now=None):
    """Newest report date whose 05:00 PT cutoff has passed."""
    local = (now or dt.datetime.now(dt.timezone.utc)).astimezone(PT)
    return local.date() if local.time() >= dt.time(5) else local.date() - dt.timedelta(days=1)


def load_bench():
    b64 = os.environ.get("INSTALLPAID_BENCHMARKS_JSON_B64")
    if b64:
        return json.loads(base64.b64decode(b64))
    return json.loads((CACHE / "benchmarks.local.json").read_text())


def fetch_window(report_date, cutoff, use_cache):
    """Rows for the 60-day window. A local cache is reused only when it was fetched
    with today's query definition."""
    cidrs, cidr_date = F.load_cidrs()
    qdef = F.definition_hash(cidrs, cidr_date)
    path = CACHE / f"window-{report_date}.local.json"
    if use_cache and path.exists():
        cached = json.loads(path.read_text())
        if cached.get("definition") == qdef:
            return cached["rows"]
    run = F.make_runner(os.environ["POSTHOG_PERSONAL_API_KEY"])
    end = cutoff - dt.timedelta(hours=F.H)
    raw = F.fetch_cohort(run, cutoff - dt.timedelta(days=M.WINDOW_DAYS), end, cutoff, cidrs)
    if use_cache:
        CACHE.mkdir(exist_ok=True)
        path.write_text(json.dumps({"definition": qdef, "rows": raw}, default=str))
    return raw


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-date", default="")
    ap.add_argument("--out", default="out")
    ap.add_argument("--variant", default="A", choices=["A", "B", "AB"])
    ap.add_argument("--cache", action="store_true")
    a = ap.parse_args(argv)
    report_date = dt.date.fromisoformat(a.report_date) if a.report_date else latest_ready_date()
    cutoff = cutoff_for(report_date)
    if cutoff > dt.datetime.now(dt.timezone.utc).replace(tzinfo=None):
        print(f"report date {report_date} is not ready: its 05:00 PT cutoff has not passed", file=sys.stderr)
        return 4
    bench = load_bench()
    cidrs, cidr_date = F.load_cidrs()
    if bench.get("definition") != F.definition_hash(cidrs, cidr_date, M.RULES):
        print("benchmarks were built with a different query, filter or IP list; rebuild them", file=sys.stderr)
        return 2
    missing = [f"{bk}/{pl}" for bk in ("aug25", "mar26") for pl in ("ios", "android")
               if not ((bench.get(bk) or {}).get(pl) or {}).get("counts", {}).get("n")]
    if missing:
        print("benchmarks incomplete: " + ", ".join(missing), file=sys.stderr)
        return 2
    try:
        raw = fetch_window(report_date, cutoff, a.cache)
    except F.Unavailable as e:
        print(f"PostHog unavailable: {e}", file=sys.stderr)
        return 3
    import ip_render_email as R
    model = M.build_model(raw, bench, report_date, cutoff, cidr_date, bench["definition"])
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report-date.txt").write_text(report_date.isoformat())
    for v in a.variant:
        subject, html, text, images = R.render(model, v, out)
        (out / f"email-{v}.html").write_text(html)
        (out / f"email-{v}.txt").write_text(text)
        (out / f"subject-{v}.txt").write_text(subject)
        (out / f"images-{v}.json").write_text(json.dumps(images))
        print(f"rendered variant {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
