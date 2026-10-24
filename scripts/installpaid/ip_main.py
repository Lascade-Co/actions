"""Daily TravelAnimator install->paid email: fetch -> model -> render.

    python3 ip_main.py [--edition pt|ist] [--report-date YYYY-MM-DD] --out out [--variant A|B|AB] [--cache]

Data cutoff: 05:00 in the edition's timezone on the report date. Installs in
[cutoff - 60 days, cutoff - 24 h], so every install has a complete first day.
Benchmarks: INSTALLPAID_BENCHMARKS_JSON_B64 (CI) or .cache/benchmarks.local.json (local).
--cache reuses .cache/window-<edition>-<cutoffISO>.local.json (local iteration only).
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

import ip_fetch as F
import ip_model as M
from ip_editions import EDITIONS, get_edition


PT = get_edition("pt").zone  # compatibility for existing callers
HERE = Path(__file__).resolve().parent
CACHE = HERE / ".cache"


def cutoff_for(report_date: dt.date, edition="pt") -> dt.datetime:
    """05:00 local on the report date, as naive UTC (DST-aware)."""
    return get_edition(edition).at(report_date, 5, 0).replace(tzinfo=None)


def latest_ready_date(now=None, edition="pt"):
    """Newest report date whose local 05:00 cutoff has passed."""
    local = (now or dt.datetime.now(dt.timezone.utc)).astimezone(get_edition(edition).zone)
    return local.date() if local.time() >= dt.time(5) else local.date() - dt.timedelta(days=1)


def build_deadline(report_date, edition, started_at):
    deadline = get_edition(edition).at(report_date, 5, 48)
    return deadline if started_at < deadline else None


def load_bench():
    b64 = os.environ.get("INSTALLPAID_BENCHMARKS_JSON_B64")
    if b64:
        return json.loads(base64.b64decode(b64))
    return json.loads((CACHE / "benchmarks.local.json").read_text())


def fetch_window(report_date, cutoff, use_cache, edition="pt", deadline=None):
    """Rows for the 60-day window. A local cache is reused only when it was fetched
    with today's query definition."""
    cidrs, cidr_date = F.load_cidrs()
    qdef = F.definition_hash(cidrs, cidr_date)
    path = CACHE / get_edition(edition).cache_name(cutoff)
    if use_cache and path.exists():
        cached = json.loads(path.read_text())
        if cached.get("definition") == qdef:
            return cached["rows"]
    run = F.make_runner(os.environ["POSTHOG_PERSONAL_API_KEY"], deadline=deadline)
    end = cutoff - dt.timedelta(hours=F.H)
    raw = F.fetch_cohort(run, cutoff - dt.timedelta(days=M.WINDOW_DAYS), end, cutoff, cidrs, deadline=deadline)
    if use_cache:
        CACHE.mkdir(exist_ok=True)
        path.write_text(json.dumps({"definition": qdef, "rows": raw}, default=str))
    return raw


EARLY_BUDGET = dt.timedelta(minutes=8)
EARLY_RESERVE = dt.timedelta(minutes=4)      # left for the base build to finish rendering
EARLY_MIN = dt.timedelta(seconds=90)


def early_deadline(build_deadline, now):
    """Own, finite budget for the optional early read; None means skip it."""
    end = now + EARLY_BUDGET
    if build_deadline is not None:
        end = min(end, build_deadline - EARLY_RESERVE)
    return end if end - now >= EARLY_MIN else None


def fetch_early_model(report_date, cutoff, edition, build_deadline, now=None, fetch=None):
    """The early read, or None. Optional by design: any failure leaves the email as it was.
    Logs never carry figures (public repository)."""
    try:
        now = now or dt.datetime.now(dt.timezone.utc)
        dl = early_deadline(build_deadline, now)
        if dl is None:
            print("early read skipped: not enough build time left")
            return None
        first, last, early_day, start, end, deadlines = M.early_window(report_date, get_edition(edition), cutoff)
        if fetch is None:
            cidrs, _ = F.load_cidrs()
            run = F.make_runner(os.environ["POSTHOG_PERSONAL_API_KEY"], deadline=dl)

            def fetch():
                return F.fetch_early(run, start, end, cutoff, deadlines, cidrs, deadline=dl)
        return M.build_early(fetch(), first, last, early_day)
    except Exception:
        print("early read unavailable; sending without it")
        return None


def main(argv=None):
    started_at = dt.datetime.now(dt.timezone.utc)
    ap = argparse.ArgumentParser()
    ap.add_argument("--edition", choices=EDITIONS, default="pt")
    ap.add_argument("--report-date", default="")
    ap.add_argument("--out", default="out")
    ap.add_argument("--variant", default="A", choices=["A", "B", "AB"])
    ap.add_argument("--cache", action="store_true")
    a = ap.parse_args(argv)
    report_date = dt.date.fromisoformat(a.report_date) if a.report_date else latest_ready_date(started_at, a.edition)
    cutoff = cutoff_for(report_date, a.edition)
    deadline = build_deadline(report_date, a.edition, started_at)
    if cutoff > dt.datetime.now(dt.timezone.utc).replace(tzinfo=None):
        print(f"report date {report_date} is not ready: its local 05:00 cutoff has not passed", file=sys.stderr)
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
        raw = fetch_window(report_date, cutoff, a.cache, a.edition, deadline)
    except F.Unavailable as e:
        print(f"PostHog unavailable: {e}", file=sys.stderr)
        return 3
    import ip_render_email as R
    model = M.build_model(raw, bench, report_date, cutoff, cidr_date, bench["definition"])
    model["edition"] = a.edition
    model["early"] = fetch_early_model(report_date, cutoff, a.edition, deadline)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report-date.txt").write_text(report_date.isoformat())
    (out / "build-timing.json").write_text(json.dumps({
        "edition": a.edition, "report_date": str(report_date), "started_at": started_at.isoformat(),
        "deadline": deadline.isoformat() if deadline else None,
    }))
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
