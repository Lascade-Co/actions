"""Pure model for the daily TravelAnimator install->paid email. No I/O.

Input rows come from ip_fetch.cohort_sql(): one row per (build, country, test flags,
install day) with per-person counts. Every rate is reach within 24 h of first install.

Window: every track covers the same fixed 7 Pacific days, ending on the latest full day
(the newest PT date every installer of which has had 24 hours by the cutoff), across all
real builds, like the benchmarks. The single latest day is reported too, but verdicts
are judged on the 7 days only.
"""
from __future__ import annotations

import datetime as dt
import math
import re
from collections import Counter, defaultdict
from zoneinfo import ZoneInfo

Z = 1.959964
EQUIV_REL = 0.10            # "no meaningful change": CI inside +-10% of the benchmark rate
WEEK_DAYS = 7               # every verdict is judged on this many full days
WINDOW_DAYS = 60
PT = ZoneInfo("America/Los_Angeles")   # install days are Pacific dates (ip_fetch cohort_sql)
# The filter rules applied after the fetch; part of the benchmark fingerprint, so a rule
# change makes the stored benchmarks refuse to load until they are rebuilt.
RULES = "real=dotted-numeric|test=gip,emu|families=ios,android4.0,android5|bench=all-real-builds"
COUNTS = ("n", "pw", "pv", "ex", "exr", "paid", "pc", "paid_pv", "pc_pv")
TRACKS = [
    ("ios", "App Store", "iOS"),
    ("android5", "Play Store", "Android 5.x"),     # upcoming release, shown above 4.0.x
    ("android4", "Play Store", "Android 4.0.x"),
]
BENCH = [("aug25", "Aug 2025"), ("mar26", "Mar 2026")]
# Android Adapty subscription_started is not linked to app users before ~Jan 2026
# (checked 13 Oct 2026: essentially none of Aug 2025's Play subscriptions link to an installer).
SERVER_UNAVAILABLE = {("android", "aug25")}
STEP_LABELS = [("pw", "Paywall seen"), ("pv", "Preview"), ("ex", "Export"), ("paid", "Paid")]


# ------------------------------------------------------------- statistics ----

def wilson(k, n):
    if not n:
        return (None, None, None)
    p = k / n
    d = 1 + Z * Z / n
    c = (p + Z * Z / (2 * n)) / d
    h = Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n)) / d
    return (p, c - h, c + h)


def newcombe(k1, n1, k0, n0):
    if not n1 or not n0:
        return None
    p1, l1, u1 = wilson(k1, n1)
    p0, l0, u0 = wilson(k0, n0)
    d = p1 - p0
    return (d, d - math.sqrt((p1 - l1) ** 2 + (u0 - p0) ** 2),
            d + math.sqrt((u1 - p1) ** 2 + (p0 - l0) ** 2))


def verdict(d, lo, hi, base):
    if lo > 0:
        return "better"
    if hi < 0:
        return "worse"
    if lo > -EQUIV_REL * base and hi < EQUIV_REL * base:
        return "same"
    return "unclear"


# ------------------------------------------------------------------ rows ----

def vkey(v):
    return tuple(int(x) if x.isdigit() else 0 for x in (v or "").split("-")[0].split("+")[0].split("."))


def plat_of(track):
    return "ios" if track == "ios" else "android"


def track_of(plat, v):
    """iOS: every build. Android: the 4.0.x and 5.x families only; anything else (old
    3.x binaries, a future 6.x) returns None and is left out."""
    if plat == "ios":
        return "ios"
    k = vkey(v)
    if k[:2] == (4, 0):
        return "android4"
    if k[:1] == (5,):
        return "android5"
    return None


def is_test(r):
    return bool(r["gip"]) or bool(r["emu"])


def is_real_build(v):
    """A store build: plain dotted numbers. Drops blanks, debug and tagged builds such as
    5.0.8-billing-test."""
    return bool(re.fullmatch(r"\d+(\.\d+)*", v or ""))


def _day(d):
    return d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d)[:10])


def add(acc, r):
    for k in COUNTS:
        acc[k] = acc.get(k, 0) + int(r.get(k) or 0)
    return acc


def split(raw):
    """{plat: rows} -> ({track: kept rows}, {track: {"test": n, "missing": n, "all": n}})."""
    kept, audit = defaultdict(list), defaultdict(Counter)
    for plat, rows in raw.items():
        for r in rows:
            v = r.get("v") or ""
            if not is_real_build(v):
                continue
            t = track_of(plat, v)
            if t is None:
                continue
            audit[t]["all"] += r["n"]
            if r.get("miss"):
                audit[t]["missing"] += r["n"]
            if is_test(r):
                audit[t]["test"] += r["n"]
                continue
            kept[t].append(dict(r, d=_day(r["d"])))
    return kept, audit


def latest_full_day(cutoff_utc):
    """Newest Pacific install day whose every installer has had 24 h by the cutoff.
    cutoff_utc is naive UTC (ip_main.cutoff_for); 24 elapsed hours, then Pacific."""
    if cutoff_utc.tzinfo is None:
        cutoff_utc = cutoff_utc.replace(tzinfo=dt.timezone.utc)
    end = (cutoff_utc - dt.timedelta(hours=24)).astimezone(PT)
    return end.date() - dt.timedelta(days=1)


def window(rows, first, last):
    """Every kept row installed on first..last (inclusive), all builds, newest build first."""
    acc, per, cc = {}, defaultdict(dict), Counter()
    for r in rows:
        if first <= r["d"] <= last:
            add(acc, r)
            add(per[r["v"]], r)
            cc[r.get("cc") or "??"] += r["n"]
    builds = [{"v": v, **per[v]} for v in sorted(per, key=vkey, reverse=True)]
    return {"counts": acc, "builds": builds, "countries": cc}


def bench_aggregate(raw):
    """Benchmark month rows -> {plat: {"counts":..., "countries":..., "test":...}}, all
    real builds pooled per platform (the 4.x/5.x split did not exist then)."""
    out = {}
    for plat, rows in raw.items():
        acc, cc, test, allp = {}, Counter(), 0, 0
        for r in rows:
            if not is_real_build(r.get("v") or ""):
                continue
            allp += r["n"]
            if is_test(r):
                test += r["n"]
                continue
            add(acc, r)
            cc[r.get("cc") or "??"] += r["n"]
        out[plat] = {"counts": acc, "countries": dict(cc), "test": test, "all": allp}
    return out


# ----------------------------------------------------------------- rates ----

def rates(c):
    """Step reach over installs, plus the per-person conditionals."""
    n, pv = c.get("n", 0), c.get("pv", 0)
    return {
        "pw": wilson(c.get("pw", 0), n), "pv": wilson(pv, n), "ex": wilson(c.get("ex", 0), n),
        "paid": wilson(c.get("paid", 0), n), "pc": wilson(c.get("pc", 0), n),
        "pv_ex": wilson(c.get("ex", 0), pv),
        "paid_if_pv": wilson(c.get("paid_pv", 0), pv),
        "paid_if_nopv": wilson(c.get("paid", 0) - c.get("paid_pv", 0), n - pv),
    }


# (step key, numerator key, denominator key) for comparisons
_FRAC = {"pw": ("pw", "n"), "pv": ("pv", "n"), "ex": ("ex", "n"), "paid": ("paid", "n"),
         "pc": ("pc", "n"), "pv_ex": ("ex", "pv")}


def _num(c, key):
    return c.get(key, 0)


def compare(now, base, step):
    """Difference vs one benchmark for one step; None when either side has no denominator."""
    k, d = _FRAC[step]
    r = newcombe(_num(now, k), _num(now, d), _num(base, k), _num(base, d))
    if r is None:                     # either side has no denominator
        return None
    diff, lo, hi = r
    b0 = _num(base, k) / _num(base, d)
    return {"diff": diff, "lo": lo, "hi": hi, "base": b0,
            "verdict": verdict(diff, lo, hi, b0)}


def build_model(raw, bench, report_date, cutoff_utc, cidr_date, bench_def):
    """raw: {plat: rows} for the 60-day window; bench: {"aug25": {plat: agg}, "mar26": ...}."""
    kept, audit = split(raw)
    last = latest_full_day(cutoff_utc)
    first = last - dt.timedelta(days=WEEK_DAYS - 1)
    tracks = []
    for tk, store, label in TRACKS:
        plat = plat_of(tk)
        rows = kept.get(tk, [])
        day = {}
        for r in rows:
            if r["d"] == last:
                add(day, r)
        w = window(rows, first, last)
        c = w["counts"]
        if not c.get("n"):
            tracks.append({"key": tk, "store": store, "label": label, "empty": True, "day": day,
                           "audit": dict(audit.get(tk, {}))})
            continue
        cmp = {}
        for bk, _ in BENCH:
            bc = bench[bk][plat]["counts"]
            client = (plat, bk) in SERVER_UNAVAILABLE
            cmp[bk] = {"client_basis": client}
            for step in ("pw", "pv", "ex"):
                cmp[bk][step] = compare(c, bc, step)
            # Client (in-app purchase event) vs client: always directional, the Android
            # event changed definition in Aug 2026. Server vs server only where both exist.
            x = compare(c, bc, "pc")
            if x:
                x["verdict"] = None
            cmp[bk]["pc"] = x
            cmp[bk]["paid"] = x if client else compare(c, bc, "paid")
        tracks.append({
            "key": tk, "store": store, "label": label, "empty": False, "day": day,
            "counts": c, "rates": rates(c), "builds": w["builds"],
            "cmp": cmp, "mix": mix(w["countries"], bench["mar26"][plat]["countries"]),
            "audit": dict(audit.get(tk, {})), "trend": trend(rows, first, last),
        })
    return {"report_date": report_date, "cutoff_utc": cutoff_utc, "cidr_date": cidr_date,
            "bench_def": bench_def, "first_day": first, "last_day": last, "tracks": tracks,
            "bench": {bk: {pl: {"counts": bench[bk][pl]["counts"], "rates": rates(bench[bk][pl]["counts"])}
                           for pl in ("ios", "android")} for bk, _ in BENCH}}


def mix(now_cc, base_cc, top=3):
    tot_n, tot_b = sum(now_cc.values()), sum(base_cc.values())
    if not tot_n:
        return []
    return [{"cc": cc, "now": n / tot_n, "base": (base_cc.get(cc, 0) / tot_b) if tot_b else None}
            for cc, n in now_cc.most_common(top)]


def trend(rows, first, last, keep=8, min_n=30):
    """Descriptive: paid-within-a-day rate per build over the same 7 days, newest last."""
    per = defaultdict(dict)
    for r in rows:
        if first <= r["d"] <= last:
            add(per[r["v"]], r)
    bs = [v for v in sorted(per, key=vkey) if per[v].get("n", 0) >= min_n]
    return [{"v": v, "n": per[v]["n"], "paid": per[v]["paid"],
             "rate": wilson(per[v]["paid"], per[v]["n"])} for v in bs[-keep:]]
