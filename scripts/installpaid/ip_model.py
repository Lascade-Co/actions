"""Pure model for the daily TravelAnimator install->paid email. No I/O.

Input rows come from ip_fetch.cohort_sql(): one row per (build, country, test flags,
install day) with per-person counts. Every rate is reach within 24 h of first install.

Pooling (outcome-blind, so the intervals stay honest): per track, take builds newest
first and stop once the pool has enough installs to EXPECT 40 payers at the track's
Mar 2026 one-day rate. Today's conversions never decide which builds are in the pool.
"""
from __future__ import annotations

import datetime as dt
import math
import re
from collections import Counter, defaultdict

Z = 1.959964
EQUIV_REL = 0.10            # "no meaningful change": CI inside +-10% of the benchmark rate
TARGET_PAYERS = 40          # pool until 40 payers are EXPECTED at the Mar 2026 rate
THIN_PAYERS = 20            # fewer actual payers than this -> no verdicts
RELEASE_MIN_INSTALLS = 10   # release day = first day with >= 10 real first installs
WINDOW_DAYS = 60
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
    3.x binaries, a future 6.x) returns None and is left out of every pool."""
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


def release_days(rows):
    """{build: (release_day, soft)}. Release = first PT day with >= 10 real first
    installers; a build that never gets there is a soft launch from its first install."""
    per = defaultdict(Counter)
    for r in rows:
        per[r["v"]][r["d"]] += r["n"]
    out = {}
    for v, days in per.items():
        hit = sorted(d for d, c in days.items() if c >= RELEASE_MIN_INSTALLS)
        out[v] = (hit[0], False) if hit else (min(days), True)
    return out


def pool(rows, target_n):
    """Newest builds first until installs >= target_n. Installs before a build's
    release day (testers, pre-release) are left out."""
    rel = release_days(rows)
    # Newest = highest version number, never the release day: a straggler old binary
    # installed by one person today must not jump ahead of the current release.
    order = sorted(rel, key=vkey, reverse=True)
    per_build = defaultdict(dict)
    for r in rows:
        if r["d"] >= rel[r["v"]][0]:
            add(per_build[r["v"]], r)
    acc, builds = {}, []
    for v in order:
        if not per_build[v]:
            continue
        add(acc, per_build[v])
        builds.append({"v": v, "released": rel[v][0], "soft": rel[v][1], **per_build[v]})
        if acc["n"] >= target_n:
            break
    days = [r["d"] for r in rows if r["v"] in {b["v"] for b in builds} and r["d"] >= rel[r["v"]][0]]
    return {"counts": acc, "builds": builds, "capped": acc.get("n", 0) < target_n,
            "first_day": min(days) if days else None, "last_day": max(days) if days else None,
            "countries": _countries(rows, {b["v"]: b["released"] for b in builds})}


def _countries(rows, rel):
    c = Counter()
    for r in rows:
        if r["v"] in rel and r["d"] >= rel[r["v"]]:
            c[r.get("cc") or "??"] += r["n"]
    return c


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


def compare(now, base, step, thin):
    """Difference vs one benchmark for one step. `directional` when the bases differ in
    kind (no verdict then); verdict suppressed for thin pools."""
    k, d = _FRAC[step]
    r = newcombe(_num(now, k), _num(now, d), _num(base, k), _num(base, d))
    if r is None:                     # either side has no denominator
        return None
    diff, lo, hi = r
    b0 = _num(base, k) / _num(base, d)
    return {"diff": diff, "lo": lo, "hi": hi, "base": b0,
            "verdict": None if thin else verdict(diff, lo, hi, b0)}


def build_model(raw, bench, report_date, cutoff_utc, cidr_date, bench_def):
    """raw: {plat: rows} for the 60-day window; bench: {"aug25": {plat: agg}, "mar26": ...}."""
    kept, audit = split(raw)
    tracks = []
    for tk, store, label in TRACKS:
        plat = plat_of(tk)
        b_mar = bench["mar26"][plat]["counts"]
        base_rate = b_mar["paid"] / b_mar["n"] if b_mar.get("n") and b_mar.get("paid") else None
        # No March rate (zero payers then) -> no way to size the pool: take every build,
        # and the pool is capped by definition, so no verdict.
        target = math.ceil(TARGET_PAYERS / base_rate) if base_rate else math.inf
        rows = kept.get(tk, [])
        if not rows:
            tracks.append({"key": tk, "store": store, "label": label, "empty": True,
                           "audit": dict(audit.get(tk, {})), "target": target})
            continue
        p = pool(rows, target)
        c = p["counts"]
        thin = c.get("paid", 0) < THIN_PAYERS or p["capped"]
        cmp = {}
        for bk, _ in BENCH:
            bc = bench[bk][plat]["counts"]
            client = (plat, bk) in SERVER_UNAVAILABLE
            cmp[bk] = {"client_basis": client}
            for step in ("pw", "pv", "ex", "pv_ex"):
                cmp[bk][step] = compare(c, bc, step, thin)
            # Client (in-app purchase event) vs client: always directional, the Android
            # event changed definition in Aug 2026. Server vs server only where both exist.
            x = compare(c, bc, "pc", thin)
            if x:
                x["verdict"] = None
            cmp[bk]["pc"] = x
            cmp[bk]["paid"] = x if client else compare(c, bc, "paid", thin)
        tracks.append({
            "key": tk, "store": store, "label": label, "empty": False, "target": target,
            "no_target": base_rate is None,
            "counts": c, "rates": rates(c), "thin": thin, "capped": p["capped"],
            "builds": p["builds"], "first_day": p["first_day"], "last_day": p["last_day"],
            "cmp": cmp, "mix": mix(p["countries"], bench["mar26"][plat]["countries"]),
            "audit": dict(audit.get(tk, {})), "trend": trend(rows),
        })
    return {"report_date": report_date, "cutoff_utc": cutoff_utc, "cidr_date": cidr_date,
            "bench_def": bench_def, "tracks": tracks,
            "bench": {bk: {pl: {"counts": bench[bk][pl]["counts"], "rates": rates(bench[bk][pl]["counts"])}
                           for pl in ("ios", "android")} for bk, _ in BENCH}}


def mix(now_cc, base_cc, top=3):
    tot_n, tot_b = sum(now_cc.values()), sum(base_cc.values())
    if not tot_n:
        return []
    return [{"cc": cc, "now": n / tot_n, "base": (base_cc.get(cc, 0) / tot_b) if tot_b else None}
            for cc, n in now_cc.most_common(top)]


def trend(rows, last=8, min_n=30):
    """Descriptive: one-day paid rate per build (installs since release), newest last."""
    rel = release_days(rows)
    per = defaultdict(dict)
    for r in rows:
        if r["d"] >= rel[r["v"]][0]:
            add(per[r["v"]], r)
    bs = [v for v in sorted(rel, key=vkey) if per[v].get("n", 0) >= min_n]
    return [{"v": v, "released": rel[v][0], "n": per[v]["n"], "paid": per[v]["paid"],
             "rate": wilson(per[v]["paid"], per[v]["n"])} for v in bs[-last:]]
