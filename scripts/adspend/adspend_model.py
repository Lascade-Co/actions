"""Pure model for the daily ad-spend email. No I/O, no clock, no network.

Grain is Project x Channel x OS. Every line is compared with the day before
(primary) and the mean of D-8..D-2 (context). The labels describe observed
spend, never a team decision: Meta pacing alone moves spend +-10-20% a day.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from pnl_money import Unavailable

DEADBAND_PCT = Decimal("0.15")
DEADBAND_USD = Decimal("10")
WINDOW_DAYS = 14
ZERO = Decimal("0")
CHANNELS = ("Meta", "Google")
UNASSIGNED = "Unassigned"
UNATTRIBUTED = "Unattributed"


def app_of(name: str, apps: dict) -> str:
    """Longest matching prefix wins; the prefix must end at a non-alphanumeric."""
    lowered = (name or "").strip().lower()
    best, best_len = UNATTRIBUTED, 0
    for project, spec in apps.items():
        for prefix in spec["prefixes"]:
            p = prefix.lower()
            if lowered.startswith(p) and len(p) > best_len:
                nxt = lowered[len(p):len(p) + 1]
                if nxt == "" or not nxt.isalnum():
                    best, best_len = project, len(p)
    return best


def os_of(name: str) -> str:
    toks = set(re.split(r"[^a-z0-9]+", (name or "").lower()))
    if "android" in toks:
        return "Android"
    if toks & {"ios", "ipados"}:
        return "iOS"
    return UNASSIGNED


def short_of(project: str, apps: dict) -> str:
    return apps.get(project, {}).get("short", project)


def pct_change(new: Decimal, old: Decimal):
    return None if old == 0 else (new - old) / old


def round_pct(p) -> int:
    return int((p * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def label_of(d: Decimal, d1: Decimal, prior_spend: bool) -> str:
    if d1 == 0 and d > 0:
        return "started"
    if d1 > 0 and d == 0:
        return "stopped"
    if d == 0 and d1 == 0:
        return "dark" if prior_spend else "steady"
    delta = d - d1
    if abs(delta / d1) >= DEADBAND_PCT and abs(delta) >= DEADBAND_USD:
        return "up" if delta > 0 else "down"
    return "steady"


def day_closed(tz: str, ad_date: date, now: datetime) -> bool:
    """True once the account's own calendar has moved past ad_date."""
    nxt = ad_date + timedelta(days=1)
    return now >= datetime(nxt.year, nxt.month, nxt.day, tzinfo=ZoneInfo(tz))


def build_model(rows: dict, table, ad_date: date, now: datetime, apps: dict) -> dict:
    """rows: {"Meta": list[Row] | Unavailable, "Google": ...} in native currency."""
    days = [ad_date - timedelta(days=n) for n in range(WINDOW_DAYS, -1, -1)]
    d, d1 = ad_date, ad_date - timedelta(days=1)
    avg_days = [ad_date - timedelta(days=n) for n in range(2, 9)]
    prior_days = [ad_date - timedelta(days=n) for n in range(1, 8)]

    channels, lines, camps = {}, {}, {}
    for channel in CHANNELS:
        data = rows.get(channel)
        status = {"status": "ok", "reason": "", "installs_ok": True, "timezones": []}
        channels[channel] = status
        if not isinstance(data, list):
            status.update(status="unavailable", reason=getattr(data, "reason", "no data"),
                          installs_ok=False)
            continue
        status["timezones"] = sorted({r.timezone for r in data})
        unclosed = sorted({r.timezone for r in data if not day_closed(r.timezone, ad_date, now)})
        if unclosed:
            status.update(status="partial", reason="day not closed in " + ", ".join(unclosed))
        status["installs_ok"] = bool(data) and all(r.installs is not None for r in data)

        converted = []
        for r in data:
            usd = table.to_usd(r.spend, r.currency)
            if usd is None:  # all-or-nothing, as in pnl_fx.convert_all
                converted = None
                status.update(status="unavailable", reason=f"no USD rate for {r.currency}",
                              installs_ok=False)
                break
            converted.append((r, usd))
        if converted is None:
            continue

        for r, usd in converted:
            if r.day not in days:
                continue
            key = (app_of(r.campaign, apps), channel, os_of(r.campaign))
            ln = lines.setdefault(key, {"spend": {}, "installs": {}})
            ln["spend"][r.day] = ln["spend"].get(r.day, ZERO) + usd
            if r.installs is not None:
                ln["installs"][r.day] = ln["installs"].get(r.day, ZERO) + r.installs
            ck = (key, r.account_id, r.campaign_id)
            camps.setdefault(ck, {})
            camps[ck][r.day] = camps[ck].get(r.day, ZERO) + usd

    def total(series, ds):
        return sum((series.get(x, ZERO) for x in ds), ZERO)

    out_lines = []
    for key, ln in lines.items():
        s = ln["spend"]
        vd, v1 = s.get(d, ZERO), s.get(d1, ZERO)
        if vd == 0 and total(s, prior_days) == 0:
            continue
        project, channel, os_ = key
        i = ln["installs"]
        inst_ok = channels[channel]["installs_ok"]
        ck = [c for (k, _, _), c in camps.items() if k == key]
        started = sum(1 for c in ck if c.get(d1, ZERO) == 0 and c.get(d, ZERO) > 0)
        stopped = sum(1 for c in ck if c.get(d1, ZERO) > 0 and c.get(d, ZERO) == 0)
        i_d, i_1 = (i.get(d, ZERO), i.get(d1, ZERO)) if inst_ok else (None, None)
        out_lines.append({
            "project": project, "channel": channel, "os": os_,
            "d": vd, "d1": v1, "avg7": total(s, avg_days) / 7,
            "label": label_of(vd, v1, total(s, prior_days) > 0),
            "pct": pct_change(vd, v1),
            "installs_d": i_d, "installs_d1": i_1,
            "cpi_d": vd / i_d if i_d else None, "cpi_d1": v1 / i_1 if i_1 else None,
            "camps_started": started, "camps_stopped": stopped,
        })

    projects = {}
    for ln in out_lines:
        p = projects.setdefault(ln["project"], {"name": ln["project"], "lines": [],
                                                "d": ZERO, "d1": ZERO, "avg7": ZERO})
        p["lines"].append(ln)
        for k in ("d", "d1", "avg7"):
            p[k] += ln[k]
    plist = sorted(projects.values(), key=lambda p: -p["d"])
    for p in plist:
        p["lines"].sort(key=lambda x: (-x["d"], x["channel"], x["os"]))

    chart = []
    for x in days:
        chart.append({"day": x, "total": sum((ln["spend"].get(x, ZERO) for ln in lines.values()), ZERO)})

    td, t1 = chart[-1]["total"], chart[-2]["total"]
    tavg = sum((c["total"] for c in chart if c["day"] in avg_days), ZERO) / 7
    movers = [ln for ln in out_lines if ln["d"] != ln["d1"]]
    top = max(movers, key=lambda ln: abs(ln["d"] - ln["d1"])) if movers else None
    if top and top["label"] == "steady":
        top = None  # a mover inside the deadband is not news

    codes = {r.currency for ch in CHANNELS if isinstance(rows.get(ch), list) for r in rows[ch]} - {"USD"}
    degraded = [c for c, s in channels.items() if s["status"] != "ok"]
    return {
        "ad_date": ad_date, "channels": channels, "partial": bool(degraded),
        "total": {"d": td, "d1": t1, "avg7": tavg,
                  "pct_d1": pct_change(td, t1), "pct_avg": pct_change(td, tavg)},
        "projects": plist, "chart": chart, "top_mover": top,
        "fx": {"day": table.day, "rates": {c: table.rates[c] for c in sorted(codes) if c in table.rates}},
    }


def _money(v: Decimal) -> str:
    from pnl_money import format_usd
    return format_usd(v)


def subject_of(model: dict, variant: str, apps: dict) -> str:
    ad = model["ad_date"]
    t = model["total"]
    head = "Ad spend + CPI" if variant == "B" else "Ad spend"
    day = f"{ad:%a} {ad.day} {ad:%b}"
    partial = " partial" if model["partial"] else ""
    pct = "" if t["pct_d1"] is None else f" ({'+' if t['pct_d1'] >= 0 else '-'}{abs(round_pct(t['pct_d1']))}%)"
    s = f"{head} · {day} — {_money(t['d'])}{partial}{pct}"
    m = model["top_mover"]
    if m:
        delta = m["d"] - m["d1"]
        arrow = "↑" if delta > 0 else "↓"
        who = " ".join(x for x in (short_of(m["project"], apps),
                                   "" if m["os"] == UNASSIGNED else m["os"], m["channel"]) if x)
        s += f" · {who} {arrow}{_money(abs(delta))}"
    return s
