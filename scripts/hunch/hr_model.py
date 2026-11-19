"""Pure UTC-day calculations for the TreasureHunch retention email."""
from __future__ import annotations

from datetime import date, timedelta
from math import sqrt
from statistics import median


START = date(2026, 10, 31)
RELEASE = date(2026, 11, 8)
GOOD, CONT = 0.30, 0.15
WINDOWS = {"d1": 1, "d3": 3, "d7any": 7, "d7": 7, "days3": 7}
COMPARISON_METRICS = ("d1", "d7any", "d7", "days3")
GROUP_FIELDS = ("n", "d1", "d3", "d7any", "d7", "days3", "paid", "returned", "returned_daily")
DAY_FIELDS = ("people", "players", "hunts", "finished", "won", "daily_players")
MARKERS = {
    date(2026, 11, 8): "Daily puzzle + atlas",
    date(2026, 11, 9): "WhatsApp reminders",
    date(2026, 11, 11): "Onboarding hints, challenges",
    date(2026, 11, 16): "Web 0.2.0, install prompt",
    date(2026, 11, 18): "Android + iOS apps",
}


def _date(value):
    return value if isinstance(value, date) else date.fromisoformat(value)


def _dates(first, last):
    return (first + timedelta(days=i) for i in range((last - first).days + 1))


def _ratio(k, n):
    return k / n if n else None


def _rate(k, n):
    return {"k": k, "n": n, "rate": _ratio(k, n)}


def window_closed(d0, window, last_day):
    return d0 + timedelta(days=window) <= last_day


def pooled(groups, first, last, metric, last_day):
    eligible = [row for row in groups if first <= row["d0"] <= last
                and window_closed(row["d0"], WINDOWS[metric], last_day)]
    return _rate(sum(row[metric] for row in eligible), sum(row["n"] for row in eligible))


def band(rate):
    if rate is None:
        return "unclear"
    return "good" if rate >= GOOD else "continue" if rate >= CONT else "stop"


def wilson(k, n):
    """Two-sided 95% Wilson score interval, or no interval for no players."""
    if not n:
        return None
    z = 1.959963984540054
    p = k / n
    denominator = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denominator
    margin = z * sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denominator
    return centre - margin, centre + margin


def _change(current, previous):
    current_interval = wilson(current["k"], current["n"])
    previous_interval = wilson(previous["k"], previous["n"])
    if current_interval is None or previous_interval is None:
        return "too early to compare"
    if current_interval[0] <= previous_interval[1] and previous_interval[0] <= current_interval[1]:
        return "no real change"
    return "up" if current["rate"] > previous["rate"] else "down"


def _fact_rate(value):
    return {"k": value["k"], "n": value["n"],
            "rate_pct": value["rate"] * 100 if value["rate"] is not None else None}


def build_model(report_date, *, groups, days, new_events=(), new_versions=(), test_number=None):
    """Build complete, calendar-aligned facts without I/O or runtime logging.

    The three-or-more-days comparison uses groups with a closed seven-day window,
    matching the approved preview. Counts remain the query's observed distinct days.
    """
    report_date = _date(report_date)
    last_day = report_date - timedelta(days=1)
    group_lookup = {_date(row["d0"]): row for row in groups}
    group_rows = [{"d0": d, **{key: int(group_lookup.get(d, {}).get(key, 0)) for key in GROUP_FIELDS}}
                  for d in _dates(START, last_day)]
    day_lookup = {_date(row["day"]): row for row in days}
    daily_rows = []
    for d in _dates(last_day - timedelta(days=7), last_day):
        row = {"day": d, **{key: int(day_lookup.get(d, {}).get(key, 0)) for key in DAY_FIELDS}}
        row.update(hunts_each=_ratio(row["hunts"], row["players"]),
                   finished_rate=_ratio(row["finished"], row["hunts"]),
                   won_rate=_ratio(row["won"], row["finished"]))
        daily_rows.append(row)

    first = last_day - timedelta(days=7)
    current = pooled(group_rows, first, last_day - timedelta(days=1), "d1", last_day)
    previous = pooled(group_rows, first - timedelta(days=7), first - timedelta(days=1), "d1", last_day)
    before = {key: pooled(group_rows, START, RELEASE - timedelta(days=1), key, last_day)
              for key in COMPARISON_METRICS}
    since = {key: pooled(group_rows, RELEASE, last_day, key, last_day) for key in COMPARISON_METRICS}
    markers = dict(MARKERS)
    versions = [{"day": _date(row["day"]), "platform": str(row["platform"]), "version": str(row["version"])}
                for row in new_versions]
    for d in sorted({row["day"] for row in versions}):
        builds = ", ".join(f'{row["platform"]} {row["version"]}' for row in versions if row["day"] == d)
        markers[d] = f"{markers[d]} · {builds}" if d in markers else builds

    visible_groups = []
    for row in group_rows:
        if row["d0"] < RELEASE:
            continue
        metrics = {}
        for key, window in WINDOWS.items():
            pending = not window_closed(row["d0"], window, last_day)
            metrics[key] = {**_rate(row[key], row["n"]), "pending": pending}
            if pending:
                metrics[key]["rate"] = None
        visible_groups.append({**row, "metrics": metrics, "thin": row["n"] < 30,
                               "marker": markers.get(row["d0"], "")})

    latest_group = next((row for row in group_rows if row["d0"] == last_day), {"n": 0, "paid": 0})
    today = {**daily_rows[-1], "new_players": latest_group["n"], "paid_new": latest_group["paid"]}
    traffic_median = median(row["people"] for row in daily_rows[:-1])
    baseline_rows = [row for row in group_rows if first <= row["d0"] <= last_day - timedelta(days=3)]
    new_baseline = sum(row["n"] for row in baseline_rows) / len(baseline_rows) if baseline_rows else 0
    paid_rows = [row for row in group_rows if first <= row["d0"] < last_day]
    paid_baseline = _rate(sum(row["paid"] for row in paid_rows), sum(row["n"] for row in paid_rows))
    daily_returners = _rate(sum(row["returned_daily"] for row in visible_groups),
                            sum(row["returned"] for row in visible_groups))

    checks = []
    if today["people"] < traffic_median / 2:
        checks.append({"kind": "traffic", "text": (
            f'Traffic: {today["people"]} people opened the game yesterday, against a median of '
            f'{traffic_median:g} a day over the previous week. Paid new players: {today["paid_new"]} '
            f'of {today["new_players"]}. Check whether the Meta campaign was paused.')})
    if today["won_rate"] is not None and today["won_rate"] >= 0.90:
        checks.append({"kind": "difficulty", "text": (
            f'Difficulty: players won {today["won_rate"]:.0%} of finished hunts yesterday. '
            'The game may be too easy to hold players past the first week.')})
    thin_start = None
    for row in reversed(visible_groups):
        if not row["thin"]:
            break
        thin_start = row["d0"]
    if thin_start is not None:
        checks.append({"kind": "thin", "text": (
            f'Player groups from {thin_start:%-d %b} onward are under 30 players each, so their rates '
            'are shown in grey and are not read.')})
    if today["new_players"] == 0:
        checks.append({"kind": "no-new-players", "text": (
            'No new players started a hunt yesterday. The latest group has no return rate to read.')})

    verdict = {"current": current, "previous": previous, "band": band(current["rate"]),
               "change": _change(current, previous), "first_day": first,
               "last_day": last_day - timedelta(days=1)}
    newest_versions = [row for row in versions if row["day"] == last_day]
    model = {
        "report_date": report_date, "last_day": last_day, "release": RELEASE,
        "verdict": verdict, "before": before, "since": since,
        "groups": visible_groups, "days": daily_rows[1:], "today": today,
        "traffic_median": traffic_median, "new_players_baseline": new_baseline,
        "paid_baseline": paid_baseline, "daily_returners": daily_returners,
        "checks": checks, "new_events": list(new_events), "new_versions": newest_versions,
        "test_number": test_number, "summary": "Summary unavailable today.",
    }
    model["facts"] = {
        "last_day": last_day.isoformat(), "release": RELEASE.isoformat(),
        "thresholds": {"good_pct": GOOD * 100, "continue_pct": CONT * 100, "thin_group_n": 30},
        "verdict": {"current": _fact_rate(current), "previous": _fact_rate(previous),
                    "band": verdict["band"], "change": verdict["change"]},
        "before": {key: _fact_rate(value) for key, value in before.items()},
        "since": {key: _fact_rate(value) for key, value in since.items()},
        "latest": {key: today[key] for key in (*DAY_FIELDS, "new_players", "paid_new")},
        "traffic_median": traffic_median, "new_players_baseline": round(new_baseline),
        "paid_baseline": _fact_rate(paid_baseline), "daily_returners": _fact_rate(daily_returners),
        "checks": checks, "new_events": list(new_events),
        "new_versions": [{"platform": row["platform"], "version": row["version"]} for row in newest_versions],
    }
    model["facts"]["latest"]["won_pct"] = today["won_rate"] * 100 if today["won_rate"] is not None else None
    return model
