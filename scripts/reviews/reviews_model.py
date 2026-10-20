#!/usr/bin/env python3
"""Pure data model: group reviews by app, compute averages, detect language."""
from datetime import datetime, timezone

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTHS = ["January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]
SHORT_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def detect_language(text):
    """Return ISO 639-1 language code. Returns 'en' on failure or short text."""
    if not text or len(text) < 20:
        return "en"
    try:
        from langdetect import detect
        return detect(text)
    except Exception:
        return "en"


def _avg_rating(reviews):
    if not reviews:
        return 0.0
    return sum(r["rating"] for r in reviews) / len(reviews)


def _format_date(dt):
    """'Monday, 20 October 2026'"""
    day_name = DAYS[dt.weekday()]
    return f"{day_name}, {dt.day} {MONTHS[dt.month - 1]} {dt.year}"


def _format_short_date(dt):
    """'20 Oct 1:00 PM'"""
    h = dt.hour
    ampm = "PM" if h >= 12 else "AM"
    h = h % 12 or 12
    return f"{dt.day} {SHORT_MONTHS[dt.month - 1]} {h}:{dt.minute:02d} {ampm}"


def build_model(apps_config, app_reviews, since_dt, now_dt):
    """Build the email model from fetched reviews.

    Args:
        apps_config: list of app dicts from reviews_apps.json
        app_reviews: dict mapping app name -> {"appstore": [...], "playstore": [...]}
        since_dt: datetime, start of the lookback window
        now_dt: datetime, current time

    Returns:
        dict with all data needed by the renderer.
    """
    apps = []
    grand_total = 0
    all_reviews = []

    for app_cfg in apps_config:
        name = app_cfg["name"]
        data = app_reviews.get(name, {"appstore": [], "playstore": []})
        appstore = data.get("appstore", [])
        playstore = data.get("playstore", [])

        for r in appstore + playstore:
            lang = detect_language(r.get("body", ""))
            r["is_non_english"] = lang != "en"

        count = len(appstore) + len(playstore)
        grand_total += count
        all_reviews.extend(appstore + playstore)

        apps.append({
            "name": name,
            "appstore_reviews": sorted(appstore, key=lambda r: r["date"], reverse=True),
            "playstore_reviews": sorted(playstore, key=lambda r: r["date"], reverse=True),
            "count": count,
            "avg_rating": _avg_rating(appstore + playstore),
        })

    return {
        "apps": apps,
        "app_names": [a["name"] for a in apps_config],
        "grand_total": grand_total,
        "overall_avg": _avg_rating(all_reviews),
        "date_str": _format_date(now_dt),
        "since_str": _format_short_date(since_dt),
    }
