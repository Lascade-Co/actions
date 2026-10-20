#!/usr/bin/env python3
"""Pure data layer: filter, detect language, build the email model. No I/O."""
from datetime import datetime, timedelta, timezone

try:
    from langdetect import detect as _detect
except ImportError:
    _detect = None

_TIMESTAMP_FMTS = [
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
]


_IST = timezone(timedelta(hours=5, minutes=30))

_TZ_MAP = {
    "Asia/Kolkata": _IST,
    "UTC": timezone.utc,
}


_SHEETS_EPOCH = datetime(1899, 12, 30)

_FUTURE_SLACK = timedelta(hours=2)


def _parse_ts(raw, tz=None, now_utc=None):
    """Parse a Google Sheets timestamp into a tz-aware datetime.

    Handles serial date numbers (from dateTimeRenderOption=SERIAL_NUMBER)
    as well as formatted date strings.  *tz* is the timezone the sheet
    timestamps are in (default: UTC).

    When *now_utc* is provided, any candidate that falls in the future
    (beyond a small slack) is rejected and the next format is tried.
    This guards against dd/mm ↔ mm/dd misparses that produce future dates.
    """
    raw = str(raw).strip()
    if tz is None:
        tz = timezone.utc
    ceiling = (now_utc + _FUTURE_SLACK) if now_utc else None

    def _accept(dt):
        if ceiling and dt > ceiling:
            return None
        return dt

    # Serial date number (days since 1899-12-30)
    try:
        serial = float(raw)
        if 1 < serial < 200000:
            dt = _SHEETS_EPOCH + timedelta(days=serial)
            result = _accept(dt.replace(tzinfo=tz))
            if result:
                return result
    except (ValueError, OverflowError):
        pass
    for fmt in _TIMESTAMP_FMTS:
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=tz)
            result = _accept(dt)
            if result:
                return result
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(raw).replace(tzinfo=tz)
        return _accept(dt)
    except Exception:
        pass
    return None


def filter_new_responses(rows, cutoff_dt, tz=None, now_utc=None):
    """Return rows whose timestamp (col 0) is after *cutoff_dt*."""
    out = []
    for row in rows:
        if not row:
            continue
        ts = _parse_ts(row[0], tz, now_utc=now_utc)
        if ts and ts > cutoff_dt:
            out.append(row)
    return out


def detect_and_translate(text, translate_fn):
    """Return (display_text, was_translated). Uses langdetect if available."""
    if not text or len(text) < 20:
        return text, False
    if _detect is None:
        return text, False
    try:
        lang = _detect(text)
    except Exception:
        return text, False
    if lang == "en":
        return text, False
    translated = translate_fn(text)
    if translated and translated != text:
        return translated, True
    return text, False


def _cell_value(row, col_index):
    """Safely get a cell value from a row."""
    if col_index < len(row):
        return str(row[col_index]).strip()
    return ""


def build_model(config, fetched_tabs, cutoff_dt, now_dt, translate_fn=None):
    """Build the template model from config + raw sheet data.

    Supports two status patterns:
    - Global: config has "has_pro_free": true + "status_col" (Smart Ruler)
    - Per-survey: survey has "has_status": true + "status_col" (Marine Radar Map Survey)
    """
    sections = []
    total = 0
    free_count = 0
    pro_count = 0

    sheet_tz = _TZ_MAP.get(config.get("timezone", "UTC"), timezone.utc)
    global_status = config.get("has_pro_free", False)
    global_status_col = config.get("status_col", 2)

    for survey in config["surveys"]:
        rows = fetched_tabs.get(survey["sheet_name"], [])
        filtered = filter_new_responses(rows, cutoff_dt, sheet_tz, now_utc=now_dt)
        if not filtered:
            continue

        has_status = survey.get("has_status", global_status)
        status_col = survey.get("status_col", global_status_col)
        status_values = survey.get("status_values", {"pro": "Pro", "free": "Free"})

        rendered_rows = []
        for row in filtered:
            total += 1

            status_label = None
            status_color = None
            if has_status:
                status_raw = _cell_value(row, status_col).lower()
                is_pro = status_raw in ("pro", "premium")
                if is_pro:
                    pro_count += 1
                    status_label = status_values.get(status_raw, "Pro")
                else:
                    free_count += 1
                    status_label = status_values.get(status_raw, "Free")
                status_color = "#5a5aad" if is_pro else "#888"

            cells = []
            for col_def in survey["columns"]:
                val = _cell_value(row, col_def["col"])
                was_translated = False
                if translate_fn and val:
                    val, was_translated = detect_and_translate(val, translate_fn)

                color = "#222"
                bold = False
                empty_text = col_def.get("empty_text")
                empty_color = col_def.get("empty_color", "#aaa")
                color_map = col_def.get("color_map", {})
                bold_list = col_def.get("bold", [])
                bold_all = col_def.get("bold_all", False)
                is_score = col_def.get("is_score", False)

                if not val and empty_text:
                    val = empty_text
                    color = empty_color
                elif not val:
                    val = "—"
                    color = "#aaa"

                if is_score and val:
                    try:
                        score = int(val)
                        val = f"{score}/5"
                        if score >= 4:
                            color = "#2a8c4a"
                        elif score == 3:
                            color = "#c9922a"
                        else:
                            color = "#c0392b"
                        bold = True
                    except ValueError:
                        pass

                if val in color_map:
                    color = color_map[val]
                if bold_all:
                    bold = True
                elif isinstance(bold_list, list) and val in bold_list:
                    bold = True

                cells.append({
                    "value": val,
                    "color": color,
                    "bold": bold,
                    "translated": was_translated,
                })

            rendered_rows.append({
                "cells": cells,
                "status_label": status_label,
                "status_color": status_color,
            })

        sections.append({
            "label": survey["label"],
            "subtitle": survey["subtitle"],
            "columns": [c["header"] for c in survey["columns"]],
            "rows": rendered_rows,
            "count": len(rendered_rows),
            "has_status": has_status,
        })

    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    short_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    date_str = f"{weekdays[now_dt.weekday()]}, {now_dt.day} {months[now_dt.month - 1]} {now_dt.year}"

    cutoff_h = cutoff_dt.hour % 12 or 12
    cutoff_ampm = "AM" if cutoff_dt.hour < 12 else "PM"
    cutoff_str = f"{cutoff_dt.day} {short_months[cutoff_dt.month - 1]} {cutoff_h}:{cutoff_dt.minute:02d} {cutoff_ampm}"

    return {
        "product_name": config.get("product_name", "Survey"),
        "email_title": config.get("email_title", "Survey Responses"),
        "sections": sections,
        "total": total,
        "free_count": free_count,
        "pro_count": pro_count,
        "date_str": date_str,
        "cutoff_str": cutoff_str,
        "sheet_url": config.get("sheet_url", ""),
    }
