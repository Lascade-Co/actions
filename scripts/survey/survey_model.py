#!/usr/bin/env python3
"""Pure data layer: filter, detect language, build the email model. No I/O."""
from datetime import datetime, timezone

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


def _parse_ts(raw):
    """Parse a Google Sheets timestamp string into a UTC-aware datetime."""
    raw = str(raw).strip()
    for fmt in _TIMESTAMP_FMTS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def filter_new_responses(rows, cutoff_dt):
    """Return rows whose timestamp (col 0) is after *cutoff_dt*."""
    out = []
    for row in rows:
        if not row:
            continue
        ts = _parse_ts(row[0])
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

    *fetched_tabs* is a dict mapping sheet_name → list of rows (header excluded).
    *translate_fn* is called as translate_fn(text) → translated_text. May be None.
    """
    sections = []
    total = 0
    free_count = 0
    pro_count = 0

    for survey in config["surveys"]:
        rows = fetched_tabs.get(survey["sheet_name"], [])
        filtered = filter_new_responses(rows, cutoff_dt)
        if not filtered:
            continue

        rendered_rows = []
        for row in filtered:
            total += 1
            status_raw = _cell_value(row, 2).lower()
            is_pro = status_raw == "pro"
            if is_pro:
                pro_count += 1
            else:
                free_count += 1

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

                if not val and empty_text:
                    val = empty_text
                    color = empty_color
                elif not val:
                    val = "—"
                    color = "#aaa"

                if val in color_map:
                    color = color_map[val]
                if val in bold_list:
                    bold = True

                cells.append({
                    "value": val,
                    "color": color,
                    "bold": bold,
                    "translated": was_translated,
                })

            rendered_rows.append({"is_pro": is_pro, "cells": cells})

        sections.append({
            "label": survey["label"],
            "subtitle": survey["subtitle"],
            "columns": [c["header"] for c in survey["columns"]],
            "rows": rendered_rows,
            "count": len(rendered_rows),
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
        "sections": sections,
        "total": total,
        "free_count": free_count,
        "pro_count": pro_count,
        "date_str": date_str,
        "cutoff_str": cutoff_str,
        "sheet_url": config.get("sheet_url", ""),
    }
