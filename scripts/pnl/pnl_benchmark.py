"""Load a closed-month benchmark and compare it with today's displayed net.

The benchmark is deliberately a secret rather than a repository file: daily
revenue and ad spend are business data. Values remain strings in JSON so money
never round-trips through a binary float.
"""

from __future__ import annotations

import base64
import binascii
import calendar
import json
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from pnl_money import Amount, Unavailable, round_usd

SCHEMA_VERSION = 1
REVENUE_SOURCES = ("App Store", "Play Store")
SPEND_SOURCES = ("Influencer", "Google Ads", "Meta Ads")
SOURCES = REVENUE_SOURCES + SPEND_SOURCES


class BenchmarkError(ValueError):
    pass


def _amount(raw, path: str) -> Decimal:
    if not isinstance(raw, str):
        raise BenchmarkError(f"{path} must be a decimal string")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise BenchmarkError(f"{path} is not a decimal string") from exc
    if not value.is_finite():
        raise BenchmarkError(f"{path} must be finite")
    return value


def _month(raw) -> date:
    if not isinstance(raw, str):
        raise BenchmarkError("month must be YYYY-MM")
    try:
        parsed = datetime.strptime(raw, "%Y-%m").date()
    except ValueError as exc:
        raise BenchmarkError("month must be YYYY-MM") from exc
    return parsed.replace(day=1)


def _expected_dates(month: date) -> list[str]:
    count = calendar.monthrange(month.year, month.month)[1]
    return [(month + timedelta(days=index)).isoformat() for index in range(count)]


def decode_benchmark(encoded: str) -> dict:
    """Decode and strictly validate ``MARKETING_NET_BENCHMARK_B64``."""
    try:
        raw = base64.b64decode(encoded, validate=True).decode("utf-8")
        payload = json.loads(raw)
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise BenchmarkError("value is not base64-encoded UTF-8 JSON") from exc

    if not isinstance(payload, dict):
        raise BenchmarkError("root must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise BenchmarkError(f"schema_version must be {SCHEMA_VERSION}")
    if payload.get("currency") != "USD":
        raise BenchmarkError("currency must be USD")

    month = _month(payload.get("month"))
    categories = payload.get("categories")
    if categories != {
        "revenue": list(REVENUE_SOURCES),
        "spend": list(SPEND_SOURCES),
    }:
        raise BenchmarkError("categories must contain the exact revenue and spend source order")
    days = payload.get("days")
    if not isinstance(days, dict):
        raise BenchmarkError("days must be an object")

    expected = _expected_dates(month)
    if set(days) != set(expected):
        missing = sorted(set(expected) - set(days))
        extra = sorted(set(days) - set(expected))
        details = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if extra:
            details.append(f"unexpected {', '.join(extra)}")
        raise BenchmarkError("days do not exactly cover the month: " + "; ".join(details))

    normalized = {}
    for day in expected:
        item = days[day]
        if not isinstance(item, list) or len(item) != len(SOURCES):
            raise BenchmarkError(f"days.{day} must contain exactly {len(SOURCES)} values")
        values = {
            label: _amount(item[index], f"days.{day}.{label}")
            for index, label in enumerate(SOURCES)
        }
        normalized[day] = {
            "revenue": {label: values[label] for label in REVENUE_SOURCES},
            "spend": {label: values[label] for label in SPEND_SOURCES},
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "month": month,
        "currency": "USD",
        "days": normalized,
    }


def benchmark_report(benchmark: dict, today: date) -> dict:
    """Build the same per-source windows as the live month-to-date report.

    App Store stops one day earlier; every other source includes the comparison
    day. This is the same deliberate window mismatch shown in the live card.
    """
    month = benchmark["month"]
    last_day = calendar.monthrange(month.year, month.month)[1]
    through = min(today.day, last_day)
    revenue = {label: Decimal("0") for label in REVENUE_SOURCES}
    spend = {label: Decimal("0") for label in SPEND_SOURCES}

    for day_text, item in benchmark["days"].items():
        day_number = date.fromisoformat(day_text).day
        if day_number <= through - 1:
            revenue["App Store"] += item["revenue"]["App Store"]
        if day_number <= through:
            revenue["Play Store"] += item["revenue"]["Play Store"]
            for label in SPEND_SOURCES:
                spend[label] += item["spend"][label]

    return {
        "revenue": {label: Amount(value) for label, value in revenue.items()},
        "spend": {label: Amount(value) for label, value in spend.items()},
        "through": through,
    }


def displayed_net(report: dict):
    """Net using the same round-each-line-once arithmetic as both renderers."""
    values = [
        report[section].get(label)
        for section, labels in (("revenue", REVENUE_SOURCES), ("spend", SPEND_SOURCES))
        for label in labels
    ]
    if not all(isinstance(value, Amount) for value in values):
        return Unavailable("comparison requires all five sources")
    revenue = sum(
        (round_usd(report["revenue"][label].usd) for label in REVENUE_SOURCES),
        Decimal("0"),
    )
    spend = sum(
        (round_usd(report["spend"][label].usd) for label in SPEND_SOURCES),
        Decimal("0"),
    )
    return Amount(revenue - spend)


def comparison(report: dict, benchmark: dict, today: date) -> dict:
    reference = benchmark_report(benchmark, today)
    current_net = displayed_net(report)
    reference_net = displayed_net(reference)
    label = f"vs {benchmark['month']:%b} 1–{reference['through']}"
    if not isinstance(current_net, Amount):
        return {"label": label, "value": current_net}
    return {"label": label, "value": Amount(current_net.usd - reference_net.usd)}
