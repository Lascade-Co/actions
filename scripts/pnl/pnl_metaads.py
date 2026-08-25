"""Meta Ads month-to-date spend from the Graph API.

Explicit include list: only the accounts named in ``account_ids`` count. One
account failing degrades only itself — the others still contribute.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from typing import Callable, Optional

import requests

from pnl_fx import RateTable, convert_all
from pnl_money import Amount, SourceValue, Unavailable, to_decimal

API_VERSION = "v26.0"
_BASE = f"https://graph.facebook.com/{API_VERSION}"
_TIMEOUT = 120


def _insights(token: str, account_id: str, start: date, end: date) -> list:
    response = requests.get(
        f"{_BASE}/act_{account_id}/insights",
        params={
            "fields": "spend,account_currency",
            "level": "account",
            "time_range": json.dumps({"since": start.isoformat(), "until": end.isoformat()}),
            "access_token": token,
        },
        timeout=_TIMEOUT,
    )
    response.raise_for_status()
    return response.json().get("data", [])


def _daily_insights(token: str, account_id: str, start: date, end: date) -> list:
    url = f"{_BASE}/act_{account_id}/insights"
    params = {
        "fields": "spend,account_currency,date_start",
        "level": "account",
        "time_increment": 1,
        "limit": 500,
        "time_range": json.dumps({"since": start.isoformat(), "until": end.isoformat()}),
        "access_token": token,
    }
    rows = []
    while url:
        response = requests.get(url, params=params, timeout=_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        rows.extend(payload.get("data", []))
        url = payload.get("paging", {}).get("next")
        params = None
    return rows


def fetch_meta_ads(
    creds: dict,
    today: date,
    table: RateTable,
    insights: Optional[Callable[[str, str, date, date], list]] = None,
) -> SourceValue:
    insights = insights or _insights
    accounts = [str(a).replace("act_", "") for a in creds.get("account_ids", [])]
    if not accounts:
        return Unavailable("Meta Ads: no account ids configured")

    start, end = today.replace(day=1), today
    by_currency: dict = {}
    failures = []
    for account_id in accounts:
        try:
            for row in insights(creds["token"], account_id, start, end):
                spend = to_decimal(row.get("spend", "0"))
                code = row.get("account_currency", "USD")
                by_currency[code] = by_currency.get(code, Decimal("0")) + spend
        except Exception as exc:
            failures.append(f"{account_id} ({type(exc).__name__})")

    if failures:
        # Any account failing refuses the whole source, matching Google Ads and
        # the all-or-nothing currency rule. Summing the accounts that answered
        # would understate spend, and understated spend overstates the net —
        # the one error a reader cannot see.
        return Unavailable(f"Meta Ads: account(s) failed — {', '.join(failures)}")

    total, blocked = convert_all(by_currency, table)
    if total is None:
        return Unavailable(f"Meta Ads: no USD rate for {', '.join(blocked)}")
    return Amount(total)


def fetch_meta_ads_daily(
    creds: dict,
    today: date,
    table: RateTable,
    insights: Optional[Callable[[str, str, date, date], list]] = None,
):
    """Return current-month spend grouped by Meta's ``date_start``."""
    insights = insights or _daily_insights
    accounts = [str(value).replace("act_", "") for value in creds.get("account_ids", [])]
    if not accounts:
        return Unavailable("Meta Ads: no account ids configured")

    start = today.replace(day=1)
    by_day = {}
    failures = []
    for account_id in accounts:
        try:
            for row in insights(creds["token"], account_id, start, today):
                day_text = row.get("date_start")
                if not day_text:
                    return Unavailable(
                        f"Meta Ads: account {account_id} returned no date_start"
                    )
                current = date.fromisoformat(day_text)
                code = row.get("account_currency", "USD")
                bucket = by_day.setdefault(current, {})
                bucket[code] = bucket.get(code, Decimal("0")) + to_decimal(
                    row.get("spend", "0")
                )
        except Exception as exc:
            failures.append(f"{account_id} ({type(exc).__name__})")
    if failures:
        return Unavailable(f"Meta Ads: account(s) failed — {', '.join(failures)}")

    dates = [
        start + timedelta(days=index)
        for index in range((today - start).days + 1)
    ]
    result = {}
    blocked_codes = set()
    for current in dates:
        total, blocked = convert_all(by_day.get(current, {}), table)
        if total is None:
            blocked_codes.update(blocked)
        else:
            result[current] = total
    if blocked_codes:
        return Unavailable(
            f"Meta Ads: no USD rate for {', '.join(sorted(blocked_codes))}"
        )
    return result
