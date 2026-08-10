"""Meta Ads month-to-date spend from the Graph API.

Explicit include list: only the accounts named in ``account_ids`` count. One
account failing degrades only itself — the others still contribute.
"""

from __future__ import annotations

import json
from datetime import date
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

    if len(failures) == len(accounts):
        return Unavailable(f"Meta Ads: every account failed — {', '.join(failures)}")

    total, blocked = convert_all(by_currency, table)
    if total is None:
        return Unavailable(f"Meta Ads: no USD rate for {', '.join(blocked)}")
    return Amount(total)
