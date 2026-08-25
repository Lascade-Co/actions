"""Month-to-date spend for one PNL head.

The figure already excludes recurring spends, counts enabled rows only, and is
signed so a refund reduces the line. Do not re-derive or adjust it.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Callable, Optional

import requests

from pnl_money import Amount, SourceValue, Unavailable, to_decimal

_TIMEOUT = 30


def fetch_head_spend(
    base_url: str,
    api_key: str,
    head: str,
    day: Optional[int] = None,
    get: Optional[Callable] = None,
) -> SourceValue:
    get = get or requests.get
    url = f"{base_url.rstrip('/')}/api/head-spend/"
    params = {"head": head}
    if day is not None:
        params["day"] = day
    try:
        response = get(url, params=params, headers={"X-Api-Key": api_key}, timeout=_TIMEOUT)
    except Exception as exc:  # network, DNS, TLS
        return Unavailable(f"PNL spend request failed: {type(exc).__name__}")

    if response.status_code == 404:
        # Deliberately not zero: the endpoint distinguishes "no such head" from
        # "quiet month" so a typo surfaces instead of reporting a plausible zero
        # every day forever.
        return Unavailable(f"PNL has no head {head!r} — check the configured key")
    if response.status_code != 200:
        if response.status_code == 409 and day is not None:
            return Unavailable("PNL daily spend attribution is unavailable")
        return Unavailable(f"PNL spend returned {response.status_code}")

    try:
        payload = response.json()
        if day is not None and payload.get("day") != day:
            raise KeyError("day")
        return Amount(to_decimal(payload["spend_usd"]))
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        return Unavailable(f"PNL spend payload unreadable: {type(exc).__name__}")


def fetch_head_spend_daily(
    base_url: str,
    api_key: str,
    head: str,
    today: date,
    get: Optional[Callable] = None,
):
    """Fetch exact current-month spend per calendar day from PNL."""
    first = today.replace(day=1)
    dates = [first + timedelta(days=index) for index in range((today - first).days + 1)]
    result = {}
    for current in dates:
        value = fetch_head_spend(
            base_url, api_key, head, day=current.day, get=get
        )
        if isinstance(value, Unavailable):
            return value
        result[current] = value.usd
    return result
