"""Month-to-date spend for one PNL head.

The figure already excludes recurring spends, counts enabled rows only, and is
signed so a refund reduces the line. Do not re-derive or adjust it.
"""

from __future__ import annotations

from typing import Callable, Optional

import requests

from pnl_money import Amount, SourceValue, Unavailable, to_decimal

_TIMEOUT = 30


def fetch_head_spend(
    base_url: str,
    api_key: str,
    head: str,
    get: Optional[Callable] = None,
) -> SourceValue:
    get = get or requests.get
    url = f"{base_url.rstrip('/')}/api/head-spend/"
    try:
        response = get(url, params={"head": head}, headers={"X-Api-Key": api_key}, timeout=_TIMEOUT)
    except Exception as exc:  # network, DNS, TLS
        return Unavailable(f"PNL spend request failed: {type(exc).__name__}")

    if response.status_code == 404:
        # Deliberately not zero: the endpoint distinguishes "no such head" from
        # "quiet month" so a typo surfaces instead of reporting a plausible zero
        # every day forever.
        return Unavailable(f"PNL has no head {head!r} — check the configured key")
    if response.status_code != 200:
        return Unavailable(f"PNL spend returned {response.status_code}")

    try:
        return Amount(to_decimal(response.json()["spend_usd"]))
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        return Unavailable(f"PNL spend payload unreadable: {type(exc).__name__}")
