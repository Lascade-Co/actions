"""Google Ads month-to-date cost, over REST.

Include-by-default: every enabled non-manager child of the MCC counts, minus an
explicit skip list. A new ad account is therefore picked up automatically rather
than silently omitted, which is the failure that matters for a spend total.

REST rather than the google-ads SDK: the SDK couples the package version to the
API version, and all this needs is two queries.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Callable, Optional

import requests

from pnl_fx import RateTable, convert_all
from pnl_money import Amount, SourceValue, Unavailable, to_decimal

API_VERSION = "v25"
_BASE = f"https://googleads.googleapis.com/{API_VERSION}"
_TOKEN_URL = "https://oauth2.googleapis.com/token"
_MICROS = Decimal("1000000")
_TIMEOUT = 120

CHILDREN_QUERY = """
    SELECT customer_client.id, customer_client.currency_code
    FROM customer_client
    WHERE customer_client.manager = FALSE AND customer_client.status = 'ENABLED'
"""

COST_QUERY = """
    SELECT metrics.cost_micros, customer.currency_code
    FROM customer
    WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


def access_token(creds: dict) -> str:
    response = requests.post(
        _TOKEN_URL,
        data={
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "refresh_token": creds["refresh_token"],
            "grant_type": "refresh_token",
        },
        timeout=_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _search(token: str, customer_id: str, query: str) -> list:
    response = requests.post(
        f"{_BASE}/customers/{customer_id}/googleAds:searchStream",
        json={"query": query},
        headers={
            "Authorization": f"Bearer {token}",
            "developer-token": _search.dev_token,
            "login-customer-id": _search.login_customer_id,
        },
        timeout=_TIMEOUT,
    )
    response.raise_for_status()
    rows = []
    for chunk in response.json():
        rows.extend(chunk.get("results", []))
    return rows


def fetch_google_ads(
    creds: dict,
    today: date,
    table: RateTable,
    search: Optional[Callable[[str, str, str], list]] = None,
) -> SourceValue:
    manager = str(creds["login_customer_id"])
    skip = {str(s) for s in creds.get("skip_customer_ids", [])}

    try:
        if search is None:
            _search.dev_token = creds["dev_token"]
            _search.login_customer_id = manager
            token = access_token(creds)
            search = _search
        else:
            token = "injected"

        children = search(token, manager, CHILDREN_QUERY)
        accounts = []
        for row in children:
            client = row.get("customerClient", {})
            customer_id = str(client.get("id", ""))
            # manager = FALSE already drops the MCC, but the skip list is the
            # stated intent and must not depend on that coincidence.
            if customer_id and customer_id not in skip:
                accounts.append((customer_id, client.get("currencyCode", "USD")))

        if not accounts:
            return Unavailable("Google Ads: the MCC returned no eligible child accounts")

        query = COST_QUERY.format(start=today.replace(day=1).isoformat(), end=today.isoformat())
        by_currency: dict = {}
        for customer_id, currency in accounts:
            for row in search(token, customer_id, query):
                micros = to_decimal(row.get("metrics", {}).get("costMicros", 0))
                code = row.get("customer", {}).get("currencyCode") or currency
                by_currency[code] = by_currency.get(code, Decimal("0")) + micros / _MICROS
        total, blocked = convert_all(by_currency, table)
        if total is None:
            # Understated spend inflates the net just as convincingly as
            # understated revenue deflates it.
            return Unavailable(f"Google Ads: no USD rate for {', '.join(blocked)}")
        return Amount(total)
    except Exception as exc:
        return Unavailable(f"Google Ads request failed: {type(exc).__name__}")
