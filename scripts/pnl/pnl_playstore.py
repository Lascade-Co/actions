"""Google Play month-to-date, estimated by calibrating gross sales (ADR-0007).

Google publishes no mid-month net figure — earnings land around the middle of
the following month. The only in-month data is ``sales/``: estimated gross,
before commission, tax and refunds. Multiplying it by ``earnings / sales`` from
the last settled month yields something comparable to the App Store's net.

The identical summation rule must apply to the factor's month and the current
month. That symmetry is load-bearing: refund rows do not reconcile against
their components, and the factor only absorbs that because both sides sum the
same way.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

from pnl_fx import RateTable, convert_all
from pnl_money import Amount, SourceValue, Unavailable, to_decimal

_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
_API = "https://storage.googleapis.com/storage/v1/b"

_SALES_AMOUNT = "Charged Amount"
_SALES_CURRENCY = "Currency of Sale"
_EARNINGS_AMOUNT = "Amount (Merchant Currency)"
_EARNINGS_CURRENCY = "Merchant Currency"

#: The account id and sequence number follow the month, so the last
#: underscore-separated segment is the account number, not the month.
_MONTH = re.compile(r"_(\d{6})")


def month_from_earnings_name(name: str) -> Optional[str]:
    match = _MONTH.search(name)
    return match.group(1) if match else None


def _sum_csv(text: str, amount_col: str, currency_col: str) -> dict:
    totals: dict = {}
    for row in csv.DictReader(io.StringIO(text)):
        if amount_col not in row or currency_col not in row:
            return {}
        try:
            amount = to_decimal((row[amount_col] or "0").replace(",", ""))
        except (InvalidOperation, ValueError):
            continue
        code = (row[currency_col] or "").strip().upper()
        totals[code] = totals.get(code, Decimal("0")) + amount
    return totals


def sum_sales_csv(text: str) -> dict:
    return _sum_csv(text, _SALES_AMOUNT, _SALES_CURRENCY)


def sum_earnings_csv(text: str) -> dict:
    return _sum_csv(text, _EARNINGS_AMOUNT, _EARNINGS_CURRENCY)


def _to_usd_total(totals: dict, table: RateTable) -> Optional[Decimal]:
    """USD sum, or ``None`` if any currency in ``totals`` has no rate.

    A month of Play sales spans ~56 currencies, so this is the hot path for
    rate lookups — the table resolves each new code once and caches it.
    """
    total, _ = convert_all(totals, table)
    return total


def derive_net_factor(sales_by_month: dict, earnings_by_month: dict, table: RateTable) -> Optional[Decimal]:
    """``earnings / sales`` for the most recent month carrying both reports."""
    for month in sorted(set(sales_by_month) & set(earnings_by_month), reverse=True):
        sales = _to_usd_total(sales_by_month[month], table)
        earnings = _to_usd_total(earnings_by_month[month], table)
        if sales is None or earnings is None or sales <= 0:
            continue
        factor = earnings / sales
        # Guard the derived value, not just the divisor: a zero-earnings month
        # gives exactly 0, which passes an `is None` check and would report Play
        # as contributing precisely nothing.
        if factor <= 0:
            continue
        return factor
    return None


class GcsStorage:
    def __init__(self, bucket: str, credentials_info: dict):
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=[_SCOPE]
        )
        self.session = AuthorizedSession(creds)
        self.bucket = bucket

    def list(self, prefix: str) -> list:
        from urllib.parse import quote

        names, token = [], None
        while True:
            params = {"prefix": prefix}
            if token:
                params["pageToken"] = token
            response = self.session.get(
                f"{_API}/{quote(self.bucket)}/o", params=params, timeout=60
            )
            response.raise_for_status()
            payload = response.json()
            names.extend(item["name"] for item in payload.get("items", []))
            token = payload.get("nextPageToken")
            if not token:
                return names

    def read_zip_csv(self, name: str) -> str:
        from urllib.parse import quote

        response = self.session.get(
            f"{_API}/{quote(self.bucket)}/o/{quote(name, safe='')}",
            params={"alt": "media"},
            timeout=180,
        )
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            parts = []
            for entry in archive.namelist():
                raw = archive.read(entry)
                # Earnings ship UTF-8; stats exports ship UTF-16 with a BOM.
                if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
                    parts.append(raw.decode("utf-16"))
                else:
                    parts.append(raw.decode("utf-8-sig"))
            return "\n".join(parts)


def _index_by_month(names: list) -> dict:
    index: dict = {}
    for name in names:
        month = month_from_earnings_name(name)
        if month:
            index.setdefault(month, []).append(name)
    return index


def _read_totals(storage, names: list, summer) -> dict:
    totals: dict = {}
    for name in names:
        for code, amount in summer(storage.read_zip_csv(name)).items():
            totals[code] = totals.get(code, Decimal("0")) + amount
    return totals


def fetch_playstore(config: dict, today: date, table: RateTable, storage=None) -> SourceValue:
    storage = storage or GcsStorage(config["bucket"], config["credentials"])
    try:
        sales_names = _index_by_month(storage.list("sales/"))
        earnings_names = _index_by_month(storage.list("earnings/"))

        current = f"{today:%Y%m}"
        if current not in sales_names:
            return Unavailable(f"Play Store: no sales report for {current}")

        # Read only what is needed. Building a full month map would download all
        # ~35 sales archives and ~34 earnings archives on every single run.
        factor = None
        for month in sorted(set(sales_names) & set(earnings_names), reverse=True):
            candidate = derive_net_factor(
                {month: _read_totals(storage, sales_names[month], sum_sales_csv)},
                {month: _read_totals(storage, earnings_names[month], sum_earnings_csv)},
                table,
            )
            if candidate is not None:
                factor = candidate
                break

        if factor is None:
            return Unavailable("Play Store: no net factor derivable — excluded")

        gross, blocked = convert_all(
            _read_totals(storage, sales_names[current], sum_sales_csv), table
        )
        if gross is None:
            return Unavailable(f"Play Store: no USD rate for {', '.join(blocked)}")
        return Amount(gross * factor)
    except Exception as exc:
        return Unavailable(f"Play Store access failed: {type(exc).__name__}")
