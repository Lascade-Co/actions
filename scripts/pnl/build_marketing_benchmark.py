#!/usr/bin/env python3
"""Fetch one closed month of daily Marketing Net source data.

The output contains private financial data and is gitignored. Encode the JSON
as base64 and store it as ``MARKETING_NET_BENCHMARK_B64`` in Infisical.
"""

from __future__ import annotations

import argparse
import base64
import calendar
import csv
import io
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Optional

import requests

import pnl_appstore
import pnl_googleads
import pnl_metaads
from pnl_appstore import DayCache, parse_sales_tsv
from pnl_benchmark import decode_benchmark
from pnl_fx import RateTable, build_rate_table, convert_all
from pnl_money import to_decimal
from pnl_playstore import (
    GcsStorage,
    _index_by_month,
    _read_totals,
    derive_net_factor,
    sum_earnings_csv,
    sum_sales_csv,
)

_GOOGLE_DAILY_QUERY = """
    SELECT segments.date, metrics.cost_micros, customer.currency_code
    FROM customer
    WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


class BuildError(RuntimeError):
    pass


def month_dates(month: date) -> list[date]:
    first = month.replace(day=1)
    count = calendar.monthrange(first.year, first.month)[1]
    return [first + timedelta(days=index) for index in range(count)]


def _convert(totals: dict, table: RateTable, label: str) -> Decimal:
    total, blocked = convert_all(totals, table)
    if total is None:
        raise BuildError(f"{label}: no USD rate for {', '.join(blocked)}")
    return total


def fetch_appstore_daily(
    config: dict,
    dates: list[date],
    table: RateTable,
    fetch_day: Optional[Callable] = None,
    cache: Optional[DayCache] = None,
) -> dict[date, Decimal]:
    fetch_day = fetch_day or pnl_appstore._fetch_day
    result = {}
    for day in dates:
        totals = cache.get(day) if cache else None
        if totals is None:
            text = fetch_day(config, day)
            if text is None:
                result[day] = Decimal("0")
                continue
            totals = parse_sales_tsv(text)
            if not totals:
                raise BuildError(f"App Store {day}: no rows parsed — check the report layout")
            if cache:
                cache.put(day, totals)
        result[day] = _convert(totals, table, f"App Store {day}")
    return result


def _sales_daily_csv(text: str) -> dict[date, dict]:
    result: dict = {}
    required = ("Order Charged Date", "Charged Amount", "Currency of Sale")
    for row in csv.DictReader(io.StringIO(text)):
        if any(column not in row for column in required):
            return {}
        try:
            day = date.fromisoformat((row["Order Charged Date"] or "")[:10])
            amount = to_decimal((row["Charged Amount"] or "0").replace(",", ""))
        except (InvalidOperation, ValueError, TypeError):
            continue
        code = (row["Currency of Sale"] or "").strip().upper()
        bucket = result.setdefault(day, {})
        bucket[code] = bucket.get(code, Decimal("0")) + amount
    return result


def _merge_daily(target: dict, incoming: dict) -> None:
    for day, totals in incoming.items():
        bucket = target.setdefault(day, {})
        for code, amount in totals.items():
            bucket[code] = bucket.get(code, Decimal("0")) + amount


def fetch_playstore_daily(
    config: dict,
    dates: list[date],
    table: RateTable,
    storage=None,
) -> dict[date, Decimal]:
    storage = storage or GcsStorage(config["bucket"], config["credentials"])
    target = f"{dates[0]:%Y%m}"
    sales_names = _index_by_month(storage.list("sales/"))
    earnings_names = _index_by_month(storage.list("earnings/"))
    if target not in sales_names or target not in earnings_names:
        raise BuildError(f"Play Store: {target} is not settled")

    factor = derive_net_factor(
        {target: _read_totals(storage, sales_names[target], sum_sales_csv)},
        {target: _read_totals(storage, earnings_names[target], sum_earnings_csv)},
        table,
    )
    if factor is None:
        raise BuildError(f"Play Store: no net factor derivable for {target}")

    by_day = {}
    for name in sales_names[target]:
        parsed = _sales_daily_csv(storage.read_zip_csv(name))
        if not parsed:
            raise BuildError(f"Play Store: no daily rows parsed from {name}")
        _merge_daily(by_day, parsed)
    return {
        day: _convert(by_day.get(day, {}), table, f"Play Store {day}") * factor
        for day in dates
    }


def _google_session(creds: dict, search: Optional[Callable]):
    manager = str(creds["login_customer_id"])
    if search is not None:
        return "injected", manager, search
    pnl_googleads._search.dev_token = creds["dev_token"]
    pnl_googleads._search.login_customer_id = manager
    return pnl_googleads.access_token(creds), manager, pnl_googleads._search


def fetch_google_ads_daily(
    creds: dict,
    dates: list[date],
    table: RateTable,
    search: Optional[Callable] = None,
) -> dict[date, Decimal]:
    token, manager, search = _google_session(creds, search)
    skip = {str(value) for value in creds.get("skip_customer_ids", [])}
    children = search(token, manager, pnl_googleads.CHILDREN_QUERY)
    accounts = []
    for row in children:
        client = row.get("customerClient", {})
        customer_id = str(client.get("id", ""))
        if customer_id and customer_id not in skip:
            accounts.append((customer_id, client.get("currencyCode", "USD")))
    if not accounts:
        raise BuildError("Google Ads: the MCC returned no eligible child accounts")

    start, end = dates[0], dates[-1]
    query = _GOOGLE_DAILY_QUERY.format(start=start.isoformat(), end=end.isoformat())
    by_day = {}
    for customer_id, fallback_currency in accounts:
        for row in search(token, customer_id, query):
            day_text = row.get("segments", {}).get("date")
            if not day_text:
                raise BuildError(f"Google Ads: account {customer_id} returned no segments.date")
            day = date.fromisoformat(day_text)
            micros = to_decimal(row.get("metrics", {}).get("costMicros", 0))
            code = row.get("customer", {}).get("currencyCode") or fallback_currency
            bucket = by_day.setdefault(day, {})
            bucket[code] = bucket.get(code, Decimal("0")) + micros / Decimal("1000000")
    return {
        day: _convert(by_day.get(day, {}), table, f"Google Ads {day}")
        for day in dates
    }


def _meta_daily_rows(token: str, account_id: str, start: date, end: date) -> list:
    url = f"{pnl_metaads._BASE}/act_{account_id}/insights"
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
        response = requests.get(url, params=params, timeout=pnl_metaads._TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        rows.extend(payload.get("data", []))
        url = payload.get("paging", {}).get("next")
        params = None
    return rows


def fetch_meta_ads_daily(
    creds: dict,
    dates: list[date],
    table: RateTable,
    insights: Optional[Callable] = None,
) -> dict[date, Decimal]:
    insights = insights or _meta_daily_rows
    accounts = [str(value).replace("act_", "") for value in creds.get("account_ids", [])]
    if not accounts:
        raise BuildError("Meta Ads: no account ids configured")
    by_day = {}
    for account_id in accounts:
        for row in insights(creds["token"], account_id, dates[0], dates[-1]):
            day_text = row.get("date_start")
            if not day_text:
                raise BuildError(f"Meta Ads: account {account_id} returned no date_start")
            day = date.fromisoformat(day_text)
            code = row.get("account_currency", "USD")
            bucket = by_day.setdefault(day, {})
            bucket[code] = bucket.get(code, Decimal("0")) + to_decimal(row.get("spend", "0"))
    return {
        day: _convert(by_day.get(day, {}), table, f"Meta Ads {day}")
        for day in dates
    }


def load_influencer_daily(path: Optional[str], dates: list[date], zero: bool) -> dict[date, Decimal]:
    if zero:
        return {day: Decimal("0") for day in dates}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BuildError(f"Influencer input is unreadable: {type(exc).__name__}") from exc
    expected = {day.isoformat() for day in dates}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise BuildError("Influencer input must contain exactly one decimal string per month day")
    result = {}
    for day in dates:
        raw = payload[day.isoformat()]
        if not isinstance(raw, str):
            raise BuildError(f"Influencer {day} must be a decimal string")
        try:
            result[day] = Decimal(raw)
        except InvalidOperation as exc:
            raise BuildError(f"Influencer {day} is not a decimal string") from exc
    return result


def _money(value: Decimal) -> str:
    return format(value, "f")


def build_payload(
    month: date,
    fx_day: date,
    appstore: dict,
    playstore: dict,
    influencer: dict,
    google: dict,
    meta: dict,
) -> dict:
    days = {}
    for day in month_dates(month):
        days[day.isoformat()] = [
            _money(appstore[day]),
            _money(playstore[day]),
            _money(influencer[day]),
            _money(google[day]),
            _money(meta[day]),
        ]
    return {
        "schema_version": 1,
        "month": f"{month:%Y-%m}",
        "currency": "USD",
        "categories": {
            "revenue": ["App Store", "Play Store"],
            "spend": ["Influencer", "Google Ads", "Meta Ads"],
        },
        "fx_rate_date": fx_day.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "play_store_method": "settled target-month earnings/sales factor",
        "days": days,
    }


def _decoded(name: str) -> dict:
    try:
        return json.loads(base64.b64decode(os.environ[name]).decode("utf-8"))
    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise BuildError(f"{name} is missing or invalid") from exc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Fetch a closed Marketing Net benchmark month")
    parser.add_argument("--month", required=True, help="closed month in YYYY-MM")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--ads-credentials-json",
        help="local ignored JSON file; otherwise ADS_CREDENTIALS_JSON_B64 is used",
    )
    influencer = parser.add_mutually_exclusive_group(required=True)
    influencer.add_argument("--influencer-daily-json")
    influencer.add_argument(
        "--influencer-zero",
        action="store_true",
        help="assert that the benchmark month had zero influencer spend",
    )
    args = parser.parse_args(argv)

    try:
        month = datetime.strptime(args.month, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        raise BuildError("--month must be YYYY-MM") from exc
    dates = month_dates(month)
    if dates[-1] >= datetime.now(timezone.utc).date():
        raise BuildError("benchmark month must be closed")
    fx_day = dates[-1]

    required = (
        "APPSTORE_ISSUER_ID",
        "APPSTORE_KEY_ID",
        "APPSTORE_P8_B64",
        "APPSTORE_VENDOR_NUMBER",
        "PLAYSTORE_SA_JSON_B64",
        "PLAYSTORE_BUCKET",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if not args.ads_credentials_json and not os.environ.get("ADS_CREDENTIALS_JSON_B64"):
        missing.append("ADS_CREDENTIALS_JSON_B64")
    if missing:
        raise BuildError("Missing required configuration: " + ", ".join(missing))

    appstore_config = {
        "issuer_id": os.environ["APPSTORE_ISSUER_ID"],
        "key_id": os.environ["APPSTORE_KEY_ID"],
        "p8": base64.b64decode(os.environ["APPSTORE_P8_B64"]).decode("utf-8"),
        "vendor_number": os.environ["APPSTORE_VENDOR_NUMBER"],
    }
    playstore_config = {
        "credentials": _decoded("PLAYSTORE_SA_JSON_B64"),
        "bucket": os.environ["PLAYSTORE_BUCKET"],
    }
    if args.ads_credentials_json:
        try:
            ads = json.loads(Path(args.ads_credentials_json).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BuildError(f"Ads credentials file is unreadable: {type(exc).__name__}") from exc
    else:
        ads = _decoded("ADS_CREDENTIALS_JSON_B64")
    table = build_rate_table([], fx_day)

    appstore = fetch_appstore_daily(
        appstore_config, dates, table, cache=DayCache(".appstore-cache")
    )
    playstore = fetch_playstore_daily(playstore_config, dates, table)
    google = fetch_google_ads_daily(ads["google"], dates, table)
    meta = fetch_meta_ads_daily(ads["meta"], dates, table)
    influencer_values = load_influencer_daily(
        args.influencer_daily_json, dates, args.influencer_zero
    )
    payload = build_payload(
        month, fx_day, appstore, playstore, influencer_values, google, meta
    )

    # Compact output keeps the environment value small. The root categories
    # make each array position explicit.
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n"
    encoded = base64.b64encode(raw.encode()).decode()
    decode_benchmark(encoded)  # final contract gate before anything is written
    Path(args.output).write_text(raw, encoding="utf-8")
    print(f"wrote {args.output} ({len(dates)} days, {len(encoded)} base64 characters)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
