"""App Store Connect month-to-date net proceeds.

One request per day. Reports publish next-day around 05:00 PT, so the window is
the 1st through yesterday; asking for today books a spurious zero on every run.
A 404 day is either a sale-less day or one Apple has not published — counted as
zero, never cached, never fatal.
"""

from __future__ import annotations

import csv
import gzip
import io
import time
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

import jwt
import requests

from pnl_fx import RateTable, convert_all
from pnl_money import Amount, SourceValue, Unavailable, to_decimal

_API = "https://api.appstoreconnect.apple.com/v1/salesReports"
_AUDIENCE = "appstoreconnect-v1"
_TOKEN_TTL = 900
_TIMEOUT = 60

_PROCEEDS = "Developer Proceeds"
_UNITS = "Units"
_CURRENCY = "Currency of Proceeds"


def window_days(today: date) -> list:
    """The 1st of ``today``'s month through yesterday. Empty on the 1st."""
    last = today - timedelta(days=1)
    if last.month != today.month or last.year != today.year:
        return []
    first = today.replace(day=1)
    span = (last - first).days + 1
    return [first + timedelta(days=i) for i in range(span)]


def make_token(config: dict, now: Optional[int] = None) -> str:
    now = int(time.time()) if now is None else now
    return jwt.encode(
        {"iss": config["issuer_id"], "iat": now, "exp": now + _TOKEN_TTL, "aud": _AUDIENCE},
        config["p8"],
        algorithm="ES256",
        headers={"kid": config["key_id"], "typ": "JWT"},
    )


def parse_sales_tsv(text: str) -> dict:
    """Sum ``Developer Proceeds`` x ``Units`` per currency.

    Proceeds are per unit, not per row. Units are signed — refunds are negative.
    An unrecognised layout yields ``{}`` rather than raising; the caller turns a
    whole window of nothing into a warning.
    """
    totals: dict = {}
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    for row in reader:
        if _PROCEEDS not in row or _UNITS not in row or _CURRENCY not in row:
            return {}
        try:
            proceeds = to_decimal(row[_PROCEEDS])
            units = int(to_decimal(row[_UNITS]))
        except (InvalidOperation, ValueError, TypeError):
            continue
        code = (row[_CURRENCY] or "").strip().upper()
        totals[code] = totals.get(code, Decimal("0")) + proceeds * units
    return totals


def _fetch_day(config: dict, day: date, attempts: int = 3) -> Optional[str]:
    """One day's report, retried on transient failure.

    A month is up to 31 sequential requests and the caller refuses the whole
    source if any one of them raises, so a single flaky response late in the
    loop would discard thirty good ones. The cache only helps the *next* run.
    """
    last = None
    for attempt in range(attempts):
        try:
            return _fetch_day_once(config, day)
        except requests.RequestException as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(2 ** attempt)
    raise last


def _fetch_day_once(config: dict, day: date) -> Optional[str]:
    response = requests.get(
        _API,
        params={
            "filter[frequency]": "DAILY",
            "filter[reportDate]": day.isoformat(),
            "filter[reportType]": "SALES",
            "filter[reportSubType]": "SUMMARY",
            "filter[vendorNumber]": config["vendor_number"],
            "filter[version]": "1_0",
        },
        headers={
            "Authorization": f"Bearer {make_token(config)}",
            "Accept": "application/a-gzip",
        },
        timeout=_TIMEOUT,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return gzip.decompress(response.content).decode("utf-8")


class DayCache:
    """Per-day parsed totals on disk. A published day is immutable.

    Only days that parsed to something are stored. A 404 is never cached —
    caching one freezes a gap Apple later fills.
    """

    def __init__(self, directory: str):
        import os

        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, day: date) -> str:
        import os

        return os.path.join(self.directory, f"{day.isoformat()}.json")

    def get(self, day: date) -> Optional[dict]:
        import json
        import os

        path = self._path(day)
        if not os.path.exists(path):
            return None
        try:
            with open(path, encoding="utf-8") as handle:
                return {k: Decimal(v) for k, v in json.load(handle).items()}
        except (ValueError, OSError, TypeError, ArithmeticError):
            # ArithmeticError covers decimal.InvalidOperation, which is NOT a
            # ValueError. Missing it would let one malformed entry escape into
            # the caller, and since restore-keys carries the cache forward the
            # bad entry would be sticky until someone purged it by hand.
            return None

    def put(self, day: date, totals: dict) -> None:
        import json

        if not totals:
            return
        try:
            with open(self._path(day), "w", encoding="utf-8") as handle:
                json.dump({k: str(v) for k, v in totals.items()}, handle)
        except OSError:
            pass  # a cache that cannot write is not a reason to fail the run


def fetch_appstore(
    config: dict,
    today: date,
    table: RateTable,
    fetch_day: Optional[Callable[[dict, date], Optional[str]]] = None,
    cache: Optional[DayCache] = None,
) -> SourceValue:
    fetch_day = fetch_day or _fetch_day
    days = window_days(today)
    if not days:
        return Unavailable("App Store: no published days in the window yet")

    # Accumulate in the reported currencies and convert once at the end, so a
    # code with no rate can be named rather than quietly left out of the sum.
    by_currency: dict = {}
    parsed_any = False
    try:
        for day in days:
            totals = cache.get(day) if cache else None
            if totals is None:
                text = fetch_day(config, day)
                if text is None:
                    continue  # 404: sale-less or not yet published. Never cached.
                totals = parse_sales_tsv(text)
                if cache:
                    cache.put(day, totals)
            if totals:
                parsed_any = True
            for code, amount in totals.items():
                by_currency[code] = by_currency.get(code, Decimal("0")) + amount
    except Exception as exc:
        return Unavailable(f"App Store request failed: {type(exc).__name__}")

    if not parsed_any:
        # A renamed column parses to zero rows with a 200. Never report that as $0.
        return Unavailable(
            f"App Store: no rows parsed across {len(days)} days — check the report layout"
        )

    total, blocked = convert_all(by_currency, table)
    if total is None:
        return Unavailable(f"App Store: no USD rate for {', '.join(blocked)}")
    return Amount(total)
