"""Currency conversion at one date for the whole run.

ADR-0006: the rate date is always yesterday, for every conversion in the run —
current-month revenue and both sides of the Play net factor alike. A month-end
date for an in-progress month is in the future, every source 404s for it, and
each non-USD row silently vanishes, leaving a plausible figure built only from
the USD portion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Callable, Iterable, Optional

import requests

_CDN_URLS = (
    "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{day}/v1/currencies/{code}.min.json",
    "https://{day}.currency-api.pages.dev/v1/currencies/{code}.min.json",
)
_FRANKFURTER = "https://api.frankfurter.dev/v1/{day}"
_TIMEOUT = 20


def rate_date(today: date) -> date:
    """Yesterday — the most recent day a rate reliably exists for."""
    return today - timedelta(days=1)


@dataclass
class RateTable:
    """Rates for one day, seeded up front and extended on demand.

    A seed list cannot be complete: a single month of Play sales carries ~56
    currencies and the set moves with wherever the apps sold that month. So an
    unseeded code is looked up when it is first seen rather than treated as
    unknown — otherwise one Bolivian sale withdraws the entire source.
    """

    rates: dict
    day: date
    missing: set = field(default_factory=set)
    fetch: Optional[Callable[[str, date], Optional[Decimal]]] = None

    def to_usd(self, amount: Decimal, currency: str) -> Optional[Decimal]:
        code = (currency or "").strip().upper()
        if code == "USD":
            return amount
        if code == "":
            # Apple leaves the proceeds currency blank on free installs, and
            # those rows are always 0.00 — zero converts to zero from anywhere.
            # A blank code on a non-zero amount is a genuine unknown, not a
            # dollar, and guessing would book foreign money at par.
            return amount if amount == 0 else None
        rate = self.rates.get(code)
        if rate is None:
            rate = self._resolve(code)
        if rate is None:
            return None
        return amount * rate

    def _resolve(self, code: str) -> Optional[Decimal]:
        """Look ``code`` up once, remembering the answer either way.

        A code that has already failed is never retried: reports repeat a
        currency on thousands of rows, and one round trip per row would be paid
        for every one of them.
        """
        if code in self.missing or self.fetch is None:
            self.missing.add(code)
            return None
        rate = self.fetch(code, self.day)
        if rate is None:
            self.missing.add(code)
            return None
        self.rates[code] = rate
        return rate


def _cdn_rate(code: str, day: date) -> Optional[Decimal]:
    for template in _CDN_URLS:
        url = template.format(day=day.isoformat(), code=code.lower())
        try:
            response = requests.get(url, timeout=_TIMEOUT)
            if response.status_code != 200:
                continue
            value = response.json().get(code.lower(), {}).get("usd")
            if value is not None:
                return Decimal(str(value))
        except (requests.RequestException, ValueError):
            continue
    return None


def _frankfurter_rate(code: str, day: date) -> Optional[Decimal]:
    try:
        response = requests.get(
            _FRANKFURTER.format(day=day.isoformat()),
            params={"base": code, "symbols": "USD"},
            timeout=_TIMEOUT,
        )
        if response.status_code != 200:
            return None
        value = response.json().get("rates", {}).get("USD")
        return None if value is None else Decimal(str(value))
    except (requests.RequestException, ValueError):
        return None


def _live_fetch(code: str, day: date) -> Optional[Decimal]:
    return _cdn_rate(code, day) or _frankfurter_rate(code, day)


def _usd_base_rates(day: date) -> dict:
    """Every currency in one request, as ``code -> USD``.

    The per-code endpoint costs one round trip per currency, and a real month
    spans ~60 of them. That matters beyond speed: ``convert_all`` is all or
    nothing, so with sixty independent lookups a single transient blip on a
    single currency discards an entire revenue source. One request collapses
    sixty chances to fail into one.

    The file is quoted as USD to everything else, so each rate is inverted.
    """
    for template in _CDN_URLS:
        url = template.format(day=day.isoformat(), code="usd")
        try:
            response = requests.get(url, timeout=_TIMEOUT)
            if response.status_code != 200:
                continue
            quoted = response.json().get("usd", {})
            rates = {}
            for code, value in quoted.items():
                try:
                    rate = Decimal(str(value))
                except (ValueError, ArithmeticError):
                    continue
                if rate > 0:
                    rates[code.upper()] = Decimal(1) / rate
            if rates:
                return rates
        except (requests.RequestException, ValueError):
            continue
    return {}


def build_rate_table(
    codes: Iterable[str],
    day: date,
    fetch: Optional[Callable[[str, date], Optional[Decimal]]] = None,
    prime: Optional[Callable[[date], dict]] = None,
) -> RateTable:
    """A table for ``day``, primed in one request and extended on first sight.

    ``codes`` only pre-warms it, so an empty list is a valid argument. Priming
    is skipped when ``fetch`` is injected, keeping the tests offline.
    """
    injected = fetch is not None
    fetch = fetch or _live_fetch
    rates = {}
    if not injected or prime is not None:
        primer = prime or _usd_base_rates
        rates.update(primer(day))
        if not rates:
            # Priming is a single point of failure for every FX-bearing source:
            # with it empty, Frankfurter's ~30 currencies cannot cover the 44-58
            # a real month carries, so all-or-nothing conversion would refuse
            # every source at once and deliver a message that is only warnings.
            # Rates barely move overnight, and the day before is still in the
            # past, so ADR-0006's no-future-date rule holds.
            rates.update(primer(day - timedelta(days=1)))

    table = RateTable(rates, day, fetch=fetch)
    wanted = {c.strip().upper() for c in codes if c and c.strip().upper() != "USD"}
    for code in sorted(wanted - set(rates)):
        rate = fetch(code, day)
        if rate is None:
            table.missing.add(code)
        else:
            rates[code] = rate
    return table


def convert_all(totals: dict, table: RateTable) -> tuple:
    """``(usd_total, [])``, or ``(None, codes)`` naming every code that blocked it.

    All or nothing, deliberately. Skipping the codes that would not convert
    leaves a figure built from the convertible portion alone — smaller than the
    truth and completely plausible, which is the one failure mode a reader
    cannot detect. Refusing the whole source is loud; a quiet shortfall is not.
    """
    running = Decimal("0")
    blocked = []
    for code, amount in sorted(totals.items()):
        converted = table.to_usd(amount, code)
        if converted is None:
            blocked.append(code or "(blank)")
        else:
            running += converted
    return (None, blocked) if blocked else (running, [])
