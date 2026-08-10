# Marketing Net Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **⚠️ SUPERSEDED — this plan was executed on 2026-08-10.** The shipped code lives in
> `scripts/pnl/` and has diverged from the blocks below. Running against live APIs found four
> defects, the largest being that the fixed `SEED_CURRENCIES` list silently dropped 33 of the
> 44 currencies Apple actually reports. **Read the code, not this plan.** The corrections are
> recorded in the spec under "Corrections found by running it live".

**Goal:** A daily GitHub Action that posts month-to-date app revenue minus month-to-date marketing spend to a Telegram group.

**Architecture:** Nine focused modules under `scripts/pnl/`, fetched by raw URL at run time (repo convention). Each data source is an independent function returning `Amount` or `Unavailable` — never a bare number that could be confused with zero. One orchestrator composes them, one renderer produces the message.

**Tech Stack:** Python 3.13, `requests`, `PyJWT` + `cryptography` (ES256 for App Store), `google-auth` (GCS). Google Ads and Meta are called over plain REST — no vendor SDK.

## Global Constraints

- Spec: [`docs/superpowers/specs/2026-08-10-marketing-net-design.md`](../specs/2026-08-10-marketing-net-design.md). ADRs [0005](../../adr/0005-marketing-net-recomputes-revenue.md), [0006](../../adr/0006-single-rate-date.md), [0007](../../adr/0007-play-net-factor.md). Vocabulary in `CONTEXT.md` under **Marketing Net**.
- **Tests use `unittest`**, matching `scripts/seo/` and `scripts/tars/`. Run with `python3 -m unittest discover -s scripts/pnl -p 'test_*.py'`. No network in any test.
- **Money is `Decimal`, never `float`.** Parse strings with `Decimal(str(x))`.
- **`Unavailable` is never coerced to `0`.** This is the single most important rule in the codebase.
- **One FX rate date per run: yesterday.** Never a month-end (ADR-0006).
- API versions pinned: **Google Ads v25**, **Meta Graph v26.0**.
- Modules import each other flatly (`from pnl_money import Amount`) — they are siblings in one directory, not a package. Follow `scripts/seo/`.
- Secrets arrive as environment variables. The chat id is read **only** from `MARKETING_NET_CHAT_ID`, never `TELEGRAM_CHAT_ID` (which exists in the same Infisical project and belongs to the PNL app).

---

### Task 1: Money primitives and the source-value type

**Files:**
- Create: `scripts/pnl/pnl_money.py`
- Test: `scripts/pnl/test_pnl_money.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `Amount(usd: Decimal)`, `Unavailable(reason: str)`, `SourceValue = Amount | Unavailable`, `to_decimal(raw) -> Decimal`, `round_usd(Decimal) -> Decimal`, `format_usd(Decimal) -> str`, `combine(list[SourceValue], reason: str) -> SourceValue`. Every later task uses these.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_money.py
import unittest
from decimal import Decimal

from pnl_money import (
    Amount,
    Unavailable,
    combine,
    format_usd,
    round_usd,
    to_decimal,
)


class ToDecimalTest(unittest.TestCase):
    def test_parses_string_without_float(self):
        self.assertEqual(to_decimal("350.5000"), Decimal("350.5000"))

    def test_parses_int(self):
        self.assertEqual(to_decimal(7), Decimal("7"))

    def test_float_does_not_leak_binary_error(self):
        # 0.1 as a float is 0.1000000000000000055511151231257827
        self.assertEqual(to_decimal(0.1), Decimal("0.1"))


class RoundUsdTest(unittest.TestCase):
    def test_rounds_half_up(self):
        self.assertEqual(round_usd(Decimal("0.5")), Decimal("1"))

    def test_rounds_to_whole_dollars(self):
        self.assertEqual(round_usd(Decimal("12429.62")), Decimal("12430"))


class FormatUsdTest(unittest.TestCase):
    def test_thousands_separator(self):
        self.assertEqual(format_usd(Decimal("12430")), "$12,430")

    def test_negative(self):
        self.assertEqual(format_usd(Decimal("-1200")), "-$1,200")

    def test_small_negative_never_renders_negative_zero(self):
        # The trap: -0.4 formats as "-0" under naive formatting.
        self.assertEqual(format_usd(Decimal("-0.4")), "$0")

    def test_small_positive_is_plain_zero(self):
        self.assertEqual(format_usd(Decimal("0.4")), "$0")


class CombineTest(unittest.TestCase):
    def test_sums_available(self):
        result = combine([Amount(Decimal("10")), Amount(Decimal("5"))], "none")
        self.assertEqual(result, Amount(Decimal("15")))

    def test_ignores_unavailable_but_keeps_the_rest(self):
        result = combine([Amount(Decimal("10")), Unavailable("meta down")], "none")
        self.assertEqual(result, Amount(Decimal("10")))

    def test_all_unavailable_yields_unavailable_not_zero(self):
        result = combine([Unavailable("a"), Unavailable("b")], "nothing readable")
        self.assertEqual(result, Unavailable("nothing readable"))

    def test_empty_yields_unavailable_not_zero(self):
        self.assertEqual(combine([], "nothing readable"), Unavailable("nothing readable"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_money -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_money'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_money.py
"""Money primitives and the two-state source value.

Every figure in this pipeline is a Decimal. A source either produced an amount
or it did not; there is no third state, and an ``Unavailable`` is never turned
into ``0`` — a plausible zero is indistinguishable from a genuinely quiet month,
which is the failure this whole pipeline is built to avoid.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Union


@dataclass(frozen=True)
class Amount:
    usd: Decimal


@dataclass(frozen=True)
class Unavailable:
    reason: str


SourceValue = Union[Amount, Unavailable]

_WHOLE = Decimal("1")


def to_decimal(raw) -> Decimal:
    """Decimal from anything, via str so a float's binary error never lands."""
    return Decimal(str(raw))


def round_usd(value: Decimal) -> Decimal:
    return value.quantize(_WHOLE, rounding=ROUND_HALF_UP)


def format_usd(value: Decimal) -> str:
    """Render whole dollars. A value that rounds to zero renders ``$0``.

    Deciding the sign from the unrounded value is what produces ``-$0`` for a
    small negative; the rounded value is the one the reader sees, so it is the
    one the sign must agree with.
    """
    rounded = round_usd(value)
    if rounded == 0:
        return "$0"
    if rounded < 0:
        return f"-${-rounded:,}"
    return f"${rounded:,}"


def combine(values: Iterable[SourceValue], reason: str) -> SourceValue:
    """Sum the available values; ``Unavailable(reason)`` when none are."""
    amounts = [v.usd for v in values if isinstance(v, Amount)]
    if not amounts:
        return Unavailable(reason)
    return Amount(sum(amounts, Decimal("0")))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_money -v`
Expected: PASS (14 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_money.py scripts/pnl/test_pnl_money.py
git commit -m "feat(marketing-net): money primitives and the two-state source value"
```

---

### Task 2: FX rate table

**Files:**
- Create: `scripts/pnl/pnl_fx.py`
- Test: `scripts/pnl/test_pnl_fx.py`

**Interfaces:**
- Consumes: `to_decimal` from `pnl_money`.
- Produces: `rate_date(today: date) -> date`, `RateTable` with `.to_usd(amount: Decimal, currency: str) -> Decimal | None` and `.missing: set[str]`, and `build_rate_table(codes: Iterable[str], day: date, fetch=None) -> RateTable`. `fetch` is `Callable[[str, date], Decimal | None]` and exists so tests never touch the network.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_fx.py
import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable, build_rate_table, rate_date


class RateDateTest(unittest.TestCase):
    def test_is_yesterday_never_today(self):
        self.assertEqual(rate_date(date(2026, 8, 10)), date(2026, 8, 9))

    def test_crosses_month_boundary(self):
        self.assertEqual(rate_date(date(2026, 8, 1)), date(2026, 7, 31))


class RateTableTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({"INR": Decimal("0.012")}, date(2026, 8, 9))

    def test_usd_passes_through_untouched(self):
        self.assertEqual(self.table.to_usd(Decimal("100"), "USD"), Decimal("100"))

    def test_blank_currency_treated_as_usd(self):
        self.assertEqual(self.table.to_usd(Decimal("100"), ""), Decimal("100"))

    def test_converts_known_currency(self):
        self.assertEqual(self.table.to_usd(Decimal("100"), "inr"), Decimal("1.200"))

    def test_unknown_currency_returns_none_and_is_recorded(self):
        self.assertIsNone(self.table.to_usd(Decimal("100"), "XYZ"))
        self.assertIn("XYZ", self.table.missing)


class BuildRateTableTest(unittest.TestCase):
    def test_fetches_each_non_usd_code_once(self):
        calls = []

        def fake_fetch(code, day):
            calls.append(code)
            return Decimal("0.5")

        table = build_rate_table(["USD", "INR", "INR", "EUR"], date(2026, 8, 9), fetch=fake_fetch)
        self.assertEqual(sorted(calls), ["EUR", "INR"])
        self.assertEqual(table.to_usd(Decimal("2"), "EUR"), Decimal("1.0"))

    def test_failed_lookup_is_missing_not_zero(self):
        table = build_rate_table(["INR"], date(2026, 8, 9), fetch=lambda code, day: None)
        self.assertIsNone(table.to_usd(Decimal("100"), "INR"))
        self.assertIn("INR", table.missing)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_fx -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_fx'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_fx.py
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
    rates: dict
    day: date
    missing: set = field(default_factory=set)

    def to_usd(self, amount: Decimal, currency: str) -> Optional[Decimal]:
        code = (currency or "").strip().upper()
        if code in ("", "USD"):
            return amount
        rate = self.rates.get(code)
        if rate is None:
            self.missing.add(code)
            return None
        return amount * rate


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


def build_rate_table(
    codes: Iterable[str],
    day: date,
    fetch: Optional[Callable[[str, date], Optional[Decimal]]] = None,
) -> RateTable:
    fetch = fetch or _live_fetch
    wanted = {c.strip().upper() for c in codes if c and c.strip().upper() != "USD"}
    rates = {}
    table = RateTable(rates, day)
    for code in sorted(wanted):
        rate = fetch(code, day)
        if rate is None:
            table.missing.add(code)
        else:
            rates[code] = rate
    return table
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_fx -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_fx.py scripts/pnl/test_pnl_fx.py
git commit -m "feat(marketing-net): FX rate table pinned to a single date"
```

---

### Task 3: Influencer spend from the PNL endpoint

**Files:**
- Create: `scripts/pnl/pnl_spend.py`
- Test: `scripts/pnl/test_pnl_spend.py`

**Interfaces:**
- Consumes: `Amount`, `Unavailable`, `SourceValue`, `to_decimal` from `pnl_money`.
- Produces: `fetch_head_spend(base_url: str, api_key: str, head: str, get=None) -> SourceValue`. `get` is `Callable[[str, dict, dict], Response]` matching `requests.get(url, params=..., headers=...)`.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_spend.py
import unittest
from decimal import Decimal

from pnl_money import Amount, Unavailable
from pnl_spend import fetch_head_spend


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def responder(response):
    def get(url, params=None, headers=None, timeout=None):
        get.seen = {"url": url, "params": params, "headers": headers}
        return response

    return get


class FetchHeadSpendTest(unittest.TestCase):
    def test_parses_four_decimal_string_as_decimal(self):
        get = responder(FakeResponse(200, {"spend_usd": "350.5000"}))
        self.assertEqual(
            fetch_head_spend("https://pnl.example", "k", "INFLUENCER_MARKETING", get=get),
            Amount(Decimal("350.5000")),
        )

    def test_empty_month_is_a_real_zero(self):
        get = responder(FakeResponse(200, {"spend_usd": "0.0000"}))
        self.assertEqual(
            fetch_head_spend("https://pnl.example", "k", "H", get=get),
            Amount(Decimal("0.0000")),
        )

    def test_sends_the_key_header_and_head_param(self):
        get = responder(FakeResponse(200, {"spend_usd": "1.0000"}))
        fetch_head_spend("https://pnl.example", "secret", "INFLUENCER_MARKETING", get=get)
        self.assertEqual(get.seen["headers"], {"X-Api-Key": "secret"})
        self.assertEqual(get.seen["params"], {"head": "INFLUENCER_MARKETING"})

    def test_404_is_unavailable_not_zero(self):
        # A 404 means the configured head key is wrong, not that the month is quiet.
        get = responder(FakeResponse(404))
        result = fetch_head_spend("https://pnl.example", "k", "TYPOD", get=get)
        self.assertIsInstance(result, Unavailable)
        self.assertIn("TYPOD", result.reason)

    def test_401_is_unavailable(self):
        get = responder(FakeResponse(401))
        result = fetch_head_spend("https://pnl.example", "k", "H", get=get)
        self.assertIsInstance(result, Unavailable)
        self.assertIn("401", result.reason)

    def test_network_error_is_unavailable(self):
        def get(url, params=None, headers=None, timeout=None):
            raise OSError("connection reset")

        result = fetch_head_spend("https://pnl.example", "k", "H", get=get)
        self.assertIsInstance(result, Unavailable)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_spend -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_spend'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_spend.py
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
    except (KeyError, ValueError, TypeError) as exc:
        return Unavailable(f"PNL spend payload unreadable: {type(exc).__name__}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_spend -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_spend.py scripts/pnl/test_pnl_spend.py
git commit -m "feat(marketing-net): PNL head-spend client"
```

---

### Task 4: App Store revenue

**Files:**
- Create: `scripts/pnl/pnl_appstore.py`
- Test: `scripts/pnl/test_pnl_appstore.py`

**Interfaces:**
- Consumes: `Amount`, `Unavailable`, `SourceValue`, `to_decimal` from `pnl_money`; `RateTable` from `pnl_fx`.
- Produces: `window_days(today: date) -> list[date]`, `parse_sales_tsv(text: str) -> dict[str, Decimal]`, `DayCache(directory)` with `.get(day) -> dict | None` and `.put(day, totals)`, `fetch_appstore(config: dict, today: date, table: RateTable, fetch_day=None, cache=None) -> SourceValue`. `config` keys: `issuer_id`, `key_id`, `p8`, `vendor_number`. `fetch_day` is `Callable[[dict, date], str | None]` returning decompressed TSV text, or `None` for a 404.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_appstore.py
import unittest
from datetime import date
from decimal import Decimal

from pnl_appstore import fetch_appstore, parse_sales_tsv, window_days
from pnl_fx import RateTable
from pnl_money import Amount, Unavailable

HEADER = "Provider\tDeveloper Proceeds\tUnits\tCurrency of Proceeds"


def tsv(*rows):
    return "\n".join([HEADER, *rows]) + "\n"


class WindowDaysTest(unittest.TestCase):
    def test_first_of_month_to_yesterday(self):
        days = window_days(date(2026, 8, 10))
        self.assertEqual(days[0], date(2026, 8, 1))
        self.assertEqual(days[-1], date(2026, 8, 9))
        self.assertEqual(len(days), 9)

    def test_on_the_first_the_window_is_empty(self):
        # Yesterday belongs to last month, so there is nothing to ask for.
        self.assertEqual(window_days(date(2026, 8, 1)), [])


class ParseSalesTsvTest(unittest.TestCase):
    def test_multiplies_proceeds_by_units(self):
        totals = parse_sales_tsv(tsv("APPLE\t1.50\t10\tUSD"))
        self.assertEqual(totals, {"USD": Decimal("15.00")})

    def test_negative_units_are_refunds(self):
        totals = parse_sales_tsv(tsv("APPLE\t1.50\t10\tUSD", "APPLE\t1.50\t-4\tUSD"))
        self.assertEqual(totals, {"USD": Decimal("9.00")})

    def test_groups_by_currency(self):
        totals = parse_sales_tsv(tsv("APPLE\t1.50\t10\tUSD", "APPLE\t100\t2\tINR"))
        self.assertEqual(totals, {"USD": Decimal("15.00"), "INR": Decimal("200")})

    def test_renamed_columns_yield_nothing(self):
        # Guarded by the caller, which warns rather than reporting $0.
        renamed = "Provider\tProceeds\tQty\tCurrency\nAPPLE\t1.50\t10\tUSD\n"
        self.assertEqual(parse_sales_tsv(renamed), {})


class FetchAppStoreTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({"INR": Decimal("0.01")}, date(2026, 8, 9))
        self.config = {"issuer_id": "i", "key_id": "k", "p8": "p", "vendor_number": "v"}

    def test_sums_days_and_converts(self):
        def fetch_day(config, day):
            return tsv("APPLE\t1.00\t1\tUSD", "APPLE\t100\t1\tINR")

        result = fetch_appstore(self.config, date(2026, 8, 3), self.table, fetch_day=fetch_day)
        # Two days (1st, 2nd) x (1 USD + 100 INR@0.01 = 2.00)
        self.assertEqual(result, Amount(Decimal("4.00")))

    def test_404_day_counts_as_zero_and_does_not_abort(self):
        def fetch_day(config, day):
            return None if day.day == 1 else tsv("APPLE\t1.00\t1\tUSD")

        result = fetch_appstore(self.config, date(2026, 8, 3), self.table, fetch_day=fetch_day)
        self.assertEqual(result, Amount(Decimal("1.00")))

    def test_empty_window_is_unavailable_not_zero(self):
        result = fetch_appstore(self.config, date(2026, 8, 1), self.table, fetch_day=lambda c, d: None)
        self.assertIsInstance(result, Unavailable)
        self.assertIn("no published days", result.reason)

    def test_every_day_parsing_to_nothing_warns_instead_of_reporting_zero(self):
        def fetch_day(config, day):
            return "Provider\tProceeds\tQty\tCurrency\nAPPLE\t1\t1\tUSD\n"

        result = fetch_appstore(self.config, date(2026, 8, 10), self.table, fetch_day=fetch_day)
        self.assertIsInstance(result, Unavailable)
        self.assertIn("layout", result.reason)

    def test_hard_error_is_unavailable(self):
        def fetch_day(config, day):
            raise OSError("tls handshake failed")

        result = fetch_appstore(self.config, date(2026, 8, 10), self.table, fetch_day=fetch_day)
        self.assertIsInstance(result, Unavailable)


class DayCacheTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({}, date(2026, 8, 9))
        self.config = {"issuer_id": "i", "key_id": "k", "p8": "p", "vendor_number": "v"}

    def test_cached_day_is_not_refetched(self):
        import tempfile

        from pnl_appstore import DayCache

        with tempfile.TemporaryDirectory() as directory:
            cache = DayCache(directory)
            asked = []

            def fetch_day(config, day):
                asked.append(day)
                return tsv("APPLE\t1.00\t1\tUSD")

            first = fetch_appstore(self.config, date(2026, 8, 3), self.table,
                                   fetch_day=fetch_day, cache=cache)
            second = fetch_appstore(self.config, date(2026, 8, 3), self.table,
                                    fetch_day=fetch_day, cache=cache)
            self.assertEqual(first, second)
            self.assertEqual(len(asked), 2)  # two days on the first pass, none on the second

    def test_404_day_is_never_cached(self):
        import tempfile

        from pnl_appstore import DayCache

        with tempfile.TemporaryDirectory() as directory:
            cache = DayCache(directory)
            calls = []

            def fetch_day(config, day):
                calls.append(day)
                return None

            fetch_appstore(self.config, date(2026, 8, 3), self.table,
                           fetch_day=fetch_day, cache=cache)
            fetch_appstore(self.config, date(2026, 8, 3), self.table,
                           fetch_day=fetch_day, cache=cache)
            # Caching a 404 would freeze a gap Apple later fills.
            self.assertEqual(len(calls), 4)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_appstore -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_appstore'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_appstore.py
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

from pnl_fx import RateTable
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


def _fetch_day(config: dict, day: date) -> Optional[str]:
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
        except (ValueError, OSError):
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

    total = Decimal("0")
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
                converted = table.to_usd(amount, code)
                if converted is not None:
                    total += converted
    except Exception as exc:
        return Unavailable(f"App Store request failed: {type(exc).__name__}")

    if not parsed_any:
        # A renamed column parses to zero rows with a 200. Never report that as $0.
        return Unavailable(
            f"App Store: no rows parsed across {len(days)} days — check the report layout"
        )
    return Amount(total)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_appstore -v`
Expected: PASS (13 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_appstore.py scripts/pnl/test_pnl_appstore.py
git commit -m "feat(marketing-net): App Store month-to-date proceeds"
```

---

### Task 5: Play Store revenue and the net factor

**Files:**
- Create: `scripts/pnl/pnl_playstore.py`
- Test: `scripts/pnl/test_pnl_playstore.py`

**Interfaces:**
- Consumes: `Amount`, `Unavailable`, `SourceValue`, `to_decimal` from `pnl_money`; `RateTable` from `pnl_fx`.
- Produces: `month_from_earnings_name(name: str) -> str | None`, `sum_sales_csv(text: str) -> dict[str, Decimal]`, `sum_earnings_csv(text: str) -> dict[str, Decimal]`, `derive_net_factor(sales_by_month, earnings_by_month, table) -> Decimal | None`, `fetch_playstore(config, today, table, storage=None) -> SourceValue`. `storage` is an object with `.list(prefix) -> list[str]` and `.read_zip_csv(name) -> str`.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_playstore.py
import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable
from pnl_money import Amount, Unavailable
from pnl_playstore import (
    derive_net_factor,
    fetch_playstore,
    month_from_earnings_name,
    sum_earnings_csv,
    sum_sales_csv,
)

SALES_HEADER = "Order Charged Date,Charged Amount,Currency of Sale,Financial Status"
EARNINGS_HEADER = "Transaction Date,Amount (Merchant Currency),Merchant Currency,Transaction Type"


class MonthFromNameTest(unittest.TestCase):
    def test_extracts_month_not_the_account_number(self):
        name = "earnings/earnings_202607_1234567890123456789-1.zip"
        self.assertEqual(month_from_earnings_name(name), "202607")

    def test_naive_last_segment_would_have_been_wrong(self):
        name = "earnings/earnings_202607_9876543210-3.zip"
        self.assertNotEqual(month_from_earnings_name(name), "987654")
        self.assertEqual(month_from_earnings_name(name), "202607")

    def test_returns_none_when_absent(self):
        self.assertIsNone(month_from_earnings_name("earnings/summary.zip"))


class SumCsvTest(unittest.TestCase):
    def test_sales_sums_by_currency_including_negative_refunds(self):
        text = "\n".join([
            SALES_HEADER,
            "2026-08-01,10.00,USD,Charged",
            "2026-08-02,-4.00,USD,Refund",
            "2026-08-02,500,INR,Charged",
        ])
        self.assertEqual(sum_sales_csv(text), {"USD": Decimal("6.00"), "INR": Decimal("500")})

    def test_earnings_sums_by_merchant_currency(self):
        # Transaction Date is free text containing a comma ("Jul 1, 2026"), so
        # real reports quote it. An unquoted fixture shifts every column and the
        # year gets read as the amount.
        text = "\n".join([
            EARNINGS_HEADER,
            '"Jul 1, 2026",7.00,USD,Charge',
            '"Jul 2, 2026",-1.00,USD,Google fee',
        ])
        self.assertEqual(sum_earnings_csv(text), {"USD": Decimal("6.00")})


class DeriveNetFactorTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({}, date(2026, 8, 9))

    def test_latest_overlapping_month_wins(self):
        sales = {"202606": {"USD": Decimal("100")}, "202607": {"USD": Decimal("200")}}
        earnings = {"202606": {"USD": Decimal("50")}, "202607": {"USD": Decimal("140")}}
        self.assertEqual(derive_net_factor(sales, earnings, self.table), Decimal("0.7"))

    def test_no_overlap_yields_none(self):
        sales = {"202607": {"USD": Decimal("200")}}
        earnings = {"202601": {"USD": Decimal("50")}}
        self.assertIsNone(derive_net_factor(sales, earnings, self.table))

    def test_zero_earnings_yields_none_not_a_zero_factor(self):
        # A factor of 0 survives an `is None` check and reports Play as exactly nothing.
        sales = {"202607": {"USD": Decimal("200")}}
        earnings = {"202607": {"USD": Decimal("0")}}
        self.assertIsNone(derive_net_factor(sales, earnings, self.table))

    def test_zero_sales_yields_none(self):
        sales = {"202607": {"USD": Decimal("0")}}
        earnings = {"202607": {"USD": Decimal("50")}}
        self.assertIsNone(derive_net_factor(sales, earnings, self.table))


class FakeStorage:
    def __init__(self, files):
        self.files = files

    def list(self, prefix):
        return [n for n in self.files if n.startswith(prefix)]

    def read_zip_csv(self, name):
        return self.files[name]


class FetchPlayStoreTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({}, date(2026, 8, 9))
        self.config = {"bucket": "b"}

    def _files(self):
        return {
            "sales/salesreport_202608.zip": "\n".join([SALES_HEADER, "2026-08-01,300,USD,Charged"]),
            "sales/salesreport_202607.zip": "\n".join([SALES_HEADER, "2026-07-01,200,USD,Charged"]),
            "earnings/earnings_202607_555-1.zip": "\n".join(
                [EARNINGS_HEADER, '"Jul 1, 2026",140,USD,Charge']
            ),
        }

    def test_applies_factor_to_current_month(self):
        storage = FakeStorage(self._files())
        result = fetch_playstore(self.config, date(2026, 8, 10), self.table, storage=storage)
        # 300 gross x (140/200 = 0.7) = 210
        self.assertEqual(result, Amount(Decimal("210.0")))

    def test_no_factor_excludes_play_and_says_so(self):
        files = self._files()
        del files["earnings/earnings_202607_555-1.zip"]
        result = fetch_playstore(self.config, date(2026, 8, 10), self.table, storage=FakeStorage(files))
        self.assertIsInstance(result, Unavailable)
        self.assertIn("factor", result.reason)

    def test_missing_sales_report_is_unavailable(self):
        files = self._files()
        del files["sales/salesreport_202608.zip"]
        result = fetch_playstore(self.config, date(2026, 8, 10), self.table, storage=FakeStorage(files))
        self.assertIsInstance(result, Unavailable)

    def test_report_present_but_no_rows_yet_is_a_real_zero(self):
        files = self._files()
        files["sales/salesreport_202608.zip"] = SALES_HEADER
        result = fetch_playstore(self.config, date(2026, 8, 10), self.table, storage=FakeStorage(files))
        self.assertEqual(result, Amount(Decimal("0")))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_playstore -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_playstore'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_playstore.py
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

from pnl_fx import RateTable
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
    running = Decimal("0")
    for code, amount in totals.items():
        converted = table.to_usd(amount, code)
        if converted is None:
            return None
        running += converted
    return running


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

        gross = _to_usd_total(_read_totals(storage, sales_names[current], sum_sales_csv), table)
        if gross is None:
            return Unavailable("Play Store: a sale currency could not be converted")
        return Amount(gross * factor)
    except Exception as exc:
        return Unavailable(f"Play Store access failed: {type(exc).__name__}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_playstore -v`
Expected: PASS (13 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_playstore.py scripts/pnl/test_pnl_playstore.py
git commit -m "feat(marketing-net): Play Store calibrated month-to-date revenue"
```

---

### Task 6: Google Ads spend

**Files:**
- Create: `scripts/pnl/pnl_googleads.py`
- Test: `scripts/pnl/test_pnl_googleads.py`

**Interfaces:**
- Consumes: `Amount`, `Unavailable`, `SourceValue`, `to_decimal` from `pnl_money`; `RateTable` from `pnl_fx`.
- Produces: `fetch_google_ads(creds: dict, today: date, table: RateTable, search=None) -> SourceValue`. `search` is `Callable[[str, str, str], list[dict]]` taking `(access_token, customer_id, query)` and returning flattened result rows. `creds` keys: `client_id`, `client_secret`, `refresh_token`, `dev_token`, `login_customer_id`, `skip_customer_ids`.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_googleads.py
import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable
from pnl_googleads import fetch_google_ads
from pnl_money import Amount, Unavailable

CREDS = {
    "client_id": "c",
    "client_secret": "s",
    "refresh_token": "r",
    "dev_token": "d",
    "login_customer_id": "6426942742",
    "skip_customer_ids": ["6426942742", "6096341923"],
}


def searcher(children, costs, seen=None):
    def search(token, customer_id, query):
        if "customer_client" in query:
            seen is None or seen.append(customer_id)
            return children
        return costs.get(customer_id, [])

    return search


class FetchGoogleAdsTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({"INR": Decimal("0.01")}, date(2026, 8, 9))

    def test_sums_children_and_converts(self):
        children = [
            {"customerClient": {"id": "111", "currencyCode": "USD"}},
            {"customerClient": {"id": "222", "currencyCode": "INR"}},
        ]
        costs = {
            "111": [{"metrics": {"costMicros": "5000000"}}],   # $5
            "222": [{"metrics": {"costMicros": "100000000"}}],  # 100 INR -> $1
        }
        result = fetch_google_ads(CREDS, date(2026, 8, 10), self.table,
                                  search=searcher(children, costs))
        self.assertEqual(result, Amount(Decimal("6")))

    def test_skip_list_is_applied_explicitly(self):
        children = [
            {"customerClient": {"id": "111", "currencyCode": "USD"}},
            {"customerClient": {"id": "6096341923", "currencyCode": "USD"}},
        ]
        costs = {
            "111": [{"metrics": {"costMicros": "5000000"}}],
            "6096341923": [{"metrics": {"costMicros": "999000000"}}],
        }
        result = fetch_google_ads(CREDS, date(2026, 8, 10), self.table,
                                  search=searcher(children, costs))
        self.assertEqual(result, Amount(Decimal("5")))

    def test_no_children_is_unavailable_not_zero(self):
        result = fetch_google_ads(CREDS, date(2026, 8, 10), self.table,
                                  search=searcher([], {}))
        self.assertIsInstance(result, Unavailable)

    def test_api_error_is_unavailable(self):
        def search(token, customer_id, query):
            raise OSError("503 backend error")

        result = fetch_google_ads(CREDS, date(2026, 8, 10), self.table, search=search)
        self.assertIsInstance(result, Unavailable)

    def test_child_with_no_spend_rows_contributes_nothing_but_does_not_fail(self):
        children = [{"customerClient": {"id": "111", "currencyCode": "USD"}}]
        result = fetch_google_ads(CREDS, date(2026, 8, 10), self.table,
                                  search=searcher(children, {"111": []}))
        self.assertEqual(result, Amount(Decimal("0")))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_googleads -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_googleads'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_googleads.py
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

from pnl_fx import RateTable
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
        total = Decimal("0")
        for customer_id, currency in accounts:
            for row in search(token, customer_id, query):
                micros = to_decimal(row.get("metrics", {}).get("costMicros", 0))
                code = row.get("customer", {}).get("currencyCode") or currency
                converted = table.to_usd(micros / _MICROS, code)
                if converted is not None:
                    total += converted
        return Amount(total)
    except Exception as exc:
        return Unavailable(f"Google Ads request failed: {type(exc).__name__}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_googleads -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_googleads.py scripts/pnl/test_pnl_googleads.py
git commit -m "feat(marketing-net): Google Ads month-to-date spend"
```

---

### Task 7: Meta Ads spend

**Files:**
- Create: `scripts/pnl/pnl_metaads.py`
- Test: `scripts/pnl/test_pnl_metaads.py`

**Interfaces:**
- Consumes: `Amount`, `Unavailable`, `SourceValue`, `to_decimal` from `pnl_money`; `RateTable` from `pnl_fx`.
- Produces: `fetch_meta_ads(creds: dict, today: date, table: RateTable, insights=None) -> SourceValue`. `insights` is `Callable[[str, str, date, date], list[dict]]` taking `(token, account_id, start, end)`. `creds` keys: `token`, `account_ids`.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_metaads.py
import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable
from pnl_metaads import fetch_meta_ads
from pnl_money import Amount, Unavailable


class FetchMetaAdsTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({"INR": Decimal("0.01")}, date(2026, 8, 9))

    def test_sums_accounts_and_converts(self):
        def insights(token, account_id, start, end):
            return {
                "111": [{"spend": "25.50", "account_currency": "USD"}],
                "222": [{"spend": "1000", "account_currency": "INR"}],
            }[account_id]

        creds = {"token": "t", "account_ids": ["111", "222"]}
        result = fetch_meta_ads(creds, date(2026, 8, 10), self.table, insights=insights)
        self.assertEqual(result, Amount(Decimal("35.50")))

    def test_strips_act_prefix(self):
        seen = []

        def insights(token, account_id, start, end):
            seen.append(account_id)
            return []

        creds = {"token": "t", "account_ids": ["act_2115160355488257"]}
        fetch_meta_ads(creds, date(2026, 8, 10), self.table, insights=insights)
        self.assertEqual(seen, ["2115160355488257"])

    def test_empty_insights_is_a_real_zero(self):
        creds = {"token": "t", "account_ids": ["111"]}
        result = fetch_meta_ads(creds, date(2026, 8, 10), self.table,
                                insights=lambda *a: [])
        self.assertEqual(result, Amount(Decimal("0")))

    def test_no_accounts_configured_is_unavailable(self):
        result = fetch_meta_ads({"token": "t", "account_ids": []}, date(2026, 8, 10),
                                self.table, insights=lambda *a: [])
        self.assertIsInstance(result, Unavailable)

    def test_expired_token_is_unavailable_not_zero(self):
        def insights(token, account_id, start, end):
            raise OSError("190 access token expired")

        creds = {"token": "t", "account_ids": ["111"]}
        result = fetch_meta_ads(creds, date(2026, 8, 10), self.table, insights=insights)
        self.assertIsInstance(result, Unavailable)

    def test_one_bad_account_does_not_kill_the_others(self):
        def insights(token, account_id, start, end):
            if account_id == "222":
                raise OSError("permission denied")
            return [{"spend": "10", "account_currency": "USD"}]

        creds = {"token": "t", "account_ids": ["111", "222"]}
        result = fetch_meta_ads(creds, date(2026, 8, 10), self.table, insights=insights)
        self.assertEqual(result, Amount(Decimal("10")))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_metaads -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_metaads'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_metaads.py
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

from pnl_fx import RateTable
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
    total = Decimal("0")
    failures = []
    for account_id in accounts:
        try:
            for row in insights(creds["token"], account_id, start, end):
                spend = to_decimal(row.get("spend", "0"))
                converted = table.to_usd(spend, row.get("account_currency", "USD"))
                if converted is not None:
                    total += converted
        except Exception as exc:
            failures.append(f"{account_id} ({type(exc).__name__})")

    if len(failures) == len(accounts):
        return Unavailable(f"Meta Ads: every account failed — {', '.join(failures)}")
    return Amount(total)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_metaads -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_metaads.py scripts/pnl/test_pnl_metaads.py
git commit -m "feat(marketing-net): Meta Ads month-to-date spend"
```

---

### Task 8: Message rendering and delivery

**Files:**
- Create: `scripts/pnl/pnl_telegram.py`
- Test: `scripts/pnl/test_pnl_telegram.py`

**Interfaces:**
- Consumes: `Amount`, `Unavailable`, `SourceValue`, `format_usd`, `round_usd`, `combine` from `pnl_money`.
- Produces: `escape(text: str) -> str`, `truncate(text: str, limit: int = 4096) -> str`, `redact(text: str, token: str) -> str`, `render(report: dict) -> str`, `send(token: str, chat_id: str, html: str, post=None, sleep=None) -> None` (raises `DeliveryError` after retries). `report` keys: `month_label`, `revenue: dict[str, SourceValue]`, `spend: dict[str, SourceValue]`, `appstore_window_label: str | None`, `warnings: list[str]`.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_pnl_telegram.py
import unittest
from decimal import Decimal

from pnl_money import Amount, Unavailable
from pnl_telegram import DeliveryError, escape, redact, render, send, truncate


class EscapeTest(unittest.TestCase):
    def test_escapes_the_three_html_entities(self):
        raw = "<urllib3.HTTPSConnection object> a&b"
        self.assertEqual(escape(raw), "&lt;urllib3.HTTPSConnection object&gt; a&amp;b")

    def test_ampersand_escaped_first_so_entities_are_not_double_escaped(self):
        self.assertEqual(escape("<a>"), "&lt;a&gt;")


class TruncateTest(unittest.TestCase):
    def test_leaves_short_text(self):
        self.assertEqual(truncate("abc", limit=10), "abc")

    def test_cuts_on_a_line_boundary_never_mid_entity(self):
        text = "line one\n" + "&amp;" * 100
        result = truncate(text, limit=20)
        self.assertTrue(result.startswith("line one"))
        self.assertNotIn("&am\n", result)
        self.assertLessEqual(len(result), 20)


class RedactTest(unittest.TestCase):
    def test_removes_the_bot_token(self):
        message = "failed https://api.telegram.org/bot123:ABC/sendMessage"
        self.assertNotIn("123:ABC", redact(message, "123:ABC"))


class RenderTest(unittest.TestCase):
    def base(self):
        return {
            "month_label": "Aug 1–10",
            "revenue": {"App Store": Amount(Decimal("12430")), "Play Store": Amount(Decimal("8110"))},
            "spend": {
                "Influencer": Amount(Decimal("350")),
                "Google Ads": Amount(Decimal("4220")),
                "Meta Ads": Amount(Decimal("3015")),
            },
            "appstore_window_label": "to Aug 9",
            "warnings": [],
        }

    def test_columns_subtract_as_displayed(self):
        html = render(self.base())
        self.assertIn("$20,540", html)
        self.assertIn("$7,585", html)
        self.assertIn("$12,955", html)

    def test_uses_pre_for_monospace_columns(self):
        self.assertIn("<pre>", render(self.base()))

    def test_unavailable_source_never_renders_as_zero(self):
        report = self.base()
        report["spend"]["Meta Ads"] = Unavailable("token expired")
        html = render(report)
        self.assertIn("unavailable", html)
        self.assertNotIn("Meta Ads       $0", html)

    def test_no_revenue_source_makes_the_net_unavailable(self):
        report = self.base()
        report["revenue"] = {
            "App Store": Unavailable("down"),
            "Play Store": Unavailable("down"),
        }
        html = render(report)
        self.assertRegex(html, r"Net\s+unavailable")

    def test_warnings_are_escaped(self):
        report = self.base()
        report["warnings"] = ["Meta failed <object at 0x1> a&b"]
        html = render(report)
        self.assertIn("&lt;object at 0x1&gt; a&amp;b", html)

    def test_never_calls_it_profit(self):
        self.assertNotIn("profit", render(self.base()).lower())


class SendTest(unittest.TestCase):
    def test_raises_after_retries(self):
        attempts = []

        def post(url, json=None, timeout=None):
            attempts.append(url)
            raise OSError("connection reset")

        with self.assertRaises(DeliveryError):
            send("123:ABC", "-1", "<pre>x</pre>", post=post, sleep=lambda s: None)
        self.assertEqual(len(attempts), 3)

    def test_delivery_error_does_not_leak_the_token(self):
        def post(url, json=None, timeout=None):
            raise OSError("failed at https://api.telegram.org/bot123:ABC/sendMessage")

        with self.assertRaises(DeliveryError) as caught:
            send("123:ABC", "-1", "<pre>x</pre>", post=post, sleep=lambda s: None)
        self.assertNotIn("123:ABC", str(caught.exception))

    def test_succeeds_without_retrying(self):
        class Ok:
            status_code = 200

        calls = []

        def post(url, json=None, timeout=None):
            calls.append(url)
            return Ok()

        send("123:ABC", "-1", "<pre>x</pre>", post=post)
        self.assertEqual(len(calls), 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_telegram -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pnl_telegram'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/pnl/pnl_telegram.py
"""Rendering and delivery.

Four ways this silently fails to send, each hit in practice: an unescaped
exception string ("400 can't parse entities" — nothing delivered, on exactly the
day a source broke); a message over 4096 characters; a truncation landing
mid-entity; and a leaked bot token in a log.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Callable, Optional

import requests

from pnl_money import Amount, SourceValue, Unavailable, combine, format_usd, round_usd

_LIMIT = 4096
_RETRIES = 3
_TIMEOUT = 30
_LABEL_WIDTH = 14


class DeliveryError(RuntimeError):
    pass


def escape(text: str) -> str:
    """Escape the three characters Telegram's HTML parser chokes on.

    Ampersand first, or the escapes themselves get double-escaped. Quotes need
    no handling: this text sits in element content, never in an attribute.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def truncate(text: str, limit: int = _LIMIT) -> str:
    """Cut on a line boundary — never mid-entity, which would emit a half
    written ``&am`` and reproduce the failure being defended against."""
    if len(text) <= limit:
        return text
    kept = []
    used = 0
    for line in text.split("\n"):
        if used + len(line) + 1 > limit:
            break
        kept.append(line)
        used += len(line) + 1
    return "\n".join(kept)


def redact(text: str, token: str) -> str:
    return text.replace(token, "<redacted>") if token else text


def _row(label: str, value: SourceValue, note: str = "") -> str:
    shown = format_usd(value.usd) if isinstance(value, Amount) else "unavailable"
    suffix = f"   ({note})" if note else ""
    return f"  {label:<{_LABEL_WIDTH}}{shown:>10}{suffix}"


def render(report: dict) -> str:
    revenue, spend = report["revenue"], report["spend"]

    # Round each line once, then derive the totals from the rounded parts, or
    # the printed column visibly fails to subtract.
    rounded_revenue = {
        k: Amount(round_usd(v.usd)) if isinstance(v, Amount) else v for k, v in revenue.items()
    }
    rounded_spend = {
        k: Amount(round_usd(v.usd)) if isinstance(v, Amount) else v for k, v in spend.items()
    }

    revenue_total = combine(rounded_revenue.values(), "no revenue source could be read")
    spend_total = combine(rounded_spend.values(), "no spend source could be read")

    lines = [f"Marketing net — {report['month_label']}", "", "Revenue"]
    for label, value in rounded_revenue.items():
        note = report.get("appstore_window_label") if label == "App Store" else ""
        lines.append(_row(label, value, note or ""))
    lines.append(_row("Total", revenue_total))
    lines += ["", "Spend"]
    for label, value in rounded_spend.items():
        lines.append(_row(label, value))
    lines.append(_row("Total", spend_total))

    if isinstance(revenue_total, Amount):
        net_spend = spend_total.usd if isinstance(spend_total, Amount) else Decimal("0")
        net: SourceValue = Amount(revenue_total.usd - net_spend)
    else:
        # A net built from spend alone reads as a catastrophic loss to anyone
        # who sees the figure before the warning.
        net = Unavailable("no revenue source could be read")
    lines += ["", _row("Net", net).lstrip()]

    body = "\n".join(lines)
    html = f"<pre>{escape(body)}</pre>"

    warnings = report.get("warnings") or []
    if warnings:
        rendered = "\n".join(f"⚠ {escape(w)}" for w in warnings)
        html = f"{html}\n\n{rendered}"
    return truncate(html)


def send(
    token: str,
    chat_id: str,
    html: str,
    post: Optional[Callable] = None,
    sleep: Optional[Callable] = None,
) -> None:
    post = post or requests.post
    sleep = sleep or time.sleep  # injectable so the suite does not really wait
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    last = ""
    for attempt in range(_RETRIES):
        try:
            response = post(
                url,
                json={"chat_id": chat_id, "text": html, "parse_mode": "HTML"},
                timeout=_TIMEOUT,
            )
            if getattr(response, "status_code", 0) == 200:
                return
            last = f"status {getattr(response, 'status_code', '?')}"
        except Exception as exc:
            last = redact(str(exc), token)
        if attempt < _RETRIES - 1:
            sleep(2 ** attempt)
    raise DeliveryError(redact(f"Telegram delivery failed after {_RETRIES} attempts: {last}", token))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/pnl && python3 -m unittest test_pnl_telegram -v`
Expected: PASS (14 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/pnl_telegram.py scripts/pnl/test_pnl_telegram.py
git commit -m "feat(marketing-net): message rendering and Telegram delivery"
```

---

### Task 9: Orchestrator

**Files:**
- Create: `scripts/pnl/marketing_net.py`
- Test: `scripts/pnl/test_marketing_net.py`

**Interfaces:**
- Consumes: every module above.
- Produces: `load_config(env: dict) -> dict` (raises `ConfigError` naming the missing key), `build_report(config, today, sources) -> dict`, `main(argv=None) -> int`. `sources` is a dict of callables so the test never touches the network.

- [ ] **Step 1: Write the failing test**

```python
# scripts/pnl/test_marketing_net.py
import unittest
from datetime import date
from decimal import Decimal

from marketing_net import ConfigError, build_report, load_config
from pnl_money import Amount, Unavailable

ENV = {
    "PNL_API_KEY": "k",
    "APPSTORE_ISSUER_ID": "i",
    "APPSTORE_KEY_ID": "kid",
    "APPSTORE_P8_B64": "cDhjb250ZW50",
    "APPSTORE_VENDOR_NUMBER": "v",
    "PLAYSTORE_SA_JSON_B64": "e30=",
    "PLAYSTORE_BUCKET": "b",
    "ADS_CREDENTIALS_JSON_B64": "eyJnb29nbGUiOiB7fSwgIm1ldGEiOiB7fX0=",
    "TELEGRAM_BOT_TOKEN": "t",
    "MARKETING_NET_CHAT_ID": "-1",
}


class LoadConfigTest(unittest.TestCase):
    def test_missing_key_names_itself(self):
        env = dict(ENV)
        del env["PLAYSTORE_BUCKET"]
        with self.assertRaises(ConfigError) as caught:
            load_config(env)
        self.assertIn("PLAYSTORE_BUCKET", str(caught.exception))

    def test_never_falls_back_to_telegram_chat_id(self):
        env = dict(ENV)
        del env["MARKETING_NET_CHAT_ID"]
        env["TELEGRAM_CHAT_ID"] = "-999"  # the PNL app's own chat
        with self.assertRaises(ConfigError) as caught:
            load_config(env)
        self.assertIn("MARKETING_NET_CHAT_ID", str(caught.exception))

    def test_decodes_base64_payloads(self):
        config = load_config(ENV)
        self.assertEqual(config["appstore"]["p8"], "p8content")
        self.assertEqual(config["ads"], {"google": {}, "meta": {}})


class BuildReportTest(unittest.TestCase):
    def sources(self, **overrides):
        base = {
            "appstore": lambda: Amount(Decimal("12430")),
            "playstore": lambda: Amount(Decimal("8110")),
            "influencer": lambda: Amount(Decimal("350")),
            "google": lambda: Amount(Decimal("4220")),
            "meta": lambda: Amount(Decimal("3015")),
        }
        base.update(overrides)
        return base

    def test_collects_all_five_sources(self):
        report = build_report(load_config(ENV), date(2026, 8, 10), self.sources())
        self.assertEqual(report["revenue"]["App Store"], Amount(Decimal("12430")))
        self.assertEqual(report["spend"]["Meta Ads"], Amount(Decimal("3015")))
        self.assertEqual(report["warnings"], [])

    def test_unavailable_source_becomes_a_warning(self):
        sources = self.sources(meta=lambda: Unavailable("token expired"))
        report = build_report(load_config(ENV), date(2026, 8, 10), sources)
        self.assertEqual(len(report["warnings"]), 1)
        self.assertIn("token expired", report["warnings"][0])

    def test_a_raising_source_does_not_abort_the_run(self):
        def boom():
            raise OSError("503")

        report = build_report(load_config(ENV), date(2026, 8, 10), self.sources(google=boom))
        self.assertIsInstance(report["spend"]["Google Ads"], Unavailable)
        self.assertEqual(len(report["warnings"]), 1)

    def test_month_label_spans_first_to_today(self):
        report = build_report(load_config(ENV), date(2026, 8, 10), self.sources())
        self.assertEqual(report["month_label"], "Aug 1–10")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/pnl && python3 -m unittest test_marketing_net -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'marketing_net'`

- [ ] **Step 3: Write minimal implementation**

```python
#!/usr/bin/env python3
"""Marketing net — month-to-date app revenue minus month-to-date marketing spend.

Composes five independent sources and posts one figure to Telegram. Nothing is
persisted: the PNL app deliberately refuses to book revenue for a month still in
progress, and this must never become a way around that.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from datetime import date, timezone, datetime

from pnl_appstore import DayCache, fetch_appstore, window_days
from pnl_fx import build_rate_table, rate_date
from pnl_googleads import fetch_google_ads
from pnl_metaads import fetch_meta_ads
from pnl_money import Amount, SourceValue, Unavailable
from pnl_playstore import fetch_playstore
from pnl_spend import fetch_head_spend
from pnl_telegram import DeliveryError, render, send

PNL_BASE_URL = "https://pnl.lascade.com"
HEAD = "INFLUENCER_MARKETING"
APPSTORE_CACHE_DIR = ".appstore-cache"

REQUIRED = [
    "PNL_API_KEY",
    "APPSTORE_ISSUER_ID",
    "APPSTORE_KEY_ID",
    "APPSTORE_P8_B64",
    "APPSTORE_VENDOR_NUMBER",
    "PLAYSTORE_SA_JSON_B64",
    "PLAYSTORE_BUCKET",
    "ADS_CREDENTIALS_JSON_B64",
    "TELEGRAM_BOT_TOKEN",
    # Deliberately not TELEGRAM_CHAT_ID: that key exists in the same Infisical
    # project and belongs to the PNL app's own alerting. Falling back to it
    # would deliver to the wrong chat and succeed while doing so.
    "MARKETING_NET_CHAT_ID",
]

# Currencies the rate table is primed with. Unknown codes seen in a report are
# added on demand by the sources themselves.
SEED_CURRENCIES = ["EUR", "GBP", "INR", "JPY", "AUD", "CAD", "BRL", "MXN", "AED", "SGD"]


class ConfigError(RuntimeError):
    pass


def _b64(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


def load_config(env: dict) -> dict:
    missing = [key for key in REQUIRED if not env.get(key)]
    if missing:
        raise ConfigError(f"Missing required configuration: {', '.join(missing)}")
    return {
        "pnl_api_key": env["PNL_API_KEY"],
        "appstore": {
            "issuer_id": env["APPSTORE_ISSUER_ID"],
            "key_id": env["APPSTORE_KEY_ID"],
            "p8": _b64(env["APPSTORE_P8_B64"]),
            "vendor_number": env["APPSTORE_VENDOR_NUMBER"],
        },
        "playstore": {
            "bucket": env["PLAYSTORE_BUCKET"],
            "credentials": json.loads(_b64(env["PLAYSTORE_SA_JSON_B64"])),
        },
        "ads": json.loads(_b64(env["ADS_CREDENTIALS_JSON_B64"])),
        "telegram_token": env["TELEGRAM_BOT_TOKEN"],
        "chat_id": env["MARKETING_NET_CHAT_ID"],
    }


def _call(source) -> SourceValue:
    try:
        return source()
    except Exception as exc:
        return Unavailable(f"{type(exc).__name__}: {exc}")


def build_report(config: dict, today: date, sources: dict) -> dict:
    revenue = {
        "App Store": _call(sources["appstore"]),
        "Play Store": _call(sources["playstore"]),
    }
    spend = {
        "Influencer": _call(sources["influencer"]),
        "Google Ads": _call(sources["google"]),
        "Meta Ads": _call(sources["meta"]),
    }
    warnings = [
        f"{label}: {value.reason}"
        for label, value in list(revenue.items()) + list(spend.items())
        if isinstance(value, Unavailable)
    ]
    days = window_days(today)
    return {
        "month_label": f"{today:%b} 1–{today.day}",
        "revenue": revenue,
        "spend": spend,
        "appstore_window_label": f"to {days[-1]:%b %-d}" if days else None,
        "warnings": warnings,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Post the marketing net to Telegram.")
    parser.add_argument("--dry-run", action="store_true", help="render but do not send")
    args = parser.parse_args(argv)

    config = load_config(os.environ)
    today = datetime.now(timezone.utc).date()
    table = build_rate_table(SEED_CURRENCIES, rate_date(today))

    sources = {
        "appstore": lambda: fetch_appstore(
            config["appstore"], today, table, cache=DayCache(APPSTORE_CACHE_DIR)
        ),
        "playstore": lambda: fetch_playstore(config["playstore"], today, table),
        "influencer": lambda: fetch_head_spend(PNL_BASE_URL, config["pnl_api_key"], HEAD),
        "google": lambda: fetch_google_ads(config["ads"]["google"], today, table),
        "meta": lambda: fetch_meta_ads(config["ads"]["meta"], today, table),
    }

    report = build_report(config, today, sources)
    html = render(report)

    with open("message.html", "w", encoding="utf-8") as handle:
        handle.write(html)

    if args.dry_run:
        print(html)
        return 0

    try:
        send(config["telegram_token"], config["chat_id"], html)
    except DeliveryError as exc:
        # The only red condition: no message means no surface carrying the
        # warnings, so the workflow status is the sole remaining signal.
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the whole suite**

Run: `cd scripts/pnl && python3 -m unittest discover -p 'test_*.py' -v`
Expected: PASS, all tasks' tests green

- [ ] **Step 5: Commit**

```bash
git add scripts/pnl/marketing_net.py scripts/pnl/test_marketing_net.py
git commit -m "feat(marketing-net): orchestrator and startup config validation"
```

---

### Task 10: Workflow

**Files:**
- Create: `.github/workflows/daily-marketing-net.yml`

**Interfaces:**
- Consumes: every module, fetched by raw URL. `marketing_net.py` reads env and writes `message.html`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Confirm the pins still resolve**

The pins in Step 2 were resolved on 2026-08-10 (`requests==2.34.2`, `pyjwt==2.13.0`,
`cryptography==50.0.0`, `google-auth==2.56.3`) and the whole suite passes against them. Re-check
only if the install step fails:

```bash
python3 -m venv /tmp/mn && /tmp/mn/bin/pip install --quiet pyjwt cryptography google-auth requests
/tmp/mn/bin/pip freeze | grep -iE '^(pyjwt|cryptography|google-auth|requests)='
```

- [ ] **Step 2: Write the workflow**

```yaml
name: Daily Marketing Net

on:
  workflow_dispatch:
    inputs:
      dry_run:
        description: Render without sending
        type: boolean
        default: false
  schedule:
    # 14:00 UTC. Apple publishes the previous day around 05:00 PT, which under
    # PST is 13:00 UTC exactly — 13:00 would sit on the publication boundary.
    - cron: '0 14 * * *'

permissions:
  contents: read

concurrency:
  group: daily-marketing-net
  cancel-in-progress: false

env:
  RAW: https://raw.githubusercontent.com/Lascade-Co/actions/main
  # Hardcoded deliberately, and NOT named TELEGRAM_CHAT_ID: that key exists in
  # the pnl Infisical project for the PNL app's own alerting, and export-type:env
  # puts it in this job's environment.
  MARKETING_NET_CHAT_ID: '-5466007383'

jobs:
  post:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Set up Python
        uses: actions/setup-python@v7
        with:
          python-version: '3.13'

      - name: Install dependencies
        # Pinned: an unwatched daily cron whose crypto and HTTP stack must not
        # change under it. Replace with the versions resolved in Step 1.
        run: pip install --quiet requests==2.34.2 pyjwt==2.13.0 cryptography==50.0.0 google-auth==2.56.3

      - name: Import secrets from Infisical
        uses: Infisical/secrets-action@v1.0.16
        with:
          method: universal
          client-id: ${{ secrets.INFISICAL_CLIENT_ID }}
          client-secret: ${{ secrets.INFISICAL_CLIENT_SECRET }}
          project-slug: pnl
          env-slug: prod
          domain: ${{ secrets.INFISICAL_DOMAIN }}
          secret-path: /
          export-type: env

      - name: Download scripts
        run: |
          for module in pnl_money pnl_fx pnl_spend pnl_appstore pnl_playstore \
                        pnl_googleads pnl_metaads pnl_telegram marketing_net; do
            curl -fsSL --retry 3 --retry-delay 2 -o "$module.py" "$RAW/scripts/pnl/$module.py"
          done

      - name: Restore the App Store day cache
        # A published day is immutable, so without this a run on the 28th
        # re-downloads 27 unchanging reports. Keyed by month and by run number so
        # each run saves a fresh entry; restore-keys pulls the newest prior one.
        uses: actions/cache@v5
        with:
          path: .appstore-cache
          key: appstore-${{ github.run_id }}
          restore-keys: appstore-

      - name: Post the marketing net
        run: |
          ARGS=()
          if [ "${{ inputs.dry_run }}" = "true" ]; then ARGS+=(--dry-run); fi
          python3 marketing_net.py "${ARGS[@]}"

      - name: Upload the rendered message
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: marketing-net-message
          path: message.html
          if-no-files-found: warn
          retention-days: 30
```

- [ ] **Step 3: Validate the YAML parses**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/daily-marketing-net.yml')); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Verify every downloaded module exists**

Run:
```bash
for m in pnl_money pnl_fx pnl_spend pnl_appstore pnl_playstore pnl_googleads pnl_metaads pnl_telegram marketing_net; do
  test -f "scripts/pnl/$m.py" || echo "MISSING $m"
done; echo checked
```
Expected: `checked` with no `MISSING` lines

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/daily-marketing-net.yml
git commit -m "feat(marketing-net): daily workflow posting the net to Telegram"
```

---

## Manual verification

After Task 10, before trusting the cron:

1. **Dry run.** Actions → Daily Marketing Net → Run workflow → `dry_run: true`. Confirm the log shows a rendered table and no message arrives in Telegram.
2. **Config validation.** The first real run proves whether every Infisical key resolves. A `ConfigError` names the missing key.
3. **Bot membership** — the one open item in the spec. A live run either delivers or fails with `403 bot is not a member`; if the latter, add the bot to group `-5466007383`.
4. **Sanity-check the figures** against Google Ads UI and Meta Ads Manager for the same window. A Play figure wildly off suggests the net factor picked an odd settled month.
5. **Check the 1st.** On the 1st of the next month, the App Store line should read `unavailable` with `no published days in the window yet`, and the net should still compute from Play plus the spend lines.
