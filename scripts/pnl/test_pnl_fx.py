import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable, build_rate_table, convert_all, rate_date


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

    def test_blank_currency_on_a_zero_row_converts_to_zero(self):
        # Apple leaves the proceeds currency blank on free installs (product
        # type 1F), always with proceeds of 0.00. Zero is zero in any currency.
        self.assertEqual(self.table.to_usd(Decimal("0"), ""), Decimal("0"))
        self.assertEqual(self.table.to_usd(Decimal("0"), " "), Decimal("0"))

    def test_blank_currency_on_a_real_amount_is_unknown_not_dollars(self):
        # Reading a blank code as USD books foreign money at par.
        self.assertIsNone(self.table.to_usd(Decimal("100"), ""))

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

    def test_an_unseeded_code_is_resolved_on_first_sight(self):
        # The live bug: reports carry ~56 currencies, a seed list carried 10,
        # and every unseeded one was treated as unpriceable.
        table = build_rate_table([], date(2026, 8, 9), fetch=lambda code, day: Decimal("0.5"))
        self.assertEqual(table.to_usd(Decimal("100"), "BOB"), Decimal("50.0"))

    def test_a_resolved_code_is_fetched_once_however_many_rows_use_it(self):
        calls = []

        def fake_fetch(code, day):
            calls.append(code)
            return Decimal("0.5")

        table = build_rate_table([], date(2026, 8, 9), fetch=fake_fetch)
        for _ in range(3):
            table.to_usd(Decimal("2"), "BOB")
        self.assertEqual(calls, ["BOB"])

    def test_a_code_that_failed_is_never_retried(self):
        calls = []

        def fake_fetch(code, day):
            calls.append(code)
            return None

        table = build_rate_table([], date(2026, 8, 9), fetch=fake_fetch)
        self.assertIsNone(table.to_usd(Decimal("2"), "XYZ"))
        self.assertIsNone(table.to_usd(Decimal("2"), "XYZ"))
        self.assertEqual(calls, ["XYZ"])

    def test_resolution_uses_the_run_rate_date_not_today(self):
        seen = []
        table = build_rate_table(
            [], date(2026, 8, 9), fetch=lambda code, day: seen.append(day) or Decimal("1")
        )
        table.to_usd(Decimal("1"), "BOB")
        self.assertEqual(seen, [date(2026, 8, 9)])


class ConvertAllTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({"INR": Decimal("0.01")}, date(2026, 8, 9))

    def test_sums_every_currency(self):
        total, blocked = convert_all({"USD": Decimal("5"), "INR": Decimal("200")}, self.table)
        self.assertEqual(total, Decimal("7.00"))
        self.assertEqual(blocked, [])

    def test_one_unpriceable_currency_withholds_the_whole_total(self):
        # Dropping it would report the convertible portion — smaller than the
        # truth and entirely plausible, which no reader can catch.
        total, blocked = convert_all({"USD": Decimal("5"), "XYZ": Decimal("200")}, self.table)
        self.assertIsNone(total)
        self.assertEqual(blocked, ["XYZ"])

    def test_names_every_blocking_code_not_just_the_first(self):
        total, blocked = convert_all({"ABC": Decimal("1"), "XYZ": Decimal("2")}, self.table)
        self.assertIsNone(total)
        self.assertEqual(blocked, ["ABC", "XYZ"])

    def test_blank_code_is_named_readably(self):
        total, blocked = convert_all({"": Decimal("3")}, self.table)
        self.assertIsNone(total)
        self.assertEqual(blocked, ["(blank)"])


class PrimeRateTableTest(unittest.TestCase):
    """One request should cover the whole run; per-code lookups are the fallback."""

    def test_primes_from_a_single_request(self):
        calls = []

        def prime(day):
            calls.append(day)
            return {"INR": Decimal("0.012"), "EUR": Decimal("1.1")}

        def fetch(code, day):
            raise AssertionError(f"should not fetch {code} individually")

        table = build_rate_table([], date(2026, 8, 9), fetch=fetch, prime=prime)
        self.assertEqual(len(calls), 1)
        self.assertEqual(table.to_usd(Decimal("100"), "INR"), Decimal("1.200"))
        self.assertEqual(table.to_usd(Decimal("2"), "EUR"), Decimal("2.2"))

    def test_code_absent_from_the_base_still_falls_back(self):
        asked = []

        def fetch(code, day):
            asked.append(code)
            return Decimal("0.5")

        table = build_rate_table([], date(2026, 8, 9), fetch=fetch,
                                 prime=lambda day: {"INR": Decimal("0.012")})
        self.assertEqual(table.to_usd(Decimal("2"), "XOF"), Decimal("1.0"))
        self.assertEqual(asked, ["XOF"])  # only the unprimed code costs a request

    def test_injected_fetch_alone_never_primes(self):
        # Guards the offline suite: a bare fetch injection must not reach the network.
        table = build_rate_table(["INR"], date(2026, 8, 9), fetch=lambda c, d: Decimal("0.5"))
        self.assertEqual(table.to_usd(Decimal("2"), "INR"), Decimal("1.0"))


class PrimeFallbackTest(unittest.TestCase):
    def test_falls_back_to_the_previous_day_when_priming_is_empty(self):
        # Priming is a single point of failure for every FX-bearing source.
        seen = []

        def prime(day):
            seen.append(day)
            return {} if day == date(2026, 8, 9) else {"INR": Decimal("0.012")}

        table = build_rate_table([], date(2026, 8, 9),
                                 fetch=lambda c, d: None, prime=prime)
        self.assertEqual(seen, [date(2026, 8, 9), date(2026, 8, 8)])
        self.assertEqual(table.to_usd(Decimal("100"), "INR"), Decimal("1.200"))
