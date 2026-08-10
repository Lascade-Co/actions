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

    def test_an_unpriceable_currency_is_never_dropped_from_the_sum(self):
        """The live failure: 33 of 44 real currencies had no rate.

        Skipping them left a figure built from the priced portion — understated
        by most of the month's revenue, and perfectly plausible on the page.
        """
        def fetch_day(config, day):
            return tsv("APPLE\t1.00\t1\tUSD", "APPLE\t100\t1\tXYZ")

        result = fetch_appstore(self.config, date(2026, 8, 3), self.table, fetch_day=fetch_day)
        self.assertIsInstance(result, Unavailable)
        self.assertIn("XYZ", result.reason)

    def test_free_installs_carry_a_blank_currency_and_are_simply_zero(self):
        # Product type 1F: proceeds 0.00, currency blank. Real rows, every day.
        def fetch_day(config, day):
            return tsv("APPLE\t1.00\t1\tUSD", "APPLE\t0.00\t1\t ")

        result = fetch_appstore(self.config, date(2026, 8, 3), self.table, fetch_day=fetch_day)
        self.assertEqual(result, Amount(Decimal("2.00")))


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
