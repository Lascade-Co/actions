import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable, build_rate_table
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

    def test_a_currency_outside_any_seed_list_still_yields_a_factor(self):
        """The live failure: real months carry ~56 currencies.

        Every settled month held at least one code the pre-seeded table did not,
        so ``earnings / sales`` came back None for all 34 of them and Play was
        excluded outright. The table must resolve what the reports contain.
        """
        files = self._files()
        files["sales/salesreport_202607.zip"] = "\n".join(
            [SALES_HEADER, "2026-07-01,100,USD,Charged", "2026-07-02,5000,BOB,Charged"]
        )
        table = build_rate_table([], date(2026, 8, 9), fetch=lambda code, day: Decimal("0.02"))
        result = fetch_playstore(self.config, date(2026, 8, 10), table, storage=FakeStorage(files))
        # sales 100 USD + 5000 BOB x 0.02 = 200; earnings 140 => factor 0.7
        self.assertEqual(result, Amount(Decimal("210.00")))

    def test_an_unpriceable_currency_names_itself_rather_than_shrinking_the_figure(self):
        files = self._files()
        files["sales/salesreport_202608.zip"] = "\n".join(
            [SALES_HEADER, "2026-08-01,300,USD,Charged", "2026-08-02,99,XYZ,Charged"]
        )
        result = fetch_playstore(
            self.config, date(2026, 8, 10), self.table, storage=FakeStorage(files)
        )
        self.assertIsInstance(result, Unavailable)
        self.assertIn("XYZ", result.reason)
