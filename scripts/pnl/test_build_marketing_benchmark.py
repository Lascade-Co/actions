import base64
import json
import tempfile
import unittest
from datetime import date
from decimal import Decimal

from build_marketing_benchmark import (
    BuildError,
    build_payload,
    fetch_appstore_daily,
    fetch_google_ads_daily,
    fetch_meta_ads_daily,
    fetch_playstore_daily,
    load_influencer_daily,
    month_dates,
)
from pnl_benchmark import decode_benchmark
from pnl_fx import RateTable


APPSTORE_HEADER = "Provider\tDeveloper Proceeds\tUnits\tCurrency of Proceeds"
SALES_HEADER = "Order Charged Date,Charged Amount,Currency of Sale,Financial Status"
EARNINGS_HEADER = (
    "Transaction Date,Amount (Merchant Currency),Merchant Currency,Transaction Type"
)


class FakeStorage:
    def __init__(self, files):
        self.files = files

    def list(self, prefix):
        return [name for name in self.files if name.startswith(prefix)]

    def read_zip_csv(self, name):
        return self.files[name]


class DateTest(unittest.TestCase):
    def test_covers_leap_month(self):
        dates = month_dates(date(2024, 2, 1))
        self.assertEqual((dates[0], dates[-1], len(dates)), (
            date(2024, 2, 1), date(2024, 2, 29), 29
        ))


class AppStoreDailyTest(unittest.TestCase):
    def test_preserves_each_day_and_converts_currency(self):
        dates = [date(2026, 3, 1), date(2026, 3, 2)]
        table = RateTable({"INR": Decimal("0.01")}, date(2026, 3, 31))

        def fetch(config, day):
            if day.day == 2:
                return None
            return APPSTORE_HEADER + "\nAPPLE\t2\t3\tUSD\nAPPLE\t100\t1\tINR\n"

        result = fetch_appstore_daily({}, dates, table, fetch_day=fetch)
        self.assertEqual(result, {
            date(2026, 3, 1): Decimal("7.00"),
            date(2026, 3, 2): Decimal("0"),
        })

    def test_unrecognised_layout_is_not_a_plausible_zero(self):
        with self.assertRaisesRegex(BuildError, "layout"):
            fetch_appstore_daily(
                {},
                [date(2026, 3, 1)],
                RateTable({}, date(2026, 3, 31)),
                fetch_day=lambda *_: "renamed,columns\n",
            )


class PlayStoreDailyTest(unittest.TestCase):
    def test_uses_settled_target_month_factor_per_sales_day(self):
        files = {
            "sales/sales_202603_a.zip": "\n".join([
                SALES_HEADER,
                "2026-03-01,100,USD,Charged",
                "2026-03-02,300,USD,Charged",
            ]),
            "earnings/earnings_202603_555-1.zip": "\n".join([
                EARNINGS_HEADER,
                '"Mar 1, 2026",200,USD,Charge',
            ]),
        }
        result = fetch_playstore_daily(
            {"bucket": "b"},
            [date(2026, 3, 1), date(2026, 3, 2), date(2026, 3, 3)],
            RateTable({}, date(2026, 3, 31)),
            storage=FakeStorage(files),
        )
        self.assertEqual(result, {
            date(2026, 3, 1): Decimal("50.0"),
            date(2026, 3, 2): Decimal("150.0"),
            date(2026, 3, 3): Decimal("0.0"),
        })

    def test_refuses_an_unsettled_month(self):
        files = {"sales/sales_202603_a.zip": SALES_HEADER}
        with self.assertRaisesRegex(BuildError, "not settled"):
            fetch_playstore_daily(
                {"bucket": "b"},
                [date(2026, 3, 1)],
                RateTable({}, date(2026, 3, 31)),
                storage=FakeStorage(files),
            )


class GoogleAdsDailyTest(unittest.TestCase):
    def test_groups_rows_by_date_across_accounts(self):
        children = [
            {"customerClient": {"id": "111", "currencyCode": "USD"}},
            {"customerClient": {"id": "222", "currencyCode": "INR"}},
        ]

        def search(token, customer_id, query):
            if "customer_client" in query:
                return children
            return {
                "111": [{
                    "segments": {"date": "2026-03-01"},
                    "metrics": {"costMicros": "5000000"},
                }],
                "222": [{
                    "segments": {"date": "2026-03-01"},
                    "metrics": {"costMicros": "100000000"},
                }],
            }[customer_id]

        creds = {"login_customer_id": "999", "skip_customer_ids": []}
        result = fetch_google_ads_daily(
            creds,
            [date(2026, 3, 1), date(2026, 3, 2)],
            RateTable({"INR": Decimal("0.01")}, date(2026, 3, 31)),
            search=search,
        )
        self.assertEqual(result, {
            date(2026, 3, 1): Decimal("6.00"),
            date(2026, 3, 2): Decimal("0"),
        })


class MetaAdsDailyTest(unittest.TestCase):
    def test_groups_rows_by_date_across_accounts(self):
        rows = {
            "111": [{
                "date_start": "2026-03-01",
                "spend": "5",
                "account_currency": "USD",
            }],
            "222": [{
                "date_start": "2026-03-01",
                "spend": "100",
                "account_currency": "INR",
            }],
        }
        result = fetch_meta_ads_daily(
            {"token": "t", "account_ids": ["111", "act_222"]},
            [date(2026, 3, 1), date(2026, 3, 2)],
            RateTable({"INR": Decimal("0.01")}, date(2026, 3, 31)),
            insights=lambda token, account, start, end: rows[account],
        )
        self.assertEqual(result, {
            date(2026, 3, 1): Decimal("6.00"),
            date(2026, 3, 2): Decimal("0"),
        })


class InfluencerDailyTest(unittest.TestCase):
    def test_zero_must_be_asserted_explicitly(self):
        dates = [date(2026, 3, 1), date(2026, 3, 2)]
        self.assertEqual(
            load_influencer_daily(None, dates, zero=True),
            {day: Decimal("0") for day in dates},
        )

    def test_input_requires_every_day(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump({"2026-03-01": "10"}, handle)
            handle.flush()
            with self.assertRaisesRegex(BuildError, "exactly one"):
                load_influencer_daily(
                    handle.name,
                    [date(2026, 3, 1), date(2026, 3, 2)],
                    zero=False,
                )


class PayloadTest(unittest.TestCase):
    def test_output_satisfies_runtime_contract(self):
        month = date(2026, 3, 1)
        dates = month_dates(month)
        zeros = {day: Decimal("0") for day in dates}
        payload = build_payload(month, date(2026, 3, 31), zeros, zeros, zeros, zeros, zeros)
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        decoded = decode_benchmark(encoded)
        self.assertEqual(decoded["month"], month)
        self.assertEqual(len(decoded["days"]), 31)


if __name__ == "__main__":
    unittest.main()
