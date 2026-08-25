import unittest
from datetime import date
from decimal import Decimal

from pnl_fx import RateTable
from pnl_googleads import fetch_google_ads, fetch_google_ads_daily
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


class FetchGoogleAdsDailyTest(unittest.TestCase):
    def setUp(self):
        self.table = RateTable({"INR": Decimal("0.01")}, date(2026, 8, 3))
        self.children = [
            {"customerClient": {"id": "111", "currencyCode": "USD"}},
            {"customerClient": {"id": "222", "currencyCode": "INR"}},
        ]

    def test_groups_accounts_by_segments_date_and_converts(self):
        costs = {
            "111": [
                {
                    "segments": {"date": "2026-08-01"},
                    "metrics": {"costMicros": "5000000"},
                },
                {
                    "segments": {"date": "2026-08-02"},
                    "metrics": {"costMicros": "2000000"},
                },
            ],
            "222": [
                {
                    "segments": {"date": "2026-08-01"},
                    "metrics": {"costMicros": "100000000"},
                }
            ],
        }
        result = fetch_google_ads_daily(
            CREDS,
            date(2026, 8, 3),
            self.table,
            search=searcher(self.children, costs),
        )
        self.assertEqual(result[date(2026, 8, 1)], Decimal("6"))
        self.assertEqual(result[date(2026, 8, 2)], Decimal("2"))
        self.assertEqual(result[date(2026, 8, 3)], Decimal("0"))

    def test_missing_segments_date_refuses_the_series(self):
        result = fetch_google_ads_daily(
            CREDS,
            date(2026, 8, 3),
            self.table,
            search=searcher(
                self.children,
                {"111": [{"metrics": {}}], "222": []},
            ),
        )
        self.assertIsInstance(result, Unavailable)
