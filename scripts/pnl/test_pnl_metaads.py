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

    def test_one_bad_account_refuses_the_whole_source(self):
        # Summing only the accounts that answered understates spend, and
        # understated spend overstates the net with nothing on screen to say so.
        # Matches Google Ads and the all-or-nothing currency rule.
        def insights(token, account_id, start, end):
            if account_id == "222":
                raise OSError("permission denied")
            return [{"spend": "10", "account_currency": "USD"}]

        creds = {"token": "t", "account_ids": ["111", "222"]}
        result = fetch_meta_ads(creds, date(2026, 8, 10), self.table, insights=insights)
        self.assertIsInstance(result, Unavailable)
        self.assertIn("222", result.reason)
