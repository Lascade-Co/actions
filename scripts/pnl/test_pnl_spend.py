import unittest
from datetime import date
from decimal import Decimal

from pnl_money import Amount, Unavailable
from pnl_spend import fetch_head_spend, fetch_head_spend_daily


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
            fetch_head_spend("https://pnl.example", "k", "INFLUENCER MARKETING", get=get),
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
        fetch_head_spend("https://pnl.example", "secret", "INFLUENCER MARKETING", get=get)
        self.assertEqual(get.seen["headers"], {"X-Api-Key": "secret"})
        self.assertEqual(get.seen["params"], {"head": "INFLUENCER MARKETING"})

    def test_sends_day_of_month_when_requested(self):
        get = responder(FakeResponse(200, {"day": 3, "spend_usd": "12.5000"}))
        result = fetch_head_spend(
            "https://pnl.example", "secret", "H", day=3, get=get
        )
        self.assertEqual(result, Amount(Decimal("12.5000")))
        self.assertEqual(get.seen["params"], {"head": "H", "day": 3})

    def test_daily_attribution_conflict_is_unavailable(self):
        result = fetch_head_spend(
            "https://pnl.example",
            "secret",
            "H",
            day=3,
            get=responder(FakeResponse(409)),
        )
        self.assertIsInstance(result, Unavailable)
        self.assertIn("daily spend attribution", result.reason)

    def test_day_response_must_echo_the_requested_day(self):
        result = fetch_head_spend(
            "https://pnl.example",
            "secret",
            "H",
            day=3,
            get=responder(FakeResponse(200, {"spend_usd": "12.5000"})),
        )
        self.assertIsInstance(result, Unavailable)

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


class NonNumericPayloadTest(unittest.TestCase):
    def test_non_numeric_spend_returns_unavailable_not_raise(self):
        # The contract is "returns a SourceValue". InvalidOperation is an
        # ArithmeticError, so a ValueError-only guard would break that.
        get = responder(FakeResponse(200, {"spend_usd": "n/a"}))
        result = fetch_head_spend("https://pnl.example", "k", "H", get=get)
        self.assertIsInstance(result, Unavailable)


class FetchHeadSpendDailyTest(unittest.TestCase):
    def test_consumes_one_exact_day_response_for_each_day_through_today(self):
        seen = []

        def get(url, params=None, headers=None, timeout=None):
            seen.append(params["day"])
            return FakeResponse(
                200,
                {"day": params["day"], "spend_usd": str(params["day"] * 10)},
            )

        result = fetch_head_spend_daily(
            "https://pnl.example", "secret", "H", date(2026, 8, 3), get=get
        )
        self.assertEqual(seen, [1, 2, 3])
        self.assertEqual(
            result,
            {
                date(2026, 8, 1): Decimal("10"),
                date(2026, 8, 2): Decimal("20"),
                date(2026, 8, 3): Decimal("30"),
            },
        )

    def test_one_unavailable_day_refuses_the_whole_series(self):
        def get(url, params=None, headers=None, timeout=None):
            return FakeResponse(
                409 if params["day"] == 2 else 200,
                {"day": params["day"], "spend_usd": "1"},
            )

        result = fetch_head_spend_daily(
            "https://pnl.example", "secret", "H", date(2026, 8, 3), get=get
        )
        self.assertIsInstance(result, Unavailable)
