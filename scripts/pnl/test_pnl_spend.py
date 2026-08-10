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
