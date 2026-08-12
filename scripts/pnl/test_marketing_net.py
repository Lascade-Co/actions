import base64
import calendar
import json
import unittest
from datetime import date
from decimal import Decimal

from marketing_net import ConfigError, build_report, load_config
from pnl_money import Amount, Unavailable

def benchmark_b64():
    days = {}
    for day in range(1, calendar.monthrange(2026, 3)[1] + 1):
        days[f"2026-03-{day:02d}"] = ["10", "20", "1", "2", "3"]
    raw = json.dumps({
        "schema_version": 1,
        "month": "2026-03",
        "currency": "USD",
        "categories": {
            "revenue": ["App Store", "Play Store"],
            "spend": ["Influencer", "Google Ads", "Meta Ads"],
        },
        "days": days,
    }).encode()
    return base64.b64encode(raw).decode()


ENV = {
    "PNL_API_KEY": "k",
    "APPSTORE_ISSUER_ID": "i",
    "APPSTORE_KEY_ID": "kid",
    "APPSTORE_P8_B64": "cDhjb250ZW50",
    "APPSTORE_VENDOR_NUMBER": "v",
    "PLAYSTORE_SA_JSON_B64": "e30=",
    "PLAYSTORE_BUCKET": "b",
    "ADS_CREDENTIALS_JSON_B64": "eyJnb29nbGUiOiB7fSwgIm1ldGEiOiB7fX0=",
    "MARKETING_NET_BENCHMARK_B64": benchmark_b64(),
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
        self.assertEqual(config["benchmark"]["month"], date(2026, 3, 1))

    def test_invalid_benchmark_names_the_key_without_echoing_it(self):
        env = dict(ENV, MARKETING_NET_BENCHMARK_B64="very-secret-bad-value")
        with self.assertRaises(ConfigError) as caught:
            load_config(env)
        self.assertIn("MARKETING_NET_BENCHMARK_B64", str(caught.exception))
        self.assertNotIn("very-secret", str(caught.exception))


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

    def test_a_currency_nobody_could_price_is_reported(self):
        from datetime import date as _date

        from pnl_fx import RateTable

        table = RateTable({}, _date(2026, 8, 9), missing={"XYZ"})
        report = build_report(load_config(ENV), date(2026, 8, 10), self.sources(), table)
        self.assertEqual(report["warnings"], ["No USD rate for: XYZ"])

    def test_month_label_spans_first_to_today(self):
        report = build_report(load_config(ENV), date(2026, 8, 10), self.sources())
        self.assertEqual(report["month_label"], "Aug 1–10")

    def test_adds_same_day_march_comparison(self):
        report = build_report(load_config(ENV), date(2026, 8, 10), self.sources())
        self.assertEqual(report["comparison"]["label"], "vs Mar 1–10")
        self.assertIsInstance(report["comparison"]["value"], Amount)

    def test_source_failure_makes_comparison_unavailable(self):
        report = build_report(
            load_config(ENV),
            date(2026, 8, 10),
            self.sources(meta=lambda: Unavailable("expired")),
        )
        self.assertIsInstance(report["comparison"]["value"], Unavailable)
