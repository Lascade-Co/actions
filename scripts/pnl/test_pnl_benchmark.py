import base64
import calendar
import json
import unittest
from datetime import date
from decimal import Decimal

from pnl_benchmark import (
    BenchmarkError,
    benchmark_report,
    comparison,
    decode_benchmark,
)
from pnl_money import Amount, Unavailable


def payload(month="2026-03"):
    year, number = (int(part) for part in month.split("-"))
    days = {}
    for day in range(1, calendar.monthrange(year, number)[1] + 1):
        key = f"{month}-{day:02d}"
        days[key] = ["10.25", "20.25", "1", "2", "3"]
    return {
        "schema_version": 1,
        "month": month,
        "currency": "USD",
        "categories": {
            "revenue": ["App Store", "Play Store"],
            "spend": ["Influencer", "Google Ads", "Meta Ads"],
        },
        "days": days,
    }


def encoded(value=None):
    raw = json.dumps(value or payload(), separators=(",", ":")).encode()
    return base64.b64encode(raw).decode()


class DecodeBenchmarkTest(unittest.TestCase):
    def test_decodes_money_as_decimal(self):
        result = decode_benchmark(encoded())
        self.assertEqual(
            result["days"]["2026-03-01"]["revenue"]["App Store"],
            Decimal("10.25"),
        )

    def test_requires_every_calendar_day(self):
        value = payload()
        del value["days"]["2026-03-17"]
        with self.assertRaisesRegex(BenchmarkError, "missing 2026-03-17"):
            decode_benchmark(encoded(value))

    def test_requires_exact_source_names(self):
        value = payload()
        value["categories"]["spend"][-1] = "Facebook"
        with self.assertRaisesRegex(BenchmarkError, "exact revenue and spend source order"):
            decode_benchmark(encoded(value))

    def test_refuses_json_numbers_for_money(self):
        value = payload()
        value["days"]["2026-03-01"][0] = 10
        with self.assertRaisesRegex(BenchmarkError, "decimal string"):
            decode_benchmark(encoded(value))

    def test_refuses_invalid_base64_without_echoing_it(self):
        with self.assertRaisesRegex(BenchmarkError, "base64-encoded"):
            decode_benchmark("not a secret")


class BenchmarkReportTest(unittest.TestCase):
    def setUp(self):
        self.benchmark = decode_benchmark(encoded())

    def test_mirrors_live_source_windows(self):
        report = benchmark_report(self.benchmark, date(2026, 8, 10))
        self.assertEqual(report["revenue"]["App Store"], Amount(Decimal("92.25")))
        self.assertEqual(report["revenue"]["Play Store"], Amount(Decimal("202.50")))
        self.assertEqual(report["spend"]["Google Ads"], Amount(Decimal("20")))
        self.assertEqual(report["through"], 10)

    def test_months_longer_than_benchmark_cap_at_its_last_day(self):
        feb = decode_benchmark(encoded(payload("2026-02")))
        report = benchmark_report(feb, date(2026, 8, 31))
        self.assertEqual(report["through"], 28)


class ComparisonTest(unittest.TestCase):
    def setUp(self):
        self.benchmark = decode_benchmark(encoded())
        self.report = {
            "revenue": {
                "App Store": Amount(Decimal("100")),
                "Play Store": Amount(Decimal("200")),
            },
            "spend": {
                "Influencer": Amount(Decimal("10")),
                "Google Ads": Amount(Decimal("20")),
                "Meta Ads": Amount(Decimal("30")),
            },
        }

    def test_returns_signed_difference_from_same_day_in_march(self):
        result = comparison(self.report, self.benchmark, date(2026, 8, 10))
        # Current: 300 - 60 = 240. Benchmark: round(92.25)+round(202.5)-60 = 235.
        self.assertEqual(result["value"], Amount(Decimal("5")))
        self.assertEqual(result["label"], "vs Mar 1–10")

    def test_incomplete_current_report_refuses_comparison(self):
        self.report["spend"]["Meta Ads"] = Unavailable("expired")
        result = comparison(self.report, self.benchmark, date(2026, 8, 10))
        self.assertIsInstance(result["value"], Unavailable)


if __name__ == "__main__":
    unittest.main()
