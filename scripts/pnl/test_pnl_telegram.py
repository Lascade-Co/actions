import unittest
from decimal import Decimal

from pnl_money import Amount, Unavailable
from pnl_telegram import DeliveryError, escape, redact, render, send, truncate


class EscapeTest(unittest.TestCase):
    def test_escapes_the_three_html_entities(self):
        raw = "<urllib3.HTTPSConnection object> a&b"
        self.assertEqual(escape(raw), "&lt;urllib3.HTTPSConnection object&gt; a&amp;b")

    def test_ampersand_escaped_first_so_entities_are_not_double_escaped(self):
        self.assertEqual(escape("<a>"), "&lt;a&gt;")


class TruncateTest(unittest.TestCase):
    def test_leaves_short_text(self):
        self.assertEqual(truncate("abc", limit=10), "abc")

    def test_cuts_on_a_line_boundary_never_mid_entity(self):
        text = "line one\n" + "&amp;" * 100
        result = truncate(text, limit=20)
        self.assertTrue(result.startswith("line one"))
        self.assertNotIn("&am\n", result)
        self.assertLessEqual(len(result), 20)


class RedactTest(unittest.TestCase):
    def test_removes_the_bot_token(self):
        message = "failed https://api.telegram.org/bot123:ABC/sendMessage"
        self.assertNotIn("123:ABC", redact(message, "123:ABC"))


class RenderTest(unittest.TestCase):
    def base(self):
        return {
            "month_label": "Aug 1–10",
            "revenue": {"App Store": Amount(Decimal("12430")), "Play Store": Amount(Decimal("8110"))},
            "spend": {
                "Influencer": Amount(Decimal("350")),
                "Google Ads": Amount(Decimal("4220")),
                "Meta Ads": Amount(Decimal("3015")),
            },
            "appstore_window_label": "to Aug 9",
            "comparison": {"label": "vs Mar 1–10", "value": Amount(Decimal("1250"))},
            "warnings": [],
        }

    def test_columns_subtract_as_displayed(self):
        html = render(self.base())
        self.assertIn("$20,540", html)
        self.assertIn("$7,585", html)
        self.assertIn("$12,955", html)

    def test_never_uses_a_code_block(self):
        html = render(self.base())
        for tag in ("<pre>", "<code>"):
            self.assertNotIn(tag, html)

    def test_unavailable_source_never_renders_as_zero(self):
        report = self.base()
        report["spend"]["Meta Ads"] = Unavailable("token expired")
        html = render(report)
        self.assertIn("unavailable", html)
        self.assertNotIn("Meta Ads · $0", html)

    def test_no_revenue_source_makes_the_net_unavailable(self):
        report = self.base()
        report["revenue"] = {
            "App Store": Unavailable("down"),
            "Play Store": Unavailable("down"),
        }
        html = render(report)
        self.assertIn("<b>Net · unavailable</b>", html)

    def test_warnings_are_escaped(self):
        report = self.base()
        report["warnings"] = ["Meta failed <object at 0x1> a&b"]
        html = render(report)
        self.assertIn("&lt;object at 0x1&gt; a&amp;b", html)

    def test_never_calls_it_profit(self):
        self.assertNotIn("profit", render(self.base()).lower())

    def test_comparison_has_explicit_sign_and_window(self):
        html = render(self.base())
        self.assertIn("<b>vs Mar 1–10 · +$1,250</b>", html)

    def test_unavailable_comparison_is_not_zero(self):
        report = self.base()
        report["comparison"]["value"] = Unavailable("incomplete")
        html = render(report)
        self.assertIn("vs Mar 1–10 · unavailable", html)
        self.assertNotIn("vs Mar 1–10 · $0", html)


class SendTest(unittest.TestCase):
    def test_raises_after_retries(self):
        attempts = []

        def post(url, json=None, timeout=None):
            attempts.append(url)
            raise OSError("connection reset")

        with self.assertRaises(DeliveryError):
            send("123:ABC", "-1", "<pre>x</pre>", post=post, sleep=lambda s: None)
        self.assertEqual(len(attempts), 3)

    def test_delivery_error_does_not_leak_the_token(self):
        def post(url, json=None, timeout=None):
            raise OSError("failed at https://api.telegram.org/bot123:ABC/sendMessage")

        with self.assertRaises(DeliveryError) as caught:
            send("123:ABC", "-1", "<pre>x</pre>", post=post, sleep=lambda s: None)
        self.assertNotIn("123:ABC", str(caught.exception))

    def test_succeeds_without_retrying(self):
        class Ok:
            status_code = 200

        calls = []

        def post(url, json=None, timeout=None):
            calls.append(url)
            return Ok()

        send("123:ABC", "-1", "<pre>x</pre>", post=post)
        self.assertEqual(len(calls), 1)


class ReviewRegressionTest(unittest.TestCase):
    """Regressions for the code-review findings of 2026-08-10."""

    def base(self):
        return {
            "month_label": "Aug 1–10",
            "revenue": {"App Store": Amount(Decimal("12430")), "Play Store": Amount(Decimal("8110"))},
            "spend": {"Influencer": Amount(Decimal("350"))},
            "appstore_window_label": None,
            "warnings": [],
        }

    def test_unreadable_spend_total_makes_the_net_unavailable(self):
        # Coercing it to zero prints net == gross revenue: a record month, on
        # the very day every spend source broke.
        report = self.base()
        report["spend"] = {"Influencer": Unavailable("down"), "Google Ads": Unavailable("down")}
        html = render(report)
        self.assertIn("<b>Net · unavailable</b>", html)
        self.assertNotIn("$20,540", html.split("Net")[-1])

    def test_net_is_its_own_bold_line(self):
        html = render(self.base())
        self.assertIn("<b>Net · ", html)

    def test_truncation_is_announced(self):
        report = self.base()
        report["warnings"] = ["x" * 6000]
        html = render(report)
        self.assertLessEqual(len(html), 4096)
        self.assertIn("truncated", html)
