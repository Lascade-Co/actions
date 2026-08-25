import calendar
import io
import unittest
from datetime import date, timedelta
from decimal import Decimal

from PIL import Image

from pnl_charts import (
    HEIGHT,
    WIDTH,
    build_marketing_net_chart_data,
    render_cumulative_marketing_net_png,
    render_daily_marketing_net_png,
)
from pnl_money import Unavailable


def benchmark():
    month = date(2026, 3, 1)
    days = {}
    for offset in range(calendar.monthrange(month.year, month.month)[1]):
        day = month + timedelta(days=offset)
        days[day.isoformat()] = {
            "revenue": {
                "App Store": Decimal(day.day * 2),
                "Play Store": Decimal(day.day * 3),
            },
            "spend": {
                "Influencer": Decimal("1"),
                "Google Ads": Decimal(day.day),
                "Meta Ads": Decimal("2"),
            },
        }
    return {"month": month, "days": days}


def live(today):
    appstore = {}
    playstore = {}
    influencer = {}
    google = {}
    meta = {}
    for day_number in range(1, today.day + 1):
        day = today.replace(day=day_number)
        playstore[day] = Decimal(day_number * 3)
        influencer[day] = Decimal("1")
        google[day] = Decimal(day_number)
        meta[day] = Decimal("2")
        if day < today:
            appstore[day] = Decimal(day_number * 2)
    return appstore, playstore, influencer, google, meta


class MarketingNetChartDataTest(unittest.TestCase):
    def test_subtracts_all_three_spend_sources_per_complete_day(self):
        today = date(2026, 8, 4)
        data = build_marketing_net_chart_data(today, *live(today), benchmark())
        self.assertEqual(
            data["current_daily"],
            {
                1: Decimal("1"),
                2: Decimal("5"),
                3: Decimal("9"),
            },
        )
        self.assertNotIn(4, data["current_daily"])
        self.assertEqual(data["current_cumulative"][3], Decimal("15"))

    def test_march_series_is_daily_revenue_minus_daily_spend(self):
        today = date(2026, 8, 4)
        data = build_marketing_net_chart_data(today, *live(today), benchmark())
        self.assertEqual(data["benchmark_daily"][1], Decimal("1"))
        self.assertEqual(data["benchmark_daily"][3], Decimal("9"))
        self.assertEqual(data["benchmark_cumulative"][3], Decimal("15"))

    def test_estimate_projects_completed_day_average_net_to_month_end(self):
        today = date(2026, 8, 4)
        data = build_marketing_net_chart_data(today, *live(today), benchmark())
        self.assertEqual(data["estimate"][3], Decimal("15"))
        self.assertEqual(data["estimate"][31], Decimal("155"))

    def test_unavailable_spend_never_becomes_a_partial_current_series(self):
        today = date(2026, 8, 4)
        appstore, playstore, influencer, _, meta = live(today)
        data = build_marketing_net_chart_data(
            today,
            appstore,
            playstore,
            influencer,
            Unavailable("API down"),
            meta,
            benchmark(),
        )
        self.assertEqual(data["current_daily"], {})
        self.assertIn("Google Ads", data["warning"])


class MarketingNetChartRenderTest(unittest.TestCase):
    def setUp(self):
        today = date(2026, 8, 26)
        self.data = build_marketing_net_chart_data(today, *live(today), benchmark())

    def _assert_high_resolution_png(self, rendered):
        image = Image.open(io.BytesIO(rendered))
        self.assertEqual(image.size, (WIDTH, HEIGHT))
        self.assertGreaterEqual(image.width, 1600)

    def test_cumulative_chart_is_high_resolution_png(self):
        self._assert_high_resolution_png(
            render_cumulative_marketing_net_png(self.data)
        )

    def test_daily_chart_is_high_resolution_png(self):
        self._assert_high_resolution_png(render_daily_marketing_net_png(self.data))

    def test_unavailable_chart_still_renders_an_explanatory_image(self):
        today = date(2026, 8, 26)
        appstore, playstore, influencer, google, _ = live(today)
        data = build_marketing_net_chart_data(
            today,
            appstore,
            playstore,
            influencer,
            google,
            Unavailable("down"),
            benchmark(),
        )
        self._assert_high_resolution_png(render_cumulative_marketing_net_png(data))


if __name__ == "__main__":
    unittest.main()
