import calendar
import io
import unittest
from datetime import date, timedelta
from decimal import Decimal

from PIL import Image

from pnl_charts import (
    HEIGHT,
    WIDTH,
    build_revenue_chart_data,
    render_cumulative_revenue_png,
    render_daily_revenue_png,
)
from pnl_money import Unavailable


def benchmark():
    month = date(2026, 3, 1)
    days = {}
    for offset in range(calendar.monthrange(month.year, month.month)[1]):
        day = month + timedelta(days=offset)
        days[day.isoformat()] = {
            "revenue": {
                "App Store": Decimal(day.day),
                "Play Store": Decimal(day.day * 2),
            },
            "spend": {},
        }
    return {"month": month, "days": days}


def live(today):
    appstore = {}
    playstore = {}
    for day_number in range(1, today.day):
        day = today.replace(day=day_number)
        appstore[day] = Decimal(day_number)
        playstore[day] = Decimal(day_number * 2)
    playstore[today] = Decimal(today.day * 2)
    return appstore, playstore


class RevenueChartDataTest(unittest.TestCase):
    def test_uses_only_complete_both_store_calendar_days(self):
        today = date(2026, 8, 4)
        appstore, playstore = live(today)
        data = build_revenue_chart_data(today, appstore, playstore, benchmark())
        self.assertEqual(data["current_daily"], {
            1: Decimal("3"),
            2: Decimal("6"),
            3: Decimal("9"),
        })
        self.assertNotIn(4, data["current_daily"])
        self.assertEqual(data["current_cumulative"][3], Decimal("18"))

    def test_estimate_projects_completed_day_average_to_month_end(self):
        today = date(2026, 8, 4)
        appstore, playstore = live(today)
        data = build_revenue_chart_data(today, appstore, playstore, benchmark())
        self.assertEqual(data["estimate"][3], Decimal("18"))
        self.assertEqual(data["estimate"][31], Decimal("186"))

    def test_unavailable_source_never_becomes_a_partial_current_series(self):
        today = date(2026, 8, 4)
        _, playstore = live(today)
        data = build_revenue_chart_data(
            today, Unavailable("token expired"), playstore, benchmark()
        )
        self.assertEqual(data["current_daily"], {})
        self.assertIn("token expired", data["warning"])


class RevenueChartRenderTest(unittest.TestCase):
    def setUp(self):
        today = date(2026, 8, 26)
        self.data = build_revenue_chart_data(today, *live(today), benchmark())

    def _assert_high_resolution_png(self, rendered):
        image = Image.open(io.BytesIO(rendered))
        self.assertEqual(image.size, (WIDTH, HEIGHT))
        self.assertGreaterEqual(image.width, 1600)

    def test_cumulative_chart_is_high_resolution_png(self):
        self._assert_high_resolution_png(render_cumulative_revenue_png(self.data))

    def test_daily_chart_is_high_resolution_png(self):
        self._assert_high_resolution_png(render_daily_revenue_png(self.data))

    def test_unavailable_chart_still_renders_an_explanatory_image(self):
        today = date(2026, 8, 26)
        _, playstore = live(today)
        data = build_revenue_chart_data(today, Unavailable("down"), playstore, benchmark())
        self._assert_high_resolution_png(render_cumulative_revenue_png(data))


if __name__ == "__main__":
    unittest.main()
