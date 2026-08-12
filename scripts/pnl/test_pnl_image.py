import unittest
from decimal import Decimal

from pnl_image import render_png
from pnl_money import Amount, Unavailable


def report(**overrides):
    base = {
        "month_label": "Aug 1–10",
        "revenue": {"App Store": Amount(Decimal("3937")), "Play Store": Amount(Decimal("5320"))},
        "spend": {
            "Influencer": Amount(Decimal("270")),
            "Google Ads": Amount(Decimal("1928")),
            "Meta Ads": Amount(Decimal("912")),
        },
        "appstore_window_label": "to Aug 9",
        "comparison": {"label": "vs Mar 1–10", "value": Amount(Decimal("1250"))},
        "warnings": [],
    }
    base.update(overrides)
    return base


class RenderPngTest(unittest.TestCase):
    def test_produces_a_png(self):
        png = render_png(report())
        self.assertTrue(png.startswith(b"\x89PNG\r\n\x1a\n"))

    def test_renders_unavailable_sources_without_raising(self):
        png = render_png(report(spend={"Meta Ads": Unavailable("token expired")}))
        self.assertTrue(png.startswith(b"\x89PNG\r\n\x1a\n"))

    def test_renders_when_every_source_is_unavailable(self):
        png = render_png(report(
            revenue={"App Store": Unavailable("down"), "Play Store": Unavailable("down")},
            spend={"Influencer": Unavailable("down")},
            appstore_window_label=None,
        ))
        self.assertTrue(png.startswith(b"\x89PNG\r\n\x1a\n"))

    def test_warnings_grow_the_canvas_rather_than_overflowing_it(self):
        from PIL import Image
        import io

        short = Image.open(io.BytesIO(render_png(report())))
        tall = Image.open(io.BytesIO(render_png(report(warnings=["a", "b", "c"]))))
        self.assertGreater(tall.height, short.height)

    def test_renders_unavailable_comparison_without_raising(self):
        png = render_png(report(comparison={
            "label": "vs Mar 1–10",
            "value": Unavailable("current report incomplete"),
        }))
        self.assertTrue(png.startswith(b"\x89PNG\r\n\x1a\n"))


class NetColourTest(unittest.TestCase):
    """Colour carries confidence, not just sign."""

    def _net_pixels(self, rep):
        import io

        from PIL import Image

        image = Image.open(io.BytesIO(render_png(rep)))
        # The net figure is the largest text on the card, sitting in the band
        # just above the footnote on the right-hand side.
        crop = image.crop((image.width // 2, 0, image.width, image.height))
        return {pixel for _, pixel in crop.getcolors(maxcolors=1 << 20)}

    def test_complete_report_nets_green(self):
        from pnl_image import POSITIVE

        self.assertIn(POSITIVE, self._net_pixels(report()))

    def test_incomplete_report_nets_amber_not_green(self):
        from pnl_image import POSITIVE, WARN

        pixels = self._net_pixels(report(
            spend={"Influencer": Amount(Decimal("270")), "Meta Ads": Unavailable("x")},
            comparison={"label": "vs Mar 1–10", "value": Unavailable("incomplete")},
        ))
        self.assertIn(WARN, pixels)
        self.assertNotIn(POSITIVE, pixels)

    def test_negative_net_is_red(self):
        from pnl_image import NEGATIVE

        pixels = self._net_pixels(report(
            revenue={"App Store": Amount(Decimal("100"))},
            spend={"Influencer": Amount(Decimal("9000"))},
        ))
        self.assertIn(NEGATIVE, pixels)
