"""Render daily and cumulative marketing-net comparison charts as PNGs.

Charts use complete calendar days only: App Store reports stop at yesterday,
so today's Play Store value is deliberately excluded. The summary card keeps
its existing, fresher per-source windows; each chart states its own boundary.
"""

from __future__ import annotations

import calendar
import io
import math
from datetime import date, timedelta
from decimal import Decimal

from PIL import Image, ImageDraw

from pnl_image import BG, MUTED, RULE, TEXT, WARN, _font
from pnl_money import Unavailable

WIDTH = 1600
HEIGHT = 1100
LEFT = 164
RIGHT = WIDTH - 54
TOP = 290
BOTTOM = HEIGHT - 130

CURRENT = (82, 183, 255)
BENCHMARK = (181, 145, 255)
ESTIMATE = (73, 211, 156)


def _month_days(value: date) -> int:
    return calendar.monthrange(value.year, value.month)[1]


def _dates_through_yesterday(today: date) -> list[date]:
    end = today - timedelta(days=1)
    if (end.year, end.month) != (today.year, today.month):
        return []
    start = today.replace(day=1)
    return [start + timedelta(days=index) for index in range((end - start).days + 1)]


def build_marketing_net_chart_data(
    today: date,
    appstore,
    playstore,
    influencer,
    google,
    meta,
    benchmark: dict,
) -> dict:
    """Build graph-ready net series without ever plotting a partial source."""
    benchmark_daily = {}
    for day_text, item in benchmark["days"].items():
        day = date.fromisoformat(day_text).day
        benchmark_daily[day] = (
            sum(item["revenue"].values(), Decimal("0"))
            - sum(item["spend"].values(), Decimal("0"))
        )

    unavailable = []
    for label, value in (
        ("App Store", appstore),
        ("Play Store", playstore),
        ("Influencer", influencer),
        ("Google Ads", google),
        ("Meta Ads", meta),
    ):
        if isinstance(value, Unavailable):
            unavailable.append(
                value.reason
                if value.reason.lower().startswith(label.lower())
                else f"{label}: {value.reason}"
            )

    current_daily = {}
    complete_dates = _dates_through_yesterday(today)
    if not unavailable and not complete_dates:
        unavailable.append("No complete current-month calendar day yet")
    if not unavailable:
        sources = (appstore, playstore, influencer, google, meta)
        missing = [
            day for day in complete_dates
            if any(day not in source for source in sources)
        ]
        if missing:
            unavailable.append(
                "Marketing-net series is missing "
                + ", ".join(day.isoformat() for day in missing)
            )
        else:
            current_daily = {
                day.day: (
                    appstore[day]
                    + playstore[day]
                    - influencer[day]
                    - google[day]
                    - meta[day]
                )
                for day in complete_dates
            }

    current_cumulative = {}
    running = Decimal("0")
    for day, value in current_daily.items():
        running += value
        current_cumulative[day] = running

    benchmark_cumulative = {}
    running = Decimal("0")
    for day in sorted(benchmark_daily):
        running += benchmark_daily[day]
        benchmark_cumulative[day] = running

    estimate = {}
    if current_cumulative:
        through = max(current_cumulative)
        daily_average = current_cumulative[through] / Decimal(through)
        estimate = {
            day: daily_average * Decimal(day)
            for day in range(through, _month_days(today) + 1)
        }

    return {
        "current_label": f"{today:%b %Y}",
        "benchmark_label": f"{benchmark['month']:%b %Y}",
        "through_label": complete_dates[-1].strftime("%b %-d") if current_daily else None,
        "current_month_days": _month_days(today),
        "benchmark_month_days": _month_days(benchmark["month"]),
        "current_daily": current_daily,
        "benchmark_daily": benchmark_daily,
        "current_cumulative": current_cumulative,
        "benchmark_cumulative": benchmark_cumulative,
        "estimate": estimate,
        "warning": "; ".join(unavailable) if unavailable else None,
    }


def _tick_step(low: float, high: float) -> float:
    span = max(high - low, 1.0)
    rough = span / 5
    power = 10 ** math.floor(math.log10(rough))
    fraction = rough / power
    nice = 1 if fraction <= 1 else 2 if fraction <= 2 else 5 if fraction <= 5 else 10
    return nice * power


def _bounds(values) -> tuple[float, float, float]:
    numbers = [float(value) for value in values]
    low = min([0.0, *numbers])
    high = max([0.0, *numbers])
    if low == high:
        high = low + 1
    step = _tick_step(low, high)
    low = math.floor(low / step) * step
    high = math.ceil(high / step) * step
    if low == high:
        high += step
    return low, high, step


def _money_tick(value: float) -> str:
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    if absolute >= 1_000_000:
        shown = f"{absolute / 1_000_000:.1f}".rstrip("0").rstrip(".") + "m"
    elif absolute >= 1_000:
        shown = f"{absolute / 1_000:.1f}".rstrip("0").rstrip(".") + "k"
    else:
        shown = f"{absolute:.0f}"
    return f"{sign}${shown}"


def _x(day: int, maximum_day: int) -> float:
    return LEFT + (day - 1) * (RIGHT - LEFT) / max(maximum_day - 1, 1)


def _y(value, low: float, high: float) -> float:
    return BOTTOM - (float(value) - low) * (BOTTOM - TOP) / (high - low)


def _canvas(title: str, subtitle: str, warning: str | None):
    image = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(image)
    draw.text((54, 38), title, font=_font(52, bold=True), fill=TEXT)
    draw.text((54, 108), subtitle, font=_font(30), fill=MUTED)
    if warning:
        draw.rounded_rectangle((54, 160, RIGHT, 218), radius=14, fill=(55, 44, 30))
        clipped = warning if len(warning) <= 105 else warning[:102] + "..."
        draw.text((74, 174), f"! {clipped}", font=_font(24), fill=WARN)
    return image, draw


def _axes(draw, values, maximum_day: int):
    low, high, step = _bounds(values or [Decimal("0")])
    axis_font = _font(24)
    tick = low
    while tick <= high + step / 10:
        y = _y(tick, low, high)
        draw.line((LEFT, y, RIGHT, y), fill=RULE, width=1)
        label = _money_tick(tick)
        draw.text(
            (LEFT - 18 - draw.textlength(label, font=axis_font), y - 15),
            label,
            font=axis_font,
            fill=MUTED,
        )
        tick += step

    x_ticks = sorted({1, 5, 10, 15, 20, 25, maximum_day})
    for day in [value for value in x_ticks if value <= maximum_day]:
        x = _x(day, maximum_day)
        draw.line((x, BOTTOM, x, BOTTOM + 8), fill=RULE, width=2)
        label = str(day)
        draw.text(
            (x - draw.textlength(label, font=axis_font) / 2, BOTTOM + 15),
            label,
            font=axis_font,
            fill=MUTED,
        )
    draw.text((RIGHT - 148, BOTTOM + 66), "Day of month", font=axis_font, fill=MUTED)
    return low, high


def _legend(draw, items):
    font = _font(25, bold=True)
    x = RIGHT
    for label, colour, dashed in reversed(items):
        width = draw.textlength(label, font=font)
        x -= width
        draw.text((x, 239), label, font=font, fill=MUTED)
        x -= 42
        if dashed:
            draw.line((x, 254, x + 28, 254), fill=colour, width=4)
            draw.line((x + 10, 254, x + 17, 254), fill=BG, width=5)
        else:
            draw.line((x, 254, x + 28, 254), fill=colour, width=5)
        x -= 26


def _line(draw, series: dict, maximum_day: int, low: float, high: float, colour, width=5):
    points = [(_x(day, maximum_day), _y(value, low, high)) for day, value in series.items()]
    if len(points) >= 2:
        draw.line(points, fill=colour, width=width, joint="curve")
    for point in points:
        draw.ellipse((point[0] - 3, point[1] - 3, point[0] + 3, point[1] + 3), fill=colour)


def _dashed_line(draw, series: dict, maximum_day: int, low: float, high: float, colour):
    points = [(_x(day, maximum_day), _y(value, low, high)) for day, value in series.items()]
    for start, end in zip(points, points[1:]):
        dx, dy = end[0] - start[0], end[1] - start[1]
        distance = max(math.hypot(dx, dy), 1)
        segments = max(int(distance / 12), 1)
        for index in range(0, segments, 2):
            a = index / segments
            b = min((index + 1) / segments, 1)
            draw.line(
                (
                    start[0] + dx * a,
                    start[1] + dy * a,
                    start[0] + dx * b,
                    start[1] + dy * b,
                ),
                fill=colour,
                width=5,
            )


def _png(image) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def render_cumulative_marketing_net_png(data: dict) -> bytes:
    through = data["through_label"]
    subtitle = (
        f"{data['current_label']} vs {data['benchmark_label']} • complete days through {through} • USD"
        if through
        else f"{data['current_label']} vs {data['benchmark_label']} • USD"
    )
    image, draw = _canvas("Cumulative marketing net", subtitle, data["warning"])
    legend = [(data["benchmark_label"], BENCHMARK, False)]
    if data["current_cumulative"]:
        legend = [
            (data["current_label"], CURRENT, False),
            *legend,
            (f"{data['current_label']} estimate", ESTIMATE, True),
        ]
    _legend(draw, legend)
    maximum_day = max(data["current_month_days"], data["benchmark_month_days"])
    series = [
        *data["current_cumulative"].values(),
        *data["benchmark_cumulative"].values(),
        *data["estimate"].values(),
    ]
    low, high = _axes(draw, series, maximum_day)
    _line(draw, data["benchmark_cumulative"], maximum_day, low, high, BENCHMARK, width=4)
    _line(draw, data["current_cumulative"], maximum_day, low, high, CURRENT, width=6)
    _dashed_line(draw, data["estimate"], maximum_day, low, high, ESTIMATE)
    return _png(image)


def render_daily_marketing_net_png(data: dict) -> bytes:
    through = data["through_label"]
    subtitle = (
        f"{data['current_label']} vs {data['benchmark_label']} • complete days through {through} • USD"
        if through
        else f"{data['current_label']} vs {data['benchmark_label']} • USD"
    )
    image, draw = _canvas("Marketing net per day", subtitle, data["warning"])
    legend = [(data["benchmark_label"], BENCHMARK, False)]
    if data["current_daily"]:
        legend.insert(0, (data["current_label"], CURRENT, False))
    _legend(draw, legend)
    maximum_day = max(data["current_month_days"], data["benchmark_month_days"])
    values = [*data["current_daily"].values(), *data["benchmark_daily"].values()]
    low, high = _axes(draw, values, maximum_day)
    baseline = _y(Decimal("0"), low, high)
    slot = (RIGHT - LEFT) / max(maximum_day - 1, 1)
    bar_width = max(slot * 0.28, 3)
    for day in range(1, maximum_day + 1):
        centre = _x(day, maximum_day)
        pairs = (
            (data["current_daily"].get(day), centre - bar_width - 1, CURRENT),
            (data["benchmark_daily"].get(day), centre + 1, BENCHMARK),
        )
        for value, x, colour in pairs:
            if value is None:
                continue
            y = _y(value, low, high)
            draw.rectangle((x, min(y, baseline), x + bar_width, max(y, baseline)), fill=colour)
    return _png(image)
