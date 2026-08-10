"""Render the report as a PNG card.

Telegram renders text proportionally, so a column of figures only lines up
inside a code block — and a code block makes the whole message read as source.
An image escapes the choice: alignment is exact and the typography is ours.

Pillow only. A headless browser would render nicer HTML but costs a browser
download on every run of a daily cron.
"""

from __future__ import annotations

import glob
import io
import os
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

from pnl_money import Amount, SourceValue, format_usd

WIDTH = 900
PAD = 56
LINE = 52
SECTION_GAP = 34

BG = (17, 20, 24)
TEXT = (233, 237, 243)
MUTED = (140, 150, 163)
RULE = (44, 50, 58)
POSITIVE = (61, 214, 140)
NEGATIVE = (248, 113, 113)
WARN = (251, 191, 114)

#: Searched in order. The workflow installs fonts-dejavu-core so the first
#: pattern hits on the runner; the rest keep local development working.
_FONT_PATTERNS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans{bold}.ttf",
    "/usr/share/fonts/**/DejaVuSans{bold}.ttf",
    "/Library/Fonts/Arial{mac_bold}.ttf",
    "/System/Library/Fonts/Supplemental/Arial{mac_bold}.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
)


class FontUnavailable(RuntimeError):
    """No usable font. The caller falls back to a text message rather than
    delivering an unreadable image."""


def _font_path(bold: bool) -> Optional[str]:
    for pattern in _FONT_PATTERNS:
        candidate = pattern.format(
            bold="-Bold" if bold else "", mac_bold=" Bold" if bold else ""
        )
        for match in sorted(glob.glob(candidate, recursive=True)):
            if os.path.exists(match):
                return match
    return None


def _font(size: int, bold: bool = False):
    path = _font_path(bold) or _font_path(not bold)
    if path is None:
        raise FontUnavailable("no TrueType font found")
    return ImageFont.truetype(path, size)


def _shown(value: SourceValue) -> str:
    return format_usd(value.usd) if isinstance(value, Amount) else "unavailable"


def _rows(report: dict) -> list:
    """Every line to draw, as ``(kind, label, text, colour)``.

    Built before any drawing so the canvas can be sized exactly — a card with a
    slab of dead space at the bottom looks broken.
    """
    from pnl_money import Unavailable, combine, round_usd

    rounded = {}
    for key in ("revenue", "spend"):
        rounded[key] = {
            label: Amount(round_usd(v.usd)) if isinstance(v, Amount) else v
            for label, v in report[key].items()
        }

    revenue_total = combine(rounded["revenue"].values(), "no revenue source could be read")
    spend_total = combine(rounded["spend"].values(), "no spend source could be read")

    if not isinstance(revenue_total, Amount):
        net: SourceValue = Unavailable("no revenue source could be read")
    elif not isinstance(spend_total, Amount):
        net = Unavailable("no spend source could be read")
    else:
        net = Amount(revenue_total.usd - spend_total.usd)

    rows = [("section", "Revenue", "", MUTED)]
    for label, value in rounded["revenue"].items():
        rows.append(("row", label, _shown(value), TEXT))
    rows.append(("total", "Total", _shown(revenue_total), TEXT))

    rows.append(("section", "Spend", "", MUTED))
    for label, value in rounded["spend"].items():
        rows.append(("row", label, _shown(value), TEXT))
    rows.append(("total", "Total", _shown(spend_total), TEXT))

    # Colour carries confidence, not just sign. Green on an incomplete figure
    # reads as "healthy" to anyone skimming, and the net is the largest thing on
    # the card — the warnings underneath lose that argument every time.
    complete = all(
        isinstance(value, Amount)
        for section in ("revenue", "spend")
        for value in rounded[section].values()
    )
    colour = TEXT
    if isinstance(net, Amount):
        if not complete:
            colour = WARN
        else:
            colour = POSITIVE if net.usd >= 0 else NEGATIVE
    rows.append(("net", "Net", _shown(net), colour))
    return rows


def render_png(report: dict) -> bytes:
    rows = _rows(report)

    title_font = _font(42, bold=True)
    date_font = _font(26)
    section_font = _font(23, bold=True)
    row_font = _font(30)
    total_font = _font(30, bold=True)
    net_label_font = _font(38, bold=True)
    net_font = _font(46, bold=True)
    small_font = _font(22)

    warnings = report.get("warnings") or []
    window = report.get("appstore_window_label")

    height = PAD + 120
    for kind, _, _, _ in rows:
        height += SECTION_GAP if kind == "section" else 0
        height += 78 if kind == "net" else LINE
    height += 40 if window else 0
    height += 36 * len(warnings)
    height += PAD

    image = Image.new("RGB", (WIDTH, int(height)), BG)
    draw = ImageDraw.Draw(image)
    right = WIDTH - PAD
    y = PAD

    draw.text((PAD, y), "Marketing net", font=title_font, fill=TEXT)
    y += 52
    draw.text((PAD, y), report["month_label"], font=date_font, fill=MUTED)
    y += 58

    for kind, label, figure, colour in rows:
        if kind == "section":
            y += SECTION_GAP
            draw.text((PAD, y), label.upper(), font=section_font, fill=MUTED)
            y += LINE
            continue

        if kind == "total":
            draw.line([(PAD, y - 10), (right, y - 10)], fill=RULE, width=1)
        if kind == "net":
            y += 14
            draw.line([(PAD, y - 12), (right, y - 12)], fill=RULE, width=2)
            y += 12

        label_font = {"row": row_font, "total": total_font, "net": net_label_font}[kind]
        figure_font = {"row": row_font, "total": total_font, "net": net_font}[kind]
        label_fill = MUTED if kind == "row" else TEXT

        draw.text((PAD, y), label, font=label_font, fill=label_fill)
        draw.text(
            (right - draw.textlength(figure, font=figure_font), y),
            figure,
            font=figure_font,
            fill=colour,
        )
        y += 78 if kind == "net" else LINE

    if window:
        y += 12
        draw.text(
            (PAD, y),
            f"App Store {window}; every other source through today.",
            font=small_font,
            fill=MUTED,
        )
        y += 28

    for warning in warnings:
        draw.text((PAD, y), f"! {warning}", font=small_font, fill=WARN)
        y += 36

    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()
