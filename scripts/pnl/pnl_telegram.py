"""Rendering and delivery.

Four ways this silently fails to send, each hit in practice: an unescaped
exception string ("400 can't parse entities" — nothing delivered, on exactly the
day a source broke); a message over 4096 characters; a truncation landing
mid-entity; and a leaked bot token in a log.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Callable, Optional

import requests

from pnl_money import Amount, SourceValue, Unavailable, combine, format_usd, round_usd

_LIMIT = 4096
_RETRIES = 3
_TIMEOUT = 30

#: Caption limit for sendPhoto, which is far tighter than sendMessage's 4096.
_CAPTION_LIMIT = 1024


class DeliveryError(RuntimeError):
    pass


def escape(text: str) -> str:
    """Escape the three characters Telegram's HTML parser chokes on.

    Ampersand first, or the escapes themselves get double-escaped. Quotes need
    no handling: this text sits in element content, never in an attribute.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def truncate(text: str, limit: int = _LIMIT) -> str:
    """Cut on a line boundary — never mid-entity, which would emit a half
    written ``&am`` and reproduce the failure being defended against.

    Warnings are appended after the table, so they are the first thing lost.
    The marker keeps that visible: a silently shortened warning list reads as a
    complete one.
    """
    if len(text) <= limit:
        return text
    marker = "\n… truncated"
    kept = []
    for line in text.split("\n"):
        candidate = "\n".join(kept + [line])
        if len(candidate) + len(marker) > limit:
            break
        kept.append(line)
    if not kept:
        return marker.lstrip("\n")
    return "\n".join(kept) + marker


def redact(text: str, token: str) -> str:
    return text.replace(token, "<redacted>") if token else text


def _shown(value: SourceValue) -> str:
    return format_usd(value.usd) if isinstance(value, Amount) else "unavailable"


def _line(label: str, value: SourceValue) -> str:
    return f"{escape(label)} · {escape(_shown(value))}"


def render(report: dict) -> str:
    revenue, spend = report["revenue"], report["spend"]

    # Round each line once, then derive the totals from the rounded parts, or
    # the printed column visibly fails to subtract.
    rounded_revenue = {
        k: Amount(round_usd(v.usd)) if isinstance(v, Amount) else v for k, v in revenue.items()
    }
    rounded_spend = {
        k: Amount(round_usd(v.usd)) if isinstance(v, Amount) else v for k, v in spend.items()
    }

    revenue_total = combine(rounded_revenue.values(), "no revenue source could be read")
    spend_total = combine(rounded_spend.values(), "no spend source could be read")

    if not isinstance(revenue_total, Amount):
        # A net built from spend alone reads as a catastrophic loss to anyone
        # who sees the figure before the warning.
        net: SourceValue = Unavailable("no revenue source could be read")
    elif not isinstance(spend_total, Amount):
        # Treating an unreadable spend total as zero would print the net as
        # equal to gross revenue — a record month, on precisely the day every
        # spend source broke.
        net = Unavailable("no spend source could be read")
    else:
        net = Amount(revenue_total.usd - spend_total.usd)

    # Tags are structure and must not be escaped; everything interpolated into
    # them must be. Each part is its own line, and no tag spans a line, so
    # line-boundary truncation can never split one.
    parts = [
        f"<b>Marketing net</b> · {escape(report['month_label'])}",
        "",
        f"<b>Revenue · {escape(_shown(revenue_total))}</b>",
    ]
    parts += [_line(label, value) for label, value in rounded_revenue.items()]
    parts += ["", f"<b>Spend · {escape(_shown(spend_total))}</b>"]
    parts += [_line(label, value) for label, value in rounded_spend.items()]
    parts += ["", f"<b>Net · {escape(_shown(net))}</b>"]

    window = report.get("appstore_window_label")
    if window:
        parts.append(
            f"<i>{escape(f'App Store {window}; every other source through today.')}</i>"
        )

    warnings = report.get("warnings") or []
    if warnings:
        parts.append("")
        parts += [f"⚠️ {escape(w)}" for w in warnings]

    return truncate("\n".join(parts))


def render_caption(report: dict) -> str:
    """What rides alongside the image.

    The card carries the figures, so the caption only needs the heading, the
    window note and any warnings — and it lives under a 1024-character limit
    rather than 4096.
    """
    parts = [f"<b>Marketing net</b> · {escape(report['month_label'])}"]

    window = report.get("appstore_window_label")
    if window:
        parts.append(
            f"<i>{escape(f'App Store {window}; every other source through today.')}</i>"
        )

    warnings = report.get("warnings") or []
    if warnings:
        parts.append("")
        parts += [f"⚠️ {escape(w)}" for w in warnings]

    return truncate("\n".join(parts), limit=_CAPTION_LIMIT)


def _deliver(token: str, method: str, call, sleep: Optional[Callable]) -> None:
    """Retry ``call`` and raise ``DeliveryError`` if it never succeeds.

    Shared so the photo and text paths cannot drift apart on retry count,
    backoff, or — the one that matters — token redaction.
    """
    sleep = sleep or time.sleep  # injectable so the suite does not really wait
    last = ""
    for attempt in range(_RETRIES):
        try:
            response = call()
            if getattr(response, "status_code", 0) == 200:
                return
            last = f"status {getattr(response, 'status_code', '?')}"
        except Exception as exc:
            last = redact(str(exc), token)
        if attempt < _RETRIES - 1:
            sleep(2 ** attempt)
    raise DeliveryError(
        redact(f"Telegram {method} failed after {_RETRIES} attempts: {last}", token)
    )


def send(
    token: str,
    chat_id: str,
    html: str,
    post: Optional[Callable] = None,
    sleep: Optional[Callable] = None,
) -> None:
    post = post or requests.post
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    _deliver(
        token,
        "sendMessage",
        lambda: post(
            url,
            json={"chat_id": chat_id, "text": html, "parse_mode": "HTML"},
            timeout=_TIMEOUT,
        ),
        sleep,
    )


def send_photo(
    token: str,
    chat_id: str,
    png: bytes,
    caption: str,
    post: Optional[Callable] = None,
    sleep: Optional[Callable] = None,
) -> None:
    post = post or requests.post
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    _deliver(
        token,
        "sendPhoto",
        lambda: post(
            url,
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": ("marketing-net.png", png, "image/png")},
            timeout=_TIMEOUT,
        ),
        sleep,
    )
