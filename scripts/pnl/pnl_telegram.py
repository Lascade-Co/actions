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
_LABEL_WIDTH = 14


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
    written ``&am`` and reproduce the failure being defended against."""
    if len(text) <= limit:
        return text
    kept = []
    used = 0
    for line in text.split("\n"):
        if used + len(line) + 1 > limit:
            break
        kept.append(line)
        used += len(line) + 1
    return "\n".join(kept)


def redact(text: str, token: str) -> str:
    return text.replace(token, "<redacted>") if token else text


def _row(label: str, value: SourceValue, note: str = "") -> str:
    shown = format_usd(value.usd) if isinstance(value, Amount) else "unavailable"
    suffix = f"   ({note})" if note else ""
    return f"  {label:<{_LABEL_WIDTH}}{shown:>10}{suffix}"


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

    lines = [f"Marketing net — {report['month_label']}", "", "Revenue"]
    for label, value in rounded_revenue.items():
        note = report.get("appstore_window_label") if label == "App Store" else ""
        lines.append(_row(label, value, note or ""))
    lines.append(_row("Total", revenue_total))
    lines += ["", "Spend"]
    for label, value in rounded_spend.items():
        lines.append(_row(label, value))
    lines.append(_row("Total", spend_total))

    if isinstance(revenue_total, Amount):
        net_spend = spend_total.usd if isinstance(spend_total, Amount) else Decimal("0")
        net: SourceValue = Amount(revenue_total.usd - net_spend)
    else:
        # A net built from spend alone reads as a catastrophic loss to anyone
        # who sees the figure before the warning.
        net = Unavailable("no revenue source could be read")
    lines += ["", _row("Net", net).lstrip()]

    body = "\n".join(lines)
    html = f"<pre>{escape(body)}</pre>"

    warnings = report.get("warnings") or []
    if warnings:
        rendered = "\n".join(f"⚠ {escape(w)}" for w in warnings)
        html = f"{html}\n\n{rendered}"
    return truncate(html)


def send(
    token: str,
    chat_id: str,
    html: str,
    post: Optional[Callable] = None,
    sleep: Optional[Callable] = None,
) -> None:
    post = post or requests.post
    sleep = sleep or time.sleep  # injectable so the suite does not really wait
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    last = ""
    for attempt in range(_RETRIES):
        try:
            response = post(
                url,
                json={"chat_id": chat_id, "text": html, "parse_mode": "HTML"},
                timeout=_TIMEOUT,
            )
            if getattr(response, "status_code", 0) == 200:
                return
            last = f"status {getattr(response, 'status_code', '?')}"
        except Exception as exc:
            last = redact(str(exc), token)
        if attempt < _RETRIES - 1:
            sleep(2 ** attempt)
    raise DeliveryError(redact(f"Telegram delivery failed after {_RETRIES} attempts: {last}", token))
