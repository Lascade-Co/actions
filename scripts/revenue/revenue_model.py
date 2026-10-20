#!/usr/bin/env python3
"""Pure data model for the revenue digest. No I/O, no clock."""
from collections import defaultdict


def format_money(amount):
    """Format a dollar amount with commas and two decimals: 1234.5 → '1,234.50'."""
    return f"{amount:,.2f}"


def _sum_cents(items):
    return sum(c["amount_cents"] for c in items)


def build_model(today_charges, today_refunds, month_charges, month_refunds, now_dt, cutoff_dt):
    """Build the template model dict from raw charge/refund lists.

    All monetary values are in dollars (float). Counts are ints.
    """
    today_total = _sum_cents(today_charges) / 100
    today_refund_total = _sum_cents(today_refunds) / 100

    month_total = _sum_cents(month_charges) / 100
    month_refund_total = _sum_cents(month_refunds) / 100

    # Group today's charges by dollar amount (credit pack)
    by_pack = defaultdict(lambda: {"count": 0, "total": 0.0})
    for c in today_charges:
        amt = c["amount_cents"] / 100
        key = f"{amt:.2f}"
        by_pack[key]["count"] += 1
        by_pack[key]["total"] += amt
    packs = [
        {"amount": key, "count": v["count"], "total": v["total"]}
        for key, v in sorted(by_pack.items(), key=lambda kv: float(kv[0]))
    ]

    # Date formatting
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]
    short_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    date_str = f"{days[now_dt.weekday()]}, {now_dt.day} {months[now_dt.month - 1]} {now_dt.year}"
    cutoff_hour = cutoff_dt.hour % 12 or 12
    cutoff_ampm = "AM" if cutoff_dt.hour < 12 else "PM"
    cutoff_str = f"{cutoff_dt.day} {short_months[cutoff_dt.month - 1]} {cutoff_hour}:{cutoff_dt.minute:02d} {cutoff_ampm}"

    month_label = f"1–{now_dt.day} {short_months[now_dt.month - 1]} {now_dt.year}"

    # Purchase Source Breakdown — commented out until metadata property is configured
    # TODO: Uncomment when purchase_source metadata key is set in Stripe
    # source_meta_key = "purchase_source"
    # by_source = defaultdict(lambda: {"count": 0, "total": 0.0})
    # for c in today_charges:
    #     src = c["metadata"].get(source_meta_key, "Unknown")
    #     by_source[src]["count"] += 1
    #     by_source[src]["total"] += c["amount_cents"] / 100
    # sources = [
    #     {"source": k, "count": v["count"], "total": v["total"]}
    #     for k, v in sorted(by_source.items(), key=lambda kv: -kv[1]["total"])
    # ]

    return {
        "today_total": today_total,
        "today_refund_total": today_refund_total,
        "today_count": len(today_charges),
        "today_refund_count": len(today_refunds),
        "packs": packs,
        "month_total": month_total,
        "month_refund_total": month_refund_total,
        "month_net": month_total - month_refund_total,
        "month_count": len(month_charges),
        "month_refund_count": len(month_refunds),
        "month_label": month_label,
        "date_str": date_str,
        "cutoff_str": cutoff_str,
        # "sources": sources,  # TODO: uncomment with Purchase Source Breakdown
    }
