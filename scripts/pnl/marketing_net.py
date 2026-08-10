#!/usr/bin/env python3
"""Marketing net — month-to-date app revenue minus month-to-date marketing spend.

Composes five independent sources and posts one figure to Telegram. Nothing is
persisted: the PNL app deliberately refuses to book revenue for a month still in
progress, and this must never become a way around that.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from datetime import date, timezone, datetime

from pnl_appstore import DayCache, fetch_appstore, window_days
from pnl_fx import build_rate_table, rate_date
from pnl_googleads import fetch_google_ads
from pnl_metaads import fetch_meta_ads
from pnl_money import Amount, SourceValue, Unavailable
from pnl_playstore import fetch_playstore
from pnl_spend import fetch_head_spend
from pnl_image import render_png
from pnl_telegram import DeliveryError, render, send, send_photo

PNL_BASE_URL = "https://pnl.lascade.com"
# The normalized key, which contains a SPACE — PNL derives keys from card
# statement descriptors, so "AWS BILL" and "INFLUENCER MARKETING" are the shape.
# The underscored spelling 404s, which the endpoint reports rather than passing
# off as a quiet month.
HEAD = "INFLUENCER MARKETING"
APPSTORE_CACHE_DIR = ".appstore-cache"

REQUIRED = [
    "PNL_API_KEY",
    "APPSTORE_ISSUER_ID",
    "APPSTORE_KEY_ID",
    "APPSTORE_P8_B64",
    "APPSTORE_VENDOR_NUMBER",
    "PLAYSTORE_SA_JSON_B64",
    "PLAYSTORE_BUCKET",
    "ADS_CREDENTIALS_JSON_B64",
    "TELEGRAM_BOT_TOKEN",
    # Deliberately not TELEGRAM_CHAT_ID: that key exists in the same Infisical
    # project and belongs to the PNL app's own alerting. Falling back to it
    # would deliver to the wrong chat and succeed while doing so.
    "MARKETING_NET_CHAT_ID",
]

# No currency list is kept here. A fixed one is a standing silent-loss bug: the
# apps sell in ~56 currencies and the set moves with the markets they reach, so
# the day a new one appears the list is wrong and nothing says so. The table
# resolves whatever each report actually contains, at one rate date per ADR-0006.


class ConfigError(RuntimeError):
    pass


def _b64(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


def load_config(env: dict) -> dict:
    missing = [key for key in REQUIRED if not env.get(key)]
    if missing:
        raise ConfigError(f"Missing required configuration: {', '.join(missing)}")
    return {
        "pnl_api_key": env["PNL_API_KEY"],
        "appstore": {
            "issuer_id": env["APPSTORE_ISSUER_ID"],
            "key_id": env["APPSTORE_KEY_ID"],
            "p8": _b64(env["APPSTORE_P8_B64"]),
            "vendor_number": env["APPSTORE_VENDOR_NUMBER"],
        },
        "playstore": {
            "bucket": env["PLAYSTORE_BUCKET"],
            "credentials": json.loads(_b64(env["PLAYSTORE_SA_JSON_B64"])),
        },
        "ads": json.loads(_b64(env["ADS_CREDENTIALS_JSON_B64"])),
        "telegram_token": env["TELEGRAM_BOT_TOKEN"],
        "chat_id": env["MARKETING_NET_CHAT_ID"],
    }


def _call(source) -> SourceValue:
    try:
        return source()
    except Exception as exc:
        return Unavailable(f"{type(exc).__name__}: {exc}")


def build_report(config: dict, today: date, sources: dict, table=None) -> dict:
    revenue = {
        "App Store": _call(sources["appstore"]),
        "Play Store": _call(sources["playstore"]),
    }
    spend = {
        "Influencer": _call(sources["influencer"]),
        "Google Ads": _call(sources["google"]),
        "Meta Ads": _call(sources["meta"]),
    }
    warnings = [
        f"{label}: {value.reason}"
        for label, value in list(revenue.items()) + list(spend.items())
        if isinstance(value, Unavailable)
    ]
    # A currency nobody could price is reported even when every source still
    # produced a figure — the sources that met it withheld their whole total,
    # and the reader is owed the reason rather than a gap.
    unpriced = sorted(getattr(table, "missing", None) or ())
    if unpriced:
        warnings.append(f"No USD rate for: {', '.join(unpriced)}")
    days = window_days(today)
    return {
        "month_label": f"{today:%b} 1–{today.day}",
        "revenue": revenue,
        "spend": spend,
        "appstore_window_label": f"to {days[-1]:%b %-d}" if days else None,
        "warnings": warnings,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Post the marketing net to Telegram.")
    parser.add_argument("--dry-run", action="store_true", help="render but do not send")
    args = parser.parse_args(argv)

    config = load_config(os.environ)
    today = datetime.now(timezone.utc).date()
    # Seeded with nothing: every code is fetched the first time a report shows it.
    table = build_rate_table([], rate_date(today))

    sources = {
        "appstore": lambda: fetch_appstore(
            config["appstore"], today, table, cache=DayCache(APPSTORE_CACHE_DIR)
        ),
        "playstore": lambda: fetch_playstore(config["playstore"], today, table),
        "influencer": lambda: fetch_head_spend(PNL_BASE_URL, config["pnl_api_key"], HEAD),
        "google": lambda: fetch_google_ads(config["ads"]["google"], today, table),
        "meta": lambda: fetch_meta_ads(config["ads"]["meta"], today, table),
    }

    report = build_report(config, today, sources, table)
    html = render(report)
    with open("message.html", "w", encoding="utf-8") as handle:
        handle.write(html)

    # The card is the message; the text rendering survives only as a fallback,
    # because a missing font must not cost us the delivery entirely.
    png = None
    try:
        png = render_png(report)
        with open("message.png", "wb") as handle:
            handle.write(png)
    except Exception as exc:
        print(f"image rendering failed, falling back to text: {exc}", file=sys.stderr)

    if args.dry_run:
        print(html)
        print(f"[dry run] image: {'message.png' if png else 'unavailable'}")
        return 0

    try:
        if png:
            send_photo(config["telegram_token"], config["chat_id"], png)
        else:
            send(config["telegram_token"], config["chat_id"], html)
    except DeliveryError as exc:
        # The only red condition: no message means no surface carrying the
        # warnings, so the workflow status is the sole remaining signal.
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
