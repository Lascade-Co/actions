#!/usr/bin/env python3
"""Build the daily ad-spend emails (variants A/B) as files. Sending is the workflow's job.

Public repo, world-readable log: this prints counts, statuses and timezones only. Never a
spend figure, campaign name, commentary or credential. Decoded secrets are masked first.

  python3 adspend_main.py --ad-date 2026-09-22 --out out/ [--variants A,B] [--break-commentary]

Writes out/email-{V}.html, .txt, .subject. Exit 0 even when a channel is unavailable (the
email carries a banner); non-zero only when nothing can be built.
"""
import argparse
import base64
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

from adspend_commentary import commentary
from adspend_fetch import fetch_google, fetch_meta
from adspend_model import WINDOW_DAYS, build_model
from adspend_render_email import render
from pnl_fx import build_rate_table
from pnl_money import Unavailable


def load_creds(env=os.environ) -> dict:
    ads = json.loads(base64.b64decode(env["ADS_CREDENTIALS_JSON_B64"]).decode("utf-8"))
    for group in ("meta", "google"):
        for key in ("token", "client_secret", "refresh_token", "dev_token"):
            value = ads.get(group, {}).get(key)
            if value:
                print(f"::add-mask::{value}")
    return ads


def status_line(name, data) -> str:
    if isinstance(data, Unavailable):
        return f"{name}: unavailable ({data.reason})"
    tzs = sorted({r.timezone for r in data})
    cur = sorted({r.currency for r in data})
    inst = "n/a" if not data else ("ok" if all(r.installs is not None for r in data) else "unavailable")
    return f"{name}: {len(data)} rows, tz={tzs}, currency={cur}, installs={inst}"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ad-date", required=True)
    ap.add_argument("--out", default="out")
    ap.add_argument("--scratch", default="scratch")
    ap.add_argument("--variants", default="A")
    ap.add_argument("--apps", default="data/adspend_apps.json")
    ap.add_argument("--break-commentary", action="store_true")
    a = ap.parse_args(argv)

    ad_date = date.fromisoformat(a.ad_date)
    start = ad_date - timedelta(days=WINDOW_DAYS)
    os.makedirs(a.out, exist_ok=True)
    os.makedirs(a.scratch, exist_ok=True)
    with open(a.apps) as fh:
        apps = json.load(fh)

    ads = load_creds()
    rows = {"Meta": fetch_meta(ads["meta"], start, ad_date),
            "Google": fetch_google(ads["google"], start, ad_date)}
    for name, data in rows.items():
        print(status_line(name, data))
    if all(isinstance(v, Unavailable) for v in rows.values()):
        print("both channels unavailable; nothing to send", file=sys.stderr)
        return 1

    table = build_rate_table([], ad_date)
    model = build_model(rows, table, ad_date, datetime.now(timezone.utc), apps)

    def broken(*_a, **_k):
        raise RuntimeError("commentary disabled for test")

    for variant in [v.strip() for v in a.variants.split(",") if v.strip()]:
        sentences = commentary(model, variant, a.scratch, run=broken) if a.break_commentary \
            else commentary(model, variant, a.scratch)
        subject, _pre, html, text = render(model, variant, sentences, apps)
        for ext, body in (("html", html), ("txt", text), ("subject", subject)):
            with open(os.path.join(a.out, f"email-{variant}.{ext}"), "w") as fh:
                fh.write(body)
        print(f"variant {variant}: wrote email, commentary={'yes' if sentences else 'no'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
