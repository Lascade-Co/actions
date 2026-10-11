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
from decimal import Decimal

import adspend_changes
from adspend_commentary import commentary
from adspend_fetch import fetch_apple, fetch_google, fetch_meta
from adspend_model import WINDOW_DAYS, build_model
from adspend_render_email import render
from pnl_fx import build_rate_table
from pnl_money import Unavailable


def _mask(value) -> None:
    """Mask a secret in the public log. Every line separately (a log line can only ever show
    one), plus the whole value encoded the way the runner decodes command data."""
    s = str(value)
    enc = s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::add-mask::{enc}")
    for line in s.splitlines():
        if line.strip() and line != s:
            print(f"::add-mask::{line.strip()}")


def load_creds(env=os.environ) -> dict:
    ads = json.loads(base64.b64decode(env["ADS_CREDENTIALS_JSON_B64"]).decode("utf-8"))
    if env.get("GITHUB_ACTIONS") != "true":
        return ads  # locally the mask lines would echo the secrets to the terminal
    # A group that is missing, null or not an object must never crash masking: that would
    # stop the whole email, and the apple block in particular is optional.
    groups = {g: ads.get(g) if isinstance(ads.get(g), dict) else {} for g in ("meta", "google", "apple", "changes")}
    for group, cfg in groups.items():
        for key in ("token", "client_secret", "refresh_token", "dev_token", "client_id", "key_id",
                    "password", "key", "email"):
            value = cfg.get(key)
            if value:
                _mask(value)
    # A multi-line value needs one mask per key line (the PEM armor is not secret).
    for line in str(groups["apple"].get("private_key", "")).splitlines():
        if line.strip() and not line.startswith("-----"):
            print(f"::add-mask::{line.strip()}")
    return ads


def reconcile(rows, table, ad_date, ads, meta_fn, google_fn) -> list:
    """Compare per-channel USD totals for ad_date with marketing-net's fetchers (same
    accounts, same FX table). Public log: prints match/MISMATCH only, never a figure."""
    out = []
    for name, fn, cfg in (("Meta", meta_fn, ads["meta"]), ("Google", google_fn, ads["google"])):
        data = rows[name]
        theirs = fn(cfg, ad_date, table)
        if isinstance(data, Unavailable) or isinstance(theirs, Unavailable):
            out.append(f"reconcile {name}: skipped (a side is unavailable)")
            continue
        mine = sum((table.to_usd(r.spend, r.currency) for r in data if r.day == ad_date), Decimal("0"))
        diff = abs(mine - theirs.get(ad_date, Decimal("0")))
        out.append(f"reconcile {name}: " + ("match" if diff < Decimal("0.01") else
                   f"MISMATCH ({'<1%' if theirs.get(ad_date) and diff / theirs[ad_date] < Decimal('0.01') else '>=1%'})"))
    return out


def status_line(name, data) -> str:
    if isinstance(data, Unavailable):
        return f"{name}: unavailable ({data.reason})"
    tzs = sorted({r.timezone for r in data})
    cur = sorted({r.currency for r in data})
    inst = "n/a" if not data else ("ok" if all(r.installs is not None for r in data) else "unavailable")
    return f"{name}: {len(data)} rows, tz={tzs}, currency={cur}, installs={inst}"


def load_changes(ads, model, ad_date, apps, fetch=None):
    """-> None when the change log is not configured, else the render's `changes` dict.
    Public log: counts or a one-word reason only, never a campaign, a why or an author."""
    cfg = adspend_changes.load(ads)
    if cfg is None:
        print("changes: not configured")
        return None
    tz = next(iter(model["channels"]["Meta"]["timezones"] or model["channels"]["Google"]["timezones"]),
              adspend_changes.DEFAULT_TZ)
    got = (fetch or adspend_changes.fetch)(cfg, ad_date, tz)
    if isinstance(got, Unavailable):
        print(f"changes: unavailable ({got.reason})")
        return {"ok": False, "by_line": {}, "orphans": []}
    by_line, orphans = adspend_changes.place(got, model, apps)
    print(f"changes: ok, {sum(map(len, by_line.values()))} placed, {len(orphans)} orphans")
    return {"ok": True, "by_line": by_line, "orphans": orphans}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ad-date", required=True)
    ap.add_argument("--out", default="out")
    ap.add_argument("--scratch", default="scratch")
    ap.add_argument("--variants", default="A")
    ap.add_argument("--apps", default="data/adspend_apps.json")
    ap.add_argument("--break-commentary", action="store_true")
    ap.add_argument("--reconcile", action="store_true")
    a = ap.parse_args(argv)

    ad_date = date.fromisoformat(a.ad_date)
    start = ad_date - timedelta(days=WINDOW_DAYS)
    os.makedirs(a.out, exist_ok=True)
    os.makedirs(a.scratch, exist_ok=True)
    with open(a.apps) as fh:
        apps = json.load(fh)

    ads = load_creds()
    rows = {"Meta": fetch_meta(ads["meta"], start, ad_date),
            "Google": fetch_google(ads["google"], start, ad_date),
            "Apple Search Ads": fetch_apple(ads.get("apple"), start, ad_date)}
    for name, data in rows.items():
        print(status_line(name, data))
    if all(isinstance(v, Unavailable) for v in rows.values()):
        print("all channels unavailable; nothing to send", file=sys.stderr)
        return 1

    table = build_rate_table([], ad_date)
    if a.reconcile:
        from pnl_googleads import fetch_google_ads_daily
        from pnl_metaads import fetch_meta_ads_daily
        for line in reconcile(rows, table, ad_date, ads, fetch_meta_ads_daily, fetch_google_ads_daily):
            print(line)
    model = build_model(rows, table, ad_date, datetime.now(timezone.utc), apps)
    changes = load_changes(ads, model, ad_date, apps)

    def broken(*_a, **_k):
        raise RuntimeError("commentary disabled for test")

    for variant in [v.strip() for v in a.variants.split(",") if v.strip()]:
        sentences = commentary(model, variant, a.scratch, run=broken, cache=False) if a.break_commentary \
            else commentary(model, variant, a.scratch)
        subject, _pre, html, text = render(model, variant, sentences, apps, changes)
        for ext, body in (("html", html), ("txt", text), ("subject", subject)):
            with open(os.path.join(a.out, f"email-{variant}.{ext}"), "w") as fh:
                fh.write(body)
        print(f"variant {variant}: wrote email, commentary={'yes' if sentences else 'no'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
