#!/usr/bin/env python3
"""Orchestrator for the daily reviews digest. Fetch, model, render, send.

  python3 reviews_main.py --report-date 2026-10-20 --apps reviews_apps.json [--dry-run] [--to angel@…]

Public repo — never log review text or response bodies, only counts and statuses.
"""
import argparse
import base64
import json
import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from reviews_fetch import fetch_appstore_reviews, fetch_playstore_reviews
from reviews_model import build_model
from reviews_render import render


def _mask(value):
    for line in str(value).splitlines():
        if line.strip():
            print(f"::add-mask::{line}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--apps", required=True, help="Path to reviews_apps.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--to", default="")
    a = ap.parse_args()

    asc_key_id = os.environ.get("ASC_KEY_ID", "")
    asc_issuer_id = os.environ.get("ASC_ISSUER_ID", "")
    asc_private_key = os.environ.get("ASC_PRIVATE_KEY", "")
    google_sa_b64 = os.environ.get("GOOGLE_SA_KEY", "")
    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")

    if asc_key_id:
        _mask(asc_key_id)
    if asc_issuer_id:
        _mask(asc_issuer_id)
    if asc_private_key:
        _mask(asc_private_key)
    if google_sa_b64:
        _mask(google_sa_b64)
    if gmail_password:
        _mask(gmail_password)

    sa_info = None
    if google_sa_b64:
        try:
            sa_info = json.loads(base64.b64decode(google_sa_b64))
        except Exception as e:
            print(f"failed to decode GOOGLE_SA_KEY: {e}", file=sys.stderr)

    with open(a.apps) as f:
        apps_config = json.load(f)

    now_utc = datetime.now(timezone.utc)
    since_dt = now_utc - timedelta(hours=24)

    app_reviews = {}
    for app in apps_config:
        name = app["name"]
        print(f"fetching {name}...")
        appstore = []
        playstore = []

        if asc_key_id and asc_issuer_id and asc_private_key:
            appstore = fetch_appstore_reviews(
                app["apple_id"], since_dt, asc_key_id, asc_issuer_id, asc_private_key)
            print(f"  appstore: {len(appstore)} reviews")
        else:
            print("  appstore: skipped (no ASC credentials)")

        if sa_info and app.get("play_package"):
            playstore = fetch_playstore_reviews(app["play_package"], since_dt, sa_info)
            print(f"  playstore: {len(playstore)} reviews")
        else:
            print("  playstore: skipped (no SA credentials)")

        app_reviews[name] = {"appstore": appstore, "playstore": playstore}

    model = build_model(apps_config, app_reviews, since_dt, now_utc)
    subject, html = render(model)

    print(f"grand total: {model['grand_total']} reviews")

    if a.dry_run:
        print(f"subject: {subject}")
        print(f"html length: {len(html)}")
        with open("reviews-email.html", "w") as f:
            f.write(html)
        print("wrote reviews-email.html")
        return

    if model["grand_total"] == 0:
        print("no reviews, skipping email")
        return

    if not gmail_user or not gmail_password:
        sys.exit("GMAIL_USER / GMAIL_APP_PASSWORD not set")

    is_test = bool(a.to)
    if is_test:
        recipients = [a.to]
        prefix = "[TEST] "
    else:
        recipients = ["angelrose@lascade.com", "cherian@lascade.com"]
        prefix = ""

    msg = MIMEMultipart("alternative")
    msg["From"] = f"Lascade Reviews <{gmail_user}>"
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = f"{prefix}{subject}"
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, recipients, msg.as_string())
    print("sent")


if __name__ == "__main__":
    main()
