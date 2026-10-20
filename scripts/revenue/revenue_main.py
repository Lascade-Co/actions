#!/usr/bin/env python3
"""Orchestrator for the daily revenue digest. Fetch, model, render, send.

  python3 revenue_main.py --report-date 2026-10-20 [--dry-run] [--to angel@…]

Public repo — never log amounts, charge IDs, or response bodies.
"""
import argparse
import json
import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from revenue_fetch import fetch_charges, fetch_refunds
from revenue_model import build_model
from revenue_render import render


def _mask(value):
    for line in str(value).splitlines():
        if line.strip():
            print(f"::add-mask::{line}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--to", default="")
    a = ap.parse_args()

    stripe_key = os.environ.get("STRIPE_API_KEY", "")
    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not stripe_key:
        sys.exit("STRIPE_API_KEY not set")
    _mask(stripe_key)
    if gmail_password:
        _mask(gmail_password)

    report_date = datetime.strptime(a.report_date, "%Y-%m-%d").date()
    now_utc = datetime.now(timezone.utc)

    cutoff_24h = now_utc - timedelta(hours=24)
    since_unix = int(cutoff_24h.timestamp())

    month_start = datetime(report_date.year, report_date.month, 1, tzinfo=timezone.utc)
    month_since_unix = int(month_start.timestamp())

    print(f"fetching charges since {cutoff_24h.isoformat()}")
    today_charges = fetch_charges(stripe_key, since_unix)
    print(f"today charges: {len(today_charges)}")
    today_refunds = fetch_refunds(stripe_key, since_unix)
    print(f"today refunds: {len(today_refunds)}")

    month_charges = fetch_charges(stripe_key, month_since_unix)
    print(f"month charges: {len(month_charges)}")
    month_refunds = fetch_refunds(stripe_key, month_since_unix)
    print(f"month refunds: {len(month_refunds)}")

    model = build_model(today_charges, today_refunds, month_charges, month_refunds,
                        now_utc, cutoff_24h)
    subject, html = render(model)

    if a.dry_run:
        print(f"subject: {subject}")
        print(f"html length: {len(html)}")
        with open("revenue-email.html", "w") as f:
            f.write(html)
        print("wrote revenue-email.html")
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
    msg["From"] = f"Lascade Revenue <{gmail_user}>"
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
