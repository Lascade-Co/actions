#!/usr/bin/env python3
"""Orchestrator for the daily survey digest. Fetch, model, render, send.

  python3 survey_main.py --report-date 2026-10-20 [--dry-run] [--to angel@…]

Public repo — never log response text, only counts and statuses.
"""
import argparse
import base64
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from functools import partial

from survey_fetch import fetch_sheet_tab, translate_text, FetchError
from survey_model import build_model
from survey_render import render

import requests


def _mask(value):
    for line in str(value).splitlines():
        if line.strip():
            print(f"::add-mask::{line}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--config", default="survey_config.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--to", default="")
    a = ap.parse_args()

    google_sa_b64 = os.environ.get("GOOGLE_SA_KEY", "")
    resend_key = os.environ.get("RESEND_API_KEY", "")

    if google_sa_b64:
        _mask(google_sa_b64)
    if resend_key:
        _mask(resend_key)

    if not google_sa_b64:
        sys.exit("GOOGLE_SA_KEY not set")

    try:
        sa_info = json.loads(base64.b64decode(google_sa_b64))
    except Exception as e:
        sys.exit(f"failed to decode GOOGLE_SA_KEY: {e}")

    with open(a.config) as f:
        config = json.load(f)

    now_utc = datetime.now(timezone.utc)
    cutoff_dt = now_utc - timedelta(hours=24)

    sheet_id = config["sheet_id"]
    fetched_tabs = {}
    for survey in config["surveys"]:
        tab = survey["sheet_name"]
        print(f"fetching tab: {tab}")
        try:
            rows = fetch_sheet_tab(sa_info, sheet_id, tab)
            fetched_tabs[tab] = rows
            print(f"  rows: {len(rows)}")
        except FetchError as e:
            print(f"  failed: {e}")

    translate_fn = partial(translate_text, sa_info)
    model = build_model(config, fetched_tabs, cutoff_dt, now_utc, translate_fn)
    subject, html = render(model)

    print(f"total new responses: {model['total']}")

    if a.dry_run:
        print(f"subject: {subject}")
        print(f"html length: {len(html)}")
        with open("survey-email.html", "w") as f:
            f.write(html)
        print("wrote survey-email.html")
        return

    if model["total"] == 0:
        print("no new responses, skipping email")
        return

    if not resend_key:
        sys.exit("RESEND_API_KEY not set")

    is_test = bool(a.to)
    if is_test:
        recipients = [a.to]
        prefix = "[TEST] "
        run_id = os.environ.get("GITHUB_RUN_ID", "local")
        run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        idem_key = f"survey-digest-test-{run_id}-{run_attempt}"
    else:
        recipients = ["angelrose@lascade.com", "cherian@lascade.com"]
        prefix = ""
        idem_key = f"survey-digest-{a.report_date}"

    payload = {
        "from": "Lascade Surveys <noreply@metrics.lascade.com>",
        "to": recipients,
        "subject": f"{prefix}{subject}",
        "html": html,
    }

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {resend_key}",
            "Idempotency-Key": idem_key,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    print(f"resend: {resp.status_code}")
    if resp.status_code not in (200, 201):
        sys.exit(f"resend failed: {resp.status_code}")
    print("sent")


if __name__ == "__main__":
    main()
