#!/usr/bin/env python3
"""Network layer: Google Sheets reads and Cloud Translation calls."""
import json
import sys
from urllib.parse import quote

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
_TRANSLATE_SCOPE = "https://www.googleapis.com/auth/cloud-translation"

_SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
_TRANSLATE_URL = "https://translation.googleapis.com/language/translate/v2"


class FetchError(Exception):
    pass


def list_sheet_tabs(sa_info, sheet_id):
    """Return a list of all tab names in the spreadsheet."""
    creds = _get_credentials(sa_info, [_SHEETS_SCOPE])
    url = f"{_SHEETS_BASE}/{sheet_id}?fields=sheets.properties.title"
    resp = requests.get(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
    if resp.status_code != 200:
        return []
    sheets = resp.json().get("sheets", [])
    return [s.get("properties", {}).get("title", "") for s in sheets]


def _get_credentials(sa_info, scopes):
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    creds.refresh(Request())
    return creds


def fetch_sheet_tab(sa_info, sheet_id, tab_name):
    """Return rows (list of lists) for *tab_name*, header row excluded."""
    creds = _get_credentials(sa_info, [_SHEETS_SCOPE])
    url = f"{_SHEETS_BASE}/{sheet_id}/values/{quote(tab_name, safe='')}?dateTimeRenderOption=SERIAL_NUMBER"
    resp = requests.get(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
    if resp.status_code != 200:
        err_body = resp.text[:200] if resp.text else "(empty)"
        print(f"sheets {resp.status_code} for {tab_name}: {err_body}", file=sys.stderr)
        raise FetchError(f"sheets-{resp.status_code}")
    values = resp.json().get("values", [])
    return values[1:] if len(values) > 1 else []


def translate_text(sa_info, text, target="en"):
    """Translate *text* to *target* via Cloud Translation v2. Returns original on failure."""
    if not text or len(text) < 3:
        return text
    try:
        creds = _get_credentials(sa_info, [_TRANSLATE_SCOPE])
        resp = requests.post(
            _TRANSLATE_URL,
            headers={"Authorization": f"Bearer {creds.token}"},
            json={"q": text, "target": target, "format": "text"},
            timeout=15,
        )
        if resp.status_code != 200:
            return text
        translations = resp.json().get("data", {}).get("translations", [])
        if translations:
            return translations[0].get("translatedText", text)
    except Exception:
        pass
    return text
