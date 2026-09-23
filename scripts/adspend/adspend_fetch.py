"""Campaign-level daily spend and installs from Meta and Google Ads.

This repo is PUBLIC and the run log is world-readable. Nothing here may put a
URL (the Meta token rides in its query string), a response body, a campaign
name or a spend figure into an exception message. Every failure is reduced to
``FetchError`` carrying the channel, an HTTP status and an API error code only.

Each channel returns ``list[Row]`` or ``Unavailable``. Any account failing makes
the whole channel unavailable: summing the accounts that answered would
understate spend, and a plausible shortfall is the error a reader cannot see.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

import requests

from pnl_money import Unavailable, to_decimal

META_VERSION = "v26.0"
GOOGLE_VERSION = "v25"
_META = f"https://graph.facebook.com/{META_VERSION}"
_GOOGLE = f"https://googleads.googleapis.com/{GOOGLE_VERSION}"
_TOKEN_URL = "https://oauth2.googleapis.com/token"
_MICROS = Decimal("1000000")
_TIMEOUT = 120


class FetchError(Exception):
    """Sanitized: channel, HTTP status and API error code only."""


@dataclass(frozen=True)
class Row:
    channel: str            # "Meta" | "Google"
    account_id: str
    timezone: str
    currency: str
    campaign_id: str
    campaign: str
    day: date
    spend: Decimal          # native currency
    installs: Optional[Decimal]   # None = unavailable, never 0


def _api_code(response) -> str:
    """The API's own error code, if it is short and plain. Never the body."""
    try:
        err = response.json().get("error", {})
        code = err.get("code") if isinstance(err, dict) else None
        if code is None and isinstance(err, dict):
            code = err.get("status")
        code = str(code) if code is not None else ""
        return code if code.replace("_", "").isalnum() and len(code) <= 40 else ""
    except Exception:
        return ""


def _request(channel: str, method: str, url: str, **kw) -> dict:
    try:
        response = requests.request(method, url, timeout=_TIMEOUT, **kw)
    except Exception as exc:
        raise FetchError(f"{channel} {type(exc).__name__}") from None
    if response.status_code >= 400:
        code = _api_code(response)
        raise FetchError(f"{channel} HTTP {response.status_code}" + (f" code {code}" if code else ""))
    try:
        return response.json()
    except Exception:
        raise FetchError(f"{channel} non-JSON response") from None


def _get_json(url: str, params: Optional[dict]) -> dict:
    return _request("Meta", "GET", url, params=params)


def _post_json(url: str, body: Optional[dict], headers: dict, data: Optional[dict] = None) -> dict:
    return _request("Google", "POST", url, json=body, data=data, headers=headers)


def _dec(value, default="0") -> Decimal:
    try:
        return to_decimal(value if value is not None else default)
    except (InvalidOperation, ValueError):
        raise FetchError("malformed number in response") from None


# ---------------------------------------------------------------- Meta

def fetch_meta(creds: dict, start: date, end: date, get: Callable = _get_json):
    accounts = [str(a).replace("act_", "") for a in creds.get("account_ids", [])]
    if not accounts:
        return Unavailable("Meta: no account ids configured")
    token = creds["token"]
    time_range = json.dumps({"since": start.isoformat(), "until": end.isoformat()})
    rows = []
    try:
        for account in accounts:
            info = get(f"{_META}/act_{account}",
                       {"fields": "timezone_name,currency", "access_token": token})
            tz, currency = info["timezone_name"], info["currency"]
            url = f"{_META}/act_{account}/insights"
            params = {
                "fields": "campaign_id,campaign_name,spend,actions,date_start",
                "level": "campaign", "time_increment": 1, "limit": 500,
                "time_range": time_range, "access_token": token,
            }
            while url:
                payload = get(url, params)
                for r in payload.get("data", []):
                    # Missing `actions` is 0 installs only because this request succeeded.
                    installs = sum(
                        (_dec(a.get("value")) for a in r.get("actions") or []
                         if a.get("action_type") == "mobile_app_install"),
                        Decimal("0"))
                    rows.append(Row("Meta", account, tz, currency, str(r["campaign_id"]),
                                    r.get("campaign_name", ""), date.fromisoformat(r["date_start"]),
                                    _dec(r.get("spend")), installs))
                url = payload.get("paging", {}).get("next")
                params = None
    except FetchError as exc:
        return Unavailable(str(exc))
    except (KeyError, TypeError, ValueError, AttributeError):
        return Unavailable("Meta malformed response")
    return rows


# -------------------------------------------------------------- Google

CHILDREN_QUERY = """
    SELECT customer_client.id, customer_client.currency_code
    FROM customer_client
    WHERE customer_client.manager = FALSE AND customer_client.status = 'ENABLED'
"""

SPEND_QUERY = """
    SELECT customer.id, customer.time_zone, customer.currency_code,
           campaign.id, campaign.name, segments.date, metrics.cost_micros
    FROM campaign
    WHERE segments.date BETWEEN '{start}' AND '{end}'
"""

# Conversions by action CATEGORY. No cost column here: segmenting by conversion
# action repeats cost across rows and would double-count spend.
INSTALL_QUERY = """
    SELECT customer.id, customer.time_zone, customer.currency_code,
           campaign.id, campaign.name, segments.date,
           segments.conversion_action_category, metrics.conversions
    FROM campaign
    WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


def _google_token(creds: dict, post: Callable) -> str:
    payload = post(_TOKEN_URL, None, {}, data={
        "client_id": creds["client_id"], "client_secret": creds["client_secret"],
        "refresh_token": creds["refresh_token"], "grant_type": "refresh_token"})
    return payload["access_token"]


def _search(post: Callable, token: str, creds: dict, customer_id: str, query: str) -> list:
    payload = post(f"{_GOOGLE}/customers/{customer_id}/googleAds:searchStream",
                   {"query": query},
                   {"Authorization": f"Bearer {token}",
                    "developer-token": creds["dev_token"],
                    "login-customer-id": str(creds["login_customer_id"])})
    out = []
    for chunk in payload if isinstance(payload, list) else [payload]:
        out.extend(chunk.get("results", []))
    return out


def _g_key(r: dict) -> tuple:
    seg = r.get("segments", {})
    return (str(r["customer"]["id"]), str(r["campaign"]["id"]), seg["date"])


def fetch_google(creds: dict, start: date, end: date, post: Callable = _post_json):
    skip = {str(s) for s in creds.get("skip_customer_ids", [])}
    window = {"start": start.isoformat(), "end": end.isoformat()}
    try:
        token = _google_token(creds, post)
        children = _search(post, token, creds, str(creds["login_customer_id"]), CHILDREN_QUERY)
        accounts = [str(c["customerClient"]["id"]) for c in children
                    if str(c.get("customerClient", {}).get("id", "")) not in skip
                    and c.get("customerClient", {}).get("id")]
        if not accounts:
            return Unavailable("Google: the MCC returned no eligible child accounts")

        # Spend: any failure here is fatal for the channel.
        spend_rows = []
        for cid in accounts:
            spend_rows.extend(_search(post, token, creds, cid, SPEND_QUERY.format(**window)))

        # Installs: a failure here degrades to installs=None (unavailable), never 0.
        installs: Optional[dict] = {}
        try:
            for cid in accounts:
                for r in _search(post, token, creds, cid, INSTALL_QUERY.format(**window)):
                    if r.get("segments", {}).get("conversionActionCategory") != "DOWNLOAD":
                        continue
                    key = _g_key(r)
                    installs.setdefault(key, [r, Decimal("0")])[1] += _dec(r["metrics"].get("conversions"))
        except (FetchError, KeyError, TypeError):
            installs = None

        merged: dict = {}
        for r in spend_rows:
            merged[_g_key(r)] = [r, _dec(r["metrics"].get("costMicros")) / _MICROS]
        if installs:  # conversion-only campaigns still get a row
            for key, (r, _) in installs.items():
                merged.setdefault(key, [r, Decimal("0")])

        rows = []
        for key, (r, spend) in merged.items():
            cust, camp = r["customer"], r["campaign"]
            inst = None if installs is None else installs.get(key, [None, Decimal("0")])[1]
            rows.append(Row("Google", key[0], cust["timeZone"], cust["currencyCode"], key[1],
                            camp.get("name", ""), date.fromisoformat(key[2]), spend, inst))
        return rows
    except FetchError as exc:
        return Unavailable(str(exc))
    except (KeyError, TypeError, ValueError, AttributeError):
        return Unavailable("Google malformed response")
