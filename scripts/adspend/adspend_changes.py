"""Campaign changes logged by the ads team, placed under the ledger line they explain.

The log is a Supabase table (`changes`) behind row-level security: the publishable
key alone reads nothing, so we sign in with email + password first. Credentials
ride in ADS_CREDENTIALS_JSON_B64 under "changes": {"url", "key", "email",
"password", "table"?}. A missing block means "not configured" and the email is
unchanged.

The change log is decoration. Every failure returns ``Unavailable`` with a
one-word reason and the email still sends. Public repo: nothing here may put a
URL, a response body, a campaign name, a "why" or an author into a reason.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

import requests

from pnl_money import Unavailable
from adspend_model import app_of, os_of

DEFAULT_TZ = "Asia/Dubai"
TIMEOUT = 20                    # per request: socket inactivity only
DEADLINE = 45                   # whole fetch: a slow-drip response must not hold up the send
PAGE = 1000                     # Supabase's default max rows per request
CHANNEL = {"meta": "Meta", "google": "Google", "apple": "Apple Search Ads"}
_FIELDS = ("app", "platform", "kind", "entity", "before_value", "after_value", "why", "author_email")


@dataclass(frozen=True)
class Change:
    at: datetime                # aware, in the ad-account timezone
    channel: str                # "Meta" | "Google" | "Apple Search Ads" | as logged
    app: str                    # as logged; display only, never used for placement
    kind: str
    entity: str                 # campaign name as logged
    before: str
    after: str
    why: str
    author: str                 # "Jishnu", from the email's local part


class _Fail(Exception):
    """Carries one safe word."""


def load(ads: dict):
    cfg = ads.get("changes") if isinstance(ads, dict) else None
    if not isinstance(cfg, dict) or not all(cfg.get(k) for k in ("url", "key", "email", "password")):
        return None
    return cfg


def _request(method: str, url: str, **kw):
    try:
        response = requests.request(method, url, timeout=TIMEOUT, **kw)
    except requests.Timeout:
        raise _Fail("timeout") from None
    except Exception:
        raise _Fail("network") from None
    if response.status_code in (400, 401, 403):
        raise _Fail("auth")
    if response.status_code >= 400:
        raise _Fail("http")
    try:
        return response.json()
    except Exception:
        raise _Fail("shape") from None


def _post(url, headers, body):
    return _request("POST", url, headers=headers, json=body)


def _get(url, headers, params):
    return _request("GET", url, headers=headers, params=params)


def window(ad_date: date, tz: str):
    """UTC bounds of [ad_date - 1, ad_date + 1) in the ad-account timezone."""
    zone = ZoneInfo(tz)
    start = datetime.combine(ad_date - timedelta(days=1), time(), zone)
    end = datetime.combine(ad_date + timedelta(days=1), time(), zone)
    return start, end


def _parse_at(value, zone) -> datetime:
    s = str(value or "").replace("Z", "+00:00")
    # Python < 3.11 only accepts 3 or 6 fractional digits; Supabase sends 1-6.
    s = re.sub(r"\.(\d{1,6})\d*(?=[+-]\d\d:\d\d$|$)", lambda m: "." + m.group(1).ljust(6, "0"), s)
    try:
        at = datetime.fromisoformat(s)
    except ValueError:
        raise _Fail("shape") from None
    if at.tzinfo is None:
        raise _Fail("shape")
    return at.astimezone(zone)


def _text(row, key) -> str:
    v = row.get(key)
    return "" if v is None else str(v).strip()


def fetch(cfg: dict, ad_date: date, tz: str = DEFAULT_TZ, post: Callable = _post, get: Callable = _get,
          deadline: float = DEADLINE):
    """-> list[Change], newest first, or Unavailable(one word). Never takes longer than
    `deadline` seconds: the worker is a daemon thread, so a stuck request can't keep the
    process (and the send) alive."""
    box = []

    def work():
        try:
            box.append(_fetch(cfg, ad_date, tz, post, get))
        except Exception:
            box.append(Unavailable("error"))
    worker = threading.Thread(target=work, daemon=True)
    worker.start()
    worker.join(deadline)
    return box[0] if box else Unavailable("timeout")


def _fetch(cfg, ad_date, tz, post, get):
    try:
        zone = ZoneInfo(tz)
    except Exception:
        zone, tz = ZoneInfo(DEFAULT_TZ), DEFAULT_TZ
    base = str(cfg["url"]).rstrip("/")
    try:
        token = post(f"{base}/auth/v1/token?grant_type=password", {"apikey": cfg["key"]},
                     {"email": cfg["email"], "password": cfg["password"]})
        token = token.get("access_token") if isinstance(token, dict) else None
        if not token:
            raise _Fail("auth")
        headers = {"apikey": cfg["key"], "Authorization": f"Bearer {token}"}
        start, end = window(ad_date, tz)
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        utc = ZoneInfo("UTC")
        rows = []
        while True:
            page = get(f"{base}/rest/v1/{cfg.get('table') or 'changes'}", headers, [
                ("select", "id,happened_at," + ",".join(_FIELDS)),
                ("happened_at", f"gte.{start.astimezone(utc).strftime(fmt)}"),
                ("happened_at", f"lt.{end.astimezone(utc).strftime(fmt)}"),
                ("order", "happened_at.desc,id.desc"),
                ("limit", str(PAGE)), ("offset", str(len(rows)))])
            if not isinstance(page, list) or not all(isinstance(r, dict) for r in page):
                raise _Fail("shape")
            if not page:            # page until empty: the server's row cap may be below PAGE
                break
            rows += page
        out = []
        for r in rows:
            platform = _text(r, "platform")
            out.append(Change(
                at=_parse_at(r.get("happened_at"), zone),
                channel=CHANNEL.get(platform.lower(), platform), app=_text(r, "app"),
                kind=_text(r, "kind"), entity=_text(r, "entity"),
                before=_text(r, "before_value"), after=_text(r, "after_value"),
                why=_text(r, "why"),
                author=_text(r, "author_email").split("@")[0].capitalize()))
        return out
    except _Fail as exc:
        return Unavailable(str(exc))
    except Exception:
        return Unavailable("error")


def line_key(change: Change, apps: dict):
    """The (project, channel, os) key build_model gives this campaign's spend."""
    os_ = "iOS" if change.channel == "Apple Search Ads" else os_of(change.entity)
    return (app_of(change.entity, apps), change.channel, os_)


def place(changes: list, model: dict, apps: dict):
    """-> (by_line {key: [Change]}, orphans [Change]); each list newest first."""
    keys = {(ln["project"], ln["channel"], ln["os"]) for p in model["projects"] for ln in p["lines"]}
    by_line, orphans = {}, []
    for c in sorted(changes, key=lambda c: c.at, reverse=True):
        k = line_key(c, apps)
        (by_line.setdefault(k, []) if k in keys else orphans).append(c)
    return by_line, orphans
