#!/usr/bin/env python3
"""Network layer: fetch reviews from App Store Connect and Google Play."""
import json
import time
from datetime import datetime, timezone

import jwt
import requests
from cryptography.hazmat.primitives.serialization import load_pem_private_key


def _mask(value):
    """Emit GitHub Actions mask command for a secret value."""
    for line in str(value).splitlines():
        if line.strip():
            print(f"::add-mask::{line}")


def _asc_jwt(key_id, issuer_id, private_key_pem):
    """Mint an App Store Connect JWT (ES256, 20-minute expiry)."""
    now = int(time.time())
    payload = {"iss": issuer_id, "iat": now, "exp": now + 1200, "aud": "appstoreconnect-v1"}
    key = load_pem_private_key(private_key_pem.encode(), password=None)
    return jwt.encode(payload, key, algorithm="ES256", headers={"kid": key_id})


def fetch_appstore_reviews(apple_id, since_dt, key_id, issuer_id, private_key_pem):
    """Fetch App Store reviews newer than since_dt. Returns list of review dicts."""
    _mask(key_id)
    _mask(issuer_id)
    _mask(private_key_pem)
    token = _asc_jwt(key_id, issuer_id, private_key_pem)
    _mask(token)

    reviews = []
    url = (f"https://api.appstoreconnect.apple.com/v1/apps/{apple_id}"
           f"/customerReviews?sort=-createdDate&limit=200")

    while url:
        try:
            resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
            if resp.status_code != 200:
                print(f"ASC API {resp.status_code} for {apple_id}")
                break

            result = resp.json()
            done = False
            for review in result.get("data", []):
                attrs = review.get("attributes", {})
                created = datetime.fromisoformat(attrs["createdDate"].replace("Z", "+00:00"))
                if created <= since_dt:
                    done = True
                    break
                reviews.append({
                    "store": "appstore",
                    "rating": attrs.get("rating", 0),
                    "title": attrs.get("title", ""),
                    "body": attrs.get("body", ""),
                    "author": attrs.get("reviewerNickname", "Unknown"),
                    "version": "",
                    "date": created.isoformat(),
                    "country": attrs.get("territory", ""),
                    "device": "",
                    "os_version": "",
                    "is_non_english": False,
                })

            if done:
                break
            url = (result.get("links") or {}).get("next")
        except Exception:
            print(f"ASC API fetch failed for {apple_id}")
            break

    return reviews


def fetch_playstore_reviews(package_name, since_dt, sa_info):
    """Fetch Play Store reviews newer than since_dt. Returns list of review dicts."""
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request as AuthRequest

        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/androidpublisher"],
        )
        creds.refresh(AuthRequest())
        _mask(creds.token)
    except Exception:
        print(f"Play Store auth failed for {package_name}")
        return []

    since_unix = int(since_dt.timestamp())
    url = (f"https://androidpublisher.googleapis.com/androidpublisher/v3"
           f"/applications/{requests.utils.quote(package_name, safe='')}/reviews")
    reviews = []

    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
        if resp.status_code != 200:
            print(f"Play Store API {resp.status_code} for {package_name}")
            return reviews

        result = resp.json()
        for review in result.get("reviews", []):
            comments = review.get("comments", [])
            if not comments:
                continue
            comment = comments[0].get("userComment", {})
            last_mod = comment.get("lastModified", {})
            seconds = int(last_mod.get("seconds", 0)) if last_mod else 0
            if seconds <= since_unix:
                continue

            ts = datetime.fromtimestamp(seconds, tz=timezone.utc)
            text = comment.get("text", "")
            rating = comment.get("starRating", 0)
            device = (comment.get("deviceMetadata") or {}).get("productName", "")
            os_ver = comment.get("androidOsVersion", "")
            app_ver = comment.get("appVersionName", "")
            app_code = comment.get("appVersionCode", "")
            version = ""
            if app_ver:
                version = f"v{app_ver}"
                if app_code:
                    version += f"+{app_code}"

            reviews.append({
                "store": "playstore",
                "rating": rating,
                "title": "",
                "body": text,
                "author": review.get("authorName", "A Google user"),
                "version": version,
                "date": ts.isoformat(),
                "country": "",
                "device": device,
                "os_version": f"Android {os_ver}" if os_ver else "",
                "is_non_english": False,
            })
    except Exception:
        print(f"Play Store fetch failed for {package_name}")

    return reviews
