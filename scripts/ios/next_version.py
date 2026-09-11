#!/usr/bin/env python3
"""
Print the marketing version a TestFlight build should use.

All TestFlight builds for one release candidate belong in the same marketing
version train. This asks App Store Connect for the latest iOS TestFlight train
and reuses it; CURRENT_PROJECT_VERSION, set by the workflow, distinguishes the
individual builds.

The answer is meant to be passed to xcodebuild as MARKETING_VERSION=... . The
repository is never modified: a PR pipeline must not push commits to
contributors' branches, and doing so on a pull_request trigger risks a
re-trigger loop.

Required environment:
    APPSTORE_API_KEY_ID        – App Store Connect API key id
    APPSTORE_ISSUER_ID         – issuer id for that key
    APPSTORE_API_PRIVATE_KEY   – the .p8 private key, PEM contents
    IOS_BUNDLE_ID              – bundle id of the app to look up
    CURRENT_VERSION            – MARKETING_VERSION as it stands in the project

Behaviour:
    Returns the highest iOS TestFlight marketing version without incrementing
    it. When CURRENT_VERSION is higher, returns CURRENT_VERSION so the release
    workflow's post-release bump can open the next train. The TestFlight
    workflow never advances the marketing version itself.

    On any failure — no credentials, app not found, API error — it warns on
    stderr and falls back to CURRENT_VERSION rather than failing. A version
    lookup should not be able to break a build; a genuinely unusable version is
    reported by the upload step with a far clearer message than this script
    could produce.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from base64 import urlsafe_b64encode

ASC = "https://api.appstoreconnect.apple.com"


def warn(msg):
    print(f"next_version: {msg}", file=sys.stderr)


def b64(data: bytes) -> str:
    return urlsafe_b64encode(data).rstrip(b"=").decode()


def der_to_jose(der: bytes) -> bytes:
    """Convert an OpenSSL DER ECDSA signature to the raw r||s JWS wants."""
    if not der or der[0] != 0x30:
        raise ValueError("not a DER SEQUENCE")
    idx = 2 if der[1] < 0x80 else 2 + (der[1] & 0x7F)

    def read_int(i):
        if der[i] != 0x02:
            raise ValueError("expected DER INTEGER")
        length = der[i + 1]
        return der[i + 2 : i + 2 + length].lstrip(b"\x00"), i + 2 + length

    r, idx = read_int(idx)
    s, _ = read_int(idx)
    return r.rjust(32, b"\x00") + s.rjust(32, b"\x00")


def make_token(key_id: str, issuer_id: str, private_key: str) -> str:
    header = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    now = int(time.time())
    payload = {
        "iss": issuer_id,
        "iat": now,
        "exp": now + 900,
        "aud": "appstoreconnect-v1",
    }
    signing_input = f"{b64(json.dumps(header).encode())}.{b64(json.dumps(payload).encode())}"

    with tempfile.NamedTemporaryFile("w", suffix=".p8", delete=False) as f:
        f.write(private_key if private_key.endswith("\n") else private_key + "\n")
        key_path = f.name
    try:
        der = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", key_path],
            input=signing_input.encode(),
            capture_output=True,
            check=True,
        ).stdout
    finally:
        os.unlink(key_path)

    return f"{signing_input}.{b64(der_to_jose(der))}"


def get(path: str, token: str):
    req = urllib.request.Request(
        f"{ASC}{path}", headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def parse(version: str):
    """'3.9.3' -> (3, 9, 3). Unparseable components sort as 0."""
    parts = (version.strip().split(".") + ["0", "0"])[:3]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            out.append(0)
    return tuple(out)


def main():
    current = os.environ.get("CURRENT_VERSION", "").strip()
    if not current:
        warn("CURRENT_VERSION is not set; nothing to fall back to")
        sys.exit(1)

    key_id = os.environ.get("APPSTORE_API_KEY_ID", "").strip()
    issuer = os.environ.get("APPSTORE_ISSUER_ID", "").strip()
    key = os.environ.get("APPSTORE_API_PRIVATE_KEY", "")
    bundle = os.environ.get("IOS_BUNDLE_ID", "").strip()

    if not all([key_id, issuer, key.strip(), bundle]):
        warn("App Store Connect credentials or bundle id missing; using CURRENT_VERSION")
        print(current)
        return

    try:
        token = make_token(key_id, issuer, key)

        apps = get(f"/v1/apps?filter[bundleId]={bundle}&limit=1", token)
        if not apps.get("data"):
            warn(f"no app found for bundle id {bundle}; using CURRENT_VERSION")
            print(current)
            return
        app_id = apps["data"][0]["id"]

        # Prerelease versions are TestFlight trains. Restrict this lookup to iOS
        # so another platform attached to the app cannot select our version.
        pre = get(
            f"/v1/preReleaseVersions?filter[app]={app_id}"
            f"&filter[platform]=IOS&sort=-version&limit=200"
            f"&fields[preReleaseVersions]=version",
            token,
        )
        testflight_versions = [
            v["attributes"]["version"]
            for v in pre.get("data", [])
            if v.get("attributes", {}).get("version")
        ]
        if not testflight_versions:
            warn("no TestFlight marketing versions; using CURRENT_VERSION")
            print(current)
            return

        latest_testflight = max(testflight_versions, key=parse)
        if parse(current) > parse(latest_testflight):
            warn(
                f"project marketing version {current} is newer than TestFlight "
                f"{latest_testflight}; using project version"
            )
            print(current)
            return

        warn(f"latest TestFlight marketing version is {latest_testflight}; reusing it")
        print(latest_testflight)

    except (urllib.error.URLError, subprocess.CalledProcessError, ValueError, KeyError) as e:
        warn(f"lookup failed ({type(e).__name__}: {e}); using CURRENT_VERSION")
        print(current)


if __name__ == "__main__":
    main()
