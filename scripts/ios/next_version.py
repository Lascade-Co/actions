#!/usr/bin/env python3
"""
Print the marketing version a TestFlight build should use.

All TestFlight builds for one release candidate belong in the same marketing
version train, so this reuses the latest iOS train rather than minting a new
version per build; CURRENT_PROJECT_VERSION, set by the workflow, distinguishes
the individual builds. A train that App Store Connect has closed is stepped
over, because it would reject the upload as ITMS-90186.

The answer is meant to be passed to xcodebuild as MARKETING_VERSION=... . The
repository is never modified: a PR pipeline must not push commits to
contributors' branches, and doing so on a pull_request trigger risks a
re-trigger loop.

Optional environment:
    PATCH_ROLLOVER             – "true": a patch of 9 rolls into the next minor (2.4.9 -> 2.5.0)
    FORCE_UPDATE               – "true": the next major, X.0.0, one above the highest App Store
                                 version (2.0.2 -> 3.0.0, 3.0.0 -> 4.0.0). Apps force-update on a
                                 higher major. A rebuild before X.0.0 reaches the App Store reuses
                                 X.0.0 instead of jumping again. Needs the Apple lookup: on failure
                                 it exits non-zero rather than guess.

Required environment:
    APPSTORE_API_KEY_ID        – App Store Connect API key id
    APPSTORE_ISSUER_ID         – issuer id for that key
    APPSTORE_API_PRIVATE_KEY   – the .p8 private key, PEM contents
    IOS_BUNDLE_ID              – bundle id of the app to look up
    CURRENT_VERSION            – MARKETING_VERSION as it stands in the project

Output, on stdout, in GITHUB_OUTPUT key=value form so the step can tee it
straight into $GITHUB_OUTPUT:

    version=4.0.2
    verified=true

Behaviour:
    The candidate is the higher of CURRENT_VERSION and the latest iOS TestFlight
    train — the project version is what opens a new train, since nothing in
    these pipelines bumps it. If that candidate is at or below the highest
    closed train, the answer becomes one patch above the highest closed train
    instead, which clears every closed train in one step.

    A train is closed when an App Store version record exists for its marketing
    version, whatever state that record is in. appVersionState is fetched and
    logged so a human can see why a train was judged closed, and is deliberately
    never branched on — see docs/adr/0013.

    On any failure — no credentials, app not found, API error — it warns on
    stderr, falls back to CURRENT_VERSION and reports verified=false. A version
    lookup should not be able to break a build during an Apple outage, but the
    fallback is a gamble on a train still being open, so the workflow surfaces
    verified=false on the PR comment and the Telegram message rather than
    letting the upload step discover it two hours later.
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


def bump_patch(version: str) -> str:
    """'4.0.9' -> '4.0.10'. The smallest step App Store Connect will accept.

    With PATCH_ROLLOVER=true the patch stays a single digit: '2.4.9' -> '2.5.0'. The major never
    changes automatically ('2.9.9' -> '2.10.0'), because apps force-update on a higher major.
    """
    major, minor, patch = parse(version)
    if os.environ.get("PATCH_ROLLOVER", "").strip().lower() == "true" and patch >= 9:
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def force_update_version(current: str, testflight_versions, closed_trains) -> str:
    """Next major train for a force update.

    Base is the highest App Store version (any state), or the project version when the app has none.
    If that next major is already an open TestFlight train (built, not yet on the App Store), reuse
    its highest version, so rebuilding a force-update release never skips a major.
    """
    base = max(closed_trains, key=parse) if closed_trains else current
    target = parse(base)[0] + 1
    open_trains = [v for v in testflight_versions if parse(v)[0] == target and v not in closed_trains]
    return max(open_trains, key=parse) if open_trains else f"{target}.0.0"


def emit(version: str, verified: bool):
    print(f"version={version}")
    print(f"verified={'true' if verified else 'false'}")


def main():
    current = os.environ.get("CURRENT_VERSION", "").strip()
    if not current:
        warn("CURRENT_VERSION is not set; nothing to fall back to")
        sys.exit(1)

    key_id = os.environ.get("APPSTORE_API_KEY_ID", "").strip()
    issuer = os.environ.get("APPSTORE_ISSUER_ID", "").strip()
    key = os.environ.get("APPSTORE_API_PRIVATE_KEY", "")
    bundle = os.environ.get("IOS_BUNDLE_ID", "").strip()
    force_update = os.environ.get("FORCE_UPDATE", "").strip().lower() == "true"

    def fallback(msg):
        # A guessed major could skip a number or repeat a live one, so a force update never guesses.
        if force_update:
            warn(f"{msg}; a force update needs the App Store lookup, stopping")
            sys.exit(1)
        warn(f"{msg}; using CURRENT_VERSION")
        emit(current, verified=False)

    if not all([key_id, issuer, key.strip(), bundle]):
        fallback("App Store Connect credentials or bundle id missing")
        return

    try:
        token = make_token(key_id, issuer, key)

        apps = get(f"/v1/apps?filter[bundleId]={bundle}&limit=1", token)
        if not apps.get("data"):
            fallback(f"no app found for bundle id {bundle}")
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

        # An App Store version record is what closes a train, whatever state it
        # is in (ADR-0013). appVersionState is read for the warning below and
        # nothing else; branching on it reopens the failure this replaced, and
        # the appStoreState it replaces is deprecated.
        store = get(
            f"/v1/apps/{app_id}/appStoreVersions"
            f"?filter[platform]=IOS&limit=200"
            f"&fields[appStoreVersions]=versionString,appVersionState",
            token,
        )
        closed_trains = {
            v["attributes"]["versionString"]: v["attributes"].get("appVersionState", "unknown state")
            for v in store.get("data", [])
            if v.get("attributes", {}).get("versionString")
        }

        if force_update:
            version = force_update_version(current, testflight_versions, closed_trains)
            warn(f"force update: next major train is {version}")
            emit(version, verified=True)
            return

        candidate = current
        if not testflight_versions:
            warn("no TestFlight marketing versions; using CURRENT_VERSION")
        else:
            latest_testflight = max(testflight_versions, key=parse)
            if parse(latest_testflight) > parse(candidate):
                candidate = latest_testflight
                warn(f"latest TestFlight train is {latest_testflight}; reusing it")
            else:
                warn(
                    f"project marketing version {current} is at or above every "
                    f"TestFlight train (highest {latest_testflight}); using it"
                )

        if closed_trains:
            highest_closed = max(closed_trains, key=parse)
            if parse(candidate) <= parse(highest_closed):
                # One step above the *highest closed* train, not above the
                # candidate: bumping 4.0.0 when 4.0.1 is closed lands on 4.0.1,
                # which is closed too. This clears all of them at once.
                stepped = bump_patch(highest_closed)
                warn(
                    f"train {candidate} is at or below the closed train "
                    f"{highest_closed} (App Store version is "
                    f"{closed_trains[highest_closed]}); using {stepped}"
                )
                candidate = stepped

        emit(candidate, verified=True)

    except (urllib.error.URLError, subprocess.CalledProcessError, ValueError, KeyError) as e:
        fallback(f"lookup failed ({type(e).__name__}: {e})")


if __name__ == "__main__":
    main()
