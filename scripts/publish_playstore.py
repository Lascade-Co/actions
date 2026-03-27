#!/usr/bin/env python3
import argparse
import base64
import html
import json
import subprocess
import time
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/androidpublisher"
API_BASE = "https://androidpublisher.googleapis.com/androidpublisher/v3"
UPLOAD_BASE = "https://androidpublisher.googleapis.com/upload/androidpublisher/v3"


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def truncate_unicode(s: str, max_chars: int) -> str:
    arr = list(s)
    if len(arr) <= max_chars:
        return s
    if max_chars <= 1:
        return "…"
    return "".join(arr[: max_chars - 1]) + "…"


def http_json(method: str, url: str, token: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    data = None
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(url, data=data, method=method, headers=headers)
    try:
        with urlopen(req) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} {method} {url}\n{err}") from e


def http_upload_octet(method: str, url: str, token: str, file_path: str) -> Dict[str, Any]:
    with open(file_path, "rb") as f:
        payload = f.read()

    req = Request(
        url,
        data=payload,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
    )
    try:
        with urlopen(req) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} UPLOAD {url}\n{err}") from e


def mint_access_token(service_account_json_path: str) -> str:
    sa = load_json(service_account_json_path)
    client_email = sa["client_email"]
    private_key = sa["private_key"]

    now = int(time.time())
    exp = now + 3600

    header = {"alg": "RS256", "typ": "JWT"}
    claim = {
        "iss": client_email,
        "scope": SCOPE,
        "aud": TOKEN_URL,
        "iat": now,
        "exp": exp,
    }

    key_path = "sa_key.pem"
    with open(key_path, "w", encoding="utf-8") as f:
        f.write(private_key)

    signing_input = (
        f"{b64url(json.dumps(header, separators=(',', ':')).encode())}"
        f".{b64url(json.dumps(claim, separators=(',', ':')).encode())}"
    )
    sig = subprocess.check_output(
        ["openssl", "dgst", "-sha256", "-sign", key_path],
        input=signing_input.encode("utf-8"),
    )
    jwt = f"{signing_input}.{b64url(sig)}"

    form = f"grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion={jwt}".encode("utf-8")
    req = Request(
        TOKEN_URL,
        data=form,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["access_token"]
    except HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Failed to mint access token: HTTP {e.code}\n{err}") from e


def keep_fields(r: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in (
            "name",
            "versionCodes",
            "status",
            "userFraction",
            "countryTargeting",
            "releaseNotes",
            "inAppUpdatePriority",
    ):
        if k in r and r[k] is not None:
            out[k] = r[k]
    return out


def create_edit(token: str, package: str) -> str:
    edit = http_json("POST", f"{API_BASE}/applications/{package}/edits", token, body={})
    return edit["id"]


def get_track(token: str, package: str, edit_id: str, track: str) -> Dict[str, Any]:
    try:
        return http_json("GET", f"{API_BASE}/applications/{package}/edits/{edit_id}/tracks/{track}", token)
    except RuntimeError:
        return {"track": track, "releases": []}


def update_track(token: str, package: str, edit_id: str, track: str, releases: List[Dict[str, Any]]) -> None:
    body = {"track": track, "releases": releases}
    http_json("PUT", f"{API_BASE}/applications/{package}/edits/{edit_id}/tracks/{track}", token, body=body)


def commit_edit(token: str, package: str, edit_id: str) -> None:
    http_json("POST", f"{API_BASE}/applications/{package}/edits/{edit_id}:commit", token, body=None)


def infer_locale_from_whatsnew(notes_file: str) -> str:
    base = notes_file.split("/")[-1]
    if base.startswith("whatsnew-"):
        return base[len("whatsnew-"):]
    return "en-US"


def upload_bundle_and_symbols(token, edit_id_b, pkg, aab, native_symbols) -> str:
    # Upload AAB (bundle)
    already_uploaded = False
    try:
        bundle = http_upload_octet(
            "POST",
            f"{UPLOAD_BASE}/applications/{pkg}/edits/{edit_id_b}/bundles?uploadType=media",
            token,
            aab,
        )
        version_code = str(bundle["versionCode"])
    except RuntimeError as e:
        if "version code that has already been used" not in str(e):
            raise
        # Bundle was uploaded in a previous committed edit; look up its version code.
        already_uploaded = True
        resp = http_json("GET", f"{API_BASE}/applications/{pkg}/edits/{edit_id_b}/bundles", token)
        codes = [b["versionCode"] for b in resp.get("bundles", [])]
        if not codes:
            raise
        version_code = str(max(codes))

    # Upload native debug symbols (nativeCode)
    try:
        http_upload_octet(
            "POST",
            f"{UPLOAD_BASE}/applications/{pkg}/edits/{edit_id_b}/apks/{version_code}/deobfuscationFiles/nativeCode?uploadType=media",
            token,
            native_symbols,
        )
    except RuntimeError:
        if not already_uploaded:
            raise
        # On retry, symbols were likely already uploaded.

    return version_code


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service-account-json", required=True)
    ap.add_argument("--package-name", required=True)
    ap.add_argument("--track", required=True)
    ap.add_argument("--aab", required=True)
    ap.add_argument("--native-symbols", required=True)
    ap.add_argument("--release-name", required=True)
    ap.add_argument("--notes-file", required=True)

    ap.add_argument("--raw-notes-file", required=True)
    ap.add_argument("--app-name", required=True)
    ap.add_argument("--github-release-url", default="")
    ap.add_argument("--play-console-production-url", default="")
    ap.add_argument("--telegram-out", required=True)

    ap.add_argument("--country", required=True)  # BD
    ap.add_argument("--user-fraction", required=True)  # 0.99
    args = ap.parse_args()

    token = mint_access_token(args.service_account_json)
    pkg = args.package_name
    track_name = args.track

    # -----------------------------
    # Phase 1: Transition any inProgress rollout into a valid fallback state.
    # A country-targeted staged rollout must sit on top of a completed release
    # so users outside the target country still have an upgrade path.
    # -----------------------------
    previous_rollout_action = "none"

    edit_id_a = create_edit(token, pkg)
    track_a = get_track(token, pkg, edit_id_a, track_name)
    releases_a = track_a.get("releases", []) or []

    # If there is an inProgress, move it to its next stable state in its own
    # commit before creating the new rollout.
    new_releases_a: List[Dict[str, Any]] = []
    has_inprogress = any(r.get("status") == "inProgress" for r in releases_a)
    has_completed_fallback = any(r.get("status") == "completed" for r in releases_a)

    if has_inprogress:
        # If there is no completed release yet, promote the current staged
        # rollout to completed so the next country rollout has a fallback.
        completing_inprogress = not has_completed_fallback
        if not completing_inprogress:
            for r in releases_a:
                if r.get("status") != "inProgress":
                    continue
                fraction = r.get("userFraction")
                if fraction is None or fraction >= 1.0:
                    completing_inprogress = True
                    break

        for r in releases_a:
            status = r.get("status")
            rr = keep_fields(r)

            # keep drafts out; they often block releases anyway
            if status == "draft":
                continue

            # Drop old completed releases when promoting inProgress to completed,
            # because the Play API only allows one completed release per track.
            if status == "completed" and completing_inprogress:
                continue

            if status == "inProgress":
                fraction = rr.get("userFraction")
                if completing_inprogress or fraction is None or fraction >= 1.0:
                    rr["status"] = "completed"
                    rr.pop("userFraction", None)
                    previous_rollout_action = "completed_for_fallback" if not has_completed_fallback else "completed"
                else:
                    rr["status"] = "halted"
                    previous_rollout_action = "halted"

            # Only inProgress releases may carry countryTargeting; strip from
            # all releases since every inProgress is now halted or completed.
            rr.pop("countryTargeting", None)
            new_releases_a.append(rr)

        update_track(token, pkg, edit_id_a, track_name, new_releases_a)
        commit_edit(token, pkg, edit_id_a)
    else:
        # No commit needed; discard edit by not committing.
        pass

    # -----------------------------
    # Phase 2: Upload new bundle + create new staged rollout
    #         Ensure only ONE staged release in payload by:
    #           - keeping COMPLETED releases only
    #           - dropping halted/draft staged releases
    # -----------------------------
    edit_id_b = create_edit(token, pkg)
    version_code = upload_bundle_and_symbols(token, edit_id_b, pkg, args.aab, args.native_symbols)

    track_b = get_track(token, pkg, edit_id_b, track_name)
    releases_b = track_b.get("releases", []) or []

    # Keep only completed releases to avoid "Too many staged releases specified."
    # Strip countryTargeting so completed releases serve ALL countries as a
    # fallback — otherwise country-targeted staged rollouts fail with
    # "does not allow any existing users to upgrade".
    completed_only: List[Dict[str, Any]] = []
    for r in releases_b:
        if r.get("status") == "completed":
            rr = keep_fields(r)
            rr.pop("countryTargeting", None)
            completed_only.append(rr)

    if not completed_only:
        raise RuntimeError(
            "Cannot start a country-targeted rollout without an existing completed fallback release. "
            "Complete the current production rollout first, or publish a completed baseline release "
            "before starting another country-only rollout."
        )

    locale = infer_locale_from_whatsnew(args.notes_file)
    notes_text = read_text(args.notes_file).strip()

    new_release = {
        "name": args.release_name,
        "status": "inProgress",
        "userFraction": float(args.user_fraction),
        "countryTargeting": {"countries": [args.country], "includeRestOfWorld": False},
        "versionCodes": [version_code],
        "releaseNotes": [{"language": locale, "text": notes_text}],
        "inAppUpdatePriority": 0,
    }

    releases_out = completed_only + [new_release]

    update_track(token, pkg, edit_id_b, track_name, releases_out)
    commit_edit(token, pkg, edit_id_b)

    # Write outputs
    with open("play_outputs.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "version_code": version_code,
                "halted_previous": previous_rollout_action == "halted",
                "previous_rollout_action": previous_rollout_action,
            },
            f,
            separators=(",", ":"),
        )

    # Telegram message
    raw_notes = read_text(args.raw_notes_file).strip() or "No release notes provided."
    raw_notes = truncate_unicode(raw_notes, 2800)

    play_link = args.play_console_production_url
    if play_link:
        play_link = play_link + ("&tab=releases" if "?" in play_link else "?tab=releases")

    msg = (
        f"<b>✅ Production rollout started</b>\n\n"
        f"<b>App:</b> {html.escape(args.app_name)}\n"
        f"<b>Version:</b> {html.escape(args.release_name)}\n"
        f"<b>VersionCode:</b> <code>{html.escape(version_code)}</code>\n"
        f"<b>Package:</b> <code>{html.escape(pkg)}</code>\n\n"
        f"<b>Rollout:</b> {html.escape(args.user_fraction)}\n"
        f"<b>Country:</b> {html.escape(args.country)} only\n"
    )

    if previous_rollout_action == "halted":
        msg += "\n<i>Note: Previous in-progress production rollout was halted automatically.</i>\n"
    elif previous_rollout_action == "completed_for_fallback":
        msg += (
            "\n<i>Note: Previous in-progress production rollout was promoted to completed automatically "
            "so this rollout still has a valid fallback release.</i>\n"
        )
    elif previous_rollout_action == "completed":
        msg += "\n<i>Note: Previous in-progress production rollout was completed automatically.</i>\n"

    msg += "\n"
    if play_link:
        msg += f"<b>Play Console:</b> <a href=\"{html.escape(play_link)}\">Open production releases</a>\n"
    if args.github_release_url:
        msg += f"<b>GitHub Release:</b> <a href=\"{html.escape(args.github_release_url)}\">{html.escape(args.release_name)}</a>\n"
    msg += f"\n<b>Release notes:</b>\n<pre>{html.escape(raw_notes)}</pre>\n"

    with open(args.telegram_out, "w", encoding="utf-8") as f:
        f.write(msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
