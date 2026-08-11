#!/usr/bin/env python3
"""Download, verify and unpack ONE pinned ANGLE release asset.

CI never builds ANGLE: it is built out of band, published as a release asset, and consumed
here by digest. Hence the conventions, which match `travel-animator-shared`'s `downloadAngle`
Gradle task:

  * the REST **asset** endpoint, because that repo is PRIVATE and the browser-facing
    `releases/download/...` URL answers 404 to an unauthenticated client;
  * redirects walked by hand, dropping `Authorization` when the host changes -- the
    pre-signed storage URL REJECTS a request still carrying a GitHub bearer token;
  * a checksum mismatch is fatal, not a warning: the digest is what makes the downloaded
    asset the binaries that were verified.

Usage:

    python fetch_pinned_angle.py --repo Lascade-Co/travel-animator-shared \
      --tag angle-be80ce59 --asset angle-be80ce59-macos.zip --sha256 dfe3d6... --out angle
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlparse

API = "https://api.github.com"
USER_AGENT = "lascade-actions-fetch-pinned-angle"

# Where a human goes when an asset is missing, keyed by the platform suffix of the asset name.
PROVENANCE = {
    "macos": "travel-animator-shared third_party/angle/PROVENANCE-macos.md",
    "windows": "Lascade-Co/actions scripts/angle/PROVENANCE-windows.md "
               "(built and published 2026-08-11; rebuild with build_angle_windows.ps1)",
    "ios": "travel-animator-shared third_party/angle/PROVENANCE.md",
}


def provenance_hint(asset: str) -> str:
    for key, doc in PROVENANCE.items():
        if key in asset:
            return doc
    return "travel-animator-shared third_party/angle/"


def fail(message: str) -> NoReturn:
    print(f"::error::{message.splitlines()[0]}", file=sys.stderr)
    print(message, file=sys.stderr)
    raise SystemExit(1)


def resolve_token() -> str:
    for name in ("GITHUB_TOKEN", "GH_TOKEN"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    try:
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, check=False
        )
    except OSError:
        result = None
    if result is not None and result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    fail(
        "No GitHub token available to fetch ANGLE. The release repository is private, so the "
        "asset needs one. Set GITHUB_TOKEN (or GH_TOKEN), or run `gh auth login`."
    )


def request(url: str, token: str, accept: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Turns every 3xx into an HTTPError so the caller can decide about the token."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def find_asset(repo: str, tag: str, asset: str, token: str) -> dict:
    url = f"{API}/repos/{repo}/releases/tags/{tag}"
    try:
        with _OPENER.open(request(url, token, "application/vnd.github+json")) as response:
            release = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            fail(
                f"No release '{tag}' on {repo}.\n"
                f"CI does not build ANGLE -- it consumes a published, checksummed asset. "
                f"Nothing has been published at this revision.\n"
                f"See {provenance_hint(asset)}."
            )
        fail(f"GET {url} failed: HTTP {error.code} {error.reason}")
    except urllib.error.URLError as error:
        fail(f"GET {url} failed: {error.reason}")

    for candidate in release.get("assets", []):
        if candidate.get("name") == asset:
            return candidate

    present = ", ".join(sorted(a.get("name", "?") for a in release.get("assets", []))) or "(none)"
    fail(
        f"Release '{tag}' on {repo} has no asset named '{asset}'.\n"
        f"Assets present: {present}\n"
        f"CI does not build ANGLE. This asset has to be built once, out of band, and "
        f"uploaded to that release.\n"
        f"See {provenance_hint(asset)}."
    )


def download(url: str, token: str, target: Path) -> None:
    """Stream `url` to `target`, walking redirects and dropping the token off-host."""
    current = url
    authorized = True
    for _ in range(6):
        req = (
            request(current, token, "application/octet-stream")
            if authorized
            else urllib.request.Request(
                current, headers={"Accept": "application/octet-stream", "User-Agent": USER_AGENT}
            )
        )
        try:
            with _OPENER.open(req) as response:
                with target.open("wb") as handle:
                    shutil.copyfileobj(response, handle, 1 << 20)
            return
        except urllib.error.HTTPError as error:
            if error.code in (301, 302, 303, 307, 308):
                location = error.headers.get("Location")
                if not location:
                    fail(f"HTTP {error.code} from {current} with no Location header")
                authorized = authorized and urlparse(location).netloc == urlparse(current).netloc
                current = location
                continue
            body = ""
            try:
                body = error.read().decode("utf-8", "replace")[:500]
            except Exception:  # noqa: BLE001 - diagnostics only
                pass
            fail(f"Download failed: HTTP {error.code} {error.reason} from {current}. {body}")
        except urllib.error.URLError as error:
            fail(f"Download failed: {error.reason} from {current}")
    fail("Download failed: too many redirects")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def unpack(archive: Path, out: Path) -> list[str]:
    """Extract `archive` into `out`, preserving permission bits that `extractall` drops.

    Backslash separators are normalised: a Windows-packed archive can write
    `include\\EGL\\egl.h`, which `zipfile` treats as a FILENAME on POSIX and extracts as
    flat files instead of a tree, silently.
    """
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    names: list[str] = []
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            if "\\" in info.filename:
                print(
                    f"note: normalising backslash separators in {info.filename!r} "
                    "-- this asset was packed on Windows against the ZIP spec",
                    file=sys.stderr,
                )
                info.filename = info.filename.replace("\\", "/")
            extracted = Path(zf.extract(info, out))
            if info.is_dir():
                continue
            names.append(info.filename)
            mode = info.external_attr >> 16
            if mode & 0o777:
                extracted.chmod(stat.S_IMODE(mode))
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="owner/name holding the release")
    parser.add_argument("--tag", required=True, help="release tag, e.g. angle-be80ce59")
    parser.add_argument("--asset", required=True, help="asset file name to download")
    parser.add_argument("--sha256", required=True, help="expected SHA-256 of the asset")
    parser.add_argument("--out", required=True, type=Path, help="directory to unpack into")
    args = parser.parse_args()

    expected = args.sha256.strip().lower()
    if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        fail(f"--sha256 must be 64 hex characters; got '{args.sha256}'")

    token = resolve_token()
    asset = find_asset(args.repo, args.tag, args.asset, token)
    print(f"asset {args.asset}: id={asset.get('id')} size={asset.get('size')}")

    archive = args.out.parent / args.asset
    archive.parent.mkdir(parents=True, exist_ok=True)
    download(f"{API}/repos/{args.repo}/releases/assets/{asset['id']}", token, archive)

    actual = sha256(archive)
    if actual != expected:
        size = archive.stat().st_size
        # Delete it: a retry must re-download rather than re-verify the same bad bytes.
        archive.unlink()
        fail(
            f"ANGLE checksum MISMATCH for {args.asset}.\n"
            f"  expected {expected}\n"
            f"  actual   {actual}  ({size} bytes)\n"
            f"Either the release asset was replaced, or the pin was not updated with it. "
            f"Both are reasons to stop: the digest is what makes the downloaded binaries the "
            f"ones that were verified. Do NOT relax this check -- fix the pin or the asset.\n"
            f"See {provenance_hint(args.asset)}."
        )
    print(f"sha256 OK: {actual}")

    names = unpack(archive, args.out)
    top_level = sorted(p.name for p in args.out.iterdir() if p.is_file())
    egl = [n for n in top_level if n.startswith(("libEGL", "EGL"))]
    gles = [n for n in top_level if n.startswith(("libGLESv2", "GLESv2"))]
    if not egl or not gles:
        fail(
            f"{args.asset} unpacked without an EGL/GLESv2 pair at its ROOT.\n"
            f"Top-level files: {top_level or '(none)'}\n"
            f"All entries: {names[:20]}{' ...' if len(names) > 20 else ''}\n"
            f"build_jvm_payload.py --angle-dir copies top-level FILES only, and ANGLE's libEGL "
            f"loads libGLESv2 BY BARE NAME from its own directory, so the two must sit flat and "
            f"together. Repackage the asset with the libraries at the archive root."
        )

    print(f"unpacked {len(names)} entries into {args.out}")
    print(f"top level: {', '.join(top_level)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
