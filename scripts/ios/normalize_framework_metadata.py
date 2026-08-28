#!/usr/bin/env python3
"""Normalize and validate version metadata in Apple framework bundles."""

from __future__ import annotations

import argparse
import plistlib
import stat
from pathlib import Path
from typing import Iterable


def framework_plists(roots: Iterable[Path]) -> list[Path]:
    plists = sorted(
        plist
        for root in roots
        for plist in root.rglob("Info.plist")
        if plist.parent.suffix == ".framework"
    )
    if not plists:
        raise ValueError("no framework Info.plist files found")
    return plists


def normalize_framework_metadata(
    roots: Iterable[Path], *, check_only: bool = False
) -> list[Path]:
    changed: list[Path] = []
    for plist in framework_plists(roots):
        raw = plist.read_bytes()
        metadata = plistlib.loads(raw)
        bundle_version = metadata.get("CFBundleVersion")
        if not isinstance(bundle_version, str) or not bundle_version.strip():
            raise ValueError(f"{plist}: missing non-empty CFBundleVersion")

        short_version = metadata.get("CFBundleShortVersionString")
        if isinstance(short_version, str) and short_version.strip():
            continue
        if check_only:
            raise ValueError(f"{plist}: missing non-empty CFBundleShortVersionString")

        metadata["CFBundleShortVersionString"] = bundle_version
        output_format = plistlib.FMT_BINARY if raw.startswith(b"bplist00") else plistlib.FMT_XML
        original_mode = stat.S_IMODE(plist.stat().st_mode)
        try:
            plist.chmod(original_mode | stat.S_IWUSR)
            plist.write_bytes(plistlib.dumps(metadata, fmt=output_format, sort_keys=False))
        finally:
            plist.chmod(original_mode)
        changed.append(plist)
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="validate without modifying plists")
    parser.add_argument("roots", nargs="+", type=Path)
    args = parser.parse_args()

    try:
        changed = normalize_framework_metadata(args.roots, check_only=args.check)
    except ValueError as error:
        parser.error(str(error))

    if args.check:
        print("Validated embedded framework version metadata")
    else:
        for plist in changed:
            print(f"Added CFBundleShortVersionString to {plist}")
        print(f"Normalized {len(changed)} framework plist(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
