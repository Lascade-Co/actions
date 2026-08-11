#!/usr/bin/env python3
"""Measure the glibc floor of every ELF in a wheel, and refuse a wheel that is
above the consumer's.

This replaces `auditwheel repair`, which measures the same floor but also VENDORS
every external library into `<pkg>.libs/`. That is wrong here in both directions:
the libraries it would vendor are exactly the ones that must come from the host
(`libEGL.so.1`/`libGL.so.1` so the GPU worker gets `libEGL_nvidia.so`;
`libfontconfig.so.1` for the host font cache), and it cannot complete on a wheel
carrying a JRE anyway -- measured 2026-08-11, it fails resolving `libjvm.so`, then
`libXtst.so.6`, on down java.desktop's X11 closure, and would end up patchelf-ing
the JRE's own `$ORIGIN` RPATHs. So this script gates, and `auditwheel show` runs
for the record only.

The manylinux tag is a GLIBC claim only. PEP 600 also wants external dependencies
limited to a whitelist; this wheel deliberately fails that half -- it needs a system
`libGL.so.1`, `libX11.so.6` and `libfontconfig.so.1`, as PyTorch's and PySide's
wheels do -- so that requirement is a runtime prerequisite no installer checks.
TARS's GPU image satisfies it, verified 2026-08-11.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ELF_MAGIC = b"\x7fELF"
GLIBC_SYMBOL = re.compile(rb"GLIBC_(\d+)\.(\d+)")
TAG = re.compile(r"manylinux_(\d+)_(\d+)_")

# Every external library the payload's ELFs name in DT_NEEDED that is NOT on the
# manylinux whitelist. Anything here turning up in a `.libs/` got vendored after all.
EXPECTED_EXTERNAL = {
    "libEGL.so.1", "libGLESv2.so.2", "libGL.so.1",   # must be the host's driver
    "libX11.so.6", "libfontconfig.so.1",             # Skiko links them; host-owned
    "libXtst.so.6", "libXi.so.6", "libXrender.so.1", "libXext.so.6",
    "libasound.so.2", "libpng16.so.16", "libuuid.so.1", "libz.so.1",
    "libfreetype.so.6",                              # java.desktop, never loaded headless
}


def elf_glibc_versions(data: bytes) -> set[tuple[int, int]]:
    """Every `GLIBC_x.y` version string in the file.

    A substring scan rather than a `.gnu.version_r` parse: over-reporting is the
    safe direction for a floor check -- a false positive fails the build loudly,
    a false negative ships a wheel that will not load.
    """
    return {(int(a), int(b)) for a, b in GLIBC_SYMBOL.findall(data)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--max-glibc", required=True,
                        help="the floor this wheel's tag claims, e.g. 2.28")
    parser.add_argument("--consumer-glibc", default="2.36",
                        help="the lowest glibc that must be able to load it "
                             "(TARS runs python:3.12-slim-bookworm = 2.36)")
    args = parser.parse_args()

    claimed = tuple(int(part) for part in args.max_glibc.split("."))
    consumer = tuple(int(part) for part in args.consumer_glibc.split("."))

    worst: tuple[int, int] = (0, 0)
    worst_file = ""
    per_file: list[tuple[tuple[int, int], str]] = []
    vendored: list[str] = []

    with zipfile.ZipFile(args.wheel) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            if ".libs/" in info.filename:
                vendored.append(info.filename)
            with zf.open(info) as handle:
                head = handle.read(4)
                if head != ELF_MAGIC:
                    continue
                data = head + handle.read()
            versions = elf_glibc_versions(data)
            if not versions:
                continue
            highest = max(versions)
            per_file.append((highest, info.filename))
            if highest > worst:
                worst, worst_file = highest, info.filename

    if not per_file:
        print(f"{args.wheel} contains no ELF binaries -- this is not a Linux "
              "platform wheel, or the payload is missing", file=sys.stderr)
        return 1

    for version, name in sorted(per_file, reverse=True)[:8]:
        print(f"  GLIBC_{version[0]}.{version[1]:<3} {name}")
    print(f"{len(per_file)} ELF binaries; highest requirement "
          f"GLIBC_{worst[0]}.{worst[1]} in {worst_file}")

    if vendored:
        # A `.libs/` here can only hold one of EXPECTED_EXTERNAL, every one of
        # which must come from the host.
        print(f"REFUSING: {len(vendored)} vendored libraries found, e.g. "
              f"{vendored[:3]}. Nothing in this payload may be vendored.",
              file=sys.stderr)
        return 1

    failed = False
    if worst > claimed:
        print(f"REFUSING: {worst_file} needs GLIBC_{worst[0]}.{worst[1]}, above the "
              f"{args.max_glibc} this wheel's tag claims. Either raise the tag "
              f"(and re-check it against the consumer floor) or find a build of "
              f"that dependency with a lower floor.", file=sys.stderr)
        failed = True
    if worst > consumer:
        print(f"REFUSING: {worst_file} needs GLIBC_{worst[0]}.{worst[1]}, above the "
              f"consumer's {args.consumer_glibc}. The wheel would install and then "
              f"fail to load on the GPU worker.", file=sys.stderr)
        failed = True
    if failed:
        return 1

    print(f"floor GLIBC_{worst[0]}.{worst[1]} <= tag {args.max_glibc} "
          f"<= consumer {args.consumer_glibc}: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
