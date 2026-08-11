#!/usr/bin/env python3
"""Fold one platform's JVM payload into the public wheel, and retag it.

Backlog C1. Input is the ordinary `travel_animator-<version>-py3-none-any.whl`
that `uv build` produces; output is
`travel_animator-<version>-py3-none-<platform tag>.whl` carrying
`tada_render/_jvm/**` from `build_jvm_payload.py`.

Why this is hand-rolled rather than `wheel unpack` + `wheel pack`:

**The execute bit.** `jre/bin/java` is the only reason this wheel does anything
at all, and a wheel records unix modes in the zip's `external_attr`. Both pip
and uv reproduce the execute bit only for entries whose zip mode carries it, so
an unpack/repack round-trip through a tool that normalises modes to 0644
produces an installed package whose `java` cannot be exec'd -- an
`OSError: [Errno 13] Permission denied` at the first render, from a wheel that
installed cleanly. Round-tripping through the filesystem also depends on the
builder's umask. Here the modes come straight off `os.stat` and the JRE's own
bits are copied through unchanged.

**Reproducibility.** Every entry is written with a fixed 1980 timestamp, so two
runs over the same inputs produce the same bytes, and the RECORD hashes that
`verify_tada_wheel.py` re-checks mean something.

The `Tag:` line in `.dist-info/WHEEL` is what `wheel pack` and every installer
read; it is rewritten here together with `Root-Is-Purelib: false` (the wheel now
contains a platform-specific binary payload, so its content belongs in
`site-packages` as platlib, not purelib) and the file is renamed to match. A
wheel whose filename and `Tag:` disagree is accepted by some installers and
rejected by others -- so both move, always together.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import stat
import sys
import zipfile
from pathlib import Path

# PEP 427: a fixed epoch, so the output is a function of the inputs alone.
FIXED_DATE = (1980, 1, 1, 0, 0, 0)


def record_hash(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def zipinfo_for(name: str, mode: int) -> zipfile.ZipInfo:
    """`mode` is a FULL `st_mode`, file-type bits included -- not `S_IMODE`.

    This is not pedantry, it is the difference between a working wheel and a
    `PermissionError`. pip decides whether to restore the execute bit with

        mode = info.external_attr >> 16
        return bool(mode and stat.S_ISREG(mode) and mode & 0o111)

    -- so an entry recorded as `0o755` with no `S_IFREG` (0o100000) fails
    `S_ISREG` and pip installs it 0644. uv's equivalent check omits `S_ISREG`,
    so the wheel works under uv and fails under pip: a difference that only
    shows up in a real `pip install`, and then only at the first render.
    Measured 2026-08-11 on `python:3.12-slim-bookworm`, pip 25.0.1:
    `PermissionError: [Errno 13] .../_jvm/jre/bin/java`.
    """
    info = zipfile.ZipInfo(name, date_time=FIXED_DATE)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = (mode & 0xFFFF) << 16
    if name.endswith("/"):
        info.external_attr |= 0x10
    return info


def rewrite_wheel_metadata(raw: bytes, tag: str) -> bytes:
    """Replace every `Tag:` with the one platform tag, and force
    `Root-Is-Purelib: false`. Unknown keys are passed through untouched --
    hatchling writes `Generator`, and a future one may write more."""
    lines: list[str] = []
    seen_purelib = False
    tagged = False
    for line in raw.decode("utf-8").splitlines():
        key = line.split(":", 1)[0].strip().lower()
        if key == "tag":
            if tagged:
                continue  # collapse a multi-tag wheel onto exactly one tag
            lines.append(f"Tag: {tag}")
            tagged = True
        elif key == "root-is-purelib":
            lines.append("Root-Is-Purelib: false")
            seen_purelib = True
        else:
            lines.append(line)
    if not tagged:
        lines.append(f"Tag: {tag}")
    if not seen_purelib:
        lines.append("Root-Is-Purelib: false")
    return ("\n".join(lines) + "\n").encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--payload", required=True, type=Path,
                        help="directory produced by build_jvm_payload.py")
    parser.add_argument("--tag", required=True,
                        help="full wheel tag, e.g. py3-none-manylinux_2_28_x86_64")
    parser.add_argument("--package", default="tada_render",
                        help="import package the payload is installed under")
    parser.add_argument("--payload-dir", default="_jvm",
                        help="subdirectory of --package; must match render_bridge.PAYLOAD_DIRNAME")
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    if len(args.tag.split("-")) != 3:
        print(f"--tag must be <python>-<abi>-<platform>, got {args.tag!r}", file=sys.stderr)
        return 1
    if args.tag.endswith("-any"):
        # The whole point of this script. A `py3-none-any` wheel sorts above
        # every platform wheel for every installer on every platform, so one
        # accidentally-untagged upload shadows the entire matrix -- for macOS
        # users too, forever, because PyPI releases are immutable.
        print(f"refusing to write a platform-independent wheel: --tag {args.tag}", file=sys.stderr)
        return 1

    name, version, *rest = Path(args.wheel).name.split("-")
    out_name = f"{name}-{version}-{args.tag}.whl"
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / out_name

    prefix = f"{args.package}/{args.payload_dir}/"
    record_rows: list[tuple[str, str, int]] = []
    dist_info: str | None = None

    with zipfile.ZipFile(args.wheel) as src:
        for info in src.infolist():
            if info.filename.endswith(".dist-info/WHEEL"):
                dist_info = info.filename.rsplit("/", 1)[0]
    if dist_info is None:
        print(f"{args.wheel} has no .dist-info/WHEEL", file=sys.stderr)
        return 1
    record_name = f"{dist_info}/RECORD"
    wheel_name = f"{dist_info}/WHEEL"

    with zipfile.ZipFile(args.wheel) as src, zipfile.ZipFile(
        out_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as dst:
        for info in src.infolist():
            if info.filename in (record_name, wheel_name):
                continue
            if info.filename.startswith(prefix):
                # A payload already inside the input wheel means this ran twice,
                # or a developer's working tree leaked a staged payload into the
                # sdist-less build. Either way the result would carry two.
                print(f"{args.wheel} already contains {info.filename}", file=sys.stderr)
                return 1
            data = src.read(info.filename)
            dst.writestr(info, data)
            if not info.filename.endswith("/"):
                record_rows.append((info.filename, record_hash(data), len(data)))

        wheel_metadata = rewrite_wheel_metadata(src.read(wheel_name), args.tag)
        dst.writestr(zipinfo_for(wheel_name, stat.S_IFREG | 0o644), wheel_metadata)
        record_rows.append(
            (wheel_name, record_hash(wheel_metadata), len(wheel_metadata))
        )

        payload_files = sorted(p for p in args.payload.rglob("*") if p.is_file())
        if not payload_files:
            print(f"{args.payload} is empty", file=sys.stderr)
            return 1
        executables = 0
        for path in payload_files:
            arcname = prefix + path.relative_to(args.payload).as_posix()
            mode = path.stat().st_mode
            if mode & stat.S_IXUSR:
                executables += 1
            data = path.read_bytes()
            # The full st_mode, not S_IMODE -- see zipinfo_for's docstring.
            dst.writestr(zipinfo_for(arcname, mode), data)
            record_rows.append((arcname, record_hash(data), len(data)))

        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        for row in record_rows:
            writer.writerow(row)
        writer.writerow((record_name, "", ""))
        dst.writestr(zipinfo_for(record_name, stat.S_IFREG | 0o644),
                     buffer.getvalue().encode("utf-8"))

    if executables == 0:
        # `jre/bin/java` at minimum. Zero means the modes were lost upstream --
        # a `zipfile`-based staging step, a Windows checkout, or an artifact
        # round-trip through `actions/upload-artifact`, which does NOT preserve
        # them (see the workflow's tar note).
        print("ERROR: no executable file in the payload; the JRE will not start.",
              file=sys.stderr)
        return 1

    print(f"{out_path}  ({out_path.stat().st_size / 1e6:.1f} MB, "
          f"{len(record_rows)} entries, {executables} executable)")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
