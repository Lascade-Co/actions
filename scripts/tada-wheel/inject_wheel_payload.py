#!/usr/bin/env python3
"""Fold a JVM payload into a wheel, and -- for the public one -- retag it.

Two callers, one rewriter:

  `--tag py3-none-<platform>`  the PUBLIC wheel. Input is the ordinary
      `travel_animator-<version>-py3-none-any.whl` that `uv build` produces;
      output is `travel_animator-<version>-py3-none-<platform tag>.whl` carrying
      `tada_render/_jvm/**` from `build_jvm_payload.py`.

  `--keep-tag`                 the PRIVATE `tada` wheel. Input and output are both
      `tada-<version>-py3-none-any.whl`, carrying `tada/_jvm/ta-prepare.jar` from
      `build_prepare_payload.py`. It stays pure and stays `any` because a jar is
      architecture-independent and the payload has no JRE and no natives -- ADR
      0015 has it borrow the public wheel's jlink'd runtime. A platform tag here
      would be a false claim that would then have to be built four times.

One script rather than two because everything below the tag is the same problem,
and it is a problem with sharp edges. Hand-rolled rather than `wheel unpack` +
`wheel pack` for two reasons. The execute bit: a round trip through the
filesystem depends on the builder's umask and on the tool not normalising modes
to 0644, either of which yields an installed `jre/bin/java` that cannot be
exec'd. Here modes come straight off `os.stat`. And reproducibility: every entry
gets a fixed 1980 timestamp, so the RECORD hashes `verify_tada_wheel.py`
re-checks mean something.

Under `--tag`, `.dist-info/WHEEL`'s `Tag:` is rewritten together with
`Root-Is-Purelib: false` (the payload is platform-specific, so it belongs in
platlib) and the file is renamed to match. A wheel whose filename and `Tag:`
disagree is accepted by some installers and rejected by others, so both move
together, always. Under `--keep-tag` neither moves: `WHEEL` is copied through
untouched, and the output keeps the input's filename -- which is what lets
`build-metadata.json`'s `wheel` field and every consumer's `tada-*.whl` glob go
on meaning what they meant.
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

    pip restores the execute bit only when `stat.S_ISREG(mode)` holds, so an entry
    recorded `0o755` with no `S_IFREG` installs 0644. uv's check omits `S_ISREG`,
    so such a wheel works under uv and fails under pip -- measured 2026-08-11 on
    `python:3.12-slim-bookworm` / pip 25.0.1 as
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
                        help="directory produced by build_jvm_payload.py "
                             "or build_prepare_payload.py")
    tagging = parser.add_mutually_exclusive_group(required=True)
    tagging.add_argument("--tag",
                         help="full wheel tag, e.g. py3-none-manylinux_2_28_x86_64")
    tagging.add_argument("--keep-tag", action="store_true",
                         help="leave the wheel's tag, WHEEL metadata and filename alone "
                              "(the private wheel: its payload is one jar, so it is still "
                              "architecture-independent)")
    parser.add_argument("--package", default="tada_render",
                        help="import package the payload is installed under")
    parser.add_argument("--payload-dir", default="_jvm",
                        help="subdirectory of --package; must match render_bridge.PAYLOAD_DIRNAME")
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    if args.tag is not None:
        if len(args.tag.split("-")) != 3:
            print(f"--tag must be <python>-<abi>-<platform>, got {args.tag!r}", file=sys.stderr)
            return 1
        if args.tag.endswith("-any"):
            # A `py3-none-any` wheel shadows the entire matrix for every installer,
            # and PyPI releases are immutable, so one bad upload is permanent. Use
            # --keep-tag when a pure wheel is genuinely what is wanted; it says so.
            print(f"refusing to write a platform-independent wheel: --tag {args.tag}",
                  file=sys.stderr)
            return 1

    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.keep_tag:
        # Same name in, same name out -- so the output MUST land somewhere else, or the
        # zip being written is the zip being read.
        out_name = Path(args.wheel).name
        if args.out_dir.resolve() == Path(args.wheel).resolve().parent:
            print(f"--keep-tag writes {out_name} unchanged, so --out-dir must not be the "
                  f"directory holding --wheel ({args.out_dir})", file=sys.stderr)
            return 1
    else:
        name, version, *rest = Path(args.wheel).name.split("-")
        out_name = f"{name}-{version}-{args.tag}.whl"
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
        # RECORD is always rebuilt (the payload adds rows). WHEEL is only held back
        # when it is about to be rewritten; under --keep-tag it copies through with
        # every other file, bytes and all.
        skipped = {record_name} if args.keep_tag else {record_name, wheel_name}
        for info in src.infolist():
            if info.filename in skipped:
                continue
            if info.filename.startswith(prefix):
                # This ran twice, or a working tree leaked a staged payload into
                # the build; either way the result would carry two.
                print(f"{args.wheel} already contains {info.filename}", file=sys.stderr)
                return 1
            data = src.read(info.filename)
            dst.writestr(info, data)
            if not info.filename.endswith("/"):
                record_rows.append((info.filename, record_hash(data), len(data)))

        if not args.keep_tag:
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

    if executables == 0 and not args.keep_tag:
        # Zero means the modes were lost upstream: a `zipfile` staging step, a
        # Windows checkout, or `actions/upload-artifact`, which does not keep them.
        #
        # Not asserted under --keep-tag: that payload is one jar and no JRE, so zero
        # is the correct answer there. `java -jar` reads it; nothing execs it.
        print("ERROR: no executable file in the payload; the JRE will not start.",
              file=sys.stderr)
        return 1

    print(f"{out_path}  ({out_path.stat().st_size / 1e6:.1f} MB, "
          f"{len(record_rows)} entries, {executables} executable)")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
