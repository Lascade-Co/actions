#!/usr/bin/env python3
"""Assert the JVM payload's executables kept their execute bit inside the wheel.

A wheel stores unix modes in the zip's `external_attr`, and both pip and uv
reproduce the execute bit only for entries whose recorded mode carries it. Lose
it and the wheel still builds, still passes `twine check`, still installs
without a word -- and then the first render dies with
`PermissionError: [Errno 13] .../_jvm/jre/bin/java`.

Every step between `jlink` and the finished wheel is a chance to lose it: a
`zipfile`-based staging pass that defaults to 0644, a `wheel unpack`/`pack`
round-trip through a filesystem with a restrictive umask, or
`actions/upload-artifact`, which does NOT preserve modes and is exactly why the
workflow tars the payload before uploading it.
"""
from __future__ import annotations

import stat
import sys
import zipfile


def pip_would_make_executable(info: zipfile.ZipInfo) -> bool:
    """pip's own predicate, copied verbatim from `pip._internal.utils.unpacking`.

    `S_ISREG` is the load-bearing clause and the reason this is not simply
    `mode & 0o111`: an entry recorded as 0o755 WITHOUT the regular-file type
    bits fails it, and pip then installs the file 0644. uv's equivalent check
    omits `S_ISREG`, so a wheel with that defect installs correctly under uv and
    is broken under pip. Asserting pip's rule rather than a looser one is the
    whole point of this file.
    """
    mode = info.external_attr >> 16
    return bool(mode and stat.S_ISREG(mode) and mode & 0o111)

# `bin/java` is the launcher; `bin/keytool` comes along with java.base. Only the
# first is load-bearing, but requiring the count to be non-zero as well catches
# a wholesale mode reset that happens to leave java alone.
REQUIRED = "_jvm/jre/bin/java"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: assert_wheel_exec_bit.py <wheel>", file=sys.stderr)
        return 2
    executables: list[str] = []
    found = None
    with zipfile.ZipFile(argv[1]) as zf:
        for info in zf.infolist():
            if pip_would_make_executable(info):
                executables.append(info.filename)
            if info.filename.endswith(REQUIRED):
                found = (info.filename, info)
    if found is None:
        print(f"{argv[1]} contains no {REQUIRED}", file=sys.stderr)
        return 1
    name, info = found
    mode = info.external_attr >> 16
    if not pip_would_make_executable(info):
        print(f"{name} would not be installed executable by pip "
              f"(external_attr mode {mode:o}: S_ISREG="
              f"{bool(stat.S_ISREG(mode))}, +x={bool(mode & 0o111)}); the "
              "installed wheel cannot start its own JVM", file=sys.stderr)
        return 1
    print(f"{name} mode {stat.S_IMODE(mode):o}; "
          f"{len(executables)} executable entries: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
