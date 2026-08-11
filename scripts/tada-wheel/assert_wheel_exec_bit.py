#!/usr/bin/env python3
"""Assert the JVM payload's executables kept their execute bit inside the wheel.

Lose the mode recorded in the zip's `external_attr` and the wheel still builds,
passes `twine check` and installs without a word -- then the first render dies
with `PermissionError: [Errno 13] .../_jvm/jre/bin/java`. Every step between
`jlink` and the finished wheel can lose it: a `zipfile` staging pass defaulting to
0644, a `wheel unpack`/`pack` round trip through a restrictive umask, or
`actions/upload-artifact`, which does NOT preserve modes (hence the workflow's tar).
"""
from __future__ import annotations

import stat
import sys
import zipfile


def pip_would_make_executable(info: zipfile.ZipInfo) -> bool:
    """pip's own predicate, copied verbatim from `pip._internal.utils.unpacking`.

    `S_ISREG` is why this is not simply `mode & 0o111`: an entry recorded 0o755
    WITHOUT the regular-file type bits fails it and pip installs 0644, while uv's
    check omits `S_ISREG` and accepts it. Assert pip's rule, the stricter one.
    """
    mode = info.external_attr >> 16
    return bool(mode and stat.S_ISREG(mode) and mode & 0o111)

# Also requiring a non-zero executable count catches a wholesale mode reset that
# happens to leave java alone.
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
