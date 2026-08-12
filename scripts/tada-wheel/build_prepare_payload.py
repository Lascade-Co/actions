#!/usr/bin/env python3
"""Assemble the JVM payload for the PRIVATE `tada` wheel: `ta-prepare.jar`.

Output layout, which is also what `verify_tada_wheel.py` asserts and what the
`tada` package will look for at `tada/_jvm/`:

    <out>/
      ta-prepare.jar   the choreography builder's CLI, EXACTLY as Gradle built it
      PAYLOAD.json     provenance -- sha256, entry point, builder class count

Two differences from `build_jvm_payload.py`, both deliberate.

**No JRE, no natives, no ANGLE.** ADR 0015: the private wheel reuses the public
wheel's jlink'd runtime (`tada_render/_jvm/jre/bin/java`) rather than shipping a
second one. The two wheels are always installed together -- that is what
`build-metadata.json`'s `wheel` + `render_wheel` pair means, and TARS's
`Dockerfile.dispatcher` installs both unconditionally. A second 44 MB JRE to run
a program that lives beside the first one is the thing that decision refused.

**The jar is COPIED, not rewritten.** `build_jvm_payload.py` must rewrite
`ta-render.jar` (Skiko's natives are resolved from the build machine, so the jar
as built is not portable). Nothing forces a rewrite here, and not rewriting buys
a guarantee the public path cannot have: the bytes in the wheel are bit-for-bit
the bytes `verifyTaRenderJarSeal` passed, so Gradle's seal and the wheel's seal
are the same assertion about the same file rather than two assertions with an
artifact round trip and a rewrite between them.

  Measured, so the size is a decision and not an oversight: stripping the
  `libskiko-*` and LWJGL natives out of this jar takes it from 35.2 MB to
  15.8 MB, and all eight `tests/goldens/frame-plan/` configs still produce
  byte-identical plans (2026-08-12, macOS arm64 build). It is NOT done, because
  `TadaPrepareMain` reaches `TaRenderCli.kt`'s shared flag parsing, and the
  renderer -- `PlaceLabelRasterizer`, `AnnotationRasterizer`, `FlagRenderer` --
  is reachable from there at the class-reference level. Those paths are not
  *executed* by prepare today; a stripped jar therefore converts any future
  prepare-side code that does reach them from a working program into a
  `LibraryLoadException` that reads as a Skiko bug rather than as packaging.
  19 MB is not worth buying that.

The identity checks below (entry point, builder classes) run here as well as in
`verify_tada_wheel.py` for the same reason Gradle's seal runs as well as the
wheel's: this step has the Gradle output in hand and can name the cause. Both
jars are `host/build/libs/ta-*.jar`, so handing this the render jar -- a wrong
artifact name, a glob that matched the sibling -- is a real mistake, and it is
much clearer here than as "the shipped jar has no builder classes" three jobs
later.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import zipfile
from pathlib import Path

PAYLOAD_JAR_NAME = "ta-prepare.jar"
PAYLOAD_MANIFEST_NAME = "PAYLOAD.json"

# host/build.gradle.kts pins both of these; they are the two facts that make a jar
# THIS jar rather than its sibling.
PREPARE_MAIN_CLASS = "com.lascade.tada.host.TadaPrepareMain"
RENDER_MAIN_CLASS = "com.lascade.tada.host.TadaHostMain"

# ADR 0006/0007's sealed package: ABSENT from ta-render.jar, and the whole reason
# this jar exists, so its PRESENCE here is the check that cannot pass vacuously.
BUILDER_PREFIX = "com/lascade/ta/shared/builder/"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main_class_of(archive: zipfile.ZipFile) -> str | None:
    """`Main-Class` from META-INF/MANIFEST.MF.

    Continuation lines are joined first: a jar manifest wraps at 72 bytes and
    continues with a single leading space, so a longer class name than today's
    would otherwise be read truncated and reported as the wrong entry point.
    """
    try:
        raw = archive.read("META-INF/MANIFEST.MF").decode("utf-8", "replace")
    except KeyError:
        return None
    lines: list[str] = []
    for line in raw.splitlines():
        if line.startswith(" ") and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    for line in lines:
        key, _, value = line.partition(":")
        if key.strip() == "Main-Class":
            return value.strip()
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", required=True, type=Path,
                        help="ta-prepare.jar as `./gradlew :host:taJars` built it")
    parser.add_argument("--out", required=True, type=Path, help="payload directory to create")
    args = parser.parse_args()

    if not args.jar.is_file():
        print(f"ERROR: no such jar: {args.jar}", file=sys.stderr)
        return 1

    with zipfile.ZipFile(args.jar) as archive:
        declared = main_class_of(archive)
        names = set(archive.namelist())
    builder = sorted(n for n in names if n.startswith(BUILDER_PREFIX) and n.endswith(".class"))
    entry_point_class = PREPARE_MAIN_CLASS.replace(".", "/") + ".class"

    if declared == RENDER_MAIN_CLASS:
        print(f"ERROR: {args.jar} is ta-render.jar, not ta-prepare.jar -- its Main-Class is "
              f"{declared}. Both are host/build/libs/ta-*.jar; check the artifact this was "
              "handed.", file=sys.stderr)
        return 1
    if declared != PREPARE_MAIN_CLASS:
        print(f"ERROR: {args.jar} declares Main-Class {declared!r}, expected "
              f"{PREPARE_MAIN_CLASS!r}.", file=sys.stderr)
        return 1
    if entry_point_class not in names:
        print(f"ERROR: {args.jar} names {PREPARE_MAIN_CLASS} as its entry point but does not "
              "carry it, so `java -jar` dies with ClassNotFoundException.", file=sys.stderr)
        return 1
    if not builder:
        print(f"ERROR: {args.jar} carries no class under '{BUILDER_PREFIX}'. That package IS "
              "ta-prepare.jar -- without it there is nothing to prepare with, and the wheel "
              "would ship a CLI whose entry point cannot load.", file=sys.stderr)
        return 1

    out: Path = args.out
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    jar_out = out / PAYLOAD_JAR_NAME
    # copy, not copy2: the mtime is irrelevant (inject_wheel_payload.py stamps every
    # entry with a fixed 1980 date) and the mode is 0644 either way -- a jar is read by
    # `java -jar`, never executed.
    shutil.copy(args.jar, jar_out)

    digest = sha256(jar_out)
    if digest != sha256(args.jar):
        # Unreachable short of a failing disk; asserted because "copied, not rewritten"
        # is the property this whole file rests on.
        print("ERROR: the copied jar does not match its source", file=sys.stderr)
        return 1

    manifest = {
        "schema_version": 1,
        "kind": "prepare",
        "jar": {
            "name": PAYLOAD_JAR_NAME,
            "sha256": digest,
            "main_class": PREPARE_MAIN_CLASS,
            "builder_classes": len(builder),
            "entries": len(names),
            "rewritten": False,
        },
        # Stated in the artifact rather than only in a comment: the one thing an
        # operator debugging a failed prepare needs to know is that the runtime is
        # somewhere else, and which somewhere.
        "jre": {
            "bundled": False,
            "source": "travel-animator wheel: tada_render/_jvm/jre (ADR 0015)",
        },
        "bytes": {"total": jar_out.stat().st_size, "jar": jar_out.stat().st_size},
    }
    (out / PAYLOAD_MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"{jar_out}: {jar_out.stat().st_size / 1e6:.1f} MB, {len(names)} entries, "
          f"{len(builder)} builder classes, Main-Class={declared}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
