#!/usr/bin/env python3
"""Assemble ONE platform's JVM payload for the public `travel-animator` wheel.

Backlog C1/C4. Output layout, which is also what `tada_render.render_bridge`
looks for and `verify_tada_wheel.py` asserts:

    <out>/
      ta-render.jar     the renderer jar, with the Skiko natives REMOVED
      jre/              jlink'd java.base + java.desktop + jdk.unsupported
      natives/          LWJGL's two JNI shims + Skiko's libskia, for THIS platform
      angle/            libEGL + libGLESv2         (macOS/Windows only)
      PAYLOAD.json      provenance -- platform tag, jar sha256, JRE version

Three things about this are load-bearing and none of them are obvious.

**`ta-render.jar` is only architecture-neutral for LWJGL.** `tada`'s
`host/build.gradle.kts` declares all six LWJGL native classifiers (commit
c8dcfec), so those ship in every jar. Skiko does not: `shared/build.gradle.kts`
resolves `skiko-awt-runtime-$hostOs-$hostArch` from the BUILD MACHINE, so a jar
built on the Linux CI runner carries `libskiko-linux-x64.so` and nothing else.
Copying that jar into a macOS wheel would ship 20 MB of dead Linux binary AND no
macOS libskia -- a `LibraryLoadException` at the first overlay draw, long after
the GL probe has passed. So the jar is rewritten here without any `libskiko-*`
entry, and the right one is staged into `natives/` instead. That makes the
SHIPPED jar genuinely platform-independent, which is why `verify_tada_wheel.py`
re-runs the builder seal against the jar inside the wheel rather than trusting
Gradle's check of the jar Gradle built.

**`natives/` is a directory, not a temp dir.** Both LWJGL and Skiko will happily
self-extract their `.so` from the classpath into `$TMPDIR` on every single run;
`-Dorg.lwjgl.librarypath` and `-Dskiko.library.path` (set by
`render_bridge.packaged_runtime`) are what stop a GPU worker unpacking 20 MB per
render. Skiko reads its property as `File(path, System.mapLibraryName("skiko-$hostId"))`
-- i.e. the file must keep its `libskiko-linux-x64.so` name, NOT be renamed to
`libskiko.so`. Verified against `LibraryLoader.findAndLoadLibrary` bytecode in
skiko 0.148.2.

**`angle/` is absent on Linux on purpose.** `GlDriver.selectNativeLibraries`
falls through to the system `libEGL.so.1`/`libGLESv2.so.2` only when it finds no
bundled payload, and that system path is the one verified on Mesa llvmpipe and
the one the NVIDIA GPU worker needs. Staging an empty `angle/` would be inert,
but staging a populated one on Linux would silently take the container off the
driver it is supposed to be using.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Platform table
# ---------------------------------------------------------------------------
#
# Three different vendors, three different spellings of the same six platforms,
# and none of them is the wheel tag. Keeping the mapping in one literal table
# beats deriving it: a derivation that is wrong is wrong silently, whereas a
# missing row here is a KeyError naming the platform.
#
#   lwjgl_dir   where the jar nests that platform's JNI shims  (<os>/<arch>/org/lwjgl/...)
#   skiko_id    Skiko's `hostId`, which is part of the library FILE NAME
#   ext         the shared-library suffix `System.mapLibraryName` produces
#   angle       whether this platform needs a bundled ANGLE (macOS/Windows: yes)
PLATFORMS = {
    "linux-x64": dict(
        lwjgl_dir="linux/x64", skiko_id="linux-x64", ext=".so", prefix="lib", angle=False,
        default_tag="manylinux_2_28_x86_64",
    ),
    "linux-arm64": dict(
        lwjgl_dir="linux/arm64", skiko_id="linux-arm64", ext=".so", prefix="lib", angle=False,
        default_tag="manylinux_2_28_aarch64",
    ),
    "macos-arm64": dict(
        lwjgl_dir="macos/arm64", skiko_id="macos-arm64", ext=".dylib", prefix="lib", angle=True,
        default_tag="macosx_11_0_arm64",
    ),
    "macos-x64": dict(
        lwjgl_dir="macos/x64", skiko_id="macos-x64", ext=".dylib", prefix="lib", angle=True,
        default_tag="macosx_11_0_x86_64",
    ),
    "windows-x64": dict(
        lwjgl_dir="windows/x64", skiko_id="windows-x64", ext=".dll", prefix="", angle=True,
        default_tag="win_amd64",
    ),
    "windows-arm64": dict(
        lwjgl_dir="windows/arm64", skiko_id="windows-arm64", ext=".dll", prefix="", angle=True,
        default_tag="win_arm64",
    ),
}

# The module set, from plan §4.2. `java.desktop` is not optional -- Skiko's AWT
# runtime is the overlay rasteriser -- and `jdk.unsupported` carries `sun.misc.Unsafe`,
# which okio and LWJGL's MemoryUtil both reach for. Everything else (jdk.jshell,
# jdk.compiler, java.sql, the whole of java.management) is dropped.
JLINK_MODULES = "java.base,java.desktop,jdk.unsupported"

# Skiko unpacks this from the classpath beside libskiko on Windows only
# (`LibraryLoader(name, additionalFile = if (hostOs.isWindows) "icudtl.dat" else null)`).
# With `skiko.library.path` set it looks for it next to the library, so it has to
# be staged here or the first text measurement fails.
SKIKO_WINDOWS_SIDECAR = "icudtl.dat"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strip_skiko_natives(source: Path, target: Path) -> list[str]:
    """Copy `source` to `target` without any `libskiko-*` / `skiko-*` entry.

    Entry order and per-entry compression are preserved; only the removed names
    differ. `ZipFile.open` on the source and `writestr` with the ORIGINAL
    `ZipInfo` keeps timestamps and external attributes intact, so the jar stays
    byte-stable across runs for a given input -- which matters, because the
    wheel's RECORD hash of this file is what a reproducibility check would
    compare.
    """
    removed: list[str] = []
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(
        target, "w", compression=zipfile.ZIP_DEFLATED
    ) as dst:
        for info in src.infolist():
            base = info.filename.rsplit("/", 1)[-1]
            if base.startswith(("libskiko-", "skiko-")):
                removed.append(info.filename)
                continue
            dst.writestr(info, src.read(info.filename))
    return removed


def extract_matching(archive: Path, wanted: dict[str, Path]) -> set[str]:
    """Pull `{zip entry basename: destination}` out of `archive`. Returns the
    basenames that were found, so the caller can report the ones that were not."""
    found: set[str] = set()
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            base = info.filename.rsplit("/", 1)[-1]
            destination = wanted.get(base)
            if destination is None or base in found:
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as source, destination.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            found.add(base)
    return found


def build_jre(java_home: Path, out: Path) -> dict[str, str]:
    """jlink, and then PROVE the result runs.

    The `java -version` is not decoration: jlink is perfectly happy to emit a
    runtime image for a module set the jar cannot actually start on, and a
    cross-compiled or mismatched `--module-path` produces an image that dies
    with a linker error the first time anything invokes it -- which, without
    this, would be inside a GPU render.
    """
    jlink = java_home / "bin" / ("jlink.exe" if os.name == "nt" else "jlink")
    subprocess.run(
        [
            str(jlink),
            "--add-modules", JLINK_MODULES,
            "--strip-debug",
            "--no-header-files",
            "--no-man-pages",
            "--compress=zip-9",
            "--output", str(out),
        ],
        check=True,
    )
    java = out / "bin" / ("java.exe" if os.name == "nt" else "java")
    version = subprocess.run(
        [str(java), "-version"], check=True, capture_output=True, text=True
    ).stderr.strip()
    release = {}
    release_file = out / "release"
    if release_file.is_file():
        for line in release_file.read_text(encoding="utf-8", errors="replace").splitlines():
            key, _, value = line.partition("=")
            release[key] = value.strip('"')
    return {
        "version": version.splitlines()[0] if version else "",
        "java_version": release.get("JAVA_VERSION", ""),
        "os_arch": release.get("OS_ARCH", ""),
        "os_name": release.get("OS_NAME", ""),
    }


def directory_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, choices=sorted(PLATFORMS))
    parser.add_argument("--jar", required=True, type=Path, help="ta-render.jar as built")
    parser.add_argument("--out", required=True, type=Path, help="payload directory to create")
    parser.add_argument("--skiko-jar", type=Path, required=True,
                        help="skiko-awt-runtime-<os>-<arch> jar for --platform")
    parser.add_argument("--angle-dir", type=Path,
                        help="directory holding libEGL + libGLESv2 (macOS/Windows only)")
    # Windows' ANGLE is a published Maven artifact
    # (org.jetbrains.skiko:skiko-awt-runtime-angle-windows-<arch>), so the leg can fetch it
    # and hand the jar straight over. macOS has no such artifact -- backlog C5 -- which is
    # exactly why --angle-dir exists as a separate door.
    parser.add_argument("--angle-jar", type=Path,
                        help="jar to extract libEGL/libGLESv2 from, e.g. Skiko's "
                             "skiko-awt-runtime-angle-windows-x64")
    parser.add_argument("--java-home", type=Path,
                        default=Path(os.environ.get("JAVA_HOME", "")),
                        help="JDK to jlink FROM; must target --platform (jlink cannot cross-target)")
    parser.add_argument("--platform-tag", help="wheel platform tag; defaults per --platform")
    args = parser.parse_args()

    spec = PLATFORMS[args.platform]
    tag = args.platform_tag or spec["default_tag"]
    out: Path = args.out
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    # 1. The jar, minus Skiko's host-specific natives.
    jar_out = out / "ta-render.jar"
    removed = strip_skiko_natives(args.jar, jar_out)
    print(f"jar: dropped {len(removed)} skiko entr{'y' if len(removed) == 1 else 'ies'}: "
          f"{removed or '(none -- jar was already skiko-free)'}")

    # 2. LWJGL's JNI shims for this platform, flattened out of the jar.
    natives = out / "natives"
    natives.mkdir()
    prefix, ext = spec["prefix"], spec["ext"]
    lwjgl_names = {f"{prefix}lwjgl{ext}": natives / f"{prefix}lwjgl{ext}",
                   f"{prefix}lwjgl_opengles{ext}": natives / f"{prefix}lwjgl_opengles{ext}"}
    # Scoped to this platform's nest so `macos/x64/...liblwjgl.dylib` cannot
    # satisfy a macos-arm64 payload -- the jar carries all six.
    with zipfile.ZipFile(args.jar) as zf:
        wanted_paths = {
            f"{spec['lwjgl_dir']}/org/lwjgl/{prefix}lwjgl{ext}": lwjgl_names[f"{prefix}lwjgl{ext}"],
            f"{spec['lwjgl_dir']}/org/lwjgl/opengles/{prefix}lwjgl_opengles{ext}":
                lwjgl_names[f"{prefix}lwjgl_opengles{ext}"],
        }
        present = set(zf.namelist())
        missing = sorted(set(wanted_paths) - present)
        if missing:
            print(f"ERROR: {args.jar} carries no LWJGL natives for {args.platform}: {missing}",
                  file=sys.stderr)
            print("       host/build.gradle.kts must declare all six classifiers (commit c8dcfec).",
                  file=sys.stderr)
            return 1
        for entry, destination in wanted_paths.items():
            with zf.open(entry) as source, destination.open("wb") as handle:
                shutil.copyfileobj(source, handle)

    # 3. Skiko's libskia, under the exact name its loader maps.
    skiko_library = f"{prefix}skiko-{spec['skiko_id']}{ext}"
    wanted = {skiko_library: natives / skiko_library}
    if spec["ext"] == ".dll":
        wanted[SKIKO_WINDOWS_SIDECAR] = natives / SKIKO_WINDOWS_SIDECAR
    found = extract_matching(args.skiko_jar, wanted)
    if skiko_library not in found:
        print(f"ERROR: {args.skiko_jar} does not contain {skiko_library}", file=sys.stderr)
        return 1
    if spec["ext"] == ".dll" and SKIKO_WINDOWS_SIDECAR not in found:
        # Not fatal here so the failure names the real cause at smoke time, but
        # loud: with skiko.library.path set, Skiko looks for it beside the library.
        print(f"WARNING: {SKIKO_WINDOWS_SIDECAR} not found in {args.skiko_jar}", file=sys.stderr)

    # 4. ANGLE, when this platform has no system GLES.
    #
    # Both files land in ONE flat directory, and that is a hard requirement rather than
    # tidiness: ANGLE's libEGL loads libGLESv2 BY BARE NAME out of its own module directory,
    # so a split payload does not fail cleanly, it SIGSEGVs (plan §9.6, GlDriver.jvm.kt's
    # header). Everything else in the source directory/jar is copied alongside them, because
    # a Windows ANGLE may also want d3dcompiler_47.dll beside it.
    angle_files: list[str] = []
    if args.angle_dir or args.angle_jar:
        if not spec["angle"]:
            print(f"ERROR: bundled ANGLE given for {args.platform}, which renders through "
                  "the system EGL/GLES. A bundled payload there takes the GPU worker off "
                  "the NVIDIA driver.", file=sys.stderr)
            return 1
        angle_out = out / "angle"
        angle_out.mkdir()
        if args.angle_dir:
            for entry in sorted(args.angle_dir.iterdir()):
                if entry.is_file():
                    shutil.copy2(entry, angle_out / entry.name)
                    angle_files.append(entry.name)
        else:
            with zipfile.ZipFile(args.angle_jar) as zf:
                for info in zf.infolist():
                    base = info.filename.rsplit("/", 1)[-1]
                    if info.is_dir() or base.startswith("META-INF") or not base:
                        continue
                    if base.endswith((".sha256", ".sha1", ".md5")):
                        continue
                    with zf.open(info) as source, (angle_out / base).open("wb") as handle:
                        shutil.copyfileobj(source, handle)
                    angle_files.append(base)
        egl = [n for n in angle_files if n.startswith(("libEGL", "EGL"))]
        gles = [n for n in angle_files if n.startswith(("libGLESv2", "GLESv2"))]
        if not egl or not gles:
            source = args.angle_dir or args.angle_jar
            print(f"ERROR: {source} must hold BOTH an EGL and a GLESv2 library; "
                  f"found {angle_files}", file=sys.stderr)
            return 1
    elif spec["angle"]:
        # macOS/Windows have no system EGL at all: GlDriver throws rather than
        # falling back, so this wheel would fail at `GlContext.createOffscreen`.
        print(f"ERROR: {args.platform} has no system EGL/GLES and no --angle-dir was given. "
              "This payload cannot render. (macOS: backlog C5 -- our own ANGLE build; "
              "Chrome's dylibs are not redistributable.)", file=sys.stderr)
        return 1

    # 5. The JRE.
    jre = build_jre(args.java_home, out / "jre")

    manifest = {
        "schema_version": 1,
        "platform": args.platform,
        "platform_tag": tag,
        "jar": {
            "name": "ta-render.jar",
            "sha256": sha256(jar_out),
            "source_sha256": sha256(args.jar),
            "skiko_entries_removed": removed,
        },
        "jre": jre,
        "jlink_modules": JLINK_MODULES.split(","),
        "natives": sorted(p.name for p in natives.iterdir()),
        "angle": angle_files,
        "bytes": {
            "total": directory_bytes(out),
            "jar": jar_out.stat().st_size,
            "jre": directory_bytes(out / "jre"),
            "natives": directory_bytes(natives),
            "angle": directory_bytes(out / "angle") if angle_files else 0,
        },
    }
    (out / "PAYLOAD.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest["bytes"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
