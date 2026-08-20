#!/usr/bin/env python3
"""Assemble ONE platform's JVM payload for the public `travel-animator` wheel.

Output layout, which is also what `tada_render.render_bridge` looks for and
`verify_tada_wheel.py` asserts:

    <out>/
      ta-render.jar     the renderer jar, with the Skiko natives REMOVED
      jre/              jlink'd, `JLINK_MODULES` wide, probed before it ships
      natives/          LWJGL's two JNI shims + Skiko's libskia, for THIS platform
      angle/            libEGL + libGLESv2         (macOS/Windows only, from the
                        pinned SHA-256-verified release asset -- never built here)
      PAYLOAD.json      provenance -- platform tag, jar sha256, JRE version

The jar is architecture-neutral for LWJGL but NOT for Skiko: `shared/build.gradle.kts`
resolves `skiko-awt-runtime-$hostOs-$hostArch` from the BUILD MACHINE, so shipping it
as-is puts a Linux libskia in a macOS wheel and throws `LibraryLoadException` at the
first overlay draw. Hence the rewrite here, with the right native staged into
`natives/` instead.

`natives/` is a real directory rather than a temp dir because both LWJGL and Skiko
otherwise self-extract 20 MB from the classpath into `$TMPDIR` on every render.
Skiko resolves `File(path, System.mapLibraryName("skiko-$hostId"))`, so its file must
keep its `libskiko-linux-x64.so` name and NOT be renamed (checked against
`LibraryLoader.findAndLoadLibrary` in skiko 0.148.2).

`angle/` is absent on Linux on purpose: `GlDriver.selectNativeLibraries` falls
through to the system `libEGL.so.1`/`libGLESv2.so.2` only when it finds no bundled
payload, and staging one there would take the container off the NVIDIA driver.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

# Platform table. Three vendors spell the same six platforms three different ways
# and none of them is the wheel tag; a literal table fails loudly where a
# derivation would be silently wrong.
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

# jdeps' set, plus jdk.crypto.ec, which nothing references statically.
#
# `java.desktop` is deliberately NOT here, and its absence is worth 14.5 MB of every
# platform wheel -- measured A/B on macos-arm64, same jar and same base wheel, 82.0 MiB
# to 67.6 MiB, against a 100 MB PyPI per-file limit the published wheels were within
# 16 MB of. The linked JRE goes from 53.8 MB to 34.1 MB on disk (62.7 to 42.9 on
# linux-arm64). It used to be listed for "Skiko's AWT overlay rasteriser", but the
# renderer never enters AWT: there is not one reference to
# `java.awt`, `javax.imageio`, `BufferedImage` or `ImageIO` in `shared/commonMain`,
# `shared/jvmMain` or `host/src/main`, and every image is decoded through Skia's own
# `Codec` / `Image.makeFromEncoded`. The module was pulled in because the ARTIFACT is
# `skiko-awt`, not because anything calls it -- jdeps sees the AWT-integration classes
# in that jar whether or not a code path reaches them.
#
# Skiko's raster surface, its PNG codec and its text measurement are all native Skia
# below the JNI boundary, so none of them needs the module. `_PROBE_SOURCE` now proves
# that ON the linked image for every platform, which is what makes this exclusion safe
# to hold: jdeps cannot tell "referenced by a jar" from "reached at run time", so the
# probe is the only thing that can. If a future `:shared` change genuinely reaches AWT,
# the probe fails at build time here rather than as a NoClassDefFoundError in a
# customer's render.
JLINK_MODULES = ",".join((
    "java.base",
    "java.instrument",
    "java.logging",     # OkHttp's TaskRunner.<clinit>
    "java.management",
    "jdk.crypto.ec",    # SunEC: no ECDHE without it
    "jdk.unsupported",  # sun.misc.Unsafe, for okio and LWJGL
))

# Modules jdeps reports against ta-render.jar that the renderer never reaches, and which
# are therefore deliberately NOT in `JLINK_MODULES`. Subtracted from the jdeps gate below.
#
# jdeps is a static reference scan over every class in the jar, so it reports
# `java.desktop` on account of the AWT-integration classes skiko-awt carries -- SkiaWindow
# and friends -- which no render path loads. There is no jdeps flag for "reachable from
# TadaHostMain", so the reference cannot be cleared statically; it is cleared by running
# the drawing stack on the linked image, which is what `_PROBE_SOURCE` now does.
#
# Never add a module here without also adding a probe assertion that exercises whatever it
# was referenced for. An entry with no probe behind it converts a build-time failure into
# a NoClassDefFoundError in somebody's render.
REFERENCED_NOT_REACHED = frozenset({"java.desktop"})

# Runs ON the linked image, offline. `java -version` cannot fail on a missing module.
#
# The Skia half of this exists because `java.desktop` is NOT in `JLINK_MODULES` (see the
# note there). jdeps cannot distinguish "the skiko-awt jar references AWT" from "the
# renderer reaches AWT", so it always demands the module and can never clear it; only
# running the drawing stack on the linked image can. Every call below is one the real
# render performs -- a raster surface (the overlay rasteriser), a paint fill, a PNG
# encode and a PNG decode through `Image.makeFromEncoded`, which is the exact entry
# point `AnnotationRasterizer` uses for raster annotation frames.
_PROBE_CLASS = "PayloadProbe"
_PROBE_SOURCE = """\
import java.security.Security;
import java.util.Arrays;
import javax.net.ssl.SSLContext;
import org.jetbrains.skia.EncodedImageFormat;
import org.jetbrains.skia.Image;
import org.jetbrains.skia.Paint;
import org.jetbrains.skia.Surface;

public class PayloadProbe {
  public static void main(String[] args) throws Exception {
    new okhttp3.OkHttpClient();
    if (Security.getProvider("SunEC") == null) {
      System.err.println("SunEC provider absent -- jdk.crypto.ec is not in the image");
      System.exit(1);
    }
    long ecdhe = Arrays.stream(
            SSLContext.getDefault().getSocketFactory().getSupportedCipherSuites())
        .filter(suite -> suite.contains("ECDHE"))
        .count();
    if (ecdhe == 0) {
      System.err.println("no ECDHE cipher suites -- this image cannot reach the basemap host");
      System.exit(1);
    }

    // Not a minimality nicety: java.desktop is 14.5 MB of a wheel that has to stay under
    // PyPI's 100 MB, and re-adding it costs that silently. If something genuinely needs
    // AWT now, put it back in JLINK_MODULES and delete this check in the same commit.
    if (ModuleLayer.boot().findModule("java.desktop").isPresent()) {
      System.err.println("java.desktop is in this image -- it costs 14.5 MB and the "
          + "renderer never enters AWT. Remove it from JLINK_MODULES.");
      System.exit(1);
    }

    // Skia raster + codec, the two things java.desktop was blamed for.
    Surface surface = Surface.Companion.makeRasterN32Premul(64, 48);
    Paint paint = new Paint();
    paint.setColor(0xFF3366CC);
    surface.getCanvas().drawRect(
        org.jetbrains.skia.Rect.Companion.makeXYWH(4f, 4f, 40f, 24f), paint);
    byte[] png = surface.makeImageSnapshot()
        .encodeToData(EncodedImageFormat.PNG, 100)
        .getBytes();
    Image decoded = Image.Companion.makeFromEncoded(png);
    if (decoded.getWidth() != 64 || decoded.getHeight() != 48) {
      System.err.println("Skia round-trip gave " + decoded.getWidth() + "x"
          + decoded.getHeight() + ", expected 64x48");
      System.exit(1);
    }
    System.out.println("okhttp + " + ecdhe + " ECDHE suites + skia raster/codec ("
        + png.length + "B png, no java.desktop)");
  }
}
"""

# Windows only. With `skiko.library.path` set Skiko looks for this beside the library
# rather than unpacking it, so it has to be staged or the first text measurement fails.
SKIKO_WINDOWS_SIDECAR = "icudtl.dat"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strip_skiko_natives(source: Path, target: Path) -> list[str]:
    """Copy `source` to `target` without any `libskiko-*` / `skiko-*` entry.

    Writing back the ORIGINAL `ZipInfo` keeps timestamps and external attributes
    intact, so the jar stays byte-stable across runs for a given input -- the
    wheel's RECORD hash of this file is what a reproducibility check compares.
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


def module_names(command: list[str]) -> set[str]:
    """`--list-modules` / `--print-module-deps` output as a set of bare module names."""
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    raw = result.stdout.replace(",", "\n").splitlines()
    return {line.split("@", 1)[0].strip() for line in raw if line.strip()}


def probe_image(java_home: Path, java: Path, jar: Path, natives: Path) -> str:
    """Compile the probe with the JDK we linked FROM, run it on the linked image.

    `natives` is the staged `natives/` directory, passed as `skiko.library.path`: the jar
    reaching this point has had its Skiko libraries stripped out, so without it Skiko has
    nothing to load and the Skia half of the probe fails for a packaging reason rather
    than a real one. It is the same property `render_bridge` sets at run time, so the
    probe loads the library exactly the way a render will.

    Because it loads libskiko for real, this probe needs whatever that library links
    against to be present in the BUILD environment -- on Linux that is `libGL.so.1`,
    `libX11.so.6` and `libfontconfig.so.1` (see `build_linux_wheel.sh`, which explains why
    they come from the host and are never vendored). `quay.io/pypa/manylinux_2_28_*`, the
    only container this runs in, carries all three, and a bare JDK image does not: moving
    the Linux build to a slimmer image means installing them, or this probe fails with an
    `UnsatisfiedLinkError` naming the missing library rather than anything about modules.
    """
    javac = java_home / "bin" / ("javac.exe" if os.name == "nt" else "javac")
    with tempfile.TemporaryDirectory() as raw:
        workdir = Path(raw)
        source = workdir / f"{_PROBE_CLASS}.java"
        source.write_text(_PROBE_SOURCE, encoding="utf-8")
        subprocess.run(
            [str(javac), "-cp", str(jar), "-d", str(workdir), str(source)], check=True
        )
        result = subprocess.run(
            [
                str(java),
                f"-Dskiko.library.path={natives}",
                "-cp", f"{jar}{os.pathsep}{workdir}",
                _PROBE_CLASS,
            ],
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        raise SystemExit(
            f"ERROR: the linked JRE cannot run the renderer's HTTP/TLS or drawing stack "
            f"(exit {result.returncode}). Add the module it names to JLINK_MODULES.\n"
            f"{result.stdout}{result.stderr}".rstrip()
        )
    return result.stdout.strip()


def build_jre(java_home: Path, out: Path, jar: Path, natives: Path) -> dict[str, str]:
    """jlink, then prove the image can run `jar`: jdeps check, then the probe."""
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

    # `base` only if the release file is unreadable.
    feature = release.get("JAVA_VERSION", "").split(".")[0] or "base"
    jdeps = java_home / "bin" / ("jdeps.exe" if os.name == "nt" else "jdeps")
    referenced = module_names(
        [str(jdeps), "--multi-release", feature, "--ignore-missing-deps",
         "--print-module-deps", str(jar)]
    )
    absent = sorted(
        referenced - module_names([str(java), "--list-modules"]) - REFERENCED_NOT_REACHED
    )
    if absent:
        raise SystemExit(
            f"ERROR: {jar.name} references modules the linked JRE does not carry: "
            f"{absent}. Add them to JLINK_MODULES."
        )
    print(f"jre: probe passed -- {probe_image(java_home, java, jar, natives)}")
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
    # THE ONLY DOOR ANGLE COMES THROUGH, on purpose. Do not add a convenience flag that
    # unpacks Skiko's published ANGLE artifact: it is a different revision, and two
    # revisions is two rasterisations of the same frame, shipped silently in a wheel.
    # This directory comes from the SHA-256-pinned release asset built from the one
    # pinned revision -- see scripts/angle/fetch_pinned_angle.py.
    parser.add_argument("--angle-dir", type=Path,
                        help="directory holding libEGL + libGLESv2 from the pinned, "
                             "checksum-verified release asset (macOS/Windows only)")
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
        # Not fatal here so the failure names the real cause at smoke time.
        print(f"WARNING: {SKIKO_WINDOWS_SIDECAR} not found in {args.skiko_jar}", file=sys.stderr)

    # 4. ANGLE, when this platform has no system GLES. Both files must land in ONE flat
    # directory: ANGLE's libEGL loads libGLESv2 BY BARE NAME out of its own module
    # directory, so a split payload SIGSEGVs rather than failing cleanly. Sibling
    # top-level files come too -- Windows also needs d3dcompiler_47.dll and the LICENSE.
    angle_files: list[str] = []
    if args.angle_dir:
        if not spec["angle"]:
            print(f"ERROR: bundled ANGLE given for {args.platform}, which renders through "
                  "the system EGL/GLES. A bundled payload there takes the GPU worker off "
                  "the NVIDIA driver.", file=sys.stderr)
            return 1
        angle_out = out / "angle"
        angle_out.mkdir()
        for entry in sorted(args.angle_dir.iterdir()):
            if entry.is_file():
                shutil.copy2(entry, angle_out / entry.name)
                angle_files.append(entry.name)
        egl = [n for n in angle_files if n.startswith(("libEGL", "EGL"))]
        gles = [n for n in angle_files if n.startswith(("libGLESv2", "GLESv2"))]
        if not egl or not gles:
            print(f"ERROR: {args.angle_dir} must hold BOTH an EGL and a GLESv2 library at its "
                  f"top level; found {angle_files}", file=sys.stderr)
            return 1
    elif spec["angle"]:
        # macOS/Windows have no system EGL at all: GlDriver throws rather than
        # falling back, so this wheel would fail at `GlContext.createOffscreen`.
        print(f"ERROR: {args.platform} has no system EGL/GLES and no --angle-dir was given. "
              "This payload cannot render. ANGLE is not built here -- it is a pinned, "
              "SHA-256-verified release asset; see scripts/angle/fetch_pinned_angle.py and "
              "the PROVENANCE documents beside it.", file=sys.stderr)
        return 1

    # 5. The JRE, checked against the jar.
    jre = build_jre(args.java_home, out / "jre", jar_out, natives)

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
