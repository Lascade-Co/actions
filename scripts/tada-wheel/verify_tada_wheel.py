"""Assert the TADA wheels carry what they must, carry no credentials, and -- for
a platform wheel -- carry a JVM payload that will actually start.

TADA ships two distributions from one revision: private `tada` (choreography builder and
credentialed fetchers) and public `travel-animator` (render engine and safe CLI). The
generated protobuf modules and bundled assets live in the public one. The public wheel is
selected by its DISTRIBUTION name (`travel_animator-*.whl`, which escapes the dash) but its
contents live under its IMPORT path (`tada_render/`).

The credential scan runs over BOTH wheels. It matters most for travel-animator, the one
that is publishable to a public index.

---------------------------------------------------------------------------
Backlog C8: the count is no longer two
---------------------------------------------------------------------------
`len(all_wheels) != 2` was correct when the bundle held one private wheel and one pure
public one. A platform matrix makes the public count N, so the assertion becomes: exactly
one private wheel, at least one public wheel, and nothing else. Each half is checked
separately, because "two files" was never the property that mattered -- "exactly one
private wheel and it is the one we think" is.

---------------------------------------------------------------------------
Backlog C11: the JVM assertions
---------------------------------------------------------------------------
Everything below the credential scan is new, and each check exists because of a specific
failure that is silent without it:

**The nested-ZIP builder seal.** Gradle's `verifyTaRenderJarSeal` proves that the jar
*Gradle built* carries no `com/lascade/ta/shared/builder/**` class. It says nothing about
the jar *in the wheel*, and between the two sit an artifact upload, an artifact download,
and `build_jvm_payload.py` rewriting the jar to strip Skiko's natives. Any of those could
in principle substitute or corrupt it, and ADR 0006/0007 exist because JVM bytecode
decompiles trivially: a leak here publishes the choreography code to PyPI. So the seal is
re-run here, on the shipped bytes, with the same two positive controls Gradle uses -- the
renderer prefix must be PRESENT, or an empty jar would pass a "no builder classes" test
while proving nothing.

**Class-file major 65.** `:host` and `:shared` both pin `jvmTarget = JVM_21`, but a build
running on a newer JDK that ignored or lost that pin emits major 66+, and the jlink'd JRE 21
this wheel ships then throws `UnsupportedClassVersionError` at the first render -- a runtime
failure from a build that was green. Checking it at package time is the only place the two
halves (what was compiled, what will run it) are both in hand.

**Natives in the same directory.** `GlDriver.jvm.kt`'s header: ANGLE's `libEGL` loads
`libGLESv2` BY BARE NAME from its own module directory, so a split payload does not fail
cleanly -- it SIGSEGVs (plan §9.6). The pair must be co-located, and the LWJGL shims must be
where `-Dorg.lwjgl.librarypath` points, which is one directory.

**Linux must have NO angle/.** It is the presence of a bundled payload that switches
`GlDriver` off the system EGL/GLES, and the system path is the one the NVIDIA GPU worker
needs. An `angle/` directory that arrived on a Linux wheel by accident would take the paid
GPU off its own driver.
"""
from pathlib import PurePosixPath
from zipfile import ZipFile
import glob
import io
import json
import re
import sys
import zipfile

# `tada-*.whl` and `travel_animator-*.whl` are disjoint: a wheel filename starts with its
# distribution name, and these two names share no prefix.
PRIVATE_GLOB = "bundle/tada-*.whl"
PUBLIC_GLOB = "bundle/travel_animator-*.whl"

REQUIRED_PRIVATE = {
    # The private package has no generated modules of its own; pin a few load-bearing
    # modules so a catastrophically empty or mis-packaged wheel still fails here.
    "tada/__init__.py",
    "tada/cli.py",
    "tada/bundle.py",
}
REQUIRED_PUBLIC = {
    "tada_render/config/proto/export_config_pb2.py",
    "tada_render/config/proto/animation_state_pb2.py",
    "tada_render/config/proto/animation_style_pb2.py",
    "tada_render/config/proto/route_pb2.py",
    "tada_render/config/proto/mcp_options_pb2.py",
    "tada_render/assets/mvt_pb2.py",
    "tada_render/animation/frame_plan_pb2.py",
    "tada_render/assets/bundled/countries.geoscade",
    "tada_render/assets/bundled/fonts/interbold.ttf",
    "tada_render/assets/bundled/flags/1x1/us.svg",
    "tada_render/assets/bundled/flags/4x3/us.svg",
    "tada_render/assets/bundled/watermark/watermark-0.png",
}

CREDENTIAL_NAMES = {".env", ".npmrc", ".pypirc", "credentials"}
CREDENTIAL_SUFFIXES = (".jks", ".key", ".p12", ".pem")

# ---------------------------------------------------------------------- JVM payload
PAYLOAD = "tada_render/_jvm/"
JAR = PAYLOAD + "ta-render.jar"
MANIFEST = PAYLOAD + "PAYLOAD.json"

# ADR 0006/0007. Must be ABSENT from ta-render.jar.
SEALED_PREFIX = "com/lascade/ta/shared/builder/"
# ...and this must be PRESENT, or the jar under test is not a renderer jar and the seal
# above passed by being empty.
REQUIRED_PREFIX = "com/lascade/ta/shared/render/"

# Java 21. `:host/build.gradle.kts` pins `jvmTarget.set(JvmTarget.JVM_21)` for exactly this
# reason; the jlink'd runtime is 21 and will not load anything higher.
CLASS_FILE_MAJOR = 65

# Per platform tag: (native library suffix, whether a bundled ANGLE is REQUIRED).
# Linux is the only row with `False`, and it is `False` in the strong sense -- an `angle/`
# there is a failure, not an extra.
PLATFORM_RULES = [
    (re.compile(r"manylinux_\d+_\d+_(x86_64|aarch64)$"), ".so", False),
    (re.compile(r"musllinux_\d+_\d+_(x86_64|aarch64)$"), ".so", False),
    (re.compile(r"macosx_\d+_\d+_(arm64|x86_64|universal2)$"), ".dylib", True),
    (re.compile(r"win_(amd64|arm64)$"), ".dll", True),
]


def fail(message: str) -> None:
    raise SystemExit(message)


def platform_tag_of(wheel: str) -> str:
    return PurePosixPath(wheel).stem.split("-")[-1]


def check_credentials(wheel: str, names: set[str]) -> None:
    forbidden = []
    for name in names:
        path = PurePosixPath(name)
        lowered = name.lower()
        if (
            any(part.lower() in CREDENTIAL_NAMES for part in path.parts)
            or lowered.endswith(CREDENTIAL_SUFFIXES)
            or ".git" in {part.lower() for part in path.parts}
        ):
            forbidden.append(name)
    if forbidden:
        fail(f"{wheel} contains credential-like files: {sorted(forbidden)}")


def check_jar(wheel: str, jar_bytes: bytes) -> None:
    """Re-run the ADR 0007 seal, and the class-file version check, on the SHIPPED jar.

    A nested ZIP: the wheel is a zip, the jar inside it is a zip, and this reads the inner
    one straight out of memory rather than extracting to disk -- so what is checked is
    literally the bytes that will be installed, with no filesystem step in between that
    could substitute them.
    """
    sealed: list[str] = []
    renderer = 0
    majors: dict[int, int] = {}
    sampled: list[str] = []

    with ZipFile(io.BytesIO(jar_bytes)) as jar:
        for info in jar.infolist():
            name = info.filename
            if not name.endswith(".class"):
                continue
            if name.startswith(SEALED_PREFIX):
                sealed.append(name)
            if name.startswith(REQUIRED_PREFIX):
                renderer += 1
                # Read the class-file header of a bounded sample rather than all ~8,000:
                # every class in this jar came out of the same compilation, so a
                # disagreement between them is not the failure mode -- a whole jar built to
                # the wrong target is. 40 spread across the renderer package is plenty, and
                # keeps this check well under a second.
                if len(sampled) < 40 and renderer % 50 == 1:
                    header = jar.read(name)[:8]
                    if header[:4] != b"\xca\xfe\xba\xbe":
                        fail(f"{wheel}: {name} is not a class file")
                    major = int.from_bytes(header[6:8], "big")
                    majors[major] = majors.get(major, 0) + 1
                    sampled.append(name)

    if sealed:
        fail(
            f"{wheel} ships a ta-render.jar carrying {len(sealed)} class(es) under "
            f"'{SEALED_PREFIX}'. The public wheel must not contain the choreography "
            f"builder (ADR 0006/0007, plan §4.1). First offenders: {sealed[:10]}"
        )
    if renderer == 0:
        fail(
            f"{wheel} ships a ta-render.jar with NO class under '{REQUIRED_PREFIX}', so it "
            "is not a renderer jar and the seal check above proved nothing."
        )
    if not sampled:
        fail(f"{wheel}: no class files sampled from ta-render.jar")
    wrong = {major: count for major, count in majors.items() if major != CLASS_FILE_MAJOR}
    if wrong:
        fail(
            f"{wheel} ships classes compiled to class-file major {sorted(wrong)} "
            f"(expected {CLASS_FILE_MAJOR} = Java 21). The jlink'd JRE in this same wheel "
            "is 21 and would throw UnsupportedClassVersionError at the first render. "
            "Check jvmTarget in host/build.gradle.kts and shared/build.gradle.kts."
        )
    print(f"    jar: seal OK ({renderer} renderer classes, 0 builder), "
          f"class-file major {CLASS_FILE_MAJOR} across {len(sampled)} sampled")


def check_payload(wheel: str, names: set[str], archive: ZipFile) -> None:
    tag = platform_tag_of(wheel)
    suffix = None
    angle_required = None
    for pattern, native_suffix, needs_angle in PLATFORM_RULES:
        if pattern.search(tag):
            suffix, angle_required = native_suffix, needs_angle
            break
    if suffix is None:
        fail(f"{wheel}: unrecognised platform tag {tag!r}; add it to PLATFORM_RULES "
             "rather than letting an unverified payload through")

    # 1. The jar, and the JRE that must run it.
    if JAR not in names:
        fail(f"{wheel} carries no {JAR}; a platform-tagged wheel with no payload is worse "
             "than a pure one, because it shadows nothing and delivers nothing")
    java = f"{PAYLOAD}jre/bin/java"
    java_exe = f"{PAYLOAD}jre/bin/java.exe"
    if java not in names and java_exe not in names:
        fail(f"{wheel} carries no jlink'd JRE ({java})")
    if f"{PAYLOAD}jre/lib/modules" not in names:
        fail(f"{wheel}'s JRE has no lib/modules; jlink produced no runtime image")

    # 2. Natives, all in ONE directory (see this module's docstring).
    natives = sorted(
        n[len(PAYLOAD) + len("natives/"):]
        for n in names
        if n.startswith(PAYLOAD + "natives/") and not n.endswith("/")
    )
    lwjgl = [n for n in natives if "lwjgl" in n]
    skiko = [n for n in natives if "skiko" in n]
    if len(lwjgl) < 2:
        fail(f"{wheel}: expected LWJGL's two JNI shims in {PAYLOAD}natives/, found {lwjgl}. "
             "Without them the first EGL call dies with UnsatisfiedLinkError, which reads "
             "as a missing GL driver rather than a packaging bug.")
    if len(skiko) != 1 or not skiko[0].endswith(suffix):
        fail(f"{wheel}: expected exactly one libskiko{suffix} in {PAYLOAD}natives/, "
             f"found {skiko}. build_jvm_payload.py strips the jar's host-specific Skiko "
             "natives, so this staged copy is the ONLY one -- a missing or wrong-platform "
             "file is a LibraryLoadException at the first overlay draw.")
    if any("/" in n for n in natives):
        fail(f"{wheel}: {PAYLOAD}natives/ has subdirectories ({natives}); "
             "-Dorg.lwjgl.librarypath names one directory, not a tree")

    # 3. ANGLE: present and co-located on macOS/Windows, ABSENT on Linux.
    angle = sorted(
        n[len(PAYLOAD) + len("angle/"):]
        for n in names
        if n.startswith(PAYLOAD + "angle/") and not n.endswith("/")
    )
    if angle_required:
        egl = [n for n in angle if n.startswith(("libEGL", "EGL"))]
        gles = [n for n in angle if n.startswith(("libGLESv2", "GLESv2"))]
        if not egl or not gles:
            fail(f"{wheel}: {tag} has no system EGL/GLES, so it needs a bundled ANGLE pair "
                 f"in {PAYLOAD}angle/; found {angle}")
        if any("/" in n for n in angle):
            fail(f"{wheel}: {PAYLOAD}angle/ must be FLAT -- ANGLE's libEGL loads libGLESv2 "
                 "by bare name from its own directory, and a split payload SIGSEGVs rather "
                 f"than failing cleanly (plan §9.6). Found {angle}")
    elif angle:
        fail(f"{wheel}: {tag} renders through the SYSTEM libEGL.so.1/libGLESv2.so.2, and it "
             f"is the presence of {PAYLOAD}angle/ that switches GlDriver off it. Shipping "
             f"one here takes the NVIDIA GPU worker off its own driver. Found {angle}")

    # 4. The seal + class-file version, on the shipped jar.
    check_jar(wheel, archive.read(JAR))

    if MANIFEST in names:
        manifest = json.loads(archive.read(MANIFEST))
        if manifest.get("platform_tag") != tag:
            fail(f"{wheel}: PAYLOAD.json says platform_tag="
                 f"{manifest.get('platform_tag')!r} but the wheel is tagged {tag!r}")
        print(f"    payload: {manifest['platform']} "
              f"jre={manifest['jre'].get('java_version')} "
              f"{manifest['bytes']['total'] / 1e6:.0f} MB uncompressed")
    print(f"    natives: {natives}")
    print(f"    angle:   {angle or '(none -- system EGL/GLES)'}")


def main() -> int:
    private = sorted(glob.glob(PRIVATE_GLOB))
    public = sorted(glob.glob(PUBLIC_GLOB))
    if len(private) != 1:
        fail(f"expected exactly one private wheel ({PRIVATE_GLOB}), found {private}")
    if not public:
        fail(f"expected at least one public wheel ({PUBLIC_GLOB}), found none")
    stray = sorted(set(glob.glob("bundle/*.whl")) - set(private) - set(public))
    if stray:
        fail(f"bundle/ holds wheels belonging to neither distribution: {stray}")

    print(f"{private[0]}")
    with ZipFile(private[0]) as archive:
        names = set(archive.namelist())
    missing = sorted(REQUIRED_PRIVATE - names)
    if missing:
        fail(f"{private[0]} is missing expected modules: {missing}")
    check_credentials(private[0], names)
    # The private wheel is server-side only and stays pure. If it ever grows a payload the
    # bundle's file count and TARS's shape checks both change, so say so here.
    leaked = sorted(n for n in names if n.startswith("tada_render/_jvm/"))
    if leaked:
        fail(f"{private[0]} carries a JVM payload ({leaked[:3]}); it belongs in the public "
             "wheel, which is the one that owns tada_render/")

    for wheel in public:
        tag = platform_tag_of(wheel)
        print(f"{wheel}  [{tag}]")
        with ZipFile(wheel) as archive:
            names = set(archive.namelist())
            missing = sorted(REQUIRED_PUBLIC - names)
            if missing:
                fail(f"{wheel} is missing expected modules: {missing}")
            check_credentials(wheel, names)
            if tag == "any":
                # Not an error here -- publish-tada-wheel.yml's own bundle step decides
                # whether a pure wheel is acceptable, and stage_public_wheel.sh refuses one
                # outright on the PyPI path. But say clearly what is not being checked.
                print("    py3-none-any: no JVM payload, none checked. "
                      "The renderer will fall back to `ta-render` on PATH.")
                continue
            check_payload(wheel, names, archive)

    print(f"OK: 1 private wheel, {len(public)} public wheel(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
