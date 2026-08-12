"""Assert the TADA wheels carry what they must, carry no credentials, and -- for
a platform wheel -- carry a JVM payload that will actually start.

TADA ships two distributions from one revision: private `tada` (choreography builder and
credentialed fetchers) and public `travel-animator` (render engine and safe CLI). The
generated protobuf modules and bundled assets live in the public one. The public wheel is
selected by its DISTRIBUTION name (`travel_animator-*.whl`, which escapes the dash) but its
contents live under its IMPORT path (`tada_render/`).

The credential scan runs over BOTH wheels. It matters most for travel-animator, the one
that is publishable to a public index.

The JVM assertions each exist because of a failure that is silent without them:

**The nested-ZIP builder seal, twice, in opposite directions.** Gradle's
`verifyTaRenderJarSeal` proves the jar GRADLE BUILT carries no
`com/lascade/ta/shared/builder/**`; between that and the jar in the wheel sit an artifact
round trip and `build_jvm_payload.py` rewriting it. JVM bytecode decompiles trivially (ADR
0006/0007), so a leak here publishes the choreography code to PyPI. Re-run on the shipped
bytes, with Gradle's positive control: the renderer prefix must be PRESENT, or an empty jar
passes a "no builder classes" test while proving nothing.

The PRIVATE wheel carries the other jar -- `ta-prepare.jar`, the same fat jar PLUS
`builder/` (ADR 0015) -- so there the same prefix must be PRESENT, and that assertion needs
no positive control of its own: presence cannot pass vacuously. A missing prepare jar is a
build failure here rather than a `prepare` that dies in a customer's dispatcher.

**Class-file major 65.** A build on a newer JDK that lost the `jvmTarget = JVM_21` pin emits
major 66+, and the jlink'd JRE 21 this wheel ships then throws `UnsupportedClassVersionError`
at the first render. Package time is the only place both halves are in hand. It binds the
private wheel's jar as hard as the public one's, and for the same runtime: `ta-prepare.jar`
has no JRE of its own, it runs on the one in the public wheel beside it.

**Natives in the same directory.** ANGLE's `libEGL` loads `libGLESv2` BY BARE NAME from its
own module directory, so a split payload SIGSEGVs rather than failing cleanly; the LWJGL
shims must also be where `-Dorg.lwjgl.librarypath` points, which is one directory.

**Linux must have NO angle/.** A bundled payload is what switches `GlDriver` off the system
EGL/GLES, so an `angle/` arriving on a Linux wheel takes the paid GPU off its own driver.
"""
from __future__ import annotations

from pathlib import PurePosixPath
from zipfile import ZipFile
import glob
import io
import json
import re
import sys

# Disjoint: a wheel filename starts with its distribution name, and these two share no prefix.
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

# The private wheel's payload: one jar, no JRE, no natives (ADR 0015). `tada/` is the
# private distribution's import package, so this installs as
# `<site-packages>/tada/_jvm/ta-prepare.jar`.
PREPARE_PAYLOAD = "tada/_jvm/"
PREPARE_JAR = PREPARE_PAYLOAD + "ta-prepare.jar"
PREPARE_MANIFEST = PREPARE_PAYLOAD + "PAYLOAD.json"

# ADR 0006/0007. Must be ABSENT from ta-render.jar.
SEALED_PREFIX = "com/lascade/ta/shared/builder/"
# ...and this must be PRESENT, or the jar under test is not a renderer jar and the seal
# above passed by being empty.
REQUIRED_PREFIX = "com/lascade/ta/shared/render/"

# host/build.gradle.kts pins each jar's `Main-Class` to its own program. Both jars are
# built from one module, so the entry point is what tells them apart -- and a manifest
# naming a class the jar does not carry is a `java -jar` that dies at runtime.
RENDER_MAIN_CLASS = "com.lascade.tada.host.TadaHostMain"
PREPARE_MAIN_CLASS = "com.lascade.tada.host.TadaPrepareMain"

# Java 21: the jlink'd runtime is 21 and will not load anything higher.
CLASS_FILE_MAJOR = 65

# How many class files to read a header from. A bounded sample rather than all ~8,000: the
# failure mode is a whole jar built to the wrong target, not disagreement within one
# compilation.
CLASS_SAMPLE_SIZE = 40

# Per platform tag: (native library suffix, whether a bundled ANGLE is REQUIRED). Linux's
# `False` is the strong sense -- an `angle/` there is a failure, not an extra.
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


def jar_main_class(jar: ZipFile) -> str | None:
    """`Main-Class` from a jar manifest, continuation lines joined first.

    A manifest wraps at 72 bytes and continues with a single leading space, so reading
    the raw lines would silently truncate a longer class name than today's and report the
    wrong entry point -- a false failure that reads like a real one.
    """
    try:
        raw = jar.read("META-INF/MANIFEST.MF").decode("utf-8", "replace")
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


def scan_jar(wheel: str, jar_name: str, jar_bytes: bytes, sample_prefix: str) -> dict:
    """Read the SHIPPED jar out of memory: its entry point, its class counts under both
    ADR 0007 prefixes, and the class-file major of a bounded sample under `sample_prefix`.

    Read from memory rather than extracted, so what is checked is literally the bytes that
    will be installed.
    """
    with ZipFile(io.BytesIO(jar_bytes)) as jar:
        names = jar.namelist()
        classes = [n for n in names if n.endswith(".class")]
        sealed = [n for n in classes if n.startswith(SEALED_PREFIX)]
        required = [n for n in classes if n.startswith(REQUIRED_PREFIX)]
        pool = sorted(n for n in classes if n.startswith(sample_prefix))
        # Spread the sample across the whole prefix rather than taking a prefix of it: a
        # jar built half on one JDK is not a thing, but a sample drawn from one directory
        # would not notice if it were.
        step = max(1, len(pool) // CLASS_SAMPLE_SIZE)
        majors: dict[int, int] = {}
        sampled: list[str] = []
        for name in pool[::step][:CLASS_SAMPLE_SIZE]:
            header = jar.read(name)[:8]
            if header[:4] != b"\xca\xfe\xba\xbe":
                fail(f"{wheel}: {jar_name} entry {name} is not a class file")
            major = int.from_bytes(header[6:8], "big")
            majors[major] = majors.get(major, 0) + 1
            sampled.append(name)
        main_class = jar_main_class(jar)

    # An empty sample is not failed here: `sample_prefix` is always the prefix its caller
    # is about to assert on, so "nothing to sample" and "wrong jar" are the same fact, and
    # the caller is the one that can say which jar it expected.
    wrong = {major: count for major, count in majors.items() if major != CLASS_FILE_MAJOR}
    if wrong:
        fail(
            f"{wheel} ships classes compiled to class-file major {sorted(wrong)} "
            f"(expected {CLASS_FILE_MAJOR} = Java 21). The jlink'd JRE that runs this jar "
            "is 21 and would throw UnsupportedClassVersionError at the first use. "
            "Check jvmTarget in host/build.gradle.kts and shared/build.gradle.kts."
        )
    return {
        "sealed": sealed,
        "required": required,
        "sampled": sampled,
        "main_class": main_class,
    }


def check_entry_point(wheel: str, jar_name: str, facts: dict, expected: str) -> None:
    if facts["main_class"] != expected:
        fail(
            f"{wheel}: {jar_name} declares Main-Class {facts['main_class']!r}, expected "
            f"{expected!r}. The two jars are built from one module and told apart by their "
            "entry point, so this is most likely the wrong jar."
        )


def check_render_jar(wheel: str, jar_bytes: bytes) -> None:
    """The public wheel's jar: the seal, its positive control, and the entry point."""
    facts = scan_jar(wheel, "ta-render.jar", jar_bytes, REQUIRED_PREFIX)
    if facts["sealed"]:
        fail(
            f"{wheel} ships a ta-render.jar carrying {len(facts['sealed'])} class(es) under "
            f"'{SEALED_PREFIX}'. The public wheel must not contain the choreography "
            f"builder (ADR 0006/0007, plan §4.1). First offenders: {facts['sealed'][:10]}"
        )
    if not facts["required"]:
        fail(
            f"{wheel} ships a ta-render.jar with NO class under '{REQUIRED_PREFIX}', so it "
            "is not a renderer jar and the seal check above proved nothing."
        )
    check_entry_point(wheel, "ta-render.jar", facts, RENDER_MAIN_CLASS)
    print(f"    jar: seal OK ({len(facts['required'])} renderer classes, 0 builder), "
          f"class-file major {CLASS_FILE_MAJOR} across {len(facts['sampled'])} sampled")


def check_prepare_jar(wheel: str, jar_bytes: bytes) -> None:
    """The private wheel's jar: the seal INVERTED -- `builder/` is what it is for.

    Sampled under the sealed prefix rather than the renderer one: those are the classes
    this jar exists to carry, so it is where a wrong-target build must be caught.
    """
    facts = scan_jar(wheel, "ta-prepare.jar", jar_bytes, SEALED_PREFIX)
    if not facts["sealed"]:
        fail(
            f"{wheel} ships a ta-prepare.jar with NO class under '{SEALED_PREFIX}'. That "
            "package is the entire reason this jar is a second artifact; without it "
            f"{PREPARE_MAIN_CLASS} cannot load and `tada prepare` fails on the first "
            "config. Did the builder package move, or is this ta-render.jar?"
        )
    check_entry_point(wheel, "ta-prepare.jar", facts, PREPARE_MAIN_CLASS)
    print(f"    jar: {len(facts['sealed'])} builder classes present, "
          f"Main-Class={facts['main_class']}, class-file major {CLASS_FILE_MAJOR} "
          f"across {len(facts['sampled'])} sampled")


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

    # 0. No stowaway prepare jar. `check_render_jar` proves `builder/` is ABSENT from
    # `ta-render.jar`, which says nothing about a SECOND jar at another path -- and
    # `ta-prepare.jar` is that jar plus `builder/`. Shipping one here publishes the
    # proprietary choreography while every seal assertion still passes, so the file set is
    # checked by name as well as the one jar's contents (ADR 0015 §4.1).
    stowaways = sorted(n for n in names if n.endswith("ta-prepare.jar"))
    if stowaways:
        fail(f"{wheel} carries {stowaways}. `ta-prepare.jar` is `ta-render.jar` PLUS "
             f"'{SEALED_PREFIX}', and this wheel is published; the render jar's seal cannot "
             "see a second jar beside it. The prepare jar belongs in the PRIVATE wheel only.")

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
    check_render_jar(wheel, archive.read(JAR))

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

    private_tag = platform_tag_of(private[0])
    print(f"{private[0]}  [{private_tag}]")
    with ZipFile(private[0]) as archive:
        names = set(archive.namelist())
        missing = sorted(REQUIRED_PRIVATE - names)
        if missing:
            fail(f"{private[0]} is missing expected modules: {missing}")
        check_credentials(private[0], names)
        # Its payload is one jar: no JRE, no natives, nothing this wheel could be built
        # four times for. Tagging it would be a claim nothing produces and every consumer
        # would then have to match.
        if private_tag != "any":
            fail(f"{private[0]} is platform-tagged ({private_tag}). The private wheel's "
                 "payload is a jar and nothing else -- ADR 0015 has it borrow the public "
                 "wheel's jlink'd JRE -- so it is architecture-independent and must stay "
                 "py3-none-any.")
        # ADR 0015. A missing jar has to fail HERE: the alternative is a dispatcher that
        # installs cleanly and then cannot prepare, which surfaces as a customer's failed
        # render rather than as a red build.
        if PREPARE_JAR not in names:
            fail(f"{private[0]} carries no {PREPARE_JAR}. `tada prepare` drives that jar as "
                 "a subprocess (ADR 0015/0016), so a wheel without it installs fine and "
                 "then fails on the first config. Check that :host:taJars ran and that "
                 "build_tada_wheel.sh was given PREPARE_JAR.")
        check_prepare_jar(private[0], archive.read(PREPARE_JAR))
        if PREPARE_MANIFEST in names:
            manifest = json.loads(archive.read(PREPARE_MANIFEST))
            if manifest.get("jre", {}).get("bundled"):
                fail(f"{private[0]}: {PREPARE_MANIFEST} claims a bundled JRE. The private "
                     "wheel runs on the public wheel's, and a second jlink'd runtime is "
                     "what ADR 0015 refused.")
            print(f"    payload: {manifest['jar']['builder_classes']} builder classes, "
                  f"{manifest['bytes']['total'] / 1e6:.0f} MB, "
                  f"rewritten={manifest['jar'].get('rewritten')}")
        stray = sorted(n for n in names
                       if n.startswith(PREPARE_PAYLOAD) and n not in {PREPARE_JAR, PREPARE_MANIFEST})
        if stray:
            fail(f"{private[0]}: {PREPARE_PAYLOAD} holds files beyond the jar and its "
                 f"manifest: {stray[:5]}. A JRE or a native library here is the second "
                 "runtime ADR 0015 refused; add it to this list only deliberately.")
        # The OTHER wheel's payload, which is a different failure: it would mean the public
        # distribution's platform-specific `_jvm/` rode in on a wheel tagged `any`.
        leaked = sorted(n for n in names if n.startswith(PAYLOAD))
        if leaked:
            fail(f"{private[0]} carries the PUBLIC wheel's JVM payload ({leaked[:3]}); that "
                 "belongs in travel-animator, which is the distribution that owns "
                 "tada_render/ and is built once per platform.")

    for wheel in public:
        tag = platform_tag_of(wheel)
        print(f"{wheel}  [{tag}]")
        with ZipFile(wheel) as archive:
            names = set(archive.namelist())
            missing = sorted(REQUIRED_PUBLIC - names)
            if missing:
                fail(f"{wheel} is missing expected modules: {missing}")
            check_credentials(wheel, names)
            # The seal below reads ta-render.jar. A whole ta-prepare.jar riding along beside
            # it would carry `builder/` past that check untouched, so it is refused by name
            # -- and on the `any` path too, which never reaches the seal at all.
            prepare = sorted(n for n in names
                             if n.rsplit("/", 1)[-1] == "ta-prepare.jar"
                             or n.startswith(PREPARE_PAYLOAD))
            if prepare:
                fail(f"{wheel} carries {prepare[:3]}. ta-prepare.jar is ta-render.jar PLUS "
                     f"'{SEALED_PREFIX}', and this distribution is publishable to PyPI: "
                     "shipping it here publishes the choreography builder (ADR 0006/0007). "
                     "It belongs in the private tada wheel and nowhere else.")
            if tag == "any":
                # Not an error here: the bundle step decides whether a pure wheel is
                # acceptable, and stage_public_wheel.sh refuses one on the PyPI path.
                print("    py3-none-any: no JVM payload, none checked. "
                      "The renderer will fall back to `ta-render` on PATH.")
                continue
            check_payload(wheel, names, archive)

    print(f"OK: 1 private wheel, {len(public)} public wheel(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
