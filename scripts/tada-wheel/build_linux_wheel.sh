#!/usr/bin/env bash
# Build ONE Linux platform wheel INSIDE quay.io/pypa/manylinux_2_28_<arch>.
# Backlog C2/C3. Run as the container's command, with the workspace bind-mounted:
#
#   docker run --rm -v "$PWD:/io" -w /io quay.io/pypa/manylinux_2_28_x86_64 \
#     bash /io/scripts/build_linux_wheel.sh
#
# Env (all required unless noted):
#   IO_WHEEL        the pure travel_animator-*.whl to fold the payload into
#   IO_JAR          ta-render.jar from the jvm-build job
#   IO_OUT          directory to write the finished wheel into
#   IO_PLATFORM     linux-x64 | linux-arm64
#   SKIKO_VERSION   (optional) must match shared/build.gradle.kts's skikoVersion
#   MANYLINUX_TAG   (optional) override the measured default; still gated below
#   JDK_URL         (optional) Temurin 21 tarball; defaults to the Adoptium API
#
# ---------------------------------------------------------------------------
# Why the container, given that nothing here is COMPILED
# ---------------------------------------------------------------------------
# Every binary in the payload is prebuilt by somebody else: the JRE comes out of
# a Temurin tarball, LWJGL's shims and Skiko's libskia out of Maven jars. jlink
# copies modules, it does not link them, so the glibc floor is the JDK vendor's
# and not the builder's -- running this on ubuntu-24.04 would produce
# byte-identical binaries. The naive reading of C2 ("build in manylinux so the
# payload is not built against glibc 2.39") is therefore not quite the reason.
#
# The container earns its place for two things that ARE different here:
#
#   1. It is where the floor gets MEASURED rather than assumed. That measurement
#      is what turned up the surprise below: aarch64 cannot reach 2.28.
#   2. glibc 2.28 is the floor being claimed, so it is the one place a `bin/java`
#      that quietly needs 2.34 fails HERE and not on the GPU worker.
#
# ---------------------------------------------------------------------------
# The manylinux level is MEASURED, and it differs by arch
# ---------------------------------------------------------------------------
# Highest versioned glibc symbol referenced, per file (measured 2026-08-11 with
# `readelf -V`, LWJGL 3.3.6 / Temurin 21.0.12+8 / Skiko 0.148.2):
#
#   Temurin JRE -- every .so and bin/java ..... GLIBC_2.17
#   libskiko-linux-{x64,arm64}.so ............. GLIBC_2.17
#   linux/x64/liblwjgl.so ..................... GLIBC_2.16
#   linux/x64/liblwjgl_opengles.so ............ GLIBC_2.2
#   linux/arm64/liblwjgl.so ................... GLIBC_2.34   <-- the whole cap
#
# x86-64 clears 2.28 with room to spare -- its true floor is 2.17, and 2_28 is
# kept only because it is what the plan decided and the extra headroom is free.
# **aarch64 cannot reach 2.28.** LWJGL builds its aarch64 native on a newer
# glibc than its x86-64 one, so one 500 KB JNI shim caps the whole arm64 wheel
# at manylinux_2_34. Confirmed by auditwheel refusing outright: `auditwheel
# repair --plat manylinux_2_28_aarch64` -> "cannot repair ... because of the
# presence of too-recent versioned symbols".
#
# Both clear the only floor that is load-bearing. TARS runs
# python:3.12-slim-bookworm (glibc 2.36) and declares linux/amd64 only, so the
# leg that reaches the GPU worker has 19 minor versions of headroom and the
# arm64 leg -- PyPI only -- has 2. `check_glibc_floor.py` keeps that true rather
# than remembered.
#
# ---------------------------------------------------------------------------
# What is NOT vendored, and why `auditwheel repair` does not run (C3)
# ---------------------------------------------------------------------------
# DT_NEEDED of the staged natives, measured the same day:
#
#   liblwjgl.so ................ libc, ld-linux              (nothing to vendor)
#   liblwjgl_opengles.so ....... nothing at all              (it dlopens GLES)
#   libskiko-linux-x64.so ...... libGL.so.1, libX11.so.6, libfontconfig.so.1,
#                                libstdc++, libm, libc
#   libskiko-linux-arm64.so .... the above PLUS libEGL.so.1
#
# So C3's libEGL/libGLESv2 concern is real but understates it: on x86-64
# auditwheel would never have seen libEGL at all, and the libraries it WOULD
# have vendored are libGL, libX11 and libfontconfig -- every one of which must
# come from the host. libGL/libEGL because the GPU worker's driver is
# `libEGL_nvidia.so` and a vendored Mesa loader would win the lookup; fontconfig
# because a vendored copy pins the wheel to this container's font cache and
# `/etc/fonts`.
#
# `auditwheel repair` is not the tool that enforces that here, because it cannot
# finish on a wheel carrying a JRE -- it insists on resolving java.desktop's
# whole X11 closure and dies on `libjvm.so`, then `libXtst.so.6`, and onward.
# The full argument, and what replaces it, is in check_glibc_floor.py's
# docstring. `auditwheel show` still runs, for the record.
#
# Verified end to end 2026-08-11: this payload in `python:3.12-slim-bookworm`
# with TARS's exact GPU package list resolves all five externals (`libx11-6`
# arrives transitively via `libgl1 -> libglx0`), Skiko rasterises, and
# `ta-render self-test` reports `system EGL/GLES ... OpenGL ES 3.2 Mesa`,
# `max_samples=4`.
set -euo pipefail

: "${IO_WHEEL:?}" "${IO_JAR:?}" "${IO_OUT:?}" "${IO_PLATFORM:?}"
SKIKO_VERSION="${SKIKO_VERSION:-0.148.2}"

case "$IO_PLATFORM" in
  linux-x64)   arch=x86_64;  jdk_arch=x64;     default_glibc=2_28 ;;
  linux-arm64) arch=aarch64; jdk_arch=aarch64; default_glibc=2_34 ;;
  *) echo "IO_PLATFORM must be linux-x64 or linux-arm64, got '$IO_PLATFORM'" >&2; exit 2 ;;
esac
plat_tag="${MANYLINUX_TAG:-manylinux_${default_glibc}_${arch}}"

# TARS's own base image. Nothing may ship above this, whatever tag is requested.
CONSUMER_GLIBC="2.36"

# The container's own arch has to match, or jlink emits a runtime for the wrong
# machine and every check below passes on a wheel nobody can run. jlink CANNOT
# cross-target (backlog C1); this is that fact, stated where it can fail.
here="$(uname -m)"
if [[ "$here" != "$arch" ]]; then
  echo "refusing to build $IO_PLATFORM on a $here container: jlink cannot cross-target" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# ---------------------------------------------------------------------------
# JDK. Temurin's tarballs are built on an old glibc (2.17, measured above); the
# distro's is not, and a `dnf install java-21-openjdk` here would silently raise
# the floor of everything jlink copies.
# ---------------------------------------------------------------------------
: "${JDK_URL:=https://api.adoptium.net/v3/binary/latest/21/ga/linux/${jdk_arch}/jdk/hotspot/normal/eclipse}"
mkdir -p "$work/jdk"
curl -sSfL --retry 5 --retry-delay 3 --retry-connrefused "$JDK_URL" \
  | tar -xz -C "$work/jdk" --strip-components=1
export JAVA_HOME="$work/jdk"
"$JAVA_HOME/bin/java" -version

# ---------------------------------------------------------------------------
# Skiko's native runtime for THIS platform -- not the build host's, which is the
# whole reason build_jvm_payload.py strips the jar (backlog C4).
# ---------------------------------------------------------------------------
skiko_jar="$work/skiko.jar"
curl -sSfL --retry 5 --retry-delay 3 --retry-connrefused -o "$skiko_jar" \
  "https://repo1.maven.org/maven2/org/jetbrains/skiko/skiko-awt-runtime-${IO_PLATFORM}/${SKIKO_VERSION}/skiko-awt-runtime-${IO_PLATFORM}-${SKIKO_VERSION}.jar"

python3="$(command -v python3.12 || command -v python3)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# No --angle-dir. Linux renders through the SYSTEM libEGL.so.1 / libGLESv2.so.2:
# verified 2026-08-11 on Mesa llvmpipe (OpenGL ES 3.2, max_samples=4) in a
# container carrying only libegl1 libegl-mesa0 libgles2 libgl1-mesa-dri, and it
# is the same path that reaches libEGL_nvidia on the GPU worker.
# build_jvm_payload.py hard-errors if an angle/ is passed for a Linux platform.
"$python3" "$script_dir/build_jvm_payload.py" \
  --platform "$IO_PLATFORM" \
  --jar "$IO_JAR" \
  --skiko-jar "$skiko_jar" \
  --java-home "$JAVA_HOME" \
  --platform-tag "$plat_tag" \
  --out "$work/payload"

mkdir -p "$IO_OUT"
"$python3" "$script_dir/inject_wheel_payload.py" \
  --wheel "$IO_WHEEL" \
  --payload "$work/payload" \
  --tag "py3-none-${plat_tag}" \
  --out-dir "$IO_OUT"

wheel_out="$(ls "$IO_OUT"/*"${plat_tag}"*.whl)"

# auditwheel's own reading, for the record and for whoever next has to re-derive
# the floor. Non-fatal: `show` never modifies anything, and its verdict is
# stricter than ours -- it also rejects non-whitelisted external deps, which
# this wheel has deliberately (libGL / libX11 / libfontconfig).
echo "::group::auditwheel show (informational)"
auditwheel show "$wheel_out" || true
echo "::endgroup::"

# The gate.
"$python3" "$script_dir/check_glibc_floor.py" "$wheel_out" \
  --max-glibc "${default_glibc/_/.}" \
  --consumer-glibc "$CONSUMER_GLIBC"

# The JRE is the only reason any of this exists; an installed `bin/java` without
# +x is a permission error at the first render, from a wheel that installed
# cleanly. inject_wheel_payload.py preserves the mode -- this proves it survived
# the trip through the container's umask and the bind mount.
"$python3" "$script_dir/assert_wheel_exec_bit.py" "$wheel_out"

# The payload manifest, copied out beside the wheel: the size table and the
# provenance the bundle's build-metadata.json quotes come from here.
cp "$work/payload/PAYLOAD.json" "$IO_OUT/PAYLOAD-${IO_PLATFORM}.json"

ls -la "$IO_OUT"
