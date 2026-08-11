#!/usr/bin/env bash
# Build ONE Linux platform wheel INSIDE quay.io/pypa/manylinux_2_28_<arch>.
# Run as the container's command, with the workspace bind-mounted:
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
# Nothing here is compiled -- every binary is prebuilt (Temurin JRE, LWJGL and
# Skiko out of Maven jars) and jlink copies modules rather than linking them. The
# container is where the glibc floor gets MEASURED and enforced, not assumed.
#
# Highest versioned glibc symbol per file (measured 2026-08-11 with `readelf -V`;
# LWJGL 3.3.6 / Temurin 21.0.12+8 / Skiko 0.148.2): Temurin and libskiko 2.17,
# x64 liblwjgl 2.16 -- but **arm64 liblwjgl.so needs 2.34**, so one 500 KB JNI
# shim caps the whole arm64 wheel at manylinux_2_34 and auditwheel refuses 2_28
# outright. Both clear the only load-bearing floor: TARS runs
# python:3.12-slim-bookworm (glibc 2.36), linux/amd64 only.
#
# `auditwheel repair` does NOT run, and must not. The natives need libGL, libX11,
# libfontconfig and (arm64) libEGL from the HOST: the GPU worker's driver is
# `libEGL_nvidia.so` and a vendored Mesa loader would win the lookup, and a
# vendored fontconfig pins the wheel to this container's font cache. It cannot
# finish on a wheel carrying a JRE anyway -- it dies resolving java.desktop's X11
# closure. check_glibc_floor.py's docstring carries the argument and what
# replaces it.
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

# jlink CANNOT cross-target: on a mismatched container it emits a runtime for the
# wrong machine and every check below passes on a wheel nobody can run.
here="$(uname -m)"
if [[ "$here" != "$arch" ]]; then
  echo "refusing to build $IO_PLATFORM on a $here container: jlink cannot cross-target" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# JDK. Temurin's tarballs are built on glibc 2.17; a `dnf install java-21-openjdk`
# here would silently raise the floor of everything jlink copies.
: "${JDK_URL:=https://api.adoptium.net/v3/binary/latest/21/ga/linux/${jdk_arch}/jdk/hotspot/normal/eclipse}"
mkdir -p "$work/jdk"
curl -sSfL --retry 5 --retry-delay 3 --retry-connrefused "$JDK_URL" \
  | tar -xz -C "$work/jdk" --strip-components=1
export JAVA_HOME="$work/jdk"
"$JAVA_HOME/bin/java" -version

# Skiko's native runtime for THIS platform -- not the build host's, which is the
# whole reason build_jvm_payload.py strips the jar.
skiko_jar="$work/skiko.jar"
curl -sSfL --retry 5 --retry-delay 3 --retry-connrefused -o "$skiko_jar" \
  "https://repo1.maven.org/maven2/org/jetbrains/skiko/skiko-awt-runtime-${IO_PLATFORM}/${SKIKO_VERSION}/skiko-awt-runtime-${IO_PLATFORM}-${SKIKO_VERSION}.jar"

python3="$(command -v python3.12 || command -v python3)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# No --angle-dir: Linux renders through the SYSTEM libEGL.so.1 / libGLESv2.so.2,
# which is the same path that reaches libEGL_nvidia on the GPU worker. Verified
# 2026-08-11 on Mesa llvmpipe (OpenGL ES 3.2, max_samples=4). build_jvm_payload.py
# hard-errors if an angle/ is passed for a Linux platform.
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

# Informational only: `show` modifies nothing, and its verdict is stricter than
# ours -- it also rejects the external deps this wheel carries deliberately.
echo "::group::auditwheel show (informational)"
auditwheel show "$wheel_out" || true
echo "::endgroup::"

# The gate.
"$python3" "$script_dir/check_glibc_floor.py" "$wheel_out" \
  --max-glibc "${default_glibc/_/.}" \
  --consumer-glibc "$CONSUMER_GLIBC"

# An installed `bin/java` without +x is a permission error at the first render,
# from a wheel that installed cleanly. Proves the mode survived the container's
# umask and the bind mount.
"$python3" "$script_dir/assert_wheel_exec_bit.py" "$wheel_out"

# The payload manifest: the bundle's build-metadata.json quotes it.
cp "$work/payload/PAYLOAD.json" "$IO_OUT/PAYLOAD-${IO_PLATFORM}.json"

ls -la "$IO_OUT"
