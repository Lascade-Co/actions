#!/usr/bin/env bash
# Push the release bundle to GHCR as :latest and print the resulting digest to stdout.
# Run from the directory that CONTAINS bundle/. Requires oras already logged in.
# Env: TARGET_SHA, TARGET_REPO (the tada commit + repo, from the dispatch payload).
# Human-readable logs go to stderr; stdout is ONLY the digest (so the caller can capture it).
#
# The bundle carries BOTH distributions built from one revision: private tada-*.whl and public
# travel_animator-*.whl. Pushing only one leaves consumers unable to install a working `tada`.
set -euo pipefail

: "${TARGET_SHA:?}" "${TARGET_REPO:?}"

shopt -s nullglob
private_wheels=(bundle/tada-*.whl)
render_wheels=(bundle/travel_animator-*.whl)
if (( ${#private_wheels[@]} != 1 )) \
  || (( ${#render_wheels[@]} != 1 )) \
  || [[ ! -f bundle/pylock.toml ]] \
  || [[ ! -f bundle/SHA256SUMS ]] \
  || [[ ! -f bundle/build-metadata.json ]]; then
  echo "release bundle is incomplete" >&2
  exit 1
fi

package="ghcr.io/lascade-co/tada-wheel"
private_wheel="$(basename "${private_wheels[0]}")"
render_wheel="$(basename "${render_wheels[0]}")"
created="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Backlog C10: the bundle is deliberately SINGLE-PLATFORM. TARS declares
# `"target_platform": "linux/amd64"` and builds only that, so pushing every platform wheel
# here would multiply a consumer's pull for artifacts it can never install. PyPI gets the
# whole matrix; GHCR gets one. The tag is read out of the wheel filename rather than trusted
# from build-metadata.json, then cross-checked against it -- two independent readings of the
# same fact, so a metadata file that disagrees with the bytes cannot be published.
render_stem="${render_wheel%.whl}"
platform_tag="${render_stem##*-}"
if [[ "$platform_tag" != manylinux_*_x86_64 ]]; then
  echo "refusing to push: the GHCR bundle must carry the manylinux x86-64 wheel," >&2
  echo "TARS's only target_platform; found $render_wheel" >&2
  exit 1
fi
declared="$(python3 -c 'import json;print(json.load(open("bundle/build-metadata.json")).get("platform_tag",""))')"
if [[ "$declared" != "$platform_tag" ]]; then
  echo "refusing to push: build-metadata.json says platform_tag=$declared but the wheel is $platform_tag" >&2
  exit 1
fi

(
  cd bundle
  # artifact-type stays at bundle.v1: the media types of the layers are unchanged and pullers
  # select layers by filename, not by ordinal. Neither the file COUNT nor the layer set has
  # changed at schema_version 3 -- what changed is that the public wheel is now
  # platform-tagged, which build-metadata.json records in `platform_tag`. A puller that
  # ignores it gets the same five files it always got, on the one platform it ever ran on.
  oras push "$package:latest" \
    --artifact-type application/vnd.lascade.tada-wheel.bundle.v1 \
    --annotation "org.opencontainers.image.created=$created" \
    --annotation "org.opencontainers.image.revision=$TARGET_SHA" \
    --annotation "org.opencontainers.image.source=https://github.com/$TARGET_REPO" \
    --annotation "com.lascade.tada.platform-tag=$platform_tag" \
    "$private_wheel:application/vnd.pypa.wheel" \
    "$render_wheel:application/vnd.pypa.wheel" \
    "pylock.toml:application/vnd.python.pylock.toml" \
    "SHA256SUMS:text/plain" \
    "build-metadata.json:application/json" >&2
)

digest="$(oras resolve "$package:latest")"
echo "Published $package:latest@$digest" >&2
printf '%s\n' "$digest"
