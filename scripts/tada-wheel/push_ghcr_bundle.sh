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
(
  cd bundle
  # artifact-type stays at bundle.v1: the media types of the layers are unchanged and pullers
  # select layers by filename, not by ordinal. Only the file COUNT grew, which is what
  # build-metadata.json's schema_version 2 records.
  oras push "$package:latest" \
    --artifact-type application/vnd.lascade.tada-wheel.bundle.v1 \
    --annotation "org.opencontainers.image.created=$created" \
    --annotation "org.opencontainers.image.revision=$TARGET_SHA" \
    --annotation "org.opencontainers.image.source=https://github.com/$TARGET_REPO" \
    "$private_wheel:application/vnd.pypa.wheel" \
    "$render_wheel:application/vnd.pypa.wheel" \
    "pylock.toml:application/vnd.python.pylock.toml" \
    "SHA256SUMS:text/plain" \
    "build-metadata.json:application/json" >&2
)

digest="$(oras resolve "$package:latest")"
echo "Published $package:latest@$digest" >&2
printf '%s\n' "$digest"
