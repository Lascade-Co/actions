#!/usr/bin/env bash
# Push the release bundle to GHCR as :latest and print the resulting digest to stdout.
# Run from the directory that CONTAINS bundle/. Requires oras already logged in.
# Env: TARGET_SHA, TARGET_REPO (the tada commit + repo, from the dispatch payload).
# Human-readable logs go to stderr; stdout is ONLY the digest (so the caller can capture it).
set -euo pipefail

: "${TARGET_SHA:?}" "${TARGET_REPO:?}"

shopt -s nullglob
wheels=(bundle/*.whl)
if (( ${#wheels[@]} != 1 )) \
  || [[ ! -f bundle/pylock.toml ]] \
  || [[ ! -f bundle/SHA256SUMS ]] \
  || [[ ! -f bundle/build-metadata.json ]]; then
  echo "release bundle is incomplete" >&2
  exit 1
fi

package="ghcr.io/lascade-co/tada-wheel"
wheel="$(basename "${wheels[0]}")"
created="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
(
  cd bundle
  oras push "$package:latest" \
    --artifact-type application/vnd.lascade.tada-wheel.bundle.v1 \
    --annotation "org.opencontainers.image.created=$created" \
    --annotation "org.opencontainers.image.revision=$TARGET_SHA" \
    --annotation "org.opencontainers.image.source=https://github.com/$TARGET_REPO" \
    "$wheel:application/vnd.pypa.wheel" \
    "pylock.toml:application/vnd.python.pylock.toml" \
    "SHA256SUMS:text/plain" \
    "build-metadata.json:application/json" >&2
)

digest="$(oras resolve "$package:latest")"
echo "Published $package:latest@$digest" >&2
printf '%s\n' "$digest"
