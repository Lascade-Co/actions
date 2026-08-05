#!/usr/bin/env bash
# Stage ONLY the public wheel into dist/ for upload. Run from the directory containing bundle/.
# Env: DIST (distribution name, e.g. travel-animator), VERSION (the version being published).
#
# The upload action publishes every file in its packages-dir, so what ends up in dist/ is the
# entire security boundary for a public release. The private `tada` wheel -- choreography
# builder and credentialed fetchers -- must never reach a public index. Hence: copy in by the
# public distribution's own glob, then assert dist/ holds exactly one file and that it is the
# expected name and version, rather than trusting the glob to have matched only what we meant.
set -euo pipefail

: "${DIST:?}" "${VERSION:?}"

rm -rf dist
mkdir dist

# A wheel filename carries the ESCAPED distribution name: PEP 427 collapses every run of
# `-`, `_` or `.` to a single `_`. That was a no-op for every distribution this script has
# seen so far (`tacli`, `tada-render` was never built here), but `travel-animator` files as
# `travel_animator-`, so globbing on $DIST verbatim matches nothing. Match on the escaped
# form; keep the messages in terms of $DIST, which is the name a human recognises.
dist_file="$(printf '%s' "$DIST" | sed -E 's/[-_.]+/_/g')"

shopt -s nullglob
wheels=(bundle/"${dist_file}"-*.whl)
if (( ${#wheels[@]} != 1 )); then
  printf 'expected exactly one %s wheel (%s-*.whl) in bundle/, found %d:\n' \
    "$DIST" "$dist_file" "${#wheels[@]}" >&2
  printf '  %s\n' "${wheels[@]}" >&2
  exit 1
fi
cp "${wheels[0]}" dist/

staged=(dist/*)
if (( ${#staged[@]} != 1 )); then
  printf 'dist/ must hold exactly one file, found:\n' >&2
  printf '  %s\n' "${staged[@]}" >&2
  exit 1
fi

name="$(basename "${staged[0]}")"
case "$name" in
  "${dist_file}-${VERSION}"-*.whl) ;;
  *)
    echo "refusing to publish unexpected artifact: $name (want ${dist_file}-${VERSION}-*.whl)" >&2
    exit 1
    ;;
esac

echo "staged $name for upload"
