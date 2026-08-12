#!/usr/bin/env bash
# Stage ONLY the public wheels into dist/ for upload. Run from the directory containing bundle/.
# Env: DIST (distribution name, e.g. travel-animator), VERSION (the version being published),
#      EXPECT_WHEELS (optional; the exact number expected).
#
# The upload action publishes every file in its packages-dir, so dist/ is the entire security
# boundary for a public release and the private `tada` wheel must never reach a public index.
# Hence: copy in by the public distribution's own glob, then assert what dist/ actually holds.
#
# A release is N wheels (one per platform leg), so dist/ is checked four ways: name+version match;
# DISTINCT platform tags (a duplicate means a leg ran twice, and PyPI rejects the second upload
# once the first is public, leaving a half-published release); no py3-none-any; and the declared
# count. **py3-none-any is fatal**, not merely wrong: it is the file every platform we did NOT
# build resolves to, so it silently ships a payload-less wheel -- and PyPI releases are
# IMMUTABLE, so it cannot be replaced, only yanked.
set -euo pipefail

: "${DIST:?}" "${VERSION:?}"

rm -rf dist
mkdir dist

# Second lock on the same door as the failing step: the cost of being wrong here is an
# immutable public release of the private wheel.
trap 'status=$?; if (( status != 0 )); then rm -rf dist; echo "dist/ emptied after failure" >&2; fi' EXIT

# A wheel filename carries the ESCAPED distribution name -- PEP 427 collapses every run of
# `-`, `_` or `.` to a single `_` -- so globbing on $DIST verbatim matches nothing for
# `travel-animator`. Match escaped; keep the messages in terms of the name a human recognises.
dist_file="$(printf '%s' "$DIST" | sed -E 's/[-_.]+/_/g')"

shopt -s nullglob
wheels=(bundle/"${dist_file}"-*.whl)
if (( ${#wheels[@]} == 0 )); then
  printf 'found no %s wheel (%s-*.whl) in bundle/\n' "$DIST" "$dist_file" >&2
  printf '  bundle holds: %s\n' "$(ls bundle 2>/dev/null | tr '\n' ' ')" >&2
  exit 1
fi
if [[ -n "${EXPECT_WHEELS:-}" ]] && (( ${#wheels[@]} != EXPECT_WHEELS )); then
  printf 'expected %s %s wheels, found %d:\n' "$EXPECT_WHEELS" "$DIST" "${#wheels[@]}" >&2
  printf '  %s\n' "${wheels[@]}" >&2
  exit 1
fi
cp "${wheels[@]}" dist/

staged=(dist/*)
if (( ${#staged[@]} != ${#wheels[@]} )); then
  # Only reachable if dist/ was not empty to begin with, or two source paths collapsed onto
  # one basename. Both mean the boundary is not what the globs above described.
  printf 'dist/ holds %d files but %d were copied in:\n' "${#staged[@]}" "${#wheels[@]}" >&2
  printf '  %s\n' "${staged[@]}" >&2
  exit 1
fi

# A newline-delimited "tag<TAB>file" list rather than an associative array: macOS ships
# bash 3.2, and being runnable there is what lets this be tested outside CI.
seen_tags=""
for path in "${staged[@]}"; do
  name="$(basename "$path")"
  case "$name" in
    "${dist_file}-${VERSION}"-*.whl) ;;
    *)
      echo "refusing to publish unexpected artifact: $name (want ${dist_file}-${VERSION}-*.whl)" >&2
      exit 1
      ;;
  esac

  # {name}-{version}-{python}-{abi}-{platform}.whl, so the tag is the last three fields. An
  # optional build number would shift that; we emit none, and this is where that breaks loudly.
  stem="${name%.whl}"
  platform_tag="${stem##*-}"
  abi_tag="${stem%-*}"; abi_tag="${abi_tag##*-}"
  python_tag="${stem%-*-*}"; python_tag="${python_tag##*-}"
  tag="${python_tag}-${abi_tag}-${platform_tag}"

  if [[ "$platform_tag" == "any" ]]; then
    echo "refusing to publish $name: a py3-none-any wheel is what every platform we did NOT" >&2
    echo "build resolves to, it carries no JVM payload, and PyPI releases cannot be replaced." >&2
    exit 1
  fi
  previous="$(printf '%s' "$seen_tags" | awk -F'\t' -v t="$tag" '$1 == t { print $2 }')"
  if [[ -n "$previous" ]]; then
    echo "refusing to publish: $name and $previous both claim tag $tag" >&2
    exit 1
  fi
  seen_tags="${seen_tags}${tag}	${name}
"
  echo "staged $name  [$tag]"
done

echo "staged ${#staged[@]} $DIST wheel(s) for upload, all distinct platform tags"
