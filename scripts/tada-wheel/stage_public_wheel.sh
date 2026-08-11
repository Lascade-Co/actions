#!/usr/bin/env bash
# Stage ONLY the public wheels into dist/ for upload. Run from the directory containing bundle/.
# Env: DIST (distribution name, e.g. travel-animator), VERSION (the version being published),
#      EXPECT_WHEELS (optional; the exact number expected -- see below).
#
# The upload action publishes every file in its packages-dir, so what ends up in dist/ is the
# entire security boundary for a public release. The private `tada` wheel -- choreography
# builder and credentialed fetchers -- must never reach a public index. Hence: copy in by the
# public distribution's own glob, then assert what dist/ holds rather than trusting the glob to
# have matched only what we meant.
#
# ---------------------------------------------------------------------------
# Backlog C9: this got STRONGER when the count stopped being one, not weaker
# ---------------------------------------------------------------------------
# The public wheel now carries a per-platform JVM payload (backlog C1), so a release is N
# wheels, one per platform leg, not one. "Exactly one file" was doing two jobs at once --
# excluding the private wheel AND pinning the count -- and only the first of those is the
# security property. Replacing it with "exactly one file" relaxed to "any number of files"
# would have thrown the boundary away, so each job is now its own assertion:
#
#   1. every staged file matches ${dist_file}-${VERSION}-*.whl   -- the private wheel, a
#      stray sdist, a leftover from another version: none of them can pass this
#   2. every platform tag is DISTINCT                            -- two files claiming the
#      same tag means a leg ran twice, and PyPI would reject the second upload after the
#      first has already gone out, leaving a half-published release
#   3. NONE of them is py3-none-any                              -- the one that matters most,
#      see below
#   4. the count matches EXPECT_WHEELS when the caller declares it
#
# **Why py3-none-any is fatal and not merely wrong.** Installers rank candidate wheels by tag
# specificity, and `any` is compatible with every platform. A single untagged wheel uploaded
# alongside the platform ones does not sit harmlessly beside them -- pip and uv will still
# prefer a platform-specific tag, but `py3-none-any` is the file every OTHER platform resolves
# to, including the ones we never built. So an accidentally-untagged upload silently ships a
# payload-less wheel to everyone the matrix does not cover, and PyPI releases are IMMUTABLE:
# it cannot be replaced, only yanked, and only by cutting a new version. That is why this is a
# hard refusal at staging time rather than a warning.
set -euo pipefail

: "${DIST:?}" "${VERSION:?}"

rm -rf dist
mkdir dist

# Any non-zero exit empties dist/ again. The workflow step failing is already enough to stop
# the upload -- this is the second lock on the same door, because the cost of being wrong here
# is an immutable public release of the private wheel.
trap 'status=$?; if (( status != 0 )); then rm -rf dist; echo "dist/ emptied after failure" >&2; fi' EXIT

# A wheel filename carries the ESCAPED distribution name: PEP 427 collapses every run of
# `-`, `_` or `.` to a single `_`. That was a no-op for every distribution this script has
# seen so far (`tacli`, `tada-render` was never built here), but `travel-animator` files as
# `travel_animator-`, so globbing on $DIST verbatim matches nothing. Match on the escaped
# form; keep the messages in terms of $DIST, which is the name a human recognises.
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

  # {name}-{version}-{python}-{abi}-{platform}.whl, so the tag is the last three
  # hyphen-separated fields. A build number (an optional 3rd field) would shift that, and we
  # do not emit one -- if that ever changes, this is where it breaks, loudly.
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
