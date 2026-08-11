#!/usr/bin/env bash
# Build the TADA wheel bundle (wheels + runtime pylock + build-metadata + SHA256SUMS).
# Run from the TADA checkout root. Env: SHARED_REVISION, TADA_REPOSITORY, TADA_REVISION.
# Identity (repository/revision) comes from TADA_* env, NOT ambient GITHUB_*, because this
# runs in the central actions repo. workflow_run_id/attempt stay ambient (the build runs here).
#
# TADA ships as TWO distributions built from one revision (ADR 0011):
#   tada             private -- choreography builder + credentialed fetchers, server-side only
#   travel-animator  public  -- render engine and the safe CLI subcommands
# travel-animator's import path is `tada_render` and its wheel filename escapes the dash to
# `travel_animator-`. Both belong in the bundle: `tada` imports `tada_render` at import time,
# so a bundle carrying only the private wheel yields a `tada` entry point that cannot start.
#
# This script builds BOTH wheels but ships the public one only as the pure input the platform
# legs fold their payload into: if PUBLIC_WHEEL is set it SWAPS in the platform-tagged wheel
# built elsewhere. One build path for the private wheel and the pylock is the thing that must
# not fork. The bundle stays at FIVE files and single-platform -- TARS declares
# `"target_platform": "linux/amd64"`, so pushing all four to GHCR would quadruple a consumer's
# pull. PyPI gets the matrix; GHCR gets manylinux x86-64.
set -euo pipefail

: "${SHARED_REVISION:?}" "${TADA_REPOSITORY:?}" "${TADA_REVISION:?}"

# Optional: a platform-tagged public wheel built by a matrix leg, shipped INSTEAD of the pure
# one `uv build` produces here. Unset means the bundle carries the pure wheel.
PUBLIC_WHEEL="${PUBLIC_WHEEL:-}"

rm -rf bundle
mkdir bundle

# --all-packages is load-bearing: plain `uv build` builds only the workspace ROOT (tada), so the
# public travel-animator wheel is silently absent until a consumer's image build cannot import it.
uv build --wheel --all-packages --out-dir bundle

# --no-emit-workspace, NOT --no-emit-project: the latter omits only the root, leaving the public
# member in the lock as an editable relative path that resolves here but fails in a consumer's
# /bundle with "Distribution not found at: file:///bundle/tada_render". Both members ship as
# wheels, so the lock carries third-party dependencies only.
uv export \
  --locked \
  --no-dev \
  --no-emit-workspace \
  --format pylock.toml \
  --output-file bundle/pylock.toml

shopt -s nullglob

# Every consumer that selects one wheel by glob depends on `tada-` and `travel_animator-`
# staying prefix-distinct.
private_wheels=(bundle/tada-*.whl)
if (( ${#private_wheels[@]} != 1 )); then
  echo "expected exactly one tada-*.whl, found ${#private_wheels[@]}:" >&2
  printf '  %s\n' "${private_wheels[@]}" >&2
  exit 1
fi

if [[ -n "$PUBLIC_WHEEL" ]]; then
  # Replace, do not add: a `py3-none-any` file left beside the platform wheel shadows it for
  # every installer -- the exact failure stage_public_wheel.sh refuses.
  if [[ ! -f "$PUBLIC_WHEEL" ]]; then
    echo "PUBLIC_WHEEL=$PUBLIC_WHEEL does not exist" >&2
    exit 1
  fi
  case "$(basename "$PUBLIC_WHEEL")" in
    *-py3-none-any.whl)
      echo "PUBLIC_WHEEL is py3-none-any; the bundle must carry the PLATFORM wheel" >&2
      exit 1
      ;;
  esac
  rm -f bundle/travel_animator-*.whl
  cp "$PUBLIC_WHEEL" bundle/
fi

render_wheels=(bundle/travel_animator-*.whl)
if (( ${#render_wheels[@]} != 1 )); then
  echo "expected exactly one travel_animator-*.whl in the bundle, found ${#render_wheels[@]}:" >&2
  printf '  %s\n' "${render_wheels[@]}" >&2
  echo "The GHCR bundle is deliberately SINGLE-PLATFORM (backlog C10)." >&2
  exit 1
fi

private_wheel="$(basename "${private_wheels[0]}")"
render_wheel="$(basename "${render_wheels[0]}")"

# {name}-{version}-{python}-{abi}-{platform}.whl
render_stem="${render_wheel%.whl}"
platform_tag="${render_stem##*-}"

WHEEL_NAME="$private_wheel" \
RENDER_WHEEL_NAME="$render_wheel" \
RENDER_WHEEL_PLATFORM_TAG="$platform_tag" \
uv run --locked python - <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import platform
import subprocess

metadata = {
    # Consumers branch on this (TARS deploy/verify_tada_bundle.py, release/validate_lock.py).
    # 3 makes reading `platform_tag` mandatory: a v2 reader must not silently accept a bundle
    # whose wheel only installs on one architecture.
    "schema_version": 3,
    "repository": os.environ["TADA_REPOSITORY"],
    "revision": os.environ["TADA_REVISION"],
    "shared_revision": os.environ["SHARED_REVISION"],
    "workflow_run_id": os.environ["GITHUB_RUN_ID"],
    "workflow_run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
    "built_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "python_version": platform.python_version(),
    "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
    # `wheel` names the PRIVATE wheel; it has since schema_version 1.
    "wheel": os.environ["WHEEL_NAME"],
    # The public wheel that `tada` imports at load time; an installer that skips it produces
    # a broken `tada` entry point.
    "render_wheel": os.environ["RENDER_WHEEL_NAME"],
    # How a deploy checks that what it pulled matches what it runs, rather than discovering it
    # at `pip install` time as "no matching distribution".
    "platform_tag": os.environ["RENDER_WHEEL_PLATFORM_TAG"],
}
Path("bundle/build-metadata.json").write_text(
    json.dumps(metadata, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

(
  cd bundle
  sha256sum "$private_wheel" "$render_wheel" pylock.toml build-metadata.json > SHA256SUMS
  sha256sum --check SHA256SUMS
)

# Asserted here as well as in the workflow: this script decides the bundle's shape, so it is
# what should refuse to emit a wrong one.
files=(bundle/*)
if (( ${#files[@]} != 5 )); then
  printf 'bundle must hold exactly five files, found %d:\n' "${#files[@]}" >&2
  printf '  %s\n' "${files[@]}" >&2
  exit 1
fi
printf 'bundle: %s [%s]\n' "$render_wheel" "$platform_tag"
