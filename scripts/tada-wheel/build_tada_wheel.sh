#!/usr/bin/env bash
# Build the TADA wheel bundle (wheels + runtime pylock + build-metadata + SHA256SUMS).
# Run from the TADA checkout root. Env: SHARED_REVISION, TADA_REPOSITORY, TADA_REVISION.
# Identity (repository/revision) comes from TADA_* env, NOT ambient GITHUB_*, because this
# runs in the central actions repo. workflow_run_id/attempt stay ambient (the build runs here).
#
# TADA ships as TWO distributions built from one revision (ADR 0011):
#   tada             private -- choreography builder + credentialed fetchers, server-side only
#   travel-animator  public  -- render engine and the safe CLI subcommands
# travel-animator's Python import path is `tada_render`, and its wheel filename escapes the
# dash: glob wheels by `travel_animator-`, expect module paths under `tada_render/`.
#
# Both belong in the bundle: `tada` imports `tada_render` at module import time, so a bundle
# carrying only the private wheel yields an install whose `tada` entry point cannot start.
#
# ---------------------------------------------------------------------------
# Backlog C8/C10: what changed for the platform matrix
# ---------------------------------------------------------------------------
# The public wheel is now built per platform and carries a JVM payload, so this script no
# longer produces the public wheel it ships. It builds BOTH wheels (the private one for real,
# the public one as the pure input the platform legs fold their payload into), and then, if
# PUBLIC_WHEEL is set, SWAPS the pure public wheel for the platform-tagged one built
# elsewhere. That keeps one build path for the private wheel and the pylock, which is the
# thing that must not fork.
#
# The bundle stays at FIVE files and single-platform (C10): TARS declares
# `"target_platform": "linux/amd64"` and builds only that, so pushing four platform wheels to
# GHCR would quadruple the pull for a consumer that uses one. PyPI gets all four; GHCR gets
# the manylinux x86-64 one. The new `platform_tag` field in build-metadata.json is how a
# consumer knows WHICH one it got, and `schema_version` goes 2 -> 3 to make reading that field
# mandatory rather than optional.
set -euo pipefail

: "${SHARED_REVISION:?}" "${TADA_REPOSITORY:?}" "${TADA_REVISION:?}"

# Optional: a platform-tagged public wheel built by a matrix leg, to be shipped INSTEAD of the
# pure one `uv build` produces here. Unset means the bundle carries the pure wheel, which is
# what a pre-matrix build (and any local reproduction of it) does.
PUBLIC_WHEEL="${PUBLIC_WHEEL:-}"

rm -rf bundle
mkdir bundle

# --all-packages is load-bearing: plain `uv build` builds only the workspace ROOT (tada), so
# the public travel-animator wheel is silently absent and nothing notices until a consumer's
# image build tries to import it.
uv build --wheel --all-packages --out-dir bundle

# --no-emit-workspace, NOT --no-emit-project. --no-emit-project omits only the root project,
# which left the public member in the lock as
#     directory = { path = "tada_render", editable = true }
# a relative path that resolves in this checkout but not in a consumer's /bundle, where
# `uv pip sync pylock.toml` then fails with
#     Distribution not found at: file:///bundle/tada_render
# Both workspace members ship as wheels here, so neither belongs in the lock: it carries
# third-party dependencies only.
uv export \
  --locked \
  --no-dev \
  --no-emit-workspace \
  --format pylock.toml \
  --output-file bundle/pylock.toml

shopt -s nullglob

# The two globs are disjoint: a wheel filename starts with its distribution name, and
# `travel_animator-` shares no prefix with `tada-`. Every consumer that selects one wheel by
# glob depends on the two names staying prefix-distinct.
private_wheels=(bundle/tada-*.whl)
if (( ${#private_wheels[@]} != 1 )); then
  echo "expected exactly one tada-*.whl, found ${#private_wheels[@]}:" >&2
  printf '  %s\n' "${private_wheels[@]}" >&2
  exit 1
fi

if [[ -n "$PUBLIC_WHEEL" ]]; then
  # Replace, do not add. Leaving the pure wheel beside the platform one would put a
  # `py3-none-any` file in the bundle, and that is the artifact that shadows every platform
  # wheel for every installer -- the exact failure stage_public_wheel.sh refuses.
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
    # 1 -> 2 when the bundle grew the public wheel. 2 -> 3 when that public wheel became
    # PLATFORM-SPECIFIC (backlog C10) and the bundle started carrying `platform_tag`.
    # Consumers that verify provenance (TARS deploy/verify_tada_bundle.py,
    # release/validate_lock.py) branch on this, and the bump is what makes reading
    # platform_tag mandatory: a v2 reader must not silently accept a v3 bundle whose wheel
    # only installs on one architecture.
    "schema_version": 3,
    "repository": os.environ["TADA_REPOSITORY"],
    "revision": os.environ["TADA_REVISION"],
    "shared_revision": os.environ["SHARED_REVISION"],
    "workflow_run_id": os.environ["GITHUB_RUN_ID"],
    "workflow_run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
    "built_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "python_version": platform.python_version(),
    "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
    # `wheel` still names the PRIVATE wheel, exactly as it did at schema_version 1.
    "wheel": os.environ["WHEEL_NAME"],
    # New at schema_version 2: the public wheel that `tada` imports at load time. An
    # installer that skips it produces a broken `tada` entry point.
    "render_wheel": os.environ["RENDER_WHEEL_NAME"],
    # New at schema_version 3. The public wheel is now platform-tagged, so the bundle is
    # only installable on a matching platform. TARS declares linux/amd64 and this is how a
    # deploy checks that what it pulled matches what it runs -- rather than discovering it
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

# Five files, and the count is asserted HERE rather than only in the workflow: this script is
# what decides the shape, so it is what should refuse to emit a wrong one. The workflow keeps
# its own check as a second reader.
files=(bundle/*)
if (( ${#files[@]} != 5 )); then
  printf 'bundle must hold exactly five files, found %d:\n' "${#files[@]}" >&2
  printf '  %s\n' "${files[@]}" >&2
  exit 1
fi
printf 'bundle: %s [%s]\n' "$render_wheel" "$platform_tag"
