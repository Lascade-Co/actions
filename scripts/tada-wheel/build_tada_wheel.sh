#!/usr/bin/env bash
# Build the TADA wheel bundle (both wheels + runtime pylock + build-metadata + SHA256SUMS).
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
set -euo pipefail

: "${SHARED_REVISION:?}" "${TADA_REPOSITORY:?}" "${TADA_REVISION:?}"

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
wheels=(bundle/*.whl)
if (( ${#wheels[@]} != 2 )); then
  printf 'expected exactly two wheels (tada + travel-animator), found %d:\n' \
    "${#wheels[@]}" >&2
  printf '  %s\n' "${wheels[@]}" >&2
  exit 1
fi

# The two globs are disjoint: a wheel filename starts with its distribution name, and
# `travel_animator-` shares no prefix with `tada-`. Every consumer that selects one wheel by
# glob depends on the two names staying prefix-distinct.
private_wheels=(bundle/tada-*.whl)
render_wheels=(bundle/travel_animator-*.whl)
if (( ${#private_wheels[@]} != 1 )) || (( ${#render_wheels[@]} != 1 )); then
  echo "expected exactly one tada-*.whl and one travel_animator-*.whl:" >&2
  printf '  %s\n' "${wheels[@]}" >&2
  exit 1
fi

private_wheel="$(basename "${private_wheels[0]}")"
render_wheel="$(basename "${render_wheels[0]}")"

WHEEL_NAME="$private_wheel" RENDER_WHEEL_NAME="$render_wheel" uv run --locked python - <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import platform
import subprocess

metadata = {
    # Bumped 1 -> 2 when the bundle grew the public wheel. Consumers that verify provenance
    # (TARS deploy/verify_tada_bundle.py) branch on this.
    "schema_version": 2,
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
