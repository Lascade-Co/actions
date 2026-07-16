#!/usr/bin/env bash
# Build the TADA wheel bundle (wheel + runtime pylock + build-metadata + SHA256SUMS).
# Run from the TADA checkout root. Env: SHARED_REVISION, TADA_REPOSITORY, TADA_REVISION.
# Identity (repository/revision) comes from TADA_* env, NOT ambient GITHUB_*, because this
# runs in the central actions repo. workflow_run_id/attempt stay ambient (the build runs here).
set -euo pipefail

: "${SHARED_REVISION:?}" "${TADA_REPOSITORY:?}" "${TADA_REVISION:?}"

rm -rf bundle
mkdir bundle
uv build --wheel --out-dir bundle
uv export \
  --locked \
  --no-dev \
  --no-emit-project \
  --format pylock.toml \
  --output-file bundle/pylock.toml

shopt -s nullglob
wheels=(bundle/*.whl)
if (( ${#wheels[@]} != 1 )); then
  echo "expected exactly one wheel, found ${#wheels[@]}" >&2
  exit 1
fi

WHEEL_NAME="$(basename "${wheels[0]}")" uv run --locked python - <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import platform
import subprocess

metadata = {
    "schema_version": 1,
    "repository": os.environ["TADA_REPOSITORY"],
    "revision": os.environ["TADA_REVISION"],
    "shared_revision": os.environ["SHARED_REVISION"],
    "workflow_run_id": os.environ["GITHUB_RUN_ID"],
    "workflow_run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
    "built_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "python_version": platform.python_version(),
    "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
    "wheel": os.environ["WHEEL_NAME"],
}
Path("bundle/build-metadata.json").write_text(
    json.dumps(metadata, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

(
  cd bundle
  sha256sum "${wheels[0]##*/}" pylock.toml build-metadata.json > SHA256SUMS
  sha256sum --check SHA256SUMS
)
