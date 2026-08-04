#!/usr/bin/env bash
# Verify the built bundle: wheel metadata via twine, then that the bundle ALONE -- with no TADA
# source tree in reach -- installs and both console scripts run. Run from the TADA checkout root.
# Requires uv on PATH and ambient GITHUB_WORKSPACE.
#
# Staging the bundle into an empty directory is the point of this script, not a tidiness detail.
# Consumers resolve pylock.toml from /bundle, a directory holding only the bundle files. Running
# `uv pip sync` from the TADA checkout instead let a pylock that referenced ./tada_render as an
# editable local path resolve against the real source tree and pass here, while failing in every
# consumer that has no such tree. Syncing from a directory with no tada_render/ means this check
# can only pass when the bundle is genuinely self-contained.
set -euo pipefail

uvx --from twine==6.2.0 twine check bundle/*.whl

staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT
cp bundle/*.whl bundle/pylock.toml "$staging/"

rm -rf .wheel-check
uv venv --python 3.12 .wheel-check
python_bin="$GITHUB_WORKSPACE/.wheel-check/bin/python"
(
  cd "$staging"
  uv pip sync --python "$python_bin" pylock.toml
  # --no-deps for both: every third-party dependency already came from pylock.toml above, and
  # the private wheel's `tacli==<version>` pin must be satisfied by the sibling wheel in this
  # directory rather than by a fetch from an index.
  uv pip install --python "$python_bin" --no-deps ./*.whl
)

# Both console scripts, from an unrelated cwd. `tada` is the private CLI and is what a consumer
# image build smoke-tests -- exactly what fails when the public wheel is missing, since it
# imports tada_render at module load. `tacli` is the public CLI and proves the public
# distribution stands on its own.
(
  cd /tmp
  "$GITHUB_WORKSPACE/.wheel-check/bin/tada" --help
  "$GITHUB_WORKSPACE/.wheel-check/bin/tacli" --help
)
