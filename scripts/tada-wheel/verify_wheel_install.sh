#!/usr/bin/env bash
# Verify the built wheel: metadata via twine, pylock resolves into a fresh venv, wheel installs,
# and the `tada` console script runs from an unrelated cwd. Run from the TADA checkout root.
# Requires uv on PATH and ambient GITHUB_WORKSPACE.
set -euo pipefail

uvx --from twine==6.2.0 twine check bundle/*.whl
rm -rf .wheel-check
uv venv --python 3.12 .wheel-check
uv pip sync --python .wheel-check/bin/python bundle/pylock.toml
uv pip install --python .wheel-check/bin/python --no-deps bundle/*.whl
(
  cd /tmp
  "$GITHUB_WORKSPACE/.wheel-check/bin/tada" --help
)
