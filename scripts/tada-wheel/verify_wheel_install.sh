#!/usr/bin/env bash
# Verify the built bundle: wheel metadata via twine, then that the bundle ALONE -- with no TADA
# source tree in reach -- installs and both console scripts run. Run from the TADA checkout root.
# Requires uv on PATH and ambient GITHUB_WORKSPACE.
#
# Staging into an EMPTY directory is the point, not tidiness: syncing from the TADA checkout let
# a pylock referencing ./tada_render as an editable local path resolve against the real source
# tree and pass here while failing in every consumer that has no such tree.
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
  # Two named globs rather than `./*.whl`, so a bundle that grew a third wheel fails here
  # instead of installing whatever was lying around. --no-deps for both: the third-party
  # dependencies came from pylock.toml above, and the private wheel's `travel-animator==`
  # pin must be satisfied by its sibling in this directory rather than by an index fetch.
  private=(tada-*.whl)
  public=(travel_animator-*.whl)
  if (( ${#private[@]} != 1 )) || (( ${#public[@]} != 1 )); then
    echo "expected one tada-*.whl and one travel_animator-*.whl in the bundle, found:" >&2
    printf '  %s\n' *.whl >&2
    exit 1
  fi
  uv pip install --python "$python_bin" --no-deps "${private[0]}" "${public[0]}"
)

# The wheel stores `_jvm/jre/bin/java` with its mode, but each installer applies its own rule
# (pip additionally requires the regular-file type bits; uv does not), so the only trustworthy
# check of the execute bit is on the INSTALLED tree. A pure wheel skips this and says so.
"$GITHUB_WORKSPACE/.wheel-check/bin/python" - <<'PY'
import os
import subprocess

# NO sys.path manipulation: the cwd's `tada_render/` is the workspace-member DIRECTORY, not
# the package, so putting it on the path imports an empty namespace package and turns a
# payload check into an ImportError that reads like a packaging bug.
from tada_render import render_bridge

root = render_bridge.payload_root()
if not (root / render_bridge.PAYLOAD_JAR_NAME).is_file():
    print(f"no JVM payload at {root}: pure wheel, nothing to smoke")
    raise SystemExit(0)

command = render_bridge.packaged_runtime()
print("launcher:", " ".join(command))
java = command[0]
if not os.access(java, os.X_OK):
    raise SystemExit(f"{java} is not executable in the INSTALLED tree; the wheel stored the "
                     "mode but this installer did not reproduce it")
# `--help` rather than `self-test`: a GL-less runner must not turn a packaging check into a
# driver check. The GL smoke is its own step, in the matrix leg that has a driver.
result = subprocess.run(command + ["--help"], capture_output=True, text=True)
if result.returncode != 0:
    raise SystemExit(f"the packaged renderer did not start (exit {result.returncode}): "
                     f"{result.stderr[-400:]}")
print("packaged renderer starts on its own JRE: OK")
PY

# Both console scripts, from an unrelated cwd. `tada` imports tada_render at module load, so it
# is what fails when the public wheel is missing; `travel-animator` proves the public
# distribution stands on its own.
(
  cd /tmp
  "$GITHUB_WORKSPACE/.wheel-check/bin/tada" --help
  "$GITHUB_WORKSPACE/.wheel-check/bin/travel-animator" --help
)
