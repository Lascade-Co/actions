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
  # Backlog C8: `./*.whl` was "install every wheel here", which was exactly right when the
  # bundle held one of each. It is still right for the GHCR bundle -- deliberately
  # single-platform, one private + one public -- but naming the two globs makes that a
  # STATEMENT rather than a coincidence, and makes a bundle that grew a third wheel fail
  # here instead of installing whatever happened to be lying around. The globs are disjoint
  # (`tada-` and `travel_animator-` share no prefix), so this cannot install the same file
  # twice.
  #
  # --no-deps for both: every third-party dependency already came from pylock.toml above, and
  # the private wheel's `travel-animator==<version>` pin must be satisfied by the sibling
  # wheel in this directory rather than by a fetch from an index.
  private=(tada-*.whl)
  public=(travel_animator-*.whl)
  if (( ${#private[@]} != 1 )) || (( ${#public[@]} != 1 )); then
    echo "expected one tada-*.whl and one travel_animator-*.whl in the bundle, found:" >&2
    printf '  %s\n' *.whl >&2
    exit 1
  fi
  uv pip install --python "$python_bin" --no-deps "${private[0]}" "${public[0]}"
)

# A platform wheel carries its own JRE and jar, and the ONE thing that makes that payload
# useful is that the installed `_jvm/jre/bin/java` is executable. It is stored in the wheel
# with its mode, but every installer applies its own rule (pip additionally requires the
# regular-file type bits; uv does not), so the only trustworthy check is on the INSTALLED
# tree. A payload-less pure wheel skips this and says so.
"$GITHUB_WORKSPACE/.wheel-check/bin/python" - <<'PY'
import os
import subprocess

# NO sys.path manipulation. This runs from the TADA checkout root, whose `tada_render/`
# is the workspace-member DIRECTORY (pyproject.toml + the package one level down), not
# the package -- putting the cwd on the path would import an empty namespace package and
# turn a payload check into an ImportError that reads like a packaging bug.
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
# `--help` rather than `self-test`: this step has no GL requirement of its own, and a
# GL-less runner must not turn a packaging check into a driver check. The GL smoke is its
# own step, in the matrix leg that has a driver.
result = subprocess.run(command + ["--help"], capture_output=True, text=True)
if result.returncode != 0:
    raise SystemExit(f"the packaged renderer did not start (exit {result.returncode}): "
                     f"{result.stderr[-400:]}")
print("packaged renderer starts on its own JRE: OK")
PY

# Both console scripts, from an unrelated cwd. `tada` is the private CLI and is what a consumer
# image build smoke-tests -- exactly what fails when the public wheel is missing, since it
# imports tada_render at module load. `travel-animator` is the public CLI and proves the public
# distribution stands on its own.
(
  cd /tmp
  "$GITHUB_WORKSPACE/.wheel-check/bin/tada" --help
  "$GITHUB_WORKSPACE/.wheel-check/bin/travel-animator" --help
)
