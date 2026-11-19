#!/usr/bin/env bash
# The Python sender freezes one payload before any HTTP attempt.
set -euo pipefail
exec python3 "$(dirname "$0")/hr_send.py" "$@"
