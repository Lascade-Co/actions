#!/usr/bin/env bash
# Keep the workflow entry point; the Python sender freezes and retries one payload.
set -euo pipefail
exec python3 "$(dirname "$0")/ip_send.py" "$@"
