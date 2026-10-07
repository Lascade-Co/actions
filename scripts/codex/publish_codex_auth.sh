#!/usr/bin/env bash
# Publish a rotated Codex auth.json to Infisical, then read it back to prove it landed.
# A rotation burns the old refresh token, so a valid new auth.json must never be lost:
# this runs even when the refresh step failed, and does nothing if nothing was rotated.
# Env: INFISICAL_CLIENT_ID, INFISICAL_CLIENT_SECRET, INFISICAL_DOMAIN, PROJECT_ID
# Optional: CODEX_AUTH_FILE, BEFORE_REFRESH_FILE, PUBLISH_RETRY_DELAY (seconds, default 10)
set -euo pipefail

AUTH="${CODEX_AUTH_FILE:-$HOME/.codex/auth.json}"
BEFORE_FILE="${BEFORE_REFRESH_FILE:-/tmp/before_refresh}"
DELAY="${PUBLISH_RETRY_DELAY:-10}"
BOUND=""
command -v timeout >/dev/null 2>&1 && BOUND="timeout 60"

AFTER=$(jq -r '.last_refresh // .tokens.last_refresh // "unknown"' "$AUTH" 2>/dev/null || echo unknown)
if [ "$AFTER" = "$(cat "$BEFORE_FILE")" ]; then
  echo "Token was not rotated; nothing to publish."
  exit 0
fi

NEW_B64=$(base64 < "$AUTH" | tr -d '\n')
for attempt in 1 2 3; do
  if INFISICAL_TOKEN=$($BOUND infisical login \
        --method=universal-auth \
        --client-id="$INFISICAL_CLIENT_ID" \
        --client-secret="$INFISICAL_CLIENT_SECRET" \
        --domain="$INFISICAL_DOMAIN" \
        --silent --plain) \
     && export INFISICAL_TOKEN \
     && $BOUND infisical secrets set "CODEX_AUTH_JSON_BASE_64=$NEW_B64" \
        --projectId="$PROJECT_ID" --env=dev --path=/Actions --type=shared --silent \
     && STORED=$($BOUND infisical secrets get CODEX_AUTH_JSON_BASE_64 \
        --projectId="$PROJECT_ID" --env=dev --path=/Actions --plain --silent) \
     && [ "$STORED" = "$NEW_B64" ]; then
    echo "Pushed refreshed auth.json to Infisical (common/dev/Actions/CODEX_AUTH_JSON_BASE_64) and read it back."
    exit 0
  fi
  echo "Infisical publish attempt $attempt failed"
  [ "$attempt" -lt 3 ] && sleep $((attempt * DELAY))
done
echo "::error::Could not publish the rotated auth.json to Infisical"
exit 1
