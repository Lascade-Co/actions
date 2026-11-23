#!/usr/bin/env bash
#
# Write Developer ID signing and notarization material for a macOS DMG release, and export
# the variables the source repo's scripts/release.sh reads (RELEASE_TEAM_ID, RELEASE_CERT,
# RELEASE_CERT_KEY, NOTARY_KEY, NOTARY_KEY_ID, NOTARY_ISSUER) through GITHUB_ENV.
# Files go under RUNNER_TEMP/signing (owner-only), never into the checkout; nothing is echoed.
#
# Usage: write_signing.sh <path for GoogleService-Info.plist>
#
# Required environment variables (Infisical /Build):
#   MACOS_TEAM_ID                     – Apple Developer Team ID
#   MACOS_DEVELOPER_ID_CERT_BASE64    – Developer ID Application certificate (.cer, DER), base64
#   MACOS_DEVELOPER_ID_KEY_BASE64     – its private key (PEM), base64
#   RUNNER_TEMP, GITHUB_ENV           – set by GitHub Actions
#
# Optional:
#   APPSTORE_API_KEY_ID / APPSTORE_API_PRIVATE_KEY / APPSTORE_ISSUER_ID
#                                     – notary API key; otherwise FALLBACK_KEY_ID /
#                                       FALLBACK_PRIVATE_KEY / FALLBACK_ISSUER_ID (org secrets)
#   SERVICE_PLIST_BASE64              – GoogleService-Info.plist, base64, written to $1
set -euo pipefail
umask 077

plist_path="${1:-}"
dir="$RUNNER_TEMP/signing"
mkdir -p "$dir"

for v in MACOS_TEAM_ID MACOS_DEVELOPER_ID_CERT_BASE64 MACOS_DEVELOPER_ID_KEY_BASE64; do
  [ -n "${!v:-}" ] || { echo "::error::$v missing in Infisical /Build"; exit 1; }
done
printf '%s' "$MACOS_DEVELOPER_ID_CERT_BASE64" | base64 --decode > "$dir/developer_id.cer"
printf '%s' "$MACOS_DEVELOPER_ID_KEY_BASE64"  | base64 --decode > "$dir/developer_id.key"

if [ -n "${APPSTORE_API_KEY_ID:-}" ] && [ -n "${APPSTORE_API_PRIVATE_KEY:-}" ] && [ -n "${APPSTORE_ISSUER_ID:-}" ]; then
  key_id="$APPSTORE_API_KEY_ID"; key="$APPSTORE_API_PRIVATE_KEY"; issuer="$APPSTORE_ISSUER_ID"
  echo "Using notary credentials from Infisical"
else
  key_id="${FALLBACK_KEY_ID:-}"; key="${FALLBACK_PRIVATE_KEY:-}"; issuer="${FALLBACK_ISSUER_ID:-}"
  echo "Using notary credentials from GitHub Secrets (fallback)"
fi
[ -n "$key_id" ] && [ -n "$key" ] && [ -n "$issuer" ] || { echo "::error::no notary API key available"; exit 1; }
printf '%s\n' "$key" > "$dir/notary.p8"

# These go into GITHUB_ENV, and the runner shows a step's environment in its public header, so mask
# them first (in this sealed step, before any later step can show them).
for value in "$MACOS_TEAM_ID" "$key_id" "$issuer"; do
  echo "::add-mask::$value"
done

{
  echo "RELEASE_TEAM_ID=$MACOS_TEAM_ID"
  echo "RELEASE_CERT=$dir/developer_id.cer"
  echo "RELEASE_CERT_KEY=$dir/developer_id.key"
  echo "NOTARY_KEY=$dir/notary.p8"
  echo "NOTARY_KEY_ID=$key_id"
  echo "NOTARY_ISSUER=$issuer"
} >> "$GITHUB_ENV"

if [ -n "${SERVICE_PLIST_BASE64:-}" ] && [ -n "$plist_path" ]; then
  printf '%s' "$SERVICE_PLIST_BASE64" | base64 --decode > "$plist_path"
fi

# release.sh validates the identity chain, so make sure Apple's Developer ID G2 intermediate is trusted.
curl -fsSL --retry 3 https://www.apple.com/certificateauthority/DeveloperIDG2CA.cer -o "$dir/DeveloperIDG2CA.cer"
sudo security add-certificates -k /Library/Keychains/System.keychain "$dir/DeveloperIDG2CA.cer" 2>/dev/null || true
