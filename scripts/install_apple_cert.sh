#!/usr/bin/env bash
#
# Install Apple certificate and provisioning profiles for CI builds.
#
# Required environment variables:
#   IOS_CERTIFICATE_BASE64            – Base64-encoded .p12 certificate
#   IOS_CERTIFICATE_PASSWORD          – Password for the .p12
#   IOS_PROVISION_PROFILE_BASE64      – Base64-encoded main app provisioning profile
#   RUNNER_TEMP                       – Temp directory (set by GitHub Actions)
#   GITHUB_ENV                        – Env file path  (set by GitHub Actions)
#
# Optional environment variables (installed only when set — apps that don't set
# them are completely unaffected):
#   IOS_NSE_PROVISION_PROFILE_BASE64    – NSE (OneSignal) provisioning profile
#   IOS_WIDGET_PROVISION_PROFILE_BASE64 – Widget extension profile
#   IOS_WATCH_PROVISION_PROFILE_BASE64  – Watch app profile
#
# Outputs (written to GITHUB_ENV, only for profiles that were installed):
#   APP_PROFILE_UUID,    APP_PROFILE_NAME
#   NSE_PROFILE_UUID,    NSE_PROFILE_NAME
#   WIDGET_PROFILE_UUID, WIDGET_PROFILE_NAME
#   WATCH_PROFILE_UUID,  WATCH_PROFILE_NAME

set -euo pipefail

# Default GitHub-Actions-provided vars so the script also runs locally / on
# non-GitHub CI under `set -u`. RUNNER_TEMP holds the keychain + temporary
# profiles; GITHUB_ENV is where profile UUID/NAME outputs are exported.
: "${RUNNER_TEMP:=/tmp}"
: "${GITHUB_ENV:=/dev/null}"

# Fail fast with a clear message if required secrets are missing, rather than
# with a cryptic base64/security error later.
if [ -z "${IOS_CERTIFICATE_BASE64:-}" ]; then
  echo "ERROR: IOS_CERTIFICATE_BASE64 is required but empty or unset" >&2
  exit 1
fi
if [ -z "${IOS_CERTIFICATE_PASSWORD:-}" ]; then
  echo "ERROR: IOS_CERTIFICATE_PASSWORD is required but empty or unset" >&2
  exit 1
fi

# --- Decode cert ---
KEYCHAIN_PATH="$RUNNER_TEMP/build.keychain-db"
# Remove the decoded .p12 and the temporary keychain (which holds the private
# key, protected only by a hardcoded password) on exit, so they never persist
# on self-hosted runners even if a later step fails. Single-quoted so
# KEYCHAIN_PATH is expanded when the trap fires (it is set just above).
trap 'rm -f certificate.p12 "$KEYCHAIN_PATH"' EXIT
printf '%s' "$IOS_CERTIFICATE_BASE64" | base64 --decode > certificate.p12

# --- Create temporary keychain ---
# Remove a leftover keychain from a previous local run so create-keychain
# doesn't fail (GitHub Actions always starts with a clean RUNNER_TEMP).
rm -f "$KEYCHAIN_PATH"
security create-keychain -p "temp_password" "$KEYCHAIN_PATH"
security set-keychain-settings -lut 21600 "$KEYCHAIN_PATH"
security unlock-keychain -p "temp_password" "$KEYCHAIN_PATH"
security import certificate.p12 -k "$KEYCHAIN_PATH" -P "$IOS_CERTIFICATE_PASSWORD" -T /usr/bin/codesign
security set-key-partition-list -S apple-tool:,apple: -s -k "temp_password" "$KEYCHAIN_PATH"

# Append to keychain search list (preserve existing keychains)
security list-keychains -d user -s "$KEYCHAIN_PATH" $(security list-keychains -d user | sed 's/"//g' | tr '\n' ' ')

# --- Install provisioning profiles (both paths for Xcode 16+ compatibility) ---
PROFILE_DIR_LEGACY="$HOME/Library/MobileDevice/Provisioning Profiles"
PROFILE_DIR_NEW="$HOME/Library/Developer/Xcode/UserData/Provisioning Profiles"
mkdir -p "$PROFILE_DIR_LEGACY" "$PROFILE_DIR_NEW"

# install_profile <base64> <PREFIX> <required|optional>
install_profile() {
  local b64="${1:-}" prefix="$2" required="$3"
  # Write to RUNNER_TEMP so partial runs never leave profiles in the workspace.
  local file="$RUNNER_TEMP/${prefix}.mobileprovision"

  if [ -z "$b64" ]; then
    if [ "$required" = "required" ]; then
      echo "ERROR: ${prefix} profile is required but its base64 variable is empty" >&2
      exit 1
    fi
    echo "Skipping ${prefix} profile (not provided)"
    return 0
  fi

  printf '%s' "$b64" | base64 --decode > "$file"

  local uuid name
  uuid=$(security cms -D -i "$file" | plutil -extract UUID raw -)
  name=$(security cms -D -i "$file" | plutil -extract Name raw -)

  cp "$file" "$PROFILE_DIR_LEGACY/$uuid.mobileprovision"
  cp "$file" "$PROFILE_DIR_NEW/$uuid.mobileprovision"

  echo "${prefix}_PROFILE_UUID=$uuid" >> "$GITHUB_ENV"
  echo "${prefix}_PROFILE_NAME=$name" >> "$GITHUB_ENV"

  echo "Installed profile: $name ($uuid)"
  rm -f "$file"
}

install_profile "${IOS_PROVISION_PROFILE_BASE64:-}"        APP    required
install_profile "${IOS_NSE_PROVISION_PROFILE_BASE64:-}"    NSE    optional
install_profile "${IOS_WIDGET_PROVISION_PROFILE_BASE64:-}" WIDGET optional
install_profile "${IOS_WATCH_PROVISION_PROFILE_BASE64:-}"  WATCH  optional

# --- Cleanup ---
# certificate.p12 and the temporary keychain are removed by the EXIT trap above.
