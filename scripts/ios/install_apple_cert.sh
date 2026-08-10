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
#   APP_PROFILE_UUID,    APP_PROFILE_NAME,    APP_PROFILE_TEAM_ID,    APP_PROFILE_BUNDLE_ID
#   NSE_PROFILE_UUID,    NSE_PROFILE_NAME,    NSE_PROFILE_TEAM_ID,    NSE_PROFILE_BUNDLE_ID
#   WIDGET_PROFILE_UUID, WIDGET_PROFILE_NAME, WIDGET_PROFILE_TEAM_ID, WIDGET_PROFILE_BUNDLE_ID
#   WATCH_PROFILE_UUID,  WATCH_PROFILE_NAME,  WATCH_PROFILE_TEAM_ID,  WATCH_PROFILE_BUNDLE_ID
# _PROFILE_BUNDLE_ID is omitted for wildcard profiles.

set -euo pipefail

# --- Decode cert ---
echo "$IOS_CERTIFICATE_BASE64" | base64 --decode > certificate.p12

# --- Create temporary keychain ---
KEYCHAIN_PATH="$RUNNER_TEMP/build.keychain-db"
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
  local file="${prefix}.mobileprovision"

  if [ -z "$b64" ]; then
    if [ "$required" = "required" ]; then
      echo "ERROR: ${prefix} profile is required but its base64 variable is empty" >&2
      exit 1
    fi
    echo "Skipping ${prefix} profile (not provided)"
    return 0
  fi

  echo "$b64" | base64 --decode > "$file"

  local plist uuid name team app_id bundle
  plist=$(security cms -D -i "$file")
  uuid=$(plutil -extract UUID raw - <<<"$plist")
  name=$(plutil -extract Name raw - <<<"$plist")
  team=$(plutil -extract TeamIdentifier.0 raw - <<<"$plist")
  app_id=$(plutil -extract Entitlements.application-identifier raw - <<<"$plist")
  bundle="${app_id#"$team".}"

  cp "$file" "$PROFILE_DIR_LEGACY/$uuid.mobileprovision"
  cp "$file" "$PROFILE_DIR_NEW/$uuid.mobileprovision"

  {
    echo "${prefix}_PROFILE_UUID=$uuid"
    echo "${prefix}_PROFILE_NAME=$name"
    echo "${prefix}_PROFILE_TEAM_ID=$team"
  } >> "$GITHUB_ENV"

  # A wildcard profile yields "*" or "com.example.*" — useless as a bundle id,
  # so leave it unset and let the workflow fall back to IOS_BUNDLE_ID.
  case "$bundle" in
    *\*) echo "Skipping ${prefix}_PROFILE_BUNDLE_ID (wildcard profile: $app_id)" ;;
    *)   echo "${prefix}_PROFILE_BUNDLE_ID=$bundle" >> "$GITHUB_ENV" ;;
  esac

  echo "Installed profile: $name ($uuid)"
  rm -f "$file"
}

install_profile "${IOS_PROVISION_PROFILE_BASE64:-}"        APP    required
install_profile "${IOS_NSE_PROVISION_PROFILE_BASE64:-}"    NSE    optional
install_profile "${IOS_WIDGET_PROVISION_PROFILE_BASE64:-}" WIDGET optional
install_profile "${IOS_WATCH_PROVISION_PROFILE_BASE64:-}"  WATCH  optional

# --- Cleanup ---
rm -f certificate.p12