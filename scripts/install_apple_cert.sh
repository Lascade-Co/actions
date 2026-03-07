#!/usr/bin/env bash
#
# Install Apple certificate and provisioning profiles for CI builds.
#
# Required environment variables:
#   IOS_CERTIFICATE_BASE64          – Base64-encoded .p12 certificate
#   IOS_CERTIFICATE_PASSWORD        – Password for the .p12
#   IOS_PROVISION_PROFILE_BASE64    – Base64-encoded app provisioning profile
#   IOS_NSE_PROVISION_PROFILE_BASE64 – Base64-encoded NSE provisioning profile
#   RUNNER_TEMP                     – Temp directory (set by GitHub Actions)
#   GITHUB_ENV                      – Env file path  (set by GitHub Actions)
#
# Outputs (written to GITHUB_ENV):
#   APP_PROFILE_UUID, APP_PROFILE_NAME
#   NSE_PROFILE_UUID, NSE_PROFILE_NAME

set -euo pipefail

# --- Decode cert and profiles ---
echo "$IOS_CERTIFICATE_BASE64" | base64 --decode > certificate.p12
echo "$IOS_PROVISION_PROFILE_BASE64" | base64 --decode > app.mobileprovision
echo "$IOS_NSE_PROVISION_PROFILE_BASE64" | base64 --decode > nse.mobileprovision

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

install_profile() {
  local file="$1" prefix="$2"

  local uuid name
  uuid=$(security cms -D -i "$file" | plutil -extract UUID raw -)
  name=$(security cms -D -i "$file" | plutil -extract Name raw -)

  cp "$file" "$PROFILE_DIR_LEGACY/$uuid.mobileprovision"
  cp "$file" "$PROFILE_DIR_NEW/$uuid.mobileprovision"

  echo "${prefix}_PROFILE_UUID=$uuid" >> "$GITHUB_ENV"
  echo "${prefix}_PROFILE_NAME=$name" >> "$GITHUB_ENV"

  echo "Installed profile: $name ($uuid)"
}

install_profile app.mobileprovision APP
install_profile nse.mobileprovision NSE

# --- Cleanup decoded files ---
rm certificate.p12 app.mobileprovision nse.mobileprovision