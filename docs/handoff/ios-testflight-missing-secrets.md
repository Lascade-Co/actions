# Handoff: iOS TestFlight — one missing secret and two pre-existing breaks

For an agent with browser access and the Infisical CLI, logged in to
`https://secrets.lascade.com` as a user with write access to the `travelanimator`
project.

The shell commands below assume macOS — `base64 -i` and `plutil` are macOS-specific
(on Linux use `base64 -w0 < file` and any plist reader).

Infisical project ID: `d3963969-c940-4fd7-84c9-7a38edaf404d`
Secret path: `/Build`

## 1. Blocking — `SERVICE_PLIST_BASE64`

`.github/workflows/ios-build-debug.yml` decodes this into `GoogleService-Info.plist`
before archiving. It does not exist in any environment of the `travelanimator`
project, so today the step writes a zero-byte plist and Firebase breaks at runtime.
The archive itself still succeeds, which is why this is invisible until a tester
opens the app.

**Source:** Firebase console → the Travel Animator project → Project settings →
Your apps → the iOS app with bundle id `com.travelanimator.routemap` → download
`GoogleService-Info.plist`.

**Set it in both environments** — `ios-build-release.yml` reads `prod`, and the
debug/PR runner reads `staging`:

```bash
PID=d3963969-c940-4fd7-84c9-7a38edaf404d
base64 -i GoogleService-Info.plist | tr -d '\n' > /tmp/plist.b64
for ENV in staging prod; do
  infisical secrets set "SERVICE_PLIST_BASE64=@/tmp/plist.b64" \
    --projectId "$PID" --env "$ENV" --path /Build
done
rm -f /tmp/plist.b64
```

**Verify** without printing the value:

```bash
for ENV in staging prod; do
  infisical secrets get SERVICE_PLIST_BASE64 --projectId "$PID" --env "$ENV" \
    --path /Build --plain --silent | base64 --decode \
    | plutil -extract BUNDLE_ID raw -
done
```

Expected: `com.travelanimator.routemap` twice.

## Not needed — `IOS_TEAM_ID` and `IOS_BUNDLE_ID`

These were previously missing too. They are now derived at build time from the App
Store provisioning profile by `scripts/ios/install_apple_cert.sh`
(`APP_PROFILE_TEAM_ID`, `APP_PROFILE_BUNDLE_ID`). Setting them in Infisical is
optional — the workflow prefers Infisical values when present and falls back to the
derived ones. Do not set them unless the profile is a wildcard, which it is not:
team `ZG6QASWQ43`, bundle `com.travelanimator.routemap`.

## 2. Pre-existing — `ios-build-release.yml` uploads a build number below the floor

App Store Connect requires the build number to strictly increase within a marketing
version. `travel-animator-ios` is at `3.9.3` with `CURRENT_PROJECT_VERSION = 213`, so
that train's floor is ~213. `ios-build-release.yml` uploads `github.run_number`,
currently ~9. Every release upload would be rejected.

Fixing it is not just an offset. PR builds now occupy `1000+run_number` dotted with
the PR number in the same train, and the two workflows have independent counters that
will cross. The release workflow needs a band that always sits above the PR builds —
see `docs/adr/0008-ios-build-number-scheme.md` for the full constraint.

## 3. Pre-existing — `increment_version.sh` does not exist

`ios-build-release.yml` runs `bash increment_version.sh` in the checked-out app repo
to bump the marketing version after a release. No such file exists in
`travel-animator-ios` on any branch. That step fails.
