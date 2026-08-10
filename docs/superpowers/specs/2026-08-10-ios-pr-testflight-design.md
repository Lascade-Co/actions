# iOS PR builds to TestFlight

Date: 2026-08-10

Vocabulary in **bold** is defined in [CONTEXT.md](../../../CONTEXT.md) under *Language — iOS
Builds*. Two decisions here have ADRs: [0008](../../adr/0008-ios-build-number-scheme.md) on the
**build number**, [0009](../../adr/0009-testflight-builds-are-release-with-debug-on.md) on the
build configuration.

## Problem

Android pull requests get an automatic debug build: the **trigger** in
`travel-animator-android` dispatches `build-debug-apk` to `Lascade-Co/actions`, the **central
runner** builds the APK, pushes it to Firebase App Distribution and reports back on the PR. iOS
has no equivalent. Reviewers of an iOS PR have no installable build unless someone manually runs
the `workflow_dispatch` trigger against a branch.

## Goal

A pull request opened in `travel-animator-ios` produces a **TestFlight build**, with status
reported back on the PR, using the same secrets structure the Android debug build already uses.

## What already exists

`.github/workflows/ios-build-debug.yml` is a `repository_dispatch` **central runner** on event
type `debug-ios`. It already mints a GitHub App token, checks out the target repo at
`refs/heads/${branch}`, configures `git ... insteadOf` so private SPM dependencies resolve,
pulls build secrets from Infisical, installs the Apple certificate and profiles via
`scripts/ios/install_apple_cert.sh`, rewrites the pbxproj to manual signing via
`scripts/ios/fix_ios_signing.py`, generates `ExportOptions.plist` with
`method: app-store-connect`, resolves SPM, archives, exports the IPA, uploads dSYMs to
Crashlytics, uploads to TestFlight with `xcrun altool`, and notifies Telegram.

It is driven by `.github/workflows/debug.yml` in `travel-animator-ios`, a `workflow_dispatch`
trigger taking a branch name.

Four facts about its current state, established while designing this:

1. **It has never completed a build.** All ten `debug-ios` runs are `cancelled`; every
   `release-ios` run is cancelled or failed. The most recent run passed every step through
   `Resolve SPM dependencies` and was cancelled during `Archive`.
2. **Its build numbers are below the train floor.** `travel-animator-ios` is at **marketing
   version** `3.9.3` with `CURRENT_PROJECT_VERSION = 213`; the runner uploads
   `github.run_number`, currently ~10. Every upload would be rejected. See ADR-0008.
3. **Three variables it reads do not exist in Infisical.** `IOS_TEAM_ID`, `IOS_BUNDLE_ID` and
   `SERVICE_PLIST_BASE64` are referenced as `env.*` but are absent from the `travelanimator`
   project in every environment and path. `APP_PROFILE_UUID` and `NSE_PROFILE_UUID` are fine —
   `install_apple_cert.sh` writes those to `GITHUB_ENV`.
4. **`increment_version.sh` does not exist** in `travel-animator-ios`, so
   `ios-build-release.yml`'s "Bump version and push" step cannot work. Pre-existing, and out of
   scope here.

(1) and (4) are not in scope. (2) is fixed by this work. (3) is reduced from three unknowns to
one, below.

## Design

### 1. Central runner: extend `ios-build-debug.yml`

No new central workflow. `ios-build-debug.yml` gains PR support the way
`android-build-debug.yml` already has it, so one runner serves both the manual branch dispatch
and the automatic PR dispatch.

**Ref selection.** A new step decides the checkout ref from the payload:

```bash
PR="${{ github.event.client_payload.pr }}"
BR="${{ github.event.client_payload.branch }}"
if [ -n "$PR" ] && [ "$PR" != "null" ]; then
  echo "ref=refs/pull/${PR}/head" >> $GITHUB_OUTPUT
elif [ -n "$BR" ]; then
  echo "ref=refs/heads/${BR}" >> $GITHUB_OUTPUT
else
  echo "No PR or branch in payload" >&2
  exit 1
fi
```

`refs/pull/N/head` is the PR head, not the merge ref — same choice Android makes, so the build
matches what the contributor pushed.

**Build configuration.** The archive keeps `-configuration Release` and adds the `DEBUG` flag:

```
SWIFT_ACTIVE_COMPILATION_CONDITIONS='DEBUG $(inherited)'
GCC_PREPROCESSOR_DEFINITIONS='DEBUG=1 $(inherited)'
```

Testers reach the 69 `#if DEBUG` sites — including the Debug Options screen — while the binary
keeps Release optimisation, clean entitlements and dSYMs. Rationale and consequences in ADR-0009.

**Build number.** A step computes it before the archive and exposes it as a job output:

```bash
BUILD=$(( 1000 + ${{ github.run_number }} ))
PR="${{ github.event.client_payload.pr }}"
if [ -n "$PR" ] && [ "$PR" != "null" ]; then BUILD="${BUILD}.${PR}"; fi
echo "build=$BUILD" >> "$GITHUB_OUTPUT"
```

Passed to `xcodebuild archive` as `CURRENT_PROJECT_VERSION="$BUILD"`, and quoted by the `publish`
job's PR comment. Rationale in ADR-0008 — including why the workflow file must never be renamed.

**Team ID and bundle ID.** `scripts/ios/install_apple_cert.sh` gains two additive exports,
derived from the App Store profile it already decodes:

```bash
APP_PROFILE_TEAM_ID    # TeamIdentifier.0
APP_PROFILE_BUNDLE_ID  # Entitlements.application-identifier, team prefix stripped
```

Skipped when the derived bundle ID ends in `*` (a wildcard profile). The workflow uses
`IOS_TEAM_ID` / `IOS_BUNDLE_ID` from Infisical when set and falls back to these otherwise. Purely
additive, so the other apps using the script are unaffected. This removes two of the three
missing keys and guarantees the values match the profile actually doing the signing.

**Infisical environment.** `env-slug` changes from `prod` to `staging`. Debug and PR builds read
staging; `ios-build-release.yml` keeps reading prod. This matches `android-build-debug.yml`.

**Concurrency.** The group key becomes `debug-ios-${{ repo }}-${{ pr || branch }}` so two PRs do
not cancel each other. `cancel-in-progress: true` stays.

**Timeout.** The `build` job gets `timeout-minutes: 60`. It currently has none, and one past run
consumed 3h1m of macOS runner time before being killed by hand.

**Reporting.** No separate report job — the PR comment is the channel, and a failed `build`
carries its own comment.

- `run-name` becomes `"${{ repo }} · ${{ branch || format('PR #{0}', pr) }}"`.
- `permissions` gains `pull-requests: write` and `issues: write`, mirroring
  `android-build-debug.yml`. The comments are written to the target repo with the GitHub App
  token, so these are for consistency rather than strictly required.
- In `build`: purge prior comments carrying the marker `<!-- central-ios-testflight -->` via
  `actions/github-script@v8`, post "build started" with a run link via
  `peter-evans/create-or-update-comment@v5`, and on `if: failure()` replace it with the failing
  stage plus the run link.
- In `publish`: mint its own App token (it currently has none), and on success replace the
  comment with the **build number**, the **marketing version** and the run link, phrased so a
  tester can find it — "TestFlight → 3.9.3 → build 1010.765".

Every PR-comment step is guarded on `client_payload.pr` being present and not `null`, so the
existing `workflow_dispatch` branch path is unaffected.

**Accepted gap.** Both Telegram notifications stay in `publish`. If `build` fails, `publish` is
skipped and Telegram says nothing — the PR comment covers it. Deliberate; not a bug to fix later.

### 2. Trigger: `travel-animator-ios`

New file `.github/workflows/pr-testflight.yml`, landed on branch `ci`:

```yaml
name: Request central TestFlight build

on:
  pull_request:
    types: [opened, reopened, ready_for_review]

jobs:
  dispatch:
    concurrency:
      group: central-ios-testflight-${{ github.event.pull_request.number }}
      cancel-in-progress: true
    if: >-
      github.event.pull_request.draft == false &&
      github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    steps:
      - name: Dispatch to central repo
        uses: peter-evans/repository-dispatch@v3
        with:
          token: ${{ secrets.CENTRAL_DISPATCH_TOKEN }}
          repository: Lascade-Co/actions
          event-type: debug-ios
          client-payload: >-
            {"repo":${{ toJSON(github.repository) }},
             "pr":${{ toJSON(github.event.pull_request.number) }},
             "branch":${{ toJSON(github.head_ref) }},
             "title":${{ toJSON(github.event.pull_request.title) }},
             "project_slug": "travelanimator"}
```

**Event types.** Drafts are skipped and build when marked ready instead. A draft opened → nothing;
marked ready → `ready_for_review` fires with `draft == false` → builds. Normal PR opened →
builds. `synchronize` is deliberately absent: it would mean a full macOS archive plus a TestFlight
upload on every push to every open PR.

**Fork guard.** `head.repo.full_name == github.repository` skips fork PRs, which have no access to
`CENTRAL_DISPATCH_TOKEN`. Same guard the Android trigger uses.

**Rollout.** `ci` is a proving ground, not the destination. Of the last 40 PRs in the repo, 38
target `main` and none target `ci`, so a trigger living only on `ci` covers nothing real. Validate
by opening a throwaway PR *from* `ci` — `pull_request` resolves the workflow from the merge ref,
so the file being on the head branch is enough to fire — then merge `ci` → `main` for actual
coverage.

### 3. Infisical

Copy five keys from `travelanimator` / `prod` / `/Build` to `travelanimator` / `staging` /
`/Build`, values unchanged:

- `IOS_CERTIFICATE_BASE64`
- `IOS_CERTIFICATE_PASSWORD`
- `IOS_PROVISION_PROFILE_BASE64`
- `IOS_NSE_PROVISION_PROFILE_BASE64`
- `IOS_SHARE_EXT_PROVISION_PROFILE_BASE64`

Identical to prod on purpose: TestFlight and the App Store take the same distribution certificate
and profiles, so a distinct "staging" certificate would produce an IPA that App Store Connect
rejects.

Done with `infisical secrets set` against project ID
`d3963969-c940-4fd7-84c9-7a38edaf404d`, reading each prod value and writing it to staging without
printing it.

Staging `/Build` already holds `KEYSTORE_BASE64`, `KEYSTORE_PASSWORD`, `KEY_ALIAS`,
`KEY_PASSWORD`, `SERVICE_JSON_BASE64`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` and
`VERCEL_PROJECT_BASE64`. None of those are touched.

### 4. Handoff document

`docs/handoff/ios-testflight-missing-secrets.md`, for an agent with browser access and the
Infisical CLI. One blocking item:

| Key | Source | Target envs |
|---|---|---|
| `SERVICE_PLIST_BASE64` | Firebase console → project settings → iOS app → download `GoogleService-Info.plist`, then `base64 -i GoogleService-Info.plist` | staging, prod |

Path `/Build` in project `d3963969-c940-4fd7-84c9-7a38edaf404d`. Both environments, because
`ios-build-release.yml` reads prod and the debug/PR runner reads staging.

The document records the consequence of it being absent — an empty value writes a zero-byte
`GoogleService-Info.plist`, which breaks Firebase at runtime — and also notes the two
pre-existing breaks the agent may as well pick up while it is in there: the missing
`increment_version.sh` in `travel-animator-ios`, and `ios-build-release.yml`'s build number
sitting below the train floor (ADR-0008 explains the constraint on any fix).

## Components and boundaries

| Unit | Purpose | Depends on |
|---|---|---|
| `travel-animator-ios/.github/workflows/pr-testflight.yml` | Turn a PR event into a dispatch | `CENTRAL_DISPATCH_TOKEN` |
| `actions/.github/workflows/ios-build-debug.yml` | Build, sign, upload, report | GitHub App token, Infisical staging `/Build`, App Store Connect API key |
| `scripts/ios/install_apple_cert.sh` | Certificate and profile installation; team/bundle ID derivation | `IOS_CERTIFICATE_*`, `IOS_*_PROVISION_PROFILE_BASE64` |
| `scripts/ios/fix_ios_signing.py` | Switch pbxproj to manual signing | `PBXPROJ_PATH`, `APP_PROFILE_NAME` |

The dispatch payload is the interface between trigger and runner:
`{repo, pr, branch, title, project_slug}`. The runner treats `pr` as optional, which is what keeps
the existing `workflow_dispatch` path working unchanged.

## Error handling

| Failure | Behaviour |
|---|---|
| Fork PR, or draft PR | Trigger job skipped by the `if` guard; nothing dispatched |
| Neither `pr` nor `branch` in payload | Ref step exits 1 immediately |
| Wildcard provisioning profile | Derived bundle ID discarded; `IOS_BUNDLE_ID` from Infisical required |
| Archive or export fails | `build` fails; PR comment replaced with the failing stage plus run link; no Telegram (accepted) |
| TestFlight upload fails | `publish` fails; PR comment and Telegram both report it |
| Telegram down | `continue-on-error: true` on the failure notification, as today |
| Build exceeds 60 minutes | Job cancelled by `timeout-minutes` |

## Testing

No unit-testable code is added — this is workflow YAML plus six lines of bash in a shell script.
Verification is end-to-end:

1. `actionlint` on both changed workflows; `shellcheck` on `install_apple_cert.sh`.
2. Confirm the five `IOS_*` keys are readable from staging `/Build` after the copy, comparing
   value lengths against prod rather than printing values.
3. Confirm the derived `APP_PROFILE_TEAM_ID` / `APP_PROFILE_BUNDLE_ID` match the real team and
   bundle identifiers, by decoding the profile locally.
4. Open a throwaway PR from `ci` in `travel-animator-ios` and confirm: the dispatch fires, the
   central run checks out `refs/pull/N/head`, the PR gets a "build started" comment, the computed
   build number is `1000+run.PR`, and the run reaches `Archive`.
5. A green TestFlight upload additionally depends on `SERVICE_PLIST_BASE64` and on whatever is
   wrong with `Archive`. Both are tracked outside this spec.

## Out of scope

- Diagnosing why no iOS archive has ever completed.
- Obtaining and setting `SERVICE_PLIST_BASE64` — delegated via the handoff document.
- Fixing `ios-build-release.yml`'s build number or the missing `increment_version.sh`.
- Merging the trigger beyond branch `ci`.
- Adding a `Beta` build configuration to the Xcode project (the cleaner form of ADR-0009).
- Running tests or lint before the iOS build.
- Assigning TestFlight groups or setting "What to Test" notes, which `altool` cannot do.
