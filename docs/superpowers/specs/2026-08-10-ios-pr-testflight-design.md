# iOS PR builds to TestFlight

Date: 2026-08-10

## Problem

Android pull requests get an automatic debug build: the PR trigger in
`travel-animator-android` dispatches `build-debug-apk` to `Lascade-Co/actions`,
the central runner builds the APK, pushes it to Firebase App Distribution and
reports back on the PR. iOS has no equivalent. Reviewers of an iOS PR have no
installable build unless someone manually runs the `workflow_dispatch` trigger
against a branch.

## Goal

A pull request opened in `travel-animator-ios` produces a TestFlight build,
with status reported back on the PR, using the same secrets structure the
Android debug build already uses.

## What already exists

`.github/workflows/ios-build-debug.yml` in this repo is a `repository_dispatch`
runner on event type `debug-ios`. It already:

- mints a GitHub App token scoped to the payload's owner,
- checks out the target repo at `refs/heads/${branch}`,
- configures `git ... insteadOf` so private SPM dependencies resolve,
- pulls build secrets from Infisical (`project_slug` from the payload,
  `env-slug: prod`, `secret-path: /Build`),
- installs the Apple certificate and provisioning profiles via
  `scripts/ios/install_apple_cert.sh`,
- rewrites the pbxproj to manual signing via `scripts/ios/fix_ios_signing.py`,
- generates `ExportOptions.plist` with `method: app-store-connect`,
- resolves SPM, archives, exports the IPA, uploads dSYMs to Crashlytics,
- uploads to TestFlight with `xcrun altool`, and notifies Telegram.

It is driven by `.github/workflows/debug.yml` in `travel-animator-ios`, a
`workflow_dispatch` trigger taking a branch name.

Two facts about its current state, established while designing this:

1. **It has never completed a build.** All ten `debug-ios` runs are
   `cancelled`; every `release-ios` run is cancelled or failed. The most recent
   run passed every step through `Resolve SPM dependencies` and was cancelled
   during `Archive`.
2. **Three variables it reads do not exist in Infisical.** `IOS_TEAM_ID`,
   `IOS_BUNDLE_ID` and `SERVICE_PLIST_BASE64` are referenced as `env.*` but are
   absent from the `travelanimator` project in every environment and path.
   `APP_PROFILE_UUID` and `NSE_PROFILE_UUID` are fine — `install_apple_cert.sh`
   writes those to `GITHUB_ENV`.

Neither is in scope to fix here. This spec wires up the PR path correctly; the
first real PR build is what will surface whether the archive itself is broken.

## Design

### 1. Central runner: extend `ios-build-debug.yml`

No new central workflow. `ios-build-debug.yml` gains PR support the way
`android-build-debug.yml` already has it, so one runner serves both the manual
branch dispatch and the automatic PR dispatch.

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

`refs/pull/N/head` is the PR head, not the merge ref — same choice Android
makes, so the build matches what the contributor pushed.

**Infisical environment.** `env-slug` changes from `prod` to `staging`. Debug
and PR builds read staging; `ios-build-release.yml` keeps reading prod. This
matches `android-build-debug.yml`, which reads staging.

**Build number.** `CURRENT_PROJECT_VERSION` changes from
`${{ github.run_number }}` to `$(date -u +%s)`.

`github.run_number` is a per-workflow counter. `ios-build-debug` sits at ~10 and
`ios-build-release` has an independent counter at similar values, so the two
workflows generate colliding build numbers, and App Store Connect rejects an
upload whose build number is already used for that marketing version. Epoch
seconds is unique across workflows, monotonic, and stays below Apple's
4294967295 ceiling until 2106.

**Concurrency.** The group key becomes
`debug-ios-${{ repo }}-${{ pr || branch }}` so two PRs do not cancel each other.
`cancel-in-progress: true` stays — a superseded build for the same PR should
die.

**Timeout.** The `build` job gets `timeout-minutes: 60`. It currently has none,
and one past run consumed 3h1m of macOS runner time before being killed by hand.

**PR feedback.** Following the Android runner:

- `run-name` becomes
  `"${{ repo }} · ${{ branch || format('PR #{0}', pr) }}"`, matching the Android
  runner.
- `permissions` gains `pull-requests: write` and `issues: write`, mirroring
  `android-build-debug.yml`. The comments themselves are written to the target
  repo with the GitHub App token, so these are for consistency rather than
  strictly required.
- Before building, purge prior comments carrying the marker
  `<!-- central-ios-testflight -->` (via `actions/github-script@v8`), then post
  "build started" with a link to the run
  (`peter-evans/create-or-update-comment@v5`, App token, `repository` set to the
  target repo).
- On success, replace it with the TestFlight build number and the run link.
- On failure, replace it with the failing stage and the run link.

Every PR-comment step is guarded on `client_payload.pr` being present and not
`null`, so the existing `workflow_dispatch` branch path is unaffected.

The existing Telegram success/failure notifications stay, with the PR number and
title added when present. The IPA artifact upload stays as it is.

### 2. Trigger: `travel-animator-ios`

New file `.github/workflows/pr-testflight.yml`, landed on branch `ci`:

```yaml
name: Request central TestFlight build

on:
  pull_request:
    types: [opened, reopened]

jobs:
  dispatch:
    concurrency:
      group: central-ios-testflight-${{ github.event.pull_request.number }}
      cancel-in-progress: true
    if: ${{ github.event.pull_request.head.repo.full_name == github.repository }}
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

**Event types.** `opened, reopened` only, not GitHub's default set. Adding
`synchronize` would mean a full macOS archive plus a TestFlight upload on every
push to every open PR.

**Fork guard.** `head.repo.full_name == github.repository` skips fork PRs, which
have no access to `CENTRAL_DISPATCH_TOKEN`. Same guard the Android trigger uses.

**Payload carries both `pr` and `branch`.** The runner prefers `pr` for the
checkout ref; `branch` is there for the run name and the Telegram message.

**Coverage across base branches.** For `pull_request`, GitHub resolves the
workflow from the PR's merge ref, so a PR whose head branch carries this file
fires even when the base branch does not have it. Reliable coverage of "any
branch" still requires the file to reach `main` and `dev`. Landing it on `ci`
first is deliberate — it validates the wiring before it applies org-wide.

### 3. Infisical

Copy five keys from `travelanimator` / `prod` / `/Build` to
`travelanimator` / `staging` / `/Build`, values unchanged:

- `IOS_CERTIFICATE_BASE64`
- `IOS_CERTIFICATE_PASSWORD`
- `IOS_PROVISION_PROFILE_BASE64`
- `IOS_NSE_PROVISION_PROFILE_BASE64`
- `IOS_SHARE_EXT_PROVISION_PROFILE_BASE64`

The values are identical to prod on purpose: TestFlight uploads must be signed
with the App Store distribution certificate and App Store provisioning
profiles. A separate "staging" certificate would produce an IPA that App Store
Connect rejects.

Done with `infisical secrets set` against project ID
`d3963969-c940-4fd7-84c9-7a38edaf404d`, reading each prod value and writing it
to staging without printing it.

Staging `/Build` already holds `KEYSTORE_BASE64`, `KEYSTORE_PASSWORD`,
`KEY_ALIAS`, `KEY_PASSWORD`, `SERVICE_JSON_BASE64`, `TELEGRAM_BOT_TOKEN`,
`TELEGRAM_CHAT_ID` and `VERCEL_PROJECT_BASE64`. None of those are touched.

### 4. Handoff document for the missing keys

`docs/handoff/ios-testflight-missing-secrets.md`, written for an agent with
browser access and the Infisical CLI. For each of `IOS_TEAM_ID`,
`IOS_BUNDLE_ID` and `SERVICE_PLIST_BASE64` it states where the workflow consumes
it, where to source the value, and the exact command to set it.

| Key | Source | Target envs |
|---|---|---|
| `IOS_TEAM_ID` | Apple Developer → Membership details → Team ID | staging, prod |
| `IOS_BUNDLE_ID` | `TravelRoute.xcodeproj/project.pbxproj` → `PRODUCT_BUNDLE_IDENTIFIER` of the main app target; cross-check against App Store Connect | staging, prod |
| `SERVICE_PLIST_BASE64` | Firebase console → project settings → iOS app → download `GoogleService-Info.plist`, then `base64 -i GoogleService-Info.plist` | staging, prod |

All three go to path `/Build` in project
`d3963969-c940-4fd7-84c9-7a38edaf404d`. Both environments, because
`ios-build-release.yml` reads prod and the debug/PR runner reads staging.

The document also records the consequence of each being absent: an empty
`IOS_TEAM_ID` or `IOS_BUNDLE_ID` produces an `ExportOptions.plist` that
`xcodebuild -exportArchive` rejects, and an empty `SERVICE_PLIST_BASE64` writes
a zero-byte `GoogleService-Info.plist` that breaks Firebase at runtime.

## Components and boundaries

| Unit | Purpose | Depends on |
|---|---|---|
| `travel-animator-ios/.github/workflows/pr-testflight.yml` | Turn a PR event into a dispatch | `CENTRAL_DISPATCH_TOKEN` |
| `actions/.github/workflows/ios-build-debug.yml` | Build, sign, upload, report | GitHub App token, Infisical staging `/Build`, App Store Connect API key |
| `scripts/ios/install_apple_cert.sh` | Certificate and profile installation | `IOS_CERTIFICATE_*`, `IOS_*_PROVISION_PROFILE_BASE64` |
| `scripts/ios/fix_ios_signing.py` | Switch pbxproj to manual signing | `PBXPROJ_PATH` |

The dispatch payload is the interface between the trigger and the runner:
`{repo, pr, branch, title, project_slug}`. The runner treats `pr` as optional,
which is what keeps the existing `workflow_dispatch` path working unchanged.

## Error handling

| Failure | Behaviour |
|---|---|
| Fork PR | Trigger job skipped by the `if` guard; nothing dispatched |
| Neither `pr` nor `branch` in payload | Ref step exits 1 immediately |
| Archive or export fails | Job fails; PR comment replaced with the failing stage plus run link; Telegram failure message |
| TestFlight upload fails | `publish` job fails; same reporting path |
| Telegram down | `continue-on-error: true` on the failure notification, as today |
| Build exceeds 60 minutes | Job cancelled by `timeout-minutes` instead of running for hours |

## Testing

No unit-testable code is added — this is workflow YAML. Verification is
end-to-end:

1. `actionlint` (or `gh workflow view`) on both changed YAML files.
2. Confirm the five `IOS_*` keys are readable from staging `/Build` after the
   copy, comparing value lengths against prod rather than printing values.
3. Open a throwaway PR against `ci` in `travel-animator-ios` and confirm: the
   dispatch fires, the central run checks out `refs/pull/N/head`, the PR gets a
   "build started" comment, and the run reaches `Archive`.
4. Reaching a green TestFlight upload depends on the three missing keys and on
   whatever is wrong with `Archive`. Both are tracked outside this spec.

## Out of scope

- Diagnosing why no iOS archive has ever completed.
- Obtaining and setting `IOS_TEAM_ID`, `IOS_BUNDLE_ID`, `SERVICE_PLIST_BASE64` —
  delegated via the handoff document.
- Merging the trigger beyond branch `ci`.
- Running tests or lint before the iOS build (the Android runner does; iOS does
  not, and adding it is a separate decision).
- Assigning TestFlight groups or setting "What to Test" notes, which `altool`
  cannot do.
