# Central KMP debug CI

The first hosted run (39952223435, 2026-10-23) confirmed the optimized iOS link
runs out of Java heap on the 7-GB arm64 `macos-26` runner at a 5-GiB Native heap.
The user then chose the free 14-GB Intel `macos-26-intel` runner (Xcode 26.6
installed); the source repository raised the Native heap to 8 GiB. Intel timing
and archive/upload acceptance remain unverified until the next hosted run. The four signing profile values were
uploaded and verified through the CLI on 2026-10-23.
Existing Android/iOS workflows are unchanged.

The authoritative approved plan is
`~/Downloads/travel-animator-ci-plan-2026-10-23/plan.md`.

## Contract

Copy `triggers/kmp-debug.yml` into the app repository and set its Infisical project
slug. It dispatches `kmp-debug` for same-repository PR opens, updates and reopens,
or a manual branch selection. It never checks out code in the privileged trigger
and does not run on merges to main. The central runner independently rejects fork
PRs and resolves the source to one immutable commit for every job. Manual builds
use unique `debug-run-<run_number>` releases; PR builds use `debug-pr-<PR>`.

`prepare` resolves that commit, reads the source version and starts the marker
comments. `check`, `android` and `ios-build` then run in parallel. Publishing jobs
require their build job and a successful `check` gate. If Claude pushes a lint
fix, the old artifacts are withheld, an immediate PR comment explains the push,
and the new commit retriggers CI. A source-HEAD lease protects concurrent changes;
the loop guard prevents repeated bot-fix commits. Unit-test failures cannot enter
the lint auto-fix path. Lint failures with no pushed fix fail the check job.

Build behavior belongs to the app's tested entrypoints:

| Entrypoint | Contract |
| --- | --- |
| `scripts/ci/android-debug.sh check` | Unit tests and both Android lint tasks; exit 0 only when all pass; `build/ci/lint-result.json` carries `unit_test_exit_code` and `lint_exit_code`; lint log at `build/ci/lint-output.txt`. |
| `scripts/ci/android-debug.sh build` | Build debug APK; `GITHUB_OUTPUT` supplies relative `apk_path`. |
| `scripts/ci/ios-testflight.sh --build-suffix N --resolve-only` | Generate source version then resolve pinned Swift packages; no link/archive/export. |
| `scripts/ci/ios-testflight.sh --build-suffix N --skip-resolve` | Generate source version, link optimized Staging framework, archive and export; outputs `ipa_path`, `dsym_path`, `build_number`, `timings_path`, `memory_path`. Never uploads. |

JDK 25 is installed without Java's dependency cache. iOS builds on the free standard
arm64 `macos-26` (7 GB) and explicitly selects Xcode 26.6; the source repository links with Kotlin/Native
release caches. The Intel `macos-26-intel` trial timed out with the link still running after 41 minutes. Linux uses `ubuntu-latest`. No iOS
native, device, screenshot or instrumented tests run in the hosted check job.
The app's complete local acceptance gate remains separate.

## Credentials and signing

Each job mints a fresh GitHub App token scoped only to the source repository.
That token also supplies `GH_TOKEN` for the app's pinned ANGLE release asset.
Infisical machine identity reads the dispatched project, environment `staging`:

- `/Local-Properties` is exported to a temporary file and rendered into Java
  properties without evaluating shell code. Values retain quotes, backslashes
  and newlines; SDK location is the runner's SDK path. Template defaults and the
  two template-excluded Zoho keys match the source template.
- `/Build` supplies `KEYSTORE_BASE64`, `SERVICE_JSON_BASE64`, `KEYSTORE_PASSWORD`,
  `KEY_ALIAS`, `KEY_PASSWORD`, `IOS_CERTIFICATE_BASE64`,
  `IOS_CERTIFICATE_PASSWORD` and the four explicit Staging profile variables.
  Android's signing values are mapped to the source's `_DEBUG` variable names.

Shared GitHub secrets remain synced from the existing shared Infisical project:
`CI_APP_CLIENT_ID`, `CI_APP_PRIVATE_KEY`, `INFISICAL_CLIENT_ID`,
`INFISICAL_CLIENT_SECRET`, `INFISICAL_DOMAIN`, `APPSTORE_API_KEY_ID`,
`APPSTORE_ISSUER_ID`, `APPSTORE_API_PRIVATE_KEY`, `TELEGRAM_BOT_TOKEN`, and
`TELEGRAM_CHAT_ID`. The source trigger uses `CENTRAL_DISPATCH_TOKEN`.
`GRADLE_ENCRYPTION_KEY`, if already provisioned, enables encrypted Gradle
configuration-cache persistence; without it the action omits that state.

The certificate is imported into a temporary keychain with automatic job-post
cleanup. Profiles are installed into Xcode's `UserData/Provisioning Profiles`
folder and explicitly cleaned with `always()`. The source owns Staging manual
signing and `ExportOptions-Staging.plist`; no central signing rewrite runs.
The committed export mapping must match the exact profile names in
[kmp-apple-setup.md](kmp-apple-setup.md).

The build uploads dSYMs using the checked-out Firebase SDK and the source Staging
Firebase plist. A separate macOS job uploads the retained IPA with `altool
--upload-package`, using a temporary private-key file deleted on success or
failure. No processing poll or tester-group API call is performed. Successful
upload comments explicitly say Apple processing is pending.

## Caches and evidence

The [four composites](kmp-composite-actions.md) provide dependency bundles,
signing, marker comments and Telegram messages. Every control download is pinned
to the workflow SHA, retried with bounded curl retries, and promoted from a
successful nonempty temporary file.

GHCR bundles cover Gradle dependencies/wrapper, Kotlin/Native, SwiftPM and ANGLE.
Exact keys include build/catalog/wrapper hashes, Kotlin version plus architecture,
`Package.resolved` plus pinned Xcode, and the ANGLE revision respectively. Each
namespace has a moving `latest` fallback. SwiftPM publishes immediately after
resolve, before the memory-sensitive link; cache publication failures are visible
warnings. Restores report selected key and byte size. Only small Gradle build and
configuration state uses the Actions cache through `setup-gradle`'s documented
selective includes/excludes. The runner must confirm actual resulting cache size
on its first run.

The workflow retains app-owned step timings and Native peak-RSS logs as small
artifacts. Diagnostic uploads exclude package checkouts, DerivedData, archives and
IPAs. GitHub records per-step timings for all orchestration steps. PR comments and
Telegram report each platform's success, failure or publishing block; notification
errors remain visible. No Firebase App Distribution step exists.

## Validation and rollout

Local central checks:

```sh
python3 -m unittest discover -s scripts/kmp -p 'test_*.py' -v
node --test scripts/kmp/pr_status_comment.test.cjs
actionlint .github/workflows/kmp-build-debug.yml triggers/kmp-debug.yml
git diff --check
```

The current Action versions were checked against official GitHub releases on
2026-10-23. `create-github-app-token` uses the exact `v3.3.0` tag: this release's
`client-id` input is absent from actionlint's older built-in `v3` metadata.
Installed Xcode 26.6 `altool` 26.40.1 help verifies the package-upload API-key form.
[Xcode runner inventory](https://github.com/actions/runner-images/blob/main/images/macos/macos-26-arm64-Readme.md),
[Gradle selective caching](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md),
[Infisical file export](https://github.com/Infisical/secrets-action/blob/v1.0.18/src/index.ts),
and [GitHub App token inputs](https://github.com/actions/create-github-app-token/blob/v3.3.0/action.yml)
are the implementation references.

The first local optimized Native link used a 5-GiB out-of-process compiler heap,
1-GiB Gradle heap, one worker and no parallel Gradle execution. It failed with Java
heap exhaustion after **118.47 s**, with **6,012,108,800 bytes (5.60 GiB)** peak RSS.
This sample predates the final Native Staging hook-constant change. It is local
failure evidence, not final-source acceptance or a hosted 7-GB measurement.
No larger/paid/self-hosted runner was selected. The user retained the 7-GB runner
after this report; that decision does not turn the failed measurement into
acceptance. Staging archive/export and the hosted iOS path remain unverified. Source evidence is archived separately by the
app implementation; see its `docs/ci.md` for the complete gate results.

Rollout order remains:

1. Retain the user-selected 7-GB runner and the documented memory failure; report
   any new OOM rather than assuming the budget is proven.
2. Make the central workflow/composites live.
3. Merge the reviewed app implementation through the coordinator. Merge the app
   trigger only after the central workflow is live.
4. Profile upload is complete: all four generated profile values were saved to
   Infisical `staging` `/Build` and verified by fresh CLI readback.
5. Open the test PR, run cold then warm, and report job/step timings, cache bytes,
   actual hosted Native RSS, TestFlight build/processing state and APK release URL.

Targets are Android ≤12 min warm / ≤20 min cold and iOS ≤25 min warm / ≤35 min
cold. No target is claimed met before those runs. Stop and report any new OOM or
warm iOS path over 35 minutes; do not change runner class without user direction.
