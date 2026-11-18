# Amper (Kotlin Toolchain) central CI

`amper-debug` and `amper-release` build Kotlin Toolchain apps (first app: TreasureHunch). The app
repo owns its build knowledge in a committed manifest; this repo owns the workflows and
`scripts/amper/control.py`, which does the file-level work. Every job checks out this repo at the run's SHA into
`control/` (`persist-credentials: false`). `amper-release`, and `amper-debug`'s `prepare` registry check, IPA parking
and `ios-publish`, run the scripts from that trusted `control/` checkout (`release.py` must run there: it loads its
siblings by path). `amper-debug`'s build steps instead fetch `control.py` into `$RUNNER_TEMP` by SHA-pinned raw URL
(`https://raw.githubusercontent.com/Lascade-Co/actions/<sha>/scripts/amper/control.py`).

## App manifest: `.ci/amper.json`

```json
{ "android_module": "androidApp", "ios_project": "iosApp/module.xcodeproj", "ios_scheme": "app",
  "ids": { "debug": "com.lascade.hunch.debug", "release": "com.lascade.hunch" },
  "ios_plist": { "debug": "iosApp/src/GoogleService-Info.plist", "release": "iosApp/firebase/release/GoogleService-Info.plist" },
  "ios_entitlements": "iosApp/src/iosApp.entitlements",
  "build_secrets_kt": "shared/src/com/lascade/hunch/platform/BuildSecrets.kt",
  "version_files": ["androidApp/module.yaml"] }
```

| Key | Meaning |
| --- | --- |
| `android_module` | Directory holding the Android `module.yaml` (`settings.android.*`). |
| `ios_project` | The generated `.xcodeproj` whose `project.pbxproj` is edited. |
| `ios_scheme` | Scheme the workflow archives. |
| `ids` | Application ID / bundle ID per variant. |
| `ios_plist` | Firebase plist per variant. Release copies its plist over the debug path. |
| `ios_entitlements` | Entitlements file. Release sets App Attest to `production`. If the file is missing, the step is skipped with a warning (the app repo adds it later). |
| `build_secrets_kt` | Kotlin file with `const val APPCHECK_DEBUG_TOKEN = "..."`. |
| `version_files` | `module.yaml` files that receive `versionName` / `versionCode`. The release commit may change only these and the pbxproj. |
| `firebase_android_app_id` | Optional. Release Firebase Android app ID (`1:<number>:android:<hex>`) for the Crashlytics mapping upload. Without it, `amper-release` takes the `<android_module>/google-services.json` client whose package is `ids.release`; with neither, the release fails. |
| `crashlytics_mapping` | Optional; only `false` has an effect: the release skips the Crashlytics mapping ID and upload (with a warning) instead of failing when no app ID is found. |

The repo root also needs the Kotlin wrapper `kotlin` containing `kotlin_cli_version=X.Y.Z`;
that line selects the toolchain version.

## `control.py` subcommands

Each prints `key=value` lines, ready to append to `$GITHUB_OUTPUT`. All take `--source DIR`
unless noted. Failures exit non-zero with an `error:` message.

| Command | Does |
| --- | --- |
| `metadata [--suffix N]` | Prints `version` (`versionName`), `build_number` (`--suffix` if given, else `versionCode`) and `toolchain`. |
| `set-version --version X --build N` | Rewrites only the `versionCode` and `versionName` lines of each `version_files` entry (exactly one of each, else it fails; read back with `yq`, because `yq -i` reflows the file and the release commits it), and `MARKETING_VERSION` / `CURRENT_PROJECT_VERSION` on the app target in the pbxproj. |
| `set-variant --variant debug\|release [--team T --profile NAME]` | Writes the IDs; for release also swaps the plist, sets the App Attest entitlement and blanks the token; for debug writes `$APPCHECK_DEBUG_TOKEN` into `BuildSecrets.kt`. With `--team` and `--profile` (both variants; debug is signed for TestFlight) it puts manual signing (`CODE_SIGN_STYLE`, `DEVELOPMENT_TEAM`, `PROVISIONING_PROFILE_SPECIFIER`, `CODE_SIGN_IDENTITY = "Apple Distribution"`) on the app target's build settings. Command-line signing overrides did not work in the spike. |
| `android-inputs` | Decodes `KEYSTORE_BASE64` (whitespace and line breaks stripped first, so line-wrapped base64 works) to `<android_module>/ci.jks` and writes `keystore.properties` from `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`. |
| `sign-apk [--build-tools DIR]` | `kotlin build -v release` leaves the APK unsigned (`signing:` in `module.yaml` applies only to `kotlin package`, the AAB). Takes the `unsigned-apk` match, runs `zipalign -f -p 4`, then `apksigner sign --ks <android_module>/ci.jks --ks-pass env:KEYSTORE_PASSWORD --ks-key-alias $KEY_ALIAS --key-pass env:KEY_PASSWORD` (passwords only through the environment), then `apksigner verify --print-certs` (to stderr). Writes `build/signed/<name without -unsigned>-signed.apk` and prints `path`. Build tools: `--build-tools`, else the newest `$ANDROID_HOME/build-tools/*` with both tools. |
| `find-artifact --kind apk\|unsigned-apk\|aab\|mapping` | `apk` is the signed output, `build/signed/*.apk`. `unsigned-apk` and `aab` glob `build/tasks/*/*.apk` or `*.aab` (the top of each task dir; the toolchain keeps a second copy inside its generated `gradle-project`); for `unsigned-apk`, a task dir whose name contains `release` (any case) wins over others. `mapping` globs `build/tasks/**/outputs/mapping/release/mapping.txt`; when both the AAB (`kotlin package`) and the APK (`kotlin build -v release`) ran R8, the two files must be identical and the bundle task's copy wins. Fails unless exactly one match; prints `path`. |
| `firebase-app-id` | Prints `app_id`: the manifest's `firebase_android_app_id`, else the release client in `google-services.json`. Fails on a malformed or missing ID; prints an empty `app_id` only when the manifest sets `"crashlytics_mapping": false`. |
| `kn-gc-guard --log F --gc-dir D --max-full-gc 20` | Fails if the build log lacks `Picked up JAVA_TOOL_OPTIONS` or any `D/gc-*.log` has more than 20 `Pause Full` lines. |
| `export-options --team T --profile NAME --bundle-id ID [--out FILE]` | Writes `ExportOptions.plist` (`app-store-connect`, manual signing). |

All pbxproj edits (IDs, versions, signing) touch only the application target's build configurations, found through its `PBXNativeTarget`; missing keys are added.

Tests: `python -m pytest scripts/amper` (yq tests skip without `yq` on PATH; fixtures are in `scripts/amper/fixtures/`).

## Infisical names

One Infisical project per app (slug passed as `project_slug`), read by the shared machine identity.

Environment `staging`, path `/Build`:

`KEYSTORE_BASE64`, `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`, `APPCHECK_DEBUG_TOKEN`,
`IOS_CERTIFICATE_BASE64`, `IOS_CERTIFICATE_PASSWORD`, `IOS_STAGING_PROFILE_APP_BASE64`, `TELEGRAM_CHAT_ID`.

Environment `prod` (that exact env slug), path `/Build`: the same names (the upload keystore), but
`IOS_PROD_PROFILE_APP_BASE64` replaces the staging profile, plus `FIREBASE_SERVICE_ACCOUNT_BASE64`: a base64 Google
service-account JSON key that may upload Crashlytics mapping files for the app's Firebase project (role Firebase
Crashlytics Admin or equivalent). `amper-release` also reads staging `/Build`, as a file it deletes, for
`APPCHECK_DEBUG_TOKEN`: `check-token` proves the release binaries do not contain it, and `check-cache` proves neither it
nor the prod `KEYSTORE_PASSWORD` is in a dependency cache before that cache is published.

`set-variant --variant debug` fails when `APPCHECK_DEBUG_TOKEN` is unset or empty; release always writes `""`.

Shared GitHub secrets (`CI_APP_CLIENT_ID`, `CI_APP_PRIVATE_KEY`, `INFISICAL_CLIENT_ID`,
`INFISICAL_CLIENT_SECRET`, `INFISICAL_DOMAIN`, store and Telegram secrets) are the ones already
listed in `docs/kmp-debug-ci.md`.

## Granting the machine identity access to a new Infisical project

1. In Infisical, open the new project, then Access Control, Machine Identities, Add Identity.
2. Choose the existing CI machine identity (the one behind `INFISICAL_CLIENT_ID`) and give it a
   read-only role on both environments: `staging` (debug, and `amper-release`'s `check-token` and `check-cache`) and
   `prod` (release).
3. Put the secrets above in the `/Build` folder.
4. Pass the project slug as `project_slug` in the trigger payload.

## GitHub App install

Install the CI GitHub App (the one behind `CI_APP_CLIENT_ID`) on the app repository with
Contents: read/write and Pull requests: write. Each job mints a token scoped to that single
repo. Also add the repo's `CENTRAL_DISPATCH_TOKEN` secret so its trigger can dispatch to this repo.

If `main` is protected (branch protection or a ruleset requiring PRs or status checks), add the CI GitHub App to its
bypass list: `amper-release`'s `push-version` pushes the `chore(release)` version commit and tag straight to `main` (one
atomic push leased on the pinned SHA), and a protected branch rejects it otherwise.

## Trigger templates

Copy these into the app repo once the central workflows are live:

- `triggers/amper-debug.yml`: `pull_request_target` and manual runs dispatch `amper-debug`.
- `triggers/amper-release.yml` (as `release.yml`): pushes to `main` dispatch `amper-release` (skip `chore(release): v*`).
- `triggers/amper-publish.yml` (as `publish-kmp.yml`): a published `vX.Y.Z` release dispatches `publish-kmp` to the
  registry-aware Play Store and App Store publishers.

The repo must also be in `config/apps.json` with its `project_slug`: `amper-release` refuses a payload whose
`project_slug` differs from the registry, because it reads that project's prod signing keys. Kotlin Toolchain apps
also set `"native_symbols": false` there: they build no `native-debug-symbols.zip`, and only that registry flag lets
`publish-playstore` publish without one (every other app, including travel-animator on `publish-kmp`, still needs
exactly one).

## `amper-debug` workflow

Payload `{repo, pr, branch, title, project_slug}`, the same as `kmp-debug`. Runs are grouped per repo plus PR
(or branch), with cancel-in-progress. Outputs: a `debug-pr-N` prerelease with the debug APK, a staging TestFlight
build, the PR comments `<!-- central-build-debug -->` and `<!-- central-ios-testflight -->`, and Telegram messages.

| Job | Does |
| --- | --- |
| `prepare` | Validates the payload, checks `config/apps.json` (`apps.py --project-slug`: the payload's `project_slug` must equal the registry's, because the jobs read that Infisical project) before minting a token, pins the source SHA, runs `metadata` (build number = run number) and hashes the dependency cache key once. |
| `android` (ubuntu, 45 min) | Staging `/Build`, `set-variant debug`, `set-version`, `android-inputs`, `./kotlin test -p android`, then `./kotlin build -m <android_module> -v release` (the module from `.ci/amper.json`) and `sign-apk`: an R8 APK zipaligned and signed with the debug keystore, so the Google sign-in SHA-1 and WhatsApp hash match. Publishes it straight to the source repo's prerelease, then deletes it and the keystore. `KOTLIN_CLI_JAVA_OPTIONS=-Xmx1536m`. No lint (the toolchain has no Android Lint). |
| `ios-build` (macos-26, Xcode 26.6, 90 min) | Reads the team, name and UUID from the staging profile, `set-variant debug --team --profile` (manual signing on the app target), `set-version`, `apple-signing`. A resolve-only warm-up (`./kotlin task` on every `:module:resolveDependenciesIosArm64` and `:module:cinteropIosArm64`, 25 min, ADR 0004 flags). Then it restores the committed `Package.resolved` and shared scheme (every `./kotlin` run deletes both) and runs `xcodebuild` resolve and archive, both with `-onlyUsePackageVersionsFromResolvedFile`, the archive with ADR 0004's `JAVA_TOOL_OPTIONS`, `kn-gc-guard`, `export-options`, `-exportArchive` and Crashlytics `upload-symbols`. It never holds the App Store Connect key: `release.py park` replaces this PR's parking draft `ci-testflight-<debug tag>` in the source repo with one holding only the IPA, and outputs the draft's numeric ID and the IPA's SHA-256. It parks with the job-start App token (see the token rule below); a guard step fails first, with `::error::App token likely expired`, when more than 55 minutes have passed since the mint (re-run with warm caches). Then it deletes the IPA, archive and build products. |
| `ios-publish` (macos-26, Xcode 26.6 via `setup-xcode` pinned to a commit SHA, 20 min) | The only job with the `APPSTORE_*` secrets, so it runs no source code: it checks out only `control/`, and `needs: [prepare, android, ios-build]` withholds the upload until the unit tests pass (no polling). `release.py unpark` downloads the IPA by ID after re-checking the ID is a draft under `ci-testflight-<prepare's tag>` holding exactly one IPA with ios-build's SHA-256. `kmp/control.py upload-ipa` uploads it, `testflight_notes.py identity` reads the identity, and `release.py discard` deletes the draft (re-checked the same way) after a successful upload; if that fails, a `::warning::` asks for manual deletion. A skipped or failed upload keeps the draft so "Re-run failed jobs" can retry; the PR's next run replaces it. |
| `testflight-notes`, `notify` | As in `kmp-debug`, driven by job outputs (`android.url`, `ios-publish.uploaded` and the IPA identity from `ios-publish`). |

Caches (`gh-bundle-cache`, keyed on the hash of `kotlin`, `project.yaml`, `**/module.yaml`, `libs.versions.toml`
and `**/Package.resolved`): the Kotlin shared cache (`~/.cache/JetBrains/Kotlin` on Linux,
`~/Library/Caches/JetBrains/Kotlin` on macOS; toolchain 0.14 keeps its Maven downloads in `.m2.cache` inside it, not
in `~/.m2`), `~/.konan/dependencies`, and `$RUNNER_TEMP/dd/SourcePackages` (resolve and archive share
`-derivedDataPath "$RUNNER_TEMP/dd"`). These hold third-party downloads only, never project build output.

**No secrets after PR code.** In `android` and `ios-build`, nothing after the first `./kotlin` or `xcodebuild` step
uses `secrets.*` (only `github.token` and earlier step outputs): minting an App token there would hand
`CI_APP_PRIVATE_KEY` to an action on a runner PR code controlled. Both jobs use the token minted at job start. `android`'s
45-minute timeout keeps it inside the token's one-hour life, and `ios-build` fails closed past 55 minutes.
`FreshTokens` in `scripts/amper/test_release.py` enforces this. `amper-release` builds main's code only, so it mints a
fresh token right before each post-build draft upload instead, and the same test enforces that.

`amper-debug` is restore-only. Its jobs run PR code (the source's `kotlin` wrapper and Xcode's Build Kotlin phase), and
`ci-bundle-*` packages are public, so a job that could publish would let a PR poison every later build, releases
included. `android` and `ios-build` get `packages: read` and restore `amper-release`'s release-only bundles (built from
main: trusted to untrusted only); no `amper-debug` job has `packages: write` and none publishes a bundle
(`DebugWorkflowShape` in `scripts/amper/test_release.py` enforces both). `bundle_cache.py` also runs `oras logout` before
it returns, so no GHCR credential stays in `~/.docker/config.json` for later steps. Only `amper-release`, which runs on
main, publishes bundles.

## Rule: no secret-bearing binaries or raw logs as Actions artifacts

Lascade-Co/actions is public, so anyone can download its run artifacts. The debug APK and IPA embed the App Check
debug token, which bypasses App Check for the `.debug` apps, including the paid WhatsApp OTP callables. Therefore:

- Binaries never pass between jobs as Actions artifacts. The APK goes from its build job straight to the private
  source repo's prerelease. The IPA goes from `ios-build` to a draft release in the private source repo (a draft
  creates no tag and fires no release event), addressed by numeric ID and checked by SHA-256, and `ios-publish`
  moves it to TestFlight and deletes the draft. That keeps the org-wide App Store Connect key out of every job that
  runs PR code. Binaries are deleted from the runners afterwards.
- `ios-diagnostics` never holds the raw `xcodebuild.log`. It holds only the GC logs, the `kn-gc-guard` summary,
  memory samples, and a filtered log of lines matching `error:|warning:|Picked up JAVA_TOOL_OPTIONS|(BUILD|ARCHIVE)
  (SUCCEEDED|FAILED)`, with any line containing `KEY`, `TOKEN`, `PASSWORD` or `SECRET` (any case) dropped.
- The same applies to `amper-release` and any future workflow here that handles signed or secret-bearing builds.
  Release binaries hold no debug token, but `mapping.txt` and the dSYMs deobfuscate the app, so `amper-release` keeps
  every binary in the private source repo's draft release instead of Actions artifacts.

## `amper-release` workflow

Payload `{repo, branch: "main", sha, project_slug}` from `triggers/amper-release.yml`. Concurrency
`amper-release-<repo>`, never cancelled. Produces a published release `vX.Y.Z` in the source repo with
`<name>-<version>-<build>.aab`, `.apk`, `.ipa`, `-mapping.txt` and `-dSYMs.zip` (plus `native-debug-symbols.zip`
when an app builds one; Kotlin Toolchain apps do not), which fires the source's `publish-kmp.yml`.

| Job | Does |
| --- | --- |
| `prepare` | `release.py validate` before any token (registered repo, registry `project_slug`, `main`, full SHA). `resolve` pins main, rejects stale dispatches and skips the bot's own bump head. `bump`: patch +1 and build +1, or minor/major when a PR merged into main for that commit carries the `minor` or `major` label. Release notes as in `kmp-release` (reuse an updated `releasenotes.txt`, else Claude over the diff since the last tag), `check-notes`, then `draft` creates (or reuses) the draft release `vX.Y.Z` targeting the SHA and outputs its numeric `release_id`. The tag does not exist yet, so every later job addresses the draft by ID (REST API; any lookup error other than 404 fails). |
| `android` (ubuntu, 60 min) | Prod `/Build`, `set-variant release`, `set-version`, `android-inputs` (upload keystore). With a Firebase app ID: drops the source's `com.crashlytics.RequireBuildId` override file from `<module>/res/values/` and runs `firebase crashlytics:mappingfile:generateid` into `<module>/res/values/crashlytics_mapping_file_id.xml` (firebase-tools pinned). `./kotlin package -m <module> -f aab` and `./kotlin build -m <module> -v release`; rejects an unsigned APK and an AAB that `jarsigner` does not verify; `check-mapping-id` proves the ID is packaged in both; uploads `mapping.txt` with `crashlytics:mappingfile:upload` using `FIREBASE_SERVICE_ACCOUNT_BASE64` (written 0600, deleted in `always()`); `sign-apk` zipaligns and signs the APK with the upload keystore (`kotlin build` leaves it unsigned) and verifies it; uploads AAB, APK and mapping to the draft by ID (`uploads.github.com/.../releases/{id}/assets`, replacing same-named assets) and outputs their `name:sha256` pairs. The Firebase CLI (`firebase-tools@15.34.2`, `--ignore-scripts`) is installed before any prod secret is in the environment. |
| `ios-build` (macos-26, Xcode 26.6, 90 min) | As `amper-debug`, with prod `/Build`, the `IOS_PROD_PROFILE_APP_BASE64` identity and `set-variant release --team --profile` (release IDs and plist, App Attest `production`, blank debug token). Exports the IPA, uploads dSYMs to Crashlytics, zips them, and uploads IPA and zip to the draft by ID, with their `name:sha256` pairs as job outputs. No TestFlight upload: `publish-appstore` takes the IPA from the release. |
| `release` | Runs only `set-version` on a fresh checkout, downloads the draft's assets (asset API, `Accept: application/octet-stream`) and body by ID, requiring exactly the uploaded names with matching SHA-256, `verify-assets` (exactly one nonempty file per required suffix, at most one native-symbols zip, nothing else), then `check-token` with staging `APPCHECK_DEBUG_TOKEN` read from an Infisical file that the step deletes: every zip entry of the AAB, APK and IPA is searched for the token in UTF-8 and UTF-16 (Kotlin/Native string literals), and only counts are printed. `push-version` commits `chore(release): vX.Y.Z (N) [skip ci]` and the annotated tag in one atomic push leased on the pinned SHA; the diff may touch only `version_files` and the pbxproj, and only their four version lines. `publish` checks the tag ref exists, then PATCHes the draft by ID to `draft: false`. |
| `notify` | Telegram `release` message (`TELEGRAM_BOT_TOKEN`, chat from prod `/Build`). |

Caches: the release jobs restore and publish release-only bundle names (`<name>-<os>-<arch>-kotlin-release`,
`-konan-release`, `-swiftpm-release`, each with its own `latest`). Only this workflow publishes bundles, and it only
runs for main; `amper-debug` restores these same bundles read-only and publishes none. A miss cold-starts.

Before any publish, each build job reads staging `/Build` as a file (after the build, so the source's build never sees
it) and runs `release.py check-cache` over every cache path it may publish: `grep -rqF` with the staging
`APPCHECK_DEBUG_TOKEN` and the prod `KEYSTORE_PASSWORD` as needles (from a 0600 file, never argv; neither is printed).
A hit, a missing or too-short secret (token under 16, password under 6 characters) or a grep error fails the step with
an `::error::`. The step is `continue-on-error` (the release does not depend on the cache), and every publish step
requires `steps.cache-scan.outcome == 'success'`, so a failed scan publishes nothing.

**Resuming a failed release: use "Re-run failed jobs" only.** It keeps `prepare`'s outputs (version, `release_id`), so
a re-run `release` job resumes: `push-version` recognises its own already-pushed commit and tag, and `publish` finishes
the same draft. "Re-run all jobs" re-runs `prepare`, which then sees the bot's bump commit on main and skips, leaving
the draft unpublished. If the build jobs failed, re-running them re-uploads to the same draft.

`scripts/amper/release.py` subcommands: `validate`, `resolve`, `bump`, `notes`, `check-notes`, `draft`, `upload`, `download`,
`verify-assets`, `check-token`, `check-cache`, `check-mapping-id`, `push-version`, `publish`, `chat-id`, and for
`amper-debug`'s TestFlight hand-off `park`, `unpark` and `discard` (parking tags `ci-testflight-debug-(pr|run)-N` only;
the `vX.Y.Z` paths still accept only release tags). It loads `scripts/kmp/apps.py`, `scripts/kmp/control.py`,
`scripts/kmp/release.py` and `scripts/amper/control.py` by path, so run it from the trusted `control/` checkout. Tests: `cd scripts/amper && python -m pytest` (run `scripts/amper` and `scripts/kmp` separately: both have
`test_control.py`).
