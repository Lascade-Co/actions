# Amper (Kotlin Toolchain) central CI

`amper-debug` and `amper-release` build Kotlin Toolchain apps (first app: TreasureHunch). The app
repo owns its build knowledge in a committed manifest; this repo owns the workflows and
`scripts/amper/control.py`, which does the file-level work. Workflows fetch the script by
SHA-pinned raw URL (`https://raw.githubusercontent.com/Lascade-Co/actions/<sha>/scripts/amper/control.py`).

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
| `firebase_android_app_id` | Optional. Release Firebase Android app ID (`1:<number>:android:<hex>`) for the Crashlytics mapping upload. Without it, `amper-release` takes the `<android_module>/google-services.json` client whose package is `ids.release`; with neither, the mapping upload is skipped with a warning. |

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
| `android-inputs` | Decodes `KEYSTORE_BASE64` to `androidApp/ci.jks` and writes `keystore.properties` from `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`. |
| `find-artifact --kind apk\|aab\|mapping` | Globs `build/tasks/*/*.apk` or `*.aab` (the top of each task dir; the toolchain keeps a second copy inside its generated `gradle-project`) or `build/tasks/**/outputs/mapping/release/mapping.txt`. For `apk`, a task dir whose name contains `release` (any case) wins over others. For `mapping`, when both the AAB (`kotlin package`) and the APK (`kotlin build -v release`) ran R8, the two files must be identical and the bundle task's copy wins. Fails unless exactly one match; prints `path`. |
| `firebase-app-id` | Prints `app_id`: the manifest's `firebase_android_app_id`, else the release client in `google-services.json`, else empty. Fails on a malformed ID. |
| `kn-gc-guard --log F --gc-dir D --max-full-gc 20` | Fails if the build log lacks `Picked up JAVA_TOOL_OPTIONS` or any `D/gc-*.log` has more than 20 `Pause Full` lines. |
| `export-options --team T --profile NAME --bundle-id ID [--out FILE]` | Writes `ExportOptions.plist` (`app-store-connect`, manual signing). |

All pbxproj edits (IDs, versions, signing) touch only the application target's build configurations, found through its `PBXNativeTarget`; missing keys are added.

Tests: `python -m pytest scripts/amper` (yq tests skip without `yq` on PATH; fixtures are in `scripts/amper/fixtures/`).

## Infisical names

One Infisical project per app (slug passed as `project_slug`), read by the shared machine identity.

Environment `staging`, path `/Build`:

`KEYSTORE_BASE64`, `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`, `APPCHECK_DEBUG_TOKEN`,
`IOS_CERTIFICATE_BASE64`, `IOS_CERTIFICATE_PASSWORD`, `IOS_STAGING_PROFILE_APP_BASE64`, `TELEGRAM_CHAT_ID`.

Environment `prod`, path `/Build`: the same names (the upload keystore), but `IOS_PROD_PROFILE_APP_BASE64` replaces the
staging profile, plus `FIREBASE_SERVICE_ACCOUNT_BASE64`: a base64 Google service-account JSON key that may upload
Crashlytics mapping files for the app's Firebase project (role Firebase Crashlytics Admin or equivalent). `amper-release`
also reads staging `/Build` once, for `APPCHECK_DEBUG_TOKEN`, to prove the release binaries do not contain it.

`set-variant --variant debug` fails when `APPCHECK_DEBUG_TOKEN` is unset or empty; release always writes `""`.

Shared GitHub secrets (`CI_APP_CLIENT_ID`, `CI_APP_PRIVATE_KEY`, `INFISICAL_CLIENT_ID`,
`INFISICAL_CLIENT_SECRET`, `INFISICAL_DOMAIN`, store and Telegram secrets) are the ones already
listed in `docs/kmp-debug-ci.md`.

## Granting the machine identity access to a new Infisical project

1. In Infisical, open the new project, then Access Control, Machine Identities, Add Identity.
2. Choose the existing CI machine identity (the one behind `INFISICAL_CLIENT_ID`) and give it a
   read-only role on the environments the workflows read (`staging` for debug, production for release).
3. Put the secrets above in the `/Build` folder.
4. Pass the project slug as `project_slug` in the trigger payload.

## GitHub App install

Install the CI GitHub App (the one behind `CI_APP_CLIENT_ID`) on the app repository with
Contents: read/write and Pull requests: write. Each job mints a token scoped to that single
repo. Also add the repo's `CENTRAL_DISPATCH_TOKEN` secret so its trigger can dispatch to this repo.

## Trigger templates

Copy these into the app repo once the central workflows are live:

- `triggers/amper-debug.yml`: `pull_request_target` and manual runs dispatch `amper-debug`.
- `triggers/amper-release.yml` (as `release.yml`): pushes to `main` dispatch `amper-release` (skip `chore(release): v*`).
- `triggers/amper-publish.yml` (as `publish-kmp.yml`): a published `vX.Y.Z` release dispatches `publish-kmp` to the
  registry-aware Play Store and App Store publishers.

The repo must also be in `config/apps.json` with its `project_slug`: `amper-release` refuses a payload whose
`project_slug` differs from the registry, because it reads that project's prod signing keys.

## `amper-debug` workflow

Payload `{repo, pr, branch, title, project_slug}`, the same as `kmp-debug`. Runs are grouped per repo plus PR
(or branch), with cancel-in-progress. Outputs: a `debug-pr-N` prerelease with the debug APK, a staging TestFlight
build, the PR comments `<!-- central-build-debug -->` and `<!-- central-ios-testflight -->`, and Telegram messages.

| Job | Does |
| --- | --- |
| `prepare` | Validates the payload, checks `config/apps.json` before minting a token, pins the source SHA, runs `metadata` (build number = run number) and hashes the dependency cache key once. |
| `android` (ubuntu, 45 min) | Staging `/Build`, `set-variant debug`, `set-version`, `android-inputs`, `./kotlin test -p android`, then `./kotlin build -m androidApp -v release`: an R8 APK signed with the debug keystore. Publishes it straight to the source repo's prerelease, then deletes it and the keystore. `KOTLIN_CLI_JAVA_OPTIONS=-Xmx1536m`. No lint (the toolchain has no Android Lint). |
| `ios-build` (macos-26, Xcode 26.6, 90 min) | Reads the team, name and UUID from the staging profile, `set-variant debug --team --profile` (manual signing on the app target), `set-version`, `apple-signing`. A resolve-only warm-up (`./kotlin task` on every `:module:resolveDependenciesIosArm64` and `:module:cinteropIosArm64`, 25 min, ADR 0004 flags). Then it restores the committed `Package.resolved` and shared scheme (every `./kotlin` run deletes both) and runs `xcodebuild` resolve and archive, both with `-onlyUsePackageVersionsFromResolvedFile`, the archive with ADR 0004's `JAVA_TOOL_OPTIONS`, `kn-gc-guard`, `export-options`, `-exportArchive` and Crashlytics `upload-symbols`. It waits for the `android` job (unit tests) to succeed, uploads to TestFlight, and deletes the IPA, archive and build products. |
| `testflight-notes`, `notify` | As in `kmp-debug`, driven by job outputs (`android.url`, `ios-build.uploaded` and the IPA identity). |

Caches (`gh-bundle-cache`, keyed on the hash of `kotlin`, `project.yaml`, `**/module.yaml`, `libs.versions.toml`
and `**/Package.resolved`): the Kotlin shared cache (`~/.cache/JetBrains/Kotlin` on Linux,
`~/Library/Caches/JetBrains/Kotlin` on macOS; toolchain 0.14 keeps its Maven downloads in `.m2.cache` inside it, not
in `~/.m2`), `~/.konan/dependencies`, and `$RUNNER_TEMP/dd/SourcePackages` (resolve and archive share
`-derivedDataPath "$RUNNER_TEMP/dd"`). These hold third-party downloads only, never project build output.
Only `android` and `ios-build` get `packages: write`.

## Rule: no secret-bearing binaries or raw logs as Actions artifacts

Lascade-Co/actions is public, so anyone can download its run artifacts. The debug APK and IPA embed the App Check
debug token, which bypasses App Check for the `.debug` apps, including the paid WhatsApp OTP callables. Therefore:

- The job that builds a binary also delivers it: the APK goes to the private source repo's prerelease and the IPA to
  TestFlight, in the same job. Binaries never pass between jobs as artifacts, and they are deleted from the runner afterwards.
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
| `prepare` | `release.py validate` before any token (registered repo, registry `project_slug`, `main`, full SHA). `resolve` pins main, rejects stale dispatches and skips the bot's own bump head. `bump`: patch +1 and build +1, or minor/major when a PR merged into main for that commit carries the `minor` or `major` label. Release notes as in `kmp-release` (reuse an updated `releasenotes.txt`, else Claude over the diff since the last tag), `check-notes`, then `draft` creates (or reuses) the draft release `vX.Y.Z` targeting the SHA. The tag does not exist yet. |
| `android` (ubuntu, 60 min) | Prod `/Build`, `set-variant release`, `set-version`, `android-inputs` (upload keystore). With a Firebase app ID: drops the source's `com.crashlytics.RequireBuildId` override file from `<module>/res/values/` and runs `firebase crashlytics:mappingfile:generateid` into `<module>/res/values/crashlytics_mapping_file_id.xml` (firebase-tools pinned). `./kotlin package -m <module> -f aab` and `./kotlin build -m <module> -v release`; rejects an unsigned APK and an AAB that `jarsigner` does not verify; `check-mapping-id` proves the ID is packaged in both; uploads `mapping.txt` with `crashlytics:mappingfile:upload` using `FIREBASE_SERVICE_ACCOUNT_BASE64` (written 0600, deleted in `always()`); uploads AAB, APK and mapping to the draft. |
| `ios-build` (macos-26, Xcode 26.6, 90 min) | As `amper-debug`, with prod `/Build`, the `IOS_PROD_PROFILE_APP_BASE64` identity and `set-variant release --team --profile` (release IDs and plist, App Attest `production`, blank debug token). Exports the IPA, uploads dSYMs to Crashlytics, zips them, and uploads IPA and zip to the draft. No TestFlight upload: `publish-appstore` takes the IPA from the release. |
| `release` | Runs only `set-version` on a fresh checkout, downloads the draft's assets and body, `verify-assets` (exactly one nonempty file per required suffix, at most one native-symbols zip, nothing else), then `check-token` with staging `APPCHECK_DEBUG_TOKEN` read from an Infisical file that the step deletes: every zip entry of the AAB, APK and IPA is searched for the token in UTF-8 and UTF-16 (Kotlin/Native string literals), and only counts are printed. `push-version` commits `chore(release): vX.Y.Z (N) [skip ci]` and the annotated tag in one atomic push leased on the pinned SHA; the diff may touch only `version_files` and the pbxproj, and only their four version lines. `publish` then publishes the draft (`--verify-tag`). |
| `notify` | Telegram `release` message (`TELEGRAM_BOT_TOKEN`, chat from prod `/Build`). |

`scripts/amper/release.py` subcommands: `validate`, `resolve`, `bump`, `notes`, `check-notes`, `draft`, `upload`,
`verify-assets`, `check-token`, `check-mapping-id`, `push-version`, `publish`, `chat-id`. It loads `scripts/kmp/apps.py`,
`scripts/kmp/control.py`, `scripts/kmp/release.py` and `scripts/amper/control.py` by path, so run it from the central
checkout. Tests: `cd scripts/amper && python -m pytest` (run `scripts/amper` and `scripts/kmp` separately: both have
`test_control.py`).
