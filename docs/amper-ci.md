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
| `version_files` | `module.yaml` files that receive `versionName` / `versionCode`. |

The repo root also needs the Kotlin wrapper `kotlin` containing `kotlin_cli_version=X.Y.Z`;
that line selects the toolchain version.

## `control.py` subcommands

Each prints `key=value` lines, ready to append to `$GITHUB_OUTPUT`. All take `--source DIR`
unless noted. Failures exit non-zero with an `error:` message.

| Command | Does |
| --- | --- |
| `metadata [--suffix N]` | Prints `version` (`versionName`), `build_number` (`--suffix` if given, else `versionCode`) and `toolchain`. |
| `set-version --version X --build N` | Patches `module.yaml` (via `yq`) and every `MARKETING_VERSION` / `CURRENT_PROJECT_VERSION` in the pbxproj. |
| `set-variant --variant debug\|release [--team T --profile NAME]` | Writes the IDs; for release also swaps the plist, sets the App Attest entitlement and blanks the token; for debug writes `$APPCHECK_DEBUG_TOKEN` into `BuildSecrets.kt`. With `--team` and `--profile` (both variants; debug is signed for TestFlight) it puts manual signing (`CODE_SIGN_STYLE`, `DEVELOPMENT_TEAM`, `PROVISIONING_PROFILE_SPECIFIER`, `CODE_SIGN_IDENTITY = "Apple Distribution"`) on the app target's build settings. Command-line signing overrides did not work in the spike. |
| `android-inputs` | Decodes `KEYSTORE_BASE64` to `androidApp/ci.jks` and writes `keystore.properties` from `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`. |
| `find-artifact --kind apk\|aab\|mapping` | Globs `build/tasks/*/*.apk` or `*.aab` (the top of each task dir; the toolchain keeps a second copy inside its generated `gradle-project`) or `build/tasks/**/outputs/mapping/release/mapping.txt`. For `apk`, a task dir whose name contains `release` (any case) wins over others. Fails unless exactly one match; prints `path`. |
| `kn-gc-guard --log F --gc-dir D --max-full-gc 20` | Fails if the build log lacks `Picked up JAVA_TOOL_OPTIONS` or any `D/gc-*.log` has more than 20 `Pause Full` lines. |
| `export-options --team T --profile NAME --bundle-id ID [--out FILE]` | Writes `ExportOptions.plist` (`app-store-connect`, manual signing). |

All pbxproj edits (IDs, versions, signing) touch only the application target's build configurations, found through its `PBXNativeTarget`; missing keys are added.

Tests: `python -m pytest scripts/amper` (yq tests skip without `yq` on PATH; fixtures are in `scripts/amper/fixtures/`).

## Infisical names

One Infisical project per app (slug passed as `project_slug`), read by the shared machine identity.

Environment `staging`, path `/Build`:

`KEYSTORE_BASE64`, `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`, `APPCHECK_DEBUG_TOKEN`,
`IOS_CERTIFICATE_BASE64`, `IOS_CERTIFICATE_PASSWORD`, `IOS_STAGING_PROFILE_APP_BASE64`, `TELEGRAM_CHAT_ID`.

Environment `prod`, path `/Build`: the same names, but `IOS_PROD_PROFILE_APP_BASE64` replaces the staging profile.

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
- `triggers/amper-release.yml`: pushes to `main` dispatch `amper-release` (skip `chore(release): v*`).
- `triggers/amper-publish.yml`: store-publish trigger.

`amper-debug.yml` exists; the release and publish templates are added with `amper-release` (Task 5 of the central CI plan).

## `amper-debug` workflow

Payload `{repo, pr, branch, title, project_slug}`, the same as `kmp-debug`. Runs are grouped per repo plus PR
(or branch), with cancel-in-progress. Outputs: a `debug-pr-N` prerelease with the debug APK, a staging TestFlight
build, the PR comments `<!-- central-build-debug -->` and `<!-- central-ios-testflight -->`, and Telegram messages.

| Job | Does |
| --- | --- |
| `prepare` | Validates the payload, checks `config/apps.json` before minting a token, pins the source SHA, runs `metadata` (build number = run number) and hashes the dependency cache key once. |
| `android` (ubuntu, 45 min) | Staging `/Build`, `set-variant debug`, `set-version`, `android-inputs`, `./kotlin test -p android`, then `./kotlin build -m androidApp -v release`: an R8 APK signed with the debug keystore. Publishes it straight to the source repo's prerelease, then deletes it and the keystore. `KOTLIN_CLI_JAVA_OPTIONS=-Xmx1536m`. No lint (the toolchain has no Android Lint). |
| `ios-build` (macos-26, Xcode 26.6, 90 min) | Reads the team, name and UUID from the staging profile, `set-variant debug --team --profile` (manual signing on the app target), `set-version`, `apple-signing`. A resolve-only warm-up (`./kotlin task` on every `:module:resolveDependenciesIosArm64` and `:module:cinteropIosArm64`, 25 min, ADR 0004 flags). Then `xcodebuild` resolve and archive with ADR 0004's `JAVA_TOOL_OPTIONS`, `kn-gc-guard`, `export-options`, `-exportArchive` and Crashlytics `upload-symbols`. It waits for the `android` job (unit tests) to succeed, uploads to TestFlight, and deletes the IPA, archive and build products. |
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
