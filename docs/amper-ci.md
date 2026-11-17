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
| `find-artifact --kind apk\|aab\|mapping` | Globs `build/tasks/*/*.apk` or `*.aab` (the top of each task dir; the toolchain keeps a second copy inside its generated `gradle-project`) or `build/tasks/**/outputs/mapping/release/mapping.txt`; fails unless exactly one match; prints `path`. |
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

These templates are added with their workflows (Tasks 3 and 5 of the central CI plan).
