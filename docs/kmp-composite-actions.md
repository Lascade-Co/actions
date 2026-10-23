# KMP CI composite actions

These actions run in the central `Lascade-Co/actions` workflows. Check out the
central repository at `${{ github.sha }}` into a separate directory (for example
`control/`), then use `./control/actions/<name>`. The app source remains the primary
working directory. Supporting scripts are downloaded from the central repository
at that same immutable runner SHA, with bounded transport retries and promotion
only after a successful nonempty download.

## `gh-bundle-cache`

Required inputs: `mode` (`restore` or `publish`), `name` (namespace normalized to lowercase),
`key` (exact content tag), and `paths` (newline-separated paths). `fallback-key`
defaults to `latest`; set an empty string to disable it. `token` defaults to the
workflow token and needs `packages: write` for publishing. Namespace each bundle
by source repository, OS and architecture so dependencies cannot cross platforms.
The registry destination is `ghcr.io/<central-owner>/ci-bundle-<name>`.

`~` and environment variables in paths expand on the runner. The same ordered
path list must be supplied at restore and publish. Select only dependency data:
SwiftPM caches, `~/.konan`, Gradle dependency caches and wrapper distributions,
and the ANGLE download directory. Do not include source code, signing material,
`local.properties`, or the whole Gradle user home. Small Gradle configuration and
build-cache state stays with `gradle/actions/setup-gradle`.

Restore tries the exact tag then the fallback, validating every archive path
before extraction. Parent-relative toolchain symlinks are preserved when their
complete link chain stays inside the selected cache root; absolute, cyclic and
cross-root links are rejected. Restored paths replace the selected destination directories;
they do not merge with stale files. Missing optional paths are omitted on publish.
An archive with none of the requested paths is rejected. `hit=true` means an exact
restore; a fallback reports `hit=false` so the caller can publish the new exact
key. `restored-key` identifies the selected tag and `bytes` reports uncompressed
archive size. A cold miss reports zero bytes. Cache transport/publish failures are
visible warnings and permit a cold dependency resolve. Publish after dependency
resolution, before later archive or packaging work can fail.

ORAS uses the official `oras-project/setup-oras@v2` action and CLI 1.3.5. GHCR retains
package visibility separately from the public central repository; keep dependency
bundles private. Publishing sets the OCI source annotation to the central repo.

## `apple-signing`

`mode=install` imports the `.p12` from `certificate-env` (default
`IOS_CERTIFICATE_BASE64`), using the password from `certificate-password-env`
(default `IOS_CERTIFICATE_PASSWORD`). Those inputs are environment variable names,
not secret values. Certificate import uses `apple-actions/import-codesign-certs@v7`
and its automatic job-post temporary-keychain cleanup.

`profiles` lists required environment variable names separated by whitespace.
Each variable contains a base64 App Store distribution profile. Installation
rejects expired, wildcard, development, ad hoc and enterprise profiles. Profiles
are installed by UUID into
`~/Library/Developer/Xcode/UserData/Provisioning Profiles`. Existing profiles are
backed up under a private runner-temporary directory and restored during cleanup;
a partial installation failure rolls back immediately. No profile/certificate
contents are printed.

Always invoke the same action with `mode=cleanup` in a final `if: always()` step.
This removes installed profiles and restores any pre-existing ones. The cleanup
handler is retained locally during installation, so it does not need another
network download after a successful install. Keychain cleanup runs independently
through the upstream action's job-post hook, including failed/cancelled builds.
Do not invoke installation twice in one job.

## `pr-status-comment`

Inputs: `token` (source-repository GitHub App token), `app-slug` (the token action's
`app-slug` output), `repository`, `pr`, `marker`, `body`, and `mode` (default
`upsert`). `marker` must be an HTML marker such as
`<!-- central-ios-testflight -->`. The action supplies that first line; callers
supply only the visible Markdown in `body`.

`upsert` updates one matching comment and removes its duplicates. `replace` removes
prior matching comments and creates a fresh comment; use it at run start. `purge`
only removes matching comments. Ownership requires both the exact marker first
line and `performed_via_github_app.slug` matching the supplied App. Human comments
and other bots are preserved. Empty, `null`, or zero PR numbers skip commenting
for manual runs. The output `comment-id` identifies a created or updated comment.
GitHub API failures fail the step visibly.

## `telegram-notify`

Required inputs are `token`, `chat-id` and plain-text `message`. No markup parsing
is enabled, so branch names and PR titles remain literal text. The Telegram API
response is checked; transport failures and Telegram rejections fail the step.
Error messages omit the token-bearing API URL. The Python standard-library
implementation works on both Linux and macOS. The researched
`appleboy/telegram-action@v1` is Docker-only, which cannot serve the macOS runner.

## Offline validation

```sh
python3 -m unittest discover -s scripts/kmp -p test_composites.py -v
node --test scripts/kmp/pr_status_comment.test.cjs
```

These checks cover archive round trips, exact/fallback behavior, traversal and
symlink escape rejection, profile validation and rollback, comment ownership and
modes, and Telegram payload/error handling. Native keychain import, GHCR access
and actual Telegram delivery require the authorized hosted workflow run.

Upstream references checked on 2026-10-23:
[ORAS installation](https://oras.land/docs/installation/),
[ORAS setup action](https://github.com/oras-project/setup-oras),
[certificate import action](https://github.com/Apple-Actions/import-codesign-certs),
[GitHub Script](https://github.com/actions/github-script), and
[Telegram action](https://github.com/appleboy/telegram-action).
