# KMP production release CI

The source `release.yml` dispatches `kmp-release` from pushes to `main` and manual runs.
The trusted central `kmp-build-release.yml` accepts only
`Lascade-Co/travel-animator-shared`, branch `main`, its immutable current SHA and project
`travelanimator`. It validates before minting source-scoped GitHub App credentials.
The entire release is serial (`kmp-release`, cancellation disabled). Stale requests fail;
the exact release bump subject from the release bot skips the next run.

The prepare job invokes the source `scripts/ci/release_version.py --apply` exactly once to
compute the new patch and integer build number. Both builds and finalization apply the
same explicit pair using that script. No central YAML or script duplicates version arithmetic.
The prepared notes are a single English text: updated `releasenotes.txt` since the latest
merged version tag, otherwise `data/RELEASE_NOTES.md` plus the Git diff through
`scripts/android/release_notes.py`. Empty or over-500 UTF-16-unit notes fail before publication;
notes are never truncated.

`android` runs the source `scripts/ci/android_release.py` on Ubuntu with JDK 25, production
Infisical `/Local-Properties` and `/Build`, release keystore/Firebase inputs and Crashlytics
mapping upload enabled. It retains AAB, universal APK, native debug symbols and R8 mapping.
The `gradle` OCI dependency bundle is restored with latest fallback and published on a miss.

`ios-build` runs the source `scripts/ci/ios_release.py` on paid `macos-26-xlarge`, Xcode 26.6,
Release configuration, production runtime inputs and a fully optimized Native link with
8-GiB Native heap and release binary caches disabled. It restores/publishes Gradle, ANGLE,
SwiftPM and Konan OCI bundles. Production Konan and SwiftPM bundles have separate names
from Staging. The archive and export retain an App Store IPA plus a dSYMs zip; production
Firebase Crashlytics receives the unzipped dSYMs. This build does not upload the IPA to Apple;
the `publish-kmp` consumer does that once it owns the publishing transaction.

Infisical `travelanimator`, `prod`, `/Build` must supply:

- Android `KEYSTORE_BASE64`, `KEYSTORE_PASSWORD`, `KEY_ALIAS`, `KEY_PASSWORD`, `SERVICE_JSON_BASE64`.
- Apple `IOS_CERTIFICATE_BASE64`, `IOS_CERTIFICATE_PASSWORD`, `SERVICE_PLIST_BASE64`.
- Apple `IOS_PROD_PROFILE_APP_BASE64`, `IOS_PROD_PROFILE_SHARE_BASE64`,
  `IOS_PROD_PROFILE_NSE_BASE64`, `IOS_PROD_PROFILE_WIDGET_BASE64`.

The source prerelease checklist defines the exact production bundle IDs and installed
profile names. The `apple-signing` composite installs the certificate and four profiles and
always removes temporary profiles. Secrets are decoded without shell evaluation and stored
with mode 0600. No generated configuration or signing input is a release asset.

Only after both builds succeed does `release` validate all six nonempty assets and notes,
commit `chore(release): vX.Y.Z (N) [skip ci]` as `github-actions[bot]`, and push the version
commit and annotated `vX.Y.Z` tag atomically with a lease on the original main SHA. Only
`gradle.properties` is staged. A concurrent source commit rejects the entire transaction.
It then creates a draft GitHub release, uploads all six assets, and publishes the non-prerelease
release last. Thus `release: published` sees all assets and the source trigger can dispatch
`publish-kmp` to both store consumers. The release body contains only the shared English notes.

Use **Re-run failed jobs** after a transient final publication failure. Finalization accepts
an already-pushed transaction only when main and the tag point to the exact bot bump with
the original source parent and identical `gradle.properties` content. It resumes an existing
draft after an interrupted upload. It fails explicitly rather than replacing an already
published release or an unrelated source/tag state. Re-running the entire workflow on the
bot head deliberately skips; do not create a second version to recover the first one.

The `notify` job always reports build/release success or failure through the shared structured
HTML Telegram composite, including failed job/step/error pointers and the run link on failure.
No PR comment or commit status is created. `publish-playstore.yml` and `publish-appstore.yml`
handle subsequent store notifications and PostHog annotations.

Offline validation:

```sh
python3 -m unittest discover -s scripts/kmp -p 'test_*.py'
actionlint .github/workflows/kmp-build-release.yml
```

Runtime/device performance pairs are not applicable: this change contains CI orchestration
and scripts only. Hosted runner billing/label, production signing and entitlements,
Crashlytics upload, GitHub release creation and store acceptance remain unverified until
configured and exercised by the coordinator. Nothing in the offline tests publishes to a
remote service; Git lease tests use temporary local bare repositories.

The App Store publisher uses `scripts/ios/publish_appstore.py` with PyJWT 2.15.1 for refreshed
ES256 authentication through the shared `scripts/ios/asc_client.py`. Its fake-client tests run
without that dependency or credentials:

```sh
python3 -m unittest discover -s scripts/ios -p 'test_publish*.py'
```

Apple documents [metadata carry-forward when creating a new version](https://developer.apple.com/help/app-store-connect/update-your-app/create-a-new-version/).
The client preserves current metadata, fills absent localization/review fields from the previous
production version, and writes only the requested shared English What's New across all locales.
App-level age ratings are not changed; the source prerelease checklist requires confirmation.
[Review submissions](https://developer.apple.com/documentation/appstoreconnectapi/post-v1-reviewsubmissions)
and [submission items](https://developer.apple.com/documentation/appstoreconnectapi/post-v1-reviewsubmissionitems)
are used instead of the deprecated single-version submission endpoint. A newly created
[phased release](https://developer.apple.com/documentation/appstoreconnectapi/phasedreleasestate)
is INACTIVE until Apple's release process starts its standard seven-day schedule. Submission
requests `AFTER_APPROVAL`; successful CI means submitted for review, not approved or live.

Final offline validation, 2026-10-24: 51 central KMP tests, 20 App Store fake-client tests and
7 existing release-note tests passed. All three affected central workflows passed actionlint
without ignores, as did both source triggers; both repositories passed `git diff --check`.
The source release/trigger/existing CI/CLI suites passed 36 tests plus six preparation tests.
