# SwiftPM manages its own package cache; CI does not pre-stage it

The iOS runner used to pre-clone all 35 SPM packages itself, fabricate a
`SourcePackages` tree with `repositories/`, `checkouts/` and a hand-written
`workspace-state.json`, point `xcodebuild` at it with
`-clonedSourcePackagesDirPath`, and pass `-disableAutomaticPackageResolution`
to stop Xcode second-guessing it. About 130 lines of Python.

**None of it was ever used.** SwiftPM addresses a mirror as
`<basename>-<hash>`, and the hash is an internal that no plain digest of the
URL reproduces — `sha256`, `sha1`, `md5`, with and without `.git`, lowercased,
canonicalised, all miss. The script guessed `sha256(url.lower())[:8]`, so
`abseil-cpp-binary` was written as `abseil-cpp-binary-55105a4a` while SwiftPM
looked for `abseil-cpp-binary-d18c01be`. Checkouts missed too: SwiftPM keys
them by repository basename (`AdaptySDK-iOS`), the script used the lowercased
identity (`adaptysdk-ios`). Not one of the 35 was found.

So every archive did a cold, full, ~7 GB network resolution *inside* the
archive step, on top of the prefetch's own wasted clone. `actions/cache`
faithfully stored and restored a directory nothing ever read. On a clean macOS
runner this never finished: a 56m50s timeout, and before that 3h03m, 1h30m and
30m runs going back to April, all killed. No iOS archive had ever completed.

**Decision.** Delete the prefetch, `-clonedSourcePackagesDirPath` and
`-disableAutomaticPackageResolution`. Run `xcodebuild -resolvePackageDependencies`
and let SwiftPM create its own directories, where the names are correct by
construction. Cache `~/Library/Caches/org.swift.swiftpm` — the shared
repository cache SwiftPM actually consults before fetching.

**Why not just fix the hash.** It is undocumented and internal to the Swift
toolchain, free to change in any Xcode release. The failure mode when it
changes is not an error: it is a silent cache miss that turns into a
60-minute hang. A cache that must guess a private implementation detail to
work is not a cache.

**Consequence — a second bug fell out of the same override.** The project's
Crashlytics build phase runs
`"${BUILD_DIR%/Build/*}/SourcePackages/checkouts/firebase-ios-sdk/Crashlytics/run"`,
which is Firebase's documented snippet and assumes SwiftPM's default
DerivedData location. `-clonedSourcePackagesDirPath` moved the packages into
the repo, so the script could not find its own binary and the archive failed
even once resolution succeeded. Removing the override fixed the hang and this
together. Verified locally: `** ARCHIVE SUCCEEDED **` in 3m10s, producing a
525 MB `.xcarchive` with `TravelRoute.app` and full dSYMs — the first
successful archive of this project.

**`ios-build-release.yml` still passes `-clonedSourcePackagesDirPath`** and so
still carries the Crashlytics defect. It is untouched here and needs the same
treatment.
