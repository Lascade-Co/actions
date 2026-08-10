# TestFlight builds are Release-optimised with the `DEBUG` flag on

A **TestFlight build** is archived `-configuration Release` but compiled with
`SWIFT_ACTIVE_COMPILATION_CONDITIONS='DEBUG $(inherited)'` and
`GCC_PREPROCESSOR_DEFINITIONS='DEBUG=1 $(inherited)'`. Testers need the `#if DEBUG` toggles —
`travel-animator-ios` has 69 gated sites, including the Debug Options screen — but they also
need a binary whose performance means something.

**Why not archive the Debug configuration.** Xcode injects
`com.apple.security.get-task-allow` based on the active build configuration, and App Store
Connect rejects binaries carrying it (the `90046` "Invalid Code Signing Entitlements" family).
It can be forced off with `CODE_SIGN_INJECT_BASE_ENTITLEMENTS=NO`, and the project's Debug
config would also need `DEBUG_INFORMATION_FORMAT=dwarf-with-dsym` since it produces `dwarf` and
therefore no dSYMs for Crashlytics. Both are workable. The blocker is `-Onone`: this is a route
rendering and video export app, and a tester judging animation smoothness on an unoptimised
binary is judging something that will never ship.

**Consequence — the flag flips all 69 sites, not just the toggles.** `#if DEBUG` also wraps code
in `TAAnalyticsManager`, `ConstantKeys`, `AppDelegate`, `OSLogSink` and
`HomeRealRouteLimitEvaluator`, and a debug premium override exists. A TestFlight build therefore
plausibly ships with analytics rerouted, verbose logging on, and premium unlocked. For internal
PR testing that is wanted. It also means such a build must never be the one submitted for
review — it cannot be, because `ios-build-release.yml` is a separate workflow that archives
Release without the flag, but the separation is load-bearing rather than incidental.

**The cleaner form of this decision** is a third build configuration in the Xcode project —
`Beta`, inheriting Release and adding the flag — which states the intent in the project instead
of in an `xcodebuild` argument. That is an app-repo change and was out of scope; if it ever
lands, this ADR is superseded by it.
