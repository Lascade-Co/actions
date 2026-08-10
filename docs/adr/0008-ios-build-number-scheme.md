# iOS build numbers are `1000 + run_number`, dotted with the PR number

App Store Connect requires the **build number** to strictly increase within a **train** — it
rejects an upload whose `CURRENT_PROJECT_VERSION` is not above the highest already accepted for
that **marketing version**. `travel-animator-ios` sits at marketing version `3.9.3` with builds
around `213`, so the train carries a floor of ~213. Both iOS runners used `github.run_number`,
which is a per-workflow counter sitting at ~10 — below the floor, so every upload would have
been rejected on arrival. No iOS build has ever completed an archive, which is why nobody had
hit this yet.

**Decision.** `ios-build-debug.yml` computes `$((1000 + github.run_number))` as the leading
component and appends the PR number as a second component for PR builds:
`1010.765` for PR #765, plain `1010` for a branch dispatch. The offset clears the train floor
with room to spare; `run_number` supplies monotonicity; the PR number rides along so a tester
looking at TestFlight can see which pull request a build came from.

**Why not `<pr>.<run_number>`.** The obvious ordering, and wrong: `pr` is monotonic in the order
pull requests are *created*, not the order builds *run*. Skipping draft PRs decouples the two —
a draft opened Monday and marked ready Wednesday builds after a higher-numbered PR opened
Tuesday, and lexicographic comparison stops at the first component and rejects it. `reopened`
does the same. Both are event types we deliberately subscribe to.

**Why not epoch seconds.** `<epoch>.<pr>` is monotonic under every ordering and is the only
scheme that also survives `ios-build-release.yml`, whose counter is independent and can
therefore always cross this one. It was rejected for legibility: `1754812345.742` defeats the
purpose of putting the PR number in the build number at all.

**Consequences.** `ios-build-debug.yml` must never be renamed, moved, or deleted and recreated —
`github.run_number` resets with the workflow file, builds would go backwards, and uploads would
be rejected until the counter climbed back. That constraint is also why the misleading
`debug-ios` event name is kept rather than corrected.

`ios-build-release.yml` is untouched and remains independently broken for the same
below-the-floor reason (its counter is at ~9). Whoever fixes it must give it a band that sits
above the PR builds — a matching offset is not enough, because the two counters advance
independently and will cross.
