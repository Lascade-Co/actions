# A train closes when its App Store version record exists, whatever state that record is in

`ios-build-debug.yml` cannot produce a **TestFlight build** for a **closed train**. App Store
Connect refuses any upload to a train whose version has shipped, rejecting it as `ITMS-90186
Invalid Pre-Release Train. The train version 'X' is closed for new build submissions` — and that
rejection lands in the `publish` job, past a macOS archive that may run to a 120-minute timeout.
A **train** is therefore closed when an App Store version record exists for its **marketing
version**, whatever state that record is in, and every train at or below it closes with it. The
**central runner** picks `max(project marketing version, highest TestFlight train)`, and if that
is at or below the highest closed train, takes one patch step above the highest closed train.

**The logic has flipped twice, and each flip fixed the previous one's failure.** `0f58ea5` read
both `appStoreVersions` and `preReleaseVersions`, took the highest of everything and always
incremented the patch — right about closed trains, but it minted a fresh **marketing version** per
build, so builds for one release candidate could not be grouped. `7b2e432` replaced it with "reuse
the highest TestFlight train verbatim, never increment", which fixed the churn and is what makes
the **build number** the thing that distinguishes builds, but deleted the `appStoreVersions`
lookup — the only signal that a train had closed. `scripts/ios/test_next_version.py` went as far
as asserting the lookup did not happen, which is the bug written down as a test. This is the third position: reuse the
train while it is open, step past it once it is closed.

**Why one patch step above the *highest closed* train, not above the candidate.** Bumping the
candidate is the obvious move and is wrong: bumping `4.0.0` when `4.0.1` is closed lands on
`4.0.1`, still closed. One step above the highest closed train is provably above all of them, so
it clears them at once — no loop, no retry.

**Why any record closes the train, rather than reading its state.** App Store Connect exposes 15
`appVersionState` values, and two finer rules were considered: allowlist the still-editable states
(`PREPARE_FOR_SUBMISSION`, `WAITING_FOR_REVIEW`, `IN_REVIEW`, the rejected states), or blocklist
only the published ones (`READY_FOR_DISTRIBUTION` and friends). Both buy nothing here: this team
creates the version record at submission time, not at the start of the release-candidate window,
so by the time one exists the train's testing life is over, and the three rules diverge only
across a long Prepare-for-Submission window this team does not have. The crude rule needs no enum,
cannot rot when Apple adds a state, and avoids `appStoreState`, deprecated in favour of
`appVersionState` — the `0f58ea5` code read the deprecated one. If the process ever changes so
records are opened early, this rule must be revisited: it would then push testers off the very
train under review.

**The enum is still read, and must not be "cleaned up".** The script requests
`fields[appStoreVersions]=versionString,appVersionState` and logs the state in its stderr warning,
purely so a human reading the job log sees *why* a train was judged closed. Nothing branches on
it. A future reader will find an unused field and want to delete it or, worse, branch on it —
branching on it reopens one of the two failure modes above.

**Why not bump the project version instead.** `ios-build-release.yml:271` runs
`bash increment_version.sh` in the app repo to bump the **marketing version** after a release, and
that file does not exist in `travel-animator-ios` on any branch — `contents/increment_version.sh`
on `main` is a 404, confirmed again today. The step fails, so the bump has never once run, as
`docs/handoff/ios-testflight-missing-secrets.md` section 3 recorded. Yet the project sits at
`MARKETING_VERSION = 4.0.1` / `CURRENT_PROJECT_VERSION = 235`, up from ADR-0008's `3.9.3` / `213`,
so somebody edits the pbxproj by hand. A fix resting on that bump rests on a human remembering.

**The project version may still open a train, deliberately.** It wins when it is higher than every
TestFlight train, and that is now the only deliberate way to open one — how a PR that legitimately
bumps `4.0.1` to `4.1.0` gets testers onto builds labelled `4.1.0`. The cost: `MARKETING_VERSION`
comes from unreviewed PR-head code, so one stray edit opens a train nobody wanted and, because the
runner always reuses the highest train, every later build from every branch inherits it and trains
never go back down. Accepted, because making App Store Connect the sole authority would mean a
`5.0.0` beta could not be tested as `5.0.0` until `5.0.0` had shipped, and a stray bump is visible
in the PR diff before the build runs.

**Why the lookup stays fail-soft.** When App Store Connect cannot be reached at all, the script
falls back to the project **marketing version** and the build proceeds — a version lookup must not
break a build during an Apple outage. The cost was weighed: a macOS runner bills at a 10x
multiplier, so a doomed build running to the timeout is ~1200 billable minutes. The compromise is
that the fallback is no longer silent — the script emits `verified=false` alongside the version,
and `ios-build-debug.yml` renders it as an explicit warning on the PR comment and the Telegram
notification, so whoever is watching knows the upload is a gamble instead of learning it from
`ITMS-90186`. Hard-failing was rejected: it blocks every iOS TestFlight build on any transient
Apple API blip.
