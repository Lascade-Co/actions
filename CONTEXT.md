# Lascade Actions

The shared vocabulary for this repo's pipelines. Two have enough domain language to need it: **iOS Builds** and **Confidentiality**.

Product reports live in Lascade-Co/product-actions (ADR 0015).

## Language — iOS Builds

The central runners that turn an iOS repo into an uploaded binary
(`.github/workflows/ios-build-debug.yml` and `ios-build-release.yml`), driven by triggers in
the app repos.

**TestFlight build**:
What the `debug-ios` runner produces — **Release** configuration compiled with the `DEBUG`
flag on, signed with the App Store distribution identity, uploaded to TestFlight. Release
optimisation so performance is representative; `DEBUG` on so testers reach the `#if DEBUG`
toggles.
_Avoid_: debug build. Android's `assembleDebug` is a genuinely different artifact, and the
shared word has cost us an afternoon already. The event type is still called `debug-ios` and
cannot be renamed — `github.run_number` feeds the **build number** and resets if the workflow
file is renamed.

**Marketing version**:
`MARKETING_VERSION` in the pbxproj — what a user sees, e.g. `3.9.3`. The workflow's `version`
output is this, never the build number.
_Avoid_: version (unqualified — it means both things to different readers).

**Build number**:
`CURRENT_PROJECT_VERSION` — what distinguishes two uploads of the same **marketing version**.
Must strictly increase within a **train**; App Store Connect rejects anything that does not.
_Avoid_: version, build version.

**Train**:
Every build sharing one **marketing version**. Monotonicity is enforced per train, so a train
carries a floor: the highest build number already uploaded to it.

**Closed train**:
A **train** App Store Connect will accept no further uploads to, rejecting them as
`ITMS-90186 Invalid Pre-Release Train`. A train closes when an App Store version record exists
for its **marketing version**, and every train below that one is closed with it.
_Avoid_: released version, closed version — it is the train that closes, and it closes before
the version is public.

**Central runner**:
A workflow in this repo that does the work, invoked by `repository_dispatch` from an app repo.
**Trigger**: the thin workflow in the app repo that dispatches to it.

**What to Test notes**:
The TestFlight text testers see first for a build: the `whatsNew` of its `en-US` beta build
localization. The central KMP debug CI writes the PR (or head commit) there after upload;
`altool` cannot.
_Avoid_: release notes, What's New — those are the App Store version's `whatsNew`.

## Relationships — iOS Builds

- A **trigger** dispatches `debug-ios` with `{repo, pr, branch, title, project_slug}`; `pr` is
  optional, and its presence is what makes the run a PR build rather than a branch build.
- The **central runner** reads Infisical `staging` for **TestFlight builds** and `prod` for
  App Store releases — the signing material is identical in both, because TestFlight and the
  App Store take the same distribution certificate and profiles.
- A **build number** must clear its **train**'s floor. The iOS app (now in the KMP repo
  `travel-animator`, formerly `travel-animator-ios`) is at marketing version `4.0.1` with builds
  around `235` (it was `3.9.3` / `213` when ADR-0008 was written), which is why a
  bare run counter cannot be used (see ADR-0008).
- A **TestFlight build** reuses the highest iOS **train** already in TestFlight, so every build for
  one release candidate shares a **marketing version** and is told apart by its **build number**.
- The project **marketing version** is used instead when it is higher. That is now the only
  deliberate way to open a new **train**: the release runner's post-release bump does not exist —
  `increment_version.sh` is absent from the app repo (`docs/handoff/ios-testflight-missing-secrets.md`).
- Neither is used if it names a **closed train**. The runner then takes one patch step above the
  highest closed train, which is guaranteed to clear all of them at once. No pipeline commits a
  **marketing version** back to the app repo; the choice lives only in the `xcodebuild` invocation.
- **Team ID** and **bundle ID** are derived from the App Store provisioning profile rather
  than stored, so they cannot drift from the profile actually doing the signing.

## Language — Confidentiality

This repo is public, so its Actions logs and artifacts are readable by anyone. Anything
private must be **sealed** before it reaches a log, an artifact or a commit.

**Repo key**:
The single age key pair that seals everything private in this repo. The public half is
committed. The private half lives in Infisical, which syncs it to this repo's Actions secrets;
people use it through `infisical run`, and the **log viewer** Worker holds a copy as a secret.
_Avoid_: encryption key, secret key (ambiguous with Actions secrets).

**Sealed**:
Encrypted to the **repo key**'s public half. Only runners and key holders can open it.
_Avoid_: encrypted, protected.

**Public metadata**:
Values allowed to appear in plain text in logs and artifact names: a repo's name, a branch,
a PR (number and title), and a commit SHA.

**Private value**:
Anything that is not **public metadata** — people's names, emails, Telegram and ad account
IDs, business numbers, source contents, build outputs. It is **masked** wherever the runner
might print it and **sealed** wherever it is stored.
_Avoid_: secret (reserved for Actions/Infisical secrets, which are one kind of private value).

**Sealed log**:
A job's own step output, plus its step summary and workflow annotations, sealed and uploaded as
an artifact. The public run log shows only each step's name, its exit code and the
**log viewer** link.
_Avoid_: encrypted log, private log.

**Sealed artifact**:
Any artifact a job uploads, sealed before upload. A job that consumes one unseals it.

**Log viewer**:
The staff-only page at `ci.lascade.com`, served by a Cloudflare Worker from `Lascade-Co/ci` behind
Cloudflare Access. Given a run ID, the Worker fetches that run's **sealed logs** and **sealed
artifacts** and opens them with the **repo key** it holds; repo keys never reach the browser, so
losing staff access ends someone's ability to read sealed output.
_Avoid_: dashboard, decoder.

**Sealed repository**:
An org repository whose workflows all seal their output, with its own **repo key** (public half
in `.github/seal/repo-key.pub`, private half loaded into the **log viewer**), a passing seal policy
check, and the `sealed-ci` topic that lists it in the **log viewer**. This repo is the first; others
reuse its **seal action**.
_Avoid_: secure repo, encrypted repo.

**Seal action**:
The pair of composite actions this repo publishes on the moving `seal-v1` tag: one installs
the sealed shell as a job's first step, the other uploads the **sealed log** as its last step.
Other **sealed repositories** use them instead of copying scripts.

**Frozen repository**:
A formerly public org repository that ran Actions and was made private and archived, with
Actions disabled, because its run logs leaked **private values**. A README notice at the top
says so. It is restored (unarchived, Actions re-enabled, then made public) only after its
workflows are sealed like this repo's. Frozen on 2026-11-20: `daily-email-digest-actions`,
`connector`, `chat-summary`, `store-reviews`, `infra-actions`, `scratch`, `motwr`.
_Avoid_: disabled repo, locked repo.

**Production environment**:
The GitHub environment named `production`, deployable only from `main`, that holds a
**sealed repository's** secrets. Every job that needs a secret declares it, so only reviewed
code merged to `main` ever sees a secret; branch runs get none.
_Avoid_: prod secrets, protected secrets.

**Reviewed main**:
The ruleset every **sealed repository** puts on `main`: changes arrive only through a pull
request approved by a `@Lascade-Co/backend` code owner (re-approved after any new push), with
the seal policy and tests passing, and no force-push or deletion. Org admins may bypass it, and so
may the CI App on a repository whose workflows commit to `main` (`bypass_apps` in
`scripts/seal/sealed_repos.json`; store-reviews' review-state commits).
_Avoid_: branch protection (the legacy GitHub feature this does not use).

**Product report**:
A scheduled email for the product team (ad spend, catchup, install → paid, marketing net,
digests, chat summaries and the like). Product reports live in the public, sealed
`Lascade-Co/product-actions`, not here; this repo keeps the build, release and deploy
pipelines other repos dispatch to, plus the sealing toolkit.
_Avoid_: daily action, digest (when meaning all of them).

**Masked**:
Registered with the runner (an Actions secret, or `::add-mask::`) so every later line in the
job prints `***` in its place, including step headers and third-party action output.

## Flagged ambiguities

- "debug" meant two incompatible things across the mobile pipelines — Android's
  `assembleDebug` (a real Debug-configuration APK) and iOS's `debug-ios` (a Release archive
  signed for the App Store). Resolved: the iOS artifact is a **TestFlight build**, and it now
  earns the name honestly by compiling Release with the `DEBUG` flag on. The event type keeps
  the old name for a mechanical reason, recorded above.
- "version" meant both `MARKETING_VERSION` and `CURRENT_PROJECT_VERSION` — resolved:
  **marketing version** and **build number**. The distinction is load-bearing; only the build
  number has a monotonicity rule.
- "floor" was nearly overloaded onto marketing versions when **closed train** was introduced —
  resolved: a floor is always a **build number** floor. The version-side concept is expressed as
  *the highest closed train*, so the glossary keeps one floor, not two.
- "closed" could name the **marketing version** or the **train** — resolved: the **train** closes.
  A marketing version is a string and has no state; the train is what App Store Connect refuses.
