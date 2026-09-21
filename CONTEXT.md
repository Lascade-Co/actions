# Lascade Actions

The shared vocabulary for this repo's pipelines. Five have enough domain language to need it:
**Daily Catchup**, **Blog SEO Audit**, **Release Blog**, **Marketing Net**, and **iOS Builds**.

## Language — Daily Catchup

The org-wide daily engineering report pipeline (`.github/workflows/daily-catchup.yml`):
discover active repos → summarise each with Codex → merge into one daily file → commit it
to the `catchup` repo and email a styled report.

**Catchup**:
The daily pipeline (and the `Lascade-Co/catchup` data repo) that captures the org's last 24h
of engineering activity.
_Avoid_: digest, standup.

**Daily file**:
The merged `daily/YYYY-MM-DD.json` — one entry per active repo with its developers, bullets,
and enrichment. The single source the commit and email steps both consume.
_Avoid_: report (that's the email), summary (that's per-repo).

**Per-repo summary**:
One repo's `summary-*.json` artifact: developers with Codex bullets plus enrichment.
Produced by the `summarize` matrix job.

**Enrichment**:
The non-commit signals attached to each repo summary — merged **PRs**, active **branches**,
the in-window release **version** tag, and the repo's **tags**. All best-effort.

**Status**:
A commit's delivery state, decided deterministically from branch/PR state in the per-repo
step (never by Codex): **Published** (reachable from the default branch), **Testing** (on a
branch with an open PR), **Work in Progress** (on a branch with no PR). Bullets are grouped
by status, and these are the email's section headings.
_Avoid_: Shipped, In Progress (earlier freeform names — superseded by these three)._

**Exclude list**:
`data/catchup_exclude.txt` — owner/repo names omitted from the daily email (one per line,
`#` comments allowed). A repo is **included by default**; add it here to opt out. Excluded
repos are still summarised and committed to the daily file. Seeded from repos lacking the
`catchup-mail` topic, then maintained by hand — the runtime no longer reads topics.

**Active repo**:
A repo with non-bot commits in the look-back window — the only repos that get summarised and
appear in the daily file. (There is no "inactive" list in the email.)

**Report JSON**:
The email's intermediate `report.json` — Codex prose (executive summary, display names,
patterns) merged with authoritative numbers and the deterministic **status** sections. The
renderer turns it into the HTML email.

**Authoritative**:
A value not trusted from Codex — computed from git/GitHub (commit counts, contributor list,
PR count, version, branches, stats) or derived deterministically (the **status** split).
Codex supplies prose only (bullets, executive summary, display names, patterns).

## Relationships — Daily Catchup

- The **discover** job lists **active repos**; **summarize** turns each into a **per-repo
  summary** with **enrichment**.
- **collect** merges all per-repo summaries into the one **daily file**.
- **commit** publishes the daily file to the **catchup** repo; **email** drops repos in the
  **exclude list**, builds the **report JSON**, and sends it. commit and email run in parallel.

## Language — Blog SEO Audit

The daily cron audit (`.github/workflows/seo-blog-audit.yml`) that checks the newest blogs on
each configured site against a fixed rule set and sends `report.html` to Telegram when
something is wrong.

**Blog**:
One article published at `www.<domain>/hub/<slug>`.
_Avoid_: post, article, hub page.

**Blog listing**:
The paginated index at `www.<domain>/hub` that an audit run reads to decide which blogs to
check.
_Avoid_: hub, blog index, archive.

**CMS**:
The WordPress installation where blogs are authored, reachable at the **origin** as a REST API
under `/wp-json/`.
_Avoid_: WordPress (in code identifiers), backend, hub.

**Origin**:
The `hub.<domain>` host the **CMS** serves from. Every non-asset path on it redirects to the
canonical host.
_Avoid_: hub host, CMS host, WP host.

**Asset URL**:
An **origin** URL under `/wp-content/uploads/` — the only class of origin URL allowed to reach
a crawler.
_Avoid_: static asset, upload, media URL.

**Crawlable position**:
A place in served HTML where a URL is a navigational signal to a crawler — `a[href]`,
canonical, `og:url`, hreflang, JSON-LD `url`/`@id`/`item`, sitemap `loc`. A URL merely present
elsewhere in the markup is not in a crawlable position.
_Avoid_: visible link, public link.

**Contextual internal link**:
An `a[href]` inside the selected blog article body that points to a different path on the
canonical host. Site-wide navigation, headers, footers, asides, self-links, and sibling
subdomains do not count.
_Avoid_: site link, local link.

**Rule**:
One named check with a fixed ID (`A1`…`I4`), a slug, and a **severity**.
_Avoid_: test, validation, assertion.

**Finding**:
One instance of a **rule** firing against one **blog**.
_Avoid_: violation, issue, error (that's a severity).

**Severity**:
A rule's weight — **error** (crawl or index correctness is actively broken), **warn**
(threshold or hygiene), **info** (recorded for visibility, expected to be non-empty). Errors
and warns gate delivery; info never does.

**Suppressed rule**:
A rule named in a **site config**'s `suppress` list — still evaluated and still reported, but unable
to trigger its pipeline's consequence: delivery in an **audit run**, the retry and attempt scoring in
the **Release Blog** pipeline. For known, accepted conditions; a suppress entry means "accepted on
this site" wherever it is read.
_Avoid_: disabled, ignored, muted.

**Site config**:
One entry in `data/seo_sites.json` — canonical host, **origin**, asset path prefixes, allowed
sibling subdomains, listing path, sitemap URL, blog count, thresholds, suppress list.

**Audit run**:
One execution of the audit for one **site config**. Sites run as parallel matrix jobs and
never share a report.

## Relationships — Blog SEO Audit

- A **site config** names exactly one **origin** and one **blog listing**.
- The **blog listing** yields the **blogs** an **audit run** checks (the first N in DOM order).
- Each **rule** yields zero or more **findings** per **blog**; **severity** decides whether an
  audit run delivers its report.
- **Origin** URLs are legitimate only as **asset URLs**. Any other origin URL is a **finding** —
  `error` when in a **crawlable position**, `warn` when merely present in the markup.

## Language — Release Blog

The per-release pipeline (`.github/workflows/release-blog.yml`) that turns a release diff into an
SEO-validated draft in the site's **CMS**, invoked by the central release runners.

**Draft**:
An unpublished post in the **CMS** — what this pipeline creates and the only thing it ever creates.
An editor turns a draft into a **blog** by publishing it; nothing in this pipeline can.
_Avoid_: release blog, generated blog, post, article.

**Release digest**:
What the writer is told about a release — the release notes, the commit subjects, the diffstat, and a
filtered patch. It is the outer bound of what a **draft** may claim: anything not in the digest is a
fabrication.
_Avoid_: context, payload, diff (the digest is more than the diff).

**Release marker**:
The `<!-- release-blog: <owner>/<repo>@<tag> -->` comment closing every **draft** — the pipeline's
identity for one release. A re-run finds its own marker and overwrites that **draft** in place rather
than adding a second one; a marker found on a published **blog** stops the run instead.
_Avoid_: fingerprint, sentinel, idempotency key.

**Placeholder**:
A sized `<img>`/`<video>` stand-in in a **draft**, carrying the real image's description as `alt` and
a generation brief alongside it, marked `data-placeholder`. Expected in a draft; an `error` (`D9`) if
it survives into a published **blog**.
_Avoid_: stand-in, dummy image, mock.

**Editor**:
The person who reviews a **draft** and publishes it. The only actor that can turn a draft into a
**blog**, and the reader the **editor checklist** is written for.

**Editor checklist**:
The block at the top of a **draft** listing every **placeholder** with its description and generation
brief, marked `data-strip-before-publish` and instructing its own deletion.

**Link candidate**:
One existing **blog** offered to the writer as a possible **contextual internal link** target —
title, canonical URL and excerpt, read from the **CMS** newest-first.
_Avoid_: backlink, back link, inbound link.

## Relationships — Release Blog

- One release produces one **draft**, addressed to the one **site config** whose `repos` list names
  the releasing repo. A repo named by no site config produces nothing; a repo named by two is a
  config error and also produces nothing — the pipeline never picks a site for you.
- The **marketing version** the writer is told about is derived from the tag, not passed in — the
  only version-shaped value a release runner reliably has. iOS cuts no tag, so it will need an
  explicit value when it adopts this.
- A release is identified by its **release marker**, never by the draft's slug — the slug is the
  writer's SEO choice and varies between runs of the same release.
- A **draft** is invisible to the **Blog SEO Audit**: the audit reads the **CMS** unauthenticated,
  which returns published posts only. Were credentials ever added there, every draft would read as
  `I1` (in the CMS, absent from `www`) — the audit's blindness to drafts is load-bearing, not
  incidental.
- **Link candidates** are read from the **CMS** and become **contextual internal links** in the
  draft. A link to anything not in the candidate list is a fabricated URL and is rejected.
- A **draft** becomes a **blog** only when an **editor** publishes it, at which point the audit's
  full rule set applies to it like any other blog.
- **Placeholders** and the **editor checklist** are legitimate in a **draft** and forbidden in a
  **blog** — the one condition this repo checks on both sides of publication, pre-publish by the
  Release Blog pipeline (which permits them) and post-publish by `D9` (which does not).

## Language — Marketing Net

The daily cron (`.github/workflows/daily-marketing-net.yml`) that posts a three-image Telegram report:
month-to-date marketing net, cumulative marketing net, and per-day marketing net. Live results are not
persisted or booked; the closed March 2026 benchmark is retained only as an Infisical secret.

**Marketing net**:
Month-to-date revenue across the app stores minus month-to-date marketing spend.
_Avoid_: profit (the PNL app defines `gross_profit` and `contribution_profit` by different
formulas), influencer net, ROI, margin.

**Source**:
One contributor to the figure — **App Store** and **Play Store** on the revenue side, **Influencer**,
**Google Ads** and **Meta Ads** on the spend side. Each is read independently and fails
independently.

**Head**:
The PNL app's classification identity for a spend line, addressed by its `normalized_key`
(`INFLUENCER MARKETING`) and never by its manager-editable display name.
_Avoid_: category, bucket, line item.

**Unavailable**:
A **source** that yielded no figure this run — shown as `unavailable` and left out of the net, never
shown as `$0`, because a plausible zero is indistinguishable from a genuinely quiet month. Covers
both breakage and an empty **window** (the App Store on the 1st, whose window ends before the month
begins); the warning text distinguishes them, the state does not.
_Avoid_: missing, failed, null, zero.

**Window**:
The date span a **source** actually covers. Windows differ per source and are stated in the message
rather than clipped to match: the App Store stops at yesterday, every other source is fresh through
today.

**Net factor**:
The ratio of earnings to sales taken from the most recent **settled month**, applied to Play Store
month-to-date sales to estimate net. It exists because Google publishes no mid-month net figure.
_Avoid_: commission rate, take rate, multiplier.

**Settled month**:
A month whose Play Store bucket holds **both** a `sales/` and an `earnings/` report — the only kind
of month a **net factor** can come from. Earnings land around the middle of the following month.

**Rate date**:
The single date whose FX rates convert every non-USD amount in a run — always yesterday, the most
recent day a rate exists for. One date covers the whole run, including both sides of the **net
factor**.

**Benchmark**:
The closed March 2026 daily **source** data stored as base64 JSON. The comparison uses the same
day-of-month and the same per-source **windows** as the live report, then shows current **marketing
net** minus March **marketing net**. It is unavailable when any live **source** is unavailable.

**Complete marketing-net day**:
A calendar day for which all five daily **sources** are available. Because App Store publishes the
following day, marketing-net charts stop at yesterday even though the card includes today's Play
Store and spend values.

**Marketing-net estimate**:
The current month's average marketing net per **complete marketing-net day**, extended to the last
calendar day of the month. It is a run-rate projection, not a confidence forecast.

**Skip list**:
The Google Ads customer ids excluded from the MCC's children. Google Ads is include-by-default with
exclusions; Meta Ads is the opposite, an explicit list of accounts to read.

## Relationships — Marketing Net

- **Marketing net** = revenue **sources** − spend **sources**, each contributing over its own **window**.
- A **source** yields either an amount or **Unavailable**; there is no third state, and **Unavailable**
  is never coerced to `0`.
- The **marketing net** is itself **Unavailable** when no revenue **source** could be read — a net
  built from spend alone reads as a catastrophic loss to anyone who sees the figure before the
  warning.
- The **Play Store** source multiplies month-to-date sales by the **net factor** derived from the
  latest **settled month**.
- Every non-USD amount converts at the one **rate date** — including both sides of the **net factor**,
  so the ratio stays immune to FX drift.
- The **benchmark** comparison is like-for-like by day and **window**; it never compares a partial
  live result with the complete March result.
- Marketing-net charts never plot a partial set of sources. Their current series stops at the latest
  **complete marketing-net day**, while the March series covers the full benchmark month.
- The **marketing-net estimate** starts at the latest current cumulative value and continues as a
  dashed line to the projected month-end cumulative value.
- **Google Ads** reads the MCC's children minus the **skip list**; **Meta Ads** reads only its listed
  accounts.
- A run is green when it delivers a message and red when the delivery itself fails — the same rule
  ADR-0003 applies to the Blog SEO Audit: findings are output, infrastructure failure is not.

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

## Relationships — iOS Builds

- A **trigger** dispatches `debug-ios` with `{repo, pr, branch, title, project_slug}`; `pr` is
  optional, and its presence is what makes the run a PR build rather than a branch build.
- The **central runner** reads Infisical `staging` for **TestFlight builds** and `prod` for
  App Store releases — the signing material is identical in both, because TestFlight and the
  App Store take the same distribution certificate and profiles.
- A **build number** must clear its **train**'s floor. `travel-animator-ios` is at marketing
  version `4.0.1` with builds around `235` (it was `3.9.3` / `213` when ADR-0008 was written),
  which is why a bare run counter cannot be used (see ADR-0008).
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

## Example dialogue

> **Dev:** "How do I keep a noisy repo out of the email?"
> **Maintainer:** "Add it to data/catchup_exclude.txt. It's still summarised and committed
> to the daily file — it just won't show up in the *email*."

> **Dev:** "The A2 finding says the **origin** is leaking. Is the **blog listing** broken?"
> **Maintainer:** "No — the **blogs** render fine. The Next.js payload embeds **CMS** API URLs
> in the served HTML, so the origin is exposed in the markup but not in a **crawlable
> position**. That's why it's `warn` and not `error`. If it ever shows up in an `a[href]`,
> that's A1 and it's an `error`."

> **Dev:** "The draft still has placehold.co images. Do I need to fix that before it goes to WP?"
> **Maintainer:** "No — **placeholders** belong in a **draft**, that's what the **editor checklist**
> is for. They only become a problem the moment it's a **blog**: `D9` fires `error` on any
> `data-placeholder` in served HTML. So the pipeline puts them in and the audit takes them out, and
> the only person who can move it between those two states is the **editor**."

> **Dev:** "Codex linked to /hub/best-travel-routes and that page doesn't exist. Why didn't it just
> pick a real one?"
> **Maintainer:** "It invented a plausible URL, which is the failure this pipeline expects. Every
> **contextual internal link** has to match a **link candidate** verbatim or validation rejects it and
> burns the retry saying so. A **draft** can only link to blogs the **CMS** actually returned."

> **Dev:** "Meta's token expired overnight. Do we post $0 for it?"
> **Maintainer:** "No — it renders **unavailable** and drops out of the **marketing net**, with a
> warning line. A zero would read as 'we spent nothing on Meta this month', which is a different
> claim, and a much more believable one, than 'we couldn't ask'."

## Flagged ambiguities

- "summary" meant both the per-repo artifact and the emailed report — resolved: **per-repo
  summary** is the artifact, **report JSON** / email is the org-wide output.
- "influencer net" (the handoff document's name for the figure) — superseded by **Marketing net**
  once Google Ads and Meta Ads spend joined it; the old name described one of three spend lines and
  invited the reader to assume the other two were excluded.
- "net" — resolved: **Marketing net** is this figure only. The PNL app's `gross_profit` and
  `contribution_profit` are separate, computed differently, and this one is never called profit.
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
- "hub" meant three things — the CMS host, the blog index page, and the articles themselves.
  Resolved: **origin** is the host, **blog listing** is the index, **blog** is one article, and
  **CMS** is the authoring system. The word "hub" survives only as the literal URL path
  `/hub`, never as vocabulary.
- "backlink" was used for the links a release blog places into existing blogs — resolved: those are
  **contextual internal links**, the term the audit already uses, and the pool they are chosen from
  is the **link candidates**. "Backlink" means an inbound link from another site and is reserved for
  that, so it never names an internal one.
