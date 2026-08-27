# Release Blog — design

**Date:** 2026-08-27
**Status:** Approved (design), grilled against CONTEXT.md 2026-08-27, pending implementation plan
**Scope:** New reusable workflow + `scripts/seo/release_blog*` module set + `data/RELEASE_BLOG.md`
+ `repos` in `data/seo_sites.json` + one new audit rule (`D9`). Wired into
`android-build-release.yml` only; Flutter and iOS adopt the same reusable workflow later.

**Vocabulary:** this pipeline creates a **draft**, which an **editor** publishes to make it a
**blog**. There is no such noun as a "release blog" — see CONTEXT.md, Language — Release Blog. Links
into existing blogs are **contextual internal links**, never "backlinks"; the pool is the **link
candidates**.

## Problem

Every Android release already turns its git diff into `releasenotes.txt` via Codex. The same diff
carries everything a feature announcement needs — what changed, in which screens, for which users —
and nothing is done with it. Marketing writes feature announcements by hand, late or not at all, and
the site's `/hub` gains no page for a shipped feature.

Meanwhile this repo already knows what a good blog looks like: `scripts/seo/` holds a 40-rule
audit that grades published blogs on title length, meta description, heading order, word count,
image alt text, internal link count and anchor quality. That knowledge is applied *after* someone
publishes, never before.

## Goal

On every Android release, turn the release diff into an SEO-validated, human-sounding HTML draft in
the site's CMS, with real contextual internal links to existing blogs and correctly-sized
placeholders — and never let any part of that affect whether the release ships.

The pipeline must be runnable end-to-end on a laptop without dispatching a workflow.

## Decisions

1. **The pipeline creates a draft and never publishes it.** An **editor** publishes. See ADR-0011.
2. **Every release produces a draft**, including internal-only ones. Predictable cadence; the editor
   bins what doesn't warrant a post. (Considered: abstain when the diff has no user-visible feature,
   the test `RELEASE_NOTES.md` already applies. Rejected — a silent skip is indistinguishable from a
   broken pipeline, and the release notes already encode that judgement for the editor to read.)
3. **A reusable workflow, not a job in the Android runner.** Flutter needs this next; iOS after.
   Invoked with `workflow_call`.
4. **Repo → site mapping lives in `data/seo_sites.json`** as a `repos` list per site. One central
   file, self-documenting. A repo matching no site skips; a repo matching two is a config error and
   also skips — the pipeline never picks a site for you.
5. **Codex CLI writes the draft** — the same `codex exec` path, auth secret and sandbox steps the
   release-notes generation already uses. No second model dependency, and it runs locally for anyone
   logged into codex.
6. **The writer sees a release digest**, not a raw diff: release notes, commit subjects, diffstat, and
   a filtered patch capped at 200 KB. The digest is the outer bound of what a draft may claim.
7. **Link candidates come from the CMS API**, sitemap as fallback.
8. **Validation reuses the audit's own rules** against an explicit allowlist, retries once, and
   publishes the better attempt regardless. Reporting, not gating. See ADR-0012.
9. **A site's `suppress` list applies here too** — a suppressed rule is evaluated and reported but
   cannot trigger the retry or affect scoring. Suppression blocks its pipeline's consequence,
   whatever that consequence is.
10. **A re-run overwrites its own draft.** Every draft closes with a **release marker**; a marker
    found on a draft means overwrite in place, a marker found on a published blog stops the run.
    See ADR-0011.
11. **A placeholder that reaches a live blog is caught by the audit**, not by this pipeline: new rule
    `D9 placeholder-media-published` (`error`), added to the Blog SEO Audit as part of this change.
12. **CMS credentials come from the app's existing Infisical project**, `prod` / `/Build` — the
    folder that already holds `KEYSTORE_BASE64`. Two new secrets: `CMS_USER`, `CMS_APP_PASSWORD`.
    `CMS_USER` must be a publishable editorial account, not a service identity: WordPress makes the
    authenticating user the public byline and `E2`'s schema `author`. See ADR-0011.
13. **The marketing version is derived from the tag**, not passed in — strip an optional leading `v`.
    It reaches the writer as context only, and the body contract keeps it out of the title.
14. **All logic lives in the Python CLI.** The workflow YAML holds no business rules, which is what
    makes local testing and later reuse cheap.

## What is produced

A WordPress draft, not a page. The theme and the Next.js front end own `<title>`, canonical, OG
tags and Article schema; the draft owns the post title, slug, excerpt and body.

Codex writes exactly two files into `out/`:

`blog.json`

```json
{
  "title": "Reshape any route by dragging a waypoint",
  "slug": "drag-waypoints-to-reshape-routes",
  "excerpt": "Waypoints are now draggable, so a route that took four taps to fix takes one. Here is how the new editor behaves, and what it means for long multi-stop trips.",
  "focus_keyword": "drag waypoints route editor",
  "media": [
    {
      "id": "m1",
      "kind": "image",
      "width": 1600,
      "height": 900,
      "alt": "The route editor with a waypoint handle dragged onto a coastal road, the elevation strip updating beneath the map",
      "prompt": "Wide screenshot-style render of a mobile map editor on a light background, a finger dragging a circular waypoint handle from an inland road onto a coastal highway, dashed route line following the drag, elevation graph strip along the bottom redrawing. 16:9. No text overlays, no app-store chrome."
    }
  ]
}
```

`blog.html` — the post body fragment only, opening with the **editor checklist** block and closing
with the **release marker**:

```html
<!-- release-blog: Lascade-Co/travel-animator-android@3.9.3 -->
```

### Body contract

Derived from the thresholds and rules already in `scripts/seo/seo_model.py`:

- **Starts at `<h2>`.** The post title is the page's only `<h1>`, rendered by the theme. A second
  `<h1>` trips `D3`; opening at `<h3>` trips `D4`.
- **Title 15–60 characters** (`title_min` / `title_max`), **excerpt 70–160** (`description_min` /
  `description_max`). The version number stays out of the title so the page does not date itself.
- **900–1400 words.** `word_count_min` is 300; a feature blog that only clears the floor is thin
  content that will not rank.
- **3–5 contextual internal links**, each to a distinct path drawn from the candidate list, with
  descriptive anchor text. `internal_links_min` is 3 and `B7` counts distinct internal paths inside
  the article body; `G4` rejects `click here`, `read more`, `learn more`, `here`, `this link`,
  `link`, `more` (`GENERIC_ANCHOR_TEXT`).
- **No JSON-LD in the fragment.** The front end emits Article schema; a second copy invites `E5`
  schema-url-mismatch. **No FAQPage schema** in particular — `E4` in this repo already treats FAQ
  schema as unsupported, so Q&A goes in as plain headings and paragraphs.
- **Never a feature absent from the diff**, the same constraint `RELEASE_NOTES.md` imposes.

### Humanised voice

Specified as prohibitions, because vague positive instructions ("write naturally") do not change
model output while named prohibitions do:

- No scene-setting openers ("In today's fast-paced world", "Picture this").
- No rule-of-three as a reflex — vary list lengths.
- No promotional puffery: revolutionary, game-changing, seamlessly, effortlessly, unlock, elevate,
  empower, robust, cutting-edge.
- Em dashes capped at two in the whole post.
- Varied sentence length; at least a few sentences under eight words.
- Second person, present tense.
- Every claim tied to something concrete from the diff — a screen name, a gesture, a number. Vague
  adjectives are what make AI copy legible as AI copy.
- No "Conclusion" heading; end on what the reader can now do.

### Placeholders

Placeholders point at `placehold.co` at exact dimensions, so layout and CLS are representative in
the editor:

```html
<figure>
  <img src="https://placehold.co/1600x900/png?text=Route+editor"
       width="1600" height="900" loading="lazy"
       data-placeholder="true" data-media-id="m1"
       alt="The route editor with a waypoint handle dragged onto a coastal road, the elevation strip updating beneath the map">
  <figcaption>Drag any waypoint to reshape the route.</figcaption>
</figure>
```

`alt` is a complete, specific sentence — subject, on-screen state, setting — descriptive enough to
brief the real image on its own. `media[].prompt` carries what belongs in a generation brief but
would be noise in alt: composition, aspect ratio, device framing, colour direction, and text that
must not appear in the image. The split exists because a 300-character alt is worse for screen
readers and reads as keyword stuffing to a crawler, while a structured `prompt` field is directly
consumable by an image-generation step later.

1–6 placeholders per post. Video placeholders use `<video>` with `width`/`height` and a `<p>` fallback
carrying the same description.

The body opens with an **editor checklist** block listing every placeholder with its alt and prompt
in plain language, so whoever publishes has the remaining work enumerated rather than hidden. The
block is wrapped in `<div class="release-blog-checklist" data-strip-before-publish="true">` and the
checklist explicitly instructs its own deletion.

## Release digest

What the writer is shown, in this order, assembled by `release_blog_draft.py`:

1. `releasenotes.txt` from the checked-out tag — what shipped, already in user-facing language.
2. `git log --oneline <base>..<tag>` — commit subjects, often the clearest feature signal.
3. `git diff --stat <base>..<tag>` — scale and which areas moved.
4. The filtered patch.

The patch drops what cannot inform a feature story and would crowd out what can:

| Dropped | Why |
|---|---|
| `*.lock`, `*.lockb`, `gradle.lockfile`, `Podfile.lock`, `pubspec.lock` | dependency churn |
| `build/`, `generated/`, `*.pb.*`, `*.g.dart`, `*.freezed.dart` | machine-written |
| `Binary files ... differ` hunks | no information at all |
| `values-*/strings.xml` | the same new strings repeated in 30 languages |

`values/strings.xml` is **kept** deliberately — new default strings are the literal words on the new
screen, and they are the single best source of concrete detail in an Android diff.

Capped at 200 KB total, with `[diff truncated — N of M bytes shown]` appended when the cap is hit, so
the writer knows it is working from a partial view rather than silently treating it as complete.

## Link candidates

`GET https://<origin_host>/wp-json/wp/v2/posts?per_page=100&status=publish&orderby=date&order=desc&_fields=title,excerpt,link,slug,date`

A dedicated `fetch_link_candidates()` in `release_blog_cms.py` — deliberately **not** a change to
`Fetcher.fetch_cms_posts`, whose `_fields` list the audit's group `I` depends on. Candidates are their
own `LinkCandidate` dataclass; `seo_model.CmsPost` is untouched.

Fallback when the CMS call fails: the site's `sitemap_url`, filtered to paths under `listing_path`,
yielding URL + slug with no title. Relevance is then guessed from slug text, which is worse but not
nothing.

Candidates are passed to Codex as a numbered list of `title | url | excerpt`, and the prompt
requires that every internal link be one of them, copied verbatim.

## Validation

The fragment is wrapped in a synthetic host page — `lang`, `<title>` from `blog.json`,
`<meta name="description">` from the excerpt, `<link rel="canonical">` to
`https://<canonical_host>/hub/<slug>`, and the title as the single `<h1>` above the body, with the
fragment inside `<article>` so `_article_root` populates `content_anchors`. That page goes through
the real `seo_parse.parse_blog()` via a hand-built `Response` (status 200, `text/html`), exactly as
`test_checks_ghi.py` already does. No change to any existing audit module.

Rules run with an empty URL-status map and a bare `SiteContext()`.

### Active pre-publish allowlist

| Rules | What they catch here |
|---|---|
| `D1 D2 D3 D4 D5 D6` | title, description, h1, heading order, word count, image alt |
| `B7 G4` | internal link count, anchor text quality |
| `A1 A2 A4 A5` | the writer linking to `hub.<domain>` or a sibling subdomain instead of `www` |
| `C1 C4 G1 G2` | stray noindex, thin body, `http://` links, fragment weight |
| `E1 E4` | fire only if JSON-LD or FAQ schema is present, which the contract forbids |

### Deliberate exclusions

- `E2 E3 E5 E6 F1 F2 F3 F4 D8 C2` — the front end's output, not the draft's. Asserting them here
  would either validate this design's own scaffolding or demand schema the contract deliberately
  omits.
- `A3 B1 B2 B3 B4 B5 B6 G3 G5 D7 C3 H1 H2 H3 I1 I2 I3 I4` — need the network or the live site. That
  is the daily Blog SEO Audit's job, after publish.
- `D9` — pure-HTML and therefore *eligible*, but a draft legitimately contains placeholders. Being
  runnable offline is not the criterion; being answerable about a draft is. See ADR-0012.

Verified 2026-08-27: every check has the uniform signature `check_x(page, site, urls, ctx)`, and the
18 rules named above read neither `urls` nor `ctx`. Verified again by importing the allowlist
against `seo_checks.RULES_BY_ID`: every id exists, and a bare fragment reports only `B7`.

**A suppressed rule** still evaluates and still appears in `validation.txt`, but cannot trigger the
retry or affect scoring — the audit's suppression semantics, generalised from delivery to whatever a
pipeline's consequence is.

### Local checks

Three things the rule engine cannot express, in `release_blog_check.py`:

1. **Every internal link must appear verbatim in the candidate list** — `error`. This is the
   anti-hallucination guard: a plausible `/hub/best-travel-routes` that was never published is a 404
   in a live blog, and it is precisely the mistake a model makes here.
2. **Slug must not collide** with an existing CMS slug — `error`. A draft must never shadow a
   published post.
3. **Shape** — non-empty title/slug/excerpt, 1–6 placeholders, no `<script>`, `<style>`, `<html>`,
   `<head>` or `<body>` in the fragment, and every `data-media-id` resolving to a `media[]` entry.
   `error`.

Plus **alt descriptiveness** — `warn`: ≥6 words, and not a generic opener used as the whole
description (`image of`, `screenshot of`, `photo of`, `picture of`). `D6` already rejects
filename-ish alt text.

### Severity handling

`error` and `warn` findings trigger the retry and drive scoring. **`info` findings are recorded and
never scored, never retried** — the same rule ADR-0003 sets for the audit, and for the same reason:
`A5` fires on every legitimate link to a sibling subdomain, so treating it as a defect would burn a
retry on correct output.

### Retry and selection

One retry, and only one, triggered by any `error` or `warn` from the active allowlist or the local
checks. The retry prompt is the original plus the previous attempt's HTML and the specific findings
against it. Attempts are scored by (error count, warn count) and the better one is published; ties
keep the first. `validation.txt` records both attempts' findings and which was chosen.

`--html`/`--meta` implies `--no-retry`: there is no generator in that path to retry.

The draft ships even when imperfect: an editor holding a named list of three warnings is better
served than an editor holding nothing.

## Publishing

### The release marker, checked first

Before anything is generated, an authenticated search looks for this release's marker:

`GET /wp-json/wp/v2/posts?status=draft,publish&search=<marker>&_fields=id,slug,status,content`

WordPress `search` is fuzzy, so the result is filtered in code for the exact marker string — a
substring match on `content.rendered`, never trust of the search ranking. Three outcomes:

| Found on | Action |
|---|---|
| nothing | generate, `POST` a new draft |
| a **draft** | generate, then overwrite that post in place: `POST /wp/v2/posts/<id>`, `status` stays `draft` |
| a published **blog** | stop before generating. Log it, upload nothing, exit 0 |

Overwriting discards edits an editor had already made to that draft. That is the accepted trade: a
re-run means the release itself was re-cut, and two near-identical drafts leave the editor guessing
which is current. A published match is never touched — regenerating over a live, edited page is the
one outcome worse than doing nothing.

Because a published match short-circuits before generation, a re-run of an already-published release
costs no Codex call.

### The write

`POST https://<origin_host>/wp-json/wp/v2/posts` (or `/<id>` to overwrite) with HTTP Basic auth
(`CMS_USER` / `CMS_APP_PASSWORD` — a WordPress application password), body:

```json
{ "status": "draft", "title": "...", "slug": "...", "excerpt": "...", "content": "<the fragment>" }
```

`date` is omitted so WordPress stamps it. A non-2xx response is logged to `validation.txt` with
status and body; the run still exits 0.

`CMS_USER` is the public byline: WordPress attributes the post to the authenticating account and the
front end feeds that into the visible byline and `E2`'s schema `author`. It must be a properly-named
editorial account with a display name, bio and avatar — see ADR-0011.

`--dry-run` stops immediately before the POST and writes `out/wp-payload.json` with the exact body
and target URL. `--publish` is its explicit opposite, so no local run reaches a live CMS by
accident.

## Failure model

Following ADR-0003, extended to this pipeline in ADR-0011: **the CLI always exits 0**, and every
step inside the reusable workflow that touches the network or a third-party tool carries
`continue-on-error: true`. Deliberately *not* `continue-on-error` on the calling job: GitHub
restricts which keys a job using `uses:` may set, and a rejected key there would fail to parse the
whole release workflow — exactly the coupling this design exists to avoid. Safety lives inside the
reusable workflow, where it is certain to be legal.

| Condition | Behaviour |
|---|---|
| `inputs.repo` matches no site's `repos` | log, no artifacts, exit 0 |
| `inputs.repo` matches two sites' `repos` | log both names as a config error, no artifacts, exit 0 |
| the release marker is found on a published blog | log, no artifacts, no Codex call, exit 0 |
| the marker search fails (non-2xx, auth rejected) | treat as "not found" and proceed; worst case a duplicate draft, which is recoverable, where skipping would silently lose a blog |
| CMS candidate fetch fails | fall back to sitemap; note it in `validation.txt` |
| Both candidate sources fail | log, skip generation entirely (a blog with no internal links is worse than no blog), exit 0 |
| Codex missing, unauthenticated, or non-zero | log stderr tail, exit 0 |
| Codex writes malformed or missing output | treat as a failed attempt; retry once; then exit 0 |
| Validation still failing after retry | publish the better attempt, artifacts carry the findings |
| `CMS_USER`/`CMS_APP_PASSWORD` absent | generate and validate, upload artifacts, skip the POST, exit 0 |
| POST fails | log status and body, exit 0 |

Artifacts upload on every run including skips, so the pipeline's own health is visible without
depending on the CMS — the same reasoning ADR-0003 gives for uploading `report.html` on clean runs.

## Modules — `scripts/seo/`

Flat siblings importing each other top-level, because the workflow `curl`s them into one working
directory (scripts-refactor spec, constraint 1).

| Module | Responsibility |
|---|---|
| `release_blog.py` | CLI entry, orchestration, artifact writing, exit-0 guarantee |
| `release_blog_digest.py` | git reading, patch filtering, the 200 KB cap, `marketing_version_from_tag()` |
| `release_blog_cms.py` | `fetch_link_candidates()`, sitemap fallback, `find_by_marker()`, `write_draft()` |
| `release_blog_draft.py` | prompt assembly, `codex exec` invocation, output parsing |
| `release_blog_check.py` | synthetic page wrap, rule allowlist, local checks, scoring |

Five modules rather than four: the digest is git-facing and the draft is model-facing, and they fail
for entirely different reasons. `release_blog_cms.py` does its own HTTP rather than reusing
`seo_fetch.Fetcher` — that transport takes no request body, so it cannot POST — but it does reuse
`seo_parse.parse_sitemap()` for the fallback.

Reused unchanged: `seo_model` (thresholds, `GENERIC_ANCHOR_TEXT`, `SiteConfig`, `Response`,
`SiteContext`), `seo_parse` (`parse_blog`), `seo_rulekit`, `seo_checks_abc/def/ghi`, `seo_fetch`
(`RequestsTransport`, `Fetcher` for the sitemap).

### Changes to existing modules

`scripts/seo/seo_model.py` — `SiteConfig` gains `repos: tuple[str, ...] = ()`, parsed in
`site_config_from_dict` from `raw.get("repos") or ()`, plus a new
`resolve_site_for_repo(path, repo) -> SiteConfig | None`. Blast radius is nil: `SiteConfig` is
constructed in exactly one place (`site_config_from_dict`) and every caller goes through the dict
parser.

`scripts/seo/seo_checks_def.py` — new rule `D9 placeholder-media-published` (`error`): fires when
served blog HTML contains `data-placeholder` or `data-strip-before-publish`. Pure-HTML, registered in
the audit's rule list and its report grouping, and **excluded from the pre-publish allowlist** by
design. Its own tests live with the audit's, in `scripts/seo/test_checks_def.py`.

`data/seo_sites.json` — each site gains `repos`:

```json
"repos": ["Lascade-Co/travel-animator-android", "Lascade-Co/travel-animator-ios"]
```

Matching is case-insensitive on the full `owner/name`.

## CLI

```
python3 scripts/seo/release_blog.py
  --site NAME | --repo OWNER/NAME     resolve the site config
  --config PATH                       data/seo_sites.json
  --tag STRING                        the release tag; marketing version derived from it
  --repo-path PATH                    checkout to diff (default: cwd)
  --base REF --head REF               diff range (default base: previous tag before head,
                                      default head: --tag)
  --notes-file PATH                   releasenotes.txt (default: <repo-path>/releasenotes.txt)
  --out DIR                           artifact directory (default: ./out)
  --dry-run | --publish               required, mutually exclusive
  --diff-file PATH                    skip git, use a saved diff
  --candidates-file PATH              skip the CMS call, use a saved candidate list
  --html PATH --meta PATH             skip Codex, validate and publish a hand-written draft
  --no-retry                          single attempt
  --ignore-marker                     generate even if this release's marker already exists
```

Outputs, identical in CI and locally: `blog.html`, `blog.json`, `wp-payload.json`,
`validation.txt`, `prompt.md`, `prompt-retry.md`.

Typical local run:

```bash
python3 scripts/seo/release_blog.py \
  --site travelanimator --config data/seo_sites.json \
  --repo-path ~/src/travel-animator-android \
  --base v3.9.2 --tag v3.9.3 \
  --out ./out --dry-run
```

## Workflow — `.github/workflows/release-blog.yml`

```yaml
on:
  workflow_call:
    inputs:
      repo:         { required: true,  type: string }   # Lascade-Co/travel-animator-android
      tag:          { required: true,  type: string }   # 3.9.3 (Android) | v3.9.3 (Flutter)
      project_slug: { required: true,  type: string }
      base:         { required: false, type: string }   # blank = previous tag before `tag`
      dry_run:      { required: false, type: boolean, default: false }
    secrets: inherit
```

`tag` serves as both the checkout ref and the source of the marketing version (leading `v`
stripped). iOS cuts no tag, so it will need an explicit override when it adopts this — a
`marketing_version` input defaulting to the derivation.

Steps: mint App token (`actions/create-github-app-token@v3`) → checkout `inputs.repo` at
`inputs.tag`, `fetch-depth: 0` → `actions/setup-python@v7` 3.13 → `pip install requests==2.34.2
beautifulsoup4==4.15.0 lxml==6.1.1` (pinned identically to the audit, and for the same reason) →
Infisical `/Build` `prod` as env → restore Codex auth from `CODEX_AUTH_JSON_BASE_64` + install codex
+ enable user namespaces (lifted verbatim from `android-build-release.yml`) → `curl` the four
`release_blog_*` modules plus `seo_model`, `seo_parse`, `seo_rulekit`, `seo_checks_*`, `seo_fetch`
and `data/seo_sites.json` from `$RAW` → one CLI call → `actions/upload-artifact@v7` with
`if: always()`.

The CLI is invoked with `--publish` unless `inputs.dry_run` is true, in which case `--dry-run`. One
of the two is always passed explicitly; there is no default.

### Caller — `android-build-release.yml`

The `release` job gains three outputs from values it already computes:

```yaml
    outputs:
      tag:  ${{ steps.increment_version.outputs.new_version }}
      base: ${{ steps.notes.outputs.base }}
```

Android tags bare (`3.9.3`), so `tag` is the version string as-is; Flutter tags `v3.9.3` and the
leading `v` is stripped when deriving the marketing version. The `Check if release notes are
outdated` step is unconditional, so `base` is always set.

New job:

```yaml
  blog:
    needs: [prepare, release]
    continue-on-error: true
    uses: ./.github/workflows/release-blog.yml
    with:
      repo:         ${{ github.event.client_payload.repo }}
      tag:          ${{ needs.release.outputs.tag }}
      project_slug: ${{ github.event.client_payload.project_slug }}
      base:         ${{ needs.release.outputs.base }}
    secrets: inherit
```

Checking out the tag rather than the branch matters: the `release` job pushes a version-bump commit,
so `HEAD` on the branch has moved by the time this job runs. The tag pins the exact tree the release
was cut from, making `base..tag` reproducible.

### Secrets

Existing, unchanged: `CI_APP_CLIENT_ID`, `CI_APP_PRIVATE_KEY`, `CODEX_AUTH_JSON_BASE_64`,
`INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_DOMAIN`.

New, per app, in Infisical `prod` `/Build`: `CMS_USER`, `CMS_APP_PASSWORD` (a WordPress application
password, not the account password). Named `CMS_*` because CONTEXT.md reserves "CMS" for code
identifiers and avoids "WordPress".

## Documentation

Written during design (2026-08-27), not deferred to implementation.

- **`docs/adr/0011-release-blog-creates-drafts.md`** — the human-in-the-loop stance: draft only,
  rejected alternatives (auto-publish, PR into a content repo, release artifact), placeholders
  legitimate in a draft and caught post-publish by `D9`, overwrite-own-draft / stop-on-published, the
  authenticating account as the public byline, and never reddening a release (extending ADR-0003 from
  a cron onto the release path).
- **`docs/adr/0012-pre-publish-validation-allowlist.md`** — why 18 rules and not the audit, why each
  exclusion class is excluded, why `D9` is excluded despite being eligible, `info` inert, suppression
  applying, one retry not a loop, and the trap that adding a network-dependent rule to the allowlist
  breaks validation silently.
- **`CONTEXT.md`** — a **Language — Release Blog** section (**draft**, **release digest**, **release
  marker**, **placeholder**, **editor**, **editor checklist**, **link candidate**), its
  **Relationships** block, two **Example dialogue** exchanges, a generalised **Suppressed rule**
  definition, and a flagged ambiguity resolving "backlink" to **contextual internal link**.

## Testing

`scripts/seo/test_release_blog.py`, no network and no model:

- site resolution by repo name — hit, miss, case difference, a repo listed under two sites
- candidate parsing from a CMS fixture; sitemap fallback filtered to `listing_path`; both-fail path
- the rule allowlist firing on deliberately bad fragments: short title, over-long excerpt, `<h3>`
  first, two internal links, `read more` anchor, a `hub.` link, missing alt, thin body
- a clean fragment producing zero findings
- local checks: hallucinated internal link, colliding slug, `<script>` present, unresolved
  `data-media-id`, 0 placeholders, 7 placeholders
- alt descriptiveness: 4-word alt, `"Screenshot of the app"`, a good alt
- retry prompt contains the previous HTML and each finding
- attempt scoring: fewer errors wins; equal errors → fewer warns; full tie → first
- `wp-payload.json` shape, including `status: draft`
- `--dry-run` performs no POST (transport double asserts zero calls)
- release digest: filters drop lockfiles / generated / binary / `values-de`, keep `values/strings.xml`;
  the 200 KB cap appends the truncation notice
- marketing version derived from `3.9.3`, `v3.9.3`, and a non-version tag (falls back to raw)
- marker handling: absent → create; on a draft → overwrite that id, `status` still `draft`; on a
  published post → no generation and no write; fuzzy search returning a non-matching post → treated
  as absent
- suppression: a suppressed rule appears in `validation.txt` but triggers no retry and does not
  change which attempt wins
- ambiguous mapping: a repo in two site configs produces no draft

Fixtures in `scripts/seo/fixtures/`: `release_blog_good.html`, `release_blog_bad.html`,
`release_blog_candidates.json`, `release_blog_sample.diff`.

`D9`'s tests belong to the audit and live in `scripts/seo/test_checks_def.py`: fires on
`data-placeholder`, fires on `data-strip-before-publish`, silent on clean HTML.

## Verification

1. `python3 -m pytest scripts/seo/test_release_blog.py` green, and the existing
   `scripts/seo/test_*.py` still green after the `seo_model` change.
2. Local `--dry-run` against a real `travel-animator-android` checkout: inspect `blog.html` for
   voice, `validation.txt` for a clean or explained result, `wp-payload.json` for shape.
3. Local `--publish` against travelanimator once, confirming the draft appears in WP admin,
   unpublished, with the checklist block intact.
4. A real release run: draft created, artifacts uploaded, release itself unaffected.
5. Publish that draft by hand and confirm the next daily Blog SEO Audit reports it clean — the
   pre-publish allowlist's predictions checked against the full live rule set.

## Out of scope

- Generating real images or video. Placeholders and prompts only; `media[].prompt` is the seam.
- Flutter and iOS wiring. The reusable workflow makes each a ~10-line addition when wanted.
- Publishing or scheduling. Drafts only — the sole update path is overwriting this pipeline's own
  unpublished draft, identified by its release marker.
- Social copy, newsletters, changelog pages.
- Translations.

## Open items

- `placehold.co` is an external dependency inside a draft. If it disappears, an editor sees broken
  images in something unpublished — annoying, not harmful. `D9` covers the case that actually matters.
- The pipeline never learns whether drafts get published. If editors stop reviewing them, the symptom
  is silence: no red run, no message, just a `/hub` that stops gaining pages. Deliberate for now
  (ADR-0011); a "drafts older than N days" nudge would belong in the daily audit, not here.
- Categories and tags are not set on the draft, so it lands in WordPress's default category. If the
  site's `/hub` filters by category this will need a per-site config value.
