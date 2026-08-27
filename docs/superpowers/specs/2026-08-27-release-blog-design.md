# Release Blog — design

**Date:** 2026-08-27
**Status:** Approved (design), pending implementation plan
**Scope:** New reusable workflow + `scripts/seo/release_blog*` module set + `data/RELEASE_BLOG.md`
+ `repos` in `data/seo_sites.json`. Wired into `android-build-release.yml` only; Flutter and iOS
adopt the same reusable workflow later.

## Problem

Every Android release already turns its git diff into `releasenotes.txt` via Codex. The same diff
carries everything a feature announcement needs — what changed, in which screens, for which users —
and nothing is done with it. Marketing writes release blogs by hand, late or not at all, and the
site's `/hub` gains no page for a shipped feature.

Meanwhile this repo already knows what a good blog looks like: `scripts/seo/` holds a 40-rule
audit that grades published blogs on title length, meta description, heading order, word count,
image alt text, internal link count and anchor quality. That knowledge is applied *after* someone
publishes, never before.

## Goal

On every Android release, turn the release diff into an SEO-validated, human-sounding HTML draft in
the site's CMS, with real internal links to existing blogs and correctly-sized media stand-ins —
and never let any part of that affect whether the release ships.

The pipeline must be runnable end-to-end on a laptop without dispatching a workflow.

## Decisions

1. **A release blog lands as a CMS draft, never published.** An editor reviews and publishes. See
   ADR-0011.
2. **Every release produces a draft**, including internal-only ones. Predictable cadence; the editor
   bins what doesn't warrant a post. (Considered: abstain when the diff has no user-visible feature,
   the test `RELEASE_NOTES.md` already applies. Rejected — a silent skip is indistinguishable from a
   broken pipeline, and the release notes already encode that judgement for the editor to read.)
3. **A reusable workflow, not a job in the Android runner.** Flutter needs this next; iOS after.
   Invoked with `workflow_call`.
4. **Repo → site mapping lives in `data/seo_sites.json`** as a `repos` list per site. One central
   file, self-documenting, and a repo with no match skips.
5. **Codex CLI writes the draft** — the same `codex exec` path, auth secret and sandbox steps the
   release-notes generation already uses. No second model dependency, and it runs locally for anyone
   logged into codex.
6. **Backlink candidates come from the CMS API**, sitemap as fallback.
7. **Validation reuses the audit's own rules** against an explicit allowlist, retries once, and
   publishes the better attempt regardless. Reporting, not gating.
8. **CMS credentials come from the app's existing Infisical project**, `prod` / `/Build` — the
   folder that already holds `KEYSTORE_BASE64`. Two new secrets: `CMS_USER`, `CMS_APP_PASSWORD`.
9. **All logic lives in the Python CLI.** The workflow YAML holds no business rules, which is what
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

`blog.html` — the post body fragment only.

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

### Media stand-ins

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

1–6 placeholders per post. Video stand-ins use `<video>` with `width`/`height` and a `<p>` fallback
carrying the same description.

The body opens with an **editor checklist** block listing every placeholder with its alt and prompt
in plain language, so whoever publishes has the remaining work enumerated rather than hidden. The
block is wrapped in `<div class="release-blog-checklist" data-strip-before-publish="true">` and the
checklist explicitly instructs its own deletion.

## Backlink candidates

`GET https://<origin_host>/wp-json/wp/v2/posts?per_page=100&status=publish&orderby=date&order=desc&_fields=title,excerpt,link,slug,date`

A dedicated `fetch_candidates()` in `release_blog_cms.py` — deliberately **not** a change to
`Fetcher.fetch_cms_posts`, whose `_fields` list the audit's group `I` depends on. Candidates are
their own `BlogCandidate` dataclass; `seo_model.CmsPost` is untouched.

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

Verified 2026-08-27: every check has the uniform signature `check_x(page, site, urls, ctx)`, and the
22 rules named above read neither `urls` nor `ctx`.

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

`POST https://<origin_host>/wp-json/wp/v2/posts` with HTTP Basic auth (`CMS_USER` /
`CMS_APP_PASSWORD` — a WordPress application password), body:

```json
{ "status": "draft", "title": "...", "slug": "...", "excerpt": "...", "content": "<the fragment>" }
```

`date` is omitted so WordPress stamps it. A non-2xx response is logged to `validation.txt` with
status and body; the run still exits 0.

`--dry-run` stops immediately before the POST and writes `out/wp-payload.json` with the exact body
and target URL. `--publish` is its explicit opposite, so no local run reaches a live CMS by
accident.

## Failure model

Following ADR-0003, extended to this pipeline in ADR-0011: **the CLI always exits 0** and the
caller's `blog` job is `continue-on-error: true`. Nothing about a blog can redden a release.

| Condition | Behaviour |
|---|---|
| `inputs.repo` matches no site's `repos` | log, no artifacts, exit 0 |
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
| `release_blog_cms.py` | `fetch_candidates()`, sitemap fallback, `post_draft()` |
| `release_blog_draft.py` | prompt assembly, `codex exec` invocation, output parsing |
| `release_blog_check.py` | synthetic page wrap, rule allowlist, local checks, scoring |

Reused unchanged: `seo_model` (thresholds, `GENERIC_ANCHOR_TEXT`, `SiteConfig`, `Response`,
`SiteContext`), `seo_parse` (`parse_blog`), `seo_rulekit`, `seo_checks_abc/def/ghi`, `seo_fetch`
(`RequestsTransport`, `Fetcher` for the sitemap).

### Changes to existing modules

`scripts/seo/seo_model.py` — `SiteConfig` gains `repos: tuple[str, ...] = ()`, parsed in
`site_config_from_dict` from `raw.get("repos") or ()`, plus a new
`resolve_site_for_repo(path, repo) -> SiteConfig | None`. Blast radius is nil: `SiteConfig` is
constructed in exactly one place (`site_config_from_dict`) and every caller goes through the dict
parser.

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
  --version STRING                    display version, copy only
  --repo-path PATH                    checkout to diff (default: cwd)
  --base REF --head REF               diff range (default base: previous tag before head)
  --notes-file PATH                   releasenotes.txt (default: <repo-path>/releasenotes.txt)
  --out DIR                           artifact directory (default: ./out)
  --dry-run | --publish               required, mutually exclusive
  --diff-file PATH                    skip git, use a saved diff
  --candidates-file PATH              skip the CMS call, use a saved candidate list
  --html PATH --meta PATH             skip Codex, validate and publish a hand-written draft
  --no-retry                          single attempt
```

Outputs, identical in CI and locally: `blog.html`, `blog.json`, `wp-payload.json`,
`validation.txt`, `prompt.md`, `prompt-retry.md`.

Typical local run:

```bash
python3 scripts/seo/release_blog.py \
  --site travelanimator --config data/seo_sites.json \
  --repo-path ~/src/travel-animator-android \
  --base v3.9.2 --head HEAD --version 3.9.3 \
  --out ./out --dry-run
```

## Workflow — `.github/workflows/release-blog.yml`

```yaml
on:
  workflow_call:
    inputs:
      repo:         { required: true,  type: string }
      ref:          { required: true,  type: string }
      version:      { required: true,  type: string }
      project_slug: { required: true,  type: string }
      base:         { required: false, type: string }
      dry_run:      { required: false, type: boolean, default: false }
    secrets: inherit
```

Steps: mint App token (`actions/create-github-app-token@v3`) → checkout `inputs.repo` at
`inputs.ref`, `fetch-depth: 0` → `actions/setup-python@v7` 3.13 → `pip install requests==2.34.2
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
      tag:     ${{ steps.increment_version.outputs.new_version }}
      version: ${{ steps.increment_version.outputs.new_version }}
      base:    ${{ steps.notes.outputs.base }}
```

`tag` and `version` are the same string on Android (tags are bare `3.9.3`) and deliberately separate
inputs, because Flutter tags `v3.9.3` while its display version is `3.9.3`. The `Check if release
notes are outdated` step is unconditional, so `base` is always set.

New job:

```yaml
  blog:
    needs: [prepare, release]
    continue-on-error: true
    uses: ./.github/workflows/release-blog.yml
    with:
      repo:         ${{ github.event.client_payload.repo }}
      ref:          ${{ needs.release.outputs.tag }}
      version:      ${{ needs.release.outputs.version }}
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

Part of the change, not a follow-up.

**`docs/adr/0011-release-blogs-are-validated-drafts.md`** — three decisions that will read as
arbitrary in six months otherwise: why a generated blog lands as a draft and never publishes itself;
why pre-publish validation reports rather than blocks (and why it therefore publishes a draft
carrying known warnings); and why a blog failure can never redden a release, extending ADR-0003's
reasoning from a cron to a release-path job. It also records what was considered and rejected —
abstaining on internal-only releases, and hard-gating the POST on error findings.

**`CONTEXT.md`** gains a **Language — Release Blog** section plus its **Relationships** block, and
one entry under **Flagged ambiguities**. The ambiguity is load-bearing: "blog" today means one
published article at `www.<domain>/hub/<slug>`, and this pipeline introduces an unpublished
generated draft. Resolution — **blog** keeps meaning the published article, so the audit's `B5`/`I3`
reasoning stays unambiguous, and **release blog** means the generated draft. Terms to define:
**release blog**, **draft**, **backlink candidate**, **placeholder**, **editor checklist**,
**pre-publish allowlist**, **attempt**.

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

Fixtures in `scripts/seo/fixtures/`: `release_blog_good.html`, `release_blog_bad.html`,
`release_blog_cms_posts.json`, `release_blog_sample.diff`.

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
- Publishing, scheduling, or updating an existing post. Draft creation only; no `PUT`.
- Social copy, newsletters, changelog pages.
- Translations.

## Open items

- `placehold.co` is an external dependency in a draft. If it ever goes away the editor sees broken
  images in an unpublished draft — annoying, not harmful. Revisit if it becomes noise.
- Nothing enforces that the checklist block is stripped before publish. The daily audit will not
  flag it. If editors forget, a `data-strip-before-publish` check belongs in the audit's rule set,
  not here.
