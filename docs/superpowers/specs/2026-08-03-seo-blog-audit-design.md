# Blog SEO Audit — design

**Date:** 2026-08-03
**Status:** Approved (design), pending implementation plan
**Scope:** New cron workflow + `scripts/seo/` module set + `data/seo_sites.json`.
No changes to existing workflows or scripts.

Vocabulary is fixed in [CONTEXT.md](../../../CONTEXT.md) — **blog**, **blog listing**, **CMS**,
**origin**, **asset URL**, **crawlable position**, **rule**, **finding**, **severity**,
**suppressed rule**, **site config**, **audit run**. The word "hub" appears only as the literal
URL path `/hub`.

## Problem

Blogs for `www.travelanimator.com` and `www.marineradar.com` are authored in a **CMS**
(WordPress) on a `hub.<domain>` **origin** and rendered by a Next.js app on the `www` host.
That split creates SEO failure modes nobody notices until rankings move: a CMS URL leaking
into a **crawlable position**, a moved upload 404ing behind a proxied image, a canonical
pointing at the wrong host. Nothing checks for these today.

## Goal

A daily cron **audit run** per site that reads the **blog listing**, takes the first 10
**blogs**, applies a fixed rule set, and delivers `report.html` to Telegram **only when a rule
of `error` or `warn` severity fires**. One script serves any number of sites via **site
config**. It reports; it never edits.

## Site architecture (verified 2026-08-03)

Both sites share one shape:

| | travelanimator | marineradar |
|---|---|---|
| Front end | Next.js on Vercel, `www` host | same |
| Origin | `hub.travelanimator.com` | `hub.marineradar.com` |
| Non-asset origin path | **308** → `www.<domain>/hub/...` | **301** → same |
| Origin refs in listing HTML | `/wp-content/uploads` **plus ~130 CMS API URLs** | `/wp-content/uploads` only (149) |
| Origin images | proxied via `/_next/image?url=<encoded>` | raw `<img src="https://hub...">` |
| `sitemap.xml` | flat `<urlset>`, 933 locs, 177 blogs | **`<sitemapindex>`, 31 children** |
| Sibling subdomains linked | `model.`, `support.`, `viral.` | `support.` |
| Blog body in server HTML | yes (headings, prose, JSON-LD all SSR) | yes |

Four consequences the design must absorb, all confirmed rather than assumed:

1. The origin redirect status differs (308 vs 301) → redirect rules match **any 3xx**, never a
   specific code.
2. Sitemap membership must **recurse one level** through `<sitemapindex>`.
3. Origin references appear in two forms — raw URL and percent-encoded inside
   `/_next/image?url=` — and both must be decoded before host/path classification.
4. Allowed sibling subdomains are per-site data, never a global constant.

Four invariants verified empirically, so the design need not assume them:

- **Blog listing DOM order is exactly CMS date-descending order.** The first 10 anchors on the
  listing matched the first 10 posts from `wp-json/wp/v2/posts` (date desc) slug-for-slug. "First
  10 in DOM order" therefore means "the 10 newest blogs". If a pinned or featured card is ever
  introduced this silently shifts which blogs get audited — a known, accepted risk.
- **`/wp-content/uploads/` is the only legitimate asset prefix.** travelanimator's listing markup
  holds 417 upload refs against 216 non-asset origin refs (172 `/wp-json/wp`, 44 `/category/*`);
  marineradar's holds 149 upload refs and nothing else.
- **Blogs are English-only.** `/es/hub/...` and `/es/hub` both 404, and blog pages carry zero
  `rel="alternate"` tags, while the marketing pages carry ~30 locales. No rule may *require*
  hreflang on a blog — its absence is correct.
- **The CMS REST API is public and unauthenticated**, and its `modified` timestamp is directly
  comparable to the `dateModified` the rendered page already exposes in JSON-LD. Spot-checked on
  three blogs modified 2026-08-01: `www` was in sync with the CMS on all three.

Two real defects already found during recon, which the rule set is built to catch:

- travelanimator's listing HTML embeds ~130 `hub.travelanimator.com/wp-json/...` and
  `/category/...` URLs inside the Next.js flight payload — not in a crawlable position, but the
  origin is exposed in served markup (rule **A2**). This is structural and expected to persist,
  which is why it ships **suppressed**.
- `sitemap.xml` lists `/hub/category/comparison-guide` (200) while the CMS slug is
  `comparision-guide` (308) — slug drift between sitemap and CMS (rules **B2**, **E3**).

## Decisions

| Decision | Choice |
|---|---|
| Blog selection | First 10 `/hub/<slug>` anchors in DOM order on the **blog listing** |
| Schedule | Daily, `0 6 * * *` UTC (10:00 Asia/Muscat) + `workflow_dispatch` |
| Run shape | Matrix job per site, `fail-fast: false`, isolated report + Telegram send each |
| Site config | `data/seo_sites.json`, read by a discover job that emits the matrix via `fromJSON` |
| Job result | **Always green.** Findings never fail the run; the report is the signal |
| Delivery gate | `error_count + warn_count > 0`. **`info` can never trigger a send** |
| Suppression | Per-site `suppress: ["A2"]`. A **suppressed rule** still evaluates and still appears in the report, but is excluded from the delivery gate |
| Delivery | `curl sendDocument` to chat `-5312322129`, token `secrets.SUBSCRIPTION_TELEGRAM_TOKEN`, matching `ship-accuracy-report.yml`. Artifact uploaded on every run |
| Sibling subdomains | Allowlisted per site; a link to one is `info`, not a finding that pages |
| CMS parity | Group I enabled per site via `cms_api`; degrades to one `info` finding if the API is unreachable ([ADR 0004](../../adr/0004-blog-seo-audit-reads-the-cms-api.md)) |
| Report content | **Bare findings** — rule ID, slug, blog, message, evidence. No remediation hints, no `wp-admin` edit links |
| Out of scope | Core Web Vitals / PageSpeed API (needs a key, adds minutes, flakes), auto-fixing, non-English locale variants (blogs are English-only and correctly carry no hreflang) |

The always-green exit contract, the severity gate, and the suppression model are recorded in
[ADR 0003](../../adr/0003-blog-seo-audit-reports-never-fails.md). The CMS coupling is recorded in
[ADR 0004](../../adr/0004-blog-seo-audit-reads-the-cms-api.md).

## Module layout

```
scripts/seo/
  seo_blog_audit.py  entry — config load, discovery, orchestration, exit contract
  seo_fetch.py       HTTP: no-redirect GET/HEAD, retry, process-wide cache, thread pools,
                     URL classification, image-header dimension probe
  seo_checks.py      rules A1–I4 as pure functions — no network, no I/O
  seo_report.py      findings → self-contained report.html
  fixtures/          saved listing + blog HTML (real and deliberately broken) for tests
  test_seo_checks.py stdlib unittest over fixtures
```

Modules land as siblings in the runner's working directory and use top-level imports, matching
`scripts/catchup/`. Each is curl'd from `$RAW/scripts/seo/`.

The hard boundary: **`seo_checks.py` performs no I/O.** Every rule has the signature
`(page: BlogPage, site: SiteConfig, urls: UrlStatusMap) -> list[Finding]`. All network work
happens in `seo_fetch.py` before rules run. This is what makes the blog fan-out free of shared
mutable state and every rule testable from a fixture with no network.

## Data flow

1. **Load config** — read `seo_sites.json`, select the `--site` entry, build `SiteConfig`.
2. **Discover** — GET `{canonical_host}{listing_path}`; collect `a[href]` matching
   `^/hub/<slug>$` in DOM order, excluding `category/`, `author/`, pagination and the listing
   itself; de-duplicate preserving order; take the first `blog_count` (10). Fewer than 10 is
   itself a finding (**H2**). When `cms_api` is true, also GET the CMS post list and union the
   two sets, tagging each blog with where it was found — that tag is what rules I1 and I3 read.
3. **Fetch blogs in parallel** — all 10 issued concurrently on a `ThreadPoolExecutor`
   (`BLOG_CONCURRENCY`, default 10, i.e. one worker per blog so wall-clock is the slowest
   single page, not the sum). Redirects **not** followed so hops stay visible. Record status,
   headers, time-to-first-byte, body. A blog that raises does not cancel its siblings.
4. **Parse** each into `BlogPage`: anchors (href + text), `img` src and every `srcset`
   candidate, `link[rel]`, og/twitter meta, canonical, JSON-LD blocks, headings, article text,
   response headers, raw HTML (needed for A2's markup scan).
5. **Network-free rules** — A1, A2, A4, A5, C1, C2, C4, D1–D6, D8, E1, E2, E4, E5, E6, F1,
   F2, F3, G1, G2, G4, G5. Run per blog, inside the same fan-out. **D7 is the one cross-blog
   rule** — duplicate title/description/H1 can only be judged once all 10 pages are parsed, so
   it runs after the fan-out joins, over the collected set.
6. **Pool URLs** — union every distinct URL across all 10 blogs, verify once each on a separate
   pool (`URL_CONCURRENCY`, default 8) with a process-wide cache, so a URL shared by six blogs
   costs one request. `HEAD`, falling back to `GET` when a host rejects `HEAD`. `og:image`
   additionally gets `Range: bytes=0-2047` and dimensions are parsed from PNG `IHDR`, JPEG
   `SOFn`, or WebP `VP8X` headers — no Pillow dependency.
7. **Site-level fetches, once** — `robots.txt` and `sitemap.xml` (recursing a `<sitemapindex>`).
8. **URL-dependent rules** — A3, B1–B6, C3, E3, F4, G3. **Parity rules** I1–I4 run here too,
   against the CMS post list fetched in step 2 — still pure functions, since the CMS response is
   just another input.
9. **Render + exit** — write `report.html`; write `has_findings` (errors + warns, excluding
   suppressed rules), `error_count`, `warn_count`, `info_count`, `suppressed_count`, `label` to
   `$GITHUB_OUTPUT`; **exit 0 unconditionally.**

## Rule catalogue

Severity: **error** = crawl or index correctness actively broken · **warn** = threshold or
hygiene · **info** = recorded for visibility, expected to be non-empty, never gates delivery.

### A. Origin hygiene

| ID | Rule | Sev | Check |
|---|---|---|---|
| A1 | `origin-link-in-crawlable-position` | error | An origin URL outside the asset prefixes appears in a **crawlable position** — `a[href]`, canonical, `og:url`, hreflang, JSON-LD `url`/`@id`/`item`, or a sitemap `loc` |
| A2 | `origin-nonasset-in-html` | warn | Same class of URL anywhere else in served markup (inline JSON, flight payload, data attributes) — origin exposure, not a navigational signal. Ships **suppressed** for travelanimator |
| A3 | `origin-asset-status` | error | Every **asset URL**, raw or wrapped in `/_next/image?url=`, returns 200 with an `image/*` or `video/*` content type |
| A4 | `crawlable-host-not-canonical` | error | A URL in a crawlable position on the site's registrable domain that is neither the canonical host nor an allowed sibling subdomain; also bare apex, `http://`, `*.vercel.app`, `localhost`, bare-IP hosts, `?p=<id>` |
| A5 | `allowlisted-subdomain-link` | info | A crawlable-position link to an allowed sibling subdomain |

### B. Link integrity

| ID | Rule | Sev | Check |
|---|---|---|---|
| B1 | `internal-link-broken` | error | Internal `a[href]` returns 4xx or 5xx |
| B2 | `internal-link-redirects` | warn | Internal `a[href]` returns any 3xx — trailing-slash, `http`→`https`, apex→`www`, or slug drift. Evidence includes the `Location` target |
| B3 | `image-broken` | error | Any `img src`, `srcset` candidate, `og:image`, `twitter:image`, or JSON-LD `image` is non-200 or not an image content type |
| B4 | `blog-missing-from-sitemap` | error | Blog URL absent from `sitemap.xml`, resolving `<sitemapindex>` children |
| B5 | `blog-not-linked-from-listing` | error | Blog URL not present as an anchor on the blog listing |
| B6 | `external-link-unreachable` | info | External URL returns 404, 410, or 5xx. **403, 429, and timeouts are recorded as `unverified` and never reported** — bot-blocking hosts (Apple, Instagram, Play) would otherwise cry wolf daily |

### C. Indexability

| ID | Rule | Sev | Check |
|---|---|---|---|
| C1 | `noindex-present` | error | `noindex` in `meta[name=robots]`, `meta[name=googlebot]`, or the `X-Robots-Tag` header |
| C2 | `canonical-invalid` | error | Canonical missing, duplicated, relative, non-https, on the wrong host, or not equal to the fetched URL after normalization |
| C3 | `robots-txt-disallows` | error | Blog path disallowed for `*` or `Googlebot` in the canonical host's `robots.txt`; also fires when no `Sitemap:` directive is declared |
| C4 | `soft-404` | error | 200 response with under 50 words of article text, or body matching not-found phrasing |

### D. On-page

| ID | Rule | Sev | Check |
|---|---|---|---|
| D1 | `title-invalid` | error / warn | Missing → error; length outside 15–60 chars → warn |
| D2 | `meta-description-invalid` | error / warn | Missing → error; length outside 70–160 chars → warn |
| D3 | `h1-invalid` | error | Zero or more than one `<h1>`, or an empty one |
| D4 | `heading-hierarchy` | warn | A heading level is skipped (h2 → h4) or a heading is empty |
| D5 | `thin-content` | warn | Article word count below 300 |
| D6 | `image-alt-missing` | error / warn | Content image with no `alt` → error; `alt` that looks like a filename (`IMG_1234.png`, `travelanimator-banner-5761`) → warn; `alt=""` accepted only with `aria-hidden="true"` or `role="presentation"`, otherwise warn |
| D7 | `duplicate-metadata` | error | Identical title, meta description, or H1 across the 10 blogs in the run |
| D8 | `document-meta-missing` | warn | Missing `<html lang>` or `meta[name=viewport]` |

### E. Structured data

| ID | Rule | Sev | Check |
|---|---|---|---|
| E1 | `jsonld-unparseable` | error | A `script[type="application/ld+json"]` block fails `json.loads`, or lacks `@context`/`@type` |
| E2 | `article-schema-incomplete` | error | No `BlogPosting`/`Article`, or missing `headline`, `image`, `datePublished`, `dateModified`, `author`, or `publisher` |
| E3 | `breadcrumb-invalid` | error | No `BreadcrumbList`, or an `item` URL off the canonical host or not returning 200 |
| E4 | `faq-schema-unsupported` | error | `FAQPage` present but its question text is absent from the rendered body — markup/content mismatch, a manual-action risk |
| E5 | `schema-url-mismatch` | error | `BlogPosting` `url`/`@id` differs from the canonical |
| E6 | `schema-date-invalid` | error | Unparseable date, `datePublished` in the future, or `dateModified` earlier than `datePublished` |

### F. Social

| ID | Rule | Sev | Check |
|---|---|---|---|
| F1 | `og-incomplete` | warn | Any of `og:title`, `og:description`, `og:url`, `og:image`, `og:type` (`article`), `og:site_name` missing |
| F2 | `og-url-mismatch` | error | `og:url` differs from the canonical |
| F3 | `twitter-incomplete` | warn | Missing `twitter:card` or `twitter:image` |
| F4 | `og-image-unsuitable` | warn | `og:image` under 1200×630, over 8 MB, or dimensions unreadable |

### G. Technical

| ID | Rule | Sev | Check |
|---|---|---|---|
| G1 | `mixed-content` | error | Any `http://` subresource (script, img, link, iframe, media) |
| G2 | `page-weight` | warn | Served HTML over 500 KB |
| G3 | `asset-cache-headers` | warn | An **asset URL** response with no `Cache-Control` |
| G4 | `anchor-text-weak` | warn | Internal anchor with empty text, or generic text (`click here`, `read more`, `here`, `this link`). Image-only anchors pass when the image has `alt` |
| G5 | `slow-response` | info | Time-to-first-byte over 1500 ms. **Advisory only** — the 10 blogs are fetched concurrently, so TTFB is measured under self-inflicted load and is not a clean performance signal. Kept at `info` so it can never masquerade as a defect; real performance work belongs to CrUX, which is out of scope |

### I. CMS parity

Only evaluated when the site config sets `"cms_api": true`. Requires one unauthenticated call to
`{origin_host}/wp-json/wp/v2/posts?per_page={blog_count*2}&_fields=slug,date,modified,status`.
If that call fails for any reason — non-200, timeout, schema change, a locked-down CMS — the group
is skipped and a single `info` finding records why. **Parity checks never fail loudly**, because a
CMS that stops answering must not look like a broken website.

This is the one group that reads a second source of truth, and it exists because every other rule
reads only `www` and therefore cannot see a blog that was never rendered at all.

| ID | Rule | Sev | Check |
|---|---|---|---|
| I1 | `blog-not-published-on-www` | error | A CMS post with `status: publish` inside the parity window either returns non-200 on the canonical host or is absent from the blog listing — published, but unreadable and undiscoverable |
| I2 | `stale-render` | warn | CMS `modified` is more than 24 h newer than the rendered JSON-LD `dateModified` — the front end is serving a stale copy |
| I3 | `unpublished-still-live` | error | A blog live on `www` has no CMS post, or a CMS post whose `status` is not `publish` — a zombie page |
| I4 | `cms-parity-skipped` | info | The CMS API was unreachable or unparseable; group I did not run. Carries the underlying error |

**Consequence for discovery:** I1 cannot be detected from the listing alone — a blog missing from
the listing is exactly the thing being looked for. So when `cms_api` is true, the audited set
becomes the **union** of the listing's first `blog_count` and the CMS's first `blog_count` by date.
A blog present in one source but not the other is itself the finding, and the union may push a run
to roughly 12 page fetches instead of 10.

### H. Harness

| ID | Rule | Sev | Check |
|---|---|---|---|
| H1 | `blog-fetch-failed` | error | A blog is unreachable or returns non-200 — recorded once, the other nine continue |
| H2 | `listing-discovery-short` | error | Fewer than `blog_count` blogs discoverable on the blog listing |
| H3 | `harness-error` | error | Unexpected exception, with traceback embedded in the report |

## Site config — `data/seo_sites.json`

```json
[
  {
    "name": "travelanimator",
    "label": "Travel Animator",
    "canonical_host": "www.travelanimator.com",
    "origin_host": "hub.travelanimator.com",
    "origin_asset_prefixes": ["/wp-content/uploads/"],
    "allowed_subdomains": [
      "model.travelanimator.com",
      "support.travelanimator.com",
      "viral.travelanimator.com"
    ],
    "listing_path": "/hub",
    "sitemap_url": "https://www.travelanimator.com/sitemap.xml",
    "blog_count": 10,
    "cms_api": true,
    "suppress": ["A2"],
    "thresholds": {}
  },
  {
    "name": "marineradar",
    "label": "MarineRadar",
    "canonical_host": "www.marineradar.com",
    "origin_host": "hub.marineradar.com",
    "origin_asset_prefixes": ["/wp-content/uploads/"],
    "allowed_subdomains": ["support.marineradar.com"],
    "listing_path": "/hub",
    "sitemap_url": "https://www.marineradar.com/sitemap.xml",
    "blog_count": 10,
    "cms_api": true,
    "suppress": [],
    "thresholds": {}
  }
]
```

`thresholds` overrides the defaults in `seo_blog_audit.py` (title/description lengths, word
count, page weight, TTFB, og:image dimensions, concurrency, timeouts) per site. Empty means
"use defaults". `suppress` holds rule IDs. `cms_api` enables group I. Adding a third site is one
config entry — no code and no workflow edit.

`"suppress": ["A2"]` ships enabled for travelanimator because its A2 exposure is structural —
the Next.js flight payload serialises CMS API responses into the markup, and that will keep
firing until the app changes. Deleting that one string is the deliberate act that re-enables
paging on it.

## Report

One self-contained `report.html`, no external assets — it has to render from a Telegram file
download with no network. Structure:

1. **Header** — site label, listing URL audited, UTC timestamp, run duration, blogs checked.
2. **Summary band** — error / warn / info / suppressed counts as large figures; one-line verdict.
3. **Rule coverage matrix** — rows = the audited blogs, columns = rule groups A–I, colored cell
   per cell (green pass, amber warn, red error, grey not-applicable). A clean run is *visibly*
   clean, which is what makes the report worth opening.
4. **Findings by severity** — errors first, each with rule ID, slug, blog, message, and
   evidence (offending URL, status code, or markup snippet, truncated to 300 chars). Findings are
   **bare**: no remediation prose and no `wp-admin` links. The rule ID and evidence are the whole
   payload.
5. **Suppressed** — every finding from a **suppressed rule**, grouped by rule, with counts and
   a note naming the config key that silenced it. Visible, never delivered on.
6. **Per-blog detail** — collapsible `<details>` per blog listing every rule evaluated with
   its outcome.
7. **Footer** — config used (hosts, allowlist, thresholds, suppress list) so a reader can tell
   whether a finding is a real defect or a threshold that wants tuning.

System font stack, light background, no JavaScript beyond native `<details>`, print-friendly.
Telegram caption: `Blog SEO Audit — <label>: N errors, M warnings`.

## Error handling

- A blog that fails to fetch becomes one `H1` finding; the remaining nine are unaffected.
- URL verification retries once on connection error or timeout, then records `unreachable`.
  A URL that never resolves cannot silently pass a rule.
- The whole run is wrapped: an unexpected exception still renders a report carrying an `H3`
  finding with the traceback, and that report is still delivered. **A silently-green cron is
  the failure mode this design most guards against** — the audit must never fail quietly.
- Per-request timeout 20 s; total run budget roughly 5 minutes for 10 blogs.
- Exit status is always 0. `$GITHUB_OUTPUT` carries the counts that gate the Telegram step.

## Workflow — `.github/workflows/seo-blog-audit.yml`

```
on: schedule (0 6 * * *) + workflow_dispatch (optional site / blog_count inputs)

job discover
  curl $RAW/data/seo_sites.json
  jq -c '[.[].name]' → outputs.sites

job audit
  needs: discover
  strategy: { fail-fast: false, matrix: { site: fromJSON(needs.discover.outputs.sites) } }
  setup-python@v6 (3.13)
  pip install requests beautifulsoup4 lxml
  curl seo_blog_audit.py seo_fetch.py seo_checks.py seo_report.py + seo_sites.json
  python3 seo_blog_audit.py --site ${{ matrix.site }} --config seo_sites.json --output report.html
  upload-artifact@v4  (if: always())
  telegram sendDocument to -5312322129  (if: steps.audit.outputs.has_findings == 'true')
```

`workflow_dispatch` may pass a single `site` to audit one target on demand, and `blog_count`
to widen a manual run beyond 10.

## Testing

- `scripts/seo/fixtures/` holds the real listing and blog HTML captured during recon, a saved CMS
  post-list JSON response, plus a deliberately-broken variant of each (origin anchor, noindex,
  missing canonical, duplicate H1, malformed JSON-LD, future date, `alt`-less image, `http://`
  subresource, a CMS post absent from the listing, a stale `dateModified`).
- `test_seo_checks.py` (stdlib `unittest`): every rule gets a **fires-on-bad** and a
  **silent-on-good** case. Rules are pure, so no mocking is needed beyond a literal
  `UrlStatusMap` dict and a literal CMS post list. Suppression is tested at the gate, not the
  rule: a suppressed rule must still produce its finding and must still leave `has_findings`
  false. Group I gets one extra case — an unreachable CMS API must yield exactly one `I4` `info`
  finding and must not produce `I1`/`I3` errors from the resulting empty post list, which is the
  obvious way to get this wrong.
- `seo_blog_audit.py --offline <fixture-dir>` runs the full pipeline against fixtures with a
  stubbed fetcher, exercising discovery, orchestration, and the report renderer without
  network. Asserted in tests, and useful for iterating on report styling.
- One live smoke run against both sites before commit, with the produced `report.html`
  reviewed by eye.

## Verification

1. `python3 -m py_compile` clean on all four modules.
2. `python3 scripts/seo/test_seo_checks.py` — every rule covered both ways, green.
3. `--offline` run produces a `report.html` whose finding set matches the broken fixtures
   exactly.
4. Live run for both sites: exit 0, report renders, and the known travelanimator A2 CMS-API
   leak appears **in the Suppressed section** while marineradar's report has none — proof the
   rule discriminates between sites rather than always firing, and that suppression works.
5. A deliberate temporary edit removing `"A2"` from `suppress` flips `has_findings` to true for
   travelanimator — proof the gate is wired to the suppress list and not hardcoded.
6. An offline run with the CMS fixture pointed at an unreachable URL yields exactly one `I4`
   `info` finding, zero `I1`/`I3` findings, and leaves `has_findings` unchanged — proof the parity
   group degrades quietly instead of inventing errors from an empty post list.
7. `actionlint` on the new workflow; `jq empty` on `data/seo_sites.json`.
8. `workflow_dispatch` run on `main` confirms the matrix expands to both sites and the Telegram
   send fires exactly once, for the site with findings.
