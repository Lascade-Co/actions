# Blog SEO Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A daily cron GitHub Action that audits the 10 newest blogs on `www.travelanimator.com/hub` and `www.marineradar.com/hub` against 45 SEO rules and delivers `report.html` to Telegram only when an error- or warn-severity rule fires.

**Architecture:** Six modules under `scripts/seo/`, curl'd individually onto the runner as siblings (matching `scripts/catchup/`). A pure domain core (`seo_model`, `seo_parse`, `seo_checks`) that performs no I/O, an I/O shell (`seo_fetch`) with an injectable transport, a renderer (`seo_report`), and an orchestrator (`seo_blog_audit`). Blogs are fetched concurrently; every distinct URL across all blogs is verified once on a second pool with a process-wide cache.

**Tech Stack:** Python 3.13, `requests`, `beautifulsoup4`, `lxml`. Tests are stdlib `unittest`. No Pillow — image dimensions come from magic-byte parsing.

**Spec:** [2026-08-03-seo-blog-audit-design.md](../specs/2026-08-03-seo-blog-audit-design.md) · **ADRs:** [0003](../../adr/0003-blog-seo-audit-reports-never-fails.md), [0004](../../adr/0004-blog-seo-audit-reads-the-cms-api.md)

**Two deliberate deviations from the spec, both improving the same boundaries:**

1. The spec named four modules; this plan uses six. `seo_model.py` (dataclasses + config loading) and `seo_parse.py` (HTML → `BlogPage`) are split out so `seo_checks.py` holds only rules and the "checks perform no I/O" boundary is enforced by imports rather than by discipline.
2. The spec named one test file; this plan uses five plus `seo_testkit.py`. Rule tests are split by group so Tasks 4, 5 and 6 can run as concurrent subagents without write conflicts on a shared file.

## Global Constraints

- Vocabulary is fixed by [CONTEXT.md](../../../CONTEXT.md): **blog**, **blog listing**, **CMS**, **origin**, **asset URL**, **crawlable position**, **rule**, **finding**, **severity**, **suppressed rule**, **site config**, **audit run**. The word "hub" appears in code only as the literal URL path `/hub`. Never name a variable `hub_*`.
- `seo_checks.py` and `seo_parse.py` must not import `requests`, `urllib.request`, `socket`, or `os`. Rules receive data; they never fetch it.
- The audit **always exits 0**. No finding at any severity may raise or set a non-zero exit code.
- `info` severity can never gate delivery. `has_findings` counts only `error` and `warn` findings from rules absent from the site's `suppress` list.
- Rule IDs are stable identifiers: `A1`–`A5`, `B1`–`B6`, `C1`–`C4`, `D1`–`D8`, `E1`–`E6`, `F1`–`F4`, `G1`–`G5`, `H1`–`H3`, `I1`–`I4`. 45 rules total (5+6+4 + 8+6+4 + 5+3+4).
- Every module is curl'd from `https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/seo/<name>` and lands as a sibling in the runner's working directory. Use flat top-level imports (`from seo_model import Finding`), never package-relative imports.
- Python 3.13. Use `X | None` unions and builtin generics; no `typing.Optional`.
- Per-request timeout 20 s. Blog concurrency 10, URL concurrency 8.
- Image content type must be sniffed from magic bytes, never from the URL extension. The origin's returned type for a `.png` URL varies: WebP negotiation is real but gated on the request `Accept` header advertising WebP **and** the Cloudflare cache state, so the same URL can answer `image/webp` or `image/png` on different days. `RequestsTransport` sends only `User-Agent`, so it usually sees `image/png`. Either way the extension is not the type — which is exactly why sniffing is mandatory and why no rule may assert a specific content type beyond "is it an image".
- Redirect rules match **any 3xx**. Never test for a specific redirect code: travelanimator's origin uses 308, marineradar's 301.

---

## File Structure

| File | Responsibility |
|---|---|
| `scripts/seo/seo_model.py` | Dataclasses, severity constants, rule registry types, `SiteConfig` loading and threshold merging. No I/O. |
| `scripts/seo/seo_parse.py` | HTML string → `BlogPage`; sitemap XML → URL set; srcset and `/_next/image` URL decoding. No I/O. |
| `scripts/seo/seo_fetch.py` | `Fetcher` with injectable transport: no-redirect GET/HEAD, retry, process-wide cache, thread pools, image dimension probe, CMS API call. |
| `scripts/seo/seo_rulekit.py` | Shared pure helpers every rule group needs: host classification, crawlable-position extraction, JSON-LD node walking. |
| `scripts/seo/seo_checks_abc.py` | Rules A1–A5 (origin hygiene), B1–B6 (link integrity), C1–C4 (indexability). |
| `scripts/seo/seo_checks_def.py` | Rules D1–D8 (on-page), E1–E6 (structured data), F1–F4 (social). |
| `scripts/seo/seo_checks_ghi.py` | Rules G1–G5 (technical), H1–H3 (harness), I1–I4 (CMS parity). |
| `scripts/seo/seo_checks.py` | Registry aggregator: imports the three groups, exposes `BLOG_RULES`, `RUN_RULES`, `ALL_RULES`, `RULES_BY_ID`. |
| `scripts/seo/seo_report.py` | `Finding` list + run metadata → self-contained `report.html`. |
| `scripts/seo/seo_blog_audit.py` | CLI entry: load config, discover, orchestrate both pools, run rules, render, write `$GITHUB_OUTPUT`, exit 0. |
| `scripts/seo/seo_testkit.py` | Test builders: `make_site`, `make_page`, `make_status`, `make_context`, `fixture`. Imported by tests only. |
| `scripts/seo/test_seo_fetch.py` | `SiteConfig` loading, fetch helpers, dimension parsing, fake transport. |
| `scripts/seo/test_seo_parse.py` | Parser tests against the HTML/XML fixtures. |
| `scripts/seo/test_checks_abc.py` | Rules A1–A5, B1–B6, C1–C4 — fires-on-bad and silent-on-good each. |
| `scripts/seo/test_checks_def.py` | Rules D1–D8, E1–E6, F1–F4. |
| `scripts/seo/test_checks_ghi.py` | Rules G1–G5, H1–H3, I1–I4, including the unreachable-CMS degradation cases. |
| `scripts/seo/test_seo_report.py` | Severity gate, suppression split, rendering, escaping, dark mode. |
| `scripts/seo/test_seo_audit.py` | Registry completeness (all 45), discovery, orchestration against a scripted transport. |
| `scripts/seo/fixtures/` | `good_blog.html`, `listing.html`, `real_blog_head.html`, `cms_posts.json`, `sitemap_index.xml`, `sitemap_child.xml`. |
| `data/seo_sites.json` | Site configs for travelanimator and marineradar. |
| `.github/workflows/seo-blog-audit.yml` | Discover job emits the matrix; audit job runs one site, uploads the artifact, sends to Telegram when gated. |

**Parallelisation:** Tasks 1–4 are sequential foundation (Task 4 establishes `seo_rulekit.py`, which Tasks 5 and 6 import). **Tasks 5, 6 and 7 are mutually independent — dispatch them as three concurrent subagents.** Each writes its own module and its own test file, so there are no shared-file conflicts. Tasks 8 and 9 are sequential and last.

Rule scope: a **blog-scope** rule has signature `(page, site, urls, ctx) -> list[Finding]`. A **run-scope** rule has signature `(pages, site, urls, ctx) -> list[Finding]` and is used where a verdict needs every blog at once (D7 duplicate metadata, C3 robots directives, I1/I3/I4 parity, H2 discovery).

---

## Task 1: Domain model and site config

**Files:**
- Create: `scripts/seo/seo_model.py`
- Create: `data/seo_sites.json`
- Test: `scripts/seo/test_seo_fetch.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `SEVERITY_ERROR`/`SEVERITY_WARN`/`SEVERITY_INFO`; dataclasses `SiteConfig`, `Response`, `UrlStatus`, `JsonLdBlock`, `Anchor`, `ImageRef`, `BlogPage`, `CmsPost`, `CmsSnapshot`, `SiteContext`, `Finding`, `Rule`; `DEFAULT_THRESHOLDS: dict`; `site_config_from_dict(raw: dict) -> SiteConfig`; `load_site_config(path: str, name: str) -> SiteConfig`; `finding(rule: Rule, severity: str, message: str, *, blog_url: str | None = None, evidence: str = "") -> Finding`.

- [ ] **Step 1: Write the failing test**

Create `scripts/seo/test_seo_fetch.py`:

```python
import unittest

from seo_model import (
    DEFAULT_THRESHOLDS,
    SEVERITY_ERROR,
    SiteConfig,
    site_config_from_dict,
)

RAW_SITE = {
    "name": "travelanimator",
    "label": "Travel Animator",
    "canonical_host": "www.travelanimator.com",
    "origin_host": "hub.travelanimator.com",
    "origin_asset_prefixes": ["/wp-content/uploads/"],
    "allowed_subdomains": ["support.travelanimator.com"],
    "listing_path": "/hub",
    "sitemap_url": "https://www.travelanimator.com/sitemap.xml",
    "blog_count": 10,
    "cms_api": True,
    "suppress": ["A2"],
    "thresholds": {"title_max": 70},
}


class SiteConfigTest(unittest.TestCase):
    def test_builds_from_dict(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertIsInstance(site, SiteConfig)
        self.assertEqual(site.name, "travelanimator")
        self.assertEqual(site.base_url, "https://www.travelanimator.com")
        self.assertEqual(site.listing_url, "https://www.travelanimator.com/hub")

    def test_thresholds_merge_over_defaults(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertEqual(site.thresholds["title_max"], 70)
        self.assertEqual(
            site.thresholds["title_min"], DEFAULT_THRESHOLDS["title_min"]
        )

    def test_suppress_and_subdomains_are_sets(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertTrue(site.is_suppressed("A2"))
        self.assertFalse(site.is_suppressed("A1"))
        self.assertIn("support.travelanimator.com", site.allowed_subdomains)

    def test_registrable_domain_derived_from_canonical_host(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertEqual(site.registrable_domain, "travelanimator.com")

    def test_missing_optional_keys_get_defaults(self):
        raw = {k: v for k, v in RAW_SITE.items() if k not in ("suppress", "thresholds", "cms_api")}
        site = site_config_from_dict(raw)
        self.assertEqual(site.suppress, frozenset())
        self.assertFalse(site.cms_api)
        self.assertEqual(site.thresholds, DEFAULT_THRESHOLDS)

    def test_severity_constant(self):
        self.assertEqual(SEVERITY_ERROR, "error")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts/seo && python3 -m unittest test_seo_fetch -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_model'`

- [ ] **Step 3: Write seo_model.py**

Create `scripts/seo/seo_model.py`:

```python
"""Domain model for the Blog SEO Audit. No I/O — importable by pure rule code."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field

SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"
SEVERITY_INFO = "info"

SEVERITY_ORDER = {SEVERITY_ERROR: 0, SEVERITY_WARN: 1, SEVERITY_INFO: 2}

DEFAULT_THRESHOLDS = {
    "title_min": 15,
    "title_max": 60,
    "description_min": 70,
    "description_max": 160,
    "word_count_min": 300,
    "soft_404_word_count": 50,
    "page_weight_bytes": 500 * 1024,
    "ttfb_ms": 1500,
    "og_image_min_width": 1200,
    "og_image_min_height": 630,
    "og_image_max_bytes": 8 * 1024 * 1024,
    "stale_render_hours": 24,
    "blog_concurrency": 10,
    "url_concurrency": 8,
    "request_timeout": 20,
}

GENERIC_ANCHOR_TEXT = frozenset(
    {"click here", "read more", "here", "this link", "link", "more", "learn more"}
)


@dataclass(frozen=True)
class SiteConfig:
    name: str
    label: str
    canonical_host: str
    origin_host: str
    origin_asset_prefixes: tuple[str, ...]
    allowed_subdomains: frozenset[str]
    listing_path: str
    sitemap_url: str
    blog_count: int
    cms_api: bool
    suppress: frozenset[str]
    thresholds: dict

    @property
    def base_url(self) -> str:
        return f"https://{self.canonical_host}"

    @property
    def listing_url(self) -> str:
        return f"{self.base_url}{self.listing_path}"

    @property
    def registrable_domain(self) -> str:
        return ".".join(self.canonical_host.split(".")[-2:])

    def is_suppressed(self, rule_id: str) -> bool:
        return rule_id in self.suppress

    def threshold(self, key: str):
        return self.thresholds[key]


def site_config_from_dict(raw: dict) -> SiteConfig:
    thresholds = dict(DEFAULT_THRESHOLDS)
    thresholds.update(raw.get("thresholds") or {})
    return SiteConfig(
        name=raw["name"],
        label=raw["label"],
        canonical_host=raw["canonical_host"],
        origin_host=raw["origin_host"],
        origin_asset_prefixes=tuple(raw["origin_asset_prefixes"]),
        allowed_subdomains=frozenset(raw.get("allowed_subdomains") or ()),
        listing_path=raw["listing_path"],
        sitemap_url=raw["sitemap_url"],
        blog_count=int(raw["blog_count"]),
        cms_api=bool(raw.get("cms_api", False)),
        suppress=frozenset(raw.get("suppress") or ()),
        thresholds=thresholds,
    )


def load_site_config(path: str, name: str) -> SiteConfig:
    with open(path, encoding="utf-8") as fh:
        entries = json.load(fh)
    for raw in entries:
        if raw["name"] == name:
            return site_config_from_dict(raw)
    known = ", ".join(sorted(e["name"] for e in entries))
    raise KeyError(f"no site named {name!r} in {path} (known: {known})")


@dataclass
class Response:
    """One HTTP response. status == 0 means the request never completed."""

    url: str
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""
    content: bytes = b""
    ttfb_ms: int = 0
    error: str | None = None

    def header(self, name: str) -> str:
        return self.headers.get(name.lower(), "")

    @property
    def ok(self) -> bool:
        return self.status == 200


@dataclass
class UrlStatus:
    url: str
    status: int = 0
    content_type: str = ""
    location: str | None = None
    cache_control: str | None = None
    verified: bool = True
    width: int | None = None
    height: int | None = None
    byte_size: int | None = None
    error: str | None = None

    @property
    def is_redirect(self) -> bool:
        return 300 <= self.status < 400

    @property
    def is_broken(self) -> bool:
        return self.status >= 400 or self.status == 0

    @property
    def is_image(self) -> bool:
        return self.content_type.startswith(("image/", "video/"))


@dataclass
class JsonLdBlock:
    raw: str
    data: object | None = None
    error: str | None = None


@dataclass
class Anchor:
    href: str
    url: str
    text: str
    image_alts: tuple[str, ...] = ()


@dataclass
class ImageRef:
    url: str
    alt: str | None = None
    aria_hidden: bool = False
    role: str | None = None
    source: str = "img"


@dataclass
class BlogPage:
    url: str
    slug: str
    response: Response
    found_in: frozenset[str] = frozenset()
    title: str | None = None
    meta_description: str | None = None
    canonicals: tuple[str, ...] = ()
    robots_meta: tuple[str, ...] = ()
    og: dict[str, str] = field(default_factory=dict)
    twitter: dict[str, str] = field(default_factory=dict)
    html_lang: str | None = None
    has_viewport: bool = False
    headings: tuple[tuple[int, str], ...] = ()
    anchors: tuple[Anchor, ...] = ()
    images: tuple[ImageRef, ...] = ()
    jsonld: tuple[JsonLdBlock, ...] = ()
    article_text: str = ""
    raw_html: str = ""
    subresources: tuple[str, ...] = ()

    @property
    def h1s(self) -> tuple[str, ...]:
        return tuple(text for level, text in self.headings if level == 1)

    @property
    def canonical(self) -> str | None:
        return self.canonicals[0] if self.canonicals else None

    @property
    def word_count(self) -> int:
        return len(self.article_text.split())


@dataclass
class CmsPost:
    slug: str
    date: str = ""
    modified: str = ""
    status: str = ""


@dataclass
class CmsSnapshot:
    posts: tuple[CmsPost, ...] = ()
    ok: bool = False
    error: str | None = None
    enabled: bool = False

    def by_slug(self) -> dict[str, CmsPost]:
        return {p.slug: p for p in self.posts}


@dataclass
class SiteContext:
    listing_urls: tuple[str, ...] = ()
    listing_ok: bool = False
    sitemap_urls: frozenset[str] = frozenset()
    sitemap_ok: bool = False
    robots_txt: str = ""
    robots_ok: bool = False
    cms: CmsSnapshot = field(default_factory=CmsSnapshot)


@dataclass(frozen=True)
class Finding:
    rule: str
    slug: str
    severity: str
    message: str
    blog_url: str | None = None
    evidence: str = ""

    def sort_key(self) -> tuple:
        return (SEVERITY_ORDER[self.severity], self.rule, self.blog_url or "")


@dataclass(frozen=True)
class Rule:
    id: str
    slug: str
    group: str
    fn: Callable
    scope: str = "blog"


def finding(
    rule: Rule,
    severity: str,
    message: str,
    *,
    blog_url: str | None = None,
    evidence: str = "",
) -> Finding:
    return Finding(
        rule=rule.id,
        slug=rule.slug,
        severity=severity,
        message=message,
        blog_url=blog_url,
        evidence=evidence[:300],
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd scripts/seo && python3 -m unittest test_seo_fetch -v`
Expected: PASS, 6 tests

- [ ] **Step 5: Create data/seo_sites.json**

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

- [ ] **Step 6: Verify the config loads and is valid JSON**

Run: `jq empty data/seo_sites.json && cd scripts/seo && python3 -c "
from seo_model import load_site_config
s = load_site_config('../../data/seo_sites.json', 'marineradar')
print(s.label, s.listing_url, s.registrable_domain, sorted(s.allowed_subdomains))
"`
Expected: `MarineRadar https://www.marineradar.com/hub marineradar.com ['support.marineradar.com']`

- [ ] **Step 7: Commit**

```bash
git add scripts/seo/seo_model.py scripts/seo/test_seo_fetch.py data/seo_sites.json
git commit -m "feat(seo): add Blog SEO Audit domain model and site config"
```

---

## Task 2: HTML parsing, sitemap parsing, and the test kit

**Files:**
- Create: `scripts/seo/seo_parse.py`
- Create: `scripts/seo/seo_testkit.py`
- Create: `scripts/seo/test_seo_parse.py`
- Create: `scripts/seo/fixtures/good_blog.html`
- Create: `scripts/seo/fixtures/listing.html`
- Create: `scripts/seo/fixtures/sitemap_index.xml`
- Create: `scripts/seo/fixtures/sitemap_child.xml`
- Create: `scripts/seo/fixtures/cms_posts.json`

**Interfaces:**
- Consumes: everything from `seo_model`.
- Produces:
  - `absolutize(base: str, href: str) -> str`
  - `normalize_url(url: str) -> str`
  - `decode_next_image(url: str) -> str | None`
  - `parse_srcset(value: str) -> list[str]`
  - `parse_blog(url: str, slug: str, response: Response, found_in: frozenset[str] = frozenset()) -> BlogPage`
  - `parse_listing(html: str, base_url: str, listing_path: str) -> list[str]`
  - `parse_sitemap(xml_text: str) -> tuple[set[str], list[str]]` returning `(page_urls, child_sitemap_urls)`
  - `slug_from_url(url: str) -> str`
  - From `seo_testkit`: `make_site(**over) -> SiteConfig`, `make_page(**over) -> BlogPage`, `make_status(url, **over) -> UrlStatus`, `make_context(**over) -> SiteContext`, `fixture(name) -> str`

**Note on test layout:** rules are pure, so rule tests build `BlogPage` literals through `seo_testkit.make_page()` rather than parsing HTML. That is more precise than fixture-driven rule tests and it lets Tasks 4, 5 and 6 own separate test files with no write conflicts. Fixtures exist to test the *parser* and the offline integration run.

- [ ] **Step 1: Write the fixtures**

Create `scripts/seo/fixtures/good_blog.html` — a blog that violates nothing:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>How to Create a Travel Animation for Instagram</title>
<meta name="description" content="Learn how to create a travel animation for Instagram Stories and Reels with TravelAnimator, step by step, including trending audio."/>
<link rel="canonical" href="https://www.travelanimator.com/hub/good-blog"/>
<meta property="og:title" content="How to Create a Travel Animation for Instagram"/>
<meta property="og:description" content="Learn how to create a travel animation for Instagram Stories and Reels with TravelAnimator, step by step, including trending audio."/>
<meta property="og:url" content="https://www.travelanimator.com/hub/good-blog"/>
<meta property="og:type" content="article"/>
<meta property="og:site_name" content="Travel Animator"/>
<meta property="og:image" content="https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"/>
<meta name="twitter:card" content="summary_large_image"/>
<meta name="twitter:image" content="https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"/>
<link rel="preload" as="image" href="https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"/>
<script type="application/ld+json">{"@context":"https://schema.org","@type":"BlogPosting","headline":"How to Create a Travel Animation for Instagram","description":"Step by step guide.","url":"https://www.travelanimator.com/hub/good-blog","image":"https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png","datePublished":"2026-07-30T10:00:00+00:00","dateModified":"2026-08-01T10:00:00+00:00","author":{"@type":"Person","name":"Jaseel"},"publisher":{"@type":"Organization","name":"Travel Animator"}}</script>
<script type="application/ld+json">{"@context":"https://schema.org","@type":"BreadcrumbList","itemListElement":[{"@type":"ListItem","position":1,"name":"Home","item":"https://www.travelanimator.com"},{"@type":"ListItem","position":2,"name":"Hub","item":"https://www.travelanimator.com/hub"}]}</script>
<script type="application/ld+json">{"@context":"https://schema.org","@type":"FAQPage","mainEntity":[{"@type":"Question","name":"Is TravelAnimator free to use?","acceptedAnswer":{"@type":"Answer","text":"Yes, a free tier exists."}}]}</script>
</head>
<body>
<nav><a href="/">Home</a><a href="/hub">Hub</a></nav>
<article>
<h1>How to Create a Travel Animation for Instagram</h1>
<img src="https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png" alt="A route animation rendered for Instagram Stories"/>
<h2>Why use TravelAnimator</h2>
<p>PARAGRAPH_ONE</p>
<h3>Pick your destinations</h3>
<p>PARAGRAPH_TWO</p>
<h2>Is TravelAnimator free to use?</h2>
<p>Yes, a free tier exists. Read the <a href="https://www.travelanimator.com/pricing">pricing page</a> for the full comparison, or open <a href="https://support.travelanimator.com/articles/export">the export guide</a>.</p>
<img src="/_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-content%2Fuploads%2F2026%2F07%2Fstep.png&amp;w=1200&amp;q=75" srcset="/_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-content%2Fuploads%2F2026%2F07%2Fstep.png&amp;w=640&amp;q=75 640w, /_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-content%2Fuploads%2F2026%2F07%2Fstep.png&amp;w=1200&amp;q=75 1200w" alt="The destination picker showing two stops"/>
<img src="/decorative-rule.svg" alt="" aria-hidden="true"/>
</article>
<footer><a href="/hub/other-blog">Another blog</a></footer>
</body>
</html>
```

Then pad the body so `word_count` clears 300. Run this from the repo root:

```bash
python3 - <<'PY'
from pathlib import Path
p = Path("scripts/seo/fixtures/good_blog.html")
html = p.read_text()
one = " ".join(["Animating a travel route turns a flat itinerary into something people watch to the end."] * 12)
two = " ".join(["Pick each destination in order and the app draws the connecting leg for you automatically."] * 12)
p.write_text(html.replace("PARAGRAPH_ONE", one).replace("PARAGRAPH_TWO", two))
words = len(p.read_text().split())
print("approx words in file:", words)
PY
```

Create `scripts/seo/fixtures/listing.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head><title>Resource Hub</title></head>
<body>
<nav><a href="/hub">Latest</a><a href="/hub/category/how-to-guide">How to Guide</a><a href="/hub/author/jaseel">Jaseel</a></nav>
<main>
<a href="/hub/good-blog"><h3>How to Create a Travel Animation for Instagram</h3></a>
<a href="/hub/second-blog"><h3>Second blog</h3></a>
<a href="/hub/third-blog"><h3>Third blog</h3></a>
<a href="/hub/good-blog">Duplicate link to the first blog</a>
</main>
<div class="pagination"><a href="/hub?page=2">2</a><a href="/hub/page/3">3</a></div>
</body>
</html>
```

Create `scripts/seo/fixtures/sitemap_index.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<sitemap><loc>https://www.marineradar.com/sitemap-blogs.xml</loc></sitemap>
<sitemap><loc>https://www.marineradar.com/sitemap-pages.xml</loc></sitemap>
</sitemapindex>
```

Create `scripts/seo/fixtures/sitemap_child.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">
<url><loc>https://www.travelanimator.com/hub</loc></url>
<url><loc>https://www.travelanimator.com/hub/good-blog</loc><xhtml:link rel="alternate" hreflang="en" href="https://www.travelanimator.com/hub/good-blog"/></url>
<url><loc>https://www.travelanimator.com/hub/second-blog</loc></url>
</urlset>
```

Create `scripts/seo/fixtures/cms_posts.json`:

```json
[
  {"slug": "good-blog", "date": "2026-07-30T10:00:00", "modified": "2026-08-01T10:00:00", "status": "publish"},
  {"slug": "second-blog", "date": "2026-07-29T10:00:00", "modified": "2026-07-29T10:00:00", "status": "publish"},
  {"slug": "never-rendered-blog", "date": "2026-07-28T10:00:00", "modified": "2026-07-28T10:00:00", "status": "publish"}
]
```

- [ ] **Step 2: Write the failing parser test**

Create `scripts/seo/test_seo_parse.py`:

```python
import unittest

from seo_model import Response
from seo_parse import (
    absolutize,
    decode_next_image,
    normalize_url,
    parse_blog,
    parse_listing,
    parse_sitemap,
    parse_srcset,
    slug_from_url,
)
from seo_testkit import fixture

BASE = "https://www.travelanimator.com"
BLOG_URL = f"{BASE}/hub/good-blog"


def good_page():
    body = fixture("good_blog.html")
    response = Response(url=BLOG_URL, status=200, headers={"content-type": "text/html"}, body=body)
    return parse_blog(BLOG_URL, "good-blog", response)


class UrlHelperTest(unittest.TestCase):
    def test_absolutize_relative(self):
        self.assertEqual(absolutize(BASE, "/hub/x"), f"{BASE}/hub/x")

    def test_absolutize_leaves_absolute(self):
        self.assertEqual(absolutize(BASE, "https://other.com/y"), "https://other.com/y")

    def test_absolutize_ignores_non_http_schemes(self):
        self.assertEqual(absolutize(BASE, "mailto:a@b.com"), "mailto:a@b.com")

    def test_normalize_strips_fragment_and_trailing_slash(self):
        self.assertEqual(normalize_url(f"{BASE}/hub/x/#top"), f"{BASE}/hub/x")

    def test_normalize_lowercases_host_only(self):
        self.assertEqual(normalize_url("https://WWW.Travelanimator.com/Hub/X"), f"{BASE}/Hub/X")

    def test_normalize_keeps_root_slash(self):
        self.assertEqual(normalize_url(f"{BASE}/"), f"{BASE}/")

    def test_decode_next_image(self):
        url = "/_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-content%2Fuploads%2Fa.png&w=640&q=75"
        self.assertEqual(
            decode_next_image(url),
            "https://hub.travelanimator.com/wp-content/uploads/a.png",
        )

    def test_decode_next_image_returns_none_for_plain_url(self):
        self.assertIsNone(decode_next_image("https://example.com/a.png"))

    def test_parse_srcset(self):
        value = "/a.png 640w, /b.png 1200w"
        self.assertEqual(parse_srcset(value), ["/a.png", "/b.png"])

    def test_parse_srcset_ignores_data_uris(self):
        self.assertEqual(parse_srcset("data:image/gif;base64,R0lGOD 1x"), [])

    def test_slug_from_url(self):
        self.assertEqual(slug_from_url(f"{BASE}/hub/good-blog"), "good-blog")


class ParseBlogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.page = good_page()

    def test_head_metadata(self):
        self.assertEqual(self.page.title, "How to Create a Travel Animation for Instagram")
        self.assertTrue(self.page.meta_description.startswith("Learn how to create"))
        self.assertEqual(self.page.canonicals, (BLOG_URL,))
        self.assertEqual(self.page.html_lang, "en")
        self.assertTrue(self.page.has_viewport)

    def test_og_and_twitter(self):
        self.assertEqual(self.page.og["og:url"], BLOG_URL)
        self.assertEqual(self.page.og["og:type"], "article")
        self.assertEqual(self.page.twitter["twitter:card"], "summary_large_image")

    def test_headings_in_document_order(self):
        levels = [level for level, _ in self.page.headings]
        self.assertEqual(levels, [1, 2, 3, 2])
        self.assertEqual(len(self.page.h1s), 1)

    def test_anchors_absolutized(self):
        urls = [a.url for a in self.page.anchors]
        self.assertIn(f"{BASE}/pricing", urls)
        self.assertIn("https://support.travelanimator.com/articles/export", urls)
        self.assertIn(f"{BASE}/hub/other-blog", urls)

    def test_anchor_text_captured(self):
        pricing = next(a for a in self.page.anchors if a.url.endswith("/pricing"))
        self.assertEqual(pricing.text, "pricing page")

    def test_images_include_srcset_and_meta_sources(self):
        sources = {i.source for i in self.page.images}
        self.assertEqual(sources, {"img", "srcset", "og", "twitter", "preload"})

    def test_next_image_urls_are_decoded_to_origin(self):
        origin_urls = [i.url for i in self.page.images if "hub.travelanimator.com" in i.url]
        self.assertTrue(any(u.endswith("/wp-content/uploads/2026/07/step.png") for u in origin_urls))
        self.assertFalse(any("_next/image" in u for u in origin_urls))

    def test_decorative_image_flags_captured(self):
        deco = next(i for i in self.page.images if i.url.endswith("decorative-rule.svg"))
        self.assertEqual(deco.alt, "")
        self.assertTrue(deco.aria_hidden)

    def test_jsonld_blocks_parsed(self):
        types = [b.data["@type"] for b in self.page.jsonld]
        self.assertEqual(types, ["BlogPosting", "BreadcrumbList", "FAQPage"])
        self.assertTrue(all(b.error is None for b in self.page.jsonld))

    def test_malformed_jsonld_records_error_without_raising(self):
        body = '<html><head><script type="application/ld+json">{"a":,}</script></head><body></body></html>'
        page = parse_blog(BLOG_URL, "x", Response(url=BLOG_URL, status=200, body=body))
        self.assertIsNone(page.jsonld[0].data)
        self.assertIsNotNone(page.jsonld[0].error)

    def test_article_text_excludes_nav_and_footer(self):
        self.assertNotIn("Another blog", self.page.article_text)
        self.assertGreater(self.page.word_count, 300)

    def test_raw_html_retained(self):
        self.assertIn("hub.travelanimator.com", self.page.raw_html)


class ParseListingTest(unittest.TestCase):
    def test_returns_blog_urls_in_dom_order_deduplicated(self):
        urls = parse_listing(fixture("listing.html"), BASE, "/hub")
        self.assertEqual(
            urls,
            [f"{BASE}/hub/good-blog", f"{BASE}/hub/second-blog", f"{BASE}/hub/third-blog"],
        )

    def test_excludes_category_author_pagination_and_listing_itself(self):
        urls = parse_listing(fixture("listing.html"), BASE, "/hub")
        joined = " ".join(urls)
        for fragment in ("category", "author", "page", "?"):
            self.assertNotIn(fragment, joined)


class ParseSitemapTest(unittest.TestCase):
    def test_index_returns_children_and_no_pages(self):
        pages, children = parse_sitemap(fixture("sitemap_index.xml"))
        self.assertEqual(pages, set())
        self.assertEqual(
            children,
            [
                "https://www.marineradar.com/sitemap-blogs.xml",
                "https://www.marineradar.com/sitemap-pages.xml",
            ],
        )

    def test_urlset_returns_pages_and_no_children(self):
        pages, children = parse_sitemap(fixture("sitemap_child.xml"))
        self.assertEqual(children, [])
        self.assertIn(BLOG_URL, pages)

    def test_malformed_xml_returns_empty_without_raising(self):
        self.assertEqual(parse_sitemap("<not xml"), (set(), []))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd scripts/seo && python3 -m unittest test_seo_parse -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_parse'`

- [ ] **Step 4: Write seo_testkit.py**

Create `scripts/seo/seo_testkit.py`:

```python
"""Shared test builders. Imported by tests only — keeps rule tests free of HTML."""

from __future__ import annotations

from pathlib import Path

from seo_model import (
    BlogPage,
    CmsSnapshot,
    Response,
    SiteContext,
    UrlStatus,
    site_config_from_dict,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"

SITE_DEFAULTS = {
    "name": "travelanimator",
    "label": "Travel Animator",
    "canonical_host": "www.travelanimator.com",
    "origin_host": "hub.travelanimator.com",
    "origin_asset_prefixes": ["/wp-content/uploads/"],
    "allowed_subdomains": ["support.travelanimator.com"],
    "listing_path": "/hub",
    "sitemap_url": "https://www.travelanimator.com/sitemap.xml",
    "blog_count": 10,
    "cms_api": True,
    "suppress": [],
    "thresholds": {},
}

BLOG_URL = "https://www.travelanimator.com/hub/good-blog"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def make_site(**over):
    raw = dict(SITE_DEFAULTS)
    raw.update(over)
    return site_config_from_dict(raw)


def make_response(**over) -> Response:
    defaults = {
        "url": BLOG_URL,
        "status": 200,
        "headers": {"content-type": "text/html; charset=utf-8"},
        "body": "",
        "ttfb_ms": 200,
    }
    defaults.update(over)
    return Response(**defaults)


def make_page(**over) -> BlogPage:
    """A BlogPage that violates no rule. Override fields to break exactly one thing."""
    defaults = {
        "url": BLOG_URL,
        "slug": "good-blog",
        "response": make_response(),
        "found_in": frozenset({"listing", "cms"}),
        "title": "How to Create a Travel Animation for Instagram",
        "meta_description": (
            "Learn how to create a travel animation for Instagram Stories and Reels "
            "with TravelAnimator, step by step, including trending audio."
        ),
        "canonicals": (BLOG_URL,),
        "robots_meta": (),
        "og": {
            "og:title": "How to Create a Travel Animation for Instagram",
            "og:description": "Step by step guide to travel animations.",
            "og:url": BLOG_URL,
            "og:type": "article",
            "og:site_name": "Travel Animator",
            "og:image": "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png",
        },
        "twitter": {
            "twitter:card": "summary_large_image",
            "twitter:image": "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png",
        },
        "html_lang": "en",
        "has_viewport": True,
        "headings": ((1, "How to Create a Travel Animation for Instagram"), (2, "Why use it")),
        "anchors": (),
        "images": (),
        "jsonld": (),
        "article_text": " ".join(["word"] * 400),
        "raw_html": "<html></html>",
        "subresources": ("https://www.travelanimator.com/app.js",),
    }
    defaults.update(over)
    return BlogPage(**defaults)


def make_status(url: str, **over) -> UrlStatus:
    defaults = {"url": url, "status": 200, "content_type": "text/html", "verified": True}
    defaults.update(over)
    return UrlStatus(**defaults)


def make_context(**over) -> SiteContext:
    defaults = {
        "listing_urls": (BLOG_URL,),
        "listing_ok": True,
        "sitemap_urls": frozenset({BLOG_URL}),
        "sitemap_ok": True,
        "robots_txt": "User-agent: *\nAllow: /\nSitemap: https://www.travelanimator.com/sitemap.xml\n",
        "robots_ok": True,
        "cms": CmsSnapshot(posts=(), ok=True, error=None, enabled=False),
    }
    defaults.update(over)
    return SiteContext(**defaults)
```

- [ ] **Step 5: Write seo_parse.py**

Create `scripts/seo/seo_parse.py`:

```python
"""HTML and XML parsing. Pure — takes strings, returns model objects."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from seo_model import Anchor, BlogPage, ImageRef, JsonLdBlock, Response

NON_CONTENT_TAGS = ("script", "style", "nav", "header", "footer", "aside", "noscript")
LISTING_EXCLUDED_SEGMENTS = ("category", "author", "page", "tag")
SUBRESOURCE_SELECTORS = (
    ("script", "src"),
    ("link", "href"),
    ("iframe", "src"),
    ("img", "src"),
    ("source", "src"),
    ("video", "src"),
    ("audio", "src"),
)


def absolutize(base: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    scheme = urlparse(href).scheme
    if scheme and scheme not in ("http", "https"):
        return href
    return urljoin(base, href)


def normalize_url(url: str) -> str:
    parts = urlparse(url)
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    return urlunparse((parts.scheme.lower(), parts.netloc.lower(), path, "", parts.query, ""))


def decode_next_image(url: str) -> str | None:
    parts = urlparse(url)
    if not parts.path.endswith("/_next/image"):
        return None
    values = parse_qs(parts.query).get("url")
    return values[0] if values else None


def resolve_image_url(base: str, raw: str) -> str:
    absolute = absolutize(base, raw)
    return decode_next_image(absolute) or absolute


def parse_srcset(value: str) -> list[str]:
    if not value or value.strip().startswith("data:"):
        return []
    candidates = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk or chunk.startswith("data:"):
            continue
        candidates.append(chunk.split()[0])
    return candidates


def slug_from_url(url: str) -> str:
    return urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]


def _meta_map(soup, attr: str, prefix: str) -> dict[str, str]:
    found = {}
    for tag in soup.find_all("meta"):
        key = tag.get(attr) or tag.get("name") or tag.get("property")
        if key and key.lower().startswith(prefix):
            found[key.lower()] = (tag.get("content") or "").strip()
    return found


def _article_text(soup) -> str:
    root = soup.find("article") or soup.find("main") or soup.body or soup
    clone = BeautifulSoup(str(root), "lxml")
    for tag in clone.find_all(NON_CONTENT_TAGS):
        tag.decompose()
    return re.sub(r"\s+", " ", clone.get_text(" ", strip=True))


def _collect_images(soup, base: str) -> tuple[ImageRef, ...]:
    images: list[ImageRef] = []
    for tag in soup.find_all("img"):
        src = tag.get("src")
        if src:
            images.append(
                ImageRef(
                    url=resolve_image_url(base, src),
                    alt=tag.get("alt"),
                    aria_hidden=(tag.get("aria-hidden") == "true"),
                    role=tag.get("role"),
                    source="img",
                )
            )
        for candidate in parse_srcset(tag.get("srcset") or ""):
            images.append(
                ImageRef(
                    url=resolve_image_url(base, candidate),
                    alt=tag.get("alt"),
                    aria_hidden=(tag.get("aria-hidden") == "true"),
                    role=tag.get("role"),
                    source="srcset",
                )
            )
    for tag in soup.find_all("link", rel="preload"):
        if tag.get("as") == "image" and tag.get("href"):
            images.append(ImageRef(url=resolve_image_url(base, tag["href"]), source="preload"))
    return tuple(images)


def _collect_anchors(soup, base: str) -> tuple[Anchor, ...]:
    anchors = []
    for tag in soup.find_all("a"):
        href = tag.get("href")
        if not href:
            continue
        anchors.append(
            Anchor(
                href=href,
                url=absolutize(base, href),
                text=tag.get_text(" ", strip=True),
                image_alts=tuple(img.get("alt") or "" for img in tag.find_all("img")),
            )
        )
    return tuple(anchors)


def _collect_jsonld(soup) -> tuple[JsonLdBlock, ...]:
    blocks = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        try:
            blocks.append(JsonLdBlock(raw=raw, data=json.loads(raw)))
        except (ValueError, TypeError) as exc:
            blocks.append(JsonLdBlock(raw=raw, data=None, error=str(exc)))
    return tuple(blocks)


def _collect_subresources(soup, base: str) -> tuple[str, ...]:
    urls = []
    for tag_name, attr in SUBRESOURCE_SELECTORS:
        for tag in soup.find_all(tag_name):
            value = tag.get(attr)
            if value:
                urls.append(absolutize(base, value))
    return tuple(urls)


def parse_blog(
    url: str, slug: str, response: Response, found_in: frozenset[str] = frozenset()
) -> BlogPage:
    soup = BeautifulSoup(response.body or "", "lxml")
    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

    description = soup.find("meta", attrs={"name": "description"})
    robots = [
        (tag.get("content") or "").lower()
        for tag in soup.find_all("meta")
        if (tag.get("name") or "").lower() in ("robots", "googlebot")
    ]
    images = list(_collect_images(soup, base))
    og = _meta_map(soup, "property", "og:")
    twitter = _meta_map(soup, "name", "twitter:")
    for key, source in (("og:image", "og"), ("twitter:image", "twitter")):
        value = og.get(key) or twitter.get(key)
        if value:
            images.append(ImageRef(url=resolve_image_url(base, value), source=source))

    html_tag = soup.find("html")
    return BlogPage(
        url=url,
        slug=slug,
        response=response,
        found_in=found_in,
        title=soup.title.get_text(strip=True) if soup.title else None,
        meta_description=(description.get("content") or "").strip() if description else None,
        canonicals=tuple(
            absolutize(base, tag["href"])
            for tag in soup.find_all("link", rel="canonical")
            if tag.get("href")
        ),
        robots_meta=tuple(robots),
        og=og,
        twitter=twitter,
        html_lang=(html_tag.get("lang") if html_tag else None),
        has_viewport=bool(soup.find("meta", attrs={"name": "viewport"})),
        headings=tuple(
            (int(tag.name[1]), tag.get_text(" ", strip=True))
            for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
        ),
        anchors=_collect_anchors(soup, base),
        images=tuple(images),
        jsonld=_collect_jsonld(soup),
        article_text=_article_text(soup),
        raw_html=response.body or "",
        subresources=_collect_subresources(soup, base),
    )


def parse_listing(html: str, base_url: str, listing_path: str) -> list[str]:
    soup = BeautifulSoup(html or "", "lxml")
    prefix = listing_path.rstrip("/") + "/"
    found: list[str] = []
    for tag in soup.find_all("a"):
        href = (tag.get("href") or "").strip()
        if not href.startswith(prefix) or "?" in href or "#" in href:
            continue
        remainder = href[len(prefix) :].strip("/")
        if not remainder or "/" in remainder:
            continue
        if remainder in LISTING_EXCLUDED_SEGMENTS:
            continue
        url = absolutize(base_url, href)
        if url not in found:
            found.append(url)
    return found


def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_sitemap(xml_text: str) -> tuple[set[str], list[str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return set(), []
    if _localname(root.tag) == "sitemapindex":
        children = [
            loc.text.strip()
            for entry in root
            for loc in entry
            if _localname(loc.tag) == "loc" and loc.text
        ]
        return set(), children
    pages = {
        loc.text.strip()
        for entry in root
        for loc in entry
        if _localname(loc.tag) == "loc" and loc.text
    }
    return pages, []
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd scripts/seo && python3 -m unittest test_seo_parse -v`
Expected: PASS, 24 tests

- [ ] **Step 7: Capture a real page head as an integration fixture**

Run from the repo root — this trims a live blog to its `<head>` so the fixture stays small:

```bash
python3 - <<'PY'
import re, urllib.request
url = "https://www.travelanimator.com/hub/how-to-create-a-travel-animation-for-instagram-stories-reels"
req = urllib.request.Request(url, headers={"User-Agent": "seo-audit/1.0"})
html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
head = html.split("</head>")[0] + "</head><body><article><h1>captured</h1></article></body></html>"
open("scripts/seo/fixtures/real_blog_head.html", "w").write(head)
print("bytes:", len(head))
PY
```

Then confirm the parser handles the real markup:

```bash
cd scripts/seo && python3 -c "
from seo_model import Response
from seo_parse import parse_blog
from seo_testkit import fixture
body = fixture('real_blog_head.html')
url = 'https://www.travelanimator.com/hub/how-to-create-a-travel-animation-for-instagram-stories-reels'
page = parse_blog(url, 'x', Response(url=url, status=200, body=body))
print('title:', page.title)
print('canonical:', page.canonical)
print('jsonld types:', [b.data.get('@type') for b in page.jsonld if b.data])
print('jsonld errors:', [b.error for b in page.jsonld if b.error])
print('origin images:', sum(1 for i in page.images if 'hub.travelanimator.com' in i.url))
"
```
Expected: a real title, a canonical on `www.travelanimator.com`, JSON-LD types including `BlogPosting`, **no** JSON-LD errors, and at least one origin image.

- [ ] **Step 8: Commit**

```bash
git add scripts/seo/seo_parse.py scripts/seo/seo_testkit.py scripts/seo/test_seo_parse.py scripts/seo/fixtures
git commit -m "feat(seo): parse blog HTML, listings, and sitemaps into the domain model"
```

---

## Task 3: HTTP layer with injectable transport

**Files:**
- Create: `scripts/seo/seo_fetch.py`
- Modify: `scripts/seo/test_seo_fetch.py` (append the fetch test classes)

**Interfaces:**
- Consumes: `seo_model` types, `seo_parse.parse_sitemap`.
- Produces:
  - `parse_image_dimensions(data: bytes) -> tuple[int, int] | None`
  - `sniff_image_type(data: bytes) -> str | None`
  - `Transport` protocol: `__call__(method: str, url: str, headers: dict, timeout: int) -> Response`
  - `RequestsTransport(session=None)` — the production transport
  - `Fetcher(transport, timeout=20)` with:
    - `.get(url: str) -> Response`
    - `.get_many(urls: list[str], concurrency: int) -> dict[str, Response]`
    - `.verify(url: str, *, want_dimensions: bool = False) -> UrlStatus`
    - `.verify_many(urls: set[str], concurrency: int, dimension_urls: set[str] = frozenset()) -> dict[str, UrlStatus]`
    - `.fetch_sitemap(url: str) -> tuple[frozenset[str], bool]`
    - `.fetch_cms_posts(origin_host: str, per_page: int) -> CmsSnapshot`

Every network call in the audit goes through `Fetcher`. Tests inject a dict-backed fake transport, so no test touches the network.

- [ ] **Step 1: Write the failing tests**

Append to `scripts/seo/test_seo_fetch.py`:

```python
import json as _json
import struct
import zlib

from seo_fetch import (
    Fetcher,
    parse_image_dimensions,
    sniff_image_type,
)
from seo_model import Response


def png_bytes(width: int, height: int) -> bytes:
    ihdr = struct.pack(">II", width, height) + b"\x08\x06\x00\x00\x00"
    chunk = struct.pack(">I", len(ihdr)) + b"IHDR" + ihdr
    chunk += struct.pack(">I", zlib.crc32(b"IHDR" + ihdr) & 0xFFFFFFFF)
    return b"\x89PNG\r\n\x1a\n" + chunk


def jpeg_bytes(width: int, height: int) -> bytes:
    sof = b"\xff\xc0" + struct.pack(">H", 17) + b"\x08" + struct.pack(">HH", height, width)
    return b"\xff\xd8\xff\xe0" + struct.pack(">H", 16) + b"JFIF\x00" + b"\x00" * 9 + sof


def webp_vp8x_bytes(width: int, height: int) -> bytes:
    payload = b"VP8X" + struct.pack("<I", 10) + b"\x00" * 4
    payload += (width - 1).to_bytes(3, "little") + (height - 1).to_bytes(3, "little")
    return b"RIFF" + struct.pack("<I", len(payload) + 4) + b"WEBP" + payload


class FakeTransport:
    """Dict-backed transport. Keys are (method, url); values are Response or an Exception."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, method, url, headers, timeout):
        self.calls.append((method, url))
        entry = self.responses.get((method, url))
        if entry is None:
            entry = self.responses.get(url)
        if entry is None:
            return Response(url=url, status=404, headers={}, body="")
        if isinstance(entry, Exception):
            raise entry
        return entry


class ImageDimensionTest(unittest.TestCase):
    def test_png(self):
        self.assertEqual(parse_image_dimensions(png_bytes(1200, 630)), (1200, 630))

    def test_jpeg(self):
        self.assertEqual(parse_image_dimensions(jpeg_bytes(800, 400)), (800, 400))

    def test_webp_vp8x(self):
        self.assertEqual(parse_image_dimensions(webp_vp8x_bytes(1600, 900)), (1600, 900))

    def test_unknown_returns_none(self):
        self.assertIsNone(parse_image_dimensions(b"not an image at all"))

    def test_truncated_png_returns_none(self):
        self.assertIsNone(parse_image_dimensions(b"\x89PNG\r\n\x1a\n"))

    def test_sniff_type_ignores_url_extension(self):
        self.assertEqual(sniff_image_type(webp_vp8x_bytes(10, 10)), "image/webp")
        self.assertEqual(sniff_image_type(png_bytes(10, 10)), "image/png")
        self.assertIsNone(sniff_image_type(b"<html>"))


class FetcherVerifyTest(unittest.TestCase):
    def test_head_success(self):
        url = "https://www.travelanimator.com/pricing"
        transport = FakeTransport(
            {("HEAD", url): Response(url=url, status=200, headers={"content-type": "text/html"})}
        )
        status = Fetcher(transport).verify(url)
        self.assertEqual(status.status, 200)
        self.assertTrue(status.verified)
        self.assertFalse(status.is_broken)

    def test_redirect_records_location_and_does_not_follow(self):
        url = "https://hub.travelanimator.com/some-post/"
        target = "https://www.travelanimator.com/hub/some-post/"
        transport = FakeTransport(
            {("HEAD", url): Response(url=url, status=308, headers={"location": target})}
        )
        status = Fetcher(transport).verify(url)
        self.assertTrue(status.is_redirect)
        self.assertEqual(status.location, target)
        self.assertNotIn(("HEAD", target), transport.calls)

    def test_head_405_falls_back_to_get(self):
        url = "https://example.com/asset.png"
        transport = FakeTransport(
            {
                ("HEAD", url): Response(url=url, status=405),
                ("GET", url): Response(url=url, status=200, headers={"content-type": "image/png"}),
            }
        )
        status = Fetcher(transport).verify(url)
        self.assertEqual(status.status, 200)
        self.assertTrue(status.is_image)

    def test_403_is_unverified_not_broken(self):
        url = "https://apps.apple.com/app/id123"
        transport = FakeTransport(
            {("HEAD", url): Response(url=url, status=403), ("GET", url): Response(url=url, status=403)}
        )
        status = Fetcher(transport).verify(url)
        self.assertFalse(status.verified)

    def test_429_is_unverified(self):
        url = "https://www.instagram.com/travelanimator"
        transport = FakeTransport(
            {("HEAD", url): Response(url=url, status=429), ("GET", url): Response(url=url, status=429)}
        )
        self.assertFalse(Fetcher(transport).verify(url).verified)

    def test_connection_error_retries_once_then_unverified(self):
        url = "https://down.example.com/"
        transport = FakeTransport({("HEAD", url): OSError("boom"), ("GET", url): OSError("boom")})
        status = Fetcher(transport).verify(url)
        self.assertEqual(status.status, 0)
        self.assertFalse(status.verified)
        self.assertIsNotNone(status.error)
        self.assertGreaterEqual(len(transport.calls), 2)

    def test_verify_many_caches_repeated_urls(self):
        url = "https://www.travelanimator.com/pricing"
        transport = FakeTransport(
            {("HEAD", url): Response(url=url, status=200, headers={"content-type": "text/html"})}
        )
        fetcher = Fetcher(transport)
        fetcher.verify_many({url}, concurrency=2)
        fetcher.verify_many({url}, concurrency=2)
        self.assertEqual(len([c for c in transport.calls if c[1] == url]), 1)

    def test_verify_with_dimensions_uses_range_request(self):
        url = "https://hub.travelanimator.com/wp-content/uploads/a.png"
        transport = FakeTransport(
            {
                ("HEAD", url): Response(url=url, status=200, headers={"content-type": "image/webp"}),
                ("GET", url): Response(
                    url=url,
                    status=200,
                    headers={"content-type": "image/webp", "content-length": "20000"},
                    content=png_bytes(1200, 630),
                ),
            }
        )
        status = Fetcher(transport).verify(url, want_dimensions=True)
        self.assertEqual((status.width, status.height), (1200, 630))
        self.assertEqual(status.byte_size, 20000)


class FetcherSitemapTest(unittest.TestCase):
    def test_recurses_one_level_through_index(self):
        index_url = "https://www.marineradar.com/sitemap.xml"
        child = "https://www.marineradar.com/sitemap-blogs.xml"
        index_xml = (
            '<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"<sitemap><loc>{child}</loc></sitemap></sitemapindex>"
        )
        child_xml = (
            '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            "<url><loc>https://www.marineradar.com/hub/a</loc></url></urlset>"
        )
        transport = FakeTransport(
            {
                ("GET", index_url): Response(url=index_url, status=200, body=index_xml),
                ("GET", child): Response(url=child, status=200, body=child_xml),
            }
        )
        urls, ok = Fetcher(transport).fetch_sitemap(index_url)
        self.assertTrue(ok)
        self.assertIn("https://www.marineradar.com/hub/a", urls)

    def test_failed_sitemap_reports_not_ok(self):
        url = "https://www.marineradar.com/sitemap.xml"
        transport = FakeTransport({("GET", url): Response(url=url, status=500)})
        urls, ok = Fetcher(transport).fetch_sitemap(url)
        self.assertFalse(ok)
        self.assertEqual(urls, frozenset())


class FetcherCmsTest(unittest.TestCase):
    CMS_URL = (
        "https://hub.travelanimator.com/wp-json/wp/v2/posts"
        "?per_page=20&_fields=slug,date,modified,status&orderby=date&order=desc"
    )

    def test_parses_posts(self):
        payload = _json.dumps(
            [{"slug": "a", "date": "2026-07-30T10:00:00", "modified": "2026-08-01T10:00:00", "status": "publish"}]
        )
        transport = FakeTransport({("GET", self.CMS_URL): Response(url=self.CMS_URL, status=200, body=payload)})
        snapshot = Fetcher(transport).fetch_cms_posts("hub.travelanimator.com", 20)
        self.assertTrue(snapshot.ok)
        self.assertTrue(snapshot.enabled)
        self.assertEqual(snapshot.posts[0].slug, "a")

    def test_non_200_degrades_to_not_ok_with_error(self):
        transport = FakeTransport({("GET", self.CMS_URL): Response(url=self.CMS_URL, status=401)})
        snapshot = Fetcher(transport).fetch_cms_posts("hub.travelanimator.com", 20)
        self.assertFalse(snapshot.ok)
        self.assertTrue(snapshot.enabled)
        self.assertEqual(snapshot.posts, ())
        self.assertIn("401", snapshot.error)

    def test_malformed_json_degrades_without_raising(self):
        transport = FakeTransport({("GET", self.CMS_URL): Response(url=self.CMS_URL, status=200, body="{oops")})
        snapshot = Fetcher(transport).fetch_cms_posts("hub.travelanimator.com", 20)
        self.assertFalse(snapshot.ok)
        self.assertIsNotNone(snapshot.error)


class FetcherGetManyTest(unittest.TestCase):
    def test_one_failure_does_not_cancel_siblings(self):
        good = "https://www.travelanimator.com/hub/a"
        bad = "https://www.travelanimator.com/hub/b"
        transport = FakeTransport(
            {
                ("GET", good): Response(url=good, status=200, body="<html></html>"),
                ("GET", bad): OSError("connection reset"),
            }
        )
        results = Fetcher(transport).get_many([good, bad], concurrency=2)
        self.assertEqual(results[good].status, 200)
        self.assertEqual(results[bad].status, 0)
        self.assertIsNotNone(results[bad].error)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts/seo && python3 -m unittest test_seo_fetch -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_fetch'`

- [ ] **Step 3: Write seo_fetch.py**

Create `scripts/seo/seo_fetch.py`:

```python
"""HTTP layer. The only module that touches the network."""

from __future__ import annotations

import json
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from seo_model import CmsPost, CmsSnapshot, Response, UrlStatus
from seo_parse import parse_sitemap

USER_AGENT = "Lascade-SEO-Audit/1.0 (+https://github.com/Lascade-Co/actions)"
DIMENSION_RANGE = "bytes=0-4095"
UNVERIFIED_STATUSES = frozenset({401, 403, 407, 429})
HEAD_REJECTED_STATUSES = frozenset({400, 403, 405, 501})


def sniff_image_type(data: bytes) -> str | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data.startswith(b"<svg") or b"<svg" in data[:200]:
        return "image/svg+xml"
    return None


def _png_dimensions(data: bytes) -> tuple[int, int] | None:
    if len(data) < 24 or data[12:16] != b"IHDR":
        return None
    return struct.unpack(">II", data[16:24])


def _jpeg_dimensions(data: bytes) -> tuple[int, int] | None:
    index = 2
    while index + 9 < len(data):
        if data[index] != 0xFF:
            index += 1
            continue
        marker = data[index + 1]
        if 0xC0 <= marker <= 0xCF and marker not in (0xC4, 0xC8, 0xCC):
            height, width = struct.unpack(">HH", data[index + 5 : index + 9])
            return width, height
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            index += 2
            continue
        length = struct.unpack(">H", data[index + 2 : index + 4])[0]
        index += 2 + length
    return None


def _webp_dimensions(data: bytes) -> tuple[int, int] | None:
    chunk = data[12:16]
    if chunk == b"VP8X" and len(data) >= 30:
        width = int.from_bytes(data[24:27], "little") + 1
        height = int.from_bytes(data[27:30], "little") + 1
        return width, height
    if chunk == b"VP8 ":
        marker = data.find(b"\x9d\x01\x2a")
        if marker != -1 and len(data) >= marker + 7:
            width = int.from_bytes(data[marker + 3 : marker + 5], "little") & 0x3FFF
            height = int.from_bytes(data[marker + 5 : marker + 7], "little") & 0x3FFF
            return width, height
    if chunk == b"VP8L" and len(data) >= 25:
        bits = int.from_bytes(data[21:25], "little")
        return (bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1
    return None


def parse_image_dimensions(data: bytes) -> tuple[int, int] | None:
    """Dimensions from magic bytes. Never trust the URL extension."""
    kind = sniff_image_type(data)
    if kind == "image/png":
        return _png_dimensions(data)
    if kind == "image/jpeg":
        return _jpeg_dimensions(data)
    if kind == "image/webp":
        return _webp_dimensions(data)
    return None


class RequestsTransport:
    """Production transport. Never follows redirects."""

    def __init__(self, session=None):
        import requests

        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def __call__(self, method: str, url: str, headers: dict, timeout: int) -> Response:
        started = time.monotonic()
        raw = self.session.request(
            method, url, headers=headers, timeout=timeout, allow_redirects=False, stream=False
        )
        ttfb_ms = int((time.monotonic() - started) * 1000)
        body = raw.text if "text" in raw.headers.get("content-type", "") or method == "GET" else ""
        return Response(
            url=url,
            status=raw.status_code,
            headers={k.lower(): v for k, v in raw.headers.items()},
            body=body,
            content=raw.content,
            ttfb_ms=ttfb_ms,
        )


class Fetcher:
    def __init__(self, transport, timeout: int = 20):
        self.transport = transport
        self.timeout = timeout
        self._status_cache: dict[str, UrlStatus] = {}
        self._lock = threading.Lock()

    def _request(self, method: str, url: str, headers: dict | None = None) -> Response:
        attempts = 0
        last_error = None
        while attempts < 2:
            attempts += 1
            try:
                return self.transport(method, url, headers or {}, self.timeout)
            except Exception as exc:  # noqa: BLE001 — any transport error is a fetch failure
                last_error = exc
        return Response(url=url, status=0, error=f"{type(last_error).__name__}: {last_error}")

    def get(self, url: str) -> Response:
        return self._request("GET", url)

    def get_many(self, urls: list[str], concurrency: int) -> dict[str, Response]:
        if not urls:
            return {}
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            return dict(zip(urls, pool.map(self.get, urls)))

    def verify(self, url: str, *, want_dimensions: bool = False) -> UrlStatus:
        with self._lock:
            cached = self._status_cache.get(url)
        if cached is not None and not (want_dimensions and cached.width is None):
            return cached

        response = self._request("HEAD", url)
        if response.status in HEAD_REJECTED_STATUSES or response.status == 0:
            response = self._request("GET", url)

        status = UrlStatus(
            url=url,
            status=response.status,
            content_type=response.header("content-type").split(";")[0].strip(),
            location=response.header("location") or None,
            cache_control=response.header("cache-control") or None,
            verified=response.status not in UNVERIFIED_STATUSES and response.status != 0,
            error=response.error,
        )

        if want_dimensions and status.status == 200:
            probe = self._request("GET", url, {"Range": DIMENSION_RANGE})
            data = probe.content or b""
            sniffed = sniff_image_type(data)
            if sniffed:
                status.content_type = sniffed
            dimensions = parse_image_dimensions(data)
            if dimensions:
                status.width, status.height = dimensions
            length = probe.header("content-length")
            status.byte_size = int(length) if length.isdigit() else None

        with self._lock:
            self._status_cache[url] = status
        return status

    def verify_many(
        self, urls, concurrency: int, dimension_urls=frozenset()
    ) -> dict[str, UrlStatus]:
        targets = [u for u in dict.fromkeys(urls) if u]
        if not targets:
            return {}

        def one(url: str) -> UrlStatus:
            return self.verify(url, want_dimensions=url in dimension_urls)

        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            return dict(zip(targets, pool.map(one, targets)))

    def fetch_sitemap(self, url: str) -> tuple[frozenset[str], bool]:
        response = self.get(url)
        if not response.ok:
            return frozenset(), False
        pages, children = parse_sitemap(response.body)
        for child in children:
            child_response = self.get(child)
            if child_response.ok:
                child_pages, _ = parse_sitemap(child_response.body)
                pages |= child_pages
        return frozenset(pages), True

    def fetch_cms_posts(self, origin_host: str, per_page: int) -> CmsSnapshot:
        url = (
            f"https://{origin_host}/wp-json/wp/v2/posts"
            f"?per_page={per_page}&_fields=slug,date,modified,status&orderby=date&order=desc"
        )
        response = self.get(url)
        if not response.ok:
            detail = response.error or f"HTTP {response.status}"
            return CmsSnapshot(posts=(), ok=False, error=detail, enabled=True)
        try:
            payload = json.loads(response.body)
            posts = tuple(
                CmsPost(
                    slug=item["slug"],
                    date=item.get("date", ""),
                    modified=item.get("modified", ""),
                    status=item.get("status", ""),
                )
                for item in payload
            )
        except (ValueError, TypeError, KeyError) as exc:
            return CmsSnapshot(posts=(), ok=False, error=f"unparseable CMS response: {exc}", enabled=True)
        return CmsSnapshot(posts=posts, ok=True, error=None, enabled=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts/seo && python3 -m unittest test_seo_fetch -v`
Expected: PASS, all classes green

- [ ] **Step 5: Verify against the live origin that dimensions survive content negotiation**

This is the check that matters: the origin serves `image/webp` for a `.png` URL, so a URL-extension-based parser would silently return `None` here.

Run: `cd scripts/seo && python3 -c "
from seo_fetch import Fetcher, RequestsTransport
f = Fetcher(RequestsTransport())
s = f.verify('https://hub.travelanimator.com/wp-content/uploads/2026/07/travelanimator-banner-5761.png', want_dimensions=True)
print('status', s.status, 'sniffed', s.content_type, 'dims', s.width, s.height, 'bytes', s.byte_size)
assert s.status == 200 and s.width and s.height, s
"`
Expected: status 200, a sniffed type (likely `image/webp` despite the `.png` URL), and non-`None` width/height.

- [ ] **Step 6: Verify the CMS call and sitemap recursion live**

Run: `cd scripts/seo && python3 -c "
from seo_fetch import Fetcher, RequestsTransport
f = Fetcher(RequestsTransport())
snap = f.fetch_cms_posts('hub.travelanimator.com', 20)
print('cms ok', snap.ok, 'posts', len(snap.posts), 'first', snap.posts[0].slug if snap.posts else None)
urls, ok = f.fetch_sitemap('https://www.marineradar.com/sitemap.xml')
print('marineradar sitemap ok', ok, 'urls', len(urls))
assert snap.ok and len(snap.posts) >= 10
assert ok and len(urls) > 30, 'sitemap index recursion produced too few URLs'
"`
Expected: `cms ok True`, at least 10 posts, and the marineradar sitemap index resolving to well over 30 URLs (proving one-level recursion works — the index alone would yield 0).

- [ ] **Step 7: Commit**

```bash
git add scripts/seo/seo_fetch.py scripts/seo/test_seo_fetch.py
git commit -m "feat(seo): add HTTP layer with injectable transport and magic-byte image sizing"
```

---

## Task 4: Rule helpers and groups A, B, C

**Files:**
- Create: `scripts/seo/seo_rulekit.py`
- Create: `scripts/seo/seo_checks_abc.py`
- Create: `scripts/seo/test_checks_abc.py`

**Interfaces:**
- Consumes: `seo_model` (all types, `finding`, severities), `seo_parse.normalize_url`.
- Produces from `seo_rulekit`:
  - `host_of(url: str) -> str`
  - `path_of(url: str) -> str`
  - `is_origin_url(site, url) -> bool`
  - `is_asset_url(site, url) -> bool` — origin host **and** path under an asset prefix
  - `is_origin_nonasset(site, url) -> bool`
  - `is_internal(site, url) -> bool` — host equals `canonical_host`
  - `is_same_registrable(site, url) -> bool`
  - `crawlable_urls(page) -> list[tuple[str, str]]` — `(url, position)` pairs
  - `asset_candidates(site, page) -> list[str]`
  - `jsonld_nodes(page) -> list[dict]` — flattens lists and `@graph`
  - `jsonld_of_type(page, *types) -> list[dict]`
  - `same_url(a: str | None, b: str | None) -> bool`
  - `truncate(text: str, limit: int = 300) -> str`
- Produces from `seo_checks_abc`: `BLOG_RULES_ABC: list[Rule]`, `RUN_RULES_ABC: list[Rule]`.

- [ ] **Step 1: Write seo_rulekit.py**

Create `scripts/seo/seo_rulekit.py`:

```python
"""Pure helpers shared by every rule group. No I/O."""

from __future__ import annotations

from urllib.parse import urlparse

from seo_parse import normalize_url


def host_of(url: str) -> str:
    return (urlparse(url).netloc or "").lower()


def path_of(url: str) -> str:
    return urlparse(url).path or "/"


def is_origin_url(site, url: str) -> bool:
    return host_of(url) == site.origin_host


def is_asset_url(site, url: str) -> bool:
    return is_origin_url(site, url) and path_of(url).startswith(site.origin_asset_prefixes)


def is_origin_nonasset(site, url: str) -> bool:
    return is_origin_url(site, url) and not is_asset_url(site, url)


def is_internal(site, url: str) -> bool:
    return host_of(url) == site.canonical_host


def is_same_registrable(site, url: str) -> bool:
    host = host_of(url)
    return host == site.registrable_domain or host.endswith("." + site.registrable_domain)


def crawlable_urls(page) -> list[tuple[str, str]]:
    """Every URL a crawler would treat as a navigational signal, with its position."""
    found: list[tuple[str, str]] = []
    for anchor in page.anchors:
        if anchor.url.startswith(("http://", "https://")):
            found.append((anchor.url, "a[href]"))
    for canonical in page.canonicals:
        found.append((canonical, "canonical"))
    if page.og.get("og:url"):
        found.append((page.og["og:url"], "og:url"))
    for node in jsonld_nodes(page):
        for key in ("url", "@id"):
            value = node.get(key)
            if isinstance(value, str) and value.startswith("http"):
                found.append((value, f"jsonld {node.get('@type', '?')}.{key}"))
        for item in node.get("itemListElement") or []:
            if isinstance(item, dict):
                target = item.get("item")
                if isinstance(target, dict):
                    target = target.get("@id") or target.get("url")
                if isinstance(target, str) and target.startswith("http"):
                    found.append((target, "jsonld BreadcrumbList.item"))
    return found


def asset_candidates(site, page) -> list[str]:
    """Origin asset URLs referenced anywhere on the page, de-duplicated."""
    urls = [image.url for image in page.images] + list(page.subresources)
    return list(dict.fromkeys(url for url in urls if is_asset_url(site, url)))


def jsonld_nodes(page) -> list[dict]:
    nodes: list[dict] = []

    def walk(value):
        if isinstance(value, dict):
            if "@graph" in value:
                walk(value["@graph"])
            nodes.append(value)
        elif isinstance(value, list):
            for entry in value:
                walk(entry)

    for block in page.jsonld:
        if block.data is not None:
            walk(block.data)
    return nodes


def jsonld_of_type(page, *types: str) -> list[dict]:
    wanted = set(types)
    matched = []
    for node in jsonld_nodes(page):
        node_type = node.get("@type")
        node_types = node_type if isinstance(node_type, list) else [node_type]
        if wanted & {t for t in node_types if isinstance(t, str)}:
            matched.append(node)
    return matched


def same_url(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return normalize_url(a) == normalize_url(b)


def truncate(text: str, limit: int = 300) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"
```

- [ ] **Step 2: Write the failing tests for groups A, B, C**

Create `scripts/seo/test_checks_abc.py`:

```python
import unittest

from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    Anchor,
    ImageRef,
    JsonLdBlock,
)
from seo_testkit import BLOG_URL, make_context, make_page, make_response, make_site, make_status

RULES = {rule.id: rule for rule in BLOG_RULES_ABC + RUN_RULES_ABC}
ASSET = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"
ORIGIN_POST = "https://hub.travelanimator.com/some-post/"


def run_rule(rule_id, page=None, *, site=None, urls=None, ctx=None, pages=None):
    rule = RULES[rule_id]
    site = site or make_site()
    ctx = ctx or make_context()
    urls = urls or {}
    if rule.scope == "run":
        return rule.fn(pages if pages is not None else [page or make_page()], site, urls, ctx)
    return rule.fn(page or make_page(), site, urls, ctx)


def anchor(url, text="link"):
    return Anchor(href=url, url=url, text=text)


class GroupATest(unittest.TestCase):
    def test_a1_fires_on_origin_anchor(self):
        page = make_page(anchors=(anchor(ORIGIN_POST),))
        findings = run_rule("A1", page)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)
        self.assertIn("hub.travelanimator.com", findings[0].evidence)

    def test_a1_fires_on_origin_canonical(self):
        page = make_page(canonicals=(ORIGIN_POST,))
        self.assertTrue(run_rule("A1", page))

    def test_a1_silent_when_origin_url_is_an_asset(self):
        page = make_page(anchors=(anchor(ASSET),))
        self.assertEqual(run_rule("A1", page), [])

    def test_a1_silent_on_good_page(self):
        self.assertEqual(run_rule("A1", make_page()), [])

    def test_a2_fires_on_cms_api_url_in_markup(self):
        raw = '<script>{"link":"https://hub.travelanimator.com/wp-json/wp/v2/categories"}</script>'
        findings = run_rule("A2", make_page(raw_html=raw))
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)

    def test_a2_detects_percent_encoded_origin_urls(self):
        raw = "/_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-json%2Fwp%2Fv2%2Fposts"
        self.assertTrue(run_rule("A2", make_page(raw_html=raw)))

    def test_a2_silent_when_only_assets_appear(self):
        raw = f'<img src="{ASSET}"/>'
        self.assertEqual(run_rule("A2", make_page(raw_html=raw)), [])

    def test_a3_fires_when_asset_is_404(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=404)}
        findings = run_rule("A3", page, urls=urls)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_a3_fires_when_asset_is_not_an_image(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=200, content_type="text/html")}
        self.assertTrue(run_rule("A3", page, urls=urls))

    def test_a3_silent_for_healthy_webp_served_from_png_url(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=200, content_type="image/webp")}
        self.assertEqual(run_rule("A3", page, urls=urls), [])

    def test_a4_fires_on_bare_apex(self):
        page = make_page(anchors=(anchor("https://travelanimator.com/pricing"),))
        self.assertEqual(run_rule("A4", page)[0].severity, SEVERITY_ERROR)

    def test_a4_fires_on_http_scheme(self):
        page = make_page(anchors=(anchor("http://www.travelanimator.com/pricing"),))
        self.assertTrue(run_rule("A4", page))

    def test_a4_fires_on_vercel_preview_host(self):
        page = make_page(anchors=(anchor("https://travelanimator-git-main.vercel.app/hub"),))
        self.assertTrue(run_rule("A4", page))

    def test_a4_fires_on_wordpress_query_id(self):
        page = make_page(anchors=(anchor("https://www.travelanimator.com/?p=123"),))
        self.assertTrue(run_rule("A4", page))

    def test_a4_silent_on_allowlisted_subdomain(self):
        page = make_page(anchors=(anchor("https://support.travelanimator.com/x"),))
        self.assertEqual(run_rule("A4", page), [])

    def test_a4_silent_on_third_party_host(self):
        page = make_page(anchors=(anchor("https://apps.apple.com/app/id1"),))
        self.assertEqual(run_rule("A4", page), [])

    def test_a5_reports_allowlisted_subdomain_as_info(self):
        page = make_page(anchors=(anchor("https://support.travelanimator.com/x"),))
        findings = run_rule("A5", page)
        self.assertEqual(findings[0].severity, SEVERITY_INFO)

    def test_a5_silent_without_subdomain_links(self):
        self.assertEqual(run_rule("A5", make_page()), [])


class GroupBTest(unittest.TestCase):
    INTERNAL = "https://www.travelanimator.com/pricing"
    EXTERNAL = "https://apps.apple.com/app/id1"

    def test_b1_fires_on_broken_internal_link(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        urls = {self.INTERNAL: make_status(self.INTERNAL, status=404)}
        self.assertEqual(run_rule("B1", page, urls=urls)[0].severity, SEVERITY_ERROR)

    def test_b1_silent_on_200(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        urls = {self.INTERNAL: make_status(self.INTERNAL)}
        self.assertEqual(run_rule("B1", page, urls=urls), [])

    def test_b2_fires_on_any_3xx(self):
        for code in (301, 302, 308):
            page = make_page(anchors=(anchor(self.INTERNAL),))
            urls = {self.INTERNAL: make_status(self.INTERNAL, status=code, location="/pricing/")}
            findings = run_rule("B2", page, urls=urls)
            self.assertEqual(findings[0].severity, SEVERITY_WARN, code)
            self.assertIn("/pricing/", findings[0].evidence)

    def test_b2_silent_on_direct_hit(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        self.assertEqual(run_rule("B2", page, urls={self.INTERNAL: make_status(self.INTERNAL)}), [])

    def test_b3_fires_on_broken_image(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=500)}
        self.assertEqual(run_rule("B3", page, urls=urls)[0].severity, SEVERITY_ERROR)

    def test_b3_silent_on_healthy_image(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, content_type="image/png")}
        self.assertEqual(run_rule("B3", page, urls=urls), [])

    def test_b4_fires_when_blog_absent_from_sitemap(self):
        ctx = make_context(sitemap_urls=frozenset({"https://www.travelanimator.com/hub/other"}))
        self.assertEqual(run_rule("B4", make_page(), ctx=ctx)[0].severity, SEVERITY_ERROR)

    def test_b4_ignores_trailing_slash_difference(self):
        ctx = make_context(sitemap_urls=frozenset({BLOG_URL + "/"}))
        self.assertEqual(run_rule("B4", make_page(), ctx=ctx), [])

    def test_b4_silent_when_sitemap_unavailable(self):
        ctx = make_context(sitemap_urls=frozenset(), sitemap_ok=False)
        self.assertEqual(run_rule("B4", make_page(), ctx=ctx), [])

    def test_b5_fires_when_not_linked_from_listing(self):
        ctx = make_context(listing_urls=("https://www.travelanimator.com/hub/other",))
        self.assertEqual(run_rule("B5", make_page(), ctx=ctx)[0].severity, SEVERITY_ERROR)

    def test_b5_silent_when_listed(self):
        self.assertEqual(run_rule("B5", make_page(), ctx=make_context()), [])

    def test_b6_reports_dead_external_link_as_info(self):
        page = make_page(anchors=(anchor(self.EXTERNAL),))
        urls = {self.EXTERNAL: make_status(self.EXTERNAL, status=404)}
        self.assertEqual(run_rule("B6", page, urls=urls)[0].severity, SEVERITY_INFO)

    def test_b6_silent_on_unverified_bot_blocked_host(self):
        page = make_page(anchors=(anchor(self.EXTERNAL),))
        urls = {self.EXTERNAL: make_status(self.EXTERNAL, status=403, verified=False)}
        self.assertEqual(run_rule("B6", page, urls=urls), [])

    def test_b6_silent_on_timeout(self):
        page = make_page(anchors=(anchor(self.EXTERNAL),))
        urls = {self.EXTERNAL: make_status(self.EXTERNAL, status=0, verified=False, error="Timeout")}
        self.assertEqual(run_rule("B6", page, urls=urls), [])


class GroupCTest(unittest.TestCase):
    def test_c1_fires_on_noindex_meta(self):
        page = make_page(robots_meta=("noindex, follow",))
        self.assertEqual(run_rule("C1", page)[0].severity, SEVERITY_ERROR)

    def test_c1_fires_on_x_robots_tag_header(self):
        page = make_page(response=make_response(headers={"x-robots-tag": "noindex"}))
        self.assertTrue(run_rule("C1", page))

    def test_c1_silent_on_index_follow(self):
        self.assertEqual(run_rule("C1", make_page(robots_meta=("index, follow",))), [])

    def test_c2_fires_when_canonical_missing(self):
        self.assertEqual(run_rule("C2", make_page(canonicals=()))[0].severity, SEVERITY_ERROR)

    def test_c2_fires_on_duplicate_canonicals(self):
        self.assertTrue(run_rule("C2", make_page(canonicals=(BLOG_URL, BLOG_URL + "?x=1"))))

    def test_c2_fires_when_canonical_points_elsewhere(self):
        page = make_page(canonicals=("https://www.travelanimator.com/hub/other",))
        self.assertTrue(run_rule("C2", page))

    def test_c2_fires_on_non_https_canonical(self):
        page = make_page(canonicals=("http://www.travelanimator.com/hub/good-blog",))
        self.assertTrue(run_rule("C2", page))

    def test_c2_silent_on_self_referencing_canonical(self):
        self.assertEqual(run_rule("C2", make_page()), [])

    def test_c3_fires_when_blog_path_disallowed(self):
        ctx = make_context(robots_txt="User-agent: *\nDisallow: /hub/\nSitemap: https://x/sitemap.xml\n")
        findings = run_rule("C3", ctx=ctx, pages=[make_page()])
        self.assertTrue(any(f.severity == SEVERITY_ERROR for f in findings))

    def test_c3_fires_when_sitemap_directive_absent(self):
        ctx = make_context(robots_txt="User-agent: *\nAllow: /\n")
        findings = run_rule("C3", ctx=ctx, pages=[make_page()])
        self.assertTrue(any("Sitemap" in f.message for f in findings))

    def test_c3_silent_on_healthy_robots(self):
        self.assertEqual(run_rule("C3", ctx=make_context(), pages=[make_page()]), [])

    def test_c3_silent_when_robots_unavailable(self):
        ctx = make_context(robots_txt="", robots_ok=False)
        self.assertEqual(run_rule("C3", ctx=ctx, pages=[make_page()]), [])

    def test_c4_fires_on_thin_body(self):
        page = make_page(article_text="Page not found")
        self.assertEqual(run_rule("C4", page)[0].severity, SEVERITY_ERROR)

    def test_c4_fires_on_not_found_phrasing_despite_length(self):
        page = make_page(article_text="This page could not be found. " + " ".join(["filler"] * 400))
        self.assertTrue(run_rule("C4", page))

    def test_c4_silent_on_real_article(self):
        self.assertEqual(run_rule("C4", make_page()), [])


class RegistryTest(unittest.TestCase):
    def test_all_ids_present_exactly_once(self):
        ids = [rule.id for rule in BLOG_RULES_ABC + RUN_RULES_ABC]
        expected = [f"A{i}" for i in range(1, 6)] + [f"B{i}" for i in range(1, 7)] + [f"C{i}" for i in range(1, 5)]
        self.assertEqual(sorted(ids), sorted(expected))
        self.assertEqual(len(ids), len(set(ids)))

    def test_every_rule_has_group_and_slug(self):
        for rule in BLOG_RULES_ABC + RUN_RULES_ABC:
            self.assertIn(rule.group, ("A", "B", "C"))
            self.assertTrue(rule.slug and "_" not in rule.slug)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd scripts/seo && python3 -m unittest test_checks_abc -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_checks_abc'`

- [ ] **Step 4: Write seo_checks_abc.py**

Create `scripts/seo/seo_checks_abc.py`:

```python
"""Rules A (origin hygiene), B (link integrity), C (indexability). Pure — no I/O."""

from __future__ import annotations

import re
from urllib.parse import unquote, urlparse
from urllib.robotparser import RobotFileParser

from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    Rule,
    finding,
)
from seo_parse import normalize_url
from seo_rulekit import (
    crawlable_urls,
    host_of,
    is_asset_url,
    is_internal,
    is_origin_nonasset,
    is_same_registrable,
    path_of,
    same_url,
    truncate,
)

MAX_FINDINGS_PER_RULE = 10
NOT_FOUND_PHRASES = (
    "page not found",
    "could not be found",
    "404",
    "no longer exists",
    "doesn't exist",
)
BAD_HOST_PATTERNS = (".vercel.app", "localhost", "127.0.0.1", "0.0.0.0")
IP_HOST = re.compile(r"^\d{1,3}(\.\d{1,3}){3}(:\d+)?$")


def _cap(findings: list, rule, extra_note: str) -> list:
    if len(findings) <= MAX_FINDINGS_PER_RULE:
        return findings
    kept = findings[:MAX_FINDINGS_PER_RULE]
    kept.append(
        finding(
            rule,
            kept[0].severity,
            f"{len(findings) - MAX_FINDINGS_PER_RULE} further {extra_note} not listed individually",
            blog_url=kept[0].blog_url,
        )
    )
    return kept


# --- Group A -----------------------------------------------------------------


def check_a1(page, site, urls, ctx):
    seen = {}
    for url, position in crawlable_urls(page):
        if is_origin_nonasset(site, url):
            seen.setdefault(url, position)
    findings = [
        finding(
            A1,
            SEVERITY_ERROR,
            f"origin URL in a crawlable position ({position})",
            blog_url=page.url,
            evidence=url,
        )
        for url, position in seen.items()
    ]
    return _cap(findings, A1, "origin URLs in crawlable positions")


def check_a2(page, site, urls, ctx):
    crawlable = {url for url, _ in crawlable_urls(page) if is_origin_nonasset(site, url)}
    haystack = page.raw_html + "\n" + unquote(page.raw_html)
    pattern = re.compile(re.escape(site.origin_host) + r"(/[^\"'\s)\\<>]*)")
    prefixes: dict[str, int] = {}
    for path in pattern.findall(haystack):
        url = f"https://{site.origin_host}{path}"
        if is_asset_url(site, url) or url in crawlable:
            continue
        prefix = "/".join(path.split("/")[:3])
        prefixes[prefix] = prefixes.get(prefix, 0) + 1
    if not prefixes:
        return []
    total = sum(prefixes.values())
    detail = ", ".join(f"{prefix} ×{count}" for prefix, count in sorted(prefixes.items()))
    return [
        finding(
            A2,
            SEVERITY_WARN,
            f"{total} non-asset origin URL reference(s) present in served markup",
            blog_url=page.url,
            evidence=truncate(detail),
        )
    ]


def check_a3(page, site, urls, ctx):
    findings = []
    for url in dict.fromkeys(
        [image.url for image in page.images] + list(page.subresources)
    ):
        if not is_asset_url(site, url):
            continue
        status = urls.get(url)
        if status is None:
            continue
        if status.status != 200:
            findings.append(
                finding(
                    A3,
                    SEVERITY_ERROR,
                    f"asset URL returned HTTP {status.status or 'no response'}",
                    blog_url=page.url,
                    evidence=url,
                )
            )
        elif not status.is_image:
            findings.append(
                finding(
                    A3,
                    SEVERITY_ERROR,
                    f"asset URL served non-image content type {status.content_type!r}",
                    blog_url=page.url,
                    evidence=url,
                )
            )
    return _cap(findings, A3, "broken asset URLs")


def check_a4(page, site, urls, ctx):
    findings = []
    for url, position in dict.fromkeys(crawlable_urls(page)):
        host = host_of(url)
        parsed = urlparse(url)
        reason = None
        if is_origin_nonasset(site, url) or is_asset_url(site, url):
            continue  # A1/A3 own origin URLs
        if any(bad in host for bad in BAD_HOST_PATTERNS) or IP_HOST.match(host):
            reason = f"non-production host {host!r}"
        elif is_same_registrable(site, url):
            if host != site.canonical_host and host not in site.allowed_subdomains:
                reason = f"host {host!r} is neither the canonical host nor allowlisted"
            elif parsed.scheme != "https":
                reason = f"insecure scheme {parsed.scheme!r} on the canonical domain"
            elif re.search(r"[?&]p=\d+", parsed.query):
                reason = "unrewritten WordPress query id"
        if reason:
            findings.append(
                finding(A4, SEVERITY_ERROR, f"{reason} ({position})", blog_url=page.url, evidence=url)
            )
    return _cap(findings, A4, "off-canonical URLs")


def check_a5(page, site, urls, ctx):
    counts: dict[str, int] = {}
    for url, _ in crawlable_urls(page):
        host = host_of(url)
        if host in site.allowed_subdomains:
            counts[host] = counts.get(host, 0) + 1
    return [
        finding(
            A5,
            SEVERITY_INFO,
            f"{count} link(s) to allowlisted subdomain {host}",
            blog_url=page.url,
        )
        for host, count in sorted(counts.items())
    ]


# --- Group B -----------------------------------------------------------------


def _internal_anchors(page, site):
    for anchor in page.anchors:
        if is_internal(site, anchor.url):
            yield anchor


def check_b1(page, site, urls, ctx):
    findings = []
    for url in dict.fromkeys(a.url for a in _internal_anchors(page, site)):
        status = urls.get(url)
        if status is not None and status.verified and status.status >= 400:
            findings.append(
                finding(
                    B1,
                    SEVERITY_ERROR,
                    f"internal link returned HTTP {status.status}",
                    blog_url=page.url,
                    evidence=url,
                )
            )
    return _cap(findings, B1, "broken internal links")


def check_b2(page, site, urls, ctx):
    findings = []
    for url in dict.fromkeys(a.url for a in _internal_anchors(page, site)):
        status = urls.get(url)
        if status is not None and status.is_redirect:
            findings.append(
                finding(
                    B2,
                    SEVERITY_WARN,
                    f"internal link redirects (HTTP {status.status})",
                    blog_url=page.url,
                    evidence=f"{url} → {status.location or '?'}",
                )
            )
    return _cap(findings, B2, "redirecting internal links")


def check_b3(page, site, urls, ctx):
    findings = []
    for url in dict.fromkeys(image.url for image in page.images):
        status = urls.get(url)
        if status is None or not status.verified:
            continue
        if status.status != 200:
            findings.append(
                finding(
                    B3,
                    SEVERITY_ERROR,
                    f"image returned HTTP {status.status or 'no response'}",
                    blog_url=page.url,
                    evidence=url,
                )
            )
        elif not status.is_image:
            findings.append(
                finding(
                    B3,
                    SEVERITY_ERROR,
                    f"image served content type {status.content_type!r}",
                    blog_url=page.url,
                    evidence=url,
                )
            )
    return _cap(findings, B3, "broken images")


def check_b4(page, site, urls, ctx):
    if not ctx.sitemap_ok:
        return []
    normalized = {normalize_url(url) for url in ctx.sitemap_urls}
    if normalize_url(page.url) in normalized:
        return []
    return [
        finding(
            B4,
            SEVERITY_ERROR,
            "blog URL is absent from sitemap.xml",
            blog_url=page.url,
            evidence=page.url,
        )
    ]


def check_b5(page, site, urls, ctx):
    if not ctx.listing_ok:
        return []
    normalized = {normalize_url(url) for url in ctx.listing_urls}
    if normalize_url(page.url) in normalized:
        return []
    return [
        finding(
            B5,
            SEVERITY_ERROR,
            "blog is not linked from the blog listing",
            blog_url=page.url,
            evidence=page.url,
        )
    ]


def check_b6(page, site, urls, ctx):
    findings = []
    for url in dict.fromkeys(a.url for a in page.anchors):
        if not url.startswith("http") or is_same_registrable(site, url):
            continue
        status = urls.get(url)
        if status is None or not status.verified:
            continue
        if status.status in (404, 410) or status.status >= 500:
            findings.append(
                finding(
                    B6,
                    SEVERITY_INFO,
                    f"external link returned HTTP {status.status}",
                    blog_url=page.url,
                    evidence=url,
                )
            )
    return _cap(findings, B6, "dead external links")


# --- Group C -----------------------------------------------------------------


def check_c1(page, site, urls, ctx):
    sources = list(page.robots_meta) + [page.response.header("x-robots-tag").lower()]
    for value in sources:
        if "noindex" in value:
            return [
                finding(
                    C1,
                    SEVERITY_ERROR,
                    "page is marked noindex",
                    blog_url=page.url,
                    evidence=truncate(value),
                )
            ]
    return []


def check_c2(page, site, urls, ctx):
    if not page.canonicals:
        return [finding(C2, SEVERITY_ERROR, "no canonical link element", blog_url=page.url)]
    if len(page.canonicals) > 1:
        return [
            finding(
                C2,
                SEVERITY_ERROR,
                f"{len(page.canonicals)} canonical elements — there must be exactly one",
                blog_url=page.url,
                evidence=truncate(", ".join(page.canonicals)),
            )
        ]
    canonical = page.canonicals[0]
    parsed = urlparse(canonical)
    if parsed.scheme != "https":
        return [
            finding(
                C2, SEVERITY_ERROR, f"canonical is not https ({parsed.scheme!r})", blog_url=page.url, evidence=canonical
            )
        ]
    if parsed.netloc.lower() != site.canonical_host:
        return [
            finding(
                C2,
                SEVERITY_ERROR,
                f"canonical points at {parsed.netloc!r}, not the canonical host",
                blog_url=page.url,
                evidence=canonical,
            )
        ]
    if not same_url(canonical, page.url):
        return [
            finding(
                C2,
                SEVERITY_ERROR,
                "canonical is not self-referencing",
                blog_url=page.url,
                evidence=f"{page.url} → {canonical}",
            )
        ]
    return []


def check_c3(pages, site, urls, ctx):
    if not ctx.robots_ok or not ctx.robots_txt:
        return []
    findings = []
    parser = RobotFileParser()
    parser.parse(ctx.robots_txt.splitlines())
    for page in pages:
        for agent in ("*", "Googlebot"):
            if not parser.can_fetch(agent, page.url):
                findings.append(
                    finding(
                        C3,
                        SEVERITY_ERROR,
                        f"robots.txt disallows {agent} from crawling this blog",
                        blog_url=page.url,
                        evidence=path_of(page.url),
                    )
                )
                break
    if "sitemap:" not in ctx.robots_txt.lower():
        findings.append(
            finding(C3, SEVERITY_ERROR, "robots.txt declares no Sitemap directive")
        )
    return findings


def check_c4(page, site, urls, ctx):
    minimum = site.threshold("soft_404_word_count")
    lowered = page.article_text.lower()
    if page.word_count < minimum:
        return [
            finding(
                C4,
                SEVERITY_ERROR,
                f"only {page.word_count} words of article text (soft 404 below {minimum})",
                blog_url=page.url,
                evidence=truncate(page.article_text, 120),
            )
        ]
    for phrase in NOT_FOUND_PHRASES:
        if phrase in lowered[:400]:
            return [
                finding(
                    C4,
                    SEVERITY_ERROR,
                    f"body opens with not-found phrasing ({phrase!r})",
                    blog_url=page.url,
                    evidence=truncate(page.article_text, 160),
                )
            ]
    return []


A1 = Rule("A1", "origin-link-in-crawlable-position", "A", check_a1)
A2 = Rule("A2", "origin-nonasset-in-html", "A", check_a2)
A3 = Rule("A3", "origin-asset-status", "A", check_a3)
A4 = Rule("A4", "crawlable-host-not-canonical", "A", check_a4)
A5 = Rule("A5", "allowlisted-subdomain-link", "A", check_a5)
B1 = Rule("B1", "internal-link-broken", "B", check_b1)
B2 = Rule("B2", "internal-link-redirects", "B", check_b2)
B3 = Rule("B3", "image-broken", "B", check_b3)
B4 = Rule("B4", "blog-missing-from-sitemap", "B", check_b4)
B5 = Rule("B5", "blog-not-linked-from-listing", "B", check_b5)
B6 = Rule("B6", "external-link-unreachable", "B", check_b6)
C1 = Rule("C1", "noindex-present", "C", check_c1)
C2 = Rule("C2", "canonical-invalid", "C", check_c2)
C3 = Rule("C3", "robots-txt-disallows", "C", check_c3, scope="run")
C4 = Rule("C4", "soft-404", "C", check_c4)

BLOG_RULES_ABC = [A1, A2, A3, A4, A5, B1, B2, B3, B4, B5, B6, C1, C2, C4]
RUN_RULES_ABC = [C3]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd scripts/seo && python3 -m unittest test_checks_abc -v`
Expected: PASS. If `test_a4_fires_on_http_scheme` fails, check ordering in `check_a4` — the host equality branch must be evaluated before the scheme branch so an `http://www.` URL reports the scheme, not the host.

- [ ] **Step 6: Verify no I/O crept into the pure modules**

Run: `cd scripts/seo && ! grep -nE "^(import|from) (requests|urllib\.request|socket|os)\b" seo_checks_abc.py seo_rulekit.py && echo "PURE OK"`
Expected: `PURE OK`

- [ ] **Step 7: Commit**

```bash
git add scripts/seo/seo_rulekit.py scripts/seo/seo_checks_abc.py scripts/seo/test_checks_abc.py
git commit -m "feat(seo): add origin hygiene, link integrity, and indexability rules"
```

---

## Task 5: Rule groups D, E, F  *(parallel with Tasks 6 and 7)*

**Files:**
- Create: `scripts/seo/seo_checks_def.py`
- Create: `scripts/seo/test_checks_def.py`

**Interfaces:**
- Consumes: `seo_model`, `seo_rulekit` (`jsonld_of_type`, `jsonld_nodes`, `same_url`, `truncate`), `seo_parse.normalize_url`.
- Produces: `BLOG_RULES_DEF: list[Rule]`, `RUN_RULES_DEF: list[Rule]`.

- [ ] **Step 1: Write the failing tests**

Create `scripts/seo/test_checks_def.py`:

```python
import unittest

from seo_checks_def import BLOG_RULES_DEF, RUN_RULES_DEF
from seo_model import SEVERITY_ERROR, SEVERITY_WARN, ImageRef, JsonLdBlock
from seo_testkit import BLOG_URL, make_context, make_page, make_site, make_status

RULES = {rule.id: rule for rule in BLOG_RULES_DEF + RUN_RULES_DEF}
ASSET = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"

ARTICLE_LD = {
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    "headline": "How to Create a Travel Animation for Instagram",
    "description": "Step by step.",
    "url": BLOG_URL,
    "image": ASSET,
    "datePublished": "2026-07-30T10:00:00+00:00",
    "dateModified": "2026-08-01T10:00:00+00:00",
    "author": {"@type": "Person", "name": "Jaseel"},
    "publisher": {"@type": "Organization", "name": "Travel Animator"},
}
BREADCRUMB_LD = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://www.travelanimator.com"},
        {"@type": "ListItem", "position": 2, "name": "Hub", "item": "https://www.travelanimator.com/hub"},
    ],
}
FAQ_LD = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "mainEntity": [
        {
            "@type": "Question",
            "name": "Is TravelAnimator free to use?",
            "acceptedAnswer": {"@type": "Answer", "text": "Yes."},
        }
    ],
}


def blocks(*payloads):
    return tuple(JsonLdBlock(raw="{}", data=payload) for payload in payloads)


def schema_page(**over):
    defaults = {
        "jsonld": blocks(ARTICLE_LD, BREADCRUMB_LD, FAQ_LD),
        "article_text": "Is TravelAnimator free to use? Yes. " + " ".join(["word"] * 400),
    }
    defaults.update(over)
    return make_page(**defaults)


def run_rule(rule_id, page=None, *, site=None, urls=None, ctx=None, pages=None):
    rule = RULES[rule_id]
    site = site or make_site()
    ctx = ctx or make_context()
    urls = urls or {}
    if rule.scope == "run":
        return rule.fn(pages if pages is not None else [page or make_page()], site, urls, ctx)
    return rule.fn(page or make_page(), site, urls, ctx)


class GroupDTest(unittest.TestCase):
    def test_d1_error_when_title_missing(self):
        self.assertEqual(run_rule("D1", make_page(title=None))[0].severity, SEVERITY_ERROR)

    def test_d1_warn_when_title_too_long(self):
        page = make_page(title="x" * 95)
        self.assertEqual(run_rule("D1", page)[0].severity, SEVERITY_WARN)

    def test_d1_warn_when_title_too_short(self):
        self.assertEqual(run_rule("D1", make_page(title="Short"))[0].severity, SEVERITY_WARN)

    def test_d1_silent_on_good_title(self):
        self.assertEqual(run_rule("D1", make_page()), [])

    def test_d2_error_when_description_missing(self):
        self.assertEqual(run_rule("D2", make_page(meta_description=None))[0].severity, SEVERITY_ERROR)

    def test_d2_warn_when_description_short(self):
        self.assertEqual(run_rule("D2", make_page(meta_description="Too short."))[0].severity, SEVERITY_WARN)

    def test_d2_silent_on_good_description(self):
        self.assertEqual(run_rule("D2", make_page()), [])

    def test_d3_fires_when_no_h1(self):
        self.assertEqual(run_rule("D3", make_page(headings=((2, "Sub"),)))[0].severity, SEVERITY_ERROR)

    def test_d3_fires_on_multiple_h1(self):
        self.assertTrue(run_rule("D3", make_page(headings=((1, "One"), (1, "Two")))))

    def test_d3_fires_on_empty_h1(self):
        self.assertTrue(run_rule("D3", make_page(headings=((1, "   "),))))

    def test_d3_silent_on_single_h1(self):
        self.assertEqual(run_rule("D3", make_page()), [])

    def test_d4_fires_on_skipped_level(self):
        page = make_page(headings=((1, "Title"), (2, "Section"), (4, "Deep")))
        self.assertEqual(run_rule("D4", page)[0].severity, SEVERITY_WARN)

    def test_d4_fires_on_empty_heading(self):
        page = make_page(headings=((1, "Title"), (2, "")))
        self.assertTrue(run_rule("D4", page))

    def test_d4_silent_on_clean_hierarchy(self):
        page = make_page(headings=((1, "Title"), (2, "Section"), (3, "Step"), (2, "Next")))
        self.assertEqual(run_rule("D4", page), [])

    def test_d5_fires_on_thin_content(self):
        page = make_page(article_text=" ".join(["word"] * 120))
        self.assertEqual(run_rule("D5", page)[0].severity, SEVERITY_WARN)

    def test_d5_silent_above_threshold(self):
        self.assertEqual(run_rule("D5", make_page()), [])

    def test_d6_error_when_alt_attribute_absent(self):
        page = make_page(images=(ImageRef(url=ASSET, alt=None, source="img"),))
        self.assertEqual(run_rule("D6", page)[0].severity, SEVERITY_ERROR)

    def test_d6_warn_when_alt_looks_like_a_filename(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="travelanimator-banner-5761.png", source="img"),))
        self.assertEqual(run_rule("D6", page)[0].severity, SEVERITY_WARN)

    def test_d6_warn_on_empty_alt_without_decorative_flag(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="", source="img"),))
        self.assertEqual(run_rule("D6", page)[0].severity, SEVERITY_WARN)

    def test_d6_silent_on_decorative_image(self):
        page = make_page(images=(ImageRef(url="/rule.svg", alt="", aria_hidden=True, source="img"),))
        self.assertEqual(run_rule("D6", page), [])

    def test_d6_ignores_meta_sourced_images(self):
        page = make_page(images=(ImageRef(url=ASSET, alt=None, source="og"),))
        self.assertEqual(run_rule("D6", page), [])

    def test_d6_silent_on_descriptive_alt(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="A route animation for Stories", source="img"),))
        self.assertEqual(run_rule("D6", page), [])

    def test_d7_fires_on_duplicate_titles(self):
        pages = [make_page(url=BLOG_URL), make_page(url=BLOG_URL + "-two")]
        findings = run_rule("D7", pages=pages)
        self.assertTrue(any(f.severity == SEVERITY_ERROR for f in findings))
        self.assertTrue(any("title" in f.message for f in findings))

    def test_d7_fires_on_duplicate_h1(self):
        pages = [
            make_page(url="https://www.travelanimator.com/hub/a", title="Title A", meta_description="A" * 100),
            make_page(url="https://www.travelanimator.com/hub/b", title="Title B", meta_description="B" * 100),
        ]
        findings = run_rule("D7", pages=pages)
        self.assertTrue(any("H1" in f.message for f in findings))

    def test_d7_silent_when_all_unique(self):
        pages = [
            make_page(
                url="https://www.travelanimator.com/hub/a",
                title="Title A is long enough here",
                meta_description="A" * 100,
                headings=((1, "Heading A"),),
            ),
            make_page(
                url="https://www.travelanimator.com/hub/b",
                title="Title B is long enough here",
                meta_description="B" * 100,
                headings=((1, "Heading B"),),
            ),
        ]
        self.assertEqual(run_rule("D7", pages=pages), [])

    def test_d8_fires_without_lang(self):
        self.assertEqual(run_rule("D8", make_page(html_lang=None))[0].severity, SEVERITY_WARN)

    def test_d8_fires_without_viewport(self):
        self.assertTrue(run_rule("D8", make_page(has_viewport=False)))

    def test_d8_silent_when_both_present(self):
        self.assertEqual(run_rule("D8", make_page()), [])


class GroupETest(unittest.TestCase):
    def test_e1_fires_on_unparseable_block(self):
        page = make_page(jsonld=(JsonLdBlock(raw="{oops", data=None, error="Expecting value"),))
        self.assertEqual(run_rule("E1", page)[0].severity, SEVERITY_ERROR)

    def test_e1_fires_when_context_or_type_missing(self):
        page = make_page(jsonld=(JsonLdBlock(raw="{}", data={"headline": "x"}),))
        self.assertTrue(run_rule("E1", page))

    def test_e1_silent_on_valid_blocks(self):
        self.assertEqual(run_rule("E1", schema_page()), [])

    def test_e2_fires_when_no_article_schema(self):
        page = schema_page(jsonld=blocks(BREADCRUMB_LD))
        self.assertEqual(run_rule("E2", page)[0].severity, SEVERITY_ERROR)

    def test_e2_fires_on_missing_required_property(self):
        incomplete = {k: v for k, v in ARTICLE_LD.items() if k != "dateModified"}
        page = schema_page(jsonld=blocks(incomplete, BREADCRUMB_LD, FAQ_LD))
        findings = run_rule("E2", page)
        self.assertIn("dateModified", findings[0].message)

    def test_e2_silent_on_complete_article(self):
        self.assertEqual(run_rule("E2", schema_page()), [])

    def test_e3_fires_when_breadcrumb_absent(self):
        page = schema_page(jsonld=blocks(ARTICLE_LD, FAQ_LD))
        self.assertEqual(run_rule("E3", page)[0].severity, SEVERITY_ERROR)

    def test_e3_fires_on_offsite_breadcrumb_item(self):
        bad = {
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://hub.travelanimator.com/"}
            ],
        }
        page = schema_page(jsonld=blocks(ARTICLE_LD, bad, FAQ_LD))
        self.assertTrue(run_rule("E3", page))

    def test_e3_fires_when_breadcrumb_item_is_broken(self):
        target = "https://www.travelanimator.com/hub"
        urls = {target: make_status(target, status=404)}
        self.assertTrue(run_rule("E3", schema_page(), urls=urls))

    def test_e3_silent_on_healthy_breadcrumb(self):
        urls = {
            "https://www.travelanimator.com": make_status("https://www.travelanimator.com"),
            "https://www.travelanimator.com/hub": make_status("https://www.travelanimator.com/hub"),
        }
        self.assertEqual(run_rule("E3", schema_page(), urls=urls), [])

    def test_e4_fires_when_faq_question_absent_from_body(self):
        page = schema_page(article_text=" ".join(["unrelated"] * 400))
        self.assertEqual(run_rule("E4", page)[0].severity, SEVERITY_ERROR)

    def test_e4_silent_when_question_visible(self):
        self.assertEqual(run_rule("E4", schema_page()), [])

    def test_e4_silent_without_faq_schema(self):
        self.assertEqual(run_rule("E4", schema_page(jsonld=blocks(ARTICLE_LD, BREADCRUMB_LD))), [])

    def test_e5_fires_when_schema_url_differs_from_canonical(self):
        divergent = dict(ARTICLE_LD, url="https://www.travelanimator.com/hub/other")
        page = schema_page(jsonld=blocks(divergent, BREADCRUMB_LD, FAQ_LD))
        self.assertEqual(run_rule("E5", page)[0].severity, SEVERITY_ERROR)

    def test_e5_silent_when_matching(self):
        self.assertEqual(run_rule("E5", schema_page()), [])

    def test_e6_fires_on_future_date_published(self):
        future = dict(ARTICLE_LD, datePublished="2099-01-01T00:00:00+00:00")
        page = schema_page(jsonld=blocks(future, BREADCRUMB_LD, FAQ_LD))
        self.assertEqual(run_rule("E6", page)[0].severity, SEVERITY_ERROR)

    def test_e6_fires_when_modified_precedes_published(self):
        inverted = dict(ARTICLE_LD, dateModified="2026-07-01T00:00:00+00:00")
        page = schema_page(jsonld=blocks(inverted, BREADCRUMB_LD, FAQ_LD))
        self.assertTrue(run_rule("E6", page))

    def test_e6_fires_on_unparseable_date(self):
        broken = dict(ARTICLE_LD, datePublished="last Tuesday")
        page = schema_page(jsonld=blocks(broken, BREADCRUMB_LD, FAQ_LD))
        self.assertTrue(run_rule("E6", page))

    def test_e6_silent_on_valid_dates(self):
        self.assertEqual(run_rule("E6", schema_page()), [])


class GroupFTest(unittest.TestCase):
    def test_f1_fires_on_missing_og_property(self):
        page = make_page(og={k: v for k, v in make_page().og.items() if k != "og:image"})
        findings = run_rule("F1", page)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)
        self.assertIn("og:image", findings[0].message)

    def test_f1_fires_when_og_type_is_not_article(self):
        page = make_page(og=dict(make_page().og, **{"og:type": "website"}))
        self.assertTrue(run_rule("F1", page))

    def test_f1_silent_when_complete(self):
        self.assertEqual(run_rule("F1", make_page()), [])

    def test_f2_fires_when_og_url_differs_from_canonical(self):
        page = make_page(og=dict(make_page().og, **{"og:url": "https://www.travelanimator.com/hub/other"}))
        self.assertEqual(run_rule("F2", page)[0].severity, SEVERITY_ERROR)

    def test_f2_silent_when_matching(self):
        self.assertEqual(run_rule("F2", make_page()), [])

    def test_f3_fires_without_twitter_card(self):
        page = make_page(twitter={"twitter:image": ASSET})
        self.assertEqual(run_rule("F3", page)[0].severity, SEVERITY_WARN)

    def test_f3_silent_when_complete(self):
        self.assertEqual(run_rule("F3", make_page()), [])

    def test_f4_fires_when_og_image_too_small(self):
        urls = {ASSET: make_status(ASSET, content_type="image/webp", width=600, height=315, byte_size=1000)}
        findings = run_rule("F4", make_page(), urls=urls)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)
        self.assertIn("600", findings[0].message)

    def test_f4_fires_when_og_image_too_large_in_bytes(self):
        urls = {
            ASSET: make_status(
                ASSET, content_type="image/webp", width=1600, height=900, byte_size=9 * 1024 * 1024
            )
        }
        self.assertTrue(run_rule("F4", make_page(), urls=urls))

    def test_f4_fires_when_dimensions_unreadable(self):
        urls = {ASSET: make_status(ASSET, content_type="image/webp", width=None, height=None)}
        self.assertTrue(run_rule("F4", make_page(), urls=urls))

    def test_f4_silent_on_suitable_image(self):
        urls = {
            ASSET: make_status(ASSET, content_type="image/webp", width=1600, height=900, byte_size=200000)
        }
        self.assertEqual(run_rule("F4", make_page(), urls=urls), [])


class RegistryTest(unittest.TestCase):
    def test_ids_present_exactly_once(self):
        ids = sorted(rule.id for rule in BLOG_RULES_DEF + RUN_RULES_DEF)
        expected = sorted(
            [f"D{i}" for i in range(1, 9)] + [f"E{i}" for i in range(1, 7)] + [f"F{i}" for i in range(1, 5)]
        )
        self.assertEqual(ids, expected)

    def test_d7_is_run_scoped(self):
        self.assertEqual({r.id for r in RUN_RULES_DEF}, {"D7"})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts/seo && python3 -m unittest test_checks_def -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_checks_def'`

- [ ] **Step 3: Write seo_checks_def.py**

Create `scripts/seo/seo_checks_def.py`:

```python
"""Rules D (on-page), E (structured data), F (social). Pure — no I/O."""

from __future__ import annotations

import re
from datetime import datetime, timezone

from seo_model import SEVERITY_ERROR, SEVERITY_WARN, Rule, finding
from seo_rulekit import host_of, jsonld_of_type, same_url, truncate

ARTICLE_TYPES = ("BlogPosting", "Article", "NewsArticle")
ARTICLE_REQUIRED = ("headline", "image", "datePublished", "dateModified", "author", "publisher")
OG_REQUIRED = ("og:title", "og:description", "og:url", "og:image", "og:type", "og:site_name")
FILENAME_ALT = re.compile(r"^[\w\-. ]+\.(png|jpe?g|webp|gif|svg|avif)$", re.IGNORECASE)
FILENAME_ISH_ALT = re.compile(r"^(img|dsc|image|photo|banner|screenshot)[-_ ]?\d+", re.IGNORECASE)
CONTENT_IMAGE_SOURCES = ("img", "srcset")


def _parse_date(value):
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# --- Group D -----------------------------------------------------------------


def check_d1(page, site, urls, ctx):
    if not page.title:
        return [finding(D1, SEVERITY_ERROR, "no <title> element", blog_url=page.url)]
    length = len(page.title)
    low, high = site.threshold("title_min"), site.threshold("title_max")
    if length < low or length > high:
        return [
            finding(
                D1,
                SEVERITY_WARN,
                f"title is {length} chars (target {low}–{high})",
                blog_url=page.url,
                evidence=truncate(page.title),
            )
        ]
    return []


def check_d2(page, site, urls, ctx):
    if not page.meta_description:
        return [finding(D2, SEVERITY_ERROR, "no meta description", blog_url=page.url)]
    length = len(page.meta_description)
    low, high = site.threshold("description_min"), site.threshold("description_max")
    if length < low or length > high:
        return [
            finding(
                D2,
                SEVERITY_WARN,
                f"meta description is {length} chars (target {low}–{high})",
                blog_url=page.url,
                evidence=truncate(page.meta_description),
            )
        ]
    return []


def check_d3(page, site, urls, ctx):
    h1s = page.h1s
    if not h1s:
        return [finding(D3, SEVERITY_ERROR, "no <h1> element", blog_url=page.url)]
    if len(h1s) > 1:
        return [
            finding(
                D3,
                SEVERITY_ERROR,
                f"{len(h1s)} <h1> elements — there must be exactly one",
                blog_url=page.url,
                evidence=truncate(" | ".join(h1s)),
            )
        ]
    if not h1s[0].strip():
        return [finding(D3, SEVERITY_ERROR, "the <h1> is empty", blog_url=page.url)]
    return []


def check_d4(page, site, urls, ctx):
    findings = []
    previous = None
    for level, text in page.headings:
        if not text.strip():
            findings.append(
                finding(D4, SEVERITY_WARN, f"empty <h{level}> element", blog_url=page.url)
            )
        if previous is not None and level > previous + 1:
            findings.append(
                finding(
                    D4,
                    SEVERITY_WARN,
                    f"heading level jumps from h{previous} to h{level}",
                    blog_url=page.url,
                    evidence=truncate(text),
                )
            )
        previous = level
    return findings


def check_d5(page, site, urls, ctx):
    minimum = site.threshold("word_count_min")
    if page.word_count >= minimum:
        return []
    return [
        finding(
            D5,
            SEVERITY_WARN,
            f"{page.word_count} words of article text (minimum {minimum})",
            blog_url=page.url,
        )
    ]


def check_d6(page, site, urls, ctx):
    findings = []
    for image in page.images:
        if image.source not in CONTENT_IMAGE_SOURCES:
            continue
        if image.alt is None:
            findings.append(
                finding(D6, SEVERITY_ERROR, "content image has no alt attribute", blog_url=page.url, evidence=image.url)
            )
        elif image.alt == "":
            if not (image.aria_hidden or image.role == "presentation"):
                findings.append(
                    finding(
                        D6,
                        SEVERITY_WARN,
                        "empty alt without aria-hidden or role=presentation",
                        blog_url=page.url,
                        evidence=image.url,
                    )
                )
        elif FILENAME_ALT.match(image.alt) or FILENAME_ISH_ALT.match(image.alt):
            findings.append(
                finding(
                    D6,
                    SEVERITY_WARN,
                    f"alt text looks like a filename ({image.alt!r})",
                    blog_url=page.url,
                    evidence=image.url,
                )
            )
    return findings


def check_d7(pages, site, urls, ctx):
    findings = []
    fields = (
        ("title", lambda page: page.title),
        ("meta description", lambda page: page.meta_description),
        ("H1", lambda page: page.h1s[0] if page.h1s else None),
    )
    for label, getter in fields:
        groups: dict[str, list[str]] = {}
        for page in pages:
            value = (getter(page) or "").strip()
            if value:
                groups.setdefault(value, []).append(page.url)
        for value, blog_urls in groups.items():
            if len(blog_urls) > 1:
                findings.append(
                    finding(
                        D7,
                        SEVERITY_ERROR,
                        f"{len(blog_urls)} blogs share the same {label}",
                        evidence=truncate(f"{value} — {', '.join(blog_urls)}"),
                    )
                )
    return findings


def check_d8(page, site, urls, ctx):
    missing = []
    if not page.html_lang:
        missing.append("<html lang>")
    if not page.has_viewport:
        missing.append("meta[name=viewport]")
    if not missing:
        return []
    return [finding(D8, SEVERITY_WARN, f"missing {' and '.join(missing)}", blog_url=page.url)]


# --- Group E -----------------------------------------------------------------


def check_e1(page, site, urls, ctx):
    findings = []
    for block in page.jsonld:
        if block.error is not None or block.data is None:
            findings.append(
                finding(
                    E1,
                    SEVERITY_ERROR,
                    f"JSON-LD block failed to parse ({block.error})",
                    blog_url=page.url,
                    evidence=truncate(block.raw, 160),
                )
            )
            continue
        candidates = block.data if isinstance(block.data, list) else [block.data]
        for entry in candidates:
            if not isinstance(entry, dict):
                continue
            if "@context" not in entry or "@type" not in entry:
                findings.append(
                    finding(
                        E1,
                        SEVERITY_ERROR,
                        "JSON-LD block is missing @context or @type",
                        blog_url=page.url,
                        evidence=truncate(block.raw, 160),
                    )
                )
    return findings


def check_e2(page, site, urls, ctx):
    articles = jsonld_of_type(page, *ARTICLE_TYPES)
    if not articles:
        return [
            finding(E2, SEVERITY_ERROR, "no BlogPosting or Article schema", blog_url=page.url)
        ]
    article = articles[0]
    missing = [key for key in ARTICLE_REQUIRED if not article.get(key)]
    if missing:
        return [
            finding(
                E2,
                SEVERITY_ERROR,
                f"article schema is missing {', '.join(missing)}",
                blog_url=page.url,
                evidence=truncate(str(sorted(article.keys()))),
            )
        ]
    return []


def check_e3(page, site, urls, ctx):
    crumbs = jsonld_of_type(page, "BreadcrumbList")
    if not crumbs:
        return [finding(E3, SEVERITY_ERROR, "no BreadcrumbList schema", blog_url=page.url)]
    findings = []
    for crumb in crumbs:
        for element in crumb.get("itemListElement") or []:
            if not isinstance(element, dict):
                continue
            target = element.get("item")
            if isinstance(target, dict):
                target = target.get("@id") or target.get("url")
            if not isinstance(target, str) or not target.startswith("http"):
                continue
            if host_of(target) != site.canonical_host:
                findings.append(
                    finding(
                        E3,
                        SEVERITY_ERROR,
                        f"breadcrumb item is off the canonical host ({host_of(target)})",
                        blog_url=page.url,
                        evidence=target,
                    )
                )
                continue
            status = urls.get(target)
            if status is not None and status.verified and status.status != 200:
                findings.append(
                    finding(
                        E3,
                        SEVERITY_ERROR,
                        f"breadcrumb item returned HTTP {status.status}",
                        blog_url=page.url,
                        evidence=target,
                    )
                )
    return findings


def check_e4(page, site, urls, ctx):
    faqs = jsonld_of_type(page, "FAQPage")
    if not faqs:
        return []
    body = " ".join(page.article_text.lower().split())
    findings = []
    for faq in faqs:
        for entry in faq.get("mainEntity") or []:
            if not isinstance(entry, dict):
                continue
            question = (entry.get("name") or "").strip()
            if question and " ".join(question.lower().split()) not in body:
                findings.append(
                    finding(
                        E4,
                        SEVERITY_ERROR,
                        "FAQPage question text is not visible in the rendered body",
                        blog_url=page.url,
                        evidence=truncate(question),
                    )
                )
    return findings


def check_e5(page, site, urls, ctx):
    articles = jsonld_of_type(page, *ARTICLE_TYPES)
    if not articles or not page.canonical:
        return []
    article = articles[0]
    for key in ("url", "@id"):
        value = article.get(key)
        if isinstance(value, str) and value and not same_url(value, page.canonical):
            return [
                finding(
                    E5,
                    SEVERITY_ERROR,
                    f"article schema {key} does not match the canonical",
                    blog_url=page.url,
                    evidence=f"{value} ≠ {page.canonical}",
                )
            ]
    return []


def check_e6(page, site, urls, ctx):
    articles = jsonld_of_type(page, *ARTICLE_TYPES)
    if not articles:
        return []
    article = articles[0]
    published_raw = article.get("datePublished")
    modified_raw = article.get("dateModified")
    published = _parse_date(published_raw)
    modified = _parse_date(modified_raw)
    if published_raw and published is None:
        return [
            finding(E6, SEVERITY_ERROR, "datePublished is unparseable", blog_url=page.url, evidence=str(published_raw))
        ]
    if modified_raw and modified is None:
        return [
            finding(E6, SEVERITY_ERROR, "dateModified is unparseable", blog_url=page.url, evidence=str(modified_raw))
        ]
    now = datetime.now(timezone.utc)
    if published and published > now:
        return [
            finding(
                E6, SEVERITY_ERROR, "datePublished is in the future", blog_url=page.url, evidence=str(published_raw)
            )
        ]
    if published and modified and modified < published:
        return [
            finding(
                E6,
                SEVERITY_ERROR,
                "dateModified precedes datePublished",
                blog_url=page.url,
                evidence=f"{modified_raw} < {published_raw}",
            )
        ]
    return []


# --- Group F -----------------------------------------------------------------


def check_f1(page, site, urls, ctx):
    missing = [key for key in OG_REQUIRED if not page.og.get(key)]
    if missing:
        return [
            finding(F1, SEVERITY_WARN, f"missing Open Graph tags: {', '.join(missing)}", blog_url=page.url)
        ]
    if page.og.get("og:type") != "article":
        return [
            finding(
                F1,
                SEVERITY_WARN,
                f"og:type is {page.og['og:type']!r}, expected 'article'",
                blog_url=page.url,
            )
        ]
    return []


def check_f2(page, site, urls, ctx):
    og_url = page.og.get("og:url")
    if not og_url or not page.canonical:
        return []
    if same_url(og_url, page.canonical):
        return []
    return [
        finding(
            F2,
            SEVERITY_ERROR,
            "og:url does not match the canonical",
            blog_url=page.url,
            evidence=f"{og_url} ≠ {page.canonical}",
        )
    ]


def check_f3(page, site, urls, ctx):
    missing = [key for key in ("twitter:card", "twitter:image") if not page.twitter.get(key)]
    if not missing:
        return []
    return [finding(F3, SEVERITY_WARN, f"missing Twitter tags: {', '.join(missing)}", blog_url=page.url)]


def check_f4(page, site, urls, ctx):
    og_image = page.og.get("og:image")
    if not og_image:
        return []
    status = urls.get(og_image)
    if status is None or status.status != 200:
        return []
    min_width = site.threshold("og_image_min_width")
    min_height = site.threshold("og_image_min_height")
    max_bytes = site.threshold("og_image_max_bytes")
    if status.width is None or status.height is None:
        return [
            finding(
                F4,
                SEVERITY_WARN,
                f"og:image dimensions unreadable (content type {status.content_type!r})",
                blog_url=page.url,
                evidence=og_image,
            )
        ]
    if status.width < min_width or status.height < min_height:
        return [
            finding(
                F4,
                SEVERITY_WARN,
                f"og:image is {status.width}×{status.height}, below {min_width}×{min_height}",
                blog_url=page.url,
                evidence=og_image,
            )
        ]
    if status.byte_size and status.byte_size > max_bytes:
        return [
            finding(
                F4,
                SEVERITY_WARN,
                f"og:image is {status.byte_size // 1024} KB, above {max_bytes // 1024} KB",
                blog_url=page.url,
                evidence=og_image,
            )
        ]
    return []


D1 = Rule("D1", "title-invalid", "D", check_d1)
D2 = Rule("D2", "meta-description-invalid", "D", check_d2)
D3 = Rule("D3", "h1-invalid", "D", check_d3)
D4 = Rule("D4", "heading-hierarchy", "D", check_d4)
D5 = Rule("D5", "thin-content", "D", check_d5)
D6 = Rule("D6", "image-alt-missing", "D", check_d6)
D7 = Rule("D7", "duplicate-metadata", "D", check_d7, scope="run")
D8 = Rule("D8", "document-meta-missing", "D", check_d8)
E1 = Rule("E1", "jsonld-unparseable", "E", check_e1)
E2 = Rule("E2", "article-schema-incomplete", "E", check_e2)
E3 = Rule("E3", "breadcrumb-invalid", "E", check_e3)
E4 = Rule("E4", "faq-schema-unsupported", "E", check_e4)
E5 = Rule("E5", "schema-url-mismatch", "E", check_e5)
E6 = Rule("E6", "schema-date-invalid", "E", check_e6)
F1 = Rule("F1", "og-incomplete", "F", check_f1)
F2 = Rule("F2", "og-url-mismatch", "F", check_f2)
F3 = Rule("F3", "twitter-incomplete", "F", check_f3)
F4 = Rule("F4", "og-image-unsuitable", "F", check_f4)

BLOG_RULES_DEF = [D1, D2, D3, D4, D5, D6, D8, E1, E2, E3, E4, E5, E6, F1, F2, F3, F4]
RUN_RULES_DEF = [D7]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts/seo && python3 -m unittest test_checks_def -v`
Expected: PASS

- [ ] **Step 5: Verify purity**

Run: `cd scripts/seo && ! grep -nE "^(import|from) (requests|urllib\.request|socket|os)\b" seo_checks_def.py && echo "PURE OK"`
Expected: `PURE OK`

- [ ] **Step 6: Commit**

```bash
git add scripts/seo/seo_checks_def.py scripts/seo/test_checks_def.py
git commit -m "feat(seo): add on-page, structured data, and social rules"
```

---

## Task 6: Rule groups G, H, I  *(parallel with Tasks 5 and 7)*

**Files:**
- Create: `scripts/seo/seo_checks_ghi.py`
- Create: `scripts/seo/test_checks_ghi.py`

**Interfaces:**
- Consumes: `seo_model`, `seo_rulekit`, `seo_parse.slug_from_url`.
- Produces: `BLOG_RULES_GHI: list[Rule]`, `RUN_RULES_GHI: list[Rule]`.

Group I is the one that reads a second source of truth ([ADR 0004](../../adr/0004-blog-seo-audit-reads-the-cms-api.md)). The failure mode to engineer against is an unreachable CMS producing an empty post list that then looks like "every blog is a zombie" — `I3` must be silent whenever `cms.ok` is false.

- [ ] **Step 1: Write the failing tests**

Create `scripts/seo/test_checks_ghi.py`:

```python
import unittest
from datetime import datetime, timedelta, timezone

from seo_checks_ghi import BLOG_RULES_GHI, RUN_RULES_GHI
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    Anchor,
    CmsPost,
    CmsSnapshot,
    ImageRef,
    JsonLdBlock,
)
from seo_testkit import BLOG_URL, make_context, make_page, make_response, make_site, make_status

RULES = {rule.id: rule for rule in BLOG_RULES_GHI + RUN_RULES_GHI}
ASSET = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"


def article_block(modified: str):
    return (
        JsonLdBlock(
            raw="{}",
            data={
                "@context": "https://schema.org",
                "@type": "BlogPosting",
                "headline": "x",
                "dateModified": modified,
            },
        ),
    )


def run_rule(rule_id, page=None, *, site=None, urls=None, ctx=None, pages=None):
    rule = RULES[rule_id]
    site = site or make_site()
    ctx = ctx or make_context()
    urls = urls or {}
    if rule.scope == "run":
        return rule.fn(pages if pages is not None else [page or make_page()], site, urls, ctx)
    return rule.fn(page or make_page(), site, urls, ctx)


class GroupGTest(unittest.TestCase):
    def test_g1_fires_on_http_subresource(self):
        page = make_page(subresources=("http://cdn.example.com/a.js",))
        self.assertEqual(run_rule("G1", page)[0].severity, SEVERITY_ERROR)

    def test_g1_silent_on_https_and_relative(self):
        page = make_page(subresources=("https://cdn.example.com/a.js", "/local.css"))
        self.assertEqual(run_rule("G1", page), [])

    def test_g2_fires_on_oversized_html(self):
        page = make_page(response=make_response(body="x" * (600 * 1024)))
        self.assertEqual(run_rule("G2", page)[0].severity, SEVERITY_WARN)

    def test_g2_silent_on_normal_html(self):
        page = make_page(response=make_response(body="x" * 1000))
        self.assertEqual(run_rule("G2", page), [])

    def test_g3_fires_when_asset_lacks_cache_control(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, content_type="image/webp", cache_control=None)}
        self.assertEqual(run_rule("G3", page, urls=urls)[0].severity, SEVERITY_WARN)

    def test_g3_silent_when_cache_control_present(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, content_type="image/webp", cache_control="public, max-age=31536000")}
        self.assertEqual(run_rule("G3", page, urls=urls), [])

    def test_g4_fires_on_generic_anchor_text(self):
        page = make_page(
            anchors=(Anchor(href="/pricing", url="https://www.travelanimator.com/pricing", text="click here"),)
        )
        self.assertEqual(run_rule("G4", page)[0].severity, SEVERITY_WARN)

    def test_g4_fires_on_empty_anchor_without_image_alt(self):
        page = make_page(
            anchors=(Anchor(href="/pricing", url="https://www.travelanimator.com/pricing", text="", image_alts=("",)),)
        )
        self.assertTrue(run_rule("G4", page))

    def test_g4_silent_when_image_only_anchor_has_alt(self):
        page = make_page(
            anchors=(
                Anchor(
                    href="/pricing",
                    url="https://www.travelanimator.com/pricing",
                    text="",
                    image_alts=("Pricing plans",),
                ),
            )
        )
        self.assertEqual(run_rule("G4", page), [])

    def test_g4_silent_on_descriptive_text(self):
        page = make_page(
            anchors=(Anchor(href="/pricing", url="https://www.travelanimator.com/pricing", text="pricing page"),)
        )
        self.assertEqual(run_rule("G4", page), [])

    def test_g4_ignores_external_anchors(self):
        page = make_page(anchors=(Anchor(href="https://x.com/a", url="https://x.com/a", text="click here"),))
        self.assertEqual(run_rule("G4", page), [])

    def test_g5_reports_slow_ttfb_as_info_only(self):
        page = make_page(response=make_response(ttfb_ms=4000))
        findings = run_rule("G5", page)
        self.assertEqual(findings[0].severity, SEVERITY_INFO)

    def test_g5_silent_when_fast(self):
        self.assertEqual(run_rule("G5", make_page(response=make_response(ttfb_ms=200))), [])


class GroupHTest(unittest.TestCase):
    def test_h1_fires_on_non_200_blog(self):
        page = make_page(response=make_response(status=404))
        self.assertEqual(run_rule("H1", page)[0].severity, SEVERITY_ERROR)

    def test_h1_fires_on_transport_failure(self):
        page = make_page(response=make_response(status=0, error="Timeout"))
        findings = run_rule("H1", page)
        self.assertIn("Timeout", findings[0].evidence)

    def test_h1_silent_on_200(self):
        self.assertEqual(run_rule("H1", make_page()), [])

    def test_h2_fires_when_fewer_blogs_than_configured(self):
        ctx = make_context(listing_urls=(BLOG_URL,))
        findings = run_rule("H2", pages=[make_page()], ctx=ctx, site=make_site(blog_count=10))
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)
        self.assertIn("1", findings[0].message)

    def test_h2_silent_when_enough_blogs_found(self):
        urls = tuple(f"https://www.travelanimator.com/hub/b{i}" for i in range(10))
        ctx = make_context(listing_urls=urls)
        self.assertEqual(run_rule("H2", pages=[make_page()], ctx=ctx, site=make_site(blog_count=10)), [])

    def test_h2_silent_when_listing_unavailable(self):
        ctx = make_context(listing_urls=(), listing_ok=False)
        self.assertEqual(run_rule("H2", pages=[make_page()], ctx=ctx), [])

    def test_h3_is_registered_for_the_orchestrator_to_emit(self):
        self.assertIn("H3", RULES)
        self.assertEqual(RULES["H3"].slug, "harness-error")


class GroupITest(unittest.TestCase):
    def cms(self, posts=(), *, ok=True, enabled=True, error=None):
        return CmsSnapshot(posts=tuple(posts), ok=ok, error=error, enabled=enabled)

    def test_i1_fires_when_published_post_is_missing_from_www(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="never-rendered", status="publish")]))
        page = make_page(url="https://www.travelanimator.com/hub/never-rendered", slug="never-rendered",
                         response=make_response(status=404), found_in=frozenset({"cms"}))
        findings = run_rule("I1", pages=[page], ctx=ctx)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_i1_fires_when_published_post_is_absent_from_listing(self):
        ctx = make_context(
            listing_urls=("https://www.travelanimator.com/hub/other",),
            cms=self.cms([CmsPost(slug="good-blog", status="publish")]),
        )
        page = make_page(found_in=frozenset({"cms"}))
        self.assertTrue(run_rule("I1", pages=[page], ctx=ctx))

    def test_i1_silent_for_draft_posts(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="draft-post", status="draft")]))
        self.assertEqual(run_rule("I1", pages=[make_page()], ctx=ctx), [])

    def test_i1_silent_when_healthy(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish")]))
        self.assertEqual(run_rule("I1", pages=[make_page()], ctx=ctx), [])

    def test_i2_fires_when_render_is_stale(self):
        now = datetime.now(timezone.utc)
        cms_modified = now.strftime("%Y-%m-%dT%H:%M:%S")
        rendered = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish", modified=cms_modified)]))
        page = make_page(jsonld=article_block(rendered))
        findings = run_rule("I2", pages=[page], ctx=ctx)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)

    def test_i2_silent_within_tolerance(self):
        now = datetime.now(timezone.utc)
        stamp = now.strftime("%Y-%m-%dT%H:%M:%S")
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish", modified=stamp)]))
        page = make_page(jsonld=article_block(stamp + "+00:00"))
        self.assertEqual(run_rule("I2", pages=[page], ctx=ctx), [])

    def test_i3_fires_on_zombie_page(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="something-else", status="publish")]))
        findings = run_rule("I3", pages=[make_page()], ctx=ctx)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_i3_fires_when_cms_status_is_not_publish(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="draft")]))
        self.assertTrue(run_rule("I3", pages=[make_page()], ctx=ctx))

    def test_i3_silent_when_cms_unreachable(self):
        """The critical case: an empty post list must not read as every blog being a zombie."""
        ctx = make_context(cms=self.cms([], ok=False, error="HTTP 500"))
        self.assertEqual(run_rule("I3", pages=[make_page()], ctx=ctx), [])

    def test_i1_silent_when_cms_unreachable(self):
        ctx = make_context(cms=self.cms([], ok=False, error="HTTP 500"))
        self.assertEqual(run_rule("I1", pages=[make_page()], ctx=ctx), [])

    def test_i4_reports_exactly_one_info_when_cms_unreachable(self):
        ctx = make_context(cms=self.cms([], ok=False, error="HTTP 401"))
        findings = run_rule("I4", pages=[make_page()], ctx=ctx)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_INFO)
        self.assertIn("401", findings[0].evidence)

    def test_i4_silent_when_cms_healthy(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish")]))
        self.assertEqual(run_rule("I4", pages=[make_page()], ctx=ctx), [])

    def test_group_i_silent_entirely_when_disabled(self):
        ctx = make_context(cms=self.cms([], ok=False, enabled=False))
        for rule_id in ("I1", "I2", "I3", "I4"):
            self.assertEqual(run_rule(rule_id, pages=[make_page()], ctx=ctx), [], rule_id)


class RegistryTest(unittest.TestCase):
    def test_ids_present_exactly_once(self):
        ids = sorted(rule.id for rule in BLOG_RULES_GHI + RUN_RULES_GHI)
        expected = sorted(
            [f"G{i}" for i in range(1, 6)] + [f"H{i}" for i in range(1, 4)] + [f"I{i}" for i in range(1, 5)]
        )
        self.assertEqual(ids, expected)

    def test_parity_and_discovery_rules_are_run_scoped(self):
        self.assertEqual({r.id for r in RUN_RULES_GHI}, {"H2", "H3", "I1", "I2", "I3", "I4"})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts/seo && python3 -m unittest test_checks_ghi -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_checks_ghi'`

- [ ] **Step 3: Write seo_checks_ghi.py**

Create `scripts/seo/seo_checks_ghi.py`:

```python
"""Rules G (technical), H (harness), I (CMS parity). Pure — no I/O."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from seo_model import (
    GENERIC_ANCHOR_TEXT,
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    Rule,
    finding,
)
from seo_parse import normalize_url
from seo_rulekit import is_asset_url, is_internal, jsonld_of_type, truncate

ARTICLE_TYPES = ("BlogPosting", "Article", "NewsArticle")


def _parse_date(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# --- Group G -----------------------------------------------------------------


def check_g1(page, site, urls, ctx):
    offenders = [url for url in dict.fromkeys(page.subresources) if url.startswith("http://")]
    if not offenders:
        return []
    return [
        finding(
            G1,
            SEVERITY_ERROR,
            f"{len(offenders)} subresource(s) loaded over plain http",
            blog_url=page.url,
            evidence=truncate(", ".join(offenders)),
        )
    ]


def check_g2(page, site, urls, ctx):
    size = len(page.response.body.encode("utf-8", "ignore"))
    limit = site.threshold("page_weight_bytes")
    if size <= limit:
        return []
    return [
        finding(
            G2,
            SEVERITY_WARN,
            f"served HTML is {size // 1024} KB (limit {limit // 1024} KB)",
            blog_url=page.url,
        )
    ]


def check_g3(page, site, urls, ctx):
    findings = []
    for url in dict.fromkeys(image.url for image in page.images):
        if not is_asset_url(site, url):
            continue
        status = urls.get(url)
        if status is not None and status.status == 200 and not status.cache_control:
            findings.append(
                finding(
                    G3,
                    SEVERITY_WARN,
                    "asset response carries no Cache-Control header",
                    blog_url=page.url,
                    evidence=url,
                )
            )
    return findings


def check_g4(page, site, urls, ctx):
    findings = []
    for anchor in page.anchors:
        if not is_internal(site, anchor.url):
            continue
        text = " ".join(anchor.text.split()).lower()
        if not text:
            if any(alt.strip() for alt in anchor.image_alts):
                continue
            findings.append(
                finding(
                    G4,
                    SEVERITY_WARN,
                    "internal link has no anchor text and no image alt",
                    blog_url=page.url,
                    evidence=anchor.url,
                )
            )
        elif text in GENERIC_ANCHOR_TEXT:
            findings.append(
                finding(
                    G4,
                    SEVERITY_WARN,
                    f"generic anchor text {anchor.text.strip()!r}",
                    blog_url=page.url,
                    evidence=anchor.url,
                )
            )
    return findings


def check_g5(page, site, urls, ctx):
    limit = site.threshold("ttfb_ms")
    if page.response.ttfb_ms <= limit:
        return []
    return [
        finding(
            G5,
            SEVERITY_INFO,
            f"time to first byte {page.response.ttfb_ms} ms (advisory; measured under concurrent load)",
            blog_url=page.url,
        )
    ]


# --- Group H -----------------------------------------------------------------


def check_h1(page, site, urls, ctx):
    if page.response.ok:
        return []
    detail = page.response.error or f"HTTP {page.response.status}"
    return [
        finding(
            H1,
            SEVERITY_ERROR,
            "blog could not be fetched",
            blog_url=page.url,
            evidence=detail,
        )
    ]


def check_h2(pages, site, urls, ctx):
    if not ctx.listing_ok:
        return []
    found = len(ctx.listing_urls)
    if found >= site.blog_count:
        return []
    return [
        finding(
            H2,
            SEVERITY_ERROR,
            f"only {found} blog(s) discoverable on the blog listing, expected {site.blog_count}",
            evidence=site.listing_url,
        )
    ]


def check_h3(pages, site, urls, ctx):
    """Placeholder registration. The orchestrator emits H3 findings directly on exception."""
    return []


# --- Group I -----------------------------------------------------------------


def _parity_enabled(ctx) -> bool:
    return ctx.cms.enabled and ctx.cms.ok


def check_i1(pages, site, urls, ctx):
    if not _parity_enabled(ctx):
        return []
    pages_by_slug = {page.slug: page for page in pages}
    listed = {normalize_url(url) for url in ctx.listing_urls}
    findings = []
    for post in ctx.cms.posts:
        if post.status != "publish":
            continue
        page = pages_by_slug.get(post.slug)
        expected_url = f"{site.base_url}{site.listing_path}/{post.slug}"
        if page is None:
            continue
        if not page.response.ok:
            findings.append(
                finding(
                    I1,
                    SEVERITY_ERROR,
                    f"CMS post is published but {site.canonical_host} returned "
                    f"{page.response.status or 'no response'}",
                    blog_url=page.url,
                    evidence=expected_url,
                )
            )
        elif normalize_url(page.url) not in listed and ctx.listing_ok:
            findings.append(
                finding(
                    I1,
                    SEVERITY_ERROR,
                    "CMS post is published but is absent from the blog listing",
                    blog_url=page.url,
                    evidence=expected_url,
                )
            )
    return findings


def check_i2(pages, site, urls, ctx):
    if not _parity_enabled(ctx):
        return []
    posts = ctx.cms.by_slug()
    tolerance = timedelta(hours=site.threshold("stale_render_hours"))
    findings = []
    for page in pages:
        post = posts.get(page.slug)
        if post is None:
            continue
        cms_modified = _parse_date(post.modified)
        articles = jsonld_of_type(page, *ARTICLE_TYPES)
        rendered = _parse_date(articles[0].get("dateModified")) if articles else None
        if cms_modified is None or rendered is None:
            continue
        if cms_modified - rendered > tolerance:
            drift = cms_modified - rendered
            findings.append(
                finding(
                    I2,
                    SEVERITY_WARN,
                    f"rendered copy is {int(drift.total_seconds() // 3600)} h behind the CMS",
                    blog_url=page.url,
                    evidence=f"CMS {post.modified} vs rendered {articles[0].get('dateModified')}",
                )
            )
    return findings


def check_i3(pages, site, urls, ctx):
    if not _parity_enabled(ctx):
        return []
    posts = ctx.cms.by_slug()
    findings = []
    for page in pages:
        if not page.response.ok:
            continue
        post = posts.get(page.slug)
        if post is None:
            findings.append(
                finding(
                    I3,
                    SEVERITY_ERROR,
                    "blog is live but has no corresponding CMS post",
                    blog_url=page.url,
                    evidence=page.slug,
                )
            )
        elif post.status != "publish":
            findings.append(
                finding(
                    I3,
                    SEVERITY_ERROR,
                    f"blog is live but its CMS status is {post.status!r}",
                    blog_url=page.url,
                    evidence=page.slug,
                )
            )
    return findings


def check_i4(pages, site, urls, ctx):
    if not ctx.cms.enabled or ctx.cms.ok:
        return []
    return [
        finding(
            I4,
            SEVERITY_INFO,
            "CMS parity checks were skipped — the CMS API did not answer",
            evidence=ctx.cms.error or "unknown error",
        )
    ]


G1 = Rule("G1", "mixed-content", "G", check_g1)
G2 = Rule("G2", "page-weight", "G", check_g2)
G3 = Rule("G3", "asset-cache-headers", "G", check_g3)
G4 = Rule("G4", "anchor-text-weak", "G", check_g4)
G5 = Rule("G5", "slow-response", "G", check_g5)
H1 = Rule("H1", "blog-fetch-failed", "H", check_h1)
H2 = Rule("H2", "listing-discovery-short", "H", check_h2, scope="run")
H3 = Rule("H3", "harness-error", "H", check_h3, scope="run")
I1 = Rule("I1", "blog-not-published-on-www", "I", check_i1, scope="run")
I2 = Rule("I2", "stale-render", "I", check_i2, scope="run")
I3 = Rule("I3", "unpublished-still-live", "I", check_i3, scope="run")
I4 = Rule("I4", "cms-parity-skipped", "I", check_i4, scope="run")

BLOG_RULES_GHI = [G1, G2, G3, G4, G5, H1]
RUN_RULES_GHI = [H2, H3, I1, I2, I3, I4]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts/seo && python3 -m unittest test_checks_ghi -v`
Expected: PASS. The `test_i3_silent_when_cms_unreachable` and `test_i1_silent_when_cms_unreachable` cases are the ones that matter most — they prove an unreachable CMS degrades instead of inventing 10 phantom errors.

- [ ] **Step 5: Verify purity**

Run: `cd scripts/seo && ! grep -nE "^(import|from) (requests|urllib\.request|socket|os)\b" seo_checks_ghi.py && echo "PURE OK"`
Expected: `PURE OK`

- [ ] **Step 6: Commit**

```bash
git add scripts/seo/seo_checks_ghi.py scripts/seo/test_checks_ghi.py
git commit -m "feat(seo): add technical, harness, and CMS parity rules"
```

---

## Task 7: Report renderer  *(parallel with Tasks 5 and 6)*

**Files:**
- Create: `scripts/seo/seo_report.py`
- Create: `scripts/seo/test_seo_report.py`

**Interfaces:**
- Consumes: `seo_model` (`Finding`, `BlogPage`, `SiteConfig`, `Rule`, severities).
- Produces:
  - `RunSummary` dataclass: `site, pages, findings, rules, started_at: str, duration_s: float, error: str | None = None`
  - `partition(findings, site) -> tuple[list[Finding], list[Finding]]` — `(active, suppressed)`
  - `counts(findings) -> dict[str, int]` — keys `error`, `warn`, `info`
  - `gate(summary) -> bool` — True when delivery should happen: any **active** `error` or `warn`
  - `render_report(summary: RunSummary) -> str`

Findings are **bare** per the spec — rule ID, slug, blog, message, evidence. No remediation prose, no `wp-admin` links.

- [ ] **Step 1: Write the failing tests**

Create `scripts/seo/test_seo_report.py`:

```python
import re
import unittest

from seo_model import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN, Finding
from seo_report import RunSummary, counts, gate, partition, render_report
from seo_testkit import BLOG_URL, make_page, make_site


def f(rule="A1", severity=SEVERITY_ERROR, message="broken", blog_url=BLOG_URL, evidence="x"):
    return Finding(rule=rule, slug="some-rule", severity=severity, message=message, blog_url=blog_url, evidence=evidence)


def summary(findings=(), *, site=None, error=None):
    from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC

    return RunSummary(
        site=site or make_site(),
        pages=[make_page()],
        findings=list(findings),
        rules=BLOG_RULES_ABC + RUN_RULES_ABC,
        started_at="2026-08-03T06:00:00+00:00",
        duration_s=12.5,
        error=error,
    )


class CountsTest(unittest.TestCase):
    def test_counts_by_severity(self):
        result = counts([f(severity=SEVERITY_ERROR), f(severity=SEVERITY_WARN), f(severity=SEVERITY_INFO), f(severity=SEVERITY_INFO)])
        self.assertEqual(result, {"error": 1, "warn": 1, "info": 2})

    def test_counts_empty(self):
        self.assertEqual(counts([]), {"error": 0, "warn": 0, "info": 0})


class PartitionTest(unittest.TestCase):
    def test_suppressed_rules_are_separated(self):
        site = make_site(suppress=["A2"])
        active, suppressed = partition([f(rule="A1"), f(rule="A2")], site)
        self.assertEqual([x.rule for x in active], ["A1"])
        self.assertEqual([x.rule for x in suppressed], ["A2"])

    def test_nothing_suppressed_by_default(self):
        active, suppressed = partition([f(rule="A1")], make_site())
        self.assertEqual(len(active), 1)
        self.assertEqual(suppressed, [])


class GateTest(unittest.TestCase):
    def test_error_opens_the_gate(self):
        self.assertTrue(gate(summary([f(severity=SEVERITY_ERROR)])))

    def test_warn_opens_the_gate(self):
        self.assertTrue(gate(summary([f(severity=SEVERITY_WARN)])))

    def test_info_never_opens_the_gate(self):
        self.assertFalse(gate(summary([f(severity=SEVERITY_INFO), f(severity=SEVERITY_INFO)])))

    def test_suppressed_error_does_not_open_the_gate(self):
        site = make_site(suppress=["A2"])
        self.assertFalse(gate(summary([f(rule="A2", severity=SEVERITY_ERROR)], site=site)))

    def test_unsuppressed_error_alongside_suppressed_still_opens(self):
        site = make_site(suppress=["A2"])
        findings = [f(rule="A2", severity=SEVERITY_ERROR), f(rule="A1", severity=SEVERITY_ERROR)]
        self.assertTrue(gate(summary(findings, site=site)))

    def test_clean_run_keeps_the_gate_shut(self):
        self.assertFalse(gate(summary([])))

    def test_harness_error_opens_the_gate(self):
        self.assertTrue(gate(summary([], error="ValueError: boom")))


class RenderTest(unittest.TestCase):
    def test_is_self_contained(self):
        html = render_report(summary([f()]))
        self.assertNotIn("http://", html.replace("http://www.w3.org", ""))
        for forbidden in ("<script src", "<link rel=\"stylesheet\"", "@import", "cdn."):
            self.assertNotIn(forbidden, html)

    def test_contains_structural_sections(self):
        html = render_report(summary([f()]))
        for heading in ("Summary", "Rule coverage", "Findings", "Per-blog detail", "Configuration"):
            self.assertIn(heading, html)

    def test_shows_site_label_and_timestamp(self):
        html = render_report(summary([f()]))
        self.assertIn("Travel Animator", html)
        self.assertIn("2026-08-03T06:00:00+00:00", html)

    def test_renders_rule_id_message_and_evidence(self):
        html = render_report(summary([f(rule="B1", message="internal link returned HTTP 404", evidence="/pricing")]))
        self.assertIn("B1", html)
        self.assertIn("internal link returned HTTP 404", html)
        self.assertIn("/pricing", html)

    def test_escapes_html_in_evidence(self):
        html = render_report(summary([f(evidence='<script>alert("x")</script>')]))
        self.assertNotIn("<script>alert", html)
        self.assertIn("&lt;script&gt;", html)

    def test_suppressed_section_appears_only_when_needed(self):
        site = make_site(suppress=["A2"])
        with_suppressed = render_report(summary([f(rule="A2")], site=site))
        self.assertIn("Suppressed", with_suppressed)
        self.assertIn("A2", with_suppressed)
        self.assertNotIn("Suppressed", render_report(summary([f(rule="A1")])))

    def test_clean_run_states_it_is_clean(self):
        html = render_report(summary([]))
        self.assertIn("No findings", html)

    def test_harness_error_is_rendered_with_traceback(self):
        html = render_report(summary([], error="Traceback: ValueError: boom"))
        self.assertIn("Harness error", html)
        self.assertIn("ValueError: boom", html)

    def test_coverage_matrix_has_a_column_per_group_and_row_per_blog(self):
        html = render_report(summary([f(rule="A1")]))
        matrix = re.search(r'<table class="matrix".*?</table>', html, re.S).group(0)
        for group in ("A", "B", "C"):
            self.assertIn(f'<th>{group}</th>', matrix)
        self.assertIn("good-blog", matrix)

    def test_findings_ordered_error_then_warn_then_info(self):
        html = render_report(
            summary([f(rule="Z9", severity=SEVERITY_INFO), f(rule="A1", severity=SEVERITY_ERROR)])
        )
        self.assertLess(html.index("A1"), html.index("Z9"))

    def test_dark_mode_styles_present(self):
        self.assertIn("prefers-color-scheme: dark", render_report(summary([])))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd scripts/seo && python3 -m unittest test_seo_report -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_report'`

- [ ] **Step 3: Write seo_report.py**

Create `scripts/seo/seo_report.py`:

```python
"""Renders findings into one self-contained report.html."""

from __future__ import annotations

from dataclasses import dataclass, field
from html import escape

from seo_model import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN, BlogPage, Finding, Rule, SiteConfig

GROUPS = ("A", "B", "C", "D", "E", "F", "G", "H", "I")

CSS = """
:root {
  --bg: #ffffff; --fg: #14181f; --muted: #5b6472; --line: #e3e7ec;
  --error: #b3261e; --warn: #8a5a00; --info: #2c5aa0; --ok: #1f7a4d;
  --error-bg: #fdecea; --warn-bg: #fdf3e0; --info-bg: #eaf1fb; --ok-bg: #e9f5ee;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #14171c; --fg: #e7eaee; --muted: #9aa3b0; --line: #2a2f38;
    --error: #ff8a80; --warn: #ffcc80; --info: #90caf9; --ok: #81c995;
    --error-bg: #35211f; --warn-bg: #322a1c; --info-bg: #1d2735; --ok-bg: #1d2b23;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 32px 24px 64px; background: var(--bg); color: var(--fg);
  font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
.wrap { max-width: 980px; margin: 0 auto; }
h1 { font-size: 24px; margin: 0 0 4px; }
h2 { font-size: 17px; margin: 40px 0 12px; padding-bottom: 6px; border-bottom: 1px solid var(--line); }
.sub { color: var(--muted); font-size: 13px; margin: 0 0 4px; }
.band { display: flex; gap: 12px; flex-wrap: wrap; margin: 20px 0 0; }
.tile { flex: 1 1 120px; border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; }
.tile .n { font-size: 28px; font-weight: 600; line-height: 1.1; }
.tile .l { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.tile.error .n { color: var(--error); } .tile.warn .n { color: var(--warn); }
.tile.info .n { color: var(--info); } .tile.ok .n { color: var(--ok); }
.verdict { margin: 16px 0 0; padding: 10px 12px; border-radius: 6px; background: var(--ok-bg); color: var(--ok); }
.verdict.bad { background: var(--error-bg); color: var(--error); }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { text-align: left; padding: 7px 9px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { color: var(--muted); font-weight: 600; font-size: 12px; }
table.matrix td.cell { text-align: center; font-weight: 600; width: 44px; }
.cell.error { background: var(--error-bg); color: var(--error); }
.cell.warn { background: var(--warn-bg); color: var(--warn); }
.cell.info { background: var(--info-bg); color: var(--info); }
.cell.ok { background: var(--ok-bg); color: var(--ok); }
.badge { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
.badge.error { background: var(--error-bg); color: var(--error); }
.badge.warn { background: var(--warn-bg); color: var(--warn); }
.badge.info { background: var(--info-bg); color: var(--info); }
code, .ev { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
.ev { color: var(--muted); word-break: break-all; }
details { border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; margin: 8px 0; }
summary { cursor: pointer; font-weight: 600; font-size: 14px; }
.harness { border: 1px solid var(--error); background: var(--error-bg); color: var(--error); border-radius: 8px; padding: 12px 14px; margin: 20px 0; }
.harness pre { white-space: pre-wrap; margin: 8px 0 0; }
"""


@dataclass
class RunSummary:
    site: SiteConfig
    pages: list[BlogPage]
    findings: list[Finding]
    rules: list[Rule]
    started_at: str
    duration_s: float
    error: str | None = None


def counts(findings) -> dict[str, int]:
    tally = {SEVERITY_ERROR: 0, SEVERITY_WARN: 0, SEVERITY_INFO: 0}
    for item in findings:
        tally[item.severity] = tally.get(item.severity, 0) + 1
    return tally


def partition(findings, site) -> tuple[list[Finding], list[Finding]]:
    active, suppressed = [], []
    for item in findings:
        (suppressed if site.is_suppressed(item.rule) else active).append(item)
    return active, suppressed


def gate(summary: RunSummary) -> bool:
    if summary.error:
        return True
    active, _ = partition(summary.findings, summary.site)
    tally = counts(active)
    return (tally[SEVERITY_ERROR] + tally[SEVERITY_WARN]) > 0


def _tile(value: int, label: str, kind: str) -> str:
    return f'<div class="tile {kind}"><div class="n">{value}</div><div class="l">{escape(label)}</div></div>'


def _matrix(summary: RunSummary, active: list[Finding]) -> str:
    worst: dict[tuple[str, str], str] = {}
    rank = {SEVERITY_ERROR: 3, SEVERITY_WARN: 2, SEVERITY_INFO: 1}
    for item in active:
        if not item.blog_url:
            continue
        key = (item.blog_url, item.rule[0])
        if rank.get(item.severity, 0) > rank.get(worst.get(key, ""), 0):
            worst[key] = item.severity
    header = "".join(f"<th>{group}</th>" for group in GROUPS)
    rows = []
    for page in summary.pages:
        cells = []
        for group in GROUPS:
            severity = worst.get((page.url, group))
            kind = severity if severity else "ok"
            mark = {"error": "●", "warn": "▲", "info": "·"}.get(severity, "✓")
            cells.append(f'<td class="cell {kind}">{mark}</td>')
        rows.append(f"<tr><td><code>{escape(page.slug)}</code></td>{''.join(cells)}</tr>")
    return (
        '<div class="scroll"><table class="matrix"><thead><tr><th>blog</th>'
        f"{header}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def _finding_rows(findings: list[Finding]) -> str:
    if not findings:
        return "<p>No findings.</p>"
    rows = []
    for item in sorted(findings, key=lambda x: x.sort_key()):
        blog = escape(item.blog_url or "— site level —")
        evidence = f'<div class="ev">{escape(item.evidence)}</div>' if item.evidence else ""
        rows.append(
            "<tr>"
            f'<td><code>{escape(item.rule)}</code></td>'
            f'<td><span class="badge {item.severity}">{item.severity}</span></td>'
            f"<td>{escape(item.message)}<br><span class=\"ev\">{blog}</span>{evidence}</td>"
            f"<td><code>{escape(item.slug)}</code></td>"
            "</tr>"
        )
    return (
        '<div class="scroll"><table><thead><tr><th>rule</th><th>severity</th>'
        f"<th>finding</th><th>slug</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def _per_blog(summary: RunSummary, active: list[Finding]) -> str:
    blocks = []
    for page in summary.pages:
        mine = [item for item in active if item.blog_url == page.url]
        tally = counts(mine)
        label = (
            f"{tally[SEVERITY_ERROR]} errors · {tally[SEVERITY_WARN]} warnings · {tally[SEVERITY_INFO]} info"
            if mine
            else "clean"
        )
        body = _finding_rows(mine) if mine else "<p>No findings.</p>"
        blocks.append(
            f"<details><summary>{escape(page.slug)} — {label}</summary>"
            f'<p class="sub"><code>{escape(page.url)}</code> — HTTP {page.response.status}, '
            f"{page.word_count} words, {page.response.ttfb_ms} ms TTFB</p>{body}</details>"
        )
    return "".join(blocks)


def _config(site) -> str:
    rows = [
        ("canonical host", site.canonical_host),
        ("origin", site.origin_host),
        ("asset prefixes", ", ".join(site.origin_asset_prefixes)),
        ("allowlisted subdomains", ", ".join(sorted(site.allowed_subdomains)) or "none"),
        ("blogs audited", str(site.blog_count)),
        ("CMS parity", "enabled" if site.cms_api else "disabled"),
        ("suppressed rules", ", ".join(sorted(site.suppress)) or "none"),
    ]
    body = "".join(f"<tr><th>{escape(k)}</th><td><code>{escape(v)}</code></td></tr>" for k, v in rows)
    return f'<div class="scroll"><table>{body}</table></div>'


def render_report(summary: RunSummary) -> str:
    active, suppressed = partition(summary.findings, summary.site)
    tally = counts(active)
    bad = (tally[SEVERITY_ERROR] + tally[SEVERITY_WARN]) > 0 or bool(summary.error)
    verdict = (
        f"{tally[SEVERITY_ERROR]} error(s) and {tally[SEVERITY_WARN]} warning(s) need attention."
        if bad
        else "No findings that require attention."
    )
    harness = (
        f'<div class="harness"><strong>Harness error</strong> — the audit did not complete normally.'
        f"<pre>{escape(summary.error)}</pre></div>"
        if summary.error
        else ""
    )
    suppressed_section = ""
    if suppressed:
        by_rule: dict[str, int] = {}
        for item in suppressed:
            by_rule[item.rule] = by_rule.get(item.rule, 0) + 1
        listing = ", ".join(f"{rule} ×{count}" for rule, count in sorted(by_rule.items()))
        suppressed_section = (
            "<h2>Suppressed</h2>"
            f'<p class="sub">Silenced by <code>suppress</code> in <code>data/seo_sites.json</code>: {escape(listing)}. '
            "These findings are reported but never trigger delivery.</p>"
            f"{_finding_rows(suppressed)}"
        )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Blog SEO Audit — {escape(summary.site.label)}</title>
<style>{CSS}</style></head>
<body><div class="wrap">
<h1>Blog SEO Audit — {escape(summary.site.label)}</h1>
<p class="sub">{escape(summary.site.listing_url)}</p>
<p class="sub">Run started {escape(summary.started_at)} · {summary.duration_s:.1f}s · {len(summary.pages)} blogs checked</p>
{harness}
<h2>Summary</h2>
<div class="band">
{_tile(tally[SEVERITY_ERROR], "errors", "error")}
{_tile(tally[SEVERITY_WARN], "warnings", "warn")}
{_tile(tally[SEVERITY_INFO], "info", "info")}
{_tile(len(suppressed), "suppressed", "ok")}
</div>
<p class="verdict{' bad' if bad else ''}">{escape(verdict)}</p>
<h2>Rule coverage</h2>
{_matrix(summary, active)}
<h2>Findings</h2>
{_finding_rows(active)}
{suppressed_section}
<h2>Per-blog detail</h2>
{_per_blog(summary, active)}
<h2>Configuration</h2>
{_config(summary.site)}
</div></body></html>
"""
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts/seo && python3 -m unittest test_seo_report -v`
Expected: PASS

- [ ] **Step 5: Eyeball the rendered report**

Run: `cd scripts/seo && python3 -c "
from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC
from seo_model import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN, Finding
from seo_report import RunSummary, render_report
from seo_testkit import BLOG_URL, make_page, make_site
findings = [
    Finding('A1','origin-link-in-crawlable-position',SEVERITY_ERROR,'origin URL in a crawlable position (a[href])',BLOG_URL,'https://hub.travelanimator.com/some-post/'),
    Finding('A2','origin-nonasset-in-html',SEVERITY_WARN,'216 non-asset origin URL reference(s) in served markup',BLOG_URL,'/wp-json/wp x172'),
    Finding('A5','allowlisted-subdomain-link',SEVERITY_INFO,'2 link(s) to allowlisted subdomain support.travelanimator.com',BLOG_URL,''),
]
s = RunSummary(site=make_site(suppress=['A2']), pages=[make_page()], findings=findings,
               rules=BLOG_RULES_ABC+RUN_RULES_ABC, started_at='2026-08-03T06:00:00+00:00', duration_s=11.4)
open('/tmp/report.html','w').write(render_report(s))
print('written /tmp/report.html')
" && open /tmp/report.html`
Expected: a readable page — summary tiles, a coverage matrix with a red cell in column A, the A1 finding under Findings, and A2 under Suppressed. Confirm it reads well in both light and dark appearance.

- [ ] **Step 6: Commit**

```bash
git add scripts/seo/seo_report.py scripts/seo/test_seo_report.py
git commit -m "feat(seo): render self-contained audit report with severity gate"
```

---

## Task 8: Rule registry and orchestrator

**Files:**
- Create: `scripts/seo/seo_checks.py`
- Create: `scripts/seo/seo_blog_audit.py`
- Create: `scripts/seo/test_seo_audit.py`

**Interfaces:**
- Consumes: every module above.
- Produces from `seo_checks`: `BLOG_RULES`, `RUN_RULES`, `ALL_RULES`, `RULES_BY_ID`.
- Produces from `seo_blog_audit`:
  - `discover(fetcher, site) -> tuple[list[str], SiteContext]`
  - `collect_urls(pages, site) -> tuple[set[str], set[str]]` — `(all_urls, dimension_urls)`
  - `evaluate(pages, site, urls, ctx, concurrency) -> list[Finding]`
  - `audit(site, fetcher) -> RunSummary`
  - `FixtureTransport(directory)` for `--offline`
  - `main(argv=None) -> int` — **always returns 0**

- [ ] **Step 1: Write seo_checks.py**

Create `scripts/seo/seo_checks.py`:

```python
"""Rule registry. Aggregates the three group modules — add new groups here."""

from __future__ import annotations

from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC
from seo_checks_def import BLOG_RULES_DEF, RUN_RULES_DEF
from seo_checks_ghi import BLOG_RULES_GHI, RUN_RULES_GHI

BLOG_RULES = BLOG_RULES_ABC + BLOG_RULES_DEF + BLOG_RULES_GHI
RUN_RULES = RUN_RULES_ABC + RUN_RULES_DEF + RUN_RULES_GHI
ALL_RULES = BLOG_RULES + RUN_RULES
RULES_BY_ID = {rule.id: rule for rule in ALL_RULES}

assert len(RULES_BY_ID) == len(ALL_RULES), "duplicate rule id in the registry"
```

- [ ] **Step 2: Write the failing orchestrator tests**

Create `scripts/seo/test_seo_audit.py`:

```python
import json
import unittest

from seo_blog_audit import audit, collect_urls, discover, evaluate
from seo_checks import ALL_RULES, BLOG_RULES, RULES_BY_ID, RUN_RULES
from seo_fetch import Fetcher
from seo_model import Response
from seo_report import counts, gate, partition
from seo_testkit import fixture, make_page, make_site

LISTING = "https://www.travelanimator.com/hub"
ROBOTS = "https://www.travelanimator.com/robots.txt"
SITEMAP = "https://www.travelanimator.com/sitemap.xml"
CMS = (
    "https://hub.travelanimator.com/wp-json/wp/v2/posts"
    "?per_page=20&_fields=slug,date,modified,status&orderby=date&order=desc"
)


class ScriptedTransport:
    """Serves fixtures for known URLs, 200-empty for anything else, and records calls."""

    def __init__(self, pages: dict, *, fail: set = frozenset()):
        self.pages = pages
        self.fail = set(fail)
        self.calls = []

    def __call__(self, method, url, headers, timeout):
        self.calls.append((method, url))
        if url in self.fail:
            raise OSError("scripted failure")
        if url in self.pages:
            status, body, extra = self.pages[url]
            head = {"content-type": "text/html; charset=utf-8", "cache-control": "public, max-age=60"}
            head.update(extra or {})
            return Response(url=url, status=status, headers=head, body=body, content=body.encode(), ttfb_ms=120)
        return Response(url=url, status=200, headers={"content-type": "text/html"}, body="", ttfb_ms=50)


def scripted(**over):
    blog = fixture("good_blog.html")
    pages = {
        LISTING: (200, fixture("listing.html"), None),
        ROBOTS: (200, "User-agent: *\nAllow: /\nSitemap: " + SITEMAP + "\n", {"content-type": "text/plain"}),
        SITEMAP: (200, fixture("sitemap_child.xml"), {"content-type": "application/xml"}),
        CMS: (200, json.dumps(json.loads(fixture("cms_posts.json"))), {"content-type": "application/json"}),
        "https://www.travelanimator.com/hub/good-blog": (200, blog, None),
        "https://www.travelanimator.com/hub/second-blog": (200, blog, None),
        "https://www.travelanimator.com/hub/third-blog": (200, blog, None),
    }
    pages.update(over)
    return ScriptedTransport(pages)


class RegistryTest(unittest.TestCase):
    def test_all_45_rules_registered(self):
        self.assertEqual(len(ALL_RULES), 45)

    def test_every_documented_id_exists(self):
        expected = (
            [f"A{i}" for i in range(1, 6)]
            + [f"B{i}" for i in range(1, 7)]
            + [f"C{i}" for i in range(1, 5)]
            + [f"D{i}" for i in range(1, 9)]
            + [f"E{i}" for i in range(1, 7)]
            + [f"F{i}" for i in range(1, 5)]
            + [f"G{i}" for i in range(1, 6)]
            + [f"H{i}" for i in range(1, 4)]
            + [f"I{i}" for i in range(1, 5)]
        )
        self.assertEqual(sorted(RULES_BY_ID), sorted(expected))

    def test_scopes_are_valid(self):
        self.assertTrue(all(rule.scope == "blog" for rule in BLOG_RULES))
        self.assertTrue(all(rule.scope == "run" for rule in RUN_RULES))

    def test_slugs_unique(self):
        slugs = [rule.slug for rule in ALL_RULES]
        self.assertEqual(len(slugs), len(set(slugs)))


class DiscoverTest(unittest.TestCase):
    def test_returns_listing_blogs_in_order(self):
        site = make_site(blog_count=3, cms_api=False)
        urls, ctx = discover(Fetcher(scripted()), site)
        self.assertEqual(urls[0], "https://www.travelanimator.com/hub/good-blog")
        self.assertEqual(len(urls), 3)
        self.assertTrue(ctx.listing_ok)
        self.assertTrue(ctx.sitemap_ok)
        self.assertTrue(ctx.robots_ok)

    def test_respects_blog_count(self):
        site = make_site(blog_count=2, cms_api=False)
        urls, _ = discover(Fetcher(scripted()), site)
        self.assertEqual(len(urls), 2)

    def test_cms_disabled_leaves_snapshot_disabled(self):
        site = make_site(cms_api=False)
        _, ctx = discover(Fetcher(scripted()), site)
        self.assertFalse(ctx.cms.enabled)

    def test_union_adds_cms_only_slugs(self):
        site = make_site(blog_count=3, cms_api=True)
        urls, ctx = discover(Fetcher(scripted()), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn("https://www.travelanimator.com/hub/never-rendered-blog", urls)

    def test_listing_failure_is_survivable(self):
        transport = scripted()
        transport.fail.add(LISTING)
        urls, ctx = discover(Fetcher(transport), make_site(cms_api=False))
        self.assertEqual(urls, [])
        self.assertFalse(ctx.listing_ok)


class CollectUrlsTest(unittest.TestCase):
    def test_dimension_urls_are_only_og_images(self):
        page = make_page()
        all_urls, dimension_urls = collect_urls([page], make_site())
        self.assertEqual(dimension_urls, {page.og["og:image"]})
        self.assertIn(page.og["og:image"], all_urls)

    def test_deduplicates_across_blogs(self):
        pages = [make_page(url="https://www.travelanimator.com/hub/a"), make_page(url="https://www.travelanimator.com/hub/b")]
        all_urls, _ = collect_urls(pages, make_site())
        self.assertEqual(len(all_urls), len(set(all_urls)))


class EvaluateTest(unittest.TestCase):
    def test_runs_blog_and_run_scoped_rules(self):
        site = make_site()
        pages = [make_page()]
        findings = evaluate(pages, site, {}, __import__("seo_testkit").make_context(), concurrency=4)
        self.assertIsInstance(findings, list)
        self.assertTrue(all(f.rule in RULES_BY_ID for f in findings))

    def test_a_raising_rule_does_not_abort_the_run(self):
        site = make_site()
        broken = make_page(headings="not-a-tuple-of-tuples")
        findings = evaluate([broken], site, {}, __import__("seo_testkit").make_context(), concurrency=2)
        self.assertTrue(any(f.rule == "H3" for f in findings))


class AuditTest(unittest.TestCase):
    def test_healthy_site_produces_a_summary(self):
        site = make_site(blog_count=3, cms_api=False)
        summary = audit(site, Fetcher(scripted()))
        self.assertEqual(len(summary.pages), 3)
        self.assertIsNone(summary.error)
        self.assertEqual(len(summary.rules), 45)

    def test_broken_blog_is_reported_and_siblings_survive(self):
        transport = scripted()
        transport.fail.add("https://www.travelanimator.com/hub/second-blog")
        summary = audit(make_site(blog_count=3, cms_api=False), Fetcher(transport))
        self.assertEqual(len(summary.pages), 3)
        self.assertTrue(any(f.rule == "H1" for f in summary.findings))
        good = [p for p in summary.pages if p.slug == "good-blog"][0]
        self.assertTrue(good.response.ok)

    def test_suppressed_rule_is_reported_but_does_not_open_the_gate(self):
        transport = scripted(
            **{
                "https://www.travelanimator.com/hub/good-blog": (
                    200,
                    fixture("good_blog.html").replace(
                        "</head>",
                        '<script>{"l":"https://hub.travelanimator.com/wp-json/wp/v2/posts"}</script></head>',
                    ),
                    None,
                )
            }
        )
        site = make_site(blog_count=1, cms_api=False, suppress=["A2"])
        summary = audit(site, Fetcher(transport))
        active, suppressed = partition(summary.findings, site)
        self.assertTrue(any(f.rule == "A2" for f in suppressed))
        self.assertFalse(any(f.rule == "A2" for f in active))

    def test_unreachable_cms_yields_one_info_and_no_parity_errors(self):
        transport = scripted()
        transport.fail.add(CMS)
        site = make_site(blog_count=3, cms_api=True)
        summary = audit(site, Fetcher(transport))
        parity = [f for f in summary.findings if f.rule.startswith("I")]
        self.assertEqual([f.rule for f in parity], ["I4"])
        self.assertEqual(counts(parity)["info"], 1)

    def test_info_only_run_keeps_the_gate_shut(self):
        site = make_site(blog_count=3, cms_api=False)
        summary = audit(site, Fetcher(scripted()))
        info_only = [f for f in summary.findings if f.severity == "info"]
        summary.findings = info_only
        self.assertFalse(gate(summary))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd scripts/seo && python3 -m unittest test_seo_audit -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'seo_blog_audit'`

- [ ] **Step 4: Write seo_blog_audit.py**

Create `scripts/seo/seo_blog_audit.py`:

```python
#!/usr/bin/env python3
"""Blog SEO Audit entry point.

Always exits 0 — findings are reported, never raised. See ADR 0003.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from seo_checks import ALL_RULES, BLOG_RULES, RULES_BY_ID, RUN_RULES
from seo_fetch import Fetcher, RequestsTransport
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    CmsSnapshot,
    Response,
    SiteContext,
    finding,
    load_site_config,
)
from seo_parse import parse_blog, parse_listing, slug_from_url
from seo_report import RunSummary, counts, gate, partition, render_report
from seo_rulekit import is_same_registrable


def discover(fetcher: Fetcher, site) -> tuple[list[str], SiteContext]:
    listing = fetcher.get(site.listing_url)
    listing_urls = parse_listing(listing.body, site.base_url, site.listing_path) if listing.ok else []
    ordered = listing_urls[: site.blog_count]

    cms = CmsSnapshot()
    if site.cms_api:
        cms = fetcher.fetch_cms_posts(site.origin_host, site.blog_count * 2)
        if cms.ok:
            for post in cms.posts[: site.blog_count]:
                url = f"{site.base_url}{site.listing_path}/{post.slug}"
                if url not in ordered:
                    ordered.append(url)

    sitemap_urls, sitemap_ok = fetcher.fetch_sitemap(site.sitemap_url)
    robots = fetcher.get(f"{site.base_url}/robots.txt")

    return ordered, SiteContext(
        listing_urls=tuple(listing_urls),
        listing_ok=listing.ok,
        sitemap_urls=sitemap_urls,
        sitemap_ok=sitemap_ok,
        robots_txt=robots.body if robots.ok else "",
        robots_ok=robots.ok,
        cms=cms,
    )


def fetch_blogs(fetcher: Fetcher, site, urls: list[str], ctx: SiteContext):
    """All blogs fetched concurrently — one worker per blog."""
    listed = set(ctx.listing_urls)
    cms_slugs = {post.slug for post in ctx.cms.posts}

    def one(url: str):
        slug = slug_from_url(url)
        found = set()
        if url in listed:
            found.add("listing")
        if slug in cms_slugs:
            found.add("cms")
        response = fetcher.get(url)
        return parse_blog(url, slug, response, frozenset(found))

    if not urls:
        return []
    with ThreadPoolExecutor(max_workers=max(1, site.threshold("blog_concurrency"))) as pool:
        return list(pool.map(one, urls))


def collect_urls(pages, site) -> tuple[set[str], set[str]]:
    all_urls: set[str] = set()
    dimension_urls: set[str] = set()
    for page in pages:
        for anchor in page.anchors:
            if anchor.url.startswith(("http://", "https://")):
                all_urls.add(anchor.url)
        for image in page.images:
            if image.url.startswith(("http://", "https://")):
                all_urls.add(image.url)
        og_image = page.og.get("og:image")
        if og_image and og_image.startswith("http"):
            dimension_urls.add(og_image)
            all_urls.add(og_image)
        for node in page.jsonld:
            if isinstance(node.data, dict):
                for element in node.data.get("itemListElement") or []:
                    if isinstance(element, dict):
                        target = element.get("item")
                        if isinstance(target, dict):
                            target = target.get("@id") or target.get("url")
                        if isinstance(target, str) and target.startswith("http"):
                            all_urls.add(target)
    return all_urls, dimension_urls


def evaluate(pages, site, urls, ctx, concurrency: int):
    """Blog rules run per blog in parallel; run-scoped rules run once over all blogs."""
    findings = []

    def for_blog(page):
        produced = []
        for rule in BLOG_RULES:
            try:
                produced.extend(rule.fn(page, site, urls, ctx))
            except Exception:  # noqa: BLE001 — a broken rule must not kill the audit
                produced.append(
                    finding(
                        RULES_BY_ID["H3"],
                        SEVERITY_ERROR,
                        f"rule {rule.id} raised while checking this blog",
                        blog_url=page.url,
                        evidence=traceback.format_exc(limit=3),
                    )
                )
        return produced

    if pages:
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            for produced in pool.map(for_blog, pages):
                findings.extend(produced)

    for rule in RUN_RULES:
        try:
            findings.extend(rule.fn(pages, site, urls, ctx))
        except Exception:  # noqa: BLE001
            findings.append(
                finding(
                    RULES_BY_ID["H3"],
                    SEVERITY_ERROR,
                    f"run-scoped rule {rule.id} raised",
                    evidence=traceback.format_exc(limit=3),
                )
            )
    return findings


def audit(site, fetcher: Fetcher) -> RunSummary:
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    urls, ctx = discover(fetcher, site)
    pages = fetch_blogs(fetcher, site, urls, ctx)
    to_verify, dimension_urls = collect_urls(pages, site)
    statuses = fetcher.verify_many(
        to_verify, concurrency=site.threshold("url_concurrency"), dimension_urls=dimension_urls
    )
    findings = evaluate(pages, site, statuses, ctx, site.threshold("blog_concurrency"))
    return RunSummary(
        site=site,
        pages=pages,
        findings=findings,
        rules=ALL_RULES,
        started_at=started_at,
        duration_s=time.monotonic() - started,
    )


class FixtureTransport:
    """Serves saved responses from a directory for --offline runs.

    Files are named by URL with non-alphanumerics replaced by underscores.
    Any URL without a file gets a 200 with an empty body.
    """

    def __init__(self, directory: str):
        self.directory = Path(directory)

    def __call__(self, method, url, headers, timeout):
        name = "".join(ch if ch.isalnum() else "_" for ch in url)[:180]
        path = self.directory / f"{name}.txt"
        if not path.exists():
            return Response(url=url, status=200, headers={"content-type": "text/html"}, body="")
        body = path.read_text(encoding="utf-8")
        return Response(
            url=url,
            status=200,
            headers={"content-type": "text/html", "cache-control": "public, max-age=60"},
            body=body,
            content=body.encode(),
            ttfb_ms=1,
        )


def write_github_output(summary: RunSummary) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    if not target:
        return
    active, suppressed = partition(summary.findings, summary.site)
    tally = counts(active)
    lines = {
        "has_findings": "true" if gate(summary) else "false",
        "error_count": tally[SEVERITY_ERROR],
        "warn_count": tally[SEVERITY_WARN],
        "info_count": tally[SEVERITY_INFO],
        "suppressed_count": len(suppressed),
        "label": summary.site.label,
    }
    with open(target, "a", encoding="utf-8") as handle:
        for key, value in lines.items():
            handle.write(f"{key}={value}\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Audit the newest blogs on a site for SEO defects.")
    parser.add_argument("--site", required=True, help="site name from the config file")
    parser.add_argument("--config", default="seo_sites.json", help="path to seo_sites.json")
    parser.add_argument("--output", default="report.html", help="where to write the report")
    parser.add_argument("--blog-count", type=int, default=None, help="override how many blogs to audit")
    parser.add_argument("--offline", default=None, help="serve responses from this fixture directory")
    args = parser.parse_args(argv)

    summary = None
    try:
        site = load_site_config(args.config, args.site)
        if args.blog_count:
            site = type(site)(**{**site.__dict__, "blog_count": args.blog_count})
        transport = FixtureTransport(args.offline) if args.offline else RequestsTransport()
        fetcher = Fetcher(transport, timeout=site.threshold("request_timeout"))
        summary = audit(site, fetcher)
    except Exception:  # noqa: BLE001 — a crash must still produce a deliverable report
        detail = traceback.format_exc()
        try:
            site = load_site_config(args.config, args.site)
        except Exception:
            site = None
        if site is None:
            sys.stderr.write(detail)
            return 0
        summary = RunSummary(
            site=site,
            pages=[],
            findings=[],
            rules=ALL_RULES,
            started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            duration_s=0.0,
            error=detail,
        )

    Path(args.output).write_text(render_report(summary), encoding="utf-8")
    write_github_output(summary)

    active, suppressed = partition(summary.findings, summary.site)
    tally = counts(active)
    print(
        f"{summary.site.label}: {tally[SEVERITY_ERROR]} errors, {tally[SEVERITY_WARN]} warnings, "
        f"{tally[SEVERITY_INFO]} info, {len(suppressed)} suppressed "
        f"across {len(summary.pages)} blogs → {args.output}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Run the orchestrator tests**

Run: `cd scripts/seo && python3 -m unittest test_seo_audit -v`
Expected: PASS. `test_all_45_rules_registered` is the one that catches a rule accidentally omitted from a group registry.

- [ ] **Step 6: Run the whole suite**

Run: `cd scripts/seo && python3 -m unittest discover -p "test_*.py" -v 2>&1 | tail -20`
Expected: OK, zero failures across all six test modules.

- [ ] **Step 7: Live smoke run against both sites**

Run: `cd scripts/seo && for s in travelanimator marineradar; do python3 seo_blog_audit.py --site "$s" --config ../../data/seo_sites.json --output "/tmp/report-$s.html"; echo "exit=$?"; done`
Expected: both print a counts line, both `exit=0`, and two report files exist. Open both and read them — this is the moment to catch a rule that fires on everything.

- [ ] **Step 8: Confirm the two known real defects surface as designed**

Run: `cd scripts/seo && python3 -c "
from seo_fetch import Fetcher, RequestsTransport
from seo_model import load_site_config
from seo_blog_audit import audit
from seo_report import gate, partition
site = load_site_config('../../data/seo_sites.json', 'travelanimator')
s = audit(site, Fetcher(RequestsTransport()))
active, suppressed = partition(s.findings, site)
print('A2 suppressed:', [f.rule for f in suppressed if f.rule == 'A2'][:1])
print('A2 active    :', [f.rule for f in active if f.rule == 'A2'][:1])
print('gate open    :', gate(s))
print('top findings :', sorted({(f.rule, f.severity) for f in active})[:12])
"`
Expected: `A2` appears in **suppressed** and not in active — proving the suppression path works against the live site. Note whatever else surfaces; genuine defects are the point, but any rule firing on all 10 blogs deserves a threshold review before Task 9.

- [ ] **Step 9: Commit**

```bash
git add scripts/seo/seo_checks.py scripts/seo/seo_blog_audit.py scripts/seo/test_seo_audit.py
git commit -m "feat(seo): add rule registry and audit orchestrator"
```

---

## Task 9: Cron workflow and end-to-end verification

**Files:**
- Create: `.github/workflows/seo-blog-audit.yml`

**Interfaces:**
- Consumes: `data/seo_sites.json` and every `scripts/seo/*.py` module via raw-`main` URLs.
- Produces: a daily matrix run, one `report.html` artifact per site, and a Telegram document per site whose gate is open.

Telegram chat `-5312322129` is hardcoded per the spec; the token comes from `secrets.SUBSCRIPTION_TELEGRAM_TOKEN`, matching `ship-accuracy-report.yml`.

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/seo-blog-audit.yml`:

```yaml
name: Blog SEO Audit

on:
  workflow_dispatch:
    inputs:
      site:
        description: 'Audit one site only (blank = all sites in data/seo_sites.json)'
        required: false
        type: string
      blog_count:
        description: 'How many blogs to audit (blank = the site config value)'
        required: false
        type: string
  schedule:
    # Daily at 06:00 UTC (10:00 Asia/Muscat)
    - cron: '0 6 * * *'

env:
  RAW: https://raw.githubusercontent.com/Lascade-Co/actions/main
  TELEGRAM_CHAT_ID: '-5312322129'

jobs:
  discover:
    runs-on: ubuntu-latest
    outputs:
      sites: ${{ steps.list.outputs.sites }}
    steps:
      - name: Build the site matrix
        id: list
        env:
          ONLY: ${{ inputs.site }}
        run: |
          curl -fsSL -o seo_sites.json "$RAW/data/seo_sites.json"
          if [ -n "$ONLY" ]; then
            SITES=$(jq -c --arg only "$ONLY" '[.[] | select(.name == $only) | .name]' seo_sites.json)
          else
            SITES=$(jq -c '[.[].name]' seo_sites.json)
          fi
          if [ "$SITES" = "[]" ]; then
            echo "No sites matched. Known: $(jq -r '[.[].name] | join(", ")' seo_sites.json)" >&2
            exit 1
          fi
          echo "sites=$SITES" >> "$GITHUB_OUTPUT"
          echo "Matrix: $SITES"

  audit:
    needs: discover
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        site: ${{ fromJSON(needs.discover.outputs.sites) }}
    steps:
      - name: Set up Python
        uses: actions/setup-python@v6
        with:
          python-version: '3.13'

      - name: Install dependencies
        run: pip install --quiet requests beautifulsoup4 lxml

      - name: Download audit scripts
        run: |
          curl -fsSL -o seo_sites.json      "$RAW/data/seo_sites.json"
          curl -fsSL -o seo_model.py        "$RAW/scripts/seo/seo_model.py"
          curl -fsSL -o seo_parse.py        "$RAW/scripts/seo/seo_parse.py"
          curl -fsSL -o seo_fetch.py        "$RAW/scripts/seo/seo_fetch.py"
          curl -fsSL -o seo_rulekit.py      "$RAW/scripts/seo/seo_rulekit.py"
          curl -fsSL -o seo_checks_abc.py   "$RAW/scripts/seo/seo_checks_abc.py"
          curl -fsSL -o seo_checks_def.py   "$RAW/scripts/seo/seo_checks_def.py"
          curl -fsSL -o seo_checks_ghi.py   "$RAW/scripts/seo/seo_checks_ghi.py"
          curl -fsSL -o seo_checks.py       "$RAW/scripts/seo/seo_checks.py"
          curl -fsSL -o seo_report.py       "$RAW/scripts/seo/seo_report.py"
          curl -fsSL -o seo_blog_audit.py   "$RAW/scripts/seo/seo_blog_audit.py"

      - name: Run the audit
        id: audit
        env:
          BLOG_COUNT: ${{ inputs.blog_count }}
        run: |
          ARGS="--site ${{ matrix.site }} --config seo_sites.json --output report.html"
          if [ -n "$BLOG_COUNT" ]; then ARGS="$ARGS --blog-count $BLOG_COUNT"; fi
          python3 seo_blog_audit.py $ARGS

      - name: Upload report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: seo-report-${{ matrix.site }}
          path: report.html
          if-no-files-found: warn
          retention-days: 30

      - name: Send report to Telegram
        if: steps.audit.outputs.has_findings == 'true'
        env:
          TELEGRAM_TOKEN: ${{ secrets.SUBSCRIPTION_TELEGRAM_TOKEN }}
          LABEL: ${{ steps.audit.outputs.label }}
          ERRORS: ${{ steps.audit.outputs.error_count }}
          WARNINGS: ${{ steps.audit.outputs.warn_count }}
        run: |
          curl -sf -X POST \
            "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendDocument" \
            -F chat_id="${TELEGRAM_CHAT_ID}" \
            -F document=@report.html \
            -F caption="Blog SEO Audit — ${LABEL}: ${ERRORS} errors, ${WARNINGS} warnings"
```

- [ ] **Step 2: Lint the workflow**

Run: `actionlint .github/workflows/seo-blog-audit.yml && python3 -c "
import yaml; yaml.safe_load(open('.github/workflows/seo-blog-audit.yml')); print('YAML OK')
"`
Expected: no actionlint output, then `YAML OK`. If `actionlint` is not installed, `brew install actionlint`.

- [ ] **Step 3: Verify the matrix expression resolves as intended**

Run: `jq -c '[.[].name]' data/seo_sites.json && jq -c --arg only marineradar '[.[] | select(.name == $only) | .name]' data/seo_sites.json`
Expected: `["travelanimator","marineradar"]` then `["marineradar"]` — the two shapes `fromJSON` will receive.

- [ ] **Step 4: Prove the gate is wired to the suppress list, not hardcoded**

Temporarily drop `"A2"` from travelanimator's `suppress` and confirm the gate flips:

```bash
cd scripts/seo
python3 - <<'PY'
import json, pathlib
p = pathlib.Path("../../data/seo_sites.json")
original = p.read_text()
data = json.loads(original)
data[0]["suppress"] = []
p.write_text(json.dumps(data, indent=2) + "\n")
PY
GITHUB_OUTPUT=/tmp/out-unsuppressed.txt python3 seo_blog_audit.py --site travelanimator --config ../../data/seo_sites.json --output /tmp/r1.html
git -C ../.. checkout data/seo_sites.json
GITHUB_OUTPUT=/tmp/out-suppressed.txt python3 seo_blog_audit.py --site travelanimator --config ../../data/seo_sites.json --output /tmp/r2.html
echo "--- without suppression ---"; grep has_findings /tmp/out-unsuppressed.txt
echo "--- with suppression ---";    grep has_findings /tmp/out-suppressed.txt
```
Expected: `has_findings=true` without suppression. With suppression it must be `false` **unless another error or warning is genuinely present** — if it stays `true`, read `/tmp/r2.html` and confirm the remaining findings are real defects rather than a mis-tuned threshold. Record which they are.

- [ ] **Step 5: Prove the CMS degradation path on a real run**

Run: `cd scripts/seo && python3 -c "
from seo_blog_audit import audit
from seo_fetch import Fetcher, RequestsTransport
from seo_model import load_site_config
site = load_site_config('../../data/seo_sites.json', 'travelanimator')
broken = type(site)(**{**site.__dict__, 'origin_host': 'hub-does-not-exist.travelanimator.com'})
s = audit(broken, Fetcher(RequestsTransport(), timeout=5))
parity = [(f.rule, f.severity) for f in s.findings if f.rule.startswith('I')]
print('parity findings:', parity)
assert parity == [('I4', 'info')], parity
print('OK — unreachable CMS degrades to exactly one info finding')
"`
Expected: `OK — unreachable CMS degrades to exactly one info finding`. This is spec verification item 6 and the failure mode ADR 0004 commits to.

- [ ] **Step 6: Confirm the audit never exits non-zero, even when everything breaks**

Run: `cd scripts/seo && python3 seo_blog_audit.py --site travelanimator --config /nonexistent.json --output /tmp/r3.html; echo "exit=$?"`
Expected: `exit=0` with the traceback on stderr — a missing config must not turn the cron red ([ADR 0003](../../adr/0003-blog-seo-audit-reports-never-fails.md)).

- [ ] **Step 7: Commit and push**

```bash
git add .github/workflows/seo-blog-audit.yml
git commit -m "feat(seo): add daily Blog SEO Audit cron workflow"
git push
```

- [ ] **Step 8: Trigger a real run on main and confirm delivery**

Run: `gh workflow run "Blog SEO Audit" --ref main && sleep 30 && gh run list --workflow "Blog SEO Audit" --limit 1`

Then once it completes:

Run: `gh run view --workflow "Blog SEO Audit" --log | grep -E "errors, |Matrix:|has_findings"`
Expected: the `discover` job logs `Matrix: ["travelanimator","marineradar"]`, both audit jobs succeed (green), each uploads an artifact, and the Telegram step runs only for sites whose gate is open. Check the chat: the document must arrive with a caption naming the site and its counts, and it must render readably when opened.

- [ ] **Step 9: Record what the first real run found**

Append a short section to the spec noting the findings the first production run surfaced and any threshold that needed tuning, then commit. This closes the loop between designed rules and observed behaviour, and it is the record that tells the next reader whether a rule earns its keep.

```bash
git add docs/superpowers/specs/2026-08-03-seo-blog-audit-design.md
git commit -m "docs(seo): record first production audit findings"
```

---

## Self-Review

**Spec coverage** — every section of the spec maps to a task:

| Spec requirement | Task |
|---|---|
| Site config, thresholds, suppress, cms_api | 1 |
| Blog listing discovery, first N in DOM order | 2, 8 |
| Sitemap index recursion | 2, 3 |
| `/_next/image` decoding, srcset, magic-byte sizing | 2, 3 |
| No-redirect fetch, retry, URL cache, two thread pools | 3 |
| Rules A1–A5, B1–B6, C1–C4 | 4 |
| Rules D1–D8, E1–E6, F1–F4 | 5 |
| Rules G1–G5, H1–H3, I1–I4 | 6 |
| Report structure, suppressed section, severity gate | 7 |
| Parallel blog fetch and parallel rule evaluation | 3, 8 |
| Always-exit-0, harness error still delivered | 8 |
| Matrix job per site, artifact always, Telegram when gated | 9 |
| Spec verification items 1–8 | 8 (steps 5–8), 9 (steps 2–8) |

**Type consistency** — `Rule.fn` takes `(page, site, urls, ctx)` at blog scope and `(pages, site, urls, ctx)` at run scope, applied consistently in Tasks 4–6 and dispatched on `rule.scope` in Task 8. `UrlStatus.verified` means "the status is trustworthy" everywhere: B1/B3/B6/E3 all skip unverified statuses, and only A3 treats a non-200 asset as an error regardless, because an origin asset that refuses us is a defect either way. `site.threshold(key)` is the single accessor for tuneable numbers; no rule inlines a literal.

**Known sharp edges for the implementer:**

- `SiteConfig` is a frozen dataclass, so the `--blog-count` override and the Task 9 step-5 origin override both rebuild it via `type(site)(**{**site.__dict__, ...})`. If `SiteConfig` gains a non-init field, that idiom breaks and needs `dataclasses.replace`.
- `parse_listing` filters on the raw `href` prefix, so a listing that ever emits absolute blog URLs would return nothing. If Task 8's `DiscoverTest` passes but a live run finds zero blogs, that is the first thing to check.
- `check_a2` scans `raw_html` plus its percent-decoded copy, so a URL appearing in both forms is counted twice. That inflates the count in the message but never changes whether the rule fires; deliberate, to keep the scan single-pass.
