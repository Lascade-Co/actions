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
        aria_label = " ".join((anchor.aria_label or "").split())
        accessible_text = aria_label or " ".join(anchor.text.split())
        text = accessible_text.lower()
        if not text:
            if any(alt.strip() for alt in anchor.image_alts):
                continue
            findings.append(
                finding(
                    G4,
                    SEVERITY_WARN,
                    "internal link has no anchor text, aria-label, or image alt",
                    blog_url=page.url,
                    evidence=anchor.url,
                )
            )
        elif text in GENERIC_ANCHOR_TEXT:
            findings.append(
                finding(
                    G4,
                    SEVERITY_WARN,
                    f"generic anchor text {accessible_text!r}",
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
        # Discovery going quiet is the most likely way this audit goes
        # silently blind (ADR 0003) — a failed listing fetch must say so,
        # not disappear into a report that looks clean because nothing
        # downstream had a listing to compare against.
        return [
            finding(
                H2,
                SEVERITY_ERROR,
                "blog listing could not be fetched",
                evidence=site.listing_url,
            )
        ]
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
        if page.slug in ctx.cms_lookup_failed:
            # The targeted lookup for this slug failed (non-200/unparseable),
            # not a genuine absence — treating a request failure as confirmed
            # absence would report a live blog as a zombie on nothing more
            # than a transient CMS error. See _fill_missing_cms_posts.
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
