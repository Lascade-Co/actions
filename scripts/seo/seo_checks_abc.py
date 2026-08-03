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
        if status is None or not status.verified:
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
            elif re.search(r"(?:^|[?&])p=\d+", parsed.query):
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
        # Hygiene, not broken crawl correctness — and run-scoped, so an error
        # here would page every single day forever on any site that simply
        # never had the directive. The per-blog disallow finding above stays
        # at error; only this run-scoped omission is downgraded.
        findings.append(
            finding(C3, SEVERITY_WARN, "robots.txt declares no Sitemap directive")
        )
    return findings


def check_c4(page, site, urls, ctx):
    if not page.response.ok:
        # Soft-404 (spec:193) is defined as a 200 response with thin content.
        # A non-200 page isn't a soft 404 — it's H1's finding, not this one.
        return []
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
