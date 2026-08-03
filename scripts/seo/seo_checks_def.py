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


def _jsonld_entry_is_valid(entry: dict) -> bool:
    """A top-level JSON-LD object is valid when it carries @context plus either
    its own @type, or a non-empty @graph whose every member node carries @type
    (the standard @graph container form — e.g. MarineRadar wraps BlogPosting/
    BreadcrumbList/FAQPage/HowTo in one @graph block, which is valid JSON-LD
    even though the container itself has no @type of its own)."""
    if "@context" not in entry:
        return False
    if "@type" in entry:
        return True
    graph = entry.get("@graph")
    return isinstance(graph, list) and bool(graph) and all(
        isinstance(node, dict) and "@type" in node for node in graph
    )


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
            if not _jsonld_entry_is_valid(entry):
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
