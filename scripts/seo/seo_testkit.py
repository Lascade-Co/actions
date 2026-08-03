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
