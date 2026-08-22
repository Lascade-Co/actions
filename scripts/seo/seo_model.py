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
    "internal_links_min": 3,
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
    aria_label: str | None = None
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
    content_anchors: tuple[Anchor, ...] = ()
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
    link: str = ""


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
    # Slugs whose targeted CMS lookup (_fill_missing_cms_posts) failed — a non-200
    # response or unparseable body, never a genuine "no such post". check_i3 must
    # skip these rather than treat a failed request as confirmed absence. Does not
    # affect cms.ok / _parity_enabled: one failed slug lookup is not a CMS outage.
    cms_lookup_failed: frozenset[str] = frozenset()


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
