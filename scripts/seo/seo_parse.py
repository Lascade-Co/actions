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
    """Pick whichever <article>/<main> candidate yields the most text.

    Pages can carry more than one <article> element (e.g. a short promo
    banner ahead of the real post body) — taking the first match understates
    word count and misfires soft-404 / thin-content / FAQ-visibility rules.
    """
    candidates = soup.find_all(("article", "main")) or [soup.body or soup]
    best = ""
    for root in candidates:
        clone = BeautifulSoup(str(root), "lxml")
        for tag in clone.find_all(NON_CONTENT_TAGS):
            tag.decompose()
        text = re.sub(r"\s+", " ", clone.get_text(" ", strip=True))
        if len(text) > len(best):
            best = text
    return best


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
