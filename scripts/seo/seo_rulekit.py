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
