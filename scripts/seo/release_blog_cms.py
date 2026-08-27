"""CMS reads and the draft write for the per-release draft pipeline."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import quote, urlparse

from seo_parse import parse_sitemap

CANDIDATE_LIMIT = 100
REQUEST_TIMEOUT = 20
TAG_STRIP = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class LinkCandidate:
    """One existing blog that may become a contextual internal link."""

    url: str
    slug: str
    title: str = ""
    excerpt: str = ""


def requests_http(method, url, *, headers=None, json_body=None, auth=None, timeout=REQUEST_TIMEOUT):
    """Production HTTP transport returning ``(status, body)`` without raising."""
    import requests

    try:
        response = requests.request(
            method,
            url,
            headers=headers or {},
            json=json_body,
            auth=auth,
            timeout=timeout,
        )
    except Exception as exc:  # network, DNS, TLS, and timeouts share one outcome here
        return 0, f"{type(exc).__name__}: {exc}"
    return response.status_code, response.text


def release_marker(repo: str, tag: str) -> str:
    return f"release-blog: {repo}@{tag}"


def _plain(html: str) -> str:
    return " ".join(TAG_STRIP.sub(" ", html or "").split())


def _cms_url(site, path: str, query: str) -> str:
    return f"https://{site.origin_host}/wp-json/wp/v2/{path}?{query}"


def _canonical_candidate_url(site, item: dict) -> str:
    """Map a CMS permalink onto the public blog URL for this site."""
    link = str(item.get("link") or "")
    slug = str(item.get("slug") or "").strip("/")
    if not slug:
        return ""
    trailing_slash = "/" if urlparse(link).path.endswith("/") else ""
    return (
        f"https://{site.canonical_host}{site.listing_path.rstrip('/')}"
        f"/{slug}{trailing_slash}"
    )


def fetch_link_candidates(site, *, http=requests_http) -> tuple[list[LinkCandidate], str]:
    """Return newest published blogs, falling back from the CMS to the sitemap."""
    query = (
        f"per_page={CANDIDATE_LIMIT}&status=publish&orderby=date&order=desc"
        "&_fields=title,excerpt,link,slug,date"
    )
    status, body = http("GET", _cms_url(site, "posts", query))
    if status == 200:
        try:
            payload = json.loads(body)
            candidates = [
                LinkCandidate(
                    url=_canonical_candidate_url(site, item),
                    slug=item.get("slug", ""),
                    title=_plain((item.get("title") or {}).get("rendered", "")),
                    excerpt=_plain((item.get("excerpt") or {}).get("rendered", "")),
                )
                for item in payload
                if _canonical_candidate_url(site, item)
            ]
            if candidates:
                return candidates, ""
        except (ValueError, TypeError, AttributeError) as exc:
            status, body = 0, f"unparseable CMS response: {exc}"

    note = f"CMS candidate read failed (HTTP {status}); fell back to the sitemap"
    sitemap_status, sitemap_body = http("GET", site.sitemap_url)
    if sitemap_status == 200:
        urls, _children = parse_sitemap(sitemap_body)
        prefix = site.listing_path.rstrip("/") + "/"
        blogs = sorted(url for url in urls if urlparse(url).path.startswith(prefix))
        if blogs:
            return [
                LinkCandidate(url=url, slug=urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
                for url in blogs
            ], note
    return [], (
        f"no link candidates: CMS returned HTTP {status} and the sitemap returned "
        f"HTTP {sitemap_status}"
    )


def find_by_marker(site, marker: str, *, http=requests_http, auth) -> dict | None:
    """Return the draft or published blog carrying the exact release marker."""
    query = (
        f"status=draft%2Cpublish&per_page=20&search={quote(marker)}"
        "&_fields=id,status,slug,content"
    )
    status, body = http("GET", _cms_url(site, "posts", query), auth=auth)
    if status != 200:
        return None
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return None
    for item in payload:
        rendered = (item.get("content") or {}).get("rendered", "")
        if marker in rendered:
            return {
                "id": item.get("id"),
                "status": item.get("status", ""),
                "slug": item.get("slug", ""),
            }
    return None


def build_payload(meta: dict, html: str) -> dict:
    """Build a draft-only CMS request body; the CMS supplies the date."""
    return {
        "status": "draft",
        "title": meta.get("title", ""),
        "slug": meta.get("slug", ""),
        "excerpt": meta.get("excerpt", ""),
        "content": html,
    }


def write_draft(site, payload: dict, *, http=requests_http, auth, draft_id=None) -> tuple[bool, str]:
    """Create a draft or overwrite the draft identified by ``draft_id``."""
    path = f"posts/{draft_id}" if draft_id else "posts"
    url = f"https://{site.origin_host}/wp-json/wp/v2/{path}"
    status, body = http("POST", url, json_body=payload, auth=auth)
    if 200 <= status < 300:
        try:
            written_id = json.loads(body).get("id")
        except (ValueError, TypeError):
            written_id = None
        verb = "overwrote" if draft_id else "created"
        return True, f"{verb} draft id {written_id}"
    return False, f"CMS write failed: HTTP {status} {body[:400]}"
