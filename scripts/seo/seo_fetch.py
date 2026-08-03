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
    while index + 9 <= len(data):
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
            f"?per_page={per_page}&_fields=slug,date,modified,status,link&orderby=date&order=desc"
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
                    link=item.get("link", ""),
                )
                for item in payload
            )
        except (ValueError, TypeError, KeyError) as exc:
            return CmsSnapshot(posts=(), ok=False, error=f"unparseable CMS response: {exc}", enabled=True)
        return CmsSnapshot(posts=posts, ok=True, error=None, enabled=True)

    def fetch_cms_post_by_slug(self, origin_host: str, slug: str) -> CmsPost | None:
        """Single targeted lookup for one slug missing from the CMS window fetched
        by fetch_cms_posts. Used to confirm a live blog genuinely has no CMS
        counterpart before I3 reports it as an orphan, without re-fetching (or
        widening) the whole window.

        Returns None on any failure — a non-200 response, unparseable JSON, or
        an empty/no-match result. The caller must treat None as "this one slug
        couldn't be confirmed", never as "the CMS is down": one missing slug is
        not a CMS outage.
        """
        url = (
            f"https://{origin_host}/wp-json/wp/v2/posts"
            f"?slug={slug}&_fields=slug,date,modified,status,link"
        )
        response = self.get(url)
        if not response.ok:
            return None
        try:
            payload = json.loads(response.body)
            if not isinstance(payload, list) or not payload:
                return None
            item = payload[0]
            return CmsPost(
                slug=item["slug"],
                date=item.get("date", ""),
                modified=item.get("modified", ""),
                status=item.get("status", ""),
                link=item.get("link", ""),
            )
        except (ValueError, TypeError, KeyError, IndexError):
            return None
