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


if __name__ == "__main__":
    unittest.main()
