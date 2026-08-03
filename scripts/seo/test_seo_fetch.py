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

    def test_one_failing_child_marks_the_whole_index_not_ok(self):
        """Critical 2: fetch_sitemap previously dropped a failing child and
        still returned ok=True, so check_b4 concluded a blog was absent from
        an incomplete URL set at error severity. A single failed child must
        make the whole fetch ok=False (check_b4 already treats sitemap_ok is
        False as 'say nothing')."""
        index_url = "https://www.marineradar.com/sitemap.xml"
        good_child = "https://www.marineradar.com/sitemap-blogs.xml"
        bad_child = "https://www.marineradar.com/sitemap-pages.xml"
        index_xml = (
            '<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"<sitemap><loc>{good_child}</loc></sitemap>"
            f"<sitemap><loc>{bad_child}</loc></sitemap></sitemapindex>"
        )
        good_child_xml = (
            '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            "<url><loc>https://www.marineradar.com/hub/a</loc></url></urlset>"
        )
        transport = FakeTransport(
            {
                ("GET", index_url): Response(url=index_url, status=200, body=index_xml),
                ("GET", good_child): Response(url=good_child, status=200, body=good_child_xml),
                ("GET", bad_child): Response(url=bad_child, status=500),
            }
        )
        urls, ok = Fetcher(transport).fetch_sitemap(index_url)
        self.assertFalse(ok)

    def test_all_children_succeeding_unions_their_urls_and_is_ok(self):
        index_url = "https://www.marineradar.com/sitemap.xml"
        child_a = "https://www.marineradar.com/sitemap-a.xml"
        child_b = "https://www.marineradar.com/sitemap-b.xml"
        index_xml = (
            '<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"<sitemap><loc>{child_a}</loc></sitemap>"
            f"<sitemap><loc>{child_b}</loc></sitemap></sitemapindex>"
        )
        child_a_xml = (
            '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            "<url><loc>https://www.marineradar.com/hub/a</loc></url></urlset>"
        )
        child_b_xml = (
            '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            "<url><loc>https://www.marineradar.com/hub/b</loc></url></urlset>"
        )
        transport = FakeTransport(
            {
                ("GET", index_url): Response(url=index_url, status=200, body=index_xml),
                ("GET", child_a): Response(url=child_a, status=200, body=child_a_xml),
                ("GET", child_b): Response(url=child_b, status=200, body=child_b_xml),
            }
        )
        urls, ok = Fetcher(transport).fetch_sitemap(index_url)
        self.assertTrue(ok)
        self.assertEqual(
            urls,
            frozenset({"https://www.marineradar.com/hub/a", "https://www.marineradar.com/hub/b"}),
        )


class FetcherCmsTest(unittest.TestCase):
    CMS_URL = (
        "https://hub.travelanimator.com/wp-json/wp/v2/posts"
        "?per_page=20&_fields=slug,date,modified,status,link&orderby=date&order=desc"
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


class FetcherCmsPostBySlugTest(unittest.TestCase):
    """Round 4, item 2: the targeted single-slug lookup discover() uses to
    confirm whether a blog missing from the CMS window genuinely has no
    published post, before I3 calls it an orphan."""

    SLUG_URL = (
        "https://hub.travelanimator.com/wp-json/wp/v2/posts"
        "?slug=good-blog&_fields=slug,date,modified,status,link"
    )

    def test_parses_the_matching_post(self):
        payload = _json.dumps(
            [
                {
                    "slug": "good-blog",
                    "date": "2026-07-30T10:00:00",
                    "modified": "2026-08-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/good-blog",
                }
            ]
        )
        transport = FakeTransport({("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=200, body=payload)})
        post = Fetcher(transport).fetch_cms_post_by_slug("hub.travelanimator.com", "good-blog")
        self.assertIsNotNone(post)
        self.assertEqual(post.slug, "good-blog")
        self.assertEqual(post.status, "publish")

    def test_empty_result_returns_none(self):
        transport = FakeTransport(
            {("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=200, body=_json.dumps([]))}
        )
        self.assertIsNone(Fetcher(transport).fetch_cms_post_by_slug("hub.travelanimator.com", "good-blog"))

    def test_non_200_returns_none_without_raising(self):
        transport = FakeTransport({("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=500)})
        self.assertIsNone(Fetcher(transport).fetch_cms_post_by_slug("hub.travelanimator.com", "good-blog"))

    def test_malformed_json_returns_none_without_raising(self):
        transport = FakeTransport(
            {("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=200, body="{oops")}
        )
        self.assertIsNone(Fetcher(transport).fetch_cms_post_by_slug("hub.travelanimator.com", "good-blog"))


class FetcherCmsPostBySlugDetailedTest(unittest.TestCase):
    """Critical 3: the tri-state lookup _fill_missing_cms_posts uses to tell
    a failed request apart from a genuine absence, so check_i3 doesn't treat
    an unreachable lookup as a confirmed zombie."""

    SLUG_URL = (
        "https://hub.travelanimator.com/wp-json/wp/v2/posts"
        "?slug=good-blog&_fields=slug,date,modified,status,link"
    )

    def test_found_returns_post_and_not_failed(self):
        payload = _json.dumps(
            [
                {
                    "slug": "good-blog",
                    "date": "2026-07-30T10:00:00",
                    "modified": "2026-08-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/good-blog",
                }
            ]
        )
        transport = FakeTransport({("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=200, body=payload)})
        post, failed = Fetcher(transport).fetch_cms_post_by_slug_detailed("hub.travelanimator.com", "good-blog")
        self.assertIsNotNone(post)
        self.assertFalse(failed)

    def test_genuine_absence_is_not_failed(self):
        transport = FakeTransport(
            {("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=200, body=_json.dumps([]))}
        )
        post, failed = Fetcher(transport).fetch_cms_post_by_slug_detailed("hub.travelanimator.com", "good-blog")
        self.assertIsNone(post)
        self.assertFalse(failed)

    def test_non_200_is_failed(self):
        transport = FakeTransport({("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=500)})
        post, failed = Fetcher(transport).fetch_cms_post_by_slug_detailed("hub.travelanimator.com", "good-blog")
        self.assertIsNone(post)
        self.assertTrue(failed)

    def test_malformed_json_is_failed(self):
        transport = FakeTransport(
            {("GET", self.SLUG_URL): Response(url=self.SLUG_URL, status=200, body="{oops")}
        )
        post, failed = Fetcher(transport).fetch_cms_post_by_slug_detailed("hub.travelanimator.com", "good-blog")
        self.assertIsNone(post)
        self.assertTrue(failed)


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
