import unittest

from seo_model import Response
from seo_parse import (
    absolutize,
    decode_next_image,
    normalize_url,
    parse_blog,
    parse_listing,
    parse_sitemap,
    parse_srcset,
    slug_from_url,
)
from seo_testkit import fixture

BASE = "https://www.travelanimator.com"
BLOG_URL = f"{BASE}/hub/good-blog"


def good_page():
    body = fixture("good_blog.html")
    response = Response(url=BLOG_URL, status=200, headers={"content-type": "text/html"}, body=body)
    return parse_blog(BLOG_URL, "good-blog", response)


class UrlHelperTest(unittest.TestCase):
    def test_absolutize_relative(self):
        self.assertEqual(absolutize(BASE, "/hub/x"), f"{BASE}/hub/x")

    def test_absolutize_leaves_absolute(self):
        self.assertEqual(absolutize(BASE, "https://other.com/y"), "https://other.com/y")

    def test_absolutize_ignores_non_http_schemes(self):
        self.assertEqual(absolutize(BASE, "mailto:a@b.com"), "mailto:a@b.com")

    def test_normalize_strips_fragment_and_trailing_slash(self):
        self.assertEqual(normalize_url(f"{BASE}/hub/x/#top"), f"{BASE}/hub/x")

    def test_normalize_lowercases_host_only(self):
        self.assertEqual(normalize_url("https://WWW.Travelanimator.com/Hub/X"), f"{BASE}/Hub/X")

    def test_normalize_keeps_root_slash(self):
        self.assertEqual(normalize_url(f"{BASE}/"), f"{BASE}/")

    def test_decode_next_image(self):
        url = "/_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-content%2Fuploads%2Fa.png&w=640&q=75"
        self.assertEqual(
            decode_next_image(url),
            "https://hub.travelanimator.com/wp-content/uploads/a.png",
        )

    def test_decode_next_image_returns_none_for_plain_url(self):
        self.assertIsNone(decode_next_image("https://example.com/a.png"))

    def test_parse_srcset(self):
        value = "/a.png 640w, /b.png 1200w"
        self.assertEqual(parse_srcset(value), ["/a.png", "/b.png"])

    def test_parse_srcset_ignores_data_uris(self):
        self.assertEqual(parse_srcset("data:image/gif;base64,R0lGOD 1x"), [])

    def test_slug_from_url(self):
        self.assertEqual(slug_from_url(f"{BASE}/hub/good-blog"), "good-blog")


class ParseBlogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.page = good_page()

    def test_head_metadata(self):
        self.assertEqual(self.page.title, "How to Create a Travel Animation for Instagram")
        self.assertTrue(self.page.meta_description.startswith("Learn how to create"))
        self.assertEqual(self.page.canonicals, (BLOG_URL,))
        self.assertEqual(self.page.html_lang, "en")
        self.assertTrue(self.page.has_viewport)

    def test_og_and_twitter(self):
        self.assertEqual(self.page.og["og:url"], BLOG_URL)
        self.assertEqual(self.page.og["og:type"], "article")
        self.assertEqual(self.page.twitter["twitter:card"], "summary_large_image")

    def test_headings_in_document_order(self):
        levels = [level for level, _ in self.page.headings]
        self.assertEqual(levels, [1, 2, 3, 2])
        self.assertEqual(len(self.page.h1s), 1)

    def test_anchors_absolutized(self):
        urls = [a.url for a in self.page.anchors]
        self.assertIn(f"{BASE}/pricing", urls)
        self.assertIn("https://support.travelanimator.com/articles/export", urls)
        self.assertIn(f"{BASE}/hub/other-blog", urls)

    def test_anchor_text_captured(self):
        pricing = next(a for a in self.page.anchors if a.url.endswith("/pricing"))
        self.assertEqual(pricing.text, "pricing page")

    def test_content_anchors_only_include_the_selected_article(self):
        urls = {a.url for a in self.page.content_anchors}
        self.assertEqual(
            urls,
            {
                f"{BASE}/pricing",
                f"{BASE}/features",
                f"{BASE}/hub/route-animation-guide",
                "https://support.travelanimator.com/articles/export",
            },
        )
        self.assertNotIn(f"{BASE}/hub/other-blog", urls)
        self.assertNotIn(f"{BASE}/hub", urls)

    def test_content_anchors_exclude_navigation_nested_inside_main(self):
        body = (
            '<main><nav><a href="/menu">Menu</a></nav>'
            '<p>Useful text with <a href="/pricing">pricing details</a>.</p>'
            '<aside><a href="/sidebar">Sidebar</a></aside></main>'
        )
        page = parse_blog(BLOG_URL, "x", Response(url=BLOG_URL, status=200, body=body))
        self.assertEqual([a.url for a in page.content_anchors], [f"{BASE}/pricing"])

    def test_images_include_srcset_and_meta_sources(self):
        """good_blog.html's BlogPosting JSON-LD carries an `image` field
        (reviewer follow-up to Important 5) — parse_blog folds that into
        page.images with source="jsonld" too, which is what makes B3 able
        to see a broken JSON-LD-sourced image at all."""
        sources = {i.source for i in self.page.images}
        self.assertEqual(sources, {"img", "srcset", "og", "twitter", "preload", "jsonld"})

    def test_next_image_urls_are_decoded_to_origin(self):
        origin_urls = [i.url for i in self.page.images if "hub.travelanimator.com" in i.url]
        self.assertTrue(any(u.endswith("/wp-content/uploads/2026/07/step.png") for u in origin_urls))
        self.assertFalse(any("_next/image" in u for u in origin_urls))

    def test_decorative_image_flags_captured(self):
        deco = next(i for i in self.page.images if i.url.endswith("decorative-rule.svg"))
        self.assertEqual(deco.alt, "")
        self.assertTrue(deco.aria_hidden)

    def test_jsonld_blocks_parsed(self):
        types = [b.data["@type"] for b in self.page.jsonld]
        self.assertEqual(types, ["BlogPosting", "BreadcrumbList", "FAQPage"])
        self.assertTrue(all(b.error is None for b in self.page.jsonld))

    def test_jsonld_image_bare_url_becomes_an_image_ref(self):
        """Reviewer follow-up to Important 5: a JSON-LD `image` given as a
        bare URL string must become a real ImageRef (source="jsonld") in
        page.images, not merely a URL collected and forgotten — that's what
        lets B3 actually report a broken one."""
        body = (
            '<html><head><script type="application/ld+json">'
            '{"@context":"https://schema.org","@type":"BlogPosting",'
            '"image":"https://hub.travelanimator.com/wp-content/uploads/bare.jpg"}'
            "</script></head><body></body></html>"
        )
        page = parse_blog(BLOG_URL, "x", Response(url=BLOG_URL, status=200, body=body))
        jsonld_images = [i for i in page.images if i.source == "jsonld"]
        self.assertEqual([i.url for i in jsonld_images], [
            "https://hub.travelanimator.com/wp-content/uploads/bare.jpg"
        ])

    def test_jsonld_image_object_and_graph_nesting_become_image_refs(self):
        """An ImageObject form (`image: {"url": ...}`) and a BlogPosting
        nested inside a top-level @graph block (MarineRadar's actual shape)
        must both be found."""
        body = (
            '<html><head><script type="application/ld+json">'
            '{"@context":"https://schema.org","@graph":[{"@type":"BlogPosting",'
            '"image":{"@type":"ImageObject","url":"https://hub.travelanimator.com/wp-content/uploads/object.jpg"}}]}'
            "</script></head><body></body></html>"
        )
        page = parse_blog(BLOG_URL, "x", Response(url=BLOG_URL, status=200, body=body))
        jsonld_images = [i for i in page.images if i.source == "jsonld"]
        self.assertEqual([i.url for i in jsonld_images], [
            "https://hub.travelanimator.com/wp-content/uploads/object.jpg"
        ])

    def test_malformed_jsonld_records_error_without_raising(self):
        body = '<html><head><script type="application/ld+json">{"a":,}</script></head><body></body></html>'
        page = parse_blog(BLOG_URL, "x", Response(url=BLOG_URL, status=200, body=body))
        self.assertIsNone(page.jsonld[0].data)
        self.assertIsNotNone(page.jsonld[0].error)

    def test_article_text_excludes_nav_and_footer(self):
        self.assertNotIn("Another blog", self.page.article_text)
        self.assertGreater(self.page.word_count, 300)

    def test_raw_html_retained(self):
        self.assertIn("hub.travelanimator.com", self.page.raw_html)


class ArticleTextSelectionTest(unittest.TestCase):
    """Regression for picking the first <article> tag instead of the real content one."""

    def test_picks_the_longest_article_candidate_not_the_first(self):
        body = fixture("multi_article.html")
        page = parse_blog(BLOG_URL, "multi-article", Response(url=BLOG_URL, status=200, body=body))
        self.assertIn("does not fit inside a five word promo banner", page.article_text)
        self.assertNotIn("Get the mobile app today", page.article_text)
        self.assertNotIn("Download now", page.article_text)
        self.assertNotIn("Another blog in the footer", page.article_text)
        self.assertGreater(page.word_count, 50)


class ParseListingTest(unittest.TestCase):
    def test_returns_blog_urls_in_dom_order_deduplicated(self):
        urls = parse_listing(fixture("listing.html"), BASE, "/hub")
        self.assertEqual(
            urls,
            [f"{BASE}/hub/good-blog", f"{BASE}/hub/second-blog", f"{BASE}/hub/third-blog"],
        )

    def test_excludes_category_author_pagination_and_listing_itself(self):
        urls = parse_listing(fixture("listing.html"), BASE, "/hub")
        joined = " ".join(urls)
        for fragment in ("category", "author", "page", "?"):
            self.assertNotIn(fragment, joined)


class ParseSitemapTest(unittest.TestCase):
    def test_index_returns_children_and_no_pages(self):
        pages, children = parse_sitemap(fixture("sitemap_index.xml"))
        self.assertEqual(pages, set())
        self.assertEqual(
            children,
            [
                "https://www.marineradar.com/sitemap-blogs.xml",
                "https://www.marineradar.com/sitemap-pages.xml",
            ],
        )

    def test_urlset_returns_pages_and_no_children(self):
        pages, children = parse_sitemap(fixture("sitemap_child.xml"))
        self.assertEqual(children, [])
        self.assertIn(BLOG_URL, pages)

    def test_malformed_xml_returns_empty_without_raising(self):
        self.assertEqual(parse_sitemap("<not xml"), (set(), []))


if __name__ == "__main__":
    unittest.main()
