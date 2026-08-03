import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from seo_blog_audit import FixtureTransport, audit, collect_urls, discover, evaluate, main, write_github_output
from seo_checks import ALL_RULES, BLOG_RULES, RULES_BY_ID, RUN_RULES
from seo_fetch import Fetcher
from seo_model import SEVERITY_ERROR, ImageRef, JsonLdBlock, Response
from seo_report import counts, gate, partition
from seo_testkit import fixture, make_context, make_page, make_response, make_site

LISTING = "https://www.travelanimator.com/hub"
ROBOTS = "https://www.travelanimator.com/robots.txt"
SITEMAP = "https://www.travelanimator.com/sitemap.xml"
CMS = (
    "https://hub.travelanimator.com/wp-json/wp/v2/posts"
    "?per_page=20&_fields=slug,date,modified,status,link&orderby=date&order=desc"
)


def cms_url_for(site) -> str:
    """The exact CMS request URL discover() will issue for this site's blog_count."""
    return (
        f"https://{site.origin_host}/wp-json/wp/v2/posts"
        f"?per_page={site.blog_count * 2}&_fields=slug,date,modified,status,link&orderby=date&order=desc"
    )


def cms_slug_lookup_url_for(site, slug: str) -> str:
    """The exact targeted single-slug CMS lookup URL discover() will issue for
    an audited blog whose slug is absent from the fetched window."""
    return (
        f"https://{site.origin_host}/wp-json/wp/v2/posts"
        f"?slug={slug}&_fields=slug,date,modified,status,link"
    )


class ScriptedTransport:
    """Serves fixtures for known URLs, 200-empty for anything else, and records calls."""

    def __init__(self, pages: dict, *, fail: set = frozenset()):
        self.pages = pages
        self.fail = set(fail)
        self.calls = []

    def __call__(self, method, url, headers, timeout):
        self.calls.append((method, url))
        if url in self.fail:
            raise OSError("scripted failure")
        if url in self.pages:
            status, body, extra = self.pages[url]
            head = {"content-type": "text/html; charset=utf-8", "cache-control": "public, max-age=60"}
            head.update(extra or {})
            return Response(url=url, status=status, headers=head, body=body, content=body.encode(), ttfb_ms=120)
        return Response(url=url, status=200, headers={"content-type": "text/html"}, body="", ttfb_ms=50)


def scripted(**over):
    blog = fixture("good_blog.html")
    pages = {
        LISTING: (200, fixture("listing.html"), None),
        ROBOTS: (200, "User-agent: *\nAllow: /\nSitemap: " + SITEMAP + "\n", {"content-type": "text/plain"}),
        SITEMAP: (200, fixture("sitemap_child.xml"), {"content-type": "application/xml"}),
        CMS: (200, json.dumps(json.loads(fixture("cms_posts.json"))), {"content-type": "application/json"}),
        "https://www.travelanimator.com/hub/good-blog": (200, blog, None),
        "https://www.travelanimator.com/hub/second-blog": (200, blog, None),
        "https://www.travelanimator.com/hub/third-blog": (200, blog, None),
    }
    pages.update(over)
    return ScriptedTransport(pages)


class RegistryTest(unittest.TestCase):
    def test_all_45_rules_registered(self):
        self.assertEqual(len(ALL_RULES), 45)

    def test_every_documented_id_exists(self):
        expected = (
            [f"A{i}" for i in range(1, 6)]
            + [f"B{i}" for i in range(1, 7)]
            + [f"C{i}" for i in range(1, 5)]
            + [f"D{i}" for i in range(1, 9)]
            + [f"E{i}" for i in range(1, 7)]
            + [f"F{i}" for i in range(1, 5)]
            + [f"G{i}" for i in range(1, 6)]
            + [f"H{i}" for i in range(1, 4)]
            + [f"I{i}" for i in range(1, 5)]
        )
        self.assertEqual(sorted(RULES_BY_ID), sorted(expected))

    def test_scopes_are_valid(self):
        self.assertTrue(all(rule.scope == "blog" for rule in BLOG_RULES))
        self.assertTrue(all(rule.scope == "run" for rule in RUN_RULES))

    def test_slugs_unique(self):
        slugs = [rule.slug for rule in ALL_RULES]
        self.assertEqual(len(slugs), len(set(slugs)))


class DiscoverTest(unittest.TestCase):
    def test_returns_listing_blogs_in_order(self):
        site = make_site(blog_count=3, cms_api=False)
        urls, ctx = discover(Fetcher(scripted()), site)
        self.assertEqual(urls[0], "https://www.travelanimator.com/hub/good-blog")
        self.assertEqual(len(urls), 3)
        self.assertTrue(ctx.listing_ok)
        self.assertTrue(ctx.sitemap_ok)
        self.assertTrue(ctx.robots_ok)

    def test_respects_blog_count(self):
        site = make_site(blog_count=2, cms_api=False)
        urls, _ = discover(Fetcher(scripted()), site)
        self.assertEqual(len(urls), 2)

    def test_cms_disabled_leaves_snapshot_disabled(self):
        site = make_site(cms_api=False)
        _, ctx = discover(Fetcher(scripted()), site)
        self.assertFalse(ctx.cms.enabled)

    def test_union_adds_cms_only_slugs(self):
        site = make_site(blog_count=3, cms_api=True)
        transport = scripted(
            **{cms_url_for(site): (200, fixture("cms_posts.json"), {"content-type": "application/json"})}
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn("https://www.travelanimator.com/hub/never-rendered-blog", urls)

    def test_listing_failure_is_survivable(self):
        transport = scripted()
        transport.fail.add(LISTING)
        urls, ctx = discover(Fetcher(transport), make_site(cms_api=False))
        self.assertEqual(urls, [])
        self.assertFalse(ctx.listing_ok)

    def test_cms_post_outside_listing_path_is_not_discovered(self):
        """Round 2, item 1: the union is redirect-aware, not path-aware. MarineRadar's
        own CMS reports /hub/<slug> in `link` for BOTH genuine hub posts and posts
        that live under /trends — a path check alone can't tell them apart (round 1's
        finding). Only a live check can: a CMS-only candidate that 3xx-redirects away
        lives in a different section of the site and must be skipped entirely, with
        no finding — matching what a real HEAD request would show."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/hub/six-saudi-supertankers-reroute"
        payload = json.dumps(
            [
                {
                    "slug": "six-saudi-supertankers-reroute",
                    "date": "2026-08-01T10:00:00",
                    "modified": "2026-08-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/six-saudi-supertankers-reroute",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (
                    308,
                    "",
                    {"location": "https://www.travelanimator.com/trends/six-saudi-supertankers-reroute"},
                ),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertNotIn(candidate, urls)

    def test_cms_candidate_redirect_resolves_to_in_section_destination_is_discovered(self):
        """Round 3, item 1: TravelAnimator's real quirk — the CMS's own link
        omits the /hub segment, so our derived candidate is one path segment
        short and 308s to the correct, same-content /hub URL. Follow that one
        hop: the destination is under listing_path, so it's genuinely in scope
        and must be included using the destination URL (the real page), not
        skipped as off-section."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/some-older-post"
        destination = "https://www.travelanimator.com/hub/some-older-post"
        payload = json.dumps(
            [
                {
                    "slug": "some-older-post",
                    "date": "2026-07-01T10:00:00",
                    "modified": "2026-07-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/some-older-post/",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (308, "", {"location": "/hub/some-older-post"}),
                destination: (200, "<html></html>", None),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn(destination, urls)
        self.assertNotIn(candidate, urls)

    def test_cms_candidate_redirect_to_different_host_is_undecidable_and_skipped(self):
        """Round 3, item 1: a redirect to a different host can't be resolved
        with one hop of confidence — undecidable, not included, and not
        confidently reported as off-section either."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/some-older-post"
        payload = json.dumps(
            [
                {
                    "slug": "some-older-post",
                    "date": "2026-07-01T10:00:00",
                    "modified": "2026-07-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/some-older-post/",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (308, "", {"location": "https://other-domain.example.com/some-older-post"}),
            }
        )
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertNotIn(candidate, urls)
        self.assertFalse(any("other-domain.example.com" in u for u in urls))
        self.assertIn("1 undecidable", stdout.getvalue())
        self.assertIn("skipped 0", stdout.getvalue())

    def test_cms_candidate_redirect_without_location_is_undecidable_and_skipped(self):
        """Round 3, item 1: a 3xx with no Location header at all can't be
        resolved — undecidable."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/some-older-post"
        payload = json.dumps(
            [
                {
                    "slug": "some-older-post",
                    "date": "2026-07-01T10:00:00",
                    "modified": "2026-07-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/some-older-post/",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (308, "", None),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertNotIn(candidate, urls)

    def test_cms_candidate_double_redirect_is_undecidable_and_skipped(self):
        """Round 3, item 1: one hop only — if the destination itself redirects
        again, it is not chased further, even if it might eventually land back
        in scope."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/some-older-post"
        first_hop = "https://www.travelanimator.com/blog/some-older-post"
        final_hub_url = "https://www.travelanimator.com/hub/some-older-post"
        payload = json.dumps(
            [
                {
                    "slug": "some-older-post",
                    "date": "2026-07-01T10:00:00",
                    "modified": "2026-07-01T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/some-older-post/",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (308, "", {"location": "/blog/some-older-post"}),
                first_hop: (308, "", {"location": "/hub/some-older-post"}),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertNotIn(candidate, urls)
        self.assertNotIn(first_hop, urls)
        self.assertNotIn(final_hub_url, urls)

    def test_union_walks_past_off_section_posts_to_find_in_section_candidates(self):
        """Round 3, item 2: the union must not stop at cms.posts[:blog_count] —
        on a site where another content type dominates recency (MarineRadar's
        /trends), the first blog_count-by-date can be entirely off-section,
        leaving genuinely in-scope posts further down the (date-descending)
        list undiscovered."""
        site = make_site(blog_count=2, cms_api=True)
        off_section_1 = "https://www.travelanimator.com/hub/off-section-1"
        off_section_2 = "https://www.travelanimator.com/hub/off-section-2"
        in_section_1 = "https://www.travelanimator.com/hub/in-section-1"
        in_section_2 = "https://www.travelanimator.com/hub/in-section-2"
        payload = json.dumps(
            [
                {
                    "slug": "off-section-1",
                    "date": "2026-08-03T10:00:00",
                    "modified": "",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/off-section-1",
                },
                {
                    "slug": "off-section-2",
                    "date": "2026-08-02T10:00:00",
                    "modified": "",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/off-section-2",
                },
                {
                    "slug": "in-section-1",
                    "date": "2026-08-01T10:00:00",
                    "modified": "",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/in-section-1",
                },
                {
                    "slug": "in-section-2",
                    "date": "2026-07-31T10:00:00",
                    "modified": "",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/in-section-2",
                },
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                off_section_1: (
                    308,
                    "",
                    {"location": "https://www.travelanimator.com/trends/off-section-1"},
                ),
                off_section_2: (
                    308,
                    "",
                    {"location": "https://www.travelanimator.com/trends/off-section-2"},
                ),
                in_section_1: (200, "<html></html>", None),
                in_section_2: (200, "<html></html>", None),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn(in_section_1, urls)
        self.assertIn(in_section_2, urls)
        self.assertNotIn(off_section_1, urls)
        self.assertNotIn(off_section_2, urls)

    def test_cms_only_candidate_returning_200_is_discovered(self):
        """Round 2, item 1: genuinely published, missing from the listing — I1's
        whole purpose — is included when the live check confirms 200."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/hub/newly-published-blog"
        payload = json.dumps(
            [
                {
                    "slug": "some-other-slug-not-used-for-routing",
                    "date": "2026-07-28T10:00:00",
                    "modified": "2026-07-28T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/newly-published-blog",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (200, "<html></html>", None),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn(candidate, urls)

    def test_cms_only_candidate_returning_404_is_still_discovered(self):
        """Round 2, item 1: published in the CMS but broken on the public site must
        still enter the audited set, so I1 can report the break — only a redirect
        means 'wrong section', a 404 means 'this /hub post is broken'."""
        site = make_site(blog_count=1, cms_api=True)
        candidate = "https://www.travelanimator.com/hub/gone-from-live-site"
        payload = json.dumps(
            [
                {
                    "slug": "some-other-slug-not-used-for-routing",
                    "date": "2026-07-28T10:00:00",
                    "modified": "2026-07-28T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/gone-from-live-site",
                }
            ]
        )
        transport = scripted(
            **{
                cms_url_for(site): (200, payload, {"content-type": "application/json"}),
                candidate: (404, "", None),
            }
        )
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn(candidate, urls)

    def test_listed_cms_candidate_is_not_redundantly_verified(self):
        """Round 2, item 1: a candidate already in the listing set is in scope by
        definition — no HEAD should be spent confirming what's already known."""
        site = make_site(blog_count=1, cms_api=True)
        already_listed = "https://www.travelanimator.com/hub/good-blog"
        payload = json.dumps(
            [
                {
                    "slug": "good-blog",
                    "date": "2026-07-30T10:00:00",
                    "modified": "2026-08-01T10:00:00",
                    "status": "publish",
                    "link": already_listed,
                }
            ]
        )
        transport = scripted(**{cms_url_for(site): (200, payload, {"content-type": "application/json"})})
        urls, ctx = discover(Fetcher(transport), site)
        self.assertIn(already_listed, urls)
        self.assertNotIn(("HEAD", already_listed), transport.calls)

    def test_cms_post_with_unparseable_link_falls_back_to_assembled_url(self):
        """Round 2, item 4: a post.link that makes urlparse raise (e.g. a malformed
        IPv6 host) must degrade to the assembled URL instead of crashing discovery."""
        site = make_site(blog_count=1, cms_api=True)
        payload = json.dumps(
            [
                {
                    "slug": "never-rendered-blog",
                    "date": "2026-07-28T10:00:00",
                    "modified": "2026-07-28T10:00:00",
                    "status": "publish",
                    "link": "http://[::1",
                }
            ]
        )
        transport = scripted(**{cms_url_for(site): (200, payload, {"content-type": "application/json"})})
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn("https://www.travelanimator.com/hub/never-rendered-blog", urls)

    def test_cms_post_under_listing_path_but_missing_from_listing_is_discovered(self):
        """F: I1's whole purpose — a genuinely-published /hub post the listing doesn't
        show yet must still be pulled in, using the CMS's own permalink path (not the
        slug field), so a rewritten slug is still resolved correctly."""
        site = make_site(blog_count=1, cms_api=True)
        payload = json.dumps(
            [
                {
                    "slug": "some-other-slug-not-used-for-routing",
                    "date": "2026-07-28T10:00:00",
                    "modified": "2026-07-28T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/never-rendered-blog",
                }
            ]
        )
        transport = scripted(**{cms_url_for(site): (200, payload, {"content-type": "application/json"})})
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn("https://www.travelanimator.com/hub/never-rendered-blog", urls)
        self.assertNotIn(
            "https://www.travelanimator.com/hub/some-other-slug-not-used-for-routing", urls
        )

    def test_cms_post_with_empty_link_falls_back_to_assembled_url(self):
        """F: a CMS that omits/blanks the link field must degrade to the old slug guess
        rather than discovering nothing."""
        site = make_site(blog_count=1, cms_api=True)
        payload = json.dumps(
            [
                {
                    "slug": "never-rendered-blog",
                    "date": "2026-07-28T10:00:00",
                    "modified": "2026-07-28T10:00:00",
                    "status": "publish",
                    "link": "",
                }
            ]
        )
        transport = scripted(**{cms_url_for(site): (200, payload, {"content-type": "application/json"})})
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn("https://www.travelanimator.com/hub/never-rendered-blog", urls)

    def test_cms_candidate_within_full_listing_but_beyond_truncation_is_not_added(self):
        """Round 4, item 1: 'missing from the listing' must be tested against
        the FULL ctx.listing_urls, not the blog_count-truncated `ordered`
        slice. listing.html has 3 blog anchors; with blog_count=2, `ordered`
        is only the first 2 — but the 3rd ('third-blog') is still genuinely on
        the listing page, just beyond the truncation, so a CMS candidate for
        it must not be added (it isn't missing, and adding it would silently
        double the audited set the way TravelAnimator's real listing did)."""
        site = make_site(blog_count=2, cms_api=True)
        payload = json.dumps(
            [
                {
                    "slug": "third-blog",
                    "date": "2026-07-29T10:00:00",
                    "modified": "2026-07-29T10:00:00",
                    "status": "publish",
                    "link": "https://hub.travelanimator.com/hub/third-blog",
                }
            ]
        )
        transport = scripted(**{cms_url_for(site): (200, payload, {"content-type": "application/json"})})
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertEqual(len(urls), 2)
        self.assertNotIn("https://www.travelanimator.com/hub/third-blog", urls)
        self.assertNotIn(("HEAD", "https://www.travelanimator.com/hub/third-blog"), transport.calls)


class CollectUrlsTest(unittest.TestCase):
    def test_dimension_urls_are_only_og_images(self):
        page = make_page()
        all_urls, dimension_urls = collect_urls([page], make_site())
        self.assertEqual(dimension_urls, {page.og["og:image"]})
        self.assertIn(page.og["og:image"], all_urls)

    def test_deduplicates_across_blogs(self):
        """B: two blogs sharing an identical og:image must collapse to exactly one
        entry, not `len(all_urls) == len(set(all_urls))`, which is tautologically
        true for any set — including an empty one — and would pass even if
        collect_urls did nothing."""
        shared_image = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"
        pages = [
            make_page(url="https://www.travelanimator.com/hub/a", slug="a"),
            make_page(url="https://www.travelanimator.com/hub/b", slug="b"),
        ]
        all_urls, dimension_urls = collect_urls(pages, make_site())
        self.assertEqual(all_urls, {shared_image})
        self.assertEqual(dimension_urls, {shared_image})

    def test_subresources_are_collected_only_when_they_are_origin_assets(self):
        """Important 5 (+ reviewer follow-up): A3's subresource branch can only
        fire if collect_urls gathers page.subresources — but only origin
        *asset* subresources are ever consulted (A3 filters on is_asset_url);
        verifying every non-asset script/style/iframe src would be dozens of
        pointless requests per blog with no rule ever reading the result."""
        asset_subresource = "https://hub.travelanimator.com/wp-content/uploads/lib.js"
        non_asset_subresource = "https://www.travelanimator.com/_next/static/chunk.js"
        page = make_page(subresources=(asset_subresource, non_asset_subresource))
        all_urls, _ = collect_urls([page], make_site())
        self.assertIn(asset_subresource, all_urls)
        self.assertNotIn(non_asset_subresource, all_urls)

    def test_graph_nested_breadcrumb_item_is_collected(self):
        """Important 5: a raw walk of node.data only ever sees the @graph
        container itself, never the nodes inside it — jsonld_nodes() must be
        used so a BreadcrumbList nested inside a top-level @graph block (the
        shape MarineRadar actually uses) still has its itemListElement
        targets collected for verification, keeping E3 alive on that site."""
        breadcrumb_target = "https://www.travelanimator.com/hub/some-post"
        page = make_page(
            jsonld=(
                JsonLdBlock(
                    raw="{}",
                    data={
                        "@context": "https://schema.org",
                        "@graph": [
                            {
                                "@type": "BreadcrumbList",
                                "itemListElement": [
                                    {"@type": "ListItem", "position": 1, "item": {"@id": breadcrumb_target}}
                                ],
                            }
                        ],
                    },
                ),
            ),
        )
        all_urls, _ = collect_urls([page], make_site())
        self.assertIn(breadcrumb_target, all_urls)

    def test_jsonld_sourced_image_is_collected_via_page_images(self):
        """Reviewer follow-up to Important 5: JSON-LD `image` values (spec:182)
        are folded into page.images by parse_blog (source="jsonld"), so
        collect_urls picks them up through its existing page.images loop —
        not through a separate, easily-inert JSON-LD walk of its own. See
        test_seo_parse.py for parse_blog actually producing that ImageRef,
        and AuditTest.test_jsonld_sourced_image_returning_404_produces_b3_finding
        for proof a broken one is actually reported."""
        jsonld_image = "https://hub.travelanimator.com/wp-content/uploads/jsonld-only.jpg"
        page = make_page(images=(ImageRef(url=jsonld_image, source="jsonld"),))
        all_urls, _ = collect_urls([page], make_site())
        self.assertIn(jsonld_image, all_urls)


class EvaluateTest(unittest.TestCase):
    def test_runs_blog_and_run_scoped_rules(self):
        site = make_site()
        pages = [make_page()]
        findings = evaluate(pages, site, {}, __import__("seo_testkit").make_context(), concurrency=4)
        self.assertIsInstance(findings, list)
        self.assertTrue(all(f.rule in RULES_BY_ID for f in findings))

    def test_a_raising_rule_does_not_abort_the_run(self):
        """D: this one broken page raises inside both a blog-scope rule (D3/D4, via
        page.h1s) and a run-scope rule (D7, which also reads page.h1s across all
        pages) — split so a regression that silences only one guard can't hide
        behind the other."""
        site = make_site()
        broken = make_page(headings="not-a-tuple-of-tuples")
        findings = evaluate([broken], site, {}, __import__("seo_testkit").make_context(), concurrency=2)
        h3 = [f for f in findings if f.rule == "H3"]
        blog_scope = [f for f in h3 if f.blog_url is not None]
        run_scope = [f for f in h3 if f.blog_url is None]
        self.assertTrue(blog_scope, "expected an H3 finding from the blog-scope guard (with a blog_url)")
        self.assertTrue(run_scope, "expected an H3 finding from the run-scope guard (blog_url=None)")

    def test_non_ok_page_runs_only_h1_among_blog_scope_rules(self):
        """Critical 1: a non-200 blog page must produce exactly one blog-scope
        finding (H1) — not all 30 blog-scope rules run against an effectively
        empty page, which previously turned one fetch failure into up to 13
        unrelated-looking findings."""
        site = make_site(cms_api=False)
        broken = make_page(response=make_response(status=502))
        findings = evaluate([broken], site, {}, make_context(), concurrency=2)
        blog_rule_ids = {rule.id for rule in BLOG_RULES}
        blog_scope = [f for f in findings if f.rule in blog_rule_ids]
        self.assertEqual(len(blog_scope), 1)
        self.assertEqual(blog_scope[0].rule, "H1")

    def test_site_unreachable_collapses_to_a_single_h1_finding(self):
        """Fix 2: every one of ten pages failing to fetch is one fact (the
        site is unreachable), not ten H1s plus every other run-scoped rule's
        artifacts on top."""
        site = make_site(blog_count=10, cms_api=True)
        pages = [
            make_page(
                url=f"https://www.travelanimator.com/hub/b{i}",
                slug=f"b{i}",
                response=make_response(status=403),
            )
            for i in range(10)
        ]
        findings = evaluate(pages, site, {}, make_context(), concurrency=4)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].rule, "H1")
        self.assertIn("10", findings[0].message)
        self.assertIn("unreachable", findings[0].message)
        self.assertIn("403", findings[0].evidence)
        self.assertIn(site.canonical_host, findings[0].evidence)

    def test_site_unreachable_not_triggered_when_one_page_is_ok(self):
        """Nine 403s and one 200 must NOT collapse — normal evaluation runs."""
        site = make_site(blog_count=10, cms_api=False)
        pages = [
            make_page(
                url=f"https://www.travelanimator.com/hub/b{i}",
                slug=f"b{i}",
                response=make_response(status=403),
            )
            for i in range(9)
        ] + [make_page(url="https://www.travelanimator.com/hub/b9", slug="b9")]
        findings = evaluate(pages, site, {}, make_context(), concurrency=4)
        h1_findings = [f for f in findings if f.rule == "H1"]
        self.assertEqual(len(h1_findings), 9)

    def test_zero_pages_does_not_crash_or_collapse(self):
        site = make_site(blog_count=10, cms_api=False)
        findings = evaluate([], site, {}, make_context(), concurrency=4)
        self.assertFalse(any("unreachable" in f.message for f in findings))


class AuditTest(unittest.TestCase):
    def test_healthy_site_produces_a_summary(self):
        site = make_site(blog_count=3, cms_api=False)
        summary = audit(site, Fetcher(scripted()))
        self.assertEqual(len(summary.pages), 3)
        self.assertIsNone(summary.error)
        self.assertEqual(len(summary.rules), 45)

    def test_summary_carries_the_discovery_context(self):
        """Reviewer follow-up: the report's Configuration table can only show
        whether the listing/sitemap/robots.txt were actually fetched if
        audit() actually wires discover()'s SiteContext onto the summary."""
        site = make_site(blog_count=3, cms_api=False)
        summary = audit(site, Fetcher(scripted()))
        self.assertIsNotNone(summary.ctx)
        self.assertTrue(summary.ctx.listing_ok)
        self.assertTrue(summary.ctx.sitemap_ok)
        self.assertTrue(summary.ctx.robots_ok)

    def test_broken_blog_is_reported_and_siblings_survive(self):
        """Critical 1: strengthened from `any(f.rule == "H1" ...)` — that
        assertion alone passes even when 12 other unrelated findings also
        fire for the same broken blog (which they did, before the fix). The
        broken blog must yield exactly one blog-scope finding, and it must be
        H1; siblings must still be evaluated normally."""
        transport = scripted()
        broken_url = "https://www.travelanimator.com/hub/second-blog"
        transport.fail.add(broken_url)
        summary = audit(make_site(blog_count=3, cms_api=False), Fetcher(transport))
        self.assertEqual(len(summary.pages), 3)
        blog_rule_ids = {rule.id for rule in BLOG_RULES}
        blog_scope = [f for f in summary.findings if f.blog_url == broken_url and f.rule in blog_rule_ids]
        self.assertEqual(len(blog_scope), 1)
        self.assertEqual(blog_scope[0].rule, "H1")
        good = [p for p in summary.pages if p.slug == "good-blog"][0]
        self.assertTrue(good.response.ok)

    def test_jsonld_sourced_image_returning_404_produces_b3_finding(self):
        """Reviewer follow-up to Important 5: a check that cannot fail is
        worse than no check. JSON-LD `image` values must be real images
        parse_blog puts into page.images (source="jsonld") — this asserts
        the full path end to end: a JSON-LD-only image URL that 404s must
        actually produce a B3 finding, not silently pass because nothing
        ever looked at it."""
        jsonld_image = "https://hub.travelanimator.com/wp-content/uploads/2026/07/jsonld-only.jpg"
        blog_html = fixture("good_blog.html").replace(
            "</head>",
            '<script type="application/ld+json">{"@context":"https://schema.org",'
            f'"@type":"BlogPosting","image":"{jsonld_image}"}}</script></head>',
        )
        transport = scripted(
            **{
                "https://www.travelanimator.com/hub/good-blog": (200, blog_html, None),
                jsonld_image: (404, "", None),
            }
        )
        site = make_site(blog_count=1, cms_api=False)
        summary = audit(site, Fetcher(transport))
        b3 = [f for f in summary.findings if f.rule == "B3" and f.evidence == jsonld_image]
        self.assertTrue(b3, "expected a B3 finding for the JSON-LD-sourced image")
        self.assertEqual(b3[0].severity, SEVERITY_ERROR)

    def test_suppressed_rule_is_reported_but_does_not_open_the_gate(self):
        transport = scripted(
            **{
                "https://www.travelanimator.com/hub/good-blog": (
                    200,
                    fixture("good_blog.html").replace(
                        "</head>",
                        '<script>{"l":"https://hub.travelanimator.com/wp-json/wp/v2/posts"}</script></head>',
                    ),
                    None,
                )
            }
        )
        site = make_site(blog_count=1, cms_api=False, suppress=["A2"])
        summary = audit(site, Fetcher(transport))
        active, suppressed = partition(summary.findings, site)
        self.assertTrue(any(f.rule == "A2" for f in suppressed))
        self.assertFalse(any(f.rule == "A2" for f in active))

    def test_unreachable_cms_yields_one_info_and_no_parity_errors(self):
        transport = scripted()
        transport.fail.add(CMS)
        site = make_site(blog_count=3, cms_api=True)
        summary = audit(site, Fetcher(transport))
        parity = [f for f in summary.findings if f.rule.startswith("I")]
        self.assertEqual([f.rule for f in parity], ["I4"])
        self.assertEqual(counts(parity)["info"], 1)

    def test_info_only_run_keeps_the_gate_shut(self):
        site = make_site(blog_count=3, cms_api=False)
        summary = audit(site, Fetcher(scripted()))
        info_only = [f for f in summary.findings if f.severity == "info"]
        summary.findings = info_only
        self.assertFalse(gate(summary))

    def test_targeted_cms_lookup_finds_post_and_i3_does_not_fire(self):
        """Round 4, item 2: 'good-blog' is absent from the (empty, here) CMS
        window, but a targeted single-slug lookup finds it — I3 must not treat
        absence from a truncated window as proof no CMS post exists."""
        site = make_site(blog_count=1, cms_api=True)
        lookup_url = cms_slug_lookup_url_for(site, "good-blog")
        transport = scripted(
            **{
                cms_url_for(site): (200, json.dumps([]), {"content-type": "application/json"}),
                lookup_url: (
                    200,
                    json.dumps(
                        [
                            {
                                "slug": "good-blog",
                                "date": "2026-07-30T10:00:00",
                                "modified": "2026-08-01T10:00:00",
                                "status": "publish",
                                "link": "https://hub.travelanimator.com/hub/good-blog",
                            }
                        ]
                    ),
                    {"content-type": "application/json"},
                ),
            }
        )
        summary = audit(site, Fetcher(transport))
        self.assertFalse(any(f.rule == "I3" for f in summary.findings))

    def test_slug_absent_from_window_and_targeted_lookup_still_fires_i3(self):
        """Round 4, item 2: if the targeted lookup also finds nothing, the
        blog genuinely has no CMS post and I3 must still fire."""
        site = make_site(blog_count=1, cms_api=True)
        lookup_url = cms_slug_lookup_url_for(site, "good-blog")
        transport = scripted(
            **{
                cms_url_for(site): (200, json.dumps([]), {"content-type": "application/json"}),
                lookup_url: (200, json.dumps([]), {"content-type": "application/json"}),
            }
        )
        summary = audit(site, Fetcher(transport))
        self.assertTrue(any(f.rule == "I3" for f in summary.findings))

    def test_failed_targeted_cms_lookup_leaves_cms_ok_true_and_does_not_fabricate(self):
        """Round 4, item 2: a failed targeted lookup is one missing slug, not
        a CMS outage — cms.ok must stay True (flipping it would silence group
        I entirely via _parity_enabled), and no post may be fabricated."""
        site = make_site(blog_count=1, cms_api=True)
        lookup_url = cms_slug_lookup_url_for(site, "good-blog")
        transport = scripted(**{cms_url_for(site): (200, json.dumps([]), {"content-type": "application/json"})})
        transport.fail.add(lookup_url)
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertEqual(ctx.cms.posts, ())

    def test_failed_targeted_cms_lookup_is_recorded_as_lookup_failed(self):
        """Critical 3: discover() must expose the failed slug on the
        SiteContext so check_i3 can skip it, distinct from a genuine absence."""
        site = make_site(blog_count=1, cms_api=True)
        lookup_url = cms_slug_lookup_url_for(site, "good-blog")
        transport = scripted(**{cms_url_for(site): (200, json.dumps([]), {"content-type": "application/json"})})
        transport.fail.add(lookup_url)
        _, ctx = discover(Fetcher(transport), site)
        self.assertIn("good-blog", ctx.cms_lookup_failed)

    def test_failed_targeted_cms_lookup_does_not_produce_i3(self):
        """Critical 3: the docstring at seo_blog_audit.py previously claimed a
        targeted lookup could never fail as a request — untrue. A failed
        lookup (non-200/unparseable) must not be reported by I3 as a
        zombie; only a lookup that genuinely finds nothing may."""
        site = make_site(blog_count=1, cms_api=True)
        lookup_url = cms_slug_lookup_url_for(site, "good-blog")
        transport = scripted(**{cms_url_for(site): (200, json.dumps([]), {"content-type": "application/json"})})
        transport.fail.add(lookup_url)
        summary = audit(site, Fetcher(transport))
        self.assertFalse(any(f.rule == "I3" for f in summary.findings))
        self.assertTrue(summary.pages[0].response.ok)


def _write_offline_config(directory: Path) -> Path:
    config_path = directory / "sites.json"
    config_path.write_text(
        json.dumps(
            [
                {
                    "name": "offline-site",
                    "label": "Offline Site",
                    "canonical_host": "www.example.com",
                    "origin_host": "hub.example.com",
                    "origin_asset_prefixes": ["/wp-content/uploads/"],
                    "allowed_subdomains": [],
                    "listing_path": "/hub",
                    "sitemap_url": "https://www.example.com/sitemap.xml",
                    "blog_count": 1,
                    "cms_api": False,
                    "suppress": [],
                    "thresholds": {},
                }
            ]
        ),
        encoding="utf-8",
    )
    return config_path


class MainTest(unittest.TestCase):
    """C: main()'s exit-0 contract had no tests — this is exactly how A survived."""

    def test_help_exits_zero(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = main(["--help"])
        self.assertEqual(code, 0)

    def test_missing_site_argument_still_exits_zero(self):
        """Regression test for A: argparse raises SystemExit(2) for a missing
        required argument. Failed before the fix (SystemExit escaped main());
        passes after."""
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = main([])
        self.assertEqual(code, 0)

    def test_non_integer_blog_count_still_exits_zero(self):
        """Regression test for A: a hand-typed workflow_dispatch value like
        '--blog-count notanumber' must not turn a cron run red. Failed before
        the fix (SystemExit escaped main()); passes after."""
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = main(["--site", "travelanimator", "--blog-count", "notanumber"])
        self.assertEqual(code, 0)

    def test_nonexistent_config_still_exits_zero(self):
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            output = str(Path(tmp) / "report.html")
            with contextlib.redirect_stderr(stderr):
                code = main(
                    [
                        "--site",
                        "travelanimator",
                        "--config",
                        str(Path(tmp) / "missing.json"),
                        "--output",
                        output,
                    ]
                )
        self.assertEqual(code, 0)
        self.assertIn("FileNotFoundError", stderr.getvalue())

    def test_offline_run_writes_a_report_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = _write_offline_config(Path(tmp))
            output = str(Path(tmp) / "report.html")
            code = main(
                [
                    "--site",
                    "offline-site",
                    "--config",
                    str(config_path),
                    "--output",
                    output,
                    "--offline",
                    tmp,
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue(Path(output).exists())
            self.assertGreater(Path(output).stat().st_size, 0)

    def test_unwritable_output_path_still_exits_zero(self):
        """Round 2, item 2: Path(args.output).write_text(...) is outside the
        try/except — an unwritable --output (e.g. a parent directory that doesn't
        exist) must not turn a successful audit into a non-zero exit."""
        with tempfile.TemporaryDirectory() as tmp:
            config_path = _write_offline_config(Path(tmp))
            bad_output = str(Path(tmp) / "no-such-subdir" / "report.html")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = main(
                    [
                        "--site",
                        "offline-site",
                        "--config",
                        str(config_path),
                        "--output",
                        bad_output,
                        "--offline",
                        tmp,
                    ]
                )
        self.assertEqual(code, 0)
        self.assertFalse(Path(bad_output).exists())

    def test_crash_inside_audit_after_site_loads_is_also_written_to_stderr(self):
        """Round 2, item 3: previously only the 'site never loaded' branch wrote the
        traceback to stderr — a crash inside audit() itself only reached the HTML
        report, undiagnosable from the Actions log without downloading it."""
        with tempfile.TemporaryDirectory() as tmp:
            config_path = _write_offline_config(Path(tmp))
            output = str(Path(tmp) / "report.html")
            stderr = io.StringIO()
            with patch("seo_blog_audit.audit", side_effect=RuntimeError("boom")):
                with contextlib.redirect_stderr(stderr):
                    code = main(
                        [
                            "--site",
                            "offline-site",
                            "--config",
                            str(config_path),
                            "--output",
                            output,
                            "--offline",
                            tmp,
                        ]
                    )
            self.assertEqual(code, 0)
            self.assertIn("RuntimeError: boom", stderr.getvalue())
            self.assertTrue(Path(output).exists())

    def test_crash_produces_an_h3_finding_and_nonzero_error_count(self):
        """Important 7: ADR 0003 and spec:349 both require the crash path to
        emit a real H3 finding carrying the traceback. Before the fix,
        findings=[] on this path meant gate() fired (via summary.error) but
        error_count/warn_count both read 0 — the Telegram caption would say
        "0 errors, 0 warnings" for a run that never completed."""
        prior_github_output = os.environ.get("GITHUB_OUTPUT")
        with tempfile.TemporaryDirectory() as tmp:
            config_path = _write_offline_config(Path(tmp))
            output = str(Path(tmp) / "report.html")
            gh_output = Path(tmp) / "gh_output.txt"
            os.environ["GITHUB_OUTPUT"] = str(gh_output)
            try:
                with patch("seo_blog_audit.audit", side_effect=RuntimeError("boom")):
                    with contextlib.redirect_stderr(io.StringIO()):
                        code = main(
                            [
                                "--site",
                                "offline-site",
                                "--config",
                                str(config_path),
                                "--output",
                                output,
                                "--offline",
                                tmp,
                            ]
                        )
                content = gh_output.read_text(encoding="utf-8")
                report_content = Path(output).read_text(encoding="utf-8")
            finally:
                if prior_github_output is None:
                    os.environ.pop("GITHUB_OUTPUT", None)
                else:
                    os.environ["GITHUB_OUTPUT"] = prior_github_output
        self.assertEqual(code, 0)
        self.assertIn("error_count=1", content)
        self.assertIn("has_findings=true", content)
        self.assertIn("boom", report_content)


class WriteGithubOutputTest(unittest.TestCase):
    def setUp(self):
        self._prior_github_output = os.environ.get("GITHUB_OUTPUT")

    def tearDown(self):
        if self._prior_github_output is None:
            os.environ.pop("GITHUB_OUTPUT", None)
        else:
            os.environ["GITHUB_OUTPUT"] = self._prior_github_output

    def _summary(self):
        site = make_site(blog_count=1, cms_api=False)
        return audit(site, Fetcher(scripted()))

    def test_noop_when_github_output_unset(self):
        os.environ.pop("GITHUB_OUTPUT", None)
        write_github_output(self._summary())  # must not raise, must not write anywhere

    def test_writes_expected_keys_when_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "gh_output.txt"
            os.environ["GITHUB_OUTPUT"] = str(target)
            write_github_output(self._summary())
            content = target.read_text(encoding="utf-8")
        for key in ("has_findings", "error_count", "warn_count", "info_count", "suppressed_count", "label"):
            self.assertIn(f"{key}=", content)


class FixtureTransportTest(unittest.TestCase):
    def test_missing_fixture_file_returns_200_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            transport = FixtureTransport(tmp)
            response = transport("GET", "https://example.com/no-such-page", {}, 10)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, "")

    def test_nonexistent_directory_returns_200_empty(self):
        transport = FixtureTransport("/tmp/does-not-exist-seo-fixture-dir-xyz-12345")
        response = transport("GET", "https://example.com/no-such-page", {}, 10)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, "")


if __name__ == "__main__":
    unittest.main()
