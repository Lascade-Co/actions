import unittest

from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    Anchor,
    ImageRef,
    JsonLdBlock,
)
from seo_testkit import BLOG_URL, make_context, make_page, make_response, make_site, make_status

RULES = {rule.id: rule for rule in BLOG_RULES_ABC + RUN_RULES_ABC}
ASSET = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"
ORIGIN_POST = "https://hub.travelanimator.com/some-post/"


def run_rule(rule_id, page=None, *, site=None, urls=None, ctx=None, pages=None):
    rule = RULES[rule_id]
    site = site or make_site()
    ctx = ctx or make_context()
    urls = urls or {}
    if rule.scope == "run":
        return rule.fn(pages if pages is not None else [page or make_page()], site, urls, ctx)
    return rule.fn(page or make_page(), site, urls, ctx)


def anchor(url, text="link"):
    return Anchor(href=url, url=url, text=text)


class GroupATest(unittest.TestCase):
    def test_a1_fires_on_origin_anchor(self):
        page = make_page(anchors=(anchor(ORIGIN_POST),))
        findings = run_rule("A1", page)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)
        self.assertIn("hub.travelanimator.com", findings[0].evidence)

    def test_a1_fires_on_origin_canonical(self):
        page = make_page(canonicals=(ORIGIN_POST,))
        self.assertTrue(run_rule("A1", page))

    def test_a1_silent_when_origin_url_is_an_asset(self):
        page = make_page(anchors=(anchor(ASSET),))
        self.assertEqual(run_rule("A1", page), [])

    def test_a1_silent_on_good_page(self):
        self.assertEqual(run_rule("A1", make_page()), [])

    def test_a2_fires_on_cms_api_url_in_markup(self):
        raw = '<script>{"link":"https://hub.travelanimator.com/wp-json/wp/v2/categories"}</script>'
        findings = run_rule("A2", make_page(raw_html=raw))
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)

    def test_a2_detects_percent_encoded_origin_urls(self):
        raw = "/_next/image?url=https%3A%2F%2Fhub.travelanimator.com%2Fwp-json%2Fwp%2Fv2%2Fposts"
        self.assertTrue(run_rule("A2", make_page(raw_html=raw)))

    def test_a2_silent_when_only_assets_appear(self):
        raw = f'<img src="{ASSET}"/>'
        self.assertEqual(run_rule("A2", make_page(raw_html=raw)), [])

    def test_a3_fires_when_asset_is_404(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=404)}
        findings = run_rule("A3", page, urls=urls)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_a3_fires_when_asset_is_not_an_image(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=200, content_type="text/html")}
        self.assertTrue(run_rule("A3", page, urls=urls))

    def test_a3_silent_for_healthy_webp_served_from_png_url(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=200, content_type="image/webp")}
        self.assertEqual(run_rule("A3", page, urls=urls), [])

    def test_a3_silent_on_unverified_bot_blocked_host(self):
        """Important 4: A3 was the only status rule ignoring `verified` — a
        403/429/timeout from the origin must not be reported as a broken
        asset, matching B1/B3/B6's gating."""
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=403, verified=False)}
        self.assertEqual(run_rule("A3", page, urls=urls), [])

    def test_a4_fires_on_bare_apex(self):
        page = make_page(anchors=(anchor("https://travelanimator.com/pricing"),))
        self.assertEqual(run_rule("A4", page)[0].severity, SEVERITY_ERROR)

    def test_a4_fires_on_http_scheme(self):
        page = make_page(anchors=(anchor("http://www.travelanimator.com/pricing"),))
        self.assertTrue(run_rule("A4", page))

    def test_a4_fires_on_vercel_preview_host(self):
        page = make_page(anchors=(anchor("https://travelanimator-git-main.vercel.app/hub"),))
        self.assertTrue(run_rule("A4", page))

    def test_a4_fires_on_wordpress_query_id(self):
        page = make_page(anchors=(anchor("https://www.travelanimator.com/?p=123"),))
        self.assertTrue(run_rule("A4", page))

    def test_a4_silent_on_allowlisted_subdomain(self):
        page = make_page(anchors=(anchor("https://support.travelanimator.com/x"),))
        self.assertEqual(run_rule("A4", page), [])

    def test_a4_silent_on_third_party_host(self):
        page = make_page(anchors=(anchor("https://apps.apple.com/app/id1"),))
        self.assertEqual(run_rule("A4", page), [])

    def test_a5_reports_allowlisted_subdomain_as_info(self):
        page = make_page(anchors=(anchor("https://support.travelanimator.com/x"),))
        findings = run_rule("A5", page)
        self.assertEqual(findings[0].severity, SEVERITY_INFO)

    def test_a5_silent_without_subdomain_links(self):
        self.assertEqual(run_rule("A5", make_page()), [])


class GroupBTest(unittest.TestCase):
    INTERNAL = "https://www.travelanimator.com/pricing"
    EXTERNAL = "https://apps.apple.com/app/id1"

    def test_b1_fires_on_broken_internal_link(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        urls = {self.INTERNAL: make_status(self.INTERNAL, status=404)}
        self.assertEqual(run_rule("B1", page, urls=urls)[0].severity, SEVERITY_ERROR)

    def test_b1_silent_on_200(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        urls = {self.INTERNAL: make_status(self.INTERNAL)}
        self.assertEqual(run_rule("B1", page, urls=urls), [])

    def test_b1_silent_on_unverified_bot_blocked_host(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        urls = {self.INTERNAL: make_status(self.INTERNAL, status=403, verified=False)}
        self.assertEqual(run_rule("B1", page, urls=urls), [])

    def test_b2_fires_on_any_3xx(self):
        for code in (301, 302, 308):
            page = make_page(anchors=(anchor(self.INTERNAL),))
            urls = {self.INTERNAL: make_status(self.INTERNAL, status=code, location="/pricing/")}
            findings = run_rule("B2", page, urls=urls)
            self.assertEqual(findings[0].severity, SEVERITY_WARN, code)
            self.assertIn("/pricing/", findings[0].evidence)

    def test_b2_silent_on_direct_hit(self):
        page = make_page(anchors=(anchor(self.INTERNAL),))
        self.assertEqual(run_rule("B2", page, urls={self.INTERNAL: make_status(self.INTERNAL)}), [])

    def test_b3_fires_on_broken_image(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=500)}
        self.assertEqual(run_rule("B3", page, urls=urls)[0].severity, SEVERITY_ERROR)

    def test_b3_silent_on_healthy_image(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, content_type="image/png")}
        self.assertEqual(run_rule("B3", page, urls=urls), [])

    def test_b3_silent_on_unverified_bot_blocked_host(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, status=403, verified=False)}
        self.assertEqual(run_rule("B3", page, urls=urls), [])

    def test_b4_fires_when_blog_absent_from_sitemap(self):
        ctx = make_context(sitemap_urls=frozenset({"https://www.travelanimator.com/hub/other"}))
        self.assertEqual(run_rule("B4", make_page(), ctx=ctx)[0].severity, SEVERITY_ERROR)

    def test_b4_ignores_trailing_slash_difference(self):
        ctx = make_context(sitemap_urls=frozenset({BLOG_URL + "/"}))
        self.assertEqual(run_rule("B4", make_page(), ctx=ctx), [])

    def test_b4_silent_when_sitemap_unavailable(self):
        ctx = make_context(sitemap_urls=frozenset(), sitemap_ok=False)
        self.assertEqual(run_rule("B4", make_page(), ctx=ctx), [])

    def test_b5_fires_when_not_linked_from_listing(self):
        ctx = make_context(listing_urls=("https://www.travelanimator.com/hub/other",))
        self.assertEqual(run_rule("B5", make_page(), ctx=ctx)[0].severity, SEVERITY_ERROR)

    def test_b5_silent_when_listed(self):
        self.assertEqual(run_rule("B5", make_page(), ctx=make_context()), [])

    def test_b6_reports_dead_external_link_as_info(self):
        page = make_page(anchors=(anchor(self.EXTERNAL),))
        urls = {self.EXTERNAL: make_status(self.EXTERNAL, status=404)}
        self.assertEqual(run_rule("B6", page, urls=urls)[0].severity, SEVERITY_INFO)

    def test_b6_silent_on_unverified_bot_blocked_host(self):
        page = make_page(anchors=(anchor(self.EXTERNAL),))
        urls = {self.EXTERNAL: make_status(self.EXTERNAL, status=403, verified=False)}
        self.assertEqual(run_rule("B6", page, urls=urls), [])

    def test_b6_silent_on_timeout(self):
        page = make_page(anchors=(anchor(self.EXTERNAL),))
        urls = {self.EXTERNAL: make_status(self.EXTERNAL, status=0, verified=False, error="Timeout")}
        self.assertEqual(run_rule("B6", page, urls=urls), [])


class GroupCTest(unittest.TestCase):
    def test_c1_fires_on_noindex_meta(self):
        page = make_page(robots_meta=("noindex, follow",))
        self.assertEqual(run_rule("C1", page)[0].severity, SEVERITY_ERROR)

    def test_c1_fires_on_x_robots_tag_header(self):
        page = make_page(response=make_response(headers={"x-robots-tag": "noindex"}))
        self.assertTrue(run_rule("C1", page))

    def test_c1_silent_on_index_follow(self):
        self.assertEqual(run_rule("C1", make_page(robots_meta=("index, follow",))), [])

    def test_c2_fires_when_canonical_missing(self):
        self.assertEqual(run_rule("C2", make_page(canonicals=()))[0].severity, SEVERITY_ERROR)

    def test_c2_fires_on_duplicate_canonicals(self):
        self.assertTrue(run_rule("C2", make_page(canonicals=(BLOG_URL, BLOG_URL + "?x=1"))))

    def test_c2_fires_when_canonical_is_not_self_referencing(self):
        # Same host as canonical_host, different path: exercises the
        # not-self-referencing branch, not the wrong-host branch.
        page = make_page(canonicals=("https://www.travelanimator.com/hub/other",))
        findings = run_rule("C2", page)
        self.assertTrue(findings)
        self.assertIn("self-referencing", findings[0].message)

    def test_c2_fires_when_canonical_points_at_wrong_host(self):
        # Neither the canonical host nor the blog's own host: exercises the
        # wrong-host branch specifically, distinct from not-self-referencing.
        page = make_page(canonicals=("https://hub.travelanimator.com/hub/good-blog",))
        findings = run_rule("C2", page)
        self.assertTrue(findings)
        self.assertIn("not the canonical host", findings[0].message)
        self.assertNotIn("self-referencing", findings[0].message)

    def test_c2_fires_on_non_https_canonical(self):
        page = make_page(canonicals=("http://www.travelanimator.com/hub/good-blog",))
        self.assertTrue(run_rule("C2", page))

    def test_c2_silent_on_self_referencing_canonical(self):
        self.assertEqual(run_rule("C2", make_page()), [])

    def test_c3_fires_when_blog_path_disallowed(self):
        ctx = make_context(robots_txt="User-agent: *\nDisallow: /hub/\nSitemap: https://x/sitemap.xml\n")
        findings = run_rule("C3", ctx=ctx, pages=[make_page()])
        self.assertTrue(any(f.severity == SEVERITY_ERROR for f in findings))

    def test_c3_fires_when_sitemap_directive_absent(self):
        """Severity item: a missing Sitemap directive is hygiene, not broken
        crawl correctness, and is run-scoped — reporting it at error would
        page every day forever on any site that simply never had it. Fixed
        to SEVERITY_WARN; the per-blog disallow finding stays at error."""
        ctx = make_context(robots_txt="User-agent: *\nAllow: /\n")
        findings = run_rule("C3", ctx=ctx, pages=[make_page()])
        sitemap_findings = [f for f in findings if "Sitemap" in f.message]
        self.assertTrue(sitemap_findings)
        self.assertEqual(sitemap_findings[0].severity, SEVERITY_WARN)

    def test_c3_silent_on_healthy_robots(self):
        self.assertEqual(run_rule("C3", ctx=make_context(), pages=[make_page()]), [])

    def test_c3_silent_when_robots_unavailable(self):
        ctx = make_context(robots_txt="", robots_ok=False)
        self.assertEqual(run_rule("C3", ctx=ctx, pages=[make_page()]), [])

    def test_c4_fires_on_thin_body(self):
        page = make_page(article_text="Page not found")
        self.assertEqual(run_rule("C4", page)[0].severity, SEVERITY_ERROR)

    def test_c4_fires_on_not_found_phrasing_despite_length(self):
        page = make_page(article_text="This page could not be found. " + " ".join(["filler"] * 400))
        self.assertTrue(run_rule("C4", page))

    def test_c4_silent_on_real_article(self):
        self.assertEqual(run_rule("C4", make_page()), [])

    def test_c4_silent_on_non_200_response(self):
        """Critical 1: soft-404 (spec:193) is defined as a 200 response with
        thin content. A non-200 page is H1's finding, not a soft 404 — a 502
        with an empty body must not also fire C4."""
        page = make_page(response=make_response(status=502), article_text="")
        self.assertEqual(run_rule("C4", page), [])


class RegistryTest(unittest.TestCase):
    def test_all_ids_present_exactly_once(self):
        ids = [rule.id for rule in BLOG_RULES_ABC + RUN_RULES_ABC]
        expected = [f"A{i}" for i in range(1, 6)] + [f"B{i}" for i in range(1, 7)] + [f"C{i}" for i in range(1, 5)]
        self.assertEqual(sorted(ids), sorted(expected))
        self.assertEqual(len(ids), len(set(ids)))

    def test_every_rule_has_group_and_slug(self):
        for rule in BLOG_RULES_ABC + RUN_RULES_ABC:
            self.assertIn(rule.group, ("A", "B", "C"))
            self.assertTrue(rule.slug and "_" not in rule.slug)


if __name__ == "__main__":
    unittest.main()
