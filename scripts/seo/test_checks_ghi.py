import unittest
from datetime import datetime, timedelta, timezone

from seo_checks_ghi import BLOG_RULES_GHI, RUN_RULES_GHI
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    Anchor,
    CmsPost,
    CmsSnapshot,
    ImageRef,
    JsonLdBlock,
)
from seo_testkit import BLOG_URL, make_context, make_page, make_response, make_site, make_status

RULES = {rule.id: rule for rule in BLOG_RULES_GHI + RUN_RULES_GHI}
ASSET = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"


def article_block(modified: str):
    return (
        JsonLdBlock(
            raw="{}",
            data={
                "@context": "https://schema.org",
                "@type": "BlogPosting",
                "headline": "x",
                "dateModified": modified,
            },
        ),
    )


def run_rule(rule_id, page=None, *, site=None, urls=None, ctx=None, pages=None):
    rule = RULES[rule_id]
    site = site or make_site()
    ctx = ctx or make_context()
    urls = urls or {}
    if rule.scope == "run":
        return rule.fn(pages if pages is not None else [page or make_page()], site, urls, ctx)
    return rule.fn(page or make_page(), site, urls, ctx)


class GroupGTest(unittest.TestCase):
    def test_g1_fires_on_http_subresource(self):
        page = make_page(subresources=("http://cdn.example.com/a.js",))
        self.assertEqual(run_rule("G1", page)[0].severity, SEVERITY_ERROR)

    def test_g1_silent_on_https_and_relative(self):
        page = make_page(subresources=("https://cdn.example.com/a.js", "/local.css"))
        self.assertEqual(run_rule("G1", page), [])

    def test_g2_fires_on_oversized_html(self):
        page = make_page(response=make_response(body="x" * (600 * 1024)))
        self.assertEqual(run_rule("G2", page)[0].severity, SEVERITY_WARN)

    def test_g2_silent_on_normal_html(self):
        page = make_page(response=make_response(body="x" * 1000))
        self.assertEqual(run_rule("G2", page), [])

    def test_g3_fires_when_asset_lacks_cache_control(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, content_type="image/webp", cache_control=None)}
        self.assertEqual(run_rule("G3", page, urls=urls)[0].severity, SEVERITY_WARN)

    def test_g3_silent_when_cache_control_present(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="x"),))
        urls = {ASSET: make_status(ASSET, content_type="image/webp", cache_control="public, max-age=31536000")}
        self.assertEqual(run_rule("G3", page, urls=urls), [])

    def test_g4_fires_on_generic_anchor_text(self):
        page = make_page(
            anchors=(Anchor(href="/pricing", url="https://www.travelanimator.com/pricing", text="click here"),)
        )
        self.assertEqual(run_rule("G4", page)[0].severity, SEVERITY_WARN)

    def test_g4_fires_on_empty_anchor_without_image_alt(self):
        page = make_page(
            anchors=(Anchor(href="/pricing", url="https://www.travelanimator.com/pricing", text="", image_alts=("",)),)
        )
        self.assertTrue(run_rule("G4", page))

    def test_g4_silent_when_image_only_anchor_has_alt(self):
        page = make_page(
            anchors=(
                Anchor(
                    href="/pricing",
                    url="https://www.travelanimator.com/pricing",
                    text="",
                    image_alts=("Pricing plans",),
                ),
            )
        )
        self.assertEqual(run_rule("G4", page), [])

    def test_g4_silent_on_descriptive_text(self):
        page = make_page(
            anchors=(Anchor(href="/pricing", url="https://www.travelanimator.com/pricing", text="pricing page"),)
        )
        self.assertEqual(run_rule("G4", page), [])

    def test_g4_ignores_external_anchors(self):
        page = make_page(anchors=(Anchor(href="https://x.com/a", url="https://x.com/a", text="click here"),))
        self.assertEqual(run_rule("G4", page), [])

    def test_g5_reports_slow_ttfb_as_info_only(self):
        page = make_page(response=make_response(ttfb_ms=4000))
        findings = run_rule("G5", page)
        self.assertEqual(findings[0].severity, SEVERITY_INFO)

    def test_g5_silent_when_fast(self):
        self.assertEqual(run_rule("G5", make_page(response=make_response(ttfb_ms=200))), [])


class GroupHTest(unittest.TestCase):
    def test_h1_fires_on_non_200_blog(self):
        page = make_page(response=make_response(status=404))
        self.assertEqual(run_rule("H1", page)[0].severity, SEVERITY_ERROR)

    def test_h1_fires_on_transport_failure(self):
        page = make_page(response=make_response(status=0, error="Timeout"))
        findings = run_rule("H1", page)
        self.assertIn("Timeout", findings[0].evidence)

    def test_h1_silent_on_200(self):
        self.assertEqual(run_rule("H1", make_page()), [])

    def test_h2_fires_when_fewer_blogs_than_configured(self):
        ctx = make_context(listing_urls=(BLOG_URL,))
        findings = run_rule("H2", pages=[make_page()], ctx=ctx, site=make_site(blog_count=10))
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)
        self.assertIn("1", findings[0].message)

    def test_h2_silent_when_enough_blogs_found(self):
        urls = tuple(f"https://www.travelanimator.com/hub/b{i}" for i in range(10))
        ctx = make_context(listing_urls=urls)
        self.assertEqual(run_rule("H2", pages=[make_page()], ctx=ctx, site=make_site(blog_count=10)), [])

    def test_h2_fires_error_when_listing_unavailable(self):
        """Fix 3: a failed listing fetch is the most likely way this audit
        goes silently blind (ADR 0003) — it must not go quiet."""
        ctx = make_context(listing_urls=(), listing_ok=False)
        site = make_site()
        findings = run_rule("H2", pages=[make_page()], ctx=ctx, site=site)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)
        self.assertIn("could not be fetched", findings[0].message)
        self.assertEqual(findings[0].evidence, site.listing_url)

    def test_h3_is_registered_for_the_orchestrator_to_emit(self):
        self.assertIn("H3", RULES)
        self.assertEqual(RULES["H3"].slug, "harness-error")


class GroupITest(unittest.TestCase):
    def cms(self, posts=(), *, ok=True, enabled=True, error=None):
        return CmsSnapshot(posts=tuple(posts), ok=ok, error=error, enabled=enabled)

    def test_i1_fires_when_published_post_is_missing_from_www(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="never-rendered", status="publish")]))
        page = make_page(url="https://www.travelanimator.com/hub/never-rendered", slug="never-rendered",
                         response=make_response(status=404))
        findings = run_rule("I1", pages=[page], ctx=ctx)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_i1_fires_when_published_post_is_absent_from_listing(self):
        ctx = make_context(
            listing_urls=("https://www.travelanimator.com/hub/other",),
            cms=self.cms([CmsPost(slug="good-blog", status="publish")]),
        )
        page = make_page()
        self.assertTrue(run_rule("I1", pages=[page], ctx=ctx))

    def test_i1_silent_for_draft_posts(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="draft-post", status="draft")]))
        self.assertEqual(run_rule("I1", pages=[make_page()], ctx=ctx), [])

    def test_i1_silent_when_healthy(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish")]))
        self.assertEqual(run_rule("I1", pages=[make_page()], ctx=ctx), [])

    def test_i2_fires_when_render_is_stale(self):
        now = datetime.now(timezone.utc)
        cms_modified = now.strftime("%Y-%m-%dT%H:%M:%S")
        rendered = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish", modified=cms_modified)]))
        page = make_page(jsonld=article_block(rendered))
        findings = run_rule("I2", pages=[page], ctx=ctx)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)

    def test_i2_silent_within_tolerance(self):
        now = datetime.now(timezone.utc)
        stamp = now.strftime("%Y-%m-%dT%H:%M:%S")
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish", modified=stamp)]))
        page = make_page(jsonld=article_block(stamp + "+00:00"))
        self.assertEqual(run_rule("I2", pages=[page], ctx=ctx), [])

    def test_i2_silent_when_cms_unreachable(self):
        """Genuine gate test: a non-empty posts snapshot whose CMS `modified` timestamp
        is far newer than the rendered `dateModified` — the exact stale-render condition
        from test_i2_fires_when_render_is_stale — but with ok=False. If _parity_enabled
        were removed, this would fire a warn finding just like that test does."""
        now = datetime.now(timezone.utc)
        cms_modified = now.strftime("%Y-%m-%dT%H:%M:%S")
        rendered = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ctx = make_context(
            cms=self.cms(
                [CmsPost(slug="good-blog", status="publish", modified=cms_modified)],
                ok=False,
                error="HTTP 500",
            )
        )
        page = make_page(jsonld=article_block(rendered))
        self.assertEqual(run_rule("I2", pages=[page], ctx=ctx), [])

    def test_i3_fires_on_zombie_page(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="something-else", status="publish")]))
        findings = run_rule("I3", pages=[make_page()], ctx=ctx)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)

    def test_i3_fires_when_cms_status_is_not_publish(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="draft")]))
        self.assertTrue(run_rule("I3", pages=[make_page()], ctx=ctx))

    def test_i3_silent_when_cms_unreachable(self):
        """The critical case: an empty post list must not read as every blog being a zombie."""
        ctx = make_context(cms=self.cms([], ok=False, error="HTTP 500"))
        self.assertEqual(run_rule("I3", pages=[make_page()], ctx=ctx), [])

    def test_i3_silent_when_slug_lookup_failed(self):
        """Critical 3: a slug whose targeted CMS lookup failed (non-200 or
        unparseable — a request failure, not a genuine absence) must be
        skipped by I3 rather than reported as a zombie."""
        ctx = make_context(cms=self.cms([]), cms_lookup_failed=frozenset({"good-blog"}))
        self.assertEqual(run_rule("I3", pages=[make_page()], ctx=ctx), [])

    def test_i3_fires_when_absent_slug_is_not_in_the_failed_set(self):
        """Critical 3: a slug genuinely absent from both the window and the
        targeted lookup — i.e. not recorded as failed — must still fire I3."""
        ctx = make_context(cms=self.cms([]), cms_lookup_failed=frozenset({"some-other-slug"}))
        self.assertTrue(run_rule("I3", pages=[make_page()], ctx=ctx))

    def test_i1_silent_when_cms_unreachable(self):
        """Genuine gate test: a non-empty posts snapshot with a page whose non-200
        response would otherwise trigger I1 (published in CMS but www errors out).
        An empty posts list would make this test vacuous — check_i1 iterates
        ctx.cms.posts, so with no posts the loop body never runs regardless of the
        gate. This fixture gives the loop something to act on."""
        ctx = make_context(
            cms=self.cms([CmsPost(slug="good-blog", status="publish")], ok=False, error="HTTP 500")
        )
        page = make_page(response=make_response(status=500))
        self.assertEqual(run_rule("I1", pages=[page], ctx=ctx), [])

    def test_i4_reports_exactly_one_info_when_cms_unreachable(self):
        ctx = make_context(cms=self.cms([], ok=False, error="HTTP 401"))
        findings = run_rule("I4", pages=[make_page()], ctx=ctx)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, SEVERITY_INFO)
        self.assertIn("401", findings[0].evidence)

    def test_i4_silent_when_cms_healthy(self):
        ctx = make_context(cms=self.cms([CmsPost(slug="good-blog", status="publish")]))
        self.assertEqual(run_rule("I4", pages=[make_page()], ctx=ctx), [])

    def test_group_i_silent_entirely_when_disabled(self):
        ctx = make_context(cms=self.cms([], ok=False, enabled=False))
        for rule_id in ("I1", "I2", "I3", "I4"):
            self.assertEqual(run_rule(rule_id, pages=[make_page()], ctx=ctx), [], rule_id)


class RegistryTest(unittest.TestCase):
    def test_ids_present_exactly_once(self):
        ids = sorted(rule.id for rule in BLOG_RULES_GHI + RUN_RULES_GHI)
        expected = sorted(
            [f"G{i}" for i in range(1, 6)] + [f"H{i}" for i in range(1, 4)] + [f"I{i}" for i in range(1, 5)]
        )
        self.assertEqual(ids, expected)

    def test_parity_and_discovery_rules_are_run_scoped(self):
        self.assertEqual({r.id for r in RUN_RULES_GHI}, {"H2", "H3", "I1", "I2", "I3", "I4"})


if __name__ == "__main__":
    unittest.main()
