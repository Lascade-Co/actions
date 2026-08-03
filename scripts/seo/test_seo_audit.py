import json
import unittest

from seo_blog_audit import audit, collect_urls, discover, evaluate
from seo_checks import ALL_RULES, BLOG_RULES, RULES_BY_ID, RUN_RULES
from seo_fetch import Fetcher
from seo_model import Response
from seo_report import counts, gate, partition
from seo_testkit import fixture, make_page, make_site

LISTING = "https://www.travelanimator.com/hub"
ROBOTS = "https://www.travelanimator.com/robots.txt"
SITEMAP = "https://www.travelanimator.com/sitemap.xml"
CMS = (
    "https://hub.travelanimator.com/wp-json/wp/v2/posts"
    "?per_page=20&_fields=slug,date,modified,status&orderby=date&order=desc"
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
        cms_url = (
            f"https://{site.origin_host}/wp-json/wp/v2/posts"
            f"?per_page={site.blog_count * 2}&_fields=slug,date,modified,status&orderby=date&order=desc"
        )
        transport = scripted(**{cms_url: (200, fixture("cms_posts.json"), {"content-type": "application/json"})})
        urls, ctx = discover(Fetcher(transport), site)
        self.assertTrue(ctx.cms.ok)
        self.assertIn("https://www.travelanimator.com/hub/never-rendered-blog", urls)

    def test_listing_failure_is_survivable(self):
        transport = scripted()
        transport.fail.add(LISTING)
        urls, ctx = discover(Fetcher(transport), make_site(cms_api=False))
        self.assertEqual(urls, [])
        self.assertFalse(ctx.listing_ok)


class CollectUrlsTest(unittest.TestCase):
    def test_dimension_urls_are_only_og_images(self):
        page = make_page()
        all_urls, dimension_urls = collect_urls([page], make_site())
        self.assertEqual(dimension_urls, {page.og["og:image"]})
        self.assertIn(page.og["og:image"], all_urls)

    def test_deduplicates_across_blogs(self):
        pages = [make_page(url="https://www.travelanimator.com/hub/a"), make_page(url="https://www.travelanimator.com/hub/b")]
        all_urls, _ = collect_urls(pages, make_site())
        self.assertEqual(len(all_urls), len(set(all_urls)))


class EvaluateTest(unittest.TestCase):
    def test_runs_blog_and_run_scoped_rules(self):
        site = make_site()
        pages = [make_page()]
        findings = evaluate(pages, site, {}, __import__("seo_testkit").make_context(), concurrency=4)
        self.assertIsInstance(findings, list)
        self.assertTrue(all(f.rule in RULES_BY_ID for f in findings))

    def test_a_raising_rule_does_not_abort_the_run(self):
        site = make_site()
        broken = make_page(headings="not-a-tuple-of-tuples")
        findings = evaluate([broken], site, {}, __import__("seo_testkit").make_context(), concurrency=2)
        self.assertTrue(any(f.rule == "H3" for f in findings))


class AuditTest(unittest.TestCase):
    def test_healthy_site_produces_a_summary(self):
        site = make_site(blog_count=3, cms_api=False)
        summary = audit(site, Fetcher(scripted()))
        self.assertEqual(len(summary.pages), 3)
        self.assertIsNone(summary.error)
        self.assertEqual(len(summary.rules), 45)

    def test_broken_blog_is_reported_and_siblings_survive(self):
        transport = scripted()
        transport.fail.add("https://www.travelanimator.com/hub/second-blog")
        summary = audit(make_site(blog_count=3, cms_api=False), Fetcher(transport))
        self.assertEqual(len(summary.pages), 3)
        self.assertTrue(any(f.rule == "H1" for f in summary.findings))
        good = [p for p in summary.pages if p.slug == "good-blog"][0]
        self.assertTrue(good.response.ok)

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


if __name__ == "__main__":
    unittest.main()
