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
from seo_model import Response
from seo_report import counts, gate, partition
from seo_testkit import fixture, make_page, make_site

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
