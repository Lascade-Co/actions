"""Tests for the Release Blog pipeline. No network, no model, no git."""

from __future__ import annotations

import json
import re
import tempfile
import unittest

from release_blog_digest import (
    PATCH_BYTE_CAP,
    build_digest,
    filter_patch,
    marketing_version_from_tag,
    truncate,
)
from release_blog_cms import (
    LinkCandidate,
    build_payload,
    fetch_link_candidates,
    find_by_marker,
    release_marker,
    write_draft,
)
from release_blog_check import (
    PRE_PUBLISH_RULE_IDS,
    Attempt,
    better,
    local_checks,
    render_report,
    run_rules,
    validate,
)
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    resolve_site_for_repo,
    site_config_from_dict,
)
from seo_testkit import fixture, make_site

CONFIG = [
    {
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
        "repos": ["Lascade-Co/travel-animator-android"],
        "suppress": [],
        "thresholds": {},
    },
    {
        "name": "marineradar",
        "label": "MarineRadar",
        "canonical_host": "www.marineradar.com",
        "origin_host": "hub.marineradar.com",
        "origin_asset_prefixes": ["/wp-content/uploads/"],
        "allowed_subdomains": [],
        "listing_path": "/hub",
        "sitemap_url": "https://www.marineradar.com/sitemap.xml",
        "blog_count": 10,
        "cms_api": True,
        "repos": ["Lascade-Co/marine-radar-android"],
        "suppress": [],
        "thresholds": {},
    },
]

MARKER = "release-blog: Lascade-Co/travel-animator-android@3.9.3"

GOOD_META = {
    "title": "Reshape any route by dragging a waypoint",
    "slug": "drag-waypoints-to-reshape-routes",
    "excerpt": (
        "Waypoints are draggable now, so a route that took four taps to fix takes one. "
        "Here is how the new editor behaves on long multi-stop trips."
    ),
    "media": [
        {
            "id": "m1",
            "kind": "image",
            "width": 1600,
            "height": 900,
            "alt": "The route editor with a waypoint handle dragged onto a coastal road, the elevation strip updating beneath the map",
            "prompt": "Wide screenshot-style render of a mobile map editor. 16:9. No text overlays.",
        }
    ],
}

CANDIDATES = [
    LinkCandidate(url="https://www.travelanimator.com/hub/route-animation-guide", slug="route-animation-guide", title="Route guide"),
    LinkCandidate(url="https://www.travelanimator.com/hub/best-travel-maps", slug="best-travel-maps", title="Map styles"),
    LinkCandidate(url="https://www.travelanimator.com/hub/export-for-instagram", slug="export-for-instagram", title="Export"),
]


def ids(findings):
    return {finding.rule for finding in findings}


def write_config(entries) -> str:
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(entries, handle)
    handle.close()
    return handle.name


class SiteResolutionTest(unittest.TestCase):
    def test_repos_defaults_to_empty_when_absent(self):
        raw = dict(CONFIG[0])
        del raw["repos"]
        self.assertEqual(site_config_from_dict(raw).repos, ())

    def test_repos_are_lowercased(self):
        raw = dict(CONFIG[0], repos=["Lascade-Co/Travel-Animator-Android"])
        self.assertEqual(site_config_from_dict(raw).repos, ("lascade-co/travel-animator-android",))

    def test_resolves_a_single_match(self):
        site, reason = resolve_site_for_repo(write_config(CONFIG), "Lascade-Co/travel-animator-android")
        self.assertEqual(site.name, "travelanimator")
        self.assertEqual(reason, "")

    def test_match_is_case_insensitive(self):
        site, _ = resolve_site_for_repo(write_config(CONFIG), "lascade-co/TRAVEL-ANIMATOR-ANDROID")
        self.assertEqual(site.name, "travelanimator")

    def test_no_match_returns_a_reason(self):
        site, reason = resolve_site_for_repo(write_config(CONFIG), "Lascade-Co/unknown-app")
        self.assertIsNone(site)
        self.assertIn("no site config", reason)
        self.assertIn("Lascade-Co/unknown-app", reason)

    def test_two_matches_is_a_config_error(self):
        entries = json.loads(json.dumps(CONFIG))
        entries[1]["repos"] = ["Lascade-Co/travel-animator-android"]
        site, reason = resolve_site_for_repo(write_config(entries), "Lascade-Co/travel-animator-android")
        self.assertIsNone(site)
        self.assertIn("two site configs", reason)
        self.assertIn("travelanimator", reason)
        self.assertIn("marineradar", reason)


class MarketingVersionTest(unittest.TestCase):
    def test_bare_version_passes_through(self):
        self.assertEqual(marketing_version_from_tag("3.9.3"), "3.9.3")

    def test_leading_v_is_stripped(self):
        self.assertEqual(marketing_version_from_tag("v3.9.3"), "3.9.3")

    def test_non_version_tag_falls_back_to_the_raw_tag(self):
        self.assertEqual(marketing_version_from_tag("release-candidate"), "release-candidate")

    def test_empty_tag_is_empty(self):
        self.assertEqual(marketing_version_from_tag(""), "")


class FilterPatchTest(unittest.TestCase):
    def setUp(self):
        self.filtered = filter_patch(fixture("release_blog_sample.diff"))

    def test_keeps_the_real_source_change(self):
        self.assertIn("WaypointDragHandler.kt", self.filtered)
        self.assertIn("elevation.recompute()", self.filtered)

    def test_keeps_default_strings_because_they_are_the_new_ui_copy(self):
        self.assertIn("values/strings.xml", self.filtered)
        self.assertIn("Drag a waypoint to reshape your route", self.filtered)

    def test_drops_translated_strings(self):
        self.assertNotIn("values-de", self.filtered)
        self.assertNotIn("Routeneditor", self.filtered)

    def test_drops_lockfiles(self):
        self.assertNotIn("gradle.lockfile", self.filtered)

    def test_drops_generated_code(self):
        self.assertNotIn("l10n.g.dart", self.filtered)

    def test_drops_binary_hunks(self):
        self.assertNotIn("logo.png", self.filtered)


class TruncateTest(unittest.TestCase):
    def test_short_text_is_untouched(self):
        self.assertEqual(truncate("short"), "short")

    def test_long_text_is_capped_and_says_so(self):
        result = truncate("x" * 300, limit=100)
        self.assertTrue(result.startswith("x" * 100))
        self.assertIn("diff truncated", result)
        self.assertIn("100 of 300 bytes", result)

    def test_default_cap_is_200kb(self):
        self.assertEqual(PATCH_BYTE_CAP, 200 * 1024)


class BuildDigestTest(unittest.TestCase):
    def test_digest_carries_all_four_sections_in_order(self):
        calls = []

        class FakeCompleted:
            def __init__(self, stdout):
                self.stdout = stdout
                self.returncode = 0

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if "log" in cmd:
                return FakeCompleted("abc1234 feat: drag waypoints\ndef5678 fix: elevation strip\n")
            if "--stat" in cmd:
                return FakeCompleted(" 4 files changed, 12 insertions(+)\n")
            return FakeCompleted(fixture("release_blog_sample.diff"))

        digest = build_digest(
            "/tmp/repo", "v3.9.2", "v3.9.3", "- Drag waypoints to reshape routes\n", run=fake_run
        )
        self.assertLess(digest.index("Release notes"), digest.index("Commits"))
        self.assertLess(digest.index("Commits"), digest.index("Changed files"))
        self.assertLess(digest.index("Changed files"), digest.index("Filtered diff"))
        self.assertIn("Drag waypoints to reshape routes", digest)
        self.assertIn("feat: drag waypoints", digest)
        self.assertIn("4 files changed", digest)
        self.assertIn("WaypointDragHandler.kt", digest)
        self.assertNotIn("values-de", digest)

    def test_git_failure_degrades_to_an_empty_section(self):
        class FakeCompleted:
            stdout = ""
            returncode = 128

        digest = build_digest("/tmp/repo", "v3.9.2", "v3.9.3", "notes\n", run=lambda cmd, **kw: FakeCompleted())
        self.assertIn("notes", digest)
        self.assertIn("unavailable", digest)


class FakeHttp:
    """Record calls and replay queued ``(status, body)`` pairs."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, *, headers=None, json_body=None, auth=None, timeout=20):
        self.calls.append({"method": method, "url": url, "json_body": json_body, "auth": auth})
        return self.responses.pop(0) if self.responses else (500, "no response queued")


class ReleaseMarkerTest(unittest.TestCase):
    def test_marker_names_the_repo_and_tag(self):
        self.assertEqual(
            release_marker("Lascade-Co/travel-animator-android", "3.9.3"),
            MARKER,
        )


class LinkCandidateTest(unittest.TestCase):
    def test_reads_title_url_and_excerpt_from_the_cms(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        candidates, note = fetch_link_candidates(make_site(), http=http)
        self.assertEqual(note, "")
        self.assertEqual(len(candidates), 3)
        self.assertEqual(candidates[0].title, "How to animate a road trip route")
        self.assertEqual(candidates[0].url, "https://www.travelanimator.com/hub/route-animation-guide")
        self.assertEqual(candidates[0].slug, "route-animation-guide")
        self.assertIn("GPX", candidates[0].excerpt)

    def test_excerpt_html_is_stripped(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        candidates, _ = fetch_link_candidates(make_site(), http=http)
        self.assertNotIn("<p>", candidates[0].excerpt)

    def test_requests_only_published_blogs(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        fetch_link_candidates(make_site(), http=http)
        self.assertIn("status=publish", http.calls[0]["url"])
        self.assertIn("per_page=100", http.calls[0]["url"])
        self.assertIn("hub.travelanimator.com", http.calls[0]["url"])

    def test_falls_back_to_the_sitemap_when_the_cms_fails(self):
        sitemap = (
            '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            "<url><loc>https://www.travelanimator.com/hub/route-animation-guide</loc></url>"
            "<url><loc>https://www.travelanimator.com/pricing</loc></url>"
            "</urlset>"
        )
        http = FakeHttp((503, "service unavailable"), (200, sitemap))
        candidates, note = fetch_link_candidates(make_site(), http=http)
        self.assertIn("sitemap", note)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].slug, "route-animation-guide")
        self.assertEqual(candidates[0].title, "")

    def test_returns_empty_when_both_sources_fail(self):
        http = FakeHttp((503, ""), (503, ""))
        candidates, note = fetch_link_candidates(make_site(), http=http)
        self.assertEqual(candidates, [])
        self.assertIn("no link candidates", note)


class MarkerSearchTest(unittest.TestCase):
    def test_finds_a_draft_carrying_the_marker(self):
        body = json.dumps(
            [{"id": 41, "status": "draft", "slug": "drag-waypoints", "content": {"rendered": f"<p>x</p><!-- {MARKER} -->"}}]
        )
        http = FakeHttp((200, body))
        found = find_by_marker(make_site(), MARKER, http=http, auth=("u", "p"))
        self.assertEqual(found["id"], 41)
        self.assertEqual(found["status"], "draft")

    def test_ignores_a_fuzzy_search_hit_without_the_exact_marker(self):
        body = json.dumps(
            [{"id": 9, "status": "draft", "slug": "other", "content": {"rendered": "<!-- release-blog: other/repo@1.0.0 -->"}}]
        )
        http = FakeHttp((200, body))
        self.assertIsNone(find_by_marker(make_site(), MARKER, http=http, auth=("u", "p")))

    def test_search_is_authenticated_and_covers_drafts(self):
        http = FakeHttp((200, "[]"))
        find_by_marker(make_site(), MARKER, http=http, auth=("u", "p"))
        self.assertIn("status=draft%2Cpublish", http.calls[0]["url"])
        self.assertEqual(http.calls[0]["auth"], ("u", "p"))

    def test_a_failed_search_reads_as_not_found(self):
        http = FakeHttp((401, "unauthorised"))
        self.assertIsNone(find_by_marker(make_site(), MARKER, http=http, auth=("u", "p")))


class WriteDraftTest(unittest.TestCase):
    def test_payload_is_a_draft(self):
        payload = build_payload(
            {"title": "T", "slug": "s", "excerpt": "E"}, "<h2>Body</h2>"
        )
        self.assertEqual(payload["status"], "draft")
        self.assertEqual(payload["content"], "<h2>Body</h2>")
        self.assertNotIn("date", payload)

    def test_creates_a_new_draft_when_no_id_is_given(self):
        http = FakeHttp((201, json.dumps({"id": 77})))
        ok, detail = write_draft(make_site(), {"status": "draft"}, http=http, auth=("u", "p"))
        self.assertTrue(ok)
        self.assertIn("77", detail)
        self.assertTrue(http.calls[0]["url"].endswith("/wp-json/wp/v2/posts"))

    def test_overwrites_the_given_draft_id(self):
        http = FakeHttp((200, json.dumps({"id": 41})))
        ok, _ = write_draft(make_site(), {"status": "draft"}, http=http, auth=("u", "p"), draft_id=41)
        self.assertTrue(ok)
        self.assertTrue(http.calls[0]["url"].endswith("/wp-json/wp/v2/posts/41"))

    def test_a_failed_write_reports_status_and_body(self):
        http = FakeHttp((403, "forbidden"))
        ok, detail = write_draft(make_site(), {"status": "draft"}, http=http, auth=("u", "p"))
        self.assertFalse(ok)
        self.assertIn("403", detail)
        self.assertIn("forbidden", detail)


class AllowlistTest(unittest.TestCase):
    def test_the_allowlist_is_exactly_the_documented_set(self):
        self.assertEqual(
            set(PRE_PUBLISH_RULE_IDS),
            {
                "A1", "A2", "A4", "A5",
                "B7",
                "C1", "C4",
                "D1", "D2", "D3", "D4", "D5", "D6",
                "E1", "E4",
                "G1", "G2", "G4",
            },
        )
        self.assertEqual(len(PRE_PUBLISH_RULE_IDS), 18)
        self.assertEqual(len(set(PRE_PUBLISH_RULE_IDS)), 18, "duplicate id in the allowlist")

    def test_d9_is_excluded_because_a_draft_may_hold_placeholders(self):
        self.assertNotIn("D9", PRE_PUBLISH_RULE_IDS)

    def test_no_allowlisted_rule_reads_the_network(self):
        import inspect

        from seo_checks import RULES_BY_ID

        for rule_id in PRE_PUBLISH_RULE_IDS:
            source = inspect.getsource(RULES_BY_ID[rule_id].fn)
            body = source.split(":", 1)[1]
            self.assertNotIn("urls.get", body, f"{rule_id} reads the network")
            self.assertNotIn("urls[", body, f"{rule_id} reads the network")


class RunRulesTest(unittest.TestCase):
    def test_a_clean_fragment_produces_no_error_or_warn(self):
        findings = run_rules(make_site(), GOOD_META, fixture("release_blog_good.html"))
        bad = [finding for finding in findings if finding.severity in (SEVERITY_ERROR, SEVERITY_WARN)]
        self.assertEqual(bad, [], f"unexpected findings: {[(f.rule, f.message) for f in bad]}")

    def test_a_bad_fragment_trips_the_expected_rules(self):
        findings = run_rules(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"))
        found = ids(findings)
        self.assertIn("D4", found)
        self.assertIn("D5", found)
        self.assertIn("D6", found)
        self.assertIn("B7", found)
        self.assertIn("G4", found)
        self.assertIn("A1", found)

    def test_a_short_title_trips_d1(self):
        findings = run_rules(make_site(), dict(GOOD_META, title="Waypoints"), fixture("release_blog_good.html"))
        self.assertIn("D1", ids(findings))

    def test_an_over_long_excerpt_trips_d2(self):
        findings = run_rules(make_site(), dict(GOOD_META, excerpt="x" * 200), fixture("release_blog_good.html"))
        self.assertIn("D2", ids(findings))

    def test_suppressed_rules_are_reported_at_info(self):
        site = make_site(suppress=["G4"])
        findings = run_rules(site, dict(GOOD_META), fixture("release_blog_bad.html"))
        g4 = [finding for finding in findings if finding.rule == "G4"]
        self.assertTrue(g4)
        self.assertTrue(all(finding.severity == SEVERITY_INFO for finding in g4))


class LocalChecksTest(unittest.TestCase):
    def test_clean_draft_passes(self):
        findings = local_checks(make_site(), GOOD_META, fixture("release_blog_good.html"), CANDIDATES)
        self.assertEqual([finding for finding in findings if finding.severity != SEVERITY_INFO], [])

    def test_a_fabricated_internal_link_is_an_error(self):
        html = fixture("release_blog_good.html").replace(
            "https://www.travelanimator.com/hub/best-travel-maps",
            "https://www.travelanimator.com/hub/best-travel-routes",
        )
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "not a link candidate" in f.message for f in findings))

    def test_a_modified_candidate_url_is_an_error(self):
        html = fixture("release_blog_good.html").replace(
            "https://www.travelanimator.com/hub/best-travel-maps",
            "https://www.travelanimator.com/hub/best-travel-maps?ref=draft",
        )
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "not a link candidate" in f.message for f in findings))

    def test_a_slug_colliding_with_a_candidate_is_an_error(self):
        meta = dict(GOOD_META, slug="route-animation-guide")
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "slug" in f.message for f in findings))

    def test_script_in_the_fragment_is_an_error(self):
        html = fixture("release_blog_good.html") + "<script>alert(1)</script>"
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "script" in f.message for f in findings))

    def test_an_unresolved_media_id_is_an_error(self):
        html = fixture("release_blog_good.html").replace('data-media-id="m1"', 'data-media-id="m7"')
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "m7" in f.message for f in findings))

    def test_zero_placeholders_is_an_error(self):
        html = re.sub(r"<figure>.*?</figure>", "", fixture("release_blog_good.html"), flags=re.DOTALL)
        findings = local_checks(make_site(), dict(GOOD_META, media=[]), html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "placeholder" in f.message for f in findings))

    def test_more_than_six_placeholders_is_an_error(self):
        meta = dict(GOOD_META, media=[dict(GOOD_META["media"][0], id=f"m{n}") for n in range(7)])
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "placeholder" in f.message for f in findings))

    def test_a_missing_release_marker_is_an_error(self):
        html = fixture("release_blog_good.html").replace(
            "<!-- release-blog: Lascade-Co/travel-animator-android@3.9.3 -->", ""
        )
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "marker" in f.message for f in findings))

    def test_a_four_word_alt_is_a_warning(self):
        meta = dict(GOOD_META, media=[dict(GOOD_META["media"][0], alt="The route editor screen")])
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_WARN and "alt" in f.message for f in findings))

    def test_a_generic_alt_opener_is_a_warning(self):
        meta = dict(
            GOOD_META,
            media=[dict(GOOD_META["media"][0], alt="Screenshot of the app showing the editor screen")],
        )
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_WARN and "alt" in f.message for f in findings))


class ScoringTest(unittest.TestCase):
    def attempt(self, errors, warns):
        return Attempt(meta={}, html="", findings=[], errors=errors, warns=warns)

    def test_fewer_errors_wins(self):
        first, second = self.attempt(2, 0), self.attempt(1, 9)
        self.assertIs(better(first, second), second)

    def test_equal_errors_fewer_warns_wins(self):
        first, second = self.attempt(1, 3), self.attempt(1, 1)
        self.assertIs(better(first, second), second)

    def test_a_full_tie_keeps_the_first(self):
        first, second = self.attempt(1, 1), self.attempt(1, 1)
        self.assertIs(better(first, second), first)


class ValidateTest(unittest.TestCase):
    def test_validate_counts_errors_and_warns(self):
        attempt = validate(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        self.assertGreater(attempt.errors, 0)
        self.assertEqual(attempt.score, (attempt.errors, attempt.warns))

    def test_a_clean_draft_scores_zero(self):
        attempt = validate(make_site(), GOOD_META, fixture("release_blog_good.html"), CANDIDATES)
        self.assertEqual(attempt.score, (0, 0), f"{[(f.rule, f.message) for f in attempt.findings]}")

    def test_report_names_every_attempt_and_the_choice(self):
        first = validate(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        second = validate(make_site(), GOOD_META, fixture("release_blog_good.html"), CANDIDATES)
        report = render_report([first, second], second, ["CMS read clean"])
        self.assertIn("attempt 1", report)
        self.assertIn("attempt 2", report)
        self.assertIn("chose attempt 2", report)
        self.assertIn("CMS read clean", report)


if __name__ == "__main__":
    unittest.main()
