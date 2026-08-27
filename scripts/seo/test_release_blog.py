"""Tests for the Release Blog pipeline. No network, no model, no git."""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import release_blog
from release_blog_digest import (
    PATCH_BYTE_CAP,
    build_digest,
    filter_patch,
    marketing_version_from_tag,
    previous_tag,
    truncate,
)
from release_blog_draft import build_prompt, parse_output, run_codex
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

BASE_PROMPT = "# Codex Draft Prompt\n\nWrite two files.\n"


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

    def test_previous_tag_ignores_newer_debug_tags(self):
        class FakeCompleted:
            returncode = 0
            stdout = "debug-pr-602\n4.0.141\ndebug-pr-601\n4.0.140\n"

        self.assertEqual(
            previous_tag("/tmp/repo", "4.0.142", run=lambda cmd, **kwargs: FakeCompleted()),
            "4.0.141",
        )


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

    def test_origin_permalink_is_mapped_to_the_public_blog_url(self):
        body = json.dumps(
            [{
                "slug": "route-animation-guide",
                "link": "https://hub.travelanimator.com/route-animation-guide/",
                "title": {"rendered": "Route animation guide"},
                "excerpt": {"rendered": ""},
            }]
        )
        candidates, _ = fetch_link_candidates(make_site(), http=FakeHttp((200, body)))
        self.assertEqual(
            candidates[0].url,
            "https://www.travelanimator.com/hub/route-animation-guide/",
        )

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


class BuildPromptTest(unittest.TestCase):
    def prompt(self, **over):
        kwargs = {
            "base_prompt": BASE_PROMPT,
            "site": make_site(),
            "marketing_version": "3.9.3",
            "marker": MARKER,
            "digest": "## Release notes\n\n- Drag waypoints\n",
            "candidates": CANDIDATES,
            "out_dir": "/tmp/out",
        }
        kwargs.update(over)
        return build_prompt(**kwargs)

    def test_base_prompt_comes_first_and_verbatim(self):
        self.assertTrue(self.prompt().startswith(BASE_PROMPT))

    def test_run_context_names_the_output_dir_marker_and_host(self):
        text = self.prompt()
        self.assertIn("/tmp/out", text)
        self.assertIn(MARKER, text)
        self.assertIn("www.travelanimator.com", text)
        self.assertIn("3.9.3", text)

    def test_candidates_are_listed_with_titles_and_urls(self):
        text = self.prompt()
        self.assertIn("https://www.travelanimator.com/hub/route-animation-guide", text)
        self.assertIn("Route guide", text)

    def test_digest_is_included(self):
        self.assertIn("Drag waypoints", self.prompt())

    def test_retry_prompt_carries_the_previous_html_and_each_finding(self):
        previous = validate(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        text = self.prompt(previous=previous.html, findings=previous.findings)
        self.assertIn("previous attempt", text.lower())
        self.assertIn("New stuff", text)
        for item in previous.findings:
            if item.severity in (SEVERITY_ERROR, SEVERITY_WARN):
                self.assertIn(item.message[:40], text)

    def test_retry_prompt_omits_info_findings(self):
        site = make_site(suppress=["G4"])
        previous = validate(site, dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        text = self.prompt(previous=previous.html, findings=previous.findings)
        self.assertNotIn("suppressed for travelanimator", text)


class ParseOutputTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def write(self, name, content):
        Path(self.dir, name).write_text(content, encoding="utf-8")

    def test_reads_both_files(self):
        self.write("blog.json", json.dumps(GOOD_META))
        self.write("blog.html", "<h2>Body</h2>")
        meta, html, error = parse_output(self.dir)
        self.assertEqual(error, "")
        self.assertEqual(meta["slug"], GOOD_META["slug"])
        self.assertEqual(html, "<h2>Body</h2>")

    def test_missing_html_is_an_error(self):
        self.write("blog.json", json.dumps(GOOD_META))
        meta, html, error = parse_output(self.dir)
        self.assertIsNone(meta)
        self.assertIn("blog.html", error)

    def test_missing_json_is_an_error(self):
        self.write("blog.html", "<h2>Body</h2>")
        _meta, _html, error = parse_output(self.dir)
        self.assertIn("blog.json", error)

    def test_unparseable_json_is_an_error(self):
        self.write("blog.json", "{not json")
        self.write("blog.html", "<h2>Body</h2>")
        _meta, _html, error = parse_output(self.dir)
        self.assertIn("blog.json", error)

    def test_empty_html_is_an_error(self):
        self.write("blog.json", json.dumps(GOOD_META))
        self.write("blog.html", "   \n")
        _meta, _html, error = parse_output(self.dir)
        self.assertIn("empty", error)


class RunCodexTest(unittest.TestCase):
    def test_invokes_codex_with_workspace_write_and_the_prompt_on_stdin(self):
        seen = {}

        class Result:
            returncode = 0
            stdout = "done"
            stderr = ""

        def fake_run(cmd, **kwargs):
            seen["cmd"] = cmd
            seen["input"] = kwargs.get("input")
            seen["cwd"] = kwargs.get("cwd")
            seen["env"] = kwargs.get("env")
            return Result()

        with patch.dict(os.environ, {"CMS_APP_PASSWORD": "must-not-reach-codex"}):
            ok, detail = run_codex("PROMPT TEXT", "/tmp/out", run=fake_run)
        self.assertTrue(ok)
        self.assertIn("codex", seen["cmd"][0])
        self.assertIn("--sandbox", seen["cmd"])
        self.assertIn("workspace-write", seen["cmd"])
        self.assertIn("--ephemeral", seen["cmd"])
        self.assertIn("--skip-git-repo-check", seen["cmd"])
        self.assertEqual(seen["input"], "PROMPT TEXT")
        self.assertEqual(seen["cwd"], "/tmp/out")
        self.assertNotIn("CMS_APP_PASSWORD", seen["env"])

    def test_a_non_zero_exit_reports_the_stderr_tail(self):
        class Result:
            returncode = 1
            stdout = ""
            stderr = "not logged in"

        ok, detail = run_codex("PROMPT", "/tmp/out", run=lambda cmd, **kw: Result())
        self.assertFalse(ok)
        self.assertIn("not logged in", detail)

    def test_a_missing_codex_binary_is_not_an_exception(self):
        def fake_run(cmd, **kwargs):
            raise FileNotFoundError("codex")

        ok, detail = run_codex("PROMPT", "/tmp/out", run=fake_run)
        self.assertFalse(ok)
        self.assertIn("codex", detail)

    def test_reports_start_and_periodic_status_while_codex_runs(self):
        class Result:
            returncode = 0
            stdout = "done"
            stderr = ""

        def slow_run(cmd, **kwargs):
            time.sleep(0.04)
            return Result()

        messages = []
        ok, _detail = run_codex(
            "PROMPT",
            "/tmp/out",
            run=slow_run,
            status=messages.append,
            status_interval=0.01,
        )
        self.assertTrue(ok)
        self.assertIn("generation started", messages[0])
        self.assertTrue(any("still generating" in message for message in messages))


class CliTest(unittest.TestCase):
    def setUp(self):
        self.out = tempfile.mkdtemp()
        self.config = write_config(CONFIG)

    def args(self, *extra):
        return [
            "--repo", "Lascade-Co/travel-animator-android",
            "--config", self.config,
            "--tag", "3.9.3",
            "--out", self.out,
            "--prompt", "../../data/RELEASE_BLOG.md",
            "--diff-file", str(Path("fixtures/release_blog_sample.diff")),
            "--notes-text", "- Drag waypoints to reshape routes",
            *extra,
        ]

    def fake_codex(self, meta=None, html=None):
        payload = json.dumps(meta or GOOD_META)
        body = html if html is not None else fixture("release_blog_good.html")

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            Path(self.out, "blog.json").write_text(payload, encoding="utf-8")
            Path(self.out, "blog.html").write_text(body, encoding="utf-8")
            return Result()

        return runner

    def test_dry_run_writes_artifacts_and_performs_no_write(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        code = release_blog.main(self.args("--dry-run"), http=http, run=self.fake_codex())
        self.assertEqual(code, 0)
        self.assertTrue(Path(self.out, "blog.html").exists())
        self.assertTrue(Path(self.out, "wp-payload.json").exists())
        self.assertTrue(Path(self.out, "validation.txt").exists())
        self.assertTrue(Path(self.out, "prompt.md").exists())
        self.assertEqual([call for call in http.calls if call["method"] == "POST"], [])

    def test_dry_run_payload_is_a_draft_with_the_generated_content(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        release_blog.main(self.args("--dry-run"), http=http, run=self.fake_codex())
        payload = json.loads(Path(self.out, "wp-payload.json").read_text())
        self.assertEqual(payload["body"]["status"], "draft")
        self.assertIn("waypoint", payload["body"]["content"].lower())
        self.assertIn("hub.travelanimator.com", payload["url"])

    def test_publish_creates_a_new_draft(self):
        http = FakeHttp(
            (200, "[]"),
            (200, fixture("release_blog_candidates.json")),
            (201, json.dumps({"id": 77})),
        )
        code = release_blog.main(
            self.args("--publish", "--cms-user", "u", "--cms-password", "p"),
            http=http,
            run=self.fake_codex(),
        )
        self.assertEqual(code, 0)
        writes = [call for call in http.calls if call["method"] == "POST"]
        self.assertEqual(len(writes), 1)
        self.assertTrue(writes[0]["url"].endswith("/wp-json/wp/v2/posts"))

    def test_publish_overwrites_an_existing_draft(self):
        existing = json.dumps(
            [{"id": 41, "status": "draft", "slug": "old", "content": {"rendered": f"<!-- {MARKER} -->"}}]
        )
        http = FakeHttp(
            (200, existing),
            (200, fixture("release_blog_candidates.json")),
            (200, json.dumps({"id": 41})),
        )
        release_blog.main(
            self.args("--publish", "--cms-user", "u", "--cms-password", "p"),
            http=http,
            run=self.fake_codex(),
        )
        writes = [call for call in http.calls if call["method"] == "POST"]
        self.assertTrue(writes[0]["url"].endswith("/posts/41"))

    def test_a_published_marker_stops_before_generating(self):
        existing = json.dumps(
            [{"id": 41, "status": "publish", "slug": "live", "content": {"rendered": f"<!-- {MARKER} -->"}}]
        )
        http = FakeHttp((200, existing))
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)
            raise AssertionError("codex must not run when the release is already published")

        code = release_blog.main(
            self.args("--publish", "--cms-user", "u", "--cms-password", "p"),
            http=http,
            run=runner,
        )
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])
        self.assertEqual([call for call in http.calls if call["method"] == "POST"], [])

    def test_missing_credentials_generate_but_do_not_write(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        code = release_blog.main(self.args("--publish"), http=http, run=self.fake_codex())
        self.assertEqual(code, 0)
        self.assertTrue(Path(self.out, "blog.html").exists())
        self.assertEqual([call for call in http.calls if call["method"] == "POST"], [])
        self.assertIn("credential", Path(self.out, "validation.txt").read_text().lower())

    def test_an_unmapped_repo_exits_zero_with_no_artifacts(self):
        code = release_blog.main(
            [
                "--repo", "Lascade-Co/some-other-app",
                "--config", self.config,
                "--tag", "1.0.0",
                "--out", self.out,
                "--dry-run",
            ],
            http=FakeHttp(),
            run=self.fake_codex(),
        )
        self.assertEqual(code, 0)
        self.assertFalse(Path(self.out, "blog.html").exists())

    def test_an_ambiguous_repo_exits_zero_with_no_artifacts(self):
        entries = json.loads(json.dumps(CONFIG))
        entries[1]["repos"] = ["Lascade-Co/travel-animator-android"]
        code = release_blog.main(
            [
                "--repo", "Lascade-Co/travel-animator-android",
                "--config", write_config(entries),
                "--tag", "3.9.3",
                "--out", self.out,
                "--dry-run",
            ],
            http=FakeHttp(),
            run=self.fake_codex(),
        )
        self.assertEqual(code, 0)
        self.assertFalse(Path(self.out, "blog.html").exists())

    def test_codex_failure_exits_zero(self):
        class Failed:
            returncode = 1
            stdout = ""
            stderr = "not logged in"

        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        code = release_blog.main(self.args("--dry-run"), http=http, run=lambda cmd, **kw: Failed())
        self.assertEqual(code, 0)

    def test_no_candidates_skips_generation(self):
        http = FakeHttp((503, ""), (503, ""))
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)
            raise AssertionError("codex must not run without link candidates")

        code = release_blog.main(self.args("--dry-run"), http=http, run=runner)
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])

    def test_a_failing_first_attempt_triggers_one_retry(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        bodies = [fixture("release_blog_bad.html"), fixture("release_blog_good.html")]
        attempts = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            attempts.append(kwargs.get("input", ""))
            Path(self.out, "blog.json").write_text(json.dumps(GOOD_META), encoding="utf-8")
            Path(self.out, "blog.html").write_text(bodies[len(attempts) - 1], encoding="utf-8")
            return Result()

        release_blog.main(self.args("--dry-run"), http=http, run=runner)
        self.assertEqual(len(attempts), 2)
        self.assertIn("PREVIOUS ATTEMPT", attempts[1])
        self.assertTrue(Path(self.out, "prompt-retry.md").exists())
        report = Path(self.out, "validation.txt").read_text()
        self.assertIn("chose attempt 2", report)

    def test_malformed_first_output_triggers_one_retry(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        attempts = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                Path(self.out, "blog.json").write_text("{bad", encoding="utf-8")
                Path(self.out, "blog.html").write_text("<h2>bad</h2>", encoding="utf-8")
            else:
                Path(self.out, "blog.json").write_text(json.dumps(GOOD_META), encoding="utf-8")
                Path(self.out, "blog.html").write_text(fixture("release_blog_good.html"), encoding="utf-8")
            return Result()

        code = release_blog.main(self.args("--dry-run"), http=http, run=runner)
        self.assertEqual(code, 0)
        self.assertEqual(len(attempts), 2)
        self.assertTrue(Path(self.out, "wp-payload.json").exists())

    def test_no_retry_flag_stops_after_one_attempt(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        attempts = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            attempts.append(1)
            Path(self.out, "blog.json").write_text(json.dumps(GOOD_META), encoding="utf-8")
            Path(self.out, "blog.html").write_text(fixture("release_blog_bad.html"), encoding="utf-8")
            return Result()

        release_blog.main(self.args("--dry-run", "--no-retry"), http=http, run=runner)
        self.assertEqual(len(attempts), 1)

    def test_validation_findings_are_printed_after_the_attempt_summary(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        output = StringIO()
        with redirect_stdout(output):
            release_blog.main(
                self.args("--dry-run", "--no-retry"),
                http=http,
                run=self.fake_codex(html=fixture("release_blog_bad.html")),
            )
        text = output.getvalue()
        self.assertIn("attempt 1: 3 errors, 4 warns", text)
        self.assertIn("attempt 1: error A1: origin URL", text)
        self.assertIn("attempt 1: warn B7:", text)

    def test_a_suppressed_finding_does_not_trigger_a_retry(self):
        entries = json.loads(json.dumps(CONFIG))
        entries[0]["suppress"] = ["G4"]
        html = fixture("release_blog_good.html").replace(">route animation guide<", ">read more<")
        attempts = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            attempts.append(1)
            Path(self.out, "blog.json").write_text(json.dumps(GOOD_META), encoding="utf-8")
            Path(self.out, "blog.html").write_text(html, encoding="utf-8")
            return Result()

        code = release_blog.main(
            [*self.args("--dry-run")[:3], write_config(entries), *self.args("--dry-run")[4:]],
            http=FakeHttp((200, fixture("release_blog_candidates.json"))),
            run=runner,
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(attempts), 1)
        self.assertIn("info", Path(self.out, "validation.txt").read_text())

    def test_html_flag_skips_codex_entirely(self):
        meta_path = Path(self.out, "hand.json")
        html_path = Path(self.out, "hand.html")
        meta_path.write_text(json.dumps(GOOD_META), encoding="utf-8")
        html_path.write_text(fixture("release_blog_good.html"), encoding="utf-8")
        http = FakeHttp((200, fixture("release_blog_candidates.json")))

        def runner(cmd, **kwargs):
            raise AssertionError("codex must not run with --html")

        code = release_blog.main(
            self.args("--dry-run", "--html", str(html_path), "--meta", str(meta_path)),
            http=http,
            run=runner,
        )
        self.assertEqual(code, 0)
        self.assertTrue(Path(self.out, "wp-payload.json").exists())

    def test_invalid_arguments_still_return_zero(self):
        self.assertEqual(release_blog.main([], http=FakeHttp(), run=self.fake_codex()), 0)


if __name__ == "__main__":
    unittest.main()
