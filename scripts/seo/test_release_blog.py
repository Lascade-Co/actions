"""Tests for the Release Blog pipeline. No network, no model, no git."""

from __future__ import annotations

import json
import tempfile
import unittest

from release_blog_digest import (
    PATCH_BYTE_CAP,
    build_digest,
    filter_patch,
    marketing_version_from_tag,
    truncate,
)
from seo_model import resolve_site_for_repo, site_config_from_dict
from seo_testkit import fixture

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


if __name__ == "__main__":
    unittest.main()
