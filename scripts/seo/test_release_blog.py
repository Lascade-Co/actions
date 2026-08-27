"""Tests for the Release Blog pipeline. No network, no model, no git."""

from __future__ import annotations

import json
import tempfile
import unittest

from seo_model import resolve_site_for_repo, site_config_from_dict

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


if __name__ == "__main__":
    unittest.main()
