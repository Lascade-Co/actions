import unittest

from seo_model import (
    DEFAULT_THRESHOLDS,
    SEVERITY_ERROR,
    SiteConfig,
    site_config_from_dict,
)

RAW_SITE = {
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
    "suppress": ["A2"],
    "thresholds": {"title_max": 70},
}


class SiteConfigTest(unittest.TestCase):
    def test_builds_from_dict(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertIsInstance(site, SiteConfig)
        self.assertEqual(site.name, "travelanimator")
        self.assertEqual(site.base_url, "https://www.travelanimator.com")
        self.assertEqual(site.listing_url, "https://www.travelanimator.com/hub")

    def test_thresholds_merge_over_defaults(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertEqual(site.thresholds["title_max"], 70)
        self.assertEqual(
            site.thresholds["title_min"], DEFAULT_THRESHOLDS["title_min"]
        )

    def test_suppress_and_subdomains_are_sets(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertTrue(site.is_suppressed("A2"))
        self.assertFalse(site.is_suppressed("A1"))
        self.assertIn("support.travelanimator.com", site.allowed_subdomains)

    def test_registrable_domain_derived_from_canonical_host(self):
        site = site_config_from_dict(RAW_SITE)
        self.assertEqual(site.registrable_domain, "travelanimator.com")

    def test_missing_optional_keys_get_defaults(self):
        raw = {k: v for k, v in RAW_SITE.items() if k not in ("suppress", "thresholds", "cms_api")}
        site = site_config_from_dict(raw)
        self.assertEqual(site.suppress, frozenset())
        self.assertFalse(site.cms_api)
        self.assertEqual(site.thresholds, DEFAULT_THRESHOLDS)

    def test_severity_constant(self):
        self.assertEqual(SEVERITY_ERROR, "error")


if __name__ == "__main__":
    unittest.main()
