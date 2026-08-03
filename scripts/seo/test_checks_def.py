import unittest

from seo_checks_def import BLOG_RULES_DEF, RUN_RULES_DEF
from seo_model import SEVERITY_ERROR, SEVERITY_WARN, ImageRef, JsonLdBlock
from seo_testkit import BLOG_URL, make_context, make_page, make_site, make_status

RULES = {rule.id: rule for rule in BLOG_RULES_DEF + RUN_RULES_DEF}
ASSET = "https://hub.travelanimator.com/wp-content/uploads/2026/07/banner.png"

ARTICLE_LD = {
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    "headline": "How to Create a Travel Animation for Instagram",
    "description": "Step by step.",
    "url": BLOG_URL,
    "image": ASSET,
    "datePublished": "2026-07-30T10:00:00+00:00",
    "dateModified": "2026-08-01T10:00:00+00:00",
    "author": {"@type": "Person", "name": "Jaseel"},
    "publisher": {"@type": "Organization", "name": "Travel Animator"},
}
BREADCRUMB_LD = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://www.travelanimator.com"},
        {"@type": "ListItem", "position": 2, "name": "Hub", "item": "https://www.travelanimator.com/hub"},
    ],
}
FAQ_LD = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "mainEntity": [
        {
            "@type": "Question",
            "name": "Is TravelAnimator free to use?",
            "acceptedAnswer": {"@type": "Answer", "text": "Yes."},
        }
    ],
}


def blocks(*payloads):
    return tuple(JsonLdBlock(raw="{}", data=payload) for payload in payloads)


def schema_page(**over):
    defaults = {
        "jsonld": blocks(ARTICLE_LD, BREADCRUMB_LD, FAQ_LD),
        "article_text": "Is TravelAnimator free to use? Yes. " + " ".join(["word"] * 400),
    }
    defaults.update(over)
    return make_page(**defaults)


def run_rule(rule_id, page=None, *, site=None, urls=None, ctx=None, pages=None):
    rule = RULES[rule_id]
    site = site or make_site()
    ctx = ctx or make_context()
    urls = urls or {}
    if rule.scope == "run":
        return rule.fn(pages if pages is not None else [page or make_page()], site, urls, ctx)
    return rule.fn(page or make_page(), site, urls, ctx)


class GroupDTest(unittest.TestCase):
    def test_d1_error_when_title_missing(self):
        self.assertEqual(run_rule("D1", make_page(title=None))[0].severity, SEVERITY_ERROR)

    def test_d1_warn_when_title_too_long(self):
        page = make_page(title="x" * 95)
        self.assertEqual(run_rule("D1", page)[0].severity, SEVERITY_WARN)

    def test_d1_warn_when_title_too_short(self):
        self.assertEqual(run_rule("D1", make_page(title="Short"))[0].severity, SEVERITY_WARN)

    def test_d1_silent_on_good_title(self):
        self.assertEqual(run_rule("D1", make_page()), [])

    def test_d2_error_when_description_missing(self):
        self.assertEqual(run_rule("D2", make_page(meta_description=None))[0].severity, SEVERITY_ERROR)

    def test_d2_warn_when_description_short(self):
        self.assertEqual(run_rule("D2", make_page(meta_description="Too short."))[0].severity, SEVERITY_WARN)

    def test_d2_silent_on_good_description(self):
        self.assertEqual(run_rule("D2", make_page()), [])

    def test_d3_fires_when_no_h1(self):
        self.assertEqual(run_rule("D3", make_page(headings=((2, "Sub"),)))[0].severity, SEVERITY_ERROR)

    def test_d3_fires_on_multiple_h1(self):
        self.assertTrue(run_rule("D3", make_page(headings=((1, "One"), (1, "Two")))))

    def test_d3_fires_on_empty_h1(self):
        self.assertTrue(run_rule("D3", make_page(headings=((1, "   "),))))

    def test_d3_silent_on_single_h1(self):
        self.assertEqual(run_rule("D3", make_page()), [])

    def test_d4_fires_on_skipped_level(self):
        page = make_page(headings=((1, "Title"), (2, "Section"), (4, "Deep")))
        self.assertEqual(run_rule("D4", page)[0].severity, SEVERITY_WARN)

    def test_d4_fires_on_empty_heading(self):
        page = make_page(headings=((1, "Title"), (2, "")))
        self.assertTrue(run_rule("D4", page))

    def test_d4_silent_on_clean_hierarchy(self):
        page = make_page(headings=((1, "Title"), (2, "Section"), (3, "Step"), (2, "Next")))
        self.assertEqual(run_rule("D4", page), [])

    def test_d5_fires_on_thin_content(self):
        page = make_page(article_text=" ".join(["word"] * 120))
        self.assertEqual(run_rule("D5", page)[0].severity, SEVERITY_WARN)

    def test_d5_silent_above_threshold(self):
        self.assertEqual(run_rule("D5", make_page()), [])

    def test_d6_error_when_alt_attribute_absent(self):
        page = make_page(images=(ImageRef(url=ASSET, alt=None, source="img"),))
        self.assertEqual(run_rule("D6", page)[0].severity, SEVERITY_ERROR)

    def test_d6_warn_when_alt_looks_like_a_filename(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="travelanimator-banner-5761.png", source="img"),))
        self.assertEqual(run_rule("D6", page)[0].severity, SEVERITY_WARN)

    def test_d6_warn_on_empty_alt_without_decorative_flag(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="", source="img"),))
        self.assertEqual(run_rule("D6", page)[0].severity, SEVERITY_WARN)

    def test_d6_silent_on_decorative_image(self):
        page = make_page(images=(ImageRef(url="/rule.svg", alt="", aria_hidden=True, source="img"),))
        self.assertEqual(run_rule("D6", page), [])

    def test_d6_ignores_meta_sourced_images(self):
        page = make_page(images=(ImageRef(url=ASSET, alt=None, source="og"),))
        self.assertEqual(run_rule("D6", page), [])

    def test_d6_silent_on_descriptive_alt(self):
        page = make_page(images=(ImageRef(url=ASSET, alt="A route animation for Stories", source="img"),))
        self.assertEqual(run_rule("D6", page), [])

    def test_d7_fires_on_duplicate_titles(self):
        pages = [make_page(url=BLOG_URL), make_page(url=BLOG_URL + "-two")]
        findings = run_rule("D7", pages=pages)
        self.assertTrue(any(f.severity == SEVERITY_ERROR for f in findings))
        self.assertTrue(any("title" in f.message for f in findings))

    def test_d7_fires_on_duplicate_h1(self):
        pages = [
            make_page(url="https://www.travelanimator.com/hub/a", title="Title A", meta_description="A" * 100),
            make_page(url="https://www.travelanimator.com/hub/b", title="Title B", meta_description="B" * 100),
        ]
        findings = run_rule("D7", pages=pages)
        self.assertTrue(any("H1" in f.message for f in findings))

    def test_d7_fires_on_duplicate_meta_description(self):
        pages = [
            make_page(
                url="https://www.travelanimator.com/hub/a",
                title="Title A is long enough here",
                meta_description="C" * 100,
                headings=((1, "Heading A"),),
            ),
            make_page(
                url="https://www.travelanimator.com/hub/b",
                title="Title B is long enough here",
                meta_description="C" * 100,
                headings=((1, "Heading B"),),
            ),
        ]
        findings = run_rule("D7", pages=pages)
        self.assertTrue(any(f.severity == SEVERITY_ERROR for f in findings))
        self.assertTrue(any("meta description" in f.message for f in findings))
        self.assertFalse(any("title" in f.message for f in findings))
        self.assertFalse(any("H1" in f.message for f in findings))

    def test_d7_silent_when_all_unique(self):
        pages = [
            make_page(
                url="https://www.travelanimator.com/hub/a",
                title="Title A is long enough here",
                meta_description="A" * 100,
                headings=((1, "Heading A"),),
            ),
            make_page(
                url="https://www.travelanimator.com/hub/b",
                title="Title B is long enough here",
                meta_description="B" * 100,
                headings=((1, "Heading B"),),
            ),
        ]
        self.assertEqual(run_rule("D7", pages=pages), [])

    def test_d8_fires_without_lang(self):
        self.assertEqual(run_rule("D8", make_page(html_lang=None))[0].severity, SEVERITY_WARN)

    def test_d8_fires_without_viewport(self):
        self.assertTrue(run_rule("D8", make_page(has_viewport=False)))

    def test_d8_silent_when_both_present(self):
        self.assertEqual(run_rule("D8", make_page()), [])


class GroupETest(unittest.TestCase):
    def test_e1_fires_on_unparseable_block(self):
        page = make_page(jsonld=(JsonLdBlock(raw="{oops", data=None, error="Expecting value"),))
        self.assertEqual(run_rule("E1", page)[0].severity, SEVERITY_ERROR)

    def test_e1_fires_when_context_or_type_missing(self):
        page = make_page(jsonld=(JsonLdBlock(raw="{}", data={"headline": "x"}),))
        self.assertTrue(run_rule("E1", page))

    def test_e1_silent_on_valid_blocks(self):
        self.assertEqual(run_rule("E1", schema_page()), [])

    def test_e1_silent_on_valid_graph_block(self):
        """Round 3, item 3: MarineRadar wraps its structured data in a standard
        @graph container — @context at the top, @type on each member node, no
        @type on the container itself. That's valid JSON-LD and must not fire."""
        graph_ld = {
            "@context": "https://schema.org",
            "@graph": [ARTICLE_LD, BREADCRUMB_LD, FAQ_LD],
        }
        page = make_page(jsonld=blocks(graph_ld))
        self.assertEqual(run_rule("E1", page), [])

    def test_e1_fires_when_graph_nodes_lack_type(self):
        graph_ld = {
            "@context": "https://schema.org",
            "@graph": [{"headline": "no @type on this node"}],
        }
        page = make_page(jsonld=blocks(graph_ld))
        self.assertTrue(run_rule("E1", page))

    def test_e1_fires_when_graph_is_empty(self):
        graph_ld = {"@context": "https://schema.org", "@graph": []}
        page = make_page(jsonld=blocks(graph_ld))
        self.assertTrue(run_rule("E1", page))

    def test_e2_fires_when_no_article_schema(self):
        page = schema_page(jsonld=blocks(BREADCRUMB_LD))
        self.assertEqual(run_rule("E2", page)[0].severity, SEVERITY_ERROR)

    def test_e2_fires_on_missing_required_property(self):
        incomplete = {k: v for k, v in ARTICLE_LD.items() if k != "dateModified"}
        page = schema_page(jsonld=blocks(incomplete, BREADCRUMB_LD, FAQ_LD))
        findings = run_rule("E2", page)
        self.assertIn("dateModified", findings[0].message)

    def test_e2_silent_on_complete_article(self):
        self.assertEqual(run_rule("E2", schema_page()), [])

    def test_e3_fires_when_breadcrumb_absent(self):
        page = schema_page(jsonld=blocks(ARTICLE_LD, FAQ_LD))
        self.assertEqual(run_rule("E3", page)[0].severity, SEVERITY_ERROR)

    def test_e3_fires_on_offsite_breadcrumb_item(self):
        bad = {
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://hub.travelanimator.com/"}
            ],
        }
        page = schema_page(jsonld=blocks(ARTICLE_LD, bad, FAQ_LD))
        self.assertTrue(run_rule("E3", page))

    def test_e3_fires_when_breadcrumb_item_is_broken(self):
        target = "https://www.travelanimator.com/hub"
        urls = {target: make_status(target, status=404)}
        self.assertTrue(run_rule("E3", schema_page(), urls=urls))

    def test_e3_silent_on_healthy_breadcrumb(self):
        urls = {
            "https://www.travelanimator.com": make_status("https://www.travelanimator.com"),
            "https://www.travelanimator.com/hub": make_status("https://www.travelanimator.com/hub"),
        }
        self.assertEqual(run_rule("E3", schema_page(), urls=urls), [])

    def test_e4_fires_when_faq_question_absent_from_body(self):
        page = schema_page(article_text=" ".join(["unrelated"] * 400))
        self.assertEqual(run_rule("E4", page)[0].severity, SEVERITY_ERROR)

    def test_e4_silent_when_question_visible(self):
        self.assertEqual(run_rule("E4", schema_page()), [])

    def test_e4_silent_when_question_whitespace_differs(self):
        messy_body = "Is   TravelAnimator\nfree to\tuse?  Yes. " + " ".join(["word"] * 400)
        page = schema_page(article_text=messy_body)
        self.assertEqual(run_rule("E4", page), [])

    def test_e4_silent_without_faq_schema(self):
        self.assertEqual(run_rule("E4", schema_page(jsonld=blocks(ARTICLE_LD, BREADCRUMB_LD))), [])

    def test_e5_fires_when_schema_url_differs_from_canonical(self):
        divergent = dict(ARTICLE_LD, url="https://www.travelanimator.com/hub/other")
        page = schema_page(jsonld=blocks(divergent, BREADCRUMB_LD, FAQ_LD))
        self.assertEqual(run_rule("E5", page)[0].severity, SEVERITY_ERROR)

    def test_e5_silent_when_matching(self):
        self.assertEqual(run_rule("E5", schema_page()), [])

    def test_e6_fires_on_future_date_published(self):
        future = dict(ARTICLE_LD, datePublished="2099-01-01T00:00:00+00:00")
        page = schema_page(jsonld=blocks(future, BREADCRUMB_LD, FAQ_LD))
        self.assertEqual(run_rule("E6", page)[0].severity, SEVERITY_ERROR)

    def test_e6_fires_when_modified_precedes_published(self):
        inverted = dict(ARTICLE_LD, dateModified="2026-07-01T00:00:00+00:00")
        page = schema_page(jsonld=blocks(inverted, BREADCRUMB_LD, FAQ_LD))
        self.assertTrue(run_rule("E6", page))

    def test_e6_fires_on_unparseable_date(self):
        broken = dict(ARTICLE_LD, datePublished="last Tuesday")
        page = schema_page(jsonld=blocks(broken, BREADCRUMB_LD, FAQ_LD))
        self.assertTrue(run_rule("E6", page))

    def test_e6_silent_on_valid_dates(self):
        self.assertEqual(run_rule("E6", schema_page()), [])


class GroupFTest(unittest.TestCase):
    def test_f1_fires_on_missing_og_property(self):
        page = make_page(og={k: v for k, v in make_page().og.items() if k != "og:image"})
        findings = run_rule("F1", page)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)
        self.assertIn("og:image", findings[0].message)

    def test_f1_fires_when_og_type_is_not_article(self):
        page = make_page(og=dict(make_page().og, **{"og:type": "website"}))
        self.assertTrue(run_rule("F1", page))

    def test_f1_silent_when_complete(self):
        self.assertEqual(run_rule("F1", make_page()), [])

    def test_f2_fires_when_og_url_differs_from_canonical(self):
        page = make_page(og=dict(make_page().og, **{"og:url": "https://www.travelanimator.com/hub/other"}))
        self.assertEqual(run_rule("F2", page)[0].severity, SEVERITY_ERROR)

    def test_f2_silent_when_matching(self):
        self.assertEqual(run_rule("F2", make_page()), [])

    def test_f3_fires_without_twitter_card(self):
        page = make_page(twitter={"twitter:image": ASSET})
        self.assertEqual(run_rule("F3", page)[0].severity, SEVERITY_WARN)

    def test_f3_silent_when_complete(self):
        self.assertEqual(run_rule("F3", make_page()), [])

    def test_f4_fires_when_og_image_too_small(self):
        urls = {ASSET: make_status(ASSET, content_type="image/webp", width=600, height=315, byte_size=1000)}
        findings = run_rule("F4", make_page(), urls=urls)
        self.assertEqual(findings[0].severity, SEVERITY_WARN)
        self.assertIn("600", findings[0].message)

    def test_f4_fires_when_og_image_too_large_in_bytes(self):
        urls = {
            ASSET: make_status(
                ASSET, content_type="image/webp", width=1600, height=900, byte_size=9 * 1024 * 1024
            )
        }
        self.assertTrue(run_rule("F4", make_page(), urls=urls))

    def test_f4_fires_when_dimensions_unreadable(self):
        urls = {ASSET: make_status(ASSET, content_type="image/webp", width=None, height=None)}
        self.assertTrue(run_rule("F4", make_page(), urls=urls))

    def test_f4_silent_on_suitable_image(self):
        urls = {
            ASSET: make_status(ASSET, content_type="image/webp", width=1600, height=900, byte_size=200000)
        }
        self.assertEqual(run_rule("F4", make_page(), urls=urls), [])

    def test_f4_silent_when_image_not_verified(self):
        self.assertEqual(run_rule("F4", make_page(), urls={}), [])

    def test_f4_silent_when_image_status_not_200(self):
        urls = {ASSET: make_status(ASSET, content_type="image/webp", status=404, width=600, height=315)}
        self.assertEqual(run_rule("F4", make_page(), urls=urls), [])


class RegistryTest(unittest.TestCase):
    def test_ids_present_exactly_once(self):
        ids = sorted(rule.id for rule in BLOG_RULES_DEF + RUN_RULES_DEF)
        expected = sorted(
            [f"D{i}" for i in range(1, 9)] + [f"E{i}" for i in range(1, 7)] + [f"F{i}" for i in range(1, 5)]
        )
        self.assertEqual(ids, expected)

    def test_d7_is_run_scoped(self):
        self.assertEqual({r.id for r in RUN_RULES_DEF}, {"D7"})


if __name__ == "__main__":
    unittest.main()
