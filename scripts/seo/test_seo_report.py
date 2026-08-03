import re
import unittest

from seo_model import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN, Finding
from seo_report import RunSummary, counts, gate, partition, render_report
from seo_testkit import BLOG_URL, make_page, make_site


def f(rule="A1", severity=SEVERITY_ERROR, message="broken", blog_url=BLOG_URL, evidence="x"):
    return Finding(rule=rule, slug="some-rule", severity=severity, message=message, blog_url=blog_url, evidence=evidence)


# Matches any src=/href=/poster=/action=/formaction= attribute value, single- or
# double-quoted, so the self-containment check below cannot be defeated by quote style.
REMOTE_ATTR_RE = re.compile(
    r"""(?:src|href|poster|action|formaction)\s*=\s*(["'])(?P<value>.*?)\1""",
    re.IGNORECASE,
)


def _is_remote_reference(value: str) -> bool:
    """True for an absolute (any scheme) or protocol-relative ("//host/...") URL."""
    return bool(re.match(r"^(?:[a-zA-Z][a-zA-Z0-9+.\-]*:|//)", value.strip()))


def summary(findings=(), *, site=None, error=None):
    from seo_checks_abc import BLOG_RULES_ABC, RUN_RULES_ABC

    return RunSummary(
        site=site or make_site(),
        pages=[make_page()],
        findings=list(findings),
        rules=BLOG_RULES_ABC + RUN_RULES_ABC,
        started_at="2026-08-03T06:00:00+00:00",
        duration_s=12.5,
        error=error,
    )


class CountsTest(unittest.TestCase):
    def test_counts_by_severity(self):
        result = counts([f(severity=SEVERITY_ERROR), f(severity=SEVERITY_WARN), f(severity=SEVERITY_INFO), f(severity=SEVERITY_INFO)])
        self.assertEqual(result, {"error": 1, "warn": 1, "info": 2})

    def test_counts_empty(self):
        self.assertEqual(counts([]), {"error": 0, "warn": 0, "info": 0})


class PartitionTest(unittest.TestCase):
    def test_suppressed_rules_are_separated(self):
        site = make_site(suppress=["A2"])
        active, suppressed = partition([f(rule="A1"), f(rule="A2")], site)
        self.assertEqual([x.rule for x in active], ["A1"])
        self.assertEqual([x.rule for x in suppressed], ["A2"])

    def test_nothing_suppressed_by_default(self):
        active, suppressed = partition([f(rule="A1")], make_site())
        self.assertEqual(len(active), 1)
        self.assertEqual(suppressed, [])


class GateTest(unittest.TestCase):
    def test_error_opens_the_gate(self):
        self.assertTrue(gate(summary([f(severity=SEVERITY_ERROR)])))

    def test_warn_opens_the_gate(self):
        self.assertTrue(gate(summary([f(severity=SEVERITY_WARN)])))

    def test_info_never_opens_the_gate(self):
        self.assertFalse(gate(summary([f(severity=SEVERITY_INFO), f(severity=SEVERITY_INFO)])))

    def test_suppressed_error_does_not_open_the_gate(self):
        site = make_site(suppress=["A2"])
        self.assertFalse(gate(summary([f(rule="A2", severity=SEVERITY_ERROR)], site=site)))

    def test_unsuppressed_error_alongside_suppressed_still_opens(self):
        site = make_site(suppress=["A2"])
        findings = [f(rule="A2", severity=SEVERITY_ERROR), f(rule="A1", severity=SEVERITY_ERROR)]
        self.assertTrue(gate(summary(findings, site=site)))

    def test_clean_run_keeps_the_gate_shut(self):
        self.assertFalse(gate(summary([])))

    def test_harness_error_opens_the_gate(self):
        self.assertTrue(gate(summary([], error="ValueError: boom")))


class RenderTest(unittest.TestCase):
    def test_is_self_contained(self):
        # Findings evidence and blog URLs legitimately contain "https://..." as
        # escaped visible *text* (e.g. inside <div class="ev">...</div>) — that is
        # data, not a network reference, so we don't scan for "://" as raw text.
        # Instead we structurally check every src=/href=/poster=/action=/formaction=
        # attribute (either quote style) and the CSS block, so this test would catch
        # an https:// reference, a single-quoted attribute, a protocol-relative
        # "//host/..." URL, or a CSS url(...)/@import — not just the four literal
        # substrings the old version checked for.
        html = render_report(summary([f()]))
        self.assertNotIn("@import", html)
        self.assertNotIn("url(", html)
        remote = [
            match.group(0)
            for match in REMOTE_ATTR_RE.finditer(html)
            if _is_remote_reference(match.group("value"))
        ]
        self.assertEqual(remote, [], f"found remote reference(s) in rendered HTML: {remote}")

    def test_contains_structural_sections(self):
        html = render_report(summary([f()]))
        for heading in ("Summary", "Rule coverage", "Findings", "Per-blog detail", "Configuration"):
            self.assertIn(heading, html)

    def test_shows_site_label_and_timestamp(self):
        html = render_report(summary([f()]))
        self.assertIn("Travel Animator", html)
        self.assertIn("2026-08-03T06:00:00+00:00", html)

    def test_renders_rule_id_message_and_evidence(self):
        html = render_report(summary([f(rule="B1", message="internal link returned HTTP 404", evidence="/pricing")]))
        self.assertIn("B1", html)
        self.assertIn("internal link returned HTTP 404", html)
        self.assertIn("/pricing", html)

    def test_escapes_html_in_evidence(self):
        html = render_report(summary([f(evidence='<script>alert("x")</script>')]))
        self.assertNotIn("<script>alert", html)
        self.assertIn("&lt;script&gt;", html)

    def test_suppressed_section_appears_only_when_needed(self):
        site = make_site(suppress=["A2"])
        with_suppressed = render_report(summary([f(rule="A2")], site=site))
        self.assertIn("Suppressed", with_suppressed)
        self.assertIn("A2", with_suppressed)
        self.assertNotIn("Suppressed", render_report(summary([f(rule="A1")])))

    def test_clean_run_states_it_is_clean(self):
        html = render_report(summary([]))
        self.assertIn("No findings", html)

    def test_harness_error_is_rendered_with_traceback(self):
        html = render_report(summary([], error="Traceback: ValueError: boom"))
        self.assertIn("Harness error", html)
        self.assertIn("ValueError: boom", html)

    def test_coverage_matrix_has_a_column_per_group_and_row_per_blog(self):
        html = render_report(summary([f(rule="A1")]))
        matrix = re.search(r'<table class="matrix".*?</table>', html, re.S).group(0)
        for group in ("A", "B", "C"):
            self.assertIn(f'<th>{group}</th>', matrix)
        self.assertIn("good-blog", matrix)

    def test_findings_ordered_error_then_warn_then_info(self):
        html = render_report(
            summary([f(rule="Z9", severity=SEVERITY_INFO), f(rule="A1", severity=SEVERITY_ERROR)])
        )
        self.assertLess(html.index("A1"), html.index("Z9"))

    def test_dark_mode_styles_present(self):
        self.assertIn("prefers-color-scheme: dark", render_report(summary([])))


if __name__ == "__main__":
    unittest.main()
