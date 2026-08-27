"""Validate a generated draft with the audit's draft-answerable rules."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html import escape
from urllib.parse import urlparse

from seo_checks import RULES_BY_ID
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_ORDER,
    SEVERITY_WARN,
    Finding,
    Response,
    SiteContext,
)
from seo_parse import parse_blog

# Draft-answerable, page-only rules. See ADR-0012 before changing this list.
PRE_PUBLISH_RULE_IDS = (
    "A1", "A2", "A4", "A5",
    "B7",
    "C1", "C4",
    "D1", "D2", "D3", "D4", "D5", "D6",
    "E1", "E4",
    "G1", "G2", "G4",
)

MIN_PLACEHOLDERS = 1
MAX_PLACEHOLDERS = 6
MIN_ALT_WORDS = 6
GENERIC_ALT_OPENERS = ("image of", "screenshot of", "photo of", "picture of", "img of")

FORBIDDEN_TAGS = ("script", "style", "html", "head", "body", "iframe")
MARKER_PATTERN = re.compile(r"<!--\s*release-blog:\s*\S+@\S+\s*-->")
MEDIA_ID_PATTERN = re.compile(r'data-media-id="([^"]+)"')


@dataclass
class Attempt:
    """One generated draft and the findings produced for it."""

    meta: dict
    html: str
    findings: list = field(default_factory=list)
    errors: int = 0
    warns: int = 0

    @property
    def score(self) -> tuple[int, int]:
        return self.errors, self.warns


def _finding(rule_id: str, slug: str, severity: str, message: str, url: str, evidence: str = "") -> Finding:
    return Finding(
        rule=rule_id,
        slug=slug,
        severity=severity,
        message=message,
        blog_url=url,
        evidence=evidence,
    )


def draft_url(site, meta: dict) -> str:
    """Return the future canonical URL used for offline validation."""
    return f"https://{site.canonical_host}{site.listing_path.rstrip('/')}/{meta.get('slug', '')}"


def wrap_fragment(site, meta: dict, html: str) -> str:
    """Wrap a body fragment in only the host fields derived from draft input."""
    url = draft_url(site, meta)
    return (
        '<!doctype html><html lang="en"><head>'
        f"<title>{escape(meta.get('title', ''))}</title>"
        f'<meta name="description" content="{escape(meta.get("excerpt", ""))}">'
        f'<link rel="canonical" href="{escape(url)}">'
        "</head><body><article>"
        f"<h1>{escape(meta.get('title', ''))}</h1>"
        f"{html}"
        "</article></body></html>"
    )


def build_page(site, meta: dict, html: str):
    url = draft_url(site, meta)
    page_html = wrap_fragment(site, meta, html)
    response = Response(
        url=url,
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=page_html,
        content=page_html.encode("utf-8"),
        ttfb_ms=0,
    )
    return parse_blog(url, meta.get("slug", ""), response)


def run_rules(site, meta: dict, html: str) -> list[Finding]:
    """Run the explicit pre-publish allowlist, honoring site suppression."""
    page = build_page(site, meta, html)
    findings: list[Finding] = []
    for rule_id in PRE_PUBLISH_RULE_IDS:
        rule = RULES_BY_ID[rule_id]
        for item in rule.fn(page, site, {}, SiteContext()):
            if site.is_suppressed(rule_id):
                item = Finding(
                    rule=item.rule,
                    slug=item.slug,
                    severity=SEVERITY_INFO,
                    message=f"{item.message} (suppressed for {site.name})",
                    blog_url=item.blog_url,
                    evidence=item.evidence,
                )
            findings.append(item)
    return findings


def local_checks(site, meta: dict, html: str, candidates) -> list[Finding]:
    """Check draft-specific constraints the published-blog rules cannot express."""
    url = draft_url(site, meta)
    out: list[Finding] = []

    def error(message, evidence=""):
        out.append(_finding("L1", "draft-shape", SEVERITY_ERROR, message, url, evidence))

    # Contextual internal link URLs must be copied verbatim from the candidates.
    allowed_urls = {candidate.url for candidate in candidates}
    page = build_page(site, meta, html)
    for anchor in page.content_anchors:
        parsed = urlparse(anchor.url)
        if parsed.netloc.lower() != site.canonical_host:
            continue
        if parsed.path.startswith(site.listing_path.rstrip("/") + "/") and anchor.url not in allowed_urls:
            error(f"internal link is not a link candidate: {anchor.url}", anchor.url)

    if meta.get("slug") in {candidate.slug for candidate in candidates}:
        error(f"slug {meta.get('slug')!r} already belongs to a published blog")

    for field_name in ("title", "slug", "excerpt"):
        if not (meta.get(field_name) or "").strip():
            error(f"{field_name} is empty")
    for tag in FORBIDDEN_TAGS:
        if re.search(rf"<\s*{tag}\b", html, re.IGNORECASE):
            error(f"fragment contains a forbidden <{tag}> element")

    media = meta.get("media") or []
    if not MIN_PLACEHOLDERS <= len(media) <= MAX_PLACEHOLDERS:
        error(f"{len(media)} placeholders declared (expected {MIN_PLACEHOLDERS}–{MAX_PLACEHOLDERS})")
    declared = {str(item.get("id")) for item in media}
    for used in MEDIA_ID_PATTERN.findall(html):
        if used not in declared:
            error(f"data-media-id {used!r} has no entry in media[]", used)

    marker = MARKER_PATTERN.search(html)
    if marker is None:
        error("fragment is missing its release marker comment")
    elif marker.end() != len(html.rstrip()):
        error("fragment does not end with its release marker comment")

    for item in media:
        alt = " ".join((item.get("alt") or "").split())
        lowered = alt.lower()
        if len(alt.split()) < MIN_ALT_WORDS:
            out.append(
                _finding(
                    "L2",
                    "alt-not-descriptive",
                    SEVERITY_WARN,
                    f"alt for {item.get('id')} is {len(alt.split())} words "
                    f"(minimum {MIN_ALT_WORDS}) — it must describe the image well enough to brief it",
                    url,
                    alt,
                )
            )
        elif any(lowered.startswith(opener) for opener in GENERIC_ALT_OPENERS):
            out.append(
                _finding(
                    "L2",
                    "alt-not-descriptive",
                    SEVERITY_WARN,
                    f"alt for {item.get('id')} opens with a generic phrase — describe the subject instead",
                    url,
                    alt,
                )
            )
    return out


def validate(site, meta: dict, html: str, candidates) -> Attempt:
    findings = run_rules(site, meta, html) + local_checks(site, meta, html, candidates)
    return Attempt(
        meta=meta,
        html=html,
        findings=findings,
        errors=sum(1 for finding in findings if finding.severity == SEVERITY_ERROR),
        warns=sum(1 for finding in findings if finding.severity == SEVERITY_WARN),
    )


def better(first: Attempt, second: Attempt) -> Attempt:
    """Return the lower-scoring attempt; preserve the first on a tie."""
    return second if second.score < first.score else first


def blocking(attempt: Attempt) -> bool:
    """Return whether the single retry would be useful."""
    return attempt.errors > 0 or attempt.warns > 0


def render_report(attempts: list[Attempt], chosen: Attempt, notes: list[str]) -> str:
    """Render all findings and identify the selected attempt."""
    lines: list[str] = []
    for note in notes:
        lines.append(f"note: {note}")
    if notes:
        lines.append("")
    for index, attempt in enumerate(attempts, start=1):
        lines.append(f"attempt {index}: {attempt.errors} errors, {attempt.warns} warns")
        for item in sorted(attempt.findings, key=lambda finding: SEVERITY_ORDER[finding.severity]):
            evidence = f" [{item.evidence}]" if item.evidence else ""
            lines.append(f"  {item.severity:<5} {item.rule:<3} {item.message}{evidence}")
        if not attempt.findings:
            lines.append("  clean")
        lines.append("")
    position = next(
        (index for index, attempt in enumerate(attempts, start=1) if attempt is chosen),
        1,
    )
    lines.append(f"chose attempt {position} ({chosen.errors} errors, {chosen.warns} warns)")
    return "\n".join(lines) + "\n"
