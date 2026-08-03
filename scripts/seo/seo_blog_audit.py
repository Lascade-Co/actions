#!/usr/bin/env python3
"""Blog SEO Audit entry point.

Always exits 0 — findings are reported, never raised. See ADR 0003.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from seo_checks import ALL_RULES, BLOG_RULES, RULES_BY_ID, RUN_RULES
from seo_fetch import Fetcher, RequestsTransport
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARN,
    CmsSnapshot,
    Response,
    SiteContext,
    finding,
    load_site_config,
)
from seo_parse import normalize_url, parse_blog, parse_listing, slug_from_url
from seo_report import RunSummary, counts, gate, partition, render_report
from seo_rulekit import is_asset_url, jsonld_nodes


def _cms_post_url(site, post) -> str:
    """Best-guess public URL for a CMS post: the path of its own permalink
    mapped onto the canonical host, falling back to the assembled slug guess
    when the CMS didn't return a usable — or parseable — link."""
    path = ""
    if post.link:
        try:
            path = urlparse(post.link).path
        except Exception:  # noqa: BLE001 — a malformed CMS link must not crash discovery
            path = ""
        if len(path) > 1:
            path = path.rstrip("/")
    if path:
        return f"{site.base_url}{path}"
    return f"{site.base_url}{site.listing_path}/{post.slug}"


def _resolve_cms_candidate(fetcher: Fetcher, site, url: str):
    """One-hop-aware verification for a CMS candidate not already in the listing.

    Returns (decision, resolved_url): decision is "include", "skip", or
    "undecidable"; resolved_url is the URL to actually audit when including —
    the redirect's destination when we followed one, otherwise the candidate
    itself.

    Exactly one redirect is followed. A same-host destination that lands
    under listing_path is the real page (include it, using the destination as
    the audited URL — that's genuinely a missing-from-the-listing post, e.g.
    when the CMS's own link omits a path segment the frontend adds back). A
    destination outside listing_path is a different section of the site
    (e.g. /trends) and is confidently out of scope. A missing Location, a
    cross-host destination, or a destination that itself redirects again are
    all ambiguous — reported as undecidable rather than chased further.
    """
    status = fetcher.verify(url)
    if not status.is_redirect:
        # 200 (published, missing from the listing — I1's whole purpose) or
        # 4xx/5xx/unreachable (published in the CMS but broken publicly) —
        # both belong in the audited set.
        return "include", url
    if not status.location:
        return "undecidable", None
    destination = urljoin(url, status.location)
    if urlparse(destination).netloc.lower() != site.canonical_host.lower():
        return "undecidable", None
    hop = fetcher.verify(destination)
    if hop.is_redirect:
        # one hop only — a further redirect is ambiguous, not chased.
        return "undecidable", None
    prefix = site.listing_path.rstrip("/") + "/"
    path = urlparse(destination).path
    if path == site.listing_path or path.startswith(prefix):
        return "include", destination
    return "skip", None


def _fill_missing_cms_posts(
    fetcher: Fetcher, site, ordered: list[str], cms: CmsSnapshot
) -> tuple[CmsSnapshot, frozenset[str]]:
    """After the CMS window fetch, an audited blog's slug may be absent from
    that window purely because it sits further down the CMS's own date-ordered
    list than we fetched — not because it has no CMS post at all (e.g.
    MarineRadar's window is dominated by /trends posts, pushing genuine hub
    posts out). I3 ("unpublished-still-live") would otherwise call every one
    of those a zombie based on a truncated window, not genuine absence.

    Only the specific slugs missing from the window get one targeted lookup
    each — zero extra requests when the window already covers everything.

    The targeted lookup is tri-state (see Fetcher.fetch_cms_post_by_slug_detailed):
    found, genuinely absent, or failed (a non-200 response or unparseable body).
    A failed lookup leaves the snapshot's posts and its `ok` flag untouched —
    one missing slug is not a CMS outage, and flipping `ok` to False here would
    silence the entire parity rule group via _parity_enabled — but it is also
    NOT proof of absence. Its slug is returned separately (the second element)
    so check_i3 can skip it instead of reporting a request failure as a
    confirmed zombie. Only genuine per-slug absence is silently accepted here;
    a request failure never is.
    """
    known_slugs = {post.slug for post in cms.posts}
    extra = []
    failed: set[str] = set()
    for url in ordered:
        slug = slug_from_url(url)
        if slug in known_slugs:
            continue
        found, lookup_failed = fetcher.fetch_cms_post_by_slug_detailed(site.origin_host, slug)
        if found is not None:
            extra.append(found)
            known_slugs.add(found.slug)
        elif lookup_failed:
            failed.add(slug)
    if not extra:
        return cms, frozenset(failed)
    return (
        CmsSnapshot(posts=cms.posts + tuple(extra), ok=cms.ok, error=cms.error, enabled=cms.enabled),
        frozenset(failed),
    )


def discover(fetcher: Fetcher, site) -> tuple[list[str], SiteContext]:
    listing = fetcher.get(site.listing_url)
    listing_urls = parse_listing(listing.body, site.base_url, site.listing_path) if listing.ok else []
    ordered = listing_urls[: site.blog_count]

    cms = CmsSnapshot()
    cms_lookup_failed: frozenset[str] = frozenset()
    if site.cms_api:
        cms = fetcher.fetch_cms_posts(site.origin_host, site.blog_count * 2)
        if cms.ok:
            listed = set(ordered)
            # Absence must be tested against the FULL listing, never the
            # blog_count-truncated `ordered` slice — a CMS post ranked just
            # beyond the truncation is still on the listing page, not missing,
            # and including it would double the audited set for no reason.
            full_listing = {normalize_url(u) for u in listing_urls}
            included = 0
            skipped = 0
            undecidable = 0
            # Walk the full fetched list (already date-descending, per the CMS
            # request's orderby=date&order=desc) rather than stopping at the
            # first blog_count posts — on a site where another content type
            # dominates recency, the first blog_count-by-date can be entirely
            # off-section, leaving genuinely-missing in-section posts
            # undiscovered further down the list.
            for post in cms.posts:
                if included >= site.blog_count:
                    break
                url = _cms_post_url(site, post)
                if url in listed or normalize_url(url) in full_listing:
                    # already in the audited set, or present on the listing
                    # page just beyond the truncated top-blog_count slice —
                    # not missing, nothing to add.
                    continue
                decision, resolved = _resolve_cms_candidate(fetcher, site, url)
                if decision == "include":
                    if resolved in listed or normalize_url(resolved) in full_listing:
                        continue
                    ordered.append(resolved)
                    listed.add(resolved)
                    included += 1
                elif decision == "skip":
                    skipped += 1
                elif decision == "undecidable":
                    undecidable += 1
            print(
                f"{site.label}: CMS union included {included} candidate(s) missing "
                f"from the listing, skipped {skipped} redirecting to another section, "
                f"{undecidable} undecidable"
            )
            cms, cms_lookup_failed = _fill_missing_cms_posts(fetcher, site, ordered, cms)

    sitemap_urls, sitemap_ok = fetcher.fetch_sitemap(site.sitemap_url)
    robots = fetcher.get(f"{site.base_url}/robots.txt")

    return ordered, SiteContext(
        listing_urls=tuple(listing_urls),
        listing_ok=listing.ok,
        sitemap_urls=sitemap_urls,
        sitemap_ok=sitemap_ok,
        robots_txt=robots.body if robots.ok else "",
        robots_ok=robots.ok,
        cms=cms,
        cms_lookup_failed=cms_lookup_failed,
    )


def fetch_blogs(fetcher: Fetcher, site, urls: list[str], ctx: SiteContext):
    """All blogs fetched concurrently — one worker per blog."""

    def one(url: str):
        slug = slug_from_url(url)
        response = fetcher.get(url)
        return parse_blog(url, slug, response)

    if not urls:
        return []
    with ThreadPoolExecutor(max_workers=max(1, site.threshold("blog_concurrency"))) as pool:
        return list(pool.map(one, urls))


def collect_urls(pages, site) -> tuple[set[str], set[str]]:
    all_urls: set[str] = set()
    dimension_urls: set[str] = set()
    for page in pages:
        for anchor in page.anchors:
            if anchor.url.startswith(("http://", "https://")):
                all_urls.add(anchor.url)
        for image in page.images:
            # JSON-LD `image` values (spec:182) are folded into page.images by
            # parse_blog (source="jsonld") — B3 already checks every entry
            # here, so no separate JSON-LD walk is needed for those.
            if image.url.startswith(("http://", "https://")):
                all_urls.add(image.url)
        for url in page.subresources:
            # Only origin *asset* subresources are ever consulted (A3 filters
            # on is_asset_url) — verifying every script/style/iframe src on a
            # Next.js page would be dozens of pointless HEADs per blog with
            # no rule ever reading the result.
            if url.startswith(("http://", "https://")) and is_asset_url(site, url):
                all_urls.add(url)
        og_image = page.og.get("og:image")
        if og_image and og_image.startswith("http"):
            dimension_urls.add(og_image)
            all_urls.add(og_image)
        # jsonld_nodes() (not a raw walk of page.jsonld) so a BreadcrumbList
        # nested inside a top-level @graph block is still found — a raw walk
        # of node.data only ever sees the @graph container itself, never the
        # nodes inside it, so breadcrumb items in that (common) shape were
        # never verified and E3 went dead on any site using it.
        for node in jsonld_nodes(page):
            for element in node.get("itemListElement") or []:
                if isinstance(element, dict):
                    target = element.get("item")
                    if isinstance(target, dict):
                        target = target.get("@id") or target.get("url")
                    if isinstance(target, str) and target.startswith("http"):
                        all_urls.add(target)
    return all_urls, dimension_urls


def evaluate(pages, site, urls, ctx, concurrency: int):
    """Blog rules run per blog in parallel; run-scoped rules run once over all blogs."""
    findings = []

    def for_blog(page):
        # A non-200 blog page is one fact (the fetch failed) — H1 reports
        # it, and running the other 29 blog-scope rules against an empty/
        # error page just multiplies that single fact into a dozen
        # unrelated-looking findings (missing title, no H1, no canonical,
        # thin content, ...) for a page nobody could have fixed content on.
        # Run-scoped rules (e.g. I1) still see this page via `pages`.
        rules = BLOG_RULES if page.response.ok else [RULES_BY_ID["H1"]]
        produced = []
        for rule in rules:
            try:
                produced.extend(rule.fn(page, site, urls, ctx))
            except Exception:  # noqa: BLE001 — a broken rule must not kill the audit
                produced.append(
                    finding(
                        RULES_BY_ID["H3"],
                        SEVERITY_ERROR,
                        f"rule {rule.id} raised while checking this blog",
                        blog_url=page.url,
                        evidence=traceback.format_exc(limit=3),
                    )
                )
        return produced

    if pages:
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            for produced in pool.map(for_blog, pages):
                findings.extend(produced)

    for rule in RUN_RULES:
        try:
            findings.extend(rule.fn(pages, site, urls, ctx))
        except Exception:  # noqa: BLE001
            findings.append(
                finding(
                    RULES_BY_ID["H3"],
                    SEVERITY_ERROR,
                    f"run-scoped rule {rule.id} raised",
                    evidence=traceback.format_exc(limit=3),
                )
            )
    return findings


def audit(site, fetcher: Fetcher) -> RunSummary:
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    urls, ctx = discover(fetcher, site)
    pages = fetch_blogs(fetcher, site, urls, ctx)
    to_verify, dimension_urls = collect_urls(pages, site)
    statuses = fetcher.verify_many(
        to_verify, concurrency=site.threshold("url_concurrency"), dimension_urls=dimension_urls
    )
    findings = evaluate(pages, site, statuses, ctx, site.threshold("blog_concurrency"))
    return RunSummary(
        site=site,
        pages=pages,
        findings=findings,
        rules=ALL_RULES,
        started_at=started_at,
        duration_s=time.monotonic() - started,
        ctx=ctx,
    )


class FixtureTransport:
    """Serves saved responses from a directory for --offline runs.

    Files are named by URL with non-alphanumerics replaced by underscores.
    Any URL without a file gets a 200 with an empty body.
    """

    def __init__(self, directory: str):
        self.directory = Path(directory)

    def __call__(self, method, url, headers, timeout):
        name = "".join(ch if ch.isalnum() else "_" for ch in url)[:180]
        path = self.directory / f"{name}.txt"
        if not path.exists():
            return Response(url=url, status=200, headers={"content-type": "text/html"}, body="")
        body = path.read_text(encoding="utf-8")
        return Response(
            url=url,
            status=200,
            headers={"content-type": "text/html", "cache-control": "public, max-age=60"},
            body=body,
            content=body.encode(),
            ttfb_ms=1,
        )


def write_github_output(summary: RunSummary) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    if not target:
        return
    active, suppressed = partition(summary.findings, summary.site)
    tally = counts(active)
    lines = {
        "has_findings": "true" if gate(summary) else "false",
        "error_count": tally[SEVERITY_ERROR],
        "warn_count": tally[SEVERITY_WARN],
        "info_count": tally[SEVERITY_INFO],
        "suppressed_count": len(suppressed),
        "label": summary.site.label,
    }
    with open(target, "a", encoding="utf-8") as handle:
        for key, value in lines.items():
            handle.write(f"{key}={value}\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Audit the newest blogs on a site for SEO defects.")
    parser.add_argument("--site", required=True, help="site name from the config file")
    parser.add_argument("--config", default="seo_sites.json", help="path to seo_sites.json")
    parser.add_argument("--output", default="report.html", help="where to write the report")
    parser.add_argument("--blog-count", type=int, default=None, help="override how many blogs to audit")
    parser.add_argument("--offline", default=None, help="serve responses from this fixture directory")
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        # argparse raises SystemExit for both --help and malformed input (e.g. a
        # missing --site or a non-integer --blog-count typed into a workflow_dispatch
        # form). It already wrote the usage/error text to stderr — nothing here is
        # allowed to escape as a non-zero exit. See ADR 0003.
        return 0

    summary = None
    try:
        site = load_site_config(args.config, args.site)
        if args.blog_count:
            site = type(site)(**{**site.__dict__, "blog_count": args.blog_count})
        transport = FixtureTransport(args.offline) if args.offline else RequestsTransport()
        fetcher = Fetcher(transport, timeout=site.threshold("request_timeout"))
        summary = audit(site, fetcher)
    except Exception:  # noqa: BLE001 — a crash must still produce a deliverable report
        detail = traceback.format_exc()
        # Written to stderr unconditionally — a failing cron run must be diagnosable
        # from the Actions log alone, without downloading the HTML artifact, whether
        # the crash happened before the site even loaded or inside audit() itself.
        sys.stderr.write(detail)
        try:
            site = load_site_config(args.config, args.site)
        except Exception:
            site = None
        if site is None:
            return 0
        summary = RunSummary(
            site=site,
            pages=[],
            # ADR 0003 and spec:349: a crashed run must still render an H3
            # finding carrying the traceback — an empty findings list here
            # leaves error_count/warn_count both 0 even though gate() fires
            # on summary.error, so the Telegram caption would read "0 errors,
            # 0 warnings" for a run that never completed.
            findings=[
                finding(
                    RULES_BY_ID["H3"],
                    SEVERITY_ERROR,
                    "the audit crashed before completing",
                    evidence=detail,
                )
            ],
            rules=ALL_RULES,
            started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            duration_s=0.0,
            error=detail,
        )

    try:
        Path(args.output).write_text(render_report(summary), encoding="utf-8")
    except Exception:  # noqa: BLE001 — an unwritable --output must not raise
        sys.stderr.write(traceback.format_exc())

    try:
        write_github_output(summary)
    except Exception:  # noqa: BLE001 — same contract for the GITHUB_OUTPUT write
        sys.stderr.write(traceback.format_exc())

    active, suppressed = partition(summary.findings, summary.site)
    tally = counts(active)
    print(
        f"{summary.site.label}: {tally[SEVERITY_ERROR]} errors, {tally[SEVERITY_WARN]} warnings, "
        f"{tally[SEVERITY_INFO]} info, {len(suppressed)} suppressed "
        f"across {len(summary.pages)} blogs → {args.output}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
