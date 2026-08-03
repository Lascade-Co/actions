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
from urllib.parse import urlparse

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
from seo_parse import parse_blog, parse_listing, slug_from_url
from seo_report import RunSummary, counts, gate, partition, render_report
from seo_rulekit import is_same_registrable


def discover(fetcher: Fetcher, site) -> tuple[list[str], SiteContext]:
    listing = fetcher.get(site.listing_url)
    listing_urls = parse_listing(listing.body, site.base_url, site.listing_path) if listing.ok else []
    ordered = listing_urls[: site.blog_count]

    cms = CmsSnapshot()
    if site.cms_api:
        cms = fetcher.fetch_cms_posts(site.origin_host, site.blog_count * 2)
        if cms.ok:
            for post in cms.posts[: site.blog_count]:
                url = f"{site.base_url}{site.listing_path}/{post.slug}"
                if url not in ordered:
                    ordered.append(url)

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
    )


def fetch_blogs(fetcher: Fetcher, site, urls: list[str], ctx: SiteContext):
    """All blogs fetched concurrently — one worker per blog."""
    listed = set(ctx.listing_urls)
    cms_slugs = {post.slug for post in ctx.cms.posts}

    def one(url: str):
        slug = slug_from_url(url)
        found = set()
        if url in listed:
            found.add("listing")
        if slug in cms_slugs:
            found.add("cms")
        response = fetcher.get(url)
        return parse_blog(url, slug, response, frozenset(found))

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
            if image.url.startswith(("http://", "https://")):
                all_urls.add(image.url)
        og_image = page.og.get("og:image")
        if og_image and og_image.startswith("http"):
            dimension_urls.add(og_image)
            all_urls.add(og_image)
        for node in page.jsonld:
            if isinstance(node.data, dict):
                for element in node.data.get("itemListElement") or []:
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
        produced = []
        for rule in BLOG_RULES:
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
    args = parser.parse_args(argv)

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
        try:
            site = load_site_config(args.config, args.site)
        except Exception:
            site = None
        if site is None:
            sys.stderr.write(detail)
            return 0
        summary = RunSummary(
            site=site,
            pages=[],
            findings=[],
            rules=ALL_RULES,
            started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            duration_s=0.0,
            error=detail,
        )

    Path(args.output).write_text(render_report(summary), encoding="utf-8")
    write_github_output(summary)

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
