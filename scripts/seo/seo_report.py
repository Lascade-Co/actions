"""Renders findings into one self-contained report.html."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape

from seo_model import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN, BlogPage, Finding, Rule, SiteConfig, SiteContext

GROUPS = ("A", "B", "C", "D", "E", "F", "G", "H", "I")

CSS = """
:root {
  --bg: #ffffff; --fg: #14181f; --muted: #5b6472; --line: #e3e7ec;
  --error: #b3261e; --warn: #8a5a00; --info: #2c5aa0; --ok: #1f7a4d;
  --error-bg: #fdecea; --warn-bg: #fdf3e0; --info-bg: #eaf1fb; --ok-bg: #e9f5ee;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #14171c; --fg: #e7eaee; --muted: #9aa3b0; --line: #2a2f38;
    --error: #ff8a80; --warn: #ffcc80; --info: #90caf9; --ok: #81c995;
    --error-bg: #35211f; --warn-bg: #322a1c; --info-bg: #1d2735; --ok-bg: #1d2b23;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 32px 24px 64px; background: var(--bg); color: var(--fg);
  font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
.wrap { max-width: 980px; margin: 0 auto; }
h1 { font-size: 24px; margin: 0 0 4px; }
h2 { font-size: 17px; margin: 40px 0 12px; padding-bottom: 6px; border-bottom: 1px solid var(--line); }
.sub { color: var(--muted); font-size: 13px; margin: 0 0 4px; }
.band { display: flex; gap: 12px; flex-wrap: wrap; margin: 20px 0 0; }
.tile { flex: 1 1 120px; border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; }
.tile .n { font-size: 28px; font-weight: 600; line-height: 1.1; }
.tile .l { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.tile.error .n { color: var(--error); } .tile.warn .n { color: var(--warn); }
.tile.info .n { color: var(--info); } .tile.ok .n { color: var(--ok); }
.verdict { margin: 16px 0 0; padding: 10px 12px; border-radius: 6px; background: var(--ok-bg); color: var(--ok); }
.verdict.bad { background: var(--error-bg); color: var(--error); }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { text-align: left; padding: 7px 9px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { color: var(--muted); font-weight: 600; font-size: 12px; }
table.matrix td.cell { text-align: center; font-weight: 600; width: 44px; }
.cell.error { background: var(--error-bg); color: var(--error); }
.cell.warn { background: var(--warn-bg); color: var(--warn); }
.cell.info { background: var(--info-bg); color: var(--info); }
.cell.ok { background: var(--ok-bg); color: var(--ok); }
.badge { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
.badge.error { background: var(--error-bg); color: var(--error); }
.badge.warn { background: var(--warn-bg); color: var(--warn); }
.badge.info { background: var(--info-bg); color: var(--info); }
code, .ev { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
.ev { color: var(--muted); word-break: break-all; }
details { border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; margin: 8px 0; }
summary { cursor: pointer; font-weight: 600; font-size: 14px; }
.harness { border: 1px solid var(--error); background: var(--error-bg); color: var(--error); border-radius: 8px; padding: 12px 14px; margin: 20px 0; }
.harness pre { white-space: pre-wrap; margin: 8px 0 0; }
"""


@dataclass
class RunSummary:
    site: SiteConfig
    pages: list[BlogPage]
    findings: list[Finding]
    rules: list[Rule]
    started_at: str
    duration_s: float
    error: str | None = None
    # None only on the crash path (main()'s except branch), where discovery
    # itself may never have run — every successful audit() populates this.
    ctx: SiteContext | None = None


def counts(findings) -> dict[str, int]:
    tally = {SEVERITY_ERROR: 0, SEVERITY_WARN: 0, SEVERITY_INFO: 0}
    for item in findings:
        tally[item.severity] = tally.get(item.severity, 0) + 1
    return tally


def partition(findings, site) -> tuple[list[Finding], list[Finding]]:
    active, suppressed = [], []
    for item in findings:
        (suppressed if site.is_suppressed(item.rule) else active).append(item)
    return active, suppressed


def gate(summary: RunSummary) -> bool:
    if summary.error:
        return True
    active, _ = partition(summary.findings, summary.site)
    tally = counts(active)
    return (tally[SEVERITY_ERROR] + tally[SEVERITY_WARN]) > 0


def _tile(value: int, label: str, kind: str) -> str:
    return f'<div class="tile {kind}"><div class="n">{value}</div><div class="l">{escape(label)}</div></div>'


def _matrix(summary: RunSummary, active: list[Finding]) -> str:
    worst: dict[tuple[str, str], str] = {}
    rank = {SEVERITY_ERROR: 3, SEVERITY_WARN: 2, SEVERITY_INFO: 1}
    for item in active:
        if not item.blog_url:
            continue
        key = (item.blog_url, item.rule[0])
        if rank.get(item.severity, 0) > rank.get(worst.get(key, ""), 0):
            worst[key] = item.severity
    header = "".join(f"<th>{group}</th>" for group in GROUPS)
    rows = []
    for page in summary.pages:
        cells = []
        for group in GROUPS:
            severity = worst.get((page.url, group))
            kind = severity if severity else "ok"
            mark = {"error": "●", "warn": "▲", "info": "·"}.get(severity, "✓")
            cells.append(f'<td class="cell {kind}">{mark}</td>')
        rows.append(f"<tr><td><code>{escape(page.slug)}</code></td>{''.join(cells)}</tr>")
    return (
        '<div class="scroll"><table class="matrix"><thead><tr><th>blog</th>'
        f"{header}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def _finding_rows(findings: list[Finding]) -> str:
    if not findings:
        return "<p>No findings.</p>"
    rows = []
    for item in sorted(findings, key=lambda x: x.sort_key()):
        blog = escape(item.blog_url or "— site level —")
        evidence = f'<div class="ev">{escape(item.evidence)}</div>' if item.evidence else ""
        rows.append(
            "<tr>"
            f'<td><code>{escape(item.rule)}</code></td>'
            f'<td><span class="badge {item.severity}">{item.severity}</span></td>'
            f"<td>{escape(item.message)}<br><span class=\"ev\">{blog}</span>{evidence}</td>"
            f"<td><code>{escape(item.slug)}</code></td>"
            "</tr>"
        )
    return (
        '<div class="scroll"><table><thead><tr><th>rule</th><th>severity</th>'
        f"<th>finding</th><th>slug</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def _per_blog(summary: RunSummary, active: list[Finding]) -> str:
    blocks = []
    for page in summary.pages:
        mine = [item for item in active if item.blog_url == page.url]
        tally = counts(mine)
        label = (
            f"{tally[SEVERITY_ERROR]} errors · {tally[SEVERITY_WARN]} warnings · {tally[SEVERITY_INFO]} info"
            if mine
            else "clean"
        )
        body = _finding_rows(mine) if mine else "<p>No findings.</p>"
        blocks.append(
            f"<details><summary>{escape(page.slug)} — {label}</summary>"
            f'<p class="sub"><code>{escape(page.url)}</code> — HTTP {page.response.status}, '
            f"{page.word_count} words, {page.response.ttfb_ms} ms TTFB</p>{body}</details>"
        )
    return "".join(blocks)


def _fetched(ok: bool | None) -> str:
    if ok is None:
        return "unknown — crashed before discovery"
    return "yes" if ok else "no — see report for the resulting findings"


def _config(site, ctx: SiteContext | None) -> str:
    # ADR 0003: silent-green is the failure mode being engineered against.
    # A partial sitemap fetch or an unreachable listing/robots.txt now
    # degrades to silence in the rule findings (B4/H2/C3 stay quiet rather
    # than firing on incomplete data) — so whether discovery itself actually
    # succeeded must always be visible somewhere, even on an all-clear run.
    rows = [
        ("canonical host", site.canonical_host),
        ("origin", site.origin_host),
        ("asset prefixes", ", ".join(site.origin_asset_prefixes)),
        ("allowlisted subdomains", ", ".join(sorted(site.allowed_subdomains)) or "none"),
        ("blogs audited", str(site.blog_count)),
        ("contextual internal links minimum", str(site.threshold("internal_links_min"))),
        ("CMS parity", "enabled" if site.cms_api else "disabled"),
        ("suppressed rules", ", ".join(sorted(site.suppress)) or "none"),
        ("blog listing fetched", _fetched(ctx.listing_ok if ctx else None)),
        ("sitemap.xml fetched", _fetched(ctx.sitemap_ok if ctx else None)),
        ("robots.txt fetched", _fetched(ctx.robots_ok if ctx else None)),
    ]
    body = "".join(f"<tr><th>{escape(k)}</th><td><code>{escape(v)}</code></td></tr>" for k, v in rows)
    return f'<div class="scroll"><table>{body}</table></div>'


def render_report(summary: RunSummary) -> str:
    active, suppressed = partition(summary.findings, summary.site)
    tally = counts(active)
    bad = (tally[SEVERITY_ERROR] + tally[SEVERITY_WARN]) > 0 or bool(summary.error)
    verdict = (
        f"{tally[SEVERITY_ERROR]} error(s) and {tally[SEVERITY_WARN]} warning(s) need attention."
        if bad
        else "No findings that require attention."
    )
    harness = (
        f'<div class="harness"><strong>Harness error</strong> — the audit did not complete normally.'
        f"<pre>{escape(summary.error)}</pre></div>"
        if summary.error
        else ""
    )
    suppressed_section = ""
    if suppressed:
        by_rule: dict[str, int] = {}
        for item in suppressed:
            by_rule[item.rule] = by_rule.get(item.rule, 0) + 1
        listing = ", ".join(f"{rule} ×{count}" for rule, count in sorted(by_rule.items()))
        suppressed_section = (
            "<h2>Suppressed</h2>"
            f'<p class="sub">Silenced by <code>suppress</code> in <code>data/seo_sites.json</code>: {escape(listing)}. '
            "These findings are reported but never trigger delivery.</p>"
            f"{_finding_rows(suppressed)}"
        )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Blog SEO Audit — {escape(summary.site.label)}</title>
<style>{CSS}</style></head>
<body><div class="wrap">
<h1>Blog SEO Audit — {escape(summary.site.label)}</h1>
<p class="sub">{escape(summary.site.listing_url)}</p>
<p class="sub">Run started {escape(summary.started_at)} · {summary.duration_s:.1f}s · {len(summary.pages)} blogs checked</p>
{harness}
<h2>Summary</h2>
<div class="band">
{_tile(tally[SEVERITY_ERROR], "errors", "error")}
{_tile(tally[SEVERITY_WARN], "warnings", "warn")}
{_tile(tally[SEVERITY_INFO], "info", "info")}
{_tile(len(suppressed), "suppressed", "ok")}
</div>
<p class="verdict{' bad' if bad else ''}">{escape(verdict)}</p>
<h2>Rule coverage</h2>
{_matrix(summary, active)}
<h2>Findings</h2>
{_finding_rows(active)}
{suppressed_section}
<h2>Per-blog detail</h2>
{_per_blog(summary, active)}
<h2>Configuration</h2>
{_config(summary.site, summary.ctx)}
</div></body></html>
"""
