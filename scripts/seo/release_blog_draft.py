"""Assemble the writer prompt, invoke Codex, and parse its draft output."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from seo_model import SEVERITY_ERROR, SEVERITY_WARN

PROMPT_URL = "https://raw.githubusercontent.com/Lascade-Co/actions/main/data/RELEASE_BLOG.md"
CODEX_TIMEOUT = 900


def load_prompt(source: str | None = None, *, http=None) -> str:
    """Read a local prompt when supplied, otherwise fetch the canonical prompt."""
    if source and not source.startswith(("http://", "https://")):
        return Path(source).read_text(encoding="utf-8")

    from release_blog_cms import requests_http

    fetch = http or requests_http
    status, body = fetch("GET", source or PROMPT_URL)
    if status != 200:
        raise RuntimeError(f"could not fetch the prompt: HTTP {status}")
    return body


def _candidate_lines(candidates) -> str:
    lines = []
    for index, candidate in enumerate(candidates, start=1):
        title = candidate.title or f"(no title — slug: {candidate.slug})"
        excerpt = f" — {candidate.excerpt}" if candidate.excerpt else ""
        lines.append(f"{index}. {title} | {candidate.url}{excerpt}")
    return "\n".join(lines) if lines else "(none available)"


def build_prompt(
    *,
    base_prompt: str,
    site,
    marketing_version: str,
    marker: str,
    digest: str,
    candidates,
    out_dir: str,
    previous: str | None = None,
    findings=None,
) -> str:
    """Append authoritative run data and actionable retry findings."""
    sections = [
        base_prompt.rstrip("\n"),
        "",
        "## RUN CONTEXT",
        "",
        f"- Site: {site.label} ({site.canonical_host})",
        f"- Blogs live under: https://{site.canonical_host}{site.listing_path}/<slug>",
        f"- Marketing version shipping now: {marketing_version}",
        f"- Write `blog.json` and `blog.html` into: {out_dir}",
        f"- Close `blog.html` with exactly this line:\n\n      <!-- {marker} -->",
        "",
        "## LINK CANDIDATES",
        "",
        "Use 3–5 of these, copied verbatim. Anything not on this list does not exist:",
        "",
        _candidate_lines(candidates),
        "",
        "## RELEASE DIGEST",
        "",
        digest.rstrip("\n"),
        "",
    ]

    actionable = [
        finding
        for finding in (findings or [])
        if finding.severity in (SEVERITY_ERROR, SEVERITY_WARN)
    ]
    if previous and actionable:
        sections += [
            "## YOUR PREVIOUS ATTEMPT FAILED VALIDATION",
            "",
            "Fix every point below. Keep what already worked — this is a revision, not a restart.",
            "",
            *(f"- [{item.severity}] {item.rule}: {item.message}" for item in actionable),
            "",
            "The HTML you produced last time:",
            "",
            "```html",
            previous.strip(),
            "```",
            "",
        ]
    return "\n".join(sections)


def run_codex(prompt_text: str, out_dir: str, *, run=subprocess.run) -> tuple[bool, str]:
    """Run Codex once in an isolated output directory without raising."""
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    try:
        for name in ("blog.json", "blog.html"):
            output.joinpath(name).unlink(missing_ok=True)
    except OSError as exc:
        return False, f"could not clear stale codex output: {exc}"
    try:
        result = run(
            ["codex", "exec", "--ephemeral", "--sandbox", "workspace-write", "-"],
            input=prompt_text,
            capture_output=True,
            text=True,
            timeout=CODEX_TIMEOUT,
            check=False,
            cwd=out_dir,
        )
    except FileNotFoundError:
        return False, "codex is not installed or not on PATH"
    except subprocess.TimeoutExpired:
        return False, f"codex timed out after {CODEX_TIMEOUT}s"
    except OSError as exc:
        return False, f"codex could not be started: {exc}"
    if result.returncode != 0:
        tail = (result.stderr or result.stdout or "").strip()[-800:]
        return False, f"codex exited {result.returncode}: {tail}"
    return True, "codex completed"


def parse_output(out_dir: str) -> tuple[dict | None, str, str]:
    """Read ``blog.json`` and ``blog.html`` without letting shape errors escape."""
    meta_path = Path(out_dir, "blog.json")
    html_path = Path(out_dir, "blog.html")
    if not meta_path.exists():
        return None, "", f"codex wrote no blog.json in {out_dir}"
    if not html_path.exists():
        return None, "", f"codex wrote no blog.html in {out_dir}"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        return None, "", f"blog.json is unparseable: {exc}"
    if not isinstance(meta, dict):
        return None, "", "blog.json is not a JSON object"
    try:
        html = html_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return None, "", f"blog.html is unreadable: {exc}"
    if not html.strip():
        return None, "", "blog.html is empty"
    return meta, html, ""
