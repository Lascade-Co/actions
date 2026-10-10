"""Assemble the writer prompt, invoke Claude, and parse its draft output."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ai"))
from claude_api import ClaudeError, generate_json

from seo_model import SEVERITY_ERROR, SEVERITY_WARN

PROMPT_URL = "https://raw.githubusercontent.com/Lascade-Co/actions/main/data/RELEASE_BLOG.md"
CLAUDE_TIMEOUT = 900
CLAUDE_STATUS_INTERVAL = 20


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
        "- Return one JSON object with `meta` (blog metadata) and `html` (the draft body).",
        f"- Close the HTML with exactly this line:\n\n      <!-- {marker} -->",
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


def _emit_status(status: Callable[[str], None] | None, message: str) -> None:
    """Report progress without allowing a display failure to stop generation."""
    if status is None:
        return
    try:
        status(message)
    except Exception:
        pass


def run_claude(
    prompt_text: str,
    out_dir: str,
    *,
    generate=None,
    status: Callable[[str], None] | None = None,
    status_interval: float = CLAUDE_STATUS_INTERVAL,
) -> tuple[bool, str]:
    """Request a draft from the API and save the same two validated artifacts."""
    generate = generate or generate_json
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    try:
        for name in ("blog.json", "blog.html"):
            output.joinpath(name).unlink(missing_ok=True)
    except OSError as exc:
        return False, f"could not clear stale claude output: {exc}"

    stopped = threading.Event()
    heartbeat = None
    _emit_status(status, "Claude generation started; this can take several minutes")
    if status is not None and status_interval > 0:
        started_at = time.monotonic()

        def report_while_running():
            while not stopped.wait(status_interval):
                elapsed = int(time.monotonic() - started_at)
                _emit_status(status, f"Claude is still generating ({elapsed}s elapsed)")

        heartbeat = threading.Thread(target=report_while_running, daemon=True)
        heartbeat.start()
    try:
        try:
            result = generate(prompt_text, max_tokens=16384, timeout=CLAUDE_TIMEOUT)
            output.joinpath("blog.json").write_text(
                json.dumps(result.get("meta"), ensure_ascii=False), encoding="utf-8")
            html = result.get("html")
            if isinstance(html, str):
                output.joinpath("blog.html").write_text(html, encoding="utf-8")
        except ClaudeError as exc:
            return False, str(exc)
        except (OSError, ValueError, TypeError, AttributeError):
            return False, "Claude draft could not be saved"

    finally:
        stopped.set()
        if heartbeat is not None:
            heartbeat.join()
    return True, "Claude completed"


def parse_output(out_dir: str) -> tuple[dict | None, str, str]:
    """Read ``blog.json`` and ``blog.html`` without letting shape errors escape."""
    meta_path = Path(out_dir, "blog.json")
    html_path = Path(out_dir, "blog.html")
    if not meta_path.exists():
        return None, "", f"Claude returned no blog.json in {out_dir}"
    if not html_path.exists():
        return None, "", f"Claude returned no blog.html in {out_dir}"
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
