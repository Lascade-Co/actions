#!/usr/bin/env python3
"""Turn one release into an SEO-validated draft in the site's CMS.

The public ``main`` entry point always returns zero. Draft generation is useful
release follow-up, never a condition for shipping the release itself.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from release_blog_check import better, blocking, render_report, validate
from release_blog_cms import (
    build_payload,
    fetch_link_candidates,
    find_by_marker,
    release_marker,
    requests_http,
    write_draft,
)
from release_blog_digest import build_digest, marketing_version_from_tag, previous_tag
from release_blog_draft import build_prompt, load_prompt, parse_output, run_codex
from seo_model import SEVERITY_ERROR, SEVERITY_WARN, load_site_config, resolve_site_for_repo


def log(message: str) -> None:
    print(f"release-blog: {message}", flush=True)


class ExitZeroArgumentParser(argparse.ArgumentParser):
    """Report invalid invocations without violating the exit-zero contract."""

    def error(self, message):
        raise ValueError(f"invalid arguments: {message}")


def build_parser() -> argparse.ArgumentParser:
    parser = ExitZeroArgumentParser(description="Write a release draft into the site's CMS.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--site", help="site config name")
    target.add_argument("--repo", help="owner/name resolved against each site's repos list")
    parser.add_argument("--config", default="seo_sites.json")
    parser.add_argument("--tag", required=True, help="release tag; the marketing version is derived from it")
    parser.add_argument("--repo-path", default=".", help="checkout from which to read the release digest")
    parser.add_argument("--base", help="release-digest base (default: the tag before --head)")
    parser.add_argument("--head", help="release-digest head (default: --tag)")
    parser.add_argument("--out", default="./out")
    parser.add_argument("--prompt", help="path or URL of RELEASE_BLOG.md (default: canonical raw URL)")
    parser.add_argument("--notes-file", help="release notes (default: <repo-path>/releasenotes.txt)")
    parser.add_argument("--notes-text", help="release notes inline, for tests and local runs")
    parser.add_argument("--diff-file", help="skip git and use this saved patch")
    parser.add_argument("--candidates-file", help="skip the CMS and use this link-candidate JSON")
    parser.add_argument("--html", help="skip Codex and validate this draft fragment")
    parser.add_argument("--meta", help="blog.json to use with --html")
    parser.add_argument("--no-retry", action="store_true")
    parser.add_argument("--ignore-marker", action="store_true")
    parser.add_argument("--cms-user", default=os.environ.get("CMS_USER", ""))
    parser.add_argument("--cms-password", default=os.environ.get("CMS_APP_PASSWORD", ""))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="stop before writing to the CMS")
    mode.add_argument("--publish", action="store_true", help="write the draft to the CMS")
    return parser


def _read_notes(args) -> str:
    if args.notes_text is not None:
        return args.notes_text
    path = Path(args.notes_file) if args.notes_file else Path(args.repo_path, "releasenotes.txt")
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _load_candidates(args, site, http):
    if args.candidates_file:
        from release_blog_cms import LinkCandidate

        raw = json.loads(Path(args.candidates_file).read_text(encoding="utf-8"))
        return [LinkCandidate(**item) for item in raw], "link candidates read from a file"
    return fetch_link_candidates(site, http=http)


def _notes_only_digest(notes: str) -> str:
    return "\n".join(
        (
            "## Release notes", "", notes.strip() or "unavailable", "",
            "## Commits", "", "```", "unavailable", "```", "",
            "## Changed files", "", "```", "unavailable", "```", "",
            "## Filtered diff", "", "```diff", "unavailable", "```", "",
        )
    )


def _digest(args, run) -> str:
    notes = _read_notes(args)
    if args.diff_file:
        from release_blog_digest import filter_patch, truncate

        patch = filter_patch(Path(args.diff_file).read_text(encoding="utf-8"))
        return "\n".join(
            (
                "## Release notes", "", notes.strip() or "unavailable", "",
                "## Filtered diff", "", "```diff", truncate(patch).strip() or "unavailable", "```", "",
            )
        )
    head = args.head or args.tag
    base = args.base or previous_tag(args.repo_path, head, run=run)
    if not base:
        log("no release-digest base could be determined; the digest carries release notes only")
        return _notes_only_digest(notes)
    return build_digest(args.repo_path, base, head, notes, run=run)


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _run(argv=None, *, http=None, run=None) -> int:
    args = build_parser().parse_args(argv)
    if bool(args.html) != bool(args.meta):
        raise ValueError("--html and --meta must be supplied together")

    http = http or requests_http
    run = run or subprocess.run
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    notes: list[str] = []

    try:
        if args.site:
            site = load_site_config(args.config, args.site)
        else:
            site, reason = resolve_site_for_repo(args.config, args.repo)
            if site is None:
                log(f"skipping: {reason}")
                return 0
    except (OSError, ValueError, KeyError) as exc:
        log(f"skipping: could not read the site config: {exc}")
        return 0
    log(f"site: {site.name} ({site.canonical_host})")

    if args.repo:
        repo = args.repo
    elif site.repos:
        repo = site.repos[0]
    else:
        repo = site.name
    marker = release_marker(repo, args.tag)
    auth = (args.cms_user, args.cms_password) if args.cms_user and args.cms_password else None

    existing = None
    if auth and not args.ignore_marker:
        existing = find_by_marker(site, marker, http=http, auth=auth)
        if existing and existing.get("status") == "publish":
            log(
                "skipping: this release is already a published blog "
                f"(id {existing['id']}, slug {existing['slug']!r})"
            )
            return 0
        if existing:
            log(f"an existing draft carries this marker (id {existing['id']}); it will be overwritten")
            notes.append(f"overwriting draft id {existing['id']}")

    try:
        candidates, note = _load_candidates(args, site, http)
    except (OSError, ValueError, TypeError) as exc:
        candidates, note = [], f"link-candidate load failed: {exc}"
    if note:
        log(note)
        notes.append(note)
    if not candidates:
        log("skipping: without link candidates a draft cannot carry contextual internal links")
        return 0
    log(f"{len(candidates)} link candidates")

    marketing_version = marketing_version_from_tag(args.tag)
    attempts = []
    if args.html:
        try:
            meta = json.loads(Path(args.meta).read_text(encoding="utf-8"))
            html = Path(args.html).read_text(encoding="utf-8")
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            log(f"skipping: could not read --html/--meta: {exc}")
            return 0
        attempts.append(validate(site, meta, html, candidates))
    else:
        try:
            base_prompt = load_prompt(args.prompt, http=http)
            digest = _digest(args, run)
        except (OSError, RuntimeError, ValueError) as exc:
            log(f"skipping: {exc}")
            return 0

        previous_html = None
        previous_findings = None
        attempt_limit = 1 if args.no_retry else 2
        for index in range(attempt_limit):
            prompt = build_prompt(
                base_prompt=base_prompt,
                site=site,
                marketing_version=marketing_version,
                marker=marker,
                digest=digest,
                candidates=candidates,
                out_dir=str(out.resolve()),
                previous=previous_html,
                findings=previous_findings,
            )
            name = "prompt.md" if index == 0 else "prompt-retry.md"
            _write_text(out / name, prompt)
            ok, detail = run_codex(
                prompt,
                str(out),
                run=run,
                status=lambda message, attempt=index + 1: log(f"attempt {attempt}: {message}"),
            )
            log(f"attempt {index + 1}: {detail}")
            if not ok:
                notes.append(detail)
                break
            meta, html, error = parse_output(str(out))
            if error:
                log(f"attempt {index + 1}: {error}")
                notes.append(error)
                if index + 1 < attempt_limit:
                    continue
                break
            attempt = validate(site, meta, html, candidates)
            attempts.append(attempt)
            log(f"attempt {index + 1}: {attempt.errors} errors, {attempt.warns} warns")
            for finding in attempt.findings:
                if finding.severity not in (SEVERITY_ERROR, SEVERITY_WARN):
                    continue
                evidence = f" [{finding.evidence}]" if finding.evidence else ""
                log(
                    f"attempt {index + 1}: {finding.severity} {finding.rule}: "
                    f"{finding.message}{evidence}"
                )
            if not blocking(attempt):
                break
            previous_html, previous_findings = html, attempt.findings

    if not attempts:
        _write_text(
            out / "validation.txt",
            "no attempt produced a draft\n" + "".join(f"note: {note}\n" for note in notes),
        )
        log("skipping: no attempt produced a draft")
        return 0

    chosen = attempts[0]
    for attempt in attempts[1:]:
        chosen = better(chosen, attempt)
    _write_text(out / "validation.txt", render_report(attempts, chosen, notes))
    _write_text(out / "blog.html", chosen.html)
    _write_text(out / "blog.json", json.dumps(chosen.meta, indent=2))

    payload = build_payload(chosen.meta, chosen.html)
    draft_id = existing["id"] if existing else None
    path = f"posts/{draft_id}" if draft_id else "posts"
    target = f"https://{site.origin_host}/wp-json/wp/v2/{path}"
    _write_text(
        out / "wp-payload.json",
        json.dumps({"url": target, "method": "POST", "body": payload}, indent=2),
    )

    if args.dry_run:
        log(f"dry run: would POST to {target}")
        return 0
    if not auth:
        message = "no CMS credentials (CMS_USER / CMS_APP_PASSWORD); draft not written"
        log(message)
        with (out / "validation.txt").open("a", encoding="utf-8") as handle:
            handle.write(f"note: {message}\n")
        return 0
    _ok, detail = write_draft(site, payload, http=http, auth=auth, draft_id=draft_id)
    log(detail)
    with (out / "validation.txt").open("a", encoding="utf-8") as handle:
        handle.write(f"note: {detail}\n")
    return 0


def main(argv=None, *, http=None, run=None) -> int:
    """Run the pipeline and convert every failure into an exit-zero skip."""
    try:
        return _run(argv, http=http, run=run)
    except SystemExit as exc:
        # ``--help`` and any argparse-controlled exit remain non-blocking.
        if exc.code not in (0, None):
            log(f"skipping: argument parser exited {exc.code}")
        return 0
    except Exception as exc:
        log(f"unexpected failure, exiting 0 anyway: {type(exc).__name__}: {exc}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
