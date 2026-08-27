"""Build the release digest shown to the draft writer."""

from __future__ import annotations

import re
import subprocess

PATCH_BYTE_CAP = 200 * 1024

VERSION_SHAPE = re.compile(r"^\d+(\.\d+)*$")

# Matched against the b-side path of each ``diff --git`` section.
DROP_PATH_PATTERNS = (
    re.compile(r"\.lock$|\.lockb$|lockfile$", re.IGNORECASE),
    re.compile(r"(^|/)(build|generated|node_modules|Pods)/"),
    re.compile(r"\.pb\.(go|py|dart|swift|kt)$|\.g\.dart$|\.freezed\.dart$|_pb2\.py$"),
    re.compile(r"(^|/)values-[a-z]{2}(-[A-Za-z0-9]+)?/strings\.xml$"),
)

FILE_HEADER = re.compile(r"^diff --git a/(?P<a>\S+) b/(?P<b>\S+)$")


def marketing_version_from_tag(tag: str) -> str:
    """Return a version-shaped tag without its optional leading ``v``."""
    candidate = tag[1:] if tag[:1].lower() == "v" else tag
    return candidate if VERSION_SHAPE.match(candidate) else tag


def _is_dropped(path: str) -> bool:
    return any(pattern.search(path) for pattern in DROP_PATH_PATTERNS)


def _keep_section(path: str | None, section: list[str]) -> bool:
    if not section:
        return False
    if path is not None and _is_dropped(path):
        return False
    return not any(line.startswith("Binary files ") for line in section)


def filter_patch(patch: str) -> str:
    """Drop whole file sections that cannot support a feature claim."""
    kept: list[str] = []
    path: str | None = None
    section: list[str] = []
    for line in patch.splitlines():
        header = FILE_HEADER.match(line)
        if header:
            if _keep_section(path, section):
                kept.extend(section)
            path, section = header.group("b"), [line]
            continue
        section.append(line)
    if _keep_section(path, section):
        kept.extend(section)
    text = "\n".join(kept).strip()
    return text + "\n" if text else ""


def truncate(text: str, limit: int = PATCH_BYTE_CAP) -> str:
    """Cap a UTF-8 patch and make truncation explicit."""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    head = encoded[:limit].decode("utf-8", errors="ignore")
    return f"{head}\n\n[diff truncated — {limit} of {len(encoded)} bytes shown]\n"


def _git(run, repo_path: str, *args: str) -> str:
    try:
        result = run(
            ["git", "-C", repo_path, *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    return result.stdout if result.returncode == 0 else ""


def previous_tag(repo_path: str, head: str, *, run=subprocess.run) -> str | None:
    """Return the tag immediately before ``head`` when one exists."""
    out = _git(run, repo_path, "describe", "--tags", "--abbrev=0", f"{head}^")
    return out.strip() or None


def build_digest(repo_path: str, base: str, head: str, notes: str, *, run=subprocess.run) -> str:
    """Assemble release notes, commit subjects, diffstat, and filtered patch."""
    span = f"{base}..{head}"
    commits = _git(run, repo_path, "log", "--oneline", "--no-merges", span)
    stat = _git(run, repo_path, "diff", "--stat", span)
    patch = filter_patch(_git(run, repo_path, "diff", span))
    return "\n".join(
        (
            "## Release notes",
            "",
            notes.strip() or "unavailable",
            "",
            "## Commits",
            "",
            "```",
            commits.strip() or "unavailable",
            "```",
            "",
            "## Changed files",
            "",
            "```",
            stat.strip() or "unavailable",
            "```",
            "",
            "## Filtered diff",
            "",
            "```diff",
            truncate(patch).strip() or "unavailable",
            "```",
            "",
        )
    )
