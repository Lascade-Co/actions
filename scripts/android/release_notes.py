"""Generate releasenotes.txt from the supplied diff using the Claude API, or check an existing file.

Google Play rejects release notes over 500 characters per language (the same text also feeds the
App Store), so generated notes over the limit are rewritten by Claude, never truncated: cutting
text can drop words mid-sentence. `--check` validates a hand-written file the same way.
"""

import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ai"))
from claude_api import ClaudeError, generate_text

# Play's per-language limit. Length is counted in UTF-16 code units (an emoji counts as 2),
# the conservative reading of Play's count; the prompt asks for 450 to leave headroom.
MAX_CHARS = 500
TARGET_CHARS = 450
MAX_REWRITES = 3
# Used when generation fails: release notes must never block a release (user rule 2026-10-26).
FALLBACK_NOTES = "Bug fixes and performance improvements"


def play_length(text):
    return len(text.encode("utf-16-le")) // 2


def generate_within_limit(prompt, generate=None):
    generate = generate or generate_text
    text = generate(prompt, max_tokens=4096).strip()
    for _ in range(MAX_REWRITES):
        if play_length(text) <= MAX_CHARS:
            return text
        text = generate(
            prompt + "\n\n## Rewrite required\n\n"
            f"Your previous release notes were {play_length(text)} characters; Google Play accepts at most "
            f"{MAX_CHARS}. Rewrite them in at most {TARGET_CHARS} characters by merging related items or "
            "dropping the least user-visible ones. Keep every bullet a complete sentence. Previous notes:\n\n"
            + text,
            max_tokens=4096,
        ).strip()
    if play_length(text) > MAX_CHARS:
        raise ClaudeError("notes-too-long")
    return text


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prompt", help="Prompt file with the appended diff (generate mode)")
    parser.add_argument("--out", default="releasenotes.txt")
    parser.add_argument("--check", metavar="FILE", help="Only validate an existing notes file's length")
    args = parser.parse_args(argv)
    if args.check:
        length = play_length(Path(args.check).read_text(encoding="utf-8").strip())
        if length > MAX_CHARS:
            print(f"{args.check} is {length} characters; Google Play allows at most {MAX_CHARS}. "
                  "Shorten it (do not truncate mid-sentence).", file=sys.stderr)
            return 1
        print(f"{args.check}: {length}/{MAX_CHARS} characters")
        return 0
    if not args.prompt:
        parser.error("--prompt is required unless --check is used")
    try:
        text = generate_within_limit(Path(args.prompt).read_text(encoding="utf-8"))
    except (ClaudeError, OSError) as exc:
        print(f"::warning title=Release notes::Generation failed ({exc}); using the fallback notes")
        text = FALLBACK_NOTES
    Path(args.out).write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
