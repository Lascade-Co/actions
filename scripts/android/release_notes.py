"""Generate releasenotes.txt from the supplied diff using the Claude API."""

import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ai"))
from claude_api import ClaudeError, generate_text


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--out", default="releasenotes.txt")
    args = parser.parse_args(argv)
    try:
        text = generate_text(Path(args.prompt).read_text(encoding="utf-8"), max_tokens=4096)
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    except (ClaudeError, OSError) as exc:
        print(f"Release notes generation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
