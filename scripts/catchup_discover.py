"""Discover Lascade-Co repos that received commits in the last N hours.

Lists every repository the GitHub App installation can access (via the
`gh` CLI with an installation token), drops archived repos, and keeps the
ones whose `pushed_at` falls within the window. Emits a GitHub Actions
matrix and a `has_repos` flag so the workflow can fan out one job per
active repo (or skip the day entirely when nothing happened).

Forks are intentionally kept; archived repos are dropped.

Usage:
    GH_TOKEN=$(gh auth token) python scripts/catchup_discover.py
    GH_TOKEN=... python scripts/catchup_discover.py --hours 24

Requires: the `gh` CLI authenticated via the GH_TOKEN environment variable.
Writes `matrix=` and `has_repos=` to $GITHUB_OUTPUT when set, else stdout.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone


def list_installation_repos():
    """Return all repos the installation token can access as dicts."""
    # gh --paginate follows Link headers; --jq streams one repo per line
    # across every page of the {total_count, repositories[]} response.
    result = subprocess.run(
        ["gh", "api", "--paginate", "/installation/repositories",
         "--jq", ".repositories[]"],
        check=True, capture_output=True, text=True,
    )
    repos = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            repos.append(json.loads(line))
    return repos


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=24,
                        help="Look-back window in hours (default: 24).")
    args = parser.parse_args()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    active = []
    for repo in list_installation_repos():
        if repo.get("archived"):
            continue
        pushed_at = repo.get("pushed_at")
        if not pushed_at:
            continue
        # pushed_at is ISO 8601 UTC, e.g. "2026-05-27T01:23:45Z".
        pushed = datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
        if pushed >= cutoff:
            active.append(repo["full_name"])

    active.sort()
    matrix = {"include": [{"repo": name} for name in active]}
    has_repos = "true" if active else "false"

    print(f"Found {len(active)} repo(s) active in the last {args.hours}h:",
          file=sys.stderr)
    for name in active:
        print(f"  - {name}", file=sys.stderr)

    output = os.environ.get("GITHUB_OUTPUT")
    lines = [f"matrix={json.dumps(matrix)}", f"has_repos={has_repos}"]
    if output:
        with open(output, "a") as fh:
            fh.write("\n".join(lines) + "\n")
    else:
        print("\n".join(lines))


if __name__ == "__main__":
    main()
