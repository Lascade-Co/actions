"""Merge per-repo catchup artifacts into the daily file and index.

Reads every `summary-*.json` produced by the matrix jobs, drops repos with no
developer activity, and writes the consolidated daily file plus an updated
index inside a checked-out copy of the `catchup` repo.

  out-dir/
    daily/YYYY-MM-DD.json   <- {date, repos: [{repo, developers: [...]}]}
    index.json              <- {"daily":  ["daily/YYYY-MM-DD.json", ...],
                                 "repos":  ["Lascade-Co/foo", ...],
                                 "users":  [{"login", "name"}, ...]}

index.json keeps a running registry: the day's repos and developers are
appended to the `repos` and `users` arrays, deduped against existing entries
(repos by full-name, users by login or name).

Re-running for the same date overwrites the daily file and leaves index.json
idempotent (paths and registry entries are appended only when absent).

Usage:
    python scripts/catchup_collect.py \
        --artifacts-dir ./artifacts --date 2026-05-27 --out-dir ./catchup
"""

import argparse
import glob
import json
import os
import sys


def load_summaries(artifacts_dir):
    repos = []
    pattern = os.path.join(artifacts_dir, "**", "summary-*.json")
    for path in sorted(glob.glob(pattern, recursive=True)):
        with open(path) as fh:
            data = json.load(fh)
        if data.get("developers"):
            repos.append(data)
    repos.sort(key=lambda r: r["repo"].lower())
    return repos


def user_key(user):
    """Dedup key for a user: login when present, else display name."""
    return user.get("login") or user.get("name")


def merge_registries(index, repos):
    """Append the day's repos and users to the global index registries.

    Both lists preserve existing order and only gain entries not already
    present (deduped by repo full-name / user login-or-name).
    """
    index.setdefault("repos", [])
    index.setdefault("users", [])

    known_repos = set(index["repos"])
    for name in sorted(r["repo"] for r in repos):
        if name not in known_repos:
            known_repos.add(name)
            index["repos"].append(name)

    known_users = {user_key(u) for u in index["users"]}
    day_users = {}
    for repo in repos:
        for dev in repo["developers"]:
            entry = {"login": dev.get("login"), "name": dev.get("name")}
            day_users.setdefault(user_key(entry), entry)
    for key in sorted(day_users, key=lambda k: (k or "").lower()):
        if key not in known_users:
            known_users.add(key)
            index["users"].append(day_users[key])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--out-dir", required=True,
                        help="checked-out catchup repo root")
    args = parser.parse_args()

    repos = load_summaries(args.artifacts_dir)
    if not repos:
        print("No repos with activity; nothing to write.", file=sys.stderr)
        # Signal "empty" so the workflow can skip the commit step.
        output = os.environ.get("GITHUB_OUTPUT")
        if output:
            with open(output, "a") as fh:
                fh.write("wrote=false\n")
        return

    rel_path = f"daily/{args.date}.json"
    daily_path = os.path.join(args.out_dir, rel_path)
    os.makedirs(os.path.dirname(daily_path), exist_ok=True)
    with open(daily_path, "w") as fh:
        json.dump({"date": args.date, "repos": repos}, fh, indent=2,
                  ensure_ascii=False)
        fh.write("\n")

    index_path = os.path.join(args.out_dir, "index.json")
    if os.path.exists(index_path):
        with open(index_path) as fh:
            index = json.load(fh)
    else:
        index = {"daily": []}
    index.setdefault("daily", [])
    if rel_path not in index["daily"]:
        index["daily"].append(rel_path)
    merge_registries(index, repos)
    with open(index_path, "w") as fh:
        json.dump(index, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    print(f"Wrote {rel_path} ({len(repos)} repo(s)) and updated index.json",
          file=sys.stderr)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as fh:
            fh.write("wrote=true\n")


if __name__ == "__main__":
    main()
