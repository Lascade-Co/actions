#!/usr/bin/env python3
"""Run Android Lint and fail only if issues are found in changed files.

Usage:
    python3 lint_android.py --base-ref main
    python3 lint_android.py --base-ref main --run-lint
    python3 lint_android.py --report <lint-xml> --changed <file1> <file2> ...

Changed files are detected automatically via git diff against --base-ref.
You can also pass them explicitly with --changed or CHANGED_FILES env var.
"""

import argparse
import glob
import os
import subprocess
import sys
import xml.etree.ElementTree as ET


def find_lint_report():
    """Auto-discover the lint XML report under build/."""
    matches = glob.glob("**/build/reports/lint-results-debug.xml", recursive=True)
    return matches[0] if matches else None


def run_gradle_lint():
    """Run ./gradlew lintDebug."""
    if not os.path.isfile("gradlew"):
        print("gradlew not found, skipping lint run.")
        return
    subprocess.run(["./gradlew", "--no-daemon", "lintDebug"], check=False)


def get_changed_files(base_ref):
    """Compute changed files by diffing HEAD against a base ref."""
    # Fetch the base branch from origin
    fetch = subprocess.run(
        ["git", "fetch", "origin", base_ref],
        capture_output=True,
        text=True,
    )
    if fetch.returncode != 0:
        print(f"Warning: git fetch failed: {fetch.stderr.strip()}")

    # Try three-dot diff with FETCH_HEAD (works after git fetch origin <branch>)
    result = subprocess.run(
        ["git", "diff", "--name-only", "FETCH_HEAD...HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Fallback: try origin/<base_ref> in case it already exists
        result = subprocess.run(
            ["git", "diff", "--name-only", f"origin/{base_ref}...HEAD"],
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        print(f"git diff failed: {result.stderr.strip()}")
        return []
    return [f for f in result.stdout.strip().split("\n") if f]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Android Lint filtered to changed files"
    )
    parser.add_argument("--report", help="Path to lint-results-debug.xml")
    parser.add_argument("--changed", nargs="*", help="Changed file paths (explicit)")
    parser.add_argument(
        "--base-ref",
        help="Base branch to diff against (e.g. main). Detects changed files via git.",
    )
    parser.add_argument(
        "--run-lint",
        action="store_true",
        help="Run ./gradlew lintDebug before checking the report",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.run_lint:
        run_gradle_lint()

    # Resolve report path
    report_path = args.report or find_lint_report()
    if not report_path or not os.path.isfile(report_path):
        print("No lint report found, skipping.")
        sys.exit(0)

    # Resolve changed files: --changed > --base-ref > CHANGED_FILES env
    changed = args.changed
    if not changed and args.base_ref:
        changed = get_changed_files(args.base_ref)
    if not changed:
        env_files = os.environ.get("CHANGED_FILES", "").strip()
        changed = env_files.split() if env_files else []

    if not changed:
        print("No changed files detected, skipping lint filter.")
        sys.exit(0)

    print(f"Checking lint results against {len(changed)} changed file(s)...")

    # Parse the lint XML report
    tree = ET.parse(report_path)
    root = tree.getroot()

    repo_root = os.getcwd()
    changed_abs = {os.path.normpath(os.path.join(repo_root, f)) for f in changed}

    issues = []
    for issue in root.findall("issue"):
        severity = issue.get("severity", "")
        if severity in ("Information", "Ignore"):
            continue
        for loc in issue.findall("location"):
            fpath = os.path.normpath(loc.get("file", ""))
            if fpath in changed_abs:
                line = loc.get("line", "?")
                col = loc.get("column", "?")
                rel = os.path.relpath(fpath, repo_root)
                issues.append(
                    f"{rel}:{line}:{col}: {severity}: "
                    f"[{issue.get('id')}] {issue.get('message')}"
                )

    if issues:
        print(f"\n::error::Android Lint found {len(issues)} issue(s) in changed files:\n")
        for i in issues:
            print(i)
        sys.exit(1)
    else:
        print("Android Lint: no issues in changed files.")


if __name__ == "__main__":
    main()