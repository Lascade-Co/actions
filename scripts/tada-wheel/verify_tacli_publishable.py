#!/usr/bin/env python3
"""Refuse to publish the public TADA distribution when it is not fit to go out.

One gate, which produces a clear message instead of a confusing failure later: the version
must not already exist on PyPI. Releases are immutable -- a version is uploaded once and
never replaced -- and a re-upload fails with a 400 that reads like a credential problem
rather than what it is.

Reads the distribution name from the pyproject rather than hardcoding it, so a rename needs
no change here.

Writes `dist` and `version` to $GITHUB_OUTPUT when set, so the workflow can assert later that
exactly the expected artifact is staged.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

PYPI_JSON_URL = "https://pypi.org/pypi/{dist}/json"


def read_project(pyproject: Path) -> dict:
    return tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]


def released_versions(dist: str) -> set[str] | None:
    """Versions already on PyPI, or None when the project does not exist yet."""

    url = PYPI_JSON_URL.format(dist=dist)
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise SystemExit(
            f"could not query PyPI for {dist} (HTTP {error.code}); refusing to guess"
        ) from error
    except urllib.error.URLError as error:
        raise SystemExit(f"could not reach PyPI: {error.reason}") from error
    return set(payload.get("releases", {}))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pyproject", type=Path, required=True)
    args = parser.parse_args()

    project = read_project(args.pyproject)
    dist = project["name"]
    version = project["version"]

    existing = released_versions(dist)
    if existing is None:
        print(f"{dist} is not yet on PyPI; this is the first release")
    elif version in existing:
        raise SystemExit(
            f"{dist} {version} is already on PyPI and versions are immutable. "
            f"Bump the version in {args.pyproject} (and the matching pin in the private "
            "package) first."
        )
    else:
        print(f"{dist} {version} is not yet on PyPI")

    print(f"publishing {dist} {version}")
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(f"dist={dist}\nversion={version}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
