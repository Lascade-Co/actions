#!/usr/bin/env python3
"""Validate a TARS repository_dispatch payload without evaluating it as shell."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


SHA = re.compile(r"^[0-9a-f]{40}$")
EXPECTED_REPOSITORY = "Lascade-Co/TARS"
EXPECTED_LOCK = "release/lock.json"
CI_KEYS = {
    "repo",
    "ref",
    "sha",
    "head_sha",
    "pr",
    "base_ref",
    "base_sha",
    "lock_file",
    "source_event",
}
DEPLOY_KEYS = {"repo", "ref", "sha", "pr", "lock_file", "source_event"}


def load_payload(event_path: Path, event_type: str) -> dict[str, str | int | None]:
    event = json.loads(event_path.read_text(encoding="utf-8"))
    payload = event.get("client_payload")
    if not isinstance(payload, dict):
        raise ValueError("repository_dispatch client_payload is required")

    expected_keys = DEPLOY_KEYS if event_type == "deploy" else CI_KEYS
    if set(payload) != expected_keys:
        missing = sorted(expected_keys - set(payload))
        extra = sorted(set(payload) - expected_keys)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise ValueError("dispatch payload keys are invalid: " + "; ".join(details))

    if payload.get("repo") != EXPECTED_REPOSITORY:
        raise ValueError("dispatch repository is not Lascade-Co/TARS")
    sha = payload.get("sha")
    if not isinstance(sha, str) or SHA.fullmatch(sha) is None:
        raise ValueError("dispatch sha must be 40 lowercase hexadecimal characters")
    if payload.get("lock_file") != EXPECTED_LOCK:
        raise ValueError("dispatch lock_file must be release/lock.json")

    ref = payload.get("ref")
    if not isinstance(ref, str) or not ref.startswith("refs/"):
        raise ValueError("dispatch ref must be a full refs/... name")

    if event_type == "deploy":
        if ref != "refs/heads/main":
            raise ValueError("production deploys must target refs/heads/main")
        if payload.get("pr") is not None:
            raise ValueError("production deploy payload pr must be null")
        if payload.get("source_event") != "push":
            raise ValueError("production source_event must be push")
    else:
        pr = payload.get("pr")
        if not isinstance(pr, int) or isinstance(pr, bool) or pr < 1:
            raise ValueError("PR number must be a positive integer")
        if payload.get("source_event") != "pull_request_target":
            raise ValueError("PR dispatch source_event must be pull_request_target")
        if ref != f"refs/pull/{pr}/merge":
            raise ValueError("PR dispatch ref must identify its synthetic merge commit")
        if payload.get("base_ref") != "main":
            raise ValueError("TARS pull requests must target main")
        for name in ("head_sha", "base_sha"):
            value = payload.get(name)
            if not isinstance(value, str) or SHA.fullmatch(value) is None:
                raise ValueError(f"PR {name} must be a full lowercase Git SHA")

    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("event", type=Path)
    parser.add_argument("--type", choices=("ci", "deploy"), required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    payload = load_payload(args.event, args.type)
    values = {
        "repo": payload["repo"],
        "sha": payload["sha"],
        "ref": payload["ref"],
        "pr": payload.get("pr") or "",
        "head_sha": payload.get("head_sha") or "",
        "base_sha": payload.get("base_sha") or "",
    }
    rendered = "".join(f"{key}={value}\n" for key, value in values.items())
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
