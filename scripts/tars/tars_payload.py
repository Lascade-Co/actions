#!/usr/bin/env python3
"""Validate a TARS repository_dispatch payload without evaluating it as shell."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


SHA = re.compile(r"^[0-9a-f]{40}$")
EXPECTED_REPOSITORY = "Lascade-Co/TARS"
CI_REQUIRED_KEYS = {"repo", "head_sha", "pr"}
# Pull requests execute the trigger from the target branch, so accept the former
# metadata fields until every open pull request has rebased onto the new trigger.
CI_ALLOWED_KEYS = CI_REQUIRED_KEYS | {
    "ref",
    "sha",
    "base_ref",
    "base_sha",
    "lock_file",
    "source_event",
}
DEPLOY_REQUIRED_KEYS = {"repo", "sha"}
DEPLOY_ALLOWED_KEYS = DEPLOY_REQUIRED_KEYS | {
    "ref",
    "pr",
    "lock_file",
    "source_event",
}


def load_payload(event_path: Path, event_type: str) -> dict[str, str | int | None]:
    event = json.loads(event_path.read_text(encoding="utf-8"))
    payload = event.get("client_payload")
    if not isinstance(payload, dict):
        raise ValueError("repository_dispatch client_payload is required")

    required_keys = DEPLOY_REQUIRED_KEYS if event_type == "deploy" else CI_REQUIRED_KEYS
    allowed_keys = DEPLOY_ALLOWED_KEYS if event_type == "deploy" else CI_ALLOWED_KEYS
    if not required_keys.issubset(payload) or not set(payload).issubset(allowed_keys):
        missing = sorted(required_keys - set(payload))
        extra = sorted(set(payload) - allowed_keys)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise ValueError("dispatch payload keys are invalid: " + "; ".join(details))

    if payload.get("repo") != EXPECTED_REPOSITORY:
        raise ValueError("dispatch repository is not Lascade-Co/TARS")

    if event_type == "deploy":
        sha = payload.get("sha")
        if not isinstance(sha, str) or SHA.fullmatch(sha) is None:
            raise ValueError("dispatch sha must be 40 lowercase hexadecimal characters")
    else:
        pr = payload.get("pr")
        if not isinstance(pr, int) or isinstance(pr, bool) or pr < 1:
            raise ValueError("PR number must be a positive integer")
        head_sha = payload.get("head_sha")
        if not isinstance(head_sha, str) or SHA.fullmatch(head_sha) is None:
            raise ValueError("PR head_sha must be a full lowercase Git SHA")

    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("event", type=Path)
    parser.add_argument("--type", choices=("ci", "deploy"), required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    payload = load_payload(args.event, args.type)
    if args.type == "deploy":
        values = {"repo": payload["repo"], "sha": payload["sha"]}
    else:
        values = {"repo": payload["repo"], "head_sha": payload["head_sha"]}
    rendered = "".join(f"{key}={value}\n" for key, value in values.items())
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
