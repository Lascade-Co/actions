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
    if "lock_file" in payload and payload["lock_file"] != EXPECTED_LOCK:
        raise ValueError("dispatch lock_file must be release/lock.json")

    if event_type == "deploy":
        sha = payload.get("sha")
        if not isinstance(sha, str) or SHA.fullmatch(sha) is None:
            raise ValueError("dispatch sha must be 40 lowercase hexadecimal characters")
        if "ref" in payload and payload["ref"] != "refs/heads/main":
            raise ValueError("production deploys must target refs/heads/main")
        if "pr" in payload and payload["pr"] is not None:
            raise ValueError("production deploy payload pr must be null")
        if "source_event" in payload and payload["source_event"] != "push":
            raise ValueError("production source_event must be push")
    else:
        pr = payload.get("pr")
        if not isinstance(pr, int) or isinstance(pr, bool) or pr < 1:
            raise ValueError("PR number must be a positive integer")
        if "source_event" in payload and payload["source_event"] != "pull_request_target":
            raise ValueError("PR dispatch source_event must be pull_request_target")
        if "ref" in payload and payload["ref"] != f"refs/pull/{pr}/merge":
            raise ValueError("PR dispatch ref must identify its GitHub merge ref")
        if "base_ref" in payload and payload["base_ref"] != "main":
            raise ValueError("TARS pull requests must target main")
        for name in ("head_sha", "sha", "base_sha"):
            if name not in payload:
                continue
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
        "sha": payload.get("sha") or "",
        "ref": payload.get("ref") or "",
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
