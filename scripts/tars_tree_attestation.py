#!/usr/bin/env python3
"""Validate TARS synthetic-merge and tested-tree deployment attestations."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


SHA = re.compile(r"[0-9a-f]{40}\Z")
TREE_CONTEXT_PREFIX = "TARS Central CI tree "
CENTRAL_RUN_URL_PREFIX = "https://github.com/Lascade-Co/actions/actions/runs/"


class AttestationError(ValueError):
    """A GitHub object does not prove the requested source tree was tested."""


def require_sha(value: Any, name: str) -> str:
    if not isinstance(value, str) or SHA.fullmatch(value) is None:
        raise AttestationError(f"{name} must be a full lowercase Git SHA")
    return value


def nested(document: dict[str, Any], *keys: str) -> Any:
    value: Any = document
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            raise AttestationError(f"GitHub response omitted {'.'.join(keys)}")
        value = value[key]
    return value


def validate_ci_pull_request(
    document: Any,
    repository: str,
    pr_number: int,
    merge_sha: str,
    head_sha: str,
    base_sha: str,
) -> None:
    if not isinstance(document, dict):
        raise AttestationError("GitHub pull request response must be an object")
    require_sha(merge_sha, "merge SHA")
    require_sha(head_sha, "head SHA")
    require_sha(base_sha, "base SHA")
    if document.get("number") != pr_number or document.get("state") != "open":
        raise AttestationError("pull request number or state changed before CI")
    if nested(document, "head", "repo", "full_name") != repository:
        raise AttestationError("pull request head is not in the trusted repository")
    if nested(document, "base", "repo", "full_name") != repository:
        raise AttestationError("pull request base repository changed before CI")
    if nested(document, "base", "ref") != "main":
        raise AttestationError("pull request no longer targets main")
    if nested(document, "head", "sha") != head_sha:
        raise AttestationError("pull request head changed before CI")
    if nested(document, "base", "sha") != base_sha:
        raise AttestationError("pull request base changed before CI")
    if document.get("merge_commit_sha") != merge_sha:
        raise AttestationError("synthetic merge commit changed before CI")


def validate_merge_commit(
    details: str,
    merge_sha: str,
    head_sha: str,
    base_sha: str,
) -> str:
    lines = details.splitlines()
    if len(lines) != 3:
        raise AttestationError("synthetic merge commit details are malformed")
    commit_sha = require_sha(lines[0], "checked-out commit SHA")
    tree_sha = require_sha(lines[1], "tested tree SHA")
    parents = lines[2].split()
    if commit_sha != require_sha(merge_sha, "merge SHA"):
        raise AttestationError("checkout is not the dispatched synthetic merge commit")
    expected_parents = [
        require_sha(base_sha, "base SHA"),
        require_sha(head_sha, "head SHA"),
    ]
    if parents != expected_parents:
        raise AttestationError("synthetic merge parents do not match base and head")
    return tree_sha


def tree_context(tree_sha: str) -> str:
    return TREE_CONTEXT_PREFIX + require_sha(tree_sha, "tree SHA")


def select_deploy_pull_request(
    document: Any,
    repository: str,
    main_sha: str,
) -> tuple[int, str]:
    require_sha(main_sha, "main SHA")
    if not isinstance(document, list):
        raise AttestationError("associated pull request response must be an array")
    if len(document) != 1:
        raise AttestationError("current main commit must have exactly one associated pull request")
    pull = document[0]
    if not isinstance(pull, dict):
        raise AttestationError("associated pull request must be an object")
    number = pull.get("number")
    if not isinstance(number, int) or isinstance(number, bool) or number < 1:
        raise AttestationError("associated pull request number is invalid")
    if pull.get("state") != "closed" or not pull.get("merged_at"):
        raise AttestationError("associated pull request is not merged")
    if pull.get("merge_commit_sha") != main_sha:
        raise AttestationError("associated pull request did not create current main")
    if nested(pull, "base", "repo", "full_name") != repository:
        raise AttestationError("associated pull request has the wrong base repository")
    if nested(pull, "base", "ref") != "main":
        raise AttestationError("associated pull request did not target main")
    if nested(pull, "head", "repo", "full_name") != repository:
        raise AttestationError("associated pull request head was not same-repository")
    return number, require_sha(nested(pull, "head", "sha"), "pull request head SHA")


def flatten_statuses(document: Any) -> list[dict[str, Any]]:
    if not isinstance(document, list):
        raise AttestationError("commit status response must be an array")
    raw_statuses: list[Any]
    if document and all(isinstance(page, list) for page in document):
        raw_statuses = [status for page in document for status in page]
    else:
        raw_statuses = document
    if not all(isinstance(status, dict) for status in raw_statuses):
        raise AttestationError("commit status response contains a non-object")
    return raw_statuses


def verify_tree_status(document: Any, tree_sha: str, expected_creator: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*\[bot\]", expected_creator):
        raise AttestationError("central GitHub App creator is invalid")
    context = tree_context(tree_sha)
    matching = [
        status for status in flatten_statuses(document) if status.get("context") == context
    ]
    if not matching:
        raise AttestationError("pull request head has no status for current main tree")
    # GitHub returns statuses in reverse chronological order, including across
    # pages, so the first exact context is authoritative.
    latest = matching[0]
    if latest.get("state") != "success":
        raise AttestationError("latest tested-tree status is not successful")
    target_url = latest.get("target_url")
    if not isinstance(target_url, str) or not target_url.startswith(
        CENTRAL_RUN_URL_PREFIX
    ):
        raise AttestationError("tested-tree status was not produced by central CI")
    if nested(latest, "creator", "login") != expected_creator:
        raise AttestationError("tested-tree status creator is not the central GitHub App")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def append_outputs(path: Path, values: dict[str, str | int]) -> None:
    with path.open("a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    ci_pr = commands.add_parser("verify-ci-pr")
    ci_pr.add_argument("--pr-file", required=True, type=Path)
    ci_pr.add_argument("--repo", required=True)
    ci_pr.add_argument("--pr", required=True, type=int)
    ci_pr.add_argument("--merge-sha", required=True)
    ci_pr.add_argument("--head-sha", required=True)
    ci_pr.add_argument("--base-sha", required=True)

    merge = commands.add_parser("verify-merge")
    merge.add_argument("--details-file", required=True, type=Path)
    merge.add_argument("--merge-sha", required=True)
    merge.add_argument("--head-sha", required=True)
    merge.add_argument("--base-sha", required=True)
    merge.add_argument("--github-output", required=True, type=Path)

    deploy_pr = commands.add_parser("select-deploy-pr")
    deploy_pr.add_argument("--prs-file", required=True, type=Path)
    deploy_pr.add_argument("--repo", required=True)
    deploy_pr.add_argument("--main-sha", required=True)
    deploy_pr.add_argument("--tree-sha", required=True)
    deploy_pr.add_argument("--github-output", required=True, type=Path)

    statuses = commands.add_parser("verify-statuses")
    statuses.add_argument("--statuses-file", required=True, type=Path)
    statuses.add_argument("--tree-sha", required=True)
    statuses.add_argument("--expected-creator", required=True)

    args = parser.parse_args()
    try:
        if args.command == "verify-ci-pr":
            validate_ci_pull_request(
                read_json(args.pr_file),
                args.repo,
                args.pr,
                args.merge_sha,
                args.head_sha,
                args.base_sha,
            )
        elif args.command == "verify-merge":
            tree_sha = validate_merge_commit(
                args.details_file.read_text(encoding="utf-8"),
                args.merge_sha,
                args.head_sha,
                args.base_sha,
            )
            append_outputs(
                args.github_output,
                {"tree_sha": tree_sha, "tree_context": tree_context(tree_sha)},
            )
        elif args.command == "select-deploy-pr":
            number, head_sha = select_deploy_pull_request(
                read_json(args.prs_file), args.repo, args.main_sha
            )
            append_outputs(
                args.github_output,
                {
                    "pr": number,
                    "head_sha": head_sha,
                    "tree_sha": require_sha(args.tree_sha, "tree SHA"),
                    "tree_context": tree_context(args.tree_sha),
                },
            )
        else:
            verify_tree_status(
                read_json(args.statuses_file), args.tree_sha, args.expected_creator
            )
    except (OSError, json.JSONDecodeError, AttestationError) as error:
        parser.exit(1, f"TARS tree attestation failed: {error}\n")


if __name__ == "__main__":
    main()
