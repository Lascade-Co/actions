#!/usr/bin/env python3
"""Expose validated TARS release-lock values to a central GitHub Actions job."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path


IMAGE_OUTPUTS = {
    "go": "go_image",
    "api_runtime": "api_runtime_image",
    "oras": "oras_image",
    "uv": "uv_image",
    "python": "python_image",
    "bundle": "bundle_image",
    "garage_upstream": "garage_upstream_image",
    "otel_upstream": "otel_upstream_image",
    "support_alpine": "support_alpine_image",
    "nginx": "nginx_image",
    "postgres": "postgres_image",
}

DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
CANONICAL_IMAGE = re.compile(
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*@sha256:[0-9a-f]{64}\Z"
)
REGISTRY = re.compile(
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*\Z"
)
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?\Z")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
ACTION_VERSION = re.compile(r"v[0-9]+(?:\.[0-9]+){0,2}(?:[-+][0-9A-Za-z.-]+)?\Z")
WORKFLOW_ACTION = re.compile(r"^\s*-?\s*uses:\s*([^@\s]+)@([^\s#]+)", re.MULTILINE)
RELEASE_DIGEST_KEYS = ("api", "dispatcher", "gpu", "garage", "otel")
RELEASE_IMAGE_KEYS = {
    "api": "TARS_API_IMAGE",
    "dispatcher": "TARS_DISPATCHER_IMAGE",
    "gpu": "TARS_GPU_IMAGE",
    "garage": "TARS_GARAGE_IMAGE",
    "otel": "TARS_OTEL_COLLECTOR_IMAGE",
}


def validate_action_versions(lock_path: Path, workflow_paths: list[Path]) -> None:
    """Require central workflow Action tags to match the source-owned lock."""

    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    expected: dict[str, str] = {}
    for action in lock["actions"].values():
        repository = action["repository"]
        version = action["version"]
        if (
            not isinstance(repository, str)
            or REPOSITORY.fullmatch(repository) is None
            or not isinstance(version, str)
            or ACTION_VERSION.fullmatch(version) is None
        ):
            raise ValueError("release lock contains an invalid action repository or version")
        expected[repository] = version
    for workflow_path in workflow_paths:
        workflow = workflow_path.read_text(encoding="utf-8")
        for repository, revision in WORKFLOW_ACTION.findall(workflow):
            if repository.startswith("./"):
                continue
            locked_version = expected.get(repository)
            if locked_version is None:
                raise ValueError(
                    f"{workflow_path}: action {repository} is absent from the release lock"
                )
            if revision != locked_version:
                raise ValueError(
                    f"{workflow_path}: {repository}@{revision} does not match "
                    f"locked version {locked_version}"
                )


def values(lock_path: Path) -> dict[str, str]:
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    if lock["target_platform"] != "linux/amd64":
        raise ValueError("target platform must be linux/amd64")
    registry = lock["registry"]
    if not isinstance(registry, str) or REGISTRY.fullmatch(registry) is None:
        raise ValueError("registry must be a canonical repository name")
    result: dict[str, str] = {}
    for key, output in IMAGE_OUTPUTS.items():
        reference = lock["images"][key]["reference"]
        if not isinstance(reference, str) or CANONICAL_IMAGE.fullmatch(reference) is None:
            raise ValueError(f"images.{key}.reference must be a canonical digest")
        result[output] = reference
    tada = lock["tada"]
    if tada["repository"] != "Lascade-Co/tada":
        raise ValueError("TADA repository must be Lascade-Co/tada")
    tada_oci = tada["oci"]
    tada_revision = tada["revision"]
    if not isinstance(tada_oci, str) or CANONICAL_IMAGE.fullmatch(tada_oci) is None:
        raise ValueError("TADA OCI reference must be a canonical digest")
    if not isinstance(tada_revision, str) or SHA.fullmatch(tada_revision) is None:
        raise ValueError("TADA revision must be a full lowercase Git SHA")
    tools = lock["tools"]
    for name in ("go", "doctl", "opentofu", "docker_buildx"):
        version = tools[name]
        if not isinstance(version, str) or VERSION.fullmatch(version) is None:
            raise ValueError(f"tools.{name} must be an exact semantic version")
    result.update(
        {
            "registry": registry,
            "target_platform": lock["target_platform"],
            "tada_repository": tada["repository"],
            "tada_oci": tada_oci,
            "tada_revision": tada_revision,
            "go_version": tools["go"],
            "doctl_version": tools["doctl"],
            "opentofu_version": tools["opentofu"],
            "docker_buildx_version": tools["docker_buildx"],
        }
    )
    return result


def release_values(
    lock_path: Path, digests: dict[str, str], endpoint_id: str
) -> dict[str, str]:
    """Build the data-only environment consumed by ``deploy/tars-deploy``."""

    if set(digests) != set(RELEASE_DIGEST_KEYS):
        raise ValueError(
            "release digests must contain exactly api, dispatcher, gpu, garage, and otel"
        )
    if (
        not isinstance(endpoint_id, str)
        or re.fullmatch(r"[A-Za-z0-9_-]{1,191}", endpoint_id) is None
    ):
        raise ValueError("Runpod endpoint ID is invalid")
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    registry = lock["registry"]
    if not isinstance(registry, str) or REGISTRY.fullmatch(registry) is None:
        raise ValueError("registry must be a canonical repository name")
    result: dict[str, str] = {}
    for component in RELEASE_DIGEST_KEYS:
        digest = digests[component]
        if not isinstance(digest, str) or DIGEST.fullmatch(digest) is None:
            raise ValueError(f"{component} image digest must be sha256 plus 64 lowercase hex")
        result[RELEASE_IMAGE_KEYS[component]] = f"{registry}@{digest}"
    nginx = lock["images"]["nginx"]["reference"]
    postgres = lock["images"]["postgres"]["reference"]
    if any(
        not isinstance(reference, str) or CANONICAL_IMAGE.fullmatch(reference) is None
        for reference in (nginx, postgres)
    ):
        raise ValueError("runtime images must be canonical digest references")
    result.update(
        {
            "NGINX_IMAGE": nginx,
            "POSTGRES_IMAGE": postgres,
            "TADA_ALLOWED_HOSTS": (
                "dashboard.lascade.com,api.maptiler.com,server.arcgisonline.com,"
                "firebasestorage.googleapis.com"
            ),
            "TADA_ESTIMATE_HD_REALTIME_FACTOR": "5.5",
            "TADA_ESTIMATE_4K_REALTIME_FACTOR": "4.0",
            "TADA_ESTIMATE_MODEL_VERSION": "runpod-ampere16-v1",
            "DISPATCHER_STOP_GRACE_PERIOD": "2h15m",
            "TARS_RUNPOD_ENDPOINT_ID": endpoint_id,
        }
    )
    return result


def write_release_environment(path: Path, environment: dict[str, str]) -> None:
    if any(
        not isinstance(key, str)
        or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key)
        or not isinstance(value, str)
        or "\n" in value
        or "\r" in value
        for key, value in environment.items()
    ):
        raise ValueError("release environment contains an unsafe key or value")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    rendered = "".join(f"{key}={value}\n" for key, value in environment.items())
    path.write_text(rendered, encoding="utf-8")
    os.chmod(path, 0o600)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("lock", type=Path)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--release-env", type=Path)
    parser.add_argument("--runpod-endpoint-id")
    parser.add_argument("--workflow", action="append", type=Path, default=[])
    for component in RELEASE_DIGEST_KEYS:
        parser.add_argument(f"--{component}-digest")
    args = parser.parse_args()

    digest_values = {
        component: getattr(args, f"{component}_digest")
        for component in RELEASE_DIGEST_KEYS
    }
    if args.release_env:
        if (
            any(value is None for value in digest_values.values())
            or args.runpod_endpoint_id is None
        ):
            parser.error(
                "--release-env requires all five --*-digest arguments and "
                "--runpod-endpoint-id"
            )
        write_release_environment(
            args.release_env,
            release_values(args.lock, digest_values, args.runpod_endpoint_id),
        )
        return
    if (
        any(value is not None for value in digest_values.values())
        or args.runpod_endpoint_id is not None
    ):
        parser.error("--*-digest and --runpod-endpoint-id arguments require --release-env")

    validate_action_versions(args.lock, args.workflow)

    rendered = "".join(f"{key}={value}\n" for key, value in values(args.lock).items())
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
