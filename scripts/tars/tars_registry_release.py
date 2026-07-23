#!/usr/bin/env python3
"""Resolve TARS exact-SHA image tags without ever overwriting an existing tag."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Callable


COMPONENTS = ("api", "dispatcher", "gpu", "garage", "otel")
MAX_TRANSIENT_CALLS = 3
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
REGISTRY = re.compile(
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*\Z"
)
IMAGE_TAG = re.compile(
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*:"
    r"(?:api|dispatcher|gpu|garage|otel)-sha-[0-9a-f]{40}\Z"
)
MEDIA_TYPES = {
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
}
Inspector = Callable[[str, Path], str | None]


class RegistryReleaseError(RuntimeError):
    """A safe, operator-actionable immutable registry release error."""


def inspect_digest(
    reference: str,
    docker_config: Path,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str | None:
    """Return a tag's manifest digest, or ``None`` only for a missing tag."""

    if IMAGE_TAG.fullmatch(reference) is None:
        raise RegistryReleaseError("image reference is not an exact-SHA TARS tag")
    command = [
        "docker",
        "buildx",
        "imagetools",
        "inspect",
        "--format",
        "{{json .Manifest}}",
        reference,
    ]
    environment = os.environ.copy()
    environment["DOCKER_CONFIG"] = str(docker_config)
    for attempt in range(1, MAX_TRANSIENT_CALLS + 1):
        try:
            completed = runner(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired):
            if attempt == MAX_TRANSIENT_CALLS:
                raise RegistryReleaseError(
                    f"registry inspection failed for {reference} after "
                    f"{MAX_TRANSIENT_CALLS} attempts"
                ) from None
            continue
        if completed.returncode == 0:
            return _manifest_digest(completed.stdout)
        failure = (completed.stdout + "\n" + completed.stderr).casefold()
        if _is_missing_manifest(reference, failure):
            return None
        if attempt == MAX_TRANSIENT_CALLS:
            raise RegistryReleaseError(
                f"registry inspection failed for {reference} after "
                f"{MAX_TRANSIENT_CALLS} attempts"
            )
    raise AssertionError("bounded registry inspection loop did not return")


def _is_missing_manifest(reference: str, failure: str) -> bool:
    """Recognize registry manifest absence without masking auth/tool failures."""

    lowered_reference = reference.casefold()
    tag = lowered_reference.rsplit(":", 1)[1]
    return (
        "manifest unknown" in failure
        or f"manifest for {lowered_reference} not found" in failure
        or f"{lowered_reference}: not found" in failure
        or (
            f"/manifests/{tag}" in failure
            and ("404 not found" in failure or "status code 404" in failure)
        )
    )


def _manifest_digest(raw: str) -> str:
    if not raw or len(raw.encode("utf-8")) > 1_048_576:
        raise RegistryReleaseError("registry returned an invalid image manifest")
    try:
        manifest = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as error:
        raise RegistryReleaseError("registry returned an invalid image manifest") from error
    digest = manifest.get("digest") if isinstance(manifest, dict) else None
    size = manifest.get("size") if isinstance(manifest, dict) else None
    if (
        not isinstance(manifest, dict)
        or manifest.get("schemaVersion") != 2
        or manifest.get("mediaType") not in MEDIA_TYPES
        or not isinstance(digest, str)
        or DIGEST.fullmatch(digest) is None
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size <= 0
    ):
        raise RegistryReleaseError("registry returned an invalid image manifest")
    return digest


def resolve_release(
    *,
    registry: str,
    release_sha: str,
    docker_config: Path,
    allow_missing: bool,
    expected_digests: dict[str, str] | None = None,
    inspector: Inspector = inspect_digest,
) -> dict[str, str]:
    """Resolve the immutable tag set before or after the component builds."""

    if REGISTRY.fullmatch(registry) is None:
        raise RegistryReleaseError("registry must be a canonical repository name")
    if SHA.fullmatch(release_sha) is None:
        raise RegistryReleaseError(
            "release SHA must be 40 lowercase hexadecimal characters"
        )
    expected = expected_digests or {}
    if not set(expected).issubset(COMPONENTS):
        raise RegistryReleaseError("unexpected component in expected image digests")
    for component, digest in expected.items():
        if DIGEST.fullmatch(digest) is None:
            raise RegistryReleaseError(
                f"expected {component} image digest is not canonical"
            )

    result: dict[str, str] = {}
    for component in COMPONENTS:
        reference = f"{registry}:{component}-sha-{release_sha}"
        digest = inspector(reference, docker_config)
        result[f"{component}_exists"] = "true" if digest is not None else "false"
        result[f"{component}_digest"] = digest or ""
        if digest is None:
            if not allow_missing:
                raise RegistryReleaseError(
                    f"exact-SHA {component} image tag is missing after build"
                )
            continue
        expected_digest = expected.get(component)
        if expected_digest is not None and digest != expected_digest:
            raise RegistryReleaseError(
                f"published {component} image digest does not match this build"
            )
    return result


def write_outputs(path: Path, outputs: dict[str, str]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for name in sorted(outputs):
            value = outputs[name]
            if "\n" in value or "\r" in value:
                raise RegistryReleaseError("registry output contains a line break")
            handle.write(f"{name}={value}\n")


def _common(command: argparse.ArgumentParser) -> None:
    command.add_argument("--registry", required=True)
    command.add_argument("--release-sha", required=True)
    command.add_argument("--docker-config", type=Path, required=True)
    command.add_argument("--github-output", type=Path, required=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    probe = commands.add_parser("probe")
    _common(probe)
    verify = commands.add_parser("verify")
    _common(verify)
    for component in COMPONENTS:
        verify.add_argument(f"--expected-{component}-digest", default="")
    args = parser.parse_args()
    try:
        expected = None
        if args.command == "verify":
            expected = {
                component: getattr(args, f"expected_{component}_digest")
                for component in COMPONENTS
                if getattr(args, f"expected_{component}_digest")
            }
        outputs = resolve_release(
            registry=args.registry,
            release_sha=args.release_sha,
            docker_config=args.docker_config,
            allow_missing=args.command == "probe",
            expected_digests=expected,
        )
        write_outputs(args.github_output, outputs)
    except RegistryReleaseError as error:
        parser.exit(1, f"TARS registry release failed: {error}\n")


if __name__ == "__main__":
    main()
