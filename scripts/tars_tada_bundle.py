#!/usr/bin/env python3
"""Fetch the locked TADA bundle before untrusted Dockerfile evaluation."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path


ORAS_IMAGE = re.compile(r"ghcr\.io/oras-project/oras@sha256:[0-9a-f]{64}\Z")
TADA_OCI = re.compile(r"ghcr\.io/lascade-co/tada-wheel@sha256:[0-9a-f]{64}\Z")
USERNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\[\]-]{0,79}\Z")


class BundleFetchError(RuntimeError):
    """The trusted TADA artifact fetch or bundle-shape check failed."""


def validate_inputs(oras_image: str, tada_oci: str, username: str) -> None:
    if ORAS_IMAGE.fullmatch(oras_image) is None:
        raise BundleFetchError("ORAS image must be its locked official digest")
    if TADA_OCI.fullmatch(tada_oci) is None:
        raise BundleFetchError("TADA artifact must be its locked package digest")
    if USERNAME.fullmatch(username) is None:
        raise BundleFetchError("GHCR username has an invalid shape")


def validate_bundle_shape(directory: Path) -> None:
    entries = list(directory.iterdir())
    files = {path.name for path in entries}
    wheels = {name for name in files if name.startswith("tada-") and name.endswith(".whl")}
    expected = {"SHA256SUMS", "pylock.toml", "build-metadata.json"} | wheels
    if len(wheels) != 1 or files != expected:
        raise BundleFetchError("TADA artifact does not contain the exact four-file bundle")
    if any(
        not stat.S_ISREG(path.lstat().st_mode) or path.stat().st_size < 1
        for path in entries
    ):
        raise BundleFetchError("TADA artifact contains an empty or non-regular entry")


def run(command: list[str], *, stdin: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            command,
            input=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=180,
        )
    except subprocess.TimeoutExpired as error:
        raise BundleFetchError("ORAS operation exceeded its three-minute timeout") from error


def fetch(
    oras_image: str,
    tada_oci: str,
    username: str,
    token_file: Path,
    output_directory: Path,
) -> None:
    validate_inputs(oras_image, tada_oci, username)
    token = token_file.read_bytes()
    if not token or any(character in token for character in b"\r\n"):
        raise BundleFetchError("GHCR token file is empty or malformed")
    if output_directory.exists() and any(output_directory.iterdir()):
        raise BundleFetchError("TADA bundle output directory must be empty")
    output_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    auth = Path(tempfile.mkdtemp(prefix="tars-oras-auth-"))
    auth.chmod(0o700)
    docker_prefix = [
        "docker",
        "run",
        "--rm",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--volume",
        f"{auth}:/auth:rw",
    ]
    oras_command = docker_prefix + [
        "--entrypoint",
        "/bin/oras",
        oras_image,
    ]
    try:
        login = run(
            oras_command
            + [
                "login",
                "ghcr.io",
                "--username",
                username,
                "--password-stdin",
                "--registry-config",
                "/auth/config.json",
            ],
            stdin=token,
        )
        if login.returncode != 0:
            raise BundleFetchError("ORAS could not authenticate to the internal package")
        pull = run(
            docker_prefix
            + [
                "--volume",
                f"{output_directory}:/bundle:rw",
                "--entrypoint",
                "/bin/oras",
                oras_image,
                "pull",
                tada_oci,
                "--output",
                "/bundle",
                "--registry-config",
                "/auth/config.json",
                "--no-tty",
            ]
        )
        if pull.returncode != 0:
            raise BundleFetchError("ORAS could not pull the locked TADA artifact")
        validate_bundle_shape(output_directory)
    finally:
        run(
            oras_command
            + ["logout", "ghcr.io", "--registry-config", "/auth/config.json"]
        )
        shutil.rmtree(auth)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oras-image", required=True)
    parser.add_argument("--tada-oci", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()
    try:
        fetch(
            args.oras_image,
            args.tada_oci,
            args.username,
            args.token_file,
            args.output_directory,
        )
    except (OSError, BundleFetchError) as error:
        parser.exit(1, f"trusted TADA bundle fetch failed: {error}\n")


if __name__ == "__main__":
    main()
