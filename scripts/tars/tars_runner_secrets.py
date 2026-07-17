#!/usr/bin/env python3
"""Capture Infisical Action exports for TARS without another API client.

The official action authenticates and masks values. This helper validates the
small runner-to-Droplet interface, writes owner-only ephemeral files, and
clears exported variables for subsequent workflow steps. Secret values are
never printed; the generated remote environment is sent only over SSH stdin.
"""

from __future__ import annotations

import argparse
import base64
import ipaddress
import json
import os
import re
import secrets
import shlex
from pathlib import Path
from typing import Mapping


DEPLOYMENT_KEYS = (
    "DOCR_READ_USERNAME",
    "DOCR_READ_PASSWORD",
    "DOCR_WRITE_TOKEN",
    "DEPLOY_SSH_HOST",
    "DEPLOY_SSH_USER",
    "DEPLOY_SSH_PRIVATE_KEY",
    "DEPLOY_SSH_KNOWN_HOSTS",
    "WIREGUARD_CONFIG",
)
BUILD_KEYS = ("DOCR_WRITE_TOKEN",)
DEPLOY_KEYS = (
    "DOCR_READ_USERNAME",
    "DOCR_READ_PASSWORD",
    "DEPLOY_SSH_HOST",
    "DEPLOY_SSH_USER",
    "DEPLOY_SSH_PRIVATE_KEY",
    "DEPLOY_SSH_KNOWN_HOSTS",
    "WIREGUARD_CONFIG",
)
RUNTIME_KEYS = (
    "POSTGRES_PASSWORD",
    "TARS_JWT_HS256_SECRET",
    "GARAGE_RPC_SECRET",
    "GARAGE_ADMIN_TOKEN",
    "GARAGE_METRICS_TOKEN",
    "GARAGE_ACCESS_KEY_ID",
    "GARAGE_SECRET_ACCESS_KEY",
    "ONEUPTIME_TOKEN",
)
REMOTE_KEYS = ("DOCR_READ_USERNAME", "DOCR_READ_PASSWORD", *RUNTIME_KEYS)
SSH_USER = re.compile(r"[a-z_][a-z0-9_-]{0,31}\Z")


class RunnerSecretError(RuntimeError):
    """A safe, operator-actionable runner secret contract error."""


def required(environment: Mapping[str, str], names: tuple[str, ...]) -> dict[str, str]:
    values: dict[str, str] = {}
    missing: list[str] = []
    for name in names:
        value = environment.get(name, "")
        if not value:
            missing.append(name)
        elif "\x00" in value:
            raise RunnerSecretError(f"{name} contains a NUL byte")
        else:
            values[name] = value
    if missing:
        raise RunnerSecretError("missing Infisical Action exports: " + ", ".join(missing))
    return values


def write_private(path: Path, value: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def clear_exports(github_env: Path, names: tuple[str, ...]) -> None:
    with github_env.open("a", encoding="utf-8") as output:
        for name in names:
            output.write(f"{name}=\n")


def write_docker_config(token: str, output_directory: Path) -> Path:
    if any(character.isspace() for character in token):
        raise RunnerSecretError("DOCR_WRITE_TOKEN must not contain whitespace")
    auth = base64.b64encode(f"{token}:{token}".encode("utf-8")).decode("ascii")
    rendered = json.dumps(
        {"auths": {"registry.digitalocean.com": {"auth": auth}}},
        separators=(",", ":"),
    )
    path = output_directory / "config.json"
    write_private(path, rendered + "\n")
    return path


def append_output(output: Path, name: str, value: str) -> None:
    delimiter = f"tars_{secrets.token_hex(16)}"
    with output.open("a", encoding="utf-8") as handle:
        handle.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")


def mask_for_actions(value: str) -> None:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        escaped = value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::add-mask::{escaped}")


def validate_connection(values: Mapping[str, str]) -> str:
    host = values["DEPLOY_SSH_HOST"]
    user = values["DEPLOY_SSH_USER"]
    encoded_wireguard = "".join(values["WIREGUARD_CONFIG"].split())
    try:
        ipaddress.IPv4Address(host)
    except ipaddress.AddressValueError as error:
        raise RunnerSecretError("DEPLOY_SSH_HOST must be a literal IPv4 address") from error
    if SSH_USER.fullmatch(user) is None:
        raise RunnerSecretError("DEPLOY_SSH_USER is not a safe Unix account name")
    try:
        decoded = base64.b64decode(encoded_wireguard, validate=True)
    except (ValueError, TypeError) as error:
        raise RunnerSecretError("WIREGUARD_CONFIG must be strict base64") from error
    if b"[Interface]" not in decoded:
        raise RunnerSecretError("WIREGUARD_CONFIG does not contain a wg-quick interface")
    if "PRIVATE KEY" not in values["DEPLOY_SSH_PRIVATE_KEY"]:
        raise RunnerSecretError("DEPLOY_SSH_PRIVATE_KEY is not a private key")
    if not values["DEPLOY_SSH_KNOWN_HOSTS"].strip():
        raise RunnerSecretError("DEPLOY_SSH_KNOWN_HOSTS is empty")
    return encoded_wireguard


def capture_build(
    environment: Mapping[str, str], output_directory: Path, github_env: Path
) -> None:
    try:
        values = required(environment, BUILD_KEYS)
        write_docker_config(values["DOCR_WRITE_TOKEN"], output_directory)
    finally:
        clear_exports(github_env, DEPLOYMENT_KEYS)


def capture_deploy(
    environment: Mapping[str, str],
    output_directory: Path,
    github_output: Path,
    github_env: Path,
) -> None:
    try:
        values = required(environment, (*DEPLOY_KEYS, *RUNTIME_KEYS))
        encoded_wireguard = validate_connection(values)
        mask_for_actions(encoded_wireguard)
        write_private(
            output_directory / "DEPLOY_SSH_PRIVATE_KEY",
            values["DEPLOY_SSH_PRIVATE_KEY"],
        )
        write_private(
            output_directory / "DEPLOY_SSH_KNOWN_HOSTS",
            values["DEPLOY_SSH_KNOWN_HOSTS"],
        )
        exports = ["export TARS_SECRET_SOURCE=environment"]
        exports.extend(f"export {name}={shlex.quote(values[name])}" for name in REMOTE_KEYS)
        write_private(output_directory / "remote-secrets.sh", "\n".join(exports) + "\n")
        append_output(github_output, "host", values["DEPLOY_SSH_HOST"])
        append_output(github_output, "user", values["DEPLOY_SSH_USER"])
        append_output(github_output, "wireguard_config", encoded_wireguard)
    finally:
        clear_exports(github_env, (*DEPLOYMENT_KEYS, *RUNTIME_KEYS))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("capture-build")
    build.add_argument("--output-directory", type=Path, required=True)
    build.add_argument("--github-env", type=Path, required=True)
    deploy = commands.add_parser("capture-deploy")
    deploy.add_argument("--output-directory", type=Path, required=True)
    deploy.add_argument("--github-output", type=Path, required=True)
    deploy.add_argument("--github-env", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "capture-build":
            capture_build(os.environ, args.output_directory, args.github_env)
        else:
            capture_deploy(
                os.environ,
                args.output_directory,
                args.github_output,
                args.github_env,
            )
    except RunnerSecretError as error:
        parser.exit(1, f"tars runner secret capture failed: {error}\n")


if __name__ == "__main__":
    main()
