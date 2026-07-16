#!/usr/bin/env python3
"""Narrow OIDC and exact-key Infisical client for TARS delivery jobs.

The client intentionally has no list or export operation. Secret values and
access tokens move between private files, HTTP bodies, and process stdin; they
are never command-line arguments or ordinary log output.
"""

from __future__ import annotations

import argparse
import base64
import ipaddress
import json
import os
import re
import secrets
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping


DOMAIN = "https://secrets.lascade.com"
AUDIENCE = "https://github.com/Lascade-Co"
IDENTITY_ID = "2d56cdc0-25e8-4afb-9ccc-c8885e98b5ae"
PROJECT_ID = "a05e73f7-f45a-43af-8ddd-3d5b4f8bd8e5"
ENVIRONMENT = "prod"
LOGIN_ENDPOINT = f"{DOMAIN}/api/v1/auth/oidc-auth/login"
SECRET_ENDPOINT = f"{DOMAIN}/api/v4/secrets"
SSH_USER = re.compile(r"[a-z_][a-z0-9_-]{0,31}\Z")


class InfisicalError(RuntimeError):
    """An authentication, API, or secret-shape error safe to print."""


Open = Callable[..., Any]


def _json_request(request: urllib.request.Request, *, opener: Open) -> dict[str, Any]:
    try:
        with opener(request, timeout=30) as response:
            raw = response.read()
    except urllib.error.HTTPError as error:
        raise InfisicalError(f"Infisical request failed with HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise InfisicalError("Infisical request failed at the network boundary") from error
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InfisicalError("Infisical returned malformed JSON") from error
    if not isinstance(value, dict):
        raise InfisicalError("Infisical returned a non-object JSON response")
    return value


def request_github_oidc_token(
    environment: Mapping[str, str], *, opener: Open = urllib.request.urlopen
) -> str:
    request_url = environment.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
    request_token = environment.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
    if not request_url.startswith("https://") or not request_token:
        raise InfisicalError("GitHub OIDC request environment is unavailable")
    separator = "&" if "?" in request_url else "?"
    url = request_url + separator + urllib.parse.urlencode({"audience": AUDIENCE})
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {request_token}", "Accept": "application/json"},
        method="GET",
    )
    response = _json_request(request, opener=opener)
    token = response.get("value")
    if not isinstance(token, str) or not token:
        raise InfisicalError("GitHub OIDC response did not contain a token")
    return token


def login_with_oidc(
    environment: Mapping[str, str], *, opener: Open = urllib.request.urlopen
) -> str:
    oidc_token = request_github_oidc_token(environment, opener=opener)
    body = json.dumps(
        {"identityId": IDENTITY_ID, "jwt": oidc_token}, separators=(",", ":")
    ).encode("utf-8")
    request = urllib.request.Request(
        LOGIN_ENDPOINT,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    response = _json_request(request, opener=opener)
    token = response.get("accessToken")
    if not isinstance(token, str) or not token:
        raise InfisicalError("Infisical OIDC login returned no access token")
    return token


def get_secret(
    access_token: str,
    secret_path: str,
    secret_name: str,
    *,
    opener: Open = urllib.request.urlopen,
) -> str:
    if not access_token or any(character in access_token for character in "\r\n"):
        raise InfisicalError("Infisical access token is invalid")
    if not secret_path.startswith("/") or ".." in secret_path.split("/"):
        raise InfisicalError("secret path must be an absolute Infisical path")
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", secret_name) is None:
        raise InfisicalError("secret name must be an uppercase contract key")
    query = urllib.parse.urlencode(
        {
            "projectId": PROJECT_ID,
            "environment": ENVIRONMENT,
            "secretPath": secret_path,
            "type": "shared",
            "viewSecretValue": "true",
            "expandSecretReferences": "false",
        }
    )
    url = f"{SECRET_ENDPOINT}/{urllib.parse.quote(secret_name, safe='')}?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    response = _json_request(request, opener=opener)
    secret = response.get("secret")
    if not isinstance(secret, dict) or secret.get("secretKey") != secret_name:
        raise InfisicalError(f"Infisical returned the wrong object for {secret_path}/{secret_name}")
    value = secret.get("secretValue")
    if not isinstance(value, str) or not value:
        raise InfisicalError(f"Infisical secret is empty: {secret_path}/{secret_name}")
    return value


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


def read_private(path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InfisicalError(f"cannot read private file {path.name}") from error
    if not value:
        raise InfisicalError(f"private file is empty: {path.name}")
    return value


def write_docker_config(token_file: Path, output_directory: Path) -> Path:
    token = read_private(token_file)
    if any(character.isspace() for character in token):
        raise InfisicalError("DOCR token must not contain whitespace")
    auth = base64.b64encode(f"{token}:{token}".encode("utf-8")).decode("ascii")
    config = json.dumps(
        {"auths": {"registry.digitalocean.com": {"auth": auth}}},
        separators=(",", ":"),
    )
    path = output_directory / "config.json"
    write_private(path, config + "\n")
    return path


def append_deploy_outputs(secrets_directory: Path, github_output: Path) -> None:
    host = read_private(secrets_directory / "DEPLOY_SSH_HOST")
    user = read_private(secrets_directory / "DEPLOY_SSH_USER")
    encoded_wireguard = "".join(
        read_private(secrets_directory / "WIREGUARD_CONFIG").split()
    )
    try:
        ipaddress.IPv4Address(host)
    except ipaddress.AddressValueError as error:
        raise InfisicalError("DEPLOY_SSH_HOST must be a literal IPv4 address") from error
    if SSH_USER.fullmatch(user) is None:
        raise InfisicalError("DEPLOY_SSH_USER is not a safe Unix account name")
    try:
        decoded = base64.b64decode(encoded_wireguard, validate=True)
    except (ValueError, TypeError) as error:
        raise InfisicalError("WIREGUARD_CONFIG must be strict base64") from error
    if b"[Interface]" not in decoded:
        raise InfisicalError("WIREGUARD_CONFIG does not contain a wg-quick interface")

    if os.environ.get("GITHUB_ACTIONS") == "true":
        for value in (host, user, encoded_wireguard):
            escaped = value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            print(f"::add-mask::{escaped}")

    with github_output.open("a", encoding="utf-8") as output:
        for name, value in (
            ("host", host),
            ("user", user),
            ("wireguard_config", encoded_wireguard),
        ):
            delimiter = f"tars_{secrets.token_hex(16)}"
            output.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    login = subcommands.add_parser("login")
    login.add_argument("--token-file", type=Path, required=True)

    get = subcommands.add_parser("get")
    get.add_argument("--token-file", type=Path, required=True)
    get.add_argument("--secret-path", required=True)
    get.add_argument("--secret-name", required=True)
    get.add_argument("--output-file", type=Path, required=True)

    docker_config = subcommands.add_parser("docker-config")
    docker_config.add_argument("--token-file", type=Path, required=True)
    docker_config.add_argument("--output-directory", type=Path, required=True)

    outputs = subcommands.add_parser("deploy-outputs")
    outputs.add_argument("--secrets-directory", type=Path, required=True)
    outputs.add_argument("--github-output", type=Path, required=True)

    args = parser.parse_args()
    try:
        if args.command == "login":
            write_private(args.token_file, login_with_oidc(os.environ))
        elif args.command == "get":
            token = read_private(args.token_file)
            write_private(
                args.output_file,
                get_secret(token, args.secret_path, args.secret_name),
            )
        elif args.command == "docker-config":
            write_docker_config(args.token_file, args.output_directory)
        else:
            append_deploy_outputs(args.secrets_directory, args.github_output)
    except InfisicalError as error:
        parser.exit(1, f"tars Infisical operation failed: {error}\n")


if __name__ == "__main__":
    main()
