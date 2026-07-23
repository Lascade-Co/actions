#!/usr/bin/env python3
"""Provision and retire immutable, TARS-owned Runpod Serverless releases.

The workflow creates one registry credential, template, and endpoint per TARS
release.  Resource names are deterministic and ownership-scoped.  Re-running a
release verifies the existing objects byte-for-byte at the public API boundary
instead of silently mutating drifted infrastructure.

Secret values are accepted only through owner-only files.  Runpod's GraphQL
API uses its documented ``api_key`` query parameter; registry credentials stay
in request bodies.  Redirects are rejected, and secret-bearing request URLs and
transport errors are never included in output files, exception text, or command
output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol


GRAPHQL_URL = "https://api.runpod.io/graphql"
REST_BASE_URL = "https://rest.runpod.io/v1"
SERVERLESS_BASE_URL = "https://api.runpod.ai/v2"
MAX_TRANSIENT_CALLS = 3
REQUEST_TIMEOUT_SECONDS = 30
ENDPOINT_TIMEOUT_MS = 7_200_000
CONTAINER_DISK_GB = 20
# Runpod accepts comma-separated pools in priority order. Keep the cheapest
# 16 GB pool primary and use the slightly dearer 24 GB pool only when primary
# supply is constrained.
GPU_POOL_SELECTOR = "AMPERE_16,AMPERE_24"
LEGACY_GPU_POOL_SELECTOR = "AMPERE_16"
WORKERS_MIN = 0
WORKERS_MAX = 2
SCALER_TYPE = "REQUEST_COUNT"
SCALER_VALUE = 1
IDLE_TIMEOUT_SECONDS = 5
RUNPOD_INIT_TIMEOUT_SECONDS = 1_200
TEMPLATE_ENV_KEYS = [{"key": "RUNPOD_INIT_TIMEOUT"}]
TEMPLATE_ENV = {"RUNPOD_INIT_TIMEOUT": str(RUNPOD_INIT_TIMEOUT_SECONDS)}
PRUNE_GRACE = timedelta(hours=24)

SHA = re.compile(r"[0-9a-f]{40}\Z")
IMMUTABLE_GPU_IMAGE = re.compile(
    r"registry\.digitalocean\.com/lascade/tars:gpu-sha-([0-9a-f]{40})"
    r"@sha256:([0-9a-f]{64})\Z"
)
RESOURCE_ID = re.compile(r"[A-Za-z0-9_-]{1,191}\Z")
ENDPOINT_NAME = re.compile(r"tars-runpod-endpoint-v1-([0-9a-f]{40})\Z")
TEMPLATE_NAME = re.compile(r"tars-runpod-template-v1-([0-9a-f]{40})\Z")
AUTH_NAME = re.compile(
    r"tars-runpod-auth-v1-([0-9a-f]{40})-([0-9a-f]{12})\Z"
)
TRANSIENT_HTTP = frozenset({408, 425, 429, 500, 502, 503, 504})


class RunpodReleaseError(RuntimeError):
    """A safe, operator-actionable Runpod release error."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Keep the GraphQL query credential on its single reviewed origin."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _open_without_redirects(
    request: urllib.request.Request, *, timeout: int
) -> Any:
    return _NO_REDIRECT_OPENER.open(request, timeout=timeout)


@dataclass(frozen=True)
class Inventory:
    endpoints: tuple[dict[str, Any], ...]
    templates: tuple[dict[str, Any], ...]
    auths: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ReleaseResources:
    endpoint_id: str
    template_id: str
    auth_id: str


@dataclass(frozen=True)
class EndpointHealth:
    in_queue: int
    in_progress: int

    @property
    def has_active_work(self) -> bool:
        return self.in_queue > 0 or self.in_progress > 0


class ReleaseAPI(Protocol):
    def inventory(self) -> Inventory: ...

    def read_endpoint(self, endpoint_id: str) -> dict[str, Any]: ...

    def read_template(self, template_id: str) -> dict[str, Any]: ...

    def read_endpoint_health(self, endpoint_id: str) -> EndpointHealth: ...

    def create_auth(self, name: str, username: str, password: str) -> dict[str, Any]: ...

    def create_template(
        self, name: str, image: str, auth_id: str
    ) -> dict[str, Any]: ...

    def create_endpoint(
        self, name: str, template_id: str
    ) -> dict[str, Any]: ...

    def configure_endpoint(self, endpoint: Mapping[str, Any]) -> dict[str, Any]: ...

    def activate_endpoint(self, endpoint: Mapping[str, Any]) -> dict[str, Any]: ...

    def zero_endpoint(self, endpoint: Mapping[str, Any]) -> dict[str, Any]: ...

    def delete_endpoint(self, endpoint_id: str) -> Inventory: ...

    def delete_template(self, template_name: str) -> Inventory: ...

    def delete_auth(self, auth_id: str) -> Inventory: ...


class RunpodClient:
    """Small standard-library Runpod client with bounded transient retries."""

    def __init__(
        self,
        api_key: str,
        *,
        opener: Callable[..., Any] = _open_without_redirects,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key or "\x00" in api_key:
            raise RunpodReleaseError("RUNPOD_API_KEY is empty or invalid")
        self._api_key = api_key
        self._graphql_url = GRAPHQL_URL + "?" + urllib.parse.urlencode(
            {"api_key": api_key}
        )
        self._opener = opener
        self._sleeper = sleeper

    def _request(
        self,
        method: str,
        url: str,
        *,
        operation: str,
        payload: Mapping[str, Any] | None = None,
        empty_ok: bool = False,
        max_calls: int = MAX_TRANSIENT_CALLS,
        bearer_auth: bool = False,
    ) -> Any:
        if max_calls < 1 or max_calls > MAX_TRANSIENT_CALLS:
            raise ValueError("max_calls is outside the reviewed retry bound")
        setup_failure: RunpodReleaseError | None = None
        try:
            body = None
            headers = {
                "Accept": "application/json",
                "User-Agent": "Lascade-TARS-delivery/1",
            }
            if bearer_auth:
                headers["Authorization"] = f"Bearer {self._api_key}"
            if payload is not None:
                body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(
                url, data=body, headers=headers, method=method
            )
        except Exception:
            setup_failure = RunpodReleaseError(
                f"Runpod {operation} request setup failed"
            )
        if setup_failure is not None:
            raise setup_failure

        for attempt in range(1, max_calls + 1):
            failure: RunpodReleaseError | None = None
            status: int | None = None
            raw: bytes | None = None
            try:
                with self._opener(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    status_value = getattr(response, "status", None)
                    status = int(
                        status_value if status_value is not None else response.getcode()
                    )
                    raw = response.read()
            except urllib.error.HTTPError as error:
                status = error.code
                try:
                    error.close()
                except Exception:
                    pass
                if status in TRANSIENT_HTTP and attempt < max_calls:
                    self._sleeper(float(2 ** (attempt - 1)))
                    continue
                failure = RunpodReleaseError(
                    f"Runpod {operation} failed with HTTP {status}"
                )
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt < max_calls:
                    self._sleeper(float(2 ** (attempt - 1)))
                    continue
                failure = RunpodReleaseError(
                    f"Runpod {operation} failed after {max_calls} calls"
                )
            except Exception:
                failure = RunpodReleaseError(f"Runpod {operation} failed")
            # Raise only after leaving the transport/decoder exception handler.
            # This keeps secret-bearing URLs and response bodies out of both the
            # displayed traceback and the safe exception's context chain.
            if failure is not None:
                raise failure
            if status is None or raw is None:
                raise AssertionError("Runpod transport returned no result")
            if not 200 <= status < 300:
                raise RunpodReleaseError(
                    f"Runpod {operation} failed with HTTP {status}"
                )
            if not raw and empty_ok:
                return None
            decode_failure: RunpodReleaseError | None = None
            try:
                document = json.loads(raw)
            except Exception:
                decode_failure = RunpodReleaseError(
                    f"Runpod {operation} returned malformed JSON"
                )
            if decode_failure is not None:
                raise decode_failure
            return document
        raise AssertionError("unreachable")

    def _graphql(
        self,
        operation: str,
        query: str,
        variables: Mapping[str, Any] | None = None,
        *,
        max_calls: int = MAX_TRANSIENT_CALLS,
    ) -> dict[str, Any]:
        document = self._request(
            "POST",
            self._graphql_url,
            operation=operation,
            payload={"query": query, "variables": dict(variables or {})},
            max_calls=max_calls,
        )
        if not isinstance(document, dict) or document.get("errors"):
            raise RunpodReleaseError(f"Runpod GraphQL {operation} failed")
        data = document.get("data")
        if not isinstance(data, dict):
            raise RunpodReleaseError(f"Runpod GraphQL {operation} returned no data")
        return data

    def _inventory(self, *, max_calls: int) -> Inventory:
        data = self._graphql(
            "inventory",
            """
            query TarsRunpodInventory {
              myself {
                endpoints {
                  id name gpuIds idleTimeout locations scalerType scalerValue
                  templateId workersMax workersMin createdAt
                }
                podTemplates {
                  id name imageName isServerless containerDiskInGb volumeInGb
                  dockerArgs env { key }
                  containerRegistryAuthId boundEndpointId
                }
                containerRegistryCreds { id name }
              }
            }
            """,
            max_calls=max_calls,
        )
        myself = data.get("myself")
        if not isinstance(myself, dict):
            raise RunpodReleaseError("Runpod inventory is missing the account object")
        return Inventory(
            endpoints=_objects(myself.get("endpoints"), "endpoints"),
            templates=_objects(myself.get("podTemplates"), "templates"),
            auths=_objects(myself.get("containerRegistryCreds"), "registry auths"),
        )

    def inventory(self) -> Inventory:
        return self._inventory(max_calls=MAX_TRANSIENT_CALLS)

    def _create_with_reconciliation(
        self,
        *,
        operation: str,
        name: str,
        description: str,
        create_once: Callable[[], dict[str, Any]],
        resources: Callable[[Inventory], tuple[dict[str, Any], ...]],
        verify_created: Callable[[Mapping[str, Any]], str],
        verify_recovered: Callable[[Mapping[str, Any]], str],
    ) -> dict[str, Any]:
        """Issue one mutation, then recover a lost response by exact name."""

        creation_failure: RunpodReleaseError | None = None
        try:
            created = create_once()
        except RunpodReleaseError as error:
            creation_failure = error
        else:
            verify_created(created)
            return created

        inventory_failure: RunpodReleaseError | None = None
        for _attempt in range(MAX_TRANSIENT_CALLS - 1):
            try:
                recovered = _one_named(
                    resources(self._inventory(max_calls=1)), name, description
                )
            except RunpodReleaseError as error:
                inventory_failure = error
                continue
            if recovered is not None:
                verify_recovered(recovered)
                return recovered

        if creation_failure is not None:
            raise creation_failure
        if inventory_failure is not None:
            raise RunpodReleaseError(
                f"Runpod {operation} outcome could not be confirmed"
            )
        raise RunpodReleaseError(f"Runpod did not retain the created TARS {description}")

    def create_auth(self, name: str, username: str, password: str) -> dict[str, Any]:
        return self._create_with_reconciliation(
            operation="create registry auth",
            name=name,
            description="registry auth",
            create_once=lambda: _object(
                self._graphql(
                    "create registry auth",
                    """
                    mutation TarsCreateRegistryAuth($input: SaveRegistryAuthInput) {
                      saveRegistryAuth(input: $input) { id name }
                    }
                    """,
                    {
                        "input": {
                            "name": name,
                            "username": username,
                            "password": password,
                        }
                    },
                    max_calls=1,
                ).get("saveRegistryAuth"),
                "created registry auth",
            ),
            resources=lambda inventory: inventory.auths,
            verify_created=lambda resource: verify_auth(resource, name),
            verify_recovered=lambda resource: verify_auth(resource, name),
        )

    def create_template(self, name: str, image: str, auth_id: str) -> dict[str, Any]:
        query = f"""
        mutation TarsCreateTemplate {{
          saveTemplate(input: {{
            name: {_graphql_string(name)}
            imageName: {_graphql_string(image)}
            containerRegistryAuthId: {_graphql_string(auth_id)}
            containerDiskInGb: {CONTAINER_DISK_GB}
            volumeInGb: 0
            isServerless: true
            dockerArgs: ""
            env: [
              {{
                key: "RUNPOD_INIT_TIMEOUT"
                value: "{RUNPOD_INIT_TIMEOUT_SECONDS}"
              }}
            ]
          }}) {{
            id name
          }}
        }}
        """
        return self._create_with_reconciliation(
            operation="create template",
            name=name,
            description="template",
            create_once=lambda: _object(
                self._graphql("create template", query, max_calls=1).get(
                    "saveTemplate"
                ),
                "created template",
            ),
            resources=lambda inventory: inventory.templates,
            verify_created=lambda resource: _verify_named_id(
                resource, name, "template ID"
            ),
            verify_recovered=lambda resource: verify_template(
                resource, expected_name=name, image=image, auth_id=auth_id
            ),
        )

    def create_endpoint(self, name: str, template_id: str) -> dict[str, Any]:
        _resource_name(name, ENDPOINT_NAME, "endpoint name")
        _resource_id(template_id, "template ID")
        creation_failure: RunpodReleaseError | None = None
        created: dict[str, Any] | None = None
        try:
            data = self._graphql(
                "create endpoint",
                _save_endpoint_query(name=name, template_id=template_id),
                max_calls=1,
            )
            created = _object(data.get("saveEndpoint"), "created endpoint")
        except RunpodReleaseError as error:
            creation_failure = error

        if created is None:
            recovered: dict[str, Any] | None = None
            for _attempt in range(MAX_TRANSIENT_CALLS - 1):
                try:
                    recovered = _one_named(
                        self._inventory(max_calls=1).endpoints,
                        name,
                        "endpoint",
                    )
                except RunpodReleaseError:
                    continue
                if recovered is not None:
                    break
            if recovered is None:
                if creation_failure is not None:
                    raise creation_failure
                raise RunpodReleaseError("Runpod did not retain the created TARS endpoint")
            verify_endpoint_base(
                recovered,
                expected_name=name,
                template_id=template_id,
            )
            created = recovered

        _expect_equal(created.get("name"), name, "created endpoint name")
        endpoint_id = _resource_id(created.get("id"), "endpoint ID")
        return self._confirmed_endpoint_patch(
            endpoint_id=endpoint_id,
            payload={"executionTimeoutMs": ENDPOINT_TIMEOUT_MS, "gpuCount": 1},
            expected_name=name,
            template_id=template_id,
            workers_max=WORKERS_MAX,
            operation="configure endpoint execution",
        )

    def _get_endpoint(self, endpoint_id: str, *, max_calls: int) -> dict[str, Any]:
        endpoint_id = _resource_id(endpoint_id, "endpoint ID")
        query = urllib.parse.urlencode({"includeTemplate": "true"})
        document = self._request(
            "GET",
            f"{REST_BASE_URL}/endpoints/{endpoint_id}?{query}",
            operation="read endpoint",
            max_calls=max_calls,
            bearer_auth=True,
        )
        return _object(document, "endpoint")

    def read_endpoint(self, endpoint_id: str) -> dict[str, Any]:
        return self._get_endpoint(endpoint_id, max_calls=MAX_TRANSIENT_CALLS)

    def read_template(self, template_id: str) -> dict[str, Any]:
        template_id = _resource_id(template_id, "template ID")
        query = urllib.parse.urlencode({"includeEndpointBoundTemplates": "true"})
        document = self._request(
            "GET",
            f"{REST_BASE_URL}/templates/{template_id}?{query}",
            operation="read template",
            max_calls=MAX_TRANSIENT_CALLS,
            bearer_auth=True,
        )
        return _object(document, "template")

    def read_endpoint_health(self, endpoint_id: str) -> EndpointHealth:
        """Read the official Serverless health counters with bounded retries."""

        endpoint_id = _resource_id(endpoint_id, "endpoint ID")
        document = self._request(
            "GET",
            f"{SERVERLESS_BASE_URL}/{endpoint_id}/health",
            operation="read endpoint health",
            max_calls=MAX_TRANSIENT_CALLS,
            bearer_auth=True,
        )
        health = _object(document, "endpoint health")
        jobs = _object(health.get("jobs"), "endpoint health jobs")
        counts: dict[str, int] = {}
        for field in ("inQueue", "inProgress"):
            value = jobs.get(field)
            if type(value) is not int or value < 0:
                raise RunpodReleaseError(
                    f"Runpod endpoint health has invalid {field}"
                )
            counts[field] = value
        return EndpointHealth(
            in_queue=counts["inQueue"],
            in_progress=counts["inProgress"],
        )

    def configure_endpoint(self, endpoint: Mapping[str, Any]) -> dict[str, Any]:
        """Reconcile the two deterministic fields absent from GraphQL inventory."""

        endpoint_id = _resource_id(endpoint.get("id"), "endpoint ID")
        name = _resource_name(endpoint.get("name"), ENDPOINT_NAME, "endpoint name")
        template_id = _resource_id(endpoint.get("templateId"), "template ID")
        verify_endpoint(
            endpoint,
            expected_name=name,
            template_id=template_id,
        )
        return self._confirmed_endpoint_patch(
            endpoint_id=endpoint_id,
            payload={"executionTimeoutMs": ENDPOINT_TIMEOUT_MS, "gpuCount": 1},
            expected_name=name,
            template_id=template_id,
            workers_max=WORKERS_MAX,
            operation="configure endpoint execution",
        )

    def _confirmed_endpoint_patch(
        self,
        *,
        endpoint_id: str,
        payload: Mapping[str, Any],
        expected_name: str,
        template_id: str,
        workers_max: int,
        operation: str,
    ) -> dict[str, Any]:
        """PATCH once and use the remaining two calls for exact verification."""

        endpoint_id = _resource_id(endpoint_id, "endpoint ID")
        patch_failure: RunpodReleaseError | None = None
        try:
            self._request(
                "PATCH",
                f"{REST_BASE_URL}/endpoints/{endpoint_id}",
                operation=operation,
                payload=payload,
                empty_ok=True,
                max_calls=1,
                bearer_auth=True,
            )
        except RunpodReleaseError as error:
            patch_failure = error

        read_failure: RunpodReleaseError | None = None
        verification_failure: RunpodReleaseError | None = None
        for _attempt in range(MAX_TRANSIENT_CALLS - 1):
            try:
                endpoint = self._get_endpoint(endpoint_id, max_calls=1)
            except RunpodReleaseError as error:
                read_failure = error
                continue
            try:
                verify_endpoint_rest(
                    endpoint,
                    endpoint_id=endpoint_id,
                    expected_name=expected_name,
                    template_id=template_id,
                    workers_max=workers_max,
                )
            except RunpodReleaseError as error:
                verification_failure = error
                continue
            return endpoint

        if verification_failure is not None:
            raise verification_failure
        if patch_failure is not None:
            raise patch_failure
        if read_failure is not None:
            raise RunpodReleaseError(
                f"Runpod {operation} outcome could not be confirmed"
            )
        raise AssertionError("endpoint patch reconciliation returned no outcome")

    def _set_endpoint_workers_max(
        self,
        endpoint: Mapping[str, Any],
        *,
        workers_max: int,
        operation: str,
    ) -> dict[str, Any]:
        endpoint_id = _resource_id(endpoint.get("id"), "endpoint ID")
        name = _resource_name(endpoint.get("name"), ENDPOINT_NAME, "endpoint name")
        template_id = _resource_id(endpoint.get("templateId"), "template ID")
        return self._confirmed_endpoint_patch(
            endpoint_id=endpoint_id,
            payload={"workersMin": WORKERS_MIN, "workersMax": workers_max},
            expected_name=name,
            template_id=template_id,
            workers_max=workers_max,
            operation=operation,
        )

    def activate_endpoint(self, endpoint: Mapping[str, Any]) -> dict[str, Any]:
        return self._set_endpoint_workers_max(
            endpoint,
            workers_max=WORKERS_MAX,
            operation="restore endpoint workers",
        )

    def zero_endpoint(self, endpoint: Mapping[str, Any]) -> dict[str, Any]:
        return self._set_endpoint_workers_max(
            endpoint,
            workers_max=0,
            operation="set endpoint workers to zero",
        )

    def _confirmed_delete(
        self,
        *,
        operation: str,
        delete_once: Callable[[], None],
        is_absent: Callable[[Inventory], bool],
        retry_delay_seconds: float,
    ) -> Inventory:
        """Bound delete retries and reconcile every ambiguous outcome by read.

        A mutating request can succeed remotely while its response is lost.  A
        mutation is therefore never retried until fresh inventory proves that
        the target still exists.  Mutations are capped at three; each is
        followed by a bounded read reconciliation.  Template retries retain
        Runpod's documented release delay after endpoint deletion.
        """

        deletion_failure: RunpodReleaseError | None = None
        retained: Inventory | None = None
        for mutation_attempt in range(1, MAX_TRANSIENT_CALLS + 1):
            deletion_failure = None
            try:
                delete_once()
            except RunpodReleaseError as error:
                deletion_failure = error

            try:
                retained = self._inventory(max_calls=MAX_TRANSIENT_CALLS - 1)
            except RunpodReleaseError:
                raise RunpodReleaseError(
                    f"Runpod {operation} deletion outcome could not be confirmed"
                )
            if is_absent(retained):
                return retained
            if mutation_attempt < MAX_TRANSIENT_CALLS:
                self._sleeper(retry_delay_seconds)

        if deletion_failure is not None:
            raise deletion_failure
        if retained is not None:
            raise RunpodReleaseError(f"Runpod retained the deleted {operation}")
        raise AssertionError("delete reconciliation returned no outcome")

    def delete_endpoint(self, endpoint_id: str) -> Inventory:
        endpoint_id = _resource_id(endpoint_id, "endpoint ID")
        return self._confirmed_delete(
            operation="TARS endpoint",
            delete_once=lambda: self._graphql(
                "delete endpoint",
                "mutation TarsDeleteEndpoint { deleteEndpoint(id: "
                f"{_graphql_string(endpoint_id)}) }}",
                max_calls=1,
            ),
            is_absent=lambda inventory: _by_id(inventory.endpoints, endpoint_id)
            is None,
            retry_delay_seconds=1.0,
        )

    def delete_template(self, template_name: str) -> Inventory:
        template_name = _resource_name(
            template_name, TEMPLATE_NAME, "template name"
        )
        query = (
            "mutation TarsDeleteTemplate { deleteTemplate(templateName: "
            f"{_graphql_string(template_name)}) }}"
        )
        return self._confirmed_delete(
            operation="TARS template",
            delete_once=lambda: self._graphql(
                "delete template", query, max_calls=1
            ),
            is_absent=lambda inventory: _one_named(
                inventory.templates, template_name, "template"
            )
            is None,
            retry_delay_seconds=60.0,
        )

    def delete_auth(self, auth_id: str) -> Inventory:
        auth_id = _resource_id(auth_id, "registry auth ID")
        return self._confirmed_delete(
            operation="TARS registry auth",
            delete_once=lambda: self._request(
                "DELETE",
                f"{REST_BASE_URL}/containerregistryauth/{auth_id}",
                operation="delete registry auth",
                empty_ok=True,
                max_calls=1,
                bearer_auth=True,
            ),
            is_absent=lambda inventory: _by_id(inventory.auths, auth_id) is None,
            retry_delay_seconds=1.0,
        )


def _objects(value: Any, description: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise RunpodReleaseError(f"Runpod inventory has invalid {description}")
    return tuple(value)


def _object(value: Any, description: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RunpodReleaseError(f"Runpod returned an invalid {description}")
    return value


def _graphql_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _resource_id(value: Any, description: str) -> str:
    if not isinstance(value, str) or RESOURCE_ID.fullmatch(value) is None:
        raise RunpodReleaseError(f"Runpod returned an invalid {description}")
    return value


def _resource_name(value: Any, pattern: re.Pattern[str], description: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise RunpodReleaseError(f"Runpod returned an invalid TARS {description}")
    return value


def _save_endpoint_query(
    *,
    name: str,
    template_id: str,
) -> str:
    _resource_name(name, ENDPOINT_NAME, "endpoint name")
    _resource_id(template_id, "template ID")
    fields = (
        f"name: {_graphql_string(name)}",
        f"templateId: {_graphql_string(template_id)}",
        f"gpuIds: {_graphql_string(GPU_POOL_SELECTOR)}",
        f"idleTimeout: {IDLE_TIMEOUT_SECONDS}",
        'locations: ""',
        f"scalerType: {_graphql_string(SCALER_TYPE)}",
        f"scalerValue: {SCALER_VALUE}",
        f"workersMin: {WORKERS_MIN}",
        f"workersMax: {WORKERS_MAX}",
    )
    rendered = "\n".join(fields)
    return f"""
    mutation TarsSaveEndpoint {{
      saveEndpoint(input: {{
        {rendered}
      }}) {{
        id name
      }}
    }}
    """


def release_names(release_sha: str, username: str, password: str) -> tuple[str, str, str]:
    if SHA.fullmatch(release_sha) is None:
        raise RunpodReleaseError("release SHA must be 40 lowercase hexadecimal characters")
    fingerprint = hashlib.sha256(
        username.encode("utf-8") + b"\0" + password.encode("utf-8")
    ).hexdigest()[:12]
    return (
        f"tars-runpod-auth-v1-{release_sha}-{fingerprint}",
        f"tars-runpod-template-v1-{release_sha}",
        f"tars-runpod-endpoint-v1-{release_sha}",
    )


def _one_named(
    resources: tuple[dict[str, Any], ...], name: str, description: str
) -> dict[str, Any] | None:
    matching = [resource for resource in resources if resource.get("name") == name]
    if len(matching) > 1:
        raise RunpodReleaseError(f"Runpod contains duplicate TARS {description} names")
    return matching[0] if matching else None


def _expect_equal(actual: Any, expected: Any, description: str) -> None:
    if actual != expected:
        raise RunpodReleaseError(f"existing Runpod {description} does not match this release")


def verify_auth(resource: Mapping[str, Any], expected_name: str) -> str:
    _expect_equal(resource.get("name"), expected_name, "registry auth name")
    return _resource_id(resource.get("id"), "registry auth ID")


def _verify_named_id(
    resource: Mapping[str, Any], expected_name: str, id_description: str
) -> str:
    _expect_equal(resource.get("name"), expected_name, "created resource name")
    return _resource_id(resource.get("id"), id_description)


def verify_template(
    resource: Mapping[str, Any], *, expected_name: str, image: str, auth_id: str
) -> str:
    expectations = {
        "name": expected_name,
        "imageName": image,
        "isServerless": True,
        "containerDiskInGb": CONTAINER_DISK_GB,
        "volumeInGb": 0,
        "containerRegistryAuthId": auth_id,
    }
    for field, expected in expectations.items():
        _expect_equal(resource.get(field), expected, f"template {field}")
    if "dockerArgs" not in resource or resource.get("dockerArgs") not in (
        None,
        "",
    ):
        raise RunpodReleaseError("existing Runpod template dockerArgs does not match this release")
    if resource.get("env") != TEMPLATE_ENV_KEYS:
        raise RunpodReleaseError("existing Runpod template env does not match this release")
    return _resource_id(resource.get("id"), "template ID")


def verify_endpoint_base(
    resource: Mapping[str, Any],
    *,
    expected_name: str,
    template_id: str,
    workers_max: int = WORKERS_MAX,
) -> str:
    expectations = {
        "name": expected_name,
        "gpuIds": GPU_POOL_SELECTOR,
        "idleTimeout": IDLE_TIMEOUT_SECONDS,
        "scalerType": SCALER_TYPE,
        "scalerValue": SCALER_VALUE,
        "templateId": template_id,
        "workersMax": workers_max,
        "workersMin": WORKERS_MIN,
    }
    for field, expected in expectations.items():
        _expect_equal(resource.get(field), expected, f"endpoint {field}")
    if resource.get("locations") not in (None, ""):
        raise RunpodReleaseError("existing Runpod endpoint restricts worker locations")
    return _resource_id(resource.get("id"), "endpoint ID")


def verify_endpoint(
    resource: Mapping[str, Any],
    *,
    expected_name: str,
    template_id: str,
    workers_max: int = WORKERS_MAX,
) -> str:
    return verify_endpoint_base(
        resource,
        expected_name=expected_name,
        template_id=template_id,
        workers_max=workers_max,
    )


def verify_endpoint_rest_base(
    resource: Mapping[str, Any],
    *,
    endpoint_id: str,
    expected_name: str,
    template_id: str,
    workers_max: int = WORKERS_MAX,
) -> str:
    """Verify REST identity and scaling fields that are never self-healed."""

    expectations = {
        "id": endpoint_id,
        "name": expected_name,
        "templateId": template_id,
        "workersMin": WORKERS_MIN,
        "workersMax": workers_max,
    }
    for field, expected in expectations.items():
        _expect_equal(resource.get(field), expected, f"REST endpoint {field}")
    # Runpod's endpoint-specific and list responses currently omit computeType
    # even though the REST schema documents it. The GraphQL side of every
    # release reconciliation separately proves the ordered primary/fallback
    # gpuIds selector before handing the endpoint off; the REST side must still
    # prove a concrete GPU selector and reject any explicit CPU identity.
    if resource.get("computeType") not in (None, "GPU"):
        raise RunpodReleaseError(
            "existing Runpod REST endpoint computeType does not match this release"
        )
    gpu_type_ids = resource.get("gpuTypeIds")
    if (
        not isinstance(gpu_type_ids, list)
        or not gpu_type_ids
        or any(
            not isinstance(gpu_type_id, str)
            or not 1 <= len(gpu_type_id) <= 191
            or gpu_type_id.strip() != gpu_type_id
            for gpu_type_id in gpu_type_ids
        )
        or len(set(gpu_type_ids)) != len(gpu_type_ids)
    ):
        raise RunpodReleaseError(
            "existing Runpod REST endpoint gpuTypeIds does not match this release"
        )
    if resource.get("cpuFlavorIds") not in (None, []) or resource.get(
        "instanceIds"
    ) not in (None, []):
        raise RunpodReleaseError(
            "existing Runpod REST endpoint CPU selectors do not match this release"
        )
    return _resource_id(resource.get("id"), "endpoint ID")


def verify_template_rest(
    resource: Mapping[str, Any],
    *,
    template_id: str,
    expected_name: str,
) -> str:
    """Verify the reviewed non-secret env value on one exact template."""

    _expect_equal(resource.get("id"), template_id, "REST template id")
    _expect_equal(resource.get("name"), expected_name, "REST template name")
    _expect_equal(resource.get("env"), TEMPLATE_ENV, "REST template env")
    return _resource_id(resource.get("id"), "template ID")


def verify_endpoint_rest(
    resource: Mapping[str, Any],
    *,
    endpoint_id: str,
    expected_name: str,
    template_id: str,
    workers_max: int = WORKERS_MAX,
) -> str:
    """Verify every deterministic field in Runpod's REST endpoint response."""

    verified_id = verify_endpoint_rest_base(
        resource,
        endpoint_id=endpoint_id,
        expected_name=expected_name,
        template_id=template_id,
        workers_max=workers_max,
    )
    for field, expected in {
        "gpuCount": 1,
        "executionTimeoutMs": ENDPOINT_TIMEOUT_MS,
    }.items():
        _expect_equal(resource.get(field), expected, f"REST endpoint {field}")
    return verified_id


def _create_or_recover(
    client: ReleaseAPI,
    *,
    resources: Callable[[Inventory], tuple[dict[str, Any], ...]],
    name: str,
    description: str,
    create: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    inventory = client.inventory()
    existing = _one_named(resources(inventory), name, description)
    if existing is not None:
        return existing
    try:
        create()
    except RunpodReleaseError:
        # A mutating request can succeed remotely while its response is lost.
        # Recover only the exact deterministic name; otherwise preserve failure.
        recovered = _one_named(resources(client.inventory()), name, description)
        if recovered is None:
            raise
        return recovered
    created = _one_named(resources(client.inventory()), name, description)
    if created is None:
        raise RunpodReleaseError(f"Runpod did not retain the created TARS {description}")
    return created


def ensure_release(
    client: ReleaseAPI,
    *,
    release_sha: str,
    gpu_image: str,
    registry_username: str,
    registry_password: str,
) -> ReleaseResources:
    image_match = IMMUTABLE_GPU_IMAGE.fullmatch(gpu_image)
    if image_match is None:
        raise RunpodReleaseError(
            "GPU image must be the immutable TARS DOCR exact-SHA tag and digest"
        )
    if image_match.group(1) != release_sha:
        raise RunpodReleaseError("GPU image tag does not match the release SHA")
    auth_name, template_name, endpoint_name = release_names(
        release_sha, registry_username, registry_password
    )
    initial = client.inventory()
    same_release_auths = []
    for candidate in initial.auths:
        match = AUTH_NAME.fullmatch(str(candidate.get("name", "")))
        if match is not None and match.group(1) == release_sha:
            same_release_auths.append(candidate)
    if len(same_release_auths) > 1:
        raise RunpodReleaseError("Runpod contains duplicate auths for this TARS release")
    if same_release_auths and same_release_auths[0].get("name") != auth_name:
        raise RunpodReleaseError(
            "existing Runpod registry auth does not match this release credential"
        )
    if (
        _one_named(initial.templates, template_name, "template") is not None
        or _one_named(initial.endpoints, endpoint_name, "endpoint") is not None
    ) and not same_release_auths:
        raise RunpodReleaseError(
            "existing Runpod release is missing its deterministic registry auth"
        )
    auth = _create_or_recover(
        client,
        resources=lambda inventory: inventory.auths,
        name=auth_name,
        description="registry auth",
        create=lambda: client.create_auth(auth_name, registry_username, registry_password),
    )
    auth_id = verify_auth(auth, auth_name)
    template = _create_or_recover(
        client,
        resources=lambda inventory: inventory.templates,
        name=template_name,
        description="template",
        create=lambda: client.create_template(template_name, gpu_image, auth_id),
    )
    template_id = verify_template(
        template, expected_name=template_name, image=gpu_image, auth_id=auth_id
    )
    verify_template_rest(
        client.read_template(template_id),
        template_id=template_id,
        expected_name=template_name,
    )
    endpoint = _create_or_recover(
        client,
        resources=lambda inventory: inventory.endpoints,
        name=endpoint_name,
        description="endpoint",
        create=lambda: client.create_endpoint(endpoint_name, template_id),
    )
    if endpoint.get("workersMax") == 0:
        # A previous prune can be interrupted after idling the endpoint.  This
        # is the only mutable rerun state: prove every other field still
        # belongs to this exact release before restoring its worker ceiling.
        verify_endpoint(
            endpoint,
            expected_name=endpoint_name,
            template_id=template_id,
            workers_max=0,
        )
        verify_endpoint_rest(
            client.activate_endpoint(endpoint),
            endpoint_id=_resource_id(endpoint.get("id"), "endpoint ID"),
            expected_name=endpoint_name,
            template_id=template_id,
        )
        endpoint = _one_named(
            client.inventory().endpoints, endpoint_name, "endpoint"
        ) or {}
    endpoint_id = verify_endpoint(
        endpoint, expected_name=endpoint_name, template_id=template_id
    )

    # A final fresh read is the immutable hand-off contract consumed by release.env.
    inventory = client.inventory()
    verify_auth(
        _one_named(inventory.auths, auth_name, "registry auth") or {}, auth_name
    )
    verify_template(
        _one_named(inventory.templates, template_name, "template") or {},
        expected_name=template_name,
        image=gpu_image,
        auth_id=auth_id,
    )
    verify_template_rest(
        client.read_template(template_id),
        template_id=template_id,
        expected_name=template_name,
    )
    verified_endpoint = _one_named(inventory.endpoints, endpoint_name, "endpoint") or {}
    verify_endpoint(
        verified_endpoint,
        expected_name=endpoint_name,
        template_id=template_id,
    )
    rest_endpoint = client.read_endpoint(endpoint_id)
    try:
        verify_endpoint_rest(
            rest_endpoint,
            endpoint_id=endpoint_id,
            expected_name=endpoint_name,
            template_id=template_id,
        )
    except RunpodReleaseError:
        # GraphQL does not expose gpuCount or executionTimeoutMs.  If endpoint
        # creation succeeded but the follow-up REST PATCH did not, exact-name
        # reuse must reconcile only those two fields.  Any identity, scaling,
        # or compute drift still fails closed before a mutation is attempted.
        verify_endpoint_rest_base(
            rest_endpoint,
            endpoint_id=endpoint_id,
            expected_name=endpoint_name,
            template_id=template_id,
        )
        rest_endpoint = client.configure_endpoint(verified_endpoint)
    verify_endpoint_rest(
        rest_endpoint,
        endpoint_id=endpoint_id,
        expected_name=endpoint_name,
        template_id=template_id,
    )
    return ReleaseResources(endpoint_id, template_id, auth_id)


def _created_at(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        result = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if result.tzinfo is None:
        return None
    return result.astimezone(timezone.utc)


def _by_id(
    resources: tuple[dict[str, Any], ...], resource_id: str
) -> dict[str, Any] | None:
    return next((resource for resource in resources if resource.get("id") == resource_id), None)


def _endpoint_release_sha(resource: Mapping[str, Any]) -> str | None:
    match = ENDPOINT_NAME.fullmatch(str(resource.get("name", "")))
    return match.group(1) if match is not None else None


def _template_release_sha(resource: Mapping[str, Any]) -> str | None:
    match = TEMPLATE_NAME.fullmatch(str(resource.get("name", "")))
    if match is None:
        return None
    image_match = IMMUTABLE_GPU_IMAGE.fullmatch(
        str(resource.get("imageName", ""))
    )
    if image_match is None or image_match.group(1) != match.group(1):
        return None
    if (
        resource.get("isServerless") is not True
        or resource.get("containerDiskInGb") != CONTAINER_DISK_GB
        or resource.get("volumeInGb") != 0
        or "dockerArgs" not in resource
        or resource.get("dockerArgs") not in (None, "")
        or resource.get("env") != TEMPLATE_ENV_KEYS
    ):
        return None
    return match.group(1)


def _legacy_template_release_sha(resource: Mapping[str, Any]) -> str | None:
    """Recognize only the exact pre-init-timeout TARS template contract."""

    match = TEMPLATE_NAME.fullmatch(str(resource.get("name", "")))
    if match is None:
        return None
    image_match = IMMUTABLE_GPU_IMAGE.fullmatch(
        str(resource.get("imageName", ""))
    )
    if image_match is None or image_match.group(1) != match.group(1):
        return None
    if (
        resource.get("isServerless") is not True
        or resource.get("containerDiskInGb") != CONTAINER_DISK_GB
        or resource.get("volumeInGb") != 0
        or "dockerArgs" not in resource
        or resource.get("dockerArgs") not in (None, "")
        or resource.get("env") != []
    ):
        return None
    return match.group(1)


def _retirement_template_variant(
    resource: Mapping[str, Any], release_sha: str
) -> str | None:
    if _template_release_sha(resource) == release_sha:
        return "current"
    if _legacy_template_release_sha(resource) == release_sha:
        return "legacy"
    return None


def verify_retirement_template_rest(
    resource: Mapping[str, Any],
    *,
    template_id: str,
    expected_name: str,
    variant: str,
) -> str:
    """Verify one of the two exact TARS template generations we may retire."""

    _expect_equal(resource.get("id"), template_id, "REST template id")
    _expect_equal(resource.get("name"), expected_name, "REST template name")
    if variant == "current":
        _expect_equal(resource.get("env"), TEMPLATE_ENV, "REST template env")
    elif variant == "legacy":
        # Runpod omits env entirely from the REST representation of the
        # historical GraphQL env=[] form. Keep this exception retirement-only;
        # current release reconciliation still requires the exact env object.
        if "env" in resource:
            raise RunpodReleaseError(
                "existing Runpod REST template env does not match this release"
            )
    else:
        raise AssertionError("unknown TARS retirement template variant")
    return _resource_id(resource.get("id"), "template ID")


def verify_retirement_endpoint(
    resource: Mapping[str, Any],
    *,
    expected_name: str,
    template_id: str,
    workers_max: int,
    variant: str,
) -> str:
    """Verify exact current or historical endpoint fields only for retirement."""

    if variant == "current":
        gpu_selector = GPU_POOL_SELECTOR
    elif variant == "legacy":
        gpu_selector = LEGACY_GPU_POOL_SELECTOR
    else:
        raise AssertionError("unknown TARS retirement endpoint variant")
    expectations = {
        "name": expected_name,
        "gpuIds": gpu_selector,
        "idleTimeout": IDLE_TIMEOUT_SECONDS,
        "scalerType": SCALER_TYPE,
        "scalerValue": SCALER_VALUE,
        "templateId": template_id,
        "workersMax": workers_max,
        "workersMin": WORKERS_MIN,
    }
    for field, expected in expectations.items():
        _expect_equal(resource.get(field), expected, f"endpoint {field}")
    if resource.get("locations") not in (None, ""):
        raise RunpodReleaseError("existing Runpod endpoint restricts worker locations")
    return _resource_id(resource.get("id"), "endpoint ID")


def _auth_release_sha(resource: Mapping[str, Any]) -> str | None:
    match = AUTH_NAME.fullmatch(str(resource.get("name", "")))
    return match.group(1) if match is not None else None


@dataclass(frozen=True)
class _RetirementCandidate:
    created: datetime
    release_sha: str
    endpoint: dict[str, Any]
    template: dict[str, Any]
    auth: dict[str, Any]
    variant: str


def _fresh_retirement_candidate(
    client: ReleaseAPI,
    candidate: _RetirementCandidate,
    *,
    cutoff: datetime,
) -> _RetirementCandidate | None:
    """Re-prove an exact, unshared chain without making a provider mutation."""

    endpoint_id = _resource_id(candidate.endpoint.get("id"), "endpoint ID")
    template_id = _resource_id(candidate.template.get("id"), "template ID")
    auth_id = _resource_id(candidate.auth.get("id"), "registry auth ID")
    fresh = client.inventory()
    endpoint = _by_id(fresh.endpoints, endpoint_id)
    template = _by_id(fresh.templates, template_id)
    auth = _by_id(fresh.auths, auth_id)
    fresh_created = (
        _created_at(endpoint.get("createdAt"))
        if endpoint is not None
        else None
    )
    if (
        endpoint is None
        or template is None
        or auth is None
        or endpoint.get("name") != candidate.endpoint.get("name")
        or endpoint.get("templateId") != template_id
        or template.get("name") != candidate.template.get("name")
        or template.get("containerRegistryAuthId") != auth_id
        or auth.get("name") != candidate.auth.get("name")
        or _endpoint_release_sha(endpoint) != candidate.release_sha
        or _retirement_template_variant(template, candidate.release_sha)
        != candidate.variant
        or _auth_release_sha(auth) != candidate.release_sha
        or fresh_created is None
        or fresh_created > cutoff
    ):
        return None
    if any(
        other.get("id") != endpoint_id and other.get("templateId") == template_id
        for other in fresh.endpoints
    ):
        return None
    workers_max = endpoint.get("workersMax")
    if workers_max not in (0, WORKERS_MAX):
        return None
    try:
        verify_retirement_endpoint(
            endpoint,
            expected_name=str(candidate.endpoint.get("name")),
            template_id=template_id,
            workers_max=workers_max,
            variant=candidate.variant,
        )
    except RunpodReleaseError:
        return None
    verify_endpoint_rest(
        client.read_endpoint(endpoint_id),
        endpoint_id=endpoint_id,
        expected_name=str(candidate.endpoint.get("name")),
        template_id=template_id,
        workers_max=workers_max,
    )
    verify_retirement_template_rest(
        client.read_template(template_id),
        template_id=template_id,
        expected_name=str(candidate.template.get("name")),
        variant=candidate.variant,
    )
    return _RetirementCandidate(
        created=fresh_created,
        release_sha=candidate.release_sha,
        endpoint=endpoint,
        template=template,
        auth=auth,
        variant=candidate.variant,
    )


def _restore_retirement_endpoint(
    client: ReleaseAPI, candidate: _RetirementCandidate
) -> None:
    endpoint_id = _resource_id(candidate.endpoint.get("id"), "endpoint ID")
    template_id = _resource_id(candidate.template.get("id"), "template ID")
    try:
        restored = client.activate_endpoint(candidate.endpoint)
        verify_endpoint_rest(
            restored,
            endpoint_id=endpoint_id,
            expected_name=str(candidate.endpoint.get("name")),
            template_id=template_id,
            workers_max=WORKERS_MAX,
        )
    except RunpodReleaseError:
        raise RunpodReleaseError(
            "Runpod endpoint worker restoration could not be confirmed; "
            "the endpoint was not deleted"
        ) from None


def prune_releases(
    client: ReleaseAPI,
    *,
    current_endpoint_id: str,
    previous_endpoint_id: str | None,
    protected_release_sha: str,
    now: datetime,
    grace: timedelta = PRUNE_GRACE,
) -> int:
    current_endpoint_id = _resource_id(current_endpoint_id, "current endpoint ID")
    if SHA.fullmatch(protected_release_sha) is None:
        raise RunpodReleaseError(
            "protected release SHA must be 40 lowercase hexadecimal characters"
        )
    protected_endpoint_ids = {current_endpoint_id}
    if previous_endpoint_id:
        protected_endpoint_ids.add(
            _resource_id(previous_endpoint_id, "previous endpoint ID")
        )
    if now.tzinfo is None:
        raise RunpodReleaseError("prune time must include a timezone")
    cutoff = now.astimezone(timezone.utc) - grace
    inventory = client.inventory()
    current = _by_id(inventory.endpoints, current_endpoint_id)
    current_release_sha = (
        _endpoint_release_sha(current) if current is not None else None
    )
    if current_release_sha is None:
        raise RunpodReleaseError("current endpoint is not a TARS-owned Runpod release")
    protected_release_shas = {current_release_sha, protected_release_sha}
    if previous_endpoint_id and previous_endpoint_id != current_endpoint_id:
        previous = _by_id(inventory.endpoints, previous_endpoint_id)
        if previous is not None:
            previous_release_sha = _endpoint_release_sha(previous)
            if previous_release_sha is None:
                raise RunpodReleaseError(
                    "previous endpoint is not a TARS-owned Runpod release"
                )
            protected_release_shas.add(previous_release_sha)

    candidates: list[_RetirementCandidate] = []
    for endpoint in inventory.endpoints:
        endpoint_id = endpoint.get("id")
        release_sha = _endpoint_release_sha(endpoint)
        if (
            endpoint_id in protected_endpoint_ids
            or release_sha is None
            or release_sha in protected_release_shas
        ):
            continue
        created = _created_at(endpoint.get("createdAt"))
        if created is None or created > cutoff:
            continue
        template_id = endpoint.get("templateId")
        template = _by_id(inventory.templates, str(template_id))
        if template is None:
            continue
        variant = _retirement_template_variant(template, release_sha)
        if variant is None:
            continue
        if template.get("boundEndpointId") not in (None, "", endpoint_id):
            continue
        auth = _by_id(inventory.auths, str(template.get("containerRegistryAuthId")))
        if (
            auth is None
            or _auth_release_sha(auth) != release_sha
        ):
            continue
        candidates.append(
            _RetirementCandidate(
                created=created,
                release_sha=release_sha,
                endpoint=endpoint,
                template=template,
                auth=auth,
                variant=variant,
            )
        )

    # Prove health for every owned candidate before making the first mutation.
    # A malformed or unavailable health response therefore fails the entire
    # operation without partially consuming the retirement set.
    idle_candidates: list[_RetirementCandidate] = []
    for candidate in sorted(candidates, key=lambda item: item.created):
        fresh = _fresh_retirement_candidate(
            client, candidate, cutoff=cutoff
        )
        if fresh is None:
            continue
        endpoint_id = _resource_id(fresh.endpoint.get("id"), "endpoint ID")
        if client.read_endpoint_health(endpoint_id).has_active_work:
            continue
        idle_candidates.append(fresh)

    retired_releases: set[str] = set()
    for candidate in idle_candidates:
        fresh = _fresh_retirement_candidate(
            client, candidate, cutoff=cutoff
        )
        if fresh is None:
            continue
        endpoint_id = _resource_id(fresh.endpoint.get("id"), "endpoint ID")
        template_id = _resource_id(fresh.template.get("id"), "template ID")
        auth_id = _resource_id(fresh.auth.get("id"), "registry auth ID")
        if client.read_endpoint_health(endpoint_id).has_active_work:
            continue
        current_workers_max = fresh.endpoint.get("workersMax")
        changed_workers_max = current_workers_max != 0
        if changed_workers_max:
            verify_endpoint_rest(
                client.zero_endpoint(fresh.endpoint),
                endpoint_id=endpoint_id,
                expected_name=str(fresh.endpoint.get("name")),
                template_id=template_id,
                workers_max=0,
            )
        try:
            post_zero_health = client.read_endpoint_health(endpoint_id)
        except RunpodReleaseError:
            if changed_workers_max:
                _restore_retirement_endpoint(client, fresh)
            raise RunpodReleaseError(
                "Runpod endpoint health could not be confirmed after workers "
                + (
                    "were zeroed; the endpoint was restored and not deleted"
                    if changed_workers_max
                    else "were checked; the endpoint was left unchanged and "
                    "not deleted"
                )
            ) from None
        if post_zero_health.has_active_work:
            if changed_workers_max:
                _restore_retirement_endpoint(client, fresh)
            raise RunpodReleaseError(
                "Runpod endpoint received work during retirement; "
                + (
                    "the endpoint was restored and not deleted"
                    if changed_workers_max
                    else "the endpoint was left unchanged and not deleted"
                )
            )
        zeroed = _by_id(client.inventory().endpoints, endpoint_id)
        if zeroed is None:
            raise RunpodReleaseError(
                "Runpod did not confirm zero workers before endpoint deletion"
            )
        verify_retirement_endpoint(
            zeroed,
            expected_name=str(fresh.endpoint.get("name")),
            template_id=template_id,
            workers_max=0,
            variant=fresh.variant,
        )
        after_endpoint = client.delete_endpoint(endpoint_id)
        if _by_id(after_endpoint.endpoints, endpoint_id) is not None:
            raise RunpodReleaseError("Runpod retained a deleted TARS endpoint")
        if any(other.get("templateId") == template_id for other in after_endpoint.endpoints):
            raise RunpodReleaseError("Runpod template became shared during retirement")
        after_template = client.delete_template(
            _resource_name(
                fresh.template.get("name"), TEMPLATE_NAME, "template name"
            )
        )
        if _by_id(after_template.templates, template_id) is not None:
            raise RunpodReleaseError("Runpod retained a deleted TARS template")
        if not any(
            other.get("containerRegistryAuthId") == auth_id
            for other in after_template.templates
        ):
            after_auth = client.delete_auth(auth_id)
            if _by_id(after_auth.auths, auth_id) is not None:
                raise RunpodReleaseError("Runpod retained a deleted TARS registry auth")
        retired_releases.add(fresh.release_sha)
    return len(retired_releases)


def read_secret(path: Path, name: str) -> str:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise RunpodReleaseError(f"cannot read the {name} secret file") from error
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RunpodReleaseError(f"{name} must be an owner-only regular file")
    if metadata.st_size <= 0 or metadata.st_size > 65_536:
        raise RunpodReleaseError(f"{name} secret file has an invalid size")
    try:
        value = path.read_text(encoding="utf-8").rstrip("\r\n")
    except (OSError, UnicodeError) as error:
        raise RunpodReleaseError(f"cannot read the {name} secret file") from error
    if not value or "\x00" in value:
        raise RunpodReleaseError(f"{name} secret file is empty or invalid")
    return value


def read_previous_endpoint(path: Path) -> str | None:
    return read_endpoint_file(path, "previous")


def read_endpoint_file(path: Path, description: str) -> str | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RunpodReleaseError(
            f"cannot read the {description} endpoint file"
        ) from error
    if not value:
        return None
    return _resource_id(value, f"{description} endpoint ID")


def select_previous_endpoint(
    *,
    current_endpoint_id: str | None,
    target_endpoint_id: str,
    stored_endpoint_id: str | None,
) -> tuple[str | None, bool]:
    """Keep the newest deployed endpoint distinct from the rollout target.

    The current endpoint replaces the stored predecessor immediately before a
    distinct rollout.  On a retry after that rollout switched production, the
    current endpoint equals the target, so the stored predecessor is retained.
    """

    target = _resource_id(target_endpoint_id, "target endpoint ID")
    current = (
        _resource_id(current_endpoint_id, "current endpoint ID")
        if current_endpoint_id
        else None
    )
    stored = (
        _resource_id(stored_endpoint_id, "stored previous endpoint ID")
        if stored_endpoint_id
        else None
    )
    if current is not None and current != target:
        return current, True
    if stored is not None and stored != target:
        return stored, False
    return None, False


def write_endpoint_file(path: Path, endpoint_id: str | None) -> None:
    try:
        path.write_text(
            f"{endpoint_id}\n" if endpoint_id is not None else "",
            encoding="utf-8",
        )
        path.chmod(0o600)
    except OSError as error:
        raise RunpodReleaseError("cannot write the endpoint selection file") from error


def append_output(path: Path, name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise RunpodReleaseError("GitHub output contains an invalid value")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    ensure = commands.add_parser("ensure")
    ensure.add_argument("--release-sha", required=True)
    ensure.add_argument("--gpu-image", required=True)
    ensure.add_argument("--api-key-file", type=Path, required=True)
    ensure.add_argument("--registry-username-file", type=Path, required=True)
    ensure.add_argument("--registry-password-file", type=Path, required=True)
    ensure.add_argument("--github-output", type=Path, required=True)
    prune = commands.add_parser("prune")
    prune.add_argument("--api-key-file", type=Path, required=True)
    prune.add_argument("--current-endpoint", required=True)
    prune.add_argument("--previous-endpoint-file", type=Path, required=True)
    prune.add_argument("--protected-release-sha", required=True)
    pre_prune = commands.add_parser("pre-prune")
    pre_prune.add_argument("--api-key-file", type=Path, required=True)
    pre_prune.add_argument("--current-endpoint-file", type=Path, required=True)
    pre_prune.add_argument("--previous-endpoint-file", type=Path, required=True)
    pre_prune.add_argument("--protected-release-sha", required=True)
    select_previous = commands.add_parser("select-previous")
    select_previous.add_argument("--current-endpoint-file", type=Path, required=True)
    select_previous.add_argument("--target-endpoint", required=True)
    select_previous.add_argument("--stored-endpoint-file", type=Path, required=True)
    select_previous.add_argument("--selected-endpoint-file", type=Path, required=True)
    select_previous.add_argument("--store-endpoint-file", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "select-previous":
            selected, should_store = select_previous_endpoint(
                current_endpoint_id=read_previous_endpoint(
                    args.current_endpoint_file
                ),
                target_endpoint_id=args.target_endpoint,
                stored_endpoint_id=read_previous_endpoint(
                    args.stored_endpoint_file
                ),
            )
            write_endpoint_file(args.selected_endpoint_file, selected)
            write_endpoint_file(
                args.store_endpoint_file,
                selected if should_store else None,
            )
            print("selected the protected TARS Runpod predecessor")
            return
        if args.command == "pre-prune":
            if SHA.fullmatch(args.protected_release_sha) is None:
                raise RunpodReleaseError(
                    "protected release SHA must be 40 lowercase hexadecimal "
                    "characters"
                )
            current_endpoint = read_endpoint_file(
                args.current_endpoint_file, "current"
            )
            previous_endpoint = read_previous_endpoint(
                args.previous_endpoint_file
            )
            if current_endpoint is None:
                print("retired 0 obsolete TARS Runpod release(s)")
                return
            api_key = read_secret(args.api_key_file, "RUNPOD_API_KEY")
            deleted = prune_releases(
                RunpodClient(api_key),
                current_endpoint_id=current_endpoint,
                previous_endpoint_id=previous_endpoint,
                protected_release_sha=args.protected_release_sha,
                now=datetime.now(timezone.utc),
                grace=timedelta(0),
            )
            print(f"retired {deleted} obsolete TARS Runpod release(s)")
            return
        api_key = read_secret(args.api_key_file, "RUNPOD_API_KEY")
        client = RunpodClient(api_key)
        if args.command == "ensure":
            username = read_secret(args.registry_username_file, "DOCR_READ_USERNAME")
            password = read_secret(args.registry_password_file, "DOCR_READ_PASSWORD")
            resources = ensure_release(
                client,
                release_sha=args.release_sha,
                gpu_image=args.gpu_image,
                registry_username=username,
                registry_password=password,
            )
            append_output(args.github_output, "endpoint_id", resources.endpoint_id)
            print("verified immutable TARS Runpod release")
        else:
            deleted = prune_releases(
                client,
                current_endpoint_id=args.current_endpoint,
                previous_endpoint_id=read_previous_endpoint(args.previous_endpoint_file),
                protected_release_sha=args.protected_release_sha,
                now=datetime.now(timezone.utc),
            )
            print(f"retired {deleted} expired TARS Runpod release(s)")
    except RunpodReleaseError as error:
        parser.exit(1, f"TARS Runpod release failed: {error}\n")


if __name__ == "__main__":
    main()
