#!/usr/bin/env python3
"""Manage the single stable TARS Runpod Serverless endpoint and template.

Ordinary production releases never create or delete Runpod resources. They
verify the stable endpoint, template, and registry-auth IDs supplied by
Infisical, capture the complete prior template contract, and update only the
template image. Existing exact v1 resources can be renamed in place through
the explicit ``migrate`` command; creation is available solely through the
explicit one-time ``bootstrap`` command.

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
import os
import re
import secrets
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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
STABLE_ENDPOINT_NAME = "tars-runpod-endpoint-v2"
STABLE_TEMPLATE_NAME = "tars-runpod-template-v2"
STABLE_AUTH_PREFIX = "tars-runpod-auth-v2-"
ROLLOUT_POLL_SECONDS = 5.0
ROLLOUT_TIMEOUT_SECONDS = 8_400.0
INACTIVE_WORKER_STATUSES = frozenset({"EXITED", "TERMINATED"})
WORKER_STATUSES = frozenset({"RUNNING", *INACTIVE_WORKER_STATUSES})
TEMPLATE_VOLUME_MOUNT_PATH = "/workspace"

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
STABLE_AUTH_NAME = re.compile(r"tars-runpod-auth-v2-([0-9a-f]{12})\Z")
OWNED_ENDPOINT_NAME = re.compile(
    r"(?:tars-runpod-endpoint-v2|tars-runpod-endpoint-v1-[0-9a-f]{40})\Z"
)
TRANSIENT_HTTP = frozenset({408, 425, 429, 500, 502, 503, 504})


class RunpodReleaseError(RuntimeError):
    """A safe, operator-actionable Runpod release error."""


class RunpodDefinitiveRequestError(RunpodReleaseError):
    """A provider response that proves the requested mutation was rejected."""


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
class StableResourceIDs:
    endpoint_id: str
    template_id: str
    auth_id: str


@dataclass(frozen=True)
class StableRolloutReceipt:
    endpoint_id: str
    template_id: str
    auth_id: str
    baseline: str
    prior_release_sha: str | None
    prior_app_gpu_image: str | None
    release_sha: str
    target_image: str
    prior_image: str
    prior_version: int
    target_version: int | None
    mode: str


@dataclass(frozen=True)
class ApplicationRolloutBaseline:
    release_sha: str
    gpu_image: str
    endpoint_id: str
    replicas: int = 1


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

    def update_template(
        self, template_id: str, name: str, image: str, auth_id: str
    ) -> dict[str, Any]: ...

    def rename_endpoint(
        self, endpoint_id: str, template_id: str
    ) -> dict[str, Any]: ...

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
                error_type = (
                    RunpodReleaseError
                    if status in TRANSIENT_HTTP
                    else RunpodDefinitiveRequestError
                )
                failure = error_type(f"Runpod {operation} failed with HTTP {status}")
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
                raise RunpodDefinitiveRequestError(
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
                  pods { id desiredStatus }
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
        _resource_name(name, OWNED_ENDPOINT_NAME, "endpoint name")
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
        query = urllib.parse.urlencode(
            {"includeTemplate": "true", "includeWorkers": "true"}
        )
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

    def update_template(
        self, template_id: str, name: str, image: str, auth_id: str
    ) -> dict[str, Any]:
        """Issue exactly one update; callers observe eventual convergence."""

        template_id = _resource_id(template_id, "template ID")
        _expect_equal(name, STABLE_TEMPLATE_NAME, "stable template name")
        _resource_id(auth_id, "registry auth ID")
        payload = stable_template_payload(name=name, image=image, auth_id=auth_id)
        try:
            document = self._request(
                "POST",
                f"{REST_BASE_URL}/templates/{template_id}/update",
                operation="update stable template",
                payload=payload,
                max_calls=1,
                bearer_auth=True,
            )
        except RunpodDefinitiveRequestError:
            raise
        except RunpodReleaseError:
            # A transport failure or malformed success response is ambiguous.
            # Never reissue the mutation; the version/image poll resolves it.
            return {}
        return _object(document, "updated stable template")

    def rename_endpoint(
        self, endpoint_id: str, template_id: str
    ) -> dict[str, Any]:
        """Rename one exact endpoint once; callers reconcile eventual state."""

        endpoint_id = _resource_id(endpoint_id, "endpoint ID")
        template_id = _resource_id(template_id, "template ID")
        try:
            document = self._request(
                "PATCH",
                f"{REST_BASE_URL}/endpoints/{endpoint_id}",
                operation="adopt stable endpoint name",
                payload={"name": STABLE_ENDPOINT_NAME},
                empty_ok=True,
                max_calls=1,
                bearer_auth=True,
            )
        except RunpodDefinitiveRequestError:
            raise
        except RunpodReleaseError:
            # The PATCH may have succeeded even if its response was lost.
            # Migration polls the exact ID and never reissues this mutation.
            return {}
        return {} if document is None else _object(
            document, "renamed stable endpoint"
        )

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
        name = _resource_name(
            endpoint.get("name"), OWNED_ENDPOINT_NAME, "endpoint name"
        )
        template_id = _resource_id(endpoint.get("templateId"), "template ID")
        return self._confirmed_endpoint_patch(
            endpoint_id=endpoint_id,
            payload={"workersMin": WORKERS_MIN, "workersMax": workers_max},
            expected_name=name,
            template_id=template_id,
            workers_max=workers_max,
            operation=operation,
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
    _resource_name(name, OWNED_ENDPOINT_NAME, "endpoint name")
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


def _owned_template_payload(
    *, name: str, image: str, auth_id: str
) -> dict[str, Any]:
    """Return the complete reviewed representation for an owned template."""

    if name != STABLE_TEMPLATE_NAME and TEMPLATE_NAME.fullmatch(name) is None:
        raise RunpodReleaseError("Runpod returned an invalid TARS template name")
    if IMMUTABLE_GPU_IMAGE.fullmatch(image) is None:
        raise RunpodReleaseError(
            "GPU image must be the immutable TARS DOCR exact-SHA tag and digest"
        )
    _resource_id(auth_id, "registry auth ID")
    return {
        "containerDiskInGb": CONTAINER_DISK_GB,
        "containerRegistryAuthId": auth_id,
        "dockerEntrypoint": [],
        "dockerStartCmd": [],
        "env": dict(TEMPLATE_ENV),
        "imageName": image,
        "isPublic": False,
        "name": name,
        "ports": [],
        "readme": "",
        "volumeInGb": 0,
        "volumeMountPath": TEMPLATE_VOLUME_MOUNT_PATH,
    }


def stable_template_payload(
    *, name: str, image: str, auth_id: str
) -> dict[str, Any]:
    """Return the complete reviewed stable-template update representation."""

    _expect_equal(name, STABLE_TEMPLATE_NAME, "stable template name")
    return _owned_template_payload(name=name, image=image, auth_id=auth_id)


def verify_owned_template_rest(
    resource: Mapping[str, Any],
    *,
    template_id: str,
    expected_name: str,
    image: str,
    auth_id: str,
) -> str:
    expected = _owned_template_payload(
        name=expected_name,
        image=image,
        auth_id=auth_id,
    )
    _expect_equal(resource.get("id"), template_id, "owned REST template id")
    for field, value in expected.items():
        _expect_equal(
            resource.get(field), value, f"owned REST template {field}"
        )
    if resource.get("isServerless") is not True:
        raise RunpodReleaseError(
            "existing Runpod owned REST template is not serverless"
        )
    return _resource_id(resource.get("id"), "template ID")


def verify_stable_template_rest(
    resource: Mapping[str, Any],
    *,
    template_id: str,
    image: str,
    auth_id: str,
) -> str:
    return verify_owned_template_rest(
        resource,
        template_id=template_id,
        expected_name=STABLE_TEMPLATE_NAME,
        image=image,
        auth_id=auth_id,
    )


def stable_auth_name(username: str, password: str) -> str:
    if not username or not password or "\x00" in username or "\x00" in password:
        raise RunpodReleaseError("registry credential is empty or invalid")
    fingerprint = hashlib.sha256(
        username.encode("utf-8") + b"\0" + password.encode("utf-8")
    ).hexdigest()[:12]
    return f"{STABLE_AUTH_PREFIX}{fingerprint}"


def _validate_stable_ids(ids: StableResourceIDs) -> StableResourceIDs:
    return StableResourceIDs(
        endpoint_id=_resource_id(ids.endpoint_id, "stable endpoint ID"),
        template_id=_resource_id(ids.template_id, "stable template ID"),
        auth_id=_resource_id(ids.auth_id, "stable registry auth ID"),
    )


def _validate_release_image(release_sha: str, gpu_image: str) -> None:
    if SHA.fullmatch(release_sha) is None:
        raise RunpodReleaseError(
            "release SHA must be 40 lowercase hexadecimal characters"
        )
    image_match = IMMUTABLE_GPU_IMAGE.fullmatch(gpu_image)
    if image_match is None:
        raise RunpodReleaseError(
            "GPU image must be the immutable TARS DOCR exact-SHA tag and digest"
        )
    if image_match.group(1) != release_sha:
        raise RunpodReleaseError("GPU image tag does not match the release SHA")


def _stable_auth_is_owned(resource: Mapping[str, Any]) -> bool:
    name = str(resource.get("name", ""))
    return (
        STABLE_AUTH_NAME.fullmatch(name) is not None
        or AUTH_NAME.fullmatch(name) is not None
    )


def _stable_inventory_resources(
    client: ReleaseAPI, ids: StableResourceIDs
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    ids = _validate_stable_ids(ids)
    inventory = client.inventory()
    endpoint = _by_id(inventory.endpoints, ids.endpoint_id)
    template = _by_id(inventory.templates, ids.template_id)
    auth = _by_id(inventory.auths, ids.auth_id)
    if endpoint is None or template is None or auth is None:
        raise RunpodReleaseError(
            "the configured stable Runpod endpoint, template, or auth is missing; "
            "ordinary deployment will not create replacements"
        )
    _expect_equal(endpoint.get("name"), STABLE_ENDPOINT_NAME, "stable endpoint name")
    _expect_equal(template.get("name"), STABLE_TEMPLATE_NAME, "stable template name")
    if not _stable_auth_is_owned(auth):
        raise RunpodReleaseError(
            "the configured stable Runpod registry auth is not TARS-owned"
        )
    _expect_equal(
        endpoint.get("templateId"), ids.template_id, "stable endpoint templateId"
    )
    _expect_equal(
        template.get("boundEndpointId"),
        ids.endpoint_id,
        "stable template bound endpoint",
    )
    if any(
        other.get("id") != ids.endpoint_id
        and other.get("templateId") == ids.template_id
        for other in inventory.endpoints
    ):
        raise RunpodReleaseError(
            "the configured stable Runpod template is shared by another endpoint"
        )
    if any(
        other.get("id") != ids.template_id
        and other.get("containerRegistryAuthId") == ids.auth_id
        for other in inventory.templates
    ):
        raise RunpodReleaseError(
            "the configured stable Runpod registry auth is shared by another template"
        )
    _expect_equal(
        template.get("containerRegistryAuthId"),
        ids.auth_id,
        "stable template registry auth",
    )
    return endpoint, template, auth


def verify_stable_topology(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    expected_image: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Prove the configured IDs form the one fixed, exact TARS topology."""

    ids = _validate_stable_ids(ids)
    endpoint, template, auth = _stable_inventory_resources(client, ids)
    image = str(template.get("imageName", ""))
    if IMMUTABLE_GPU_IMAGE.fullmatch(image) is None:
        raise RunpodReleaseError(
            "existing Runpod stable template image is not immutable"
        )
    if expected_image is not None:
        _expect_equal(image, expected_image, "stable template image")
    verify_auth(auth, str(auth.get("name")))
    verify_template(
        template,
        expected_name=STABLE_TEMPLATE_NAME,
        image=image,
        auth_id=ids.auth_id,
    )
    verify_endpoint(
        endpoint,
        expected_name=STABLE_ENDPOINT_NAME,
        template_id=ids.template_id,
    )
    verify_stable_template_rest(
        client.read_template(ids.template_id),
        template_id=ids.template_id,
        image=image,
        auth_id=ids.auth_id,
    )
    rest_endpoint = client.read_endpoint(ids.endpoint_id)
    verify_endpoint_rest(
        rest_endpoint,
        endpoint_id=ids.endpoint_id,
        expected_name=STABLE_ENDPOINT_NAME,
        template_id=ids.template_id,
    )
    _verify_endpoint_template(rest_endpoint, ids=ids, image=image)
    version = _endpoint_version(rest_endpoint)
    _workers(rest_endpoint)
    verify_active_worker_generation(
        endpoint,
        rest_endpoint,
        image=image,
        auth_id=ids.auth_id,
        version=version,
    )
    return endpoint, template, rest_endpoint


def _endpoint_version(endpoint: Mapping[str, Any]) -> int:
    value = endpoint.get("version")
    if type(value) is not int or value < 0:
        raise RunpodReleaseError("Runpod stable endpoint has an invalid version")
    return value


def _workers(endpoint: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    value = endpoint.get("workers")
    if not isinstance(value, list) or any(
        not isinstance(worker, dict) for worker in value
    ):
        raise RunpodReleaseError(
            "Runpod stable endpoint has invalid worker inventory"
        )
    return tuple(value)


def _pod_statuses(endpoint: Mapping[str, Any]) -> dict[str, str]:
    value = endpoint.get("pods")
    if not isinstance(value, list):
        raise RunpodReleaseError(
            "Runpod stable endpoint has invalid pod inventory"
        )
    result: dict[str, str] = {}
    for pod in value:
        if not isinstance(pod, dict):
            raise RunpodReleaseError(
                "Runpod stable endpoint has invalid pod inventory"
            )
        pod_id = _resource_id(pod.get("id"), "worker ID")
        status = pod.get("desiredStatus")
        if not isinstance(status, str) or status not in WORKER_STATUSES:
            raise RunpodReleaseError(
                "Runpod stable endpoint has invalid worker status"
            )
        if pod_id in result:
            raise RunpodReleaseError(
                "Runpod stable endpoint contains duplicate worker IDs"
            )
        result[pod_id] = status
    return result


def _active_workers(
    inventory_endpoint: Mapping[str, Any],
    rest_endpoint: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    statuses = _pod_statuses(inventory_endpoint)
    workers_by_id: dict[str, dict[str, Any]] = {}
    for worker in _workers(rest_endpoint):
        worker_id = _resource_id(worker.get("id"), "worker ID")
        if worker_id in workers_by_id:
            raise RunpodReleaseError(
                "Runpod stable endpoint contains duplicate REST worker IDs"
            )
        workers_by_id[worker_id] = worker
    active_ids = {
        worker_id
        for worker_id, status in statuses.items()
        if status not in INACTIVE_WORKER_STATUSES
    }
    missing = active_ids - set(workers_by_id)
    if missing:
        raise RunpodReleaseError(
            "Runpod active worker is missing from the worker-inclusive endpoint read"
        )
    unclassified = set(workers_by_id) - set(statuses)
    if unclassified:
        raise RunpodReleaseError(
            "Runpod REST worker is missing an authoritative worker status"
        )
    return tuple(workers_by_id[worker_id] for worker_id in sorted(active_ids))


def _stable_observation(
    client: ReleaseAPI, ids: StableResourceIDs
) -> tuple[dict[str, Any], dict[str, Any]]:
    inventory_endpoint, _, _ = _stable_inventory_resources(client, ids)
    rest_endpoint = client.read_endpoint(ids.endpoint_id)
    verify_endpoint_rest(
        rest_endpoint,
        endpoint_id=ids.endpoint_id,
        expected_name=STABLE_ENDPOINT_NAME,
        template_id=ids.template_id,
    )
    _endpoint_version(rest_endpoint)
    _workers(rest_endpoint)
    return inventory_endpoint, rest_endpoint


def wait_for_stable_idle(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Wait for the paused dispatcher to leave no queued or running GPU work."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Runpod rollout timing must be positive")
    deadline = clock() + timeout_seconds
    while True:
        health = client.read_endpoint_health(ids.endpoint_id)
        inventory_endpoint, rest_endpoint = _stable_observation(client, ids)
        if (
            health.in_queue == 0
            and health.in_progress == 0
            and not _active_workers(inventory_endpoint, rest_endpoint)
        ):
            return
        if clock() >= deadline:
            raise RunpodReleaseError(
                "Runpod stable endpoint did not drain before the rollout deadline"
            )
        sleeper(poll_seconds)


def _verify_endpoint_template(
    endpoint: Mapping[str, Any],
    *,
    ids: StableResourceIDs,
    image: str,
) -> None:
    template = endpoint.get("template")
    if not isinstance(template, dict):
        raise RunpodReleaseError(
            "Runpod stable endpoint omitted its bound template"
        )
    verify_stable_template_rest(
        template,
        template_id=ids.template_id,
        image=image,
        auth_id=ids.auth_id,
    )


def wait_for_stable_template_image(
    client: ReleaseAPI,
    *,
    template_id: str,
    image: str,
    auth_id: str,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Runpod rollout timing must be positive")
    deadline = clock() + timeout_seconds
    while True:
        try:
            verify_stable_template_rest(
                client.read_template(template_id),
                template_id=template_id,
                image=image,
                auth_id=auth_id,
            )
            return
        except RunpodReleaseError:
            if clock() >= deadline:
                raise RunpodReleaseError(
                    "Runpod stable template did not converge before the deadline"
                ) from None
        sleeper(poll_seconds)


def wait_for_stable_version(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    image: str,
    previous_version: int,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """Wait until Runpod exposes a new endpoint version for the template."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Runpod rollout timing must be positive")
    deadline = clock() + timeout_seconds
    while True:
        try:
            inventory_endpoint, _, endpoint = verify_stable_topology(
                client,
                ids=ids,
                expected_image=image,
            )
            version = _endpoint_version(endpoint)
            active = _active_workers(inventory_endpoint, endpoint)
        except RunpodReleaseError:
            version = -1
            active = ({"not": "converged"},)
        if version > previous_version and not active:
            return version
        if clock() >= deadline:
            raise RunpodReleaseError(
                "Runpod stable endpoint version did not converge before the deadline"
            )
        sleeper(poll_seconds)


def verify_active_worker_generation(
    inventory_endpoint: Mapping[str, Any],
    rest_endpoint: Mapping[str, Any],
    *,
    image: str,
    auth_id: str,
    version: int,
) -> None:
    """Allow historical EXITED rows but reject every active old generation."""

    for worker in _active_workers(inventory_endpoint, rest_endpoint):
        _expect_equal(worker.get("image"), image, "active worker image")
        _expect_equal(
            worker.get("containerRegistryAuthId"),
            auth_id,
            "active worker registry auth",
        )
        _expect_equal(
            worker.get("slsVersion"), version, "active worker slsVersion"
        )


def wait_for_active_worker_generation(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    image: str,
    version: int,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Runpod rollout timing must be positive")
    deadline = clock() + timeout_seconds
    while True:
        inventory_endpoint, endpoint = _stable_observation(client, ids)
        try:
            _verify_endpoint_template(endpoint, ids=ids, image=image)
            _expect_equal(
                _endpoint_version(endpoint),
                version,
                "stable endpoint version",
            )
            verify_active_worker_generation(
                inventory_endpoint,
                endpoint,
                image=image,
                auth_id=ids.auth_id,
                version=version,
            )
            return
        except RunpodReleaseError:
            if clock() >= deadline:
                raise RunpodReleaseError(
                    "Runpod retained an active superseded worker past the deadline"
                ) from None
        sleeper(poll_seconds)


def _write_private_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise RunpodReleaseError(
                "durable Runpod boundary contains a duplicate JSON object key"
            )
        document[key] = value
    return document


def _receipt_document(receipt: StableRolloutReceipt) -> dict[str, Any]:
    return {
        "auth_id": receipt.auth_id,
        "baseline": receipt.baseline,
        "endpoint_id": receipt.endpoint_id,
        "mode": receipt.mode,
        "prior_app_gpu_image": receipt.prior_app_gpu_image,
        "prior_image": receipt.prior_image,
        "prior_release_sha": receipt.prior_release_sha,
        "prior_version": receipt.prior_version,
        "release_sha": receipt.release_sha,
        "target_image": receipt.target_image,
        "target_version": receipt.target_version,
        "template_id": receipt.template_id,
    }


def _application_baseline_document(
    baseline: ApplicationRolloutBaseline,
) -> dict[str, Any]:
    return {
        "baseline": "existing",
        "endpoint_id": baseline.endpoint_id,
        "gpu_image": baseline.gpu_image,
        "kind": "application",
        "release_sha": baseline.release_sha,
        "replicas": baseline.replicas,
    }


def _validate_application_baseline(
    baseline: ApplicationRolloutBaseline,
) -> ApplicationRolloutBaseline:
    if (
        not isinstance(baseline.release_sha, str)
        or not isinstance(baseline.gpu_image, str)
        or not isinstance(baseline.endpoint_id, str)
        or isinstance(baseline.replicas, bool)
        or not isinstance(baseline.replicas, int)
    ):
        raise RunpodReleaseError(
            "application rollout baseline has invalid fields"
        )
    _validate_release_image(baseline.release_sha, baseline.gpu_image)
    _resource_id(baseline.endpoint_id, "application baseline endpoint")
    if baseline.replicas != 1:
        raise RunpodReleaseError(
            "application rollout baseline must restore one dispatcher replica"
        )
    return baseline


def write_application_baseline(
    path: Path, baseline: ApplicationRolloutBaseline
) -> None:
    _validate_application_baseline(baseline)
    _write_private_json(path, _application_baseline_document(baseline))


def read_application_baseline(path: Path) -> ApplicationRolloutBaseline:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_size <= 0
            or metadata.st_size > 65_536
        ):
            raise RunpodReleaseError(
                "application rollout baseline must be an owner-only regular file"
            )
        document = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except RunpodReleaseError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RunpodReleaseError(
            "cannot read the application rollout baseline"
        ) from error
    keys = {
        "baseline",
        "endpoint_id",
        "gpu_image",
        "kind",
        "release_sha",
        "replicas",
    }
    if not isinstance(document, dict) or set(document) != keys:
        raise RunpodReleaseError(
            "application rollout baseline has an invalid schema"
        )
    if document["baseline"] != "existing" or document["kind"] != "application":
        raise RunpodReleaseError(
            "application rollout baseline has an invalid kind"
        )
    try:
        baseline = ApplicationRolloutBaseline(
            release_sha=document["release_sha"],
            gpu_image=document["gpu_image"],
            endpoint_id=document["endpoint_id"],
            replicas=document["replicas"],
        )
    except (KeyError, TypeError) as error:
        raise RunpodReleaseError(
            "application rollout baseline has invalid fields"
        ) from error
    return _validate_application_baseline(baseline)


def write_rollout_receipt(path: Path, receipt: StableRolloutReceipt) -> None:
    if receipt.mode not in ("noop", "update"):
        raise RunpodReleaseError("stable rollout receipt has an invalid mode")
    if receipt.baseline not in ("existing", "greenfield"):
        raise RunpodReleaseError(
            "stable rollout receipt has an invalid application baseline"
        )
    _validate_release_image(receipt.release_sha, receipt.target_image)
    if IMMUTABLE_GPU_IMAGE.fullmatch(receipt.prior_image) is None:
        raise RunpodReleaseError(
            "stable rollout receipt prior image is not immutable"
        )
    _validate_stable_ids(
        StableResourceIDs(
            receipt.endpoint_id, receipt.template_id, receipt.auth_id
        )
    )
    if type(receipt.prior_version) is not int or receipt.prior_version < 0:
        raise RunpodReleaseError(
            "stable rollout receipt has an invalid prior version"
        )
    if receipt.target_version is not None and (
        type(receipt.target_version) is not int or receipt.target_version < 0
    ):
        raise RunpodReleaseError(
            "stable rollout receipt has an invalid target version"
        )
    if receipt.mode == "noop" and (
        receipt.prior_image != receipt.target_image
        or receipt.target_version != receipt.prior_version
    ):
        raise RunpodReleaseError(
            "stable no-op rollout receipt has inconsistent versions"
        )
    if receipt.mode == "update" and (
        receipt.prior_image == receipt.target_image
        or (
            receipt.target_version is not None
            and receipt.target_version <= receipt.prior_version
        )
    ):
        raise RunpodReleaseError(
            "stable update rollout receipt has inconsistent versions"
        )
    if receipt.baseline == "existing":
        if (
            not isinstance(receipt.prior_release_sha, str)
            or not isinstance(receipt.prior_app_gpu_image, str)
        ):
            raise RunpodReleaseError(
                "existing rollout receipt is missing its application baseline"
            )
        _validate_release_image(
            receipt.prior_release_sha, receipt.prior_app_gpu_image
        )
        if receipt.prior_app_gpu_image != receipt.prior_image:
            raise RunpodReleaseError(
                "application and provider rollback images do not match"
            )
    elif (
        receipt.prior_release_sha is not None
        or receipt.prior_app_gpu_image is not None
        or receipt.prior_image != receipt.target_image
        or receipt.mode != "noop"
    ):
        raise RunpodReleaseError(
            "greenfield rollout receipt has an invalid provider baseline"
        )
    _write_private_json(path, _receipt_document(receipt))


def read_rollout_receipt(path: Path) -> StableRolloutReceipt:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_size <= 0
            or metadata.st_size > 65_536
        ):
            raise RunpodReleaseError(
                "stable rollout receipt must be an owner-only regular file"
            )
        document = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except RunpodReleaseError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RunpodReleaseError("cannot read the stable rollout receipt") from error
    keys = {
        "auth_id",
        "baseline",
        "endpoint_id",
        "mode",
        "prior_app_gpu_image",
        "prior_image",
        "prior_release_sha",
        "prior_version",
        "release_sha",
        "target_image",
        "target_version",
        "template_id",
    }
    if not isinstance(document, dict) or set(document) != keys:
        raise RunpodReleaseError("stable rollout receipt has an invalid schema")
    try:
        receipt = StableRolloutReceipt(
            endpoint_id=document["endpoint_id"],
            template_id=document["template_id"],
            auth_id=document["auth_id"],
            baseline=document["baseline"],
            prior_release_sha=document["prior_release_sha"],
            prior_app_gpu_image=document["prior_app_gpu_image"],
            release_sha=document["release_sha"],
            target_image=document["target_image"],
            prior_image=document["prior_image"],
            prior_version=document["prior_version"],
            target_version=document["target_version"],
            mode=document["mode"],
        )
    except (KeyError, TypeError) as error:
        raise RunpodReleaseError(
            "stable rollout receipt has invalid fields"
        ) from error
    write_rollout_receipt(path, receipt)
    return receipt


def read_stable_id(path: Path, description: str) -> str:
    return _resource_id(read_secret(path, description), description)


def read_stable_ids(
    endpoint_path: Path, template_path: Path, auth_path: Path
) -> StableResourceIDs:
    return StableResourceIDs(
        endpoint_id=read_stable_id(endpoint_path, "RUNPOD_ENDPOINT_ID"),
        template_id=read_stable_id(template_path, "RUNPOD_TEMPLATE_ID"),
        auth_id=read_stable_id(auth_path, "RUNPOD_REGISTRY_AUTH_ID"),
    )


def prepare_stable_release(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    release_sha: str,
    gpu_image: str,
    prior_release_sha: str | None,
    prior_app_gpu_image: str | None,
    greenfield: bool,
    receipt_path: Path,
) -> StableRolloutReceipt:
    """Capture the app/provider rollback boundary without mutating Runpod."""

    ids = _validate_stable_ids(ids)
    _validate_release_image(release_sha, gpu_image)
    _, template, endpoint = verify_stable_topology(client, ids=ids)
    prior_image = str(template.get("imageName", ""))
    prior_version = _endpoint_version(endpoint)
    if greenfield:
        if prior_release_sha is not None or prior_app_gpu_image is not None:
            raise RunpodReleaseError(
                "greenfield preparation cannot include a prior application"
            )
        _expect_equal(
            prior_image,
            gpu_image,
            "greenfield provider image",
        )
        baseline = "greenfield"
    else:
        if prior_release_sha is None or prior_app_gpu_image is None:
            raise RunpodReleaseError(
                "existing deployment preparation requires the prior app SHA and image"
            )
        _validate_release_image(prior_release_sha, prior_app_gpu_image)
        _expect_equal(
            prior_image,
            prior_app_gpu_image,
            "provider image bound to the live dispatcher",
        )
        baseline = "existing"
    mode = "noop" if prior_image == gpu_image else "update"
    receipt = StableRolloutReceipt(
        endpoint_id=ids.endpoint_id,
        template_id=ids.template_id,
        auth_id=ids.auth_id,
        baseline=baseline,
        prior_release_sha=prior_release_sha,
        prior_app_gpu_image=prior_app_gpu_image,
        release_sha=release_sha,
        target_image=gpu_image,
        prior_image=prior_image,
        prior_version=prior_version,
        target_version=prior_version if mode == "noop" else None,
        mode=mode,
    )
    write_rollout_receipt(receipt_path, receipt)
    return receipt


def stage_prepared_stable_release(
    client: ReleaseAPI,
    *,
    receipt: StableRolloutReceipt,
    receipt_path: Path,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> StableRolloutReceipt:
    """Apply a previously persisted boundary; never infer a new baseline."""

    ids = _validate_stable_ids(
        StableResourceIDs(
            receipt.endpoint_id, receipt.template_id, receipt.auth_id
        )
    )
    _, _, endpoint = verify_stable_topology(
        client, ids=ids, expected_image=receipt.prior_image
    )
    _expect_equal(
        _endpoint_version(endpoint),
        receipt.prior_version,
        "prepared stable endpoint version",
    )
    wait_for_stable_idle(
        client,
        ids=ids,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    inventory_endpoint, _, settled_endpoint = verify_stable_topology(
        client, ids=ids, expected_image=receipt.prior_image
    )
    _verify_endpoint_template(
        settled_endpoint,
        ids=ids,
        image=receipt.prior_image,
    )
    settled_version = _endpoint_version(settled_endpoint)
    _expect_equal(
        settled_version,
        receipt.prior_version,
        "prepared stable endpoint version after drain",
    )
    verify_active_worker_generation(
        inventory_endpoint,
        settled_endpoint,
        image=receipt.prior_image,
        auth_id=ids.auth_id,
        version=settled_version,
    )
    if receipt.mode == "noop":
        return receipt

    client.update_template(
        ids.template_id,
        STABLE_TEMPLATE_NAME,
        receipt.target_image,
        ids.auth_id,
    )
    target_version = wait_for_stable_version(
        client,
        ids=ids,
        image=receipt.target_image,
        previous_version=receipt.prior_version,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    receipt = StableRolloutReceipt(
        **{
            **_receipt_document(receipt),
            "target_version": target_version,
        }
    )
    write_rollout_receipt(receipt_path, receipt)
    return receipt


def stage_stable_release(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    release_sha: str,
    gpu_image: str,
    prior_release_sha: str | None,
    prior_app_gpu_image: str | None,
    greenfield: bool,
    receipt_path: Path,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> StableRolloutReceipt:
    """Test/operator convenience wrapper around the durable two-phase API."""

    receipt = prepare_stable_release(
        client,
        ids=ids,
        release_sha=release_sha,
        gpu_image=gpu_image,
        prior_release_sha=prior_release_sha,
        prior_app_gpu_image=prior_app_gpu_image,
        greenfield=greenfield,
        receipt_path=receipt_path,
    )
    return stage_prepared_stable_release(
        client,
        receipt=receipt,
        receipt_path=receipt_path,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )


def rollback_stable_release(
    client: ReleaseAPI,
    *,
    receipt: StableRolloutReceipt,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    ids = _validate_stable_ids(
        StableResourceIDs(
            receipt.endpoint_id, receipt.template_id, receipt.auth_id
        )
    )
    _, template, endpoint = verify_stable_topology(client, ids=ids)
    current_image = str(template.get("imageName", ""))
    if current_image == receipt.prior_image:
        wait_for_stable_idle(
            client,
            ids=ids,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            sleeper=sleeper,
            clock=clock,
        )
        inventory_endpoint, _, settled_endpoint = verify_stable_topology(
            client, ids=ids, expected_image=receipt.prior_image
        )
        _verify_endpoint_template(
            settled_endpoint,
            ids=ids,
            image=receipt.prior_image,
        )
        settled_version = _endpoint_version(settled_endpoint)
        verify_active_worker_generation(
            inventory_endpoint,
            settled_endpoint,
            image=receipt.prior_image,
            auth_id=ids.auth_id,
            version=settled_version,
        )
        return
    if receipt.mode == "noop":
        raise RunpodReleaseError(
            "a no-op rollout receipt does not match the stable template"
        )
    if current_image != receipt.target_image:
        raise RunpodReleaseError(
            "stable template drifted after staging; refusing blind rollback"
        )
    current_version = _endpoint_version(endpoint)
    wait_for_stable_idle(
        client,
        ids=ids,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    inventory_endpoint, _, settled_endpoint = verify_stable_topology(
        client,
        ids=ids,
        expected_image=receipt.target_image,
    )
    _verify_endpoint_template(
        settled_endpoint,
        ids=ids,
        image=receipt.target_image,
    )
    settled_version = _endpoint_version(settled_endpoint)
    _expect_equal(
        settled_version,
        current_version,
        "stable target endpoint version after drain",
    )
    if receipt.target_version is not None:
        _expect_equal(
            settled_version,
            receipt.target_version,
            "receipt target endpoint version before rollback",
        )
    verify_active_worker_generation(
        inventory_endpoint,
        settled_endpoint,
        image=receipt.target_image,
        auth_id=ids.auth_id,
        version=settled_version,
    )
    client.update_template(
        ids.template_id,
        STABLE_TEMPLATE_NAME,
        receipt.prior_image,
        ids.auth_id,
    )
    rollback_version = wait_for_stable_version(
        client,
        ids=ids,
        image=receipt.prior_image,
        previous_version=current_version,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    verify_stable_topology(
        client, ids=ids, expected_image=receipt.prior_image
    )
    wait_for_active_worker_generation(
        client,
        ids=ids,
        image=receipt.prior_image,
        version=rollback_version,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )


def verify_application_generation(
    client: ReleaseAPI,
    *,
    baseline: ApplicationRolloutBaseline,
    ids: StableResourceIDs,
) -> int:
    """Verify the deployed application references one exact provider generation."""

    baseline = _validate_application_baseline(baseline)
    ids = _validate_stable_ids(ids)
    _expect_equal(
        ids.endpoint_id,
        baseline.endpoint_id,
        "application baseline stable endpoint",
    )
    inventory_endpoint, _, rest_endpoint = verify_stable_topology(
        client,
        ids=ids,
        expected_image=baseline.gpu_image,
    )
    _verify_endpoint_template(
        rest_endpoint,
        ids=ids,
        image=baseline.gpu_image,
    )
    version = _endpoint_version(rest_endpoint)
    verify_active_worker_generation(
        inventory_endpoint,
        rest_endpoint,
        image=baseline.gpu_image,
        auth_id=ids.auth_id,
        version=version,
    )
    return version


def finalize_stable_release(
    client: ReleaseAPI,
    *,
    receipt: StableRolloutReceipt,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    ids = _validate_stable_ids(
        StableResourceIDs(
            receipt.endpoint_id, receipt.template_id, receipt.auth_id
        )
    )
    if receipt.target_version is None:
        raise RunpodReleaseError(
            "stable rollout receipt has no confirmed target version"
        )
    verify_stable_topology(client, ids=ids, expected_image=receipt.target_image)
    wait_for_active_worker_generation(
        client,
        ids=ids,
        image=receipt.target_image,
        version=receipt.target_version,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )


def verify_receipt_target(
    client: ReleaseAPI,
    *,
    receipt: StableRolloutReceipt,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """Verify an interrupted rollout reached its exact idle provider target."""

    ids = _validate_stable_ids(
        StableResourceIDs(
            receipt.endpoint_id, receipt.template_id, receipt.auth_id
        )
    )
    _, _, rest_endpoint = verify_stable_topology(
        client, ids=ids, expected_image=receipt.target_image
    )
    _verify_endpoint_template(
        rest_endpoint,
        ids=ids,
        image=receipt.target_image,
    )
    version = _endpoint_version(rest_endpoint)
    if receipt.mode == "update" and version <= receipt.prior_version:
        raise RunpodReleaseError(
            "stable endpoint version did not advance to the receipt target"
        )
    if receipt.target_version is not None:
        _expect_equal(
            version, receipt.target_version, "receipt target endpoint version"
        )
    wait_for_stable_idle(
        client,
        ids=ids,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    endpoint, _, rest_endpoint = verify_stable_topology(
        client, ids=ids, expected_image=receipt.target_image
    )
    _verify_endpoint_template(
        rest_endpoint,
        ids=ids,
        image=receipt.target_image,
    )
    settled_version = _endpoint_version(rest_endpoint)
    _expect_equal(
        settled_version,
        version,
        "receipt target endpoint version after drain",
    )
    verify_active_worker_generation(
        endpoint,
        rest_endpoint,
        image=receipt.target_image,
        auth_id=ids.auth_id,
        version=settled_version,
    )
    return settled_version


def _adoptable_inventory_resources(
    client: ReleaseAPI,
    ids: StableResourceIDs,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    Inventory,
]:
    """Resolve one exact current-or-partially-adopted resource chain."""

    ids = _validate_stable_ids(ids)
    inventory = client.inventory()
    endpoint = _by_id(inventory.endpoints, ids.endpoint_id)
    template = _by_id(inventory.templates, ids.template_id)
    auth = _by_id(inventory.auths, ids.auth_id)
    if endpoint is None or template is None or auth is None:
        raise RunpodReleaseError(
            "the configured Runpod endpoint, template, or auth to adopt is missing"
        )
    for resources, stable_name, resource_id, description in (
        (
            inventory.endpoints,
            STABLE_ENDPOINT_NAME,
            ids.endpoint_id,
            "endpoint",
        ),
        (
            inventory.templates,
            STABLE_TEMPLATE_NAME,
            ids.template_id,
            "template",
        ),
    ):
        named = _one_named(resources, stable_name, f"stable {description}")
        if named is not None and named.get("id") != resource_id:
            raise RunpodReleaseError(
                f"another Runpod {description} already owns the stable name"
            )
    return endpoint, template, auth, inventory


def verify_adoptable_topology(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Prove exact ownership before renaming an existing v1 chain in place."""

    ids = _validate_stable_ids(ids)
    endpoint, template, auth, inventory = _adoptable_inventory_resources(
        client, ids
    )
    endpoint_name = str(endpoint.get("name", ""))
    endpoint_match = ENDPOINT_NAME.fullmatch(endpoint_name)
    if endpoint_name != STABLE_ENDPOINT_NAME and endpoint_match is None:
        raise RunpodReleaseError(
            "configured Runpod endpoint is not an adoptable TARS resource"
        )
    template_name = str(template.get("name", ""))
    template_match = TEMPLATE_NAME.fullmatch(template_name)
    if template_name != STABLE_TEMPLATE_NAME and template_match is None:
        raise RunpodReleaseError(
            "configured Runpod template is not an adoptable TARS resource"
        )
    image = str(template.get("imageName", ""))
    image_match = IMMUTABLE_GPU_IMAGE.fullmatch(image)
    if image_match is None:
        raise RunpodReleaseError(
            "adopted Runpod template image is not immutable"
        )
    release_sha = image_match.group(1)
    for match, description in (
        (endpoint_match, "endpoint"),
        (template_match, "template"),
    ):
        if match is not None and match.group(1) != release_sha:
            raise RunpodReleaseError(
                f"adopted Runpod {description} name does not match its image"
            )
    auth_name = str(auth.get("name", ""))
    auth_match = AUTH_NAME.fullmatch(auth_name)
    if STABLE_AUTH_NAME.fullmatch(auth_name) is None and auth_match is None:
        raise RunpodReleaseError(
            "configured Runpod registry auth is not an adoptable TARS resource"
        )
    if (
        (endpoint_match is not None or template_match is not None)
        and auth_match is not None
        and auth_match.group(1) != release_sha
    ):
        raise RunpodReleaseError(
            "adopted Runpod registry auth name does not match its image"
        )
    _expect_equal(
        endpoint.get("templateId"), ids.template_id, "adopted endpoint template"
    )
    _expect_equal(
        template.get("containerRegistryAuthId"),
        ids.auth_id,
        "adopted template registry auth",
    )
    _expect_equal(
        template.get("boundEndpointId"),
        ids.endpoint_id,
        "adopted template bound endpoint",
    )
    if any(
        other.get("id") != ids.endpoint_id
        and other.get("templateId") == ids.template_id
        for other in inventory.endpoints
    ):
        raise RunpodReleaseError(
            "configured Runpod template is shared by another endpoint"
        )
    if any(
        other.get("id") != ids.template_id
        and other.get("containerRegistryAuthId") == ids.auth_id
        for other in inventory.templates
    ):
        raise RunpodReleaseError(
            "configured Runpod registry auth is shared by another template"
        )
    verify_auth(auth, auth_name)
    verify_template(
        template,
        expected_name=template_name,
        image=image,
        auth_id=ids.auth_id,
    )
    verify_endpoint(
        endpoint,
        expected_name=endpoint_name,
        template_id=ids.template_id,
    )
    verify_owned_template_rest(
        client.read_template(ids.template_id),
        template_id=ids.template_id,
        expected_name=template_name,
        image=image,
        auth_id=ids.auth_id,
    )
    rest_endpoint = client.read_endpoint(ids.endpoint_id)
    verify_endpoint_rest(
        rest_endpoint,
        endpoint_id=ids.endpoint_id,
        expected_name=endpoint_name,
        template_id=ids.template_id,
    )
    embedded_template = rest_endpoint.get("template")
    if not isinstance(embedded_template, dict):
        raise RunpodReleaseError(
            "Runpod endpoint to adopt omitted its bound template"
        )
    verify_owned_template_rest(
        embedded_template,
        template_id=ids.template_id,
        expected_name=template_name,
        image=image,
        auth_id=ids.auth_id,
    )
    version = _endpoint_version(rest_endpoint)
    _workers(rest_endpoint)
    verify_active_worker_generation(
        endpoint,
        rest_endpoint,
        image=image,
        auth_id=ids.auth_id,
        version=version,
    )
    return endpoint, template, rest_endpoint


def wait_for_adoptable_idle(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Wait until an exact existing chain has no queued or active work."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Runpod rollout timing must be positive")
    deadline = clock() + timeout_seconds
    while True:
        endpoint, template, rest_endpoint = verify_adoptable_topology(
            client, ids=ids
        )
        health = client.read_endpoint_health(ids.endpoint_id)
        if not health.has_active_work and not _active_workers(
            endpoint, rest_endpoint
        ):
            return endpoint, template, rest_endpoint
        if clock() >= deadline:
            raise RunpodReleaseError(
                "Runpod endpoint to adopt did not drain before the deadline"
            )
        sleeper(poll_seconds)


def wait_for_adopted_topology(
    client: ReleaseAPI,
    *,
    ids: StableResourceIDs,
    image: str,
    minimum_version: int,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Reconcile an ambiguous rename without ever reissuing a mutation."""

    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Runpod rollout timing must be positive")
    deadline = clock() + timeout_seconds
    while True:
        try:
            endpoint, _, rest_endpoint = verify_stable_topology(
                client, ids=ids, expected_image=image
            )
            health = client.read_endpoint_health(ids.endpoint_id)
            if (
                _endpoint_version(rest_endpoint) >= minimum_version
                and not health.has_active_work
                and not _active_workers(endpoint, rest_endpoint)
            ):
                return
        except RunpodReleaseError:
            pass
        if clock() >= deadline:
            raise RunpodReleaseError(
                "Runpod stable-resource adoption did not converge before the deadline"
            )
        sleeper(poll_seconds)


def discover_adoptable_ids(
    client: ReleaseAPI,
    *,
    endpoint_id: str,
) -> StableResourceIDs:
    """Derive the exact bound template and auth from one existing endpoint."""

    endpoint_id = _resource_id(endpoint_id, "endpoint ID to adopt")
    inventory = client.inventory()
    endpoint = _by_id(inventory.endpoints, endpoint_id)
    if endpoint is None:
        raise RunpodReleaseError(
            "the configured Runpod endpoint to adopt is missing"
        )
    endpoint_name = str(endpoint.get("name", ""))
    if (
        endpoint_name != STABLE_ENDPOINT_NAME
        and ENDPOINT_NAME.fullmatch(endpoint_name) is None
    ):
        raise RunpodReleaseError(
            "configured Runpod endpoint is not an adoptable TARS resource"
        )
    template_id = _resource_id(
        endpoint.get("templateId"), "bound template ID to adopt"
    )
    template = _by_id(inventory.templates, template_id)
    if template is None:
        raise RunpodReleaseError(
            "the Runpod endpoint to adopt is missing its bound template"
        )
    auth_id = _resource_id(
        template.get("containerRegistryAuthId"),
        "bound registry auth ID to adopt",
    )
    if _by_id(inventory.auths, auth_id) is None:
        raise RunpodReleaseError(
            "the Runpod template to adopt is missing its registry auth"
        )
    return StableResourceIDs(endpoint_id, template_id, auth_id)


def adopt_existing_stable_resources(
    client: ReleaseAPI,
    *,
    endpoint_id: str,
    timeout_seconds: float = ROLLOUT_TIMEOUT_SECONDS,
    poll_seconds: float = ROLLOUT_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> StableResourceIDs:
    """Rename one exact idle v1 chain in place; never create or delete."""

    ids = discover_adoptable_ids(client, endpoint_id=endpoint_id)
    endpoint, template, rest_endpoint = wait_for_adoptable_idle(
        client,
        ids=ids,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    image = str(template.get("imageName", ""))
    prior_version = _endpoint_version(rest_endpoint)
    template_changed = template.get("name") != STABLE_TEMPLATE_NAME
    if template_changed:
        client.update_template(
            ids.template_id,
            STABLE_TEMPLATE_NAME,
            image,
            ids.auth_id,
        )
        wait_for_stable_template_image(
            client,
            template_id=ids.template_id,
            image=image,
            auth_id=ids.auth_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            sleeper=sleeper,
            clock=clock,
        )
    if endpoint.get("name") != STABLE_ENDPOINT_NAME:
        client.rename_endpoint(ids.endpoint_id, ids.template_id)
    wait_for_adopted_topology(
        client,
        ids=ids,
        image=image,
        minimum_version=prior_version + (1 if template_changed else 0),
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        sleeper=sleeper,
        clock=clock,
    )
    return ids


def bootstrap_stable_resources(
    client: ReleaseAPI,
    *,
    release_sha: str,
    gpu_image: str,
    registry_username: str,
    registry_password: str,
) -> StableResourceIDs:
    """Explicit one-time creation path; never called by the main workflow."""

    _validate_release_image(release_sha, gpu_image)
    auth_name = stable_auth_name(registry_username, registry_password)
    initial = client.inventory()
    stable_auths = tuple(
        auth
        for auth in initial.auths
        if STABLE_AUTH_NAME.fullmatch(str(auth.get("name", ""))) is not None
    )
    if len(stable_auths) > 1:
        raise RunpodReleaseError(
            "Runpod contains multiple stable TARS registry auths"
        )
    if stable_auths and stable_auths[0].get("name") != auth_name:
        raise RunpodReleaseError(
            "stable Runpod registry credential differs; rotate it explicitly"
        )
    auth = _create_or_recover(
        client,
        resources=lambda inventory: inventory.auths,
        name=auth_name,
        description="stable registry auth",
        create=lambda: client.create_auth(
            auth_name, registry_username, registry_password
        ),
    )
    auth_id = verify_auth(auth, auth_name)
    template = _create_or_recover(
        client,
        resources=lambda inventory: inventory.templates,
        name=STABLE_TEMPLATE_NAME,
        description="stable template",
        create=lambda: client.create_template(
            STABLE_TEMPLATE_NAME, gpu_image, auth_id
        ),
    )
    template_id = _resource_id(template.get("id"), "stable template ID")
    client.update_template(
        template_id, STABLE_TEMPLATE_NAME, gpu_image, auth_id
    )
    wait_for_stable_template_image(
        client,
        template_id=template_id,
        image=gpu_image,
        auth_id=auth_id,
    )
    endpoint = _create_or_recover(
        client,
        resources=lambda inventory: inventory.endpoints,
        name=STABLE_ENDPOINT_NAME,
        description="stable endpoint",
        create=lambda: client.create_endpoint(STABLE_ENDPOINT_NAME, template_id),
    )
    endpoint_id = _resource_id(endpoint.get("id"), "stable endpoint ID")
    ids = StableResourceIDs(endpoint_id, template_id, auth_id)
    verify_stable_topology(client, ids=ids, expected_image=gpu_image)
    return ids


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


def _legacy_retirement_chains(
    client: ReleaseAPI, *, stable_ids: StableResourceIDs
) -> tuple[dict[str, str], ...]:
    """Build an exhaustive exact v1 inventory without mutating it."""

    stable_ids = _validate_stable_ids(stable_ids)
    verify_stable_topology(client, ids=stable_ids)
    inventory = client.inventory()
    legacy_endpoints = tuple(
        endpoint
        for endpoint in inventory.endpoints
        if endpoint.get("id") != stable_ids.endpoint_id
        and ENDPOINT_NAME.fullmatch(str(endpoint.get("name", ""))) is not None
    )
    legacy_templates = tuple(
        template
        for template in inventory.templates
        if template.get("id") != stable_ids.template_id
        and TEMPLATE_NAME.fullmatch(str(template.get("name", ""))) is not None
    )
    legacy_auths = tuple(
        auth
        for auth in inventory.auths
        if auth.get("id") != stable_ids.auth_id
        and AUTH_NAME.fullmatch(str(auth.get("name", ""))) is not None
    )
    chains: list[dict[str, str]] = []
    used_template_ids: set[str] = set()
    used_auth_ids: set[str] = set()
    for endpoint in legacy_endpoints:
        endpoint_id = _resource_id(endpoint.get("id"), "legacy endpoint ID")
        release_sha = _endpoint_release_sha(endpoint)
        if release_sha is None:
            raise RunpodReleaseError("legacy endpoint has an invalid release name")
        template_id = _resource_id(
            endpoint.get("templateId"), "legacy template ID"
        )
        template = _by_id(inventory.templates, template_id)
        if template is None:
            raise RunpodReleaseError("legacy endpoint is missing its template")
        variant = _retirement_template_variant(template, release_sha)
        if variant is None:
            raise RunpodReleaseError(
                "legacy template does not match its endpoint release"
            )
        auth_id = _resource_id(
            template.get("containerRegistryAuthId"), "legacy registry auth ID"
        )
        auth = _by_id(inventory.auths, auth_id)
        if auth is None or _auth_release_sha(auth) != release_sha:
            raise RunpodReleaseError(
                "legacy template is missing its exact registry auth"
            )
        if any(
            other.get("id") != endpoint_id
            and other.get("templateId") == template_id
            for other in inventory.endpoints
        ):
            raise RunpodReleaseError("legacy template is shared by another endpoint")
        workers_max = endpoint.get("workersMax")
        if workers_max not in (0, WORKERS_MAX):
            raise RunpodReleaseError(
                "legacy endpoint has an unexpected worker ceiling"
            )
        endpoint_name = _resource_name(
            endpoint.get("name"), ENDPOINT_NAME, "legacy endpoint name"
        )
        template_name = _resource_name(
            template.get("name"), TEMPLATE_NAME, "legacy template name"
        )
        auth_name = _resource_name(
            auth.get("name"), AUTH_NAME, "legacy registry auth name"
        )
        verify_retirement_endpoint(
            endpoint,
            expected_name=endpoint_name,
            template_id=template_id,
            workers_max=workers_max,
            variant=variant,
        )
        verify_endpoint_rest(
            client.read_endpoint(endpoint_id),
            endpoint_id=endpoint_id,
            expected_name=endpoint_name,
            template_id=template_id,
            workers_max=workers_max,
        )
        verify_retirement_template_rest(
            client.read_template(template_id),
            template_id=template_id,
            expected_name=template_name,
            variant=variant,
        )
        used_template_ids.add(template_id)
        used_auth_ids.add(auth_id)
        chains.append(
            {
                "auth_id": auth_id,
                "auth_name": auth_name,
                "endpoint_id": endpoint_id,
                "endpoint_name": endpoint_name,
                "release_sha": release_sha,
                "template_id": template_id,
                "template_name": template_name,
                "variant": variant,
            }
        )
    if {
        _resource_id(item.get("id"), "legacy template ID")
        for item in legacy_templates
    } != used_template_ids:
        raise RunpodReleaseError(
            "orphaned legacy templates require explicit operator inspection"
        )
    if {
        _resource_id(item.get("id"), "legacy registry auth ID")
        for item in legacy_auths
    } != used_auth_ids:
        raise RunpodReleaseError(
            "orphaned legacy registry auths require explicit operator inspection"
        )
    return tuple(sorted(chains, key=lambda chain: chain["endpoint_id"]))


def _legacy_plan_body(
    stable_ids: StableResourceIDs,
    chains: tuple[dict[str, str], ...],
) -> dict[str, Any]:
    stable_ids = _validate_stable_ids(stable_ids)
    return {
        "schema": 1,
        "stable": {
            "auth_id": stable_ids.auth_id,
            "endpoint_id": stable_ids.endpoint_id,
            "template_id": stable_ids.template_id,
        },
        "legacy_chains": [dict(chain) for chain in chains],
    }


def build_legacy_retirement_plan(
    client: ReleaseAPI,
    *,
    stable_ids: StableResourceIDs,
    output_path: Path,
) -> str:
    body = _legacy_plan_body(
        stable_ids,
        _legacy_retirement_chains(client, stable_ids=stable_ids),
    )
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    confirmation = hashlib.sha256(canonical).hexdigest()
    _write_private_json(
        output_path, {**body, "confirmation_sha256": confirmation}
    )
    return confirmation


def _read_legacy_retirement_plan(
    path: Path,
) -> tuple[StableResourceIDs, tuple[dict[str, str], ...], str]:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_size <= 0
            or metadata.st_size > 1_048_576
        ):
            raise RunpodReleaseError(
                "legacy retirement plan must be an owner-only regular file"
            )
        document = json.loads(path.read_text(encoding="utf-8"))
    except RunpodReleaseError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RunpodReleaseError(
            "cannot read the legacy retirement plan"
        ) from error
    if not isinstance(document, dict) or set(document) != {
        "confirmation_sha256",
        "legacy_chains",
        "schema",
        "stable",
    }:
        raise RunpodReleaseError("legacy retirement plan has an invalid schema")
    stable = document.get("stable")
    raw_chains = document.get("legacy_chains")
    confirmation = document.get("confirmation_sha256")
    if (
        document.get("schema") != 1
        or not isinstance(stable, dict)
        or set(stable) != {"auth_id", "endpoint_id", "template_id"}
        or not isinstance(raw_chains, list)
        or not isinstance(confirmation, str)
        or re.fullmatch(r"[0-9a-f]{64}", confirmation) is None
    ):
        raise RunpodReleaseError("legacy retirement plan has invalid fields")
    ids = _validate_stable_ids(
        StableResourceIDs(
            endpoint_id=stable.get("endpoint_id"),
            template_id=stable.get("template_id"),
            auth_id=stable.get("auth_id"),
        )
    )
    expected_keys = {
        "auth_id",
        "auth_name",
        "endpoint_id",
        "endpoint_name",
        "release_sha",
        "template_id",
        "template_name",
        "variant",
    }
    chains: list[dict[str, str]] = []
    for raw in raw_chains:
        if (
            not isinstance(raw, dict)
            or set(raw) != expected_keys
            or any(not isinstance(value, str) for value in raw.values())
        ):
            raise RunpodReleaseError(
                "legacy retirement plan contains an invalid chain"
            )
        chain = dict(raw)
        _resource_id(chain["endpoint_id"], "legacy endpoint ID")
        _resource_id(chain["template_id"], "legacy template ID")
        _resource_id(chain["auth_id"], "legacy registry auth ID")
        _resource_name(
            chain["endpoint_name"], ENDPOINT_NAME, "legacy endpoint name"
        )
        _resource_name(
            chain["template_name"], TEMPLATE_NAME, "legacy template name"
        )
        _resource_name(chain["auth_name"], AUTH_NAME, "legacy registry auth name")
        if SHA.fullmatch(chain["release_sha"]) is None or chain[
            "variant"
        ] not in ("current", "legacy"):
            raise RunpodReleaseError(
                "legacy retirement plan contains an invalid chain"
            )
        chains.append(chain)
    ordered = tuple(sorted(chains, key=lambda chain: chain["endpoint_id"]))
    if tuple(chains) != ordered:
        raise RunpodReleaseError("legacy retirement plan is not canonical")
    body = _legacy_plan_body(ids, ordered)
    actual_confirmation = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if actual_confirmation != confirmation:
        raise RunpodReleaseError(
            "legacy retirement plan confirmation does not match its inventory"
        )
    return ids, ordered, confirmation


def _verify_planned_legacy_inventory(
    client: ReleaseAPI,
    *,
    stable_ids: StableResourceIDs,
    chains: tuple[dict[str, str], ...],
) -> tuple[
    Inventory,
    tuple[tuple[dict[str, str], dict[str, Any], dict[str, Any]], ...],
]:
    verify_stable_topology(client, ids=stable_ids)
    inventory = client.inventory()
    planned_endpoint_ids = {chain["endpoint_id"] for chain in chains}
    planned_template_ids = {chain["template_id"] for chain in chains}
    planned_auth_ids = {chain["auth_id"] for chain in chains}
    unexpected = [
        resource
        for resource in inventory.endpoints
        if ENDPOINT_NAME.fullmatch(str(resource.get("name", ""))) is not None
        and resource.get("id") not in planned_endpoint_ids
    ]
    unexpected.extend(
        resource
        for resource in inventory.templates
        if TEMPLATE_NAME.fullmatch(str(resource.get("name", ""))) is not None
        and resource.get("id") not in planned_template_ids
    )
    unexpected.extend(
        resource
        for resource in inventory.auths
        if AUTH_NAME.fullmatch(str(resource.get("name", ""))) is not None
        and resource.get("id") not in planned_auth_ids
        and resource.get("id") != stable_ids.auth_id
    )
    if unexpected:
        raise RunpodReleaseError(
            "legacy Runpod inventory changed after the retirement plan was made"
        )
    active: list[tuple[dict[str, str], dict[str, Any], dict[str, Any]]] = []
    for chain in chains:
        endpoint = _by_id(inventory.endpoints, chain["endpoint_id"])
        template = _by_id(inventory.templates, chain["template_id"])
        auth = _by_id(inventory.auths, chain["auth_id"])
        if endpoint is None:
            if template is None and auth is None:
                continue
            if template is not None:
                _expect_equal(
                    template.get("name"),
                    chain["template_name"],
                    "planned legacy template name",
                )
                if any(
                    other.get("templateId") == chain["template_id"]
                    for other in inventory.endpoints
                ):
                    raise RunpodReleaseError(
                        "planned legacy template became shared"
                    )
            if auth is not None:
                _expect_equal(
                    auth.get("name"),
                    chain["auth_name"],
                    "planned legacy registry auth name",
                )
            active.append((chain, {}, {}))
            continue
        if template is None or auth is None:
            raise RunpodReleaseError(
                "planned legacy endpoint lost a required dependency"
            )
        _expect_equal(
            endpoint.get("name"),
            chain["endpoint_name"],
            "planned legacy endpoint name",
        )
        _expect_equal(
            endpoint.get("templateId"),
            chain["template_id"],
            "planned legacy endpoint template",
        )
        _expect_equal(
            template.get("name"),
            chain["template_name"],
            "planned legacy template name",
        )
        _expect_equal(
            template.get("containerRegistryAuthId"),
            chain["auth_id"],
            "planned legacy template registry auth",
        )
        _expect_equal(
            auth.get("name"),
            chain["auth_name"],
            "planned legacy registry auth name",
        )
        workers_max = endpoint.get("workersMax")
        if workers_max not in (0, WORKERS_MAX):
            raise RunpodReleaseError(
                "planned legacy endpoint has an unexpected worker ceiling"
            )
        verify_retirement_endpoint(
            endpoint,
            expected_name=chain["endpoint_name"],
            template_id=chain["template_id"],
            workers_max=workers_max,
            variant=chain["variant"],
        )
        rest_endpoint = client.read_endpoint(chain["endpoint_id"])
        verify_endpoint_rest(
            rest_endpoint,
            endpoint_id=chain["endpoint_id"],
            expected_name=chain["endpoint_name"],
            template_id=chain["template_id"],
            workers_max=workers_max,
        )
        verify_retirement_template_rest(
            client.read_template(chain["template_id"]),
            template_id=chain["template_id"],
            expected_name=chain["template_name"],
            variant=chain["variant"],
        )
        active.append((chain, endpoint, rest_endpoint))
    return inventory, tuple(active)


def retire_legacy_resources(
    client: ReleaseAPI,
    *,
    plan_path: Path,
    confirmation_sha256: str,
) -> int:
    """Apply an exact operator-approved plan; safe to retry after interruption."""

    stable_ids, chains, confirmation = _read_legacy_retirement_plan(plan_path)
    if confirmation_sha256 != confirmation:
        raise RunpodReleaseError(
            "legacy retirement confirmation does not match the exact plan"
        )
    _, remaining = _verify_planned_legacy_inventory(
        client, stable_ids=stable_ids, chains=chains
    )
    # Prove every still-live endpoint is idle before the first mutation.
    for chain, endpoint, rest_endpoint in remaining:
        if not endpoint:
            continue
        health = client.read_endpoint_health(chain["endpoint_id"])
        if health.has_active_work or _active_workers(endpoint, rest_endpoint):
            raise RunpodReleaseError(
                "legacy retirement refused queued, running, or active worker work"
            )

    retired = 0
    for chain in chains:
        inventory, _remaining = _verify_planned_legacy_inventory(
            client, stable_ids=stable_ids, chains=chains
        )
        endpoint = _by_id(inventory.endpoints, chain["endpoint_id"])
        template = _by_id(inventory.templates, chain["template_id"])
        auth = _by_id(inventory.auths, chain["auth_id"])
        if endpoint is not None:
            rest_endpoint = client.read_endpoint(chain["endpoint_id"])
            if (
                client.read_endpoint_health(chain["endpoint_id"]).has_active_work
                or _active_workers(endpoint, rest_endpoint)
            ):
                raise RunpodReleaseError(
                    "legacy endpoint received work during retirement"
                )
            if endpoint.get("workersMax") != 0:
                client.zero_endpoint(endpoint)
            refreshed = client.inventory()
            endpoint = _by_id(refreshed.endpoints, chain["endpoint_id"])
            if endpoint is None:
                raise RunpodReleaseError(
                    "legacy endpoint disappeared before deletion was confirmed"
                )
            rest_endpoint = client.read_endpoint(chain["endpoint_id"])
            if (
                client.read_endpoint_health(chain["endpoint_id"]).has_active_work
                or _active_workers(endpoint, rest_endpoint)
            ):
                raise RunpodReleaseError(
                    "legacy endpoint received work after workers were zeroed"
                )
            client.delete_endpoint(chain["endpoint_id"])
            retired += 1
        inventory = client.inventory()
        template = _by_id(inventory.templates, chain["template_id"])
        if template is not None:
            if any(
                other.get("templateId") == chain["template_id"]
                for other in inventory.endpoints
            ):
                raise RunpodReleaseError(
                    "planned legacy template became shared during retirement"
                )
            client.delete_template(chain["template_name"])
        inventory = client.inventory()
        auth = _by_id(inventory.auths, chain["auth_id"])
        if auth is not None:
            if any(
                other.get("containerRegistryAuthId") == chain["auth_id"]
                for other in inventory.templates
            ):
                raise RunpodReleaseError(
                    "planned legacy registry auth became shared during retirement"
                )
            client.delete_auth(chain["auth_id"])
    _verify_planned_legacy_inventory(
        client, stable_ids=stable_ids, chains=chains
    )
    return retired


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


def append_output(path: Path, name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise RunpodReleaseError("GitHub output contains an invalid value")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_application = commands.add_parser("prepare-application")
    prepare_application.add_argument("--release-sha", required=True)
    prepare_application.add_argument("--gpu-image", required=True)
    prepare_application.add_argument("--endpoint-id", required=True)
    prepare_application.add_argument("--boundary-file", type=Path, required=True)
    verify_application = commands.add_parser("verify-application")
    verify_application.add_argument("--api-key-file", type=Path, required=True)
    verify_application.add_argument("--boundary-file", type=Path, required=True)
    verify_application.add_argument(
        "--endpoint-id-file", type=Path, required=True
    )
    verify_application.add_argument(
        "--template-id-file", type=Path, required=True
    )
    verify_application.add_argument("--auth-id-file", type=Path, required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--release-sha", required=True)
    prepare.add_argument("--gpu-image", required=True)
    prepare.add_argument("--api-key-file", type=Path, required=True)
    prepare.add_argument("--endpoint-id-file", type=Path, required=True)
    prepare.add_argument("--template-id-file", type=Path, required=True)
    prepare.add_argument("--auth-id-file", type=Path, required=True)
    prepare.add_argument("--prior-release-sha")
    prepare.add_argument("--prior-gpu-image")
    prepare.add_argument("--greenfield", action="store_true")
    prepare.add_argument("--receipt-file", type=Path, required=True)
    stage = commands.add_parser("stage")
    stage.add_argument("--api-key-file", type=Path, required=True)
    stage.add_argument("--receipt-file", type=Path, required=True)
    stage.add_argument("--github-output", type=Path, required=True)
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--api-key-file", type=Path, required=True)
    rollback.add_argument("--receipt-file", type=Path, required=True)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--api-key-file", type=Path, required=True)
    finalize.add_argument("--receipt-file", type=Path, required=True)
    verify_target = commands.add_parser("verify-target")
    verify_target.add_argument("--api-key-file", type=Path, required=True)
    verify_target.add_argument("--receipt-file", type=Path, required=True)
    describe = commands.add_parser("describe")
    describe.add_argument("--receipt-file", type=Path, required=True)
    describe.add_argument("--github-output", type=Path, required=True)
    describe_boundary = commands.add_parser("describe-boundary")
    describe_boundary.add_argument("--boundary-file", type=Path, required=True)
    describe_boundary.add_argument("--github-output", type=Path, required=True)
    migrate = commands.add_parser("migrate")
    migrate.add_argument("--api-key-file", type=Path, required=True)
    migrate.add_argument("--endpoint-id-file", type=Path, required=True)
    migrate.add_argument("--ids-output", type=Path, required=True)
    migrate.add_argument(
        "--confirm-adopt-existing-resources",
        action="store_true",
        help="required acknowledgement that this is a one-time operator action",
    )
    retirement_plan = commands.add_parser("retire-legacy-plan")
    retirement_plan.add_argument("--api-key-file", type=Path, required=True)
    retirement_plan.add_argument("--endpoint-id-file", type=Path, required=True)
    retirement_plan.add_argument("--template-id-file", type=Path, required=True)
    retirement_plan.add_argument("--auth-id-file", type=Path, required=True)
    retirement_plan.add_argument("--plan-output", type=Path, required=True)
    retire_legacy = commands.add_parser("retire-legacy")
    retire_legacy.add_argument("--api-key-file", type=Path, required=True)
    retire_legacy.add_argument("--plan-file", type=Path, required=True)
    retire_legacy.add_argument("--confirmation-sha256", required=True)
    bootstrap = commands.add_parser("bootstrap")
    bootstrap.add_argument("--release-sha", required=True)
    bootstrap.add_argument("--gpu-image", required=True)
    bootstrap.add_argument("--api-key-file", type=Path, required=True)
    bootstrap.add_argument("--registry-username-file", type=Path, required=True)
    bootstrap.add_argument("--registry-password-file", type=Path, required=True)
    bootstrap.add_argument("--ids-output", type=Path, required=True)
    bootstrap.add_argument(
        "--confirm-create-stable-resources",
        action="store_true",
        help="required acknowledgement that this is a one-time operator action",
    )
    args = parser.parse_args()
    try:
        if args.command == "prepare-application":
            write_application_baseline(
                args.boundary_file,
                ApplicationRolloutBaseline(
                    release_sha=args.release_sha,
                    gpu_image=args.gpu_image,
                    endpoint_id=args.endpoint_id,
                ),
            )
            print("prepared the durable pre-drain application boundary")
            return
        if args.command == "describe-boundary":
            try:
                baseline = read_application_baseline(args.boundary_file)
            except RunpodReleaseError as application_error:
                try:
                    receipt = read_rollout_receipt(args.boundary_file)
                except RunpodReleaseError:
                    raise RunpodReleaseError(
                        "durable Runpod boundary is neither an application "
                        "baseline nor a rollout receipt"
                    ) from application_error
                outputs = (
                    ("kind", "rollout"),
                    ("baseline", receipt.baseline),
                    ("endpoint_id", receipt.endpoint_id),
                    ("app_gpu_image", ""),
                    ("app_release_sha", ""),
                    ("replicas", ""),
                    ("prior_gpu_image", receipt.prior_app_gpu_image or ""),
                    ("prior_release_sha", receipt.prior_release_sha or ""),
                    ("release_sha", receipt.release_sha),
                    ("target_image", receipt.target_image),
                )
            else:
                outputs = (
                    ("kind", "application"),
                    ("baseline", "existing"),
                    ("endpoint_id", baseline.endpoint_id),
                    ("app_gpu_image", baseline.gpu_image),
                    ("app_release_sha", baseline.release_sha),
                    ("replicas", str(baseline.replicas)),
                    ("prior_gpu_image", ""),
                    ("prior_release_sha", ""),
                    ("release_sha", ""),
                    ("target_image", ""),
                )
            for name, value in outputs:
                append_output(args.github_output, name, value)
            print("validated the durable Runpod boundary")
            return
        if args.command == "describe":
            receipt = read_rollout_receipt(args.receipt_file)
            for name, value in (
                ("baseline", receipt.baseline),
                ("endpoint_id", receipt.endpoint_id),
                ("prior_gpu_image", receipt.prior_app_gpu_image or ""),
                ("prior_release_sha", receipt.prior_release_sha or ""),
                ("release_sha", receipt.release_sha),
                ("target_image", receipt.target_image),
            ):
                append_output(args.github_output, name, value)
            print("validated the durable Runpod rollout boundary")
            return
        if args.command in (
            "prepare",
            "stage",
            "rollback",
            "finalize",
            "verify-application",
            "verify-target",
            "migrate",
            "bootstrap",
            "retire-legacy-plan",
            "retire-legacy",
        ):
            api_key = read_secret(args.api_key_file, "RUNPOD_API_KEY")
            client = RunpodClient(api_key)
            if args.command == "verify-application":
                verify_application_generation(
                    client,
                    baseline=read_application_baseline(args.boundary_file),
                    ids=read_stable_ids(
                        args.endpoint_id_file,
                        args.template_id_file,
                        args.auth_id_file,
                    ),
                )
                print(
                    "verified the application baseline against the exact "
                    "Runpod generation"
                )
                return
            if args.command == "prepare":
                ids = read_stable_ids(
                    args.endpoint_id_file,
                    args.template_id_file,
                    args.auth_id_file,
                )
                if args.greenfield and (
                    args.prior_release_sha is not None
                    or args.prior_gpu_image is not None
                ):
                    raise RunpodReleaseError(
                        "--greenfield cannot be combined with prior app inputs"
                    )
                if not args.greenfield and (
                    args.prior_release_sha is None
                    or args.prior_gpu_image is None
                ):
                    raise RunpodReleaseError(
                        "existing preparation requires --prior-release-sha "
                        "and --prior-gpu-image"
                    )
                prepare_stable_release(
                    client,
                    ids=ids,
                    release_sha=args.release_sha,
                    gpu_image=args.gpu_image,
                    prior_release_sha=args.prior_release_sha,
                    prior_app_gpu_image=args.prior_gpu_image,
                    greenfield=args.greenfield,
                    receipt_path=args.receipt_file,
                )
                print("prepared the coupled app/provider rollback boundary")
                return
            if args.command == "stage":
                receipt = stage_prepared_stable_release(
                    client,
                    receipt=read_rollout_receipt(args.receipt_file),
                    receipt_path=args.receipt_file,
                )
                append_output(
                    args.github_output, "endpoint_id", receipt.endpoint_id
                )
                append_output(
                    args.github_output,
                    "changed",
                    "true" if receipt.mode == "update" else "false",
                )
                append_output(
                    args.github_output,
                    "endpoint_version",
                    str(receipt.target_version),
                )
                print("staged the immutable image on the stable Runpod template")
                return
            if args.command == "rollback":
                rollback_stable_release(
                    client,
                    receipt=read_rollout_receipt(args.receipt_file),
                )
                print("restored the prior stable Runpod template image")
                return
            if args.command == "finalize":
                finalize_stable_release(
                    client,
                    receipt=read_rollout_receipt(args.receipt_file),
                )
                print("verified no active superseded Runpod worker remains")
                return
            if args.command == "verify-target":
                verify_receipt_target(
                    client,
                    receipt=read_rollout_receipt(args.receipt_file),
                )
                print("verified the exact idle Runpod receipt target")
                return
            if args.command == "migrate":
                if not args.confirm_adopt_existing_resources:
                    raise RunpodReleaseError(
                        "explicit stable migration requires "
                        "--confirm-adopt-existing-resources"
                    )
                ids = adopt_existing_stable_resources(
                    client,
                    endpoint_id=read_stable_id(
                        args.endpoint_id_file,
                        "existing RUNPOD_ENDPOINT_ID",
                    ),
                )
                _write_private_json(
                    args.ids_output,
                    {
                        "RUNPOD_ENDPOINT_ID": ids.endpoint_id,
                        "RUNPOD_TEMPLATE_ID": ids.template_id,
                        "RUNPOD_REGISTRY_AUTH_ID": ids.auth_id,
                    },
                )
                print(
                    "adopted the existing Runpod topology in place; "
                    "the emitted Infisical IDs are unchanged"
                )
                return
            if args.command == "retire-legacy-plan":
                ids = read_stable_ids(
                    args.endpoint_id_file,
                    args.template_id_file,
                    args.auth_id_file,
                )
                confirmation = build_legacy_retirement_plan(
                    client,
                    stable_ids=ids,
                    output_path=args.plan_output,
                )
                print(
                    "legacy retirement plan created; confirmation SHA-256: "
                    f"{confirmation}"
                )
                return
            if args.command == "retire-legacy":
                deleted = retire_legacy_resources(
                    client,
                    plan_path=args.plan_file,
                    confirmation_sha256=args.confirmation_sha256,
                )
                print(f"retired {deleted} legacy TARS Runpod release(s)")
                return
            if not args.confirm_create_stable_resources:
                raise RunpodReleaseError(
                    "explicit stable bootstrap requires "
                    "--confirm-create-stable-resources"
                )
            ids = bootstrap_stable_resources(
                client,
                release_sha=args.release_sha,
                gpu_image=args.gpu_image,
                registry_username=read_secret(
                    args.registry_username_file, "DOCR_READ_USERNAME"
                ),
                registry_password=read_secret(
                    args.registry_password_file, "DOCR_READ_PASSWORD"
                ),
            )
            _write_private_json(
                args.ids_output,
                {
                    "RUNPOD_ENDPOINT_ID": ids.endpoint_id,
                    "RUNPOD_TEMPLATE_ID": ids.template_id,
                    "RUNPOD_REGISTRY_AUTH_ID": ids.auth_id,
                },
            )
            print(
                "created or verified the one-time stable Runpod topology; "
                "store the emitted IDs in Infisical /deployment"
            )
            return
    except RunpodReleaseError as error:
        parser.exit(1, f"TARS Runpod release failed: {error}\n")


if __name__ == "__main__":
    main()
