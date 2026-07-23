import base64
import email.message
import io
import json
import subprocess
import tempfile
import traceback
import unittest
import urllib.error
import urllib.parse
import urllib.request
import urllib.response
import tars_runpod_release
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from tars_lock_outputs import (
    release_values,
    validate_action_versions,
    values,
    write_release_environment,
)
from tars_payload import load_payload
from tars_registry_release import (
    RegistryReleaseError,
    inspect_digest,
    resolve_release,
)
from tars_runner_secrets import (
    BUILD_KEYS,
    DEPLOY_KEYS,
    DEPLOYMENT_KEYS,
    RUNTIME_KEYS,
    RunnerSecretError,
    capture_build,
    capture_deploy,
)
from tars_runpod_release import (
    ENDPOINT_TIMEOUT_MS,
    GPU_POOL_SELECTOR,
    Inventory,
    RunpodClient,
    RunpodReleaseError,
    _NoRedirectHandler,
    ensure_release,
    prune_releases,
    read_secret,
    release_names,
)
from tars_tada_bundle import BundleFetchError, fetch, validate_bundle_shape, validate_inputs


class PayloadTest(unittest.TestCase):
    def write_event(self, payload: dict) -> Path:
        handle = tempfile.NamedTemporaryFile("w", delete=False)
        json.dump({"client_payload": payload}, handle)
        handle.close()
        return Path(handle.name)

    def test_accepts_exact_deploy_payload(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
            }
        )
        self.assertEqual(load_payload(event, "deploy")["sha"], "a" * 40)

    def test_accepts_legacy_deploy_payload_during_trigger_rollout(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
                "ref": "refs/heads/main",
                "pr": None,
                "lock_file": "release/lock.json",
                "source_event": "push",
            }
        )
        self.assertEqual(load_payload(event, "deploy")["sha"], "a" * 40)

    def test_rejects_untrusted_repository(self) -> None:
        event = self.write_event(
            {
                "repo": "attacker/TARS",
                "sha": "a" * 40,
            }
        )
        with self.assertRaisesRegex(ValueError, "repository"):
            load_payload(event, "deploy")

    def test_rejects_deploy_payload_with_extra_keys(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
                "image": "registry.example/mutable:latest",
            }
        )
        with self.assertRaisesRegex(ValueError, "unexpected image"):
            load_payload(event, "deploy")

    def test_rejects_invalid_deploy_sha(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "main",
            }
        )
        with self.assertRaisesRegex(ValueError, "dispatch sha"):
            load_payload(event, "deploy")

    def test_accepts_exact_pull_request_payload(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "head_sha": "b" * 40,
                "pr": 12,
            }
        )
        self.assertEqual(load_payload(event, "ci")["pr"], 12)

    def test_accepts_legacy_pull_request_payload_during_trigger_rollout(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": None,
                "head_sha": "b" * 40,
                "ref": "refs/pull/12/merge",
                "pr": 12,
                "base_ref": "main",
                "base_sha": "c" * 40,
                "lock_file": "release/lock.json",
                "source_event": "pull_request_target",
            }
        )
        self.assertEqual(load_payload(event, "ci")["head_sha"], "b" * 40)

    def test_rejects_invalid_pull_request_head(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "head_sha": None,
                "pr": 12,
            }
        )
        with self.assertRaisesRegex(ValueError, "head_sha"):
            load_payload(event, "ci")


class LockOutputTest(unittest.TestCase):
    def test_exports_all_build_and_runtime_images(self) -> None:
        images = {
            key: {"reference": f"registry.example/{key}@sha256:{'a' * 64}"}
            for key in (
                "go",
                "api_runtime",
                "oras",
                "uv",
                "python",
                "bundle",
                "garage_upstream",
                "otel_upstream",
                "support_alpine",
                "nginx",
                "postgres",
            )
        }
        lock_data = {
            "registry": "registry.example/tars",
            "target_platform": "linux/amd64",
            "images": images,
            "tada": {
                "repository": "Lascade-Co/tada",
                "oci": f"ghcr.io/lascade/tada@sha256:{'b' * 64}",
                "revision": "c" * 40,
            },
            "tools": {
                "go": "1.26.5",
                "doctl": "1.163.0",
                "opentofu": "1.12.4",
                "docker_buildx": "0.34.1",
            },
        }
        handle = tempfile.NamedTemporaryFile("w", delete=False)
        json.dump(lock_data, handle)
        handle.close()
        output = values(Path(handle.name))
        self.assertEqual(output["target_platform"], "linux/amd64")
        self.assertEqual(output["go_version"], "1.26.5")
        self.assertEqual(output["opentofu_version"], "1.12.4")
        self.assertEqual(output["docker_buildx_version"], "0.34.1")
        self.assertIn("@sha256:", output["postgres_image"])


class RegistryReleaseTest(unittest.TestCase):
    REGISTRY = "registry.digitalocean.com/lascade/tars"
    RELEASE_SHA = "a" * 40
    DIGESTS = {
        component: "sha256:" + str(index) * 64
        for index, component in enumerate(
            ("api", "dispatcher", "gpu", "garage", "otel"), start=1
        )
    }

    @staticmethod
    def manifest(digest: str) -> str:
        return json.dumps(
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": digest,
                "size": 123,
            }
        )

    def test_inspect_returns_the_registry_manifest_digest(self) -> None:
        runner = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=self.manifest(self.DIGESTS["api"]),
                stderr="",
            )
        )
        digest = inspect_digest(
            self.REGISTRY + ":api-sha-" + self.RELEASE_SHA,
            Path("/private/docker-config"),
            runner=runner,
        )
        self.assertEqual(digest, self.DIGESTS["api"])
        command = runner.call_args.args[0]
        self.assertEqual(
            command[:5],
            ["docker", "buildx", "imagetools", "inspect", "--format"],
        )
        self.assertEqual(
            runner.call_args.kwargs["env"]["DOCKER_CONFIG"],
            "/private/docker-config",
        )

    def test_inspect_treats_only_a_missing_manifest_as_absent(self) -> None:
        missing = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="manifest unknown"
            )
        )
        self.assertIsNone(
            inspect_digest(
                self.REGISTRY + ":api-sha-" + self.RELEASE_SHA,
                Path("/private/docker-config"),
                runner=missing,
            )
        )
        self.assertEqual(missing.call_count, 1)

        transient = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="temporary registry failure"
            )
        )
        sleeper = mock.Mock()
        with self.assertRaisesRegex(RegistryReleaseError, "inspection failed"):
            inspect_digest(
                self.REGISTRY + ":api-sha-" + self.RELEASE_SHA,
                Path("/private/docker-config"),
                runner=transient,
                sleeper=sleeper,
            )
        self.assertEqual(transient.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in sleeper.call_args_list],
            [1, 2],
        )

        for unsafe_failure in (
            "docker-credential-helper: executable file not found",
            "repository does not exist or may require authorization",
        ):
            with self.subTest(failure=unsafe_failure):
                failed_auth_or_tool = mock.Mock(
                    return_value=subprocess.CompletedProcess(
                        args=[],
                        returncode=1,
                        stdout="",
                        stderr=unsafe_failure,
                    )
                )
                with self.assertRaisesRegex(RegistryReleaseError, "inspection failed"):
                    inspect_digest(
                        self.REGISTRY + ":api-sha-" + self.RELEASE_SHA,
                        Path("/private/docker-config"),
                        runner=failed_auth_or_tool,
                        sleeper=lambda _delay: None,
                    )
                self.assertEqual(failed_auth_or_tool.call_count, 3)

    def test_inspect_rejects_malformed_success_without_retrying(self) -> None:
        runner = mock.Mock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout='{"mediaType":"application/vnd.oci.image.manifest.v1+json"}',
                stderr="",
            )
        )
        with self.assertRaisesRegex(RegistryReleaseError, "manifest"):
            inspect_digest(
                self.REGISTRY + ":api-sha-" + self.RELEASE_SHA,
                Path("/private/docker-config"),
                runner=runner,
            )
        self.assertEqual(runner.call_count, 1)

    def test_probe_supports_partial_tags_and_verify_requires_every_tag(self) -> None:
        available = {
            "api": self.DIGESTS["api"],
            "gpu": self.DIGESTS["gpu"],
        }

        def inspector(reference: str, _docker_config: Path) -> str | None:
            component = reference.rsplit(":", 1)[1].split("-sha-", 1)[0]
            return available.get(component)

        probed = resolve_release(
            registry=self.REGISTRY,
            release_sha=self.RELEASE_SHA,
            docker_config=Path("/private/docker-config"),
            allow_missing=True,
            inspector=inspector,
        )
        self.assertEqual(probed["api_exists"], "true")
        self.assertEqual(probed["dispatcher_exists"], "false")
        self.assertEqual(probed["gpu_digest"], self.DIGESTS["gpu"])
        with self.assertRaisesRegex(RegistryReleaseError, "dispatcher"):
            resolve_release(
                registry=self.REGISTRY,
                release_sha=self.RELEASE_SHA,
                docker_config=Path("/private/docker-config"),
                allow_missing=False,
                inspector=inspector,
            )

    def test_verify_reuses_exact_digests_and_rejects_a_build_mismatch(self) -> None:
        def inspector(reference: str, _docker_config: Path) -> str:
            component = reference.rsplit(":", 1)[1].split("-sha-", 1)[0]
            return self.DIGESTS[component]

        resolved = resolve_release(
            registry=self.REGISTRY,
            release_sha=self.RELEASE_SHA,
            docker_config=Path("/private/docker-config"),
            allow_missing=False,
            expected_digests=self.DIGESTS,
            inspector=inspector,
        )
        self.assertEqual(
            {component: resolved[f"{component}_digest"] for component in self.DIGESTS},
            self.DIGESTS,
        )
        with self.assertRaisesRegex(RegistryReleaseError, "gpu"):
            resolve_release(
                registry=self.REGISTRY,
                release_sha=self.RELEASE_SHA,
                docker_config=Path("/private/docker-config"),
                allow_missing=False,
                expected_digests={
                    **self.DIGESTS,
                    "gpu": "sha256:" + "f" * 64,
                },
                inspector=inspector,
            )


class LockReleaseEnvironmentTest(unittest.TestCase):
    def test_writes_data_only_release_environment(self) -> None:
        images = {
            "nginx": {"reference": f"docker.io/library/nginx@sha256:{'a' * 64}"},
            "postgres": {"reference": f"docker.io/library/postgres@sha256:{'b' * 64}"},
        }
        lock_data = {
            "registry": "registry.digitalocean.com/lascade/tars",
            "images": images,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock = root / "lock.json"
            lock.write_text(json.dumps(lock_data), encoding="utf-8")
            environment = release_values(
                lock,
                {
                    "api": "sha256:" + "1" * 64,
                    "dispatcher": "sha256:" + "2" * 64,
                    "gpu": "sha256:" + "3" * 64,
                    "garage": "sha256:" + "4" * 64,
                    "otel": "sha256:" + "5" * 64,
                },
                "endpoint-test",
            )
            release = root / "release.env"
            write_release_environment(release, environment)
            rendered = release.read_text(encoding="utf-8")
            self.assertIn(
                "TARS_API_IMAGE=registry.digitalocean.com/lascade/tars@sha256:", rendered
            )
            self.assertIn(
                "TADA_ALLOWED_HOSTS=dashboard.lascade.com,api.maptiler.com,"
                "server.arcgisonline.com,firebasestorage.googleapis.com",
                rendered,
            )
            self.assertIn("TARS_DISPATCHER_IMAGE=", rendered)
            self.assertIn("TARS_GPU_IMAGE=", rendered)
            self.assertIn("TARS_RUNPOD_ENDPOINT_ID=endpoint-test", rendered)
            self.assertIn("DISPATCHER_STOP_GRACE_PERIOD=2h15m", rendered)
            self.assertIn("TADA_ESTIMATE_HD_REALTIME_FACTOR=5.5", rendered)
            self.assertIn("TADA_ESTIMATE_4K_REALTIME_FACTOR=4.0", rendered)
            self.assertIn("TADA_ESTIMATE_MODEL_VERSION=runpod-ampere16-v1", rendered)
            self.assertNotIn("TOKEN", rendered)

    def test_rejects_noncanonical_built_digest(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            json.dump({"registry": "registry.example/tars", "images": {}}, handle)
        with self.assertRaisesRegex(ValueError, "lowercase hex"):
            release_values(
                Path(handle.name),
                {
                    "api": "latest",
                    "dispatcher": "latest",
                    "gpu": "latest",
                    "garage": "latest",
                    "otel": "latest",
                },
                "endpoint-test",
            )

    def test_central_action_versions_must_match_source_release_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock = root / "lock.json"
            workflow = root / "workflow.yml"
            lock.write_text(
                json.dumps(
                    {
                        "actions": {
                            "checkout": {
                                "repository": "actions/checkout",
                                "version": "v7.0.0",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            workflow.write_text(
                "steps:\n  - uses: actions/checkout@v7.0.0\n",
                encoding="utf-8",
            )
            validate_action_versions(lock, [workflow])
            workflow.write_text(
                "steps:\n  - uses: actions/checkout@v6\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "does not match locked version"):
                validate_action_versions(lock, [workflow])


class TrustedBundleFetchTest(unittest.TestCase):
    def test_attaches_token_stdin_only_to_the_oras_login_container(self) -> None:
        oras_image = "ghcr.io/oras-project/oras@sha256:" + "a" * 64
        tada_oci = "ghcr.io/lascade-co/tada-wheel@sha256:" + "b" * 64
        calls: list[tuple[list[str], bytes | None]] = []

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            token_file = root / "ghcr-token"
            token_file.write_bytes(b"test-token")
            output = root / "bundle"

            def fake_run(
                command: list[str], *, stdin: bytes | None = None
            ) -> subprocess.CompletedProcess[bytes]:
                calls.append((command, stdin))
                if "pull" in command:
                    for name in (
                        "SHA256SUMS",
                        "pylock.toml",
                        "build-metadata.json",
                        "tada-0.1.0-py3-none-any.whl",
                    ):
                        (output / name).write_text("data", encoding="utf-8")
                return subprocess.CompletedProcess(command, 0, b"", b"")

            with mock.patch("tars_tada_bundle.run", side_effect=fake_run):
                fetch(oras_image, tada_oci, "github-actions", token_file, output)

        self.assertEqual(len(calls), 3)
        login_command, login_stdin = calls[0]
        self.assertLess(
            login_command.index("--interactive"), login_command.index(oras_image)
        )
        self.assertEqual(login_stdin, b"test-token")
        for command, stdin in calls[1:]:
            self.assertNotIn("--interactive", command)
            self.assertIsNone(stdin)
        for command, _stdin in calls:
            self.assertNotIn("test-token", command)

    def test_accepts_only_locked_official_refs_and_exact_bundle_shape(self) -> None:
        validate_inputs(
            "ghcr.io/oras-project/oras@sha256:" + "a" * 64,
            "ghcr.io/lascade-co/tada-wheel@sha256:" + "b" * 64,
            "github-actions",
        )
        validate_inputs(
            "ghcr.io/oras-project/oras@sha256:" + "a" * 64,
            "ghcr.io/lascade-co/tada-wheel@sha256:" + "b" * 64,
            "lascade-actions[bot]",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (
                "SHA256SUMS",
                "pylock.toml",
                "build-metadata.json",
                "tada-0.1.0-py3-none-any.whl",
            ):
                (root / name).write_text("data", encoding="utf-8")
            validate_bundle_shape(root)
            (root / "unexpected.txt").write_text("data", encoding="utf-8")
            with self.assertRaisesRegex(BundleFetchError, "exact four-file"):
                validate_bundle_shape(root)

    def test_rejects_mutable_tada_or_oras_references(self) -> None:
        with self.assertRaises(BundleFetchError):
            validate_inputs(
                "ghcr.io/oras-project/oras:latest",
                "ghcr.io/lascade-co/tada-wheel@sha256:" + "b" * 64,
                "github-actions",
            )


class RunnerSecretsTest(unittest.TestCase):
    def environment(self) -> dict[str, str]:
        wireguard = base64.b64encode(
            b"[Interface]\nPrivateKey = wireguard-secret\n"
        ).decode()
        return {
            "DOCR_READ_USERNAME": "registry-reader",
            "DOCR_READ_PASSWORD": "registry password with spaces",
            "DOCR_WRITE_TOKEN": "write-token",
            "DEPLOY_SSH_HOST": "10.20.30.40",
            "DEPLOY_SSH_USER": "ubuntu",
            "DEPLOY_SSH_PRIVATE_KEY": "-----BEGIN OPENSSH PRIVATE KEY-----\nkey\n",
            "DEPLOY_SSH_KNOWN_HOSTS": "10.20.30.40 ssh-ed25519 key\n",
            "WIREGUARD_CONFIG": wireguard,
            "POSTGRES_PASSWORD": "a" * 64,
            "TARS_JWT_HS256_SECRET": "issuer 'secret' with spaces",
            "GARAGE_RPC_SECRET": "b" * 64,
            "GARAGE_ADMIN_TOKEN": "c" * 64,
            "GARAGE_METRICS_TOKEN": "d" * 64,
            "GARAGE_ACCESS_KEY_ID": "GK" + "e" * 32,
            "GARAGE_SECRET_ACCESS_KEY": "f" * 64,
            "ONEUPTIME_TOKEN": "oneuptime-token",
            "RUNPOD_API_KEY": "runpod-api-secret",
        }

    def test_build_capture_writes_private_docker_config_and_clears_exports(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            github_env = root / "github-env"
            capture_build(self.environment(), root / "docker", github_env)
            config_path = root / "docker" / "config.json"
            config = json.loads(config_path.read_text(encoding="utf-8"))
            encoded = config["auths"]["registry.digitalocean.com"]["auth"]
            self.assertEqual(base64.b64decode(encoded), b"write-token:write-token")
            self.assertEqual(config_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                {path.name for path in (root / "docker").iterdir()},
                {"config.json"},
            )
            cleared = github_env.read_text(encoding="utf-8")
            self.assertEqual(
                cleared,
                "".join(
                    f"{name}=\n" for name in (*DEPLOYMENT_KEYS, *RUNTIME_KEYS)
                ),
            )

    def test_build_capture_requires_only_the_registry_write_credential(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture_build(
                {"DOCR_WRITE_TOKEN": "write-token"},
                root / "docker",
                root / "github-env",
            )
            self.assertEqual(BUILD_KEYS, ("DOCR_WRITE_TOKEN",))
            self.assertTrue((root / "docker" / "config.json").is_file())

    def test_build_capture_clears_known_exports_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            github_env = root / "github-env"
            with self.assertRaisesRegex(RunnerSecretError, "DOCR_WRITE_TOKEN"):
                capture_build({}, root / "docker", github_env)
            self.assertEqual(
                github_env.read_text(encoding="utf-8"),
                "".join(
                    f"{name}=\n" for name in (*DEPLOYMENT_KEYS, *RUNTIME_KEYS)
                ),
            )

    def test_deploy_capture_validates_and_shell_quotes_delegated_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_directory = root / "captured"
            github_output = root / "github-output"
            github_env = root / "github-env"
            environment = self.environment()
            capture_deploy(
                environment, output_directory, github_output, github_env
            )
            for name in (
                "DEPLOY_SSH_PRIVATE_KEY",
                "DEPLOY_SSH_KNOWN_HOSTS",
                "RUNPOD_API_KEY",
                "remote-secrets.sh",
            ):
                self.assertEqual(
                    (output_directory / name).stat().st_mode & 0o777, 0o600
                )
            shell = subprocess.run(
                [
                    "/bin/bash",
                    "-c",
                    'source "$1"; printf "%s\\0%s\\0%s" "$TARS_SECRET_SOURCE" '
                    '"$DOCR_READ_PASSWORD" "$TARS_JWT_HS256_SECRET"',
                    "capture-test",
                    str(output_directory / "remote-secrets.sh"),
                ],
                check=True,
                capture_output=True,
            )
            self.assertEqual(
                shell.stdout,
                b"environment\0registry password with spaces\0issuer 'secret' with spaces",
            )
            rendered = github_output.read_text(encoding="utf-8")
            self.assertIn("host<<", rendered)
            self.assertIn("10.20.30.40", rendered)
            self.assertIn("wireguard_config<<", rendered)
            self.assertEqual(
                github_env.read_text(encoding="utf-8"),
                "".join(
                    f"{name}=\n" for name in (*DEPLOYMENT_KEYS, *RUNTIME_KEYS)
                ),
            )

    def test_deploy_capture_rejects_invalid_connection_before_writing(self) -> None:
        environment = self.environment()
        environment["DEPLOY_SSH_HOST"] = "control.example.com"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RunnerSecretError, "literal IPv4"):
                capture_deploy(
                    environment,
                    root / "captured",
                    root / "github-output",
                    root / "github-env",
                )
            self.assertEqual(
                (root / "github-env").read_text(encoding="utf-8"),
                "".join(f"{name}=\n" for name in (*DEPLOYMENT_KEYS, *RUNTIME_KEYS)),
            )

    def test_deploy_capture_does_not_require_registry_write_credential(self) -> None:
        environment = self.environment()
        del environment["DOCR_WRITE_TOKEN"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture_deploy(
                environment,
                root / "captured",
                root / "github-output",
                root / "github-env",
            )
        self.assertNotIn("DOCR_WRITE_TOKEN", DEPLOY_KEYS)


class FakeReleaseAPI:
    def __init__(self) -> None:
        self.endpoints: list[dict] = []
        self.templates: list[dict] = []
        self.auths: list[dict] = []
        self.calls: list[str] = []
        self.template_env = {"RUNPOD_INIT_TIMEOUT": "1200"}
        self.template_env_by_id: dict[str, object] = {}
        self.health_by_endpoint: dict[str, list[object]] = {}
        self.health_calls: list[str] = []

    def inventory(self) -> Inventory:
        return Inventory(tuple(self.endpoints), tuple(self.templates), tuple(self.auths))

    def read_endpoint(self, endpoint_id: str) -> dict:
        endpoint = next(item for item in self.endpoints if item["id"] == endpoint_id)
        return {
            "id": endpoint["id"],
            "name": endpoint["name"],
            "templateId": endpoint["templateId"],
            "workersMin": endpoint["workersMin"],
            "workersMax": endpoint["workersMax"],
            "computeType": endpoint["computeType"],
            "gpuCount": endpoint["gpuCount"],
            "executionTimeoutMs": endpoint["executionTimeoutMs"],
            "gpuTypeIds": [
                "NVIDIA RTX A4000",
                "NVIDIA RTX A4500",
                "NVIDIA RTX 4000 Ada Generation",
                "NVIDIA RTX 2000 Ada Generation",
                "NVIDIA L4",
                "NVIDIA RTX A5000",
                "NVIDIA GeForce RTX 3090",
            ],
        }

    def read_template(self, template_id: str) -> dict:
        template = next(item for item in self.templates if item["id"] == template_id)
        return {
            "id": template["id"],
            "name": template["name"],
            "env": (
                self.template_env_by_id[template_id]
                if template_id in self.template_env_by_id
                else dict(self.template_env)
            ),
        }

    def read_endpoint_health(
        self, endpoint_id: str
    ) -> tars_runpod_release.EndpointHealth:
        self.health_calls.append(endpoint_id)
        outcomes = self.health_by_endpoint.get(endpoint_id)
        if not outcomes:
            return tars_runpod_release.EndpointHealth(0, 0)
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if not isinstance(outcome, tars_runpod_release.EndpointHealth):
            raise AssertionError("invalid fake endpoint health outcome")
        return outcome

    def create_auth(self, name: str, username: str, password: str) -> dict:
        self.calls.append("create_auth")
        resource = {"id": f"auth{len(self.auths) + 1}", "name": name}
        self.auths.append(resource)
        return resource

    def create_template(self, name: str, image: str, auth_id: str) -> dict:
        self.calls.append("create_template")
        resource = {
            "id": f"template{len(self.templates) + 1}",
            "name": name,
            "imageName": image,
            "isServerless": True,
            "containerDiskInGb": 20,
            "volumeInGb": 0,
            "dockerArgs": None,
            "env": [{"key": "RUNPOD_INIT_TIMEOUT"}],
            "containerRegistryAuthId": auth_id,
            "boundEndpointId": None,
        }
        self.templates.append(resource)
        return resource

    def create_endpoint(self, name: str, template_id: str) -> dict:
        self.calls.append("create_endpoint")
        resource = self.endpoint(
            endpoint_id=f"endpoint{len(self.endpoints) + 1}",
            name=name,
            template_id=template_id,
            created=datetime.now(timezone.utc),
        )
        self.endpoints.append(resource)
        for template in self.templates:
            if template["id"] == template_id:
                template["boundEndpointId"] = resource["id"]
        return resource

    @staticmethod
    def endpoint(
        *, endpoint_id: str, name: str, template_id: str, created: datetime
    ) -> dict:
        return {
            "id": endpoint_id,
            "name": name,
            "gpuIds": GPU_POOL_SELECTOR,
            "idleTimeout": 5,
            "locations": "",
            "scalerType": "REQUEST_COUNT",
            "scalerValue": 1,
            "templateId": template_id,
            "workersMax": 2,
            "workersMin": 0,
            "gpuCount": 1,
            "computeType": "GPU",
            "executionTimeoutMs": 7_200_000,
            "createdAt": created.isoformat().replace("+00:00", "Z"),
        }

    def seed_release(self, release_sha: str, suffix: str, created: datetime) -> None:
        auth_id = f"auth-{suffix}"
        template_id = f"template-{suffix}"
        endpoint_id = f"endpoint-{suffix}"
        self.auths.append(
            {
                "id": auth_id,
                "name": f"tars-runpod-auth-v1-{release_sha}-{'f' * 12}",
            }
        )
        self.templates.append(
            {
                "id": template_id,
                "name": f"tars-runpod-template-v1-{release_sha}",
                "imageName": (
                    "registry.digitalocean.com/lascade/tars:gpu-sha-"
                    + release_sha
                    + "@sha256:"
                    + "a" * 64
                ),
                "isServerless": True,
                "containerDiskInGb": 20,
                "volumeInGb": 0,
                "dockerArgs": None,
                "env": [{"key": "RUNPOD_INIT_TIMEOUT"}],
                "containerRegistryAuthId": auth_id,
                "boundEndpointId": endpoint_id,
            }
        )
        self.endpoints.append(
            self.endpoint(
                endpoint_id=endpoint_id,
                name=f"tars-runpod-endpoint-v1-{release_sha}",
                template_id=template_id,
                created=created,
            )
        )

    def zero_endpoint(self, endpoint: dict) -> dict:
        self.calls.append(f"zero:{endpoint['id']}")
        stored = next(item for item in self.endpoints if item["id"] == endpoint["id"])
        stored["workersMin"] = 0
        stored["workersMax"] = 0
        return self.read_endpoint(stored["id"])

    def activate_endpoint(self, endpoint: dict) -> dict:
        self.calls.append(f"activate:{endpoint['id']}")
        stored = next(item for item in self.endpoints if item["id"] == endpoint["id"])
        stored["workersMin"] = 0
        stored["workersMax"] = 2
        return self.read_endpoint(stored["id"])

    def configure_endpoint(self, endpoint: dict) -> dict:
        self.calls.append(f"configure:{endpoint['id']}")
        stored = next(item for item in self.endpoints if item["id"] == endpoint["id"])
        stored["gpuCount"] = 1
        stored["executionTimeoutMs"] = ENDPOINT_TIMEOUT_MS
        return self.read_endpoint(stored["id"])

    def delete_endpoint(self, endpoint_id: str) -> Inventory:
        self.calls.append(f"delete_endpoint:{endpoint_id}")
        self.endpoints = [item for item in self.endpoints if item["id"] != endpoint_id]
        for template in self.templates:
            if template.get("boundEndpointId") == endpoint_id:
                template["boundEndpointId"] = None
        return self.inventory()

    def delete_template(self, template_name: str) -> Inventory:
        self.calls.append(f"delete_template:{template_name}")
        self.templates = [item for item in self.templates if item["name"] != template_name]
        return self.inventory()

    def delete_auth(self, auth_id: str) -> Inventory:
        self.calls.append(f"delete_auth:{auth_id}")
        self.auths = [item for item in self.auths if item["id"] != auth_id]
        return self.inventory()


class RunpodReleaseTest(unittest.TestCase):
    IMAGE = (
        "registry.digitalocean.com/lascade/tars:gpu-sha-"
        + "a" * 40
        + "@sha256:"
        + "a" * 64
    )

    def test_endpoint_health_uses_official_bearer_api_and_exact_job_counts(
        self,
    ) -> None:
        captured = []

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return (
                    b'{"jobs":{"completed":3,"failed":0,"inProgress":1,'
                    b'"inQueue":2,"retried":0},"workers":{"ready":1}}'
                )

        def open_request(request, *, timeout):
            captured.append((request, timeout))
            return Response()

        health = RunpodClient("api-key", opener=open_request).read_endpoint_health(
            "endpoint-id"
        )

        self.assertEqual(
            health,
            tars_runpod_release.EndpointHealth(in_queue=2, in_progress=1),
        )
        request, timeout = captured[0]
        self.assertEqual(
            request.full_url,
            "https://api.runpod.ai/v2/endpoint-id/health",
        )
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer api-key")
        self.assertEqual(timeout, tars_runpod_release.REQUEST_TIMEOUT_SECONDS)

    def test_endpoint_health_retries_transient_transport_at_most_three_times(
        self,
    ) -> None:
        calls = 0
        sleeps = []

        def fail(_request, *, timeout):
            nonlocal calls
            calls += 1
            raise urllib.error.URLError("offline")

        client = RunpodClient("api-key", opener=fail, sleeper=sleeps.append)
        with self.assertRaisesRegex(RunpodReleaseError, "after 3 calls"):
            client.read_endpoint_health("endpoint-id")

        self.assertEqual(calls, 3)
        self.assertEqual(sleeps, [1.0, 2.0])

    def test_endpoint_health_rejects_malformed_active_job_counts(self) -> None:
        class Response:
            status = 200

            def __init__(self, body: bytes) -> None:
                self.body = body

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return self.body

        for body in (
            b"{}",
            b'{"jobs":[]}',
            b'{"jobs":{"inQueue":0}}',
            b'{"jobs":{"inQueue":false,"inProgress":0}}',
            b'{"jobs":{"inQueue":-1,"inProgress":0}}',
        ):
            with self.subTest(body=body):
                client = RunpodClient(
                    "api-key",
                    opener=lambda _request, *, timeout, body=body: Response(body),
                )
                with self.assertRaisesRegex(
                    RunpodReleaseError, "endpoint health"
                ):
                    client.read_endpoint_health("endpoint-id")

    def test_ensure_is_deterministic_idempotent_and_secret_free(self) -> None:
        api = FakeReleaseAPI()
        resources = ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="registry-reader",
            registry_password="super-secret-registry-password",
        )
        first_calls = list(api.calls)
        repeated = ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="registry-reader",
            registry_password="super-secret-registry-password",
        )
        self.assertEqual(resources, repeated)
        self.assertEqual(
            first_calls, ["create_auth", "create_template", "create_endpoint"]
        )
        self.assertEqual(api.calls, first_calls)
        names = release_names(
            "a" * 40, "registry-reader", "super-secret-registry-password"
        )
        self.assertNotIn("super-secret-registry-password", " ".join(names))

    def test_same_sha_with_a_different_digest_fails_without_provider_mutation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        calls = list(api.calls)
        changed_digest = self.IMAGE.rsplit(":", 1)[0] + ":" + "b" * 64
        with self.assertRaisesRegex(RunpodReleaseError, "imageName"):
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=changed_digest,
                registry_username="reader",
                registry_password="password",
            )
        self.assertEqual(api.calls, calls)

    def test_ensure_rejects_nonimmutable_or_mismatched_gpu_images(self) -> None:
        invalid_images = (
            "registry.digitalocean.com/lascade/tars@sha256:" + "a" * 64,
            "registry.digitalocean.com/lascade/tars:gpu-sha-" + "a" * 40,
            (
                "registry.digitalocean.com/lascade/tars:gpu-sha-"
                + "b" * 40
                + "@sha256:"
                + "a" * 64
            ),
        )
        for image in invalid_images:
            with self.subTest(image=image), self.assertRaises(RunpodReleaseError):
                ensure_release(
                    FakeReleaseAPI(),
                    release_sha="a" * 40,
                    gpu_image=image,
                    registry_username="reader",
                    registry_password="password",
                )

    def test_template_verification_requires_only_reviewed_runtime_fields(self) -> None:
        base = {
            "id": "template1",
            "name": "tars-runpod-template-v1-" + "a" * 40,
            "imageName": self.IMAGE,
            "isServerless": True,
            "containerDiskInGb": 20,
            "volumeInGb": 0,
            "containerRegistryAuthId": "auth1",
            "dockerArgs": "",
            "env": [{"key": "RUNPOD_INIT_TIMEOUT"}],
        }
        for docker_args in (None, ""):
            with self.subTest(docker_args=docker_args):
                resource = {**base, "dockerArgs": docker_args}
                self.assertEqual(
                    tars_runpod_release.verify_template(
                        resource,
                        expected_name=base["name"],
                        image=self.IMAGE,
                        auth_id="auth1",
                    ),
                    "template1",
                )
                self.assertEqual(
                    tars_runpod_release._template_release_sha(resource),
                    "a" * 40,
                )

        invalid = (
            (
                "missing dockerArgs",
                {
                    key: value
                    for key, value in base.items()
                    if key != "dockerArgs"
                },
            ),
            (
                "missing env",
                {key: value for key, value in base.items() if key != "env"},
            ),
            ("nonempty dockerArgs", {**base, "dockerArgs": "python handler.py"}),
            ("empty env", {**base, "env": []}),
            ("null env", {**base, "env": None}),
            ("unreviewed env", {**base, "env": [{"key": "UNREVIEWED"}]}),
            (
                "extra env",
                {
                    **base,
                    "env": [
                        {"key": "RUNPOD_INIT_TIMEOUT"},
                        {"key": "UNREVIEWED"},
                    ],
                },
            ),
            (
                "duplicate env",
                {
                    **base,
                    "env": [
                        {"key": "RUNPOD_INIT_TIMEOUT"},
                        {"key": "RUNPOD_INIT_TIMEOUT"},
                    ],
                },
            ),
        )
        for label, resource in invalid:
            with self.subTest(label=label), self.assertRaises(RunpodReleaseError):
                tars_runpod_release.verify_template(
                    resource,
                    expected_name=base["name"],
                    image=self.IMAGE,
                    auth_id="auth1",
                )
            self.assertIsNone(tars_runpod_release._template_release_sha(resource))

    def test_template_rest_verification_requires_exact_init_timeout(self) -> None:
        base = {
            "id": "template1",
            "name": "tars-runpod-template-v1-" + "a" * 40,
            "env": {"RUNPOD_INIT_TIMEOUT": "1200"},
        }
        self.assertEqual(
            tars_runpod_release.verify_template_rest(
                base,
                template_id="template1",
                expected_name=base["name"],
            ),
            "template1",
        )
        invalid = (
            ("missing env", {key: value for key, value in base.items() if key != "env"}),
            ("empty env", {**base, "env": {}}),
            (
                "wrong timeout",
                {**base, "env": {"RUNPOD_INIT_TIMEOUT": "800"}},
            ),
            (
                "extra env",
                {
                    **base,
                    "env": {
                        "RUNPOD_INIT_TIMEOUT": "1200",
                        "UNREVIEWED": "value",
                    },
                },
            ),
        )
        for label, resource in invalid:
            with self.subTest(label=label), self.assertRaises(RunpodReleaseError):
                tars_runpod_release.verify_template_rest(
                    resource,
                    template_id="template1",
                    expected_name=base["name"],
                )

    def test_ensure_verifies_rest_and_graphql_endpoint_contracts(self) -> None:
        api = FakeReleaseAPI()
        with mock.patch.object(
            api, "read_endpoint", wraps=api.read_endpoint
        ) as read_endpoint:
            resources = ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="registry-reader",
                registry_password="registry-password",
            )

        read_endpoint.assert_called_with(resources.endpoint_id)

    def test_ensure_rejects_template_env_drift_before_endpoint_creation(self) -> None:
        api = FakeReleaseAPI()
        api.template_env = {"RUNPOD_INIT_TIMEOUT": "800"}

        with self.assertRaisesRegex(RunpodReleaseError, "REST template env"):
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="registry-reader",
                registry_password="registry-password",
            )

        self.assertEqual(api.calls, ["create_auth", "create_template"])
        self.assertEqual(api.endpoints, [])

    def test_exact_rerun_rejects_drift_instead_of_mutating(self) -> None:
        api = FakeReleaseAPI()
        ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        api.endpoints[0]["workersMax"] = 1
        with self.assertRaisesRegex(RunpodReleaseError, "workersMax"):
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="reader",
                registry_password="password",
            )

    def test_exact_rerun_rejects_gpu_pool_removal_or_reordering(self) -> None:
        for selector in ("AMPERE_16", "AMPERE_24,AMPERE_16"):
            with self.subTest(selector=selector):
                api = FakeReleaseAPI()
                ensure_release(
                    api,
                    release_sha="a" * 40,
                    gpu_image=self.IMAGE,
                    registry_username="reader",
                    registry_password="password",
                )
                calls_before_drift = list(api.calls)
                api.endpoints[0]["gpuIds"] = selector

                with self.assertRaisesRegex(RunpodReleaseError, "gpuIds"):
                    ensure_release(
                        api,
                        release_sha="a" * 40,
                        gpu_image=self.IMAGE,
                        registry_username="reader",
                        registry_password="password",
                    )

                self.assertEqual(api.calls, calls_before_drift)

    def test_exact_rerun_repairs_only_deterministic_rest_execution_fields(self) -> None:
        api = FakeReleaseAPI()
        expected = ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        api.endpoints[0]["gpuCount"] = 4
        api.endpoints[0]["executionTimeoutMs"] = 60_000
        calls_before_repair = list(api.calls)

        repaired = ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )

        self.assertEqual(repaired, expected)
        self.assertEqual(api.endpoints[0]["gpuCount"], 1)
        self.assertEqual(api.endpoints[0]["executionTimeoutMs"], ENDPOINT_TIMEOUT_MS)
        self.assertEqual(
            api.calls, calls_before_repair + ["configure:endpoint1"]
        )

        calls_after_repair = list(api.calls)
        self.assertEqual(
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="reader",
                registry_password="password",
            ),
            expected,
        )
        self.assertEqual(api.calls, calls_after_repair)

    def test_exact_rerun_does_not_repair_rest_identity_drift(self) -> None:
        api = FakeReleaseAPI()
        ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        api.endpoints[0]["computeType"] = "CPU"
        calls_before = list(api.calls)

        with self.assertRaisesRegex(RunpodReleaseError, "computeType"):
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="reader",
                registry_password="password",
            )

        self.assertEqual(api.calls, calls_before)

    def test_exact_rerun_restores_only_a_verified_idle_endpoint(self) -> None:
        api = FakeReleaseAPI()
        expected = ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        api.endpoints[0]["workersMax"] = 0
        calls_before = list(api.calls)
        actual = ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        self.assertEqual(actual, expected)
        self.assertEqual(api.endpoints[0]["workersMax"], 2)
        self.assertEqual(api.calls, calls_before + ["activate:endpoint1"])

    def test_idle_rerun_rejects_other_drift_before_activation(self) -> None:
        api = FakeReleaseAPI()
        ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        api.endpoints[0]["workersMax"] = 0
        api.endpoints[0]["scalerValue"] = 2
        calls_before = list(api.calls)
        with self.assertRaisesRegex(RunpodReleaseError, "scalerValue"):
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="reader",
                registry_password="password",
            )
        self.assertEqual(api.calls, calls_before)
        self.assertEqual(api.endpoints[0]["workersMax"], 0)

    def test_credential_rotation_for_same_release_fails_before_creating_orphans(self) -> None:
        api = FakeReleaseAPI()
        ensure_release(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="first-password",
        )
        calls = list(api.calls)
        with self.assertRaisesRegex(RunpodReleaseError, "credential"):
            ensure_release(
                api,
                release_sha="a" * 40,
                gpu_image=self.IMAGE,
                registry_username="reader",
                registry_password="rotated-password",
            )
        self.assertEqual(api.calls, calls)
        self.assertEqual(len(api.auths), 1)

    def test_endpoint_creation_splits_documented_graphql_and_rest_fields(self) -> None:
        client = RunpodClient("api-key")
        response = FakeReleaseAPI.endpoint(
            endpoint_id="endpoint1",
            name="tars-runpod-endpoint-v1-" + "a" * 40,
            template_id="template1",
            created=datetime.now(timezone.utc),
        )
        with (
            mock.patch.object(
                client,
                "_graphql",
                return_value={
                    "saveEndpoint": {
                        "id": response["id"],
                        "name": response["name"],
                    }
                },
            ) as graphql,
            mock.patch.object(
                client, "_confirmed_endpoint_patch", return_value=response
            ) as patch_endpoint,
        ):
            created = client.create_endpoint(response["name"], "template1")
        self.assertEqual(created, response)
        query = graphql.call_args.args[1]
        self.assertIn(f'gpuIds: "{GPU_POOL_SELECTOR}"', query)
        self.assertIn("workersMin: 0", query)
        self.assertIn("workersMax: 2", query)
        self.assertIn('scalerType: "REQUEST_COUNT"', query)
        self.assertIn("scalerValue: 1", query)
        self.assertIn('locations: ""', query)
        self.assertNotIn("executionTimeoutMs", query)
        self.assertNotIn("gpuCount", query)
        self.assertNotIn("computeType", query)
        self.assertEqual(
            GPU_POOL_SELECTOR.split(","), ["AMPERE_16", "AMPERE_24"]
        )
        patch_endpoint.assert_called_once_with(
            endpoint_id="endpoint1",
            payload={"executionTimeoutMs": ENDPOINT_TIMEOUT_MS, "gpuCount": 1},
            expected_name=response["name"],
            template_id="template1",
            workers_max=2,
            operation="configure endpoint execution",
        )

    def test_graphql_inventory_uses_only_graphql_endpoint_contract_fields(self) -> None:
        client = RunpodClient("api-key")
        with mock.patch.object(
            client,
            "_graphql",
            return_value={
                "myself": {
                    "endpoints": [],
                    "podTemplates": [],
                    "containerRegistryCreds": [],
                }
            },
        ) as graphql:
            self.assertEqual(client.inventory(), Inventory((), (), ()))

        query = graphql.call_args.args[1]
        for field in ("gpuIds", "locations", "scalerType", "scalerValue"):
            self.assertIn(field, query)
        self.assertRegex(query, r"env\s*\{\s*key\s*\}")
        self.assertNotRegex(query, r"env\s*\{[^}]*\bvalue\b")
        for rest_only_field in ("gpuCount", "computeType", "executionTimeoutMs"):
            self.assertNotIn(rest_only_field, query)

    def test_endpoint_scaling_uses_rest_patch_and_exact_target(self) -> None:
        client = RunpodClient("api-key")
        endpoint = FakeReleaseAPI.endpoint(
            endpoint_id="endpoint1",
            name="tars-runpod-endpoint-v1-" + "a" * 40,
            template_id="template1",
            created=datetime.now(timezone.utc),
        )
        zeroed = {**endpoint, "workersMax": 0}
        with mock.patch.object(
            client, "_confirmed_endpoint_patch", return_value=zeroed
        ) as patch_endpoint:
            self.assertEqual(client.zero_endpoint(endpoint), zeroed)
        patch_endpoint.assert_called_once_with(
            endpoint_id="endpoint1",
            payload={"workersMin": 0, "workersMax": 0},
            expected_name=endpoint["name"],
            template_id="template1",
            workers_max=0,
            operation="set endpoint workers to zero",
        )

    def test_endpoint_reconfigure_mutates_only_rest_execution_fields(self) -> None:
        client = RunpodClient("api-key")
        endpoint = FakeReleaseAPI.endpoint(
            endpoint_id="endpoint1",
            name="tars-runpod-endpoint-v1-" + "a" * 40,
            template_id="template1",
            created=datetime.now(timezone.utc),
        )
        with mock.patch.object(
            client, "_confirmed_endpoint_patch", return_value=endpoint
        ) as patch_endpoint:
            self.assertEqual(client.configure_endpoint(endpoint), endpoint)
        patch_endpoint.assert_called_once_with(
            endpoint_id="endpoint1",
            payload={"executionTimeoutMs": ENDPOINT_TIMEOUT_MS, "gpuCount": 1},
            expected_name=endpoint["name"],
            template_id="template1",
            workers_max=2,
            operation="configure endpoint execution",
        )

    def test_endpoint_create_recovers_a_lost_graphql_response_before_rest_patch(self) -> None:
        endpoint = FakeReleaseAPI.endpoint(
            endpoint_id="endpoint1",
            name="tars-runpod-endpoint-v1-" + "a" * 40,
            template_id="template1",
            created=datetime.now(timezone.utc),
        )
        client = RunpodClient("api-key")
        with (
            mock.patch.object(
                client,
                "_graphql",
                side_effect=(
                    RunpodReleaseError("response lost"),
                    {
                        "myself": {
                            "endpoints": [endpoint],
                            "podTemplates": [],
                            "containerRegistryCreds": [],
                        }
                    },
                ),
            ) as graphql,
            mock.patch.object(
                client, "_confirmed_endpoint_patch", return_value=endpoint
            ) as patch_endpoint,
        ):
            self.assertEqual(
                client.create_endpoint(endpoint["name"], "template1"), endpoint
            )

        self.assertEqual(graphql.call_count, 2)
        self.assertEqual(graphql.call_args_list[0].kwargs["max_calls"], 1)
        self.assertEqual(graphql.call_args_list[1].kwargs["max_calls"], 1)
        patch_endpoint.assert_called_once()

    def test_template_mutation_sets_registry_auth_exactly_once(self) -> None:
        client = RunpodClient("api-key")
        with mock.patch.object(
            client,
            "_graphql",
            return_value={"saveTemplate": {"id": "template1", "name": "template"}},
        ) as graphql:
            client.create_template("template", self.IMAGE, "auth1")
        query = graphql.call_args.args[1]
        self.assertEqual(query.count("containerRegistryAuthId"), 1)
        self.assertIn(f"imageName: {json.dumps(self.IMAGE)}", query)
        self.assertEqual(query.count('dockerArgs: ""'), 1)
        self.assertEqual(query.count('key: "RUNPOD_INIT_TIMEOUT"'), 1)
        self.assertEqual(query.count('value: "1200"'), 1)
        self.assertNotRegex(query, r"env:\s*\[\s*\]")

    def test_template_rest_read_targets_one_bound_template(self) -> None:
        template = {
            "id": "template1",
            "name": "tars-runpod-template-v1-" + "a" * 40,
            "env": {"RUNPOD_INIT_TIMEOUT": "1200"},
        }
        requests: list[urllib.request.Request] = []

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps(template).encode("utf-8")

        def open_request(request, *, timeout):
            requests.append(request)
            return Response()

        client = RunpodClient("api-key", opener=open_request)
        self.assertEqual(client.read_template("template1"), template)
        self.assertEqual(len(requests), 1)
        parsed = urllib.parse.urlsplit(requests[0].full_url)
        self.assertEqual(parsed.path, "/v1/templates/template1")
        self.assertEqual(
            urllib.parse.parse_qs(parsed.query),
            {"includeEndpointBoundTemplates": ["true"]},
        )
        self.assertEqual(requests[0].get_header("Authorization"), "Bearer api-key")

    def test_auth_and_template_create_recover_lost_responses_without_reissuing_mutations(self) -> None:
        release_sha = "a" * 40
        auth_name = f"tars-runpod-auth-v1-{release_sha}-{'f' * 12}"
        template_name = f"tars-runpod-template-v1-{release_sha}"
        auth = {"id": "auth1", "name": auth_name}
        template = {
            "id": "template1",
            "name": template_name,
            "imageName": self.IMAGE,
            "isServerless": True,
            "containerDiskInGb": 20,
            "volumeInGb": 0,
            "dockerArgs": None,
            "env": [{"key": "RUNPOD_INIT_TIMEOUT"}],
            "containerRegistryAuthId": "auth1",
            "boundEndpointId": None,
        }
        cases = (
            (
                "auth",
                {"endpoints": [], "podTemplates": [], "containerRegistryCreds": [auth]},
                lambda client: client.create_auth(auth_name, "reader", "password"),
                auth,
            ),
            (
                "template",
                {
                    "endpoints": [],
                    "podTemplates": [template],
                    "containerRegistryCreds": [auth],
                },
                lambda client: client.create_template(
                    template_name, self.IMAGE, "auth1"
                ),
                template,
            ),
        )

        class Response:
            status = 200

            def __init__(self, inventory):
                self.inventory = inventory

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps(
                    {"data": {"myself": self.inventory}}
                ).encode("utf-8")

        for label, inventory, create, expected in cases:
            with self.subTest(label=label):
                requests: list[urllib.request.Request] = []

                def lose_create_response(request, *, timeout):
                    requests.append(request)
                    if len(requests) == 1:
                        raise urllib.error.URLError("response lost")
                    return Response(inventory)

                client = RunpodClient("api-key", opener=lose_create_response)
                self.assertEqual(create(client), expected)
                self.assertEqual(len(requests), 2)
                first_body = json.loads(requests[0].data)
                self.assertIn("mutation", first_body["query"])
                second_body = json.loads(requests[1].data)
                self.assertIn("query TarsRunpodInventory", second_body["query"])

    def test_auth_and_template_create_stop_after_three_calls_when_unresolved(self) -> None:
        operations = (
            (
                "auth",
                lambda client: client.create_auth(
                    "tars-runpod-auth-v1-" + "a" * 40 + "-" + "f" * 12,
                    "reader",
                    "password",
                ),
            ),
            (
                "template",
                lambda client: client.create_template(
                    "tars-runpod-template-v1-" + "a" * 40,
                    self.IMAGE,
                    "auth1",
                ),
            ),
        )
        for label, create in operations:
            with self.subTest(label=label):
                calls = 0

                def fail(_request, *, timeout):
                    nonlocal calls
                    calls += 1
                    raise urllib.error.URLError("unavailable")

                client = RunpodClient("api-key", opener=fail)
                with self.assertRaises(RunpodReleaseError):
                    create(client)
                self.assertEqual(calls, 3)

    def test_network_failures_stop_after_three_calls_without_echoing_secrets(self) -> None:
        calls = 0
        sleeps: list[float] = []

        def fail(_request, *, timeout):
            nonlocal calls
            calls += 1
            raise urllib.error.URLError("api-key registry-password")

        client = RunpodClient("api-key", opener=fail, sleeper=sleeps.append)
        with self.assertRaises(RunpodReleaseError) as caught:
            client.inventory()
        self.assertEqual(calls, 3)
        self.assertEqual(sleeps, [1.0, 2.0])
        self.assertNotIn("api-key", str(caught.exception))
        self.assertNotIn("registry-password", str(caught.exception))

    def test_endpoint_rest_patch_recovers_a_lost_response_and_verifies_exactly(self) -> None:
        graphql_endpoint = FakeReleaseAPI.endpoint(
            endpoint_id="endpoint1",
            name="tars-runpod-endpoint-v1-" + "a" * 40,
            template_id="template1",
            created=datetime.now(timezone.utc),
        )
        endpoint = {
            "id": graphql_endpoint["id"],
            "name": graphql_endpoint["name"],
            "templateId": graphql_endpoint["templateId"],
            "workersMin": graphql_endpoint["workersMin"],
            "workersMax": graphql_endpoint["workersMax"],
            "gpuCount": graphql_endpoint["gpuCount"],
            "executionTimeoutMs": graphql_endpoint["executionTimeoutMs"],
            "gpuTypeIds": [
                "NVIDIA RTX A4000",
                "NVIDIA RTX A4500",
                "NVIDIA RTX 4000 Ada Generation",
                "NVIDIA RTX 2000 Ada Generation",
            ],
        }
        calls: list[urllib.request.Request] = []

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps(endpoint).encode("utf-8")

        def lose_patch_response(request, *, timeout):
            calls.append(request)
            if len(calls) == 1:
                raise urllib.error.URLError("response lost")
            return Response()

        client = RunpodClient("api-key", opener=lose_patch_response)
        verified = client._confirmed_endpoint_patch(
            endpoint_id="endpoint1",
            payload={"executionTimeoutMs": ENDPOINT_TIMEOUT_MS, "gpuCount": 1},
            expected_name=graphql_endpoint["name"],
            template_id="template1",
            workers_max=2,
            operation="configure endpoint execution",
        )

        self.assertEqual(verified, endpoint)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].method, "PATCH")
        self.assertEqual(calls[1].method, "GET")
        self.assertEqual(
            urllib.parse.parse_qs(urllib.parse.urlsplit(calls[1].full_url).query),
            {"includeTemplate": ["true"]},
        )
        self.assertEqual(calls[0].get_header("Authorization"), "Bearer api-key")

    def test_endpoint_rest_requires_gpu_identity_when_compute_type_is_omitted(self) -> None:
        api = FakeReleaseAPI()
        endpoint = FakeReleaseAPI.endpoint(
            endpoint_id="endpoint1",
            name="tars-runpod-endpoint-v1-" + "a" * 40,
            template_id="template1",
            created=datetime.now(timezone.utc),
        )
        api.endpoints.append(endpoint)
        live_shape = api.read_endpoint("endpoint1")
        live_shape.pop("computeType")
        self.assertEqual(
            tars_runpod_release.verify_endpoint_rest(
                live_shape,
                endpoint_id="endpoint1",
                expected_name=endpoint["name"],
                template_id="template1",
            ),
            "endpoint1",
        )

        invalid = (
            (
                "missing GPU selectors",
                {
                    key: value
                    for key, value in live_shape.items()
                    if key != "gpuTypeIds"
                },
            ),
            ("empty GPU selectors", {**live_shape, "gpuTypeIds": []}),
            (
                "non-list GPU selectors",
                {**live_shape, "gpuTypeIds": "AMPERE_16"},
            ),
            ("malformed GPU selectors", {**live_shape, "gpuTypeIds": [{}]}),
            (
                "duplicate GPU selectors",
                {**live_shape, "gpuTypeIds": ["NVIDIA RTX A4000"] * 2},
            ),
            ("explicit CPU compute", {**live_shape, "computeType": "CPU"}),
            ("CPU flavor selectors", {**live_shape, "cpuFlavorIds": ["cpu3c"]}),
            (
                "CPU instance selectors",
                {**live_shape, "instanceIds": ["cpu3c-8-16"]},
            ),
        )
        for label, drift in invalid:
            with self.subTest(label=label), self.assertRaises(RunpodReleaseError):
                tars_runpod_release.verify_endpoint_rest(
                    drift,
                    endpoint_id="endpoint1",
                    expected_name=endpoint["name"],
                    template_id="template1",
                )

    def test_endpoint_rest_patch_stops_after_three_total_calls(self) -> None:
        calls = 0

        def fail(_request, *, timeout):
            nonlocal calls
            calls += 1
            raise urllib.error.URLError("unavailable")

        client = RunpodClient("api-key", opener=fail)
        with self.assertRaises(RunpodReleaseError):
            client._confirmed_endpoint_patch(
                endpoint_id="endpoint1",
                payload={"workersMin": 0, "workersMax": 0},
                expected_name="tars-runpod-endpoint-v1-" + "a" * 40,
                template_id="template1",
                workers_max=0,
                operation="set endpoint workers to zero",
            )
        self.assertEqual(calls, 3)

    def test_all_deletes_treat_absence_after_lost_response_as_success(self) -> None:
        empty_inventory = {
            "data": {
                "myself": {
                    "endpoints": [],
                    "podTemplates": [],
                    "containerRegistryCreds": [],
                }
            }
        }

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps(empty_inventory).encode("utf-8")

        operations = (
            ("endpoint", lambda client: client.delete_endpoint("endpoint1")),
            (
                "template",
                lambda client: client.delete_template(
                    "tars-runpod-template-v1-" + "a" * 40
                ),
            ),
            ("auth", lambda client: client.delete_auth("auth1")),
        )
        for label, operation in operations:
            with self.subTest(label=label):
                calls = 0

                def lose_delete_response(_request, *, timeout):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise urllib.error.URLError("response lost")
                    return Response()

                client = RunpodClient("api-key", opener=lose_delete_response)
                inventory = operation(client)
                self.assertEqual(inventory, Inventory((), (), ()))
                self.assertEqual(calls, 2)

    def test_all_deletes_stop_after_three_total_calls_when_unresolved(self) -> None:
        operations = (
            ("endpoint", lambda client: client.delete_endpoint("endpoint1")),
            (
                "template",
                lambda client: client.delete_template(
                    "tars-runpod-template-v1-" + "a" * 40
                ),
            ),
            ("auth", lambda client: client.delete_auth("auth1")),
        )
        for label, operation in operations:
            with self.subTest(label=label):
                calls = 0

                def fail(_request, *, timeout):
                    nonlocal calls
                    calls += 1
                    raise urllib.error.URLError("unavailable")

                client = RunpodClient("api-key", opener=fail)
                with self.assertRaises(RunpodReleaseError):
                    operation(client)
                self.assertEqual(calls, 3)

    def test_template_delete_retries_three_mutations_with_release_delay(self) -> None:
        template_name = "tars-runpod-template-v1-" + "a" * 40
        retained = Inventory(
            (),
            ({"id": "template1", "name": template_name},),
            (),
        )
        absent = Inventory((), (), ())
        sleeps: list[float] = []
        client = RunpodClient("api-key", sleeper=sleeps.append)
        with (
            mock.patch.object(client, "_graphql", return_value={}) as delete,
            mock.patch.object(
                client, "_inventory", side_effect=(retained, retained, absent)
            ) as inventory,
        ):
            self.assertEqual(client.delete_template(template_name), absent)

        self.assertEqual(delete.call_count, 3)
        self.assertEqual(inventory.call_count, 3)
        self.assertTrue(
            all(call.kwargs["max_calls"] == 1 for call in delete.call_args_list)
        )
        self.assertTrue(
            all(call.kwargs["max_calls"] == 2 for call in inventory.call_args_list)
        )
        self.assertEqual(sleeps, [60.0, 60.0])

    def test_graphql_transport_error_does_not_retain_secret_url_context(self) -> None:
        def fail(request, *, timeout):
            raise urllib.error.HTTPError(
                request.full_url,
                401,
                "denied api-key",
                {},
                None,
            )

        client = RunpodClient("api-key", opener=fail)
        with self.assertRaises(RunpodReleaseError) as caught:
            client.inventory()
        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertIsNone(caught.exception.__context__)
        self.assertNotIn("api-key", rendered)

    def test_graphql_never_follows_any_supported_http_redirect(self) -> None:
        class RedirectTransport(urllib.request.BaseHandler):
            handler_order = 100

            def __init__(self, code: int) -> None:
                self.code = code
                self.calls: list[str] = []

            def http_open(self, request):
                self.calls.append(request.full_url)
                headers = email.message.Message()
                headers["Location"] = (
                    "http://attacker.invalid/capture?api_key=api-key"
                )
                response = urllib.response.addinfourl(
                    io.BytesIO(b""), headers, request.full_url, self.code
                )
                response.msg = "redirect"
                return response

        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code):
                transport = RedirectTransport(code)
                opener = urllib.request.build_opener(
                    _NoRedirectHandler(), transport
                )
                with mock.patch(
                    "tars_runpod_release._NO_REDIRECT_OPENER", opener
                ):
                    client = RunpodClient("api-key")
                    client._graphql_url = (
                        "http://runpod.invalid/graphql?api_key=api-key"
                    )
                    with self.assertRaises(RunpodReleaseError) as caught:
                        client.inventory()
                self.assertEqual(
                    transport.calls,
                    ["http://runpod.invalid/graphql?api_key=api-key"],
                )
                rendered = "".join(traceback.format_exception(caught.exception))
                self.assertIsNone(caught.exception.__context__)
                self.assertNotIn("api-key", rendered)

    def test_unexpected_opener_and_response_errors_are_redaction_safe(self) -> None:
        class SecretFailure(RuntimeError):
            def __init__(self, full_url: str) -> None:
                self.full_url = full_url
                super().__init__(f"unexpected transport failure for {full_url}")

        calls: list[str] = []

        def fail_in_opener(request, *, timeout):
            calls.append("opener")
            raise SecretFailure(request.full_url)

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                calls.append("response")
                raise SecretFailure("https://api.runpod.io/graphql?api_key=api-key")

        for label, opener in (
            ("opener", fail_in_opener),
            ("response", lambda _request, *, timeout: Response()),
        ):
            with self.subTest(label=label):
                calls.clear()
                client = RunpodClient("api-key", opener=opener)
                with self.assertRaises(RunpodReleaseError) as caught:
                    client.inventory()
                rendered = "".join(traceback.format_exception(caught.exception))
                self.assertEqual(calls, [label])
                self.assertIsNone(caught.exception.__context__)
                self.assertNotIn("api-key", rendered)

    def test_graphql_uses_documented_query_auth_without_header_and_hides_errors(self) -> None:
        captured = []

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b'{"errors":[{"message":"registry-password"}]}'

        def open_request(request, *, timeout):
            captured.append((request, timeout))
            return Response()

        client = RunpodClient("api-key", opener=open_request)
        with self.assertRaises(RunpodReleaseError) as caught:
            client.create_auth("name", "reader", "registry-password")
        request, _timeout = captured[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(
            (parsed.scheme, parsed.netloc, parsed.path),
            ("https", "api.runpod.io", "/graphql"),
        )
        self.assertEqual(urllib.parse.parse_qs(parsed.query), {"api_key": ["api-key"]})
        self.assertIsNone(request.get_header("Authorization"))
        self.assertNotIn("api-key", str(caught.exception))
        self.assertNotIn("registry-password", str(caught.exception))

    def test_secret_files_must_be_owner_only_and_errors_do_not_echo_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "secret"
            path.write_text("do-not-print-this", encoding="utf-8")
            path.chmod(0o644)
            with self.assertRaises(RunpodReleaseError) as caught:
                read_secret(path, "RUNPOD_API_KEY")
            self.assertNotIn("do-not-print-this", str(caught.exception))

    def test_prune_protects_current_previous_and_only_deletes_owned_expired(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "previous", now - timedelta(days=10))
        api.seed_release("c" * 40, "expired", now - timedelta(days=2))
        api.seed_release("d" * 40, "young", now - timedelta(hours=2))
        api.endpoints.append(
            {
                **FakeReleaseAPI.endpoint(
                    endpoint_id="foreign-endpoint",
                    name="some-other-project",
                    template_id="foreign-template",
                    created=now - timedelta(days=30),
                )
            }
        )
        api.seed_release("e" * 40, "mismatch", now - timedelta(days=30))
        mismatched_template = next(
            template for template in api.templates if template["id"] == "template-mismatch"
        )
        mismatched_template["name"] = "tars-runpod-template-v1-" + "f" * 40
        deleted = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id="endpoint-previous",
            protected_release_sha="a" * 40,
            now=now,
        )
        self.assertEqual(deleted, 1)
        self.assertEqual(
            api.calls,
            [
                "zero:endpoint-expired",
                "delete_endpoint:endpoint-expired",
                "delete_template:tars-runpod-template-v1-" + "c" * 40,
                "delete_auth:auth-expired",
            ],
        )
        remaining = {endpoint["id"] for endpoint in api.endpoints}
        self.assertIn("endpoint-current", remaining)
        self.assertIn("endpoint-previous", remaining)
        self.assertIn("endpoint-young", remaining)
        self.assertIn("endpoint-mismatch", remaining)
        self.assertIn("foreign-endpoint", remaining)

    def test_prune_skips_endpoint_with_queued_or_running_work(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "busy", now - timedelta(days=2))
        api.health_by_endpoint["endpoint-busy"] = [
            tars_runpod_release.EndpointHealth(1, 0)
        ]

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.calls, [])
        self.assertEqual(api.health_calls, ["endpoint-busy"])
        self.assertIn(
            "endpoint-busy", {endpoint["id"] for endpoint in api.endpoints}
        )

    def test_prune_preflights_all_health_before_any_mutation(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "first", now - timedelta(days=3))
        api.seed_release("c" * 40, "second", now - timedelta(days=2))
        api.health_by_endpoint["endpoint-second"] = [
            RunpodReleaseError("Runpod endpoint health is unavailable")
        ]

        with self.assertRaisesRegex(RunpodReleaseError, "health"):
            prune_releases(
                api,
                current_endpoint_id="endpoint-current",
                previous_endpoint_id=None,
                protected_release_sha="a" * 40,
                now=now,
            )

        self.assertEqual(api.calls, [])
        self.assertEqual(
            api.health_calls, ["endpoint-first", "endpoint-second"]
        )
        self.assertIn(
            "endpoint-first", {endpoint["id"] for endpoint in api.endpoints}
        )

    def test_prune_rechecks_immediately_before_zeroing(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "raced", now - timedelta(days=2))
        api.health_by_endpoint["endpoint-raced"] = [
            tars_runpod_release.EndpointHealth(0, 0),
            tars_runpod_release.EndpointHealth(0, 1),
        ]

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.calls, [])
        self.assertEqual(
            api.health_calls, ["endpoint-raced", "endpoint-raced"]
        )

    def test_prune_restores_endpoint_if_work_appears_after_zero(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "raced", now - timedelta(days=2))
        api.health_by_endpoint["endpoint-raced"] = [
            tars_runpod_release.EndpointHealth(0, 0),
            tars_runpod_release.EndpointHealth(0, 0),
            tars_runpod_release.EndpointHealth(1, 0),
        ]

        with self.assertRaisesRegex(RunpodReleaseError, "received work"):
            prune_releases(
                api,
                current_endpoint_id="endpoint-current",
                previous_endpoint_id=None,
                protected_release_sha="a" * 40,
                now=now,
            )

        self.assertEqual(
            api.calls, ["zero:endpoint-raced", "activate:endpoint-raced"]
        )
        raced = next(
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] == "endpoint-raced"
        )
        self.assertEqual(raced["workersMax"], 2)

    def test_prune_preserves_preexisting_zero_limit_if_work_appears(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "already-zero", now - timedelta(days=2))
        endpoint = next(
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] == "endpoint-already-zero"
        )
        endpoint["workersMax"] = 0
        api.health_by_endpoint["endpoint-already-zero"] = [
            tars_runpod_release.EndpointHealth(0, 0),
            tars_runpod_release.EndpointHealth(0, 0),
            tars_runpod_release.EndpointHealth(0, 1),
        ]

        with self.assertRaisesRegex(RunpodReleaseError, "left unchanged"):
            prune_releases(
                api,
                current_endpoint_id="endpoint-current",
                previous_endpoint_id=None,
                protected_release_sha="a" * 40,
                now=now,
            )

        self.assertEqual(api.calls, [])
        self.assertEqual(endpoint["workersMax"], 0)
        self.assertIn(
            "endpoint-already-zero",
            {candidate["id"] for candidate in api.endpoints},
        )

    def test_prune_restores_endpoint_if_post_zero_health_is_unavailable(
        self,
    ) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "unknown", now - timedelta(days=2))
        api.health_by_endpoint["endpoint-unknown"] = [
            tars_runpod_release.EndpointHealth(0, 0),
            tars_runpod_release.EndpointHealth(0, 0),
            RunpodReleaseError("Runpod endpoint health is unavailable"),
        ]

        with self.assertRaisesRegex(
            RunpodReleaseError, "could not be confirmed after workers"
        ):
            prune_releases(
                api,
                current_endpoint_id="endpoint-current",
                previous_endpoint_id=None,
                protected_release_sha="a" * 40,
                now=now,
            )

        self.assertEqual(
            api.calls, ["zero:endpoint-unknown", "activate:endpoint-unknown"]
        )
        self.assertNotIn(
            "delete_endpoint:endpoint-unknown", api.calls
        )

    def test_prune_retires_only_the_exact_legacy_tars_chain(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "legacy", now - timedelta(days=2))
        legacy_template = next(
            template
            for template in api.templates
            if template["id"] == "template-legacy"
        )
        legacy_template["env"] = []
        api.template_env_by_id["template-legacy"] = None
        legacy_endpoint = next(
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] == "endpoint-legacy"
        )
        legacy_endpoint["gpuIds"] = "AMPERE_16"

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
            grace=timedelta(0),
        )

        self.assertEqual(retired, 1)
        self.assertEqual(
            api.calls,
            [
                "zero:endpoint-legacy",
                "delete_endpoint:endpoint-legacy",
                "delete_template:tars-runpod-template-v1-" + "b" * 40,
                "delete_auth:auth-legacy",
            ],
        )

    def test_prune_rejects_mixed_legacy_contracts_without_health_or_mutation(
        self,
    ) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        for label, graphql_env, rest_env, gpu_ids in (
            (
                "legacy-env-current-gpu",
                [],
                None,
                GPU_POOL_SELECTOR,
            ),
            (
                "current-env-legacy-gpu",
                [{"key": "RUNPOD_INIT_TIMEOUT"}],
                {"RUNPOD_INIT_TIMEOUT": "1200"},
                "AMPERE_16",
            ),
        ):
            with self.subTest(label=label):
                api = FakeReleaseAPI()
                api.seed_release("a" * 40, "current", now)
                api.seed_release("b" * 40, label, now - timedelta(days=2))
                template_id = f"template-{label}"
                endpoint_id = f"endpoint-{label}"
                next(
                    template
                    for template in api.templates
                    if template["id"] == template_id
                )["env"] = graphql_env
                api.template_env_by_id[template_id] = rest_env
                next(
                    endpoint
                    for endpoint in api.endpoints
                    if endpoint["id"] == endpoint_id
                )["gpuIds"] = gpu_ids

                retired = prune_releases(
                    api,
                    current_endpoint_id="endpoint-current",
                    previous_endpoint_id=None,
                    protected_release_sha="a" * 40,
                    now=now,
                    grace=timedelta(0),
                )

                self.assertEqual(retired, 0)
                self.assertEqual(api.calls, [])
                self.assertEqual(api.health_calls, [])

    def test_prune_rejects_legacy_rest_env_drift_before_mutation(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "legacy", now - timedelta(days=2))
        next(
            template
            for template in api.templates
            if template["id"] == "template-legacy"
        )["env"] = []
        next(
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] == "endpoint-legacy"
        )["gpuIds"] = "AMPERE_16"
        api.template_env_by_id["template-legacy"] = {}

        with self.assertRaisesRegex(RunpodReleaseError, "REST template env"):
            prune_releases(
                api,
                current_endpoint_id="endpoint-current",
                previous_endpoint_id=None,
                protected_release_sha="a" * 40,
                now=now,
                grace=timedelta(0),
            )

        self.assertEqual(api.calls, [])
        self.assertEqual(api.health_calls, [])

    def test_pre_prune_cli_empty_current_file_never_contacts_runpod(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "current"
            previous = root / "previous"
            current.write_text("", encoding="utf-8")
            previous.write_text("endpoint-previous\n", encoding="utf-8")
            argv = [
                "tars_runpod_release.py",
                "pre-prune",
                "--api-key-file",
                str(root / "missing-api-key"),
                "--current-endpoint-file",
                str(current),
                "--previous-endpoint-file",
                str(previous),
                "--protected-release-sha",
                "a" * 40,
            ]

            with (
                mock.patch("sys.argv", argv),
                mock.patch("sys.stdout", new=io.StringIO()) as output,
                mock.patch.object(
                    tars_runpod_release, "RunpodClient"
                ) as client_type,
                mock.patch.object(
                    tars_runpod_release, "prune_releases"
                ) as prune,
            ):
                tars_runpod_release.main()

            client_type.assert_not_called()
            prune.assert_not_called()
            self.assertIn("retired 0", output.getvalue())

    def test_pre_prune_cli_uses_zero_grace_and_endpoint_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            api_key = root / "api-key"
            current = root / "current"
            previous = root / "previous"
            api_key.write_text("api-key\n", encoding="utf-8")
            api_key.chmod(0o600)
            current.write_text("endpoint-current\n", encoding="utf-8")
            previous.write_text("endpoint-previous\n", encoding="utf-8")
            argv = [
                "tars_runpod_release.py",
                "pre-prune",
                "--api-key-file",
                str(api_key),
                "--current-endpoint-file",
                str(current),
                "--previous-endpoint-file",
                str(previous),
                "--protected-release-sha",
                "a" * 40,
            ]
            client = object()

            with (
                mock.patch("sys.argv", argv),
                mock.patch("sys.stdout", new=io.StringIO()),
                mock.patch.object(
                    tars_runpod_release,
                    "RunpodClient",
                    return_value=client,
                ),
                mock.patch.object(
                    tars_runpod_release,
                    "prune_releases",
                    return_value=2,
                ) as prune,
            ):
                tars_runpod_release.main()

            prune.assert_called_once()
            kwargs = prune.call_args.kwargs
            self.assertIs(prune.call_args.args[0], client)
            self.assertEqual(kwargs["current_endpoint_id"], "endpoint-current")
            self.assertEqual(kwargs["previous_endpoint_id"], "endpoint-previous")
            self.assertEqual(kwargs["protected_release_sha"], "a" * 40)
            self.assertEqual(kwargs["grace"], timedelta(0))

    def test_prune_does_not_delete_unageable_partial_chain_orphans(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "partial", now - timedelta(days=2))
        api.endpoints = [
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] != "endpoint-partial"
        ]

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.calls, [])
        self.assertIn(
            "template-partial", {template["id"] for template in api.templates}
        )
        self.assertIn("auth-partial", {auth["id"] for auth in api.auths})

    def test_zero_grace_prune_rejects_malformed_fresh_creation_time(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("b" * 40, "drifted", now - timedelta(days=2))
        original_inventory = api.inventory
        inventory_calls = 0

        def inventory_with_timestamp_drift() -> Inventory:
            nonlocal inventory_calls
            inventory_calls += 1
            if inventory_calls == 2:
                next(
                    endpoint
                    for endpoint in api.endpoints
                    if endpoint["id"] == "endpoint-drifted"
                )["createdAt"] = "not-a-timestamp"
            return original_inventory()

        api.inventory = inventory_with_timestamp_drift  # type: ignore[method-assign]

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
            grace=timedelta(0),
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.health_calls, [])
        self.assertEqual(api.calls, [])
        self.assertIn(
            "endpoint-drifted", {endpoint["id"] for endpoint in api.endpoints}
        )

    def test_prune_rejects_template_image_tag_from_another_release(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("e" * 40, "mismatch", now - timedelta(days=2))
        mismatched_template = next(
            template for template in api.templates if template["id"] == "template-mismatch"
        )
        mismatched_template["imageName"] = (
            "registry.digitalocean.com/lascade/tars:gpu-sha-"
            + "f" * 40
            + "@sha256:"
            + "a" * 64
        )

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.calls, [])
        self.assertIn(
            "endpoint-mismatch", {endpoint["id"] for endpoint in api.endpoints}
        )

    def test_prune_rejects_template_with_unproven_runtime_fields(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("e" * 40, "runtime-drift", now - timedelta(days=2))
        drifted_template = next(
            template
            for template in api.templates
            if template["id"] == "template-runtime-drift"
        )
        del drifted_template["env"]

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="a" * 40,
            now=now,
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.calls, [])
        self.assertIn(
            "endpoint-runtime-drift",
            {endpoint["id"] for endpoint in api.endpoints},
        )

    def test_prune_rejects_template_env_value_drift_before_mutation(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("a" * 40, "current", now)
        api.seed_release("e" * 40, "runtime-drift", now - timedelta(days=2))
        api.template_env = {"RUNPOD_INIT_TIMEOUT": "800"}

        with self.assertRaisesRegex(RunpodReleaseError, "REST template env"):
            prune_releases(
                api,
                current_endpoint_id="endpoint-current",
                previous_endpoint_id=None,
                protected_release_sha="a" * 40,
                now=now,
            )

        self.assertEqual(api.calls, [])
        self.assertIn(
            "endpoint-runtime-drift",
            {endpoint["id"] for endpoint in api.endpoints},
        )

    def test_prune_protects_the_observed_main_release_sha(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        api.seed_release("b" * 40, "current", now)
        api.seed_release("c" * 40, "observed-main", now - timedelta(days=2))

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha="c" * 40,
            now=now,
        )

        self.assertEqual(retired, 0)
        self.assertEqual(api.calls, [])
        self.assertIn(
            "endpoint-observed-main", {endpoint["id"] for endpoint in api.endpoints}
        )

    def test_newer_partial_release_survives_older_release_prune(self) -> None:
        now = datetime(2026, 7, 21, tzinfo=timezone.utc)
        api = FakeReleaseAPI()
        old_sha = "a" * 40
        deployed_sha = "b" * 40
        newer_sha = "c" * 40
        api.seed_release(old_sha, "old", now - timedelta(days=2))
        api.seed_release(deployed_sha, "current", now)
        api.seed_release(newer_sha, "newer-partial", now)
        api.endpoints = [
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] != "endpoint-newer-partial"
        ]
        next(
            template
            for template in api.templates
            if template["id"] == "template-newer-partial"
        )["boundEndpointId"] = None

        retired = prune_releases(
            api,
            current_endpoint_id="endpoint-current",
            previous_endpoint_id=None,
            protected_release_sha=newer_sha,
            now=now,
        )

        self.assertEqual(retired, 1)
        self.assertEqual(
            api.calls,
            [
                "zero:endpoint-old",
                "delete_endpoint:endpoint-old",
                "delete_template:tars-runpod-template-v1-" + old_sha,
                "delete_auth:auth-old",
            ],
        )
        remaining_templates = {item["id"] for item in api.templates}
        self.assertIn("template-newer-partial", remaining_templates)
        remaining_auths = {auth["id"] for auth in api.auths}
        self.assertIn("auth-newer-partial", remaining_auths)


class WorkflowContractTest(unittest.TestCase):
    ROOT = Path(__file__).resolve().parent.parent.parent

    def test_predecessor_selection_survives_failed_rollout_retry(self) -> None:
        endpoint_a = "endpoint-a"
        endpoint_b = "endpoint-b"

        selected, should_store = tars_runpod_release.select_previous_endpoint(
            current_endpoint_id=endpoint_a,
            target_endpoint_id=endpoint_b,
            stored_endpoint_id=None,
        )
        self.assertEqual(selected, endpoint_a)
        self.assertTrue(should_store)

        selected_on_retry, should_store_on_retry = (
            tars_runpod_release.select_previous_endpoint(
                current_endpoint_id=endpoint_b,
                target_endpoint_id=endpoint_b,
                stored_endpoint_id=selected,
            )
        )
        self.assertEqual(selected_on_retry, endpoint_a)
        self.assertFalse(should_store_on_retry)

        selected_for_next_release, should_store_for_next_release = (
            tars_runpod_release.select_previous_endpoint(
                current_endpoint_id=endpoint_b,
                target_endpoint_id="endpoint-c",
                stored_endpoint_id=selected_on_retry,
            )
        )
        self.assertEqual(selected_for_next_release, endpoint_b)
        self.assertTrue(should_store_for_next_release)

    def test_endpoint_selection_files_are_empty_when_no_endpoint_exists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "endpoint"
            tars_runpod_release.write_endpoint_file(path, None)
            self.assertEqual(path.read_bytes(), b"")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

            tars_runpod_release.write_endpoint_file(path, "endpoint-a")
            self.assertEqual(path.read_bytes(), b"endpoint-a\n")

    def test_deploy_workflow_has_immutable_and_non_cancelled_deploy_contract(self) -> None:
        import re

        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("types: [tars-deploy]", workflow)
        self.assertIn("id-token: write", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn("timeout-minutes: 360", workflow)
        self.assertIn("timeout-minutes: 120", workflow)
        self.assertIn(".tars-release-sha", workflow)
        self.assertIn("DEPLOY_SSH_KNOWN_HOSTS", workflow)
        self.assertEqual(workflow.count("Infisical/secrets-action@v1.0.16"), 3)
        self.assertEqual(workflow.count("method: universal"), 3)
        self.assertEqual(
            workflow.count("client-id: ${{ secrets.INFISICAL_CLIENT_ID }}"), 3
        )
        self.assertEqual(
            workflow.count("client-secret: ${{ secrets.INFISICAL_CLIENT_SECRET }}"), 3
        )
        self.assertNotIn("oidc-audience:", workflow)
        self.assertEqual(workflow.count('include-imports: "false"'), 3)
        self.assertEqual(workflow.count('recursive: "false"'), 3)
        self.assertEqual(workflow.count("secret-path: /deployment"), 2)
        self.assertEqual(workflow.count("secret-path: /runtime"), 1)
        self.assertEqual(workflow.count("tars_runner_secrets.py capture-build"), 1)
        self.assertEqual(workflow.count("tars_runner_secrets.py capture-deploy"), 1)
        self.assertNotIn("tars_infisical.py", workflow)
        self.assertNotIn("INFISICAL_TOKEN", workflow)
        self.assertIn(
            'cat "$RUNNER_TEMP/tars-deploy-secrets/remote-secrets.sh"', workflow
        )
        self.assertNotIn("ssh-keyscan", workflow)
        self.assertNotIn("sudo -n", workflow)
        self.assertNotIn(":latest", workflow)
        self.assertNotIn("@latest", workflow)
        self.assertEqual(workflow.count("ServerAliveInterval=15"), 3)
        self.assertEqual(workflow.count("ServerAliveCountMax=3"), 3)
        self.assertEqual(workflow.count("TCPKeepAlive=yes"), 3)
        self.assertNotIn("/pulls", workflow)
        self.assertNotIn("tars_tree_attestation.py", workflow)
        self.assertNotIn("permission-pull-requests", workflow)
        self.assertNotIn("permission-statuses", workflow)
        self.assertEqual(workflow.count("git/ref/heads/main"), 4)
        self.assertNotIn("gh api /installation", workflow)
        self.assertNotIn("Dockerfile.worker", workflow)
        self.assertNotIn("TARS_WORKER_IMAGE", workflow)
        self.assertIn(
            "DISPATCHER_DIGEST: ${{ steps.images.outputs.dispatcher_digest }}",
            workflow,
        )
        self.assertIn("GPU_DIGEST: ${{ steps.images.outputs.gpu_digest }}", workflow)
        self.assertIn("source/Dockerfile.dispatcher", workflow)
        self.assertIn("source/Dockerfile.gpu", workflow)
        self.assertNotIn("tars_worker_render_gate.py", workflow)
        self.assertIn("tars_tada_bundle.py", workflow)
        self.assertEqual(
            workflow.count("tada_bundle=${{ runner.temp }}/tada-bundle"), 2
        )
        self.assertEqual(workflow.count("tars_runpod_release.py ensure"), 1)
        self.assertEqual(workflow.count("tars_runpod_release.py pre-prune"), 1)
        self.assertEqual(workflow.count("tars_runpod_release.py prune"), 1)
        self.assertEqual(workflow.count("tars_registry_release.py probe"), 1)
        self.assertEqual(workflow.count("tars_registry_release.py verify"), 1)
        self.assertEqual(
            workflow.count("if: steps.existing-images.outputs."), 5
        )
        self.assertIn(
            ":gpu-sha-${{ steps.payload.outputs.sha }}@${{ steps.artifact.outputs.gpu_digest }}",
            workflow,
        )
        for component in ("api", "dispatcher", "gpu", "garage", "otel"):
            self.assertIn(
                f"{component}_digest: "
                f"${{{{ steps.images.outputs.{component}_digest }}}}",
                workflow,
            )
        self.assertNotIn("date -u +%Y-%m-%dT%H:%M:%SZ", workflow)
        self.assertIn(
            'git -C source show -s --format=%cI "$RELEASE_SHA"', workflow
        )
        self.assertIn(
            'git -C source show -s --format=%ct "$RELEASE_SHA"', workflow
        )
        self.assertEqual(workflow.count("provenance: false"), 5)
        self.assertEqual(
            workflow.count(
                "SOURCE_DATE_EPOCH: ${{ steps.metadata.outputs.source_date_epoch }}"
            ),
            5,
        )
        self.assertNotIn(
            'rm -rf "${DOCKER_CONFIG:-$RUNNER_TEMP/tars-docker-config}"', workflow
        )
        self.assertIn("--protected-release-sha", workflow)
        self.assertIn(
            "if: steps.prune-main.outputs.should_prune == 'true'", workflow
        )
        self.assertIn("current-runpod-endpoint", workflow)
        self.assertIn("tars_runpod_release.py select-previous", workflow)
        self.assertIn("/srv/tars/deployment/runpod-previous-endpoint", workflow)
        self.assertIn(".tars-runpod-predecessor-$RELEASE_SHA", workflow)
        self.assertIn('mv -f -- "$temporary" "$state"', workflow)
        self.assertIn("--runpod-endpoint-id", workflow)
        self.assertNotIn("ghcr_token=${{ github.token }}", workflow)
        build, deploy = workflow.split("\n  deploy:\n", 1)
        self.assertEqual(workflow.count("packages: read"), 1)
        self.assertIn("packages: read", build)
        self.assertNotIn("packages: read", deploy)
        self.assertNotIn("secret-path: /runtime", build)
        self.assertNotIn("RUNPOD_API_KEY", build)
        self.assertNotIn("DOCR_READ_USERNAME", build)
        self.assertNotIn("DOCR_READ_PASSWORD", build)
        self.assertIn("RUNPOD_API_KEY", deploy)
        self.assertIn("DOCR_READ_USERNAME", deploy)
        self.assertIn("DOCR_READ_PASSWORD", deploy)
        self.assertLess(
            deploy.index("Confirm the final current main revision"),
            deploy.index("Pass delegated runtime secrets"),
        )
        self.assertLess(
            deploy.index("Capture the currently deployed Runpod endpoint"),
            deploy.index(
                "Retire obsolete Runpod releases before reserving worker capacity"
            ),
        )
        self.assertLess(
            deploy.index(
                "Retire obsolete Runpod releases before reserving worker capacity"
            ),
            deploy.index("Provision or verify the immutable Runpod release"),
        )
        self.assertLess(
            deploy.index("Provision or verify the immutable Runpod release"),
            deploy.index("Pass delegated runtime secrets"),
        )
        self.assertLess(
            deploy.index("Pass delegated runtime secrets"),
            deploy.index("Mint a fresh post-rollout TARS GitHub App token"),
        )
        self.assertLess(
            deploy.index("Mint a fresh post-rollout TARS GitHub App token"),
            deploy.index("Reconfirm current main before Runpod retirement"),
        )
        self.assertLess(
            deploy.index("Reconfirm current main before Runpod retirement"),
            deploy.index("Retire expired Runpod releases"),
        )
        post_rollout_gate = deploy[
            deploy.index("Reconfirm current main before Runpod retirement") :
            deploy.index("Retire expired Runpod releases")
        ]
        self.assertIn('if [ "$RELEASE_SHA" != "$main_sha" ]; then', post_rollout_gate)
        self.assertIn(
            'echo "should_prune=false" >> "$GITHUB_OUTPUT"', post_rollout_gate
        )
        self.assertIn('echo "sha=$main_sha" >> "$GITHUB_OUTPUT"', post_rollout_gate)
        self.assertIn(
            "GH_TOKEN: ${{ steps.prune-app-token.outputs.token }}",
            post_rollout_gate,
        )
        bootstrap = deploy.index('"$bundle/deploy/tars-deploy" bootstrap-stateful')
        application = deploy.index('exec "$bundle/deploy/tars-deploy" deploy')
        self.assertLess(bootstrap, application)
        bootstrap_command = deploy[bootstrap:application]
        self.assertIn('--bundle-dir "$bundle" || exit', bootstrap_command)
        self.assertNotIn("stateful_record=", deploy)

    def test_delivery_workflow_uses_latest_reviewed_action_versions(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        for reference in (
            "actions/checkout@v7.0.0",
            "actions/create-github-app-token@v3.2.0",
            "Infisical/secrets-action@v1.0.16",
            "docker/setup-buildx-action@v4.2.0",
            "docker/build-push-action@v7.3.0",
            "actions/upload-artifact@v7.0.1",
            "actions/download-artifact@v8.0.1",
            "rohittp0/wiregaurd@v3",
        ):
            self.assertIn(reference, workflow)

    def test_ci_runs_only_core_collaboration_checks(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("\n  verify:\n", workflow)
        self.assertIn("\n  report:\n", workflow)
        self.assertNotIn("\n  prepare:\n", workflow)
        self.assertNotIn("\n  worker_gate:\n", workflow)
        self.assertIn("go test -count=1 ./...", workflow)
        self.assertIn("unittest discover -s release", workflow)
        self.assertIn("unittest discover -s deploy", workflow)
        self.assertIn("unittest discover -s runpod_worker", workflow)
        self.assertIn("services:\n      postgres:", workflow)
        self.assertIn("TARS_TEST_DATABASE_URL:", workflow)
        self.assertIn(
            "postgres@sha256:bb377b7239d2774ac8cc76f481596ce96c5a6b5e9d141f6d0a0ee371a6e7c0f2",
            workflow,
        )
        for removed_check in (
            "setup-opentofu",
            "build-push-action",
            "tars_tree_attestation.py",
            "tars_worker_render_gate.py",
            "go vet",
            "go generate",
        ):
            self.assertNotIn(removed_check, workflow)
        self.assertEqual(workflow.count("context='TARS Central CI'"), 1)
        self.assertNotIn("TARS Central CI tree", workflow)

    def test_ci_merges_current_main_into_the_exact_dispatched_head(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("ref: ${{ steps.payload.outputs.head_sha }}", workflow)
        self.assertIn("fetch-depth: 0", workflow)
        self.assertIn("merge --no-commit --no-ff origin/main", workflow)
        self.assertIn("EXPECTED_HEAD_SHA", workflow)
        self.assertNotIn("refs/pull/{0}/merge", workflow)
        self.assertNotIn("tars_tree_attestation.py", workflow)

    def test_ci_separates_status_credentials_from_pull_request_execution(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-ci.yml").read_text(
            encoding="utf-8"
        )
        verify, report = workflow.split("\n  report:\n", 1)

        self.assertEqual(workflow.count("permission-statuses: write"), 1)
        self.assertNotIn("permission-statuses: write", verify)
        self.assertIn("permission-statuses: write", report)
        self.assertNotIn("packages: read", verify)
        self.assertNotIn("path: source", report)
        self.assertIn("needs: verify", report)
        self.assertIn("if: always()", report)
        self.assertEqual(workflow.count("ref: ${{ github.sha }}"), 2)

    def test_delivery_jobs_pin_the_central_checkout_to_the_dispatch_sha(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(workflow.count("ref: ${{ github.sha }}"), 2)

    def test_tars_ci_uses_latest_reviewed_action_versions(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-ci.yml").read_text(
            encoding="utf-8"
        )
        for reference in (
            "actions/checkout@v7.0.0",
            "actions/create-github-app-token@v3.2.0",
            "actions/setup-go@v6.4.0",
        ):
            self.assertIn(reference, workflow)


if __name__ == "__main__":
    unittest.main()
