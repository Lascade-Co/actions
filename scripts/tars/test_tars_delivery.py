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
    CONNECTION_KEYS,
    DEPLOY_KEYS,
    DEPLOYMENT_KEYS,
    RUNTIME_KEYS,
    RunnerSecretError,
    capture_build,
    capture_connection,
    capture_deploy,
)
from tars_runpod_release import (
    ENDPOINT_TIMEOUT_MS,
    GPU_POOL_SELECTOR,
    Inventory,
    RunpodClient,
    RunpodReleaseError,
    _NoRedirectHandler,
    read_secret,
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
            "RUNPOD_ENDPOINT_ID": "stable-endpoint",
            "RUNPOD_TEMPLATE_ID": "stable-template",
            "RUNPOD_REGISTRY_AUTH_ID": "stable-auth",
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

    def test_connection_capture_requires_only_connection_secrets(self) -> None:
        environment = {name: self.environment()[name] for name in CONNECTION_KEYS}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_directory = root / "captured"
            github_output = root / "github-output"
            github_env = root / "github-env"
            capture_connection(
                environment,
                output_directory,
                github_output,
                github_env,
            )
            self.assertEqual(
                {path.name for path in output_directory.iterdir()},
                {"DEPLOY_SSH_PRIVATE_KEY", "DEPLOY_SSH_KNOWN_HOSTS"},
            )
            for path in output_directory.iterdir():
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            rendered = github_output.read_text(encoding="utf-8")
            self.assertIn("host<<", rendered)
            self.assertIn("10.20.30.40", rendered)
            self.assertIn("user<<", rendered)
            self.assertIn("ubuntu", rendered)
            self.assertIn("wireguard_config<<", rendered)
            self.assertEqual(
                github_env.read_text(encoding="utf-8"),
                "".join(
                    f"{name}=\n" for name in (*DEPLOYMENT_KEYS, *RUNTIME_KEYS)
                ),
            )

    def test_connection_capture_clears_exports_on_validation_failure(self) -> None:
        environment = {name: self.environment()[name] for name in CONNECTION_KEYS}
        environment["DEPLOY_SSH_HOST"] = "control.example.com"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RunnerSecretError, "literal IPv4"):
                capture_connection(
                    environment,
                    root / "captured",
                    root / "github-output",
                    root / "github-env",
                )
            self.assertEqual(
                (root / "github-env").read_text(encoding="utf-8"),
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
                "RUNPOD_ENDPOINT_ID",
                "RUNPOD_TEMPLATE_ID",
                "RUNPOD_REGISTRY_AUTH_ID",
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
    OMIT_TEMPLATE_ENV = object()

    def __init__(self) -> None:
        self.endpoints: list[dict] = []
        self.templates: list[dict] = []
        self.auths: list[dict] = []
        self.calls: list[str] = []
        self.template_env = {"RUNPOD_INIT_TIMEOUT": "1200"}
        self.template_env_by_id: dict[str, object] = {}
        self.health_by_endpoint: dict[str, list[object]] = {}
        self.health_calls: list[str] = []
        self.update_observer = None

    def inventory(self) -> Inventory:
        return Inventory(tuple(self.endpoints), tuple(self.templates), tuple(self.auths))

    def read_endpoint(self, endpoint_id: str) -> dict:
        endpoint = next(item for item in self.endpoints if item["id"] == endpoint_id)
        template = next(
            (
                item
                for item in self.templates
                if item["id"] == endpoint["templateId"]
            ),
            None,
        )
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
                "NVIDIA GeForce RTX 4090",
            ],
            "version": endpoint.get("version", 0),
            "workers": [dict(worker) for worker in endpoint.get("workers", [])],
            "template": self._rest_template(template) if template is not None else {},
        }

    def read_template(self, template_id: str) -> dict:
        template = next(item for item in self.templates if item["id"] == template_id)
        resource = self._rest_template(template)
        environment = (
            self.template_env_by_id[template_id]
            if template_id in self.template_env_by_id
            else dict(self.template_env)
        )
        if environment is not self.OMIT_TEMPLATE_ENV:
            resource["env"] = environment
        else:
            resource.pop("env", None)
        return resource

    @staticmethod
    def _rest_template(template: dict) -> dict:
        return {
            "id": template["id"],
            "name": template["name"],
            "imageName": template.get("imageName"),
            "containerRegistryAuthId": template.get("containerRegistryAuthId"),
            "containerDiskInGb": template.get("containerDiskInGb"),
            "volumeInGb": template.get("volumeInGb"),
            "volumeMountPath": template.get("volumeMountPath", "/workspace"),
            "dockerEntrypoint": list(template.get("dockerEntrypoint", [])),
            "dockerStartCmd": list(template.get("dockerStartCmd", [])),
            "env": {"RUNPOD_INIT_TIMEOUT": "1200"},
            "isPublic": template.get("isPublic", False),
            "isServerless": template.get("isServerless"),
            "ports": list(template.get("ports", [])),
            "readme": template.get("readme", ""),
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

    def update_template(
        self, template_id: str, name: str, image: str, auth_id: str
    ) -> dict:
        self.calls.append(f"update_template:{image}")
        if self.update_observer is not None:
            self.update_observer()
        template = next(item for item in self.templates if item["id"] == template_id)
        template.update(
            {
                "name": name,
                "imageName": image,
                "containerRegistryAuthId": auth_id,
                "containerDiskInGb": 20,
                "volumeInGb": 0,
                "volumeMountPath": "/workspace",
                "dockerEntrypoint": [],
                "dockerStartCmd": [],
                "isPublic": False,
                "isServerless": True,
                "ports": [],
                "readme": "",
            }
        )
        for endpoint in self.endpoints:
            if endpoint["templateId"] == template_id:
                endpoint["version"] = endpoint.get("version", 0) + 1
        return self.read_template(template_id)

    def rename_endpoint(self, endpoint_id: str, template_id: str) -> dict:
        self.calls.append(f"rename_endpoint:{endpoint_id}")
        endpoint = next(item for item in self.endpoints if item["id"] == endpoint_id)
        if endpoint["templateId"] != template_id:
            raise AssertionError("fake endpoint/template mismatch")
        endpoint["name"] = "tars-runpod-endpoint-v2"
        return self.read_endpoint(endpoint_id)

    def add_ada24_fallback(
        self, endpoint_id: str, template_id: str
    ) -> dict:
        self.calls.append(f"add_ada24_fallback:{endpoint_id}")
        endpoint = next(item for item in self.endpoints if item["id"] == endpoint_id)
        if endpoint["templateId"] != template_id:
            raise AssertionError("fake endpoint/template mismatch")
        endpoint["gpuIds"] = GPU_POOL_SELECTOR
        endpoint["version"] = endpoint.get("version", 0) + 1
        return self.read_endpoint(endpoint_id)

    @staticmethod
    def endpoint(
        *, endpoint_id: str, name: str, template_id: str, created: datetime
    ) -> dict:
        return {
            "id": endpoint_id,
            "name": name,
            "type": "QB",
            "gpuIds": GPU_POOL_SELECTOR,
            "idleTimeout": 5,
            "locations": "",
            "networkVolumeId": None,
            "flashBootType": "OFF",
            "scalerType": "REQUEST_COUNT",
            "scalerValue": 1,
            "templateId": template_id,
            "workersMax": 2,
            "workersMin": 0,
            "gpuCount": 1,
            "computeType": "GPU",
            "executionTimeoutMs": 7_200_000,
            "createdAt": created.isoformat().replace("+00:00", "Z"),
            "version": 0,
            "pods": [],
            "workers": [],
        }

    def seed_stable(self, image: str, *, version: int = 7) -> None:
        self.auths.append(
            {
                "id": "stable-auth",
                "name": "tars-runpod-auth-v2-" + "f" * 12,
            }
        )
        self.templates.append(
            {
                "id": "stable-template",
                "name": "tars-runpod-template-v2",
                "imageName": image,
                "isServerless": True,
                "containerDiskInGb": 20,
                "volumeInGb": 0,
                "volumeMountPath": "/workspace",
                "dockerArgs": None,
                "dockerEntrypoint": [],
                "dockerStartCmd": [],
                "env": [{"key": "RUNPOD_INIT_TIMEOUT"}],
                "isPublic": False,
                "ports": [],
                "readme": "",
                "containerRegistryAuthId": "stable-auth",
                "boundEndpointId": "stable-endpoint",
            }
        )
        endpoint = self.endpoint(
            endpoint_id="stable-endpoint",
            name="tars-runpod-endpoint-v2",
            template_id="stable-template",
            created=datetime.now(timezone.utc),
        )
        endpoint["version"] = version
        self.endpoints.append(endpoint)

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


class RacingWorkerInventoryAPI(FakeReleaseAPI):
    ACTIVE_MISSING_REST = "active-missing-rest"
    REST_MISSING_STATUS = "rest-missing-status"

    def __init__(self, race: str, transient_reads: int) -> None:
        super().__init__()
        if race not in (
            self.ACTIVE_MISSING_REST,
            self.REST_MISSING_STATUS,
        ):
            raise ValueError("invalid worker inventory race")
        if transient_reads < 0:
            raise ValueError("transient reads must not be negative")
        self.race = race
        self.transient_reads = transient_reads
        self.correlation_inventory_calls = 0
        self.correlation_endpoint_calls = 0

    def inventory(self) -> Inventory:
        self.correlation_inventory_calls += 1
        inventory = super().inventory()
        if (
            self.race == self.REST_MISSING_STATUS
            and self.correlation_inventory_calls <= self.transient_reads
        ):
            endpoints = tuple(
                {**endpoint, "pods": []}
                if endpoint.get("id") == "stable-endpoint"
                else endpoint
                for endpoint in inventory.endpoints
            )
            return Inventory(endpoints, inventory.templates, inventory.auths)
        return inventory

    def read_endpoint(self, endpoint_id: str) -> dict:
        self.correlation_endpoint_calls += 1
        endpoint = super().read_endpoint(endpoint_id)
        if (
            self.race == self.ACTIVE_MISSING_REST
            and self.correlation_endpoint_calls <= self.transient_reads
        ):
            endpoint["workers"] = []
        return endpoint


class RunpodReleaseTest(unittest.TestCase):
    IMAGE = (
        "registry.digitalocean.com/lascade/tars:gpu-sha-"
        + "a" * 40
        + "@sha256:"
        + "a" * 64
    )
    TARGET_IMAGE = (
        "registry.digitalocean.com/lascade/tars:gpu-sha-"
        + "b" * 40
        + "@sha256:"
        + "b" * 64
    )
    IMAGE_WITH_DIFFERENT_DIGEST = (
        "registry.digitalocean.com/lascade/tars:gpu-sha-"
        + "a" * 40
        + "@sha256:"
        + "c" * 64
    )
    LEGACY_APPLICATION_IMAGE = (
        "registry.digitalocean.com/lascade/tars@sha256:" + "a" * 64
    )
    LEGACY_APPLICATION_IMAGE_WITH_DIFFERENT_DIGEST = (
        "registry.digitalocean.com/lascade/tars@sha256:" + "c" * 64
    )
    LEGACY_TARGET_IMAGE = (
        "registry.digitalocean.com/lascade/tars@sha256:" + "b" * 64
    )
    OTHER_RELEASE_WITH_SAME_DIGEST = (
        "registry.digitalocean.com/lascade/tars:gpu-sha-"
        + "b" * 40
        + "@sha256:"
        + "a" * 64
    )

    @staticmethod
    def stable_ids() -> tars_runpod_release.StableResourceIDs:
        return tars_runpod_release.StableResourceIDs(
            "stable-endpoint", "stable-template", "stable-auth"
        )

    def racing_worker_api(
        self,
        race: str,
        transient_reads: int,
        *,
        desired_status: str = "RUNNING",
        worker_image: str | None = None,
    ) -> RacingWorkerInventoryAPI:
        api = RacingWorkerInventoryAPI(race, transient_reads)
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.endpoints[0]["pods"] = [
            {"id": "worker", "desiredStatus": desired_status}
        ]
        api.endpoints[0]["workers"] = [
            {
                "id": "worker",
                "image": worker_image or self.TARGET_IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 8,
            }
        ]
        return api

    @classmethod
    def stable_update_receipt(
        cls,
        *,
        prior_version: int = 7,
        target_version: int | None = 8,
    ) -> tars_runpod_release.StableRolloutReceipt:
        return tars_runpod_release.StableRolloutReceipt(
            endpoint_id="stable-endpoint",
            template_id="stable-template",
            auth_id="stable-auth",
            baseline="existing",
            prior_release_sha="a" * 40,
            prior_app_gpu_image=cls.IMAGE,
            release_sha="b" * 40,
            target_image=cls.TARGET_IMAGE,
            prior_image=cls.IMAGE,
            prior_version=prior_version,
            target_version=target_version,
            mode="update",
        )

    def test_stable_stage_updates_in_place_and_writes_receipt_before_mutation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            receipt_path = Path(directory) / "rollout.json"

            def verify_prepared_receipt() -> None:
                receipt = tars_runpod_release.read_rollout_receipt(receipt_path)
                self.assertEqual(receipt.prior_image, self.IMAGE)
                self.assertEqual(receipt.prior_version, 7)
                self.assertIsNone(receipt.target_version)

            api.update_observer = verify_prepared_receipt
            receipt = tars_runpod_release.stage_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=receipt_path,
            )

            self.assertEqual(
                api.calls, [f"update_template:{self.TARGET_IMAGE}"]
            )
            self.assertNotIn("create_auth", api.calls)
            self.assertNotIn("create_template", api.calls)
            self.assertNotIn("create_endpoint", api.calls)
            self.assertEqual(receipt.endpoint_id, "stable-endpoint")
            self.assertEqual(receipt.template_id, "stable-template")
            self.assertEqual(receipt.auth_id, "stable-auth")
            self.assertEqual(receipt.target_version, 8)
            self.assertEqual(receipt_path.stat().st_mode & 0o777, 0o600)
            stored = tars_runpod_release.read_rollout_receipt(receipt_path)
            self.assertEqual(stored.prior_image, self.IMAGE)
            self.assertEqual(stored.target_image, self.TARGET_IMAGE)
            self.assertEqual(stored.target_version, 8)

    def test_stable_stage_waits_for_standalone_rest_template(self) -> None:
        class StaleRESTTemplateAPI(FakeReleaseAPI):
            def read_template(self, template_id: str) -> dict:
                template = super().read_template(template_id)
                if template["imageName"] == RunpodReleaseTest.TARGET_IMAGE:
                    template["imageName"] = RunpodReleaseTest.IMAGE
                return template

        api = StaleRESTTemplateAPI()
        api.seed_stable(self.IMAGE, version=7)
        ticks = iter((0.0, 0.0, 1.0))
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                RunpodReleaseError, "did not converge"
            ):
                tars_runpod_release.stage_stable_release(
                    api,
                    ids=self.stable_ids(),
                    release_sha="b" * 40,
                    gpu_image=self.TARGET_IMAGE,
                    prior_release_sha="a" * 40,
                    prior_app_gpu_image=self.IMAGE,
                    greenfield=False,
                    receipt_path=Path(directory) / "rollout.json",
                    timeout_seconds=0.5,
                    poll_seconds=0.1,
                    sleeper=lambda _seconds: None,
                    clock=lambda: next(ticks),
                )
        self.assertEqual(
            api.calls, [f"update_template:{self.TARGET_IMAGE}"]
        )

    def test_durable_prepared_boundary_recovers_after_runner_receipt_loss(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            durable = Path(directory) / "control-boundary.json"
            runner = Path(directory) / "runner-receipt.json"
            prepared = tars_runpod_release.prepare_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=durable,
            )
            tars_runpod_release.write_rollout_receipt(runner, prepared)

            tars_runpod_release.stage_prepared_stable_release(
                api,
                receipt=prepared,
                receipt_path=runner,
            )
            self.assertEqual(api.templates[0]["imageName"], self.TARGET_IMAGE)

            # A replacement runner has only the control-node boundary written
            # before mutation, not the runner-local post-stage receipt.
            tars_runpod_release.rollback_stable_release(
                api,
                receipt=tars_runpod_release.read_rollout_receipt(durable),
            )

        self.assertEqual(api.templates[0]["imageName"], self.IMAGE)
        self.assertEqual(api.endpoints[0]["version"], 9)

    def test_prepare_refuses_live_app_and_provider_image_mismatch(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            boundary = Path(directory) / "boundary.json"
            with self.assertRaisesRegex(
                RunpodReleaseError, "GPU image tag does not match"
            ):
                tars_runpod_release.prepare_stable_release(
                    api,
                    ids=self.stable_ids(),
                    release_sha="b" * 40,
                    gpu_image=self.TARGET_IMAGE,
                    prior_release_sha="b" * 40,
                    prior_app_gpu_image=self.TARGET_IMAGE,
                    greenfield=False,
                    receipt_path=boundary,
                )
            self.assertFalse(boundary.exists())
        self.assertEqual(api.calls, [])

    def test_prepare_refuses_same_release_sha_with_different_gpu_digest(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                RunpodReleaseError, "rollback image digests do not match"
            ):
                tars_runpod_release.prepare_stable_release(
                    api,
                    ids=self.stable_ids(),
                    release_sha="b" * 40,
                    gpu_image=self.TARGET_IMAGE,
                    prior_release_sha="a" * 40,
                    prior_app_gpu_image=self.IMAGE_WITH_DIFFERENT_DIGEST,
                    greenfield=False,
                    receipt_path=Path(directory) / "boundary.json",
                )
        self.assertEqual(api.calls, [])

    def test_stage_refuses_provider_drift_after_boundary_persistence(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            boundary = Path(directory) / "boundary.json"
            receipt = tars_runpod_release.prepare_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=boundary,
            )
            api.templates[0]["imageName"] = self.TARGET_IMAGE
            api.endpoints[0]["version"] = 8
            with self.assertRaisesRegex(
                RunpodReleaseError, "stable template image"
            ):
                tars_runpod_release.stage_prepared_stable_release(
                    api,
                    receipt=receipt,
                    receipt_path=boundary,
                )
        self.assertEqual(api.calls, [])

    def test_stage_rereads_exact_provider_boundary_after_idle_wait(self) -> None:
        class DriftingReleaseAPI(FakeReleaseAPI):
            def read_endpoint_health(
                self, endpoint_id: str
            ) -> tars_runpod_release.EndpointHealth:
                health = super().read_endpoint_health(endpoint_id)
                if len(self.health_calls) == 2:
                    self.templates[0]["imageName"] = (
                        RunpodReleaseTest.IMAGE_WITH_DIFFERENT_DIGEST
                    )
                return health

        api = DriftingReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            boundary = Path(directory) / "boundary.json"
            receipt = tars_runpod_release.prepare_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=boundary,
            )
            api.health_by_endpoint["stable-endpoint"] = [
                tars_runpod_release.EndpointHealth(1, 0),
                tars_runpod_release.EndpointHealth(0, 0),
            ]
            with self.assertRaisesRegex(
                RunpodReleaseError, "stable template image"
            ):
                tars_runpod_release.stage_prepared_stable_release(
                    api,
                    receipt=receipt,
                    receipt_path=boundary,
                    sleeper=lambda _seconds: None,
                )
        self.assertEqual(api.calls, [])

    def test_application_baseline_is_exact_and_owner_only(self) -> None:
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            endpoint_id="stable-endpoint",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "boundary.json"
            tars_runpod_release.write_application_baseline(path, baseline)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                tars_runpod_release.read_application_baseline(path),
                baseline,
            )
            document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            set(document),
            {
                "baseline",
                "endpoint_id",
                "gpu_image",
                "kind",
                "release_sha",
                "replicas",
            },
        )
        self.assertEqual(document["kind"], "application")
        self.assertEqual(document["replicas"], 1)

    def test_application_baseline_round_trips_legacy_digest_only_image(
        self,
    ) -> None:
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.LEGACY_APPLICATION_IMAGE,
            endpoint_id="stable-endpoint",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "boundary.json"
            tars_runpod_release.write_application_baseline(path, baseline)

            self.assertEqual(
                tars_runpod_release.read_application_baseline(path),
                baseline,
            )
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8"))["gpu_image"],
                self.LEGACY_APPLICATION_IMAGE,
            )

    def test_application_baseline_rejects_wrong_exact_sha_tag(self) -> None:
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.TARGET_IMAGE,
            endpoint_id="stable-endpoint",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "boundary.json"
            with self.assertRaisesRegex(
                RunpodReleaseError,
                "application GPU image tag does not match",
            ):
                tars_runpod_release.write_application_baseline(path, baseline)
            self.assertFalse(path.exists())

    def test_application_baseline_rejects_other_digest_only_shapes(
        self,
    ) -> None:
        invalid_images = (
            "registry.example.com/lascade/tars@sha256:" + "a" * 64,
            "registry.digitalocean.com/lascade/tars:latest",
            "registry.digitalocean.com/lascade/tars@sha256:" + "a" * 63,
            "registry.digitalocean.com/lascade/tars@sha256:" + "A" * 64,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, image in enumerate(invalid_images):
                with self.subTest(image=image):
                    path = root / f"boundary-{index}.json"
                    baseline = (
                        tars_runpod_release.ApplicationRolloutBaseline(
                            release_sha="a" * 40,
                            gpu_image=image,
                            endpoint_id="stable-endpoint",
                        )
                    )
                    with self.assertRaisesRegex(
                        RunpodReleaseError,
                        "immutable TARS DOCR digest",
                    ):
                        tars_runpod_release.write_application_baseline(
                            path, baseline
                        )
                    self.assertFalse(path.exists())

    def test_prepare_accepts_legacy_app_reference_for_strict_provider(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            boundary = Path(directory) / "boundary.json"
            receipt = tars_runpod_release.prepare_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.LEGACY_APPLICATION_IMAGE,
                greenfield=False,
                receipt_path=boundary,
            )
            stored = tars_runpod_release.read_rollout_receipt(boundary)

        self.assertEqual(
            receipt.prior_app_gpu_image,
            self.LEGACY_APPLICATION_IMAGE,
        )
        self.assertEqual(receipt.prior_image, self.IMAGE)
        self.assertEqual(stored, receipt)
        self.assertEqual(api.calls, [])

    def test_rollout_receipt_rejects_legacy_app_provider_mismatch(
        self,
    ) -> None:
        cases = (
            (
                "digest",
                "a" * 40,
                self.LEGACY_APPLICATION_IMAGE_WITH_DIFFERENT_DIGEST,
                self.IMAGE,
                "rollback image digests do not match",
            ),
            (
                "release",
                "b" * 40,
                self.LEGACY_APPLICATION_IMAGE,
                self.IMAGE,
                "GPU image tag does not match",
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for (
                label,
                prior_release_sha,
                prior_app_image,
                provider_image,
                message,
            ) in cases:
                with self.subTest(label=label):
                    path = root / f"{label}.json"
                    receipt = tars_runpod_release.StableRolloutReceipt(
                        endpoint_id="stable-endpoint",
                        template_id="stable-template",
                        auth_id="stable-auth",
                        baseline="existing",
                        prior_release_sha=prior_release_sha,
                        prior_app_gpu_image=prior_app_image,
                        release_sha="b" * 40,
                        target_image=self.TARGET_IMAGE,
                        prior_image=provider_image,
                        prior_version=7,
                        target_version=8,
                        mode="update",
                    )
                    with self.assertRaisesRegex(
                        RunpodReleaseError, message
                    ):
                        tars_runpod_release.write_rollout_receipt(
                            path, receipt
                        )
                    self.assertFalse(path.exists())

    def test_provider_and_target_images_remain_strictly_tagged(self) -> None:
        cases = (
            ("target", self.IMAGE, self.LEGACY_TARGET_IMAGE),
            ("provider", self.LEGACY_APPLICATION_IMAGE, self.TARGET_IMAGE),
        )
        for label, provider_image, target_image in cases:
            with self.subTest(label=label):
                api = FakeReleaseAPI()
                api.seed_stable(provider_image, version=7)
                with tempfile.TemporaryDirectory() as directory:
                    boundary = Path(directory) / "boundary.json"
                    with self.assertRaisesRegex(
                        RunpodReleaseError,
                        (
                            "exact-SHA tag and digest"
                            if label == "target"
                            else "stable template image is not immutable"
                        ),
                    ):
                        tars_runpod_release.prepare_stable_release(
                            api,
                            ids=self.stable_ids(),
                            release_sha="b" * 40,
                            gpu_image=target_image,
                            prior_release_sha="a" * 40,
                            prior_app_gpu_image=(
                                self.LEGACY_APPLICATION_IMAGE
                            ),
                            greenfield=False,
                            receipt_path=boundary,
                        )
                    self.assertFalse(boundary.exists())

    def test_provider_image_projects_to_exact_application_digest_reference(
        self,
    ) -> None:
        self.assertEqual(
            tars_runpod_release._application_image_for_provider(
                "a" * 40, self.IMAGE
            ),
            self.LEGACY_APPLICATION_IMAGE,
        )
        with self.assertRaisesRegex(
            RunpodReleaseError, "exact-SHA tag and digest"
        ):
            tars_runpod_release._application_image_for_provider(
                "a" * 40, self.LEGACY_APPLICATION_IMAGE
            )

    def test_boundary_readers_reject_duplicate_json_keys_recursively(self) -> None:
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            endpoint_id="stable-endpoint",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            application_path = root / "application.json"
            tars_runpod_release.write_application_baseline(
                application_path, baseline
            )
            application_path.write_text(
                application_path.read_text(encoding="utf-8").replace(
                    '"endpoint_id":"stable-endpoint"',
                    (
                        '"endpoint_id":"stable-endpoint",'
                        '"endpoint_id":"shadow-endpoint"'
                    ),
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                RunpodReleaseError, "duplicate JSON object key"
            ):
                tars_runpod_release.read_application_baseline(application_path)

            receipt_path = root / "receipt.json"
            tars_runpod_release.write_rollout_receipt(
                receipt_path, self.stable_update_receipt()
            )
            receipt_path.write_text(
                receipt_path.read_text(encoding="utf-8").replace(
                    '"target_version":8',
                    '"target_version":{"version":8,"version":9}',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                RunpodReleaseError, "duplicate JSON object key"
            ):
                tars_runpod_release.read_rollout_receipt(receipt_path)

    def test_rollout_receipt_rejects_boolean_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for field, message in (
                ("prior_version", "invalid prior version"),
                ("target_version", "invalid target version"),
            ):
                with self.subTest(field=field):
                    path = root / f"{field}.json"
                    tars_runpod_release.write_rollout_receipt(
                        path, self.stable_update_receipt()
                    )
                    path.write_text(
                        path.read_text(encoding="utf-8").replace(
                            f'"{field}":'
                            + ("7" if field == "prior_version" else "8"),
                            f'"{field}":true',
                        ),
                        encoding="utf-8",
                    )
                    with self.assertRaisesRegex(
                        RunpodReleaseError, message
                    ):
                        tars_runpod_release.read_rollout_receipt(path)

    def test_verify_application_accepts_only_the_current_worker_generation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["pods"] = [
            {"id": "historical", "desiredStatus": "EXITED"},
            {"id": "current", "desiredStatus": "RUNNING"},
        ]
        api.endpoints[0]["workers"] = [
            {
                "id": "historical",
                "image": self.TARGET_IMAGE,
                "containerRegistryAuthId": "old-auth",
                "slsVersion": 6,
            },
            {
                "id": "current",
                "image": self.IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 7,
            },
        ]
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            endpoint_id="stable-endpoint",
        )

        version = tars_runpod_release.verify_application_generation(
            api,
            baseline=baseline,
            ids=self.stable_ids(),
        )

        self.assertEqual(version, 7)
        self.assertEqual(api.calls, [])

    def test_verify_application_accepts_legacy_app_reference_for_provider(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.LEGACY_APPLICATION_IMAGE,
            endpoint_id="stable-endpoint",
        )

        version = tars_runpod_release.verify_application_generation(
            api,
            baseline=baseline,
            ids=self.stable_ids(),
        )

        self.assertEqual(version, 7)
        self.assertEqual(api.calls, [])

    def test_verify_application_rejects_legacy_app_provider_drift(
        self,
    ) -> None:
        cases = (
            (
                "digest",
                self.IMAGE_WITH_DIFFERENT_DIGEST,
                "rollback image digests do not match",
            ),
            (
                "release",
                self.OTHER_RELEASE_WITH_SAME_DIGEST,
                "GPU image tag does not match",
            ),
        )
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.LEGACY_APPLICATION_IMAGE,
            endpoint_id="stable-endpoint",
        )
        for label, provider_image, message in cases:
            with self.subTest(label=label):
                api = FakeReleaseAPI()
                api.seed_stable(provider_image, version=7)
                with self.assertRaisesRegex(
                    RunpodReleaseError, message
                ):
                    tars_runpod_release.verify_application_generation(
                        api,
                        baseline=baseline,
                        ids=self.stable_ids(),
                    )
                self.assertEqual(api.calls, [])

    def test_verify_application_rejects_endpoint_or_active_worker_drift(
        self,
    ) -> None:
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            endpoint_id="stable-endpoint",
        )
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with self.assertRaisesRegex(
            RunpodReleaseError, "application baseline stable endpoint"
        ):
            tars_runpod_release.verify_application_generation(
                api,
                baseline=baseline,
                ids=tars_runpod_release.StableResourceIDs(
                    "other-endpoint", "stable-template", "stable-auth"
                ),
            )
        self.assertEqual(api.calls, [])

        for field, value, message in (
            ("image", self.TARGET_IMAGE, "active worker image"),
            (
                "containerRegistryAuthId",
                "old-auth",
                "active worker registry auth",
            ),
            ("slsVersion", 6, "active worker slsVersion"),
        ):
            with self.subTest(field=field):
                api = FakeReleaseAPI()
                api.seed_stable(self.IMAGE, version=7)
                api.endpoints[0]["pods"] = [
                    {"id": "worker", "desiredStatus": "RUNNING"}
                ]
                worker = {
                    "id": "worker",
                    "image": self.IMAGE,
                    "containerRegistryAuthId": "stable-auth",
                    "slsVersion": 7,
                }
                worker[field] = value
                api.endpoints[0]["workers"] = [worker]
                with self.assertRaisesRegex(RunpodReleaseError, message):
                    tars_runpod_release.verify_application_generation(
                        api,
                        baseline=baseline,
                        ids=self.stable_ids(),
                    )
                self.assertEqual(api.calls, [])

    def test_verify_application_cli_reads_all_stable_ids_without_mutation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        baseline = tars_runpod_release.ApplicationRolloutBaseline(
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            endpoint_id="stable-endpoint",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boundary = root / "application.json"
            tars_runpod_release.write_application_baseline(boundary, baseline)
            original_boundary = boundary.read_bytes()
            secret_values = {
                "api-key": "runpod-key",
                "endpoint-id": "stable-endpoint",
                "template-id": "stable-template",
                "auth-id": "stable-auth",
            }
            secret_paths: dict[str, Path] = {}
            for name, value in secret_values.items():
                path = root / name
                path.write_text(value, encoding="utf-8")
                path.chmod(0o600)
                secret_paths[name] = path
            arguments = [
                "tars_runpod_release.py",
                "verify-application",
                "--api-key-file",
                str(secret_paths["api-key"]),
                "--boundary-file",
                str(boundary),
                "--endpoint-id-file",
                str(secret_paths["endpoint-id"]),
                "--template-id-file",
                str(secret_paths["template-id"]),
                "--auth-id-file",
                str(secret_paths["auth-id"]),
            ]
            missing_endpoint_arguments = [
                argument
                for index, argument in enumerate(arguments)
                if index not in (6, 7)
            ]
            with (
                mock.patch("sys.argv", missing_endpoint_arguments),
                mock.patch("sys.stderr", io.StringIO()),
                self.assertRaises(SystemExit) as exit_error,
            ):
                tars_runpod_release.main()
            self.assertEqual(exit_error.exception.code, 2)
            with (
                mock.patch(
                    "sys.argv",
                    arguments,
                ),
                mock.patch.object(
                    tars_runpod_release,
                    "RunpodClient",
                    return_value=api,
                ),
            ):
                tars_runpod_release.main()

            self.assertEqual(boundary.read_bytes(), original_boundary)
        self.assertEqual(api.calls, [])

    def test_greenfield_boundary_requires_bootstrapped_target_and_no_prior_app(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=3)
        with tempfile.TemporaryDirectory() as directory:
            boundary = Path(directory) / "boundary.json"
            receipt = tars_runpod_release.stage_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha=None,
                prior_app_gpu_image=None,
                greenfield=True,
                receipt_path=boundary,
            )
            tars_runpod_release.rollback_stable_release(
                api, receipt=receipt
            )

        self.assertEqual(receipt.baseline, "greenfield")
        self.assertEqual(receipt.mode, "noop")
        self.assertEqual(api.templates[0]["imageName"], self.TARGET_IMAGE)
        self.assertEqual(api.calls, [])

    def test_stable_stage_missing_configured_resources_never_bootstraps(self) -> None:
        api = FakeReleaseAPI()
        with tempfile.TemporaryDirectory() as directory:
            receipt_path = Path(directory) / "rollout.json"
            with self.assertRaisesRegex(
                RunpodReleaseError, "ordinary deployment will not create"
            ):
                tars_runpod_release.stage_stable_release(
                    api,
                    ids=self.stable_ids(),
                    release_sha="b" * 40,
                    gpu_image=self.TARGET_IMAGE,
                    prior_release_sha="a" * 40,
                    prior_app_gpu_image=self.IMAGE,
                    greenfield=False,
                    receipt_path=receipt_path,
                )
            self.assertEqual(api.calls, [])
            self.assertFalse(receipt_path.exists())

    def test_stable_topology_requires_exclusive_bound_endpoint(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=11)
        api.templates[0]["boundEndpointId"] = "different-endpoint"
        with self.assertRaisesRegex(
            RunpodReleaseError, "stable template bound endpoint"
        ):
            tars_runpod_release.verify_stable_topology(
                api,
                ids=self.stable_ids(),
                expected_image=self.TARGET_IMAGE,
            )

    def test_stable_topology_accepts_missing_inverse_endpoint_binding(self) -> None:
        for marker in (None, "missing"):
            with self.subTest(marker=marker):
                api = FakeReleaseAPI()
                api.seed_stable(self.TARGET_IMAGE, version=11)
                if marker == "missing":
                    api.templates[0].pop("boundEndpointId")
                else:
                    api.templates[0]["boundEndpointId"] = marker

                tars_runpod_release.verify_stable_topology(
                    api,
                    ids=self.stable_ids(),
                    expected_image=self.TARGET_IMAGE,
                )

    def test_stable_topology_accepts_empty_rest_command_serialization(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=11)
        api.templates[0]["dockerArgs"] = "{}"

        tars_runpod_release.verify_stable_topology(
            api,
            ids=self.stable_ids(),
            expected_image=self.TARGET_IMAGE,
        )

        for docker_args in ('{"cmd":["unexpected"]}', "[]", "{ }"):
            with self.subTest(docker_args=docker_args):
                api.templates[0]["dockerArgs"] = docker_args
                with self.assertRaisesRegex(
                    RunpodReleaseError, "template dockerArgs"
                ):
                    tars_runpod_release.verify_stable_topology(
                        api,
                        ids=self.stable_ids(),
                        expected_image=self.TARGET_IMAGE,
                    )

    def test_stable_topology_requires_exclusive_template_and_auth(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=11)
        api.templates[0]["boundEndpointId"] = None
        shared = FakeReleaseAPI.endpoint(
            endpoint_id="shared-endpoint",
            name="unrelated-endpoint",
            template_id="stable-template",
            created=datetime.now(timezone.utc),
        )
        api.endpoints.append(shared)
        with self.assertRaisesRegex(
            RunpodReleaseError, "shared by another endpoint"
        ):
            tars_runpod_release.verify_stable_topology(
                api,
                ids=self.stable_ids(),
                expected_image=self.TARGET_IMAGE,
            )

        api.endpoints.pop()
        api.templates.append(
            {
                **api.templates[0],
                "id": "shared-template",
                "name": "unrelated-template",
                "boundEndpointId": None,
            }
        )
        with self.assertRaisesRegex(
            RunpodReleaseError, "registry auth is shared"
        ):
            tars_runpod_release.verify_stable_topology(
                api,
                ids=self.stable_ids(),
                expected_image=self.TARGET_IMAGE,
            )

    def test_stable_topology_retries_active_worker_missing_from_rest_once(
        self,
    ) -> None:
        api = self.racing_worker_api(
            RacingWorkerInventoryAPI.ACTIVE_MISSING_REST,
            1,
        )

        sleeps: list[float] = []
        endpoint, template, rest_endpoint = (
            tars_runpod_release.verify_stable_topology(
                api,
                ids=self.stable_ids(),
                expected_image=self.TARGET_IMAGE,
                sleeper=sleeps.append,
            )
        )

        self.assertEqual(endpoint["id"], "stable-endpoint")
        self.assertEqual(template["id"], "stable-template")
        self.assertEqual(rest_endpoint["workers"][0]["id"], "worker")
        self.assertEqual(api.correlation_inventory_calls, 2)
        self.assertEqual(api.correlation_endpoint_calls, 2)
        self.assertEqual(sleeps, [1.0])

    def test_stable_idle_retries_rest_worker_missing_status_twice(
        self,
    ) -> None:
        api = self.racing_worker_api(
            RacingWorkerInventoryAPI.REST_MISSING_STATUS,
            2,
            desired_status="EXITED",
        )

        sleeps: list[float] = []
        tars_runpod_release.wait_for_stable_idle(
            api,
            ids=self.stable_ids(),
            sleeper=sleeps.append,
        )

        self.assertEqual(api.correlation_inventory_calls, 3)
        self.assertEqual(api.correlation_endpoint_calls, 3)
        self.assertEqual(sleeps, [1.0, 2.0])
        self.assertEqual(api.health_calls, ["stable-endpoint"])

    def test_stable_idle_reads_health_after_correlated_worker_state(self) -> None:
        events: list[str] = []

        class OrderedHealthAPI(FakeReleaseAPI):
            def inventory(self) -> Inventory:
                events.append("inventory")
                return super().inventory()

            def read_endpoint(self, endpoint_id: str) -> dict:
                events.append("rest")
                return super().read_endpoint(endpoint_id)

            def read_endpoint_health(
                self, endpoint_id: str
            ) -> tars_runpod_release.EndpointHealth:
                events.append("health")
                return super().read_endpoint_health(endpoint_id)

        api = OrderedHealthAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)

        tars_runpod_release.wait_for_stable_idle(
            api,
            ids=self.stable_ids(),
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(events, ["inventory", "rest", "health"])

    def test_stable_topology_fails_on_third_inventory_correlation_race(
        self,
    ) -> None:
        for race, message in (
            (
                RacingWorkerInventoryAPI.ACTIVE_MISSING_REST,
                "active worker is missing",
            ),
            (
                RacingWorkerInventoryAPI.REST_MISSING_STATUS,
                "authoritative worker status",
            ),
        ):
            with self.subTest(race=race):
                api = self.racing_worker_api(race, 3)
                sleeps: list[float] = []
                with self.assertRaisesRegex(
                    tars_runpod_release.RunpodWorkerInventoryCorrelationError,
                    message,
                ):
                    tars_runpod_release.verify_stable_topology(
                        api,
                        ids=self.stable_ids(),
                        expected_image=self.TARGET_IMAGE,
                        sleeper=sleeps.append,
                    )
                self.assertEqual(api.correlation_inventory_calls, 3)
                self.assertEqual(api.correlation_endpoint_calls, 3)
                self.assertEqual(sleeps, [1.0, 2.0])

    def test_stable_topology_does_not_retry_generation_mismatch(self) -> None:
        api = self.racing_worker_api(
            RacingWorkerInventoryAPI.ACTIVE_MISSING_REST,
            0,
            worker_image=self.IMAGE,
        )

        with self.assertRaisesRegex(
            RunpodReleaseError, "active worker image"
        ):
            tars_runpod_release.verify_stable_topology(
                api,
                ids=self.stable_ids(),
                expected_image=self.TARGET_IMAGE,
                sleeper=lambda _seconds: self.fail(
                    "a definitive generation mismatch must not be retried"
                ),
            )

        self.assertEqual(api.correlation_inventory_calls, 1)
        self.assertEqual(api.correlation_endpoint_calls, 1)

    def test_stable_version_does_not_restart_correlation_budget(self) -> None:
        api = self.racing_worker_api(
            RacingWorkerInventoryAPI.REST_MISSING_STATUS,
            3,
        )
        sleeps: list[float] = []

        with self.assertRaises(
            tars_runpod_release.RunpodWorkerInventoryCorrelationError
        ):
            tars_runpod_release.wait_for_stable_version(
                api,
                ids=self.stable_ids(),
                image=self.TARGET_IMAGE,
                previous_version=7,
                sleeper=sleeps.append,
            )

        self.assertEqual(api.correlation_inventory_calls, 3)
        self.assertEqual(api.correlation_endpoint_calls, 3)
        self.assertEqual(sleeps, [1.0, 2.0])

    def test_adoption_does_not_restart_correlation_budget(self) -> None:
        api = self.racing_worker_api(
            RacingWorkerInventoryAPI.ACTIVE_MISSING_REST,
            3,
        )
        sleeps: list[float] = []

        with self.assertRaises(
            tars_runpod_release.RunpodWorkerInventoryCorrelationError
        ):
            tars_runpod_release.wait_for_adopted_topology(
                api,
                ids=self.stable_ids(),
                image=self.TARGET_IMAGE,
                minimum_version=8,
                sleeper=sleeps.append,
            )

        self.assertEqual(api.correlation_inventory_calls, 3)
        self.assertEqual(api.correlation_endpoint_calls, 3)
        self.assertEqual(sleeps, [1.0, 2.0])

    def test_stable_stage_same_image_is_noop_without_version_bump(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=11)
        api.health_by_endpoint["stable-endpoint"] = [
            tars_runpod_release.EndpointHealth(1, 0),
            tars_runpod_release.EndpointHealth(0, 0),
        ]
        with tempfile.TemporaryDirectory() as directory:
            receipt = tars_runpod_release.stage_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="b" * 40,
                prior_app_gpu_image=self.TARGET_IMAGE,
                greenfield=False,
                receipt_path=Path(directory) / "rollout.json",
                sleeper=lambda _seconds: None,
            )
        self.assertEqual(receipt.mode, "noop")
        self.assertEqual(receipt.prior_version, 11)
        self.assertEqual(receipt.target_version, 11)
        self.assertEqual(api.calls, [])
        self.assertEqual(
            api.health_calls, ["stable-endpoint", "stable-endpoint"]
        )

    def test_stable_noop_stage_refuses_stale_embedded_template(self) -> None:
        class StaleEmbeddedTemplateAPI(FakeReleaseAPI):
            def read_endpoint(self, endpoint_id: str) -> dict:
                endpoint = super().read_endpoint(endpoint_id)
                endpoint["template"]["imageName"] = (
                    RunpodReleaseTest.IMAGE
                )
                return endpoint

        api = StaleEmbeddedTemplateAPI()
        api.seed_stable(self.TARGET_IMAGE, version=11)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                RunpodReleaseError, "REST template imageName"
            ):
                tars_runpod_release.stage_stable_release(
                    api,
                    ids=self.stable_ids(),
                    release_sha="b" * 40,
                    gpu_image=self.TARGET_IMAGE,
                    prior_release_sha="b" * 40,
                    prior_app_gpu_image=self.TARGET_IMAGE,
                    greenfield=False,
                    receipt_path=Path(directory) / "rollout.json",
                )
        self.assertEqual(api.calls, [])

    def test_stable_rollback_restores_exact_prior_image_and_new_version(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            receipt = tars_runpod_release.stage_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=Path(directory) / "rollout.json",
            )
            tars_runpod_release.rollback_stable_release(
                api, receipt=receipt
            )
        self.assertEqual(
            [call for call in api.calls if call.startswith("update_template:")],
            [
                f"update_template:{self.TARGET_IMAGE}",
                f"update_template:{self.IMAGE}",
            ],
        )
        self.assertEqual(api.templates[0]["imageName"], self.IMAGE)
        self.assertEqual(api.endpoints[0]["version"], 9)

    def test_stable_rollback_refuses_stale_embedded_prior_template(self) -> None:
        class StaleEmbeddedTemplateAPI(FakeReleaseAPI):
            def read_endpoint(self, endpoint_id: str) -> dict:
                endpoint = super().read_endpoint(endpoint_id)
                endpoint["template"]["imageName"] = (
                    RunpodReleaseTest.TARGET_IMAGE
                )
                return endpoint

        api = StaleEmbeddedTemplateAPI()
        api.seed_stable(self.IMAGE, version=9)
        with self.assertRaisesRegex(
            RunpodReleaseError, "REST template imageName"
        ):
            tars_runpod_release.rollback_stable_release(
                api,
                receipt=self.stable_update_receipt(),
            )
        self.assertEqual(api.calls, [])

    def test_stable_rollback_refuses_same_sha_with_unrecorded_digest(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            receipt = tars_runpod_release.stage_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=Path(directory) / "rollout.json",
            )
            api.templates[0]["imageName"] = self.IMAGE_WITH_DIFFERENT_DIGEST
            with self.assertRaisesRegex(
                RunpodReleaseError, "drifted after staging"
            ):
                tars_runpod_release.rollback_stable_release(
                    api, receipt=receipt
                )
        self.assertEqual(
            [
                call
                for call in api.calls
                if call.startswith("update_template:")
            ],
            [f"update_template:{self.TARGET_IMAGE}"],
        )

    def test_stable_rollback_refuses_image_drift_during_idle_wait(self) -> None:
        class DriftingReleaseAPI(FakeReleaseAPI):
            def read_endpoint_health(
                self, endpoint_id: str
            ) -> tars_runpod_release.EndpointHealth:
                health = super().read_endpoint_health(endpoint_id)
                if len(self.health_calls) == 2:
                    self.templates[0]["imageName"] = (
                        RunpodReleaseTest.IMAGE_WITH_DIFFERENT_DIGEST
                    )
                return health

        api = DriftingReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.health_by_endpoint["stable-endpoint"] = [
            tars_runpod_release.EndpointHealth(1, 0),
            tars_runpod_release.EndpointHealth(0, 0),
        ]
        with self.assertRaisesRegex(
            RunpodReleaseError, "stable template image"
        ):
            tars_runpod_release.rollback_stable_release(
                api,
                receipt=self.stable_update_receipt(),
                sleeper=lambda _seconds: None,
            )
        self.assertEqual(api.calls, [])

    def test_stable_rollback_refuses_version_drift_during_idle_wait(self) -> None:
        class DriftingReleaseAPI(FakeReleaseAPI):
            def read_endpoint_health(
                self, endpoint_id: str
            ) -> tars_runpod_release.EndpointHealth:
                health = super().read_endpoint_health(endpoint_id)
                if len(self.health_calls) == 2:
                    self.endpoints[0]["version"] += 1
                return health

        api = DriftingReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.health_by_endpoint["stable-endpoint"] = [
            tars_runpod_release.EndpointHealth(1, 0),
            tars_runpod_release.EndpointHealth(0, 0),
        ]
        with self.assertRaisesRegex(
            RunpodReleaseError, "version after drain"
        ):
            tars_runpod_release.rollback_stable_release(
                api,
                receipt=self.stable_update_receipt(),
                sleeper=lambda _seconds: None,
            )
        self.assertEqual(api.calls, [])

    def test_stable_rollback_requires_recorded_target_version(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        with self.assertRaisesRegex(
            RunpodReleaseError, "target endpoint version before rollback"
        ):
            tars_runpod_release.rollback_stable_release(
                api,
                receipt=self.stable_update_receipt(target_version=9),
            )
        self.assertEqual(api.calls, [])

    def test_verify_target_rereads_topology_after_the_idle_wait(self) -> None:
        class DriftingReleaseAPI(FakeReleaseAPI):
            def read_endpoint_health(
                self, endpoint_id: str
            ) -> tars_runpod_release.EndpointHealth:
                health = super().read_endpoint_health(endpoint_id)
                if len(self.health_calls) == 2:
                    self.endpoints[0]["version"] += 1
                return health

        api = DriftingReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=3)
        with tempfile.TemporaryDirectory() as directory:
            receipt = tars_runpod_release.prepare_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha=None,
                prior_app_gpu_image=None,
                greenfield=True,
                receipt_path=Path(directory) / "boundary.json",
            )
            api.health_by_endpoint["stable-endpoint"] = [
                tars_runpod_release.EndpointHealth(1, 0),
                tars_runpod_release.EndpointHealth(0, 0),
            ]
            with self.assertRaisesRegex(
                RunpodReleaseError, "version after drain"
            ):
                tars_runpod_release.verify_receipt_target(
                    api,
                    receipt=receipt,
                    sleeper=lambda _seconds: None,
                )

    def test_verify_target_refuses_embedded_template_drift_after_idle(
        self,
    ) -> None:
        class DriftingEmbeddedTemplateAPI(FakeReleaseAPI):
            def __init__(self) -> None:
                super().__init__()
                self.endpoint_reads = 0

            def read_endpoint(self, endpoint_id: str) -> dict:
                self.endpoint_reads += 1
                endpoint = super().read_endpoint(endpoint_id)
                if self.endpoint_reads >= 3:
                    endpoint["template"]["imageName"] = (
                        RunpodReleaseTest.IMAGE
                    )
                return endpoint

        api = DriftingEmbeddedTemplateAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        with self.assertRaisesRegex(
            RunpodReleaseError, "REST template imageName"
        ):
            tars_runpod_release.verify_receipt_target(
                api,
                receipt=self.stable_update_receipt(),
            )
        self.assertEqual(api.calls, [])

    def test_finalize_allows_inactive_history_and_verifies_active_generation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        with tempfile.TemporaryDirectory() as directory:
            receipt = tars_runpod_release.stage_stable_release(
                api,
                ids=self.stable_ids(),
                release_sha="b" * 40,
                gpu_image=self.TARGET_IMAGE,
                prior_release_sha="a" * 40,
                prior_app_gpu_image=self.IMAGE,
                greenfield=False,
                receipt_path=Path(directory) / "rollout.json",
            )
        api.endpoints[0]["pods"] = [
            {"id": "exited-worker", "desiredStatus": "EXITED"},
            {"id": "terminated-worker", "desiredStatus": "TERMINATED"},
            {"id": "new-worker", "desiredStatus": "RUNNING"},
        ]
        api.endpoints[0]["workers"] = [
            {
                "id": "exited-worker",
                "image": self.IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 6,
            },
            {
                "id": "terminated-worker",
                "image": self.IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 7,
            },
            {
                "id": "new-worker",
                "image": self.TARGET_IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 8,
            },
        ]
        tars_runpod_release.finalize_stable_release(api, receipt=receipt)

    def test_finalize_rejects_unknown_worker_status(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.endpoints[0]["pods"] = [
            {"id": "unknown-worker", "desiredStatus": "STOPPED"}
        ]
        with self.assertRaisesRegex(
            RunpodReleaseError, "invalid worker status"
        ):
            tars_runpod_release.verify_active_worker_generation(
                api.endpoints[0],
                api.read_endpoint("stable-endpoint"),
                image=self.TARGET_IMAGE,
                auth_id="stable-auth",
                version=8,
            )

    def test_finalize_rejects_worker_without_authoritative_status(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.endpoints[0]["workers"] = [
            {
                "id": "unclassified-worker",
                "image": self.TARGET_IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 8,
            }
        ]
        with self.assertRaisesRegex(
            RunpodReleaseError, "authoritative worker status"
        ):
            tars_runpod_release.verify_active_worker_generation(
                api.endpoints[0],
                api.read_endpoint("stable-endpoint"),
                image=self.TARGET_IMAGE,
                auth_id="stable-auth",
                version=8,
            )

    def test_finalize_rejects_active_old_sls_version(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.endpoints[0]["pods"] = [
            {"id": "old-worker", "desiredStatus": "RUNNING"}
        ]
        api.endpoints[0]["workers"] = [
            {
                "id": "old-worker",
                "image": self.TARGET_IMAGE,
                "containerRegistryAuthId": "stable-auth",
                "slsVersion": 7,
            }
        ]
        receipt = tars_runpod_release.StableRolloutReceipt(
            endpoint_id="stable-endpoint",
            template_id="stable-template",
            auth_id="stable-auth",
            baseline="existing",
            prior_release_sha="a" * 40,
            prior_app_gpu_image=self.IMAGE,
            release_sha="b" * 40,
            target_image=self.TARGET_IMAGE,
            prior_image=self.IMAGE,
            prior_version=7,
            target_version=8,
            mode="update",
        )
        ticks = iter((0.0, 1.0))
        with self.assertRaisesRegex(
            RunpodReleaseError, "active worker slsVersion"
        ):
            tars_runpod_release.finalize_stable_release(
                api,
                receipt=receipt,
                timeout_seconds=0.5,
                poll_seconds=0.1,
                sleeper=lambda _seconds: None,
                clock=lambda: next(ticks),
            )

    def test_explicit_stable_bootstrap_is_only_creation_path(self) -> None:
        api = FakeReleaseAPI()
        ids = tars_runpod_release.bootstrap_stable_resources(
            api,
            release_sha="a" * 40,
            gpu_image=self.IMAGE,
            registry_username="reader",
            registry_password="password",
        )
        self.assertEqual(ids.endpoint_id, "endpoint1")
        self.assertEqual(ids.template_id, "template1")
        self.assertEqual(ids.auth_id, "auth1")
        self.assertEqual(
            api.calls[:4],
            [
                "create_auth",
                "create_template",
                f"update_template:{self.IMAGE}",
                "create_endpoint",
            ],
        )
        self.assertRegex(api.auths[0]["name"], r"^tars-runpod-auth-v2-[0-9a-f]{12}$")

    def test_explicit_ada24_transition_updates_exact_stable_endpoint_once(
        self,
    ) -> None:
        class AppliedWithLostResponseAPI(FakeReleaseAPI):
            def add_ada24_fallback(
                self, endpoint_id: str, template_id: str
            ) -> dict:
                super().add_ada24_fallback(endpoint_id, template_id)
                return {}

        api = AppliedWithLostResponseAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["gpuIds"] = (
            tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
        )

        ids = tars_runpod_release.add_ada24_fallback_to_stable_endpoint(
            api,
            ids=self.stable_ids(),
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(ids, self.stable_ids())
        self.assertEqual(api.endpoints[0]["gpuIds"], GPU_POOL_SELECTOR)
        self.assertEqual(api.templates[0]["imageName"], self.IMAGE)
        self.assertEqual(
            api.calls, ["add_ada24_fallback:stable-endpoint"]
        )
        self.assertEqual(
            api.health_calls,
            ["stable-endpoint", "stable-endpoint"],
        )

    def test_explicit_ada24_transition_is_idempotent_without_mutation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=8)

        ids = tars_runpod_release.add_ada24_fallback_to_stable_endpoint(
            api,
            ids=self.stable_ids(),
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(ids, self.stable_ids())
        self.assertEqual(api.calls, [])
        self.assertEqual(api.health_calls, ["stable-endpoint"])

    def test_explicit_ada24_transition_refuses_provider_work_before_mutation(
        self,
    ) -> None:
        cases = (
            (
                "queued",
                tars_runpod_release.EndpointHealth(1, 0),
                False,
            ),
            (
                "in-progress",
                tars_runpod_release.EndpointHealth(0, 1),
                False,
            ),
            (
                "active-worker",
                tars_runpod_release.EndpointHealth(0, 0),
                True,
            ),
        )
        for name, health, active_worker in cases:
            with self.subTest(name=name):
                api = FakeReleaseAPI()
                api.seed_stable(self.IMAGE, version=7)
                api.endpoints[0]["gpuIds"] = (
                    tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
                )
                api.health_by_endpoint["stable-endpoint"] = [health]
                if active_worker:
                    api.endpoints[0]["pods"] = [
                        {"id": "worker", "desiredStatus": "RUNNING"}
                    ]
                    api.endpoints[0]["workers"] = [
                        {
                            "id": "worker",
                            "image": self.IMAGE,
                            "containerRegistryAuthId": "stable-auth",
                            "slsVersion": 7,
                        }
                    ]

                with self.assertRaisesRegex(
                    RunpodReleaseError,
                    "requires zero queued, in-progress, and active worker",
                ):
                    (
                        tars_runpod_release
                        .add_ada24_fallback_to_stable_endpoint(
                            api,
                            ids=self.stable_ids(),
                            sleeper=lambda _seconds: None,
                        )
                    )
                self.assertEqual(api.calls, [])

    def test_explicit_ada24_transition_refuses_unreviewed_selector(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["gpuIds"] = "AMPERE_24,ADA_24"

        with self.assertRaisesRegex(RunpodReleaseError, "endpoint gpuIds"):
            tars_runpod_release.add_ada24_fallback_to_stable_endpoint(
                api,
                ids=self.stable_ids(),
            )

        self.assertEqual(api.calls, [])
        self.assertEqual(api.health_calls, [])

    def test_explicit_ada24_transition_never_reissues_unconfirmed_mutation(
        self,
    ) -> None:
        class UnconfirmedTransitionAPI(FakeReleaseAPI):
            def add_ada24_fallback(
                self, endpoint_id: str, template_id: str
            ) -> dict:
                self.calls.append(f"add_ada24_fallback:{endpoint_id}")
                return {}

        api = UnconfirmedTransitionAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["gpuIds"] = (
            tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
        )
        ticks = iter((0.0, 1.0))

        with self.assertRaisesRegex(
            RunpodReleaseError, "did not converge"
        ):
            tars_runpod_release.add_ada24_fallback_to_stable_endpoint(
                api,
                ids=self.stable_ids(),
                timeout_seconds=0.5,
                poll_seconds=0.1,
                sleeper=lambda _seconds: None,
                clock=lambda: next(ticks),
            )

        self.assertEqual(
            api.calls, ["add_ada24_fallback:stable-endpoint"]
        )

    def test_explicit_ada24_transition_uses_exact_state_not_undocumented_version(
        self,
    ) -> None:
        class NoVersionTransitionAPI(FakeReleaseAPI):
            def add_ada24_fallback(
                self, endpoint_id: str, template_id: str
            ) -> dict:
                self.calls.append(f"add_ada24_fallback:{endpoint_id}")
                endpoint = next(
                    item
                    for item in self.endpoints
                    if item["id"] == endpoint_id
                )
                if endpoint["templateId"] != template_id:
                    raise AssertionError("fake endpoint/template mismatch")
                endpoint["gpuIds"] = GPU_POOL_SELECTOR
                return self.read_endpoint(endpoint_id)

        api = NoVersionTransitionAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["gpuIds"] = (
            tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
        )

        ids = tars_runpod_release.add_ada24_fallback_to_stable_endpoint(
            api,
            ids=self.stable_ids(),
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(ids, self.stable_ids())
        self.assertEqual(api.endpoints[0]["version"], 7)
        self.assertEqual(
            api.calls, ["add_ada24_fallback:stable-endpoint"]
        )

    def test_explicit_ada24_transition_refuses_image_drift_after_mutation(
        self,
    ) -> None:
        class ImageDriftTransitionAPI(FakeReleaseAPI):
            def add_ada24_fallback(
                self, endpoint_id: str, template_id: str
            ) -> dict:
                result = super().add_ada24_fallback(
                    endpoint_id, template_id
                )
                self.templates[0]["imageName"] = (
                    RunpodReleaseTest.TARGET_IMAGE
                )
                return result

        api = ImageDriftTransitionAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["gpuIds"] = (
            tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
        )

        with self.assertRaisesRegex(
            RunpodReleaseError, "stable template image"
        ):
            tars_runpod_release.add_ada24_fallback_to_stable_endpoint(
                api,
                ids=self.stable_ids(),
                sleeper=lambda _seconds: None,
            )

        self.assertEqual(
            api.calls, ["add_ada24_fallback:stable-endpoint"]
        )

    def test_explicit_ada24_cli_requires_confirmation_and_all_stable_ids(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.IMAGE, version=7)
        api.endpoints[0]["gpuIds"] = (
            tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths: dict[str, Path] = {}
            for name, value in {
                "api": "runpod-key",
                "endpoint": "stable-endpoint",
                "template": "stable-template",
                "auth": "stable-auth",
            }.items():
                path = root / name
                path.write_text(value, encoding="utf-8")
                path.chmod(0o600)
                paths[name] = path
            arguments = [
                "tars_runpod_release.py",
                "add-ada24-fallback",
                "--api-key-file",
                str(paths["api"]),
                "--endpoint-id-file",
                str(paths["endpoint"]),
                "--template-id-file",
                str(paths["template"]),
                "--auth-id-file",
                str(paths["auth"]),
            ]
            with (
                mock.patch("sys.argv", arguments),
                mock.patch("sys.stderr", io.StringIO()),
                mock.patch.object(
                    tars_runpod_release,
                    "RunpodClient",
                    return_value=api,
                ),
                self.assertRaises(SystemExit) as exit_error,
            ):
                tars_runpod_release.main()
            self.assertEqual(exit_error.exception.code, 1)
            self.assertEqual(api.calls, [])

            with (
                mock.patch(
                    "sys.argv",
                    [*arguments, "--confirm-add-ada24-fallback"],
                ),
                mock.patch("sys.stdout", io.StringIO()),
                mock.patch.object(
                    tars_runpod_release,
                    "RunpodClient",
                    return_value=api,
                ),
            ):
                tars_runpod_release.main()

        self.assertEqual(
            api.calls, ["add_ada24_fallback:stable-endpoint"]
        )

    def test_ordinary_deploy_never_invokes_ada24_transition(self) -> None:
        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github"
            / "workflows"
            / "tars-deploy.yml"
        )
        self.assertNotIn(
            "add-ada24-fallback",
            workflow.read_text(encoding="utf-8"),
        )

    def test_ada24_runbook_retains_lock_and_drain_on_uncertainty(self) -> None:
        runbook = (
            Path(__file__).resolve().parent / "RUNPOD_STABLE.md"
        ).read_text(encoding="utf-8")
        for required in (
            "no queued or running `Central TARS Production Delivery` workflow",
            "atomically create `tars_deploy_lock`",
            "ensure the dispatcher is at zero",
            "retain the lock and ensure",
            "Rerun the same idempotent",
            "wait for exact 1/1 convergence",
            "verify API readiness",
        ):
            with self.subTest(required=required):
                self.assertIn(required, runbook)
        self.assertNotIn(
            "Use an exit trap like the migration procedure below",
            runbook,
        )

    def test_explicit_migration_adopts_existing_ids_without_creation(self) -> None:
        api = FakeReleaseAPI()
        api.seed_release(
            "a" * 40,
            "current",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.endpoints[0]["gpuIds"] = (
            tars_runpod_release.PRE_ADA24_GPU_POOL_SELECTOR
        )
        ids = tars_runpod_release.StableResourceIDs(
            "endpoint-current", "template-current", "auth-current"
        )

        adopted = tars_runpod_release.adopt_existing_stable_resources(
            api,
            endpoint_id=ids.endpoint_id,
            sleeper=lambda _seconds: None,
        )
        calls_after_adoption = list(api.calls)
        repeated = tars_runpod_release.adopt_existing_stable_resources(
            api,
            endpoint_id=ids.endpoint_id,
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(adopted, ids)
        self.assertEqual(repeated, ids)
        self.assertEqual(
            calls_after_adoption,
            [
                f"update_template:{self.IMAGE}",
                "rename_endpoint:endpoint-current",
            ],
        )
        self.assertEqual(api.calls, calls_after_adoption)
        self.assertEqual(
            api.endpoints[0]["name"], "tars-runpod-endpoint-v2"
        )
        self.assertEqual(
            api.templates[0]["name"], "tars-runpod-template-v2"
        )
        self.assertEqual(api.endpoints[0]["version"], 1)
        self.assertFalse(
            any(call.startswith("create_") for call in api.calls)
        )
        self.assertFalse(
            any(call.startswith("delete_") for call in api.calls)
        )

    def test_explicit_migration_accepts_missing_inverse_endpoint_binding(
        self,
    ) -> None:
        for marker in (None, "missing"):
            with self.subTest(marker=marker):
                api = FakeReleaseAPI()
                api.seed_release(
                    "a" * 40,
                    "current",
                    datetime.now(timezone.utc) - timedelta(days=1),
                )
                if marker == "missing":
                    api.templates[0].pop("boundEndpointId")
                else:
                    api.templates[0]["boundEndpointId"] = marker

                adopted = (
                    tars_runpod_release.adopt_existing_stable_resources(
                        api,
                        endpoint_id="endpoint-current",
                        sleeper=lambda _seconds: None,
                    )
                )

                self.assertEqual(
                    adopted,
                    tars_runpod_release.StableResourceIDs(
                        "endpoint-current",
                        "template-current",
                        "auth-current",
                    ),
                )

    def test_explicit_migration_accepts_sparse_rest_template_defaults(
        self,
    ) -> None:
        class SparseRESTTemplateAPI(FakeReleaseAPI):
            def read_endpoint(self, endpoint_id: str) -> dict:
                resource = super().read_endpoint(endpoint_id)
                resource["template"].pop("imageName", None)
                return resource

            @staticmethod
            def _rest_template(template: dict) -> dict:
                resource = FakeReleaseAPI._rest_template(template)
                for field in (
                    "dockerEntrypoint",
                    "dockerStartCmd",
                    "isPublic",
                    "ports",
                    "readme",
                    "volumeInGb",
                    "volumeMountPath",
                ):
                    resource.pop(field, None)
                return resource

        api = SparseRESTTemplateAPI()
        api.seed_release(
            "a" * 40,
            "current",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.templates[0]["boundEndpointId"] = None

        adopted = tars_runpod_release.adopt_existing_stable_resources(
            api,
            endpoint_id="endpoint-current",
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(
            adopted,
            tars_runpod_release.StableResourceIDs(
                "endpoint-current",
                "template-current",
                "auth-current",
            ),
        )

    def test_sparse_rest_template_rejects_explicit_non_defaults(self) -> None:
        base = {
            "id": "stable-template",
            "name": "tars-runpod-template-v2",
            "imageName": self.TARGET_IMAGE,
            "containerRegistryAuthId": "stable-auth",
            "containerDiskInGb": 20,
            "env": {"RUNPOD_INIT_TIMEOUT": "1200"},
            "isServerless": True,
        }
        cases = {
            "dockerEntrypoint": ["/bin/sh"],
            "dockerStartCmd": ["serve"],
            "isPublic": True,
            "ports": ["8080/http"],
            "readme": "unexpected",
            "volumeInGb": 1,
            "volumeMountPath": "/unexpected",
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    RunpodReleaseError,
                    f"owned REST template {field}",
                ):
                    tars_runpod_release.verify_owned_template_rest(
                        {**base, field: value},
                        template_id="stable-template",
                        expected_name="tars-runpod-template-v2",
                        image=self.TARGET_IMAGE,
                        auth_id="stable-auth",
                    )

    def test_explicit_migration_rejects_conflicting_inverse_binding(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_release(
            "a" * 40,
            "current",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.templates[0]["boundEndpointId"] = "different-endpoint"

        with self.assertRaisesRegex(
            RunpodReleaseError, "adopted template bound endpoint"
        ):
            tars_runpod_release.adopt_existing_stable_resources(
                api,
                endpoint_id="endpoint-current",
                sleeper=lambda _seconds: None,
            )

        self.assertEqual(api.calls, [])

    def test_explicit_migration_rejects_shared_template(self) -> None:
        api = FakeReleaseAPI()
        api.seed_release(
            "a" * 40,
            "current",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.templates[0]["boundEndpointId"] = None
        api.endpoints.append(
            FakeReleaseAPI.endpoint(
                endpoint_id="shared-endpoint",
                name="unrelated-endpoint",
                template_id="template-current",
                created=datetime.now(timezone.utc),
            )
        )

        with self.assertRaisesRegex(
            RunpodReleaseError, "shared by another endpoint"
        ):
            tars_runpod_release.adopt_existing_stable_resources(
                api,
                endpoint_id="endpoint-current",
                sleeper=lambda _seconds: None,
            )

        self.assertEqual(api.calls, [])

    def test_explicit_migration_refuses_stale_embedded_template(self) -> None:
        class StaleEmbeddedTemplateAPI(FakeReleaseAPI):
            def read_endpoint(self, endpoint_id: str) -> dict:
                endpoint = super().read_endpoint(endpoint_id)
                endpoint["template"]["imageName"] = (
                    RunpodReleaseTest.TARGET_IMAGE
                )
                return endpoint

        api = StaleEmbeddedTemplateAPI()
        api.seed_release(
            "a" * 40,
            "current",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        with self.assertRaisesRegex(
            RunpodReleaseError, "REST template imageName"
        ):
            tars_runpod_release.adopt_existing_stable_resources(
                api,
                endpoint_id="endpoint-current",
                sleeper=lambda _seconds: None,
            )
        self.assertEqual(api.calls, [])

    def test_explicit_migration_resumes_after_template_rename(self) -> None:
        api = FakeReleaseAPI()
        api.seed_release(
            "a" * 40,
            "partial",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.templates[0]["name"] = "tars-runpod-template-v2"
        ids = tars_runpod_release.StableResourceIDs(
            "endpoint-partial", "template-partial", "auth-partial"
        )

        tars_runpod_release.adopt_existing_stable_resources(
            api,
            endpoint_id=ids.endpoint_id,
            sleeper=lambda _seconds: None,
        )

        self.assertEqual(
            api.calls, ["rename_endpoint:endpoint-partial"]
        )
        self.assertEqual(
            api.endpoints[0]["name"], "tars-runpod-endpoint-v2"
        )

    def test_explicit_migration_refuses_legacy_gpu_contract_before_mutation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_release(
            "a" * 40,
            "legacy-gpu",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.endpoints[0]["gpuIds"] = "AMPERE_16"
        ids = tars_runpod_release.StableResourceIDs(
            "endpoint-legacy-gpu",
            "template-legacy-gpu",
            "auth-legacy-gpu",
        )

        with self.assertRaisesRegex(
            RunpodReleaseError, "endpoint gpuIds"
        ):
            tars_runpod_release.adopt_existing_stable_resources(
                api,
                endpoint_id=ids.endpoint_id,
                sleeper=lambda _seconds: None,
            )
        self.assertEqual(api.calls, [])

    def test_explicit_migration_drains_active_worker_before_mutation(self) -> None:
        api = FakeReleaseAPI()
        api.seed_release(
            "a" * 40,
            "busy",
            datetime.now(timezone.utc) - timedelta(days=1),
        )
        api.endpoints[0]["pods"] = [
            {"id": "busy-worker", "desiredStatus": "RUNNING"}
        ]
        api.endpoints[0]["workers"] = [
            {
                "id": "busy-worker",
                "image": self.IMAGE,
                "containerRegistryAuthId": "auth-busy",
                "slsVersion": 0,
            }
        ]
        ids = tars_runpod_release.StableResourceIDs(
            "endpoint-busy", "template-busy", "auth-busy"
        )
        ticks = iter((0.0, 1.0))

        with self.assertRaisesRegex(
            RunpodReleaseError, "did not drain"
        ):
            tars_runpod_release.adopt_existing_stable_resources(
                api,
                endpoint_id=ids.endpoint_id,
                timeout_seconds=0.5,
                poll_seconds=0.1,
                sleeper=lambda _seconds: None,
                clock=lambda: next(ticks),
            )
        self.assertEqual(api.calls, [])

    def test_stable_template_update_uses_one_official_post_without_blind_retry(
        self,
    ) -> None:
        calls: list[urllib.request.Request] = []

        def lose_update_response(request, *, timeout):
            calls.append(request)
            raise urllib.error.URLError("response lost")

        updated = RunpodClient(
            "api-key", opener=lose_update_response
        ).update_template(
            "stable-template",
            "tars-runpod-template-v2",
            self.TARGET_IMAGE,
            "stable-auth",
        )

        self.assertEqual(updated, {})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].method, "POST")
        self.assertEqual(
            urllib.parse.urlsplit(calls[0].full_url).path,
            "/v1/templates/stable-template/update",
        )
        self.assertEqual(
            json.loads(calls[0].data),
            tars_runpod_release.stable_template_payload(
                name="tars-runpod-template-v2",
                image=self.TARGET_IMAGE,
                auth_id="stable-auth",
            ),
        )

    def test_stable_template_update_stops_on_definitive_http_rejection(
        self,
    ) -> None:
        calls = 0

        def reject(request, *, timeout):
            nonlocal calls
            calls += 1
            raise urllib.error.HTTPError(
                request.full_url,
                400,
                "bad request",
                email.message.Message(),
                io.BytesIO(b"rejected"),
            )

        with self.assertRaisesRegex(
            RunpodReleaseError, "failed with HTTP 400"
        ):
            RunpodClient("api-key", opener=reject).update_template(
                "stable-template",
                "tars-runpod-template-v2",
                self.TARGET_IMAGE,
                "stable-auth",
            )
        self.assertEqual(calls, 1)

    def test_stable_endpoint_rename_uses_one_patch_without_blind_retry(
        self,
    ) -> None:
        calls: list[urllib.request.Request] = []

        def lose_rename_response(request, *, timeout):
            calls.append(request)
            raise urllib.error.URLError("response lost")

        renamed = RunpodClient(
            "api-key", opener=lose_rename_response
        ).rename_endpoint("stable-endpoint", "stable-template")

        self.assertEqual(renamed, {})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].method, "PATCH")
        self.assertEqual(
            urllib.parse.urlsplit(calls[0].full_url).path,
            "/v1/endpoints/stable-endpoint",
        )
        self.assertEqual(
            json.loads(calls[0].data),
            {"name": "tars-runpod-endpoint-v2"},
        )
        self.assertEqual(
            calls[0].get_header("Authorization"), "Bearer api-key"
        )

    def test_ada24_transition_uses_one_full_graphql_save_for_existing_id(
        self,
    ) -> None:
        client = RunpodClient("api-key")
        with mock.patch.object(
            client,
            "_graphql",
            return_value={
                "saveEndpoint": {
                    "id": "stable-endpoint",
                    "name": "tars-runpod-endpoint-v2",
                }
            },
        ) as graphql:
            updated = client.add_ada24_fallback(
                "stable-endpoint", "stable-template"
            )

        self.assertEqual(updated["id"], "stable-endpoint")
        graphql.assert_called_once()
        self.assertEqual(
            graphql.call_args.args[0],
            "add stable ADA 24 GB fallback",
        )
        self.assertEqual(graphql.call_args.kwargs["max_calls"], 1)
        query = graphql.call_args.args[1]
        for field in (
            'id: "stable-endpoint"',
            'name: "tars-runpod-endpoint-v2"',
            'templateId: "stable-template"',
            'type: "QB"',
            f'gpuIds: "{GPU_POOL_SELECTOR}"',
            "idleTimeout: 5",
            'locations: ""',
            "networkVolumeId: null",
            "flashBootType: OFF",
            'scalerType: "REQUEST_COUNT"',
            "scalerValue: 1",
            "workersMin: 0",
            "workersMax: 2",
        ):
            with self.subTest(field=field):
                self.assertIn(field, query)
        for rest_only_field in (
            "executionTimeoutMs",
            "gpuCount",
            "computeType",
        ):
            self.assertNotIn(rest_only_field, query)

    def test_ada24_transition_lost_response_is_never_blindly_retried(
        self,
    ) -> None:
        outcomes = (
            RunpodReleaseError("response lost"),
            {},
        )
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                client = RunpodClient("api-key")
                with mock.patch.object(
                    client,
                    "_graphql",
                    side_effect=(
                        outcome
                        if isinstance(outcome, Exception)
                        else None
                    ),
                    return_value=(
                        outcome
                        if not isinstance(outcome, Exception)
                        else None
                    ),
                ) as graphql:
                    self.assertEqual(
                        client.add_ada24_fallback(
                            "stable-endpoint", "stable-template"
                        ),
                        {},
                    )

                self.assertEqual(graphql.call_count, 1)
                self.assertEqual(
                    graphql.call_args.kwargs["max_calls"], 1
                )

    def test_ada24_transition_stops_on_definitive_graphql_rejection(
        self,
    ) -> None:
        outcomes = (
            tars_runpod_release.RunpodDefinitiveRequestError("rejected"),
            {
                "saveEndpoint": {
                    "id": "other-endpoint",
                    "name": "tars-runpod-endpoint-v2",
                }
            },
        )
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                client = RunpodClient("api-key")
                with mock.patch.object(
                    client,
                    "_graphql",
                    side_effect=(
                        outcome
                        if isinstance(outcome, Exception)
                        else None
                    ),
                    return_value=(
                        outcome
                        if not isinstance(outcome, Exception)
                        else None
                    ),
                ) as graphql:
                    with self.assertRaisesRegex(
                        RunpodReleaseError,
                        "rejected|different endpoint identity",
                    ):
                        client.add_ada24_fallback(
                            "stable-endpoint", "stable-template"
                        )

                self.assertEqual(graphql.call_count, 1)

    def test_explicit_legacy_retirement_is_exact_idempotent_and_protects_stable(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.seed_release(
            "c" * 40, "old-one", datetime.now(timezone.utc) - timedelta(days=2)
        )
        api.seed_release(
            "d" * 40, "old-two", datetime.now(timezone.utc) - timedelta(days=2)
        )
        with tempfile.TemporaryDirectory() as directory:
            plan = Path(directory) / "legacy-plan.json"
            confirmation = tars_runpod_release.build_legacy_retirement_plan(
                api, stable_ids=self.stable_ids(), output_path=plan
            )
            self.assertEqual(plan.stat().st_mode & 0o777, 0o600)
            retired = tars_runpod_release.retire_legacy_resources(
                api,
                plan_path=plan,
                confirmation_sha256=confirmation,
            )
            repeated = tars_runpod_release.retire_legacy_resources(
                api,
                plan_path=plan,
                confirmation_sha256=confirmation,
            )
        self.assertEqual(retired, 2)
        self.assertEqual(repeated, 0)
        self.assertEqual(
            [endpoint["id"] for endpoint in api.endpoints],
            ["stable-endpoint"],
        )
        self.assertEqual(
            [template["id"] for template in api.templates],
            ["stable-template"],
        )
        self.assertEqual([auth["id"] for auth in api.auths], ["stable-auth"])
        self.assertIn("zero:endpoint-old-one", api.calls)
        self.assertIn("zero:endpoint-old-two", api.calls)

    def test_legacy_retirement_refuses_active_non_exited_worker_before_mutation(
        self,
    ) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.seed_release(
            "c" * 40, "busy", datetime.now(timezone.utc) - timedelta(days=2)
        )
        legacy = next(
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] == "endpoint-busy"
        )
        legacy["pods"] = [{"id": "busy-worker", "desiredStatus": "RUNNING"}]
        legacy["workers"] = [
            {
                "id": "busy-worker",
                "image": self.IMAGE,
                "containerRegistryAuthId": "auth-busy",
                "slsVersion": 1,
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            plan = Path(directory) / "legacy-plan.json"
            confirmation = tars_runpod_release.build_legacy_retirement_plan(
                api, stable_ids=self.stable_ids(), output_path=plan
            )
            with self.assertRaisesRegex(
                RunpodReleaseError, "active worker work"
            ):
                tars_runpod_release.retire_legacy_resources(
                    api,
                    plan_path=plan,
                    confirmation_sha256=confirmation,
                )
        self.assertFalse(any(call.startswith("zero:") for call in api.calls))
        self.assertFalse(
            any(call.startswith("delete_") for call in api.calls)
        )

    def test_legacy_retirement_allows_historical_exited_worker_rows(self) -> None:
        api = FakeReleaseAPI()
        api.seed_stable(self.TARGET_IMAGE, version=8)
        api.seed_release(
            "c" * 40, "exited", datetime.now(timezone.utc) - timedelta(days=2)
        )
        legacy = next(
            endpoint
            for endpoint in api.endpoints
            if endpoint["id"] == "endpoint-exited"
        )
        legacy["pods"] = [{"id": "old-worker", "desiredStatus": "EXITED"}]
        legacy["workers"] = [
            {
                "id": "old-worker",
                "image": self.IMAGE,
                "containerRegistryAuthId": "auth-exited",
                "slsVersion": 1,
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            plan = Path(directory) / "legacy-plan.json"
            confirmation = tars_runpod_release.build_legacy_retirement_plan(
                api, stable_ids=self.stable_ids(), output_path=plan
            )
            retired = tars_runpod_release.retire_legacy_resources(
                api,
                plan_path=plan,
                confirmation_sha256=confirmation,
            )
        self.assertEqual(retired, 1)

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
            GPU_POOL_SELECTOR.split(","),
            ["AMPERE_16", "AMPERE_24", "ADA_24"],
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
        for field in (
            "type",
            "gpuIds",
            "locations",
            "networkVolumeId",
            "flashBootType",
            "scalerType",
            "scalerValue",
        ):
            self.assertIn(field, query)
        self.assertRegex(query, r"env\s*\{\s*key\s*\}")
        self.assertNotRegex(query, r"env\s*\{[^}]*\bvalue\b")
        for rest_only_field in ("gpuCount", "computeType", "executionTimeoutMs"):
            self.assertNotIn(rest_only_field, query)

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
            {"includeTemplate": ["true"], "includeWorkers": ["true"]},
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

class WorkflowContractTest(unittest.TestCase):
    ROOT = Path(__file__).resolve().parent.parent.parent

    def test_deploy_workflow_has_immutable_and_non_cancelled_deploy_contract(self) -> None:
        import re

        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("types: [tars-deploy]", workflow)
        self.assertNotIn("id-token:", workflow)
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
        ssh_option_blocks = workflow.count("ssh_options=(")
        self.assertGreaterEqual(ssh_option_blocks, 1)
        self.assertEqual(
            workflow.count("ServerAliveInterval=15"), ssh_option_blocks
        )
        self.assertEqual(
            workflow.count("ServerAliveCountMax=3"), ssh_option_blocks
        )
        self.assertEqual(workflow.count("TCPKeepAlive=yes"), ssh_option_blocks)
        self.assertNotIn("/pulls", workflow)
        self.assertNotIn("tars_tree_attestation.py", workflow)
        self.assertNotIn("permission-pull-requests", workflow)
        self.assertNotIn("permission-statuses", workflow)
        self.assertEqual(workflow.count("git/ref/heads/main"), 3)
        self.assertNotIn("gh api /installation", workflow)
        self.assertNotIn("Dockerfile.worker", workflow)
        self.assertNotIn("TARS_WORKER_IMAGE", workflow)
        self.assertIn(
            "DISPATCHER_DIGEST: ${{ steps.images.outputs.dispatcher_digest }}",
            workflow,
        )
        self.assertIn("GPU_DIGEST: ${{ steps.images.outputs.gpu_digest }}", workflow)
        provider_image = (
            "${{ steps.artifact.outputs.registry }}:gpu-sha-"
            "${{ steps.payload.outputs.sha }}@"
            "${{ steps.artifact.outputs.gpu_digest }}"
        )
        application_image = (
            "${{ steps.artifact.outputs.registry }}@"
            "${{ steps.artifact.outputs.gpu_digest }}"
        )
        self.assertEqual(workflow.count(provider_image), 1)
        self.assertEqual(workflow.count(application_image), 2)
        self.assertEqual(workflow.count("APPLICATION_GPU_IMAGE:"), 2)
        self.assertIn("target_app_gpu_image", workflow)
        self.assertIn("source/Dockerfile.dispatcher", workflow)
        self.assertIn("source/Dockerfile.gpu", workflow)
        self.assertNotIn("tars_worker_render_gate.py", workflow)
        self.assertIn("tars_tada_bundle.py", workflow)
        self.assertEqual(
            workflow.count("tada_bundle=${{ runner.temp }}/tada-bundle"), 2
        )
        self.assertEqual(workflow.count("tars_runpod_release.py prepare"), 1)
        self.assertEqual(workflow.count("tars_runpod_release.py stage"), 1)
        self.assertEqual(workflow.count("tars_runpod_release.py rollback"), 2)
        self.assertEqual(workflow.count("tars_runpod_release.py verify-target"), 2)
        self.assertEqual(workflow.count("verify-application \\"), 3)
        self.assertEqual(workflow.count("--dispatcher-already-drained"), 3)
        self.assertEqual(
            workflow.count(
                'deployer="/srv/tars/incoming/$target_sha/deploy/tars-deploy"'
            ),
            1,
        )
        self.assertEqual(
            workflow.count(
                'deployer="/srv/tars/incoming/$recovery_sha/deploy/tars-deploy"'
            ),
            1,
        )
        self.assertIn(
            "RECOVERY_SHA: ${{ steps.payload.outputs.sha }}",
            workflow,
        )
        self.assertNotIn("--filter desired-state=running", workflow)
        dispatcher_zero_scales = workflow.count(
            "docker service scale tars_dispatcher=0"
        ) + workflow.count('docker service scale "$service=0"')
        self.assertEqual(dispatcher_zero_scales, 5)
        self.assertEqual(
            workflow.count("--format '{{.CurrentState}}'"),
            dispatcher_zero_scales,
        )
        self.assertEqual(
            workflow.count(
                "Complete|Shutdown|Failed|Rejected|Remove|Orphaned"
            ),
            dispatcher_zero_scales,
        )
        self.assertNotIn("tars_runpod_release.py finalize", workflow)
        self.assertNotIn("tars_runpod_release.py ensure", workflow)
        self.assertNotIn("tars_runpod_release.py pre-prune", workflow)
        self.assertNotIn("tars_runpod_release.py prune", workflow)
        self.assertNotIn("tars_runpod_release.py migrate", workflow)
        self.assertNotIn("tars_runpod_release.py bootstrap", workflow)
        self.assertNotIn("tars_runpod_release.py retire-legacy", workflow)
        self.assertEqual(
            workflow.count("--workflow central/.github/workflows/tars-unlock.yml"),
            2,
        )
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
        self.assertNotIn("--protected-release-sha", workflow)
        self.assertNotIn("current-runpod-endpoint", workflow)
        self.assertNotIn("tars_runpod_release.py select-previous", workflow)
        self.assertNotIn("/srv/tars/deployment/runpod-previous-endpoint", workflow)
        self.assertIn("RUNPOD_ENDPOINT_ID", workflow)
        self.assertIn("RUNPOD_TEMPLATE_ID", workflow)
        self.assertIn("RUNPOD_REGISTRY_AUTH_ID", workflow)
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
        source_upload = deploy.index("Upload the verified source bundle")
        lock_preflight = deploy.index("Refuse an orphaned deployment lock")
        final_main = deploy.index("Confirm the final current main revision")
        pause = deploy.index(
            "Reconcile any durable boundary before changing application state"
        )
        interrupted_describe = deploy.index(
            "Describe the interrupted rollback boundary"
        )
        interrupted_classify = deploy.index(
            "Classify the interrupted application transaction"
        )
        interrupted_reconcile = deploy.index(
            "Reconcile the interrupted provider generation"
        )
        interrupted_verify = deploy.index(
            "Verify recovery and retire the interrupted boundary"
        )
        baseline = deploy.index("Capture the coupled application baseline")
        prepare = deploy.index(
            "Prepare and persist the coupled rollout boundary"
        )
        stage = deploy.index(
            "Stage the immutable image on the stable Runpod template"
        )
        self.assertLess(source_upload, lock_preflight)
        self.assertLess(lock_preflight, final_main)
        self.assertLess(final_main, pause)
        self.assertLess(pause, interrupted_describe)
        self.assertLess(interrupted_describe, interrupted_classify)
        self.assertLess(interrupted_classify, interrupted_reconcile)
        self.assertLess(interrupted_describe, interrupted_reconcile)
        self.assertLess(interrupted_reconcile, interrupted_verify)
        self.assertLess(interrupted_verify, baseline)
        self.assertLess(baseline, prepare)
        self.assertLess(prepare, stage)
        self.assertLess(pause, stage)
        lock_preflight_command = deploy[lock_preflight:final_main]
        self.assertIn("docker config inspect tars_deploy_lock", lock_preflight_command)
        self.assertIn(
            "Run 'Manual TARS deployment-lock recovery'",
            lock_preflight_command,
        )
        self.assertIn("expected_owner=$owner", lock_preflight_command)
        self.assertIn("recovery_sha=$recovery_sha", lock_preflight_command)
        initial_boundary = deploy[pause:interrupted_describe]
        self.assertIn("exit 42", initial_boundary)
        self.assertIn(
            "the durable Runpod boundary is invalid or unreadable",
            initial_boundary,
        )
        self.assertIn("stat -c '%a'", initial_boundary)
        self.assertIn("stat -c '%u'", initial_boundary)
        self.assertIn("describe-boundary", initial_boundary)
        self.assertIn('if [ "$kind" = application ]', initial_boundary)
        self.assertIn("verify-application", initial_boundary)
        self.assertLess(
            initial_boundary.index("verify-application"),
            initial_boundary.index(
                'docker service scale "tars_dispatcher=$replicas"'
            ),
        )
        self.assertLess(
            initial_boundary.index(
                'docker service scale "tars_dispatcher=$replicas"'
            ),
            initial_boundary.index('rm -f "$boundary"'),
        )
        interrupted_verification = deploy[interrupted_verify:baseline]
        self.assertIn("verify-application", interrupted_verification)
        self.assertIn("TARGET_APP_GPU_IMAGE", interrupted_verification)
        self.assertIn("TARS_GPU_IMAGE_DIGEST", interrupted_verification)
        self.assertIn("TARS_RUNPOD_ENDPOINT_ID", interrupted_verification)
        self.assertIn(
            "docker service scale tars_dispatcher=1",
            interrupted_verification,
        )
        self.assertLess(
            interrupted_verification.index("verify-application"),
            interrupted_verification.index(
                "docker service scale tars_dispatcher=1"
            ),
        )
        self.assertLess(
            interrupted_verification.index(
                'test "$live_endpoint" = "$expected_endpoint"'
            ),
            interrupted_verification.rindex('rm -f "$boundary"'),
        )
        prepare_command = deploy[prepare:stage]
        self.assertIn("--prior-release-sha", prepare_command)
        self.assertIn("--prior-gpu-image", prepare_command)
        self.assertIn("--greenfield", prepare_command)
        self.assertIn("install -m 0600", prepare_command)
        self.assertIn("stat -c '%u'", prepare_command)
        self.assertLess(
            prepare_command.index("tars_runpod_release.py prepare"),
            prepare_command.index("install -m 0600"),
        )
        self.assertLess(
            prepare_command.index("install -m 0600"),
            prepare_command.index('mv -f "$temporary" "$boundary"'),
        )
        baseline_command = deploy[baseline:prepare]
        self.assertIn("prepare-application", baseline_command)
        self.assertIn("tars-application-boundary.json", baseline_command)
        self.assertIn(
            'if [ "$replicas" != 1 ]',
            baseline_command,
        )
        self.assertLess(
            baseline_command.index("prepare-application"),
            baseline_command.index("install -m 0600"),
        )
        self.assertLess(
            baseline_command.index('mv -f "$temporary" "$boundary"'),
            baseline_command.index(
                "docker service scale tars_dispatcher=0"
            ),
        )
        self.assertIn("sha256sum", prepare_command)
        self.assertIn('if [ "$app_mode" = existing ]', prepare_command)
        self.assertLess(
            stage,
            deploy.index("Pass delegated runtime secrets"),
        )
        rollback = deploy.index(
            "Reconcile the durable generation after failure or cancellation"
        )
        cleanup = deploy.index("Remove runner-side deployment credentials")
        application_step = deploy.index(
            "Pass delegated runtime secrets to the deployment interface"
        )
        accepted_provider = deploy.index(
            "Verify the smoke-accepted provider generation"
        )
        commit_generation = deploy.index(
            "Commit the accepted app-provider generation"
        )
        self.assertLess(application_step, accepted_provider)
        self.assertLess(accepted_provider, commit_generation)
        accepted_provider_command = deploy[
            accepted_provider:commit_generation
        ]
        self.assertIn(
            "tars_runpod_release.py verify-target",
            accepted_provider_command,
        )
        accepted_application_command = deploy[commit_generation:rollback]
        self.assertIn(
            "APPLICATION_GPU_IMAGE", accepted_application_command
        )
        self.assertNotIn(":gpu-sha-", accepted_application_command)
        self.assertLess(application_step, rollback)
        self.assertLess(rollback, cleanup)
        rollback_command = deploy[rollback:cleanup]
        self.assertIn("if: failure() || cancelled()", rollback_command)
        self.assertIn("--dispatcher-already-drained", rollback_command)
        self.assertIn(
            'deployer="/srv/tars/incoming/$target_sha/deploy/tars-deploy"',
            rollback_command,
        )
        self.assertIn("docker service scale \"$service=0\"", rollback_command)
        self.assertIn("tars_runpod_release.py rollback", rollback_command)
        self.assertIn("/srv/tars/deployment/runpod-rollout-boundary.json", rollback_command)
        self.assertIn("tars_runpod_release.py describe", rollback_command)
        self.assertIn("tars_runpod_release.py describe-boundary", rollback_command)
        self.assertIn("target_app_gpu_image", rollback_command)
        self.assertIn("APPLICATION_GPU_IMAGE", rollback_command)
        self.assertIn('if [ "$kind" = application ]', rollback_command)
        self.assertIn("verify-application", rollback_command)
        self.assertIn("tars_runpod_release.py verify-target", rollback_command)
        self.assertIn('test "$live_sha" = "$expected_sha"', rollback_command)
        self.assertIn('test "$live_gpu" = "$expected_gpu"', rollback_command)
        self.assertIn(
            'test "$live_endpoint" = "$expected_endpoint"', rollback_command
        )
        self.assertIn("/srv/tars/deployment/current.json", rollback_command)
        self.assertLess(
            rollback_command.index("verify-application"),
            rollback_command.index(
                'docker service scale "tars_dispatcher=$replicas"'
            ),
        )
        self.assertLess(
            rollback_command.index(
                "deployment lock does not belong to the rollout boundary"
            ),
            rollback_command.index("tars_runpod_release.py rollback"),
        )
        interrupted_classification = deploy[
            interrupted_classify:interrupted_reconcile
        ]
        self.assertLess(
            interrupted_classification.index(
                "interrupted application lock does not belong"
            ),
            deploy[interrupted_classify:].index(
                "Reconcile the interrupted provider generation"
            ),
        )
        self.assertLess(
            rollback_command.rindex("tars_runpod_release.py rollback"),
            rollback_command.rindex('docker service scale tars_dispatcher=1'),
        )
        self.assertLess(
            rollback_command.rindex(
                'test "$live_endpoint" = "$expected_endpoint"'
            ),
            rollback_command.rindex('rm -f "$boundary"'),
        )
        self.assertNotIn("tars-dispatcher-before-rollout", deploy)
        for temporary_name in (
            "tars-initial-app-state",
            "tars-initial-boundary-values",
            "tars-app-baseline",
            "tars-application-boundary.json",
            "tars-recovered-application-boundary.json",
            "tars-prior-runpod-boundary.json",
            "tars-runpod-rollout.json",
            "tars-runpod-rollback-values",
        ):
            self.assertIn(temporary_name, deploy[cleanup:])
        bootstrap = deploy.index('"$bundle/deploy/tars-deploy" bootstrap-stateful')
        application = deploy.index('exec "$bundle/deploy/tars-deploy" deploy')
        self.assertLess(bootstrap, application)
        bootstrap_command = deploy[bootstrap:application]
        self.assertIn('--bundle-dir "$bundle" || exit', bootstrap_command)
        self.assertNotIn("stateful_record=", deploy)
        application_command = deploy[application:accepted_provider]
        self.assertIn("--dispatcher-already-drained", application_command)

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

    def test_unlock_workflow_reuses_the_verified_recovery_interface(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-unlock.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("expected_owner:", workflow)
        self.assertIn("recovery_sha:", workflow)
        self.assertIn('RECOVERY_REF: ${{ github.ref }}', workflow)
        self.assertIn(
            'if [ "$RECOVERY_REF" != "refs/heads/main" ]; then',
            workflow,
        )
        self.assertIn("group: tars-production-delivery", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn("timeout-minutes: 15", workflow)
        for reference in (
            "actions/checkout@v7.0.0",
            "actions/create-github-app-token@v3.2.0",
            "Infisical/secrets-action@v1.0.16",
            "rohittp0/wiregaurd@v3",
        ):
            self.assertIn(reference, workflow)
        self.assertEqual(workflow.count("secret-path: /deployment"), 1)
        self.assertNotIn("secret-path: /runtime", workflow)
        self.assertIn("tars_runner_secrets.py capture-connection", workflow)
        self.assertNotIn("remote-secrets.sh", workflow)
        self.assertIn("StrictHostKeyChecking=yes", workflow)
        self.assertIn("ServerAliveInterval=15", workflow)
        self.assertIn("ServerAliveCountMax=3", workflow)
        self.assertIn("TCPKeepAlive=yes", workflow)
        self.assertNotIn("ssh-keyscan", workflow)
        self.assertNotIn("set -x", workflow)
        self.assertEqual(
            workflow.count("repos/Lascade-Co/TARS/git/ref/heads/main"),
            2,
        )
        self.assertIn('if [ "$sha" != "$RECOVERY_SHA" ]; then', workflow)
        self.assertIn('if [ "$live_main_sha" != "$RECOVERY_SHA" ]; then', workflow)
        self.assertIn('bundle="/srv/tars/incoming/$recovery_sha"', workflow)
        self.assertNotIn('bundle="/srv/tars/incoming/$expected_owner"', workflow)
        self.assertIn(
            'test "$(cat "$bundle/.tars-release-sha")" = "$recovery_sha"',
            workflow,
        )
        self.assertIn("export TARS_SECRET_SOURCE=environment", workflow)
        self.assertIn(
            '"$bundle/deploy/tars-deploy" unlock --expected-owner "$expected_owner"',
            workflow,
        )
        self.assertNotIn(
            "/srv/tars/deployment/runpod-previous-endpoint", workflow
        )
        cleanup = workflow[workflow.index("Remove runner-side recovery credentials") :]
        self.assertIn("if: always()", cleanup)

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
