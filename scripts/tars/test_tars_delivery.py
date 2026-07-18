import base64
import json
import os
import stat
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from tars_lock_outputs import (
    release_values,
    validate_action_versions,
    values,
    write_release_environment,
)
from tars_payload import load_payload
from tars_runner_secrets import (
    BUILD_KEYS,
    DEPLOY_KEYS,
    DEPLOYMENT_KEYS,
    RUNTIME_KEYS,
    RunnerSecretError,
    capture_build,
    capture_deploy,
)
from tars_tada_bundle import BundleFetchError, fetch, validate_bundle_shape, validate_inputs
from tars_worker_render_gate import (
    ALLOWED_HOSTS,
    OUTPUT_DIMENSIONS,
    RENDER_CONTAINER_LIMITS,
    RenderGateError,
    expected_worker_labels,
    load_variants,
    prepare_bind_directory,
    render_command,
    validate_event_stream,
    validate_ffprobe,
    validate_iso_bmff,
    validate_schema_v2_event,
    validate_worker_labels,
)


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
                    "worker": "sha256:" + "2" * 64,
                    "garage": "sha256:" + "3" * 64,
                    "otel": "sha256:" + "4" * 64,
                },
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
            self.assertIn("TADA_ESTIMATE_HD_REALTIME_FACTOR=5.5", rendered)
            self.assertIn("TADA_ESTIMATE_4K_REALTIME_FACTOR=4.0", rendered)
            self.assertIn(
                "TADA_ESTIMATE_MODEL_VERSION=do-s-1vcpu-1gb-hd-v1", rendered
            )
            self.assertIn("WORKER_STOP_GRACE_PERIOD=50m", rendered)
            self.assertNotIn("TOKEN", rendered)

    def test_rejects_noncanonical_built_digest(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            json.dump({"registry": "registry.example/tars", "images": {}}, handle)
        with self.assertRaisesRegex(ValueError, "lowercase hex"):
            release_values(
                Path(handle.name),
                {"api": "latest", "worker": "latest", "garage": "latest", "otel": "latest"},
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
            cleared = github_env.read_text(encoding="utf-8")
            self.assertEqual(
                cleared,
                "".join(f"{name}=\n" for name in DEPLOYMENT_KEYS),
            )

    def test_build_capture_requires_only_the_write_credential(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture_build(
                {"DOCR_WRITE_TOKEN": "write-token"},
                root / "docker",
                root / "github-env",
            )
            self.assertEqual(BUILD_KEYS, ("DOCR_WRITE_TOKEN",))

    def test_build_capture_clears_known_exports_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            github_env = root / "github-env"
            with self.assertRaisesRegex(RunnerSecretError, "DOCR_WRITE_TOKEN"):
                capture_build({}, root / "docker", github_env)
            self.assertEqual(
                github_env.read_text(encoding="utf-8"),
                "".join(f"{name}=\n" for name in DEPLOYMENT_KEYS),
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


class WorkerRenderGateTest(unittest.TestCase):
    def test_bind_directories_are_world_writable_despite_runner_umask(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            previous_umask = os.umask(0o022)
            try:
                attempt = root / "hd" / "attempt"
                cache = root / "cache"
                prepare_bind_directory(attempt, parents=True)
                prepare_bind_directory(cache)
            finally:
                os.umask(previous_umask)

            self.assertEqual(stat.S_IMODE(attempt.stat().st_mode), 0o777)
            self.assertEqual(stat.S_IMODE(cache.stat().st_mode), 0o777)

    def test_derives_the_tada_4k_enum_from_the_hd_smoke_config(self) -> None:
        config = {
            "schema_version": 4,
            "animation_state": {
                "resolution": "RESOLUTION_HD",
                "video_duration": 2,
            },
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            json.dump(config, handle)
            path = Path(handle.name)
        variants = load_variants(path)
        self.assertEqual([variant[0] for variant in variants], ["hd", "4k"])
        four_k = json.loads(variants[1][2])
        self.assertEqual(four_k["animation_state"]["resolution"], "RESOLUTION_FHD")

    def test_uses_fixed_stdin_argv_and_the_exact_allowlist(self) -> None:
        command = render_command(
            "tars-worker-render-gate:sha-" + "a" * 40,
            Path("/tmp/gate"),
            "4k",
            "gate-container",
        )
        self.assertIn(
            "TADA_ALLOWED_HOSTS=" + ",".join(ALLOWED_HOSTS),
            command,
        )
        limits = command.index("--cpus")
        self.assertEqual(command[limits : limits + 6], list(RENDER_CONTAINER_LIMITS))
        self.assertEqual(
            RENDER_CONTAINER_LIMITS,
            ("--cpus", "1", "--memory", "640m", "--memory-swap", "1280m"),
        )
        tada = command.index("/opt/tada/bin/tada")
        self.assertEqual(
            command[tada + 2 :],
            [
                "render",
                "-",
                "-o",
                "/gate/4k/attempt/output.mp4",
                "--cache-dir",
                "/gate/cache",
                "--attempt-dir",
                "/gate/4k/attempt",
                "--progress-json",
            ],
        )

    def test_validates_progress_iso_bmff_and_ffprobe_contracts(self) -> None:
        output = "/gate/hd/attempt/output.mp4"
        events = b"\n".join(
            json.dumps(event, separators=(",", ":")).encode()
            for event in (
                {
                    "schema_version": 2,
                    "event": "estimate",
                    "estimated_seconds": 2,
                    "video_duration_seconds": 2,
                    "resolution": "hd",
                    "estimate_model_version": "cpu-heuristic-v1",
                },
                {
                    "schema_version": 2,
                    "event": "progress",
                    "phase": "render",
                    "completed": 1,
                    "total": 2,
                },
                {"schema_version": 2, "event": "completed", "output": output},
            )
        )
        validate_event_stream(events, "hd", output)
        validate_ffprobe(
            b'{"streams":[{"codec_type":"video","width":1080,"height":1080}],'
            b'"format":{"format_name":"mov,mp4"}}',
            OUTPUT_DIMENSIONS["hd"],
        )
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            for box_type in (b"ftyp", b"moov", b"mdat"):
                handle.write(struct.pack(">I4s", 8, box_type))
            path = Path(handle.name)
        self.assertEqual(validate_iso_bmff(path), 24)

    def test_validates_tars_and_locked_tada_image_labels(self) -> None:
        source_sha = "a" * 40
        tada_revision = "b" * 40
        tada_oci = "ghcr.io/lascade-co/tada-wheel@sha256:" + "c" * 64
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            json.dump(
                {"tada": {"oci": tada_oci, "revision": tada_revision}},
                handle,
            )
            lock = Path(handle.name)
        expected = expected_worker_labels(lock, source_sha)
        self.assertEqual(
            expected,
            {
                "org.opencontainers.image.revision": source_sha,
                "com.lascade.tada.bundle": tada_oci,
                "com.lascade.tada.revision": tada_revision,
            },
        )
        validate_worker_labels(json.dumps(expected).encode(), expected)
        with self.assertRaisesRegex(RenderGateError, "com.lascade.tada.revision"):
            validate_worker_labels(
                json.dumps({**expected, "com.lascade.tada.revision": "wrong"}).encode(),
                expected,
            )

    def test_rejects_wrong_video_dimensions(self) -> None:
        probe = (
            b'{"streams":[{"codec_type":"video","width":1080,"height":1080}],'
            b'"format":{"format_name":"mov,mp4"}}'
        )
        with self.assertRaisesRegex(RenderGateError, "2160x2160"):
            validate_ffprobe(probe, OUTPUT_DIMENSIONS["4k"])

    def test_rejects_a_failed_renderer_terminal(self) -> None:
        events = b"\n".join(
            (
                b'{"schema_version":2,"event":"estimate","estimated_seconds":2,'
                b'"video_duration_seconds":2,"resolution":"4k",'
                b'"estimate_model_version":"cpu-test-v1"}',
                b'{"schema_version":2,"event":"progress","phase":"render","completed":0,"total":1}',
                b'{"schema_version":2,"event":"failed","exit_code":1,'
                b'"error_code":"internal_render_error","retryable":true,'
                b'"message":"render failed"}',
            )
        )
        with self.assertRaisesRegex(RenderGateError, "confirm the requested output"):
            validate_event_stream(events, "4k", "/gate/4k/attempt/output.mp4")

    def test_rejects_schema_v2_events_that_production_rejects(self) -> None:
        invalid_events = (
            {"schema_version": 2, "event": "estimate", "resolution": "hd"},
            {
                "schema_version": 2,
                "event": "estimate",
                "estimated_seconds": 0,
                "video_duration_seconds": 2,
                "resolution": "hd",
                "estimate_model_version": "cpu-test-v1",
            },
            {"schema_version": 2, "event": "progress", "phase": "render"},
            {
                "schema_version": 2,
                "event": "progress",
                "phase": "render",
                "completed": 2,
                "total": 1,
            },
            {
                "schema_version": 2,
                "event": "completed",
                "output": "/gate/hd/attempt/output.mp4",
                "surprise": True,
            },
            {
                "schema_version": 2,
                "event": "completed",
                "output": "/gate/hd/attempt/output.mp4",
                "retryable": False,
            },
            {
                "schema_version": 2,
                "event": "failed",
                "exit_code": 1,
                "error_code": "internal_render_error",
                "retryable": True,
            },
        )
        for event in invalid_events:
            with self.subTest(event=event):
                with self.assertRaises(RenderGateError):
                    validate_schema_v2_event(event)

    def test_accepts_every_valid_schema_v2_event_variant(self) -> None:
        valid_events = (
            {
                "schema_version": 2,
                "event": "estimate",
                "estimated_seconds": 2,
                "video_duration_seconds": 0,
                "resolution": "hd",
                "estimate_model_version": "cpu-test-v1",
            },
            {
                "schema_version": 2,
                "event": "progress",
                "phase": "assets",
                "completed": 0,
                "total": None,
            },
            {
                "schema_version": 2,
                "event": "completed",
                "output": "/gate/hd/attempt/output.mp4",
            },
            {
                "schema_version": 2,
                "event": "failed",
                "exit_code": 2,
                "error_code": "worker_io_failed",
                "retryable": False,
                "message": "write failed",
            },
        )
        for event in valid_events:
            with self.subTest(event=event):
                validate_schema_v2_event(event)

    def test_rejects_success_stream_state_that_production_rejects(self) -> None:
        output = "/gate/hd/attempt/output.mp4"
        estimate = {
            "schema_version": 2,
            "event": "estimate",
            "estimated_seconds": 2,
            "video_duration_seconds": 2,
            "resolution": "hd",
            "estimate_model_version": "cpu-test-v1",
        }
        progress = {
            "schema_version": 2,
            "event": "progress",
            "phase": "render",
            "completed": 1,
            "total": 2,
        }
        completed = {"schema_version": 2, "event": "completed", "output": output}
        invalid_streams = (
            (estimate, progress, completed, progress),
            (estimate, estimate, progress, completed),
            (progress, estimate, completed),
            (
                estimate,
                progress,
                {**progress, "phase": "tiles"},
                completed,
            ),
            (
                estimate,
                {**progress, "phase": "contact_sheet"},
                completed,
            ),
            (estimate, progress, {**progress, "completed": 0}, completed),
            (estimate, progress, {**progress, "total": 3}, completed),
            (
                {**estimate, "estimated_seconds": (1 << 63) - 1},
                progress,
                completed,
            ),
            (estimate, progress, {**completed, "output": "/wrong/output.mp4"}),
        )
        for events in invalid_streams:
            with self.subTest(events=events):
                stdout = b"\n".join(
                    json.dumps(event, separators=(",", ":")).encode()
                    for event in events
                )
                with self.assertRaises(RenderGateError):
                    validate_event_stream(stdout, "hd", output)


class WorkflowContractTest(unittest.TestCase):
    ROOT = Path(__file__).resolve().parent.parent.parent

    def test_deploy_workflow_has_immutable_and_non_cancelled_deploy_contract(self) -> None:
        import re

        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("types: [tars-deploy]", workflow)
        self.assertIn("id-token: write", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn("caps worker_count at six", workflow)
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
        self.assertEqual(workflow.count("ServerAliveInterval=15"), 2)
        self.assertEqual(workflow.count("ServerAliveCountMax=3"), 2)
        self.assertEqual(workflow.count("TCPKeepAlive=yes"), 2)
        self.assertNotIn("/pulls", workflow)
        self.assertNotIn("tars_tree_attestation.py", workflow)
        self.assertNotIn("permission-pull-requests", workflow)
        self.assertNotIn("permission-statuses", workflow)
        self.assertEqual(workflow.count("git/ref/heads/main"), 3)
        self.assertNotIn("gh api /installation", workflow)
        self.assertIn("WORKER_DIGEST: ${{ steps.worker.outputs.digest }}", workflow)
        self.assertNotIn("tars_worker_render_gate.py", workflow)
        self.assertIn("tars_tada_bundle.py", workflow)
        self.assertIn("tada_bundle=${{ runner.temp }}/tada-bundle", workflow)
        self.assertNotIn("ghcr_token=${{ github.token }}", workflow)
        build, deploy = workflow.split("\n  deploy:\n", 1)
        self.assertEqual(workflow.count("packages: read"), 1)
        self.assertIn("packages: read", build)
        self.assertNotIn("packages: read", deploy)
        self.assertLess(
            deploy.index("Confirm the final current main revision"),
            deploy.index("Pass delegated runtime secrets"),
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
