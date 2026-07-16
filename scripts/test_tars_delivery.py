import base64
import io
import json
import os
import stat
import struct
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from tars_infisical import (
    AUDIENCE,
    IDENTITY_ID,
    PROJECT_ID,
    append_deploy_outputs,
    get_secret,
    login_with_oidc,
    write_docker_config,
    write_private,
)
from tars_lock_outputs import (
    release_values,
    validate_action_pins,
    values,
    write_release_environment,
)
from tars_payload import load_payload
from tars_tada_bundle import BundleFetchError, validate_bundle_shape, validate_inputs
from tars_tree_attestation import (
    AttestationError,
    select_deploy_pull_request,
    tree_context,
    validate_ci_pull_request,
    validate_merge_commit,
    verify_tree_status,
)
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


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeOpener:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, *, timeout):
        self.requests.append((request, timeout))
        return FakeResponse(json.dumps(self.responses.pop(0)).encode("utf-8"))


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
                "ref": "refs/heads/main",
                "pr": None,
                "lock_file": "release/lock.json",
                "source_event": "push",
            }
        )
        with self.assertRaisesRegex(ValueError, "repository"):
            load_payload(event, "deploy")

    def test_rejects_deploy_payload_with_extra_keys(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
                "ref": "refs/heads/main",
                "pr": None,
                "lock_file": "release/lock.json",
                "source_event": "push",
                "image": "registry.example/mutable:latest",
            }
        )
        with self.assertRaisesRegex(ValueError, "unexpected image"):
            load_payload(event, "deploy")

    def test_rejects_manual_production_dispatch(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
                "ref": "refs/heads/main",
                "pr": None,
                "lock_file": "release/lock.json",
                "source_event": "workflow_dispatch",
            }
        )
        with self.assertRaisesRegex(ValueError, "must be push"):
            load_payload(event, "deploy")

    def test_accepts_exact_pull_request_payload(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
                "head_sha": "b" * 40,
                "ref": "refs/pull/12/merge",
                "pr": 12,
                "base_ref": "main",
                "base_sha": "c" * 40,
                "lock_file": "release/lock.json",
                "source_event": "pull_request_target",
            }
        )
        self.assertEqual(load_payload(event, "ci")["pr"], 12)

    def test_rejects_untrusted_pull_request_source_event(self) -> None:
        event = self.write_event(
            {
                "repo": "Lascade-Co/TARS",
                "sha": "a" * 40,
                "head_sha": "b" * 40,
                "ref": "refs/pull/12/merge",
                "pr": 12,
                "base_ref": "main",
                "base_sha": "c" * 40,
                "lock_file": "release/lock.json",
                "source_event": "pull_request",
            }
        )
        with self.assertRaisesRegex(ValueError, "pull_request_target"):
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
            self.assertIn("WORKER_STOP_GRACE_PERIOD=41m", rendered)
            self.assertNotIn("TOKEN", rendered)

    def test_rejects_noncanonical_built_digest(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            json.dump({"registry": "registry.example/tars", "images": {}}, handle)
        with self.assertRaisesRegex(ValueError, "lowercase hex"):
            release_values(
                Path(handle.name),
                {"api": "latest", "worker": "latest", "garage": "latest", "otel": "latest"},
            )

    def test_central_action_pins_must_match_source_release_lock(self) -> None:
        locked_commit = "a" * 40
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
                                "commit": locked_commit,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            workflow.write_text(
                f"steps:\n  - uses: actions/checkout@{locked_commit}\n",
                encoding="utf-8",
            )
            validate_action_pins(lock, [workflow])
            workflow.write_text(
                "steps:\n  - uses: actions/checkout@v6\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "does not match locked commit"):
                validate_action_pins(lock, [workflow])


class TrustedBundleFetchTest(unittest.TestCase):
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


class InfisicalTest(unittest.TestCase):
    def test_oidc_login_uses_github_audience_and_official_endpoint(self) -> None:
        opener = FakeOpener([{"value": "github-jwt"}, {"accessToken": "infisical-token"}])
        token = login_with_oidc(
            {
                "ACTIONS_ID_TOKEN_REQUEST_URL": "https://oidc.actions.example/token?api-version=2.0",
                "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
            },
            opener=opener,
        )
        self.assertEqual(token, "infisical-token")
        oidc_request = opener.requests[0][0]
        self.assertEqual(parse_qs(urlparse(oidc_request.full_url).query)["audience"], [AUDIENCE])
        self.assertEqual(oidc_request.get_header("Authorization"), "Bearer request-token")
        login_request = opener.requests[1][0]
        self.assertEqual(
            login_request.full_url,
            "https://secrets.lascade.com/api/v1/auth/oidc-auth/login",
        )
        self.assertEqual(
            json.loads(login_request.data),
            {"identityId": IDENTITY_ID, "jwt": "github-jwt"},
        )

    def test_get_secret_uses_exact_v4_read_not_list_or_export(self) -> None:
        opener = FakeOpener(
            [{"secret": {"secretKey": "DOCR_WRITE_TOKEN", "secretValue": "write-token"}}]
        )
        value = get_secret(
            "access-token",
            "/deployment",
            "DOCR_WRITE_TOKEN",
            opener=opener,
        )
        self.assertEqual(value, "write-token")
        request = opener.requests[0][0]
        parsed = urlparse(request.full_url)
        self.assertEqual(parsed.path, "/api/v4/secrets/DOCR_WRITE_TOKEN")
        query = parse_qs(parsed.query)
        self.assertEqual(query["projectId"], [PROJECT_ID])
        self.assertEqual(query["environment"], ["prod"])
        self.assertEqual(query["secretPath"], ["/deployment"])
        self.assertEqual(query["expandSecretReferences"], ["false"])
        self.assertEqual(request.get_header("Authorization"), "Bearer access-token")

    def test_docker_config_keeps_token_out_of_process_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            token_file = root / "token"
            write_private(token_file, "secret-token")
            config_path = write_docker_config(token_file, root / "docker")
            config = json.loads(config_path.read_text(encoding="utf-8"))
            encoded = config["auths"]["registry.digitalocean.com"]["auth"]
            self.assertEqual(base64.b64decode(encoded), b"secret-token:secret-token")
            self.assertEqual(config_path.stat().st_mode & 0o777, 0o600)

    def test_deploy_outputs_validate_host_user_and_wireguard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_private(root / "DEPLOY_SSH_HOST", "10.20.30.40")
            write_private(root / "DEPLOY_SSH_USER", "ubuntu")
            wireguard = base64.b64encode(b"[Interface]\nPrivateKey = secret\n").decode()
            write_private(root / "WIREGUARD_CONFIG", wireguard)
            output = root / "github-output"
            append_deploy_outputs(root, output)
            rendered = output.read_text(encoding="utf-8")
            self.assertIn("host<<", rendered)
            self.assertIn("10.20.30.40", rendered)
            self.assertIn("wireguard_config<<", rendered)


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


class TreeAttestationTest(unittest.TestCase):
    REPO = "Lascade-Co/TARS"
    MERGE = "a" * 40
    HEAD = "b" * 40
    BASE = "c" * 40
    TREE = "d" * 40

    def open_pull(self) -> dict:
        return {
            "number": 12,
            "state": "open",
            "merge_commit_sha": self.MERGE,
            "head": {"sha": self.HEAD, "repo": {"full_name": self.REPO}},
            "base": {
                "sha": self.BASE,
                "ref": "main",
                "repo": {"full_name": self.REPO},
            },
        }

    def merged_pull(self) -> dict:
        pull = self.open_pull()
        pull.update(
            {
                "state": "closed",
                "merged_at": "2026-07-16T12:00:00Z",
                "merge_commit_sha": self.MERGE,
            }
        )
        return pull

    def test_validates_live_pr_and_exact_synthetic_merge_parents(self) -> None:
        validate_ci_pull_request(
            self.open_pull(), self.REPO, 12, self.MERGE, self.HEAD, self.BASE
        )
        tree = validate_merge_commit(
            f"{self.MERGE}\n{self.TREE}\n{self.BASE} {self.HEAD}\n",
            self.MERGE,
            self.HEAD,
            self.BASE,
        )
        self.assertEqual(tree, self.TREE)
        with self.assertRaisesRegex(AttestationError, "parents"):
            validate_merge_commit(
                f"{self.MERGE}\n{self.TREE}\n{self.HEAD} {self.BASE}\n",
                self.MERGE,
                self.HEAD,
                self.BASE,
            )

    def test_deploy_requires_one_exact_merged_pull_request(self) -> None:
        number, head = select_deploy_pull_request(
            [self.merged_pull()], self.REPO, self.MERGE
        )
        self.assertEqual((number, head), (12, self.HEAD))
        with self.assertRaisesRegex(AttestationError, "exactly one"):
            select_deploy_pull_request(
                [self.merged_pull(), self.merged_pull()], self.REPO, self.MERGE
            )
        with self.assertRaisesRegex(AttestationError, "exactly one"):
            select_deploy_pull_request([], self.REPO, self.MERGE)

    def test_latest_exact_tree_status_must_be_central_success(self) -> None:
        context = tree_context(self.TREE)
        central_url = "https://github.com/Lascade-Co/actions/actions/runs/123"
        creator = "lascade-ci[bot]"
        verify_tree_status(
            [
                [
                    {
                        "context": context,
                        "state": "success",
                        "target_url": central_url,
                        "creator": {"login": creator},
                    }
                ]
            ],
            self.TREE,
            creator,
        )
        with self.assertRaisesRegex(AttestationError, "not successful"):
            verify_tree_status(
                [
                    {
                        "context": context,
                        "state": "failure",
                        "target_url": central_url,
                        "creator": {"login": creator},
                    },
                    {
                        "context": context,
                        "state": "success",
                        "target_url": central_url,
                        "creator": {"login": creator},
                    },
                ],
                self.TREE,
                creator,
            )
        with self.assertRaisesRegex(AttestationError, "not produced by central"):
            verify_tree_status(
                [
                    {
                        "context": context,
                        "state": "success",
                        "target_url": "https://attacker.example/run/1",
                        "creator": {"login": creator},
                    }
                ],
                self.TREE,
                creator,
            )
        with self.assertRaisesRegex(AttestationError, "creator"):
            verify_tree_status(
                [
                    {
                        "context": context,
                        "state": "success",
                        "target_url": central_url,
                        "creator": {"login": "attacker[bot]"},
                    }
                ],
                self.TREE,
                creator,
            )


class WorkflowContractTest(unittest.TestCase):
    ROOT = Path(__file__).resolve().parent.parent

    def test_deploy_workflow_has_immutable_and_non_cancelled_deploy_contract(self) -> None:
        import re

        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("types: [tars-deploy]", workflow)
        self.assertIn("id-token: write", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn("caps worker_count at seven", workflow)
        self.assertIn("timeout-minutes: 360", workflow)
        self.assertIn("timeout-minutes: 120", workflow)
        self.assertIn(".tars-release-sha", workflow)
        self.assertIn("DEPLOY_SSH_KNOWN_HOSTS", workflow)
        self.assertEqual(workflow.count("tars_infisical.py login"), 2)
        self.assertEqual(workflow.count("--secret-name DOCR_WRITE_TOKEN"), 1)
        self.assertEqual(
            re.findall(r"--secret-name ([A-Z0-9_]+)", workflow),
            [
                "DOCR_WRITE_TOKEN",
                "DEPLOY_SSH_HOST",
                "DEPLOY_SSH_USER",
                "DEPLOY_SSH_PRIVATE_KEY",
                "DEPLOY_SSH_KNOWN_HOSTS",
                "WIREGUARD_CONFIG",
            ],
        )
        self.assertIn("cat \"$RUNNER_TEMP/tars-deploy-secrets/infisical.token\"", workflow)
        self.assertNotIn("infisical export", workflow)
        self.assertNotIn("Infisical/secrets-action", workflow)
        self.assertNotIn("ssh-keyscan", workflow)
        self.assertNotIn(":latest", workflow)
        self.assertEqual(workflow.count("ServerAliveInterval=15"), 2)
        self.assertEqual(workflow.count("ServerAliveCountMax=3"), 2)
        self.assertEqual(workflow.count("TCPKeepAlive=yes"), 2)
        self.assertIn("commits/$RELEASE_SHA/pulls", workflow)
        self.assertIn("commits/$main_sha/pulls", workflow)
        self.assertEqual(workflow.count("tars_tree_attestation.py verify-statuses"), 2)
        self.assertEqual(workflow.count("--expected-creator"), 2)
        self.assertEqual(workflow.count("steps.app-token.outputs.app-slug"), 2)
        self.assertNotIn("gh api /installation", workflow)
        self.assertIn("steps.lock.outputs.registry }}@${{ steps.worker.outputs.digest", workflow)
        self.assertIn("Gate the exact pushed worker digest with HD and 4K renders", workflow)
        self.assertIn("tars_tada_bundle.py", workflow)
        self.assertIn("tada_bundle=${{ runner.temp }}/tada-bundle", workflow)
        self.assertNotIn("ghcr_token=${{ github.token }}", workflow)

    def test_delivery_workflow_uses_locked_transfer_and_network_actions(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
            workflow,
        )
        self.assertIn(
            "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
            workflow,
        )
        self.assertIn(
            "rohittp0/wiregaurd@e6a265873578a2ccbc5ffa73657ce0991b36e650",
            workflow,
        )

    def test_ci_uses_locked_postgres_nginx_tofu_and_both_stacks(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-ci.yml").read_text(
            encoding="utf-8"
        )
        postgres = (
            "docker.io/library/postgres@sha256:"
            "bb377b7239d2774ac8cc76f481596ce96c5a6b5e9d141f6d0a0ee371a6e7c0f2"
        )
        self.assertEqual(workflow.count(f"image: {postgres}"), 1)
        self.assertIn("LOCKED_POSTGRES_IMAGE: ${{ steps.lock.outputs.postgres_image }}", workflow)
        self.assertIn("NGINX_IMAGE: ${{ steps.lock.outputs.nginx_image }}", workflow)
        self.assertIn("sh deploy/nginx/update-cloudflare-cidrs.sh", workflow)
        self.assertIn(
            "git diff --exit-code -- deploy/nginx/cloudflare-real-ip.conf",
            workflow,
        )
        self.assertIn("sh deploy/nginx/check-config.sh", workflow)
        self.assertIn(
            "opentofu/setup-opentofu@9d84900f3238fab8cd84ce47d658d25dd008be2f",
            workflow,
        )
        self.assertIn('tofu_version: ${{ steps.lock.outputs.opentofu_version }}', workflow)
        self.assertIn("docker stack config -c stack.yml", workflow)
        self.assertIn("docker stack config -c stack.stateful.yml", workflow)
        self.assertIn(
            "TADA_ALLOWED_HOSTS: dashboard.lascade.com,api.maptiler.com,"
            "server.arcgisonline.com,firebasestorage.googleapis.com",
            workflow,
        )
        self.assertIn("load: true", workflow)
        self.assertIn("tars-worker-render-gate:sha-${{ steps.payload.outputs.sha }}", workflow)
        self.assertIn("timeout --signal=TERM --kill-after=30s 25m", workflow)
        self.assertIn("tars_worker_render_gate.py", workflow)
        self.assertIn("--config source/deploy/smoke-config.json", workflow)
        self.assertIn("--lock source/release/lock.json", workflow)
        self.assertIn("--source-sha ${{ steps.payload.outputs.sha }}", workflow)
        self.assertIn("tars_tree_attestation.py verify-ci-pr", workflow)
        self.assertIn("tars_tree_attestation.py verify-merge", workflow)
        self.assertIn("steps.payload.outputs.head_sha", workflow)
        self.assertIn("TARS Central CI tree ", tree_context("e" * 40))
        self.assertIn("build-contexts:", workflow)
        self.assertIn("tada_bundle=${{ runner.temp }}/tada-bundle", workflow)
        self.assertNotIn("ghcr_token=${{ github.token }}", workflow)
        self.assertIn("tars_tada_bundle.py", workflow)
        self.assertEqual(
            ALLOWED_HOSTS,
            (
                "dashboard.lascade.com",
                "api.maptiler.com",
                "server.arcgisonline.com",
                "firebasestorage.googleapis.com",
            ),
        )

    def test_ci_separates_status_credentials_from_pull_request_execution(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-ci.yml").read_text(
            encoding="utf-8"
        )
        prepare, remainder = workflow.split("\n  verify:\n", 1)
        verify, remainder = remainder.split("\n  worker_gate:\n", 1)
        worker_gate, finalize = remainder.split("\n  finalize:\n", 1)

        self.assertIn("\n  prepare:\n", prepare)
        self.assertEqual(workflow.count("permission-statuses: write"), 2)
        self.assertIn("permission-statuses: write", prepare)
        self.assertNotIn("permission-statuses: write", verify)
        self.assertNotIn("permission-statuses: write", worker_gate)
        self.assertIn("permission-statuses: write", finalize)
        self.assertNotIn("packages: read", verify)
        self.assertIn("packages: read", worker_gate)
        self.assertNotIn("Report final TARS status", verify)
        self.assertNotIn("Report final TARS status", worker_gate)
        self.assertNotIn("path: source", finalize)
        self.assertIn("needs: [prepare, verify, worker_gate]", finalize)
        self.assertIn("if: always() && needs.prepare.result == 'success'", finalize)
        self.assertIn("Mint a fresh TARS status token", finalize)
        self.assertEqual(workflow.count("ref: ${{ github.sha }}"), 4)
        self.assertIn("Fetch the locked TADA bundle with trusted immutable code", worker_gate)
        self.assertIn("tars_worker_render_gate.py", worker_gate)
        self.assertNotIn("tars_worker_render_gate.py", verify)
        self.assertNotIn("GHCR_TOKEN", verify)
        self.assertNotIn("python3 source/release/validate_lock.py", worker_gate)

    def test_delivery_jobs_pin_the_central_checkout_to_the_dispatch_sha(self) -> None:
        workflow = (self.ROOT / ".github/workflows/tars-deploy.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(workflow.count("ref: ${{ github.sha }}"), 2)

    def test_tars_workflows_pin_every_action_to_a_commit(self) -> None:
        import re

        for name in ("tars-ci.yml", "tars-deploy.yml"):
            workflow = (self.ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
            revisions = re.findall(r"uses:\s*[^@\s]+@([^\s#]+)", workflow)
            self.assertTrue(revisions)
            self.assertTrue(all(re.fullmatch(r"[0-9a-f]{40}", revision) for revision in revisions))


if __name__ == "__main__":
    unittest.main()
