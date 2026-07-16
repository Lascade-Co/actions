#!/usr/bin/env python3
"""Run representative HD and 4K renders in a just-built TARS worker image.

Each renderer gets 1 vCPU, 640 MiB RAM and 1280 MiB combined RAM-plus-swap.
That matches the production Swarm worker's 640-MiB memory limit and Docker's
default equal-sized swap allowance when Swarm cannot set ``memswap_limit``.
This immutable-image release gate does not replace the separate authenticated
smoke render after production rollout.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import shutil
import struct
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any


ALLOWED_HOSTS = (
    "dashboard.lascade.com",
    "api.maptiler.com",
    "server.arcgisonline.com",
    "firebasestorage.googleapis.com",
)
MAX_CONFIG_BYTES = 5 * 1024 * 1024
MAX_OUTPUT_BYTES = 2 * 1024 * 1024 * 1024
IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@-]{0,255}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
TADA_OCI = re.compile(r"ghcr\.io/lascade-co/tada-wheel@sha256:[0-9a-f]{64}\Z")
OUTPUT_DIMENSIONS = {"hd": (1080, 1080), "4k": (2160, 2160)}
RENDER_CONTAINER_LIMITS = (
    "--cpus",
    "1",
    "--memory",
    "640m",
    "--memory-swap",
    "1280m",
)
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
MAX_ESTIMATED_SECONDS = INT64_MAX // 2_000_000_000
PROGRESS_PHASES = ("assets", "tiles", "render", "contact_sheet")
PHASE_RANK = {phase: rank for rank, phase in enumerate(PROGRESS_PHASES, start=1)}
PRE_ESTIMATE_FAILURES = frozenset(
    {
        "config_too_large",
        "unsupported_schema_version",
        "invalid_render_config",
        "worker_configuration_error",
        "worker_io_failed",
    }
)
EVENT_FIELDS = {
    "estimate": frozenset(
        {
            "schema_version",
            "event",
            "estimated_seconds",
            "video_duration_seconds",
            "resolution",
            "estimate_model_version",
        }
    ),
    "progress": frozenset(
        {"schema_version", "event", "phase", "completed", "total"}
    ),
    "completed": frozenset({"schema_version", "event", "output"}),
    "failed": frozenset(
        {
            "schema_version",
            "event",
            "exit_code",
            "error_code",
            "retryable",
            "message",
        }
    ),
}


class RenderGateError(RuntimeError):
    """The immutable worker image failed its renderer release gate."""


def is_int64(value: Any) -> bool:
    return type(value) is int and INT64_MIN <= value <= INT64_MAX


def validate_schema_v2_event(event: Any) -> None:
    """Validate one event exactly as TARS' Go schema-v2 parser expects it."""

    if not isinstance(event, dict) or type(event.get("schema_version")) is not int:
        raise RenderGateError("renderer emitted an unsupported progress event")
    if event["schema_version"] != 2:
        raise RenderGateError("renderer emitted an unsupported progress event")

    event_name = event.get("event")
    if not isinstance(event_name, str) or event_name not in EVENT_FIELDS:
        raise RenderGateError("renderer emitted an unknown progress event")
    allowed = EVENT_FIELDS[event_name]
    unexpected = set(event) - allowed
    if unexpected:
        raise RenderGateError(
            f"renderer {event_name} event contains forbidden fields: "
            + ", ".join(sorted(unexpected))
        )

    if event_name == "estimate":
        required = allowed
        if set(event) != required:
            raise RenderGateError("renderer estimate event is missing required fields")
        estimated = event["estimated_seconds"]
        duration = event["video_duration_seconds"]
        if not is_int64(estimated) or estimated <= 0 or not is_int64(duration):
            raise RenderGateError("renderer estimate event has invalid durations")
        if event["resolution"] not in ("hd", "4k"):
            raise RenderGateError("renderer estimate event has an invalid resolution")
        model = event["estimate_model_version"]
        if not isinstance(model, str) or not model:
            raise RenderGateError("renderer estimate event has an invalid model version")
        return

    if event_name == "progress":
        required = allowed - {"total"}
        if not required.issubset(event):
            raise RenderGateError("renderer progress event is missing required fields")
        if event["phase"] not in PROGRESS_PHASES:
            raise RenderGateError("renderer emitted an unknown progress phase")
        completed = event["completed"]
        total = event.get("total")
        if not is_int64(completed) or completed < 0:
            raise RenderGateError("renderer progress event has invalid counters")
        if total is not None and (
            not is_int64(total) or total < 0 or completed > total
        ):
            raise RenderGateError("renderer progress event has invalid counters")
        return

    if event_name == "completed":
        if set(event) != allowed:
            raise RenderGateError("renderer completed event is missing required fields")
        output = event["output"]
        if not isinstance(output, str) or not output:
            raise RenderGateError("renderer completed event has an invalid output")
        return

    if set(event) != allowed:
        raise RenderGateError("renderer failed event is missing required fields")
    if type(event["exit_code"]) is not int or event["exit_code"] not in (1, 2):
        raise RenderGateError("renderer failed event has an invalid exit code")
    if type(event["retryable"]) is not bool:
        raise RenderGateError("renderer failed event has an invalid retryable value")
    for field in ("error_code", "message"):
        if not isinstance(event[field], str) or not event[field]:
            raise RenderGateError(f"renderer failed event has an invalid {field}")


def load_variants(config_path: Path) -> list[tuple[str, str, bytes]]:
    raw = config_path.read_bytes()
    if len(raw) > MAX_CONFIG_BYTES:
        raise RenderGateError("HD smoke config exceeds the 5 MiB admission limit")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RenderGateError("HD smoke config is not valid JSON") from error
    if not isinstance(document, dict) or document.get("schema_version") != 4:
        raise RenderGateError("HD smoke config must use schema_version 4")
    animation = document.get("animation_state")
    if not isinstance(animation, dict) or animation.get("resolution") != "RESOLUTION_HD":
        raise RenderGateError("HD smoke config must use RESOLUTION_HD")

    variants: list[tuple[str, str, bytes]] = []
    for name, enum_value, event_value in (
        ("hd", "RESOLUTION_HD", "hd"),
        ("4k", "RESOLUTION_FHD", "4k"),
    ):
        variant = copy.deepcopy(document)
        variant["animation_state"]["resolution"] = enum_value
        rendered = json.dumps(
            variant,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(rendered) > MAX_CONFIG_BYTES:
            raise RenderGateError(f"{name} smoke config exceeds the 5 MiB admission limit")
        variants.append((name, event_value, rendered))
    return variants


def validate_event_stream(stdout: bytes, expected_resolution: str, output_path: str) -> None:
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(stdout.splitlines(), start=1):
        try:
            event = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RenderGateError(
                f"renderer stdout line {line_number} is not JSON"
            ) from error
        validate_schema_v2_event(event)
        events.append(event)
    if not events:
        raise RenderGateError("renderer emitted no progress events")

    saw_estimate = False
    saw_progress = False
    terminal: dict[str, Any] | None = None
    last_phase_rank = 0
    counters: dict[str, tuple[int, int | None]] = {}
    for event in events:
        if terminal is not None:
            raise RenderGateError("renderer emitted data after its terminal event")
        event_name = event["event"]
        if event_name == "estimate":
            if saw_estimate:
                raise RenderGateError("renderer emitted more than one estimate")
            if saw_progress:
                raise RenderGateError("renderer estimate followed a progress event")
            if event["estimated_seconds"] > MAX_ESTIMATED_SECONDS:
                raise RenderGateError("renderer estimate exceeds the supported deadline range")
            if event["resolution"] != expected_resolution:
                raise RenderGateError("renderer estimate reported the wrong resolution")
            saw_estimate = True
            continue

        if event_name == "progress":
            saw_progress = True
            phase = event["phase"]
            if not saw_estimate and phase != "assets":
                raise RenderGateError("renderer emitted non-asset progress before its estimate")
            rank = PHASE_RANK[phase]
            if rank < last_phase_rank:
                raise RenderGateError("renderer progress phase regressed")
            if phase == "contact_sheet" and "render" not in counters:
                raise RenderGateError("renderer contact sheet progress preceded rendering")

            completed = event["completed"]
            total = event.get("total")
            previous = counters.get(phase)
            if previous is not None:
                previous_completed, previous_total = previous
                if completed < previous_completed:
                    raise RenderGateError("renderer progress counter regressed")
                if previous_total is not None and total != previous_total:
                    raise RenderGateError("renderer progress total changed")
                if previous_total is not None:
                    total = previous_total
            counters[phase] = (completed, total)
            last_phase_rank = rank
            continue

        if event_name == "completed":
            if not saw_estimate:
                raise RenderGateError("renderer completed without an estimate")
            if event["output"] != output_path:
                raise RenderGateError("renderer completed event named the wrong output")
            terminal = event
            continue

        if not saw_estimate and event["error_code"] not in PRE_ESTIMATE_FAILURES:
            raise RenderGateError("renderer failed without an estimate")
        terminal = event

    if not saw_estimate or not saw_progress:
        raise RenderGateError("renderer did not emit one estimate before progress")
    if terminal is None:
        raise RenderGateError("renderer emitted no terminal event")
    if terminal["event"] != "completed" or terminal["output"] != output_path:
        raise RenderGateError("renderer terminal event did not confirm the requested output")


def validate_iso_bmff(path: Path) -> int:
    size = path.stat().st_size
    if size < 1 or size > MAX_OUTPUT_BYTES:
        raise RenderGateError("rendered MP4 has an invalid size")
    boxes: set[bytes] = set()
    with path.open("rb") as stream:
        offset = 0
        while offset < size:
            stream.seek(offset)
            header = stream.read(8)
            if len(header) != 8:
                raise RenderGateError("rendered output has a truncated ISO-BMFF box")
            box_size, box_type = struct.unpack(">I4s", header)
            header_size = 8
            if box_size == 1:
                extended = stream.read(8)
                if len(extended) != 8:
                    raise RenderGateError("rendered output has a truncated large box")
                box_size = struct.unpack(">Q", extended)[0]
                header_size = 16
            elif box_size == 0:
                box_size = size - offset
            if box_size < header_size or offset + box_size > size:
                raise RenderGateError("rendered output has an invalid ISO-BMFF box size")
            boxes.add(box_type)
            offset += box_size
    if not {b"ftyp", b"moov", b"mdat"}.issubset(boxes):
        raise RenderGateError("rendered output is missing required MP4 boxes")
    return size


def validate_ffprobe(stdout: bytes, expected_dimensions: tuple[int, int]) -> None:
    try:
        document = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RenderGateError("ffprobe returned malformed JSON") from error
    if not isinstance(document, dict):
        raise RenderGateError("ffprobe returned a non-object document")
    format_value = document.get("format")
    format_name = format_value.get("format_name") if isinstance(format_value, dict) else None
    if not isinstance(format_name, str) or "mp4" not in format_name.split(","):
        raise RenderGateError("ffprobe did not identify an MP4 container")
    streams = document.get("streams")
    if not isinstance(streams, list):
        raise RenderGateError("ffprobe found no video stream")
    video_streams = [
        stream
        for stream in streams
        if isinstance(stream, dict) and stream.get("codec_type") == "video"
    ]
    if not video_streams:
        raise RenderGateError("ffprobe found no video stream")
    expected_width, expected_height = expected_dimensions
    if not any(
        stream.get("width") == expected_width and stream.get("height") == expected_height
        for stream in video_streams
    ):
        raise RenderGateError(
            f"ffprobe did not find a {expected_width}x{expected_height} video stream"
        )


def expected_worker_labels(lock_path: Path, source_sha: str) -> dict[str, str]:
    if SHA.fullmatch(source_sha) is None:
        raise RenderGateError("source SHA must be 40 lowercase hexadecimal characters")
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        tada = lock["tada"]
        tada_oci = tada["oci"]
        tada_revision = tada["revision"]
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise RenderGateError("release lock does not contain the TADA label contract") from error
    if (
        not isinstance(tada_oci, str)
        or TADA_OCI.fullmatch(tada_oci) is None
        or not isinstance(tada_revision, str)
        or SHA.fullmatch(tada_revision) is None
    ):
        raise RenderGateError("release lock has an invalid TADA label contract")
    return {
        "org.opencontainers.image.revision": source_sha,
        "com.lascade.tada.bundle": tada_oci,
        "com.lascade.tada.revision": tada_revision,
    }


def validate_worker_labels(stdout: bytes, expected: dict[str, str]) -> None:
    try:
        labels = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RenderGateError("docker returned malformed worker image labels") from error
    if not isinstance(labels, dict):
        raise RenderGateError("worker image has no OCI labels")
    for name, value in expected.items():
        if labels.get(name) != value:
            raise RenderGateError(f"worker image label does not match the release lock: {name}")


def run(
    command: list[str],
    *,
    timeout_seconds: int,
    stdin: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            command,
            input=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise RenderGateError(
            f"command exceeded its {timeout_seconds}-second timeout"
        ) from error


def force_remove_container(name: str) -> None:
    subprocess.run(
        ["docker", "rm", "--force", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=30,
    )


def make_workspace_owned_by_runner(
    image: str,
    root: Path,
    target: str = "/gate",
) -> None:
    result = run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0:0",
            "--volume",
            f"{root}:/gate:rw",
            "--entrypoint",
            "/bin/chown",
            image,
            "-R",
            f"{os.getuid()}:{os.getgid()}",
            target,
        ],
        timeout_seconds=60,
    )
    if result.returncode != 0:
        raise RenderGateError("could not recover ownership of render-gate files")


def render_command(image: str, root: Path, name: str, container: str) -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        "--interactive",
        "--name",
        container,
        *RENDER_CONTAINER_LIMITS,
        "--env",
        f"TADA_ALLOWED_HOSTS={','.join(ALLOWED_HOSTS)}",
        "--volume",
        f"{root}:/gate:rw",
        "--entrypoint",
        "/opt/tada/bin/tada",
        image,
        "render",
        "-",
        "-o",
        f"/gate/{name}/attempt/output.mp4",
        "--cache-dir",
        "/gate/cache",
        "--attempt-dir",
        f"/gate/{name}/attempt",
        "--progress-json",
    ]


def prepare_bind_directory(path: Path, *, parents: bool = False) -> None:
    """Create a directory writable by the image's non-root uid.

    ``Path.mkdir(mode=0o777)`` still applies the host process umask. GitHub
    runners normally use 0o022, which would leave these runner-owned bind-mount
    directories at 0o755 and make them unwritable by the worker's uid 10001.
    """

    path.mkdir(mode=0o777, parents=parents, exist_ok=True)
    path.chmod(0o777)


def render_variant(
    image: str,
    root: Path,
    name: str,
    expected_resolution: str,
    config: bytes,
    timeout_seconds: int,
) -> int:
    attempt = root / name / "attempt"
    prepare_bind_directory(attempt, parents=True)
    cache = root / "cache"
    prepare_bind_directory(cache)
    output_in_container = f"/gate/{name}/attempt/output.mp4"
    container = f"tars-render-gate-{os.getpid()}-{uuid.uuid4().hex[:12]}"
    command = render_command(image, root, name, container)
    try:
        rendered = run(command, timeout_seconds=timeout_seconds, stdin=config)
    finally:
        force_remove_container(container)
    if rendered.returncode != 0:
        details = rendered.stderr.decode("utf-8", errors="replace")
        raise RenderGateError(f"{name} renderer exited with status {rendered.returncode}. Stderr:\n{details}")

    # Do not change ownership of the persistent cache between variants: the
    # worker's uid 10001 must be able to reuse it for the 4K pass.
    make_workspace_owned_by_runner(image, root, f"/gate/{name}")
    validate_event_stream(rendered.stdout, expected_resolution, output_in_container)
    output = attempt / "output.mp4"
    size = validate_iso_bmff(output)

    probe = run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0:0",
            "--volume",
            f"{root}:/gate:ro",
            "--entrypoint",
            "/usr/bin/ffprobe",
            image,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "format=format_name:stream=codec_type,width,height",
            "-of",
            "json",
            output_in_container,
        ],
        timeout_seconds=60,
    )
    if probe.returncode != 0:
        details = probe.stderr.decode("utf-8", errors="replace")
        raise RenderGateError(f"ffprobe rejected the {name} output: {details}")
    validate_ffprobe(probe.stdout, OUTPUT_DIMENSIONS[name])
    return size


def execute(
    image: str,
    config_path: Path,
    lock_path: Path,
    source_sha: str,
    timeout_seconds: int,
) -> None:
    if IMAGE.fullmatch(image) is None:
        raise RenderGateError("worker image reference contains unsupported characters")
    if timeout_seconds < 1 or timeout_seconds > 1800:
        raise RenderGateError("per-render timeout must be between 1 and 1800 seconds")
    inspect = run(
        [
            "docker",
            "image",
            "inspect",
            "--format",
            "{{json .Config.Labels}}",
            image,
        ],
        timeout_seconds=30,
    )
    if inspect.returncode != 0:
        raise RenderGateError("the loaded worker image is unavailable")
    validate_worker_labels(
        inspect.stdout,
        expected_worker_labels(lock_path, source_sha),
    )

    variants = load_variants(config_path)
    workspace = Path(tempfile.mkdtemp(prefix="tars-worker-render-gate-"))
    workspace.chmod(0o777)
    try:
        for name, expected_resolution, config in variants:
            size = render_variant(
                image,
                workspace,
                name,
                expected_resolution,
                config,
                timeout_seconds,
            )
            print(f"{name} worker render passed ({size} bytes)")
    finally:
        try:
            make_workspace_owned_by_runner(image, workspace)
        finally:
            shutil.rmtree(workspace)
    if workspace.exists():
        raise RenderGateError("render-gate workspace cleanup failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    try:
        execute(
            args.image,
            args.config,
            args.lock,
            args.source_sha,
            args.timeout_seconds,
        )
    except (OSError, RenderGateError) as error:
        parser.exit(1, f"TARS worker render gate failed: {error}\n")


if __name__ == "__main__":
    main()
