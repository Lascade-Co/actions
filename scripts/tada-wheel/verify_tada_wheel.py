"""Assert both TADA wheels carry their expected modules and no credential-like files.

TADA ships two distributions from one revision: private `tada` (choreography builder and
credentialed fetchers) and public `travel-animator` (render engine and safe CLI). The
generated protobuf modules and bundled assets live in the public one. The public wheel is
selected by its DISTRIBUTION name (`travel_animator-*.whl`, which escapes the dash) but its
contents live under its IMPORT path (`tada_render/`).

The credential scan runs over BOTH wheels. It matters most for travel-animator, the one
that is publishable to a public index.
"""
from pathlib import PurePosixPath
from zipfile import ZipFile
import glob

# `tada-*.whl` and `travel_animator-*.whl` are disjoint: a wheel filename starts with its
# distribution name, and these two names share no prefix. Each pattern therefore selects
# exactly one wheel.
REQUIRED_BY_WHEEL = {
    "tada-*.whl": {
        # The private package has no generated modules of its own; pin a few load-bearing
        # modules so a catastrophically empty or mis-packaged wheel still fails here.
        "tada/__init__.py",
        "tada/cli.py",
        "tada/bundle.py",
    },
    "travel_animator-*.whl": {
        "tada_render/config/proto/export_config_pb2.py",
        "tada_render/config/proto/animation_state_pb2.py",
        "tada_render/config/proto/animation_style_pb2.py",
        "tada_render/config/proto/route_pb2.py",
        "tada_render/config/proto/mcp_options_pb2.py",
        "tada_render/assets/mvt_pb2.py",
        "tada_render/animation/frame_plan_pb2.py",
        "tada_render/assets/bundled/countries.geoscade",
        "tada_render/assets/bundled/fonts/interbold.ttf",
        "tada_render/assets/bundled/flags/1x1/us.svg",
        "tada_render/assets/bundled/flags/4x3/us.svg",
        "tada_render/assets/bundled/watermark/watermark-0.png",
    },
}

CREDENTIAL_NAMES = {".env", ".npmrc", ".pypirc", "credentials"}
CREDENTIAL_SUFFIXES = (".jks", ".key", ".p12", ".pem")

all_wheels = sorted(glob.glob("bundle/*.whl"))
if len(all_wheels) != 2:
    raise SystemExit(f"expected exactly two wheels, found {all_wheels}")

for pattern, required in REQUIRED_BY_WHEEL.items():
    matches = sorted(glob.glob(f"bundle/{pattern}"))
    if len(matches) != 1:
        raise SystemExit(f"expected exactly one {pattern}, found {matches}")
    wheel = matches[0]

    with ZipFile(wheel) as archive:
        names = set(archive.namelist())

    missing = sorted(required - names)
    if missing:
        raise SystemExit(f"{wheel} is missing expected modules: {missing}")

    forbidden = []
    for name in names:
        path = PurePosixPath(name)
        lowered = name.lower()
        if (
            any(part.lower() in CREDENTIAL_NAMES for part in path.parts)
            or lowered.endswith(CREDENTIAL_SUFFIXES)
            or ".git" in {part.lower() for part in path.parts}
        ):
            forbidden.append(name)
    if forbidden:
        raise SystemExit(f"{wheel} contains credential-like files: {sorted(forbidden)}")
