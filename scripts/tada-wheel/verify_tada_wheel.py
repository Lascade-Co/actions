"""Assert the built TADA wheel has its generated modules and no credential-like files."""
from pathlib import PurePosixPath
from zipfile import ZipFile
import glob

wheels = glob.glob("bundle/*.whl")
if len(wheels) != 1:
    raise SystemExit(f"expected exactly one wheel, found {wheels}")

with ZipFile(wheels[0]) as archive:
    names = set(archive.namelist())

required = {
    "tada/config/proto/export_config_pb2.py",
    "tada/config/proto/animation_state_pb2.py",
    "tada/config/proto/animation_style_pb2.py",
    "tada/config/proto/route_pb2.py",
    "tada/config/proto/mcp_options_pb2.py",
    "tada/assets/mvt_pb2.py",
    "tada/assets/bundled/countries.geoscade",
    "tada/assets/bundled/fonts/interbold.ttf",
    "tada/assets/bundled/flags/1x1/us.svg",
    "tada/assets/bundled/flags/4x3/us.svg",
    "tada/assets/bundled/watermark/watermark-0.png",
}
missing = sorted(required - names)
if missing:
    raise SystemExit(f"wheel is missing generated modules: {missing}")

credential_names = {".env", ".npmrc", ".pypirc", "credentials"}
credential_suffixes = (".jks", ".key", ".p12", ".pem")
forbidden = []
for name in names:
    path = PurePosixPath(name)
    lowered = name.lower()
    if (
        any(part.lower() in credential_names for part in path.parts)
        or lowered.endswith(credential_suffixes)
        or ".git" in {part.lower() for part in path.parts}
    ):
        forbidden.append(name)
if forbidden:
    raise SystemExit(f"wheel contains credential-like files: {sorted(forbidden)}")
