#!/usr/bin/env python3
"""Pre-clone SPM packages from Package.resolved to avoid xcodebuild hangs on CI."""

import json, subprocess, sys, concurrent.futures
from pathlib import Path


def find_package_resolved():
    for p in Path(".").rglob("Package.resolved"):
        if "SourcePackages" not in str(p):
            return p
    return None


def clone_package(pin, checkout_dir):
    identity = pin["identity"]
    location = pin["location"]
    state = pin["state"]
    revision = state.get("revision", "")
    branch = state.get("branch", "")

    checkout_path = checkout_dir / identity
    if checkout_path.exists():
        return f"[skip] {identity} — already exists"

    try:
        # Clone with depth 1 for speed
        subprocess.run(
            ["git", "clone", "--depth", "1", location, str(checkout_path)],
            check=True, capture_output=True, text=True, timeout=300,
        )

        if revision:
            # Fetch the exact pinned revision
            subprocess.run(
                ["git", "-C", str(checkout_path), "fetch", "origin", revision, "--depth", "1"],
                capture_output=True, text=True, timeout=300,
            )
            subprocess.run(
                ["git", "-C", str(checkout_path), "checkout", revision],
                check=True, capture_output=True, text=True, timeout=60,
            )
        elif branch:
            subprocess.run(
                ["git", "-C", str(checkout_path), "checkout", branch],
                capture_output=True, text=True, timeout=60,
            )

        short = revision[:8] if revision else branch
        return f"[ok]   {identity} @ {short}"

    except subprocess.TimeoutExpired:
        return f"[timeout] {identity} — clone timed out after 5 min"
    except subprocess.CalledProcessError as e:
        return f"[err]  {identity} — {e.stderr.strip() if e.stderr else e}"
    except Exception as e:
        return f"[err]  {identity} — {e}"


def main():
    resolved = find_package_resolved()
    if not resolved:
        print("Package.resolved not found, skipping pre-clone")
        sys.exit(0)

    print(f"Reading {resolved}")
    data = json.loads(resolved.read_text())
    pins = [p for p in data.get("pins", []) if p.get("kind") == "remoteSourceControl"]
    print(f"Found {len(pins)} packages to pre-clone")

    checkout_dir = Path("SourcePackages/checkouts")
    checkout_dir.mkdir(parents=True, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(clone_package, pin, checkout_dir): pin for pin in pins}
        for f in concurrent.futures.as_completed(futures):
            print(f.result())

    print("Pre-clone complete")


if __name__ == "__main__":
    main()
