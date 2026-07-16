# Central TADA Wheel Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `Lascade-Co/tada`'s wheel build+publish out of its own repo into a central runner in `Lascade-Co/actions`, triggered by `repository_dispatch`, eliminating the `SHARED_REPO_TOKEN` PAT.

**Architecture:** A thin trigger workflow in `tada` fires `repository_dispatch` (`publish-tada-wheel`) to `actions`, carrying `{repo, branch, sha}`. A central runner in `actions` mints a CI GitHub App token scoped to `tada` + `travel-animator-shared`, checks both out, runs the validate/build/verify pipeline (all >10-line logic extracted to `scripts/` and fetched via raw URL, per repo convention), then publishes the bundle to `ghcr.io/lascade-co/tada-wheel:latest` and prunes prior versions — all with the app token. Publish-only on `main` (no PR validation).

**Tech Stack:** GitHub Actions, `actions/create-github-app-token@v3`, `peter-evans/repository-dispatch@v4`, `uv`, ORAS, GHCR, `gh` CLI.

## Global Constraints

- **Action versions — the exact refs below** (major tags where the ref exists and is proven by the sibling runners in this repo; `setup-uv` has NO bare major tag so it is pinned to a full version): `actions/checkout@v6`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `actions/create-github-app-token@v3`, `astral-sh/setup-uv@v8.3.2`, `oras-project/setup-oras@v2`, `peter-evans/repository-dispatch@v4`.
- `uv` binary version `0.8.9`, Python `3.12` (pinned via `setup-uv` inputs for reproducibility with `uv.lock`); `EGL_PLATFORM: surfaceless`, `LIBGL_ALWAYS_SOFTWARE: "1"` on the build job.
- **Custom logic >10 lines lives in `scripts/` and is fetched via `https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/<name>`** (repo convention; the `actions` repo is public, as the sibling workflows already rely on). Only short linear guards (≤~11 lines, no algorithm) stay inline: owner/repo split, stale-guard, bundle re-verify/count guard, GHCR login/logout.
- **The published artifact's identity fields come from the dispatch payload**, never ambient `GITHUB_*`: build-metadata `repository`←`payload.repo`, `revision`←`payload.sha` (via `TADA_REPOSITORY`/`TADA_REVISION` env into `build_tada_wheel.sh`); ORAS `image.source`←`https://github.com/{payload.repo}`, `image.revision`←`payload.sha` (via `TARGET_REPO`/`TARGET_SHA` env into `push_ghcr_bundle.sh`); stale-guard queries `repos/{payload.repo}/…` vs `payload.sha`. Only `workflow_run_id`/`workflow_run_attempt` stay ambient (the build runs in `actions`).
- GHCR package is `ghcr.io/lascade-co/tada-wheel` (lowercase, hardcoded); it stays **linked to the `tada` repo**.
- No `SHARED_REPO_TOKEN` anywhere. No `github.token`/`GITHUB_TOKEN` for GHCR/stale-guard/prune — the CI App token does all three. GHCR login uses username `x-access-token`.
- Event type string is exactly `publish-tada-wheel` in both the trigger and the central runner.

---

## Prerequisites (manual — outside the file edits; required before the first real run)

1. **CI App `Packages: read & write`** — the CI GitHub App (`CI_APP_CLIENT_ID`/`CI_APP_PRIVATE_KEY`) must have the *Packages* permission (read **and** write), or GHCR login/push and the prune fail.
2. **`tada-wheel` stays linked to `tada`** — already true. The repo-scoped app token derives package rights from this link.
3. **`CENTRAL_DISPATCH_TOKEN` secret in `tada`** — add a token that can `POST` a `repository_dispatch` to `Lascade-Co/actions`.

## Merge ordering (critical)

The central runner runs from `actions`'s **default branch** and fetches its scripts from `.../main/scripts/...`. So **Tasks 1–3 (the `actions` changes) must be merged to `actions` `main` BEFORE `tada`'s trigger goes live (Task 4).** If TADA's trigger merges first, a push to TADA `main` dispatches to a handler that doesn't exist yet (silent no-op), and even once it exists it would fetch scripts not yet on `main`.

## File Structure (all in the `actions` repo unless noted)

- **Create** `scripts/build_tada_wheel.sh` — builds the bundle: wheel + pylock + build-metadata.json + SHA256SUMS. Env-driven.
- **Create** `scripts/verify_wheel_install.sh` — twine-checks the wheel, resolves pylock into a fresh venv, installs the wheel, runs the `tada` console script.
- **Create** `scripts/verify_tada_wheel.py` — asserts required generated modules present and no credential-like files, in the built wheel.
- **Create** `scripts/push_ghcr_bundle.sh` — pushes the bundle to GHCR `:latest`; prints the resulting digest to stdout (Actions-agnostic).
- **Create** `scripts/prune_ghcr_versions.sh` — deletes every non-current version of a GHCR container package.
- **Create** `.github/workflows/publish-tada-wheel.yml` — the central runner: `validate` → `publish`.
- **Create** `triggers/publish-wheel.yml` — the copy-into-`tada` trigger template.
- **Modify (in place, no commit)** `/Users/rohittp/Data/Lascade/TADA/.github/workflows/publish-wheel.yml` — replace WIP 372-line workflow with the trigger.
- **Already created** `docs/adr/0002-central-tada-wheel-publish.md` — commit with Task 2.

## Verification tooling

- **actionlint** (bundles shellcheck for inline `run:`): `docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest -color <file>`
- **shellcheck** for `.sh`: `docker run --rm -v "$PWD":/repo -w /repo koalaman/shellcheck:stable <file...>`
- **python syntax** (no bytecode side effects): `python3 -c "import ast,sys; [ast.parse(open(p).read(), p) for p in sys.argv[1:]]; print('py OK')" <file>`

Docker-free: `brew install actionlint shellcheck`.

---

### Task 1: Extract build/verify/push/prune scripts (`actions` repo)

**Files:**
- Create: `scripts/build_tada_wheel.sh`, `scripts/verify_wheel_install.sh`, `scripts/verify_tada_wheel.py`, `scripts/push_ghcr_bundle.sh`, `scripts/prune_ghcr_versions.sh` (all under `/Users/rohittp/Data/Lascade/actions/`)

**Interfaces (env/cwd contracts consumed by Task 2):**
- `build_tada_wheel.sh`: env `SHARED_REVISION`, `TADA_REPOSITORY`, `TADA_REVISION` (+ ambient `GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT`); cwd = TADA checkout; writes `bundle/` (4 files).
- `verify_wheel_install.sh`: cwd = TADA checkout; needs `uv` on PATH and ambient `GITHUB_WORKSPACE`.
- `verify_tada_wheel.py`: no args; cwd = TADA checkout (globs `bundle/*.whl`).
- `push_ghcr_bundle.sh`: env `TARGET_SHA`, `TARGET_REPO`; cwd = dir containing `bundle/`; ORAS already logged in; prints digest to stdout, logs to stderr.
- `prune_ghcr_versions.sh`: env `OWNER`, `PACKAGE`, `CURRENT_DIGEST`, `GH_TOKEN`; no checkout needed.

- [ ] **Step 1: Create the branch**

```bash
cd /Users/rohittp/Data/Lascade/actions
git checkout -b feat/central-tada-wheel
```

- [ ] **Step 2: Write `scripts/build_tada_wheel.sh`**

```bash
#!/usr/bin/env bash
# Build the TADA wheel bundle (wheel + runtime pylock + build-metadata + SHA256SUMS).
# Run from the TADA checkout root. Env: SHARED_REVISION, TADA_REPOSITORY, TADA_REVISION.
# Identity (repository/revision) comes from TADA_* env, NOT ambient GITHUB_*, because this
# runs in the central actions repo. workflow_run_id/attempt stay ambient (the build runs here).
set -euo pipefail

: "${SHARED_REVISION:?}" "${TADA_REPOSITORY:?}" "${TADA_REVISION:?}"

rm -rf bundle
mkdir bundle
uv build --wheel --out-dir bundle
uv export \
  --locked \
  --no-dev \
  --no-emit-project \
  --format pylock.toml \
  --output-file bundle/pylock.toml

shopt -s nullglob
wheels=(bundle/*.whl)
if (( ${#wheels[@]} != 1 )); then
  echo "expected exactly one wheel, found ${#wheels[@]}" >&2
  exit 1
fi

WHEEL_NAME="$(basename "${wheels[0]}")" uv run --locked python - <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import platform
import subprocess

metadata = {
    "schema_version": 1,
    "repository": os.environ["TADA_REPOSITORY"],
    "revision": os.environ["TADA_REVISION"],
    "shared_revision": os.environ["SHARED_REVISION"],
    "workflow_run_id": os.environ["GITHUB_RUN_ID"],
    "workflow_run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
    "built_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "python_version": platform.python_version(),
    "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
    "wheel": os.environ["WHEEL_NAME"],
}
Path("bundle/build-metadata.json").write_text(
    json.dumps(metadata, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

(
  cd bundle
  sha256sum "${wheels[0]##*/}" pylock.toml build-metadata.json > SHA256SUMS
  sha256sum --check SHA256SUMS
)
```

- [ ] **Step 3: Write `scripts/verify_wheel_install.sh`**

```bash
#!/usr/bin/env bash
# Verify the built wheel: metadata via twine, pylock resolves into a fresh venv, wheel installs,
# and the `tada` console script runs from an unrelated cwd. Run from the TADA checkout root.
# Requires uv on PATH and ambient GITHUB_WORKSPACE.
set -euo pipefail

uvx --from twine==6.2.0 twine check bundle/*.whl
rm -rf .wheel-check
uv venv --python 3.12 .wheel-check
uv pip sync --python .wheel-check/bin/python bundle/pylock.toml
uv pip install --python .wheel-check/bin/python --no-deps bundle/*.whl
(
  cd /tmp
  "$GITHUB_WORKSPACE/.wheel-check/bin/tada" --help
)
```

- [ ] **Step 4: Write `scripts/verify_tada_wheel.py`**

```python
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
```

- [ ] **Step 5: Write `scripts/push_ghcr_bundle.sh`**

```bash
#!/usr/bin/env bash
# Push the release bundle to GHCR as :latest and print the resulting digest to stdout.
# Run from the directory that CONTAINS bundle/. Requires oras already logged in.
# Env: TARGET_SHA, TARGET_REPO (the tada commit + repo, from the dispatch payload).
# Human-readable logs go to stderr; stdout is ONLY the digest (so the caller can capture it).
set -euo pipefail

: "${TARGET_SHA:?}" "${TARGET_REPO:?}"

shopt -s nullglob
wheels=(bundle/*.whl)
if (( ${#wheels[@]} != 1 )) \
  || [[ ! -f bundle/pylock.toml ]] \
  || [[ ! -f bundle/SHA256SUMS ]] \
  || [[ ! -f bundle/build-metadata.json ]]; then
  echo "release bundle is incomplete" >&2
  exit 1
fi

package="ghcr.io/lascade-co/tada-wheel"
wheel="$(basename "${wheels[0]}")"
created="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
(
  cd bundle
  oras push "$package:latest" \
    --artifact-type application/vnd.lascade.tada-wheel.bundle.v1 \
    --annotation "org.opencontainers.image.created=$created" \
    --annotation "org.opencontainers.image.revision=$TARGET_SHA" \
    --annotation "org.opencontainers.image.source=https://github.com/$TARGET_REPO" \
    "$wheel:application/vnd.pypa.wheel" \
    "pylock.toml:application/vnd.python.pylock.toml" \
    "SHA256SUMS:text/plain" \
    "build-metadata.json:application/json" >&2
)

digest="$(oras resolve "$package:latest")"
echo "Published $package:latest@$digest" >&2
printf '%s\n' "$digest"
```

- [ ] **Step 6: Write `scripts/prune_ghcr_versions.sh`**

```bash
#!/usr/bin/env bash
# Delete every version of a GHCR container package except the freshly-pushed current one.
# Env: OWNER (org login), PACKAGE (container name), CURRENT_DIGEST, GH_TOKEN.
set -euo pipefail

: "${OWNER:?}" "${PACKAGE:?}" "${CURRENT_DIGEST:?}" "${GH_TOKEN:?}"

versions_api="/orgs/$OWNER/packages/container/$PACKAGE/versions"

versions_file="$(mktemp)"
current_is_tagged=false
for _ in {1..10}; do
  gh api --paginate "$versions_api?per_page=100" \
    --jq '.[] | [.id, .name, ((.metadata.container.tags // []) | join(","))] | @tsv' \
    > "$versions_file"

  while IFS=$'\t' read -r _id digest tags; do
    if [[ "$digest" == "$CURRENT_DIGEST" && ",$tags," == *",latest,"* ]]; then
      current_is_tagged=true
      break
    fi
  done < "$versions_file"

  if [[ "$current_is_tagged" == true ]]; then
    break
  fi
  sleep 3
done

if [[ "$current_is_tagged" != true ]]; then
  echo "current digest is not yet visible with the latest tag; refusing cleanup" >&2
  exit 1
fi

while IFS= read -r id; do
  [[ -z "$id" ]] && continue
  echo "Deleting previous package version $id"
  gh api --method DELETE "$versions_api/$id"
done < <(
  awk -F '\t' -v current="$CURRENT_DIGEST" \
    '$2 != current { print $1 }' \
    "$versions_file"
)

only_current_remains=false
for _ in {1..10}; do
  gh api --paginate "$versions_api?per_page=100" \
    --jq '.[] | [.id, .name, ((.metadata.container.tags // []) | join(","))] | @tsv' \
    > "$versions_file"

  version_count="$(wc -l < "$versions_file" | tr -d ' ')"
  if [[ "$version_count" == "1" ]]; then
    IFS=$'\t' read -r _id digest tags < "$versions_file"
    if [[ "$digest" == "$CURRENT_DIGEST" && "$tags" == "latest" ]]; then
      only_current_remains=true
      break
    fi
  fi
  sleep 3
done

if [[ "$only_current_remains" != true ]]; then
  echo "package cleanup did not leave exactly the current latest version:" >&2
  cat "$versions_file" >&2
  exit 1
fi
```

- [ ] **Step 7: Lint the scripts (fail-fast; expect PASS)**

```bash
set -euo pipefail
cd /Users/rohittp/Data/Lascade/actions
docker run --rm -v "$PWD":/repo -w /repo koalaman/shellcheck:stable \
  scripts/build_tada_wheel.sh scripts/verify_wheel_install.sh \
  scripts/push_ghcr_bundle.sh scripts/prune_ghcr_versions.sh
python3 -c "import ast,sys; [ast.parse(open(p).read(), p) for p in sys.argv[1:]]; print('py OK')" \
  scripts/verify_tada_wheel.py
```

Expected: shellcheck exits 0 (no errors), `py OK`. If shellcheck errors, fix before committing.

- [ ] **Step 8: Commit**

```bash
cd /Users/rohittp/Data/Lascade/actions
git add scripts/build_tada_wheel.sh scripts/verify_wheel_install.sh scripts/verify_tada_wheel.py scripts/push_ghcr_bundle.sh scripts/prune_ghcr_versions.sh
git commit -m "feat: add TADA wheel build/verify/push/prune scripts"
```

---

### Task 2: Central runner workflow (`actions` repo)

**Files:**
- Create: `/Users/rohittp/Data/Lascade/actions/.github/workflows/publish-tada-wheel.yml`
- Commit alongside: `/Users/rohittp/Data/Lascade/actions/docs/adr/0002-central-tada-wheel-publish.md` (already on disk)

**Interfaces:**
- Consumes: dispatch `client_payload` `{ repo, branch, sha }` (Task 3/4) and the five Task 1 scripts (via raw URL).
- Produces: `ghcr.io/lascade-co/tada-wheel:latest` (terminal deliverable).

- [ ] **Step 1: Write the central runner file**

Create `/Users/rohittp/Data/Lascade/actions/.github/workflows/publish-tada-wheel.yml` with exactly:

```yaml
name: Central Publish TADA Wheel

on:
  repository_dispatch:
    types: [publish-tada-wheel]

# A newer dispatch cancels older runs of this group. The publish job additionally
# takes a dedicated non-cancel group to serialize concurrent GHCR mutations, and
# re-checks that the built commit is still main's head before replacing latest.
# (A superseding dispatch can still cancel an in-flight run; the stale-guard is what
# guarantees a superseded commit is never what ends up published.)
concurrency:
  group: publish-tada-wheel-${{ github.event.client_payload.repo }}
  cancel-in-progress: true

permissions:
  contents: read

jobs:
  validate:
    name: Test and build wheel
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      EGL_PLATFORM: surfaceless
      LIBGL_ALWAYS_SOFTWARE: "1"
    steps:
      - name: Extract owner/repo
        id: repo
        shell: bash
        run: |
          FULL="${{ github.event.client_payload.repo }}"
          echo "owner=${FULL%%/*}" >> "$GITHUB_OUTPUT"
          echo "name=${FULL#*/}"   >> "$GITHUB_OUTPUT"

      - name: Mint GitHub App token (tada + shared)
        id: app-token
        uses: actions/create-github-app-token@v3
        with:
          client-id: ${{ secrets.CI_APP_CLIENT_ID }}
          private-key: ${{ secrets.CI_APP_PRIVATE_KEY }}
          owner: ${{ steps.repo.outputs.owner }}
          repositories: |
            ${{ steps.repo.outputs.name }}
            travel-animator-shared

      - name: Check out TADA @ dispatched commit
        uses: actions/checkout@v6
        with:
          repository: ${{ github.event.client_payload.repo }}
          ref: ${{ github.event.client_payload.sha }}
          token: ${{ steps.app-token.outputs.token }}
          fetch-depth: 1
          persist-credentials: false

      - name: Read pinned shared revision
        id: shared
        run: echo "revision=$(git rev-parse HEAD:shared)" >> "$GITHUB_OUTPUT"

      - name: Check out private shared protos
        uses: actions/checkout@v6
        with:
          repository: Lascade-Co/travel-animator-shared
          ref: ${{ steps.shared.outputs.revision }}
          path: shared
          token: ${{ steps.app-token.outputs.token }}
          persist-credentials: false

      - name: Install headless OpenGL runtime
        run: |
          sudo apt-get update
          sudo apt-get install --yes --no-install-recommends \
            libegl1 \
            libgl1 \
            libgl1-mesa-dri

      - name: Install pinned uv and Python
        uses: astral-sh/setup-uv@v8.3.2
        with:
          version: "0.8.9"
          python-version: "3.12"
          enable-cache: true
          cache-dependency-glob: uv.lock

      - name: Sync locked environment
        run: uv sync --locked

      - name: Generate protobuf modules
        run: uv run --locked python hatch_build.py

      - name: Run tests from the lockfile
        run: uv run --locked pytest

      - name: Build wheel and runtime pylock
        env:
          SHARED_REVISION: ${{ steps.shared.outputs.revision }}
          TADA_REPOSITORY: ${{ github.event.client_payload.repo }}
          TADA_REVISION: ${{ github.event.client_payload.sha }}
        run: |
          curl -sSfL https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/build_tada_wheel.sh -o "$RUNNER_TEMP/build_tada_wheel.sh"
          bash "$RUNNER_TEMP/build_tada_wheel.sh"

      - name: Verify wheel metadata and pylock resolution
        run: |
          curl -sSfL https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/verify_wheel_install.sh -o "$RUNNER_TEMP/verify_wheel_install.sh"
          bash "$RUNNER_TEMP/verify_wheel_install.sh"

      - name: Verify wheel contents contain no credential files
        run: |
          curl -sSfL https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/verify_tada_wheel.py -o "$RUNNER_TEMP/verify_tada_wheel.py"
          uv run --locked python "$RUNNER_TEMP/verify_tada_wheel.py"

      - name: Verify release bundle
        run: |
          shopt -s nullglob
          files=(bundle/*)
          if (( ${#files[@]} != 4 )); then
            printf 'expected four bundle files, found:\n' >&2
            printf '  %s\n' "${files[@]}" >&2
            exit 1
          fi
          (
            cd bundle
            sha256sum --check SHA256SUMS
          )

      - name: Upload publish bundle
        uses: actions/upload-artifact@v7
        with:
          name: tada-wheel-bundle
          path: bundle/
          if-no-files-found: error
          include-hidden-files: false
          retention-days: 1
          compression-level: 0

  publish:
    name: Publish latest wheel bundle
    needs: validate
    runs-on: ubuntu-latest
    timeout-minutes: 15
    concurrency:
      group: publish-tada-wheel-latest
      cancel-in-progress: false
    permissions:
      contents: read
    steps:
      - name: Extract owner/repo
        id: repo
        shell: bash
        run: |
          FULL="${{ github.event.client_payload.repo }}"
          echo "owner=${FULL%%/*}" >> "$GITHUB_OUTPUT"
          echo "name=${FULL#*/}"   >> "$GITHUB_OUTPUT"

      - name: Mint GitHub App token (tada, packages)
        id: app-token
        uses: actions/create-github-app-token@v3
        with:
          client-id: ${{ secrets.CI_APP_CLIENT_ID }}
          private-key: ${{ secrets.CI_APP_PRIVATE_KEY }}
          owner: ${{ steps.repo.outputs.owner }}
          repositories: ${{ steps.repo.outputs.name }}

      - name: Download verified bundle
        uses: actions/download-artifact@v8
        with:
          name: tada-wheel-bundle
          path: bundle

      - name: Refuse a stale main publication
        env:
          GH_TOKEN: ${{ steps.app-token.outputs.token }}
          DISPATCHED_SHA: ${{ github.event.client_payload.sha }}
          TARGET_REPO: ${{ github.event.client_payload.repo }}
        run: |
          main_sha="$(gh api "repos/$TARGET_REPO/git/ref/heads/main" --jq .object.sha)"
          if [[ "$DISPATCHED_SHA" != "$main_sha" ]]; then
            echo "refusing to replace latest: dispatched=$DISPATCHED_SHA main=$main_sha" >&2
            exit 1
          fi

      - name: Re-verify downloaded bundle
        run: |
          files=(bundle/*)
          if (( ${#files[@]} != 4 )); then
            echo "downloaded release bundle must contain exactly four files" >&2
            exit 1
          fi
          (
            cd bundle
            sha256sum --check SHA256SUMS
          )

      - name: Install ORAS
        uses: oras-project/setup-oras@v2
        with:
          version: 1.3.2

      - name: Log in to GHCR
        env:
          GHCR_TOKEN: ${{ steps.app-token.outputs.token }}
        run: |
          printf '%s' "$GHCR_TOKEN" | oras login ghcr.io \
            --username x-access-token \
            --password-stdin

      - name: Push release bundle as latest
        id: push
        env:
          TARGET_SHA: ${{ github.event.client_payload.sha }}
          TARGET_REPO: ${{ github.event.client_payload.repo }}
        run: |
          curl -sSfL https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/push_ghcr_bundle.sh -o "$RUNNER_TEMP/push_ghcr_bundle.sh"
          digest="$(bash "$RUNNER_TEMP/push_ghcr_bundle.sh")"
          echo "digest=$digest" >> "$GITHUB_OUTPUT"

      - name: Delete every previous package version
        env:
          GH_TOKEN: ${{ steps.app-token.outputs.token }}
          CURRENT_DIGEST: ${{ steps.push.outputs.digest }}
          OWNER: ${{ steps.repo.outputs.owner }}
          PACKAGE: tada-wheel
        run: |
          curl -sSfL https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/prune_ghcr_versions.sh -o "$RUNNER_TEMP/prune_ghcr_versions.sh"
          bash "$RUNNER_TEMP/prune_ghcr_versions.sh"

      - name: Log out of GHCR
        if: always()
        run: oras logout ghcr.io || true
```

- [ ] **Step 2: Lint the central runner (expect PASS)**

```bash
set -euo pipefail
cd /Users/rohittp/Data/Lascade/actions
docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest -color .github/workflows/publish-tada-wheel.yml
```

Expected: exit 0. SC2086 quoting notes on `${{ }}` match repo style and are acceptable; any *syntax*/*expression* error must be fixed.

- [ ] **Step 3: Structural checks across workflow + scripts (fail-fast)**

```bash
set -euo pipefail
cd /Users/rohittp/Data/Lascade/actions
F=.github/workflows/publish-tada-wheel.yml
fail() { echo "FAIL: $1" >&2; exit 1; }
# workflow: token elimination + payload identity for the pieces that stay in the workflow
grep -q "SHARED_REPO_TOKEN" "$F" && fail "SHARED_REPO_TOKEN present" || true
grep -Eq "github\.token|secrets\.GITHUB_TOKEN" "$F" && fail "GITHUB_TOKEN used" || true
grep -q 'types: \[publish-tada-wheel\]' "$F" || fail "event type"
grep -q 'TADA_REPOSITORY: ${{ github.event.client_payload.repo }}' "$F" || fail "build repo env from payload"
grep -q 'TADA_REVISION: ${{ github.event.client_payload.sha }}' "$F" || fail "build revision env from payload"
grep -q 'repos/$TARGET_REPO/git/ref/heads/main' "$F" || fail "stale-guard from payload"
grep -q 'username x-access-token' "$F" || fail "GHCR username"
# scripts: identity that moved into the extracted scripts
grep -q 'os.environ\["TADA_REPOSITORY"\]' scripts/build_tada_wheel.sh || fail "metadata repo from env"
grep -q 'os.environ\["TADA_REVISION"\]' scripts/build_tada_wheel.sh || fail "metadata revision from env"
grep -q 'image.revision=$TARGET_SHA' scripts/push_ghcr_bundle.sh || fail "ORAS revision from payload"
grep -q 'image.source=https://github.com/$TARGET_REPO' scripts/push_ghcr_bundle.sh || fail "ORAS source from payload"
echo "ALL STRUCTURAL CHECKS PASS"
```

Expected: `ALL STRUCTURAL CHECKS PASS`.

- [ ] **Step 4: Commit (central runner + ADR)**

```bash
cd /Users/rohittp/Data/Lascade/actions
git add .github/workflows/publish-tada-wheel.yml docs/adr/0002-central-tada-wheel-publish.md
git commit -m "feat: central runner for TADA wheel publishing

repository_dispatch runner that mints a CI App token for tada +
travel-animator-shared (replacing SHARED_REPO_TOKEN), builds/verifies
via scripts/, and pushes+prunes ghcr.io/lascade-co/tada-wheel with the
app token. Publish-only on main. See docs/adr/0002."
```

---

### Task 3: Trigger template (`actions` repo)

**Files:**
- Create: `/Users/rohittp/Data/Lascade/actions/triggers/publish-wheel.yml`

**Interfaces:**
- Consumes: `secrets.CENTRAL_DISPATCH_TOKEN` (in the target repo).
- Produces: `client_payload` `{ repo, branch, sha }` for Task 2's runner.

- [ ] **Step 1: Write the trigger template**

Create `/Users/rohittp/Data/Lascade/actions/triggers/publish-wheel.yml` with exactly:

```yaml
# Copy this workflow into Lascade-Co/tada at .github/workflows/publish-wheel.yml.
# Requires a CENTRAL_DISPATCH_TOKEN secret that can POST a repository_dispatch to
# Lascade-Co/actions. The central runner (publish-tada-wheel) builds and publishes.

name: "Publish Wheel Trigger"

on:
  workflow_dispatch:
  push:
    branches: [main]

jobs:
  trigger-publish:
    runs-on: ubuntu-latest
    steps:
      - name: Dispatch to central repo
        uses: peter-evans/repository-dispatch@v4
        with:
          token: ${{ secrets.CENTRAL_DISPATCH_TOKEN }}
          repository: Lascade-Co/actions
          event-type: publish-tada-wheel
          client-payload: >-
            {
              "repo": ${{ toJSON(github.repository) }},
              "branch": ${{ toJSON(github.ref_name) }},
              "sha": ${{ toJSON(github.sha) }}
            }
```

- [ ] **Step 2: Lint + structural check (fail-fast; expect PASS)**

```bash
set -euo pipefail
cd /Users/rohittp/Data/Lascade/actions
docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest -color triggers/publish-wheel.yml
grep -q 'event-type: publish-tada-wheel' triggers/publish-wheel.yml || { echo "FAIL: event type"; exit 1; }
grep -q '"sha": ${{ toJSON(github.sha) }}' triggers/publish-wheel.yml || { echo "FAIL: sha payload"; exit 1; }
echo "TRIGGER OK"
```

Expected: actionlint exit 0, `TRIGGER OK`.

- [ ] **Step 3: Commit**

```bash
cd /Users/rohittp/Data/Lascade/actions
git add triggers/publish-wheel.yml
git commit -m "feat: add publish-wheel trigger template"
```

---

### Task 4: Replace TADA's workflow with the trigger (`tada` repo — edit in place, NO commit)

**Files:**
- Modify (full replace): `/Users/rohittp/Data/Lascade/TADA/.github/workflows/publish-wheel.yml`

**Context:** The `tada` worktree is on `main` with ~20 uncommitted changes, and `.github/` is untracked WIP. Per the user's decision, **do not branch, stage, or commit** in `tada` — rewrite the file in place and leave it for the user to commit with their WIP.

- [ ] **Step 1: Rewrite the workflow in place**

Overwrite `/Users/rohittp/Data/Lascade/TADA/.github/workflows/publish-wheel.yml` with exactly (Task 3's template minus its leading "Copy this workflow…" comment, which lives in its target):

```yaml
name: "Publish Wheel Trigger"

on:
  workflow_dispatch:
  push:
    branches: [main]

jobs:
  trigger-publish:
    runs-on: ubuntu-latest
    steps:
      - name: Dispatch to central repo
        uses: peter-evans/repository-dispatch@v4
        with:
          token: ${{ secrets.CENTRAL_DISPATCH_TOKEN }}
          repository: Lascade-Co/actions
          event-type: publish-tada-wheel
          client-payload: >-
            {
              "repo": ${{ toJSON(github.repository) }},
              "branch": ${{ toJSON(github.ref_name) }},
              "sha": ${{ toJSON(github.sha) }}
            }
```

- [ ] **Step 2: Confirm replacement + lint; DO NOT stage or commit**

```bash
set -euo pipefail
cd /Users/rohittp/Data/Lascade/TADA
if grep -Eq "SHARED_REPO_TOKEN|oras|actions/upload-artifact" .github/workflows/publish-wheel.yml; then
  echo "FAIL: old content remains"; exit 1
fi
echo "OK: fully replaced"
wc -l .github/workflows/publish-wheel.yml   # expect ~22 lines
docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest -color .github/workflows/publish-wheel.yml
git status --short .github/workflows/publish-wheel.yml   # changed but left unstaged
```

Expected: `OK: fully replaced`, ~22 lines, actionlint exit 0. Do NOT run `git add`/`git commit` in `tada`.

- [ ] **Step 3: Tell the user**

Report that TADA's `publish-wheel.yml` is now the trigger, left unstaged for them to commit with their WIP, and restate the three Prerequisites + the merge-ordering rule.

---

## Post-merge end-to-end verification (manual — after Tasks 1–3 merged to `actions` main, prerequisites done, TADA's trigger committed/merged)

1. From `tada` → Actions → "Publish Wheel Trigger" → **Run workflow** on `main` (or push to `main`). Confirm the dispatch step succeeds.
2. In `actions` → Actions → "Central Publish TADA Wheel": confirm `validate` passes (tests + build + all verify steps) and `publish` completes.
3. Confirm the new `ghcr.io/lascade-co/tada-wheel:latest` digest exists and prior versions were pruned to exactly one.
4. Inspect `build-metadata.json` in the pushed bundle: `repository` == `Lascade-Co/tada`, `revision` == the built commit sha (NOT an `actions` sha).
5. **If `publish` fails at GHCR login/push or prune** → CI App missing `Packages: read & write` (Prereq 1) or package not linked to `tada` (Prereq 2). Fix and re-run via `workflow_dispatch`.

## Self-Review notes (completed by plan author)

- **Spec coverage:** scripts extracted per convention incl. ORAS push + install verify (Task 1) ✓; central runner with combined app-token shared checkout replacing `SHARED_REPO_TOKEN` (Task 2) ✓; GHCR via app token, `x-access-token` login (Task 2) ✓; full prune preserved as script (Tasks 1+2) ✓; identity remapping to payload — build env + push env + stale-guard (Global Constraints + Task 2 Step 3 checks) ✓; publish-only on main / no `pull_request` anywhere ✓; `workflow_dispatch` on trigger (Task 3) ✓; combined validate token / separate publish token (Task 2) ✓; verified action refs incl. pinned `setup-uv@v8.3.2` (Global Constraints) ✓; ADR ✓; TADA edit in place, no commit (Task 4) ✓; merge ordering documented ✓.
- **Placeholder scan:** none — all scripts and YAML inlined verbatim; the only "copy from" reference (Task 4) points at Task 3's fully-shown content.
- **Type/name consistency:** `publish-tada-wheel` event type identical in trigger + runner; artifact `tada-wheel-bundle` identical in upload/download; script env contracts (`SHARED_REVISION`/`TADA_REPOSITORY`/`TADA_REVISION`; `TARGET_SHA`/`TARGET_REPO`; `OWNER`/`PACKAGE`/`CURRENT_DIGEST`/`GH_TOKEN`) match between Task 1 scripts and Task 2 step `env:` blocks; `push_ghcr_bundle.sh` prints digest to stdout and Task 2 captures it into `steps.push.outputs.digest`, consumed by the prune step; raw-URL basenames match the Task 1 filenames.
```
