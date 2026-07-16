# Scripts folder refactor — design

**Date:** 2026-07-17
**Status:** Approved (design), pending implementation plan
**Scope:** `scripts/` only. `data/` is explicitly out of scope.

## Problem

The `scripts/` folder holds 24 loose files with no grouping, making it hard to
find or reason about which script belongs to which pipeline. Workflows reference
these scripts by flat path, so the mess leaks into every `run:` step.

## Goal

Group the 24 scripts into domain subfolders and update every reference in the
workflows so nothing breaks. This is a relocation plus reference-path update.
**One** script content edit is unavoidable — `test_tars_delivery.py` resolves a
repo-root path relative to its own depth (see constraint 4).

## Target layout (24 files → 7 domain folders)

```
scripts/
  android/     lint_android.py, publish_playstore.py
  ios/         fix_ios_signing.py, install_apple_cert.sh, resolve_spm.py
  catchup/     catchup_collect.py, catchup_discover.py, catchup_render_email.py,
               catchup_repo.py, catchup_report.py
  crashlytics/ crashlytics_blame.py, crashlytics_report.py
  tars/        tars_infisical.py, tars_lock_outputs.py, tars_payload.py,
               tars_tada_bundle.py, tars_worker_render_gate.py,
               test_tars_delivery.py
  tada-wheel/  build_tada_wheel.sh, verify_tada_wheel.py, verify_wheel_install.sh,
               prune_ghcr_versions.sh, push_ghcr_bundle.sh
  accuracy/    ship_accuracy_report.py
```

`resolve_spm.py` is filed under `ios/` (Swift Package Manager is iOS/Swift
tooling). `publish_playstore.py` under `android/` (Play Store = Android
publishing). The five `publish-tada-wheel.yml` scripts (build/verify/push/prune)
form one pipeline → `tada-wheel/`.

## Correctness constraints (must not break)

1. **All six `tars/` files stay co-located.** `test_tars_delivery.py` does
   top-level `from tars_infisical import …`, `from tars_lock_outputs import …`,
   `from tars_payload import …`, `from tars_tada_bundle import …`,
   `from tars_worker_render_gate import …`. Relative imports only resolve if the
   modules remain siblings. The grouping keeps them together.
2. **`crashlytics_blame.py` → `crashlytics_report.py`** is a docstring/help-text
   mention only (no code import) — no runtime coupling, but both land in
   `crashlytics/` anyway.
3. No other cross-references between scripts. All other moves are independent.
4. **`test_tars_delivery.py` computes `ROOT = Path(__file__).resolve().parent.parent`**
   (line 713) to read `.github/workflows/tars-*.yml`. Today the file is one level
   under the repo root (`scripts/`), so `parent.parent` = repo root. Moving it to
   `scripts/tars/` makes it two levels deep, so the ROOT must become
   `.parent.parent.parent`. This is the only required content edit. Its workflow
   assertions match script **basenames** (`assertIn("tars_tada_bundle.py")`,
   `count("tars_infisical.py login") == 2`), so they stay green after the paths
   gain a `tars/` prefix. Verified: `catchup_collect.py` and `lint_android.py`
   resolve paths from arguments/`os.getcwd()`, not `__file__`, and no bash script
   resolves its own dir — so nothing else breaks on relocation.

## Reference updates ("update the usage in the actions")

Three invocation mechanisms, all inside this repo. Every reference below must be
updated in the same change. A per-file text replace of `scripts/<name>` →
`scripts/<folder>/<name>` correctly covers both the bare and `central/`-prefixed
forms.

### 1. Raw-`main` URL fetches (`raw.githubusercontent.com/.../main/scripts/X` and `$RAW/scripts/X`)

| Workflow | Line(s) | Script → folder |
|---|---|---|
| android-build-debug.yml | 199 | lint_android.py → `android/` |
| daily-catchup.yml | 38, 95, 133, 178, 240, 250 | catchup_discover/repo/collect/report/render_email → `catchup/` |
| flutter-build-debug.yml | 326, 357 | install_apple_cert.sh, fix_ios_signing.py → `ios/` |
| flutter-build-release.yml | 333, 364, 483 | install_apple_cert.sh, fix_ios_signing.py → `ios/`; publish_playstore.py → `android/` |
| ios-build-debug.yml | 103, 123 | install_apple_cert.sh, fix_ios_signing.py → `ios/` |
| ios-build-release.yml | 106, 126 | install_apple_cert.sh, fix_ios_signing.py → `ios/` |
| publish-playstore.yml | 140 | publish_playstore.py → `android/` |
| publish-tada-wheel.yml | 101, 106, 111, 220, 231 | build_tada_wheel.sh, verify_wheel_install.sh, verify_tada_wheel.py, push_ghcr_bundle.sh, prune_ghcr_versions.sh → `tada-wheel/` |
| ship-accuracy-report.yml | 21 | ship_accuracy_report.py → `accuracy/` |

### 2. Local-checkout paths (`central/scripts/tars_X.py`)

| Workflow | Line(s) | Script → folder |
|---|---|---|
| tars-ci.yml | 49, 113 | tars_payload.py → `tars/` |
| tars-deploy.yml | 44, 264 | tars_payload.py → `tars/` |
| tars-deploy.yml | 83, 213 | tars_lock_outputs.py → `tars/` |
| tars-deploy.yml | 98 | tars_tada_bundle.py → `tars/` |
| tars-deploy.yml | 109, 111, 116, 317, 319, 322, 325, 328, 331, 338 | tars_infisical.py → `tars/` |

### Scripts with no workflow references (moved only, no usage to update)

`resolve_spm.py`, `crashlytics_blame.py`, `crashlytics_report.py`,
`tars_worker_render_gate.py`, `test_tars_delivery.py` are not referenced by any
workflow (`tars_worker_render_gate.py` and the test are only imported by
`test_tars_delivery.py`, which has no CI runner). They are relocated with the
rest but require no reference edits.

### 3. Non-workflow references

- `.claude/settings.local.json:14` — absolute path to `crashlytics_report.py`.
  Local/personal permission entry; update to `scripts/crashlytics/crashlytics_report.py`.
- `CLAUDE.md:9` — scripts convention. Rewrite it to **mandate** that new scripts
  go in a domain subfolder under `scripts/` (e.g. `scripts/ios/`,
  `scripts/catchup/`) and be invoked via the raw URL **including that subfolder**:
  `https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/<domain>/<name>`.
  Fix the pre-existing "scrips" typo on the same line. This keeps `scripts/` from
  silently re-accumulating loose files.

## Mechanics & safety

- Use `git mv` for every file to preserve history.
- **Single atomic merge to `main`.** All raw URLs pin to `/main/`, so the file
  moves and their URL updates must land together. Until merge, `main` is
  unchanged → zero transient breakage; after merge, files and URLs are
  consistent. Confirmed: no repo outside `Lascade-Co/actions` curls these URLs,
  so no external fallout.

## Verification

1. `grep -rnE 'scripts/[a-zA-Z0-9_.-]+\.(py|sh)'` over `.github/` returns **only**
   subfolder paths — zero remaining flat `scripts/<file>` references.
2. Each moved file exists at its new path; `git status` shows renames, not
   delete+add (history preserved).
3. Every workflow YAML parses; run `actionlint` if available.
4. `python3 -m py_compile` succeeds on every moved `.py` file (syntax intact
   after relocation).
5. **Run `test_tars_delivery.py` from its new location** (`scripts/tars/`). This
   is a real behavioral check, not just static: it exercises the `tars/`
   co-located imports AND its `WorkflowContractTest` cases re-read the (now
   updated) `tars-ci.yml` / `tars-deploy.yml`. All 36 tests currently pass; they
   must still pass after the move + ROOT fix. (This also confirms the earlier
   `run-name` additions to the tars workflows didn't break their contracts.)
6. Residual runtime risk for the curl-fetched scripts is low: contents unchanged,
   only file locations and reference strings move.

## Out of scope

- `data/` folder reorganization.
- Any change to script logic, arguments, or behavior.
- Wiring `test_tars_delivery.py` into a CI runner (it currently has none).
