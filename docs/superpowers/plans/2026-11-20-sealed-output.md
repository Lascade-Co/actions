# Sealed Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop this public repo's Actions logs and artifacts from exposing **private values**. Every job's own output becomes a **sealed log**, every artifact a **sealed artifact**, and a **log viewer** at `https://ci.lascade.com` opens them in the browser.

**Architecture:** A first step in each job installs `seal-bash` and `seal-bash-e`, custom Actions shells set as the workflow default. Each runs a `run:` script with its stdout and stderr redirected straight to a per-step file (a file, not a pipe, so background processes can't hold the step open). The step summary and workflow commands go to the same per-step file that is age-encrypted to the **repo key**. The public log keeps one marker line per step. A final `if: always()` step uploads the sealed segments as an artifact. Artifacts are packed with `seal-pack` before upload and opened with `seal-unpack` after download. A static page in `viewer/`, deployed to GitHub Pages, fetches a run's public job logs and sealed artifacts with the person's read-only token. It decrypts them with the repo key stored in browser storage and puts each sealed segment back in place of its marker.

**Tech Stack:** bash + perl (runner wrapper), age v1.3.2 (binary), Python 3.13 + pytest + PyYAML (policy and purge tests), vanilla ES modules (viewer), vendored `age-encryption@0.3.1`, `fflate@0.8.3`, `ansi_up@6.0.6`, Node `node --test` (viewer unit tests), `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `actions/configure-pages`, `actions/upload-pages-artifact`, `actions/deploy-pages` (latest majors).

**Spec:** `docs/adr/0014-keep-the-runner-public-and-seal-private-output.md` and the "Language — Confidentiality" section of `CONTEXT.md`. Read both before starting.

## Global Constraints

- The **repo key** is one age X25519 pair. The public half is `scripts/seal/repo-key.pub`. The private half lives only in Infisical, synced to Actions as secret `REPO_AGE_KEY`. Never write the private half to disk, a log or a commit.
- age binary: v1.3.2 from `https://github.com/FiloSottile/age/releases/download/v1.3.2/age-v1.3.2-<os-arch>.tar.gz`. SHA-256: linux-amd64 `cbe24006683f8eb669266162894b9a522a1af52f2665fbc63a4bb032ed26ac10`, linux-arm64 `6b8dc4333c53a5a57c9e5834e3a48f92605d7154014cd07269ff3327db5d37f4`, darwin-arm64 `e2020b073c44f692685a24d6abc378817eb81ffaaf49fd0531ef8565f767f2f5`. Any other platform fails the install.
- **Public metadata** (may stay in plain text): repo name, branch, PR number and title, commit SHA. Everything else is a **private value**.
- Exempt from sealing: workflows whose only trigger is `workflow_call` *and* that no workflow in this repo calls via `uses: ./.github/workflows/…`. Today that is `deploy-trigger.yml`, `static-pages-deploy-trigger.yml` and `vinext-deploy-trigger.yml`, which run in their callers' repos. `release-blog.yml` is called by `android-build-release.yml`, so it is sealed.
- `actions/upload-pages-artifact` uploads the public site and is the only unsealed upload allowed.
- Artifact retention for every upload is `retention-days: 7`.
- Only `::add-mask::` lines may pass from a sealed step to the public log.
- Control downloads are pinned to `${{ github.workflow_sha }}`. Use bounded curl retries, and promote a download only if it succeeded and is nonempty (repo CLAUDE.md).
- Viewer origin: `https://ci.lascade.com`. Browser storage keys: `ci.repoKey`, `ci.githubToken`. All third-party JS is vendored with recorded SHA-256 (no CDN at runtime).
- No GitHub Actions run may be started in a private repo (user CLAUDE.md). This repo is public, so pushes and test runs here are allowed.

### Fixed formats shared between tasks

- **Setup step**: the first step of every sealed job, copied verbatim:
  ```yaml
      - name: Seal job output
        shell: bash
        run: |
          u="https://raw.githubusercontent.com/Lascade-Co/actions/${{ github.workflow_sha }}/scripts/seal/install.sh"
          curl -fsSL --retry 3 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 60 -o "$RUNNER_TEMP/seal-install.tmp" "$u"
          test -s "$RUNNER_TEMP/seal-install.tmp" && mv "$RUNNER_TEMP/seal-install.tmp" "$RUNNER_TEMP/seal-install.sh"
          SEAL_REF="${{ github.workflow_sha }}" bash "$RUNNER_TEMP/seal-install.sh"
  ```
- **Upload step**: the last step of every sealed job, copied verbatim:
  ```yaml
      - name: Upload sealed log
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: ${{ env.SEAL_LOG_ARTIFACT }}
          path: ${{ runner.temp }}/seal/out/
          retention-days: 7
          if-no-files-found: ignore
  ```
- **Shell modes** must match what each step runs with today:
  - `seal-bash {0}` runs `bash --noprofile --norc -eo pipefail`, the same as `shell: bash`.
  - `seal-bash-e {0}` runs `bash -e`, the same as GitHub's implicit default.
  
  Conversion:
  - A workflow or job whose `defaults.run.shell` is `bash` switches to `seal-bash {0}`.
  - Every other workflow gets `defaults: { run: { shell: "seal-bash-e {0}" } }`.
  - A step with explicit `shell: bash` becomes `shell: seal-bash {0}`.
  - In composite actions, `shell: bash` becomes `shell: seal-bash {0}`.
- **Sealed log artifact**: name `sealed-log-<GITHUB_JOB>-<GITHUB_RUN_ATTEMPT>-<8 hex random>`, exported by `install.sh` as `SEAL_LOG_ARTIFACT` via `$GITHUB_ENV`. It contains `meta.json.age` (`{"job","attempt","runner_name","os","started_at"}`), plus `<seq>.log.age` per step and `<seq>.summary.md.age` when that step wrote a summary. `<seq>` is a 1-based decimal integer.
- **Segment file**: the step's raw combined stdout/stderr, unmodified. There are no per-line timestamps, because a timestamping pipe would hang on background processes. The marker line's own log timestamp marks when the step ended.
- **Marker line**: `seal-bash` prints exactly one per step to the public log:
  `🔒 sealed <artifact>#<seq> https://ci.lascade.com/?run=<GITHUB_RUN_ID>&attempt=<GITHUB_RUN_ATTEMPT>`
  The viewer matches `/🔒 sealed (sealed-log-[A-Za-z0-9_.-]+)#(\d+)/`.
- **Sealed artifact contents**: one file, `<artifact-name>.tgz.age`, a gzip tar whose members are relative to the longest common directory of the packed paths (the same root `upload-artifact` would use).

## Review Focus

1. **A step that prints ~200k lines** (xcodebuild, Gradle `--info`) must not slow the job noticeably or drop lines. Test `test_large_output_is_complete` in Task 1.
2. **A step that fails, is cancelled, or times out** must still leave its segment sealed and uploaded, and still exit with the step's own exit code. Tests `test_exit_code_propagates` and `test_segment_sealed_on_failure` in Task 1. The e2e job in Task 4 checks the uploaded artifact.
3. **A private value in a third-party action's `with:` input** (for example a `steps.x.outputs.y` that holds a report number) is still printed by the runner. The policy test cannot prove a value is public, so Task 4's `docs/sealed-output.md` says to `::add-mask::` it before it reaches a header. The e2e canary test proves masked values print as `***`.
4. **A job where setup failed** (network down) must fail at the first sealed step, not run unsealed. Test `test_missing_wrapper_fails_closed` in Task 4.
5. **Viewer given a run with no sealed logs** (older run, exempt workflow, or expired artifacts) must still show the public log with a clear "nothing sealed" notice, not a blank page. Test `renders public log when no sealed artifacts` in Task 2.

---

### Task 0: Generate the repo key (the user does it, before merge)

The private half must never appear in this session's transcript, so the user generates the key in their own terminal, not via `!`.

**Files:**
- Create: `scripts/seal/repo-key.pub`

- [ ] **Step 1:** Ask the user to run `age-keygen` in a separate terminal (`brew install age` if needed). They save the `AGE-SECRET-KEY-1…` line in Infisical as `REPO_AGE_KEY`, in the project and environment that syncs to `Lascade-Co/actions`, and paste back only the `age1…` public key.
- [ ] **Step 2:** Validate it matches `^age1[02-9ac-hj-np-z]{58}$`.
- [ ] **Step 3:** Write the `age1…` public key (one line) to `scripts/seal/repo-key.pub`.
- [ ] **Step 4:** After the user confirms, verify `gh secret list -R Lascade-Co/actions | grep REPO_AGE_KEY` shows the synced secret.
- [ ] **Step 5:** Commit `scripts/seal/repo-key.pub` (`chore(seal): add repo key public half`).

### Task 1: Runner sealing toolkit

**Files:**
- Create: `scripts/seal/install.sh`, `scripts/seal/seal-bash` (also installed as `seal-bash-e`), `scripts/seal/seal-pack`, `scripts/seal/seal-unpack`
- Test: `scripts/seal/test_seal_toolkit.py`

**Interfaces:**
- Consumes: `scripts/seal/repo-key.pub` (Task 0). Tests generate their own throwaway key and point `SEAL_PUBKEY_FILE` at it.
- Produces:
  - `install.sh` (env `SEAL_REF` = commit SHA; `RUNNER_TEMP`, `GITHUB_ENV`, `GITHUB_PATH`, `GITHUB_JOB`, `GITHUB_RUN_ATTEMPT`, `RUNNER_NAME`, `RUNNER_OS` from the runner). It installs verified age/age-keygen and the three tools into `$RUNNER_TEMP/seal/bin`, and fetches `repo-key.pub` at `SEAL_REF` to `$RUNNER_TEMP/seal/repo-key.pub` with the curl rules above. It appends the bin dir to `$GITHUB_PATH`, symlinks the tools into `/usr/local/bin` with `sudo`, writes `SEAL_LOG_ARTIFACT=…` to `$GITHUB_ENV`, writes `$RUNNER_TEMP/seal/out/meta.json.age`, and prints the viewer URL once. `SEAL_SOURCE_DIR=<dir>` (tests only) copies the tools from a local dir instead of curl.
  - `seal-bash <script>` / `seal-bash-e <script>`: run `bash --noprofile --norc -eo pipefail <script>` / `bash -e <script>` with `>"$RUNNER_TEMP/seal/log/<seq>.log" 2>&1` and `GITHUB_STEP_SUMMARY` pointed at a temp file. When the script exits, they print the log's `^::add-mask::` lines to stdout, so masks take effect from the next step. It prints the marker line, encrypts to `$RUNNER_TEMP/seal/out/<seq>.log.age` (and `<seq>.summary.md.age` if the summary is nonempty), deletes the plaintext, and exits with the script's exit code. `<seq>` comes from an increment of `$RUNNER_TEMP/seal/seq`.
  - `seal-pack [--allow-empty] <artifact-name> <path-or-glob>...` → writes `$RUNNER_TEMP/seal/art/<artifact-name>/<artifact-name>.tgz.age` and prints that directory. If nothing matches, it exits 1 unless `--allow-empty` is given, in which case it exits 0 and prints the empty dir.
  - `seal-unpack <dir> [<dest>=<dir>]` (env `REPO_AGE_KEY`): decrypts every `*.tgz.age` in `<dir>` into `<dest>` and deletes each `.age` file. The identity comes in through process substitution, never a file. It exits 1 if no `.tgz.age` is found.

- [ ] **Step 1: Write the failing tests** in `scripts/seal/test_seal_toolkit.py`. A pytest fixture builds a fake runner env in `tmp_path` (`RUNNER_TEMP`, `GITHUB_ENV`, `GITHUB_PATH`, `GITHUB_JOB=build`, `GITHUB_RUN_ATTEMPT=1`, `GITHUB_RUN_ID=42`). It generates a key with the installed `age-keygen` and runs `install.sh` with `SEAL_SOURCE_DIR=scripts/seal` (sudo symlink skipped when `SEAL_NO_SUDO=1`). Tests:
  - `test_install_exports_artifact_name`: `GITHUB_ENV` contains a line matching `^SEAL_LOG_ARTIFACT=sealed-log-build-1-[0-9a-f]{8}$`, and `meta.json.age` decrypts to JSON with `job == "build"`.
  - `test_install_rejects_bad_checksum`: with `SEAL_AGE_SHA256_OVERRIDE=00…` install exits non-zero and leaves no `seal-bash`.
  - `test_stdout_shows_only_marker_and_masks`: script `echo CANARY-123; echo "::add-mask::m1"; echo "::error::CANARY-456" >&2` → stdout lines are exactly `["::add-mask::m1", "🔒 sealed <name>#1 https://ci.lascade.com/?run=42&attempt=1"]`, and the decrypted `1.log.age` contains both canaries.
  - `test_shell_modes_match_github`: `false | true; echo ok` → under `seal-bash-e` it exits 0 and logs `ok`, and under `seal-bash` it exits 1.
  - `test_background_process_does_not_block`: `sleep 300 & echo started` → the wrapper returns within 5 s with exit 0.
  - `test_summary_is_sealed`: script `echo "# r" >> "$GITHUB_STEP_SUMMARY"` → `1.summary.md.age` decrypts to `# r\n`, and the fixture's real summary file stays empty.
  - `test_exit_code_propagates`: `exit 3` → wrapper exits 3. `false; echo after` → exits 1 and `after` is absent from the log (the `-e` semantics are kept).
  - `test_segment_sealed_on_failure`: after `exit 3`, `1.log.age` exists and no plaintext `log/1.log` remains.
  - `test_seq_increments`: two invocations produce `1.log.age` and `2.log.age`.
  - `test_large_output_is_complete`: `seq 1 200000` → the decrypted log has 200000 lines in order, in under 20 s.
  - `test_pack_single_file` and `test_pack_single_dir`: packing just `a/x.txt` unpacks to `x.txt`. Packing just `a/b` unpacks to `y.bin` with no `a/b` prefix, matching `upload-artifact`'s root rule.
  - `test_pack_unpack_roundtrip`: files `a/x.txt`, `a/b/y.bin` (random 1 MB), symlink `a/l → x.txt`. `seal-pack art a/x.txt a/b` then `seal-unpack` into a new dir recreates `x.txt` and `b/y.bin` relative to `a`, byte-identical, with the symlink preserved when packed via `a`.
  - `test_pack_no_match`: no match → exit 1. With `--allow-empty` → exit 0 and no `.age` file.
  - `test_unpack_without_key_fails`: `REPO_AGE_KEY` unset → exit non-zero and nothing extracted.
- [ ] **Step 2:** Run `.venv/bin/python -m pytest scripts/seal/test_seal_toolkit.py -q`. Expected: FAIL (scripts missing).
- [ ] **Step 3: Implement the scripts.** `seal-bash-e` is `seal-bash` with a different bash flag set (one script, mode chosen by `basename "$0"`, installed under both names). It seals in a `trap … EXIT INT TERM` so a killed, cancelled or failing script still seals. `install.sh` picks the platform from `uname -s`/`uname -m` (`Linux x86_64`→linux-amd64, `Linux aarch64`→linux-arm64, `Darwin arm64`→darwin-arm64). It verifies with `sha256sum` or `shasum -a 256`, and builds the random suffix from `od -An -N4 -tx1 /dev/urandom | tr -d ' \n'`. `seal-pack` uses `python3 -c` for `os.path.commonpath` of the expanded matches, then `tar -czf - -C <root> <rel paths…> | age -R <pubkey> -o <out>`. All scripts start with `set -euo pipefail`.
- [ ] **Step 4:** Run the same pytest command. Expected: all pass.
- [ ] **Step 5:** Commit (`feat(seal): runner sealing toolkit`).

### Task 1b: Spikes on real runners and a real browser (gate before Tasks 2–4 fan out)

Nothing in this design has run on a real runner yet. Prove the load-bearing assumptions on a branch push to this public repo; that's free, and the user's rule covers only private repos.

- [ ] **Step 1: Runner spike.** Write a temporary `.github/workflows/spike-seal.yml` with `ubuntu-latest` and `macos-latest` jobs. Each job has the setup step, `defaults.run.shell: seal-bash-e {0}`, and a step `echo CANARY; echo "::add-mask::M"`. A later step uses a temporary composite at `actions/spike-seal/action.yml` with `shell: seal-bash {0}`. A third-party step (`actions/github-script`) prints `M`. Push the branch. Check that:
  - both jobs pass
  - the public log shows the marker lines, no `CANARY`, and `***` for `M` in the third-party step
  - the sealed-log artifact decrypts locally (`infisical run -- …`, key from the user's Infisical)
  
  If the custom shell isn't found via `GITHUB_PATH`, check whether the `/usr/local/bin` symlink fixes it and record which one did. Delete the spike files and the branch afterwards.
- [ ] **Step 2: Browser spike.** Ask the user to create a fine-grained token: any account, "Public repositories (read-only)", no other permissions. Serve a scratch HTML page on `http://localhost` that, with that token:
  - lists the spike run's jobs
  - fetches one job log via `/actions/jobs/{id}/logs`
  - fetches the sealed-log artifact zip via `/actions/artifacts/{id}/zip`, following the redirect

  Check all three succeed in Chrome (claude-in-chrome tools) and show no CORS errors in the console. If the fine-grained token is refused for artifacts, record which token type works; the viewer's settings copy must name it. Delete the scratch page afterwards.
- [ ] **Step 3:** Record the outcomes (shell resolution path, token type) at the top of `docs/sealed-output.md` (created in Task 4). Only then start Tasks 2–4.

### Task 2: Log viewer

**Files:**
- Create: `viewer/index.html`, `viewer/app.js`, `viewer/style.css`
- Create: `viewer/lib/github.js`, `viewer/lib/joblog.js`, `viewer/lib/sealed.js`, `viewer/lib/tar.js`
- Create: `viewer/vendor/age-encryption.js`, `viewer/vendor/fflate.js`, `viewer/vendor/ansi_up.js`, `viewer/vendor/SHA256SUMS`
- Test: `viewer/lib/joblog.test.mjs`, `viewer/lib/tar.test.mjs`, `viewer/lib/sealed.test.mjs`

**Interfaces:**
- Consumes: the marker line, sealed log layout and sealed artifact contents from "Fixed formats".
- Produces:
  - `github.js`: `listJobs(token, runId, attempt) -> Promise<Job[]>` (paginated `/actions/runs/{id}/attempts/{n}/jobs`), `jobLog(token, jobId) -> Promise<string>`, `listArtifacts(token, runId) -> Promise<Artifact[]>`, `artifactZip(token, artifactId) -> Promise<Uint8Array>`. Repo fixed to `Lascade-Co/actions`. Every request sends `Authorization: Bearer <token>` and follows the redirect.
  - `joblog.js`: `parseJobLog(text) -> Section[]`, where `Section = {title, lines: Line[]}` is split at `##[group]Run ` headers (lines before the first header go to a "Set up job" section), and `Line = {ts, text, marker?: {artifact, seq}}`. Also `mergeSealed(sections, segments: Map<"artifact#seq", string[]>) -> Section[]`, which replaces each marker line with its segment's lines tagged `sealed: true`. A marker with no segment becomes one line `🔒 sealed output unavailable (<reason>)`.
  - `sealed.js`: `openSealedLog(zipBytes, identity) -> Promise<{meta, segments: Map<string,string[]>, summaries: Map<string,string>}>` and `openSealedArtifact(zipBytes, identity) -> Promise<{name, entries: TarEntry[]}>`.
  - `tar.js`: `readTar(bytes) -> TarEntry[]`, with `TarEntry = {path, type: "file"|"dir"|"symlink", data?: Uint8Array, linkname?}`. Supports ustar plus PAX `path`/`linkpath` long names.

- [ ] **Step 1: Vendor dependencies.** Fetch the ESM builds of `age-encryption@0.3.1`, `fflate@0.8.3` and `ansi_up@6.0.6` from the npm registry tarballs into `viewer/vendor/`, and record `sha256sum` output in `viewer/vendor/SHA256SUMS`.
- [ ] **Step 2: Write the failing tests.**
  - `joblog.test.mjs`: `parseJobLog` on a fixture holding three `##[group]Run` headers, one marker line `2026-11-20T10:00:00.0000000Z 🔒 sealed sealed-log-build-1-0a1b2c3d#2 https://ci.lascade.com/?run=1&attempt=1` and preamble lines → 4 sections, with the marker parsed as `{artifact:"sealed-log-build-1-0a1b2c3d", seq:2}`. `mergeSealed` puts segment lines in place of the marker, in order, with `sealed:true`. A missing segment yields the "unavailable" line. Test `renders public log when no sealed artifacts`: `mergeSealed(sections, new Map())` keeps every non-marker line.
  - `tar.test.mjs`: build a tar in the test with Node's `child_process.execFileSync("tar", …)` containing a file, a nested dir, a symlink and a 150-char path. `readTar` returns all four with correct paths, types, data and linkname.
  - `sealed.test.mjs`: with `age-encryption`, generate an identity, encrypt `"line1\nline2"` as `2.log.age` and `{"job":"build"}` as `meta.json.age`, and zip them with fflate. `openSealedLog` returns `meta.job === "build"` and `segments.get("<artifact>#2")` equal to `["line1","line2"]`. A wrong identity rejects with an error whose message contains `repo key`.
- [ ] **Step 3:** Run `node --test viewer/lib/*.test.mjs`. Expected: FAIL.
- [ ] **Step 4: Implement the libs**, then the page.
  - `index.html` has a CSP meta: `default-src 'self'; connect-src https://api.github.com https://*.blob.core.windows.net; img-src 'self' data:; style-src 'self'; script-src 'self'`.
  - `ansi_up` runs in class mode (`use_classes = true`) with colour classes in `style.css`, because the CSP forbids inline styles.
  - `app.js` reads `?run=` and `&attempt=` (default: latest attempt). A settings dialog stores `ci.repoKey` and `ci.githubToken`, with a "forget" button.
  - The page shows a jobs sidebar (name, conclusion icon, duration) and collapsible sections (open by default when failed). Lines are rendered with `ansi_up` and HTML-escaped. Each line has an `#L<section>-<n>` anchor, and there's a search box that filters lines.
  - Sealed lines get a lock gutter mark. Step summaries render as escaped preformatted Markdown under their step.
  - An Artifacts panel lists non-`sealed-log-*` artifacts. "Unseal" decrypts the artifact, previews text entries (≤1 MB, UTF-8) inline, offers per-entry download, and offers "Download all (.tar.gz)" of the decrypted gzip bytes.
  - Errors (401 token, bad key, expired artifact) appear as one banner line naming the fix.
- [ ] **Step 5:** Run `node --test viewer/lib/*.test.mjs`. Expected: all pass.
- [ ] **Step 6:** Commit (`feat(viewer): sealed log viewer`).

### Task 3: Convert workflows and composite actions

Split across up to four parallel subagents by file group; each edits only its files:
- (a) `amper-debug`, `amper-release`, `kmp-build-debug`, `kmp-build-release`
- (b) `flutter-build-debug`, `flutter-build-release`, `ios-build-debug`, `ios-build-release`, `publish-appstore`, `publish-spm`, `asc-build-status`
- (c) `android-build-debug`, `android-build-release`, `release-blog`, `publish-playstore`, `deploy-runner`, `docker-image`, `cloudflare-deploy`, `vercel-deploy`, `static-pages-deploy-runner`, `vinext-deploy-runner`, and `actions/*/action.yml`
- (d) `daily-*`, `install-paid-watchdog`, `seo-blog-audit`, `ship-accuracy-report`, `test-ai`, `test-catchup`

**Files:** the workflow files above in `.github/workflows/`, plus `actions/{apple-signing,gh-bundle-cache,pr-status-comment,telegram-notify}/action.yml`.

**Interfaces:**
- Consumes: the setup step, upload step, workflow default, `seal-pack` and `seal-unpack` (Task 1).
- Produces: workflows that pass `scripts/seal/test_workflow_policy.py` (Task 4).

Rules for each sealed workflow:
1. Apply the "Shell modes" conversion exactly. Never change a step's effective flags: no step gains or loses `pipefail`.
2. In every job that has `steps:`, insert the setup step first and the upload step last.
3. Replace each `actions/upload-artifact` (except the sealed-log one and `upload-pages-artifact`) with a `run: seal-pack [--allow-empty] <name> <paths…>` step that has `id: pack_<n>`. Use `--allow-empty` iff the original had `if-no-files-found: ignore`/`warn`. It is followed by `upload-artifact` with the same `name`, `path: ${{ runner.temp }}/seal/art/<name>/`, `retention-days: 7`, and the original `compression-level: 0` (the payload is already compressed and encrypted). Keep `id`, `if:` and `overwrite` as they were.
4. After each `actions/download-artifact`, add a `run: seal-unpack <download path>` step with `env: { REPO_AGE_KEY: ${{ secrets.REPO_AGE_KEY }} }`. For `merge-multiple: true` downloads, one `seal-unpack` on the merged dir unpacks them all.
5. Set any existing `retention-days` above 7 to 7.
6. A `run:` that expands a non-secret, non-metadata private value via `${{ }}` into its script text must move it to `env:` and `::add-mask::` it first. Report every such site in the task summary.
7. Apply the decisions the user makes on "Third-party output" (Task 7, Step 0). Defaults if they accept the recommendations:
   - `gradle/actions/setup-gradle` gets `add-job-summary: never`.
   - `docker/build-push-action` gets env `DOCKER_BUILD_SUMMARY: false` and `DOCKER_BUILD_RECORD_UPLOAD: false`.
   - `anthropics/claude-code-action` keeps its full output hidden (`show_full_output: false`, its default; set it explicitly).
   - `actions/github-script` scripts may print only public metadata.

- [ ] **Step 1:** Convert the group's files per rules 1–6.
- [ ] **Step 2:** Run `actionlint` (`brew install actionlint` locally if missing; remove afterwards if you installed it) on the group's files. Expected: no errors.
- [ ] **Step 3:** Run `.venv/bin/python -m pytest scripts/seal/test_workflow_policy.py -q -k "<group files>"` once Task 4 exists. Before that, rely on Step 2 and self-review against rules 1–6.
- [ ] **Step 4:** Run the repo's existing tests that parse these workflows (e.g. `scripts/kmp/test_composites.py`, `scripts/amper/test_*.py`, `node --test scripts/pages scripts/vinext`). Expected: pass. Update assertions only where they pin the literal old step list.
- [ ] **Step 5:** Commit per group (`feat(seal): seal <group> workflows`).

### Task 4: Policy test, e2e workflow, author docs

**Files:**
- Create: `scripts/seal/test_workflow_policy.py`, `.github/workflows/test-seal.yml`, `.github/workflows/test-seal-e2e.yml`, `docs/sealed-output.md`
- Modify: `CLAUDE.md` (Conventions: add the sealing rules in four bullets and a link to `docs/sealed-output.md`)

**Interfaces:**
- Consumes: the formats from "Fixed formats", and Task 1's tools.

- [ ] **Step 1: Write `test_workflow_policy.py`**, parametrized per workflow and composite file using PyYAML. For non-exempt workflows:
  - `test_default_shell_is_sealed`
  - `test_setup_step_first` (the step equals the setup step verbatim)
  - `test_upload_step_last`
  - `test_no_unsealed_shell` (no `run` step other than setup sets `shell` to anything but `seal-bash {0}`)
  - `test_uploads_are_sealed` (every `upload-artifact` path starts with `${{ runner.temp }}/seal/` and the previous step runs `seal-pack`, except `upload-pages-artifact`)
  - `test_downloads_are_unsealed` (the step after every `download-artifact` runs `seal-unpack` with `REPO_AGE_KEY` from secrets)
  - `test_retention_at_most_7`

  For all workflows:
  - `test_no_pull_request_target`
  - `test_pull_request_workflows_use_no_secrets` (the file text has no `secrets.` when `pull_request` is a trigger)
  - `test_exempt_set_is_exactly` `{"deploy-trigger.yml","static-pages-deploy-trigger.yml","vinext-deploy-trigger.yml"}` (computed by the exemption rule)

  For composites, `test_composite_run_steps_sealed`. Also `test_missing_wrapper_fails_closed`: run `bash -c 'command -v seal-bash'` with a clean PATH, assert it fails, and assert the docs state that a job without setup fails at its first step. This pins the fail-closed behaviour: the runner errors when the shell command is not found.
- [ ] **Step 2:** Run it before Task 3 lands. Expected: FAIL on unconverted workflows. After Task 3, expect PASS.
- [ ] **Step 3: Write two workflows**, both sealed and both with paths `scripts/seal/**`, `viewer/**`, `.github/workflows/**` and `actions/**`. `test-seal.yml` triggers on `push` and `pull_request` and uses no secrets. `test-seal-e2e.yml` triggers on `push` only, because it needs `REPO_AGE_KEY` and the policy forbids secrets in `pull_request` workflows.
  - `test-seal.yml` job `unit` runs on `ubuntu-latest` and `macos-latest` (matrix). It runs pytest on `scripts/seal/` (`pytest==9.1.1 pyyaml`), and on ubuntu runs `node --test viewer/lib/*.test.mjs`.
  - `test-seal-e2e.yml` job `e2e-produce`. Its step prints `CANARY-$GITHUB_RUN_ID` and masks then prints `MASKED-$GITHUB_RUN_ID`. It also runs `seal-pack e2e-art <file containing CANARY>` and uploads that.
  - `test-seal-e2e.yml` job `e2e-verify` runs after produce, with `permissions: actions: read`. It fetches the produce job's public log via `gh api` and asserts `CANARY-` and `MASKED-` are absent and the marker line is present. It downloads the produce job's sealed-log artifact and `e2e-art`, unseals both with `REPO_AGE_KEY`, and asserts `CANARY-` is present in both. It asserts `MASKED-` appears in the sealed log, since masking applies only to the public log.
- [ ] **Step 4: Write `docs/sealed-output.md`** in ≤1 page. Cover: what's sealed, the setup and upload snippets, `seal-pack`/`seal-unpack`, when to `::add-mask::` (Review Focus 3), the exemption rule, how to open a run (viewer URL), and how to rotate the repo key (new pair → Infisical → commit `.pub`; old artifacts expire within 7 days).
- [ ] **Step 5:** Commit (`test(seal): workflow policy, e2e check and author docs`).

### Task 5: Scrub real IDs and emails from tests and docs

**Files:**
- Modify: `scripts/pnl/test_pnl_googleads.py`, `scripts/pnl/test_pnl_metaads.py`, `docs/superpowers/plans/2026-08-10-marketing-net.md`
- Modify: `scripts/adspend/test_adspend.py`, `scripts/hunch/test_hr_delivery.py`, `scripts/installpaid/test_ip_delivery.py`, `scripts/installpaid/test_ip_sender.py`

- [ ] **Step 1:** Replace the IDs `6426942742` → `1111111111`, `6096341923` → `2222222222` and `act_2115160355488257` / `2115160355488257` → `act_3333333333333333` / `3333333333333333`.
- [ ] **Step 2:** In the four test files, replace `@lascade.com` addresses with `@example.com` addresses. Where a test asserts a production default recipient, assert against the imported production constant instead of a literal.
- [ ] **Step 3:** Run `git grep -nE "6426942742|6096341923|2115160355488257"`. Expected: no output. Run `git grep -n "@lascade.com" -- 'scripts/**/test_*'`. Expected: no output.
- [ ] **Step 4:** Run `.venv/bin/python -m pytest scripts/pnl scripts/adspend scripts/hunch scripts/installpaid -q`. Expected: same pass count as on `main`.
- [ ] **Step 5:** Commit (`test: replace real ad account IDs and emails with fakes`).

### Task 6: Archive-and-purge tool

**Files:**
- Create: `scripts/seal/purge_runs.py`
- Test: `scripts/seal/test_purge_runs.py`

**Interfaces:**
- Produces: `python3 scripts/seal/purge_runs.py --repo Lascade-Co/actions --archive ~/Downloads/actions-log-archive [--dry-run]`. The token comes from `gh auth token`.

Behaviour:
- Repeatedly fetch `GET /repos/{repo}/actions/runs?per_page=100` (page 1; deletions shift pages) until no `completed` runs remain. Skip runs whose `status != "completed"`.
- For each run, `GET /runs/{id}/logs`:
  - 200 → write `<archive>/<id>.zip` via a temp file, renamed into place only when it's nonempty.
  - 404/410 → log as `expired`.
  - Any other status → stop without deleting.
- Append `id\tworkflow\tevent\tcreated_at\tconclusion\tlog_status\n` to `<archive>/index.tsv`, then `DELETE /runs/{id}` and require 204.
- Honor `X-RateLimit-Remaining`/`Reset` and `Retry-After` by sleeping. Resume is idempotent: a run already in `index.tsv` whose zip exists is deleted without re-downloading.

- [ ] **Step 1: Write failing tests** with a fake HTTP client:
  - `test_archives_then_deletes`
  - `test_expired_logs_still_deleted`
  - `test_server_error_stops_without_delete`
  - `test_skips_in_progress`
  - `test_empty_zip_not_promoted`
  - `test_dry_run_deletes_nothing`
  - `test_resume_skips_redownload`
  - `test_rate_limit_sleeps_until_reset` (sleep is injected)
- [ ] **Step 2:** Run `.venv/bin/python -m pytest scripts/seal/test_purge_runs.py -q`. Expected: FAIL.
- [ ] **Step 3:** Implement with `urllib.request` (no new deps) and a pluggable `http` and `sleep`.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit (`feat(seal): archive-and-purge tool for past runs`).

### Task 7: Ship and switch on (with the user, in order)

- [ ] **Step 0: Third-party output (decide before Task 3).** These actions print to the public log, and masking can't hide what they print. Show the user this list with the recommended handling from Task 3 rule 7, and get a decision on each:
  - `anthropics/claude-code-action` (lint-fix agent)
  - `docker/build-push-action` (build output and build summary)
  - `gradle/actions/setup-gradle` (job summary)
  - `actions/github-script` (11 uses)
  - `cloudflare/wrangler-action` (deploy output)
  - `softprops/action-gh-release`
  - `appleboy/telegram-action`
  - `stefanzweifel/git-auto-commit-action`
  - `github/branch-deploy`
  - `digitalocean/action-doctl`

- [ ] **Step 1:** Confirm `REPO_AGE_KEY` is synced (Task 0, Step 4). Open a PR with Tasks 0–6. `test-seal` `unit` runs on the PR. The secret-dependent e2e jobs run on push to the branch: check that the e2e jobs pass there.
- [ ] **Step 2:** Ask the user before merging. After merge, check the first scheduled and dispatched runs: the public log shows only markers, and the viewer opens them.
- [ ] **Step 3: Pages and DNS.**
  - Create `.github/workflows/viewer-pages.yml` in the PR (sealed; on push to `main` with paths `viewer/**`; `configure-pages`, `upload-pages-artifact` with path `viewer`, `deploy-pages`; `permissions: pages: write, id-token: write`).
  - After merge, run `gh api -X POST repos/Lascade-Co/actions/pages -f build_type=workflow`, then `gh api -X PUT repos/Lascade-Co/actions/pages -f cname=ci.lascade.com`.
  - Create the DNS record via the Cloudflare connector: `CNAME ci → lascade-co.github.io`, proxied off.
  - When the certificate is issued, run `gh api -X PUT repos/Lascade-Co/actions/pages -F https_enforced=true`.
- [ ] **Step 4: Open a real run in the viewer** with the Chrome tools. Check that a KMP debug run and a daily-catchup run show sealed lines in place, and that the catchup report artifact previews inline.
- [ ] **Step 5:** Set fork-PR approval: `gh api -X PUT repos/Lascade-Co/actions/actions/permissions/fork-pr-contributor-approval -f approval_policy=all_external_contributors`.
- [ ] **Step 6: Purge.** This comes before lowering retention, because a 7-day setting would expire logs before they're archived.
  - Run `purge_runs.py --dry-run` and show the user the counts.
  - On their go-ahead, run it for real in the background.
  - Report the archive size and the number of runs deleted or expired.
  - Check that `gh api 'repos/Lascade-Co/actions/actions/runs?per_page=1' --jq .total_count` reports only runs started after the purge began.
- [ ] **Step 6b:** Set retention: `gh api -X PUT repos/Lascade-Co/actions/actions/permissions/artifact-and-log-retention -F days=7`.
- [ ] **Step 7:** Clean up scratch files and any locally installed tools (actionlint, age) created during execution.
