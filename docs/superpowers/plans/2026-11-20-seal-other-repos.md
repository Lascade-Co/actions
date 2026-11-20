# Seal Other Repos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let any Lascade-Co repo become a **sealed repository** by reusing this repo's toolkit, and make the **log viewer** work for any sealed repo. Seal `Lascade-Co/connector` first and restore it to public.

**Architecture:**
- **Seal action.** Two composite actions in this repo, published on the moving tag `seal-v1`:
  - `actions/seal` runs `scripts/seal/install.sh` from the action's own checkout, with no curl for the scripts. It reads the caller's `.github/seal/repo-key.pub` through the contents API at `github.sha`.
  - `actions/seal-upload` uploads the **sealed log**.
- **Policy.** A reusable workflow, `.github/workflows/seal-policy.yml`, runs the generalised policy test against a caller's checkout.
- **Viewer.** It gains `?repo=`, a home page listing public repos with the `sealed-ci` topic, and one key per repo, each checked against that repo's public key.

**Tech Stack:** bash, age v1.3.2, Python 3.13 + pytest + PyYAML, vanilla ES modules (viewer), GitHub composite actions and reusable workflows.

**Spec:** `docs/adr/0014-keep-the-runner-public-and-seal-private-output.md`, `CONTEXT.md` ("Language — Confidentiality": **sealed repository**, **seal action**, **frozen repository**), and `docs/sealed-output.md`. The decisions below come from the 2026-11-20 grilling session.

## Global Constraints

- **Delivery.**
  - Other repos use `Lascade-Co/actions/actions/seal@seal-v1` as each job's first step and `Lascade-Co/actions/actions/seal-upload@seal-v1` as its last step, with `if: always()`.
  - `seal-v1` is a moving tag. It is moved only by an explicit `git tag -f seal-v1 <sha> && git push -f origin seal-v1`, after the toolkit tests pass on `main`.
  - This repo keeps its existing verbatim setup and upload steps.
- **Public key.** Each sealed repo commits `.github/seal/repo-key.pub`, one `age1…` line. The seal action fetches it with `GET /repos/{repo}/contents/.github/seal/repo-key.pub?ref={sha}`, using the job's `github.token`, so it also works while the repo is private.
- **Marker line.** It becomes `🔒 sealed <artifact>#<seq> https://ci.lascade.com/?repo=<repo-name>&run=<id>&attempt=<n>`. The viewer still accepts the old form without `repo=`, which means `actions`.
- **Discovery.** A sealed repo has the GitHub topic `sealed-ci`. The viewer home lists `search/repositories?q=org:Lascade-Co+topic:sealed-ci+is:public`, plus an "all public repos" toggle.
- **Viewer key storage.**
  - One key per repo under `ci.repoKey.<repo>`; the existing `ci.repoKey` migrates to `ci.repoKey.actions`. There is one shared `ci.githubToken`.
  - A key is accepted only if its recipient equals that repo's `repo-key.pub`.
  - The token is still sent only to `api.github.com`. CSP, the frame guard and Forget keep their current guarantees, and Forget clears every per-repo key.
- **Policy.** Each sealed repo adds `.github/workflows/seal-policy.yml`, which calls `Lascade-Co/actions/.github/workflows/seal-policy.yml@seal-v1` on `push` and `pull_request`, with no secrets. The generalised policy accepts either:
  - this repo's verbatim setup and upload steps, or
  - the composite pair as the first and last steps, with no `with:` beyond what the composites define.
  
  All other rules are unchanged: sealed default shell, `seal-pack`/`seal-unpack`, retention of at most 7 days, no `pull_request_target`, no secrets in `pull_request` workflows, and the exemption rule.
- **Each repo's CLAUDE.md** gains the standard "Sealed output" section from `docs/sealed-repo-claude.md`. It links to `docs/sealed-output.md` in this repo and states that the policy workflow enforces the rules.
- **Never run Actions in a private repo** (user CLAUDE.md). connector stays Actions-disabled until it is public.
- **Purge before public.** All runs from before sealing are purged (logs archived to `~/Downloads/actions-log-archive/<repo>/`) before a repo is made public.

## Review Focus

1. **A repo whose `repo-key.pub` is missing or malformed.** The seal step must fail the job before any run step, with an error naming the file. It must never fall back to another repo's key.
2. **The seal action used before `actions/checkout`, or in a job that never checks out.** The key comes from the API, so neither may matter. Test this.
3. **A token lacking `contents: read`.** Jobs with restrictive `permissions:` must still read the key file. The policy should flag jobs whose `permissions:` omit `contents: read`. Test this.
4. **Old marker URLs without `repo=`.** Sealed logs already in this repo's artifacts must still open in the viewer.
5. **A viewer key pasted for the wrong repo.** It must be rejected with a clear message, never silently used to try decrypting another repo's logs.

---

### Task 1: Seal action (composite) and toolkit generalisation

**Files:**
- Create: `actions/seal/action.yml`, `actions/seal-upload/action.yml`
- Modify: `scripts/seal/install.sh`, `scripts/seal/seal-bash`
- Test: `scripts/seal/test_seal_toolkit.py`

**Interfaces:**
- **`actions/seal`**:
  - inputs `token` (default `${{ github.token }}`) and `repository` (default `${{ github.repository }}`)
  - runs `bash "$GITHUB_ACTION_PATH/../../scripts/seal/install.sh"` with `SEAL_SOURCE_DIR` unset
  - new env `SEAL_TOOLKIT_DIR=$GITHUB_ACTION_PATH/../../scripts/seal`, a production knob (not test-only), from which `install.sh` copies the tools instead of using curl; `SEAL_KEY_REPO`/`SEAL_KEY_REF` choose the key file's repo and commit
- **`actions/seal-upload`**: wraps `actions/upload-artifact@v7` with `name: ${{ env.SEAL_LOG_ARTIFACT }}`, `path: ${{ runner.temp }}/seal/out/`, `retention-days: 7`, `if-no-files-found: ignore`. The caller adds `if: always()` on the `uses:` step, since a composite can't force it.
- **`install.sh`** gains two ways to get the key:
  - default (this repo): fetch `scripts/seal/repo-key.pub` at `SEAL_REF`, as today
  - when `SEAL_KEY_REPO` is set: contents API `repos/$SEAL_KEY_REPO/contents/.github/seal/repo-key.pub?ref=$SEAL_KEY_REF`, `Accept: application/vnd.github.raw`, token from `SEAL_TOKEN`; it must match `^age1[02-9ac-hj-np-z]{58}$` or the job fails
- **`seal-bash`**: the marker and install banner include `repo=${GITHUB_REPOSITORY#*/}`.

- [ ] **Step 1:** Write failing tests:
  - `test_marker_includes_repo` (`GITHUB_REPOSITORY=Lascade-Co/connector` → marker has `?repo=connector&run=…`)
  - `test_install_from_toolkit_dir_without_curl`
  - `test_key_from_caller_repo` (stub the contents API with a local `http.server`)
  - `test_bad_caller_key_fails_before_run_steps`
  - `test_missing_caller_key_names_the_file`
  - a YAML test that `actions/seal*/action.yml` are composite, pass the documented inputs, and use only `shell: bash` in the setup step
- [ ] **Step 2:** Implement. Keep every existing test passing; this repo's setup path is unchanged.
- [ ] **Step 3:** Run `pytest scripts/seal/test_seal_toolkit.py`; all pass. Commit.

### Task 2: Generalised policy and reusable policy workflow

**Files:**
- Modify: `scripts/seal/test_workflow_policy.py` (it takes the repo root from env `SEAL_POLICY_ROOT`, defaulting to this repo; repo-specific constants such as `EXPECTED_EXEMPT` apply only when the root is this repo)
- Create: `.github/workflows/seal-policy.yml` (`workflow_call`; a sealed job using the verbatim setup step at `workflow_sha` only when the caller is this repo, otherwise the composite; it checks out the caller at `github.sha` into `target/` and this repo at the `seal-v1` ref into `control/`, then runs pytest with `SEAL_POLICY_ROOT=target`)
- Create: fixtures in `scripts/seal/testdata/composite/`
- Test: `scripts/seal/test_workflow_policy.py`

- [ ] **Step 1:** Write failing fixture tests:
  - a good workflow using the composite pair passes
  - composite pair missing `if: always()` on the upload fails
  - composite at a ref other than `@seal-v1` fails
  - a job with `permissions:` lacking `contents: read` fails (Review Focus 3)
  - a missing `.github/seal/repo-key.pub` fails, as a repo-level check that runs only when the root isn't this repo
- [ ] **Step 2:** Implement. This repo still passes unchanged (342+ tests).
- [ ] **Step 3:** Commit.

### Task 3: Viewer generalisation

**Files:**
- Modify: `viewer/app.js`, `viewer/lib/github.js`, `viewer/lib/joblog.js`, `viewer/lib/sealed.js`, `viewer/lib/runs.js`, `viewer/index.html`, `viewer/style.css`
- Test: the matching `viewer/lib/*.test.mjs`

**Interfaces:**
- `github.js`: every function takes a `repo` argument (validated `^[A-Za-z0-9._-]+$`, owner fixed to `Lascade-Co`), plus new `listSealedRepos(token)` and `listPublicRepos(token, page)`, and `repoPublicKey(token, repo)`.
- `sealed.js`: new `recipientOf(identity) -> Promise<string>`.
- Routes:
  - `?` → repo picker (sealed repos, with an "all public repos" toggle)
  - `?repo=X` → runs list
  - `?repo=X&run=N` → run view
  - `?run=N` with no repo → `repo=actions`

- [ ] **Step 1:** Write failing tests:
  - repo param validation
  - marker binding needs both repo and run (an old marker with no repo binds to `actions` only)
  - per-repo key storage and migration of `ci.repoKey`
  - `recipientOf` matches `scripts/seal/repo-key.pub` for a test identity
  - a wrong-repo key is rejected
- [ ] **Step 2:** Implement the settings UI as one token field plus a key list per repo (add, replace, forget). The run view asks for the key of the repo being viewed. Forget clears everything.
- [ ] **Step 3:** Run `node --test viewer/lib/*.test.mjs`. Browser smoke check with stubbed responses. Commit.

### Task 4: Docs and CLAUDE.md template

**Files:**
- Modify: `docs/sealed-output.md` (new section "Sealing another repository": the composite pair, the key file, policy caller, topic, purge-before-public, moving `seal-v1`)
- Create: `docs/sealed-repo-claude.md` (the standard CLAUDE.md section, under 15 lines)
- Modify: this repo's `CLAUDE.md` (one bullet on moving `seal-v1` after toolkit changes)

- [ ] **Step 1:** Write the docs. Keep `docs/sealed-output.md` to about one and a half pages.
- [ ] **Step 2:** Commit.

### Task 5: Seal connector (in a connector checkout, branch `seal/sealed-output`)

**Files (connector):**
- Create: `.github/seal/repo-key.pub` (`age1zfj69xfrz6dsak42q4negze2c2382rwg7xjdftr55wpczamfwspqfg38k0`)
- Create: `.github/workflows/seal-policy.yml`
- Modify: every workflow with steps:
  - `_reusable-etl.yml`, `main.yml`, `backfill.yml` (prep job) and `tests.yml`; jobs that only call workflows are untouched
  - composite pair first and last; default shell `seal-bash-e {0}` (`seal-bash {0}` where `shell: bash` was used)
  - the expanded `${{ inputs.pipeline }}` in `run:` text moves to env (public metadata, not masked)
  - Slack notify unchanged (secret)
- Modify: `CLAUDE.md` (standard section), `README.md` (freeze notice replaced by a 3-line "CI output is sealed; open runs at https://ci.lascade.com/?repo=connector" note)

- [ ] **Step 1:** Convert the workflows. Run the generalised policy locally with `SEAL_POLICY_ROOT=<connector checkout> pytest scripts/seal/test_workflow_policy.py` until it's clean. Run connector's own `pytest tests` to check nothing else changed. Commit on the branch; don't push yet.

### Task 6: Rollout (controller, with the user)

- [ ] **Step 1:** Start the connector purge in the background: `purge_runs.py --repo Lascade-Co/connector --archive ~/Downloads/actions-log-archive/connector`. It shares the token's rate limit with the `actions` purge.
- [ ] **Step 2:** Merge Tasks 1–4 to this repo's `main` and push. Wait for `Test sealing` and `Test sealing end to end` to pass, then create the tag: `git tag seal-v1 <main sha> && git push origin seal-v1`.
- [ ] **Step 3:** Unarchive connector. It stays private with Actions disabled. Push the connector branch to `main`, then add the topic: `gh api -X PUT repos/Lascade-Co/connector/topics -f 'names[]=sealed-ci'`.
- [ ] **Step 4:** When the purge reports `done` and `gh api repos/Lascade-Co/connector/actions/runs --jq .total_count` is 0:
  - make connector public
  - enable Actions
  - dispatch `tests.yml` and one ETL workflow (`daily-ga4.yml`)
  - check the public logs contain only markers, and that the viewer at `?repo=connector` decrypts them with the connector key
- [ ] **Step 5:** Report to the user. The other frozen repos follow the same Task 5–6 pattern, one at a time, each after its own grilling about repo-specific workflows.
