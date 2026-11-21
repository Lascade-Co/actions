# Product Actions Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every **product report** out of `Lascade-Co/actions` into a new public **sealed repository**, `Lascade-Co/product-actions`. Fold in `daily-email-digest-actions` and `chat-summary` on one shared plumbing package. Delete Release Blog. Put **reviewed main** and **production environment** secrets on all five sealed repos.

**Architecture:**
- `product-actions` is a fresh repo; nothing is imported with git history.
- Its reports share `scripts/common/` for the gate, hold, Resend delivery, dates, editions, run settings and Telegram. Each report keeps its own fetch, model and render code unchanged.
- Workflows check out the repo at `github.sha` and fetch `scripts/ai/claude_api.py` from `Lascade-Co/actions@main`.
- Repo governance is code: `scripts/seal/repo_settings.py` in actions applies and checks:
  - the ruleset
  - the `production` environment
  - fork approval
  - team access

  The seal policy test gains an environment rule so the setup cannot drift.

**Tech Stack:** Python 3.13, pytest 9.1.1, requests 2.34.2, PyYAML 6.0.3, anthropic 1.13.0 (through `claude_api.py`), google-auth (chat-summary Sheets), GitHub Actions (checkout@v7, upload-artifact@v7, download-artifact@v8, setup-python@v7), GitHub REST (rulesets, environments), age sealing via `Lascade-Co/actions/actions/seal@seal-v1`.

**Spec:**
- `docs/adr/0015-product-reports-move-out-and-sealed-repos-review-main.md`
- `CONTEXT.md`: **Product report**, **Reviewed main**, **Production environment**, plus the Confidentiality terms
- `docs/sealed-output.md`
- Research notes, which are not committed until Task 12's review (`/Users/rohittp/Data/Lascade/product-actions/docs/research/`):
  - `plumbing-inventory.md`: today's gate, hold and delivery copies, with signatures and differences
  - `chat-summary-spec.md`: the behavioural spec for the Python port, with verbatim prompts, subjects and keys
  - `chat-summary-golden/email-golden.json`: byte-exact HTML, text and subject for the cases `mixed`, `empty` and `unprocessedOnly`
  - `secrets-and-removals.md`: every secret-using job and every Release Blog and report reference

Source trees for the two frozen repos are in the scratchpad at `.../scratchpad/split/{daily-email-digest-actions,chat-summary}`. If that is gone, re-download them with `gh api repos/Lascade-Co/<repo>/tarball`.

## Global Constraints

- **Repo and visibility.**
  - The new repo is `Lascade-Co/product-actions`. It is public and sealed, has the topic `sealed-ci`, and its local working dir is `/Users/rohittp/Data/Lascade/product-actions`.
  - It is created private with Actions disabled. It goes public only after Task 12. Actions is enabled only after it is public.
  - Never start a run in a private repo.
- **No history.** One initial commit per source, with the message naming the source repo and SHA: actions `5bc5286`, and the HEADs of daily-email-digest-actions and chat-summary at download.
- **Sealing.** Follow `docs/sealed-output.md` "Sealing another repository" exactly:
  - every job's first step is `Lascade-Co/actions/actions/seal@seal-v1`
  - every job's last step is `Lascade-Co/actions/actions/seal-upload@seal-v1` with `if: always()`
  - the default shell is `seal-bash {0}` or `seal-bash-e {0}`
  - artifacts go through `seal-pack`, with retention ≤ 7 days
  - `.github/seal/repo-key.pub` holds the user-supplied `age1…` key
  - the private half is `REPO_AGE_KEY`, kept in Infisical only and never written to disk or chat
- **GitHub Free plan.** Rulesets and environment secrets work only on public repos. Apply settings and environment syncs to product-actions only after it is public, and before Actions is enabled.
- **Production environment.**
  - Every job that references `secrets.<NAME>` (other than `secrets.GITHUB_TOKEN`) declares `environment: production`, or a name listed in the repo's `.github/seal/environments`, one name per line.
  - Today only actions lists extra names: `cloudflare-prod` and `cloudflare-staging`, which deploy-runner keeps.
  - An `environment:` expression passes only if every string literal in it is an allowed name.
  - Every environment that holds secrets has a deployment branch policy allowing only the `main` branch.
  - Jobs that call a local reusable workflow carry no `environment:`; the called jobs do.
  - Cross-repo reusable calls that pass secrets are listed in `.github/seal/env-exempt`, one line each: `<workflow file>:<job id>  # reason`.
- **Reviewed main ruleset**, named exactly `Reviewed main`, on `~DEFAULT_BRANCH`:
  - rules `deletion` and `non_fast_forward`
  - rule `pull_request` with `required_approving_review_count: 1`, `require_code_owner_review: true`, `dismiss_stale_reviews_on_push: true`, `require_last_push_approval: true`
  - rule `required_status_checks` with the repo's list below, `strict_required_status_checks_policy: false`
  - bypass actor `OrganizationAdmin` (`actor_id: 1`), `bypass_mode: always`
- **Seal tag ruleset, actions only.** A second ruleset, named exactly `Protected seal tags`:
  - target `tag`, include `refs/tags/seal-v*`
  - rules `update`, `deletion` and `creation`
  - bypass actor `OrganizationAdmin` (`actor_id: 1`), `bypass_mode: always`

  Every secret job in every sealed repo runs `@seal-v1` first, so moving that tag must be as guarded as `main`.
- **Code owners.** `.github/CODEOWNERS` contains exactly `* @Lascade-Co/backend`. The backend team has `push` permission on all five repos; the product team has `push` on product-actions only.
- **Required checks.**

  | Repo | Required checks |
  |---|---|
  | actions | `unit`, `test` |
  | connector | `policy / policy`, `unittest` |
  | infra-actions | `policy / policy`, `test` |
  | store-reviews | `policy / policy` |
  | product-actions | `policy / policy`, `test` |

  A required-check workflow runs on every `pull_request` with no `paths:` filter.
- **Fork PRs.** Approval policy `all_external_contributors` on all five repos.
- **Report behaviour.** These rulings settle the cases where the copies differed.
  - Keep each report's send time:
    - catchup and ad spend: gate to 05:45 `America/Los_Angeles`, then send right after the build
    - hunch and install → paid: gate 05:05, hold to 05:55 local
    - digests and chat-summary: gate 05:05, hold 05:55
    - marketing net, ship accuracy and SEO audit: keep their crons
  - No Resend `scheduled_at` anywhere. The hold replaces it.
  - Resend idempotency keys stay verbatim per report, so a switch-day resend dedupes at Resend:
    - `daily-report-<D>`
    - `ad-spend-<V>-<AD>-<sha1(MAIL_TO)[:8]>`
    - `hunch-retention-<D>[-manual-<run>]`
    - `install-paid-<ed>-<D>-scheduled|-manual-<run>`
    - `<digest>/<edition>/<D>`
    - `chat-summary/<SHEET_ID_ENV>/<edition>/<D>`
    - the incomplete-checkout sha256 key
  - Delivery semantics are the digest's:
    - only HTTP 200 counts as accepted
    - 409 `concurrent_idempotent_requests`, 429, 5xx and transport errors retry
    - 409 `invalid_idempotent_request` and any other 4xx fail closed
    - 4 attempts, backoff `min(2**(n-1), 15)` s, timeout 60 s
    - errors never include upstream response text

    Ad spend's "409 = sent" is dropped.
  - Send-step markers:
    - every real send step is named `Deliver <label>[ <PT|IST>] for <YYYY-MM-DD>`
    - previews are `Dry run …` and test sends are `Test send …`
    - reports with several sends per run add an aggregate job named `Delivered <label>[ <PT|IST>] for <D>`
    - the gate dedupes on the exact marker it is given
    - the gate's legacy path (`sent_step=None`, the 16:00 cutoff, job named `email`) is removed

    Labels:

    | Report | Label |
    |---|---|
    | catchup | `catchup` |
    | ad spend | `ad spend <A\|B>`, aggregate `ad spend` |
    | hunch | `hunch retention` |
    | install → paid | `install paid` (with edition) |
    | digests and chat-summary | unchanged from their current names |

  - The manual-run default report date everywhere is the latest date whose 05:00 local cutoff has passed (`dates.report_date_for`). chat-summary's tab is still today in `Asia/Kolkata`.
  - Previews: `dry_run` writes `out/`, `seal-pack` packs it as `preview-<report>-<edition>`, and it is uploaded from `${{ runner.temp }}/seal/art/`. There is no `PREVIEW_KEY` and no openssl.
- **Python layout.**
  - Flat imports.
  - `pytest.ini` sets `pythonpath` to `scripts/common`, `scripts/ai` and every `scripts/<report>` directory.
  - Module and test-file basenames are unique across the repo; rename on collision using the report's prefix (`ic_`, `cs_`).
  - Workflows set `PYTHONPATH` to the same dirs they use.
- **Shared Claude client.**
  - `.github/actions/fetch-ai/action.yml` (a sealed composite, ≤ 10 lines of shell) downloads `scripts/ai/claude_api.py` and `scripts/ai/requirements.txt` from `https://raw.githubusercontent.com/Lascade-Co/actions/main/` with `curl -fsSL --retry 3`.
  - Local dev uses `scripts/fetch-ai.sh`, which does the same.
  - `scripts/ai/` is gitignored in product-actions.
- **Model.** chat-summary uses `claude_api.generate_json` / `generate_text` (default model `claude-sonnet-5-5`, env `CLAUDE_API_KEY`). Gemini and `GEMINI_API_KEY` are removed.
- **Commits.** End each commit message with the session's attribution lines. Never push to `main` of a private repo with Actions enabled.

## Review Focus

1. **Switch-day duplicate.** A report that already went out from actions earlier that day is sent again from product-actions with the same Resend key, and Resend should dedupe it. Tasks 7–11 pin each report's key format to the old string.
2. **Unmerged code reaching a real send.** A `workflow_dispatch` of a branch with `recovery=true` must fail in settings before any secret job. Task 6 has `test_real_send_requires_main`.
3. **DST.** The PT gate and hold on 2026-03-08 and 2026-11-01 should still land at 05:05 and 05:55 Pacific wall-clock. Task 6 has `test_plan_dst_spring` and `test_plan_dst_fall`.
4. **A late start.** A run that starts after the hold target should send at once with the original report date, never skip. Task 6 has `test_hold_late_returns_immediately`.
5. **Malformed Claude output.** For chat-summary:
   - missing, duplicate or invalid enum cells → those rows count as unprocessed and the email still builds
   - a `ClaudeError` while translating → that chunk's rows are marked `Translation Error`
   - a `ClaudeError` while summarising → the build aborts

   Task 11 has the tests.

---

## Phase A — Lascade-Co/actions (branch `product-split`, in a worktree)

### Task 1: Seal policy environment rule

**Files:**
- Modify: `scripts/seal/test_workflow_policy.py`
- Create: `scripts/seal/testdata/env/` (fixture workflows)
- Modify: `docs/sealed-output.md` (new "Production environment" section)

**Interfaces:**
- Produces:
  - Rule `secret_jobs_use_production`: a job with `steps:` whose YAML text matches `secrets\.(?!GITHUB_TOKEN\b)[A-Za-z_]` must have `environment:` set to an allowed name. That is `production` or a line of `.github/seal/environments`, given as a string, as `name:` of a mapping, or as an expression whose string literals are all allowed.
  - Exempt from the rule:
    - jobs with `uses: ./.github/workflows/…` (the called workflow is checked instead)
    - jobs listed in `.github/seal/env-exempt`
    - workflows in `EXPECTED_EXEMPT`
  - A cross-repo `uses:` job that passes `secrets:` and is not listed fails.
  - `env-exempt` format: `<file>:<job>  # <reason>`. A line without a reason fails.

- [ ] **Step 1: Write failing tests**
  - `test_secret_job_without_environment_fails` uses a fixture job with `${{ secrets.FOO }}` and no environment, and asserts the violation names the file and job.
  - `test_github_token_only_needs_no_environment`.
  - `test_environment_mapping_name_production_passes`.
  - `test_local_reusable_caller_is_exempt_and_callee_checked`: the caller has `secrets: inherit`, the callee job lacks an environment, and the violation is on the callee.
  - `test_cross_repo_call_with_secrets_needs_exempt_entry`.
  - `test_env_exempt_line_without_reason_fails`.
  - `test_other_environment_name_fails`: `environment: cloudflare-prod` on a secret job is a violation unless `.github/seal/environments` lists it.
  - `test_listed_environment_expression_passes`: `${{ inputs.env_slug == 'prod' && 'cloudflare-prod' || 'cloudflare-staging' }}` with both names listed passes; listing only one fails.
- [ ] **Step 2: Run** `python -m pytest -q scripts/seal/test_workflow_policy.py -k environment`. Expected: FAIL.
- [ ] **Step 3: Implement** the rule next to `SEALED_RULES` and run it for every non-exempt workflow and for `.github/actions` composites. Composites never reference secrets, so they are skipped. Read `.github/seal/env-exempt` from `SEAL_POLICY_ROOT`.
- [ ] **Step 4: Run** the full policy test. Expected: the new rule FAILS on this repo's real workflows. That is expected and Task 3 fixes it. The fixture tests PASS.
- [ ] **Step 5: Commit** `feat(seal): policy requires the production environment for secret jobs`.

### Task 2: Repo settings tool

**Files:**
- Create: `scripts/seal/repo_settings.py`, `scripts/seal/test_repo_settings.py`, `scripts/seal/sealed_repos.json`

**Interfaces:**
- `sealed_repos.json`:
  - `{"<repo>": {"required_checks": [...], "teams": {"backend": "push", "product": "push"?}, "secret_exempt": [...]}}`
  - It covers the five repos, with the required checks from Global Constraints.
  - `teams.product` applies only to product-actions.
  - store-reviews has `secret_exempt: ["CI_APP_CLIENT_ID", "CI_APP_PRIVATE_KEY"]`.
- `ruleset_payload(required_checks: list[str]) -> dict` returns exactly the Global Constraints ruleset.
- `tag_ruleset_payload(patterns: list[str]) -> dict` returns the `Protected seal tags` ruleset. `sealed_repos.json` gives actions `"protect_tags": ["refs/tags/seal-v*"]`.
- `environment_calls(repo) -> list[tuple[str, str, dict|None]]` returns the method, path and body for:
  - `PUT /repos/Lascade-Co/{repo}/environments/production` with `{"deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}`
  - `POST .../environments/production/deployment-branch-policies` with `{"name": "main", "type": "branch"}`

  The same branch policy goes on every other existing environment that holds secrets.
- `plan(repo, current: dict) -> list[call]`: the idempotent diff. The ruleset is created or updated by name, the environment policy is added only if missing, `PUT /repos/{repo}/actions/permissions/fork-pr-contributor-approval {"approval_policy": "all_external_contributors"}` is applied, and team access goes through `PUT /orgs/Lascade-Co/teams/{team}/repos/Lascade-Co/{repo}`.
- `check(repo, current) -> list[str]` returns problems, including repo-level Actions secrets not in `secret_exempt` and a missing or wrong `.github/CODEOWNERS`.
- CLI: `python3 scripts/seal/repo_settings.py {check|apply} <repo>...`. It reads the current state with `gh api`. `apply` prints each call before making it, and `--dry-run` only prints.
- `check <repo> --offline <path>` checks only the local files (`.github/CODEOWNERS`, `.github/seal/env-exempt` format, `.github/seal/repo-key.pub` is one `age1…` line) and makes no API call.

- [ ] **Step 1: Failing tests:**
  - `test_ruleset_payload_exact`: compares against a literal dict built from the Global Constraints values.
  - `test_tag_ruleset_only_for_actions`.
  - `test_plan_is_empty_when_current_matches`.
  - `test_plan_updates_ruleset_by_name`.
  - `test_check_flags_repo_secret_not_exempt`.
  - `test_check_flags_missing_codeowners`.
  - `test_product_team_only_on_product_actions`.
- [ ] **Step 2: Run** `python -m pytest -q scripts/seal/test_repo_settings.py`. Expected: FAIL.
- [ ] **Step 3: Implement.** The functions are pure, and only the CLI calls `gh api`.
- [ ] **Step 4: Run** it. Expected: PASS. Then run `python3 scripts/seal/repo_settings.py check actions connector infra-actions store-reviews` (read-only) and save the output for Task 15.
- [ ] **Step 5: Commit** `feat(seal): repo settings tool for reviewed main and production environment`.

### Task 3: actions jobs on the production environment

**Files:**
- Modify: every staying workflow in `secrets-and-removals.md` § actions
- Modify: `test-seal.yml`, `test-ai.yml`, `test-seal-e2e.yml`, `deploy-runner.yml`
- Create: `.github/CODEOWNERS`

- [ ] **Step 1:** Add `environment: production` to every job listed as using secrets, across the repository_dispatch workflows and asc-build-status. Also cover the report workflows that are still here until Task 14 (daily-*, install-paid-watchdog, ship-accuracy-report, seo-blog-audit), so the policy passes on this branch.
- [ ] **Step 2: deploy-runner.**
  - deploy-runner keeps its `cloudflare-prod`/`cloudflare-staging` expression.
  - Create `.github/seal/environments` listing `cloudflare-prod` and `cloudflare-staging`.
  - Its repo-level secrets (`CI_APP_*`, `DIGITAL_OCEAN_SSH_KEY`, `INFISICAL_*`) get synced into both of those environments as well (user step in Task 15).
- [ ] **Step 3: test-seal-e2e.yml.** Set `on: push: branches: [main]` and keep `paths:`. Add `environment: production` on `e2e-verify`.
- [ ] **Step 4:** Remove the `paths:` filters from `test-seal.yml` and `test-ai.yml`, so they can be required checks.
- [ ] **Step 5:** Create `.github/CODEOWNERS` with `* @Lascade-Co/backend`.
- [ ] **Step 6: Run** `python -m pytest -q scripts/seal`. Expected: PASS, including Task 1's rule on the real workflows.
- [ ] **Step 7: Commit** `ci: secret jobs use the main-only production environment`.

### Task 4: Delete Release Blog

**Files:** every path in `secrets-and-removals.md` § "Release Blog removal".

- [ ] **Step 1:** Delete the listed files. In `android-build-release.yml`, remove the `blog` job and its comment, and the `release` job outputs `tag` and `base`. Keep the `notes` step.
- [ ] **Step 2:** In `test-ai.yml`, narrow `data/RELEASE_*.md` to `data/RELEASE_NOTES.md` and drop the `release-blog.yml` path. The pytest line is unchanged here; deleting `test_release_blog.py` removes those tests, and Task 14 narrows the line.
- [ ] **Step 3:**
  - CONTEXT.md: delete the Release Blog sections and the "backlink" ambiguity, and reword the D9/placeholder lines so they no longer name Release Blog.
  - Add `Superseded by ADR 0015: Release Blog was deleted.` under the titles of ADR 0011 and 0012.
  - Fix `docs/sealed-output.md:71` and the `docs/claude-ai.md` rows.
- [ ] **Step 4: Run** `grep -rn -i 'release.blog\|release_blog' --exclude-dir=.git . | grep -v 'docs/adr/001[12]\|docs/adr/0015'`. Expected: no output. Then run `python -m pytest -q scripts/seal scripts/ai scripts/android scripts/seo`. Expected: PASS.
- [ ] **Step 5: Commit** `chore: delete Release Blog`.

## Phase B — Lascade-Co/product-actions (local dir; GitHub repo private, Actions disabled)

### Task 5: Scaffold

**Files:**
- Create in `/Users/rohittp/Data/Lascade/product-actions`:
  - `README.md`, `CLAUDE.md`, `CONTEXT.md`
  - `.github/CODEOWNERS`, `.github/seal/repo-key.pub`, `.github/seal/env-exempt` (empty, with a header comment)
  - `.github/workflows/seal-policy.yml`, `.github/workflows/test.yml`
  - `.github/actions/fetch-ai/action.yml`
  - `scripts/fetch-ai.sh`, `pytest.ini`, `requirements.txt`, `.gitignore`
  - `docs/adr/`

- [ ] **Step 1: `git init -b main`.** `.gitignore` covers `scripts/ai/`, `out/`, `.cache/`, `*.local.json`, `__pycache__/`, `.env` and `docs/research/` (research stays local until Task 12).
- [ ] **Step 2: Workflows.**
  - `seal-policy.yml` is verbatim from `docs/sealed-output.md` step 5.
  - `test.yml` triggers on `push` and `pull_request` with no paths and no secrets. It has one job named `test`: seal, checkout@v7, setup-python@v7 3.13, fetch-ai, `pip install -r requirements.txt pytest==9.1.1`, `python -m pytest -q`, then seal-upload.
- [ ] **Step 3: `CLAUDE.md`** holds:
  - the "Sealed output" section from `docs/sealed-repo-claude.md`
  - the production environment rule
  - the layout and import rules from Global Constraints
  - "How the product team changes a report": branch → PR → backend review → merge → `workflow_dispatch` with `dry_run=true`, then open the preview at ci.lascade.com
- [ ] **Step 4: `CONTEXT.md`** gets the moved sections from actions: Daily Catchup, Blog SEO Audit, Marketing Net, Settled decisions — Install → paid, and their example dialogues and flagged ambiguities. Add sections written from the two frozen READMEs: **Digest**, **Edition**, **Report date**, **Hold**, **Delivery marker**, **Chat summary**.
- [ ] **Step 5:** Copy ADRs 0001 and 0003–0007 into `docs/adr/` under their original numbers, plus a copy of ADR 0015 for context.
- [ ] **Step 6: `repo-key.pub`.** Ask the user for the `age1…` public key; that is their key step. Until then commit a placeholder only in the local repo, and replace it before Task 12.
- [ ] **Step 7: Shared files for the parallel tasks.** Tasks 7–11 run in parallel and must not edit `pytest.ini`, `requirements.txt`, `test.yml` or `CONTEXT.md`. Declare them complete here:
  - `pythonpath` lists every report dir: `scripts/common`, `scripts/ai`, `scripts/{catchup,adspend,pnl,hunch,installpaid,accuracy,seo,container_credits,conversion,incomplete_checkout,revenue,reviews,survey,ta_trigger,chat_summary}`.
  - `requirements.txt` is the union of the actions reports' installs, the digest's requirements and google-auth, all pinned.
  - Each of Tasks 7–11 works in its own worktree off this commit. The controller merges them in order and resolves any conflict.
- [ ] **Step 8: Commit** `chore: scaffold product-actions`.

### Task 6: `scripts/common`

**Files:**
- Create in `scripts/common/`: `gate.py`, `hold.py`, `dates.py`, `editions.py`, `delivery.py`, `workflow.py`, `notify.py`, and a `test_*.py` per module.

**Interfaces** (other tasks consume these exact names):
- **`gate.py`:**
  - `now_utc()`, `parse_zone(v)`, `parse_target(v) -> (h, m)`
  - `target_for(report_date, tz, target) -> datetime`
  - `plan(now, tz, target) -> (report_date, wait_s, proceed)`
  - `phase2_wait(report_date, now, tz, target) -> int`
  - `already_sent(report_date, now, *, workflow: str, sent_step: str, repo: str | None = None, fetch=gh_json) -> bool`, raising `DedupeUnavailable`
  - `emit(**kv)`, writing to stdout and `GITHUB_OUTPUT`
  - CLI `--phase 1|2 --event --tz --target --workflow --sent-step` (all required) and `--report-date --remaining` (optional)
  - `repo` defaults to `$GITHUB_REPOSITORY`
  - phase 1 honours `--report-date`; a manual event skips dedupe and sleep; a schedule event fails open on `DedupeUnavailable`
  - no job-name filter; no legacy path
- **`hold.py`:** `hold_until(report_date: date, tz: str, target: str = "05:55") -> None` prints `hold_seconds=N` and never raises when late. CLI `--report-date --tz --target`.
- **`dates.py`:** the digest's `report_date_for(now, tz)`, `window_for(report_date, tz)`, `subject(name, report_date, tail)`, `long_date`, `DAYS`, `MONTHS`, `SHORT_MONTHS`, unchanged.
- **`editions.py`:**
  - `Edition(name, timezone)` with `.zone`, `.label` (`"PT"`/`"IST"`) and `.at(day, h, m)`
  - `EDITIONS = {"pt": Edition("pt", "America/Los_Angeles"), "ist": Edition("ist", "Asia/Kolkata")}`
  - `get_edition(name)`
- **`delivery.py`:**
  - `DeliveryError`, `load_config(path=None)` (reads `data/email_config.json`), `mask(value)`
  - `write_preview(name, edition, subject, html, text=None, out_dir="out") -> list[Path]`
  - `freeze_envelope(path, key, payload)`: O_EXCL, mode 0600, compact JSON preserving key order
  - `send_frozen(path, api_key, *, max_attempts=4, session=None, sleep_fn=time.sleep) -> {"accepted", "status", "attempts"}`
  - `send_digest(...)`: the digest's config wrapper, unchanged
- **`workflow.py`:**
  - `SettingsError`
  - `run_mode(event, recovery, to, dry_run) -> "real"|"test"|"dry"`
  - `guard_real_run(env, report_date, latest_report_date, now)`, which raises on any of:
    - `GITHUB_RUN_ATTEMPT != "1"`
    - a manual real run without `recovery=true`, or with a `GITHUB_REF` other than `refs/heads/main`
    - a future day
    - a run created more than 36 h ago
  - `step_name(label, edition_label, report_date, mode) -> str`
  - `dedupe(workflow, sent_step, report_date, event) -> bool`, using gate semantics
  - the digest's `WORKFLOWS`, `settings`, `digest_args` and `resolve_schedule`
- **`notify.py`:**
  - `redact(text, token)`, `escape(text)`
  - `telegram_send(token, chat_id, text, *, parse_mode=None, post=None, sleep=None)`
  - `telegram_send_document(token, chat_id, filename, data: bytes, caption=None, ...)`
  - `telegram_send_photo`, `telegram_send_media_group`
  - 3 attempts, backoff `2**attempt`, timeout 30, HTTP 200 only, token redacted from errors
  - taken from `pnl_telegram.py`, whose `render` stays in pnl

- [ ] **Step 1: Copy the sources.** The digest's `scripts/common/*` and `tests/test_{dates,delivery,hold,gate_dedupe,workflow}.py` are the base, plus actions' `scripts/catchup/test_catchup_gate.py` (adapted to required `--target`/`--workflow`/`--sent-step`) and `scripts/pnl/test_pnl_telegram.py` (send parts only). Convert imports to flat ones.
- [ ] **Step 2: New failing tests:**
  - `test_plan_dst_spring`: now `2026-03-08T12:00Z`, tz PT, target (5,5) → `target_for` equals `2026-03-08 05:05-07:00`.
  - `test_plan_dst_fall`: `2026-11-01`, expected `05:05-08:00`.
  - `test_hold_late_returns_immediately`: now is past the target, no sleep is called, and it prints `hold_seconds=0`.
  - `test_real_send_requires_main`: `run_mode("workflow_dispatch", True, "", False) == "real"` and `guard_real_run` with `GITHUB_REF=refs/heads/x` raises `SettingsError`.
  - `test_already_sent_requires_sent_step`: calling with an empty `sent_step` raises `ValueError`.
  - `test_send_frozen_409_invalid_fails_closed` and `test_send_frozen_error_has_no_upstream_text`: the upstream body `"secret detail"` must not appear in the `DeliveryError` message.
  - `test_telegram_send_document_redacts_token`.
- [ ] **Step 3: Run** `python -m pytest -q scripts/common`. Expected: FAIL on the new tests only.
- [ ] **Step 4: Implement.** Unify on the rulings in Global Constraints and delete `hint`.
- [ ] **Step 5: Run.** Expected: PASS.
- [ ] **Step 6: Commit** `feat(common): shared report plumbing`.

Tasks 7–11 are independent once Task 6 is done, and run as parallel subagents. Each one:
- copies its sources (initial-copy commit)
- rewires them to `scripts/common`
- rewrites its workflows to the repo pattern
- pins its Resend key and send-step names in tests
- runs its own tests plus the policy test

The **workflow pattern** for every job:
- the seal step first
- `actions/checkout@v7` at `github.sha`, sparse to the dirs it needs
- the `./.github/actions/fetch-ai` step where Claude is used
- `environment: production` if the job references any secret
- seal-upload last

Gate jobs use `scripts/common/gate.py` and need no secrets. Dry runs upload sealed previews.

### Task 7: Catchup, ad spend, marketing net

**Files:**
- Copy from actions: `scripts/catchup`, `scripts/adspend`, `scripts/pnl`
- Copy data: `data/{CATCHUP_CLASSIFY,CATCHUP_REPORT,CATCHUP_SUMMARY}.md`, `catchup_exclude.txt`, `catchup_icons.json`, `catchup_people.json`, `adspend_apps.json`
- Copy workflows: `daily-catchup.yml`, `daily-ad-spend.yml`, `daily-marketing-net.yml`, `test-catchup.yml` (folded into `test.yml`; delete it)
- Delete: `scripts/catchup/catchup_gate.py`, `scripts/adspend/send_resend.sh`
- Create: `scripts/catchup/catchup_send.py`, `scripts/adspend/adspend_send.py`

**Interfaces:**
- **catchup:** `catchup_send.main(["--report-date", D, "--html", path, "--subject", s, "--to"?])` builds the payload, takes the key `daily-report-<D>` (test: `daily-report-test-<run_id>-<attempt>`), calls `freeze_envelope`, then `send_frozen`. Its send step is `Deliver catchup for <D>`.
- **ad spend:** `adspend_send.main(["--variant", "A|B", "--ad-date", AD, ...])` uses the key `ad-spend-<V>-<AD>-<sha1(MAIL_TO)[:8]>`. Its send steps are `Deliver ad spend <V> for <D>`, and the aggregate job is `Delivered ad spend for <D>`.
- **marketing net:** imports `notify` in place of the send functions of `pnl_telegram`.
- `catchup_collect` still commits to `Lascade-Co/catchup` through the CI App token (`CI_APP_CLIENT_ID`/`CI_APP_PRIVATE_KEY`, `environment: production`).

- [ ] **Step 1: Failing tests:**
  - `test_catchup_key_format` expects `daily-report-2026-11-21`.
  - `test_adspend_key_format` expects `ad-spend-A-2026-11-20-` followed by `hashlib.sha1(b"x@y").hexdigest()[:8]`.
  - `test_catchup_sent_step_name` and `test_adspend_sent_step_names`.
- [ ] **Step 2:** Implement and rewire. The gates pass `--tz America/Los_Angeles --target 05:45`.
- [ ] **Step 3: Run** `python -m pytest -q scripts/catchup scripts/adspend scripts/pnl scripts/common`, then the policy test with `SEAL_POLICY_ROOT=$PWD SEAL_POLICY_THIS_REPO=Lascade-Co/product-actions python -m pytest -q <actions>/scripts/seal/test_workflow_policy.py`. Expected: PASS.
- [ ] **Step 4: Commit** `feat: catchup, ad spend and marketing net on shared plumbing`.

### Task 8: TreasureHunch retention, install → paid, watchdog

**Files:**
- Copy: `scripts/hunch`, `scripts/installpaid`, `data/installpaid_google_ipranges.json`
- Copy workflows: `daily-hunch-retention.yml`, `daily-install-paid.yml`, `install-paid-watchdog.yml`
- Delete: `send_resend_hr.sh`, `send_resend_ip.sh`, and the `scheduled_at`/deadline code in `hr_send.py` and `ip_send.py`, which become thin wrappers over `delivery`

**Interfaces:**
- Keys stay `hunch-retention-<D>[-manual-<run>]` and `install-paid-<ed>-<D>-scheduled|-manual-<run>`.
- Steps:
  - hunch: `Deliver hunch retention for <D>`
  - install-paid: `Deliver install paid <PT|IST> for <D>`, forced runs `Forced send install paid <ED> for <D>`
  - watchdog: `Prep alert install paid PT for <D>` / `Alert install paid PT for <D>`
- `ip_watchdog` reads the new step names.
- Each `notify` job uses `notify.telegram_send`.
- The hold step is `python3 scripts/common/hold.py --report-date D --tz <tz> --target 05:55`.

- [ ] **Step 1: Failing tests:**
  - `test_hunch_key_scheduled_and_manual` and `test_ip_key_formats`
  - `test_no_scheduled_at_in_payload` (asserts the payload has no `"scheduled_at"`)
  - `test_watchdog_matches_new_step_names`
- [ ] **Step 2:** Implement. Delete the tests that pin the removed deadline behaviour (`test_ip_deadline.py` and the `scheduled_at` cases), and record each deletion in the commit body.
- [ ] **Step 3: Run** these suites and the policy test. Expected: PASS.
- [ ] **Step 4: Commit** `feat: hunch and install-to-paid on shared plumbing`.

### Task 9: Ship accuracy and blog SEO audit

**Files:**
- Copy: `scripts/accuracy`, `scripts/seo` (minus `release_blog*`), `data/seo_sites.json`
- Copy workflows: `ship-accuracy-report.yml`, `seo-blog-audit.yml`

Their scripts are now read from the checkout at `github.sha`, not from raw `main`. Inline `sendDocument` curls become `notify.telegram_send_document`.

- [ ] **Step 1: Failing test:** `test_accuracy_uses_notify` monkeypatches `notify.telegram_send_document` and asserts one call with a `.html` or `.csv` filename.
- [ ] **Step 2:** Implement.
- [ ] **Step 3: Run** both suites and the policy test. Expected: PASS.
- [ ] **Step 4: Commit** `feat: ship accuracy and SEO audit in product-actions`.

### Task 10: Digests

**Files:**
- Copy from daily-email-digest-actions:
  - `scripts/{container_credits,conversion,incomplete_checkout,revenue,reviews,survey,ta_trigger}`
  - `scripts/common/email_shell.py` → `scripts/common/`
  - `data/*`
  - `tests/*`, placed next to their modules
  - workflows `_digest.yml`, `daily-{container-credits,conversion,incomplete-checkout,revenue,reviews,survey}.yml`, `live-translation-check.yml`, `ta-digest-trigger.yml`
- Rename: `incomplete_checkout/{fetch,main,render}.py` → `ic_fetch.py`, `ic_main.py`, `ic_render.py`

**Interfaces:**
- Keys and step names are unchanged: `<digest>/<edition>/<D>` and `Deliver <digest> <ED> for <D>`, aggregate `Delivered <ED> for <D>`, TA `Delivered TA <ED> for <D>`.
- `python -m scripts.x.y` calls become `python3 scripts/<x>/<y>.py` with `PYTHONPATH`.
- The `PREVIEW_KEY` openssl steps become seal-pack.
- `_digest.yml`'s job gets `environment: production`. Callers keep `secrets: inherit`.

- [ ] **Step 1: Failing tests:**
  - `test_digest_preview_is_sealed_pack`: parse `_digest.yml`; it contains no `openssl` and no `PREVIEW_KEY`, and uploads from `${{ runner.temp }}/seal/art/`.
  - `test_digest_job_has_production_environment`.
- [ ] **Step 2:** Implement. The digest's `test_workflow_contract.py` and `test_shell_sync.py` are updated for the new paths.
- [ ] **Step 3: Run** `python -m pytest -q scripts` and the policy test. Expected: PASS.
- [ ] **Step 4: Commit** `feat: daily digests in product-actions`.

### Task 11: chat-summary in Python on Claude

**Files:**
- Create in `scripts/chat_summary/`: `cs_sheets.py`, `cs_translate.py`, `cs_summarize.py`, `cs_render.py`, `cs_pipeline.py`, `cs_main.py`, `cs_editions.py` (from `scripts/gate/editions.py`)
- Create: `scripts/chat_summary/apps_script/{travel-animator,marine-radar}/extract.gs` (reference copies)
- Create: `scripts/chat_summary/products.json` (from `products/*/config.js`)
- Create: tests `test_cs_*.py` and `fixtures/email-golden.json` (copied from research)
- Create: `.github/workflows/chat-summary.yml`
- Not copied: `scripts/gate/catchup_gate.py`

**Interfaces:**
- **`cs_sheets`:**
  - `SheetsClient.from_env()` reads `GOOGLE_SERVICE_ACCOUNT_JSON` through google-auth plus REST with `requests`
  - `read_tab(sheet_id, tab) -> list[list[str]]`
  - `write(sheet_id, range_a1, values)`
  - `tab_name_for(now) -> str`, which is always today in `Asia/Kolkata`
- **`cs_translate`:**
  - `translate_rows(rows, *, generate=claude_api.generate_json) -> TranslateResult`
  - chunks of at most 20,000 characters, each chat cut to 5,000
  - `max_tokens=32000`
  - a `ClaudeError` marks that chunk's rows `Translation Error`
- **`cs_summarize`:**
  - `summarize(rows, product, *, generate=claude_api.generate_json) -> SummaryResult`
  - one call for all pending rows
  - an omitted, duplicate or invalid-enum cell leaves that row unprocessed
  - a `ClaudeError` raises
- **`cs_render`:** `render(args) -> (subject, html, text)`, byte-identical to the golden fixture.
- **`cs_main`:**
  - `build|send` per product and edition
  - keys `chat-summary/<SHEET_ID_ENV>/<edition>/<D>`, test `chat-summary/test/<SHEET_ID_ENV>/<edition>/<D>/<run_id>`
  - step `Deliver <product> <ED> for <D>`, aggregate `Delivered <ED> for <D>`
- Prompts are reused verbatim from `chat-summary-spec.md` and asked for JSON output.

- [ ] **Step 1: Failing tests.** Port the 103 JS tests listed in the spec to pytest:
  - **rendering and template components:** compare bytes with the golden for `mixed`, `empty` and `unprocessedOnly`
  - **delivery:** this is now the `delivery` behaviour; only the key format tests stay here
  - **pipeline:** includes `… — 1 chats` and the `Sept` short month
  - **gate and workflow:** structural YAML assertions in place of the JS expression evaluator
  - **diagnostics, subject**
  - **gemini** becomes `test_cs_claude_wrapper`, which monkeypatches `generate_json`
  - **New tests from Review Focus 5:**
    - `test_summarize_missing_cell_counts_unprocessed`
    - `test_summarize_invalid_enum_counts_unprocessed`
    - `test_translate_claude_error_marks_chunk`
    - `test_summarize_claude_error_aborts_build`
- [ ] **Step 2: Run** `python -m pytest -q scripts/chat_summary`. Expected: FAIL.
- [ ] **Step 3:** Implement. `chat-summary.yml` keeps the IST cron `9 13,16,19 * * *`, has no PT cron (cron-job.org dispatches PT), and keeps the jobs gate, gate2, email (matrix product × edition), done and keepalive.
- [ ] **Step 4: Run** it, then the full suite and the policy test. Expected: PASS.
- [ ] **Step 5: Commit** `feat: chat-summary ported to Python on Claude`.

### Task 12: Public-readiness review

- [ ] **Step 1:** Scan the whole tree, excluding `docs/research/`, for private values. Public metadata such as work emails and app names may stay. Patterns:
  - `act_[0-9]`, `[0-9]{3}-[0-9]{3}-[0-9]{4}` (Google Ads IDs), `sk_live`, `rk_`, `AKIA`, `AIza`, `-----BEGIN`
  - 44-character Google Sheet IDs, Stripe account IDs, phone numbers, non-`lascade.com` emails

  Every hit is fixed or written down as public metadata.
- [ ] **Step 2:** Decide on `docs/research/`. Keep `chat-summary-spec.md` only if Step 1 is clean for it; otherwise delete the folder. Remove the `.gitignore` line accordingly.
- [ ] **Step 3: Run:**
  - `python -m pytest -q` (all) and the policy test
  - `python3 <actions>/scripts/seal/repo_settings.py check product-actions --offline /Users/rohittp/Data/Lascade/product-actions`

  Expected: PASS. Confirm `repo-key.pub` is the user's real key.
- [ ] **Step 4: Commit** `chore: public-readiness review`.

## Phase C — Other repos, cutover, teardown (controller with the user)

### Task 13: connector, infra-actions, store-reviews

- [ ] **Step 1:** In each repo's local clone (scratchpad), add `environment: production`:
  - connector: `_reusable-etl.yml:etl`, `google-backfill.yml:backfill`, `main.yml:run-pipeline`
  - infra-actions: `apply`, `check`, `drift`
  - store-reviews: `reply-review.yml:reply`, `review-sync.yml:sync` and `commit-state`
- [ ] **Step 2:** In store-reviews, add `.github/seal/env-exempt` with `dashboard-deploy.yml:deploy  # cross-repo static-pages trigger; CI_APP_* stay repo-level`. Add `.github/CODEOWNERS` to all three.
- [ ] **Step 3:** Run each repo's policy against the Task 1 policy (`SEAL_POLICY_ROOT=<clone>`). Expected: PASS. Commit `ci: secret jobs use the main-only production environment`.
- [ ] **Step 4:** Push. These repos are public, so the policy runs are free. Declaring a new environment is harmless while the secrets are still repo-level. Then run `repo_settings.py apply` for the three repos; the user already asked for these settings in chat.

### Task 14: actions — remove the moved reports

- [ ] **Step 1:** Delete everything in `secrets-and-removals.md` § "Report move references", except `data/tg_user.json`. That covers workflows, scripts, data, ADRs 0001 and 0003–0007, and the CONTEXT sections.
  - Set CONTEXT.md line 4 to "Two have enough domain language to need it: **iOS Builds** and **Confidentiality**."
  - Add a one-line pointer: "Product reports live in Lascade-Co/product-actions (ADR 0015)."
- [ ] **Step 2:**
  - `test-ai.yml`: pytest `scripts/ai scripts/android/test_release_notes.py`; drop the beautifulsoup4, lxml, pyjwt and cryptography pins if the remaining tests pass without them.
  - Fix the `docs/claude-ai.md` pytest line, the CLAUDE.md:9 example (`scripts/ios/`) and the `.gitignore` report lines.
- [ ] **Step 3: Run** `python -m pytest -q scripts` and `grep -rln 'scripts/\(catchup\|adspend\|hunch\|installpaid\|pnl\|accuracy\)' --exclude-dir=.git .`. Expected: PASS, and grep finds only `docs/superpowers` history.
- [ ] **Step 4: Commit** `chore: product reports moved to Lascade-Co/product-actions`. Do not merge yet; Task 15 merges it at cutover.

### Task 15: Go-live runbook

Steps marked **(user)** are the user's own. All others are the controller's, and each external action is announced before it runs.

- [ ] **(user)** Generate the product-actions key pair and give the `age1…` public key. Create the Infisical project `product-actions` (env prod). Its secrets:
  - from actions: `RESEND_API_KEY`, `CLAUDE_API_KEY`, `CI_APP_CLIENT_ID`, `CI_APP_PRIVATE_KEY`, `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_DOMAIN`, `INSTALLPAID_TELEGRAM_TOKEN`, `INSTALLPAID_TELEGRAM_CHAT`, `INSTALLPAID_BENCHMARKS_JSON_B64`, `SUBSCRIPTION_TELEGRAM_TOKEN`, `POSTHOG_QUERY_API_KEY`, `HUNCH_POSTHOG_QUERY_API_KEY`, `DATALASTIC_API_KEY`
  - for the SEO audit: its Telegram secrets
  - from the digest repo: `ADAPTY_KEY_GEOANIMATOR`, `ADAPTY_KEY_MARINERADAR`, `ADAPTY_KEY_SMARTRULER`, `ASC_ISSUER_ID`, `ASC_KEY_ID`, `ASC_PRIVATE_KEY`, `GOOGLE_API_KEY`, `GOOGLE_SA_KEY`, `MAIL_FROM`, `POSTHOG_GEO_API_KEY`, `STRIPE_API_KEY`, `TA_DIGEST_DISPATCH_TOKEN`
  - from chat-summary: `GOOGLE_SERVICE_ACCOUNT_JSON`, `MAIL_TO`, `MAIL_TO_PT`, `TRAVEL_ANIMATOR_SHEET_ID`, `MARINE_RADAR_SHEET_ID`
  - `REPO_AGE_KEY`

  The final list comes from `grep -ho 'secrets\.[A-Z_0-9]*' .github/workflows/*.yml | sort -u` in product-actions, minus `GITHUB_TOKEN`. Sync it to the GitHub **environment** `production` of Lascade-Co/product-actions.
- [ ] Create `Lascade-Co/product-actions`: private, disable Actions (`gh api -X PUT repos/Lascade-Co/product-actions/actions/permissions -F enabled=false`), add topic `sealed-ci`, push `main`.
- [ ] **(user)** Make product-actions public, which keeps Actions disabled.
- [ ] Run `repo_settings.py apply product-actions`. Because of the Free plan, it only works once the repo is public.
- [ ] **(user)** Sync the product-actions Infisical project to the `production` environment. Then run `repo_settings.py check product-actions` and confirm the secret names exist in the environment (`gh api repos/Lascade-Co/product-actions/environments/production/secrets`).
- [ ] Enable Actions. Check that `policy / policy` and `test` pass on `main`.
- [ ] Merge the actions branch (Tasks 1–4). The org admin bypasses review. Merge order for the other repos:
  1. Jobs now declare environments while the secrets are still repo-level; repo-level secrets stay visible to those jobs.
  2. **(user)** Add a second Infisical sync for each repo, alongside the repo-level one: actions → `production`, `cloudflare-prod` and `cloudflare-staging` (with `CF_ACCESS_*`), and connector, infra-actions, store-reviews → `production`.
  3. Confirm one scheduled or dispatched run per repo succeeds.
  4. Only then remove the repo-level syncs and secrets. Store-reviews keeps the CI_APP pair repo-level.
- [ ] Run `repo_settings.py apply actions`, which includes the `Protected seal tags` ruleset.
- [ ] After `unit` and `test` pass on main, move the tag: `git tag -f seal-v1 <main sha> && git push -f origin seal-v1` (admin bypass). The env rule now applies everywhere, and Task 13 already complies.
- [ ] At the cutover, merge the Task 14 branch (report removal).
- [ ] Dispatch `dry_run=true` once per product-actions report from `main`, and open every preview at `https://ci.lascade.com/?repo=product-actions`. On these runs, confirm that a called job with `environment: production` under `secrets: inherit` receives the environment secrets. Do the same for connector `etl` on its next run. If it does not, stop and revise before deleting the repo-level secrets.
- [ ] **(user)** Repoint the cron-job.org chat-summary PT job to `Lascade-Co/product-actions`, workflow `chat-summary.yml`, with a new fine-grained token limited to product-actions (Actions: read and write).
- [ ] Run `repo_settings.py check` on all five repos. It reports only the expected store-reviews exemption. Repo-level secrets: delete those the check flags, after the user confirms the env syncs have landed. Then watch the first scheduled day and report each report's run.

### Task 16: Teardown

- [ ] Delete `Lascade-Co/daily-email-digest-actions` and `Lascade-Co/chat-summary`. This is permanent with no backup, per the user. Ask once more right before running `gh repo delete --yes` on each one.
- [ ] **(user)** Delete the old Infisical projects for the digest and chat-summary repos. Remove from the actions project the secrets used only by reports: `RESEND_API_KEY`, `HUNCH_POSTHOG_QUERY_API_KEY`, `INSTALLPAID_*`, `POSTHOG_QUERY_API_KEY`, `DATALASTIC_API_KEY`.
- [ ] In actions:
  - CONTEXT.md: remove the two repos from the **Frozen repository** list (deleted 2026-11-xx).
  - `docs/sealed-output.md`: add "Reviewed main and the production environment" with the `repo_settings.py` usage.
  - Commit through a PR.
- [ ] Clean up: delete the scratchpad clones and the `split/` folder, remove the worktree, and delete `docs/research/` if it was not committed.
