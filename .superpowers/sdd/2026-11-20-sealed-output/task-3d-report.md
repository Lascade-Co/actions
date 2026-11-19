# Task 3d report (report and test workflows)

Commit: feat(seal): seal report and test workflows. Base reset to seal/sealed-output bd46297.

All 10 files: workflow default `shell: "seal-bash-e {0}"` (no workflow/job set a shell, so implicit bash -e); setup step first and sealed-log upload last in every job (daily-catchup 7 jobs, daily-hunch-retention 4, daily-install-paid 4, daily-ad-spend 3, seo-blog-audit 2, one each elsewhere). Explicit `shell: bash` steps -> `seal-bash {0}` (daily-ad-spend 1, daily-catchup 2: Install Claude API SDK); setup step keeps `shell: bash`.

Uploads converted (seal-pack + upload, compression-level 0, retention 7):
- daily-catchup: summary-<safe> (pack_1, summarize job), daily-report (pack_2, collect job; if wrote=='true')
- daily-marketing-net: marketing-net-message (--allow-empty; retention 30 -> 7)
- seo-blog-audit: seo-report-<site> (--allow-empty; retention 30 -> 7)
Downloads: daily-catchup collect (merge-multiple into artifacts -> one `seal-unpack artifacts`); commit and email jobs download daily-report to sealed-in/daily-report then `seal-unpack sealed-in/daily-report .` (dest = workspace root, preserving the original layout; unpacks non-recursively so the catchup/ checkout is untouched). Unseal steps carry env REPO_AGE_KEY.

test-ai and test-catchup: no secrets reference, no downloads; got setup/upload steps and default shell only.

Rule 6/7: no step needed moving to env. `${{ }}` in run text only expands public metadata or non-sensitive control values: event_name, run_id/attempt, matrix.repo / matrix.site / steps.repo.outputs.safe (repo/site names), inputs booleans, and report/ad dates (needs.gate*.outputs.report_date, steps.dates.outputs.ad_date; the dates also appear in step names and concurrency groups). Existing `${{ steps.app-token.outputs.token }}` into checkout `token:` is a masked token. Infisical/secrets-action inputs are secrets. Existing step names derived from outputs (`send_step`, `alert_step`) are intentionally public dedupe markers. Watchdog MESSAGE (report numbers) already flows via env into a sealed step. No `::add-mask::` lines existed in these files. No gradle/docker/claude-code-action/github-script in this group.

actionlint: clean on 9 files; daily-catchup.yml reports only the pre-existing `client-id` vs `app-id` complaint for create-github-app-token@v3 (local actionlint metadata is stale; lines untouched).

Tests: scripts/installpaid/test_ip_workflow.py pinned `steps[-1]` as the send step; now `[-2]` (upload step is last). Baseline: all pass given deps (test_render_variant_b needs matplotlib, installed in scratch venv). After: 1109 passed (adspend catchup hunch installpaid seo ai accuracy).

## Fix report
- daily-catchup collect: `Unseal repo summaries` now has `if: hashFiles('artifacts/*.tgz.age') != ''` so an empty download stays green (wrote=false), matching the original.
- test_ip_workflow.py: send step now found by `send_resend_ip.sh` in its run text instead of a fixed index.
- actionlint clean (apart from the pre-existing app-id/client-id note); installpaid+hunch tests: 457 passed.
