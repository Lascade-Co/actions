# Task 4 report

Status: done. Branch worktree-agent-aed8b635281623287 (reset to seal/sealed-output bd46297 first).

Files: scripts/seal/test_workflow_policy.py, scripts/seal/testdata/ (14 synthetic fixtures), .github/workflows/test-seal.yml,
.github/workflows/test-seal-e2e.yml, docs/sealed-output.md, CLAUDE.md (4 bullets).

Tests: fixtures 33 passed (good fixtures pass every rule; each bad fixture trips its rule; exempt set exact; fail-closed; both
new workflows pass the policy). Real workflows RED as expected: 161 failed / 162 passed overall; test_all_workflows_classified
reports "35 unsealed" (31 workflows + 4 composites), none of them exempt ones.

Notes: seal-unpack handles only *.tgz.age, so e2e-verify fetches the sealed-log zip with gh api and decrypts with age directly
(key file in verify dir, removed after). Public log fetched with curl -L + token (gh api refuses escape-sequence bodies per spike).
e2e workflows not run (no pushes).

## Fix report (review items 1-6)
Changes: secrets check now flags `secrets.`/`secrets[`, `secrets: inherit`, bare `${{ secrets }}`, a `secrets:` key (fixtures bad-pr-secrets-{index,bare,inherit,key}.yml);
e2e curl uses -fsSL (no --location-trusted), key never written to disk (`age -d -i <(printf ...)`), `trap 'rm -rf "$work"' EXIT`;
CLAUDE.md bullets each link docs/sealed-output.md; *.yaml/action.yaml globbed; upload path normalised with posixpath (bad-upload-traversal.yml),
previous step must invoke seal-pack as a command (bad-upload-pack-mention.yml), upload-artifact/merge flagged and retention-checked (bad-upload-merge.yml);
retention documented as a ceiling in the test docstring (good.yml 1-day pages upload passes) and docs ("at most 7 days").
Command: pytest scripts/seal/test_workflow_policy.py -q -k "fixture or exempt or fails_closed or test-seal or all_workflows"
Output: 42 passed, 1 failed (test_all_workflows_classified, expected RED). Full file: 161 failed, 171 passed (real unconverted workflows).

Setup-step working-directory ruling: only `working-directory: ${{ github.workspace }}` is accepted as an extra key (fixtures good-setup-workdir, bad-setup-workdir, bad-setup-extra-key). Fixtures+meta: 43 passed.
