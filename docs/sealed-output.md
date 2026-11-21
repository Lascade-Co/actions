# Sealed output

This repo is public, so its job logs are too. Every non-exempt job seals its step output: the public log shows
one marker line per step, and the real output goes into an age-encrypted artifact that only the repo key can read.

**What is sealed:** every `run` step's stdout and stderr, and its step summary. **What stays public:** repo name,
branch, PR number and title, commit SHA, step names, and lines starting `::add-mask::`.

## Author rules

1. The first step of every job is the setup step (it installs `seal-bash`, `seal-bash-e`, `seal-pack`, `seal-unpack`):

   ```yaml
   - name: Seal job output
     shell: bash
     run: |
       u="https://raw.githubusercontent.com/Lascade-Co/actions/${{ github.workflow_sha }}/scripts/seal/install.sh"
       curl -fsSL --retry 3 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 60 -o "$RUNNER_TEMP/seal-install.tmp" "$u"
       test -s "$RUNNER_TEMP/seal-install.tmp" && mv "$RUNNER_TEMP/seal-install.tmp" "$RUNNER_TEMP/seal-install.sh"
       SEAL_REF="${{ github.workflow_sha }}" bash "$RUNNER_TEMP/seal-install.sh"
   ```

   The only key you may add is `working-directory: ${{ github.workspace }}`, for jobs whose default
   working directory (e.g. `source`) does not exist yet at step 1.

2. The last step uploads the sealed log:

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

3. Shell: `defaults.run.shell: "seal-bash {0}"` (same flags as `shell: bash`) when the workflow used `bash`, otherwise
   `"seal-bash-e {0}"` (same as the implicit default, `bash -e`). Job-level `defaults` may override. Step-level
   `shell: bash` becomes `shell: seal-bash {0}`. Composite actions use `shell: seal-bash {0}`.
   A job without the setup step fails at its first step with "Unable to locate executable file: seal-bash", so a
   missing setup fails at its first step instead of leaking output.
4. Artifacts: `seal-pack [--allow-empty] [--include-hidden] <name> <path>...` writes
   `$RUNNER_TEMP/seal/art/<name>/<name>.tgz.age`; upload that directory with `retention-days` of at most 7 days (the ceiling; shorter is fine). After a
   `download-artifact`, the next step runs `seal-unpack <dir> [<dest>]` with `REPO_AGE_KEY: ${{ secrets.REPO_AGE_KEY }}`.
   `actions/upload-pages-artifact` (public site) is the only unsealed upload. An `upload-artifact` path is either
   `${{ runner.temp }}/seal/out/` (rule 2) or `${{ runner.temp }}/seal/art/<name>/`, and `seal-pack` refuses any
   file under `$RUNNER_TEMP/seal`.
5. Masks. Use `echo "::add-mask::$VALUE"` for a private value a later unsealed step (third-party actions,
   `actions/github-script`) might print. A mask protects only lines printed after it, so mask in an EARLIER sealed
   step, with the value computed inside that step (e.g. from `$GITHUB_EVENT_PATH` or a file), never via the same
   step's `env:`. Do not mask public metadata (repo, branch, PR number/title/comment, SHA, versions, app names):
   masked values are dropped from job outputs and short masked words can break the viewer's marker lines.
   Values registered with `::add-mask::` in a sealed step are also replaced by `***` in that job's sealed logs from
   that point on (masks registered by other actions are not); secrets printed without a mask are stored in the
   sealed log as printed. Those values are kept in `$RUNNER_TEMP/seal/masks`, which every later step and action in
   the same job (same user) can read; it is never uploaded. Each step's log is redacted in memory, so a very large
   log, or a cancel that kills the redaction, can drop that step's sealed segment; it never leaks the plaintext.
6. The runner prints each step's `run:` script (with `${{ }}` already expanded) and its evaluated `env:` block in
   the PUBLIC step header before the sealed shell runs. Never expand private values into `run:` text, `env:` or a
   third-party `with:`.
7. Workflow annotations (`::warning::`/`::error::`) from run steps are sealed and appear only in the log viewer.
8. Never set `SEAL_*` or `seal_*` variables in `env:` (workflow, job or step level); the setup step and composites set
   what they need, and the policy fails any workflow or composite that does.

## Production environment

Every job that references `secrets.<NAME>` (anything other than `secrets.GITHUB_TOKEN`) must declare
`environment: production` (a string, or `name:` of a mapping), so the secret is only released to a run on a branch the
environment allows (`main`). The policy test fails the job otherwise.

- `.github/seal/environments` (optional): extra allowed environment names, one per line; `#` comment lines and blanks
  are ignored. Absent means only `production`. An `environment:` expression passes only if every value it can yield is
  an allowed string literal (condition operands such as `'prod'` in `inputs.env == 'prod' && 'a' || 'b'` are not values).
- Jobs that call a local reusable workflow (`uses: ./.github/workflows/...`) carry no `environment:`; the called
  workflow's jobs do.
- `.github/seal/env-exempt` (optional): one line per job that is exempt, `<workflow file>:<job id>  # <reason>`, for
  example `release.yml:call  # the callee job declares production`. A line without a reason fails. A cross-repo
  `uses:` job that passes `secrets:` must be listed here.
- Workflows exempt from sealing (`workflow_call`-only files nothing here calls, in this repo) are skipped; composite
  actions never reference secrets.
- Every environment that holds secrets should have a deployment branch policy allowing only `main`.

## Exemption

A workflow is exempt when its only trigger is `workflow_call` and no workflow here calls it via
`uses: ./.github/workflows/...` (the `*-deploy-trigger.yml` files, which run in callers' repos). Called workflows
are sealed. `pull_request` workflows must not use secrets; `pull_request_target` is banned.
`scripts/seal/test_workflow_policy.py` enforces all of this.

## Reading a run

Open the marker link in the log, `https://ci.lascade.com/?repo=<repo>&run=<run id>&attempt=<n>` (older markers
without `repo=` open as `actions` runs). In Settings, paste a GitHub fine-grained token with Repository access "Public
repositories (read-only)" and no other permissions (shared by all repos), and the private key (`REPO_AGE_KEY`) of each
repo you read; keys stay only in your browser, one per repo. A key is checked against the repo's committed
`.github/seal/repo-key.pub` (`scripts/seal/repo-key.pub` for `actions`) before it is saved or used, so a key for another
repo or a rotated key is rejected. "Forget all" clears the token, every key and everything decrypted.

`https://ci.lascade.com/` lists public Lascade-Co repos with the `sealed-ci` topic ("Show all public repos" adds the
rest, marked "not sealed"); `?repo=<repo>` lists that repo's recent runs. "Export logs" in a run downloads the job's or
every job's log as plain text with sealed output merged in (needs the repo's key).

## Generating or rotating a repo key

The private half must never touch disk or chat. macOS (Linux: pipe to `xclip -selection clipboard` or `wl-copy`):

```sh
k=$(age-keygen 2>/dev/null); printf '%s' "$k" | grep '^AGE-SECRET-KEY-' | tr -d '\n' | pbcopy; printf '%s\n' "$k" | grep -o 'age1[0-9a-z]*'; unset k
```

1. Paste the clipboard into Infisical as `REPO_AGE_KEY`, `AGE-SECRET-KEY-1` prefix intact (it syncs to the repo's
   Actions secrets). The command prints the public `age1...` line.
2. Commit only that line: `scripts/seal/repo-key.pub` here, `.github/seal/repo-key.pub` in other repos.
3. After a rotation, artifacts sealed to the old key become unreadable; they expire within 7 days.

## Sealing another repository

Other Lascade-Co repos use the same toolkit through two composite actions. Checklist for a frozen repository (see
`CONTEXT.md`). Actions stays disabled while the repo is private, including while you commit these changes.

1. Generate the key pair as above and commit `.github/seal/repo-key.pub`.
2. First step of every job, no `with:`:

   ```yaml
   - name: Seal job output
     uses: Lascade-Co/actions/actions/seal@seal-v1
   ```

   Jobs with a `permissions:` block need `contents: read`; the seal step reads the key file through the API.
3. Last step of every job:

   ```yaml
   - if: always()
     uses: Lascade-Co/actions/actions/seal-upload@seal-v1
   ```

   Repos that pin actions by SHA may pin both seal steps to the same full commit SHA of `Lascade-Co/actions` instead of
   `@seal-v1` (every seal reference in a workflow file must use the same ref) and bump it deliberately; the policy check
   caller stays on `@seal-v1`.
4. Default shell `seal-bash {0}` / `seal-bash-e {0}`, masks, and `seal-pack`/`seal-unpack` artifacts follow the rules
   above unchanged.
5. Add `.github/workflows/seal-policy.yml`, which enforces the rules in that repo:

   ```yaml
   name: Seal policy
   on: [push, pull_request]
   permissions:
     contents: read
   jobs:
     policy:
       uses: Lascade-Co/actions/.github/workflows/seal-policy.yml@seal-v1
   ```

6. Add the `sealed-ci` topic, and the "Sealed output" section from `docs/sealed-repo-claude.md` to the repo's `CLAUDE.md`.
7. Before the repo goes public, purge every pre-sealing run (`scripts/seal/purge_runs.py`, archive to
   `~/Downloads/actions-log-archive/<repo>/`).
8. Only after the purge: make public, then enable Actions, then verify at `https://ci.lascade.com/?repo=<repo>`.

## Updating the seal action

`seal-v1` is a moving tag. After toolkit changes pass on `main`, run `git tag -f seal-v1 <sha> && git push -f origin seal-v1`
from a `main` commit whose seal tests passed; a bad move breaks every sealed repo's next run.
