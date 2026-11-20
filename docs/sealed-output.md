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
   `actions/upload-pages-artifact` (public site) is the only unsealed upload.
5. Masks. Use `echo "::add-mask::$VALUE"` for a private value a later unsealed step (third-party actions,
   `actions/github-script`) might print. A mask protects only lines printed after it, so mask in an EARLIER sealed
   step, with the value computed inside that step (e.g. from `$GITHUB_EVENT_PATH` or a file), never via the same
   step's `env:`. Do not mask public metadata (repo, branch, PR number/title/comment, SHA, versions, app names):
   masked values are dropped from job outputs and short masked words can break the viewer's marker lines.
6. The runner prints each step's `run:` script (with `${{ }}` already expanded) and its evaluated `env:` block in
   the PUBLIC step header before the sealed shell runs. Never expand private values into `run:` text, `env:` or a
   third-party `with:`.
7. Workflow annotations (`::warning::`/`::error::`) from run steps are sealed and appear only in the log viewer.

## Exemption

A workflow is exempt when its only trigger is `workflow_call` and no workflow here calls it via
`uses: ./.github/workflows/...` (the `*-deploy-trigger.yml` files, which run in callers' repos). Called workflows
such as `release-blog.yml` are sealed. `pull_request` workflows must not use secrets; `pull_request_target` is banned.
`scripts/seal/test_workflow_policy.py` enforces all of this.

## Reading a run

Open the marker link in the log, `https://ci.lascade.com/?repo=<repo>&run=<run id>&attempt=<n>`. Markers printed
before repos were named have no `repo=` and open as `actions` runs. In Settings, paste a GitHub fine-grained token with
Repository access "Public repositories (read-only)" and no other permissions (`ci.githubToken`, shared by all repos),
and the private key of each repo you read (that repo's `REPO_AGE_KEY`, stored only in your browser as
`ci.repoKey.<repo>`; an older single `ci.repoKey` moves to `ci.repoKey.actions`). Before a key is saved or used, the
viewer checks that its public key equals the repo's committed `.github/seal/repo-key.pub` (`scripts/seal/repo-key.pub`
for `actions`) and rejects a key for another repo. Settings lists the saved keys to replace or forget one; "Forget all"
clears the token, every key and everything decrypted.

`https://ci.lascade.com/` lists the public Lascade-Co repos with the `sealed-ci` topic ("Show all public repos" lists
the rest, marked "not sealed"); the "Open" box takes a GitHub run link of any Lascade-Co repo or a run ID.
`?repo=<repo>` lists that repo's recent runs (filter by workflow and status, 25 per page; it works without a token at
GitHub's 60 requests an hour). In a run, "Export logs" downloads the job's or every job's log as plain text, named
after the repo, with sealed output merged in and ANSI codes stripped; it needs the repo's key.

## Rotate the repo key

1. `age-keygen -o key.txt`; put the whole `AGE-SECRET-KEY-1...` line, prefix included, into Infisical as
   `REPO_AGE_KEY` (it syncs to the Actions secret). Delete `key.txt`.
2. Write the new public key (`age-keygen -y`) to `scripts/seal/repo-key.pub` and commit it.
3. Artifacts sealed to the old key become unreadable with the new one; they expire within 7 days.
