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
   A job without the setup step fails at its first step with "seal-bash: command not found", so a missing setup
   fails at its first step instead of leaking output.
4. Artifacts: `seal-pack [--allow-empty] [--include-hidden] <name> <path>...` writes
   `$RUNNER_TEMP/seal/art/<name>/<name>.tgz.age`; upload that directory with `retention-days` of at most 7 days (the ceiling; shorter is fine). After a
   `download-artifact`, the next step runs `seal-unpack <dir> [<dest>]` with `REPO_AGE_KEY: ${{ secrets.REPO_AGE_KEY }}`.
   `actions/upload-pages-artifact` (public site) is the only unsealed upload.
5. Use `echo "::add-mask::$VALUE"` for any secret or private value a later unsealed step (third-party actions,
   `actions/github-script`) might print. Sealed steps forward only mask lines, so masking costs nothing in the sealed log.

## Exemption

A workflow is exempt when its only trigger is `workflow_call` and no workflow here calls it via
`uses: ./.github/workflows/...` (the `*-deploy-trigger.yml` files, which run in callers' repos). Called workflows
such as `release-blog.yml` are sealed. `pull_request` workflows must not use secrets; `pull_request_target` is banned.
`scripts/seal/test_workflow_policy.py` enforces all of this.

## Reading a run

Open the marker link in the log, `https://ci.lascade.com/?run=<run id>&attempt=<n>`. Paste the repo private key
(stored only in your browser as `ci.repoKey`) and a GitHub fine-grained token with Repository access "Public
repositories (read-only)" and no other permissions (`ci.githubToken`).

## Rotate the repo key

1. `age-keygen -o key.txt`; put the whole `AGE-SECRET-KEY-1...` line, prefix included, into Infisical as
   `REPO_AGE_KEY` (it syncs to the Actions secret). Delete `key.txt`.
2. Write the new public key (`age-keygen -y`) to `scripts/seal/repo-key.pub` and commit it.
3. Artifacts sealed to the old key become unreadable with the new one; they expire within 7 days.
