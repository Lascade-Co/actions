# TADA wheel publishing moved to the central runner

`Lascade-Co/tada` previously built, validated, and published its wheel entirely in its own
`publish-wheel.yml`. We moved publishing to a central runner in this repo
(`.github/workflows/publish-tada-wheel.yml`), triggered by `repository_dispatch` from a thin
trigger workflow in `tada`, matching the flutter/spm/docker pattern.

**Why.** The old workflow checked out the private `travel-animator-shared` repo with a
long-lived `SHARED_REPO_TOKEN` PAT stored in `tada`. The central runner authenticates with the
CI GitHub App, which already has access to every repo in the org, so it can mint a token scoped
to both `tada` and `travel-animator-shared` and check them out with no per-repo PAT. The same
app token replaces `tada`'s `GITHUB_TOKEN` for the GHCR push and the version prune. Eliminating
the shared PAT was the motivating trade-off.

**What we gave up.** The old workflow also ran on `pull_request`, gating merges with the test
suite, wheel build, and credential scan. A `repository_dispatch` run happens in another repo and
cannot act as a native required PR status check, so we dropped PR-time validation: the central
runner is **publish-only on `main`** (plus manual `workflow_dispatch`). Validation still runs —
tests, wheel build, metadata/credential/bundle checks — but only on the push that publishes,
not before merge. If a pre-merge gate is needed later, it would have to be reinstated as an
in-repo `tada` workflow (which would reintroduce a shared-repo token for that path).

**Package ownership.** The `ghcr.io/lascade-co/tada-wheel` package stays linked to the `tada`
repo. That link is what lets a CI App token scoped only to `tada` push `:latest` and prune every
prior version through the org container API — no org-wide package admin required. The CI App must
have the `Packages: read & write` permission enabled for this to work.

**Identity remapping.** Because the build now runs in the `actions` repo, ambient `GITHUB_*`
variables refer to `actions`, not `tada`. The published artifact's identity — build-metadata
`repository`/`revision`, the ORAS `image.source`/`image.revision` annotations, and the stale-main
guard — is taken from the dispatch payload (`repo`, `sha`) instead. Only `workflow_run_id`/
`workflow_run_attempt` in the metadata point at the `actions` build run, which is where the build
genuinely executed.
