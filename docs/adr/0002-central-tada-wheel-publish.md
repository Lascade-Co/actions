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
cannot act as a native required PR status check, so we dropped PR-time validation. The central
runner responds only to `repository_dispatch` (never `pull_request`); the thin `tada` trigger
fires it on push to `main` and on manual `workflow_dispatch`, so it is **publish-only on `main`**
plus on-demand re-runs. Validation still runs —
tests, wheel build, metadata/credential/bundle checks — but only on the push that publishes,
not before merge. If a pre-merge gate is needed later, it would have to be reinstated as an
in-repo `tada` workflow (which would reintroduce a shared-repo token for that path).

**Package ownership & GHCR auth.** The `ghcr.io/lascade-co/tada-wheel` package is created,
pushed, and pruned by the **`actions` repo's own `GITHUB_TOKEN`** (the runner executes in
`Lascade-Co/actions`, and `permissions: packages: write` lets that token create/write/delete the
package, which auto-links to `Lascade-Co/actions`). We first tried a `tada`-scoped CI App token,
but a GitHub App **installation** is forbidden from *creating* an org package *and* from *writing*
one even after connecting it to `tada` — both confirmed with `403`. GHCR's "connect
repository"/"Manage Actions access" grants apply to a repo's `GITHUB_TOKEN`, not to
app-installation tokens. The App token is still used for the private `travel-animator-shared`
checkout and the stale-main guard (both need `tada` read access), so `SHARED_REPO_TOKEN` stays
eliminated — only the GHCR steps moved to `GITHUB_TOKEN`. Consequence: the package links to
`Lascade-Co/actions`, not `tada`; the pull path `ghcr.io/lascade-co/tada-wheel` is unchanged.

**Identity remapping.** Because the build now runs in the `actions` repo, ambient `GITHUB_*`
variables refer to `actions`, not `tada`. The published artifact's identity — build-metadata
`repository`/`revision`, the ORAS `image.source`/`image.revision` annotations, and the stale-main
guard — is taken from the dispatch payload (`repo`, `sha`) instead. Only `workflow_run_id`/
`workflow_run_attempt` in the metadata point at the `actions` build run, which is where the build
genuinely executed.
