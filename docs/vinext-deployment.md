# Shared Vinext Workers deployment

The public runner contains deployment logic only. Each source repository owns its
`wrangler.jsonc`, `package.json`, and caller workflow. The caller supplies only
its Infisical project slug.
No project registry or deployment manifest is stored here.

| Source branch | Wrangler environment | Infisical environment | Runner action |
| --- | --- | --- | --- |
| `dev` | `staging` | `staging` | Upload a version with preview alias `dev` |
| `main` | `production` | `prod` | Deploy the active production version |

A main-only project omits `staging` in `wrangler.jsonc` and triggers only on
`main`. The branch selects the environment; the dispatch cannot override it.

## Source repository contract

- The repository belongs to `Lascade-Co`, has root `package.json` and
  `wrangler.jsonc`, and keeps `wrangler.jsonc` in the strict JSON subset of
  JSONC. Both files are fetched through the GitHub API at the exact dispatched
  commit. The runner checks that this commit remains the branch head.
- `package.json` pins `packageManager` and an exact `wrangler` version. It has
  `check:deploy` and `build:vinext` scripts. The build emits `dist/server/wrangler.json`
  and `dist/client`. The runner uses Node 24 and frozen dependency installs.
- `wrangler.jsonc` declares the account, Worker, and each deployed environment.
  Environments must target the same account and Worker. Declare resource
  bindings inside each environment because Cloudflare does not inherit them.
  The generated config is compared against this source configuration, then
  sanitized to a deployment-only bundle; executable hooks and routes are
  discarded.
- The source workflow calls
  `Lascade-Co/actions/.github/workflows/vinext-deploy-trigger.yml@main` with
  `project_slug`. It passes `CENTRAL_DISPATCH_TOKEN` as the reusable workflow
  secret.

The CI GitHub App needs read access to the source repository and any private
submodules. The build checkout token is read-only and scoped to the source repo plus
the submodules declared at the dispatched commit. It remains during the job for recursive
submodule cleanup. The central Cloudflare token and Infisical machine identity must be
scoped to resources the source repositories are allowed to deploy; source
maintainers control the Worker target in `wrangler.jsonc` and the Infisical
project slug in the caller workflow.

## Infisical contract

The runner reads the caller's project, environment `staging` or `prod`, path
`/`. Each environment must contain:

- App build variables prefixed `NEXT_PUBLIC_`. Only these are passed to the
  build. They are embedded into browser assets.
- `VINEXT_REQUIRED_BUILD_VARIABLES`: comma-separated names of public build
  variables that must exist and be nonempty. Use an empty string if none.
- `VINEXT_RUNTIME_SECRETS`: comma-separated names of Worker runtime secrets
  that must exist. Use an empty string if none. Only these values are uploaded
  as Worker secrets.
- `VINEXT_WORKER_NAME` and `VINEXT_ACCOUNT_ID`: the exact Worker target
  authorized for that project. The runner compares them to the pinned source
  Wrangler config before building.

The runner reads `.gitmodules` from the exact source commit. Submodules must
use same-organization GitHub URLs and have pinned gitlinks. The checkout token
is scoped to the source repo plus the listed submodule repositories. Nested
private submodules need separate support before they can be checked out.

These `VINEXT_*` values are deployment metadata and are never passed to the
app. Other Infisical values are ignored. Local `.env*` and `.dev.vars*` files
in the repository root are removed from the fresh CI checkout before building;
environment files are excluded from the deployment artifact.

The Worker must have an initial active deployment before the shared runner can
update it. A missing Worker fails before upload, including for production.
This guard lets the runner verify existing secret bindings and protect the
production version during staging uploads. The runner does not
create KV or R2 resources; source `wrangler.jsonc` must refer to existing
resources. Vinext's optional `VINEXT_KV_CACHE` binding is unnecessary when its
default in-memory cache is acceptable. App-data KV or R2 bindings remain valid.

The public run title and summary show the source repository and target
environment. The runner masks private Infisical values during plan resolution
and masks all exported values before build or deploy commands run. Metadata
keys, account/Worker identifiers, and secret names are not credentials.
API errors exclude response bodies, and temporary secret JSON is deleted.

The central run reports `vinext/staging` or `vinext/production` on the source
commit. The caller job only confirms dispatch. A successful central run means
Cloudflare accepted the upload and the runner verified the resulting version
and deployment state. It does not report application health. Domain changes
and authenticated UI checks are separate rollout steps.
