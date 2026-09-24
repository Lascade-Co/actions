# Shared Vinext Workers deployment

The public runner contains deployment logic only. Each source repository owns its
`wrangler.jsonc`, `package.json`, and caller workflow. The caller supplies only
its Infisical project slug.
No project registry or deployment manifest is stored here.

| Source branch | Wrangler environment | Infisical environment | Runner action |
| --- | --- | --- | --- |
| `dev` with source `previews` block | `staging` build plus native Preview `stg` | `staging` | Deploy a Worker Preview with staging secrets |
| `dev` without source `previews` block | `staging` | `staging` | Upload a version with preview alias `dev` |
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
  It is the sole source of the deployment destination. Environments must
  target the same account and Worker. Declare resource
  bindings inside each environment because Cloudflare does not inherit them.
  The generated config is compared against this source configuration, then
  sanitized to a deployment-only bundle; executable hooks and routes are
  discarded.
- For native Worker Previews, add a top-level `previews` block and pin Wrangler
  4.135.0 or later. The top level contains production bindings; `previews`
  contains staging bindings. Retain `env.staging` and `env.production` for the
  Vinext build, with matching bindings. The runner validates both source and
  generated configuration and uses the stable Preview name `stg` for `dev`.
  Without a `previews` block, the existing version-alias path is unchanged.
- Native Preview projects derive runtime secret names from the selected
  Infisical environment. The runner writes those names into both
  `secrets.required` and `previews.secrets.required` in the sanitized bundle.
  Production deploy and Preview
  deployment both pass a temporary `--secrets-file` to the pinned Wrangler,
  so the secrets are included with the deployment. Preview deployment uses
  `--ignore-base-config` so dashboard Base settings cannot add bindings or secrets.
  Native projects must not declare plaintext `vars` at the top level, in
  `previews`, or in either Vinext environment. The runner rejects generated
  plaintext `vars` and checks the deployed Preview secret names and types.
- The source workflow calls
  `Lascade-Co/actions/.github/workflows/vinext-deploy-trigger.yml@main` with
  `project_slug`. It passes `CENTRAL_DISPATCH_TOKEN` as the reusable workflow
  secret.

The CI GitHub App needs read access to the source repository and any private
submodules. The build checkout token is read-only and scoped to the source repo plus
the submodules declared at the dispatched commit. It remains during the job for recursive
submodule cleanup. Source maintainers control the Worker target in
`wrangler.jsonc` and the Infisical project slug in the caller workflow. The
central Cloudflare token must be scoped to the Workers and accounts these
maintainers may deploy; the runner does not authorize the destination through
Infisical. Scope the Infisical machine identity to the intended projects.

## Infisical contract

The runner reads the caller's project, environment `staging` or `prod`, path
`/`. It classifies the values as follows:

- App build variables prefixed `NEXT_PUBLIC_`. These are passed to the build
  and embedded into browser assets. Discovered public values must be nonempty.
- Every other value becomes a Worker runtime secret automatically, including
  `VINEXT_*` keys and values imported into this Infisical environment. Runtime
  values must be nonempty. Invalid or system-reserved variable names fail
  preparation to prevent runner environment shadowing. Runtime names also may
  not collide with Worker resource or asset binding names.

The runner reads `.gitmodules` from the exact source commit. Submodules must
use same-organization GitHub URLs and have pinned gitlinks. The checkout token
is scoped to the source repo plus the listed submodule repositories. Nested
private submodules need separate support before they can be checked out.

No build-variable or runtime-secret name lists are required. Local
`.env*` and `.dev.vars*` files in the repository root are removed from the
fresh CI checkout before building. Environment files are excluded from the
deployment artifact. If Infisical key names change between preparation and
build or deploy, the run fails so a new run can capture the complete set.

The legacy alias path requires an initial active Worker deployment before the
shared runner can update it. Native Previews can be created before the first
production deployment. The runner checks that a native Preview does not change
an existing production deployment. The runner does not
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
and deployment state. For native Previews it checks Wrangler's deployment ID,
Preview URL, and an unchanged active production deployment. It does not report
application health. Domain changes
and authenticated UI checks are separate rollout steps.
