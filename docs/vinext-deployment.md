# Shared Vinext deployments

One existing Worker per project serves production and an independent `dev`
version preview. Source pushes on `dev` and `main` call the reusable trigger,
which sends `vinext-deploy` to this repository. Direct pushes deploy too; use
branch protection if changes must arrive through merged PRs.

| Source branch | Build target | Default Infisical environment | Operation |
| --- | --- | --- | --- |
| `dev` | `staging` | `staging` | `wrangler versions upload --preview-alias dev` |
| `main` | `production` | `prod` | `wrangler deploy` |

The generated configuration must name the same Worker in both targets, enable
`workers_dev` and `preview_urls`, and emit a prebuilt `no_bundle` entry. Version
uploads never promote staging to production. Domains and redirects remain
managed separately. Roamjoy uses the `stg.roamjoy.com` redirect to its Workers
preview and `www.roamjoy.com` for production.

## Onboard a project

1. Initialize the Worker and its resources once. This runner deliberately fails
   if no active Worker deployment exists. Provision caches/databases separately;
   it does not run stateful resource migrations or automatically create them.
2. Add the source repository to `data/vinext-projects.json`. Register its account,
   Worker, Node and exact Wrangler version, package manager, working directory,
   output directory/config path, build/check script names, and same-owner private
   submodule repository names. Pin `packageManager` in the source `package.json`
   and commit its matching lockfile. pnpm, npm and Yarn are supported; the working
   directory must contain its own package file and lockfile.
3. Register `build_variables`, `required_build_variables`, and `runtime_secrets`.
   Build values are intentionally visible to the build and potentially browsers;
   never allowlist server credentials there. Runtime names cannot overlap build
   names or begin with `NEXT_PUBLIC_`. Runner/tool environment names are rejected.
4. Supply the required `project_slug` input in the source repository caller
   (for example, `with: { project_slug: roam-joy-web }`). The trigger forwards it
   to the runner; there is no central project-slug variable or registry fallback.
   Configure environment slugs and folder paths under `environments.staging` and
   `environments.production` in the central registry.
5. Register **all** supported resource bindings for each target. The runner compares
   generated bindings with those exact objects. Keep staging/production KV IDs
   distinct. Other shared mutable resources need the same deliberate isolation
   review. Basic KV/Images, R2, D1, services and the binding families listed in
   `config.mjs` are supported. Configurations requiring custom builds, containers,
   migrations, local certificate/module files or additional binding types need an
   explicit extension and tests; they are not automatically provisioned.
6. Set each target's HTTPS `smoke_url`. Set `check_noindex: true` for projects that
   emit `X-Robots-Tag: noindex` on staging and omit it on production. Prefer a fast
   non-personalized page. Supply the actual deployment host, not a redirect alias.
7. Make `CENTRAL_DISPATCH_TOKEN` available to the source repo. It needs permission
   to send repository dispatches to `Lascade-Co/actions`. Copy
   `triggers/vinext-trigger.yml` into the source `.github/workflows/`.

Land the central runner and project registration on this repository's default
branch **before** enabling the caller. `repository_dispatch` runs only workflows
on the default branch. The backend `deploy-pr` and Pages `cloudflare-commit`
workflows remain independent.

### Central credentials

- `CI_APP_CLIENT_ID`, `CI_APP_PRIVATE_KEY`: GitHub App installed on the source and
  declared private submodules; contents read and source commit statuses write.
  Tokens are narrowed by job/repository. Checkout credentials are read-only and
  retained until checkout post-cleanup for recursive submodule compatibility.
- `INFISICAL_DOMAIN`, `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`: existing
  Universal Auth machine identity, authorized for each registered project/env/path.
- `CLOUDFLARE_API_TOKEN`: scoped to the registered accounts with Workers upload,
  versions, deployments, and required resource permissions. `CLOUDFLARE_ACCOUNT_ID`
  is taken from the reviewed registry, not a caller payload or exported secret.

Roamjoy supplies `project_slug: roam-joy-web` from its caller workflow. Initial
folder/environment mappings are `/`, `staging` and `prod`.
Both targets need the five required public values listed in the registry,
including `NEXT_PUBLIC_SITE_URL`, plus their own `LAABHAM_SECRET_KEY`. Local
`.env` and `.env.prod` files are not used by CI. The central machine identity must
have access to the caller-selected Infisical project before the first run.

## Pipeline guarantees and boundaries

- Validate the registered project and exact source SHA, check out that SHA, and
  use recorded submodule gitlinks rather than remote submodule branch tips.
- Download central scripts from this workflow's immutable `github.sha`. Caller
  payloads cannot choose commands, accounts or Worker names. The registered source
  repository supplies the Infisical project slug; branch mapping still determines
  the environment. Restrict machine-identity access to intended Infisical projects.
- Fetch secrets through the same Universal Auth and raw-secrets endpoints used
  by the official Infisical action. Its file mode writes unescaped dotenv values;
  the small `secrets.mjs` exporter instead masks fetched values and writes only
  allowlisted build/runtime keys as mode-0600 JSON, preserving quotes/newlines.
  Direct secrets override imports; later imports override earlier imports.
- Build on a runner without Cloudflare deployment credentials or runtime secrets.
  Strip source dotenv files in the disposable checkout; supply selected build
  values explicitly. Frozen installation, registered checks and build must pass.
- Transfer only the output bundle, removing generated dotenv files and rejecting
  symlinks/dependencies. Record source SHA/target in the artifact. The deploy job
  revalidates and sanitizes configuration before using standalone pinned Wrangler;
  it never installs or executes application scripts.
- Queue deploy jobs by account+Worker (`queue: max`, no in-flight cancellation),
  sharing the lock across both targets. Recheck branch HEAD just before upload;
  superseded deployments upload nothing. External/manual deployments do not share
  this lock and must not run concurrently.
- Supply every runtime secret during code upload. Check active and latest version
  secret names first. Staging must leave production deployment metadata unchanged;
  production must get 100% traffic and leave the recorded `dev` alias unchanged.
- Source commit statuses `vinext/staging` / `vinext/production` link to the central
  run. A green caller means dispatch accepted; the central status reports completion.
  Invalid preparation fails before credentials/export, visible in central Actions.
- Smoke failures report deployment failure but do not automatically roll back a
  release that may already be active. Page/indexing checks are not full API, CORS,
  catalog, authentication or payment verification. Artifacts expire after one day.

## Secrets and rollback

To rotate a value, update Infisical for that environment and deploy a fresh source
commit (or rerun the existing request if it is still branch HEAD). To add a runtime
secret, populate it for **both** targets before extending the registry allowlist.
Omitted remote secrets are inherited by Cloudflare, so removing a name from the
registry does not delete it: the runner stops until the removal has been reviewed
and performed deliberately. Never use `wrangler secret put`/`secret bulk` as a
preview update; use code upload with `--secrets-file`.

For a code rollback, revert the source change on the appropriate branch so the
runner rebuilds with that environment's current configuration. An emergency
production rollback may select a **verified production** version through Wrangler;
do not choose the latest uploaded version blindly, since it may be staging. A
preview rollback is a new preview upload from a revert on `dev`, never a production
deployment. Record the resulting version and verify headers/backend behavior.

## Local validation

```sh
node --test scripts/vinext/*.test.mjs
actionlint .github/workflows/vinext-deploy-trigger.yml .github/workflows/vinext-deploy-runner.yml triggers/vinext-trigger.yml
```

Tests use synthetic values and mocked GitHub/Cloudflare/Infisical responses; they
do not deploy or prove live credentials. Pilot staging after onboarding, verify
active production is unchanged, then pilot production and verify the preview.
