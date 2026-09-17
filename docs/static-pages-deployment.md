# Common static Pages deployment

The reusable `static-pages-deploy-trigger.yml` dispatches `static-pages-deploy`
to this repository. Any `Lascade-Co` repository can use it for a static
npm application. Callers provide `base_path` (default `.`), the relative directory
containing `package.json`, the lockfile, and `wrangler.json`. The runner uses
Node 24, executes `npm ci`, `npm run lint --if-present`, and `npm run build`,
then deploys `dist/`. It rejects path traversal and symlinked build directories.
No arbitrary build commands or server runtime are supported.

The source repository owns its target. Place a standard JSON `wrangler.json`
inside `base_path` with these fields:

```json
{
  "$schema": "https://unpkg.com/wrangler/config-schema.json",
  "name": "your-pages-project",
  "account_id": "your-32-character-cloudflare-account-id",
  "pages_build_output_dir": "./dist",
  "compatibility_date": "2026-09-17"
}
```

Only these fields are accepted; `$schema` is optional. There is no central
project registry. The runner reads this config from the requested immutable
source commit before building and again before deployment. It validates the
account, project name, output directory, and date, and uses only the validated
account/project as deployment arguments. It never executes source configuration
or copies it into the deployment job.

Create a Direct Upload Pages project with production branch `main` before the
first run. Copy `triggers/static-pages-trigger.yml` into the application's
`.github/workflows/deploy-pages.yml` and adjust `base_path` and push path filters.
Manual runs on `main` are supported.

Both repositories need `CI_APP_CLIENT_ID` and `CI_APP_PRIVATE_KEY`. The GitHub
App must be installed on `actions` with Contents write (repository dispatch),
and source repositories with Contents read and Commit statuses write. The central repository
alone needs `CLOUDFLARE_API_TOKEN` with Pages edit access to the source-configured
accounts. No Cloudflare secrets enter the source checkout or npm build job.
Source reads and checkout use short-lived, repository-scoped GitHub App tokens,
supporting private source repositories. Checkout does not persist credentials.
Only dispatch requests for repositories in `Lascade-Co` are accepted.

The runner accepts `main` and full lowercase 40-character commit SHAs. It checks
main HEAD before building and again inside a deploy job serialized by account
and project, skipping superseded requests. It checks out the exact requested
SHA and uploads only static output. The deploy job runs no application code:
it downloads the artifact, rejects symlinks, runtime handlers, and secret/config
files, and uploads through Wrangler. Commit status `cloudflare-pages` links back
to the central run for visibility.

There is a small unavoidable race if main advances during the actual upload;
the subsequent main deployment replaces it. Any newer main commit can supersede
a request, including unrelated or bot changes that do not trigger a deployment;
run the source workflow manually on main to retry in that case. Central control
code is pinned to each runner commit; source target config is pinned to the
requested source commit. Account IDs are resolved locally in the deploy job,
avoiding cross-job secret masking.

## Optional build environment

Callers may set `project_slug` and optionally `infisical_path` (default `/`) to
import an Infisical project's `prod` environment after `npm ci` and before lint
and build. The central repository supplies `INFISICAL_CLIENT_ID`,
`INFISICAL_CLIENT_SECRET`, and `INFISICAL_DOMAIN` to the official Infisical action.
Without `project_slug`, the import is skipped. Select a folder containing only
browser-safe build variables: Vite embeds `VITE_*` values in public assets.
Deployment credentials and Infisical imports stay in separate jobs. The current
store reviews caller has no build environment import.

## Store reviews example

`Lascade-Co/store-reviews` uses `base_path: dashboard`. Its
`dashboard/wrangler.json` selects account `c491dcc34f5883e8500e42a3e05f7bb6`
and project `store-reviews-dashboard`; the default URL is
`https://store-reviews-dashboard.pages.dev`.
Configure the R2 bucket's CORS policy to allow GET from that Pages origin, and
set the source repository's `SITE_BASE_URL` secret to the deployed URL so Slack
notifications open the correct dashboard. Keep existing permitted origins when
changing CORS.
