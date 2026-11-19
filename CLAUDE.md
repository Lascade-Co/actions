# Actions
This repo contains GitHub actions that are invokend by `repository_dispatch` / `workflow_dispatch` / `schedule` events. These actions are triggered from other repositories in the org. 

## Conventions

- When writing any action always use the latest versions of GitHub Actions.
- Always look for (search the web) for prebuilt actions (that are well reputed) instead of writing custom scripts
- If custom scripts are needed inline in the action if they are less than 10 lines of code.
- For more complex scripts, place the script in a domain subfolder under scripts/ (e.g. scripts/ios/, scripts/catchup/) and invoke it using the GitHub raw url, including the subfolder (https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/<domain>/<name>)
- When writing an action that is using `repository_dispatch` write the corrosponding trigger action and place it in the triggers folder.
- Sealed output: this repo is public, so every workflow job (except `workflow_call`-only workflows nothing here calls) starts with the verbatim "Seal job output" setup step, ends with the verbatim "Upload sealed log" step, and uses `seal-bash {0}` / `seal-bash-e {0}` as its default shell (job-level `defaults` may override); the setup step may add only `working-directory: ${{ github.workspace }}`. See `docs/sealed-output.md`.
- Never use plain `shell: bash` (or any other unsealed shell) outside the setup step, and use `echo "::add-mask::$VALUE"` before a third-party action or `github-script` could print a private value (mask in an earlier sealed step, never via the same step's `env:`; never mask public metadata; the public step header shows expanded `run:` and `env:`, so keep private values out of them). See `docs/sealed-output.md`.
- Pack artifacts with `seal-pack` and upload from `$RUNNER_TEMP/seal/` with `retention-days` of at most 7; after `download-artifact`, run `seal-unpack` with `REPO_AGE_KEY` from secrets. Only `upload-pages-artifact` may upload unsealed. See `docs/sealed-output.md`.
- `pull_request` workflows may not reference secrets and `pull_request_target` is banned; `scripts/seal/test_workflow_policy.py` enforces the sealing rules. See `docs/sealed-output.md`.


## Vinext deployment

Vinext deployments are source-owned: fetch `package.json`, `wrangler.jsonc`,
and any `.gitmodules` at the dispatched commit, then read build and runtime
values from Infisical when the caller supplies `project_slug`. An omitted or empty
slug skips Infisical and requires empty build/runtime lists; keep source and
deployment validation unchanged. Derive checkout scope from pinned submodules.
The Worker destination comes solely from the source `wrangler.jsonc`.
An explicit D1 `migrations_dir` opts that target into pre-deploy Wrangler SQL
migrations. Copy only validated top-level `.sql` files into the artifact, verify
their hashes again on the deploy runner, and apply them before upload while the
Worker lock is held. Keep `migrations_dir` out of the deployed Worker config and
keep resource identities exact. D1 bindings without `migrations_dir` are not
migrated.
Classify Infisical `NEXT_PUBLIC_*` keys as build values and every other key
as a runtime secret, including `VINEXT_*`. Ignore blank and whitespace-only
values after resolving import overrides; preserve nonempty values verbatim.
Keep source-owned `cache` and Worker-only `exports` behavior exact; native
bundles keep shared exports/production cache at the base and Preview cache under
`previews.cache`. Never add `previews.exports` or lifecycle exports. Preserve
rate-limit identities/policies and collision-check their `name` fields.
Keep the system-name validation
guard for keys that could shadow runner process settings.
Keep this public runner free of project-specific registries and examples. `dev`
uses native Preview `stg` when source Wrangler declares `previews`, otherwise
uploads legacy alias `dev`; `main` deploys production. Native Preview bundles
keep production bindings at the top level and staging bindings under `previews`;
repeated validation must preserve that split. VPC Service bindings use
source-owned `vpc_services` identities; keep staging-only services under
`previews` and `env.staging`, out of production. Runtime secret names are derived
from Infisical into top-level `secrets.required`; values come
only from Infisical via `--secrets-file`. Do not add `previews.secrets`:
Wrangler 4.135 warns on and ignores it. Reject plaintext Wrangler `vars` for native
Preview projects. Wrangler writes asset progress to stdout even with
`preview --json`, so verify the tagged deployment and secret bindings through
the Cloudflare Preview API. Serialize both targets by account/Worker.
Never use secret bulk for previews. See
`docs/vinext-deployment.md` before changing the shared runner.
The reusable trigger accepts source pushes and manual `workflow_dispatch`
runs on `main` or `dev`.

Pin control downloads to the runner SHA; retry transport errors with bounded curl
retries and promote only successful, nonempty temporary downloads.

## Static Pages deployment

Static Pages callers supply `base_path` and keep their target in that directory's
`wrangler.json`; there is no central project registry. See `docs/static-pages-deployment.md`.
Keep npm builds isolated from deployment credentials, validate static artifacts,
and recheck main HEAD inside the serialized Pages deployment job.
