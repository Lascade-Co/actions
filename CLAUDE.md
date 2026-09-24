# Actions
This repo contains GitHub actions that are invokend by `repository_dispatch` / `workflow_dispatch` / `schedule` events. These actions are triggered from other repositories in the org. 

## Conventions

- When writing any action always use the latest versions of GitHub Actions.
- Always look for (search the web) for prebuilt actions (that are well reputed) instead of writing custom scripts
- If custom scripts are needed inline in the action if they are less than 10 lines of code.
- For more complex scripts, place the script in a domain subfolder under scripts/ (e.g. scripts/ios/, scripts/catchup/) and invoke it using the GitHub raw url, including the subfolder (https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/<domain>/<name>)
- When writing an action that is using `repository_dispatch` write the corrosponding trigger action and place it in the triggers folder.


## Vinext deployment

Vinext deployments are source-owned: fetch `package.json`, `wrangler.jsonc`,
and any `.gitmodules` at the dispatched commit, then read build and runtime
values from Infisical. Derive checkout scope from pinned submodules.
The Worker destination comes solely from the source `wrangler.jsonc`.
Classify Infisical `NEXT_PUBLIC_*` keys as build values. Legacy sources use
other app keys as runtime secrets; native Preview sources use only names in
`secrets.required`. Reserve `VINEXT_*` for runner metadata.
Keep this public runner free of project-specific registries and examples. `dev`
uses native Preview `stg` when source Wrangler declares `previews`, otherwise
uploads legacy alias `dev`; `main` deploys production. Native Preview projects
declare runtime secret names in both production and Preview `secrets.required`;
values come only from Infisical. Reject plaintext Wrangler `vars` for native
Preview projects and verify returned Preview secret bindings. Serialize both
targets by account/Worker.
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
