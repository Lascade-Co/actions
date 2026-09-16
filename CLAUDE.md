# Actions
This repo contains GitHub actions that are invokend by `repository_dispatch` / `workflow_dispatch` / `schedule` events. These actions are triggered from other repositories in the org. 

## Conventions

- When writing any action always use the latest versions of GitHub Actions.
- Always look for (search the web) for prebuilt actions (that are well reputed) instead of writing custom scripts
- If custom scripts are needed inline in the action if they are less than 10 lines of code.
- For more complex scripts, place the script in a domain subfolder under scripts/ (e.g. scripts/ios/, scripts/catchup/) and invoke it using the GitHub raw url, including the subfolder (https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/<domain>/<name>)
- When writing an action that is using `repository_dispatch` write the corrosponding trigger action and place it in the triggers folder.


## Vinext deployment

Register projects in `data/vinext-projects.json`; `dev` uploads alias `dev`, `main`
deploys production. The caller supplies required `project_slug`; do not store its
Infisical project selection centrally. Keep both targets serialized by account/Worker. Infisical
exports only allowlisted build/runtime values; never use secret bulk for previews.
See `docs/vinext-deployment.md` before changing the shared runner.

Pin control downloads to the runner SHA; retry transport errors with bounded curl
retries and promote only successful, nonempty temporary downloads.
