# Actions
This repo contains GitHub actions that are invokend by `repository_dispatch` / `workflow_dispatch` / `schedule` events. These actions are triggered from other repositories in the org. 

## Conventions

- When writing any action always use the latest versions of GitHub Actions.
- Always look for (search the web) for prebuilt actions (that are well reputed) instead of writing custom scripts
- If custom scripts are needed inline in the action if they are less than 10 lines of code.
- For more complex scripts, write the script to the scrips folder and invoke it using GitHub raw url (https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/)
- When writing an action that is using `repository_dispatch` write the corrosponding trigger action and place it in the triggers folder.

