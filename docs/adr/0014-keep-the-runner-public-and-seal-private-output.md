# Keep the runner public and seal private output

This repository's Actions logs and artifacts are public, and they were leaking **private values**:
debug APKs and staging IPAs of private apps, per-repo catchup summaries, daily business reports,
and build output. We keep the repository public and **seal** everything private instead of making
it private.

**Why not make it private.** That is the only complete fix, because GitHub cannot hide logs or
artifacts on a public repository. The cost decided it. Billing for 2026-11-01 to 11-19 showed this
repo using 4,858 macOS minutes ($301), 18,855 Linux minutes ($113) and about 97 GB of artifact
storage ($14). That is roughly $675/month. The org is on the Free plan and already uses all of its
included private minutes, so all of it would be billed. Going private would also break the
`raw.githubusercontent.com` script fetches and stop public callers such as `store-reviews` from
using the reusable workflows.

**What we do instead.**
- Every artifact is a **sealed artifact**, encrypted with age to the **repo key**.
- Every job writes its own step output, step summary and annotations to a **sealed log**. The
  public log shows only step names, exit codes and a **log viewer** link.
- The repo's own composite actions use the same sealed shell.
- Private values that third-party actions or step headers might print are **masked**.
- Log and artifact retention is 7 days.
- Fork pull requests always need approval, and no `pull_request` workflow may use secrets.

**Accepted residual leaks.** **Public metadata** (repo names, branches, PR numbers and titles, and
commit SHAs) stays visible in step headers, in artifact and run names, and in third-party action
output. So does the shape of each workflow. Any private value that is neither a secret nor
explicitly masked can still appear in a third-party action's own output. History before this
decision stays in git and in the six public forks; we chose not to rewrite it.

**Why one key, and why a key the runners hold.** Jobs hand sealed artifacts to later jobs, so
runners must be able to unseal. Anyone who can change a workflow on `main` can already read every
secret, so a second, human-only key would not protect anything more. The private half lives only
in Infisical, which syncs it to Actions secrets.

**Why the log viewer has its own subdomain.** It keeps the repo key in browser storage. Four other
org Pages sites share the `lascade-co.github.io` origin, and any script on them could read that
storage, so the viewer is served from `ci.lascade.com`.
