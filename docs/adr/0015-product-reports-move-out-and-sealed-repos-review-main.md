# Product reports move out; sealed repositories use reviewed main and production-environment secrets

This repository had grown two unrelated jobs. One is the build, release and deploy pipelines that
other org repositories dispatch to every day. The other is a set of **product reports**: scheduled
emails for the product team (ad spend, catchup, TreasureHunch retention, install → paid and its
watchdog, marketing net, ship accuracy and the blog SEO audit). Two **frozen repositories**,
`daily-email-digest-actions` and `chat-summary`, held more product reports.

**What we decided.**
- All product reports move to a new public **sealed repository**, `Lascade-Co/product-actions`,
  which the product team (non-technical) can edit. This repo keeps only the pipelines, the
  sealing toolkit and the **log viewer**.
- `daily-email-digest-actions` and `chat-summary` are folded in and then deleted, without a backup.
- Release Blog is deleted, not moved.
- Code arrives as a fresh copy with no git history. The two frozen repositories were private and
  their history was never reviewed for **private values**, so only reviewed current files go
  public. `Lascade-Co/actions` keeps its own public history.
- The report plumbing (send-time gate, hold, Resend delivery, delivery-history dedupe, edition and
  report-date logic) becomes one shared copy. It existed three to six times over. Each report keeps
  its exact current email HTML; only the plumbing is shared.
- `chat-summary` is rewritten from Node to Python on that shared plumbing, and moves from Gemini to
  Claude through `scripts/ai/claude_api.py`. product-actions fetches that one file from this
  repo's `main`; there is no second copy.
- The cutover accepts one duplicate or missed email on the switch day rather than adding
  cross-repo dedupe code.

**Reviewed main on every sealed repository.** With non-technical writers, an unreviewed workflow
change could send wrong emails or expose secrets. Every **sealed repository** (`actions`,
`connector`, `infra-actions`, `store-reviews`, `product-actions`) gets the same **reviewed main**
ruleset:
- changes reach `main` only through a pull request
- a `@Lascade-Co/backend` code owner must approve, and approval is dismissed on a new push
- the seal policy and test checks must pass
- force-push and deletion are blocked

Org admins bypass it fully, so urgent fixes are never blocked when no second reviewer is around.

**Why secrets live in a main-only environment.** A ruleset on `main` does not stop anyone with Write
access from pushing a branch that edits a workflow and running it. That run gets every repository
secret. So each sealed repository keeps its secrets in a **production environment** that only
`main` can deploy to, and every secret-using job declares it. Only reviewed code ever sees a
secret. The policy test enforces it, and the moving `seal-v1` tag that every secret job runs first
gets its own admin-only tag ruleset in `Lascade-Co/actions`. Branch runs and pull requests test
with fixtures. Previews of a change come after merge,
through a manual dry run or test send. Jobs that already need another environment (such as the
Cloudflare deploy environments) get the same main-only branch policy and their own copy of the
secrets they use.

**Why the product team gets Write, not forks.** Forking cannot be disabled on a public repository.
Forks are harmless here: fork pull requests get no secrets, `pull_request_target` is banned, and
fork workflows wait for approval. Write access keeps the product team's work in branches of the
one repository, where the ruleset applies.

**Rejected alternatives.**
- A private product repository would need no sealing, but every run would be billed. The reports
  run about 35 times a day.
- Moving the reports into the existing private `product-intelligence` repository was rejected for
  the same reason.
- Copying `claude_api.py` would let the two copies drift.
- Keeping chat-summary in Node would leave a second implementation of the plumbing.
