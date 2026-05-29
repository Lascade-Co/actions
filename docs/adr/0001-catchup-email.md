# Daily Catchup email: tag filter, rendering split, and parallel jobs

Three decisions made when adding the daily email to `daily-catchup.yml`:

**1. Email scope via a checked-in exclude list, not repo tags.** Every active repo is still
summarised and committed to the `catchup` data repo (full historical record). The *email* is
scoped by `data/catchup_exclude.txt` (fetched at runtime via the `$RAW` URL): a repo is
included by default and listed in that file to opt out. We started with a topic allow-list
(`catchup-mail`) but switched to a deny-list because new repos should appear without anyone
remembering to tag them, and a reviewable file beats GitHub topic state. The file was seeded
once from the repos lacking the topic, then maintained by hand; the runtime no longer reads
topics. Consequence: the email can't list quiet repos (they produce no summary), so there is
no "Inactive Repositories" section.

**2. Codex → structured JSON → deterministic Python renderer, with deterministic
classification.** Codex emits **prose only** (bullets, executive summary, display names,
patterns); `catchup_render_email.py` turns the merged `report.json` into the email HTML. We
deliberately do **not** let the LLM emit raw HTML (email markup must be Outlook/Gmail/
dark-mode safe and reproducible, and the renderer is unit-testable without an LLM), and we
do **not** let the LLM categorise work. A commit's status — **Published** (on the default
branch), **Testing** (branch with an open PR), **Work in Progress** (branch with no PR) — is
decided deterministically in the per-repo step from branch/PR state, and those statuses are
the email's sections. All numbers (commit counts, versions, PR counts, stats) are likewise
computed, never trusted from Codex.

**3. Split `commit` and `email` into parallel jobs after a merge-only `collect`.** `collect`
produces the daily file and uploads it as an artifact; `commit` (publish to `catchup`) and
`email` (build + send) both consume it and run concurrently. This keeps an email/Resend
failure from ever blocking the data commit, and vice versa.
