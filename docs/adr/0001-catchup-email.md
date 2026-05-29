# Daily Catchup email: tag filter, rendering split, and parallel jobs

Three decisions made when adding the daily email to `daily-catchup.yml`:

**1. Email-only tag filter via a per-repo `tags` field.** Every active repo is still
summarised and committed to the `catchup` data repo (full historical record). The *email*
is scoped to repos carrying the `catchup-mail` GitHub topic. We carry each repo's topics in
the merged daily file (`tags`) rather than computing a tagged set in `discover`, so the
filter lives entirely in the email step. Consequence: the email cannot list tagged-but-quiet
repos (they produce no summary), so there is no "Inactive Repositories" section.

**2. Codex → structured JSON → deterministic Python renderer.** Codex emits prose and
section structure only (`report.json`); `catchup_render_email.py` turns that into the email
HTML. We deliberately do **not** let the LLM emit raw HTML — email markup must be
Outlook/Gmail/dark-mode safe and reproducible, and the renderer is unit-testable without an
LLM or network. All numbers (commit counts, versions, PR counts, stats) are injected
deterministically, never trusted from Codex — the same principle `catchup_repo.py` already
applies to bullets.

**3. Split `commit` and `email` into parallel jobs after a merge-only `collect`.** `collect`
produces the daily file and uploads it as an artifact; `commit` (publish to `catchup`) and
`email` (build + send) both consume it and run concurrently. This keeps an email/Resend
failure from ever blocking the data commit, and vice versa.
