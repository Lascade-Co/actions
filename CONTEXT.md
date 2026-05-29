# Daily Catchup

The org-wide daily engineering report pipeline (`.github/workflows/daily-catchup.yml`):
discover active repos → summarise each with Codex → merge into one daily file → commit it
to the `catchup` repo and email a styled report.

## Language

**Catchup**:
The daily pipeline (and the `Lascade-Co/catchup` data repo) that captures the org's last 24h
of engineering activity.
_Avoid_: digest, standup.

**Daily file**:
The merged `daily/YYYY-MM-DD.json` — one entry per active repo with its developers, bullets,
and enrichment. The single source the commit and email steps both consume.
_Avoid_: report (that's the email), summary (that's per-repo).

**Per-repo summary**:
One repo's `summary-*.json` artifact: developers with Codex bullets plus enrichment.
Produced by the `summarize` matrix job.

**Enrichment**:
The non-commit signals attached to each repo summary — merged **PRs**, active **branches**,
the in-window release **version** tag, and the repo's **tags**. All best-effort.

**Tags**:
A repo's GitHub **topics**, carried verbatim in the daily file. The email includes a repo
only when its tags contain `catchup-mail`.
_Avoid_: labels (those are on issues/PRs), topics (use "tags" in this codebase).

**Active repo**:
A repo with non-bot commits in the look-back window — the only repos that get summarised and
appear in the daily file. (There is no "inactive" list in the email.)

**Report JSON**:
The email's intermediate `report.json` — Codex prose (executive summary, per-repo sections,
patterns) merged with authoritative numbers. The renderer turns it into the HTML email.

**Authoritative**:
A value computed from git/GitHub and never trusted from Codex (commit counts, contributor
list, PR count, version, branches, stats). Codex supplies prose only.

## Relationships

- The **discover** job lists **active repos**; **summarize** turns each into a **per-repo
  summary** with **enrichment**.
- **collect** merges all per-repo summaries into the one **daily file**.
- **commit** publishes the daily file to the **catchup** repo; **email** filters it by
  **tags** (`catchup-mail`), builds the **report JSON**, and sends it. commit and email run
  in parallel.

## Example dialogue

> **Dev:** "Does tagging a repo `catchup-mail` stop other repos from being tracked?"
> **Maintainer:** "No — every active repo is still summarised and committed to the daily
> file. The tag only decides what shows up in the *email*."

## Flagged ambiguities

- "summary" meant both the per-repo artifact and the emailed report — resolved: **per-repo
  summary** is the artifact, **report JSON** / email is the org-wide output.
