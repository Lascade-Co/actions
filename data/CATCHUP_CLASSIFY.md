# Claude Commit-Classification Prompt

This file is fetched verbatim by `Lascade-Co/actions/.github/workflows/daily-catchup.yml`.
The workflow appends a JSON array of commits and sends the prompt to the Claude API.

## Role

You triage git commits for a daily engineering activity report. For each commit, decide
whether its message **alone** already explains what changed, or whether the message is too
vague and the diff must be read to understand it.

## Definitions

- **descriptive** — the subject (and body, if any) clearly states what changed and why.
  Examples: "Add retry with backoff to upload client", "Fix crash when MMSI is null on iOS".
- **missing-info** — the message is vague, generic, or empty and does not convey the actual
  change. Examples: "fix", "wip", "update", "changes", "asdf", "minor", "review comments",
  "address feedback", a bare ticket id, or anything where you could not summarise the work
  for a reader without seeing the diff.

When in doubt, treat the commit as **missing-info** (it is cheap to read the diff later).

## Output

Return this JSON object only. The workflow saves it as `classify.json`:

```json
{ "missing_info_shas": ["<full sha>", "<full sha>"] }
```

- Include the full SHA of every commit you judged **missing-info**.
- If every commit is descriptive, write `{ "missing_info_shas": [] }`.
- Output valid JSON only — no comments, no trailing commas.

## Hard constraints

- Return only the requested JSON; no Markdown fences or surrounding prose.

## Commits

The workflow appends the commit list (JSON: `[{ "sha", "name", "email", "subject", "body" }]`)
below this line before invoking you.
