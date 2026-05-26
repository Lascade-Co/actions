# Codex Commit-Classification Prompt

This file is fetched verbatim by `Lascade-Co/actions/.github/workflows/daily-catchup.yml`.
The workflow appends a JSON array of commits below this line and hands the result to the
`@openai/codex` CLI (`codex exec --sandbox workspace-write`).

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

Write a single file `classify.json` at the current working directory, and modify NOTHING else:

```json
{ "missing_info_shas": ["<full sha>", "<full sha>"] }
```

- Include the full SHA of every commit you judged **missing-info**.
- If every commit is descriptive, write `{ "missing_info_shas": [] }`.
- Output valid JSON only — no comments, no trailing commas.

## Hard constraints

- DO NOT run `git`, `gradle`, build, test, or network commands. Only write `classify.json`.
- DO NOT create commits.

## Commits

The workflow appends the commit list (JSON: `[{ "sha", "name", "email", "subject", "body" }]`)
below this line before invoking you.
