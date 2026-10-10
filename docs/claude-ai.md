# Claude access in Actions

All AI jobs use the repository secret `CLAUDE_API_KEY`. They do not restore login
sessions, refresh JWTs, or synchronize AI credentials through Infisical.
Existing Infisical integrations still supply the unrelated build, reporting and CMS credentials.

Content generation uses the official `anthropic` Python SDK, pinned in
`scripts/ai/requirements.txt`, through `scripts/ai/claude_api.py`. Workflows fetch
the helper alongside their domain scripts at the runner's commit. The API receives
only the assembled prompt; it has no local tools or access to the process environment.

| Workflow | Claude use | Failure behavior |
| --- | --- | --- |
| Daily Catchup | Commit classification, developer summaries, executive prose | Deterministic statuses, counts and source bullets remain available. |
| Daily Ad Spend | Optional commentary from computed facts | Commentary is omitted; the report still sends. |
| Release Blog | JSON metadata and HTML draft | Existing SEO validation, one revision and draft-only CMS behavior remain; generation cannot fail the release. |
| Android release | Release notes from the supplied diff | Generation remains best-effort and does not replace existing notes on an API failure. |
| Android debug | Source edits for failing lint through `anthropics/claude-code-action@v1` | The workflow pushes only after a successful action and `Status: SUCCESS` report; otherwise the lint job fails. |

All workflows and standalone Python invocations use `claude-sonnet-5-5`, the
current Sonnet model, directly. No repository model variable is required. Claude
Code receives the key through its `anthropic_api_key` action input, using the
existing secret name.

The SDK retries transient failures twice. API error bodies and generated report
content are never printed by the shared helper. Empty, truncated, refused and
invalid JSON responses trigger the consumer's fallback. Release Blog writes the API
response to the existing `blog.json` and `blog.html` artifacts, then applies the same
validation and CMS gates as before.

Android lint uses read, search and file-edit tools. The workflow owns git operations
and PR comments. It recognizes both old Codex and new Claude auto-fix commit subjects
to prevent a migration-time retry loop. Its report is now `claude-report.md`.

The former `refresh-codex-auth.yml` workflow and all `scripts/codex/` helpers and
tests have been removed. `CODEX_AUTH_JSON_BASE_64` is no longer consumed by this repo;
no replacement refresh workflow or OAuth credential is needed.

Run the offline checks with the test dependencies from `.github/workflows/test-ai.yml`:

```sh
python -m pytest -q scripts/ai scripts/android/test_release_notes.py scripts/adspend scripts/catchup scripts/seo
```

These tests exercise the real SDK against a mocked HTTP transport and verify the
existing report, revision, draft and fallback contracts. They do not make paid API
requests, send reports, or publish changes to other repositories.
