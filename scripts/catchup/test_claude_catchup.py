import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import catchup_repo as repo
import catchup_report as report
from claude_api import ClaudeError


@pytest.mark.parametrize("module", [repo, report])
def test_api_json_becomes_the_same_local_artifact(tmp_path, monkeypatch, module):
    template = tmp_path / "template.md"
    template.write_text("Task instructions")
    (tmp_path / "result.json").write_text("stale output")

    def generate(prompt, **kwargs):
        assert "Task instructions" in prompt and "Supplied payload" in prompt
        return {"result": "fresh"}

    monkeypatch.setattr(module, "generate_json", generate)
    assert module.run_claude(template, "Supplied payload", tmp_path, "result.json") == {"result": "fresh"}
    assert json.loads((tmp_path / "result.json").read_text()) == {"result": "fresh"}


@pytest.mark.parametrize("reason", ["auth", "timeout", "invalid-json", "incomplete-response"])
def test_report_keeps_all_work_and_sends_after_api_failure(tmp_path, monkeypatch, reason):
    daily, output, gh_output = tmp_path / "daily.json", tmp_path / "report.json", tmp_path / "github-output"
    daily.write_text(json.dumps({"date": "2026-10-10", "repos": [{
        "repo": "Example/app", "prs": [], "branches": [],
        "developers": [{"name": "Ada", "commit_count": 1,
                        "bullets": {"Published": ["Fixed route editing"]}}],
    }]}))
    monkeypatch.setattr(sys, "argv", ["catchup_report.py", "--daily", str(daily),
                                    "--report-prompt", "unused", "--out", str(output)])
    monkeypatch.setenv("GITHUB_OUTPUT", str(gh_output))

    def fail(*args, **kwargs):
        raise ClaudeError(reason)

    monkeypatch.setattr(report, "run_claude", fail)
    report.main()
    result = output.read_text()
    assert "Fixed route editing" in result
    assert "send=true" in gh_output.read_text()


def test_repo_classification_and_summary_fall_back_without_api(tmp_path, monkeypatch):
    output = tmp_path / "summary.json"
    monkeypatch.setattr(sys, "argv", ["catchup_repo.py", "--repo", "Example/app",
                                    "--classify-prompt", "unused", "--summary-prompt", "unused",
                                    "--out", str(output)])
    commit = {"sha": "a" * 40, "name": "Ada", "email": "ada@example.invalid",
              "subject": "Fixed route editing", "body": ""}
    monkeypatch.setattr(repo, "clone", lambda *args: None)
    monkeypatch.setattr(repo, "collect_commits", lambda *args: [commit])
    monkeypatch.setattr(repo, "resolve_logins", lambda _repo, devs: devs)
    monkeypatch.setattr(repo, "default_branch_name", lambda *args: "main")
    monkeypatch.setattr(repo, "classify_status", lambda *args: {commit["sha"]: "Published"})
    monkeypatch.setattr(repo, "build_summary_payload", lambda *args: {})
    monkeypatch.setattr(repo, "gather_prs", lambda *args: [])
    monkeypatch.setattr(repo, "gather_branches", lambda *args: [])
    monkeypatch.setattr(repo, "gather_version", lambda *args: None)

    def fail(*args, **kwargs):
        raise ClaudeError("auth")

    monkeypatch.setattr(repo, "run_claude", fail)
    repo.main()
    dev = json.loads(output.read_text())["developers"][0]
    assert dev["commit_count"] == 1
    assert dev["bullets"]["Published"] == ["• Fixed route editing"]
