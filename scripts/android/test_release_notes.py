import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_notes as notes
from claude_api import ClaudeError


def test_generated_notes_replace_the_requested_file(tmp_path, monkeypatch):
    prompt, output = tmp_path / "prompt.md", tmp_path / "releasenotes.txt"
    prompt.write_text("Supplied diff")
    output.write_text("old notes")

    def generate(text, **kwargs):
        assert text == "Supplied diff"
        return "- Fixed route editing."

    monkeypatch.setattr(notes, "generate_text", generate)
    assert notes.main(["--prompt", str(prompt), "--out", str(output)]) == 0
    assert output.read_text() == "- Fixed route editing.\n"


@pytest.mark.parametrize("reason", ["auth", "incomplete-response", "timeout"])
def test_failed_generation_falls_back_without_failing_the_release(tmp_path, monkeypatch, reason, capsys):
    prompt, output = tmp_path / "prompt.md", tmp_path / "releasenotes.txt"
    prompt.write_text("Supplied diff")
    output.write_text("existing notes")

    def fail(*args, **kwargs):
        raise ClaudeError(reason)

    monkeypatch.setattr(notes, "generate_text", fail)
    assert notes.main(["--prompt", str(prompt), "--out", str(output)]) == 0
    assert output.read_text() == "Bug fixes and performance improvements\n"
    assert "::warning" in capsys.readouterr().out


def test_overlong_notes_are_rewritten_by_claude_never_truncated(tmp_path, monkeypatch):
    prompt, output = tmp_path / "prompt.md", tmp_path / "releasenotes.txt"
    prompt.write_text("Supplied diff")
    calls = []

    def generate(text, **kwargs):
        calls.append(text)
        return "- " + "Long sentence about a feature. " * 20 if len(calls) == 1 else "- Shorter, complete notes."

    monkeypatch.setattr(notes, "generate_text", generate)
    assert notes.main(["--prompt", str(prompt), "--out", str(output)]) == 0
    assert output.read_text() == "- Shorter, complete notes.\n"
    assert "Rewrite required" in calls[1] and "at most 500" in calls[1]


def test_notes_still_too_long_fall_back(tmp_path, monkeypatch):
    prompt, output = tmp_path / "prompt.md", tmp_path / "releasenotes.txt"
    prompt.write_text("Supplied diff")
    output.write_text("existing notes")
    monkeypatch.setattr(notes, "generate_text", lambda text, **kwargs: "x" * 501)
    assert notes.main(["--prompt", str(prompt), "--out", str(output)]) == 0
    assert output.read_text() == "Bug fixes and performance improvements\n"


def test_length_counts_emoji_as_two_and_check_mode_rejects_overlong_files(tmp_path):
    assert notes.play_length("🚀") == 2
    ok, bad = tmp_path / "ok.txt", tmp_path / "bad.txt"
    ok.write_text("🚀" * 250)
    bad.write_text("🚀" * 250 + "a")
    assert notes.main(["--check", str(ok)]) == 0
    assert notes.main(["--check", str(bad)]) == 1
