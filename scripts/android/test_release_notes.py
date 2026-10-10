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
def test_failed_generation_preserves_existing_notes(tmp_path, monkeypatch, reason):
    prompt, output = tmp_path / "prompt.md", tmp_path / "releasenotes.txt"
    prompt.write_text("Supplied diff")
    output.write_text("existing notes")

    def fail(*args, **kwargs):
        raise ClaudeError(reason)

    monkeypatch.setattr(notes, "generate_text", fail)
    assert notes.main(["--prompt", str(prompt), "--out", str(output)]) == 1
    assert output.read_text() == "existing notes"
