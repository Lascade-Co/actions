"""The optional summary may not invent quantities or leak API failures."""
import datetime as dt

import pytest


FACTS = {
    "new_players": 5,
    "traffic_median": 244,
    "next_day_pct": 8.6,
    "since_release_pct": 9.4,
    "stop_line_pct": 15,
    "returners": 96,
    "daily_returners": 85,
    "daily_returners_pct": 88.5416666667,
}
GOOD = (
    "New players fell to 5 while next-day return remains below the 15% stop line. "
    "The latest groups returned at 8.6%, with no real change from the previous groups. "
    "Since the release, next-day return is 9.4%, still short of the level that supports continuing. "
    "The daily puzzle reached 85 of 96 returning players, or 89%, while recent groups are too small to read. "
    "Check whether the paid campaign is still running."
)


def test_validator_accepts_factual_counts_and_rounded_percentages():
    from hr_commentary import validate_summary

    assert validate_summary(GOOD, FACTS) is None
    assert validate_summary(GOOD.replace("89%", "88.5%"), FACTS) is None


@pytest.mark.parametrize("text,reason", [
    (GOOD.replace("8.6%", "12.7%"), "number"),
    (GOOD.replace("5 while", "999 while"), "number"),
    (GOOD.replace("5 while", "5k while"), "number"),
    (GOOD.replace("5 while", "-5 while"), "number"),
    (GOOD.replace("5 while", ".5 while"), "number"),
    (GOOD.replace("New players", "Cherian says new players"), "name"),
    (GOOD.replace("The latest", "Alice says the latest"), "name"),
    (GOOD + " https://example.com", "url"),
    (GOOD.replace("latest", "**latest**"), "markdown"),
    ("Too short.", "length"),
    (GOOD * 3, "length"),
    (GOOD.replace("fell to 5", "fell three times to 5"), "ratio"),
    (GOOD.replace("fell to 5", "fell to five"), "number"),
    (GOOD.replace("Check whether", "We should check whether"), "advice"),
    (GOOD.replace("Check whether", "Check and confirm whether"), "advice"),
    (GOOD.replace(". ", "; "), "sentences"),
])
def test_validator_rejects_unsupported_prose(text, reason):
    from hr_commentary import validate_summary

    assert validate_summary(text, FACTS) == reason


def test_summary_retries_with_only_reason_and_never_logs_facts(monkeypatch, capsys):
    import hr_commentary as C

    prompts = []

    def generate(prompt, **kwargs):
        prompts.append(prompt)
        return GOOD.replace("8.6%", "12.7%") if len(prompts) == 1 else GOOD

    monkeypatch.setattr(C, "generate_text", generate)
    assert C.summarize(FACTS) == GOOD
    assert len(prompts) == 2
    assert "number" in prompts[1]
    assert "12.7%" not in prompts[1]
    assert capsys.readouterr().out == "summary: ok\n"


def test_summary_invalid_twice_falls_back(monkeypatch, capsys):
    import hr_commentary as C

    calls = []

    def generate(prompt, **kwargs):
        calls.append(prompt)
        return "secret short summary"

    monkeypatch.setattr(C, "generate_text", generate)
    assert C.summarize(FACTS) == "Summary unavailable today."
    assert len(calls) == 2
    assert capsys.readouterr().out == "summary: fallback:length\n"


@pytest.mark.parametrize("error,reason", [
    (RuntimeError("private API body and 999 people"), "api"),
    (TimeoutError("private prompt"), "timeout"),
])
def test_summary_api_errors_are_optional_and_sanitized(monkeypatch, capsys, error, reason):
    import hr_commentary as C

    def generate(*args, **kwargs):
        raise error

    monkeypatch.setattr(C, "generate_text", generate)
    assert C.summarize(FACTS) == "Summary unavailable today."
    assert capsys.readouterr().out == f"summary: fallback:{reason}\n"


def test_summary_missing_credentials_uses_safe_claude_reason(monkeypatch, capsys):
    import hr_commentary as C

    def generate(*args, **kwargs):
        raise C.ClaudeError("auth")

    monkeypatch.setattr(C, "generate_text", generate)
    assert C.summarize(FACTS) == "Summary unavailable today."
    assert capsys.readouterr().out == "summary: fallback:auth\n"


def test_summary_skips_when_build_deadline_is_close(monkeypatch, capsys):
    import hr_commentary as C

    def generate(*args, **kwargs):
        pytest.fail("summary should not consume the rendering reserve")

    monkeypatch.setattr(C, "generate_text", generate)
    now = dt.datetime(2026, 11, 19, 13, 47, 50, tzinfo=dt.timezone.utc)
    assert C.summarize(FACTS, deadline=now + dt.timedelta(seconds=10), clock=lambda: now) == C.FALLBACK
    assert capsys.readouterr().out == "summary: fallback:deadline\n"
