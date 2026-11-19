"""Behaviour checks for the UTC retention model and the approved email layout."""
from datetime import date, timedelta
from html import unescape
import json
import re

import pytest

from preview_fixture import REPORT_DATE, preview_inputs


def build(*, report_date=REPORT_DATE, groups=(), days=(), **kwargs):
    from hr_model import build_model
    return build_model(report_date, groups=list(groups), days=list(days), **kwargs)


def group(day, n=100, **counts):
    row = {name: 0 for name in ("d1", "d3", "d7any", "d7", "days3", "paid", "returned", "returned_daily")}
    return {"d0": day, "n": n, **row, **counts}


def engagement(day, people=100, **counts):
    return {"day": day, "people": people, "players": 10, "hunts": 30,
            "finished": 20, "won": 10, "daily_players": 5, **counts}


def text_of(html):
    return unescape(re.sub(r"<[^>]*>", " ", html))


def test_unclosed_windows_are_pending_and_excluded_from_pooled_rates():
    model = build(groups=[
        group("2026-11-11", n=40, d1=4, d3=8, d7any=12, d7=2, days3=3),
        group("2026-11-15", n=50, d1=10, d3=20, d7any=30, d7=5, days3=8),
        group("2026-11-18", n=100, d1=100, d3=100, d7any=100, d7=100, days3=100),
    ])
    rows = {row["d0"]: row for row in model["groups"]}
    assert rows[date(2026, 11, 18)]["metrics"]["d1"]["pending"] is True
    assert rows[date(2026, 11, 15)]["metrics"]["d3"]["pending"] is False
    assert rows[date(2026, 11, 15)]["metrics"]["d7any"]["pending"] is True
    assert model["since"]["d1"] == {"k": 14, "n": 90, "rate": 14 / 90}
    assert model["since"]["d7any"] == {"k": 12, "n": 40, "rate": 0.3}
    assert model["since"]["days3"] == {"k": 3, "n": 40, "rate": 0.075}


def test_before_release_d1_uses_its_own_one_day_window():
    model = build(report_date=date(2026, 11, 9), groups=[group("2026-11-07", n=100, d1=20)])
    assert model["before"]["d1"] == {"k": 20, "n": 100, "rate": 0.2}
    assert model["before"]["d7"] == {"k": 0, "n": 0, "rate": None}


@pytest.mark.parametrize(("k", "expected"), [(14, "stop"), (15, "continue"), (29, "continue"), (30, "good")])
def test_pooled_verdict_has_inclusive_band_edges(k, expected):
    model = build(groups=[group("2026-11-11", n=90, d1=k), group("2026-11-17", n=10)])
    assert model["verdict"]["current"] == {"k": k, "n": 100, "rate": k / 100}
    assert model["verdict"]["band"] == expected


def test_wilson_overlap_prevents_claiming_a_change():
    model = build(groups=[group("2026-11-04", d1=20), group("2026-11-11", d1=25)])
    assert model["verdict"]["change"] == "no real change"


@pytest.mark.parametrize(("previous", "current", "change"), [(100, 500, "up"), (500, 100, "down")])
def test_disjoint_wilson_intervals_report_direction(previous, current, change):
    model = build(groups=[group("2026-11-04", n=1000, d1=previous), group("2026-11-11", n=1000, d1=current)])
    assert model["verdict"]["change"] == change


def test_checks_use_eight_day_traffic_input_in_the_required_order():
    days = [engagement(f"2026-11-{d:02}", people=p) for d, p in zip(range(11, 19), (500, 100, 200, 300, 400, 600, 700, 20))]
    days[-1].update(finished=10, won=9)
    model = build(groups=[group("2026-11-16", n=40), group("2026-11-17", n=10)], days=days)
    assert model["traffic_median"] == 400
    assert [check["kind"] for check in model["checks"]] == ["traffic", "difficulty", "thin", "no-new-players"]
    assert "median of 400" in model["checks"][0]["text"]
    assert "Paid new players: 0 of 0" in model["checks"][0]["text"]
    assert "17 Nov onward" in model["checks"][2]["text"]
    assert len(model["days"]) == 7
    assert model["days"][0]["day"] == date(2026, 11, 12)


def test_checks_do_not_fire_on_exact_traffic_boundary_or_subthreshold_difficulty():
    days = [engagement(f"2026-11-{d:02}") for d in range(11, 19)]
    days[-1].update(people=50, finished=100, won=89)
    model = build(groups=[group("2026-11-18", n=30)], days=days)
    assert model["checks"] == []


def test_subject_and_preview_headline_figures_match_approved_preview():
    from hr_render_email import render
    model = build(**preview_inputs())
    subject, html, text = render(model)
    assert subject == "TreasureHunch · Wed 18 Nov: next-day return 8.6% (stop line 15%) · new players 5, was ~113"
    assert model["verdict"]["current"] == {"k": 58, "n": 675, "rate": 58 / 675}
    assert model["since"]["d1"] == {"k": 69, "n": 732, "rate": 69 / 732}
    assert model["since"]["d7any"] == {"k": 33, "n": 149, "rate": 33 / 149}
    assert model["since"]["d7"] == {"k": 2, "n": 149, "rate": 2 / 149}
    assert model["daily_returners"] == {"k": 85, "n": 96, "rate": 85 / 96}
    visible = " ".join(text_of(html).split())
    for figure in ("8.6%", "58 of 675", "9.4%", "69 of 732", "22.1%", "33 of 149", "1.3%", "2 of 149", "85 of 96"):
        assert figure in visible
        assert figure in text


@pytest.mark.parametrize(("k", "word"), [(15, "worth continuing 15%"), (30, "good 30%")])
def test_manual_subject_uses_current_verdict_band(k, word):
    from hr_render_email import render
    subject, html, _ = render(build(groups=[group("2026-11-11", d1=k)], test_number=4))
    assert subject.startswith("[Test 4] TreasureHunch · Wed 18 Nov:")
    assert f"({word})" in subject
    assert "Stop line." not in html


@pytest.mark.parametrize("test_number", [None, 42])
def test_subject_is_preserved_in_title_without_a_visible_body_paragraph(test_number):
    from hr_render_email import render
    subject, html, _ = render(build(**preview_inputs(), test_number=test_number))
    title = re.search(r"<title>(.*?)</title>", html, re.S).group(1)
    body = re.search(r"<body\b[^>]*>(.*?)</body>", html, re.S).group(1)
    assert unescape(title) == subject
    assert "Subject:" not in text_of(body)


def test_render_contains_every_section_and_escapes_external_text():
    from hr_render_email import render
    model = build(**preview_inputs())
    model["summary"] = "<script>private</script> & an executive summary."
    _, html, text = render(model)
    for heading in ("Retention", "The read", "Check first", "Did the 8 Nov release work?",
                    "Player groups since 8 Nov", "Engagement, last 7 days", "What changed on Wed 18 Nov"):
        assert heading in html
        assert heading in text
    assert "&lt;script&gt;private&lt;/script&gt;" in html
    assert "<script>private</script>" not in html
    assert "None" not in html
    assert "nan" not in html.lower()


def test_empty_data_renders_without_division_errors_or_false_temporal_claims():
    from hr_render_email import render
    model = build()
    _, html, text = render(model)
    assert model["verdict"]["band"] == "unclear"
    assert "pending" in html
    assert "None" not in html
    assert "nan" not in html.lower()
    assert "first store-app installs" not in text
    assert "almost all Instagram ads in India" not in text
    assert "It is the reason people return." not in text
    assert "No new events first seen" in text


def test_new_versions_add_markers_and_changed_section_only_shows_last_day():
    from hr_render_email import render
    model = build(new_versions=[
        {"day": "2026-11-14", "platform": "Web", "version": "0.2.1"},
        {"day": "2026-11-18", "platform": "iOS", "version": "0.1.3"},
    ])
    rows = {row["d0"]: row for row in model["groups"]}
    assert "Web 0.2.1" in rows[date(2026, 11, 14)]["marker"]
    assert "Android + iOS apps" in rows[date(2026, 11, 18)]["marker"]
    assert "iOS 0.1.3" in rows[date(2026, 11, 18)]["marker"]
    assert len(model["new_versions"]) == 1
    _, _, text = render(model)
    changed = text.split("What changed on Wed 18 Nov", 1)[1]
    assert "iOS 0.1.3" in changed
    assert "Web 0.2.1" not in changed


def test_facts_are_json_safe_and_distinguish_percentages_from_counts():
    model = build(**preview_inputs())
    facts = model["facts"]
    json.dumps(facts, allow_nan=False)
    assert facts["verdict"]["current"]["rate_pct"] == pytest.approx(100 * 58 / 675)
    assert facts["daily_returners"]["k"] == 85
    assert facts["daily_returners"]["n"] == 96
    assert facts["new_players_baseline"] == 113
