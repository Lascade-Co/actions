"""Offline tests for the install->paid email. Synthetic figures only: the repository is
public, so no real counts belong here.

    cd scripts/installpaid && python3 -m pytest -q test_installpaid.py
"""
import datetime as dt
import io
import json
import os
import re
import sys
from contextlib import redirect_stdout, redirect_stderr

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ip_fetch as F  # noqa: E402
import ip_model as M  # noqa: E402
import ip_render_email as R  # noqa: E402
from test_ip_delivery import TestEditionDelivery  # noqa: E402,F401
from test_ip_crosscheck import TestMuscatCrosscheck  # noqa: E402,F401
from test_ip_sender import TestSender  # noqa: E402,F401
from test_ip_deadline import TestAbsoluteDeadline  # noqa: E402,F401
from test_ip_workflow import TestDailyWorkflow  # noqa: E402,F401

D0 = dt.date(2026, 10, 1)
CIDRS = ["70.32.128.0/19", "2001:4860::/32"]


def row(v, d, n, paid=0, pc=None, pw=None, pv=None, ex=None, paid_pv=None, cc="US", gip=False, emu=False, miss=False):
    pv = n // 2 if pv is None else pv
    return {"v": v, "cc": cc, "gip": gip, "emu": emu, "miss": miss, "d": (D0 + dt.timedelta(days=d)).isoformat(),
            "n": n, "pw": n if pw is None else pw, "pv": pv, "ex": pv // 2 if ex is None else ex,
            "exr": pv // 2 if ex is None else ex, "paid": paid, "pc": paid if pc is None else pc,
            "paid_pv": paid if paid_pv is None else paid_pv, "pc_pv": paid if paid_pv is None else paid_pv}


def bench(ios_rate=0.05, and_rate=0.01, aug_android_paid=0):
    def agg(n, paid, pc):
        return {"counts": {"n": n, "pw": n * 9 // 10, "pv": n * 7 // 10, "ex": n * 3 // 10, "exr": n * 3 // 10,
                           "paid": paid, "pc": pc, "paid_pv": paid * 9 // 10, "pc_pv": pc * 9 // 10},
                "countries": {"US": n // 2, "IN": n // 2}, "test": 0, "all": n}
    return {"definition": "test", "cidr_date": "2026-10-19",
            "aug25": {"ios": agg(10000, int(10000 * ios_rate), int(10000 * ios_rate)),
                      "android": agg(10000, aug_android_paid, int(10000 * and_rate))},
            "mar26": {"ios": agg(10000, int(10000 * ios_rate), int(10000 * ios_rate)),
                      "android": agg(10000, int(10000 * and_rate), int(10000 * and_rate))}}


def model(raw, b=None):
    return M.build_model(raw, b or bench(), dt.date(2026, 10, 20), dt.datetime(2026, 10, 20, 12), "2026-10-19", "test")


def track(m, key):
    return next(t for t in m["tracks"] if t["key"] == key)


# ---------------------------------------------------------------- SQL ----

def test_cohort_sql_window_and_dedupe():
    s, e, c = dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 15), dt.datetime(2026, 9, 20)
    q = F.cohort_sql(s, e, c, "ios", CIDRS)
    assert "minIf(timestamp, event = 'Application Installed') AS t0" in q          # first install wins
    assert "toDateTime('2026-03-05 00:00:00', 'UTC')" in q                            # 180-day lookback
    assert "timestamp < toDateTime('2026-09-16 00:00:00', 'UTC')" in q               # scan = end + 24h
    assert q.count("x < t0 + toIntervalHour(24)") >= 5                               # 24h reach everywhere
    assert "LIMIT 50000" in q and "('iOS','iPadOS')" in q


def test_cohort_sql_refuses_immature_installs():
    try:
        F.cohort_sql(dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 20), dt.datetime(2026, 9, 20, 12), "ios", CIDRS)
    except ValueError:
        return
    raise AssertionError("an install younger than 24h must not be counted")


def test_export_must_follow_first_preview_and_paid_is_independent():
    q = F.cohort_sql(dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 2), dt.datetime(2026, 9, 5), "android", CIDRS)
    assert "pv1 AND arrayExists(x -> x >= fp" in q                    # export before the first preview: not counted
    assert "arrayMin(arrayFilter(x -> x >= t0 AND x < t0 + toIntervalHour(24), a_pv)) AS fp" in q
    assert "arrayExists(x -> x >= t0 AND x < t0 + toIntervalHour(24), a_paid) AS paid1" in q   # pay-before-preview counts
    assert "countIf(paid1 AND pv1) AS paid_pv" in q


def test_first_install_attributes_ignore_nulls_of_later_installs():
    q = F.cohort_sql(dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 2), dt.datetime(2026, 9, 5), "ios", CIDRS)
    for prop in ("$app_version", "$geoip_country_code", "$os", "$device_model", "$ip"):
        assert f"argMinIf(ifNull(toString(properties.{prop}), ''), timestamp" in q


def test_test_device_flags():
    q = F.cohort_sql(dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 2), dt.datetime(2026, 9, 5), "android", CIDRS)
    assert "startsWith(ifNull(toString(model), ''), 'sdk_')" in q and "LIKE" not in q
    assert "isIPAddressInRange(if(ip = '', '0.0.0.0', ip), '70.32.128.0/19')" in q


# --------------------------------------------------------------- model ----

def test_tracks_and_builds():
    assert M.track_of("ios", "4.1.1") == "ios"
    assert M.track_of("android", "4.0.155") == "android4"
    assert M.track_of("android", "5.0.8") == "android5"
    assert M.track_of("android", "3.4.93") is None and M.track_of("android", "6.0.0") is None
    assert M.is_real_build("4.0.155")
    assert not M.is_real_build("5.0.8-billing-test") and not M.is_real_build("4.1-debug") and not M.is_real_build("")


# Report date 20 Oct, cutoff 05:00 PDT -> latest full day Sun 18 Oct (D0 + 17), week 12-18 Oct (D0 + 11 .. 17).
LAST, FIRST = 17, 11


def test_test_installs_are_audited_and_dropped():
    raw = {"ios": [], "android": [row("4.0.9", LAST, 13, gip=True), row("4.0.9", LAST, 40)]}
    kept, audit = M.split(raw)
    assert audit["android4"]["test"] == 13 and sum(r["n"] for r in kept["android4"]) == 40


def test_latest_full_day_for_both_editions_across_dst():
    import ip_main
    cases = [("pt", "2026-10-20", "2026-10-18"), ("ist", "2026-10-20", "2026-10-17"),
             ("pt", "2026-03-09", "2026-03-07"), ("pt", "2026-11-02", "2026-10-31"),
             ("ist", "2026-03-09", "2026-03-06"), ("ist", "2026-11-02", "2026-10-30")]
    for edition, rd, want in cases:
        got = M.latest_full_day(ip_main.cutoff_for(dt.date.fromisoformat(rd), edition))
        assert got == dt.date.fromisoformat(want), (edition, rd, got)


def test_week_is_fixed_and_covers_every_build():
    raw = {"ios": [row("4.2.0", LAST, 400, paid=20, cc="DE"), row("4.1.0", FIRST, 300, paid=15, cc="FR"),
                   row("4.1.0", FIRST + 2, 100, paid=1, cc="DE"),
                   row("4.0.0", FIRST - 1, 900, paid=45, cc="JP"),      # the day before the week: left out
                   row("4.2.0", LAST + 1, 700, paid=35, cc="BR")],      # the partial day after: left out
           "android": []}
    m = model(raw)
    t = track(m, "ios")
    assert (m["first_day"], m["last_day"]) == (D0 + dt.timedelta(days=FIRST), D0 + dt.timedelta(days=LAST))
    assert t["counts"]["n"] == 800 and t["counts"]["paid"] == 36
    assert [b["v"] for b in t["builds"]] == ["4.2.0", "4.1.0"]            # newest version first
    assert t["day"]["n"] == 400 and t["day"]["paid"] == 20
    assert [(p["v"], p["n"], p["paid"]) for p in t["trend"]] == [("4.1.0", 400, 16), ("4.2.0", 400, 20)]
    assert [(x["cc"], x["now"]) for x in t["mix"]] == [("DE", 500 / 800), ("FR", 300 / 800)]


def test_by_build_needs_30_installs_and_keeps_the_newest_8():
    rows = [row(f"4.1.{i}", LAST, 40) for i in range(10)] + [row("4.2.0", LAST, 29)]
    t = track(model({"ios": rows, "android": []}), "ios")
    assert [p["v"] for p in t["trend"]] == [f"4.1.{i}" for i in range(2, 10)]     # 4.2.0 has under 30


def test_verdicts_are_given_with_few_payers():
    t = track(model({"ios": [], "android": [row("5.0.8", LAST, 300, paid=2)]}), "android5")
    assert t["cmp"]["mar26"]["paid"]["verdict"] in ("better", "worse", "same", "unclear")
    assert all(t["cmp"]["mar26"][s]["verdict"] is not None for s in ("pw", "pv", "ex"))


def test_zero_march_rate_still_compares():
    b = bench()
    b["mar26"]["android"]["counts"].update(paid=0, paid_pv=0)
    t = track(model({"ios": [], "android": [row("5.0.8", LAST, 300, paid=2)]}, b), "android5")
    assert not t["empty"] and t["cmp"]["mar26"]["paid"] is not None


def test_small_builds_are_counted_not_hidden():
    m = model({"ios": [], "android": [row("5.0.9", LAST, 3), row("5.0.8", LAST, 300, paid=2)]})
    html, text = R.builds_txt(track(m, "android5"))
    assert text == "5.0.8 +1 small build (3 installs)"
    m = model({"ios": [row(f"4.1.{i}", LAST, 50) for i in range(5)] + [row("4.0.1", LAST, 1), row("4.0.2", LAST, 2)],
               "android": []})
    assert R.builds_txt(track(m, "ios"))[1] == "4.1.4 · 4.1.3 · 4.1.2 +2 more +2 small builds (3 installs)"


def test_empty_tracks_render(tmp_path):
    outside = row("4.1.0", FIRST - 3, 900, paid=45)
    for raw in ({"ios": [], "android": [row("4.0.150", LAST, 900, paid=9)]},       # empty iOS
                {"ios": [row("4.1.0", LAST, 900, paid=45)], "android": []},       # empty Android
                {"ios": [], "android": []},                                       # all empty
                {"ios": [outside], "android": []}):                               # rows only outside the week
        m = model(raw)
        subj, html, text, _ = R.render(m, "A", tmp_path)
        empty = [t for t in m["tracks"] if t["empty"]]
        assert empty and all(f"{t['label']} —" in subj for t in empty)
        assert "No installs on 12–18 Oct." in html and "no installs on 12–18 Oct" in text


def test_zero_install_last_day_shows_a_dash(tmp_path):
    m = model({"ios": [row("4.1.0", FIRST, 900, paid=45)], "android": []})
    t = track(m, "ios")
    assert not t["empty"] and R.day_rate(t) == "—"
    subj, html, _, _ = R.render(m, "A", tmp_path)
    assert "iOS —" in subj and "Sun 18 Oct ·" in html


def test_android_aug25_compares_client_with_client(tmp_path):
    m = model({"ios": [], "android": [row("4.0.150", LAST, 9000, paid=90, pc=120)]},     # server and client diverge
              bench(and_rate=0.01, aug_android_paid=0))
    t = track(m, "android4")
    assert t["cmp"]["aug25"]["client_basis"] and not t["cmp"]["mar26"]["client_basis"]
    aug = t["cmp"]["aug25"]["paid"]
    assert aug["verdict"] is None                                      # directional only
    assert abs(aug["diff"] - (120 / 9000 - 0.01)) < 1e-9               # client vs client
    assert t["cmp"]["mar26"]["paid"]["verdict"] is not None
    _, html, text, _ = R.render(m, "A", tmp_path)
    assert "Aug: see †" in R.step_verdict(t, "paid")[1]
    assert "Paid: 1.00% · Mar ±0.00 pts · Aug —†" in text               # server row: no Aug figure
    assert "Paid, in-app event †: 1.33% · Mar +0.33 pts · Aug +0.33 pts" in text
    row_html = html.split("Paid, in-app event †")[1].split("</tr>")[0]
    assert f'bgcolor="{R.C_THIN}"' in row_html                          # the directional bar itself is grey


def test_read_block_combines_or_splits_benchmarks():
    m = model({"ios": [row("4.1.0", LAST, 20000, paid=1000, pw=19000, pv=18000, ex=9000)], "android": []})
    t = track(m, "ios")
    t["cmp"]["mar26"]["pv"]["verdict"], t["cmp"]["aug25"]["pv"]["verdict"] = "better", "better"
    assert R.step_verdict(t, "pv")[1] == "better vs both"
    t["cmp"]["aug25"]["pv"]["verdict"] = "same"
    assert R.step_verdict(t, "pv")[1] == "better vs Mar, same vs Aug"
    t["cmp"]["mar26"]["pv"]["verdict"] = "unclear"
    assert R.step_verdict(t, "pv")[1] == "can't tell yet vs Mar, same vs Aug"


def test_check_line_flags_a_step_under_half_of_march(tmp_path):
    m = model({"ios": [row("4.1.0", LAST, 1000, paid=50, pw=300)], "android": []})   # Mar paywall 90%
    _, html, text, _ = R.render(m, "A", tmp_path)
    assert "Check: saw the paywall 30.0% vs Mar 90.0%. Investigate." in text and "Check:" in html


def test_week_formatting_crosses_months_and_years():
    d = dt.date
    assert R.week_txt(d(2030, 10, 12), d(2030, 10, 18)) == "12–18 Oct"
    assert R.week_txt(d(2030, 10, 26), d(2030, 11, 1)) == "26 Oct–1 Nov"
    assert R.week_txt(d(2026, 12, 29), d(2027, 1, 4)) == "29 Dec 2026–4 Jan 2027"


def test_conditionals_use_person_level_counts():
    r = M.rates({"n": 1000, "pw": 900, "pv": 600, "ex": 300, "paid": 50, "pc": 50, "paid_pv": 45})
    assert r["pv_ex"][0] == 0.5 and r["paid_if_pv"][0] == 45 / 600 and r["paid_if_nopv"][0] == 5 / 400


def test_verdict_rules():
    assert M.verdict(0.01, 0.002, 0.02, 0.05) == "better"
    assert M.verdict(-0.01, -0.02, -0.001, 0.05) == "worse"
    assert M.verdict(0.0, -0.004, 0.004, 0.05) == "same"
    assert M.verdict(0.0, -0.02, 0.02, 0.05) == "unclear"


# --------------------------------------------------------------- render ----

def test_render_variant_a(tmp_path):
    raw = {"ios": [row("4.1.1", LAST, 500, paid=30), row("4.1.0", FIRST, 500, paid=22)],
           "android": [row("4.0.155", LAST, 6000, paid=70), row("5.0.8", LAST, 200, paid=1)]}
    m = model(raw)
    subj, html, text, imgs = R.render(m, "A", tmp_path)
    assert subj == "TA install→paid · Sun 18 Oct: iOS 6.0% · Android 5.x 0.50% · Android 4.0.x 1.17%"
    assert "<img" not in html and not imgs
    assert "App Store · iOS" in html and "Play Store · Android 5.x" in text
    assert "latest full day Sun 18 Oct · last 7 days 12–18 Oct" in html and "7-day verdicts vs Mar 2026 / Aug 2025" in text
    assert "† Android Aug 2025" in text and "data to 05:00 PT" in text
    assert "pool" not in (html + text).lower() and "today" not in (html + text).lower()
    # one class attribute per tag (a duplicate drops the dark-mode class), and every role colour has a dark override
    assert not re.search(r'<[^>]*\bclass="[^"]*"[^>]*\bclass="', html)
    dark = " ".join(re.findall(r"@media \(prefers-color-scheme: dark\) \{(.*?)\n    \}", html, re.S))
    used = set(re.findall(r'class="[^"]*?\b(hl-[a-z]+|tick-[a-z]+-dm)\b', html))
    assert {"hl-num", "hl-ctx", "hl-lvl", "tick-aug-dm"} <= used
    for cls in used:
        assert f".{cls} {{" in dark, cls                                # inside the dark media query
        assert f"[data-ogsc] .{cls} {{" in html, cls                    # and the Outlook dark override


def test_render_variant_b(tmp_path):
    raw = {"ios": [row("4.1.1", LAST, 500, paid=30), row("4.1.0", FIRST, 500, paid=22)],
           "android": [row("4.0.155", LAST, 6000, paid=70), row("5.0.8", LAST, 200, paid=1)]}
    _, html_b, _, imgs_b = R.render(model(raw), "B", tmp_path)
    # funnel for every track; a by-build trend only where >= 2 builds (iOS here)
    assert len(imgs_b) == 4 and all(os.path.exists(i["path"]) for i in imgs_b)
    assert all(i["name"] in html_b for i in imgs_b)
    assert "Previewed a trip</td>" in html_b                            # values as text, not only in the PNG


def test_ist_edition_labels_its_cutoff(tmp_path):
    import ip_main
    m = M.build_model({"ios": [row("4.1.0", 16, 900, paid=45)], "android": []}, bench(), dt.date(2026, 10, 20),
                      ip_main.cutoff_for(dt.date(2026, 10, 20), "ist"), "2026-10-19", "test")
    m["edition"] = "ist"
    subj, html, text, _ = R.render(m, "A", tmp_path)
    assert subj.startswith("TA install→paid · Sat 17 Oct:") and "data to 05:00 IST" in text
    assert "last 7 days 11–17 Oct" in html


def test_logs_carry_no_figures(tmp_path, monkeypatch):
    """ip_main prints step names only: no counts or rates reach the public run log."""
    import ip_main
    raw = {"ios": [row("4.1.1", LAST, 987, paid=61)], "android": [row("4.0.155", LAST, 6543, paid=77)]}
    b = bench()
    cidrs, cidr_date = F.load_cidrs()
    b["definition"] = F.definition_hash(cidrs, cidr_date, M.RULES)
    monkeypatch.setattr(ip_main, "load_bench", lambda: b)
    monkeypatch.setattr(ip_main, "fetch_window", lambda *a: raw)
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        assert ip_main.main(["--report-date", "2026-10-20", "--out", str(tmp_path), "--variant", "A"]) == 0
    logged = out.getvalue() + err.getvalue()
    assert not re.search(r"\d", logged), logged
    html = (tmp_path / "email-A.html").read_text()
    assert "987" in html and "6,543" in html                            # tracks rendered with data, not empty


def test_stale_benchmarks_are_refused(tmp_path, monkeypatch):
    import ip_main
    b = bench()
    b["definition"] = "obsolete"
    monkeypatch.setattr(ip_main, "load_bench", lambda: b)
    monkeypatch.setattr(ip_main, "fetch_window", lambda *a: {"ios": [], "android": []})
    with redirect_stderr(io.StringIO()):
        assert ip_main.main(["--report-date", "2026-10-20", "--out", str(tmp_path)]) == 2


# ----------------------------------------------------------- transport ----

class _Resp:
    def __init__(self, code, body):
        self.status_code, self._b = code, body

    def json(self):
        return self._b


def _done(rows, more=False):
    return {"query_status": {"complete": True, "error": False,
                             "results": {"columns": ["a"], "results": rows, "hasMore": more}}}


def _runner(monkeypatch, posts, gets):
    monkeypatch.setattr(F.requests, "post", lambda *a, **k: posts.pop(0))
    monkeypatch.setattr(F.requests, "get", lambda *a, **k: gets.pop(0))
    monkeypatch.setattr(F.time, "sleep", lambda s: None)
    return F.make_runner("synthetic", retries=2, poll_s=0)


def test_async_poll_until_complete(monkeypatch):
    run = _runner(monkeypatch, [_Resp(202, {"query_status": {"id": "q"}})],
                  [_Resp(202, {"query_status": {"id": "q", "complete": False}}), _Resp(200, _done([[1]]))])
    assert run("x") == [{"a": 1}]


def test_truncated_result_is_refused(monkeypatch):
    run = _runner(monkeypatch, [_Resp(202, {"query_status": {"id": "q"}})], [_Resp(200, _done([[1]], more=True))])
    try:
        run("x")
    except F.Unavailable:
        return
    raise AssertionError("a truncated result must not be used")


def test_server_errors_retry_then_fail(monkeypatch):
    run = _runner(monkeypatch, [_Resp(504, {}), _Resp(202, {"query_status": {"id": "q"}})], [_Resp(200, _done([[2]]))])
    assert run("x") == [{"a": 2}]
    run = _runner(monkeypatch, [_Resp(504, {}), _Resp(504, {})], [])
    try:
        run("x")
    except F.Unavailable as e:
        assert not re.search(r"\d{3,}", str(e).replace("504", "")), "no figures in the error"
        return
    raise AssertionError("must raise Unavailable")


def test_rejected_query_does_not_retry(monkeypatch):
    run = _runner(monkeypatch, [_Resp(400, {})], [])
    try:
        run("x")
    except F.Unavailable:
        return
    raise AssertionError("must raise Unavailable")


def test_fingerprint_covers_both_platforms(monkeypatch):
    base = F.definition_hash(CIDRS, "d")
    monkeypatch.setitem(F.PLAT_OS, "android", "('Android','AndroidTV')")
    assert F.definition_hash(CIDRS, "d") != base
    assert F.definition_hash(CIDRS, "d", "rules-a") != F.definition_hash(CIDRS, "d", "rules-b")


def test_incomplete_benchmarks_are_refused(tmp_path, monkeypatch):
    import ip_main
    b = bench()
    cidrs, cidr_date = F.load_cidrs()
    b["definition"] = F.definition_hash(cidrs, cidr_date, M.RULES)
    b["mar26"]["ios"]["counts"]["n"] = 0
    monkeypatch.setattr(ip_main, "load_bench", lambda: b)
    monkeypatch.setattr(ip_main, "fetch_window", lambda *a: {"ios": [], "android": []})
    with redirect_stderr(io.StringIO()):
        assert ip_main.main(["--report-date", "2026-10-20", "--out", str(tmp_path)]) == 2


def test_android_rows_are_like_for_like(tmp_path):
    """The server Paid row carries no Aug 2025 mark; the in-app event row carries both."""
    m = model({"ios": [], "android": [row("4.0.150", LAST, 9000, paid=45, pc=180)]}, bench(aug_android_paid=0))
    t = track(m, "android4")
    assert R.steps_for(t) == ["pw", "pv", "ex", "paid", "pc"]
    assert R.bench_for(t, m, "aug25", "paid") is None and R.bench_for(t, m, "aug25", "pc") is not None
    assert R.steps_for(track(model({"ios": [row("4.1.0", LAST, 900, paid=45)], "android": []}), "ios")) == \
        ["pw", "pv", "ex", "paid"]
    _, html, text, _ = R.render(m, "A", tmp_path)
    assert "Paid: 0.50% · Mar −0.50 pts (worse) · Aug —†" in text
    assert "Paid, in-app event †: 2.0%" in text and "Aug —†" in html


def test_crosscheck_compares_every_field():
    import ip_crosscheck as X
    a = [dict(row("4.1.0", 1, 100, paid=5), gip=False), dict(row("4.1.0", 1, 7, cc="IN"), gip=True)]
    b = [dict(r) for r in a]
    assert X.by_key(a) == X.by_key(b)
    b[1]["pc_pv"] += 1
    assert X.by_key(a) != X.by_key(b)
    b = [dict(r) for r in a]
    b[1]["cc"] = "US"
    assert X.by_key(a) != X.by_key(b)
    b = [dict(r) for r in a]
    b[1]["d"] = "2026-10-05"                       # same counts, different install day
    assert X.by_key(a) != X.by_key(b)
    b = [dict(r) for r in a]
    b[0]["exr"] += 1
    assert X.by_key(a) != X.by_key(b)
    assert set(X.FIELDS) == set(F.COUNTS)


# ---- Phase B: delivery ----

sys.path.insert(0, os.path.join(HERE, "..", "catchup"))


def _utc(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)


def test_latest_ready_date_respects_05_pt_cutoff_and_dst():
    import ip_main
    # PDT (UTC-7): 05:00 PT = 12:00 UTC
    assert ip_main.latest_ready_date(_utc("2026-10-20T11:59:00")) == dt.date(2026, 10, 19)
    assert ip_main.latest_ready_date(_utc("2026-10-20T12:00:00")) == dt.date(2026, 10, 20)
    # PST (UTC-8): 05:00 PT = 13:00 UTC
    assert ip_main.latest_ready_date(_utc("2026-12-01T12:30:00")) == dt.date(2026, 11, 30)
    assert ip_main.latest_ready_date(_utc("2026-12-01T13:00:00")) == dt.date(2026, 12, 1)
    # evening manual run: the gate's "tomorrow" is not ready, today is
    assert ip_main.latest_ready_date(_utc("2026-10-21T01:30:00")) == dt.date(2026, 10, 20)


def test_future_report_date_is_refused(monkeypatch):
    import ip_main
    future = (dt.datetime.now(ip_main.PT) + dt.timedelta(days=1)).date().isoformat()
    monkeypatch.setattr(ip_main, "load_bench", lambda: (_ for _ in ()).throw(AssertionError("must not get this far")))
    err = io.StringIO()
    with redirect_stderr(err):
        assert ip_main.main(["--report-date", future, "--out", "/tmp/unused"]) == 4
    assert "not ready" in err.getvalue()


def _decide(monkeypatch, now, sent=False, alerted=False):
    import ip_watchdog as W
    seen = {}
    monkeypatch.setattr(W.G, "already_sent", lambda *a, **k: seen.update(k) or sent)
    monkeypatch.setattr(W, "alerted_today", (lambda *a, **k: alerted) if not isinstance(alerted, Exception)
                        else (lambda *a, **k: (_ for _ in ()).throw(alerted)))
    with redirect_stderr(io.StringIO()):
        out = W.decide(_utc(now))
    assert not seen or seen["sent_step"] == "Deliver for "
    return out[0]


def test_watchdog_window_and_dedupe(monkeypatch):
    assert not _decide(monkeypatch, "2026-12-01T13:20:00")          # 05:20 PST: before the window
    assert _decide(monkeypatch, "2026-12-01T14:20:00")              # 06:20 PST, nothing sent: alert
    assert _decide(monkeypatch, "2026-10-20T13:20:00")              # 06:20 PDT: alert
    assert not _decide(monkeypatch, "2026-10-20T19:20:00")          # 12:20 PDT: after the window
    assert not _decide(monkeypatch, "2026-10-20T14:20:00", sent=True)
    assert not _decide(monkeypatch, "2026-10-20T14:20:00", alerted=True)
    assert _decide(monkeypatch, "2026-10-20T14:20:00", alerted=RuntimeError("api down"))  # fail towards alerting


def test_watchdog_counts_only_a_successful_alert_step_for_today(monkeypatch):
    import ip_watchdog as W
    monkeypatch.setenv("GITHUB_RUN_ID", "9")
    today = dt.date(2026, 10, 20)

    def fetch(runs, steps):
        def f(path):
            if "/jobs" in path:
                rid = int(path.split("/runs/")[1].split("/")[0])
                return {"jobs": [{"steps": steps.get(rid, [])}]}
            return {"workflow_runs": runs}
        return f
    ok = {"name": "Alert PT for 2026-10-20", "conclusion": "success"}
    tr = {"event": "schedule", "head_branch": "main", "head_repository": {"full_name": "Lascade-Co/actions"}}
    assert W.alerted_today(today, fetch([dict(tr, id=7)], {7: [ok]}))
    assert W.alerted_today(today, fetch([dict(tr, id=9)], {9: [ok]}))                               # earlier attempt of this run
    assert not W.alerted_today(today, fetch([dict(tr, id=7)], {7: [dict(ok, conclusion="failure")]}))  # send failed
    assert not W.alerted_today(today, fetch([dict(tr, id=7)], {7: [dict(ok, name="Alert PT for 2026-10-19")]}))
    assert not W.alerted_today(today, fetch([dict(tr, id=7, event="pull_request")], {7: [ok]}))     # fork copy
    assert not W.alerted_today(today, fetch([dict(tr, id=7, head_repository={"full_name": "x/actions"})], {7: [ok]}))


def test_gate_dedupe_matches_the_report_date_step():
    import catchup_gate as G
    rd, now = dt.date(2026, 10, 21), _utc("2026-10-21T12:50:00")
    job = lambda name: {"name": "email", "conclusion": "success", "completed_at": "2026-10-20T23:10:00Z",
                        "steps": [{"name": name, "conclusion": "success"}]}
    f = lambda j: (lambda path: {"jobs": [j]} if "/jobs" in path else {"workflow_runs": [
        {"id": 5, "event": "schedule", "head_branch": "main", "head_repository": {"full_name": "Lascade-Co/actions"}}]})
    # finished after 16:00 PT the day before, but for the previous date: not today's send
    assert not G.already_sent(rd, now, f(job("Send for 2026-10-20")), workflow="w", sent_step="Send for ")
    assert G.already_sent(rd, now, f(job("Send for 2026-10-21")), workflow="w", sent_step="Send for ")


def test_subject_carries_the_latest_full_day():
    m = model({"ios": [row("4.1.0", LAST, 900, paid=45)], "android": []})
    assert R.subject_of(m) == "TA install→paid · Sun 18 Oct: iOS 5.0% · Android 5.x — · Android 4.0.x —"


def test_dated_dedupe_counts_recovery_runs_of_any_event():
    import catchup_gate as G
    seen = []
    def f(path):
        seen.append(path)
        return {"jobs": [{"name": "email", "conclusion": "success", "completed_at": "2026-10-21T13:00:00Z",
                          "steps": [{"name": "Deliver for 2026-10-21", "conclusion": "success"}]}]} \
            if "/jobs" in path else {"workflow_runs": [dict(run)]}
    run = {"id": 5, "event": "workflow_dispatch", "head_branch": "main",
           "head_repository": {"full_name": "Lascade-Co/actions"}}
    assert G.already_sent(dt.date(2026, 10, 21), _utc("2026-10-21T14:00:00"), f, workflow="w", sent_step="Deliver for ")
    for bad in ({"event": "pull_request"}, {"head_branch": "evil"},
                {"head_repository": {"full_name": "someone/actions"}}):
        run = {"id": 5, "event": "workflow_dispatch", "head_branch": "main",
               "head_repository": {"full_name": "Lascade-Co/actions"}, **bad}
        assert not G.already_sent(dt.date(2026, 10, 21), _utc("2026-10-21T14:00:00"), f, workflow="w",
                                  sent_step="Deliver for "), bad
    run = {"id": 5, "event": "schedule", "head_branch": "main", "head_repository": {"full_name": "Lascade-Co/actions"}}
    assert not G.already_sent(dt.date(2026, 10, 21), _utc("2026-10-21T14:00:00"), f, workflow="w", sent_step="Send for ")
    assert "event=schedule" not in seen[0]
    seen.clear()
    G.already_sent(dt.date(2026, 10, 21), _utc("2026-10-21T14:00:00"), f, workflow="w")   # legacy callers
    assert "event=schedule" in seen[0]


# Include edition and phase cases in the required verification entrypoint.
from test_ip_watchdog import TestWatchdogEditions  # noqa: E402, F401


# ------------------------------------------------------------ early read ----

import ip_editions as E  # noqa: E402
import ip_main as IM  # noqa: E402

EARLY_DAY, LAST, FIRST = 18, 17, 11      # model(): report 20 Oct, cutoff 05:00 PDT -> last full day 18 Oct


def early_raw(day_paid=3, day_n=100, base_paid=40, base_n=1000):
    """Synthetic early rows: the newest day plus a 7-day same-basis baseline, iOS only."""
    rows = [row("4.1.0", EARLY_DAY, day_n, paid=day_paid)]
    rows += [row("4.1.0", FIRST + i, base_n // 7, paid=base_paid // 7) for i in range(7)]
    return {"ios": rows, "android": []}


def early_model(raw=None, **kw):
    m = model({"ios": [row("4.1.0", d, 500, paid=25) for d in range(FIRST, LAST + 1)], "android": []})
    m["edition"] = "pt"
    m["early"] = M.build_early(raw or early_raw(**kw), m["first_day"], m["last_day"], m["last_day"] + dt.timedelta(days=1))
    return m


def test_early_sql_is_opt_in_and_caps_every_window():
    s, e, c = dt.datetime(2026, 10, 12, 7), dt.datetime(2026, 10, 20, 7), dt.datetime(2026, 10, 20, 12)
    dls = {dt.date(2026, 10, 19): c, dt.date(2026, 10, 18): dt.datetime(2026, 10, 19, 12)}
    base = F.cohort_sql(dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 15), dt.datetime(2026, 9, 20), "ios", CIDRS)
    assert " dl" not in base and "multiIf" not in base           # default SQL unchanged
    with __import__("pytest").raises(ValueError):
        F.cohort_sql(s, e, c, "ios", CIDRS)                       # young installs need deadlines
    sql = F.cohort_sql(s, e, c, "ios", CIDRS, deadlines=dls)
    assert sql.count("AND x < dl") == 7                           # pw, pv, ex, exr, paid, pc, fp + export-after-preview
    assert "toDate('2026-10-18'), toDateTime('2026-10-19 12:00:00', 'UTC')" in sql
    assert "timestamp < toDateTime('2026-10-20 12:00:00', 'UTC')" in sql   # events scanned to the cutoff


def test_early_window_is_dst_safe_for_both_editions():
    for ed, rd, early, dl_early, dl_prev, end in [
        ("pt", dt.date(2026, 10, 23), dt.date(2026, 10, 22), "2026-10-23 12:00", "2026-10-22 12:00", "2026-10-23 07:00"),
        ("pt", dt.date(2026, 11, 2), dt.date(2026, 11, 1), "2026-11-02 13:00", "2026-11-01 13:00", "2026-11-02 08:00"),
        ("pt", dt.date(2026, 3, 9), dt.date(2026, 3, 8), "2026-03-09 12:00", "2026-03-08 12:00", "2026-03-09 07:00"),
        ("ist", dt.date(2026, 10, 23), dt.date(2026, 10, 21), "2026-10-22 23:30", "2026-10-21 23:30", "2026-10-22 07:00"),
    ]:
        first, last, e, start, stop, dls = M.early_window(rd, E.get_edition(ed), IM.cutoff_for(rd, ed))
        assert e == early and last == early - dt.timedelta(days=1) and first == last - dt.timedelta(days=6)
        assert f"{dls[early]:%Y-%m-%d %H:%M}" == dl_early and f"{dls[last]:%Y-%m-%d %H:%M}" == dl_prev
        assert f"{stop:%Y-%m-%d %H:%M}" == end and stop <= IM.cutoff_for(rd, ed)
        assert len(dls) == 8


def test_early_model_counts_and_verdicts():
    m = early_model(day_paid=1, day_n=100, base_paid=420, base_n=7000)
    et = m["early"]["tracks"]["ios"]
    assert et["day"]["n"] == 100 and et["day"]["paid"] == 1
    assert et["base"]["n"] == 7000
    assert et["cmp"]["paid"]["verdict"] == "worse"
    assert m["early"]["tracks"]["android4"]["day"] == {} and m["early"]["tracks"]["android4"]["cmp"] == {}


def test_early_render_subject_dateline_grid_and_block(tmp_path):
    m = early_model(day_paid=3, day_n=100)
    subj, html, text, _ = R.render(m, "A", tmp_path)
    assert subj.startswith("TA install→paid · Mon 19 Oct (early): iOS 3.0%")
    assert "early Mon 19 Oct · latest full day Sun 18 Oct" in html
    assert "Early read, provisional: what Mon 19 Oct installs had done by 05:00 PT Tue 20 Oct" in html
    assert "Mon 19 Oct (early)" in html and "No installs yet on Mon 19 Oct." in html
    assert "Early read, provisional" in text and "Judged on the 7 days, never the single day" not in text
    assert "measured at the same point" in text


def test_without_early_the_email_is_unchanged(tmp_path):
    m = early_model()
    m["early"] = None
    subj, html, text, _ = R.render(m, "A", tmp_path)
    assert subj.startswith("TA install→paid · Sun 18 Oct: ")
    assert "Early read" not in html and "Judged on the 7 days, never the single day" in text


def test_early_render_failure_falls_back(tmp_path, monkeypatch):
    m = early_model()
    monkeypatch.setattr(R, "early_lines", lambda *a: (_ for _ in ()).throw(RuntimeError("boom")))
    with redirect_stdout(io.StringIO()) as out:
        subj, html, _, _ = R.render(m, "A", tmp_path)
    assert "(early)" not in subj and "Early read" not in html
    assert "boom" not in out.getvalue()


def test_early_fetch_failures_and_budget_degrade_to_none():
    rd, cut = dt.date(2026, 10, 20), dt.datetime(2026, 10, 20, 12)
    now = dt.datetime(2026, 10, 20, 12, 10, tzinfo=dt.timezone.utc)

    def boom():
        raise F.Unavailable("x")

    def malformed():
        return {"ios": [{"v": "4.1.0", "d": "not-a-date", "n": "x"}], "android": []}

    def half():
        F.check_deadline(now - dt.timedelta(seconds=1))         # deadline passed after one platform

    for fetch in (boom, malformed, half):
        with redirect_stdout(io.StringIO()) as out:
            assert IM.fetch_early_model(rd, cut, "pt", None, now=now, fetch=fetch) is None
        assert "unavailable" in out.getvalue()
    # Build deadline too close: skipped without fetching.
    with redirect_stdout(io.StringIO()) as out:
        assert IM.fetch_early_model(rd, cut, "pt", now + dt.timedelta(minutes=5), now=now, fetch=boom) is None
    assert "skipped" in out.getvalue()
    assert IM.early_deadline(None, now) == now + IM.EARLY_BUDGET
    assert IM.early_deadline(now + dt.timedelta(minutes=10), now) == now + dt.timedelta(minutes=6)


def test_early_track_with_empty_baseline_shows_figure(tmp_path):
    raw = {"ios": [row("4.1.0", EARLY_DAY, 100, paid=2)], "android": []}
    m = early_model(raw)
    assert m["early"]["tracks"]["ios"]["cmp"]["paid"] is None
    subj, html, _, _ = R.render(m, "A", tmp_path)
    assert "iOS 2.0%" in subj and "can&#x27;t tell yet" in html
