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


def test_release_day_ignores_test_installs():
    raw = {"ios": [], "android": [row("4.0.9", 0, 13, gip=True), row("4.0.9", 0, 1), row("4.0.9", 3, 40)]}
    kept, audit = M.split(raw)
    assert audit["android4"]["test"] == 13
    assert M.release_days(kept["android4"])["4.0.9"] == (D0 + dt.timedelta(days=3), False)


def test_pool_is_outcome_blind():
    """Same installs, different conversions -> the same builds in the pool."""
    def raw(paid_new):
        return {"ios": [row("4.2.0", 10, 400, paid=paid_new), row("4.1.0", 5, 400, paid=20), row("4.0.0", 0, 900, paid=45)],
                "android": []}
    a = [b["v"] for b in track(model(raw(0)), "ios")["builds"]]
    b = [b["v"] for b in track(model(raw(40)), "ios")["builds"]]
    assert a == b == ["4.2.0", "4.1.0"]          # target = ceil(40 / 0.05) = 800 installs


def test_pool_orders_by_version_and_skips_pre_release_installs():
    rows = [row("4.0.10", 5, 30), row("4.0.10", 2, 3),     # 3 testers before release day: left out
            row("4.0.3", 9, 1), row("4.0.11", 9, 2)]        # straggler old binary today; genuine soft launch
    p = M.pool([dict(r, d=M._day(r["d"])) for r in rows], 10_000)
    assert [b["v"] for b in p["builds"]] == ["4.0.11", "4.0.10", "4.0.3"]   # newest version first
    assert p["counts"]["n"] == 33 and p["builds"][0]["soft"] and p["capped"]
    p2 = M.pool([dict(r, d=M._day(r["d"])) for r in rows], 20)
    assert [b["v"] for b in p2["builds"]] == ["4.0.11", "4.0.10"]          # straggler not needed


def test_soft_launch_and_small_builds_are_shown():
    m = model({"ios": [], "android": [row("5.0.9", 3, 3), row("5.0.8", 1, 300, paid=2)]})
    txt = R.builds_txt(track(m, "android5"))
    assert "5.0.8" in txt and "1 build with under 5 installs (3 in all)" in txt
    m = model({"ios": [], "android": [row("5.0.9", 3, 8), row("5.0.8", 1, 300, paid=2)]})
    assert "5.0.9 (soft launch)" in R.builds_txt(track(m, "android5"))


def test_zero_march_rate_is_not_an_empty_track():
    b = bench()
    b["mar26"]["android"]["counts"].update(paid=0, paid_pv=0)
    t = track(model({"ios": [], "android": [row("5.0.8", 1, 300, paid=2)]}, b), "android5")
    assert not t["empty"] and t["no_target"] and t["thin"]
    assert t["cmp"]["mar26"]["paid"] is not None                     # zero numerator still compares
    assert "no March 2026 rate" in R.verdict_line(t, model({"ios": [], "android": [row("5.0.8", 1, 300, paid=2)]}, b))[1]


def test_thin_pool_has_no_verdicts():
    m = model({"ios": [], "android": [row("5.0.8", 1, 300, paid=2)]})
    t = track(m, "android5")
    assert t["thin"] and t["capped"]
    assert all(t["cmp"][bk][s] is None or t["cmp"][bk][s]["verdict"] is None
               for bk in ("aug25", "mar26") for s in ("pw", "pv", "ex", "pv_ex", "paid"))
    assert "Too few to call" in R.verdict_line(t, m)[1]


def test_empty_track():
    t = track(model({"ios": [row("4.1.0", 1, 900, paid=45)], "android": []}), "android4")
    assert t["empty"]


def test_android_aug25_compares_client_with_client():
    m = model({"ios": [], "android": [row("4.0.150", 1, 9000, paid=90, pc=120)]},     # server and client diverge
              bench(and_rate=0.01, aug_android_paid=0))
    t = track(m, "android4")
    line = R.verdict_line(t, m)[1]
    assert "1.33%" in line and "in-app purchase event" in line           # today's client figure is named
    assert t["cmp"]["aug25"]["client_basis"] and not t["cmp"]["mar26"]["client_basis"]
    aug = t["cmp"]["aug25"]["paid"]
    assert aug["verdict"] is None                                      # directional only
    assert abs(aug["diff"] - (120 / 9000 - 0.01)) < 1e-9               # client vs client
    assert t["cmp"]["mar26"]["paid"]["verdict"] is not None
    assert "directional only" in R.verdict_line(t, m)[1] and "†" in R.verdict_line(t, m)[1]


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
    raw = {"ios": [row("4.1.1", 10, 500, paid=30), row("4.1.0", 5, 500, paid=22)],
           "android": [row("4.0.155", 3, 6000, paid=70), row("5.0.8", 2, 200, paid=1)]}
    m = model(raw)
    subj, html, text, imgs = R.render(m, "A", tmp_path)
    assert subj.startswith("TA install→paid") and "<img" not in html and not imgs
    assert "App Store · iOS" in html and "Play Store · Android 5.x" in text
    assert "Pool:" in text and "† Android August 2025" in text


def test_render_variant_b(tmp_path):
    import pytest
    pytest.importorskip("matplotlib")
    raw = {"ios": [row("4.1.1", 10, 500, paid=30), row("4.1.0", 5, 500, paid=22)],
           "android": [row("4.0.155", 3, 6000, paid=70), row("5.0.8", 2, 200, paid=1)]}
    _, html_b, _, imgs_b = R.render(model(raw), "B", tmp_path)
    # funnel for every track; a by-build trend only where >= 2 builds (iOS here)
    assert len(imgs_b) == 4 and all(os.path.exists(i["path"]) for i in imgs_b)
    assert all(i["name"] in html_b for i in imgs_b)
    assert "Previewed a trip</td>" in html_b                            # values as text, not only in the PNG


def test_logs_carry_no_figures(tmp_path, monkeypatch):
    """ip_main prints step names only: no counts or rates reach the public run log."""
    import ip_main
    raw = {"ios": [row("4.1.1", 10, 987, paid=61)], "android": [row("4.0.155", 3, 6543, paid=77)]}
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
    assert (tmp_path / "email-A.html").exists()


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
    m = model({"ios": [], "android": [row("4.0.150", 1, 9000, paid=45, pc=180)]}, bench(aug_android_paid=0))
    t = track(m, "android4")
    assert R.steps_for(t) == ["pw", "pv", "ex", "paid", "pc"]
    assert R.bench_for(t, m, "aug25", "paid") is None and R.bench_for(t, m, "aug25", "pc") is not None
    assert R.steps_for(track(model({"ios": [row("4.1.0", 1, 900, paid=45)], "android": []}), "ios")) == \
        ["pw", "pv", "ex", "paid"]
    _, _, text, _ = R.render(m, "A", tmp_path)
    assert "Paid: 0.50% (Mar 2026 1.00%, Aug 2025 —)" in text
    assert "Paid, in-app event †: 2.0%" in text


def test_thin_android_still_states_client_comparison(tmp_path):
    m = model({"ios": [], "android": [row("5.0.8", 1, 400, paid=2, pc=1)]}, bench(aug_android_paid=0))
    _, _, text, _ = R.render(m, "A", tmp_path)
    assert "Paid, in-app event †: 0.25%" in text


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
    assert not seen or seen["sent_step"] == "Send for "
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
            if "/jobs?filter=all" in path:
                rid = int(path.split("/runs/")[1].split("/")[0])
                return {"jobs": [{"steps": steps.get(rid, [])}]}
            return {"workflow_runs": runs}
        return f
    ok = {"name": "Alert for 2026-10-20", "conclusion": "success"}
    assert W.alerted_today(today, fetch([{"id": 7}], {7: [ok]}))
    assert W.alerted_today(today, fetch([{"id": 9}], {9: [ok]}))                               # earlier attempt of this run
    assert not W.alerted_today(today, fetch([{"id": 7}], {7: [dict(ok, conclusion="failure")]}))  # send failed
    assert not W.alerted_today(today, fetch([{"id": 7}], {7: [dict(ok, name="Alert for 2026-10-19")]}))


def test_gate_dedupe_matches_the_report_date_step():
    import catchup_gate as G
    rd, now = dt.date(2026, 10, 21), _utc("2026-10-21T12:50:00")
    job = lambda name: {"name": "email", "conclusion": "success", "completed_at": "2026-10-20T23:10:00Z",
                        "steps": [{"name": name, "conclusion": "success"}]}
    f = lambda j: (lambda path: {"jobs": [j]} if "/jobs" in path else {"workflow_runs": [{"id": 5}]})
    # finished after 16:00 PT the day before, but for the previous date: not today's send
    assert not G.already_sent(rd, now, f(job("Send for 2026-10-20")), workflow="w", sent_step="Send for ")
    assert G.already_sent(rd, now, f(job("Send for 2026-10-21")), workflow="w", sent_step="Send for ")


def test_subject_carries_the_report_date():
    m = model({"ios": [row("4.1.0", 1, 900, paid=45)], "android": []})
    s = R.subject_of(m)
    assert s.startswith(f"TA install→paid · {m['report_date']:%a} {m['report_date'].day} {m['report_date']:%b} — iOS ")
