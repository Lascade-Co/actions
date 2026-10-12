import json, os, sys
from datetime import date, datetime, timezone
from decimal import Decimal as D
HERE = os.path.dirname(__file__)
sys.path[:0] = [HERE, os.path.join(HERE, "..", "pnl")]
import adspend_fetch as f
import adspend_model as m
from pnl_fx import RateTable
from pnl_money import Unavailable

APPS = json.load(open(os.path.join(HERE, "..", "..", "data", "adspend_apps.json")))
AD = date(2026, 9, 22)
NOW = datetime(2026, 9, 23, 12, 45, tzinfo=timezone.utc)
TABLE = RateTable({"AED": D("0.25")}, date(2026, 9, 22))


def row(camp, day, spend, ch="Meta", cur="USD", inst=D(0), tz="Asia/Dubai", acct="1", cid=None):
    return f.Row(ch, acct, tz, cur, cid or camp, camp, day, D(str(spend)), inst)


def d(n):
    from datetime import timedelta
    return AD - timedelta(days=n)


# ---- naming
def test_app_and_os():
    assert m.app_of("TA - WW EN - IOS - SUBS", APPS) == "Travel Animator"
    assert m.app_of("ta-x", APPS) == "Travel Animator"
    assert m.app_of("TAX campaign", APPS) == m.UNATTRIBUTED      # prefix must end at a boundary
    assert m.app_of("AR CHART", APPS) == "Smart Ruler"
    assert m.app_of("Mail AI - US", APPS) == "Mail AI"
    assert m.app_of("Random", APPS) == m.UNATTRIBUTED
    assert m.os_of("TA - IOS AEM - X") == "iOS" and m.os_of("MR ANDROID") == "Android"
    assert m.os_of("TA - WW") == m.UNASSIGNED


# ---- labels at the deadband edges
def test_labels():
    L = m.label_of
    assert L(D(0), D(0), False) == "steady" and L(D(0), D(0), True) == "dark"
    assert L(D(5), D(0), False) == "started" and L(D(0), D(5), False) == "stopped"
    assert L(D(115), D(100), False) == "up"          # exactly 15% and $15
    assert L(D("114.99"), D(100), False) == "steady"
    assert L(D(85), D(100), False) == "down"
    assert L(D(11), D(10), False) == "steady"        # +10% only
    assert L(D(19), D(10), False) == "steady"        # +90% but $9 < $10
    assert L(D(20), D(10), False) == "up"            # +100% and $10


# ---- model
def base_rows():
    r = []
    for n in range(0, 15):
        r.append(row("TA - WW - IOS - A", d(n), 100 if n else 130, inst=D(10)))
    r.append(row("MR - X", d(1), 50)); r.append(row("MR - X", d(0), 0))   # stopped
    r.append(row("TA - AED - ANDROID", d(0), 40, cur="AED", tz="Asia/Dubai", inst=D(4)))
    r.append(row("Nope - 1", d(0), 5))
    return r


def test_model_basics():
    model = m.build_model({"Meta": base_rows(), "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    assert model["partial"] is False         # empty Google list = fetched, nothing spent
    ta = next(p for p in model["projects"] if p["name"] == "Travel Animator")
    ios = next(l for l in ta["lines"] if l["os"] == "iOS")
    assert (ios["d"], ios["d1"], ios["label"]) == (D(130), D(100), "up")
    assert ios["avg7"] == D(100)                       # D-8..D-2
    droid = next(l for l in ta["lines"] if l["os"] == "Android")
    assert droid["d"] == D(10) and droid["label"] == "started"   # 40 AED * 0.25
    mr = next(p for p in model["projects"] if p["name"] == "Marine Radar")
    assert mr["lines"][0]["label"] == "stopped" and mr["lines"][0]["camps_stopped"] == 1
    assert any(p["name"] == m.UNATTRIBUTED for p in model["projects"])
    assert len(model["chart"]) == 15 and model["chart"][-1]["day"] == AD
    assert model["fx"]["rates"] == {"AED": D("0.25")}


def test_missing_rate_makes_channel_unavailable():
    rows = base_rows() + [row("TA - Z", d(0), 1, cur="XXX")]
    model = m.build_model({"Meta": rows, "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    assert model["channels"]["Meta"]["status"] == "unavailable" and model["partial"]
    assert model["total"]["d"] == 0


def test_unavailable_channel_and_partial_subject():
    model = m.build_model({"Meta": base_rows(), "Google": Unavailable("Google HTTP 503"), "Apple Search Ads": []},
                          TABLE, AD, NOW, APPS)
    assert model["channels"]["Google"]["status"] == "unavailable"
    s = m.subject_of(model, "A", APPS)
    assert "partial" in s and s.startswith("Ad spend · Tue 22 Sep — $")


def test_unclosed_day_marks_partial():
    early = datetime(2026, 9, 22, 22, 0, tzinfo=timezone.utc)   # 02:00 Dubai on the 23rd? no: 02:00 on 23rd -> closed
    assert m.day_closed("Asia/Dubai", AD, early) is True
    assert m.day_closed("Asia/Dubai", AD, datetime(2026, 9, 22, 19, 0, tzinfo=timezone.utc)) is False
    model = m.build_model({"Meta": base_rows(), "Google": [], "Apple Search Ads": []}, TABLE, AD,
                          datetime(2026, 9, 22, 19, 0, tzinfo=timezone.utc), APPS)
    assert model["channels"]["Meta"]["status"] == "partial"


def test_subject_and_variants():
    model = m.build_model({"Meta": base_rows(), "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    a, b = m.subject_of(model, "A", APPS), m.subject_of(model, "B", APPS)
    assert a.startswith("Ad spend · Tue 22 Sep — ") and b.startswith("Ad spend + CPI · Tue 22 Sep — ")
    assert a.endswith("· MR Meta ↓$50")      # biggest absolute mover: the $50 stop


def test_installs_unavailable_yields_none():
    rows = [row("TA - IOS", d(1), 10, ch="Google", inst=None), row("TA - IOS", d(0), 20, ch="Google", inst=None)]
    model = m.build_model({"Meta": [], "Google": rows, "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    assert model["channels"]["Google"]["installs_ok"] is False
    ln = model["projects"][0]["lines"][0]
    assert ln["installs_d"] is None and ln["cpi_d"] is None


# ---- fetchers
SENTINEL = "SENTINEL_SECRET_123"


def meta_get(payloads, boom=None):
    def get(url, params):
        if boom:
            raise f.FetchError(boom)
        return payloads.pop(0)
    return get


def test_meta_fetch_and_paging():
    info = {"timezone_name": "Asia/Dubai", "currency": "AED"}
    p1 = {"data": [{"campaign_id": "1", "campaign_name": "TA - IOS", "spend": "10.5",
                    "date_start": "2026-09-22",
                    "actions": [{"action_type": "mobile_app_install", "value": "3"}]}],
          "paging": {"next": "https://x/next"}}
    p2 = {"data": [{"campaign_id": "2", "campaign_name": "TA - ANDROID", "spend": "1",
                    "date_start": "2026-09-22"}]}
    rows = f.fetch_meta({"token": SENTINEL, "account_ids": ["act_9"]}, d(14), AD, meta_get([info, p1, p2]))
    assert [(r.campaign_id, r.installs) for r in rows] == [("1", D(3)), ("2", D(0))]
    assert rows[0].currency == "AED" and rows[0].account_id == "9"


def test_meta_failure_is_sanitized_and_whole_channel():
    out = f.fetch_meta({"token": SENTINEL, "account_ids": ["1", "2"]}, d(14), AD,
                       meta_get([], boom="Meta HTTP 400 code 190"))
    assert isinstance(out, Unavailable) and SENTINEL not in out.reason


class Resp:
    def __init__(self, code, body, url=""):
        self.status_code, self._b = code, body

    def json(self):
        if isinstance(self._b, Exception):
            raise self._b
        return self._b


def test_request_never_leaks(monkeypatch=None):
    import requests
    orig = requests.request
    try:
        for resp in (Resp(400, {"error": {"code": 190, "message": SENTINEL}}),
                     Resp(500, {"error": {"message": SENTINEL}}),
                     Resp(200, ValueError(SENTINEL))):
            requests.request = lambda *a, _r=resp, **k: _r
            try:
                f._request("Meta", "GET", f"https://x/?access_token={SENTINEL}", params={})
                assert False, "should raise"
            except f.FetchError as e:
                assert SENTINEL not in str(e) and SENTINEL not in repr(e)

        def timeout(*a, **k):
            raise requests.Timeout(f"https://x/?access_token={SENTINEL}")
        requests.request = timeout
        try:
            f._request("Meta", "GET", "https://x", params={})
        except f.FetchError as e:
            assert SENTINEL not in str(e) and str(e) == "Meta Timeout"
    finally:
        requests.request = orig


GCREDS = {"client_id": "a", "client_secret": SENTINEL, "refresh_token": SENTINEL,
          "dev_token": SENTINEL, "login_customer_id": "100", "skip_customer_ids": ["300"]}


def g_post(spend_rows, install_rows, install_boom=False):
    def post(url, body, headers, data=None):
        if "oauth2" in url:
            return {"access_token": SENTINEL}
        q = body["query"]
        if "customer_client" in q:
            return [{"results": [{"customerClient": {"id": "200"}}, {"customerClient": {"id": "300"}}]}]
        assert "/customers/300/" not in url
        if "conversion_action_category" in q:
            if install_boom:
                raise f.FetchError("Google HTTP 500")
            return [{"results": install_rows}]
        return [{"results": spend_rows}]
    return post


def grow(camp_id, name, cost=None, cat=None, conv=None, cust="200", day="2026-09-22"):
    r = {"customer": {"id": cust, "timeZone": "Asia/Dubai", "currencyCode": "AED"},
         "campaign": {"id": camp_id, "name": name}, "segments": {"date": day}, "metrics": {}}
    if cost is not None:
        r["metrics"]["costMicros"] = str(cost)
    if cat:
        r["segments"]["conversionActionCategory"] = cat
        r["metrics"]["conversions"] = conv
    return r


def test_google_join_fractional_dupes_and_conversion_only():
    spend = [grow("1", "TA - IOS", cost=5_000_000), grow("2", "TA - IOS", cost=7_000_000, cust="201")]
    installs = [grow("1", "TA - IOS", cat="DOWNLOAD", conv=2.5),
                grow("1", "TA - IOS", cat="PURCHASE", conv=9),
                grow("1", "TA - IOS", cat="DOWNLOAD", conv=1.25),
                grow("2", "TA - IOS", cat="DOWNLOAD", conv=4, cust="201"),
                grow("3", "TA - ONLY", cat="DOWNLOAD", conv=1)]           # conversion-only
    rows = f.fetch_google(GCREDS, d(14), AD, g_post(spend, installs))
    by = {(r.account_id, r.campaign_id): r for r in rows}
    assert by[("200", "1")].installs == D("3.75") and by[("200", "1")].spend == D(5)
    assert by[("201", "2")].installs == D(4)                       # same name, other account: not merged
    assert by[("200", "3")].spend == 0 and by[("200", "3")].installs == D(1)


def test_google_install_query_failure_is_none_not_zero():
    rows = f.fetch_google(GCREDS, d(14), AD, g_post([grow("1", "TA - IOS", cost=1_000_000)], [], install_boom=True))
    assert len(rows) == 1 and rows[0].installs is None and rows[0].spend == D(1)


def test_google_spend_failure_is_unavailable_and_sanitized():
    def post(url, body, headers, data=None):
        if "oauth2" in url:
            return {"access_token": SENTINEL}
        if "customer_client" in body["query"]:
            return [{"results": [{"customerClient": {"id": "200"}}]}]
        raise f.FetchError("Google HTTP 403 code PERMISSION_DENIED")
    out = f.fetch_google(GCREDS, d(14), AD, post)
    assert isinstance(out, Unavailable) and SENTINEL not in out.reason


# ---- commentary
import adspend_commentary as c
import adspend_render_email as r


def full_model():
    return m.build_model({"Meta": base_rows(), "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)


def js(*sentences):
    """Claude's answer shape: items of an observed spend sentence and the facts it uses."""
    return json.dumps({"items": [{"observed": t, "refs": refs} for t, refs in sentences]})


def test_validator_accepts_and_fills():
    facts = c.build_facts(full_model(), "A")
    fid = next(k for k, v in facts.items() if k != "t" and v["label"] == "up")
    out = c.validate(js((f"Travel Animator spend rose to {{{fid}.d}} from {{{fid}.d1}}.", [fid])), facts,
                     c._names(facts))
    assert out == [f"Travel Animator spend rose to {facts[fid]['show']['d']} from {facts[fid]['show']['d1']}."]


def test_validator_rejects():
    facts = c.build_facts(full_model(), "A")
    up = next(k for k, v in facts.items() if k != "t" and v["label"] == "up")
    stopped = next(k for k, v in facts.items() if v["label"] == "stopped")
    names = c._names(facts)
    bad = [
        js(("Spend rose 18%.", [up])),                                   # literal digit
        js(("Spend rose {f99.d}.", ["f99"])),                             # unknown fact
        js((f"Spend rose {{{up}.nope}}.", [up])),                         # unknown field
        js((f"Spend fell {{{up}.d}}.", [up])),                            # contradicts label
        js((f"Marine Radar spend rose {{{up}.d}}.", [up])),               # name not in referenced fact
        js((f"Spend rose {{{up}.d}} and you should cut.", [up])),         # advice
        js((f"The team raised {{{up}.d}}.", [up])),                       # attribution
        js((f"Spend rose {{{up}.d}}.", [])),                              # no refs
        js(*[(f"Spend rose {{{up}.d}}.", [up])] * 3),                     # >2 sentences
        "not json", "", js(("Spend rose {t.d}.", ["f1"] if "f1" in facts and "t" not in ["f1"] else ["zz"])),
    ]
    for b in bad:
        assert c.validate(b, facts, names) == [], b
    assert c.validate(js((f"Spend stopped at {{{stopped}.d}}.", [stopped])), facts, names)


def test_variant_a_never_gets_installs():
    facts = c.build_facts(full_model(), "A")
    assert all("installs" not in f["show"] and "cpi" not in f["show"] for f in facts.values())
    factsb = c.build_facts(full_model(), "B")
    assert any("installs" in f["show"] for f in factsb.values())


def test_commentary_failure_and_cache(tmp_path=None):
    import tempfile
    d_ = tempfile.mkdtemp()
    calls = []

    def crash(*a, **k):
        calls.append(1)
        raise FileNotFoundError("claude")
    assert c.commentary(full_model(), "A", d_, run=crash) == []
    assert c.commentary(full_model(), "A", d_, run=crash) == []      # a failure is never cached
    assert len(calls) == 2
    d2 = tempfile.mkdtemp()
    facts = c.build_facts(full_model(), "A")
    up = next(k for k, v in facts.items() if k != "t" and v["label"] == "up")
    ok = lambda *a, **k: js((f"Spend rose to {{{up}.d}}.", [up]))
    first = c.commentary(full_model(), "A", d2, run=ok)
    assert first and c.commentary(full_model(), "A", d2, run=crash) == first   # cache wins


# ---- render
def test_render_variants_and_stability():
    model = full_model()
    sa, pa, ha, ta = r.render(model, "A", [], APPS)
    sb, pb, hb, tb = r.render(model, "B", ["Spend was steady."], APPS)
    assert sa.startswith("Ad spend ·") and sb.startswith("Ad spend + CPI ·")
    assert "CPI" not in ha and "CPI $" in hb and "installs" in hb and "installs" not in ha
    assert r.render(model, "A", [], APPS)[2] == ha                     # byte-stable
    assert "Spend was steady." in hb and pb == "Spend was steady."
    for h in (ha, hb):
        assert "github.com/Lascade-Co/actions/actions/runs" not in h and "run_id" not in h
        assert "#c0392b" not in h and "#27ae60" not in h              # no verdict colours
    assert "Travel Animator" in ta and "Marine Radar" in ta 
    assert "AED" not in ha + hb + ta + tb and "All figures in US dollars." in ha


def test_render_banner_on_partial():
    model = m.build_model({"Meta": base_rows(), "Google": Unavailable("Google HTTP 503"), "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    _, _, h, t = r.render(model, "A", [], APPS)
    assert "Google unavailable (Google HTTP 503)" in h and "partial" in h.lower()
    rows = [row("TA - IOS", d(1), 10, ch="Google", inst=None), row("TA - IOS", d(0), 20, ch="Google", inst=None)]
    mb = m.build_model({"Meta": [], "Google": rows, "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    hb = r.render(mb, "B", [], APPS)[2]
    assert "Google installs unavailable" in hb and "installs \u2014" in hb or "installs —" in hb


# ---- main (no network)
def _logged(fn):
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        out = fn()
    return out, buf.getvalue()


def test_commentary_skip_reasons_are_one_safe_word():
    import tempfile
    from claude_api import ClaudeError
    secret = "Spend $491 Travel Animator"

    def timeout(*a, **k):
        raise ClaudeError("timeout")

    def junk(*a, **k):
        return "not json " + secret

    def auth(*a, **k):
        raise ClaudeError("auth")

    def other(*a, **k):
        raise RuntimeError("model overloaded " + secret)

    for run, word in ((timeout, "timeout"), (junk, "rejected:shape"), (auth, "auth"), (other, "claude-error")):
        out, log = _logged(lambda: c.commentary(full_model(), "A", tempfile.mkdtemp(), run=run))
        assert out == []
        assert f"::warning title=Ad spend synopsis skipped::{word}" in log, (word, log)
        assert "$" not in log and "Travel" not in log


def test_empty_cache_entry_does_not_block_a_rerun():
    import tempfile
    d_ = tempfile.mkdtemp()
    model = full_model()
    facts = c.build_facts(model, "A")
    up = next(k for k, v in facts.items() if k != "t" and v["label"] == "up")
    c.commentary(model, "A", d_, run=lambda *a, **k: js((f"Spend rose to {{{up}.d}}.", [up])))
    path = os.path.join(d_, "commentary-A.json")
    cached = json.load(open(path))
    json.dump({"facts": cached["facts"], "sentences": []}, open(path, "w"))     # a [] left by an old run
    again = c.commentary(model, "A", d_, run=lambda *a, **k: js((f"Spend rose to {{{up}.d}}.", [up])))
    assert again, "an old empty cache entry must not suppress the synopsis"
    assert json.load(open(path))["sentences"] == again


def test_break_commentary_bypasses_a_good_cache():
    import tempfile
    d_ = tempfile.mkdtemp()
    model = full_model()
    facts = c.build_facts(model, "A")
    up = next(k for k, v in facts.items() if k != "t" and v["label"] == "up")
    assert c.commentary(model, "A", d_, run=lambda *a, **k: js((f"Spend rose to {{{up}.d}}.", [up])))

    def broken(*a, **k):
        raise RuntimeError("off")
    assert _logged(lambda: c.commentary(model, "A", d_, run=broken, cache=False))[0] == []


def test_summary_missing_notice_only_when_there_is_no_synopsis():
    model = full_model()
    h, t = r.render(model, "A", [], APPS)[2:]
    assert "Summary unavailable today." in h and "Summary unavailable today." in t
    h2, t2 = r.render(model, "A", ["Spend rose."], APPS)[2:]
    assert "Summary unavailable today." not in h2 and "Summary unavailable today." not in t2


def test_main_masks_and_never_logs_figures(capsys=None):
    import base64, io, contextlib, tempfile
    import adspend_main as mm
    secret = {"meta": {"token": SENTINEL + "M", "account_ids": ["1"]},
              "google": {"client_id": "a", "client_secret": SENTINEL + "C", "refresh_token": SENTINEL + "R",
                         "dev_token": SENTINEL + "D", "login_customer_id": "1"},
              "apple": {"client_id": SENTINEL + "AC", "team_id": "t", "key_id": SENTINEL + "AK", "org_id": "9",
                        "private_key": "-----BEGIN PRIVATE KEY-----\n" + SENTINEL + "L1\n" + SENTINEL + "L2\n-----END PRIVATE KEY-----\n"}}
    os.environ["ADS_CREDENTIALS_JSON_B64"] = base64.b64encode(json.dumps(secret).encode()).decode()
    mm.fetch_meta = lambda *a, **k: base_rows()
    mm.fetch_google = lambda *a, **k: Unavailable("Google HTTP 503")
    mm.fetch_apple = lambda *a, **k: [row("TA - TIER 1 - BRAND - EXACT", AD, 12, ch="Apple Search Ads")]
    os.environ["GITHUB_ACTIONS"] = "true"
    mm.build_rate_table = lambda *a, **k: TABLE
    out, scratch = tempfile.mkdtemp(), tempfile.mkdtemp()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = mm.main(["--ad-date", "2026-09-22", "--out", out, "--scratch", scratch,
                      "--apps", os.path.join(HERE, "..", "..", "data", "adspend_apps.json"),
                      "--variants", "A,B", "--break-commentary"])
    log = buf.getvalue()
    assert rc == 0 and os.path.exists(os.path.join(out, "email-A.html")) and os.path.exists(os.path.join(out, "email-B.subject"))
    assert "::add-mask::" + SENTINEL + "M" in log
    for tail in ("L1", "L2", "AC", "AK"):
        assert "::add-mask::" + SENTINEL + tail in log
    visible = "\n".join(l for l in log.splitlines() if not l.startswith("::add-mask::"))
    assert "$" not in visible and "TA - " not in visible and SENTINEL not in visible
    assert "commentary=no" in visible and "Google: unavailable" in visible
    assert "Apple Search Ads: 1 rows" in visible
    # Meta down but Apple up: still sendable. Only all-channels-down is "nothing to send".
    mm.fetch_meta = lambda *a, **k: Unavailable("Meta HTTP 500")
    with contextlib.redirect_stdout(io.StringIO()):
        assert mm.main(["--ad-date", "2026-09-22", "--out", out, "--scratch", scratch,
                        "--apps", os.path.join(HERE, "..", "..", "data", "adspend_apps.json")]) == 0
    mm.fetch_apple = lambda *a, **k: Unavailable("Apple Search Ads HTTP 500")
    with contextlib.redirect_stdout(io.StringIO()):
        assert mm.main(["--ad-date", "2026-09-22", "--out", out, "--scratch", scratch,
                        "--apps", os.path.join(HERE, "..", "..", "data", "adspend_apps.json")]) == 1


def test_masks_only_on_actions():
    import base64, io, contextlib
    import adspend_main as mm
    secret = {"meta": {"token": SENTINEL + "M"},
              "apple": {"client_id": SENTINEL + "AC", "key_id": SENTINEL + "AK",
                        "private_key": "-----BEGIN PRIVATE KEY-----\n" + SENTINEL + "L1\n-----END PRIVATE KEY-----\n"}}
    env = {"ADS_CREDENTIALS_JSON_B64": base64.b64encode(json.dumps(secret).encode()).decode()}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mm.load_creds(env)
    assert buf.getvalue() == ""                       # local run: nothing that echoes a secret
    with contextlib.redirect_stdout(buf):
        mm.load_creds({**env, "GITHUB_ACTIONS": "true"})
    out = buf.getvalue()
    assert "::add-mask::" + SENTINEL + "L1" in out and "::add-mask::-----BEGIN" not in out


def test_reconcile_prints_no_figures():
    import adspend_main as mm
    rows = {"Meta": [row("TA - IOS", AD, 100)], "Google": Unavailable("x")}
    ads = {"meta": {}, "google": {}}
    ok = mm.reconcile(rows, TABLE, AD, ads, lambda *a: {AD: D(100)}, lambda *a: {})
    bad = mm.reconcile(rows, TABLE, AD, ads, lambda *a: {AD: D(99)}, lambda *a: {})
    assert ok[0] == "reconcile Meta: match" and ok[1].endswith("skipped (a side is unavailable)")
    assert bad[0] == "reconcile Meta: MISMATCH (>=1%)" or bad[0].startswith("reconcile Meta: MISMATCH")
    assert not any("$" in l or "100" in l for l in ok + bad)


def test_small_dollar_move_is_not_shown_as_big_percent():
    rows = [row("TA - IOS", d(1), 8), row("TA - IOS", d(0), 17)]      # +112% but only $9
    model = m.build_model({"Meta": rows, "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    h = r.render(model, "A", [], APPS)[2]
    assert "small change vs Mon (+$9)" in h and "steady vs Mon (+1" not in h


# ---- Apple Search Ads
def _ec_pem():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption()).decode()


ASA_CREDS = {"client_id": "SEARCHADS.x", "team_id": "SEARCHADS.t", "key_id": "kid", "org_id": "9",
             "private_key": _ec_pem()}


def _asa_fakes(report_pages, acls=None, token_error=None, report_error=None):
    calls = {"post": [], "get": []}

    def post(url, body, headers, data=None):
        calls["post"].append((url, body, headers, data))
        if "appleid.apple.com" in url:
            if token_error:
                raise f.FetchError(token_error)
            return {"access_token": "T"}
        if report_error:
            raise f.FetchError(report_error)
        return report_pages[len(calls["post"]) - 2]

    def get(url, headers):
        calls["get"].append((url, headers))
        return {"data": [{"orgId": 9, "timeZone": "Asia/Dubai"}] if acls is None else acls}
    return post, get, calls


def _asa_page(items, total, offset=0):
    return {"data": {"reportingDataResponse": {"row": items}},
            "pagination": {"totalResults": total, "startIndex": offset, "itemsPerPage": 1}}


def _asa_item(cid, name, days):
    return {"metadata": {"campaignId": cid, "campaignName": name},
            "granularity": [{"date": dt, "localSpend": {"amount": amt, "currency": "USD"},
                             **({"totalInstalls": inst} if inst is not None else {})}
                            for dt, amt, inst in days]}


def test_fetch_apple_pages_and_parses():
    import jwt
    item = _asa_item(7, "TA - TIER 1 - BRAND - EXACT",
                     [("2026-09-21", "10.5", 2), ("2026-09-22", "12.60", None), ("2026-09-20", "0", 0)])
    item["granularity"].append({"date": "2026-09-19"})        # idle day: date only
    pages = [_asa_page([item], 2),
             _asa_page([_asa_item(8, "AR - X", [("2026-09-22", "4", 1)])], 2)]
    post, get, calls = _asa_fakes(pages)
    rows = f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD, post=post, get=get)
    assert [(r.channel, r.timezone, r.currency, r.campaign_id, r.day, r.spend, r.installs) for r in rows] == [
        ("Apple Search Ads", "Asia/Dubai", "USD", "7", date(2026, 9, 21), D("10.5"), D(2)),
        ("Apple Search Ads", "Asia/Dubai", "USD", "7", date(2026, 9, 22), D("12.60"), D(0)),
        ("Apple Search Ads", "Asia/Dubai", "USD", "8", date(2026, 9, 22), D(4), D(1))]   # zero-zero day skipped
    token_url, _, _, form = calls["post"][0]
    assert form["grant_type"] == "client_credentials" and form["scope"] == "searchadsorg"
    claims = jwt.decode(form["client_secret"], options={"verify_signature": False})
    assert claims["iss"] == "SEARCHADS.t" and claims["sub"] == "SEARCHADS.x" and claims["aud"] == "https://appleid.apple.com"
    assert jwt.get_unverified_header(form["client_secret"])["kid"] == "kid"
    (_, b1, h1, _), (_, b2, _, _) = calls["post"][1], calls["post"][2]
    assert h1["X-AP-Context"] == "orgId=9" and h1["Authorization"] == "Bearer T"
    assert b1["timeZone"] == "ORTZ" and b1["granularity"] == "DAILY" and b1["startTime"] == "2026-09-08"
    assert b1["returnGrandTotals"] is False and b1["returnRowTotals"] is False
    assert b1["selector"]["orderBy"][0]["field"] == "localSpend"   # Apple 400s without orderBy
    assert b1["selector"]["pagination"]["offset"] == 0 and b2["selector"]["pagination"]["offset"] == 1


def test_fetch_apple_failures_are_sanitized():
    for kw in ({"token_error": "Apple Search Ads HTTP 401"}, {"report_error": "Apple Search Ads HTTP 403 code 403"}):
        post, get, _ = _asa_fakes([], **kw)
        out = f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD, post=post, get=get)
        assert isinstance(out, Unavailable) and out.reason.startswith("Apple Search Ads HTTP 40")
        assert ASA_CREDS["private_key"][40:60] not in out.reason
    post, get, _ = _asa_fakes([], acls=[{"orgId": 1, "timeZone": "Asia/Dubai"}])
    out = f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD, post=post, get=get)
    assert isinstance(out, Unavailable) and "timezone" in out.reason
    ok_empty = {"data": {"reportingDataResponse": {"row": []}}, "pagination": {"totalResults": 0}}
    post, get, _ = _asa_fakes([ok_empty])
    assert f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD, post=post, get=get) == []   # genuinely empty = fetched, nothing spent
    bad_pages = [{"unexpected": 1},                                                      # no envelope
                 {"data": {"reportingDataResponse": {"row": []}}},                       # no pagination
                 {"data": {"reportingDataResponse": {"row": []}}, "pagination": {"totalResults": "x"}},
                 {"data": {"reportingDataResponse": {"row": []}}, "pagination": {"totalResults": 5}}]  # empty page before total
    for page in bad_pages:
        post, get, _ = _asa_fakes([page])
        out = f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD, post=post, get=get)
        assert isinstance(out, Unavailable) and out.reason == "Apple Search Ads malformed response", page
    post, get, _ = _asa_fakes([{"data": {"reportingDataResponse": {"row": [{"metadata": {"campaignId": 1}, "granularity": [{"date": "2026-09-22", "localSpend": {"amount": "1"}}]}]}}}])
    assert isinstance(f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD, post=post, get=get), Unavailable)


def test_fetch_apple_not_configured():
    for creds in (None, {}, {**ASA_CREDS, "private_key": ""}):
        out = f.fetch_apple(creds, date(2026, 9, 8), AD)
        assert isinstance(out, Unavailable) and out.reason == "Apple Search Ads: not configured"


def test_apple_rows_are_ios_without_os_token():
    rows = [row("TA - TIER 1 - BRAND - EXACT", d(1), 10, ch="Apple Search Ads"),
            row("TA - TIER 1 - BRAND - EXACT", d(0), 12, ch="Apple Search Ads")]
    model = m.build_model({"Meta": [], "Google": [], "Apple Search Ads": rows}, TABLE, AD, NOW, APPS)
    ta = next(p for p in model["projects"] if p["name"] == "Travel Animator")
    assert [(l["channel"], l["os"]) for l in ta["lines"]] == [("Apple Search Ads", "iOS")]
    assert model["partial"] is False


def test_footnotes_cover_apple():
    h, t = r.render(full_model(), "A", [], APPS)[2:]
    for body in (h, t):
        assert "not included" not in body and "Meta and Google only" not in body
    assert "Meta, Google and Apple Search Ads" in h and "Apple Search Ads" in t


def test_commentary_cache_is_keyed_on_facts():
    import tempfile
    d_ = tempfile.mkdtemp()
    calls = []

    m1 = full_model()
    facts1 = c.build_facts(m1, "A")
    up1 = next(k for k, v in facts1.items() if k != "t" and v["label"] == "up")

    def ok(prompt, scratch):
        calls.append(1)
        return js((f"Spend rose to {{{up1}.d}}.", [up1]))
    c.commentary(m1, "A", d_, run=ok)
    c.commentary(m1, "A", d_, run=ok)
    assert len(calls) == 1                              # same facts: cache reused
    rows = base_rows() + [row("TA - TIER 1", d(0), 20, ch="Apple Search Ads")]
    m2 = m.build_model({"Meta": rows, "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    c.commentary(m2, "A", d_, run=ok)
    assert len(calls) == 2                              # facts changed: not this morning's prose


def test_non_object_apple_block_is_not_configured():
    import base64, io, contextlib
    import adspend_main as mm
    for bad in (None, "x", 5, []):
        env = {"ADS_CREDENTIALS_JSON_B64": base64.b64encode(json.dumps(
            {"meta": None, "google": "x", "apple": bad}).encode()).decode(), "GITHUB_ACTIONS": "true"}
        with contextlib.redirect_stdout(io.StringIO()):
            assert mm.load_creds(env)["apple"] == bad          # masking must not crash
        assert isinstance(f.fetch_apple(bad, date(2026, 9, 8), AD), Unavailable)


def test_apple_missing_creds_main_path_sends_with_warning():
    import base64, io, contextlib, tempfile
    import adspend_main as mm
    secret = {"meta": {"token": "m", "account_ids": ["1"]}, "google": {"client_id": "a"}}   # no apple key
    os.environ["ADS_CREDENTIALS_JSON_B64"] = base64.b64encode(json.dumps(secret).encode()).decode()
    os.environ.pop("GITHUB_ACTIONS", None)
    mm.fetch_meta = lambda *a, **k: base_rows()
    mm.fetch_google = lambda *a, **k: []
    mm.fetch_apple = f.fetch_apple                               # the real not-configured path
    mm.build_rate_table = lambda *a, **k: TABLE
    out, scratch = tempfile.mkdtemp(), tempfile.mkdtemp()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = mm.main(["--ad-date", "2026-09-22", "--out", out, "--scratch", scratch, "--break-commentary",
                      "--apps", os.path.join(HERE, "..", "..", "data", "adspend_apps.json")])
    assert rc == 0 and "Apple Search Ads: unavailable (Apple Search Ads: not configured)" in buf.getvalue()
    html = open(os.path.join(out, "email-A.html")).read()
    assert "Apple Search Ads unavailable (Apple Search Ads: not configured). Totals below are partial." in html
    assert "! Apple Search Ads: not configured" in open(os.path.join(out, "email-A.txt")).read()
    assert "partial" in open(os.path.join(out, "email-A.subject")).read()


def test_apple_errors_never_carry_secrets():
    import requests
    marker = SENTINEL + "LEAK"

    class Resp:
        status_code = 401
        def json(self):
            return {"error": {"code": 401, "message": marker}, "detail": marker}
    orig = requests.request
    try:
        requests.request = lambda *a, **k: Resp()
        out = f.fetch_apple({**ASA_CREDS, "private_key": ASA_CREDS["private_key"]}, date(2026, 9, 8), AD)
        assert isinstance(out, Unavailable) and marker not in out.reason and "BEGIN" not in out.reason

        def boom(*a, **k):
            raise RuntimeError(marker)
        requests.request = boom
        out = f.fetch_apple(ASA_CREDS, date(2026, 9, 8), AD)
        assert isinstance(out, Unavailable) and marker not in out.reason
    finally:
        requests.request = orig


# ---- change log (adspend_changes)

import adspend_changes as ch
from adspend_render_email import render as _render
from zoneinfo import ZoneInfo as _Z

CH_CFG = {"url": "https://x.supabase.co", "key": "sb_publishable_" + SENTINEL, "email": "c@x.com",
          "password": SENTINEL + "PW"}


def ch_row(entity, at="2026-09-22T15:14:51.19+00:00", platform="meta", kind="Paused", why="Over budget.",
           before=None, after="217$ per day", author="jishnu@lascade.com", rid="1"):
    return {"id": rid, "happened_at": at, "platform": platform, "kind": kind, "entity": entity,
            "before_value": before, "after_value": after, "why": why, "author_email": author}


def ch_fetch(pages, token="T"):
    calls = []

    def post(url, headers, body):
        calls.append(("POST", url, headers, body))
        return {"access_token": token} if token else {}

    def get(url, headers, params):
        calls.append(("GET", url, headers, params))
        return pages.pop(0)
    return post, get, calls


def test_changes_window_and_signin():
    post, get, calls = ch_fetch([[ch_row("TA - WW - IOS - A")], []])
    out = ch.fetch(CH_CFG, AD, "Asia/Dubai", post=post, get=get)
    assert len(out) == 1 and out[0].author == "Jishnu" and out[0].channel == "Meta"
    assert out[0].at.tzinfo is not None and out[0].at.strftime("%a %H:%M") == "Tue 19:14"   # UTC+4
    assert calls[0][0] == "POST" and calls[0][1].endswith("/auth/v1/token?grant_type=password")
    params = calls[1][3]
    assert ("happened_at", "gte.2026-09-20T20:00:00Z") in params      # Dubai 21 Sep 00:00
    assert ("happened_at", "lt.2026-09-22T20:00:00Z") in params       # Dubai 23 Sep 00:00
    assert calls[1][2]["Authorization"] == "Bearer T"


def test_changes_paginate():
    orig = ch.PAGE
    try:
        ch.PAGE = 2
        post, get, calls = ch_fetch([[ch_row("A", rid="1"), ch_row("B", rid="2")], [ch_row("C", rid="3")], []])
        out = ch.fetch(CH_CFG, AD, post=post, get=get)
        assert len(out) == 3
        assert [dict(c[3])["offset"] for c in calls[1:]] == ["0", "2", "3"]
        # A server row cap below PAGE (2 per page, PAGE 1000) must not truncate.
        ch.PAGE = 1000
        post, get, calls = ch_fetch([[ch_row("A"), ch_row("B")], [ch_row("C")], []])
        assert len(ch.fetch(CH_CFG, AD, post=post, get=get)) == 3
    finally:
        ch.PAGE = orig


def test_changes_failure_is_unavailable_and_sanitized():
    import requests

    class Resp:
        def __init__(self, code, body=None):
            self.status_code, self._b = code, body
        def json(self):
            if self._b is None:
                raise ValueError(SENTINEL)
            return self._b
    orig = requests.request
    try:
        for resp, word in ((Resp(401, {"msg": SENTINEL}), "auth"), (Resp(500, {"msg": SENTINEL}), "http"),
                           (Resp(200, None), "shape")):
            requests.request = lambda *a, _r=resp, **k: _r
            out = ch.fetch(CH_CFG, AD)
            assert isinstance(out, Unavailable) and out.reason == word, (word, out)

        def slow(*a, **k):
            raise requests.Timeout(SENTINEL + "https://x.supabase.co")
        requests.request = slow
        assert ch.fetch(CH_CFG, AD).reason == "timeout"
    finally:
        requests.request = orig
    post, get, _ = ch_fetch([], token=None)
    assert ch.fetch(CH_CFG, AD, post=post, get=get).reason == "auth"
    post, get, _ = ch_fetch([{"not": "a list"}])
    assert ch.fetch(CH_CFG, AD, post=post, get=get).reason == "shape"
    post, get, _ = ch_fetch([[ch_row("A", at="garbage")], []])
    assert ch.fetch(CH_CFG, AD, post=post, get=get).reason == "shape"
    # Overall deadline: a request that never finishes can't hold up the send.
    import time as _t
    post, _g, _ = ch_fetch([])
    t0 = _t.monotonic()
    out = ch.fetch(CH_CFG, AD, post=post, get=lambda *a: _t.sleep(5), deadline=0.2)
    assert out.reason == "timeout" and _t.monotonic() - t0 < 2


def test_changes_load_contract():
    assert ch.load({}) is None and ch.load({"changes": None}) is None
    assert ch.load({"changes": {"url": "u"}}) is None
    assert ch.load({"changes": CH_CFG}) == CH_CFG


def _c(entity, channel="Meta", at=datetime(2026, 9, 22, 19, 14, tzinfo=_Z("Asia/Dubai")), **kw):
    base = dict(at=at, channel=channel, app="", kind="Paused", entity=entity, before="", after="", why="", author="Jishnu")
    base.update(kw)
    return ch.Change(**base)


def test_place_matches_spend_attribution():
    model = full_model()
    ta_ios = _c("TA - WW - IOS - A")
    unattributed = _c("Nope - 2")            # app field is never consulted
    google_ta = _c("TA - WW - ANDROID", channel="Google")    # no Google line has spend
    by_line, orphans = ch.place([ta_ios, unattributed, google_ta], model, APPS)
    assert by_line[("Travel Animator", "Meta", "iOS")] == [ta_ios]
    assert by_line[("Unattributed", "Meta", "Unassigned")] == [unattributed]
    assert orphans == [google_ta]
    # The key is the one build_model gives the campaign's spend, Apple always iOS.
    asa = _c("TA - TIER 1 - BRAND - EXACT", channel="Apple Search Ads")
    assert ch.line_key(asa, APPS) == ("Travel Animator", "Apple Search Ads", "iOS")
    model2 = m.build_model({"Meta": [], "Google": [], "Apple Search Ads":
                            [row("TA - TIER 1 - BRAND - EXACT", AD, 12, ch="Apple Search Ads")]},
                           TABLE, AD, NOW, APPS)
    assert ch.place([asa], model2, APPS)[0] == {("Travel Animator", "Apple Search Ads", "iOS"): [asa]}


def test_render_cause_line_and_no_change_logged():
    model = full_model()
    long_why = "word " * 60
    cause = _c("TA - WW - IOS - A", why=long_why.strip(), after="130$ per day")
    changes = {"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [cause]}, "orphans": []}
    _s, _p, h, t = _render(model, "A", [], APPS, changes)
    # No plain-English sentence: the log's own words, author first, nothing lost.
    assert "Paused · TA - WW - IOS - A · 130$ per day" in h and "<em>" not in h and "— Jishnu" not in h
    assert "Tue 19:14" in h and "Tue 19:14  Paused · TA - WW - IOS - A" in t
    assert "…" in h and long_why.strip() not in h                       # capped
    # iOS moved and has a cause: no phrase. Android started, MR stopped, no cause: phrase.
    assert h.count("no change logged") == 3                             # Android, Unattributed, MR
    assert "#c0392b" not in h and "#27ae60" not in h
    assert "as logged by the ads team" in h and "Change log unavailable today." not in h
    # Log unreachable: no phrase, a footnote instead.
    _s, _p, h2, t2 = _render(model, "A", [], APPS, {"ok": False, "by_line": {}, "orphans": []})
    assert "no change logged" not in h2 + t2
    assert "Change log unavailable today." in h2 and "Change log unavailable today." in t2
    assert "as logged by the ads team" in t and "as logged by the ads team" not in t2


def test_render_escapes_and_orphans():
    model = full_model()
    evil = _c("<script>x</script>", channel="Google", why="<b>why</b>", author="<i>", app="Pingee")
    changes = {"ok": True, "by_line": {}, "orphans": [evil]}
    h, t = _render(model, "A", [], APPS, changes)[2:]
    assert "<script>" not in h and "&lt;script&gt;" in h and "&lt;b&gt;why&lt;/b&gt;" in h
    assert "Also changed" in h and "Also changed:" in t and "Pingee Google" in t and "Pingee Google" in h
    # Steady lines never carry the phrase; nothing configured = no change-log trace at all.
    h3, t3 = _render(model, "A", [], APPS)[2:]
    assert "no change logged" not in h3 and "Also changed" not in h3 and "as logged" not in h3


def test_render_without_changes_is_byte_stable():
    import hashlib
    # Re-baselined 2026-10-11 for the fixed product order (only change for this fixture:
    # Unattributed moved after Marine Radar; verified by diffing the text output).
    want = {"A": "ca50b252087e64100cd7392c48d3d9a2de52ded2934bc76286f19fb173a6973d",
            "B": "69644beae2125ab209483f6575b548cb495d832c4a1245fea515fbd024f76146"}
    for v, sent in (("A", []), ("B", ["Spend was steady."])):
        out = _render(full_model(), v, sent, APPS)
        assert hashlib.sha256("\x00".join(out).encode()).hexdigest() == want[v], v


def test_main_never_logs_change_content():
    import io, contextlib
    import adspend_main as mm
    model = full_model()
    cause = _c("TA - WW - IOS - A", why="Secret reason " + SENTINEL)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        got = mm.load_changes({"changes": CH_CFG}, model, AD, APPS, fetch=lambda *a: [cause, _c("ZZ - 1", channel="Google")])
        none = mm.load_changes({}, model, AD, APPS)
        bad = mm.load_changes({"changes": CH_CFG}, model, AD, APPS, fetch=lambda *a: Unavailable("auth"))
    log = buf.getvalue()
    assert got["ok"] and none is None and bad == {"ok": False, "by_line": {}, "orphans": []}
    assert "changes: ok, 1 placed, 1 orphans" in log and "changes: not configured" in log
    assert "changes: unavailable (auth)" in log
    assert SENTINEL not in log and "TA - " not in log and "Jishnu" not in log and "ZZ" not in log
    # Masking covers the change-log password, key and email on Actions.
    import base64
    env = {"ADS_CREDENTIALS_JSON_B64": base64.b64encode(json.dumps({"changes": CH_CFG}).encode()).decode(),
           "GITHUB_ACTIONS": "true"}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mm.load_creds(env)
    for v in (CH_CFG["password"], CH_CFG["key"], CH_CFG["email"]):
        assert "::add-mask::" + v in buf.getvalue()
    # A multi-line or %-bearing secret: every line masked, whole value command-encoded.
    tricky = {**CH_CFG, "password": "HEAD%" + SENTINEL + "\nTAIL" + SENTINEL}
    env["ADS_CREDENTIALS_JSON_B64"] = base64.b64encode(json.dumps({"changes": tricky}).encode()).decode()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mm.load_creds(env)
    lines = buf.getvalue().splitlines()
    assert all(l.startswith("::add-mask::") for l in lines)
    assert "::add-mask::TAIL" + SENTINEL in lines and "::add-mask::HEAD%" + SENTINEL in lines
    assert "::add-mask::HEAD%25" + SENTINEL + "%0ATAIL" + SENTINEL in lines



# ---- round 2: GeoAnimator, change table, structured lead, narration, two registers
def test_app_of_tokens_and_new_projects():
    for name in ("GEO - WW EN - INST - ANDROID", "GEO - WW - INST - ANDROID", "GEO - IN EN - INST - ANDROID",
                 "Brand test - geoanimator - IN"):
        assert m.app_of(name, APPS) == "GeoAnimator", name
    assert m.app_of("MR - WW - geoanimator promo", APPS) == "Marine Radar"     # the prefix always wins
    assert m.app_of("GEOX - 1", APPS) == m.UNATTRIBUTED                      # prefix ends at a boundary
    assert m.app_of("PTW - ASA - Tier1 - Competitor", APPS) == m.UNATTRIBUTED   # not tracked
    assert m.app_of("C1WH - US - EN", APPS) == m.UNATTRIBUTED
    model = m.build_model({"Meta": [row("GEO - WW EN - INST - ANDROID", AD, 16)], "Google": [],
                           "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    assert [p["name"] for p in model["projects"]] == ["GeoAnimator"]
    geo = _c("GEO - WW EN - INST - ANDROID", kind="Launched")
    assert ch.place([geo], model, APPS)[0] == {("GeoAnimator", "Meta", "Android"): [geo]}


def _changes_fixture():
    cause = _c("TA - WW - IOS - A", kind="Paused", after="130$ per day", why="Over budget.", app="TravelAnimator")
    orphan = _c("PTW - ASA - Tier1 - Category", channel="Apple Search Ads", kind="Launched",
                after="25$ per day", app="Plant the World")
    return {"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [cause]}, "orphans": [orphan]}, cause, orphan


def test_change_table():
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    assert [r["id"] for r in table] == ["c1", "c2"] and table[0]["change"] is cause and table[1]["change"] is orphan
    facts = c.build_facts(model, "A")
    ta_ios = next(k for k, v in facts.items() if v.get("key") == ("Travel Animator", "Meta", "iOS"))
    assert table[0]["line_fact"] == ta_ios and table[1]["line_fact"] is None
    assert table[0]["dir"] == "down" and table[1]["dir"] == "up"
    assert table[0]["show"] == {"budget": "$130 a day"}
    assert (table[1]["project"], table[1]["channel"], table[1]["os"]) == (m.UNATTRIBUTED, "Apple Search Ads", "iOS")
    assert c.change_table(None, model, APPS) == [] and c.change_table({"ok": False, "by_line": {}, "orphans": []}, model, APPS) == []
    for ok, want in (("45$ per day", "45"), ("$45/day", "45"), ("45.50 $ a day", "45.50"), ("$45 daily", "45")):
        assert c.parse_budget(ok) == D(want), ok
    for bad in ("45$ per month", "$45$ per day", "about 45$ per day", "45.5.0$", "45 per day", "", None):
        assert c.parse_budget(bad) is None, bad
    assert c._budget_words(D("45.5")) == "$45.50 a day"
    assert c._direction("Budget", "40$ per day", "60$ per day") == "up"
    assert c._direction("Budget", "60$ per day", "40$ per day") == "down"
    assert c._direction("Targeting / geo", "", "45$ per day") is None
    # The cache key follows attribution: the registry moving a campaign changes it.
    apps2 = {**APPS, "Plant the World": {"short": "PTW", "prefixes": ["PTW"]}}
    assert c.change_table(changes, model, apps2)[1]["key"] != table[1]["key"]
    names = c.name_universe(APPS, table)
    assert "Jishnu" in names and "Plant the World" in names and "" not in names and "GEO" not in names


def _lead(*items):
    return json.dumps({"items": list(items)})


def _reason(raw, facts, table, names):
    try:
        c._check(raw, facts, table, names)
    except c._Reject as exc:
        return exc.word
    return None


def test_validate_reasons_are_one_word():
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    facts = c.build_facts(model, "A")
    names = c.name_universe(APPS, table) | c._names(facts)
    ios = table[0]["line_fact"]
    other = next(k for k, v in facts.items() if k not in ("t", ios))
    good = _lead({"observed": f"Travel Animator Meta spend rose to {{{ios}.d}}.", "refs": [ios],
                  "cause": {"change": "c1", "action": "Paused the iPhone campaign at {c1.budget}",
                            "reason": "because it overspent"}})
    out = c._check(good, facts, table, names)
    assert out == [f"Travel Animator Meta spend rose to {facts[ios]['show']['d']}. "
                   "Paused the iPhone campaign at $130 a day because it overspent."]
    obs = {"observed": f"Spend rose to {{{ios}.d}}.", "refs": [ios]}
    cases = {
        "shape": ["not json", _lead(), _lead({"observed": "x"}),
                  _lead({**obs, "cause": {"change": "c1", "action": "Paused it. Then more."}})],
        "digit": [_lead({"observed": "Spend rose 18%.", "refs": [ios]}),
                  _lead({**obs, "cause": {"change": "c1", "action": "Paused campaign 7."}})],
        "placeholder": [_lead({"observed": "Spend rose to {c1.budget}.", "refs": [ios]}),
                        _lead({**obs, "cause": {"change": "c1", "action": f"Paused it at {{{ios}.d}}."}})],
        "advice": [_lead({"observed": f"Spend rose to {{{ios}.d}} against budget.", "refs": [ios]}),
                   _lead({**obs, "cause": {"change": "c1", "action": "Paused it, as the team decided."}})],
        "direction": [_lead({"observed": f"Spend fell to {{{ios}.d}}.", "refs": [ios],
                             "cause": {"change": "c1", "action": "Paused it."}}),
                      _lead({**obs, "cause": {"change": "c1", "action": "Raised the spend cap."}})],
        "name": [_lead({"observed": f"Jishnu's spend rose to {{{ios}.d}}.", "refs": [ios]}),
                 _lead({**obs, "cause": {"change": "c1", "action": "Paused the Marine Radar campaign."}}),
                 _lead({**obs, "cause": {"change": "c1", "action": "Jishnu paused the iPhone campaign."}}),
                 _lead({**obs, "cause": {"change": "c1", "action": "Paused the Google campaign."}})],
        "link": [_lead({**obs, "cause": {"change": "c2", "action": "Launched it."}}),         # orphan
                 _lead({"observed": "Total spend was {t.d}.", "refs": ["t"],
                        "cause": {"change": "c1", "action": "Paused it."}}),
                 _lead({"observed": f"Spend rose to {{{ios}.d}} and {{{other}.d}}.", "refs": [ios, other],
                        "cause": {"change": "c1", "action": "Paused it."}})],
        "code": [_lead({**obs, "cause": {"change": "c1", "action": "Paused TA - WW - IOS - A."}}),
                 _lead({**obs, "cause": {"change": "c1", "action": "Paused the ASA - Brand campaign."}})],
        "length": [_lead({**obs, "cause": {"change": "c1", "action": "Paused " + "paused " * 30 + "it."}})],
    }
    for word, raws in cases.items():
        for raw in raws:
            assert _reason(raw, facts, table, names) == word, (word, raw, _reason(raw, facts, table, names))
    # A down cause under an up line is fine: the cause is checked against what the person did.
    assert c._check(_lead({**obs, "cause": {"change": "c1", "action": "Cut the iPhone campaign."}}),
                    facts, table, names)
    # "budget" belongs to causes only; "Apple" is a backed alias of Apple Search Ads.
    assert c._check(_lead({**obs, "cause": {"change": "c1", "action": "Paused it to protect the budget."}}),
                    facts, table, names)
    assert c.validate(js(("Spend rose 18%.", [ios])), facts, names) == []


def test_commentary_retries_with_reason():
    import tempfile
    model = full_model()
    facts = c.build_facts(model, "A")
    up = next(k for k, v in facts.items() if k != "t" and v["label"] == "up")
    prompts = []

    def flaky(prompt, scratch):
        prompts.append(prompt)
        return js(("Spend rose 18%.", [up])) if len(prompts) == 1 else js((f"Spend rose to {{{up}.d}}.", [up]))
    out, log = _logged(lambda: c.commentary(model, "A", tempfile.mkdtemp(), run=flaky))
    assert out and len(prompts) == 2 and "rejected for: digit" in prompts[1] and "Spend rose 18%." in prompts[1]
    assert "synopsis: retry (digit)" in log and "18%" not in log and "warning" not in log
    prompts.clear()
    bad = lambda p, s: prompts.append(p) or js(("Spend rose 18%.", [up]))
    out, log = _logged(lambda: c.commentary(model, "A", tempfile.mkdtemp(), run=bad))
    assert out == [] and len(prompts) == 3 and "::warning title=Ad spend synopsis skipped::rejected:digit" in log
    from claude_api import ClaudeError
    calls = []

    def down(p, s):
        calls.append(1)
        raise ClaudeError("rate-limit")
    out, log = _logged(lambda: c.commentary(model, "A", tempfile.mkdtemp(), run=down))
    assert out == [] and len(calls) == 1 and "skipped::rate-limit" in log


def test_commentary_with_changes_reaches_claude_and_the_cache():
    import tempfile
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    names = c.name_universe(APPS, table)
    ios = table[0]["line_fact"]
    seen = []

    def ok(prompt, scratch):
        seen.append(prompt)
        return _lead({"observed": f"Travel Animator spend rose to {{{ios}.d}}.", "refs": [ios],
                      "cause": {"change": "c1", "action": "Paused the iPhone campaign at {c1.budget}."}})
    d_ = tempfile.mkdtemp()
    out = c.commentary(model, "A", d_, run=ok, table=table, names=names)
    assert out[0].endswith("Paused the iPhone campaign at $130 a day.")
    assert '"line_fact"' in seen[0] and "Over budget." in seen[0]
    c.commentary(model, "A", d_, run=ok, table=table, names=names)
    assert len(seen) == 1                                     # cached
    c.commentary(model, "A", d_, run=ok, table=table[:1], names=names)
    assert len(seen) == 2                                     # a different change set is not this prose


def test_narrate():
    import tempfile
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    names = c.name_universe(APPS, table)
    calls = []

    def half(prompt, scratch, max_tokens=None):
        calls.append((prompt, max_tokens))
        return json.dumps({"c1": "Paused the iPhone campaign at {c1.budget} because it overspent.",
                           "c2": "Launched PTW - ASA - Category."})
    d_ = tempfile.mkdtemp()
    out, log = _logged(lambda: c.narrate(table, d_, half, names=names))
    assert out == {table[0]["key"]: "Paused the iPhone campaign at $130 a day because it overspent."}
    assert len(calls) == 3 and "c2: code" in calls[1][0] and calls[0][1] == 4096 + 120 * 2
    assert "narration: ok 1 of 2" in log and "narration: retry (code)" in log
    assert "Jishnu" not in log and "PTW" not in log and "iPhone" not in log
    cached = json.load(open(os.path.join(d_, "narration.json")))
    assert cached == {table[0]["key"]: "Paused the iPhone campaign at {self.budget} because it overspent."}
    calls.clear()
    fixed = lambda p, s, max_tokens=None: calls.append(p) or json.dumps(
        {"c2": "Launched a Plant the World category campaign on Apple at {c2.budget}."})
    out2, _ = _logged(lambda: c.narrate(table, d_, fixed, names=names))
    assert len(calls) == 1 and '"c1"' not in calls[0].split("CHANGES:")[1]     # c1 came from the cache
    assert out2[table[1]["key"]].endswith("at $25 a day.") and out2[table[0]["key"]].endswith("overspent.")
    row = table[0]
    for text, word in (("Paused it. Then resumed it.", "shape"), ("Paused ads.Resumed.", "shape"),
                       ("Did " + "x " * 25 + "done.", "length"), ("Paused the Marine Radar ads.", "name"),
                       ("Paused campaign 7.", "digit"), ("Jishnu paused it.", "name"),
                       ("Paused it at {c2.budget}.", "placeholder"), ("Should pause it.", "advice")):
        try:
            c._check_narration(text, row, names)
            raise AssertionError(text)
        except c._Reject as exc:
            assert exc.word == word, (text, exc.word)
    assert c._check_narration("Set the budget to {c1.budget}.", row, names) == "Set the budget to $130 a day."
    # An empty logged app never poisons the name check.
    blank = c.change_table({"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [_c("TA - WW - IOS - A", app="")]},
                            "orphans": []}, model, APPS)
    assert c._check_narration("Paused the iPhone campaign.", blank[0], c.name_universe(APPS, blank))
    # A cached template that no longer validates is regenerated.
    json.dump({table[0]["key"]: "Paused it. Twice."}, open(os.path.join(d_, "narration.json"), "w"))
    calls.clear()
    _logged(lambda: c.narrate(table[:1], d_, lambda p, s, max_tokens=None: calls.append(p) or json.dumps(
        {"c1": "Paused the iPhone campaign."}), names=names))
    assert len(calls) == 1
    # Batches, transport failure, empty table.
    many = [{**table[0], "id": f"c{i}", "key": f"k{i}"} for i in range(1, 42)]
    sizes = []
    _logged(lambda: c.narrate(many, tempfile.mkdtemp(), lambda p, s, max_tokens=None: sizes.append(max_tokens) or json.dumps(
        {f"c{i}": "Paused the iPhone campaign." for i in range(1, 42)}), names=names))
    assert sizes == [4096 + 120 * 40, 4096 + 120 * 1]
    from claude_api import ClaudeError

    def boom(*a, **k):
        raise ClaudeError("auth")
    out3, log3 = _logged(lambda: c.narrate(table, tempfile.mkdtemp(), boom, names=names))
    assert out3 == {} and "narration: unavailable (auth)" in log3
    assert c.narrate([], d_, boom, names=names) == {}


def test_api_runner_leaves_room_for_thinking():
    import tempfile
    seen = []
    real = c.generate_text
    c.generate_text = lambda prompt, **kw: seen.append(kw) or json.dumps({"c1": "Paused the iPhone campaign."})
    try:
        c._run_claude("prompt", "scratch")                     # synopsis: the runner's default
        model = full_model()
        changes, _cause, _orphan = _changes_fixture()
        table = c.change_table(changes, model, APPS)[:1]
        _logged(lambda: c.narrate(table, tempfile.mkdtemp(), c._run_claude, names=c.name_universe(APPS, table)))
    finally:
        c.generate_text = real
    assert seen == [{"max_tokens": 4096, "timeout": 90}, {"max_tokens": 4096 + 120, "timeout": 90}]


def test_render_two_registers():
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    changes = {**changes, "text": {cause: "Paused the iPhone campaign because it overspent."}}
    _s, _p, h, t = _render(model, "A", [], APPS, changes)
    assert ('<span style="font-size:14px; color:#222222;" class="text-main">Paused the iPhone campaign '
            'because it overspent.</span>') in h
    assert 'class="text-muted">Tue 19:14</span>' in h and "<em>" not in h and "— Jishnu" not in h
    assert "      Tue 19:14  Paused the iPhone campaign because it overspent." in t
    # The orphan has no sentence: the log's words, with its app and channel, still escaped.
    assert ('class="text-muted">Tue 19:14 · Plant the World Apple Search Ads</span>&nbsp;&nbsp;<span style="font-size:14px; '
            'color:#222222;" class="text-main">Launched · PTW - ASA - Tier1 - Category · 25$ per day') in h
    assert "  Tue 19:14 · Plant the World Apple Search Ads  Launched" in t
    # A narrated orphan keeps its app and channel in the stamp.
    narrated = {**changes, "text": {**changes["text"], orphan: "Launched a category campaign."}}
    h4 = _render(model, "A", [], APPS, narrated)[2]
    assert 'Tue 19:14 · Plant the World Apple Search Ads</span>&nbsp;&nbsp;<span style="font-size:14px; color:#222222;" class="text-main">Launched a category campaign.' in h4
    assert "Change log &#8599;" in h and "Change log: https://jishnu-ads-taskboard.vercel.app/" in t
    assert "plain-English rendering" in h
    h0, t0 = _render(model, "A", [], APPS)[2:]
    assert "Change log" not in h0 and "Change log" not in t0


def test_main_picks_runner_and_guards_none():
    import adspend_main as mm
    for env, which, want in (({"CLAUDE_API_KEY": "k"}, lambda n: "/x/claude", "claude: api"),
                             ({}, lambda n: "/x/claude", "claude: cli"),
                             ({"GITHUB_ACTIONS": "true"}, lambda n: "/x/claude", "claude: none"),
                             ({}, lambda n: None, "claude: none")):
        run, log = _logged(lambda: mm.pick_runner(env, which))
        assert log.strip() == want, (env, log)
        assert run is (mm._run_cli if want == "claude: cli" else mm._run_claude)
    model = full_model()
    assert c.change_table(None, model, APPS) == []
    assert c.name_universe(APPS, [])            # no table: still a usable name set



def test_codex_round2_findings():
    import tempfile
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    names = c.name_universe(APPS, table)
    facts = c.build_facts(model, "A")
    ios = table[0]["line_fact"]
    # 1. An observation carries no cause, no code and one sentence only.
    for observed, word in (("Total spend was {t.d} after pausing TA - WW - A.", "code"),
                           ("Total spend was {t.d} because Meta was paused.", "cause"),
                           ("Total spend was {t.d} after stopping the campaign.", "cause"),
                           ("Spend fell to {t.d} following campaign pauses.", "cause"),
                           ("Total spend was {t.d}. It was quiet.", "shape")):
        assert _reason(_lead({"observed": observed, "refs": ["t"]}), facts, table, names) == word, observed
    # 2. Identical changes share one sentence; a shifted id still reuses the cached template.
    twin = {"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [cause, cause]}, "orphans": []}
    t2 = c.change_table(twin, model, APPS)
    assert t2[0]["key"] == t2[1]["key"] and (t2[0]["id"], t2[1]["id"]) == ("c1", "c2")
    d_ = tempfile.mkdtemp()
    calls = []
    run = lambda p, s, max_tokens=None: calls.append(p) or json.dumps(
        {"c1": "Paused the iPhone campaign at {c1.budget}.", "c2": "Paused it at {c2.budget}."})
    out, _ = _logged(lambda: c.narrate(t2, d_, run, names=names))
    assert out == {t2[0]["key"]: "Paused the iPhone campaign at $130 a day."} and len(calls) == 1
    shifted = {"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [_c("TA - WW - IOS - A", kind="Launched", at=cause.at.replace(hour=20)), cause]},
               "orphans": []}
    t3 = c.change_table(shifted, model, APPS)
    assert t3[1]["change"] is cause and t3[1]["id"] == "c2"
    calls.clear()
    out, _ = _logged(lambda: c.narrate(t3, d_, lambda p, s, max_tokens=None: calls.append(p) or json.dumps(
        {"c1": "Launched the iPhone campaign."}), names=names))
    assert out[t3[1]["key"]] == "Paused the iPhone campaign at $130 a day." and '"c2"' not in calls[0].split("CHANGES:")[1]
    # 3. A short code of another project is a name; lower-case "geo" is not.
    for text, word in (("Paused the MR campaign.", "name"), ("Jishnu paused the campaign.", "name")):
        try:
            c._check_narration(text, table[0], names); raise AssertionError(text)
        except c._Reject as exc:
            assert exc.word == word
    assert c._check_narration("Paused the TA iPhone campaign over geo targeting.", table[0], names)
    # 4. The logged spelling of a backed app is the same name.
    assert c._check(_lead({"observed": f"TravelAnimator Meta spend rose to {{{ios}.d}}.", "refs": [ios]}),
                    facts, table, names)
    # 5. Direction is judged on the action, not on the reason.
    assert c._check(_lead({"observed": f"Spend rose to {{{ios}.d}}.", "refs": [ios],
                           "cause": {"change": "c1", "action": "Paused the campaign",
                                     "reason": "over higher acquisition costs, which had increased."}}),
                    facts, table, names)
    assert _reason(_lead({"observed": f"Spend rose to {{{ios}.d}}.", "refs": [ios],
                          "cause": {"change": "c1", "action": "Paused the campaign because costs increased"}}),
                   facts, table, names) == "cause"
    assert _reason(_lead({"observed": f"Spend rose to {{{ios}.d}}.", "refs": [ios],
                          "cause": {"change": "c1", "action": "Set the campaign to a higher level"}}),
                   facts, table, names) == "direction"
    # Honest factual observations pass.
    for observed in ("Travel Animator Meta spend changed from {%s.d1} to {%s.d}." % (ios, ios),
                     "Travel Animator Meta spend rose since yesterday to {%s.d}." % ios,
                     "Travel Animator Meta spend was {%s.d} vs. {%s.d1} the day before." % (ios, ios)):
        assert c._check(_lead({"observed": observed, "refs": [ios]}), facts, table, names), observed
    # A registered alias of another project is still that project; validate() keeps the short codes.
    assert _reason(_lead({"observed": f"Geo Animator Meta spend rose to {{{ios}.d}}.", "refs": [ios]}),
                   facts, table, names) == "name"
    assert c.validate(_lead({"observed": f"MR spend rose to {{{ios}.d}}.", "refs": [ios]}), facts, names) == []
    # An unwritable cache never stops the email.
    ro = tempfile.mkdtemp()
    os.makedirs(os.path.join(ro, "commentary-A.json"))          # a directory where the file should go
    os.makedirs(os.path.join(ro, "narration.json"))
    assert c.commentary(model, "A", ro, run=lambda p, s: _lead({"observed": f"Travel Animator spend rose to {{{ios}.d}}.",
                                                                 "refs": [ios]}), table=table, names=names)
    out, _ = _logged(lambda: c.narrate(table[:1], ro, lambda p, s, max_tokens=None: json.dumps(
        {"c1": "Paused the iPhone campaign."}), names=names))
    assert out
    assert _reason(_lead({"observed": f"Spend rose to {{{ios}.d}}.", "refs": [ios],
                          "cause": {"change": "c1", "action": "Raised the campaign", "reason": "because it worked"}}),
                   facts, table, names) == "direction"
    # 6. A cached lead is re-validated: a rule that now fails it forces a fresh answer.
    d_ = tempfile.mkdtemp()
    good = lambda p, s: _lead({"observed": f"Travel Animator spend rose to {{{ios}.d}}.", "refs": [ios]})
    assert c.commentary(model, "A", d_, run=good, table=table, names=names)
    path = os.path.join(d_, "commentary-A.json")
    cached = json.load(open(path))
    assert cached["v"] == c.CACHE_VERSION and cached["raw"]
    cached["raw"] = _lead({"observed": f"Jishnu's spend rose to {{{ios}.d}}.", "refs": [ios]})
    json.dump(cached, open(path, "w"))
    calls = []
    c.commentary(model, "A", d_, run=lambda p, s: calls.append(p) or good(p, s), table=table, names=names)
    assert len(calls) == 1



def test_synopsis_salvage_after_three_failures():
    import tempfile
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    names = c.name_universe(APPS, table)
    ios = table[0]["line_fact"]
    other = next(k for k, v in c.build_facts(model, "A").items() if k not in ("t", ios))
    long_cause = {"change": "c1", "action": "Paused the iPhone campaign", "reason": "because " + "it overspent " * 15}
    answer = _lead({"observed": f"Travel Animator Meta spend rose to {{{ios}.d}}.", "refs": [ios], "cause": long_cause},
                   {"observed": "Spend rose 18%.", "refs": [other]})
    calls = []
    d_ = tempfile.mkdtemp()
    out, log = _logged(lambda: c.commentary(model, "A", d_, run=lambda p, s: calls.append(p) or answer,
                                            table=table, names=names))
    facts = c.build_facts(model, "A")
    assert out == [f"Travel Animator Meta spend rose to {facts[ios]['show']['d']}."]     # cause and bad item dropped
    assert len(calls) == 3 and "synopsis: partial (length)" in log and "warning" not in log
    assert not os.path.exists(os.path.join(d_, "commentary-A.json"))                   # a salvage is never cached
    # A later call failing in transport still leaves the first answer's good item.
    from claude_api import ClaudeError
    seq = [answer]

    def then_timeout(p, s):
        if seq:
            return seq.pop()
        raise ClaudeError("timeout")
    out, log = _logged(lambda: c.commentary(model, "A", tempfile.mkdtemp(), run=then_timeout, table=table, names=names))
    assert out == [f"Travel Animator Meta spend rose to {facts[ios]['show']['d']}."] and "synopsis: partial" in log
    assert "warning" not in log
    fenced = "```json\n" + _lead({"observed": f"Travel Animator Meta spend rose to {{{ios}.d}}.", "refs": [ios]}) + "\n```"
    assert c.commentary(model, "A", d_, run=lambda p, s: fenced, table=table, names=names)
    assert os.path.exists(os.path.join(d_, "commentary-A.json"))



def test_budget_unit_is_never_doubled():
    model = full_model()
    changes, cause, orphan = _changes_fixture()
    table = c.change_table(changes, model, APPS)
    names = c.name_universe(APPS, table)
    for text, want in (("Paused the iPhone campaign, budgeted at {c1.budget} daily.", "budgeted at $130 a day."),
                       ("Paused the iPhone campaign at {c1.budget} a day.", "at $130 a day."),
                       ("Paused the iPhone campaign on a daily budget of {c1.budget}.", "on a budget of $130 a day.")):
        assert c._check_narration(text, table[0], names).endswith(want), text



def test_prose_never_names_a_person():
    model = full_model()
    named = _c("TA - WW - IOS - A", why="Jishnu asked for this; jishnu's call.", after="130$ per day")
    changes = {"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [named]}, "orphans": []}
    h, t = _render(model, "A", [], APPS, changes)[2:]
    h, t = (x.replace(r.CHANGE_LOG_URL, "") for x in (h, t))     # the log's web address is not prose
    assert "jishnu" not in h.lower() and "jishnu" not in t.lower()
    assert "the ads team asked for this; the ads team call." in t
    table = c.change_table(changes, model, APPS)
    names = c.name_universe(APPS, table)
    assert "author" not in c._payload_row(table[0])
    for text in ("Jishnu paused the iPhone campaign.", "Paused the iPhone campaign because Jishnu asked."):
        try:
            c._check_narration(text, table[0], names)
            raise AssertionError(text)
        except c._Reject as exc:
            assert exc.word == "name"



# ---- fixed product order, quiet dark lines
def test_fixed_product_order():
    rows = [row("PINGEE - WW", AD, 500), row("Nope - 1", AD, 400), row("GEO - WW - ANDROID", AD, 300),
            row("Mail AI - US", AD, 500), row("MR - IOS", AD, 20), row("TA - IOS", AD, 10), row("AR - IOS", AD, 5)]
    model = m.build_model({"Meta": rows, "Google": [], "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    assert [p["name"] for p in model["projects"]] == [
        "Travel Animator", "Marine Radar", "Smart Ruler", "GeoAnimator", "Mail AI", "Pingee", m.UNATTRIBUTED]
    assert [APPS[n]["rank"] for n in ("Travel Animator", "Marine Radar", "Smart Ruler", "GeoAnimator")] == [1, 2, 3, 4]


def _fold_model():
    rows = [row("TA - IOS", d(1), 90), row("TA - IOS", AD, 100),                  # active
            row("TA - ANDROID", d(3), 20),                                        # dark
            row("TA - WW", d(4), 30, ch="Google"),                                 # dark, no OS
            row("TA - X - ANDROID", d(1), 50, ch="Google"),                        # stopped yesterday
            row("MR - IOS", d(3), 10)]                                            # MR: dark only
    google = [r_ for r_ in rows if r_.channel == "Google"]
    return m.build_model({"Meta": [r_ for r_ in rows if r_.channel == "Meta"], "Google": google,
                          "Apple Search Ads": []}, TABLE, AD, NOW, APPS)


def test_dark_lines_fold():
    model = _fold_model()
    h, t = _render(model, "A", [], APPS)[2:]
    assert "  No spend: Google · Meta Android" in t
    assert "Google: $0" not in t and "Meta Android: $0" not in t                  # no $0 rows for dark lines
    assert "Google Android: $0 - spend stopped, was $50 Mon" in t                  # a stop stays loud
    assert "  Meta iOS: $100 - steady vs Mon" in t
    assert "Marine Radar  $0\n  No spend: Meta iOS" in t                           # heading + fold only
    assert ">No spend: Google · Meta Android</p>" in h and 'class="text-muted">No spend: Meta iOS</p>' in h
    assert h.count("No spend:") == 2
    # A dark line with a logged change keeps its row; the fold names only the rest.
    dark = _c("TA - ANDROID", kind="Paused")
    changes = {"ok": True, "by_line": {("Travel Animator", "Meta", "Android"): [dark]}, "orphans": []}
    t2 = _render(model, "A", [], APPS, changes)[3]
    assert "  Meta Android: $0 - no spend Mon or yesterday" in t2 and "  No spend: Google\n" in t2


def test_facts_independent_of_display_order():
    # Twelve tied dark lines (six projects x two channels): more than MAX_FACTS, all tied at zero.
    camps = ["TA", "MR", "AR", "GEO", "PINGEE", "Mail AI"]
    meta = [row(f"{p} - IOS", d(3), 10) for p in camps]
    google = [row(f"{p} - IOS", d(3), 10, ch="Google") for p in camps]
    model = m.build_model({"Meta": meta, "Google": google, "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    lines = [ln for p in model["projects"] for ln in p["lines"]]
    assert len(lines) == 12 > c.MAX_FACTS
    flipped = {**model, "projects": [{**p, "lines": list(reversed(p["lines"]))} for p in reversed(model["projects"])]}
    for v in ("A", "B"):
        facts = c.build_facts(model, v)
        assert facts == c.build_facts(flipped, v)
        assert [f["key"] for k, f in facts.items() if k != "t"] == sorted(
            (ln["project"], ln["channel"], ln["os"]) for ln in lines)[:c.MAX_FACTS]
    # Change ids follow line keys, not the page: reordering projects renumbers nothing.
    ta, mr = _c("TA - IOS"), _c("MR - IOS")
    changes = {"ok": True, "by_line": {("Travel Animator", "Meta", "iOS"): [ta], ("Marine Radar", "Meta", "iOS"): [mr]},
               "orphans": []}
    a = [(r_["id"], r_["change"]) for r_ in c.change_table(changes, model, APPS)]
    b = [(r_["id"], r_["change"]) for r_ in c.change_table(changes, flipped, APPS)]
    assert a == b and a[0][1] is mr                                           # "Marine Radar" < "Travel Animator"

if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
    print("ok")
