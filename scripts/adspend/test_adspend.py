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
    model = m.build_model({"Meta": base_rows(), "Google": []}, TABLE, AD, NOW, APPS)
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
    model = m.build_model({"Meta": rows, "Google": []}, TABLE, AD, NOW, APPS)
    assert model["channels"]["Meta"]["status"] == "unavailable" and model["partial"]
    assert model["total"]["d"] == 0


def test_unavailable_channel_and_partial_subject():
    model = m.build_model({"Meta": base_rows(), "Google": Unavailable("Google HTTP 503")},
                          TABLE, AD, NOW, APPS)
    assert model["channels"]["Google"]["status"] == "unavailable"
    s = m.subject_of(model, "A", APPS)
    assert "partial" in s and s.startswith("Ad spend · Tue 22 Sep — $")


def test_unclosed_day_marks_partial():
    early = datetime(2026, 9, 22, 22, 0, tzinfo=timezone.utc)   # 02:00 Dubai on the 23rd? no: 02:00 on 23rd -> closed
    assert m.day_closed("Asia/Dubai", AD, early) is True
    assert m.day_closed("Asia/Dubai", AD, datetime(2026, 9, 22, 19, 0, tzinfo=timezone.utc)) is False
    model = m.build_model({"Meta": base_rows(), "Google": []}, TABLE, AD,
                          datetime(2026, 9, 22, 19, 0, tzinfo=timezone.utc), APPS)
    assert model["channels"]["Meta"]["status"] == "partial"


def test_subject_and_variants():
    model = m.build_model({"Meta": base_rows(), "Google": []}, TABLE, AD, NOW, APPS)
    a, b = m.subject_of(model, "A", APPS), m.subject_of(model, "B", APPS)
    assert a.startswith("Ad spend · Tue 22 Sep — ") and b.startswith("Ad spend + CPI · Tue 22 Sep — ")
    assert a.endswith("· MR Meta ↓$50")      # biggest absolute mover: the $50 stop


def test_installs_unavailable_yields_none():
    rows = [row("TA - IOS", d(1), 10, ch="Google", inst=None), row("TA - IOS", d(0), 20, ch="Google", inst=None)]
    model = m.build_model({"Meta": [], "Google": rows}, TABLE, AD, NOW, APPS)
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
    return m.build_model({"Meta": base_rows(), "Google": []}, TABLE, AD, NOW, APPS)


def js(*sentences):
    return json.dumps({"sentences": [{"text": t, "refs": refs} for t, refs in sentences]})


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
        raise FileNotFoundError("codex")
    assert c.commentary(full_model(), "A", d_, run=crash) == []
    assert c.commentary(full_model(), "A", d_, run=crash) == []      # cached outcome, no second call
    assert len(calls) == 1
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
    model = m.build_model({"Meta": base_rows(), "Google": Unavailable("Google HTTP 503")}, TABLE, AD, NOW, APPS)
    _, _, h, t = r.render(model, "A", [], APPS)
    assert "Google unavailable (Google HTTP 503)" in h and "partial" in h.lower()
    rows = [row("TA - IOS", d(1), 10, ch="Google", inst=None), row("TA - IOS", d(0), 20, ch="Google", inst=None)]
    mb = m.build_model({"Meta": [], "Google": rows}, TABLE, AD, NOW, APPS)
    hb = r.render(mb, "B", [], APPS)[2]
    assert "Google installs unavailable" in hb and "installs \u2014" in hb or "installs —" in hb


# ---- main (no network)
def test_main_masks_and_never_logs_figures(capsys=None):
    import base64, io, contextlib, tempfile
    import adspend_main as mm
    secret = {"meta": {"token": SENTINEL + "M", "account_ids": ["1"]},
              "google": {"client_id": "a", "client_secret": SENTINEL + "C", "refresh_token": SENTINEL + "R",
                         "dev_token": SENTINEL + "D", "login_customer_id": "1"}}
    os.environ["ADS_CREDENTIALS_JSON_B64"] = base64.b64encode(json.dumps(secret).encode()).decode()
    mm.fetch_meta = lambda *a, **k: base_rows()
    mm.fetch_google = lambda *a, **k: Unavailable("Google HTTP 503")
    mm.build_rate_table = lambda *a, **k: TABLE
    out, scratch = tempfile.mkdtemp(), tempfile.mkdtemp()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = mm.main(["--ad-date", "2026-09-22", "--out", out, "--scratch", scratch,
                      "--apps", os.path.join(HERE, "..", "..", "data", "adspend_apps.json"),
                      "--break-commentary"])
    log = buf.getvalue()
    assert rc == 0 and os.path.exists(os.path.join(out, "email-A.html")) and os.path.exists(os.path.join(out, "email-B.subject"))
    assert "::add-mask::" + SENTINEL + "M" in log
    visible = "\n".join(l for l in log.splitlines() if not l.startswith("::add-mask::"))
    assert "$" not in visible and "TA - " not in visible and SENTINEL not in visible
    assert "commentary=no" in visible and "Google: unavailable" in visible
    mm.fetch_meta = lambda *a, **k: Unavailable("Meta HTTP 500")
    with contextlib.redirect_stdout(io.StringIO()):
        assert mm.main(["--ad-date", "2026-09-22", "--out", out, "--scratch", scratch,
                        "--apps", os.path.join(HERE, "..", "..", "data", "adspend_apps.json")]) == 1


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
    print("ok")
