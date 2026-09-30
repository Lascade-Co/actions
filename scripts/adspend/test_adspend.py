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
    model = m.build_model({"Meta": base_rows(), "Google": Unavailable("Google HTTP 503"), "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    _, _, h, t = r.render(model, "A", [], APPS)
    assert "Google unavailable (Google HTTP 503)" in h and "partial" in h.lower()
    rows = [row("TA - IOS", d(1), 10, ch="Google", inst=None), row("TA - IOS", d(0), 20, ch="Google", inst=None)]
    mb = m.build_model({"Meta": [], "Google": rows, "Apple Search Ads": []}, TABLE, AD, NOW, APPS)
    hb = r.render(mb, "B", [], APPS)[2]
    assert "Google installs unavailable" in hb and "installs \u2014" in hb or "installs —" in hb


# ---- main (no network)
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

    def ok(prompt, scratch):
        calls.append(1)
        return "[]"
    m1 = full_model()
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


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
    print("ok")
