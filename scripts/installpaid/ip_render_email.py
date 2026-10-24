"""Render the install->paid model to an email: (subject, html, text, images).

Chassis (`HEAD` and the row helpers) follows scripts/adspend/adspend_render_email.py, which
was mined from `Style Template/styleguide-email.html` and proven in Gmail web/iOS, light
and dark. Design: typography does the work. Words are coloured by role so the email scans
at a glance: figures ink, context words grey, verdict words green (better), red (worse) or
blue (same / can't tell yet). Bars use the same colours against Mar 2026; grey = directional
only, no verdict. Benchmark ticks are amber (Aug 2025) and charcoal (Mar 2026).

Every section covers the same 7 full Pacific days and every build; the latest full day is
shown too, but better or worse is judged on the 7 days only.

Variant A draws every chart from table cells (works with images off and in dark mode).
Variant B draws them as PNGs (ip_charts.py), inline by CID when sent.
"""
from __future__ import annotations

from html import escape as esc

import ip_model as M
from ip_editions import get_edition

FONT_HEADING = "'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif"
FONT_BODY = "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"
ACCENT = "#2980b9"
C_BETTER, C_WORSE, C_LEVEL, C_THIN = "#2a8c4a", "#c0392b", "#2980b9", "#a8a49e"
C_AUG, C_MAR = "#9a6a10", "#444444"
# Word roles (all >= 4.5:1 on #f5f3f1; dark-mode values in HEAD).
INK, CTX = "#222222", "#66615a"
T_BETTER, T_WORSE, T_LEVEL = "#1e7a3c", "#b0392c", "#1f6aa5"
TRACK_BG = "#ebe8e4"
WORKFLOW_URL = "https://github.com/Lascade-Co/actions/actions/workflows/daily-install-paid.yml"
STEP_NAMES = {"pw": "Saw the paywall", "pv": "Previewed a trip", "ex": "Exported a video", "paid": "Paid",
              "pc": "Paid, in-app event †"}

HEAD = """\
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" lang="en" dir="ltr">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<meta http-equiv="X-UA-Compatible" content="IE=edge" />
<meta name="color-scheme" content="light dark" />
<meta name="supported-color-schemes" content="light dark" />
<meta name="format-detection" content="telephone=no, date=no, address=no, email=no" />
<title>__TITLE__</title>
<!--[if mso]>
<noscript><xml><o:OfficeDocumentSettings><o:AllowPNG/><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript>
<![endif]-->
<!--[if !mso]><!-->
<style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');</style>
<!--<![endif]-->
<!--[if mso]>
<style type="text/css">
    body, table, td, th, p, li, a, span { font-family: 'Segoe UI', Arial, sans-serif !important; }
</style>
<![endif]-->
<style type="text/css">
    #outlook a { padding: 0; }
    body { margin: 0 !important; padding: 0 !important; width: 100% !important; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
    table { border-collapse: collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; }
    td { border-collapse: collapse; }
    img { border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; display: block; -ms-interpolation-mode: bicubic; }
    p { margin: 0; Margin: 0; }
    a { color: #222222; }
    a[x-apple-data-detectors] {
        color: inherit !important; text-decoration: none !important; font-size: inherit !important;
        font-family: inherit !important; font-weight: inherit !important; line-height: inherit !important;
    }
    /* table-layout:fixed is the one mechanism that makes "fill the row" cells resolve on
       Gmail's iOS app (adspend_render_email.py, Gmail-iOS width probe T17-T20). */
    .tfix { width: 100% !important; table-layout: fixed !important; }
    @media only screen and (max-width: 480px) {
        .email-container { width: 100% !important; max-width: 100% !important; }
        .email-container td.body-cell { padding-left: 20px !important; padding-right: 20px !important; }
        h1.email-h1 { font-size: 26px !important; }
        .bar-label-cell { width: 92px !important; font-size: 11px !important; }
        .bar-val-cell { width: 70px !important; }
        .stat-fig { font-size: 22px !important; }
    }
    @media (prefers-color-scheme: dark) {
        .body-bg { background-color: #1a1a18 !important; }
        .email-body-bg { background-color: #222220 !important; }
        .card-bg { background-color: #2a2927 !important; }
        .text-main { color: #e0ddd8 !important; }
        .heading-dm { color: #ece9e4 !important; }
        .text-muted { color: #a8a39c !important; }
        .divider-line { border-color: #3a3835 !important; }
        .bar-track-dm { background-color: #33312e !important; }
        .accent-dm { color: #5b9bd5 !important; }
        .accent-border-dm { border-top-color: #5b9bd5 !important; }
        .tick-mar-dm { background-color: #d8d4ce !important; }
    }
    [data-ogsc] .body-bg { background-color: #1a1a18 !important; }
    [data-ogsc] .email-body-bg { background-color: #222220 !important; }
    [data-ogsc] .card-bg { background-color: #2a2927 !important; }
    [data-ogsc] .text-main { color: #e0ddd8 !important; }
    [data-ogsc] .heading-dm { color: #ece9e4 !important; }
    [data-ogsc] .text-muted { color: #a8a39c !important; }
    [data-ogsc] .divider-line { border-color: #3a3835 !important; }
    [data-ogsc] .bar-track-dm { background-color: #33312e !important; }
    [data-ogsc] .accent-dm { color: #5b9bd5 !important; }
    [data-ogsc] .accent-border-dm { border-top-color: #5b9bd5 !important; }
    [data-ogsc] .tick-mar-dm { background-color: #d8d4ce !important; }
    @media (prefers-color-scheme: dark) {
        .hl-num { color: #ece9e4 !important; } .hl-ctx { color: #a8a39c !important; }
        .hl-good { color: #5cb87a !important; } .hl-bad { color: #e8776b !important; } .hl-lvl { color: #6aa8dc !important; }
        .tick-aug-dm { background-color: #d9a54a !important; }
    }
    [data-ogsc] .hl-num { color: #ece9e4 !important; } [data-ogsc] .hl-ctx { color: #a8a39c !important; }
    [data-ogsc] .hl-good { color: #5cb87a !important; } [data-ogsc] .hl-bad { color: #e8776b !important; }
    [data-ogsc] .hl-lvl { color: #6aa8dc !important; } [data-ogsc] .tick-aug-dm { background-color: #d9a54a !important; }
</style>
</head>
"""

TABLE = '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"'
VERDICT_WORD = {"better": "better", "worse": "worse", "same": "same", "unclear": "can't tell yet"}
ROLE = {"better": ("hl-good", T_BETTER), "worse": ("hl-bad", T_WORSE),
        "same": ("hl-lvl", T_LEVEL), "unclear": ("hl-lvl", T_LEVEL)}
SHORT = {"pw": "Paywall", "pv": "Preview", "ex": "Export", "paid": "Paid"}


# ------------------------------------------------------------ formatting ----

def pct(p, signed=False):
    """Rates under 2% keep two decimals (Android lives there); others one."""
    if p is None:
        return "—"
    v = 100 * p
    s = f"{abs(v):.2f}" if abs(v) < 2 else f"{abs(v):.1f}"
    sign = ("+" if v > 0 else "−" if v < 0 else "±") if signed else ("−" if v < 0 else "")
    return f"{sign}{s}%"


def pts(d):
    v = 100 * d
    s = f"{abs(v):.2f}" if abs(v) < 2 else f"{abs(v):.1f}"
    return f"{'+' if v > 0 else '−' if v < 0 else '±'}{s} pts"


def num(n):
    return f"{n:,}"


def day(d):
    return f"{d:%-d %b}" if d else "—"


def day_label(d):
    return f"{d:%a %-d %b}"


def week_txt(a, b):
    """12–18 Oct · 26 Oct–1 Nov · 29 Dec 2026–4 Jan 2027."""
    if a.year != b.year:
        return f"{a:%-d %b %Y}–{b:%-d %b %Y}"
    start = f"{a:%-d}" if a.month == b.month else day(a)
    return f"{start}–{day(b)}"


def day_rate(t):
    d = t["day"]
    return pct(d["paid"] / d["n"]) if d.get("n") else "—"


def early_of(model):
    """The early read, or None when it is missing or failed."""
    return model.get("early") or None


def early_track(model, t):
    e = early_of(model)
    return e["tracks"].get(t["key"]) if e else None


def early_rate(et):
    d = (et or {}).get("day") or {}
    return pct(d["paid"] / d["n"]) if d.get("n") else "—"


def verdict_color(c):
    """Bar colour vs Mar 2026; grey when the comparison is directional only (no verdict)."""
    if not c:
        return C_LEVEL
    if c.get("verdict") is None:
        return C_THIN
    return {"better": C_BETTER, "worse": C_WORSE}.get(c["verdict"], C_LEVEL)


def steps_for(t):
    """Chart rows. Where a benchmark has no server figure (Android Aug 2025) the email
    adds a client-event row, so every mark sits next to a like-for-like figure."""
    return ["pw", "pv", "ex", "paid"] + (["pc"] if t["cmp"]["aug25"]["client_basis"] else [])


def bench_for(t, model, bk, step):
    """Benchmark value for a chart row, or None when there is no like-for-like figure."""
    if step == "paid" and t["cmp"][bk]["client_basis"]:
        return None
    return model["bench"][bk][M.plat_of(t["key"])]["rates"][step][0]


def week_of(model):
    return week_txt(model["first_day"], model["last_day"])


# ------------------------------------------------------------ word roles ----

def _n(x):
    """A figure: ink, semibold, tabular."""
    return (f'<span class="hl-num" style="color:{INK}; font-weight:600; font-variant-numeric:tabular-nums;">'
            f'{esc(str(x))}</span>')


def _c(x):
    """A context word: grey."""
    return f'<span class="hl-ctx" style="color:{CTX};">{esc(str(x))}</span>'


def _v(verdict, word=None):
    """A verdict word: the only coloured text."""
    cls, col = ROLE[verdict]
    return f'<span class="{cls}" style="color:{col}; font-weight:600;">{esc(word or VERDICT_WORD[verdict])}</span>'


def step_verdict(t, k):
    """-> (html, text): 'better vs both', or "can't tell yet vs Mar, better vs Aug".
    Android paid has no like-for-like Aug server figure: that comparison sits on the † row."""
    m, a = t["cmp"]["mar26"][k], t["cmp"]["aug25"][k]
    mv, av = m and m.get("verdict"), a and a.get("verdict")
    if k == "paid" and t["cmp"]["aug25"]["client_basis"]:
        av = "client"
    if mv and av and mv == av:
        return f'{_v(mv)} {_c("vs both")}', f"{VERDICT_WORD[mv]} vs both"
    h, x = [], []
    if mv:
        h.append(f'{_v(mv)} {_c("vs Mar")}')
        x.append(f"{VERDICT_WORD[mv]} vs Mar")
    if av == "client":
        h.append(_c("Aug: see †"))
        x.append("Aug: see †")
    elif av:
        h.append(f'{_v(av)} {_c("vs Aug")}')
        x.append(f"{VERDICT_WORD[av]} vs Aug")
    return f'{_c(",")} '.join(h), ", ".join(x)


def read_lines(t, model):
    """The 6 AM conclusion for one track: (paid html, paid text, steps html, steps text)."""
    lab = day_label(model["last_day"])
    if t["empty"]:
        s = f"No installs on {week_of(model)}."
        return _c(s), s, "", ""
    ph, px = step_verdict(t, "paid")
    week = pct(t["rates"]["paid"][0])
    l1h = (f'{_n("Paid")} &nbsp;{_c(lab)} {_n(day_rate(t))} {_c("·")} {_c("7 days")} {_n(week)} {_c("·")} {ph}')
    l1x = f"Paid  {lab} {day_rate(t)} · 7 days {week} · {px}"
    sh, sx = [], []
    for k in ("pw", "pv", "ex"):
        h, x = step_verdict(t, k)
        sh.append(f"{_n(SHORT[k])} {h}")
        sx.append(f"{SHORT[k]} {x}")
    return l1h, l1x, f'{_c("Steps")} &nbsp;' + f' &nbsp;{_c("·")}&nbsp; '.join(sh), "Steps  " + " · ".join(sx)


def builds_txt(t):
    """-> (html, text): up to 3 builds with 5+ installs, newest first, then the rest counted."""
    big = [b for b in t["builds"] if b["n"] >= 5]
    small = [b for b in t["builds"] if b["n"] < 5]
    h, x = f' {_c("·")} '.join(_n(b["v"]) for b in big[:3]), " · ".join(b["v"] for b in big[:3])
    if len(big) > 3:
        h += f' {_n("+" + str(len(big) - 3))} {_c("more")}'
        x += f" +{len(big) - 3} more"
    if small:
        k, pl = sum(b["n"] for b in small), "s" if len(small) > 1 else ""
        il = "s" if k != 1 else ""
        h += f' {_n("+" + str(len(small)))} {_c("small build" + pl + " (")}{_n(num(k))} {_c("install" + il + ")")}'
        x += f" +{len(small)} small build{pl} ({num(k)} install{il})"
    return h, x


def checks(t, model):
    """Data guard: a non-paid step under half its March rate. Neutral: no cause claimed."""
    mar = model["bench"]["mar26"][M.plat_of(t["key"])]["rates"]
    out = []
    for k in ("pw", "pv", "ex"):
        now, base = t["rates"][k][0], mar[k][0]
        if base and base > 0 and t["counts"]["n"] > 0 and now < base / 2:
            out.append((STEP_NAMES[k].lower(), now, base))
    return out


def mix_txt(t):
    if not t["mix"]:
        return "", ""
    part = lambda m: (f"Mar {pct(m['base'])}" if m["base"] is not None else None)
    h = f' {_c("·")} '.join(f'{_n(m["cc"])} {_n(pct(m["now"]))}' + (f' {_c("(" + part(m) + ")")}' if part(m) else "")
                            for m in t["mix"])
    x = " · ".join(f"{m['cc']} {pct(m['now'])}" + (f" ({part(m)})" if part(m) else "") for m in t["mix"])
    return f'{_c("Install mix")} {h}', f"Install mix {x}"


# ---------------------------------------------------------------- chrome ----

def _section_open():
    return (f'{TABLE} style="background-color:#f5f3f1;" bgcolor="#f5f3f1" class="body-bg email-body-bg">')


def _row(inner, top=24):
    return _section_open() + f'<tr><td style="padding:{top}px 40px 0 40px;" class="body-cell">{inner}</td></tr></table>'


def _p(text, size=15, weight=400, color=INK, cls="text-main", mt=0, extra=""):
    return (f'<p style="margin:{mt}px 0 0 0; Margin:{mt}px 0 0 0; font-family:{FONT_BODY}; '
            f'font-size:{size}px; font-weight:{weight}; line-height:1.5; color:{color};{extra}" '
            f'class="{cls}">{text}</p>')


def _muted(text, size=12, mt=0, extra=""):
    return _p(text, size=size, color=CTX, cls="text-muted", mt=mt, extra=extra)


def _caps(text, color=CTX, cls="text-muted"):
    return (f'<span style="font-family:{FONT_HEADING}; font-size:11px; font-weight:600; '
            f'text-transform:uppercase; letter-spacing:0.04em; color:{color};" class="{cls}">{esc(text)}</span>')


def _masthead(eyebrow, title, dateline):
    return (
        _section_open() +
        f'<tr><td style="padding:40px 40px 8px 40px;" class="body-cell">'
        f'<p style="margin:0; font-family:{FONT_HEADING}; font-size:12px; font-weight:700; '
        f'letter-spacing:0.08em; text-transform:uppercase; color:{ACCENT};" class="accent-dm">{esc(eyebrow)}</p></td></tr>'
        f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
        f'<h1 class="email-h1 heading-dm text-main" style="margin:0; font-family:{FONT_HEADING}; '
        f'font-size:30px; font-weight:700; line-height:1.1; letter-spacing:-0.02em; color:{INK};">{esc(title)}</h1></td></tr>'
        f'<tr><td style="padding:6px 40px 0 40px;" class="body-cell">'
        f'<p style="margin:0; font-family:{FONT_HEADING}; font-size:12px; font-weight:600; '
        f'text-transform:uppercase; letter-spacing:0.04em; color:{CTX};" class="text-muted">{esc(dateline)}</p></td></tr>'
        f'<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
        f'{TABLE} class="tfix"><tr><td bgcolor="{ACCENT}" style="border-top:3px solid {ACCENT}; font-size:1px; '
        f'line-height:1px; mso-line-height-rule:exactly;" class="accent-border-dm">&nbsp;</td></tr></table></td></tr></table>')


def dateline(model):
    rd = model["report_date"]
    early = f" · early {day_label(early_of(model)['day'])}" if early_of(model) else ""
    return f"{rd:%a %-d %b}{early} · latest full day {day_label(model['last_day'])} · last 7 days {week_of(model)}"


def _stat_grid(model):
    lab = day_label(model["last_day"])
    cells = []
    for t in model["tracks"]:
        d = t["day"]
        et = early_track(model, t)
        if t["empty"]:
            gloss, week = _c("no installs"), ""
        else:
            pv = (t["cmp"]["mar26"]["paid"] or {}).get("verdict") or "unclear"
            cls, col = ROLE[pv]
            gloss = f'{_c(lab + " ·")} {_n(d.get("paid", 0))} {_c("of")} {_n(num(d.get("n", 0)))}'
            week = (f'<p style="margin:6px 0 0 0; font-family:{FONT_HEADING}; font-size:13px; font-weight:700; '
                    f'color:{col};" class="{cls}">7 days: {esc(pct(t["rates"]["paid"][0]))}</p>')
        if et is not None:
            ed = et["day"]
            elab = day_label(early_of(model)["day"])
            gloss = (f'{_c(elab + " (early) ·")} {_n(ed.get("paid", 0))} {_c("of")} {_n(num(ed.get("n", 0)))}'
                     + (f'</p><p style="margin:2px 0 0 0; font-family:{FONT_BODY}; font-size:12px;">'
                        f'{_c(lab + " ·")} {_n(day_rate(t))}' if not t["empty"] else ""))
            figure = early_rate(et)
        else:
            figure = day_rate(t) if not t["empty"] else "—"
        cells.append(
            f'<td valign="top" style="padding:0 8px 0 0;"><div style="min-height:30px;">{_caps(t["label"])}</div>'
            f'<p style="margin:4px 0 0 0; font-family:{FONT_HEADING}; font-size:26px; font-weight:700; '
            f'line-height:1.1; color:{INK};" class="stat-fig hl-num">{esc(figure)}</p>'
            f'<p style="margin:4px 0 0 0; font-family:{FONT_BODY}; font-size:12px;">{gloss}</p>{week}</td>')
    return _row(f'{TABLE} class="tfix"><tr>{"".join(cells)}</tr></table>', 28)


def early_lines(t, model):
    """(paid html, paid text, steps html, steps text) for the newest day's early read."""
    e, et = early_of(model), early_track(model, t)
    lab = day_label(e["day"])
    if not et["day"].get("n"):
        s = f"No installs yet on {lab}."
        return _c(s), s, "", ""
    base = week_txt(model["first_day"], model["last_day"])

    def word(k):
        c = et["cmp"].get(k)
        return (c or {}).get("verdict") or "unclear"
    pv = word("paid")
    ph = (f'{_n("Paid")} &nbsp;{_c(lab)} {_n(early_rate(et))} {_c("·")} {_v(pv)} {_c("vs " + base)}')
    px = f"Paid  {lab} {early_rate(et)} · {VERDICT_WORD[pv]} vs {base}"
    sh = [f"{_n(SHORT[k])} {_v(word(k))}" for k in ("pw", "pv", "ex")]
    sx = [f"{SHORT[k]} {VERDICT_WORD[word(k)]}" for k in ("pw", "pv", "ex")]
    return ph, px, f'{_c("Steps")} &nbsp;' + f' &nbsp;{_c("·")}&nbsp; '.join(sh), "Steps  " + " · ".join(sx)


def early_caption(model):
    e = early_of(model)
    label = get_edition(model.get("edition", "pt")).label
    return (f"Early read, provisional: what {day_label(e['day'])} installs had done by 05:00 {label} "
            f"{day_label(model['report_date'])}. Late events can still change it. Compared with the 7 days before "
            f"({week_txt(model['first_day'], model['last_day'])}), measured the same way.")


def _early(model):
    if not early_of(model):
        return ""
    out = [f'<p style="margin:0; font-family:{FONT_BODY}; font-size:12px; color:{CTX};" class="hl-ctx">'
           f'{esc(early_caption(model))}</p>']
    for t in model["tracks"]:
        l1, _, l2, _ = early_lines(t, model)
        out.append(f'<p style="margin:14px 0 0 0; font-family:{FONT_HEADING}; font-size:15px; font-weight:700; '
                   f'color:{INK};" class="hl-num">{esc(t["label"])}</p>'
                   f'<p style="margin:2px 0 0 0; font-family:{FONT_BODY}; font-size:15px; line-height:1.55;" '
                   f'class="text-main">{l1}</p>' +
                   (f'<p style="margin:2px 0 0 0; font-family:{FONT_BODY}; font-size:15px; line-height:1.55;" '
                    f'class="text-main">{l2}</p>' if l2 else ""))
    return _row("".join(out), 24)


def _lead(model):
    out = [f'<p style="margin:0; font-family:{FONT_BODY}; font-size:12px; color:{CTX};" class="hl-ctx">'
           f'7-day verdicts vs Mar 2026 / Aug 2025</p>']
    for t in model["tracks"]:
        l1, _, l2, _ = read_lines(t, model)
        out.append(f'<p style="margin:14px 0 0 0; font-family:{FONT_HEADING}; font-size:15px; font-weight:700; '
                   f'color:{INK};" class="hl-num">{esc(t["label"])}</p>'
                   f'<p style="margin:2px 0 0 0; font-family:{FONT_BODY}; font-size:15px; line-height:1.55;" '
                   f'class="text-main">{l1}</p>' +
                   (f'<p style="margin:2px 0 0 0; font-family:{FONT_BODY}; font-size:15px; line-height:1.55;" '
                    f'class="text-main">{l2}</p>' if l2 else ""))
    return _row("".join(out), 24)


def _legend():
    sw = lambda c, w, h, cls="": (f'<span class="{cls}" style="display:inline-block; width:{w}px; height:{h}px; '
                                  f'background-color:{c}; vertical-align:middle;"></span>')
    items = [(sw(C_BETTER, 10, 10), "better"), (sw(C_WORSE, 10, 10), "worse"),
             (sw(C_LEVEL, 10, 10), "same / can't tell yet (bar colour vs Mar 2026)"),
             (sw(C_THIN, 10, 10), "grey: directional only, no verdict"),
             (sw(C_AUG, 3, 12, "tick-aug-dm"), "Aug 2025"), (sw(C_MAR, 3, 12, "tick-mar-dm"), "Mar 2026"),
             ("", "deltas in points vs the named month")]
    body = " &nbsp;&nbsp; ".join(
        f'<span style="white-space:nowrap;">{s}&nbsp;<span style="font-family:{FONT_BODY}; font-size:11px; '
        f'color:{CTX};" class="hl-ctx">{esc(x)}</span></span>' for s, x in items)
    return f'<p style="margin:10px 0 0 0; line-height:1.8;">{body}</p>'


# ------------------------------------------------------ variant A charts ----

def _w(x):
    return max(0.0, min(100.0, round(x, 1)))


def _bar(value, scale, color, ticks):
    """One bar on a 0..scale track, plus a tick row underneath for the benchmarks.
    ticks: [(value, color, cls)]. All table cells: no images, survives dark mode."""
    fill = _w(100 * value / scale) if value else 0.0
    bar = (f'{TABLE} class="tfix"><tr>' +
           (f'<td width="{fill}%" bgcolor="{color}" style="background-color:{color}; height:16px; font-size:0; '
            f'line-height:0;">&nbsp;</td>' if fill >= 0.5 else '') +
           (f'<td bgcolor="{TRACK_BG}" style="background-color:{TRACK_BG}; height:16px; font-size:0; line-height:0;" '
            f'class="bar-track-dm">&nbsp;</td>' if fill < 99.5 else '') + '</tr></table>')
    cells, pos = [], 0.0
    for v, c, cls in sorted(((tv, tc, tcl) for tv, tc, tcl in ticks if tv is not None), key=lambda x: x[0]):
        x = _w(100 * v / scale)
        gap = round(x - pos, 1)
        if gap >= 0.5:
            cells.append(f'<td width="{gap}%" style="height:7px; font-size:0; line-height:0;">&nbsp;</td>')
        cells.append(f'<td width="3" bgcolor="{c}" style="background-color:{c}; width:3px; height:7px; font-size:0; '
                     f'line-height:0;" class="{cls}">&nbsp;</td>')
        pos = x + 0.6
    cells.append('<td style="height:7px; font-size:0; line-height:0;">&nbsp;</td>')
    return bar + f'{TABLE} class="tfix"><tr>{"".join(cells)}</tr></table>'


def _scale_for(vals):
    m = max((v for v in vals if v is not None), default=0)
    for s in (0.01, 0.02, 0.03, 0.05, 0.08, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0):
        if m <= s * 0.92:
            return s
    return 1.0


def delta(t, model, bk, k):
    """-> (html, text) for one benchmark delta; coloured only when better or worse."""
    lab = "Mar" if bk == "mar26" else "Aug"
    if bench_for(t, model, bk, k) is None:
        return f'<span class="hl-ctx" style="color:{CTX};">{lab} —†</span>', f"{lab} —†"
    c = t["cmp"][bk][k]
    if not c:
        return f'<span class="hl-ctx" style="color:{CTX};">{lab} —</span>', f"{lab} —"
    s, v = pts(c["diff"]), c.get("verdict")
    if v in ("better", "worse"):
        cls, col = ROLE[v]
        return (f'<span class="{cls}" style="color:{col}; font-weight:600;">{lab} {esc(s.replace(" pts", ""))}</span>',
                f"{lab} {s} ({v})")
    return f'<span class="hl-ctx" style="color:{CTX};">{lab} {esc(s.replace(" pts", ""))}</span>', f"{lab} {s}"


def _funnel_a(t, model):
    rows = []
    for step in steps_for(t):
        now = t["rates"][step][0]
        aug, mar = bench_for(t, model, "aug25", step), bench_for(t, model, "mar26", step)
        scale = 1.0 if step not in ("paid", "pc") else _scale_for([now, aug, mar])
        color = verdict_color(t["cmp"]["mar26"][step])
        label = STEP_NAMES[step] + (f'<br/><span style="font-size:10px; color:{CTX}; font-weight:400;" class="hl-ctx">'
                                    f'scale 0–{int(scale * 100)}%</span>' if step in ("paid", "pc") else "")
        rows.append(
            f'<tr><td class="bar-label-cell" width="128" valign="top" style="width:128px; padding:4px 10px 10px 0; '
            f'font-family:{FONT_BODY}; font-size:12px; font-weight:600; color:{INK}; line-height:1.3;">'
            f'<span class="text-main">{label}</span></td>'
            f'<td valign="top" style="padding:5px 0 10px 0;">'
            f'{_bar(now, scale, color, [(aug, C_AUG, "tick-aug-dm"), (mar, C_MAR, "tick-mar-dm")])}</td>'
            f'<td class="bar-val-cell" width="84" align="right" valign="top" style="width:84px; padding:2px 0 10px 8px; '
            f'font-family:{FONT_BODY}; font-size:11px; line-height:1.35; font-variant-numeric:tabular-nums;">'
            f'<span class="hl-num" style="font-family:{FONT_HEADING}; font-size:14px; font-weight:700; color:{INK};">'
            f'{pct(now)}</span><br/>{delta(t, model, "mar26", step)[0]}<br/>{delta(t, model, "aug25", step)[0]}</td></tr>')
    return f'{TABLE} class="tfix">{"".join(rows)}</table>' + _client_note(t)


def _client_note(t):
    """Why Android has a separate in-app event row carrying the Aug 2025 comparison."""
    if not t["cmp"]["aug25"]["client_basis"]:
        return ""
    return (f'<p style="margin:2px 0 0 0; font-family:{FONT_BODY}; font-size:11px; color:{CTX};" class="hl-ctx">'
            f'† Aug 2025 is compared on the in-app purchase event row, like for like; directional only.</p>')


def trend_note(t, model):
    """Why there is no by-build chart: zero or one build with 30+ installs in the week."""
    if not t["trend"]:
        return f"No build had 30 or more installs on {week_of(model)}, so there is nothing to compare by build."
    return (f"Only one build ({t['trend'][0]['v']}) had 30 or more installs on {week_of(model)}, "
            "so there is nothing to compare by build.")


def _trend_a(t, model):
    """Paid within a day, per build over the same 7 days, newest first: one horizontal bar
    per row (same grammar as the funnel, so it holds at phone width), Mar 2026 as a tick."""
    pts_ = t["trend"]
    head = f'<p style="margin:22px 0 10px 0;">{_caps("Paid within a day, by build")}</p>'
    if len(pts_) < 2:
        return head + _muted(esc(trend_note(t, model)), 12)
    mar = model["bench"]["mar26"][M.plat_of(t["key"])]["rates"]["paid"][0]
    scale = _scale_for([p["rate"][0] for p in pts_] + [mar])
    rows = []
    for p in reversed(pts_):
        v = p["rate"][0]
        rows.append(
            f'<tr><td class="bar-label-cell" width="128" valign="top" style="width:128px; padding:2px 10px 8px 0; '
            f'font-family:{FONT_BODY}; font-size:12px; font-weight:600; color:{INK}; line-height:1.3; white-space:nowrap;">'
            f'<span class="text-main">{esc(p["v"])}</span> '
            f'<span style="font-size:10px; font-weight:400; color:{CTX};" class="text-muted">n={num(p["n"])}</span></td>'
            f'<td valign="top" style="padding:4px 0 8px 0;">{_bar(v, scale, ACCENT, [(mar, C_MAR, "tick-mar-dm")])}</td>'
            f'<td class="bar-val-cell heading-dm" width="84" align="right" valign="top" style="width:84px; padding:2px 0 8px 8px; '
            f'font-family:{FONT_HEADING}; font-size:13px; font-weight:700; color:{INK}; '
            f'font-variant-numeric:tabular-nums; white-space:nowrap;">{pct(v)}</td></tr>')
    return (head + f'{TABLE} class="tfix">{"".join(rows)}</table>'
            + _muted(f"Scale 0–{int(scale * 100)}%. Dark tick = Mar 2026 ({pct(mar)}). Each build's own installs, "
                     f"{week_of(model)}. Build bars carry no verdict colour. Single builds are small: read the "
                     "section, not one bar.", 11, 2))


# ------------------------------------------------------ variant B charts ----

def _img(src, alt, w=520):
    return (f'<img src="{esc(src)}" width="{w}" alt="{esc(alt)}" style="display:block; width:100%; max-width:{w}px; '
            f'height:auto; border:0;" />')


def _alt_funnel(t):
    r = t["rates"]
    return "; ".join(f"{STEP_NAMES[s]} {pct(r[s][0])}" for s in steps_for(t))


def _values_table(t, model):
    """Variant B: the chart's numbers as text, so blocked images lose nothing."""
    th = (f'font-family:{FONT_HEADING}; font-size:10px; font-weight:600; text-transform:uppercase; '
          f'letter-spacing:0.04em; color:{CTX}; padding:6px 0 4px 0; text-align:right;')
    td = (f'font-family:{FONT_BODY}; font-size:12px; color:{INK}; padding:3px 0; text-align:right; '
          f'font-variant-numeric:tabular-nums; border-top:1px solid #e4e1dd;')
    rows = [f'<tr><td style="{th} text-align:left;" class="text-muted">Step</td>'
            f'<td style="{th}" class="text-muted">7 days</td><td style="{th}" class="text-muted">vs Mar</td>'
            f'<td style="{th}" class="text-muted">Mar 2026</td><td style="{th}" class="text-muted">Aug 2025</td></tr>']
    for s in steps_for(t):
        cmp = t["cmp"]["mar26"][s]
        aug, mar = bench_for(t, model, "aug25", s), bench_for(t, model, "mar26", s)
        rows.append(
            f'<tr><td style="{td} text-align:left;" class="text-main divider-line">{STEP_NAMES[s]}</td>'
            f'<td style="{td} font-weight:700;" class="text-main divider-line">{pct(t["rates"][s][0])}</td>'
            f'<td style="{td}" class="text-main divider-line">{pts(cmp["diff"]) if cmp else "—"}</td>'
            f'<td style="{td}" class="text-main divider-line">{pct(mar)}</td>'
            f'<td style="{td}" class="text-main divider-line">{pct(aug)}</td></tr>')
    return f'<div style="height:8px; line-height:8px; font-size:0;">&nbsp;</div>{TABLE}>{"".join(rows)}</table>'


def _build_table(t):
    """Variant B: the by-build chart as text."""
    th = (f'font-family:{FONT_HEADING}; font-size:10px; font-weight:600; text-transform:uppercase; '
          f'letter-spacing:0.04em; color:{CTX}; padding:6px 0 4px 0; text-align:right;')
    td = (f'font-family:{FONT_BODY}; font-size:12px; color:{INK}; padding:3px 0; text-align:right; '
          f'font-variant-numeric:tabular-nums; border-top:1px solid #e4e1dd;')
    rows = [f'<tr><td style="{th} text-align:left;" class="text-muted">Build</td>'
            f'<td style="{th}" class="text-muted">Installs</td>'
            f'<td style="{th}" class="text-muted">Payers</td><td style="{th}" class="text-muted">Paid</td></tr>']
    for p in reversed(t["trend"]):
        rows.append(
            f'<tr><td style="{td} text-align:left;" class="text-main divider-line">{esc(p["v"])}</td>'
            f'<td style="{td}" class="text-main divider-line">{num(p["n"])}</td>'
            f'<td style="{td}" class="text-main divider-line">{num(p["paid"])}</td>'
            f'<td style="{td}" class="text-main divider-line">{pct(p["rate"][0])}</td></tr>')
    return f'<div style="height:8px; line-height:8px; font-size:0;">&nbsp;</div>{TABLE}>{"".join(rows)}</table>'


# ---------------------------------------------------------------- tracks ----

def _track(t, model, variant, out_dir, images):
    head = (f'{TABLE} class="tfix"><tr><td style="font-family:{FONT_HEADING}; font-size:18px; font-weight:700; '
            f'color:{INK};" class="heading-dm">{esc(t["store"])} · {esc(t["label"])}</td></tr></table>')
    if t["empty"]:
        return _row(head + _muted(esc(f"No installs on {week_of(model)}."), 13, 6), 34)
    c = t["counts"]
    sub = (f'{_c("Builds")} {builds_txt(t)[0]} {_c("·")} {_n(num(c["n"]))} {_c("installs ·")} '
           f'{_n(num(c["paid"]))} {_c("payers")}')
    parts = [head, f'<p style="margin:4px 0 0 0; font-family:{FONT_BODY}; font-size:13px; line-height:1.5;">{sub}</p>']
    for lab, now, base in checks(t, model):
        parts.append(f'<p style="margin:8px 0 0 0; font-family:{FONT_BODY}; font-size:13px;">{_v("worse", "Check:")} '
                     f'{_c(lab)} {_n(pct(now))} {_c("vs Mar")} {_c(pct(base))}{_c(". Investigate.")}</p>')
    parts.append('<div style="height:14px; line-height:14px; font-size:0;">&nbsp;</div>')
    if variant == "A":
        parts += [_funnel_a(t, model), _trend_a(t, model)]
    else:
        import ip_charts
        f = ip_charts.funnel_png(t, model, out_dir)
        images.append(f)
        parts += [_img(f["name"], f"{t['label']} funnel: {_alt_funnel(t)}"), _values_table(t, model) + _client_note(t)]
        if len(t["trend"]) >= 2:
            g = ip_charts.trend_png(t, model, out_dir)
            images.append(g)
            parts.append(f'<div style="height:12px; line-height:12px; font-size:0;">&nbsp;</div>' +
                         _img(g["name"], f"{t['label']} paid within a day by build: " +
                              ", ".join(f"{p['v']} {pct(p['rate'][0])}" for p in t["trend"])) +
                         _build_table(t) +
                         _muted(f"Each build's own installs, {week_of(model)}. Build bars carry no verdict colour.", 11, 2))
        else:
            parts.append(_muted(esc(trend_note(t, model)), 12, 12))
    mh, _ = mix_txt(t)
    if mh:
        parts.append(f'<p style="margin:14px 0 0 0; font-family:{FONT_BODY}; font-size:13px; line-height:1.5;">{mh}</p>')
    return _row("".join(parts), 34)


def footnote_lines(model):
    edition = get_edition(model.get("edition", "pt"))
    return [
        "Rates are what people did within 24 hours of first install, as a share of installs; export = a video export "
        "after the first preview; paid = a subscription with a price above zero. Pacific install days, data to "
        f"05:00 {edition.label}.",
        "Better or worse when the 95% interval of the difference excludes zero; otherwise same when the whole interval "
        "lies within ±10% of the benchmark rate; otherwise can't tell yet. " + (
            "Verdicts against Mar 2026 / Aug 2025 are judged on the 7 full days; the early read is a separate check of "
            "the newest day against the 7 days before it, measured at the same point."
            if early_of(model) else "Judged on the 7 days, never the single day."),
        "† Android Aug 2025 has no linked server-side subscription data, so that comparison uses the in-app purchase "
        "event on both sides; the event changed in Aug 2026, so read it as directional.",
        "Benchmarks are whole months, same query and filters. Suspected test installs removed. Install mix differs from "
        "the benchmarks, so differences don't show a build caused them.",
    ]


def _footnotes(model):
    lines = footnote_lines(model)
    body = "".join(_muted(esc(x), 11, 0 if i == 0 else 6) for i, x in enumerate(lines))
    link = (f'<p style="margin:12px 0 0 0; font-family:{FONT_BODY}; font-size:11px; text-align:center;" class="text-muted">'
            f'<a href="{esc(WORKFLOW_URL)}" style="color:{CTX}; text-decoration:underline;" class="hl-ctx">Workflow &#8599;</a></p>')
    return _section_open() + f'<tr><td style="padding:34px 40px 40px 40px;" class="body-cell">{body}{link}</td></tr></table>'


def subject_of(model):
    """Dated by the newest day shown (the early day when present), so Gmail never threads
    two days together."""
    e = early_of(model)
    if e:
        bits = [f"{t['label']} {early_rate(early_track(model, t))}" for t in model["tracks"]]
        return f"TA install→paid · {day_label(e['day'])} (early): " + " · ".join(bits)
    bits = [f"{t['label']} {'—' if t['empty'] else day_rate(t)}" for t in model["tracks"]]
    return f"TA install→paid · {day_label(model['last_day'])}: " + " · ".join(bits)


def _text(model, subject):
    lab = day_label(model["last_day"])
    out = [subject, "", "LASCADE · TRAVELANIMATOR", "Install → paid", dateline(model), ""]
    for t in model["tracks"]:
        if t["empty"]:
            out.append(f"{t['label']}: no installs on {week_of(model)}")
            continue
        d = t["day"]
        out.append(f"{t['label']}: {day_rate(t)} on {lab} ({d.get('paid', 0)} of {num(d.get('n', 0))}) · "
                   f"7 days {pct(t['rates']['paid'][0])}")
    if early_of(model):
        out += ["", early_caption(model)]
        for t in model["tracks"]:
            _, l1, _, l2 = early_lines(t, model)
            out += [t["label"], "  " + l1] + (["  " + l2] if l2 else [])
    out += ["", "7-day verdicts vs Mar 2026 / Aug 2025"]
    for t in model["tracks"]:
        _, l1, _, l2 = read_lines(t, model)
        out += [t["label"], "  " + l1] + (["  " + l2] if l2 else [])
    for t in model["tracks"]:
        out += ["", f"{t['store']} · {t['label']}"]
        if t["empty"]:
            out.append(f"  No installs on {week_of(model)}.")
            continue
        c = t["counts"]
        out.append(f"  Builds {builds_txt(t)[1]} · {num(c['n'])} installs · {num(c['paid'])} payers")
        for k in steps_for(t):
            out.append(f"  {STEP_NAMES[k]}: {pct(t['rates'][k][0])} · {delta(t, model, 'mar26', k)[1]} · "
                       f"{delta(t, model, 'aug25', k)[1]}")
        if t["cmp"]["aug25"]["client_basis"]:
            out.append("  † Aug 2025 is compared on the in-app purchase event row, like for like; directional only.")
        if len(t["trend"]) >= 2:
            out.append("  Paid within a day, by build: " +
                       " · ".join(f"{p['v']} {pct(p['rate'][0])} (n={num(p['n'])})" for p in reversed(t["trend"])))
        else:
            out.append("  By build: " + trend_note(t, model))
        out += [f"  Check: {lab_} {pct(now)} vs Mar {pct(base)}. Investigate." for lab_, now, base in checks(t, model)]
        mx = mix_txt(t)[1]
        if mx:
            out.append("  " + mx)
    out += ["", "Notes:"] + [f"- {x}" for x in footnote_lines(model)] + ["", "Workflow: " + WORKFLOW_URL]
    return "\n".join(out)


def render(model, variant, out_dir):
    """-> (subject, html, text, images). The early read is optional: if rendering it fails,
    the email is rendered again without it."""
    try:
        return _render(model, variant, out_dir)
    except Exception:
        if not early_of(model):
            raise
        print("early read could not be rendered; sending without it")
        return _render(dict(model, early=None), variant, out_dir)


def _render(model, variant, out_dir):
    """-> (subject, html, text, images). images: [{"name", "path"}] for variant B; the html
    references images by bare file name (the sender swaps in cid: URLs)."""
    subject = subject_of(model)
    images = []
    first = next((t for t in model["tracks"] if not t["empty"]), None)
    if early_of(model) and model["tracks"]:
        _, l1, _, l2 = early_lines(model["tracks"][0], model)
        preheader = f"{model['tracks'][0]['label']}: {l1}. {l2}." if l2 else f"{model['tracks'][0]['label']}: {l1}"
    elif first:
        _, l1, _, l2 = read_lines(first, model)
        preheader = f"{first['label']}: {l1}. {l2}."
    else:
        preheader = subject
    parts = [
        _masthead("Lascade · TravelAnimator", "Install → paid", dateline(model)),
        _stat_grid(model), _early(model), _lead(model), _row(_legend(), 14),
        "".join(_track(t, model, variant, out_dir, images) for t in model["tracks"]),
        _footnotes(model),
    ]
    html = (HEAD.replace("__TITLE__", esc(subject)) +
            '<body style="margin:0; padding:0; background-color:#f5f3f1; width:100%; -webkit-text-size-adjust:100%;" class="body-bg">'
            f'<div style="display:none; font-size:1px; color:#f5f3f1; line-height:1px; max-height:0px; max-width:0px; '
            f'opacity:0; overflow:hidden; mso-hide:all;">{esc(preheader)}&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;</div>'
            f'{TABLE} style="background-color:#f5f3f1;" class="body-bg"><tr><td align="center" valign="top" style="padding:20px 10px;">'
            '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="600" align="center" '
            'style="width:100%; max-width:600px;" class="email-container"><tr><td>' + "".join(parts) +
            '</td></tr></table></td></tr></table></body></html>')
    return subject, html, _text(model, subject), images
