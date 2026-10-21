"""Render the install->paid model to an email: (subject, html, text, images).

Chassis (`HEAD` and the row helpers) follows scripts/adspend/adspend_render_email.py, which
was mined from `Style Template/styleguide-email.html` and proven in Gmail web/iOS, light
and dark. Design: typography does the work, prose stays #222. Colour appears only in the
charts, and it always means something: green = better than Mar 2026, red = worse,
blue = level or too close to call, grey = too few payers to judge. Benchmark ticks are
amber (Aug 2025) and charcoal (Mar 2026).

Variant A draws every chart from table cells (works with images off and in dark mode).
Variant B draws them as PNGs (ip_charts.py), inline by CID when sent.
"""
from __future__ import annotations

import datetime as dt
from html import escape as esc

import ip_model as M
from ip_editions import get_edition

FONT_HEADING = "'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif"
FONT_BODY = "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"
ACCENT = "#2980b9"
C_BETTER, C_WORSE, C_LEVEL, C_THIN = "#2a8c4a", "#c0392b", "#2980b9", "#a8a49e"
C_AUG, C_MAR = "#c9922a", "#444444"
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
        .text-muted { color: #a8a49e !important; }
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
    [data-ogsc] .text-muted { color: #a8a49e !important; }
    [data-ogsc] .divider-line { border-color: #3a3835 !important; }
    [data-ogsc] .bar-track-dm { background-color: #33312e !important; }
    [data-ogsc] .accent-dm { color: #5b9bd5 !important; }
    [data-ogsc] .accent-border-dm { border-top-color: #5b9bd5 !important; }
    [data-ogsc] .tick-mar-dm { background-color: #d8d4ce !important; }
</style>
</head>
"""

TABLE = '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"'


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


def verdict_color(c, thin):
    if thin or not c or c.get("verdict") is None:
        return C_THIN if thin else C_LEVEL
    return {"better": C_BETTER, "worse": C_WORSE}.get(c["verdict"], C_LEVEL)


PHRASE = {"better": "above", "worse": "below", "same": "level with", "unclear": "too close to call against"}


def vs_phrase(c, bench_label, base_txt):
    if not c:
        return f"no {bench_label} figure"
    if c["verdict"] is None:
        return f"{pts(c['diff'])} against {bench_label} ({base_txt}), directional only"
    if c["verdict"] in ("better", "worse"):
        return f"{PHRASE[c['verdict']]} {bench_label} ({base_txt}) by {pts(abs(c['diff']))[1:]}"
    return f"{PHRASE[c['verdict']]} {bench_label} ({base_txt})"


def paid_key(t, bk):
    """Which paid figure is compared against benchmark bk: client event when the
    benchmark has no server figure (Android Aug 2025)."""
    return "pc" if t["cmp"][bk]["client_basis"] else "paid"


def steps_for(t):
    """Chart rows. Where a benchmark has no server figure (Android Aug 2025) the email
    adds a client-event row, so every mark sits next to a like-for-like figure."""
    return ["pw", "pv", "ex", "paid"] + (["pc"] if t["cmp"]["aug25"]["client_basis"] else [])


def bench_for(t, model, bk, step):
    """Benchmark value for a chart row, or None when there is no like-for-like figure."""
    if step == "paid" and t["cmp"][bk]["client_basis"]:
        return None
    return model["bench"][bk][M.plat_of(t["key"])]["rates"][step][0]


def builds_txt(t, limit=4):
    """Pool membership, newest first. Soft launches (never 10 real installs in a day) are
    labelled; builds with only a handful of installs are summarised, not hidden."""
    big = [b for b in t["builds"] if b["n"] >= 5]
    small = [b for b in t["builds"] if b["n"] < 5]
    names = [b["v"] + (" (soft launch)" if b["soft"] else "") for b in big[:limit]]
    more = len(big) - len(names)
    out = ", ".join(names) + (f" +{more} older" if more > 0 else "")
    if small:
        out += (f"{' + ' if out else ''}{len(small)} build{'s' if len(small) > 1 else ''} with under 5 installs "
                f"({num(sum(b['n'] for b in small))} in all)")
    return out


def verdict_line(t, model):
    """Deterministic one-sentence verdict for a track."""
    name = f"{t['store']} · {t['label']}"
    if t["empty"]:
        return name, "No matured installs in the last 60 days."
    c, r = t["counts"], t["rates"]
    head = f"{pct(r['paid'][0])} paid within a day ({num(c['paid'])} of {num(c['n'])} installs; {builds_txt(t)})."
    if t["thin"]:
        if t.get("no_target"):
            why = "no March 2026 rate to size the pool"
        elif t["capped"]:
            why = "the pool ran out of builds"
        else:
            why = "fewer than 20 payers"
        return name, head + f" Too few to call: {why}. Shown for reference only."
    bits, verdicts = [], []
    for bk, lab in reversed(M.BENCH):
        pk = paid_key(t, bk)
        base = pct(model["bench"][bk][M.plat_of(t["key"])]["rates"][pk][0]) + ("†" if pk == "pc" else "")
        c = t["cmp"][bk]["paid"]
        verdicts.append(c and c["verdict"])
        phrase = vs_phrase(c, lab, base)
        if pk == "pc" and c:
            # Say which figure the difference is between: not the server rate quoted above.
            phrase = (f"{pts(c['diff'])} against {lab} on the in-app purchase event "
                      f"(today {pct(r['pc'][0])} vs {base}), directional only")
        bits.append((phrase, lab, base))
    if verdicts[0] == verdicts[1] and verdicts[0] in ("same", "unclear"):
        tail = f"{PHRASE[verdicts[0]]} {bits[0][1]} ({bits[0][2]}) and {bits[1][1]} ({bits[1][2]})"
    else:
        tail = f"{bits[0][0]}; {bits[1][0]}"
    return name, head + " " + tail[0].upper() + tail[1:] + "."


# ---------------------------------------------------------------- chrome ----

def _section_open():
    return (f'{TABLE} style="background-color:#f5f3f1;" bgcolor="#f5f3f1" class="body-bg email-body-bg">')


def _row(inner, top=24):
    return _section_open() + f'<tr><td style="padding:{top}px 40px 0 40px;" class="body-cell">{inner}</td></tr></table>'


def _p(text, size=15, weight=400, color="#222222", cls="text-main", mt=0, extra=""):
    return (f'<p style="margin:{mt}px 0 0 0; Margin:{mt}px 0 0 0; font-family:{FONT_BODY}; '
            f'font-size:{size}px; font-weight:{weight}; line-height:1.5; color:{color};{extra}" '
            f'class="{cls}">{text}</p>')


def _muted(text, size=12, mt=0, extra=""):
    return _p(text, size=size, color="#777777", cls="text-muted", mt=mt, extra=extra)


def _caps(text, color="#777777", cls="text-muted"):
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
        f'font-size:30px; font-weight:700; line-height:1.1; letter-spacing:-0.02em; color:#222222;">{esc(title)}</h1></td></tr>'
        f'<tr><td style="padding:6px 40px 0 40px;" class="body-cell">'
        f'<p style="margin:0; font-family:{FONT_HEADING}; font-size:12px; font-weight:600; '
        f'text-transform:uppercase; letter-spacing:0.04em; color:#777777;" class="text-muted">{esc(dateline)}</p></td></tr>'
        f'<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
        f'{TABLE} class="tfix"><tr><td bgcolor="{ACCENT}" style="border-top:3px solid {ACCENT}; font-size:1px; '
        f'line-height:1px; mso-line-height-rule:exactly;" class="accent-border-dm">&nbsp;</td></tr></table></td></tr></table>')


def _stat_grid(model):
    cells = []
    for t in model["tracks"]:
        fig = "—" if t["empty"] else pct(t["rates"]["paid"][0])
        col = C_THIN if t["empty"] or t["thin"] else verdict_color(t["cmp"]["mar26"]["paid"], False)
        if t["empty"]:
            gloss = "no installs"
        elif t["thin"]:
            gloss = f"{num(t['counts']['paid'])} payers · too few"
        else:
            gloss = f"{num(t['counts']['paid'])} of {num(t['counts']['n'])}"
        cells.append(
            f'<td valign="top" style="padding:0 8px 0 0;">{_caps(t["label"])}'
            f'<p style="margin:4px 0 0 0; font-family:{FONT_HEADING}; font-size:26px; font-weight:700; '
            f'line-height:1.1; letter-spacing:-0.01em; color:{col}; font-variant-numeric:tabular-nums;" '
            f'class="stat-fig">{esc(fig)}</p>{_muted(esc(gloss), 12, 4)}</td>')
    return _row(f'{TABLE} class="tfix"><tr>{"".join(cells)}</tr></table>', 28)


def _lead(model):
    out = []
    for i, t in enumerate(model["tracks"]):
        name, line = verdict_line(t, model)
        out.append(_p(f'<strong style="font-weight:600;">{esc(name)}.</strong> {esc(line)}', 16, mt=0 if i == 0 else 10))
    return _row("".join(out), 22)


def _legend():
    sw = lambda c, w, h: (f'<span style="display:inline-block; width:{w}px; height:{h}px; background-color:{c}; '
                          f'vertical-align:middle;"></span>')
    items = [(sw(C_BETTER, 10, 10), "better than Mar 2026"), (sw(C_WORSE, 10, 10), "worse"),
             (sw(C_LEVEL, 10, 10), "level / too close"), (sw(C_AUG, 2, 12), "Aug 2025"),
             (sw(C_MAR, 2, 12), "Mar 2026 · changes shown in points vs Mar 2026")]
    body = " &nbsp;&nbsp; ".join(f'{s}&nbsp;<span style="font-family:{FONT_BODY}; font-size:11px; color:#777777;" '
                                 f'class="text-muted">{esc(t)}</span>' for s, t in items)
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


def _funnel_a(t, model):
    plat = M.plat_of(t["key"])
    b = model["bench"]
    rows = []
    for step in steps_for(t):
        now = t["rates"][step][0]
        aug, mar = bench_for(t, model, "aug25", step), bench_for(t, model, "mar26", step)
        scale = 1.0 if step not in ("paid", "pc") else _scale_for([now, aug, mar])
        color = verdict_color(t["cmp"]["mar26"][step], t["thin"])
        cmp = t["cmp"]["mar26"][step]
        delta = pts(cmp["diff"]) if cmp else ""
        label = STEP_NAMES[step] + (f'<br/><span style="font-size:10px; color:#999999; font-weight:400;">'
                                    f'scale 0–{int(scale * 100)}%</span>' if step in ("paid", "pc") else "")
        rows.append(
            f'<tr><td class="bar-label-cell" width="128" valign="top" style="width:128px; padding:4px 10px 10px 0; '
            f'font-family:{FONT_BODY}; font-size:12px; font-weight:600; color:#444444; line-height:1.3;">'
            f'<span class="text-main">{label}</span></td>'
            f'<td valign="top" style="padding:5px 0 10px 0;">{_bar(now, scale, color, [(aug, C_AUG, ""), (mar, C_MAR, "tick-mar-dm")])}</td>'
            f'<td class="bar-val-cell" width="84" align="right" valign="top" style="width:84px; padding:2px 0 10px 8px;">'
            f'<p style="margin:0; font-family:{FONT_HEADING}; font-size:14px; font-weight:700; color:#222222; '
            f'font-variant-numeric:tabular-nums;" class="heading-dm">{pct(now)}</p>'
            f'<p style="margin:0; font-family:{FONT_BODY}; font-size:10px; color:#777777; '
            f'font-variant-numeric:tabular-nums;" class="text-muted">{esc(delta)}</p></td></tr>')
    return f'{TABLE} class="tfix">{"".join(rows)}</table>' + _client_note(t)


def _client_note(t):
    """Why Android has a separate in-app event row carrying the Aug 2025 mark."""
    if not t["cmp"]["aug25"]["client_basis"]:
        return ""
    return _muted("† Android had no linked server-side subscriptions in Aug 2025, so the Aug 2025 mark sits on "
                  "the in-app purchase event row, compared like for like. That event changed in Aug 2026: "
                  "read the row as directional.", 11, 2)


def _trend_a(t, model):
    """Paid within a day, per recent build, newest first: one horizontal bar per row
    (same grammar as the funnel, so it holds at phone width), Mar 2026 as a tick."""
    pts_ = t["trend"]
    if len(pts_) < 2:
        return ""
    mar = model["bench"]["mar26"][M.plat_of(t["key"])]["rates"]["paid"][0]
    scale = _scale_for([p["rate"][0] for p in pts_] + [mar])
    rows = []
    for p in reversed(pts_):
        v = p["rate"][0]
        pooled = any(b["v"] == p["v"] for b in t["builds"])
        col = ACCENT if pooled else "#b9c9d6"
        rows.append(
            f'<tr><td class="bar-label-cell" width="128" valign="top" style="width:128px; padding:2px 10px 8px 0; '
            f'font-family:{FONT_BODY}; font-size:12px; font-weight:600; color:#444444; line-height:1.3; white-space:nowrap;">'
            f'<span class="text-main">{esc(p["v"])}</span> '
            f'<span style="font-size:10px; font-weight:400; color:#999999;" class="text-muted">n={num(p["n"])}</span></td>'
            f'<td valign="top" style="padding:4px 0 8px 0;">{_bar(v, scale, col, [(mar, C_MAR, "tick-mar-dm")])}</td>'
            f'<td class="bar-val-cell" width="84" align="right" valign="top" style="width:84px; padding:2px 0 8px 8px; '
            f'font-family:{FONT_HEADING}; font-size:13px; font-weight:700; color:#222222; '
            f'font-variant-numeric:tabular-nums; white-space:nowrap;" class="heading-dm">{pct(v)}</td></tr>')
    return (f'<p style="margin:22px 0 10px 0;">{_caps("Paid within a day, by build")}</p>'
            f'{TABLE} class="tfix">{"".join(rows)}</table>'
            + _muted(f"Scale 0–{int(scale * 100)}%. Dark tick = Mar 2026 ({pct(mar)}). Blue bars are in today's pool. "
                     "Single builds are small: read the pool, not one bar.", 11, 2))


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
          f'letter-spacing:0.04em; color:#777777; padding:6px 0 4px 0; text-align:right;')
    td = (f'font-family:{FONT_BODY}; font-size:12px; color:#222222; padding:3px 0; text-align:right; '
          f'font-variant-numeric:tabular-nums; border-top:1px solid #e4e1dd;')
    rows = [f'<tr><td style="{th} text-align:left;" class="text-muted">Step</td>'
            f'<td style="{th}" class="text-muted">Today</td><td style="{th}" class="text-muted">vs Mar</td>'
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
          f'letter-spacing:0.04em; color:#777777; padding:6px 0 4px 0; text-align:right;')
    td = (f'font-family:{FONT_BODY}; font-size:12px; color:#222222; padding:3px 0; text-align:right; '
          f'font-variant-numeric:tabular-nums; border-top:1px solid #e4e1dd;')
    pooled = {b["v"] for b in t["builds"]}
    rows = [f'<tr><td style="{th} text-align:left;" class="text-muted">Build</td>'
            f'<td style="{th}" class="text-muted">Released</td><td style="{th}" class="text-muted">Installs</td>'
            f'<td style="{th}" class="text-muted">Payers</td><td style="{th}" class="text-muted">Paid</td></tr>']
    for p in reversed(t["trend"]):
        rows.append(
            f'<tr><td style="{td} text-align:left;" class="text-main divider-line">{esc(p["v"])}'
            f'{" · in pool" if p["v"] in pooled else ""}</td>'
            f'<td style="{td}" class="text-main divider-line">{day(p["released"])}</td>'
            f'<td style="{td}" class="text-main divider-line">{num(p["n"])}</td>'
            f'<td style="{td}" class="text-main divider-line">{num(p["paid"])}</td>'
            f'<td style="{td}" class="text-main divider-line">{pct(p["rate"][0])}</td></tr>')
    return f'<div style="height:8px; line-height:8px; font-size:0;">&nbsp;</div>{TABLE}>{"".join(rows)}</table>'


# ---------------------------------------------------------------- tracks ----

def _detail_lines(t, model):
    plat = M.plat_of(t["key"])
    r, mar, aug = t["rates"], model["bench"]["mar26"][plat]["rates"], model["bench"]["aug25"][plat]["rates"]
    lines = [
        f"Previewers who then exported: {pct(r['pv_ex'][0])} (Mar 2026 {pct(mar['pv_ex'][0])}, Aug 2025 {pct(aug['pv_ex'][0])}).",
        f"Paid within a day: {pct(r['paid_if_pv'][0])} of previewers vs {pct(r['paid_if_nopv'][0])} of people who never "
        f"previewed (Mar 2026: {pct(mar['paid_if_pv'][0])} vs {pct(mar['paid_if_nopv'][0])}). An association, not a cause: "
        f"the paywall comes before preview.",
    ]
    if t["mix"]:
        lines.append("Top countries: " + ", ".join(
            f"{m['cc']} {pct(m['now'])}" + (f" (Mar {pct(m['base'])})" if m["base"] is not None else "")
            for m in t["mix"]) + ".")
    return lines


def _track(t, model, variant, out_dir, images):
    head = (f'{TABLE} class="tfix"><tr><td style="font-family:{FONT_HEADING}; font-size:18px; font-weight:700; '
            f'color:#222222;" class="heading-dm">{esc(t["store"])} · {esc(t["label"])}</td></tr></table>')
    if t["empty"]:
        return _row(head + _muted("No matured installs in the last 60 days.", 13, 6), 34)
    c = t["counts"]
    sub = (f"Builds {builds_txt(t, 6)} · installs {day(t['first_day'])}–{day(t['last_day'])} · "
           f"{num(c['n'])} installs · {num(c['paid'])} payers")
    parts = [head, _muted(esc(sub), 12, 4)]
    if t["thin"]:
        parts.append(_p("Too few payers to judge. Funnel marks are grey: read them as reference, not a verdict. Build-chart shading only shows pool membership.", 13, 600, mt=8))
    if variant == "A":
        parts += [f'<div style="height:14px; line-height:14px; font-size:0;">&nbsp;</div>', _funnel_a(t, model),
                  _trend_a(t, model)]
    else:
        import ip_charts
        f = ip_charts.funnel_png(t, model, out_dir)
        images.append(f)
        parts += [f'<div style="height:14px; line-height:14px; font-size:0;">&nbsp;</div>',
                  _img(f["name"], f"{t['label']} funnel: {_alt_funnel(t)}")]
        parts.append(_values_table(t, model) + _client_note(t))
        if len(t["trend"]) >= 2:
            g = ip_charts.trend_png(t, model, out_dir)
            images.append(g)
            parts.append(f'<div style="height:12px; line-height:12px; font-size:0;">&nbsp;</div>' +
                         _img(g["name"], f"{t['label']} paid within a day by build: " +
                              ", ".join(f"{p['v']} {pct(p['rate'][0])}" for p in t["trend"])) +
                         _build_table(t))
    parts += [_p(esc(x), 14, mt=12 if i == 0 else 6) for i, x in enumerate(_detail_lines(t, model))]
    return _row("".join(parts), 34)


def _audit_line(model):
    bits = []
    for t in model["tracks"]:
        a = t["audit"]
        allp = a.get("all", 0)
        if not allp:
            continue
        bits.append(f"{t['label']} {num(a.get('test', 0))} of {num(allp)} ({pct(a.get('test', 0) / allp)}; "
                    f"{pct(a.get('missing', 0) / allp)} had no IP or device model to check)")
    return ("Excluded as suspected test installs (Google IP ranges, emulators), out of all installs in the "
            "60-day window: " + "; ".join(bits) + ".")


def footnote_lines(model):
    edition = get_edition(model.get("edition", "pt"))
    return [
        "Every rate is reach within 24 hours of a person's first install (a reinstall within 180 days does "
        f"not restart the clock). Installs from the last 60 days that are at least a day old; data to 05:00 {edition.label}.",
        "Builds marked soft launch never reached 10 real installs in a day; old binaries outside Android 4.0.x and "
        "5.x are left out. Each track pools its newest builds until it has enough installs to expect 40 payers at its March 2026 rate, "
        "so the pool never depends on today's results. Under 20 actual payers, or out of builds: no verdict.",
        "Better or worse only when the 95% interval of the difference excludes zero; level when it sits within ±10% of "
        "the benchmark; otherwise too close to call. Bar colour compares with March 2026.",
        "Export counts an export after the person's first preview. Paid = Production subscription_started with a "
        "price above zero (server side).",
        "† Android August 2025 has no linked server-side subscriptions, so that comparison uses the in-app purchase "
        "event on both sides and is directional only (the event also changed in August 2026).",
        _audit_line(model),
        "All countries are included. Country mix differs from the benchmark months, so differences are observed, "
        "not caused by the build.",
        f"Benchmarks: whole months, same query and filters (definition {model['bench_def']}, IP list {model['cidr_date'][:10]}).",
    ]


def _footnotes(model):
    lines = footnote_lines(model)
    body = "".join(_muted(esc(x), 11, 0 if i == 0 else 6) for i, x in enumerate(lines))
    link = (f'<p style="margin:12px 0 0 0; font-family:{FONT_BODY}; font-size:11px; text-align:center;" class="text-muted">'
            f'<a href="{esc(WORKFLOW_URL)}" style="color:#888888; text-decoration:underline;">Workflow &#8599;</a></p>')
    return _section_open() + f'<tr><td style="padding:34px 40px 40px 40px;" class="body-cell">{body}{link}</td></tr></table>'


def subject_of(model):
    bits = []
    for t in model["tracks"]:
        if t["empty"]:
            continue
        bits.append(f"{t['label']} {pct(t['rates']['paid'][0])}" + (" (thin)" if t["thin"] else ""))
    rd = model["report_date"]   # dated, like the ad-spend email, so Gmail never threads two days together
    return f"TA install→paid · {rd:%a} {day(rd)} — " + " · ".join(bits)


def maturity_label(model):
    cutoff = model["cutoff_utc"]
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=dt.timezone.utc)
    endpoint = (cutoff.astimezone(dt.timezone.utc) - dt.timedelta(hours=24)).astimezone(
        get_edition(model.get("edition", "pt")).zone)
    return f"{day(endpoint)}, {endpoint:%H:%M %Z}"


def render(model, variant, out_dir):
    """-> (subject, html, text, images). images: [{"name", "path"}] for variant B; the html
    references images by bare file name (the sender swaps in cid: URLs)."""
    rd = model["report_date"]
    subject = subject_of(model)
    images = []
    preheader = verdict_line(model["tracks"][0], model)[1] if model["tracks"] else subject
    parts = [
        _masthead("Lascade · TravelAnimator", "Install → paid, day one",
                  f"{rd:%a %-d %b} · installs to {maturity_label(model)} · App Store and Play Store"),
        _stat_grid(model), _lead(model), _row(_legend(), 14),
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
    text = ["LASCADE · TRAVELANIMATOR", "Install → paid, day one", f"{rd:%A %-d %B}",
            f"Installs to {maturity_label(model)}", ""]
    for t in model["tracks"]:
        name, line = verdict_line(t, model)
        text += [f"{name}: {line}"]
        if not t["empty"]:
            text.append(f"  Pool: {builds_txt(t, 6)}; installs {day(t['first_day'])}–{day(t['last_day'])}")
            for s in steps_for(t):
                text.append(f"  {STEP_NAMES[s]}: {pct(t['rates'][s][0])} (Mar 2026 "
                            f"{pct(bench_for(t, model, 'mar26', s))}, Aug 2025 {pct(bench_for(t, model, 'aug25', s))})")
            text += [f"  {x}" for x in _detail_lines(t, model)]
        text.append("")
    text += ["Notes:"] + [f"- {x}" for x in footnote_lines(model)] + ["", "Workflow: " + WORKFLOW_URL]
    return subject, html, "\n".join(text), images
