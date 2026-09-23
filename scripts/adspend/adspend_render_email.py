"""Render the ad-spend model to an email: (subject, preheader, html, text).

Chassis (`HEAD`) is copied verbatim from Clear's sharing-overall/render_email.py, which
was mined from `Style Template/styleguide-email.html` and proven in Gmail web/iOS,
light and dark. Design: typography does the work; no red/green verdict colour, body
prose is #222, accent blue marks structure only.
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from html import escape as esc

from pnl_money import format_usd
from adspend_model import DEADBAND_PCT, DEADBAND_USD, UNASSIGNED, round_pct, subject_of

FONT_HEADING = "'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif"
FONT_BODY = "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"
ACCENT = "#2980b9"
WORKFLOW_URL = "https://github.com/Lascade-Co/actions/actions/workflows/daily-ad-spend.yml"

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
    h1, h2, h3, h4, h5, h6 { font-family: 'Segoe UI', Arial, sans-serif !important; }
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
    .ExternalClass { width: 100%; }
    .ExternalClass, .ExternalClass p, .ExternalClass span, .ExternalClass font, .ExternalClass td, .ExternalClass div { line-height: 100%; }
    a[x-apple-data-detectors] {
        color: inherit !important; text-decoration: none !important; font-size: inherit !important;
        font-family: inherit !important; font-weight: inherit !important; line-height: inherit !important;
    }
    /* Proven via the Gmail-iOS width probe (T17-T20): a plain width=100%
       attribute or inline/class width does not reliably resolve on Gmail's
       iOS app for a "fill the remaining row" cell, regardless of channel —
       but table-layout:fixed does, both for a fixed-sides/flexible-middle
       row and for N equal auto columns. This is now the one mechanism used
       for every such cell in this document (see _divider, _masthead,
       _stat_hero, _ledger_row, _bar_chart, _daily_chart). */
    .tfix { width: 100% !important; table-layout: fixed !important; }
    @media only screen and (max-width: 480px) {
        .email-container { width: 100% !important; max-width: 100% !important; }
        .email-container td.body-cell { padding-left: 20px !important; padding-right: 20px !important; }
        h1.email-h1 { font-size: 26px !important; }
        .bar-label-cell { width: 100px !important; font-size: 11px !important; }
    }
    @media (prefers-color-scheme: dark) {
        .body-bg { background-color: #1a1a18 !important; }
        .email-body-bg { background-color: #222220 !important; }
        .card-bg { background-color: #2a2927 !important; }
        .text-main { color: #e0ddd8 !important; }
        .heading-dm { color: #ece9e4 !important; }
        .text-muted { color: #a8a49e !important; }
        .divider-line { border-color: #3a3835 !important; }
        .bar-track-dm { background-color: #1a1a18 !important; }
        .accent-dm { color: #5b9bd5 !important; }
        .accent-border-dm { border-top-color: #5b9bd5 !important; }
        .bar-fill-accent-dm { background-color: #5b9bd5 !important; }
    }
    [data-ogsc] .body-bg { background-color: #1a1a18 !important; }
    [data-ogsc] .email-body-bg { background-color: #222220 !important; }
    [data-ogsc] .card-bg { background-color: #2a2927 !important; }
    [data-ogsc] .text-main { color: #e0ddd8 !important; }
    [data-ogsc] .heading-dm { color: #ece9e4 !important; }
    [data-ogsc] .text-muted { color: #a8a49e !important; }
    [data-ogsc] .divider-line { border-color: #3a3835 !important; }
    [data-ogsc] .bar-track-dm { background-color: #1a1a18 !important; }
    [data-ogsc] .accent-dm { color: #5b9bd5 !important; }
    [data-ogsc] .accent-border-dm { border-top-color: #5b9bd5 !important; }
    [data-ogsc] .bar-fill-accent-dm { background-color: #5b9bd5 !important; }
</style>
</head>
"""


def _section_open() -> str:
    return ('<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            'style="background-color:#f5f3f1;" bgcolor="#f5f3f1" class="body-bg email-body-bg">')


def _p(text, size=15, weight=400, color="#222222", cls="text-main", mt=0, extra=""):
    return (f'<p style="margin:{mt}px 0 0 0; Margin:{mt}px 0 0 0; font-family:{FONT_BODY}; '
            f'font-size:{size}px; font-weight:{weight}; line-height:1.5; color:{color};{extra}" '
            f'class="{cls}">{text}</p>')


def _muted(text, size=12, mt=0, extra=""):
    return _p(text, size=size, color="#777777", cls="text-muted", mt=mt, extra=extra)


def _small_caps(text, color="#777777"):
    return (f'<span style="font-family:{FONT_HEADING}; font-size:11px; font-weight:600; '
            f'text-transform:uppercase; letter-spacing:0.04em; color:{color};">{esc(text)}</span>')


def _row(inner: str, top=24) -> str:
    return _section_open() + f'<tr><td style="padding:{top}px 40px 0 40px;" class="body-cell">{inner}</td></tr></table>'


def _masthead(eyebrow, title, dateline) -> str:
    return (
        _section_open() +
        f'<tr><td style="padding:40px 40px 8px 40px;" class="body-cell">'
        f'<p style="margin:0; Margin:0; font-family:{FONT_HEADING}; font-size:12px; font-weight:700; '
        f'letter-spacing:0.08em; text-transform:uppercase; color:{ACCENT};" class="accent-dm">{esc(eyebrow)}</p></td></tr>'
        f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
        f'<h1 class="email-h1 heading-dm text-main" style="margin:0; Margin:0; font-family:{FONT_HEADING}; '
        f'font-size:30px; font-weight:700; line-height:1.1; letter-spacing:-0.02em; color:#222222;">{esc(title)}</h1></td></tr>'
        f'<tr><td style="padding:6px 40px 0 40px;" class="body-cell">'
        f'<p style="margin:0; Margin:0; font-family:{FONT_HEADING}; font-size:12px; font-weight:600; '
        f'text-transform:uppercase; letter-spacing:0.04em; color:#777777;" class="text-muted">{esc(dateline)}</p></td></tr>'
        f'<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
        f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" class="tfix">'
        f'<tr><td bgcolor="{ACCENT}" style="border-top:3px solid {ACCENT}; font-size:1px; line-height:1px; '
        f'mso-line-height-rule:exactly;" class="accent-border-dm">&nbsp;</td></tr></table></td></tr></table>'
    )


def _usd(v):
    return format_usd(v)


def _pct(p):
    if p is None:
        return "new"
    n = round_pct(p)
    return f"{'+' if n >= 0 else '-'}{abs(n)}%"


def _stat_grid(model) -> str:
    t = model["total"]
    d1 = (model["ad_date"] - timedelta(days=1)).strftime("%a")
    cols = [
        ("Yesterday" + (" · partial" if model["partial"] else ""), _usd(t["d"]), f"{model['ad_date']:%a %-d %b}"),
        (f"vs {d1}", _pct(t["pct_d1"]), f"{_usd(t['d1'])} the day before"),
        ("vs 7-day avg", _pct(t["pct_avg"]), f"{_usd(t['avg7'])} average"),
    ]
    cells = "".join(
        f'<td valign="top" style="padding:0 8px 0 0;">{_small_caps(label)}'
        f'<p style="margin:4px 0 0 0; Margin:4px 0 0 0; font-family:{FONT_HEADING}; font-size:26px; font-weight:700; '
        f'line-height:1.1; letter-spacing:-0.01em; color:#222222; font-variant-numeric:tabular-nums;" '
        f'class="heading-dm">{esc(fig)}</p>{_muted(esc(gloss), 12, 4)}</td>'
        for label, fig, gloss in cols)
    return _row(f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" class="tfix"><tr>{cells}</tr></table>', 28)


def _banners(model, variant) -> str:
    notes = []
    for ch, s in model["channels"].items():
        if s["status"] == "unavailable":
            notes.append(f"{ch} unavailable ({s['reason']}). Totals below are partial.")
        elif s["status"] == "partial":
            notes.append(f"{ch}: {s['reason']}. Figures may be partial.")
        elif variant == "B" and not s["installs_ok"] and s["timezones"]:
            notes.append(f"{ch} installs unavailable today; shown as —.")
    if not notes:
        return ""
    body = "".join(_p(esc(n), 14, 600, mt=4 if i else 0) for i, n in enumerate(notes))
    return _row(f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" class="tfix">'
                f'<tr><td style="border-left:3px solid {ACCENT}; padding:2px 0 2px 14px;">{body}</td></tr></table>', 20)


def _detail(ln, d1_name, variant) -> str:
    lab = ln["label"]
    if lab == "started":
        bits = ["spend started"]
    elif lab == "stopped":
        bits = [f"spend stopped, was {_usd(ln['d1'])} {d1_name}"]
    elif lab == "dark":
        bits = [f"no spend {d1_name} or yesterday"]
    elif lab == "steady":
        bits = [f"steady vs {d1_name} ({_pct(ln['pct'])})" if ln["pct"] is not None else "steady"]
    else:
        bits = [f"spend {lab} {abs(round_pct(ln['pct']))}% vs {d1_name}"]
    if lab != "started":
        bits.append(f"7d avg {_usd(ln['avg7'])}")
    if variant == "B":
        if ln["installs_d"] is None:
            bits.append("installs —")
        else:
            inst = f"{int(ln['installs_d'].quantize(Decimal('1')))} installs"
            if ln["cpi_d"] is not None:
                cpi = f"CPI ${ln['cpi_d'].quantize(Decimal('0.01'))}"
                if ln["cpi_d1"] is not None:
                    cpi += f" ({d1_name} ${ln['cpi_d1'].quantize(Decimal('0.01'))})"
                inst += f" · {cpi}"
            bits.append(inst)
    ev = []
    if ln["camps_started"]:
        ev.append(f"{ln['camps_started']} campaign{'s' if ln['camps_started'] > 1 else ''} started")
    if ln["camps_stopped"]:
        ev.append(f"{ln['camps_stopped']} stopped")
    if ev:
        bits.append(", ".join(ev))
    return " · ".join(bits)


def _ledger_line(ln, d1_name, variant) -> str:
    os_ = "" if ln["os"] == UNASSIGNED else f" · {ln['os']}"
    weight = "700" if ln["label"] in ("started", "stopped") else "600"
    return (
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" class="tfix" '
        'style="margin-top:10px;"><tr>'
        f'<td style="font-family:{FONT_HEADING}; font-size:12px; font-weight:{weight}; text-transform:uppercase; '
        f'letter-spacing:0.04em; color:#777777; padding:0 8px 2px 0; vertical-align:bottom;" class="text-muted">'
        f'{esc(ln["channel"] + os_)}</td>'
        f'<td style="border-bottom:1px dotted #999999; font-size:1px; line-height:1px; padding-bottom:4px;" '
        f'class="divider-line">&nbsp;</td>'
        f'<td align="right" style="font-family:{FONT_HEADING}; font-size:16px; font-weight:700; color:#222222; '
        f'padding:0 0 2px 8px; font-variant-numeric:tabular-nums;" class="heading-dm">{esc(_usd(ln["d"]))}</td>'
        '</tr></table>' + _muted(esc(_detail(ln, d1_name, variant)), 12, 0, "padding-bottom:2px;")
    )


def _project(p, d1_name, variant) -> str:
    head = (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" class="tfix"><tr>'
            f'<td style="font-family:{FONT_HEADING}; font-size:18px; font-weight:700; color:#222222;" class="heading-dm">{esc(p["name"])}</td>'
            f'<td align="right" style="font-family:{FONT_HEADING}; font-size:18px; font-weight:700; color:#222222; '
            f'font-variant-numeric:tabular-nums;" class="heading-dm">{esc(_usd(p["d"]))}</td></tr></table>')
    return _row(head + "".join(_ledger_line(ln, d1_name, variant) for ln in p["lines"]), 30)


def _compact(v: Decimal) -> str:
    n = int(v.quantize(Decimal("1")))
    return f"{n / 1000:.1f}k" if n >= 1000 else str(n)


def _chart(model) -> str:
    pts = model["chart"][-14:]
    vals = [p["total"] for p in pts]
    vmax = max(vals) if vals else Decimal(0)
    MAX_PX, BAR_W = 56, 16
    peak_i = vals.index(vmax) if vmax > 0 else -1
    cells = []
    for i, (pt, v) in enumerate(zip(pts, vals)):
        h = max(2, round(float(v / vmax) * MAX_PX)) if vmax > 0 and v > 0 else 0
        label = (f'<p style="margin:0 0 2px 0; Margin:0 0 2px 0; font-family:{FONT_HEADING}; font-size:9px; '
                 f'font-weight:700; color:#444444;" class="text-main">{_compact(v)}</p>') if v > 0 else \
                '<p style="margin:0 0 2px 0; Margin:0 0 2px 0; font-size:9px;">&nbsp;</p>'
        bar = (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="{BAR_W}"><tr>'
               f'<td width="{BAR_W}" bgcolor="{ACCENT}" style="background-color:{ACCENT}; width:{BAR_W}px; height:{h}px; '
               f'font-size:0; line-height:0;" class="bar-fill-accent-dm">&nbsp;</td></tr></table>') if h else ""
        show_day = i in (0, len(pts) - 1, peak_i)
        cells.append(
            f'<td valign="bottom" align="center" style="padding:0 1px;">{label}'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0"><tr><td valign="bottom" '
            f'style="height:{MAX_PX}px;">{bar}</td></tr></table>'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"><tr>'
            f'<td width="100%" bgcolor="#dddddd" style="background-color:#dddddd; height:1px; font-size:0; line-height:0;" '
            f'class="divider-line">&nbsp;</td></tr></table>'
            f'<p style="margin:3px 0 0 0; Margin:3px 0 0 0; font-family:{FONT_HEADING}; font-size:10px; '
            f'font-weight:{700 if i == peak_i else 400}; color:#999999;" class="text-muted">'
            f'{pt["day"].day if show_day else "&nbsp;"}</p></td>')
    title = "Daily total spend · last 14 days" + (f" · peak {pts[peak_i]['day']:%-d %b} ({_usd(vmax)})" if peak_i >= 0 else "")
    return _row(f'<p style="margin:0 0 12px 0; Margin:0 0 12px 0;">{_small_caps(title)}</p>' +
                f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" class="tfix"><tr>{"".join(cells)}</tr></table>', 32)


def _footnotes(model, variant) -> str:
    lines = ["Channels covered: Meta and Google. Apple Search Ads is not included."]
    tz = "; ".join(f"{ch} {', '.join(s['timezones'])}" for ch, s in model["channels"].items() if s["timezones"])
    if tz:
        lines.append(f"Each channel's own ad-account day ({tz}), not the Pacific day.")
    lines.append("All figures in US dollars.")
    lines.append(f"A line is up or down only when it moves at least {int(DEADBAND_PCT * 100)}% and {_usd(DEADBAND_USD)} "
                 "against the day before. Meta pacing alone can move a campaign 10–20% a day.")
    if variant == "B":
        lines.append("Installs are as attributed by each platform. iOS installs for the latest day are delayed "
                     "and modelled, so they are usually undercounted.")
    body = "".join(_muted(esc(l), 11, 0 if i == 0 else 6, "text-align:center;") for i, l in enumerate(lines))
    link = (f'<p style="margin:10px 0 0 0; Margin:10px 0 0 0; font-family:{FONT_BODY}; font-size:11px; text-align:center;" '
            f'class="text-muted"><a href="{esc(WORKFLOW_URL)}" style="color:#888888; text-decoration:underline;" '
            f'class="email-link">Workflow &#8599;</a></p>')
    return _section_open() + f'<tr><td style="padding:32px 40px 40px 40px;" class="body-cell">{body}{link}</td></tr></table>'


def render(model: dict, variant: str, sentences: list, apps: dict) -> tuple:
    """Pure. -> (subject, preheader, html, text). Byte-stable for a given input:
    no run id, timestamp or URL that changes per run."""
    ad = model["ad_date"]
    d1_name = (ad - timedelta(days=1)).strftime("%a")
    subject = subject_of(model, variant, apps)
    preheader = sentences[0] if sentences else subject
    eyebrow = "Lascade · Ad spend" + (" + CPI" if variant == "B" else "")
    lead = _row(_p(esc(" ".join(sentences)), 17, 400, mt=0), 22) if sentences else ""
    parts = [
        _masthead(eyebrow, f"{ad:%A} {ad.day} {ad:%B}", "ad-account day · Meta, Google"),
        _banners(model, variant), lead, _stat_grid(model),
        "".join(_project(p, d1_name, variant) for p in model["projects"]),
        _chart(model), _footnotes(model, variant),
    ]
    html = (
        HEAD.replace("__TITLE__", esc(subject)) +
        '<body style="margin:0; padding:0; background-color:#f5f3f1; width:100%; -webkit-text-size-adjust:100%; '
        '-ms-text-size-adjust:100%;" class="body-bg">'
        f'<div style="display:none; font-size:1px; color:#f5f3f1; line-height:1px; max-height:0px; max-width:0px; '
        f'opacity:0; overflow:hidden; mso-hide:all;">{esc(preheader)}'
        '&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;</div>'
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
        'style="background-color:#f5f3f1;" class="body-bg"><tr><td align="center" valign="top" style="padding:20px 10px;">'
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="600" align="center" '
        'style="width:100%; max-width:600px;" class="email-container"><tr><td>' + "".join(parts) +
        '</td></tr></table></td></tr></table></body></html>')

    t = model["total"]
    text = [eyebrow.upper(), f"{ad:%A} {ad.day} {ad:%B}", ""]
    text += [f"! {s['reason']}" for s in model["channels"].values() if s["status"] != "ok"]
    if sentences:
        text += ["", " ".join(sentences)]
    text += ["", f"Yesterday {_usd(t['d'])}{' (partial)' if model['partial'] else ''}",
             f"vs {d1_name}: {_pct(t['pct_d1'])} ({_usd(t['d1'])})", f"vs 7-day avg: {_pct(t['pct_avg'])} ({_usd(t['avg7'])})"]
    for p in model["projects"]:
        text += ["", f"{p['name']}  {_usd(p['d'])}"]
        for ln in p["lines"]:
            os_ = "" if ln["os"] == UNASSIGNED else f" {ln['os']}"
            text.append(f"  {ln['channel']}{os_}: {_usd(ln['d'])} - {_detail(ln, d1_name, variant)}")
    text += ["", "Last 14 days: " + ", ".join(_compact(p["total"]) for p in model["chart"][-14:])]
    text += ["", "Meta and Google only. Each channel's own ad-account day. Workflow: " + WORKFLOW_URL]
    return subject, preheader, html, "\n".join(text)
