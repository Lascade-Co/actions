"""Render the approved TreasureHunch layout with local install-paid chassis helpers."""
from __future__ import annotations

from datetime import timedelta
from html import escape as esc

import hr_model as M


FONT_HEADING = "'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif"

FONT_BODY = "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"

ACCENT = "#2980b9"

C_BETTER, C_WORSE, C_LEVEL, C_THIN = "#2a8c4a", "#c0392b", "#2980b9", "#a8a49e"

INK, CTX = "#222222", "#66615a"

T_BETTER, T_WORSE, T_LEVEL = "#1e7a3c", "#b0392c", "#1f6aa5"

TRACK_BG = "#ebe8e4"

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


WORD = {
    "good": ("Good.", "hl-good", T_BETTER),
    "continue": ("Worth continuing.", "hl-lvl", T_LEVEL),
    "stop": ("Stop line.", "hl-bad", T_WORSE),
    "unclear": ("Too early to tell.", "hl-lvl", T_LEVEL),
}


def pct(rate, decimals=1):
    return "—" if rate is None else f"{rate:.{decimals}%}"


def ctx(text):
    return f'<span style="color:{CTX};" class="hl-ctx">{esc(str(text))}</span>'


def ink(text):
    return f'<span style="color:{INK}; font-weight:600;" class="hl-num">{esc(str(text))}</span>'


def colored(text, key):
    _, cls, color = WORD[key]
    return f'<span style="color:{color}; font-weight:700;" class="{cls}">{esc(text)}</span>'


def h2(text):
    return _row(f'<h2 class="heading-dm text-main" style="margin:0; font-family:{FONT_HEADING}; font-size:18px; '
                f'font-weight:700; color:{INK};">{esc(text)}</h2>', 32)


def th(text, align="left"):
    return (f'<td align="{align}" style="padding:0 6px 6px 0; font-family:{FONT_HEADING}; font-size:11px; '
            f'font-weight:600; text-transform:uppercase; letter-spacing:0.04em; color:{CTX};" '
            f'class="text-muted">{esc(text)}</td>')


def _range(first, last):
    return f'{first:%-d}–{last:%-d %b}' if first.month == last.month and first.year == last.year else f'{first:%-d %b}–{last:%-d %b}'


def _stat(label, figure, gloss, key=None):
    color = WORD[key][2] if key else INK
    cls = WORD[key][1] if key else "hl-num"
    return (f'<td valign="top" style="padding:0 8px 0 0;"><div style="min-height:30px;">{_caps(label)}</div>'
            f'<p style="margin:4px 0 0 0; font-family:{FONT_HEADING}; font-size:26px; font-weight:700; '
            f'line-height:1.1; color:{color};" class="stat-fig {cls}">{esc(figure)}</p>'
            f'<p style="margin:4px 0 0 0; font-family:{FONT_BODY}; font-size:12px;">{gloss}</p></td>')


def _ba_row(label, before, since, note=""):
    if since["rate"] is None or before["rate"] is None:
        after_rate = ctx(pct(since["rate"]))
    else:
        after_rate = colored(pct(since["rate"]), "good" if since["rate"] > before["rate"] else "stop")
    return (f'<tr><td style="padding:8px 8px 8px 0; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:14px; color:{INK};" class="text-main divider-line">{esc(label)}'
            + (f'<br/><span style="font-size:11px; color:{CTX};" class="hl-ctx">{esc(note)}</span>' if note else "")
            + f'</td><td align="right" style="padding:8px; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:14px;" class="divider-line">{ink(pct(before["rate"]))}<br/>{ctx(f"{before['k']} of {before['n']}")}</td>'
            f'<td align="right" style="padding:8px 0 8px 8px; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:14px;" class="divider-line">{after_rate}<br/>{ctx(f"{since['k']} of {since['n']}")}</td></tr>')


def _cell(row, key):
    metric = row["metrics"][key]
    if metric["pending"]:
        return ctx("pending")
    return (ctx if row["thin"] else ink)(pct(metric["rate"], 0))


def _group_row(row):
    d0, n = row["d0"], row["n"]
    value = row["metrics"]["d1"]["rate"]
    color = C_THIN if row["thin"] else {"good": C_BETTER, "continue": C_LEVEL, "stop": C_WORSE}.get(M.band(value), C_THIN)
    if row["metrics"]["d1"]["pending"]:
        bar = f'<span style="font-family:{FONT_BODY}; font-size:13px;">{ctx("pending")}</span>'
    else:
        bar = _bar(value or 0, 0.5, color, [(M.GOOD, "#444444", "tick-mar-dm"), (M.CONT, "#9a6a10", "tick-aug-dm")])
    return (f'<tr><td valign="top" style="padding:7px 6px 7px 0; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:13px; width:74px;" class="divider-line bar-label-cell">{ink(f"{d0:%a %-d}")}'
            + (f'<br/><span style="font-size:10px; color:{ACCENT};" class="accent-dm">{esc(row["marker"])}</span>' if row["marker"] else "")
            + f'</td><td valign="top" align="right" style="padding:7px 6px; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:13px; width:34px;" class="divider-line">{ctx(n) if row["thin"] else ink(n)}</td>'
            f'<td valign="middle" style="padding:7px 6px; border-top:1px solid #e0dcd6;" class="divider-line">{bar}</td>'
            f'<td valign="top" align="right" style="padding:7px 6px; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:13px; width:44px;" class="divider-line">{_cell(row, "d1")}</td>'
            f'<td valign="top" align="right" style="padding:7px 6px; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:13px; width:52px;" class="divider-line">{_cell(row, "d3")}</td>'
            f'<td valign="top" align="right" style="padding:7px 0 7px 6px; border-top:1px solid #e0dcd6; font-family:{FONT_BODY}; '
            f'font-size:13px; width:52px;" class="divider-line">{_cell(row, "d7any")}</td></tr>')


def _engagement_row(row):
    hunts_each = f'{row["hunts_each"]:.1f}' if row["hunts_each"] is not None else "—"
    values = [f'{row["day"]:%a %-d}', row["players"], hunts_each, pct(row["finished_rate"], 0),
              pct(row["won_rate"], 0), row["daily_players"]]
    return "<tr>" + "".join(
        f'<td align="{"left" if i == 0 else "right"}" style="padding:6px {0 if i in (0, 5) else 6}px; border-top:1px solid #e0dcd6; '
        f'font-family:{FONT_BODY}; font-size:13px;" class="divider-line">{ink(value)}</td>'
        for i, value in enumerate(values)) + "</tr>"


def render(model):
    """Return the subject, email-safe HTML, and equivalent plain text; never log facts."""
    last = model["last_day"]
    verdict = model["verdict"]
    current, previous = verdict["current"], verdict["previous"]
    before, since, today = model["before"], model["since"], model["today"]
    label = f"{last:%a %-d %b}"
    subject_line = {"good": "good 30%", "continue": "worth continuing 15%"}.get(verdict["band"], "stop line 15%")
    subject = (f'TreasureHunch · {label}: next-day return {pct(current["rate"])} ({subject_line}) · '
               f'new players {today["new_players"]}, was ~{round(model["new_players_baseline"])}')
    if model.get("test_number") is not None:
        subject = f'[Test {model["test_number"]}] {subject}'

    grid = _row(f'{TABLE} class="tfix"><tr>' + "".join([
        _stat("Back next day", pct(current["rate"]), f'{ctx("last 7 groups ·")} {ink(current["k"])} {ctx("of")} {ink(current["n"])}', verdict["band"]),
        _stat("Back in 7 days", pct(since["d7any"]["rate"], 0), f'{ctx("since 8 Nov ·")} {ink(since["d7any"]["k"])} {ctx("of")} {ink(since["d7any"]["n"])}'),
        _stat("New players", str(today["new_players"]), ctx(f'{label} · paid {today["paid_new"]}'), "stop" if today["new_players"] < 30 else None),
        _stat("Players", str(today["players"]), ctx(f'{label} · {today["hunts"]} hunts')),
    ]) + '</tr></table>', 28)

    span = _range(verdict["first_day"], verdict["last_day"])
    change = verdict["change"]
    change_tail = f"so {change}." if change == "no real change" else f"so the rate is {change}." if change in ("up", "down") else "it is too early to compare."
    verdict_text = (f'{WORD[verdict["band"]][0]} Next-day return over the last 7 player groups is '
                    f'{pct(current["rate"])} ({current["k"]} of {current["n"]}, {span}). '
                    f'Good is 30%, worth continuing is 15%. The 7 groups before were {pct(previous["rate"])} '
                    f'({previous["k"]} of {previous["n"]}), {change_tail}')
    verdict_html = _row(_p(
        f'{colored(WORD[verdict["band"]][0], verdict["band"])} Next-day return over the last 7 player groups is '
        f'{ink(pct(current["rate"]))} {ctx(f"({current['k']} of {current['n']}, {span})")}. '
        f'Good is 30%, worth continuing is 15%. The 7 groups before were {ink(pct(previous["rate"]))} '
        f'{ctx(f"({previous['k']} of {previous['n']})")}, {esc(change_tail)}', 15), 20)

    summary = model.get("summary") or "Summary unavailable today."
    summary_html = _row(f'{TABLE} class="tfix"><tr><td class="card-bg" bgcolor="#ebe8e4" style="background-color:#ebe8e4; '
                        f'padding:16px 18px; border-left:3px solid {ACCENT};">{_caps("The read")}'
                        + _p(esc(summary), 15, mt=6) + '</td></tr></table>', 20)
    check_text = [check["text"] for check in model["checks"]] or ["No checks triggered today."]
    check_html = _row("".join(_p(f'{_caps("Check")} &nbsp;{esc(line)}', 14, mt=0 if i == 0 else 10)
                              for i, line in enumerate(check_text)), 12)

    seven_day_end = last - timedelta(days=7)
    closed_note = (f'groups whose 7 days have ended: {_range(M.RELEASE, seven_day_end)}'
                   if seven_day_end >= M.RELEASE else 'no groups since 8 Nov have finished their 7 days')
    comparisons = [
        ("Back next day", "d1", ""),
        ("Back at least once in 7 days", "d7any", closed_note),
        ("Played on day 7", "d7", "the strict industry measure; casual-game top 10% ≈ 12%"),
        ("Played on 3 or more days", "days3", ""),
    ]
    comparison_note = ("Before = first hunt 31 Oct–7 Nov. Since = 8 Nov onward. Groups whose window has not ended are "
                       "left out of each row.")
    ba = _row(f'{TABLE} class="tfix"><tr>{th("")}{th("Before 8 Nov", "right")}{th("Since 8 Nov", "right")}</tr>'
              + "".join(_ba_row(label_, before[key], since[key], note) for label_, key, note in comparisons)
              + '</table>' + _muted(comparison_note, mt=8), 12)

    group_note = ("Bar = back next day, on a 0–50% scale. Ticks: charcoal = 30% good, amber = 15% worth "
                  "continuing. Red is under 15%. Grey = fewer than 30 players, not read.")
    groups_html = _row(f'{TABLE} class="tfix"><tr>{th("First hunt")}{th("New", "right")}{th("Back next day")}'
                       f'{th("D1", "right")}{th("≤3 days", "right")}{th("≤7 days", "right")}</tr>'
                       + "".join(_group_row(row) for row in model["groups"]) + '</table>'
                       + _muted(group_note, mt=8), 12)

    returners = model["daily_returners"]
    returning_text = (f'{returners["k"]} of {returners["n"]} players who came back since 8 Nov played the daily puzzle '
                      f'{pct(returners["rate"], 0)}.')
    if returners["rate"] is not None and returners["rate"] > 0.5:
        returning_text += " Most returning players played the daily puzzle."
    engagement_html = _row(f'{TABLE} class="tfix"><tr>{th("Day")}{th("Players", "right")}{th("Hunts each", "right")}'
                           f'{th("Finished", "right")}{th("Won", "right")}{th("Daily puzzle", "right")}</tr>'
                           + "".join(_engagement_row(row) for row in model["days"]) + '</table>'
                           + _p(f'{ink(f"{returners['k']} of {returners['n']}")} '
                                f'{ctx("players who came back since 8 Nov played the daily puzzle")} '
                                f'{ink(pct(returners["rate"], 0))}{ctx(".")}'
                                + (ctx(" Most returning players played the daily puzzle.") if returners["rate"] is not None and returners["rate"] > 0.5 else ""), 14, mt=12), 12)

    new_events = ", ".join(model["new_events"])
    new_builds = " and ".join(f'{row["platform"]} {row["version"]}' for row in model["new_versions"])
    events_text = f"New events first seen: {new_events}." if new_events else "No new events first seen."
    builds_text = f"New app builds: {new_builds}." if new_builds else "No new app builds first seen."
    paid_text = (f'Paid share of new players: {today["paid_new"]} of {today["new_players"]} '
                 f'(the week before: {pct(model["paid_baseline"]["rate"], 0)}).')
    changed = _row(_p((f'{ctx("New events first seen:")} {ink(new_events)}{ctx(".")}' if new_events else ctx(events_text))
                      + " " + (f'{ctx("New app builds:")} {ink(new_builds)}{ctx(".")}' if new_builds else ctx(builds_text)), 14)
                   + _p(f'{ctx("Paid share of new players:")} {ink(f"{today['paid_new']} of {today['new_players']}")} '
                        f'{ctx(f"(the week before: {pct(model['paid_baseline']['rate'], 0)}).")}', 14, mt=8), 12)

    footer = (f'Days are UTC days, through the end of {label}. A player is someone who started a hunt; '
              'a player group is everyone whose first hunt fell on the same day. "Back next day" means the player '
              'started a hunt on the next day. Pass lines come from the TreasureHunch green-light board. '
              'Source: PostHog project 701052.')
    foot = _row(_muted(esc(footer)) + '<p style="margin:0; padding-bottom:40px;"></p>', 32)
    headings = ["Check first", "Did the 8 Nov release work?", "Player groups since 8 Nov",
                "Engagement, last 7 days", f"What changed on {label}"]
    parts = [_masthead("Lascade · TreasureHunch", "Retention", f"{label} · release line 8 Nov · UTC days"),
             grid, verdict_html, summary_html, h2(headings[0]), check_html, h2(headings[1]), ba,
             h2(headings[2]), groups_html, h2(headings[3]), engagement_html, h2(headings[4]), changed, foot]
    html = (HEAD.replace("__TITLE__", esc(subject))
            + '<body style="margin:0; padding:0; background-color:#f5f3f1; width:100%;" class="body-bg">'
            + f'{TABLE} style="background-color:#f5f3f1;" class="body-bg"><tr><td align="center" valign="top" style="padding:20px 10px;">'
            + '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="600" align="center" '
            'style="width:100%; max-width:600px;" class="email-container"><tr><td>'
            + "".join(parts) + '</td></tr></table></td></tr></table></body></html>')

    lines = [subject, "", "Lascade · TreasureHunch", "Retention", f"{label} · release line 8 Nov · UTC days", "",
             f'Back next day: {pct(current["rate"])} ({current["k"]} of {current["n"]}; last 7 groups)',
             f'Back in 7 days: {pct(since["d7any"]["rate"], 0)} ({since["d7any"]["k"]} of {since["d7any"]["n"]}; since 8 Nov)',
             f'New players: {today["new_players"]} (paid {today["paid_new"]})',
             f'Players: {today["players"]} ({today["hunts"]} hunts)', "", verdict_text, "", "The read", summary, "", headings[0], *check_text, "", headings[1]]
    for label_, key, note in comparisons:
        b, a = before[key], since[key]
        lines.append(f'{label_}: before {pct(b["rate"])} ({b["k"]} of {b["n"]}); '
                     f'since {pct(a["rate"])} ({a["k"]} of {a["n"]})' + (f" · {note}" if note else ""))
    lines += [comparison_note, "", headings[2], "First hunt | New | D1 | ≤3 days | ≤7 days"]
    for row in model["groups"]:
        cells = ["pending" if row["metrics"][key]["pending"] else pct(row["metrics"][key]["rate"], 0)
                 for key in ("d1", "d3", "d7any")]
        lines.append(f'{row["d0"]:%a %-d}: {row["n"]} | ' + " | ".join(cells)
                     + (" · grey, not read" if row["thin"] else "") + (f' · {row["marker"]}' if row["marker"] else ""))
    lines += [group_note, "", headings[3], "Day | Players | Hunts each | Finished | Won | Daily puzzle"]
    for row in model["days"]:
        hunts_each = f'{row["hunts_each"]:.1f}' if row["hunts_each"] is not None else "—"
        lines.append(f'{row["day"]:%a %-d}: {row["players"]} | {hunts_each} | {pct(row["finished_rate"], 0)} | '
                     f'{pct(row["won_rate"], 0)} | {row["daily_players"]}')
    lines += [returning_text, "", headings[4], events_text + " " + builds_text, paid_text, "", footer]
    return subject, html, "\n".join(lines)
