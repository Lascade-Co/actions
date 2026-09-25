"""Render report.json into the Lascade Daily Report HTML email. Pure JSON -> HTML.

No LLM and no network, so it can be unit-tested standalone: feed it a sample
report.json and open the result in a browser. The markup follows the cream
"Daily Report" style guide (table-based, Inter font, Outlook ghost tables,
dark-mode `class=` hooks).

Usage:
    python scripts/catchup/catchup_render_email.py --report report.json \\
        --org-label Lascade --hours 24 --people data/catchup_people.json \\
        --out email.html
"""

import argparse
import json
import re
import sys
from html import escape

FONT = "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"

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
<title>__ORG_LABEL__ Daily Report</title>
<!--[if mso]>
<noscript><xml><o:OfficeDocumentSettings><o:AllowPNG/><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript>
<style type="text/css">body, table, td, th, p, li, a, span, h1, h2 { font-family: 'Segoe UI', Arial, sans-serif !important; }</style>
<![endif]-->
<!--[if !mso]><!-->
<style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');</style>
<!--<![endif]-->
<style type="text/css">
body{margin:0 !important;padding:0 !important;width:100% !important;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%}
table{border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt}
td{border-collapse:collapse}
p{margin:0;Margin:0}
ul{margin:0;Margin:0}
a[x-apple-data-detectors]{color:inherit !important;text-decoration:none !important}
@media only screen and (max-width:480px){
.email-container{width:100% !important;max-width:100% !important}
.email-container td.body-cell{padding-left:20px !important;padding-right:20px !important}
h1.email-h1{font-size:26px !important}
h2.email-h2{font-size:20px !important}
}
@media (prefers-color-scheme:dark){
.body-bg{background-color:#1a1a18 !important}
.email-body-bg{background-color:#222220 !important}
.text-main{color:#e0ddd8 !important}
.heading-dm{color:#ece9e4 !important}
.text-muted{color:#a8a49e !important}
.divider-line{border-color:#3a3835 !important}
.header-border{border-color:#5a5855 !important}
.callout-info-bg{background-color:#2a2927 !important}
.callout-info-border{background-color:#5a6a9a !important}
.callout-warning-bg{background-color:#2a2927 !important}
__DARK__
}
[data-ogsc] .body-bg{background-color:#1a1a18 !important}
[data-ogsc] .email-body-bg{background-color:#222220 !important}
[data-ogsc] .text-main{color:#e0ddd8 !important}
[data-ogsc] .heading-dm{color:#ece9e4 !important}
[data-ogsc] .text-muted{color:#a8a49e !important}
[data-ogsc] .divider-line{border-color:#3a3835 !important}
[data-ogsc] .header-border{border-color:#5a5855 !important}
[data-ogsc] .callout-info-bg{background-color:#2a2927 !important}
[data-ogsc] .callout-info-border{background-color:#5a6a9a !important}
[data-ogsc] .callout-warning-bg{background-color:#2a2927 !important}
__OGSC__
</style>
</head>
"""


# Gmail clips messages around 102KB; warn well before that.
MAX_EMAIL_BYTES = 90_000
# Gmail drops a <style> block over 8,192 characters; warn before that.
MAX_STYLE_CHARS = 7_500


# Author tag colours: (light bg, light text, dark bg, dark text). Every pair is
# at least 4.5:1 at 11px; tests enforce it.
TAG_PALETTE = [
    ("#dbe6f7", "#1f4a85", "#263a5c", "#b9cdf2"),  # blue
    ("#f7e0cf", "#8a3c08", "#4a3222", "#f2bd92"),  # orange
    ("#d3ece7", "#0e5750", "#21403c", "#9ddad1"),  # teal
    ("#f6d9dc", "#8c2432", "#4d2a2f", "#f2a9b3"),  # rose
    ("#dcebd3", "#2f5f1c", "#2a3d24", "#acd99b"),  # green
    ("#e5dcf3", "#5a2f8c", "#3a2c52", "#cfb8f0"),  # purple
    ("#efe6c2", "#66500a", "#443b1f", "#e8d17f"),  # gold
    ("#f5d8ea", "#8a2260", "#4a2a3f", "#f2a9d2"),  # pink
]

# Everyone who is not a pinned regular: (light bg, light text, dark bg, dark text).
# Same 4.5:1 rule as the palette; tests enforce it.
TAG_GREY = ("#e8e6e3", "#555555", "#3a3835", "#e0ddd8")

# Group key -> (marker, light colour, dark colour, class). These are text colours,
# so they are darker than the style guide's #2a8c4a / #c9922a (3.83:1 / 2.49:1 on
# cream); those stay reserved for non-text accents such as the callout bar.
STATUS = {
    "done": ("●", "#1e6b38", "#7fcf98", "status-done"),
    "testing": ("◐", "#8a5a0b", "#e0b465", "status-testing"),
    "in_progress": ("○", "#4a5680", "#a9b4dd", "status-wip"),
}


def _dark_rules():
    """(selector, declarations) for every class whose dark look is generated here."""
    rules = [(f".tag-c{i}", f"background-color:{db} !important;color:{dt} !important")
             for i, (_, _, db, dt) in enumerate(TAG_PALETTE)]
    rules.append((".tag-grey", f"background-color:{TAG_GREY[2]} !important;color:{TAG_GREY[3]} !important"))
    rules += [(f".{cls}", f"color:{dark} !important") for _, _, dark, cls in STATUS.values()]
    rules += [(".callout-warning-head", "color:#e0b465 !important"),
              (".callout-info-head", "color:#e0ddd8 !important")]
    return rules


def slot_for(key, pins=None):
    """Palette slot for a colour key: its valid pin, else None (a grey tag).

    Only pinned regulars get a colour. Nothing is hashed, so a newcomer can never
    borrow a regular's colour.
    """
    pin = (pins or {}).get(key)
    return pin if type(pin) is int and 0 <= pin < len(TAG_PALETTE) else None


def load_pins(path):
    """{lowercased canonical name: slot} from each person's `colour` in the people
    file (data/catchup_people.json). That key equals the report's colour key after
    the people pass. Unusable input is ignored, never fatal: everyone shows grey."""
    if not path:
        return {}
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"People file {path} unusable ({exc}); using grey tags.", file=sys.stderr)
        return {}
    people = data.get("people") if isinstance(data, dict) else None
    if not isinstance(people, list):
        print(f"People file {path} has no people list; using grey tags.", file=sys.stderr)
        return {}
    return {p["name"].strip().lower(): p["colour"] for p in people
            if isinstance(p, dict) and isinstance(p.get("name"), str) and p["name"].strip()
            and type(p.get("colour")) is int and 0 <= p["colour"] < len(TAG_PALETTE)}


def esc(value):
    return escape(str(value if value is not None else ""))


def oversized_style_blocks(html):
    """Sizes of any <style> block over MAX_STYLE_CHARS (Gmail would drop it)."""
    return [len(m) for m in re.findall(r"<style[^>]*>(.*?)</style>", html, re.S)
            if len(m) > MAX_STYLE_CHARS]


def section(rows):
    return ('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
            'width="100%" style="background-color:#f5f3f1;" class="email-body-bg">'
            + rows + '</table>')


def cell(inner, pad, extra=""):
    return f'<tr><td style="padding:{pad};{extra}" class="body-cell">{inner}</td></tr>'


def render_header(report, org_label, hours):
    date = esc(report.get("date", ""))
    title = esc(f"{org_label} Daily Report")
    window = esc(f"Last {hours} hours" if hours else "Last 24 hours")
    rule = ('<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"><tr>'
            '<td style="border-top:2px solid #222222;font-size:1px;line-height:1px;'
            'mso-line-height-rule:exactly;" class="header-border">&nbsp;</td></tr></table>')
    return section(
        cell(f'<p style="font-family:{FONT};font-size:12px;font-weight:600;letter-spacing:0.08em;'
             f'text-transform:uppercase;color:#555555;" class="text-muted">Daily Report</p>',
             "40px 40px 10px 40px") +
        cell(f'<h1 class="email-h1 heading-dm" style="margin:0;font-family:{FONT};font-size:30px;'
             f'font-weight:700;line-height:1.1;letter-spacing:-0.02em;color:#222222;">{title}</h1>',
             "10px 40px 0 40px") +
        cell(f'<p style="font-family:{FONT};font-size:13px;line-height:1.5;color:#666666;" '
             f'class="text-muted">{date} · {window}</p>', "8px 40px 0 40px") +
        cell(rule, "20px 40px 0 40px"))


def render_headline(report):
    text = str(report.get("headline") or "")
    if not text:
        return ""
    return section(cell(
        f'<p style="font-family:{FONT};font-size:18px;line-height:1.55;color:#555555;" '
        f'class="text-muted">{esc(text)}</p>', "20px 40px 0 40px"))


def render_needs_you(report):
    """Decisions, or a note that the day couldn't be assessed. Nothing on a quiet, assessed day."""
    decisions = report.get("decisions_needed") or []
    if decisions:
        accent, bg, head = "#c9922a", "#faf3e6", "#8a5a0b"
        bar_cls, bg_cls, head_cls = "", "callout-warning-bg", "callout-warning-head"
        text = "; ".join(esc(d) for d in decisions)
    elif report.get("assessed") is False:
        accent, bg, head = "#222222", "#ece9e6", "#222222"
        bar_cls, bg_cls, head_cls = "callout-info-border", "callout-info-bg", "callout-info-head"
        text = "Couldn't be assessed today."
    else:
        return ""
    callout = (
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"><tr>'
        f'<td width="4" bgcolor="{accent}" style="background-color:{accent};" class="{bar_cls}"></td>'
        f'<td bgcolor="{bg}" style="background-color:{bg};padding:16px 20px;" class="{bg_cls}">'
        f'<p style="margin:0 0 4px 0;font-family:{FONT};font-size:13px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:0.04em;color:{head};" class="{head_cls}">Needs you</p>'
        f'<p style="font-family:{FONT};font-size:15px;line-height:1.55;color:#222222;" '
        f'class="text-main">{text}</p></td></tr></table>')
    return section(cell(callout, "24px 40px 0 40px"))


MAX_AUTHORS = 5


def _key(person):
    return person.get("key") or person.get("name", "").lower() or "team"


def author_bits(contributors):
    """[('Cherian 4', key), ..., ('+N more', None)]: top MAX_AUTHORS by commits."""
    people = [c for c in contributors if c.get("name")]
    bits = [(f'{c["name"]} {c.get("commits", 0)}', _key(c)) for c in people[:MAX_AUTHORS]]
    if len(people) > MAX_AUTHORS:
        bits.append((f"+{len(people) - MAX_AUTHORS} more", None))
    return bits


def tag(label, key, pins=None):
    """An author tag. The font family comes from the product's cell it sits in."""
    slot = slot_for(key, pins)
    bg, fg = TAG_GREY[:2] if slot is None else TAG_PALETTE[slot][:2]
    cls = "tag-grey" if slot is None else f"tag-c{slot}"
    return (f'<span style="display:inline-block;font-size:11px;font-weight:500;'
            f'line-height:1.4;padding:2px 7px;border-radius:4px;background-color:{bg};'
            f'color:{fg};white-space:nowrap;" class="{cls}">{esc(label)}</span>')


def _li(text, authors, status, pins):
    """One bullet: a status marker hanging in the left padding, then text and author tags.
    Size, spacing and colour are inherited from the list, the font from the product's cell."""
    marker, color, _, status_cls = status
    suffix = " " + " ".join(tag(a["name"], _key(a), pins) for a in authors) if authors else ""
    return (f'<li style="list-style:none;margin:6px 0;padding-left:22px;line-height:1.6;'
            f'word-break:break-word;">'
            f'<span aria-hidden="true" style="display:inline-block;width:22px;margin-left:-22px;'
            f'color:{color};" class="{status_cls}">{marker}</span>{esc(text)}{suffix}</li>')


def _ul(items, muted=False):
    """The bullets of one list. It carries the size, spacing and colour (and the
    dark-mode class) that every item would otherwise repeat; muted lists are grey.
    The font family comes from the product's cell."""
    tcolor, cls = ("#666666", "text-muted") if muted else ("#222222", "text-main")
    # role="list": WebKit drops list semantics once list-style is none.
    return (f'<ul role="list" style="list-style:none;padding:0;margin:8px 0 0 0;'
            f'font-size:16px;letter-spacing:0.01em;color:{tcolor};" class="{cls}">{"".join(items)}</ul>')


def _label(text, top, color="#555555", cls="text-muted"):
    return (f'<p style="margin:{top}px 0 0 0;font-size:13px;font-weight:600;'
            f'text-transform:uppercase;letter-spacing:0.03em;color:{color};" '
            f'class="{cls}">{esc(text)}</p>')


def render_group(group, pins=None):
    bullets = group.get("bullets") or []
    also = group.get("also") or []
    if not bullets and not also:
        return ""
    status = STATUS.get(group.get("key"), STATUS["in_progress"])
    out = _label(group.get("label"), 20, status[1], status[3])
    if bullets:
        out += _ul(_li(b.get("text"), b.get("authors") or [], status, pins) for b in bullets)
    if also:
        out += _label("Also (technical detail)", 12)
        out += _ul((_li(a.get("text"), [a["author"]] if a.get("author") else [], status, pins)
                    for a in also), muted=True)
    return out


def facts_line(repo):
    """'v4.0.40 · 3 PRs merged': only the parts present; empty when neither is."""
    parts = []
    version = str(repo.get("version") or "").strip()
    if version:
        parts.append(esc(version))
    prs = repo.get("prs_merged")
    if type(prs) is int and prs > 0:
        parts.append(f"{prs} PR{'s' if prs != 1 else ''} merged")
    if not parts:
        return ""
    return (f'<p style="margin:4px 0 0 0;font-size:13px;line-height:1.5;'
            f'color:#666666;" class="text-muted">{" · ".join(parts)}</p>')


def render_repo(repo, pins=None):
    body = "".join(render_group(g, pins) for g in repo.get("groups") or [])
    bits = author_bits(repo.get("contributors") or [])
    if not body and not bits:
        return ""
    if bits:
        shown = [tag(label, key, pins) if key else esc(label) for label, key in bits]
        body += (f'<p style="margin:16px 0 0 0;font-size:12px;line-height:2;'
                 f'color:#666666;" class="text-muted">{" ".join(shown)}</p>')
    divider = ('<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
               '<tr><td style="border-top:1px solid #dddddd;font-size:1px;line-height:1px;" '
               'class="divider-line">&nbsp;</td></tr></table>')
    emoji = esc(repo.get("emoji") or "")
    title = f'{emoji} {esc(repo.get("display_name"))}' if emoji else esc(repo.get("display_name"))
    heading = (f'<h2 class="email-h2 heading-dm" style="margin:0;font-family:{FONT};font-size:22px;'
               f'font-weight:700;line-height:1.3;letter-spacing:-0.015em;color:#222222;">'
               f'{title}</h2>')
    return section(cell(divider, "28px 40px 0 40px") + cell(heading + facts_line(repo) + body, "20px 40px 0 40px",
                                                                 f"font-family:{FONT};"))


def render_footer(date, hours):
    window = esc(f"the last {hours} hours" if hours else "the last 24 hours")
    return section(cell(
        f'<p style="font-family:{FONT};font-size:13px;font-style:italic;line-height:1.5;'
        f'color:#666666;" class="text-muted">Generated {esc(date)} · Covers {window}</p>',
        "32px 40px 32px 40px"))


def render(report, org_label="Daily", hours=None, pins=None):
    preheader = (f'<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;'
                 f'font-size:1px;line-height:1px;color:#f5f3f1;">{esc(report.get("headline") or "")}</div>')
    body = [preheader, render_header(report, org_label, hours),
            render_headline(report),
            render_needs_you(report)]
    body += [render_repo(repo, pins) for repo in report.get("repos", [])]
    body.append(render_footer(report.get("date", ""), hours))

    inner = "".join(body)
    rules = _dark_rules()
    head = (HEAD.replace("__ORG_LABEL__", esc(org_label))
            .replace("__DARK__", "\n".join(f"{sel}{{{decl}}}" for sel, decl in rules))
            .replace("__OGSC__", "\n".join(f"[data-ogsc] {sel}{{{decl}}}" for sel, decl in rules)))
    return (
        head +
        '<body style="margin:0;padding:0;background-color:#f5f3f1;width:100%;" '
        'class="body-bg">'
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
        'width="100%" style="background-color:#f5f3f1;" class="body-bg"><tr>'
        '<td align="center" valign="top" style="padding:20px 10px;">'
        '<!--[if mso]><table role="presentation" cellspacing="0" cellpadding="0" '
        'border="0" width="600" align="center"><tr><td><![endif]-->'
        '<div style="max-width:600px;width:100%;margin:0 auto;" class="email-container">'
        + inner +
        '</div>'
        '<!--[if mso]></td></tr></table><![endif]-->'
        '</td></tr></table></body></html>'
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, help="report.json input")
    parser.add_argument("--org-label", default="Lascade",
                        help="Short org/brand name used in the title and header. "
                             "Default: 'Lascade'.")
    parser.add_argument("--hours", type=int, default=None,
                        help="Look-back window in hours, for the header/footer "
                             "label. Defaults to 24 if omitted.")
    parser.add_argument("--people", help="people file whose `colour` fields pin each "
                                         "regular's tag colour (data/catchup_people.json)")
    parser.add_argument("--out", required=True, help="email.html output")
    args = parser.parse_args()

    with open(args.report) as fh:
        report = json.load(fh)
    html = render(report, org_label=args.org_label, hours=args.hours or 24,
                  pins=load_pins(args.people))
    with open(args.out, "w") as fh:
        fh.write(html)
    size = len(html.encode("utf-8"))
    print(f"Wrote {args.out} ({size} bytes).")
    for n in oversized_style_blocks(html):
        print(f"::warning::A <style> block is {n} characters; Gmail drops blocks over 8,192.")
    if size > MAX_EMAIL_BYTES:
        print(f"::warning::Email is {size} bytes; Gmail clips messages near 102KB.")


if __name__ == "__main__":
    main()
