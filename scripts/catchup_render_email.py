"""Render report.json into the daily HTML email. Pure JSON -> HTML.

No LLM and no network, so it can be unit-tested standalone: feed it a sample
report.json and open the result in a browser. The markup follows the cream
"Lascade Daily Report" style guide (table-based, Inter font, Outlook ghost
tables, dark-mode `class=` hooks).

Usage:
    python scripts/catchup_render_email.py --report report.json --out email.html
"""

import argparse
import json
from html import escape

FONT = ("'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, "
        "'Helvetica Neue', sans-serif")

# Per-repo accent palette (tint = emoji-box bg, accent = badge/avatar bg),
# cycled by repo order so each card is visually distinct.
PALETTE = [
    {"tint": "#e3f2fd", "accent": "#1565c0"},
    {"tint": "#e8f5e9", "accent": "#2a8c4a"},
    {"tint": "#e8eaf6", "accent": "#283593"},
    {"tint": "#fff3e0", "accent": "#e65100"},
    {"tint": "#f3e5f5", "accent": "#6a1b9a"},
    {"tint": "#e0f7fa", "accent": "#00695c"},
]

HEAD = """\
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" dir="ltr">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<meta http-equiv="X-UA-Compatible" content="IE=edge" />
<meta name="color-scheme" content="light dark" />
<meta name="supported-color-schemes" content="light dark" />
<title>Lascade Daily Report</title>
<!--[if !mso]><!-->
<style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');</style>
<!--<![endif]-->
<style type="text/css">
  body { margin:0 !important; padding:0 !important; width:100% !important; -webkit-text-size-adjust:100%; -ms-text-size-adjust:100%; }
  table { border-collapse:collapse; mso-table-lspace:0pt; mso-table-rspace:0pt; }
  img { border:0; line-height:100%; outline:none; text-decoration:none; -ms-interpolation-mode:bicubic; }
  p { margin:0; Margin:0; }
  @media only screen and (max-width:480px) {
    .email-container { width:100% !important; max-width:100% !important; }
    .body-cell { padding-left:20px !important; padding-right:20px !important; }
  }
  @media (prefers-color-scheme: dark) {
    .body-bg { background-color:#1a1a18 !important; }
    .email-body-bg { background-color:#222220 !important; }
    .card-bg { background-color:#2a2927 !important; }
    .text-main { color:#e0ddd8 !important; }
    .heading-dm { color:#ece9e4 !important; }
    .text-muted { color:#a8a49e !important; }
    .divider-line { border-color:#3a3835 !important; }
    .chip-dm { background-color:#3a3835 !important; color:#e0ddd8 !important; }
  }
  [data-ogsc] .body-bg { background-color:#1a1a18 !important; }
  [data-ogsc] .email-body-bg { background-color:#222220 !important; }
  [data-ogsc] .card-bg { background-color:#2a2927 !important; }
  [data-ogsc] .text-main { color:#e0ddd8 !important; }
  [data-ogsc] .heading-dm { color:#ece9e4 !important; }
  [data-ogsc] .text-muted { color:#a8a49e !important; }
</style>
</head>
"""


def esc(value):
    return escape(str(value if value is not None else ""))


def initials(name):
    parts = [p for p in str(name).split(" ") if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def divider():
    return (
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
        'width="100%" style="background-color:#f5f3f1;" class="body-bg email-body-bg">'
        '<tr><td style="padding:20px 40px;" class="body-cell">'
        '<table role="presentation" width="100%"><tr>'
        '<td style="border-top:1px solid #ddd;font-size:1px;line-height:1px;" '
        'class="divider-line">&nbsp;</td></tr></table></td></tr></table>'
    )


def section_open():
    return ('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
            'width="100%" style="background-color:#f5f3f1;" class="body-bg email-body-bg">')


def render_header(report):
    date = esc(report.get("date", ""))
    return (
        section_open() +
        f'<tr><td style="padding:40px 40px 8px 40px;" class="body-cell">'
        f'<p style="margin:0;font-family:{FONT};font-size:12px;font-weight:600;'
        f'letter-spacing:0.08em;text-transform:uppercase;color:#555;" '
        f'class="text-muted">Daily Report</p></td></tr>'
        f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
        f'<h1 style="margin:0;font-family:{FONT};font-size:30px;font-weight:700;'
        f'line-height:1.1;letter-spacing:-0.02em;color:#222;" class="heading-dm">'
        f'Lascade Daily Report</h1></td></tr>'
        f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
        f'<p style="margin:0;font-family:{FONT};font-size:14px;line-height:1.5;'
        f'color:#555;" class="text-muted">{date} · Last 24 hours</p></td></tr>'
        '</table>'
    )


def render_stats(stats):
    cells = [
        (stats.get("commits", 0), "Commits"),
        (stats.get("repos_active", 0), "Repos Active"),
        (stats.get("contributors", 0), "Contributors"),
    ]
    tds = []
    for i, (value, label) in enumerate(cells):
        if i:
            tds.append('<td style="width:4px;"></td>')
        tds.append(
            f'<td style="width:33%;text-align:center;padding:16px 8px;'
            f'background:#fff;" class="card-bg">'
            f'<p style="margin:0;font-family:{FONT};font-size:28px;font-weight:700;'
            f'color:#222;" class="heading-dm">{esc(value)}</p>'
            f'<p style="margin:4px 0 0;font-family:{FONT};font-size:11px;'
            f'font-weight:600;text-transform:uppercase;letter-spacing:0.04em;'
            f'color:#777;" class="text-muted">{esc(label)}</p></td>'
        )
    return (
        section_open() +
        '<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
        'width="100%"><tr>' + "".join(tds) + '</tr></table></td></tr></table>'
    )


def render_summary(text):
    if not text:
        return ""
    return (
        section_open() +
        f'<tr><td style="padding:24px 40px 0 40px;" class="body-cell">'
        f'<h2 style="margin:0;font-family:{FONT};font-size:22px;font-weight:700;'
        f'color:#222;" class="heading-dm">Executive Summary</h2></td></tr>'
        f'<tr><td style="padding:12px 40px 0 40px;" class="body-cell">'
        f'<table role="presentation" width="100%"><tr>'
        f'<td style="width:4px;background-color:#222;"></td>'
        f'<td style="padding:16px 20px;background-color:#f0eeec;" class="card-bg">'
        f'<p style="margin:0;font-family:{FONT};font-size:16px;line-height:1.65;'
        f'color:#222;" class="text-main">{esc(text)}</p>'
        f'</td></tr></table></td></tr></table>'
    )


def render_distribution(repos):
    if not repos:
        return ""
    top = max((r.get("commit_count", 0) for r in repos), default=0)
    if top <= 0:
        return ""
    rows = []
    for r in repos:
        count = r.get("commit_count", 0)
        pct = max(8, round(100 * count / top))
        rows.append(
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
            f'width="100%" style="margin-bottom:6px;"><tr>'
            f'<td style="width:120px;text-align:right;padding-right:12px;'
            f'font-family:{FONT};font-size:13px;font-weight:600;color:#444;" '
            f'class="text-main">{esc(r.get("display_name"))}</td>'
            f'<td style="padding:4px 0;">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
            f'style="width:{pct}%;"><tr>'
            f'<td style="height:24px;background:#3498db;padding-left:8px;'
            f'font-family:{FONT};font-size:12px;font-weight:700;color:#fff;">'
            f'{esc(count)}</td></tr></table></td></tr></table>'
        )
    return (
        section_open() +
        '<tr><td style="padding:24px 40px 8px 40px;" class="body-cell">'
        f'<p style="margin:0 0 12px;font-family:{FONT};font-size:11px;'
        f'font-weight:600;text-transform:uppercase;letter-spacing:0.06em;'
        f'color:#999;" class="text-muted">Commit Distribution</p>'
        + "".join(rows) + '</td></tr></table>'
    )


def render_repo(repo, accent):
    name = esc(repo.get("display_name"))
    emoji = esc(repo.get("emoji") or "📦")

    # Meta line: "N commits · M contributors · K PRs · version".
    bits = [f'{repo.get("commit_count", 0)} commits',
            f'{len(repo.get("contributors", []))} contributors']
    if repo.get("prs_merged"):
        bits.append(f'{repo["prs_merged"]} PRs merged')
    if repo.get("version"):
        bits.append(esc(repo["version"]))
    meta = " · ".join(bits)

    head = (
        section_open() +
        f'<tr><td style="padding:0 40px;" class="body-cell">'
        f'<table role="presentation" cellspacing="0" cellpadding="0" border="0"><tr>'
        f'<td style="width:40px;height:40px;background-color:{accent["tint"]};'
        f'text-align:center;vertical-align:middle;font-size:20px;">{emoji}</td>'
        f'<td style="padding-left:12px;">'
        f'<h2 style="margin:0;font-family:{FONT};font-size:22px;font-weight:700;'
        f'color:#222;display:inline;" class="heading-dm">{name}</h2>&nbsp;'
        f'<span style="display:inline-block;font-family:{FONT};font-size:11px;'
        f'font-weight:600;text-transform:uppercase;letter-spacing:0.05em;'
        f'padding:3px 8px;border-radius:3px;background:{accent["accent"]};'
        f'color:#fff;">Active</span></td></tr></table></td></tr>'
        f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
        f'<p style="margin:0;font-family:{FONT};font-size:14px;color:#555;" '
        f'class="text-muted">{meta}</p></td></tr>'
    )

    blocks = [head]

    # Categorised sections.
    for section in repo.get("sections", []):
        items = section.get("items", [])
        if not items:
            continue
        rows = []
        for item in items:
            suffix = ""
            if item.get("pr"):
                suffix += f' (PR #{esc(item["pr"])})'
            if item.get("author"):
                suffix += (f' <span style="font-family:{FONT};font-size:12px;'
                           f'font-weight:600;color:#777;">— {esc(item["author"])}'
                           f'</span>')
            rows.append(
                f'<tr><td style="padding:2px 0 2px 16px;font-family:{FONT};'
                f'font-size:16px;line-height:1.65;color:#222;" class="text-main">'
                f'• {esc(item.get("text"))}{suffix}</td></tr>'
            )
        blocks.append(
            f'<tr><td style="padding:12px 40px 0 40px;" class="body-cell">'
            f'<h3 style="margin:0 0 8px;font-family:{FONT};font-size:16px;'
            f'font-weight:600;color:#222;" class="heading-dm">'
            f'{esc(section.get("title"))}</h3>'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
            f'width="100%">{"".join(rows)}</table></td></tr>'
        )

    # Contributor avatars row.
    contributors = repo.get("contributors", [])
    if contributors:
        cells = []
        for c in contributors:
            label = "commit" if c.get("commits") == 1 else "commits"
            cells.append(
                f'<td style="width:32px;height:32px;border-radius:50%;'
                f'background-color:{accent["accent"]};text-align:center;'
                f'vertical-align:middle;font-family:{FONT};font-size:11px;'
                f'font-weight:700;color:#fff;">{esc(initials(c.get("name")))}</td>'
                f'<td style="padding-left:8px;padding-right:20px;">'
                f'<p style="margin:0;font-family:{FONT};font-size:13px;'
                f'font-weight:600;color:#222;" class="heading-dm">'
                f'{esc(c.get("name"))}</p>'
                f'<p style="margin:0;font-family:{FONT};font-size:11px;color:#888;" '
                f'class="text-muted">{esc(c.get("commits", 0))} {label}</p></td>'
            )
        blocks.append(
            f'<tr><td style="padding:16px 40px 0 40px;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0">'
            f'<tr>{"".join(cells)}</tr></table></td></tr>'
        )

    # Branch chips.
    branches = repo.get("branches", [])
    if branches:
        chips = "".join(
            f'<span style="display:inline-block;font-family:{FONT};font-size:12px;'
            f'font-weight:500;padding:4px 10px;border-radius:12px;'
            f'background:#e8e6e3;color:#444;margin:2px;" class="chip-dm">'
            f'{esc(b)}</span>' for b in branches
        )
        blocks.append(
            f'<tr><td style="padding:12px 40px 0 40px;" class="body-cell">'
            f'{chips}</td></tr>'
        )

    return "".join(blocks) + '</table>'


def render_patterns(patterns):
    if not patterns:
        return ""
    rows = "".join(
        f'<tr><td style="padding:2px 0 6px 16px;font-family:{FONT};font-size:16px;'
        f'line-height:1.65;color:#222;" class="text-main">• {esc(p)}</td></tr>'
        for p in patterns
    )
    return (
        section_open() +
        f'<tr><td style="padding:0 40px;" class="body-cell">'
        f'<h2 style="margin:0 0 12px;font-family:{FONT};font-size:22px;'
        f'font-weight:700;color:#222;" class="heading-dm">Patterns Worth Noting</h2>'
        f'</td></tr><tr><td style="padding:0 40px;" class="body-cell">'
        f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
        f'width="100%">{rows}</table></td></tr></table>'
    )


def render_footer(date):
    return (
        section_open() +
        f'<tr><td style="padding:32px 40px;font-family:{FONT};font-size:13px;'
        f'font-style:italic;line-height:1.5;color:#555;border-top:1px solid #ddd;" '
        f'class="text-muted divider-line">Generated {esc(date)} · Covers the last '
        f'24 hours · Data from git log</td></tr></table>'
    )


def render(report):
    repos = report.get("repos", [])
    body = [render_header(report), render_stats(report.get("stats", {})),
            render_summary(report.get("executive_summary", "")),
            render_distribution(repos)]
    for i, repo in enumerate(repos):
        body.append(divider())
        body.append(render_repo(repo, PALETTE[i % len(PALETTE)]))
    if report.get("patterns"):
        body.append(divider())
        body.append(render_patterns(report["patterns"]))
    body.append(divider())
    body.append(render_footer(report.get("date", "")))

    inner = "".join(body)
    return (
        HEAD +
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
    parser.add_argument("--out", required=True, help="email.html output")
    args = parser.parse_args()

    with open(args.report) as fh:
        report = json.load(fh)
    html = render(report)
    with open(args.out, "w") as fh:
        fh.write(html)
    print(f"Wrote {args.out} ({len(html)} bytes).")


if __name__ == "__main__":
    main()
