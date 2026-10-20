#!/usr/bin/env python3
"""Pure HTML renderer for the Reviews Digest email.

Faithfully ports the email design from MarineRadarReviews.gs:
Inter font, #f5f3f1 background, 600px container, table-based layout.
"""
from html import escape as esc

FONT = "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"


def render(model):
    """Render the reviews digest email.

    Args:
        model: dict from reviews_model.build_model()

    Returns:
        (subject_line, html_body) tuple
    """
    m = model
    subject = f"Reviews — {m['date_str']}"
    html = _doc_open(m)
    html += _header(m)
    html += _rating_cards(m["apps"])
    for app in m["apps"]:
        if app["count"] == 0:
            html += _app_header(app["name"], 0)
            html += _empty_card("No new reviews")
        else:
            html += _app_header(app["name"], app["count"])
            if app["appstore_reviews"]:
                html += _store_label("App Store", len(app["appstore_reviews"]))
                for r in app["appstore_reviews"]:
                    html += _review_card(r)
            if app["playstore_reviews"]:
                html += _store_label("Play Store", len(app["playstore_reviews"]))
                for r in app["playstore_reviews"]:
                    html += _review_card(r)
    html += _footer()
    html += '</div></td></tr></table></body></html>'
    return subject, html


def _doc_open(m):
    gt = m["grand_total"]
    avg = m["overall_avg"]
    app_count = len(m["apps"])
    preheader = (f"{gt} new review{'s' if gt != 1 else ''} across {app_count} apps"
                 f" · Avg {avg:.1f}★")
    h = ('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
         '"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">'
         '<html xmlns="http://www.w3.org/1999/xhtml" lang="en" dir="ltr">'
         '<head>'
         '<meta charset="utf-8" />'
         '<meta name="viewport" content="width=device-width, initial-scale=1.0" />'
         '<meta http-equiv="X-UA-Compatible" content="IE=edge" />'
         '<meta name="color-scheme" content="light" />'
         '<meta name="supported-color-schemes" content="light" />'
         '<meta name="format-detection" content="telephone=no, date=no, address=no, email=no" />'
         '<title>Reviews</title>'
         "<style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');</style>"
         '<style>'
         '#outlook a { padding: 0; }'
         'body { margin: 0 !important; padding: 0 !important; width: 100% !important; -webkit-text-size-adjust: 100%; }'
         'table { border-collapse: collapse; }'
         'a { color: #222222; }'
         '.ExternalClass, .ExternalClass p, .ExternalClass span, .ExternalClass td, .ExternalClass div { line-height: 100%; }'
         '@media only screen and (max-width: 480px) {'
         '  .email-container { width: 100% !important; max-width: 100% !important; }'
         '  .body-cell { padding-left: 16px !important; padding-right: 16px !important; }'
         '  h1.email-h1 { font-size: 24px !important; }'
         '  h2.email-h2 { font-size: 17px !important; }'
         '  h3.email-h3 { font-size: 15px !important; }'
         '}'
         '</style></head>'
         '<body style="margin:0; padding:0; background-color:#f5f3f1; width:100%;">'
         f'<div style="display:none; font-size:1px; color:#f5f3f1; line-height:1px; '
         f'max-height:0px; max-width:0px; opacity:0; overflow:hidden;">{esc(preheader)}</div>'
         '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
         'style="background-color:#f5f3f1;">'
         '<tr><td align="center" valign="top" style="padding:20px 10px;">'
         '<div style="max-width:600px; width:100%; margin:0 auto;" class="email-container">')
    return h


def _header(m):
    gt = m["grand_total"]
    chips = ""
    for name in m["app_names"]:
        chips += (f'<span style="display:inline-block; font-family:{FONT}; font-size:12px; '
                  f'font-weight:600; color:#555; background:#e8e6e3; padding:3px 10px; '
                  f'border-radius:12px; margin-right:4px;">{esc(name)}</span>')
    count_label = f'{gt} review{"s" if gt != 1 else ""}'
    h = (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
         f'style="background-color:#f5f3f1;">'
         f'<tr><td style="padding:40px 40px 10px 40px;" class="body-cell">'
         f'<p style="margin:0; font-family:{FONT}; font-size:12px; font-weight:600; '
         f'letter-spacing:0.08em; text-transform:uppercase; color:#888888;">Daily Review Digest</p>'
         f'</td></tr>'
         f'<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
         f'<h1 class="email-h1" style="margin:0; font-family:{FONT}; font-size:28px; '
         f'font-weight:700; line-height:1.15; letter-spacing:-0.02em; color:#222222;">Reviews</h1>'
         f'</td></tr>'
         f'<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
         f'<p style="margin:0;">{chips}</p></td></tr>'
         f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
         f'<p style="margin:0; font-family:{FONT}; font-size:14px; line-height:1.5; color:#888888;">'
         f'{esc(m["date_str"])} &nbsp;&middot;&nbsp; '
         f'<span style="display:inline-block; background:#222222; color:#ffffff; font-size:12px; '
         f'font-weight:600; padding:2px 8px; border-radius:10px;">{count_label}</span>'
         f'</p></td></tr>'
         f'<tr><td style="padding:4px 40px 0 40px;" class="body-cell">'
         f'<p style="margin:0; font-family:{FONT}; font-size:12px; line-height:1.5; '
         f'color:#aaaaaa;">Since {esc(m["since_str"])}</p>'
         f'</td></tr>'
         f'<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
         f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
         f'<tr><td style="border-top:2px solid #222222; font-size:1px; line-height:1px;">'
         f'&nbsp;</td></tr></table>'
         f'</td></tr></table>')
    return h


def _rating_cards(apps):
    cells = ""
    for i, app in enumerate(apps):
        if i > 0:
            cells += '<td style="width:8px;">&nbsp;</td>'
        if app["avg_rating"] >= 4:
            color = "#2a8c4a"
        elif app["avg_rating"] >= 3:
            color = "#c9922a"
        elif app["avg_rating"] > 0:
            color = "#c0392b"
        else:
            color = "#aaa"
        cells += (f'<td style="text-align:center; padding:14px 8px; background:#ffffff; '
                  f'border-radius:6px;">'
                  f'<p style="margin:0; font-family:{FONT}; font-size:11px; font-weight:600; '
                  f'color:#555;">{esc(app["name"])}</p>')
        if app["count"] > 0:
            cells += (f'<p style="margin:4px 0 0 0; font-family:{FONT}; font-size:20px; '
                      f'font-weight:700; color:{color};">{app["avg_rating"]:.1f} ★</p>'
                      f'<p style="margin:2px 0 0 0; font-family:{FONT}; font-size:11px; '
                      f'color:#888;">{app["count"]} review{"s" if app["count"] != 1 else ""}</p>')
        else:
            cells += (f'<p style="margin:4px 0 0 0; font-family:{FONT}; font-size:13px; '
                      f'color:#aaa;">No reviews</p>')
        cells += '</td>'
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#f5f3f1;">'
            f'<tr><td style="padding:24px 40px 0 40px;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
            f'<tr>{cells}</tr></table></td></tr></table>')


def _app_header(name, count):
    count_label = f'{count} review{"s" if count != 1 else ""}'
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#f5f3f1;">'
            f'<tr><td style="padding:28px 40px 0 40px;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
            f'<tr><td style="border-top:1px solid #ddd; font-size:1px; line-height:1px;">'
            f'&nbsp;</td></tr></table>'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="margin-top:16px;"><tr>'
            f'<td><h2 class="email-h2" style="margin:0; font-family:{FONT}; font-size:20px; '
            f'font-weight:700; line-height:1.3; color:#222222;">{esc(name)}</h2></td>'
            f'<td style="text-align:right; white-space:nowrap; vertical-align:bottom;">'
            f'<span style="font-family:{FONT}; font-size:12px; font-weight:600; '
            f'color:#888888;">{count_label}</span></td>'
            f'</tr></table></td></tr></table>')


def _store_label(store_name, count):
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#f5f3f1;">'
            f'<tr><td style="padding:12px 40px 0 40px;" class="body-cell">'
            f'<p style="margin:0; font-family:{FONT}; font-size:12px; font-weight:600; '
            f'letter-spacing:0.05em; text-transform:uppercase; color:#888888;">'
            f'{esc(store_name)} &nbsp;&middot;&nbsp; {count}</p>'
            f'</td></tr></table>')


def _review_card(r):
    rating = r.get("rating", 0)
    if rating >= 4:
        star_color = "#2a8c4a"
    elif rating >= 3:
        star_color = "#c9922a"
    else:
        star_color = "#c0392b"
    filled = "★" * rating
    empty = "★" * (5 - rating)

    stars = f'<span style="font-size:14px; font-weight:700; color:{star_color};">{filled}</span>'
    if empty:
        stars += f'<span style="font-size:14px; color:#ddd;">{empty}</span>'
    title = r.get("title", "")
    if title:
        stars += (f'<span style="font-size:13px; font-weight:600; color:#222; '
                  f'padding-left:6px;">{esc(title)}</span>')

    meta = ""
    if r.get("store") == "appstore":
        meta = f'<span style="font-size:11px; color:#aaa;">{esc(r.get("country", ""))}</span>'
    else:
        parts = [p for p in [r.get("device", ""), r.get("os_version", "")] if p]
        meta = f'<span style="font-size:11px; color:#aaa;">{esc(" · ".join(parts))}</span>'

    footer_parts = []
    if r.get("is_non_english"):
        footer_parts.append('<span style="font-style:italic; color:#bbb;">Non-English</span>')
    footer_parts.append(esc(r.get("author", "")))
    if r.get("version"):
        footer_parts.append(esc(r["version"]))
    footer = " &nbsp;&middot;&nbsp; ".join(footer_parts)

    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#f5f3f1;">'
            f'<tr><td style="padding:6px 40px 0 40px;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#ffffff; border-radius:6px; font-family:{FONT};">'
            f'<tr><td style="padding:14px 18px;">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
            f'<tr><td>{stars}</td>'
            f'<td style="text-align:right; white-space:nowrap;">{meta}</td></tr></table>'
            f'<p style="margin:6px 0 0 0; font-size:13px; line-height:1.45; color:#444;">'
            f'{esc(r.get("body", ""))}</p>'
            f'<p style="margin:6px 0 0 0; font-size:11px; color:#aaa;">{footer}</p>'
            f'</td></tr></table></td></tr></table>')


def _empty_card(message):
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#f5f3f1;">'
            f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#ffffff; border-radius:6px;">'
            f'<tr><td style="padding:18px 24px; text-align:center;">'
            f'<p style="margin:0; font-family:{FONT}; font-size:13px; color:#aaa;">'
            f'{esc(message)}</p>'
            f'</td></tr></table></td></tr></table>')


def _footer():
    return (f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
            f'style="background-color:#f5f3f1;">'
            f'<tr><td style="padding:28px 40px 0 40px;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
            f'<tr><td style="border-top:2px solid #222222; font-size:1px; line-height:1px;">'
            f'&nbsp;</td></tr></table></td></tr>'
            f'<tr><td style="padding:18px 40px 8px 40px; text-align:center;" class="body-cell">'
            f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center">'
            f'<tr>'
            f'<td style="border-radius:4px; background-color:#222222;">'
            f'<a href="https://appstoreconnect.apple.com" target="_blank" '
            f'style="display:inline-block; padding:10px 24px; font-family:{FONT}; font-size:13px; '
            f'font-weight:600; color:#ffffff; text-decoration:none;">App Store Connect &#8599;</a>'
            f'</td>'
            f'<td style="width:10px;">&nbsp;</td>'
            f'<td style="border-radius:4px; background-color:#222222;">'
            f'<a href="https://play.google.com/console" target="_blank" '
            f'style="display:inline-block; padding:10px 24px; font-family:{FONT}; font-size:13px; '
            f'font-weight:600; color:#ffffff; text-decoration:none;">Play Console &#8599;</a>'
            f'</td>'
            f'</tr></table></td></tr>'
            f'<tr><td style="padding:8px 40px 40px 40px; text-align:center;" class="body-cell">'
            f'<p style="margin:0; font-family:{FONT}; font-size:12px; line-height:1.5; '
            f'color:#aaaaaa;">Sent automatically by Lascade &middot; Reviews Digest</p>'
            f'</td></tr></table>')
