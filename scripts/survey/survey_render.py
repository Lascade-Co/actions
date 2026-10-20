#!/usr/bin/env python3
"""Pure HTML renderer for the Survey Digest email.

Product-agnostic: reads product_name / email_title from the model.
Faithfully ports the HTML from the GS scripts — same structure, same inline
styles, same Outlook conditionals. No I/O, no clock, no network.
"""
from html import escape as _esc

F = "'Inter',system-ui,-apple-system,'Segoe UI',Roboto,'Helvetica Neue',sans-serif"
FH = "'Inter',system-ui,-apple-system,'Segoe UI',sans-serif"


def render(model):
    """Return (subject, html)."""
    total = model["total"]
    sections = model["sections"]
    date_str = model["date_str"]
    cutoff_str = model["cutoff_str"]
    sheet_url = model["sheet_url"]
    product_name = model["product_name"]
    email_title = model["email_title"]

    subject = f"{product_name} Surveys — {total} new response{'s' if total != 1 else ''}"
    preheader = f"{total} new survey response{'s' if total != 1 else ''}"

    h = ""

    # ── Head ──
    h += '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
    h += '"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">'
    h += '<html xmlns="http://www.w3.org/1999/xhtml" lang="en" dir="ltr"><head>'
    h += '<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>'
    h += '<meta http-equiv="X-UA-Compatible" content="IE=edge"/>'
    h += '<meta name="color-scheme" content="light"/><meta name="supported-color-schemes" content="light"/>'
    h += '<meta name="format-detection" content="telephone=no,date=no,address=no,email=no"/>'
    h += f'<title>{_esc(email_title)}</title>'
    h += '<!--[if mso]><noscript><xml><o:OfficeDocumentSettings><o:AllowPNG/>'
    h += '<o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript><![endif]-->'
    h += '<!--[if !mso]><!--><style>@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap");</style><!--<![endif]-->'
    h += '<!--[if mso]><style>body,table,td,th,p,li,a,span{font-family:"Segoe UI",Arial,sans-serif!important;}'
    h += 'h1,h2,h3{font-family:"Segoe UI",Arial,sans-serif!important;}</style><![endif]-->'
    h += "<style>"
    h += "#outlook a{padding:0}"
    h += "body{margin:0!important;padding:0!important;width:100%!important;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%}"
    h += "table{border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt}"
    h += "td{border-collapse:collapse}p{margin:0;Margin:0}a{color:#222}"
    h += ".ExternalClass{width:100%}"
    h += ".ExternalClass,.ExternalClass p,.ExternalClass span,.ExternalClass font,"
    h += ".ExternalClass td,.ExternalClass div{line-height:100%}"
    h += "a[x-apple-data-detectors]{color:inherit!important;text-decoration:none!important}"
    h += "@media only screen and (max-width:480px){"
    h += ".email-container{width:100%!important;max-width:100%!important}"
    h += ".body-cell{padding-left:16px!important;padding-right:16px!important}"
    h += "h1.email-h1{font-size:24px!important}"
    h += "h2.email-h2{font-size:17px!important}"
    h += ".data-table td,.data-table th{font-size:12px!important;padding:6px 8px!important}}"
    h += "</style></head>"

    # ── Body open ──
    h += '<body style="margin:0;padding:0;background-color:#f5f3f1;width:100%;">'
    h += '<div style="display:none;font-size:1px;color:#f5f3f1;line-height:1px;'
    h += 'max-height:0;max-width:0;opacity:0;overflow:hidden;mso-hide:all;">'
    h += _esc(preheader) + "&zwnj;&nbsp;" * 4 + "</div>"
    h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" '
    h += 'style="background-color:#f5f3f1;"><tr><td align="center" valign="top" style="padding:20px 10px;">'
    h += '<!--[if mso]><table role="presentation" cellspacing="0" cellpadding="0" border="0" '
    h += 'width="600" align="center"><tr><td><![endif]-->'
    h += '<div style="max-width:600px;width:100%;margin:0 auto;" class="email-container">'

    # ── Header ──
    h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="background-color:#f5f3f1;">'
    h += f'<tr><td style="padding:40px 40px 10px 40px;text-align:left;" class="body-cell">'
    h += f'<p style="margin:0;font-family:{FH};font-size:12px;font-weight:600;letter-spacing:0.08em;'
    h += 'text-transform:uppercase;color:#888;">Daily Survey Digest</p></td></tr>'
    h += f'<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
    h += f'<h1 class="email-h1" style="margin:0;font-family:{FH};font-size:28px;font-weight:700;'
    h += f'line-height:1.15;letter-spacing:-0.02em;color:#222;">{_esc(email_title)}</h1></td></tr>'
    h += f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
    h += f'<p style="margin:0;font-family:{F};font-size:14px;line-height:1.5;color:#888;">'
    h += _esc(date_str) + " &nbsp;&middot;&nbsp; "
    h += '<span style="display:inline-block;background:#222;color:#fff;font-size:12px;font-weight:600;'
    h += f'padding:2px 8px;border-radius:10px;">{total} response{"s" if total != 1 else ""}</span>'
    h += "</p></td></tr>"
    h += f'<tr><td style="padding:4px 40px 0 40px;" class="body-cell">'
    h += f'<p style="margin:0;font-family:{F};font-size:12px;line-height:1.5;color:#aaa;">'
    h += "Since " + _esc(cutoff_str) + "</p></td></tr>"
    h += '<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
    h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
    h += '<tr><td style="border-top:2px solid #222;font-size:1px;line-height:1px;">&nbsp;</td></tr></table></td></tr>'
    h += "</table>"

    # ── Section tables ──
    for si, sec in enumerate(sections):
        pad_top = "24" if si == 0 else "28"
        has_status = sec["has_status"]

        h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="background-color:#f5f3f1;">'
        h += f'<tr><td style="padding:{pad_top}px 40px 0 40px;" class="body-cell">'
        h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"><tr>'
        h += f'<td><h2 class="email-h2" style="margin:0;font-family:{FH};font-size:18px;font-weight:700;'
        h += f'line-height:1.3;color:#222;">{_esc(sec["label"])} &nbsp;'
        h += f'<span style="font-weight:400;font-size:14px;color:#888;">{_esc(sec["subtitle"])}</span></h2></td>'
        h += f'<td style="text-align:right;white-space:nowrap;vertical-align:bottom;">'
        h += f'<span style="font-family:{FH};font-size:12px;font-weight:600;color:#888;">'
        count = sec["count"]
        h += f'{count} response{"s" if count != 1 else ""}</span></td>'
        h += "</tr></table></td></tr>"

        # Data table
        h += '<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
        h += f'<table class="data-table" role="presentation" cellspacing="0" cellpadding="0" border="0" '
        h += f'width="100%" style="background-color:#fff;border-radius:6px;font-family:{F};font-size:13px;line-height:1.4;color:#222;">'

        # Header row
        h += "<tr>"
        if has_status:
            h += _th_cell("Status")
        for col_header in sec["columns"]:
            h += _th_cell(col_header)
        h += "</tr>"

        # Data rows
        rows = sec["rows"]
        for ri, row in enumerate(rows):
            stripe = " background-color:#faf9f7;" if ri % 2 == 1 else ""
            is_last = ri == len(rows) - 1
            h += f'<tr{f" style={chr(34)}{stripe}{chr(34)}" if stripe else ""}>'

            # Status cell (conditional)
            if has_status:
                sl = row.get("status_label") or "Free"
                sc = row.get("status_color") or "#888"
                bl_radius = "border-radius:0 0 0 6px;" if is_last else ""
                h += f'<td style="padding:8px 14px;font-size:12px;font-weight:600;color:{sc};{bl_radius}">'
                h += f"{_esc(sl)}</td>"

            # Data cells
            cells = row["cells"]
            for ci, cell in enumerate(cells):
                last_col = ci == len(cells) - 1
                first_col = ci == 0 and not has_status
                bl = "border-radius:0 0 0 6px;" if is_last and first_col else ""
                br = "border-radius:0 0 6px 0;" if is_last and last_col else ""
                weight = "font-weight:600;" if cell["bold"] else ""
                h += f'<td style="padding:8px 14px;color:{cell["color"]};{weight}{bl}{br}">'
                h += _esc(cell["value"]) + "</td>"

            h += "</tr>"

        h += "</table></td></tr></table>"

    # ── Footer ──
    h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="background-color:#f5f3f1;">'
    h += '<tr><td style="padding:28px 40px 0 40px;" class="body-cell">'
    h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
    h += '<tr><td style="border-top:2px solid #222;font-size:1px;line-height:1px;">&nbsp;</td></tr></table></td></tr>'
    h += '<tr><td style="padding:18px 40px 8px 40px;text-align:center;" class="body-cell">'
    h += '<table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center"><tr>'
    h += '<td style="border-radius:4px;background-color:#222;">'
    h += f'<a href="{_esc(sheet_url)}" target="_blank" style="display:inline-block;padding:10px 24px;'
    h += f'font-family:{FH};font-size:13px;font-weight:600;color:#fff;text-decoration:none;'
    h += 'letter-spacing:0.01em;">View all in Sheets &#8599;</a></td></tr></table></td></tr>'
    h += '<tr><td style="padding:8px 40px 40px 40px;text-align:center;" class="body-cell">'
    h += f'<p style="margin:0;font-family:{F};font-size:12px;line-height:1.5;color:#aaa;">'
    h += f"Sent automatically by Lascade &middot; {_esc(product_name)} Survey Digest</p></td></tr></table>"

    # ── Close ──
    h += "</div>"
    h += "<!--[if mso]></td></tr></table><![endif]-->"
    h += "</td></tr></table></body></html>"

    return subject, h


def _th_cell(text):
    return (
        '<th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:600;'
        'text-transform:uppercase;letter-spacing:0.05em;color:#888;'
        f'border-bottom:1px solid #e8e6e3;">{_esc(text)}</th>'
    )
