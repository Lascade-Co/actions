#!/usr/bin/env python3
"""Pure HTML renderer for the MarineRadar Web Revenue Digest email.

Faithfully ports the design from the Apps Script version. No I/O — takes a model
dict, returns (subject, html).
"""
from html import escape
from revenue_model import format_money

F = "'Inter',system-ui,-apple-system,'Segoe UI',Roboto,'Helvetica Neue',sans-serif"
FH = "'Inter',system-ui,-apple-system,'Segoe UI',sans-serif"
STRIPE_DASHBOARD = "https://dashboard.stripe.com/payments"


def render(model):
    """Return (subject_line, html_body) from a model dict."""
    m = model
    subject = (
        f"MarineRadar Web Revenue Digest — "
        f"${format_money(m['today_total'])} "
        f"({m['today_count']} purchase{'s' if m['today_count'] != 1 else ''})"
    )

    preheader = (
        f"${format_money(m['today_total'])} revenue today "
        f"({m['today_count']} purchases) · "
        f"${format_money(m['month_total'])} MTD"
    )

    h = []
    _a = h.append

    # Head
    _a('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
       '"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">')
    _a('<html xmlns="http://www.w3.org/1999/xhtml" lang="en" dir="ltr"><head>')
    _a('<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>')
    _a('<meta http-equiv="X-UA-Compatible" content="IE=edge"/>')
    _a('<meta name="color-scheme" content="light"/><meta name="supported-color-schemes" content="light"/>')
    _a('<meta name="format-detection" content="telephone=no,date=no,address=no,email=no"/>')
    _a('<title>MarineRadar Web Revenue</title>')
    _a('<!--[if mso]><noscript><xml><o:OfficeDocumentSettings><o:AllowPNG/>'
       '<o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml>'
       '</noscript><![endif]-->')
    _a('<!--[if !mso]><!--><style>@import url("https://fonts.googleapis.com/'
       'css2?family=Inter:wght@400;500;600;700&display=swap");</style><!--<![endif]-->')
    _a('<!--[if mso]><style>body,table,td,th,p,li,a,span{font-family:"Segoe UI",'
       'Arial,sans-serif!important;}h1,h2,h3{font-family:"Segoe UI",Arial,'
       'sans-serif!important;}</style><![endif]-->')
    _a('<style>')
    _a('#outlook a{padding:0}'
       'body{margin:0!important;padding:0!important;width:100%!important;'
       '-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%}'
       'table{border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt}'
       'td{border-collapse:collapse}p{margin:0;Margin:0}a{color:#222}'
       '.ExternalClass{width:100%}'
       '.ExternalClass,.ExternalClass p,.ExternalClass span,.ExternalClass font,'
       '.ExternalClass td,.ExternalClass div{line-height:100%}'
       'a[x-apple-data-detectors]{color:inherit!important;text-decoration:none!important}')
    _a('@media only screen and (max-width:480px){'
       '.email-container{width:100%!important;max-width:100%!important}'
       '.body-cell{padding-left:16px!important;padding-right:16px!important}'
       'h1.email-h1{font-size:24px!important}'
       'h2.email-h2{font-size:17px!important}'
       '.stat-col{display:block!important;width:100%!important;max-width:100%!important;'
       'padding:0 0 8px 0!important}'
       '.data-table td,.data-table th{font-size:12px!important;padding:6px 8px!important}}')
    _a('</style></head>')

    # Body open
    _a('<body style="margin:0;padding:0;background-color:#f5f3f1;width:100%;">')
    _a(f'<div style="display:none;font-size:1px;color:#f5f3f1;line-height:1px;'
       f'max-height:0;max-width:0;opacity:0;overflow:hidden;mso-hide:all;">'
       f'{escape(preheader)}&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;</div>')
    _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       'width="100%" style="background-color:#f5f3f1;"><tr><td align="center" '
       'valign="top" style="padding:20px 10px;">')
    _a('<!--[if mso]><table role="presentation" cellspacing="0" cellpadding="0" '
       'border="0" width="600" align="center"><tr><td><![endif]-->')
    _a('<div style="max-width:600px;width:100%;margin:0 auto;" class="email-container">')

    # --- Header ---
    _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       'width="100%" style="background-color:#f5f3f1;">')
    _a(f'<tr><td style="padding:40px 40px 10px 40px;text-align:left;" class="body-cell">'
       f'<p style="margin:0;font-family:{FH};font-size:12px;font-weight:600;'
       f'letter-spacing:0.08em;text-transform:uppercase;color:#888;">'
       f'Daily Revenue Digest</p></td></tr>')
    _a(f'<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
       f'<h1 class="email-h1" style="margin:0;font-family:{FH};font-size:28px;'
       f'font-weight:700;line-height:1.15;letter-spacing:-0.02em;color:#222;">'
       f'MarineRadar Web Revenue Digest</h1></td></tr>')
    _a(f'<tr><td style="padding:8px 40px 0 40px;" class="body-cell">'
       f'<p style="margin:0;font-family:{F};font-size:14px;line-height:1.5;color:#888;">'
       f'{escape(m["date_str"])}</p></td></tr>')
    _a(f'<tr><td style="padding:4px 40px 0 40px;" class="body-cell">'
       f'<p style="margin:0;font-family:{F};font-size:12px;line-height:1.5;color:#aaa;">'
       f'Since {escape(m["cutoff_str"])}</p></td></tr>')
    _a('<tr><td style="padding:20px 40px 0 40px;" class="body-cell">'
       '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
       '<tr><td style="border-top:2px solid #222;font-size:1px;line-height:1px;">'
       '&nbsp;</td></tr></table></td></tr>')
    _a('</table>')

    # --- Today summary: two stat cards ---
    _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       'width="100%" style="background-color:#f5f3f1;">')
    _a('<tr><td style="padding:24px 40px 0 40px;" class="body-cell">')
    _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"><tr>')

    # Purchases card
    _a(f'<td width="48%" style="vertical-align:top;" class="stat-col">'
       f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       f'width="100%" style="background-color:#fff;border-radius:6px;">'
       f'<tr><td style="padding:18px 20px;text-align:center;">'
       f'<p style="margin:0;font-family:{FH};font-size:11px;font-weight:600;'
       f'letter-spacing:0.08em;text-transform:uppercase;color:#888;">Purchases</p>'
       f'<p style="margin:6px 0 0 0;font-family:{FH};font-size:30px;font-weight:700;'
       f'line-height:1.1;letter-spacing:-0.02em;color:#2a8c4a;">'
       f'${format_money(m["today_total"])}</p>'
       f'<p style="margin:4px 0 0 0;font-family:{FH};font-size:12px;color:#888;">'
       f'{m["today_count"]} transaction{"s" if m["today_count"] != 1 else ""}</p>'
       f'</td></tr></table></td>')

    _a('<td width="4%">&nbsp;</td>')

    # Refunds card
    _a(f'<td width="48%" style="vertical-align:top;" class="stat-col">'
       f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       f'width="100%" style="background-color:#fff;border-radius:6px;">'
       f'<tr><td style="padding:18px 20px;text-align:center;">'
       f'<p style="margin:0;font-family:{FH};font-size:11px;font-weight:600;'
       f'letter-spacing:0.08em;text-transform:uppercase;color:#888;">Refunded</p>')
    if m["today_refund_count"] > 0:
        _a(f'<p style="margin:6px 0 0 0;font-family:{FH};font-size:30px;font-weight:700;'
           f'line-height:1.1;letter-spacing:-0.02em;color:#c0392b;">'
           f'${format_money(m["today_refund_total"])}</p>'
           f'<p style="margin:4px 0 0 0;font-family:{FH};font-size:12px;color:#888;">'
           f'{m["today_refund_count"]} refund{"s" if m["today_refund_count"] != 1 else ""}</p>')
    else:
        _a(f'<p style="margin:6px 0 0 0;font-family:{FH};font-size:30px;font-weight:700;'
           f'line-height:1.1;letter-spacing:-0.02em;color:#888;">$0.00</p>'
           f'<p style="margin:4px 0 0 0;font-family:{FH};font-size:12px;color:#aaa;">'
           f'No refunds</p>')
    _a('</td></tr></table></td>')
    _a('</tr></table></td></tr></table>')

    # Purchase Source Breakdown — commented out until metadata property is configured
    # TODO: Uncomment when purchase_source is set in Stripe metadata
    # if m.get("sources"):
    #     _a(_render_source_table(m["sources"]))

    # --- Credit Pack Breakdown ---
    if m["packs"]:
        _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
           'width="100%" style="background-color:#f5f3f1;">')
        _a(f'<tr><td style="padding:28px 40px 0 40px;" class="body-cell">'
           f'<h2 class="email-h2" style="margin:0;font-family:{FH};font-size:18px;'
           f'font-weight:700;line-height:1.3;color:#222;">Credit Pack Breakdown</h2>'
           f'</td></tr>')
        _a(f'<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
           f'<table class="data-table" role="presentation" cellspacing="0" cellpadding="0" '
           f'border="0" width="100%" style="background-color:#fff;border-radius:6px;'
           f'font-family:{F};font-size:13px;line-height:1.4;color:#222;">')
        # Header row
        th_style = ('padding:10px 14px;font-size:11px;font-weight:600;text-transform:uppercase;'
                    'letter-spacing:0.05em;color:#888;border-bottom:1px solid #e8e6e3')
        _a(f'<tr><th style="{th_style};text-align:left;">Pack</th>'
           f'<th style="{th_style};text-align:right;">Count</th>'
           f'<th style="{th_style};text-align:right;">Total</th></tr>')
        for i, pack in enumerate(m["packs"]):
            stripe = " background-color:#faf9f7;" if i % 2 == 1 else ""
            is_last = i == len(m["packs"]) - 1
            bl = "border-radius:0 0 0 6px;" if is_last else ""
            br = "border-radius:0 0 6px 0;" if is_last else ""
            row_style = f' style="{stripe}"' if stripe else ""
            _a(f'<tr{row_style}>'
               f'<td style="padding:8px 14px;font-weight:600;{bl}">${pack["amount"]}</td>'
               f'<td style="padding:8px 14px;text-align:right;">{pack["count"]}</td>'
               f'<td style="padding:8px 14px;text-align:right;font-weight:600;{br}">'
               f'${format_money(pack["total"])}</td></tr>')
        _a('</table></td></tr></table>')

    # --- Month to Date ---
    _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       'width="100%" style="background-color:#f5f3f1;">')
    _a(f'<tr><td style="padding:28px 40px 0 40px;" class="body-cell">'
       f'<h2 class="email-h2" style="margin:0;font-family:{FH};font-size:18px;'
       f'font-weight:700;line-height:1.3;color:#222;">Month to Date &nbsp;'
       f'<span style="font-weight:400;font-size:14px;color:#888;">'
       f'{escape(m["month_label"])}</span></h2></td></tr>')
    _a(f'<tr><td style="padding:10px 40px 0 40px;" class="body-cell">'
       f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       f'width="100%" style="background-color:#fff;border-radius:6px;">'
       f'<tr><td style="padding:16px 24px;">'
       f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       f'width="100%" style="font-family:{FH};font-size:14px;color:#222;">')
    _a(f'<tr><td style="padding:6px 0;color:#888;">Total Revenue</td>'
       f'<td style="padding:6px 0;text-align:right;font-weight:700;font-size:16px;'
       f'color:#2a8c4a;">${format_money(m["month_total"])}</td></tr>')
    _a(f'<tr><td style="padding:6px 0;color:#888;">Total Refunded</td>'
       f'<td style="padding:6px 0;text-align:right;font-weight:700;font-size:16px;'
       f'color:#c0392b;">&minus;${format_money(m["month_refund_total"])}</td></tr>')
    _a('<tr><td colspan="2" style="padding:4px 0;">'
       '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
       '<tr><td style="border-top:1px solid #e8e6e3;font-size:1px;line-height:1px;">'
       '&nbsp;</td></tr></table></td></tr>')
    _a(f'<tr><td style="padding:6px 0;font-weight:600;">Net Revenue</td>'
       f'<td style="padding:6px 0;text-align:right;font-weight:700;font-size:18px;">'
       f'${format_money(m["month_net"])}</td></tr>')
    _a(f'<tr><td style="padding:4px 0;color:#aaa;font-size:12px;">Transactions</td>'
       f'<td style="padding:4px 0;text-align:right;color:#888;font-size:12px;">'
       f'{m["month_count"]} purchases &middot; {m["month_refund_count"]} refunds</td></tr>')
    _a('</table></td></tr></table></td></tr></table>')

    # --- Footer ---
    _a('<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
       'width="100%" style="background-color:#f5f3f1;">')
    _a('<tr><td style="padding:28px 40px 0 40px;" class="body-cell">'
       '<table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">'
       '<tr><td style="border-top:2px solid #222;font-size:1px;line-height:1px;">'
       '&nbsp;</td></tr></table></td></tr>')
    _a(f'<tr><td style="padding:18px 40px 8px 40px;text-align:center;" class="body-cell">'
       f'<table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center">'
       f'<tr><td style="border-radius:4px;background-color:#222;">'
       f'<a href="{STRIPE_DASHBOARD}" target="_blank" style="display:inline-block;'
       f'padding:10px 24px;font-family:{FH};font-size:13px;font-weight:600;color:#fff;'
       f'text-decoration:none;letter-spacing:0.01em;">View in Stripe &#8599;</a>'
       f'</td></tr></table></td></tr>')
    _a(f'<tr><td style="padding:8px 40px 40px 40px;text-align:center;" class="body-cell">'
       f'<p style="margin:0;font-family:{F};font-size:12px;line-height:1.5;color:#aaa;">'
       f'Sent automatically by Lascade &middot; MarineRadar Revenue Digest</p></td></tr>')
    _a('</table>')

    # Close
    _a('</div>')
    _a('<!--[if mso]></td></tr></table><![endif]-->')
    _a('</td></tr></table></body></html>')

    return subject, "".join(h)


# Purchase Source Breakdown renderer — commented out until metadata is configured
# TODO: Uncomment when purchase_source is set in Stripe metadata
# def _render_source_table(sources):
#     ...
