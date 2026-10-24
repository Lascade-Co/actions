#!/usr/bin/env python3
"""Send one Telegram build notification (HTML), failing visibly on API errors.

Structured mode (TELEGRAM_KIND set) composes the message here so every untrusted field (branch, PR
title, error text) is HTML-escaped once. Success messages carry the artifact link only; the workflow
run link and a pointer to the failing job, step and first error are added only to failures.
Legacy mode (TELEGRAM_MESSAGE only) still sends plain text.
"""
import html
import json
import os
import re
import urllib.error
import urllib.request

KINDS = {
    'android': {'label': 'Android debug APK', 'jobs': ('unit-tests', 'check', 'android', 'publish-android')},
    'testflight': {'label': 'TestFlight', 'jobs': ('unit-tests', 'check', 'ios-build', 'ios-publish')},
}
GENERIC_ERROR = re.compile(r'^Process completed with exit code \d+\.?$')
# A pointer for where to look, not a log: one short annotation per failed job.
MAX_ERRORS = 1
MAX_ERROR_CHARS = 160


def send(token, chat_id, message, opener=urllib.request.urlopen, html_mode=False):
    if not token or not chat_id or not message:
        raise ValueError('Telegram token, chat ID and message are required')
    body = {'chat_id': chat_id, 'text': message, 'link_preview_options': {'is_disabled': True}}
    if html_mode:
        body['parse_mode'] = 'HTML'
    request = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage',
                                     data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
    try:
        with opener(request, timeout=30) as response:
            result = json.load(response)
    except (urllib.error.URLError, TimeoutError):
        # urllib exception strings include the token-bearing URL; never print them.
        raise RuntimeError('Telegram transport failed') from None
    if not result.get('ok'):
        raise RuntimeError(f'Telegram rejected notification (error code {result.get("error_code", "unknown")})')
    print('Telegram notification sent')


def _shorten(text, limit=MAX_ERROR_CHARS):
    text = ' '.join(text.split())
    return text if len(text) <= limit else text[:limit - 1] + '…'


def failure_details(api, repository, run_id, token, jobs, opener=urllib.request.urlopen):
    """Failed jobs, their failed steps and error annotations for this run. Best effort: never raises."""
    def get(path):
        request = urllib.request.Request(f'{api}/repos/{repository}/{path}', headers={
            'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json'})
        with opener(request, timeout=20) as response:
            return json.load(response)
    try:
        listed = get(f'actions/runs/{run_id}/jobs?per_page=100').get('jobs', [])
    except Exception:
        return []
    details = []
    for job in listed:
        if job.get('name') not in jobs or job.get('conclusion') not in ('failure', 'timed_out'):
            continue
        steps = [step.get('name', '') for step in job.get('steps', []) if step.get('conclusion') in ('failure', 'timed_out')]
        errors = []
        try:
            for note in get(f'check-runs/{job["id"]}/annotations?per_page=50'):
                message = (note.get('message') or '').strip()
                if note.get('annotation_level') == 'failure' and message and not GENERIC_ERROR.match(message):
                    errors.append(_shorten(message))
        except Exception:
            pass
        if job.get('conclusion') == 'timed_out':
            errors.insert(0, 'Job timed out')
        details.append({'job': job['name'], 'steps': steps, 'errors': list(dict.fromkeys(errors))[:MAX_ERRORS]})
    return details


def compose(kind, status, fields, details=()):
    """HTML message. status is success, failure or withheld (lint auto-fix pushed; CI retriggers)."""
    e = html.escape
    label = KINDS[kind]['label']
    build = fields.get('build_number')
    name = f'{label} {build}' if kind == 'testflight' and build else label
    header = {'success': f'✅ <b>{e(name)} ready</b>',
              'failure': f'❌ <b>{e(name)} failed</b>',
              'withheld': f'🔧 <b>{e(name)} withheld</b>'}[status]
    source = ' · '.join(part for part in (
        f'<b>{e(fields["repo"])}</b>' if fields.get('repo') else '',
        f'<code>{e(fields["branch"])}</code>' if fields.get('branch') else '') if part)
    lines = [header, source]
    if fields.get('title'):
        title = e(fields['title'])
        lines.append(f'<a href="{e(fields["pr_url"], quote=True)}">{title}</a>' if fields.get('pr_url') else f'<i>{title}</i>')
    lines.append('')
    if status == 'success':
        if kind == 'android' and fields.get('link'):
            lines.append(f'📦 <a href="{e(fields["link"], quote=True)}">Download APK</a>')
        elif kind == 'testflight':
            lines.append('Apple is processing it; internal testers get it automatically.')
    elif status == 'withheld':
        lines.append('Claude pushed an Android Lint auto-fix. CI retriggers on the new commit.')
    else:
        if fields.get('results'):
            lines.append(e(fields['results']))
        for detail in details:
            step = f' › {e(", ".join(detail["steps"]))}' if detail['steps'] else ''
            lines.append(f'• <b>{e(detail["job"])}</b>{step}')
            lines.extend(f'  <code>{e(error)}</code>' for error in detail['errors'])
        if fields.get('run_url'):
            lines.append(f'🔎 <a href="{e(fields["run_url"], quote=True)}">Open workflow run</a>')
    return re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip()


def main():
    env = os.environ
    kind = env.get('TELEGRAM_KIND', '')
    if not kind:
        send(env['TELEGRAM_TOKEN'], env['TELEGRAM_CHAT'], env['TELEGRAM_MESSAGE'])
        return
    if kind not in KINDS or env.get('TELEGRAM_STATUS') not in ('success', 'failure', 'withheld'):
        raise ValueError('Unknown Telegram notification kind or status')
    status = env['TELEGRAM_STATUS']
    fields = {key: env.get('TELEGRAM_' + key.upper(), '') for key in
              ('repo', 'branch', 'title', 'pr_url', 'link', 'build_number', 'results', 'run_url')}
    details = []
    if status == 'failure' and env.get('GITHUB_RUN_ID') and env.get('TELEGRAM_GITHUB_TOKEN'):
        details = failure_details(env.get('GITHUB_API_URL', 'https://api.github.com'), env['GITHUB_REPOSITORY'],
                                  env['GITHUB_RUN_ID'], env['TELEGRAM_GITHUB_TOKEN'], KINDS[kind]['jobs'])
    send(env['TELEGRAM_TOKEN'], env['TELEGRAM_CHAT'], compose(kind, status, fields, details), html_mode=True)


if __name__ == '__main__':
    main()
