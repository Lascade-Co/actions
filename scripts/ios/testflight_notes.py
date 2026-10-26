#!/usr/bin/env python3
"""TestFlight "What to Test" notes for central KMP debug builds.

identity IPA        Write the IPA's bundle_id, version and build_number to GITHUB_OUTPUT.
compose ...         Build the notes from GitHub's PR (or commit) JSON; PR text is untrusted data.
publish ...         Wait for the uploaded build to appear in App Store Connect, then create or
                    update its en-US betaBuildLocalization whatsNew, retrying while Apple refuses.

Only publish uses credentials/network (env as in asc_client.py). Notes are never printed.
API: https://developer.apple.com/documentation/appstoreconnectapi/post-v1-betabuildlocalizations
     https://developer.apple.com/documentation/appstoreconnectapi/patch-v1-betabuildlocalizations-_id_
     https://developer.apple.com/documentation/appstoreconnectapi/get-v1-builds-_id_-betabuildlocalizations
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import time
import unicodedata
import urllib.error

from asc_client import ApiError, Client, find_app, find_build, ipa_identity, resource

LIMIT = 4000  # App Store Connect's "What to Test" length limit (characters).
LOCALE = 'en-US'
PR_MARKER = '\n\n\u2026 (truncated; full description on the PR)'
COMMIT_MARKER = '\n\n\u2026 (truncated)'
EMPTY_BODY = '(No PR description.)'
# App Store Connect rejects these in whatsNew ("Text for whatsNew contains invalid characters"):
# every supplementary-plane character (emoji, flags, ZWJ sequences), variation selectors, enclosing
# keycaps, box drawing/blocks, misc symbols/dingbats, braille, misc symbols and arrows, private use
# and U+FFFD. Source: third-party PATCH probe, https://github.com/rorkai/App-Store-Connect-CLI/issues/2754
REJECTED_CLASS = ('[\U00010000-\U0010FFFF\uFE00-\uFE0F\u200D\u20DD-\u20E4\u2500-\u259F'
                  '\u2600-\u27BF\u2800-\u28FF\u2B00-\u2BFF\uE000-\uF8FF\uFFFD]')
REJECTED = re.compile(REJECTED_CLASS)
# A removed symbol that began a line or word takes one following space with it.
REJECTED_WORD = re.compile(r'(^|(?<= ))(?:' + REJECTED_CLASS + ')+ ', re.M)
HTML_COMMENT = re.compile(r'<!--.*?(?:-->|\Z)', re.S)
# Retry these while the fresh build is processing; 400/401/403 and invalid notes text are final.
RETRY_STATUSES = {404, 409, 422, 429, 500, 502, 503, 504}


def clean(text, single_line=False):
    text = HTML_COMMENT.sub('', str(text or '').replace('\r\n', '\n').replace('\r', '\n'))
    text = REJECTED.sub('', REJECTED_WORD.sub(r'\1', text))
    # Controls, invisible format characters and unassigned code points carry no review value.
    text = ''.join(c for c in text if c in '\n\t' or unicodedata.category(c) not in ('Cc', 'Cf', 'Cs', 'Co', 'Cn'))
    if single_line:
        return ' '.join(text.split())
    lines = [line.rstrip() for line in text.split('\n')]
    return re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip('\n').strip()


def cut(text, room):
    """At most `room` characters, preferring a line then word boundary; never strands a combining mark."""
    if len(text) <= room:
        return text
    end = max(room, 0)
    while end and unicodedata.combining(text[end]):
        end -= 1
    head = text[:end]
    for separator, window in (('\n', 400), (' ', 80)):
        index = head.rfind(separator)
        if index >= max(1, end - window):
            head = head[:index]
            break
    return head.rstrip()


def fit(header, body, marker):
    text = header + '\n\n' + body if body else header
    if len(text) <= LIMIT:
        return text
    room = LIMIT - len(header) - 2 - len(marker)
    if room <= 0:
        return cut(header, LIMIT)
    return header + '\n\n' + cut(body, room) + marker


def compose(branch, sha, pull=None, commit=None):
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('Invalid source commit SHA')
    short = sha[:7]
    branch = clean(branch, single_line=True)
    if pull is not None:
        number = int(pull['number'])
        header = '\n'.join([f'PR #{number} \u00b7 {clean(pull.get("title"), single_line=True)}',
                            f'Branch {branch} @ {short}', clean(pull.get('html_url'), single_line=True)])
        return fit(header, clean(pull.get('body')) or EMPTY_BODY, PR_MARKER)
    message = clean((commit or {}).get('commit', {}).get('message'))
    subject = message.split('\n', 1)[0].strip() if message else ''
    return fit(f'Branch {branch} @ {short}', subject, COMMIT_MARKER)


def output(values):
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        for key, value in values.items():
            stream.write(f'{key}={value}\n')


def identity(path):
    bundle, version, number = ipa_identity(path)
    if not re.fullmatch(r'[A-Za-z0-9.-]+', bundle) or not re.fullmatch(r'[0-9]+(\.[0-9]+){0,2}', version) \
            or not re.fullmatch(r'[0-9]+(\.[0-9]+){0,2}', number):
        raise ValueError('Unexpected IPA identity')
    return {'bundle_id': bundle, 'version': version, 'build_number': number}


def final(error):
    if error.status not in RETRY_STATUSES:
        return True
    # Rejected notes text never becomes valid by waiting.
    return any(str(e.get('code', '')).startswith('ENTITY_ERROR.ATTRIBUTE') and 'whatsNew' in str(e.get('detail', ''))
               for e in error.errors)


class Notes:
    def __init__(self, client, sleep=time.sleep, monotonic=time.monotonic, log=print):
        self.client, self.sleep, self.monotonic, self.log = client, sleep, monotonic, log

    def write(self, build, notes):
        existing = [loc for loc in self.client.list(f'builds/{build}/betaBuildLocalizations', {'limit': 200})
                    if loc.get('attributes', {}).get('locale') == LOCALE]
        if existing:
            identifier = existing[0]['id']
            self.client.request('PATCH', f'betaBuildLocalizations/{identifier}', data=resource(
                'betaBuildLocalizations', {'whatsNew': notes}, identifier=identifier))
            return 'updated'
        self.client.request('POST', 'betaBuildLocalizations', data=resource(
            'betaBuildLocalizations', {'locale': LOCALE, 'whatsNew': notes}, {'build': ('builds', build)}))
        return 'created'

    def publish(self, bundle, version, number, notes, timeout):
        deadline = self.monotonic() + timeout
        delay, app, build = 15, None, None
        while True:
            try:
                app = app or find_app(self.client, bundle)
                if build is None:
                    found = find_build(self.client, app, version, number)
                    build = found['id'] if found else None
                if build is not None:
                    action = self.write(build, notes)
                    self.log(f'{action.capitalize()} {LOCALE} What to Test notes ({len(notes)} characters) '
                             f'for {version} ({number})', flush=True)
                    return action
                state = 'not visible yet'
            except ApiError as error:
                if final(error):
                    raise
                state = ' '.join(str(error).split())
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                state = f'transport error {type(error).__name__}'
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                raise TimeoutError(f'Timed out setting What to Test notes for {version} ({number}): {state}')
            self.log(f'Waiting for {version} ({number}): {state}', flush=True)
            self.sleep(min(delay, remaining))
            delay = min(delay * 2, 120)


def workflow_escape(text):
    return text.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    child = sub.add_parser('identity')
    child.add_argument('ipa')
    child = sub.add_parser('compose')
    child.add_argument('--branch', required=True)
    child.add_argument('--sha', required=True)
    source = child.add_mutually_exclusive_group(required=True)
    source.add_argument('--pull', help="GitHub's pulls/{number} JSON")
    source.add_argument('--commit', help="GitHub's commits/{sha} JSON (manual runs)")
    child.add_argument('--output', required=True)
    child = sub.add_parser('publish')
    for name in ('--bundle-id', '--version', '--build-number', '--notes-file'):
        child.add_argument(name, required=True)
    child.add_argument('--timeout', type=int, default=1800, help='Total seconds to wait (default 1800)')
    args = parser.parse_args(argv)
    if args.command == 'identity':
        output(identity(args.ipa))
    elif args.command == 'compose':
        pull = json.loads(Path(args.pull).read_text()) if args.pull else None
        commit = json.loads(Path(args.commit).read_text()) if args.commit else None
        notes = compose(args.branch, args.sha, pull, commit)
        Path(args.output).write_text(notes)
        print(f'Composed {len(notes)} characters of What to Test notes')
    else:
        notes = Path(args.notes_file).read_text()
        if not notes.strip() or len(notes) > LIMIT:
            raise ValueError(f'Notes must contain 1-{LIMIT} characters')
        Notes(Client()).publish(args.bundle_id, args.version, args.build_number, notes, args.timeout)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ApiError, ValueError, OSError, TimeoutError, KeyError) as exc:
        print(f'::error title=TestFlight notes::{workflow_escape(str(exc))}', file=sys.stderr)
        raise SystemExit(1)
