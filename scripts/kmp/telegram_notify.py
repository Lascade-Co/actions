#!/usr/bin/env python3
"""Send one plain-text Telegram notification, failing visibly on API errors."""
import json
import os
import urllib.error
import urllib.request


def send(token, chat_id, message, opener=urllib.request.urlopen):
    if not token or not chat_id or not message:
        raise ValueError('Telegram token, chat ID and message are required')
    payload = json.dumps({'chat_id': chat_id, 'text': message,
                          'link_preview_options': {'is_disabled': True}}).encode()
    request = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage',
                                     data=payload, headers={'Content-Type': 'application/json'})
    try:
        with opener(request, timeout=30) as response:
            result = json.load(response)
    except (urllib.error.URLError, TimeoutError) as error:
        # urllib exception strings include the token-bearing URL; never print them.
        raise RuntimeError('Telegram transport failed') from None
    if not result.get('ok'):
        raise RuntimeError(f'Telegram rejected notification (error code {result.get("error_code", "unknown")})')
    print('Telegram notification sent')


if __name__ == '__main__':
    send(os.environ['TELEGRAM_TOKEN'], os.environ['TELEGRAM_CHAT'], os.environ['TELEGRAM_MESSAGE'])
