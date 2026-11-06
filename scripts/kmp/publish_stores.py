#!/usr/bin/env python3
"""Decide whether one store publisher acts on a publish dispatch; write `selected` and `pr_number` outputs.

The client_payload may carry `stores`, a JSON array of distinct "android"/"ios" strings (possibly empty).
An absent key selects both stores, so older dispatches (`publish-playstore` from other apps, the
`publish-appstore` retrigger) keep publishing. `pr_number`, when present, is the positive JSON integer of
the source PR whose labels chose the stores. Every other shape fails, whatever the event type.
"""
import argparse
import json
from pathlib import Path

STORES = ('android', 'ios')


def select(payload, store):
    """Return (selected, pr_number) for `store`; pr_number is '' when absent. Raises ValueError on bad shapes."""
    if store not in STORES:
        raise ValueError(f'store must be one of {", ".join(STORES)}')
    if not isinstance(payload, dict):
        raise ValueError('client_payload must be a JSON object')
    pr_number = payload.get('pr_number')
    if 'pr_number' in payload and (type(pr_number) is not int or pr_number < 1):
        raise ValueError('pr_number must be a positive JSON integer when present')
    if 'stores' not in payload:
        selected = True
    else:
        stores = payload['stores']
        if not isinstance(stores, list) or any(type(entry) is not str or entry not in STORES for entry in stores):
            raise ValueError('stores must be a JSON array of "android" and/or "ios" when present')
        if len(set(stores)) != len(stores):
            raise ValueError('stores must not repeat a store')
        selected = store in stores
    return selected, '' if pr_number is None else str(pr_number)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--event', required=True, help='GitHub event JSON ($GITHUB_EVENT_PATH)')
    parser.add_argument('--store', required=True, choices=STORES, help='the store this publisher owns')
    parser.add_argument('--output', required=True, help='file to append selected= and pr_number= to ($GITHUB_OUTPUT)')
    args = parser.parse_args()
    event = json.loads(Path(args.event).read_text())
    selected, pr_number = select(event.get('client_payload'), args.store)
    with Path(args.output).open('a') as stream:
        stream.write(f'selected={"true" if selected else "false"}\npr_number={pr_number}\n')
    print(f'{args.store}: {"selected" if selected else "not selected; publishing is skipped"}')


if __name__ == '__main__':
    main()
