"""Translate the App Store "What's New" notes into the locales an App Store version already has.

Only What's New is localised (promotional text and the rest of the listing are untouched).
English locales keep the shared notes verbatim. Translation uses scripts/ai/claude_api.py
(CLAUDE_API_KEY). It is fail-soft: any locale that cannot be translated, or whose translation
Apple would reject, is omitted so the caller falls back to the English notes. Never truncates.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ai'))

from asc_client import strip_rejected  # noqa: E402

WHATS_NEW_LIMIT = 4000


def is_english(locale):
    return locale.lower().split('-')[0] == 'en'


def prompt(locales, notes):
    return (
        'Translate the mobile app release notes below into each App Store Connect locale listed.\n'
        'Rules: keep the meaning, tone and line breaks; keep product names, brand names and numbers '
        'unchanged; do not add or remove points; plain text only, no emoji or symbols; use the '
        'regional variant implied by the locale code (for example es-MX, pt-BR, zh-Hans, zh-Hant).\n'
        'Return one JSON object mapping each locale code exactly as given to its translated text, '
        'with no other keys and nothing outside the JSON.\n\n'
        f'Locales: {json.dumps(locales)}\n\n<release_notes>\n{notes}\n</release_notes>')


def clean(locales, raw):
    """Keep only valid, non-empty translations that Apple will accept; the rest fall back to English."""
    result = {}
    for locale in locales:
        text = raw.get(locale)
        if not isinstance(text, str):
            print(f"::warning title=What's New localisation::no translation for {locale}; using English")
            continue
        text = '\n'.join(line.rstrip() for line in strip_rejected(text).split('\n')).strip()
        if not text or len(text) > WHATS_NEW_LIMIT:
            print(f"::warning title=What's New localisation::unusable translation for {locale}; using English")
            continue
        result[locale] = text
    return result


def translate(locales, notes, generate_json=None):
    """{locale: translated notes} for the non-English locales; {} on any failure."""
    targets = sorted({locale for locale in locales if not is_english(locale)})
    if not targets:
        return {}
    if generate_json is None:
        from claude_api import generate_json
    try:
        raw = generate_json(prompt(targets, notes))
    except Exception as exc:  # noqa: BLE001 - ClaudeError carries a safe, fixed reason; never block a release
        print(f"::warning title=What's New localisation::translation failed ({exc}); using English everywhere")
        return {}
    return clean(targets, raw)
