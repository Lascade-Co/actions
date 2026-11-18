"""Offline checks for the Play publisher's optional native debug symbols."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import publish_playstore as play


def uploads(monkeypatch):
    calls = []
    monkeypatch.setattr(play, 'http_upload_octet',
                        lambda method, url, token, path: calls.append((url, path)) or {'versionCode': 7})
    return calls


def test_symbols_upload_when_present(monkeypatch):
    calls = uploads(monkeypatch)
    assert play.upload_bundle_and_symbols('t', 'e', 'com.x', 'a.aab', 'native-debug-symbols.zip') == '7'
    assert [path for _, path in calls] == ['a.aab', 'native-debug-symbols.zip']
    assert 'deobfuscationFiles/nativeCode' in calls[1][0]


def test_no_symbols_uploads_only_the_bundle(monkeypatch):
    # Kotlin Toolchain apps build no native-debug-symbols.zip.
    calls = uploads(monkeypatch)
    assert play.upload_bundle_and_symbols('t', 'e', 'com.x', 'a.aab', '') == '7'
    assert [path for _, path in calls] == ['a.aab']


def test_native_symbols_argument_is_optional():
    source = (Path(__file__).with_name('publish_playstore.py')).read_text()
    assert 'ap.add_argument("--native-symbols", default="")' in source
