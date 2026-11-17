"""Offline tests for the store-publisher app registry (config/apps.json). Run with pytest."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts' / 'kmp'))
sys.path.insert(0, str(ROOT / 'scripts' / 'ios'))

import apps  # noqa: E402
import publish_appstore  # noqa: E402
import publish_metadata  # noqa: E402

TA = 'Lascade-Co/travel-animator'
RELEASE = {'id': 42, 'tag_name': 'v1.2.3', 'body': 'Notes.'}


def payload(repo, package):
    return {'repo': repo, 'release_id': 42, 'package_name': package}


def test_treasurehunch_bundle_id():
    assert apps.load_app('Lascade-Co/TreasureHunch')['bundle_id'] == 'com.lascade.hunch'


def test_unknown_repo_raises_not_registered():
    with pytest.raises(SystemExit, match='not registered'):
        apps.load_app('Lascade-Co/unknown')


def test_old_shared_name_is_no_longer_registered():
    with pytest.raises(SystemExit, match='not registered'):
        apps.load_app('Lascade-Co/travel-animator-shared')


def test_renamed_travel_animator_is_accepted_with_unchanged_values():
    app = apps.load_app(TA)
    assert app['bundle_id'] == app['package_name'] == 'com.travelanimator.routemap'
    assert app['rollout'] == {'country': 'BD', 'fraction': 0.99}
    assert app['metadata_repo'] == TA


def test_treasurehunch_has_no_metadata_repo():
    assert apps.load_app('Lascade-Co/TreasureHunch')['metadata_repo'] is None


def test_metadata_validate_accepts_registered_repo_and_exposes_registry_values():
    outputs, _ = publish_metadata.validate(payload(TA, 'com.travelanimator.routemap'), RELEASE, 'Lascade-Co/actions')
    assert outputs['bundle_id'] == 'com.travelanimator.routemap'
    assert outputs['metadata_repo'] == TA
    outputs, _ = publish_metadata.validate(payload('Lascade-Co/TreasureHunch', 'com.lascade.hunch'), RELEASE, 'Lascade-Co/actions')
    assert outputs['metadata_repo'] == ''


@pytest.mark.parametrize('repo,package', [
    ('Lascade-Co/unknown', 'com.travelanimator.routemap'),
    (TA, 'com.lascade.hunch'),  # the bundle comes from the registry, never from the payload
    ('Lascade-Co/travel-animator-shared', 'com.travelanimator.routemap'),
])
def test_metadata_validate_rejects_unregistered_or_mismatched(repo, package):
    with pytest.raises(ValueError):
        publish_metadata.validate(payload(repo, package), RELEASE, 'Lascade-Co/actions')


def run_cli(*args, tmp_path):
    out = tmp_path / 'out'
    result = subprocess.run([sys.executable, str(ROOT / 'scripts/kmp/apps.py'), *args, '--output', str(out)],
                            capture_output=True, text=True)
    return result, (out.read_text() if out.exists() else '')


def test_cli_writes_registry_outputs(tmp_path):
    result, out = run_cli('--repo', 'Lascade-Co/TreasureHunch', '--package-name', 'com.lascade.hunch', tmp_path=tmp_path)
    assert result.returncode == 0, result.stderr
    assert out.splitlines() == ['bundle_id=com.lascade.hunch', 'package_name=com.lascade.hunch', 'metadata_repo=',
                                'rollout_country=IN', 'rollout_fraction=0.2', 'native_symbols=false']


def test_native_symbols_required_unless_the_registry_opts_out(tmp_path):
    # Kotlin Toolchain apps build no native-debug-symbols.zip; travel-animator keeps the strict rule on publish-kmp too.
    assert apps.outputs(apps.load_app(TA))['native_symbols'] == 'true'
    assert apps.outputs(apps.load_app('Lascade-Co/TreasureHunch'))['native_symbols'] == 'false'
    result, out = run_cli('--repo', 'Lascade-Co/other-app', '--allow-unregistered', tmp_path=tmp_path)
    assert 'native_symbols=true' in out.splitlines()


def test_play_publisher_keys_optional_symbols_on_the_registry():
    playstore = (ROOT / '.github/workflows/publish-playstore.yml').read_text()
    assert 'NATIVE_SYMBOLS: ${{ steps.app.outputs.native_symbols }}' in playstore
    assert "os.environ['NATIVE_SYMBOLS'] == 'false'" in playstore
    assert "EVENT_TYPE'] == 'publish-kmp' else set()" not in playstore


def test_cli_rejects_payload_package_that_differs_from_registry(tmp_path):
    result, out = run_cli('--repo', TA, '--package-name', 'com.lascade.hunch', tmp_path=tmp_path)
    assert result.returncode != 0 and out == ''


def test_cli_rejects_unregistered_repo(tmp_path):
    result, out = run_cli('--repo', 'Lascade-Co/unknown', tmp_path=tmp_path)
    assert result.returncode != 0 and 'not registered' in result.stderr and out == ''


def test_cli_allow_unregistered_keeps_legacy_play_rollout(tmp_path):
    result, out = run_cli('--repo', 'Lascade-Co/other-app', '--allow-unregistered', tmp_path=tmp_path)
    assert result.returncode == 0, result.stderr
    assert 'rollout_country=BD' in out and 'rollout_fraction=0.99' in out


def test_publish_appstore_has_no_default_bundle_id():
    with pytest.raises(SystemExit):
        publish_appstore.main(['--ipa', 'x.ipa', '--version', '1.0.0', '--notes-file', 'n.txt'])


def workflow(name):
    return (ROOT / '.github/workflows' / name).read_text()


def test_workflows_take_identifiers_from_registry_not_literals():
    appstore, playstore = workflow('publish-appstore.yml'), workflow('publish-playstore.yml')
    for text in (appstore, playstore):
        assert 'travel-animator-shared' not in text
        assert 'com.travelanimator' not in text
        assert 'scripts/kmp/apps.py' in text
    assert "'--bundle-id', os.environ['BUNDLE_ID']" in appstore
    assert 'BUNDLE_ID: ${{ steps.app.outputs.bundle_id }}' in appstore
    assert '${{ steps.app.outputs.rollout_country }}' in playstore
    assert '${{ steps.app.outputs.rollout_fraction }}' in playstore
    assert 'TARGET_COUNTRY' not in playstore and 'TARGET_FRACTION' not in playstore
    # the registry check precedes the first download in each workflow
    assert appstore.index('apps.py') < appstore.index('gh release download')
    assert playstore.index('apps.py') < playstore.index('release-downloader')


def test_registry_file_is_valid_and_complete():
    data = json.loads((ROOT / 'config/apps.json').read_text())
    for entry in data.values():
        assert {'bundle_id', 'package_name', 'metadata_repo', 'rollout', 'project_slug'} <= set(entry)
        assert set(entry) <= {'bundle_id', 'package_name', 'metadata_repo', 'rollout', 'project_slug', 'native_symbols'}
        assert isinstance(entry.get('native_symbols', True), bool)
    # the release workflows read prod signing secrets from this Infisical project, so the payload must match it
    assert data['Lascade-Co/TreasureHunch']['project_slug'] == 'treasurehunch'
    assert data['Lascade-Co/travel-animator']['project_slug'] == 'travelanimator'
