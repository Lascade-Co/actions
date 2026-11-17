#!/usr/bin/env python3
"""File-level helpers for the central Amper (Kotlin Toolchain) debug and release workflows.

Every subcommand prints key=value lines suitable for $GITHUB_OUTPUT. The app repo describes itself
in `.ci/amper.json`; see docs/amper-ci.md.
"""
import argparse
import base64
import glob
import json
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys

MANIFEST = '.ci/amper.json'
APPATTEST_KEY = 'com.apple.developer.devicecheck.appattest-environment'
VERSION_RE = re.compile(r'^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$')
TOKEN_LINE = re.compile(r'^(\s*const val APPCHECK_DEBUG_TOKEN\s*=\s*)".*"\s*$', re.M)


def fail(message):
    sys.exit('error: ' + message)


def manifest(src):
    path = Path(src) / MANIFEST
    if not path.is_file():
        fail(f'{MANIFEST} not found in {src}')
    return json.loads(path.read_text())


def emit(**values):
    for key, value in values.items():
        value = str(value)
        if '\n' in value or '\r' in value:
            fail('multiline output: ' + key)
        print(f'{key}={value}')


def yq(expr, file, env=None):
    result = subprocess.run(['yq', '-i', expr, str(file)], capture_output=True, text=True,
                            env={**os.environ, **(env or {})})
    if result.returncode:
        fail(f'yq failed on {file}: {result.stderr.strip()}')


def yq_get(expr, file):
    result = subprocess.run(['yq', '-r', expr, str(file)], capture_output=True, text=True)
    if result.returncode or result.stdout.strip() in ('', 'null'):
        fail(f'yq could not read {expr} from {file}: {result.stderr.strip()}')
    return result.stdout.strip()


def module_yaml(src, m):
    return Path(src) / m['android_module'] / 'module.yaml'


def pbxproj(src, m):
    return Path(src) / m['ios_project'] / 'project.pbxproj'


def app_config_ids(text):
    """Build configuration ids of the application target(s), via PBXNativeTarget -> XCConfigurationList."""
    ids = []
    for target in re.finditer(r'isa = PBXNativeTarget;.*?\n\t\t\};', text, re.S):
        body = target.group(0)
        if not re.search(r'productType = "?com\.apple\.product-type\.application"?;', body):
            continue
        lst = re.search(r'buildConfigurationList = (\w+)', body).group(1)
        cfgs = re.search(rf'\n\t\t{lst}[^\n]*= \{{.*?buildConfigurations = \((.*?)\);', text, re.S)
        ids += re.findall(r'(\w{12,})', cfgs.group(1))
    if not ids:
        fail('no application target found in the pbxproj')
    return ids


def set_app_settings(text, settings):
    """Set KEY = value; (replace or add) in the app target's build configurations only."""
    for cid in app_config_ids(text):
        match = re.search(rf'(\n\t\t{cid}[^\n]*= \{{\n\t\t\tisa = XCBuildConfiguration;\n\t\t\tbuildSettings = \{{)(.*?)(\n\t\t\t\}};)', text, re.S)
        if not match:
            fail('build configuration not found: ' + cid)
        body = match.group(2)
        for key, value in settings.items():
            line = f'\t\t\t\t{key} = {value};'
            pattern = re.compile(rf'^\t\t\t\t{key} = [^;\n]*;', re.M)
            body = pattern.sub(lambda m: line, body, count=1) if pattern.search(body) else body + '\n' + line
        text = text[:match.start(2)] + body + text[match.end(2):]
    return text


def cmd_metadata(a):
    src = Path(a.source)
    m = manifest(src)
    yml = module_yaml(src, m)
    wrapper = src / 'kotlin'
    found = re.search(r'^kotlin_cli_version=([A-Za-z0-9._+-]+)\s*$', wrapper.read_text(), re.M) if wrapper.is_file() else None
    if not found:
        fail('kotlin_cli_version= not found in the kotlin wrapper')
    build = a.suffix if a.suffix is not None else yq_get('.settings.android.versionCode', yml)
    emit(version=yq_get('.settings.android.versionName', yml), build_number=build, toolchain=found.group(1))


VERSION_LINES = {'versionCode': re.compile(r'^(\s+versionCode:)[ \t]*\S+[ \t]*$', re.M),
                 'versionName': re.compile(r'^(\s+versionName:)[ \t]*\S+[ \t]*$', re.M)}


def set_yaml_version(path, version, build):
    """Rewrite only the versionCode and versionName lines (yq -i reflows the file, and release commits it)."""
    text = path.read_text()
    for key, value in (('versionCode', str(build)), ('versionName', f'"{version}"')):
        if len(VERSION_LINES[key].findall(text)) != 1:
            fail(f'expected exactly one {key} line in {path}')
        text = VERSION_LINES[key].sub(lambda mm: f'{mm.group(1)} {value}', text)
    path.write_text(text)
    if (yq_get('.settings.android.versionName', path), yq_get('.settings.android.versionCode', path)) != (version, str(build)):
        fail(f'{path}: settings.android version did not read back as {version} ({build})')


def cmd_set_version(a):
    if not VERSION_RE.match(a.version):
        fail('invalid version: ' + a.version)
    src = Path(a.source)
    m = manifest(src)
    for rel in m['version_files']:
        set_yaml_version(src / rel, a.version, a.build)
    pbx = pbxproj(src, m)
    pbx.write_text(set_app_settings(pbx.read_text(), {'MARKETING_VERSION': a.version, 'CURRENT_PROJECT_VERSION': a.build}))
    emit(version=a.version, build_number=a.build)


def cmd_set_variant(a):
    src = Path(a.source)
    m = manifest(src)
    ident = m['ids'][a.variant]
    if not re.fullmatch(r'[A-Za-z0-9.-]+', ident):
        fail('invalid bundle id: ' + ident)
    release = a.variant == 'release'
    if bool(a.team) != bool(a.profile):
        fail('--team and --profile go together')
    if a.team and not re.fullmatch(r'[A-Za-z0-9]+', a.team):
        fail('invalid team id')

    yq('.settings.android.applicationId = strenv(ID)', module_yaml(src, m), {'ID': ident})
    pbx = pbxproj(src, m)
    settings = {'PRODUCT_BUNDLE_IDENTIFIER': ident}
    if a.team:
        # Manual signing must live on the target's build settings; command-line overrides did not take.
        settings.update(CODE_SIGN_STYLE='Manual', CODE_SIGN_IDENTITY='"Apple Distribution"', DEVELOPMENT_TEAM=a.team,
                        PROVISIONING_PROFILE_SPECIFIER='"' + a.profile.replace('"', '').replace('\\', '') + '"')
    text = set_app_settings(pbx.read_text(), settings)
    pbx.write_text(text)

    if release:
        debug_plist, release_plist = (src / m['ios_plist'][k] for k in ('debug', 'release'))
        debug_plist.write_bytes(release_plist.read_bytes())
        ent = src / m['ios_entitlements']
        if ent.is_file():
            data = plistlib.loads(ent.read_bytes())
            data[APPATTEST_KEY] = 'production'
            ent.write_bytes(plistlib.dumps(data))
        else:
            print(f'warning: {ent} not found; skipping App Attest entitlement', file=sys.stderr)

    token = '' if release else os.environ.get('APPCHECK_DEBUG_TOKEN', '')
    if not release and not token:
        fail('APPCHECK_DEBUG_TOKEN is unset or empty for the debug variant')
    if re.search(r'["\\$\n\r]', token):
        fail('APPCHECK_DEBUG_TOKEN contains characters unsafe for a Kotlin string')
    kt = src / m['build_secrets_kt']
    body = kt.read_text()
    if not TOKEN_LINE.search(body):
        fail('APPCHECK_DEBUG_TOKEN line not found in ' + m['build_secrets_kt'])
    kt.write_text(TOKEN_LINE.sub(lambda mm: f'{mm.group(1)}"{token}"', body))
    emit(variant=a.variant, bundle_id=ident)


def write_private(path, data):
    path.unlink(missing_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)


def cmd_android_inputs(a):
    src = Path(a.source)
    m = manifest(src)
    mod = src / m['android_module']
    write_private(mod / 'ci.jks', base64.b64decode(os.environ['KEYSTORE_BASE64'], validate=True))
    props = {'storeFile': 'ci.jks', 'storePassword': os.environ['KEYSTORE_PASSWORD'],
             'keyAlias': os.environ['KEY_ALIAS'], 'keyPassword': os.environ['KEY_PASSWORD']}
    for key, value in props.items():
        if '\n' in value or '\r' in value:
            fail('multiline value for ' + key)
    path = mod / 'keystore.properties'
    write_private(path, ''.join(f'{k}={v}\n' for k, v in props.items()).encode())
    emit(keystore='ci.jks')


PATTERNS = {
    # Top level of each task dir only: the toolchain keeps a second copy inside its generated gradle-project.
    'apk': 'build/tasks/*/*.apk',
    'aab': 'build/tasks/*/*.aab',
    'mapping': 'build/tasks/**/outputs/mapping/release/mapping.txt',
}


def task_dir(src, path):
    return Path(path).relative_to(Path(src) / 'build/tasks').parts[0].lower()


def cmd_find_artifact(a):
    matches = sorted(glob.glob(str(Path(a.source) / PATTERNS[a.kind]), recursive=True))
    if a.kind == 'apk':
        # A debug APK from an earlier task can sit next to the release one; the release variant wins.
        release = [m for m in matches if 'release' in Path(m).parent.name.lower()]
        matches = release or matches
    if a.kind == 'mapping' and len(matches) > 1:
        # `kotlin package` (AAB) and `kotlin build -v release` (APK) each run R8. Keep the AAB's mapping, which is
        # what Play and Crashlytics see, and only when both runs agree, so one file deobfuscates both binaries.
        if len({Path(m).read_bytes() for m in matches}) != 1:
            fail(f'the R8 mapping files differ: {matches}')
        matches = [m for m in matches if 'bundle' in task_dir(a.source, m)]
    if len(matches) != 1:
        fail(f'expected exactly one {a.kind} under build/tasks, found {len(matches)}: {matches}')
    emit(path=matches[0])


FIREBASE_APP_ID = re.compile(r'1:[0-9]+:android:[0-9a-f]+')


def cmd_firebase_app_id(a):
    """Release Android app ID for Crashlytics: the manifest's firebase_android_app_id, else the
    google-services.json client whose package is the release ID. Neither fails, unless the manifest opts out
    with "crashlytics_mapping": false (then app_id is empty and the mapping upload is skipped)."""
    src = Path(a.source)
    m = manifest(src)
    if m.get('crashlytics_mapping', True) is False:
        emit(app_id='')
        return
    app = m.get('firebase_android_app_id')
    if app is None:
        services = src / m['android_module'] / 'google-services.json'
        clients = json.loads(services.read_text()).get('client', []) if services.is_file() else []
        found = [c['client_info']['mobilesdk_app_id'] for c in clients
                 if c.get('client_info', {}).get('android_client_info', {}).get('package_name') == m['ids']['release']]
        if len(found) > 1:
            fail('google-services.json has more than one client for ' + m['ids']['release'])
        app = found[0] if found else ''
    if app and not (isinstance(app, str) and FIREBASE_APP_ID.fullmatch(app)):
        fail('invalid Firebase Android app ID')
    if not app:
        fail('no release Firebase Android app ID (set firebase_android_app_id, add the release client to '
             'google-services.json, or opt out with "crashlytics_mapping": false)')
    emit(app_id=app)


FULL_GC = re.compile(r'GC\((\d+)\) Pause Full')


def cmd_gc_guard(a):
    if 'Picked up JAVA_TOOL_OPTIONS' not in Path(a.log).read_text(errors='replace'):
        fail('build log has no "Picked up JAVA_TOOL_OPTIONS"; the JVM flags did not reach the build')
    logs = sorted(Path(a.gc_dir).glob('gc-*.log'))
    if not logs:
        fail('no gc-*.log files in ' + str(a.gc_dir))
    worst = 0
    for log in logs:
        count = sum(1 for line in log.read_text(errors='replace').splitlines() if FULL_GC.search(line))
        worst = max(worst, count)
        if count > a.max_full_gc:
            fail(f'{log.name}: {count} Pause Full GCs (limit {a.max_full_gc})')
    emit(max_full_gc=worst)


def cmd_export_options(a):
    data = {'method': 'app-store-connect', 'teamID': a.team, 'signingStyle': 'manual',
            'provisioningProfiles': {a.bundle_id: a.profile},
            'stripSwiftSymbols': True, 'compileBitcode': False}
    Path(a.out).write_bytes(plistlib.dumps(data))
    emit(path=a.out)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='cmd', required=True)

    def add(name, fn, *args):
        s = sub.add_parser(name)
        s.set_defaults(fn=fn)
        for flag, opts in args:
            s.add_argument(flag, **opts)

    src = ('--source', dict(required=True))
    variant = ('--variant', dict(required=True, choices=['debug', 'release']))
    add('metadata', cmd_metadata, src, ('--suffix', dict(type=int)))
    add('set-version', cmd_set_version, src, ('--version', dict(required=True)), ('--build', dict(required=True, type=int)))
    add('set-variant', cmd_set_variant, src, variant, ('--team', {}), ('--profile', {}))
    add('android-inputs', cmd_android_inputs, src)
    add('find-artifact', cmd_find_artifact, src, ('--kind', dict(required=True, choices=sorted(PATTERNS))))
    add('firebase-app-id', cmd_firebase_app_id, src)
    add('kn-gc-guard', cmd_gc_guard, ('--log', dict(required=True)), ('--gc-dir', dict(required=True)),
        ('--max-full-gc', dict(type=int, default=20)))
    add('export-options', cmd_export_options, ('--team', dict(required=True)),
        ('--profile', dict(required=True)), ('--bundle-id', dict(required=True)),
        ('--out', dict(default='ExportOptions.plist')))
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    args.fn(args)


if __name__ == '__main__':
    main()
