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
SETTINGS_BLOCK = re.compile(r'buildSettings = \{.*?\n\t\t\t\};', re.S)


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


def set_setting(text, key, value, only_blocks_with=None):
    """Rewrite `KEY = value;` in every buildSettings block (optionally only those containing a marker)."""
    def block(match):
        body = match.group(0)
        if only_blocks_with and only_blocks_with not in body:
            return body
        return re.sub(rf'^(\s*){key} = [^;\n]*;', lambda m: f'{m.group(1)}{key} = {value};', body, flags=re.M)
    return SETTINGS_BLOCK.sub(block, text)


def add_setting_after(text, after_key, key, value, marker):
    def block(match):
        body = match.group(0)
        if marker not in body:
            return body
        body = re.sub(rf'^\s*{key} = [^;\n]*;\n', '', body, flags=re.M)
        return re.sub(rf'^(\s*){after_key} = [^;\n]*;\n',
                      lambda m: f'{m.group(0)}{m.group(1)}{key} = {value};\n', body, count=1, flags=re.M)
    return SETTINGS_BLOCK.sub(block, text)


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


def cmd_set_version(a):
    if not VERSION_RE.match(a.version):
        fail('invalid version: ' + a.version)
    src = Path(a.source)
    m = manifest(src)
    for rel in m['version_files']:
        yq('.settings.android.versionName = strenv(V) | .settings.android.versionCode = (strenv(B) | tonumber)',
           src / rel, {'V': a.version, 'B': str(a.build)})
    pbx = pbxproj(src, m)
    text = set_setting(pbx.read_text(), 'MARKETING_VERSION', a.version)
    text = set_setting(text, 'CURRENT_PROJECT_VERSION', a.build)
    pbx.write_text(text)
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
    if a.team and not release:
        fail('--team/--profile apply to the release variant only')
    if a.team and not re.fullmatch(r'[A-Za-z0-9]+', a.team):
        fail('invalid team id')

    yq('.settings.android.applicationId = strenv(ID)', module_yaml(src, m), {'ID': ident})
    pbx = pbxproj(src, m)
    text = set_setting(pbx.read_text(), 'PRODUCT_BUNDLE_IDENTIFIER', ident)
    if a.team:
        # Manual signing must live on the target's build settings; command-line overrides did not take.
        marker = 'PRODUCT_BUNDLE_IDENTIFIER'
        text = set_setting(text, 'CODE_SIGN_STYLE', 'Manual', marker)
        text = set_setting(text, 'CODE_SIGN_IDENTITY', '"Apple Distribution"', marker)
        text = set_setting(text, 'DEVELOPMENT_TEAM', a.team, marker)
        text = add_setting_after(text, 'DEVELOPMENT_TEAM', 'PROVISIONING_PROFILE_SPECIFIER',
                                 '"' + a.profile.replace('"', '').replace('\\', '') + '"', marker)
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
    if re.search(r'["\\$\n\r]', token):
        fail('APPCHECK_DEBUG_TOKEN contains characters unsafe for a Kotlin string')
    kt = src / m['build_secrets_kt']
    body = kt.read_text()
    if not TOKEN_LINE.search(body):
        fail('APPCHECK_DEBUG_TOKEN line not found in ' + m['build_secrets_kt'])
    kt.write_text(TOKEN_LINE.sub(lambda mm: f'{mm.group(1)}"{token}"', body))
    emit(variant=a.variant, bundle_id=ident)


def cmd_android_inputs(a):
    src = Path(a.source)
    m = manifest(src)
    mod = src / m['android_module']
    (mod / 'ci.jks').write_bytes(base64.b64decode(os.environ['KEYSTORE_BASE64'], validate=True))
    props = {'storeFile': 'ci.jks', 'storePassword': os.environ['KEYSTORE_PASSWORD'],
             'keyAlias': os.environ['KEY_ALIAS'], 'keyPassword': os.environ['KEY_PASSWORD']}
    for key, value in props.items():
        if '\n' in value or '\r' in value:
            fail('multiline value for ' + key)
    path = mod / 'keystore.properties'
    path.write_text(''.join(f'{k}={v}\n' for k, v in props.items()))
    path.chmod(0o600)
    emit(keystore='ci.jks')


PATTERNS = {
    'apk': 'build/tasks/**/*.apk',
    'aab': 'build/tasks/**/*.aab',
    'mapping': 'build/tasks/**/outputs/mapping/release/mapping.txt',
}


def cmd_find_artifact(a):
    matches = sorted(glob.glob(str(Path(a.source) / PATTERNS[a.kind]), recursive=True))
    if len(matches) != 1:
        fail(f'expected exactly one {a.kind} under build/tasks, found {len(matches)}: {matches}')
    emit(path=matches[0])


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
    add('kn-gc-guard', cmd_gc_guard, ('--log', dict(required=True)), ('--gc-dir', dict(required=True)),
        ('--max-full-gc', dict(type=int, default=20)))
    add('export-options', cmd_export_options, variant, ('--team', dict(required=True)),
        ('--profile', dict(required=True)), ('--bundle-id', dict(required=True)),
        ('--out', dict(default='ExportOptions.plist')))
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    args.fn(args)


if __name__ == '__main__':
    main()
