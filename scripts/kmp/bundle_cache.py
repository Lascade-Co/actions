#!/usr/bin/env python3
"""Restore/publish explicitly selected dependency directories as an OCI artifact."""
import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile


def output(**values):
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        for key, value in values.items():
            stream.write(f'{key.replace("_", "-")}={str(value).lower() if isinstance(value, bool) else value}\n')


def paths_from_env():
    paths = [Path(os.path.expandvars(os.path.expanduser(line.strip()))).absolute()
             for line in os.environ['CACHE_PATHS'].splitlines() if line.strip()]
    if not paths:
        raise ValueError('At least one cache path is required')
    forbidden = {Path("/"), Path.home(), Path.cwd()}
    if any(path in forbidden for path in paths):
        raise ValueError('Select dependency subdirectories, never a root/home/workspace directory')
    for index, path in enumerate(paths):
        if any(other == path or other in path.parents or path in other.parents for other in paths[:index]):
            raise ValueError('Selected cache paths must not overlap')
    return paths


def pack(paths, destination):
    present = [i for i, path in enumerate(paths) if path.exists()]
    if not present:
        raise ValueError('None of the selected cache paths exist')
    with tarfile.open(destination, 'w') as archive:
        for i in present:
            # SwiftPM local clones may share inodes across selected directories.
            # Each root must remain independently relocatable: materialize the
            # first copy per root, while retaining hardlinks inside that root.
            archive.inodes.clear()
            archive.add(paths[i], arcname=f'path-{i}')


def validate_members(members, path_count):
    entries = {}
    for member in members:
        parts = PurePosixPath(member.name).parts
        if not parts or '..' in parts or member.name.startswith('/'):
            raise ValueError('Unsafe cache archive path')
        if not re.fullmatch(r'path-[0-9]+', parts[0]) or int(parts[0][5:]) >= path_count:
            raise ValueError('Unexpected cache archive root')
        name = '/'.join(parts)
        if name in entries:
            raise ValueError('Duplicate cache archive path')
        entries[name] = member
        if member.islnk() or member.issym():
            if not member.linkname or PurePosixPath(member.linkname).is_absolute():
                raise ValueError('Unsafe cache archive link')
        elif not (member.isdir() or member.isfile()):
            raise ValueError('Unsupported cache archive entry')

    # Resolve complete link chains in archive coordinates before writing files.
    # Handle '..' after following earlier symlinks, matching filesystem semantics;
    # lexical normpath alone can hide an escape through a symlinked ancestor.
    for name in entries:
        expected_root = PurePosixPath(name).parts[0]
        pending = list(PurePosixPath(name).parts)
        resolved = []
        redirects = 0
        while pending:
            part = pending.pop(0)
            if part == '.':
                continue
            if part == '..':
                if len(resolved) <= 1:
                    raise ValueError('Cache archive link escapes selected path')
                resolved.pop()
                continue
            if not resolved and part != expected_root:
                raise ValueError('Cache archive link crosses selected paths')
            resolved.append(part)
            entry = entries.get('/'.join(resolved))
            if entry and (entry.issym() or entry.islnk()):
                redirects += 1
                if redirects > 256:
                    raise ValueError('Cyclic or excessive cache archive link chain')
                target = list(PurePosixPath(entry.linkname).parts)
                if entry.islnk():
                    if pending:
                        raise ValueError('Hard link cannot be an archive directory')
                    resolved = []  # Tar hard-link targets are archive-root-relative.
                else:
                    resolved.pop()  # Symlink targets are relative to their parent.
                pending = target + pending
        if not resolved or resolved[0] != expected_root:
            raise ValueError('Cache archive link escapes selected path')


def unpack(archive_path, paths):
    # Native toolchains legitimately use links such as lib/plugins/a.so ->
    # ../../libexec/a.so. Allow those inside one selected cache root only.
    with tarfile.open(archive_path) as archive:
        members = archive.getmembers()
        validate_members(members, len(paths))
        with tempfile.TemporaryDirectory() as staging:
            # Keep tarfile's data filter as a second check during extraction,
            # isolated from all existing destination symlinks.
            archive.extractall(staging, members=members, filter='data')
            for i, destination in enumerate(paths):
                source = Path(staging) / f'path-{i}'
                if source.exists():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if destination.is_symlink() or destination.is_file():
                        destination.unlink()
                    elif destination.exists():
                        shutil.rmtree(destination)
                    shutil.move(str(source), str(destination))


def run_oras(*arguments, **kwargs):
    return subprocess.run(['oras', *arguments], check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['restore', 'publish'])
    args = parser.parse_args()
    name = os.environ['CACHE_NAME'].lower()
    key = os.environ['CACHE_KEY']
    fallback = os.environ.get('CACHE_FALLBACK_KEY', '')
    if not re.fullmatch(r'[a-z0-9][a-z0-9._-]{0,127}', name):
        raise ValueError('Invalid cache name')
    for tag in [key, fallback]:
        if tag and not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}', tag):
            raise ValueError('Invalid cache tag')
    if not key:
        raise ValueError('Cache key is required')
    repository = os.environ['GITHUB_REPOSITORY'].lower()
    registry = f'ghcr.io/{repository.split("/")[0]}/ci-bundle-{name}'
    paths = paths_from_env()
    run_oras('login', 'ghcr.io', '-u', os.environ['GITHUB_ACTOR'], '--password-stdin',
             input=os.environ['GH_TOKEN'], text=True, stdout=subprocess.DEVNULL)
    with tempfile.TemporaryDirectory(prefix='bundle-cache-') as work:
        work = Path(work)
        if args.mode == 'restore':
            for tag in dict.fromkeys([key, fallback]):
                if not tag:
                    continue
                download = work / tag
                download.mkdir()
                try:
                    run_oras('pull', f'{registry}:{tag}', '-o', str(download))
                    bundle = download / 'cache.tar'
                    size = bundle.stat().st_size
                    unpack(bundle, paths)
                except (subprocess.CalledProcessError, OSError, ValueError, tarfile.TarError) as error:
                    print(f'::warning::Cache {tag} unavailable ({type(error).__name__}); trying next key')
                    continue
                output(hit=tag == key, restored_key=tag, bytes=size)
                print(f'Restored {size} bytes from {registry}:{tag}')
                return
            output(hit=False, restored_key='', bytes=0)
            print('No usable cache bundle; dependencies will resolve cold')
        else:
            bundle = work / 'cache.tar'
            pack(paths, bundle)
            size = bundle.stat().st_size
            run_oras('push', f'{registry}:{key}', '--artifact-type',
                     'application/vnd.lascade.dependency-cache.v1', '--annotation',
                     f'org.opencontainers.image.source=https://github.com/{repository}',
                     'cache.tar:application/octet-stream', cwd=work)
            if fallback and fallback != key:
                run_oras('tag', f'{registry}:{key}', fallback)
            output(bytes=size)
            print(f'Published {size} bytes to {registry}:{key}')


if __name__ == '__main__':
    main()
