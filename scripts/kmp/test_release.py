"""Offline release orchestration gates: dispatch trust, notes, assets and atomic Git leases."""
import base64
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('release', Path(__file__).with_name('release.py'))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


@contextmanager
def in_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True, stderr=subprocess.DEVNULL).strip()


class Dispatch(unittest.TestCase):
    payload = dict(repo=release.SOURCE_REPO, branch='main', sha='a'*40, project_slug='travelanimator')

    def test_main_sha_pinned(self):
        calls=[]
        resolved=release.resolve(self.payload, 'Lascade-Co/actions',
                                 lambda path: calls.append(path) or {'sha':'a'*40,'commit':{'message':'Merge reviewed release'}})
        self.assertEqual(resolved['skip'],'false')
        self.assertEqual(calls,['repos/Lascade-Co/travel-animator-shared/commits/main'])

    def test_invalid_scope_does_not_use_credentials(self):
        for change in ({'repo':'attacker/repo'}, {'repo':'Lascade-Co/other'}, {'branch':'dev'},
                       {'sha':'a'*39}, {'project_slug':'other'}):
            with self.subTest(change=change),self.assertRaises(ValueError):
                release.resolve(dict(self.payload,**change),'Lascade-Co/actions',lambda path:self.fail('API called'))

    def test_stale_dispatch_fails_before_source_runs(self):
        with self.assertRaisesRegex(ValueError,'stale'):
            release.resolve(self.payload,'Lascade-Co/actions',lambda path:{'sha':'b'*40,'commit':{'message':'New work'}})

    def test_manual_dispatch_on_bot_bump_skips(self):
        result=release.resolve(self.payload,'Lascade-Co/actions',lambda path:{'sha':'b'*40,'author':{'type':'Bot'},'commit':{'message':'chore(release): v5.0.9 (42) [skip ci]'}})
        self.assertEqual(result['skip'],'true')

    def test_subject_alone_does_not_skip_human_commit(self):
        with self.assertRaisesRegex(ValueError,'stale'):
            release.resolve(self.payload,'Lascade-Co/actions',lambda path:{'sha':'b'*40,
                'author':{'type':'User'},'commit':{'message':'chore(release): v5.0.9 (42) [skip ci]',
                                                  'author':{'name':'Contributor'}}})


class Notes(unittest.TestCase):
    def test_notes_reject_empty_and_utf16_overflow_without_truncation(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'notes'
            for value in ('  ', '😀'*251):
                path.write_text(value)
                with self.assertRaises(ValueError): release.check_notes(path)
                self.assertEqual(path.read_text(),value)
            path.write_text('😀'*250)
            self.assertEqual(release.check_notes(path),'😀'*250)

    def test_fresh_notes_reused_stale_notes_prompt_uses_tag_diff(self):
        with tempfile.TemporaryDirectory() as temp,in_directory(temp):
            root=Path(temp);git(root,'init');git(root,'config','user.email','test@example.com');git(root,'config','user.name','Test')
            Path('releasenotes.txt').write_text('Old notes');git(root,'add','.');git(root,'commit','-m','initial');git(root,'tag','v5.0.8')
            Path('template').write_text('Write notes')
            Path('feature').write_text('User visible fix');git(root,'add','feature');git(root,'commit','-m','feature')
            self.assertEqual(release.notes('template','prompt')['outdated'],'true')
            self.assertIn('User visible fix',Path('prompt').read_text())
            Path('releasenotes.txt').write_text('New notes');git(root,'add','releasenotes.txt');git(root,'commit','-m','notes')
            self.assertEqual(release.notes('template','prompt')['outdated'],'false')


    def test_untagged_release_diffs_from_the_previous_main_and_bounds_the_prompt(self):
        with tempfile.TemporaryDirectory() as temp,in_directory(temp),patch.object(release,'MAX_PROMPT_DIFF_CHARS',200):
            root=Path(temp);git(root,'init');git(root,'config','user.email','test@example.com');git(root,'config','user.name','Test')
            Path('old').write_text('ancient history '*100);git(root,'add','.');git(root,'commit','-m','ancient')
            Path('template').write_text('Write notes')
            Path('feature').write_text('New fix '+'x'*1000);Path('icon.bin').write_bytes(b'\x00\x01'*50)
            git(root,'add','feature','icon.bin');git(root,'commit','-m','Fix the route line')
            self.assertEqual(release.notes('template','prompt')['outdated'],'true')
            prompt=Path('prompt').read_text()
            self.assertIn('- Fix the route line',prompt)
            self.assertNotIn('ancient history',prompt)
            self.assertNotIn('\x00',prompt)
            self.assertIn('[diff truncated',prompt)


class Assets(unittest.TestCase):
    def fixture(self,path):
        for name in ('TravelAnimator.aab','TravelAnimator-universal.apk','TravelAnimator.ipa','native-debug-symbols.zip','mapping.txt','TravelAnimator-dSYMs.zip'):
            (path/name).write_bytes(b'asset')

    def test_release_requires_every_nonempty_unique_asset(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);self.fixture(folder)
            self.assertEqual(len(release.release_assets(folder)),len(release.ARTIFACT_SUFFIXES))  # 4 while iOS is disabled
            (folder/'extra.apk').write_bytes(b'duplicate')
            with self.assertRaisesRegex(ValueError,'.apk'):release.release_assets(folder)
            (folder/'extra.apk').unlink();(folder/'mapping.txt').write_bytes(b'')
            with self.assertRaisesRegex(ValueError,'mapping'):release.release_assets(folder)

    def test_release_published_only_after_all_assets_uploaded(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(release.subprocess,'run') as run:
            folder=Path(temp);self.fixture(folder);notes=folder/'notes';notes.write_text('Fixed route export.')
            run.return_value=subprocess.CompletedProcess([],1,'')
            release.publish(release.SOURCE_REPO,'v5.0.9',folder,str(notes))
            commands=[call.args[0] for call in run.call_args_list]
            self.assertIn('--draft',commands[1]);self.assertEqual(commands[2][2],'upload')
            self.assertEqual(commands[3][2],'edit');self.assertIn('--draft=false',commands[3])
            self.assertIn('--prerelease=false',commands[3])

    def test_never_changes_a_published_release(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(release.subprocess,'run') as run:
            folder=Path(temp);self.fixture(folder);notes=folder/'notes';notes.write_text('Notes')
            run.return_value=subprocess.CompletedProcess([],0,json.dumps({'isDraft':False}))
            with self.assertRaisesRegex(ValueError,'already published'):release.publish(release.SOURCE_REPO,'v5.0.9',folder,str(notes))
            self.assertEqual(run.call_count,1)

    def test_signing_inputs_private_and_use_correct_release_paths(self):
        with tempfile.TemporaryDirectory() as temp,in_directory(temp),patch.dict(os.environ,{
            'KEYSTORE_BASE64':base64.b64encode(b'keystore').decode(),
            'SERVICE_JSON_BASE64':base64.b64encode(b'firebase').decode(),
            'SERVICE_PLIST_BASE64':base64.b64encode(b'plist').decode()}):
            release.install_inputs('android');release.install_inputs('ios')
            self.assertEqual(Path('androidApp/keystore.jks').read_bytes(),b'keystore')
            plist=Path('iosApp/iosApp/Config/Release/GoogleService-Info.plist')
            self.assertEqual(plist.read_bytes(),b'plist');self.assertEqual(plist.stat().st_mode & 0o777,0o600)


class GitTransaction(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name);self.remote=self.base/'remote';self.source=self.base/'source'
        git(self.base,'init','--bare',str(self.remote));git(self.base,'clone',str(self.remote),str(self.source))
        git(self.source,'checkout','-b','main');git(self.source,'config','user.name','Test');git(self.source,'config','user.email','test@example.com')
        (self.source/'gradle.properties').write_text('VERSION=5.0.8\nVERSION_CODE=41\n')
        git(self.source,'add','.');git(self.source,'commit','-m','base');git(self.source,'push','origin','main')
        self.sha=git(self.source,'rev-parse','HEAD')
        (self.source/'gradle.properties').write_text('VERSION=5.0.9\nVERSION_CODE=42\n')

    def test_bump_tag_and_commit_are_atomic_and_contain_only_version(self):
        with in_directory(self.source): result=release.push_version(self.sha,'5.0.9','42')
        self.assertEqual(git(self.remote,'rev-parse','main'),result['release_sha'])
        self.assertEqual(git(self.remote,'rev-parse','v5.0.9^{}'),result['release_sha'])
        self.assertEqual(git(self.remote,'log','-1','--pretty=%s','main'),'chore(release): v5.0.9 (42) [skip ci]')
        self.assertEqual(git(self.remote,'diff','--name-only',self.sha,'main'),'gradle.properties')

    def test_concurrent_main_commit_rejects_entire_push_including_tag(self):
        other=self.base/'other';git(self.base,'clone','--branch','main',str(self.remote),str(other))
        git(other,'config','user.name','Other');git(other,'config','user.email','other@example.com')
        (other/'new-work').write_text('Concurrent contributor');git(other,'add','.');git(other,'commit','-m','new work');git(other,'push','origin','main')
        newsha=git(other,'rev-parse','HEAD')
        with in_directory(self.source),self.assertRaisesRegex(ValueError,'changed'):release.push_version(self.sha,'5.0.9','42')
        self.assertEqual(git(self.remote,'rev-parse','main'),newsha)
        self.assertEqual(git(self.remote,'tag'),'')

    def test_safe_retry_resumes_only_exact_atomic_version_transaction(self):
        with in_directory(self.source): first=release.push_version(self.sha,'5.0.9','42')
        git(self.source,'checkout','--detach',self.sha)
        (self.source/'gradle.properties').write_text('VERSION=5.0.9\nVERSION_CODE=42\n')
        with in_directory(self.source): second=release.push_version(self.sha,'5.0.9','42')
        self.assertEqual(first,second)
        self.assertEqual(git(self.remote,'rev-parse','main'),first['release_sha'])

    def test_lease_rejects_contributor_commit_between_remote_check_and_push(self):
        other=self.base/'other';git(self.base,'clone','--branch','main',str(self.remote),str(other))
        git(other,'config','user.name','Other');git(other,'config','user.email','other@example.com')
        (other/'new-work').write_text('Concurrent contributor');git(other,'add','.');git(other,'commit','-m','new work')
        expected=git(other,'rev-parse','HEAD')
        original=release.git
        def race(*args):
            value=original(*args)
            if args[0]=='ls-remote':git(other,'push','origin','main')
            return value
        with in_directory(self.source),patch.object(release,'git',side_effect=race),self.assertRaises(subprocess.CalledProcessError):
            release.push_version(self.sha,'5.0.9','42')
        self.assertEqual(git(self.remote,'rev-parse','main'),expected)
        self.assertEqual(git(self.remote,'tag'),'')

    def test_refuses_unrelated_tracked_changes(self):
        git(self.source,'add','gradle.properties')
        with in_directory(self.source),self.assertRaisesRegex(ValueError,'Only'):release.push_version(self.sha,'5.0.9','42')


if __name__=='__main__':unittest.main()
