"""Release/store messages share escaped structured HTML and bounded failure pointers."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('telegram_notify',Path(__file__).with_name('telegram_notify.py'))
notify=importlib.util.module_from_spec(spec)
spec.loader.exec_module(notify)


class ReleaseNotifications(unittest.TestCase):
    fields={'repo':'Lascade-Co/app','branch':'main','title':'5.0.9 <review>',
            'link':'https://github.com/org/app/releases/tag/v5.0.9','run_url':'https://github.com/actions/runs/1'}

    def test_new_kinds_have_correct_jobs_and_success_text(self):
        for kind,text in [('release','KMP release ready'),('playstore','Play production rollout submitted'),
                          ('appstore','App Store submitted for review')]:
            with self.subTest(kind=kind):
                result=notify.compose(kind,'success',self.fields)
                self.assertIn(text,result);self.assertIn('View release',result)
                self.assertNotIn('Open workflow run',result)
                self.assertIn('&lt;review&gt;',result)
        self.assertEqual(notify.KINDS['playstore']['jobs'],('publish',))
        self.assertEqual(notify.KINDS['appstore']['jobs'],('publish',))
        self.assertIn('ios-build',notify.KINDS['release']['jobs'])

    def test_failure_keeps_step_and_apple_error_pointer_and_escapes_html(self):
        details=[{'job':'publish','steps':['Submit <version>'],'errors':['Apple & profile mismatch']}]
        result=notify.compose('appstore','failure',self.fields,details)
        self.assertIn('Submit &lt;version&gt;',result)
        self.assertIn('Apple &amp; profile mismatch',result)
        self.assertIn('Open workflow run',result)
        self.assertNotIn('Submitted for App Store review',result)

    def test_store_success_indicates_actual_handoff(self):
        self.assertIn('7-day phased rollout',notify.compose('appstore','success',self.fields))
        self.assertIn('Bangladesh, 99%',notify.compose('playstore','success',self.fields))

    def test_prepared_app_store_version_is_not_reported_as_submitted(self):
        result=notify.compose('appstore-prepared','success',self.fields)
        self.assertIn('App Store version prepared, not submitted',result)
        self.assertIn('submit for review there',result);self.assertIn('View release',result)
        self.assertNotIn('Submitted for App Store review',result)
        self.assertEqual(notify.KINDS['appstore-prepared']['jobs'],('publish',))
        self.assertIn('App Store preparation failed',notify.compose('appstore-prepared','failure',self.fields))



class SkippedStoreNotifications(unittest.TestCase):
    fields={'repo':'Lascade-Co/travel-animator-shared','title':'Travel <Animator> · v5.0.15',
            'link':'https://github.com/Lascade-Co/travel-animator-shared/releases/tag/v5.0.15?a=1&b=2',
            'run_url':'https://github.com/actions/runs/1'}

    def test_skip_names_the_pr_label_and_links_the_pr(self):
        for kind,header,label in [('appstore','App Store skipped','no-ios'),('playstore','Play Store skipped','no-android')]:
            with self.subTest(kind=kind):
                result=notify.compose(kind,'skipped',self.fields|{'pr_number':'57'})
                self.assertEqual(result,'\n'.join([
                    f'⏭️ <b>{header}</b>','<b>Lascade-Co/travel-animator-shared</b>',
                    '<i>Travel &lt;Animator&gt; · v5.0.15</i>','',
                    f'Reason: <code>{label}</code> on <a href="https://github.com/Lascade-Co/travel-animator-shared/pull/57">#57</a>',
                    '📦 <a href="https://github.com/Lascade-Co/travel-animator-shared/releases/tag/v5.0.15?a=1&amp;b=2">View release</a>']))

    def test_skip_without_pr_says_not_selected_and_never_points_at_the_run(self):
        for kind,header in [('appstore','App Store skipped'),('playstore','Play Store skipped')]:
            with self.subTest(kind=kind):
                result=notify.compose(kind,'skipped',self.fields)
                self.assertIn(f'⏭️ <b>{header}</b>',result)
                self.assertIn('Reason: not selected for this release.',result)
                for absent in ('/pull/','Open workflow run','failed','submitted','rollout'):
                    self.assertNotIn(absent,result)

    def test_skip_escapes_the_repository_in_the_pr_link(self):
        result=notify.compose('appstore','skipped',self.fields|{'repo':'org/a"b<c>','pr_number':'7'})
        self.assertIn('<b>org/a&quot;b&lt;c&gt;</b>',result)
        self.assertIn('href="https://github.com/org/a&quot;b&lt;c&gt;/pull/7"',result)

    def test_skip_rejects_bad_pr_numbers_and_non_store_kinds(self):
        for bad in ('0','-1','5a','<b>','7\n'):
            with self.subTest(pr_number=bad),self.assertRaises(ValueError):
                notify.compose('playstore','skipped',self.fields|{'pr_number':bad})
        for kind in ('release','android','testflight','appstore-prepared'):
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                notify.compose(kind,'skipped',self.fields)

    def test_main_sends_skipped_without_looking_up_failures(self):
        env={'TELEGRAM_TOKEN':'t','TELEGRAM_CHAT':'-1','TELEGRAM_KIND':'appstore','TELEGRAM_STATUS':'skipped',
             'TELEGRAM_REPO':'Lascade-Co/travel-animator-shared','TELEGRAM_TITLE':'TA · v5.0.15','TELEGRAM_PR_NUMBER':'57',
             'GITHUB_RUN_ID':'9','TELEGRAM_GITHUB_TOKEN':'gh','GITHUB_REPOSITORY':'Lascade-Co/actions'}
        sent=[]
        with patch.dict(notify.os.environ,env,clear=True),\
             patch.object(notify,'failure_details',side_effect=AssertionError('no lookup for skipped')),\
             patch.object(notify,'send',lambda token,chat,message,html_mode=False:sent.append((message,html_mode))):
            notify.main()
        self.assertEqual(len(sent),1);self.assertTrue(sent[0][1])
        self.assertIn('<code>no-ios</code> on <a href="https://github.com/Lascade-Co/travel-animator-shared/pull/57">#57</a>',sent[0][0])


if __name__=='__main__':unittest.main()
