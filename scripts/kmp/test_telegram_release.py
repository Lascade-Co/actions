"""Release/store messages share escaped structured HTML and bounded failure pointers."""
import importlib.util
from pathlib import Path
import unittest

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


if __name__=='__main__':unittest.main()
