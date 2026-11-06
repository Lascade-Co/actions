"""The optional `stores`/`pr_number` dispatch fields select each store publisher through one shared rule."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('publish_stores', Path(__file__).with_name('publish_stores.py'))
stores = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stores)

ROOT = Path(__file__).resolve().parents[2]
BASE = {'repo': 'Lascade-Co/travel-animator-shared', 'release_id': 42, 'package_name': 'com.travelanimator.routemap'}


class SelectionRule(unittest.TestCase):
    def test_absent_stores_selects_both_for_older_dispatches(self):
        for store in ('ios', 'android'):
            with self.subTest(store=store):
                self.assertEqual(stores.select(BASE, store), (True, ''))
                self.assertEqual(stores.select({}, store), (True, ''))

    def test_empty_array_selects_neither(self):
        for store in ('ios', 'android'):
            with self.subTest(store=store):
                self.assertEqual(stores.select(BASE | {'stores': []}, store), (False, ''))

    def test_each_store_is_selected_only_when_listed(self):
        cases = {('android',): {'android': True, 'ios': False}, ('ios',): {'android': False, 'ios': True},
                 ('android', 'ios'): {'android': True, 'ios': True}, ('ios', 'android'): {'android': True, 'ios': True}}
        for listed, expected in cases.items():
            for store, selected in expected.items():
                with self.subTest(listed=listed, store=store):
                    self.assertEqual(stores.select(BASE | {'stores': list(listed)}, store)[0], selected)

    def test_invalid_stores_shapes_fail(self):
        for bad in (None, 'ios', 'android,ios', {'ios': True}, True, 1, ['iOS'], ['web'], [''], ['ios', 'ios'],
                    ['android', 'ios', 'android'], [1], [None], [['ios']], ['ios ']):
            for store in ('ios', 'android'):
                with self.subTest(stores=bad, store=store), self.assertRaisesRegex(ValueError, 'stores'):
                    stores.select(BASE | {'stores': bad}, store)

    def test_pr_number_is_an_optional_positive_json_integer(self):
        self.assertEqual(stores.select(BASE | {'stores': ['android'], 'pr_number': 57}, 'ios'), (False, '57'))
        self.assertEqual(stores.select(BASE | {'pr_number': 1}, 'android'), (True, '1'))
        for bad in (0, -3, '57', 57.0, True, False, None, [57], {'n': 57}):
            with self.subTest(pr_number=bad), self.assertRaisesRegex(ValueError, 'pr_number'):
                stores.select(BASE | {'stores': ['ios'], 'pr_number': bad}, 'ios')

    def test_unknown_store_argument_and_non_object_payload_fail(self):
        with self.assertRaises(ValueError):
            stores.select(BASE, 'web')
        for bad in (None, [], 'payload'):
            with self.subTest(payload=bad), self.assertRaises(ValueError):
                stores.select(bad, 'ios')


class CommandLine(unittest.TestCase):
    def run_main(self, event, store):
        with tempfile.TemporaryDirectory() as directory:
            event_path, output = Path(directory, 'event.json'), Path(directory, 'output')
            event_path.write_text(json.dumps(event))
            output.write_text('earlier=1\n')
            argv = ['publish_stores.py', '--event', str(event_path), '--store', store, '--output', str(output)]
            with patch('sys.argv', argv), contextlib.redirect_stdout(io.StringIO()):
                stores.main()
            return output.read_text()

    def test_writes_selected_and_validated_pr_number_outputs(self):
        event = {'action': 'publish-kmp', 'client_payload': BASE | {'stores': ['android'], 'pr_number': 57}}
        self.assertEqual(self.run_main(event, 'ios'), 'earlier=1\nselected=false\npr_number=57\n')
        self.assertEqual(self.run_main(event, 'android'), 'earlier=1\nselected=true\npr_number=57\n')
        legacy = {'action': 'publish-playstore', 'client_payload': {'repo': 'Lascade-Co/other'}}
        self.assertEqual(self.run_main(legacy, 'android'), 'earlier=1\nselected=true\npr_number=\n')

    def test_invalid_payload_fails_without_writing_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            event_path, output = Path(directory, 'event.json'), Path(directory, 'output')
            event_path.write_text(json.dumps({'client_payload': BASE | {'stores': 'ios'}}))
            argv = ['publish_stores.py', '--event', str(event_path), '--store', 'ios', '--output', str(output)]
            with patch('sys.argv', argv), self.assertRaises(ValueError):
                stores.main()
            self.assertFalse(output.exists())


class WorkflowWiring(unittest.TestCase):
    """Each publisher runs the shared rule for its own store and gates every store or PostHog step on it."""
    WORKFLOWS = {'publish-appstore.yml': ('ios', ('Download IPA from GitHub release', 'Install App Store JWT support',
                                                  'Upload exact build, wait for processing, and submit for review',
                                                  'Create PostHog annotation')),
                 'publish-playstore.yml': ('android', ('Download release assets', 'Locate AAB + native debug symbols',
                                                       'Set up Python', 'Publish rollout + write Telegram message (python)',
                                                       'Create PostHog annotation'))}
    GATE = "steps.stores.outputs.selected == 'true'"

    @staticmethod
    def steps(text):
        return re.split(r'\n(?=      - )', text.split('\n    steps:\n', 1)[1])

    def test_publishers_select_their_own_store_and_gate_store_steps(self):
        for name, (store, gated) in self.WORKFLOWS.items():
            text = (ROOT / '.github/workflows' / name).read_text()
            with self.subTest(workflow=name):
                self.assertIn(f'publish_stores.py --event "$GITHUB_EVENT_PATH" --store {store} --output "$GITHUB_OUTPUT"', text)
                steps = self.steps(text)
                for title in gated:
                    block = next(step for step in steps if f'name: {title}\n' in step)
                    self.assertRegex(block, r"\n        if: steps\.stores\.outputs\.selected == 'true'( &&|\n)", title)
                for step in steps:
                    if 'steps.stores.outputs.selected' not in step.split('\n        with:')[0]:
                        self.assertNotRegex(step, r'publish_appstore\.py|publish_playstore\.py|posthog\.com|release-downloader')
                self.assertIn("status: ${{ job.status != 'success' && 'failure' || "
                              "steps.stores.outputs.selected == 'false' && 'skipped' || 'success' }}", text)
                self.assertIn('pr-number: ${{ steps.stores.outputs.pr_number }}', text)

    def test_skipped_app_store_is_never_reported_as_prepared(self):
        text = (ROOT / '.github/workflows/publish-appstore.yml').read_text()
        self.assertIn("kind: ${{ steps.stores.outputs.selected == 'true' && steps.meta.outputs.submit == 'false' "
                      "&& 'appstore-prepared' || 'appstore' }}", text)


if __name__ == '__main__':
    unittest.main()
