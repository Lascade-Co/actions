import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('kmp_control', Path(__file__).with_name('control.py'))
control = importlib.util.module_from_spec(spec)
spec.loader.exec_module(control)


class DispatchTests(unittest.TestCase):
    payload = dict(repo='Lascade-Co/example', pr=17, branch='feature', project_slug='example')

    def test_resolves_open_same_repository_pr_once_to_immutable_sha(self):
        calls = []
        def get(path):
            calls.append(path)
            return dict(state='open', head=dict(repo=dict(full_name='Lascade-Co/example'), ref='fix/$example', sha='a'*40))
        resolved = control.resolve(self.payload, 'Lascade-Co/actions', get)
        self.assertEqual(resolved['sha'], 'a'*40)
        self.assertEqual(resolved['branch'], 'fix/$example')
        self.assertEqual(resolved['tag'], 'debug-pr-17')
        self.assertEqual(calls, ['repos/Lascade-Co/example/pulls/17'])

    def test_fork_pr_never_reaches_secret_bearing_jobs(self):
        def get(path):
            return dict(state='open', head=dict(repo=dict(full_name='outsider/example'), ref='fix', sha='a'*40))
        with self.assertRaisesRegex(ValueError, 'Fork'):
            control.resolve(self.payload, 'Lascade-Co/actions', get)

    def test_invalid_scope_rejected_before_api_token_use(self):
        for changes in [dict(repo='outsider/repo'), dict(repo='Lascade-Co/../x'), dict(pr='1;echo x'),dict(project_slug='')]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                control.resolve(dict(self.payload, **changes), 'Lascade-Co/actions', lambda p:self.fail('API must not run'))

    def test_manual_branch_is_url_encoded_and_has_unique_run_tag(self):
        calls = []
        with patch.dict(os.environ, GITHUB_RUN_NUMBER='22'):
            result = control.resolve(dict(self.payload, pr='', branch='feature/a#b'), 'Lascade-Co/actions',
                                     lambda path: calls.append(path) or {'sha':'b'*40})
        self.assertEqual(calls, ['repos/Lascade-Co/example/commits/feature%2Fa%23b'])
        self.assertEqual(result['tag'], 'debug-run-22')


class SecretsTests(unittest.TestCase):
    def test_never_executes_shell_and_preserves_apostrophes_and_multiline(self):
        text="TOKEN='a'b $(touch /tmp/not-run)'\nOTHER='first\nsecond'"
        self.assertEqual(control.parse_secret_file(text), {'TOKEN':"a'b $(touch /tmp/not-run)", 'OTHER':'first\nsecond'})

    def test_local_properties_defaults_exclusions_escaping_and_sdk(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory)/'in', Path(directory)/'out'
            source.write_text("TOKEN=' leading\\slash\nnext'\nZOHO_APP_KEY='excluded'")
            control.local_properties(source, target, '/sdk')
            text = target.read_text()
            self.assertIn('PLAY_LICENSE_KEY=\n', text)
            self.assertIn('TOKEN=\\ leading\\\\slash\\nnext\n', text)
            self.assertIn('sdk.dir=/sdk\n', text)
            self.assertNotIn('ZOHO', text)
            self.assertFalse(source.exists())
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)

    def test_malformed_secret_export_fails_without_logging_values(self):
        with self.assertRaisesRegex(ValueError, '^Malformed Infisical export$'):
            control.parse_secret_file('SUPER_SECRET=not-quoted')


class LintGateTests(unittest.TestCase):
    def state(self, unit, lint, subject='change'):
        import json
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'result.json'
            path.write_text(json.dumps(dict(unit_test_exit_code=unit,lint_exit_code=lint)))
            with patch.object(control.subprocess, 'check_output', return_value=subject):
                return control.lint_state(path)

    def test_unit_failure_never_triggers_autofix(self):
        self.assertEqual(self.state(1,1)['autofix_allowed'],'false')

    def test_lint_only_failure_can_trigger_autofix(self):
        self.assertEqual(self.state(0,1)['autofix_allowed'],'true')

    def test_lint_only_job_result_without_unit_fields_can_autofix(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'result.json'
            path.write_text(json.dumps(dict(lint_exit_code=1)))
            with patch.object(control.subprocess, 'check_output', return_value='change'):
                state=control.lint_state(path)
        self.assertEqual(state['autofix_allowed'],'true')
        self.assertEqual(state['units_passed'],'true')

    def test_prior_bot_fix_never_loops(self):
        for author in ('Claude','Codex'):
            self.assertEqual(self.state(0,1,'lint: Lint fix by '+author)['autofix_allowed'],'false')

    def test_missing_success_report_never_pushes(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(control, 'output') as out:
            with patch.object(control.Path, 'exists', return_value=False), patch.object(control.subprocess, 'run') as run:
                control.push_fix('branch','a'*40)
            run.assert_not_called()
            out.assert_called_once_with({'pushed':'false'})


if __name__ == '__main__':
    unittest.main()
