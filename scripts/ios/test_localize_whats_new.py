import io
import contextlib
import unittest

import localize_whats_new as loc
from test_publish_appstore import FakeClient
import publish_appstore as asc


def quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


class TranslateTests(unittest.TestCase):
    def test_english_locales_are_never_sent_and_nothing_is_called_when_only_english(self):
        def boom(_):
            raise AssertionError('should not call the model')
        self.assertEqual(loc.translate(['en-US', 'en-GB'], 'Hi', generate_json=boom), {})

    def test_translates_only_non_english_and_strips_rejected_characters(self):
        seen = []
        def fake(prompt):
            seen.append(prompt)
            return {'fr-FR': 'Bonjour \U0001F680', 'de-DE': 'Hallo'}
        result = loc.translate(['en-US', 'fr-FR', 'de-DE'], 'Hi', generate_json=fake)
        self.assertEqual(result, {'fr-FR': 'Bonjour', 'de-DE': 'Hallo'})
        self.assertIn('["de-DE", "fr-FR"]', seen[0])

    def test_missing_empty_or_oversize_translation_falls_back_per_locale(self):
        raw = {'fr-FR': 'ok', 'de-DE': '  ', 'es-ES': 'x' * 4001}
        result = quiet(loc.translate, ['fr-FR', 'de-DE', 'es-ES', 'ja'], 'Hi', generate_json=lambda _: raw)
        self.assertEqual(result, {'fr-FR': 'ok'})

    def test_locale_matching_is_case_insensitive(self):
        raw = {'pt-br': 'Olá', 'ZH-HANS': 'Nǐ hǎo'}
        result = loc.translate(['pt-BR', 'zh-Hans'], 'Hi', generate_json=lambda _: raw)
        self.assertEqual(result, {'pt-BR': 'Olá', 'zh-Hans': 'Nǐ hǎo'})

    def test_model_failure_is_fail_soft(self):
        def fail(_):
            raise RuntimeError('Claude API: auth')
        self.assertEqual(quiet(loc.translate, ['fr-FR'], 'Hi', generate_json=fail), {})


class PublisherLocalisationTests(unittest.TestCase):
    def test_whats_new_is_localised_per_locale_and_nothing_else_changes(self):
        client = FakeClient(target=False)
        calls = []
        def localize(locales, notes):
            calls.append((locales, notes))
            return {'fr-FR': 'Notes françaises'}
        asc.Publisher(client, localize=localize).publish('app', '5.0.9', client.builds[0], 'English notes.')
        patched = {c[2]['data']['id']: c[2]['data']['attributes'] for c in client.calls
                   if c[0] == 'PATCH' and c[1].startswith('appStoreVersionLocalizations/')}
        created = [c[2]['data']['attributes'] for c in client.calls if c[:2] == ('POST', 'appStoreVersionLocalizations')]
        texts = [a['whatsNew'] for a in [*patched.values(), *created]]
        self.assertIn('Notes françaises', texts)
        self.assertIn('English notes.', texts)  # en locale keeps the shared notes
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1], 'English notes.')
        # The cloned en-US locale already has a description, so carry must touch only whatsNew
        # and never overwrite existing metadata with the previous version's values.
        self.assertEqual(set(patched['en-new']), {'whatsNew', 'supportUrl'})
        self.assertNotIn('description', patched['en-new'])


if __name__ == '__main__':
    unittest.main()
