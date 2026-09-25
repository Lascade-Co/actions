"""Tests for the daily email's people map, report merge and renderer.

Run: python3 -m unittest discover -s scripts/catchup -p 'test_catchup_email.py' -v

This repo is public, so every fixture here is synthetic: no real report content.
"""
import contextlib
import json
import os
import re
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import catchup_render_email as E  # noqa: E402
import catchup_report as R  # noqa: E402

DATA = os.path.join(HERE, "..", "..", "data")
PEOPLE_FILE = os.path.join(DATA, "catchup_people.json")
PAGE_LIGHT = "#f5f3f1"


def repo(name, bullets, commits=1):
    return {"repo": f"Lascade-Co/{name}", "prs": [], "branches": [],
            "developers": [{"name": "Ada Lovelace", "commit_count": commits, "bullets": bullets}]}


def multi_repo(name, devs):
    return {"repo": f"Lascade-Co/{name}", "prs": [], "branches": [], "developers": devs}


def dev(name, login, bullets, commits=1):
    return {"name": name, "login": login, "commit_count": commits, "bullets": bullets}


def codex_for(rid, **groups):
    return {"headline": "h", "decisions_needed": [],
            "repos": [{"repo": rid, "display_name": "Name", "emoji": "🌳", **groups}]}


def html_of(report, pins=None):
    report["date"] = "2026-09-24"
    return E.render(report, "Lascade", 24, pins=pins)


def _lum(hexcolor):
    def f(c):
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    r, g, b = (int(hexcolor[i:i + 2], 16) / 255 for i in (1, 3, 5))
    return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b)


def contrast(a, b):
    hi, lo = sorted((_lum(a), _lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def dark_css(h):
    """{class: declarations} from the prefers-color-scheme:dark block."""
    media = h.split("@media (prefers-color-scheme:dark){")[1].split("\n}")[0]
    return dict(re.findall(r"\.([\w-]+)\{([^}]*)\}", media))


def ogsc_css(h):
    return dict(re.findall(r"\[data-ogsc\] \.([\w-]+)\{([^}]*)\}", h))


def colour(decls, prop="color"):
    return re.search(rf"(?:^|;){prop}:(#[0-9a-fA-F]{{6}})", decls).group(1)


def tag_classes(h, name):
    """Tag classes ('tag-c3' / 'tag-grey') of every tag showing this person, 'Ada' or 'Ada 3'."""
    return re.findall(rf'class="(tag-(?:c\d|grey))">{re.escape(name)}(?: \d+)?</span>', h)


@contextlib.contextmanager
def captured_log():
    logged = []
    with mock.patch.object(R, "log", logged.append):
        yield logged


def people_of(*entries, bots=()):
    """load_people() on an in-memory people file."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "people.json")
        with open(path, "w") as fh:
            json.dump({"people": list(entries), "bots": list(bots)}, fh)
        return R.load_people(path)


CHECKED_IN = R.load_people(PEOPLE_FILE)


def fake_codex(prompt_path, payload_text, scratch, output_name):
    """Stands in for `codex exec`: one bullet per input item, citing that item's id."""
    keys = {"Done": "done", "Testing": "testing", "In progress": "in_progress"}
    repos = []
    for r in json.loads(payload_text)["repos"]:
        entry = {"repo": r["repo"], "display_name": r["repo"].split("/")[-1]}
        for label, items in r["work"].items():
            entry[keys[label]] = [{"text": f"about {w['text']}", "from": [w["id"]]} for w in items]
        repos.append(entry)
    return {"headline": "h", "decisions_needed": [], "repos": repos}


def run_report(repos, people=PEOPLE_FILE):
    """report.json from the real main(): exclusions, people pass, payload, (fake) Codex, merge.

    `people` is a path, a dict/str written to a temp file, or None for no --people flag.
    """
    with tempfile.TemporaryDirectory() as d:
        daily, out = os.path.join(d, "daily.json"), os.path.join(d, "report.json")
        with open(daily, "w") as fh:
            json.dump({"date": "2026-09-24", "repos": repos}, fh)
        argv = ["catchup_report.py", "--daily", daily, "--report-prompt", "unused.md", "--out", out]
        if people is not None:
            path = people
            if not os.path.exists(str(people)):
                path = os.path.join(d, "people.json")
                with open(path, "w") as fh:
                    fh.write(people if isinstance(people, str) else json.dumps(people))
            argv += ["--people", path]
        with mock.patch.object(R, "run_codex", fake_codex), mock.patch.object(sys, "argv", argv), \
                mock.patch.dict(os.environ), captured_log():
            os.environ.pop("GITHUB_OUTPUT", None)
            R.main()
        with open(out) as fh:
            return json.load(fh)


class MergeTests(unittest.TestCase):
    def test_full_length_bullet_never_truncated(self):
        long = " ".join(f"word{i}" for i in range(60))
        r = repo("a", {"Published": ["src"]})
        rep = R.merge([r], codex_for("Lascade-Co/a", done=[{"text": long, "from": ["R0.P1"]}]))
        self.assertIn(long, html_of(rep))
        self.assertNotIn("…", html_of(rep))

    def test_many_bullets_all_render(self):
        src = [f"s{i}" for i in range(8)]
        r = repo("a", {"Published": src})
        done = [{"text": f"outcome {i}", "from": [f"R0.P{i + 1}"]} for i in range(8)]
        h = html_of(R.merge([r], codex_for("Lascade-Co/a", done=done)))
        for i in range(8):
            self.assertIn(f"outcome {i}", h)
        self.assertNotIn("Also (technical detail)", h)

    def test_testing_and_in_progress_are_separate_groups(self):
        r = repo("a", {"Testing": ["t"], "Work in Progress": ["w"]})
        rep = R.merge([r], codex_for("Lascade-Co/a",
                                     testing=[{"text": "T", "from": ["R0.T1"]}],
                                     in_progress=[{"text": "W", "from": ["R0.W1"]}]))
        self.assertEqual([g["label"] for g in rep["repos"][0]["groups"]], ["Testing", "In progress"])

    def test_group_with_no_source_work_is_not_rendered(self):
        r = repo("a", {"Testing": ["t"]})
        rep = R.merge([r], codex_for("Lascade-Co/a", done=[{"text": "Invented", "from": ["R0.P1"]}]))
        self.assertNotIn("Invented", html_of(rep))
        self.assertEqual([g["key"] for g in rep["repos"][0]["groups"]], ["testing"])

    def test_invented_bullet_dropped_and_source_shown_verbatim(self):
        r = repo("a", {"Published": ["Real source detail"]})
        rep = R.merge([r], codex_for("Lascade-Co/a", done=[{"text": "Invented", "from": ["R9.P9"]}]))
        h = html_of(rep)
        self.assertNotIn("Invented", h)
        self.assertIn("Real source detail", h)
        self.assertIn("Also (technical detail)", h)

    def test_uncovered_source_bullet_is_shown_verbatim(self):
        r = repo("a", {"Published": ["one", "two"]})
        rep = R.merge([r], codex_for("Lascade-Co/a", done=[{"text": "First", "from": ["R0.P1"]}]))
        h = html_of(rep)
        self.assertIn("First", h)
        self.assertIn("two", h)
        self.assertNotIn(">one<", h.split("Also")[0])

    def test_codex_failure_still_shows_all_work(self):
        r = repo("a", {"Published": ["p1"], "Testing": ["t1"]})
        rep = R.merge([r], {}, assessed=False)
        h = html_of(rep)
        self.assertIn("p1", h)
        self.assertIn("t1", h)
        self.assertIn("Couldn't be assessed", h)

    def test_malformed_codex_shapes_do_not_crash(self):
        r = repo("a", {"Published": ["p"]})
        for bad in [{}, [], None, {"repos": None}, {"repos": [None]}, {"repos": [{"repo": []}]},
                    {"repos": [{"repo": "Lascade-Co/a", "done": "str"}]},
                    {"repos": [{"repo": "Lascade-Co/a", "done": [None, 5, {"text": 1, "from": 2}]}]}]:
            rep = R.merge([r], bad)
            self.assertIn("p", html_of(rep))

    def test_missing_assessment_fields_are_not_reported_as_nothing(self):
        r = repo("a", {"Published": ["p"]})
        for bad in [{}, {"repos": []}, {"decisions_needed": []}]:
            self.assertIn("Couldn't be assessed", html_of(R.merge([r], bad)))
        ok = R.merge([r], {"repos": [], "decisions_needed": []})
        self.assertNotIn("Needs you", html_of(ok))

    def test_duplicate_repo_entries_are_ignored(self):
        r = repo("a", {"Published": ["p"]})
        c = codex_for("Lascade-Co/a", done=[{"text": "A", "from": ["R0.P1"]}])
        c["repos"].append(dict(c["repos"][0]))
        h = html_of(R.merge([r], c))
        self.assertNotIn(">A<", h)
        self.assertIn("p", h)

    def test_normalisation_edge_cases(self):
        r = repo("a", {"Published": "abc", "Testing": None, "Work in Progress": ["", None, 42], "Weird": ["w"]})
        work = R.normalize_work(r, 0)
        self.assertEqual([w["text"] for w in work["done"]], ["abc"])
        self.assertEqual(work["testing"], [])
        self.assertEqual([w["text"] for w in work["in_progress"]], ["42", "w"])
        legacy = R.normalize_work({"repo": "x", "developers": [{"bullets": ["legacy"]}]}, 0)
        self.assertEqual(legacy["in_progress"][0]["text"], "legacy")

    def test_decisions_are_not_clipped(self):
        long = "d" * 400
        c = codex_for("Lascade-Co/a")
        c["decisions_needed"] = [long]
        self.assertIn(long, html_of(R.merge([repo("a", {"Published": ["p"]})], c)))

    def test_invalid_decisions_are_not_a_quiet_day(self):
        r = repo("a", {"Published": ["p"]})
        for bad in ([None], [""], ["  "], [5], [{"x": 1}]):
            c = codex_for("Lascade-Co/a")
            c["decisions_needed"] = bad
            rep = R.merge([r], c)
            self.assertFalse(rep["assessed"], bad)
            self.assertIn("Couldn't be assessed", html_of(rep))

    def test_valid_decision_still_shown_alongside_an_invalid_one(self):
        c = codex_for("Lascade-Co/a")
        c["decisions_needed"] = ["Pick one", ""]
        h = html_of(R.merge([repo("a", {"Published": ["p"]})], c))
        self.assertIn("Pick one", h)


class IconTests(unittest.TestCase):
    def test_icon_map_wins_and_codex_emoji_is_ignored(self):
        r = repo("a", {"Published": ["p"]})
        rep = R.merge([r], codex_for("Lascade-Co/a"), icons={"Lascade-Co/a": "🤖"})
        self.assertEqual(rep["repos"][0]["emoji"], "🤖")      # codex_for says 🌳

    def test_unknown_repo_gets_default_even_if_codex_picked_one(self):
        r = repo("a", {"Published": ["p"]})
        for icons in (None, {}, {"Lascade-Co/other": "🤖"}):
            rep = R.merge([r], codex_for("Lascade-Co/a"), icons=icons)
            self.assertEqual(rep["repos"][0]["emoji"], "📦")

    def test_load_icons_survives_a_bad_edit(self):
        with tempfile.TemporaryDirectory() as d:
            def write(text):
                path = os.path.join(d, "icons.json")
                with open(path, "w") as fh:
                    fh.write(text)
                return path
            with captured_log():
                self.assertEqual(R.load_icons(None), {})
                self.assertEqual(R.load_icons(os.path.join(d, "missing.json")), {})
                self.assertEqual(R.load_icons(write("{not json")), {})
                self.assertEqual(R.load_icons(write("[1, 2]")), {})
                self.assertEqual(R.load_icons(write('{"a/b": " 🤖 ", "c/d": "", "e/f": 5}')),
                                 {"a/b": "🤖"})

    def test_checked_in_icon_map_is_valid(self):
        with open(os.path.join(DATA, "catchup_icons.json")) as fh:
            icons = json.load(fh)
        self.assertIsInstance(icons, dict)
        self.assertTrue(icons)
        for repo_name, emoji in icons.items():
            self.assertRegex(repo_name, r"^Lascade-Co/[^/\s]+$")
            self.assertIsInstance(emoji, str)
            self.assertTrue(emoji.strip(), repo_name)


class SoftLengthTests(unittest.TestCase):
    def _run(self, n_words):
        text = " ".join(["word"] * n_words)
        r = repo("a", {"Published": ["src"]})
        with captured_log() as logged:
            rep = R.merge([r], codex_for("Lascade-Co/a", done=[{"text": text, "from": ["R0.P1"]}]))
        group = rep["repos"][0]["groups"][0]
        self.assertEqual([b["text"] for b in group["bullets"]], [text])  # never altered
        self.assertEqual(group["also"], [])          # coverage unchanged
        return [m for m in logged if m.startswith("Long bullet")]

    def test_twenty_words_no_warning(self):
        self.assertEqual(self._run(20), [])

    def test_twenty_one_words_warns_with_context(self):
        warnings = self._run(21)
        self.assertEqual(len(warnings), 1)
        self.assertIn("Lascade-Co/a Done", warnings[0])

    def test_logs_never_contain_bullet_text(self):
        # The workflow logs of this repo are public: they may name the repo and the
        # word count, never what the work was.
        secret = " ".join(["confidential"] * 25)
        with captured_log() as logged:
            R.merge([repo("a", {"Published": [secret]})],
                    codex_for("Lascade-Co/a", done=[{"text": secret, "from": ["R0.P1"]}]))
            R.merge([repo("a", {"Published": [secret]})], {}, assessed=False)
            R.merge([repo("a", {"Weird": [secret]})], {}, assessed=False)
        long = [m for m in logged if m.startswith("Long bullet")]
        self.assertEqual(len(long), 1)
        self.assertIn("25 words", long[0])
        self.assertTrue(len(logged) >= 3)
        for m in logged:
            self.assertNotIn("confidential", m)


class PeopleTests(unittest.TestCase):
    def test_aliases_match_name_or_login_case_insensitively_and_canonical_counts(self):
        aliases, bots = CHECKED_IN
        self.assertEqual(aliases["rohit t p"], "Rohit")
        self.assertEqual(aliases["rohittp0"], "Rohit")
        self.assertEqual(aliases["rohit"], "Rohit")                # canonical name is an alias too
        self.assertEqual(aliases["mushfiq"], "Mushfiq")            # ...so git name 'mushfiq' merges
        self.assertEqual(aliases["mushfiqhumayoon"], "Mushfiq")
        self.assertEqual(aliases["adarsh"], "Adarsh")
        self.assertEqual(bots, {"claude", "deploy", "lascadesevices"})

    def test_bad_people_file_is_tolerated(self):
        with tempfile.TemporaryDirectory() as d, captured_log():
            def write(text):
                path = os.path.join(d, "p.json")
                with open(path, "w") as fh:
                    fh.write(text)
                return path
            empty = ({}, set())
            self.assertEqual(R.load_people(None), empty)
            self.assertEqual(R.load_people(os.path.join(d, "missing.json")), empty)
            self.assertEqual(R.load_people(write("{not json")), empty)
            self.assertEqual(R.load_people(write("[1]")), empty)
            self.assertEqual(R.load_people(write('{"people": "x", "bots": 5}')), empty)
            aliases, bots = R.load_people(write(json.dumps({
                "people": [5, None, {"name": ""}, {"aliases": ["x"]}, {"name": 7},
                           {"name": "Ann", "aliases": "nope"},
                           {"name": "Bob", "aliases": [1, "", " Bobby ", None]}],
                "bots": [1, "", " Beep ", None]})))
            self.assertEqual(aliases, {"ann": "Ann", "bob": "Bob", "bobby": "Bob"})
            self.assertEqual(bots, {"beep"})

    def test_conflicting_alias_is_logged_and_ignored(self):
        with captured_log() as logged:
            aliases, _ = people_of({"name": "Ann", "aliases": ["shared", "Annie"]},
                                   {"name": "Bob", "aliases": ["Shared", "Bobby"]})
        self.assertNotIn("shared", aliases)
        self.assertEqual((aliases["annie"], aliases["bobby"]), ("Ann", "Bob"))
        self.assertEqual((aliases["ann"], aliases["bob"]), ("Ann", "Bob"))
        self.assertTrue(any("shared" in m.lower() for m in logged))

    def test_a_canonical_name_listed_under_someone_else_is_a_conflict(self):
        with captured_log():
            aliases, _ = people_of({"name": "Riyan"}, {"name": "Ryyan", "aliases": ["Riyan", "R"]})
        self.assertNotIn("riyan", aliases)
        self.assertEqual((aliases["ryyan"], aliases["r"]), ("Ryyan", "Ryyan"))

    def test_apply_people_merges_duplicates_into_the_first_position(self):
        r = multi_repo("a", [dev("Rohit T P", "rohittp0", {"Published": ["a", "b"]}, 3),
                             dev("Varsha Shaheen", "VarshaShaheen", {"Published": ["v"]}, 1),
                             dev("rohittp0", "rohittp0", {"Published": ["c"], "Testing": ["t"]}, 2)])
        before = json.dumps(r, sort_keys=True)
        out = R.apply_people([r], CHECKED_IN)
        self.assertEqual(json.dumps(r, sort_keys=True), before)                  # input untouched
        devs = out[0]["developers"]
        self.assertEqual([(d["name"], d["login"], d["commit_count"]) for d in devs],
                         [("Rohit", "person:Rohit", 5), ("Varsha", "person:Varsha", 1)])
        self.assertEqual(devs[0]["bullets"], {"Published": ["a", "b", "c"], "Testing": ["t"]})

    def test_login_only_match(self):
        r = multi_repo("a", [dev("Some Git Name", "vaishnav-0", {"Published": ["x"]})])
        self.assertEqual(R.apply_people([r], CHECKED_IN)[0]["developers"][0]["name"], "Vaishnav")

    def test_canonical_name_plus_alias_become_one_person(self):
        r = multi_repo("a", [dev("Riyan", None, {"Published": ["x"]}, 1),
                             dev("Muhammed Riyan", "Riyaaaaan", {"Published": ["y"]}, 2)])
        devs = R.apply_people([r], CHECKED_IN)[0]["developers"]
        self.assertEqual([(d["name"], d["commit_count"]) for d in devs], [("Riyan", 3)])

    def test_unmapped_person_is_left_alone(self):
        d = dev("Ada Lovelace", "ada", {"Published": ["x"]}, 4)
        out = R.apply_people([multi_repo("a", [d])], CHECKED_IN)[0]["developers"]
        self.assertEqual(out, [d])

    def test_no_people_means_no_change(self):
        r = multi_repo("a", [dev("Rohit T P", "rohittp0", {"Published": ["x"]})])
        self.assertEqual(R.apply_people([r], R.load_people(None)), [r])

    def test_merge_tolerates_odd_bullet_shapes(self):
        r = multi_repo("a", [dev("Rohit T P", "x", {"Published": "s", "Testing": None}, 1),
                             dev("rohittp0", "x", ["legacy flat list"], 1),
                             {"name": "Rohit", "bullets": None}])
        devs = R.apply_people([r], CHECKED_IN)[0]["developers"]
        self.assertEqual(len(devs), 1)
        self.assertEqual(devs[0]["commit_count"], 2)
        self.assertEqual(devs[0]["bullets"]["Published"], ["s"])
        self.assertEqual(devs[0]["bullets"]["Work in Progress"], ["legacy flat list"])

    def test_alias_merge_sums_commits_and_keeps_one_colour_everywhere(self):
        r = multi_repo("a", [dev("Rohit T P", "rohittp0", {"Published": ["x", "y"]}, 3),
                             dev("rohittp0", "rohittp0", {"Published": ["z"]}, 2)])
        active = R.apply_people([r], CHECKED_IN)
        rep = R.merge(active, codex_for("Lascade-Co/a", done=[{"text": "T", "from": ["R0.P1"]}]))
        self.assertEqual(rep["repos"][0]["commit_count"], 5)
        self.assertEqual([(c["name"], c["commits"], c["key"]) for c in rep["repos"][0]["contributors"]],
                         [("Rohit", 5, "rohit")])
        h = html_of(rep, pins=E.load_pins(PEOPLE_FILE))
        self.assertIn("Also (technical detail)", h)
        classes = tag_classes(h, "Rohit")
        self.assertGreaterEqual(len(classes), 3)                   # bullet, Also item, count line
        self.assertEqual(set(classes), {"tag-c0"})

    def test_full_pipeline_with_interleaved_aliases_attributes_each_bullet_correctly(self):
        # Rohit, Varsha, Rohit again: merging the two Rohit entries renumbers ids, so the
        # payload Codex cites from must be built AFTER the people pass or bullets go to the wrong person.
        r = multi_repo("a", [dev("Rohit T P", "rohittp0", {"Published": ["r1"]}, 2),
                             dev("Varsha Shaheen", "VarshaShaheen", {"Published": ["v1"]}, 1),
                             dev("rohittp0", "rohittp0", {"Published": ["r2"]}, 1)])
        rep = run_report([r])
        bullets = rep["repos"][0]["groups"][0]["bullets"]
        self.assertEqual([(b["text"], [a["name"] for a in b["authors"]]) for b in bullets],
                         [("about r1", ["Rohit"]), ("about r2", ["Rohit"]), ("about v1", ["Varsha"])])
        self.assertEqual(rep["repos"][0]["groups"][0]["also"], [])
        self.assertEqual([(c["name"], c["commits"]) for c in rep["repos"][0]["contributors"]],
                         [("Rohit", 3), ("Varsha", 1)])

    def test_bad_people_file_does_not_stop_the_report(self):
        r = multi_repo("a", [dev("Rohit T P", "rohittp0", {"Published": ["x"]})])
        for bad in ("{oops", {"people": [1, {"name": "X", "aliases": [1]}], "bots": "no"}, "[]"):
            rep = run_report([r], people=bad)
            self.assertEqual(rep["repos"][0]["contributors"][0]["name"], "Rohit")   # first-name rule only
            self.assertIn("about x", html_of(rep))

    def test_no_people_flag_leaves_names_alone(self):
        r = multi_repo("a", [dev("Ada Lovelace", "ada", {"Published": ["x"]})])
        self.assertEqual(run_report([r], people=None)["repos"][0]["contributors"][0]["name"], "Ada")


class BotTests(unittest.TestCase):
    def test_bot_work_is_shown_without_a_tag_or_a_count(self):
        r = multi_repo("a", [dev("Ada Lovelace", "ada", {"Published": ["human work"]}, 2),
                             dev("Claude", "claude", {"Published": ["bot work", "bot extra"]}, 7)])
        active = R.apply_people([r], CHECKED_IN)
        codex = codex_for("Lascade-Co/a", done=[{"text": "Bot did it", "from": ["R0.P2"]},
                                                {"text": "Both did it", "from": ["R0.P1", "R0.P2"]}])
        rep = R.merge(active, codex)
        h = html_of(rep)
        rp = rep["repos"][0]
        self.assertIn("Bot did it", h)
        self.assertIn("bot extra", h)                               # uncovered: still shown, verbatim
        self.assertNotIn("Claude", h)
        self.assertEqual([b["authors"] for b in rp["groups"][0]["bullets"]],
                         [[], [{"name": "Ada", "key": "ada lovelace"}]])
        self.assertEqual([a["author"] for a in rp["groups"][0]["also"]], [None])
        self.assertEqual([p["name"] for p in rp["contributors"]], ["Ada"])
        self.assertEqual(rp["commit_count"], 2)
        self.assertEqual(rep["stats"], {"commits": 2, "repos_active": 1, "contributors": 1})

    def test_repo_with_only_bot_work_still_renders_its_bullets(self):
        r = multi_repo("a", [dev("Claude", "claude", {"Published": ["only bot"]}, 3)])
        rep = R.merge(R.apply_people([r], CHECKED_IN), {}, assessed=False)
        h = html_of(rep)
        self.assertIn("only bot", h)
        self.assertNotIn("Claude", h)
        self.assertIn("</h2>", h)
        self.assertEqual(rep["repos"][0]["contributors"], [])

    def test_checked_in_map_hides_both_known_bots(self):
        r = multi_repo("a", [dev("Deploy", "Lascadesevices", {"Published": ["deploy work"]}),
                             dev("Claude", "claude", {"Published": ["claude work"]})])
        h = html_of(run_report([r]))
        self.assertIn("about deploy work", h)
        self.assertIn("about claude work", h)
        self.assertNotIn("Deploy</span>", h)
        self.assertNotIn("Claude</span>", h)
        self.assertNotIn("tag-", h.split("</head>")[1])


class NameTests(unittest.TestCase):
    def test_canonical_name_is_shown_as_is_and_a_first_name_clash_shows_full_names(self):
        r = multi_repo("a", [dev("Rohit T P", "rohittp0", {"Published": ["x"]}),
                             dev("Rohit Sharma", "rsharma", {"Published": ["y"]}),
                             dev("Ada Lovelace", "ada", {"Published": ["z"]})])
        rep = R.merge(R.apply_people([r], CHECKED_IN), {}, assessed=False)
        self.assertEqual([c["name"] for c in rep["repos"][0]["contributors"]],
                         ["Rohit", "Rohit Sharma", "Ada"])

    def test_a_canonical_name_with_a_space_is_not_cut_to_its_first_word(self):
        people = people_of({"name": "Mary Ann", "aliases": ["ma"]})
        r = multi_repo("a", [dev("ma", "x", {"Published": ["x"]})])
        rep = R.merge(R.apply_people([r], people), {}, assessed=False)
        self.assertEqual(rep["repos"][0]["contributors"][0]["name"], "Mary Ann")

    def test_unmapped_person_gets_a_grey_tag_with_their_name(self):
        r = multi_repo("a", [dev("Ada Lovelace", "ada", {"Published": ["x"]})])
        rep = R.merge(R.apply_people([r], CHECKED_IN), codex_for("Lascade-Co/a", done=[{"text": "T", "from": ["R0.P1"]}]))
        h = html_of(rep, pins=E.load_pins(PEOPLE_FILE))
        self.assertGreaterEqual(len(tag_classes(h, "Ada")), 2)
        self.assertEqual(set(tag_classes(h, "Ada")), {"tag-grey"})

    def test_riyan_and_ryyan_are_different_people_with_different_colours(self):
        r = multi_repo("a", [dev("Muhammed Riyan", "Riyaaaaan", {"Published": ["x"]}, 2),
                             dev("Ryyan Safar", "ryyansafar", {"Published": ["y"]}, 1)])
        rep = R.merge(R.apply_people([r], CHECKED_IN), {}, assessed=False)
        self.assertEqual([c["name"] for c in rep["repos"][0]["contributors"]], ["Riyan", "Ryyan"])
        h = html_of(rep, pins=E.load_pins(PEOPLE_FILE))
        self.assertNotEqual(tag_classes(h, "Riyan")[0], tag_classes(h, "Ryyan")[0])
        pins = E.load_pins(PEOPLE_FILE)
        self.assertNotEqual(pins["riyan"], pins["ryyan"])

    def test_checked_in_people_file_is_well_formed(self):
        with open(PEOPLE_FILE) as fh:
            data = json.load(fh)
        people = data["people"]
        self.assertTrue(people)
        names = [p.get("name", "").strip() for p in people]
        self.assertTrue(all(names), "every entry needs a name")
        self.assertEqual(len({n.lower() for n in names}), len(names), "duplicate name")
        colours = [p["colour"] for p in people if "colour" in p]
        self.assertTrue(all(type(c) is int and 0 <= c < len(E.TAG_PALETTE) for c in colours))
        self.assertEqual(len(set(colours)), len(colours), "two people share a colour")
        owners = {}
        for p in people:
            for a in [p["name"]] + p.get("aliases", []):
                owners.setdefault(a.strip().lower(), set()).add(p["name"])
        self.assertEqual({a: o for a, o in owners.items() if len(o) > 1}, {}, "alias under two people")
        bots = data["bots"]
        self.assertTrue(bots and all(isinstance(b, str) and b.strip() for b in bots))
        self.assertFalse(set(owners) & CHECKED_IN[1], "a bot is also listed as a person")


class RenderTests(unittest.TestCase):
    def test_authors_line_top_five_then_more(self):
        people = [{"name": f"P{i}", "commits": 9 - i, "key": f"p{i}"} for i in range(7)]
        bits = E.author_bits(people)
        self.assertEqual(bits[-1], ("+2 more", None))
        self.assertIn(("P4 5", "p4"), bits)
        self.assertNotIn(("P5 4", "p5"), bits)

    def test_html_escaping(self):
        r = repo("a", {"Published": ["<script>x</script>"]})
        h = html_of(R.merge([r], {}, assessed=False))
        self.assertNotIn("<script>x", h)
        self.assertIn("&lt;script&gt;", h)

    def test_repo_with_only_authors_still_renders(self):
        self.assertNotEqual(E.render_repo({"display_name": "X", "contributors": [{"name": "A", "commits": 1}]}), "")
        self.assertEqual(E.render_repo({"display_name": "X", "contributors": []}), "")

    def test_org_label_defaults_to_lascade(self):
        with tempfile.TemporaryDirectory() as d:
            rep, out = os.path.join(d, "report.json"), os.path.join(d, "email.html")
            with open(rep, "w") as fh:
                json.dump({"date": "2026-09-24", "headline": "h", "assessed": True, "repos": []}, fh)
            with mock.patch.object(sys, "argv", ["x", "--report", rep, "--out", out]), \
                    contextlib.redirect_stdout(open(os.devnull, "w")):
                E.main()
            with open(out) as fh:
                h = fh.read()
        self.assertIn("<title>Lascade Daily Report</title>", h)
        self.assertIn(">Lascade Daily Report</h1>", h)

    def test_people_flag_supplies_the_colours(self):
        with tempfile.TemporaryDirectory() as d:
            rep, out = os.path.join(d, "report.json"), os.path.join(d, "email.html")
            with open(rep, "w") as fh:
                json.dump({"date": "2026-09-24", "headline": "h", "assessed": True, "repos": [
                    {"display_name": "X", "contributors": [{"name": "Rohit", "commits": 3, "key": "rohit"},
                                                           {"name": "Ada", "commits": 1, "key": "ada lovelace"}]}]}, fh)
            with mock.patch.object(sys, "argv", ["x", "--report", rep, "--people", PEOPLE_FILE, "--out", out]), \
                    contextlib.redirect_stdout(open(os.devnull, "w")):
                E.main()
            with open(out) as fh:
                h = fh.read()
        self.assertEqual(tag_classes(h, "Rohit"), ["tag-c0"])
        self.assertEqual(tag_classes(h, "Ada"), ["tag-grey"])


class FactsLineTests(unittest.TestCase):
    def facts(self, version=None, prs=0):
        r = repo("a", {"Published": ["p"]})
        r["version"] = version
        r["prs"] = [{"number": n, "title": "t", "author": "x"} for n in range(prs)]
        h = html_of(R.merge([r], {}, assessed=False))
        m = re.search(r'</h2><p style="[^"]*" class="text-muted">([^<]*)</p>', h)
        return m.group(1) if m else None, h

    def test_version_only(self):
        self.assertEqual(self.facts(version="v4.0.40")[0], "v4.0.40")

    def test_prs_only(self):
        self.assertEqual(self.facts(prs=3)[0], "3 PRs merged")

    def test_both(self):
        self.assertEqual(self.facts(version="v4.0.40", prs=3)[0], "v4.0.40 · 3 PRs merged")

    def test_singular_pr(self):
        self.assertEqual(self.facts(prs=1)[0], "1 PR merged")

    def test_neither_means_no_line(self):
        facts, h = self.facts()
        self.assertIsNone(facts)
        self.assertRegex(h, r'</h2><p[^>]*class="status-done">Done</p>')     # straight into the first group

    def test_escaped(self):
        facts, h = self.facts(version="<b>1</b>")
        self.assertEqual(facts, "&lt;b&gt;1&lt;/b&gt;")
        self.assertNotIn("<b>1", h)

    def test_it_is_a_quiet_13px_grey_line_and_there_are_no_branch_chips(self):
        r = repo("a", {"Published": ["p"]})
        r["version"], r["branches"] = "v1", ["feature/secret-branch-name"]
        h = html_of(R.merge([r], {}, assessed=False))
        self.assertRegex(h, r'</h2><p style="[^"]*font-size:13px;[^"]*color:#666666;[^"]*" class="text-muted">v1</p>')
        self.assertNotIn("secret-branch-name", h)


class AuthorTests(unittest.TestCase):
    def test_bullet_authors_come_from_cited_ids_deduped_in_order(self):
        r = multi_repo("a", [dev("Ada Lovelace", "ada", {"Published": ["x", "y"]}),
                             dev("Bob Ray", "bob", {"Published": ["z"]})])
        c = codex_for("Lascade-Co/a", done=[
            {"text": "Both", "from": ["R0.P3", "R0.P1", "R0.P2"]},
            {"text": "Bob only", "from": ["R0.P3", "R9.P9"]},
            {"text": "Nobody", "from": ["R9.P9"]}])
        bullets = R.merge([r], c)["repos"][0]["groups"][0]["bullets"]
        ada = {"name": "Ada", "key": "ada lovelace"}
        bob = {"name": "Bob", "key": "bob ray"}
        self.assertEqual([(b["text"], b["authors"]) for b in bullets],
                         [("Both", [ada, bob]), ("Bob only", [bob])])

    def test_first_name_collision_uses_full_names(self):
        r = multi_repo("a", [dev("Alex Smith", "as", {"Published": ["x"]}, 3),
                             dev("Alex Jones", "aj", {"Published": ["y"]}, 2)])
        c = codex_for("Lascade-Co/a", done=[{"text": "T", "from": ["R0.P1", "R0.P2"]}])
        rep = R.merge([r], c)["repos"][0]
        authors = rep["groups"][0]["bullets"][0]["authors"]
        self.assertEqual([a["name"] for a in authors], ["Alex Smith", "Alex Jones"])
        self.assertEqual([x["name"] for x in rep["contributors"]], ["Alex Smith", "Alex Jones"])
        self.assertEqual(len({a["key"] for a in authors}), 2)          # different people, different colour keys

    def test_names_render_escaped_as_tags(self):
        r = multi_repo("a", [dev("<b>Eve", "eve", {"Published": ["x"]})])
        c = codex_for("Lascade-Co/a", done=[{"text": "T", "from": ["R0.P1"]}])
        h = html_of(R.merge([r], c))
        self.assertIn("&lt;b&gt;Eve", h)
        self.assertNotIn("<b>Eve", h)
        self.assertRegex(h, r'class="tag-(?:c\d|grey)">&lt;b&gt;Eve')


class ColourTests(unittest.TestCase):
    def rendered(self, devs, codex_done=None, name="a", pins=None):
        r = multi_repo(name, devs)
        ids = [f"R0.P{i + 1}" for i in range(sum(len(d["bullets"].get("Published", [])) for d in devs))]
        c = codex_for(f"Lascade-Co/{name}", done=codex_done or [{"text": "T", "from": ids[:1]}])
        return html_of(R.merge([r], c), pins=pins)

    def test_unpinned_people_are_grey_never_hashed(self):
        self.assertIsNone(E.slot_for("ada lovelace"))
        self.assertIsNone(E.slot_for("ada lovelace", {}))
        self.assertIsNone(E.slot_for("ada lovelace", {"someone else": 3}))
        h = self.rendered([dev("Ada Lovelace", "ada", {"Published": ["x"]})])
        self.assertEqual(set(tag_classes(h, "Ada")), {"tag-grey"})

    def test_a_pinned_person_keeps_one_colour_across_repos_and_reports(self):
        pins = {"ada lovelace": 5}
        a = [dev("Ada Lovelace", "ada", {"Published": ["x"]})]
        b = [dev("Ada Lovelace", "ada", {"Published": ["x"]}), dev("Bob Ray", "bob", {"Published": ["y"]})]
        first = tag_classes(self.rendered(a, pins=pins), "Ada")
        other = tag_classes(self.rendered(b, name="b", pins=pins), "Ada")
        self.assertTrue(first)
        self.assertEqual(set(first) | set(other), {"tag-c5"})

    def test_login_lookup_failure_does_not_change_the_colour(self):
        pins = {"ada lovelace": 2}
        with_login = [dev("Ada Lovelace", "ada", {"Published": ["x"]})]
        no_login = [dev("Ada Lovelace", None, {"Published": ["x"]})]
        self.assertEqual(set(tag_classes(self.rendered(with_login, pins=pins), "Ada")),
                         set(tag_classes(self.rendered(no_login, pins=pins), "Ada")))

    def test_missing_name_falls_back_to_team_without_crashing(self):
        h = self.rendered([dev(None, None, {"Published": ["x"]})])
        self.assertEqual(set(tag_classes(h, "Team")), {"tag-grey"})
        rep = R.merge([multi_repo("a", [dev("", None, {"Published": ["x"]})])], {}, assessed=False)
        self.assertEqual(rep["repos"][0]["contributors"][0]["key"], "team")

    def test_same_first_name_gets_different_colour_keys(self):
        rep = R.merge([multi_repo("a", [dev("Alex Smith", "as", {"Published": ["x"]}),
                                        dev("Alex Jones", "aj", {"Published": ["y"]})])], {}, assessed=False)
        keys = [c["key"] for c in rep["repos"][0]["contributors"]]
        self.assertEqual(sorted(keys), ["alex jones", "alex smith"])

    def test_bullet_authors_dedupe_by_identity_not_display_name(self):
        r = multi_repo("a", [dev("Sam Lee", "s1", {"Published": ["x"]}), dev("Sam Lee", "s2", {"Published": ["y"]})])
        c = codex_for("Lascade-Co/a", done=[{"text": "T", "from": ["R0.P1", "R0.P2"]}])
        authors = R.merge([r], c)["repos"][0]["groups"][0]["bullets"][0]["authors"]
        self.assertEqual(len(authors), 2)

    def test_one_person_has_one_colour_in_bullets_also_items_and_count_line(self):
        devs = [dev("Ada Lovelace", "ada", {"Published": ["x", "y"]}, 2)]
        h = self.rendered(devs, codex_done=[{"text": "T", "from": ["R0.P1"]}], pins={"ada lovelace": 4})
        self.assertIn("Also (technical detail)", h)
        classes = tag_classes(h, "Ada")
        self.assertGreaterEqual(len(classes), 3)                                  # bullet, also, count line
        self.assertEqual(set(classes), {"tag-c4"})

    def test_pins_are_validated(self):
        self.assertEqual(E.slot_for("ada", {"ada": 3}), 3)
        for bad in (99, -1, True, "3", None, 2.0):
            self.assertIsNone(E.slot_for("ada", {"ada": bad}), bad)      # out of range or wrong type: grey

    def test_load_pins_reads_the_people_file_by_lowercased_canonical_name(self):
        with tempfile.TemporaryDirectory() as d, contextlib.redirect_stderr(open(os.devnull, "w")):
            def write(text):
                path = os.path.join(d, "people.json")
                with open(path, "w") as fh:
                    fh.write(text)
                return path
            good = write(json.dumps({"people": [
                {"name": " Austin Sarner ", "colour": 2}, {"name": "bad", "colour": 99},
                {"name": "worse", "colour": "x"}, {"name": "flag", "colour": True},
                {"name": "neg", "colour": -1}, {"name": "none"}, {"colour": 1}, 5, None]}))
            self.assertEqual(E.load_pins(good), {"austin sarner": 2})
            for bad in ("{nope", "[1]", '{"people": "x"}', '{"people": [1]}'):
                self.assertEqual(E.load_pins(write(bad)), {})
            self.assertEqual(E.load_pins(os.path.join(d, "missing.json")), {})
            self.assertEqual(E.load_pins(None), {})

    def test_checked_in_regulars_each_get_their_own_colour(self):
        pins = E.load_pins(PEOPLE_FILE)
        self.assertEqual(len(pins), 8)
        self.assertEqual(len(set(pins.values())), 8)
        self.assertEqual(pins["cherian"], 6)                            # gold, as in the Impending email
        for other in ("fasna", "angel", "shaan", "ada lovelace"):
            self.assertIsNone(E.slot_for(other, pins))


class DesignTests(unittest.TestCase):
    PINS = {f"person{i} x": i for i in range(8)}

    def fixtures(self):
        many = [dev(f"Person{i} X", f"p{i}", {"Published": [f"s{i}"], "Testing": ["t"],
                                               "Work in Progress": ["w"]}, 9 - i) for i in range(8)]
        many.append(dev("Grey Person", "g", {"Published": ["s"], "Testing": ["t"], "Work in Progress": ["w"]}, 1))
        big = R.merge([multi_repo("big", many)], {}, assessed=False)
        ok = R.merge([repo("a", {"Published": ["p"]})], {"repos": [], "decisions_needed": []})
        warn = R.merge([repo("a", {"Published": ["p"]})],
                       {"repos": [], "decisions_needed": ["Pick one"]})
        long_name = R.merge([multi_repo("a", [dev("Bartholomew-Maximilian Featherstonehaugh", "b",
                                                  {"Published": ["p"]})])], {}, assessed=False)
        return [big, ok, warn, long_name]

    def test_style_blocks_stay_under_gmail_limit(self):
        for rep in self.fixtures():
            self.assertEqual(E.oversized_style_blocks(html_of(rep)), [])
        self.assertLess(max(len(m) for m in re.findall(
            r"<style[^>]*>(.*?)</style>", html_of(self.fixtures()[1]), re.S)), 7500)

    def test_dark_rules_exist_in_both_hooks_for_every_class_used(self):
        h = "".join(html_of(rep, pins=self.PINS) for rep in self.fixtures())
        dark, ogsc = dark_css(h), ogsc_css(h)
        self.assertEqual(dark, ogsc)                       # same rules under prefers-color-scheme and [data-ogsc]
        body = h.split("</head>")[1]
        used = {c for attr in re.findall(r'class="([^"]+)"', body) for c in attr.split()}
        self.assertIn("tag-grey", used)
        for cls in used:
            if cls.startswith(("tag-", "status-", "callout-")):
                self.assertIn(cls, dark, cls)
                self.assertIn(cls, ogsc, cls)
        self.assertIn("tag-grey", dark)
        self.assertIn("tag-grey", ogsc)

    def test_needs_you_hidden_on_a_quiet_day_but_shown_otherwise(self):
        _, ok, warn, _ = self.fixtures()
        self.assertNotIn("Needs you", html_of(ok))
        self.assertIn("#faf3e6", html_of(warn))
        self.assertIn("Pick one", html_of(warn))
        unassessed = html_of(R.merge([repo("a", {"Published": ["p"]})], {}, assessed=False))
        self.assertIn("#ece9e6", unassessed)
        self.assertIn("Couldn't be assessed", unassessed)

    def test_callout_headings_meet_contrast_in_light_and_dark(self):
        _, _, warn, _ = self.fixtures()
        unassessed = R.merge([repo("a", {"Published": ["p"]})], {}, assessed=False)
        for rep in (warn, unassessed):
            h = html_of(rep)
            head, cls = re.search(r'color:(#[0-9a-f]{6});" class="(callout-[\w-]+-head)">Needs you', h).groups()
            bg_light = re.search(r'<td bgcolor="(#[0-9a-f]{6})"', h).group(1)
            dark = dark_css(h)
            bg_dark = colour(dark[cls.replace("-head", "-bg")], "background-color")
            self.assertGreaterEqual(contrast(head, bg_light), 4.5, cls)
            self.assertGreaterEqual(contrast(colour(dark[cls]), bg_dark), 4.5, cls)

    def test_status_bullets_carry_their_marker_and_colour(self):
        r = repo("a", {"Published": ["p"], "Testing": ["t"], "Work in Progress": ["w"]})
        c = codex_for("Lascade-Co/a", done=[{"text": "D", "from": ["R0.P1"]}],
                      testing=[{"text": "T", "from": ["R0.T1"]}],
                      in_progress=[{"text": "W", "from": ["R0.W1"]}])
        h = html_of(R.merge([r], c))
        for cls, marker, label in (("status-done", "●", "Done"), ("status-testing", "◐", "Testing"),
                                   ("status-wip", "○", "In progress")):
            self.assertRegex(h, rf'aria-hidden="true"[^>]*class="{cls}">{marker}</span>')
            self.assertIn(f'class="{cls}">{label}</p>', h)                  # coloured label
            self.assertNotIn(f'text-muted">{label}</p>', h)                 # muted class would override it in dark
        self.assertEqual(h.count("<ul"), h.count('<ul role="list"'))       # list semantics survive list-style:none

    def test_also_items_keep_the_group_marker_but_are_muted(self):
        r = repo("a", {"Testing": ["only source"]})
        h = html_of(R.merge([r], codex_for("Lascade-Co/a")))                # nothing cited: shown as "Also"
        self.assertRegex(h, r'color:#666666;[^"]*" class="text-muted"><li[^>]*><span aria-hidden="true"'
                            r'[^>]*class="status-testing">◐</span>only source')

    def test_every_tag_and_status_colour_meets_contrast_in_light_and_dark(self):
        h = html_of(R.merge([multi_repo("big", [
            dev(f"P{i} X", f"l{i}", {"Published": [f"s{i}"], "Testing": ["t"], "Work in Progress": ["w"]})
            for i in range(8)] + [dev("Grey Person", "g", {"Published": ["s"]})])], {}, assessed=False),
            pins={f"p{i} x": i for i in range(8)})
        dark = dark_css(h)
        page_dark = colour(dark["email-body-bg"], "background-color")
        tags = re.findall(r'background-color:(#[0-9a-f]{6});color:(#[0-9a-f]{6});[^"]*" class="(tag-(?:c\d|grey))"', h)
        self.assertEqual({t[2] for t in tags}, {f"tag-c{i}" for i in range(8)} | {"tag-grey"})   # whole palette + grey
        for bg, fg, cls in tags:
            self.assertGreaterEqual(contrast(fg, bg), 4.5, f"{cls} light")
            rule = dark[cls]
            self.assertGreaterEqual(contrast(colour(rule), colour(rule, "background-color")), 4.5, f"{cls} dark")
        statuses = re.findall(r'color:(#[0-9a-f]{6});" class="(status-\w+)"', h)
        self.assertEqual({s[1] for s in statuses}, {"status-done", "status-testing", "status-wip"})
        for fg, cls in statuses:
            self.assertGreaterEqual(contrast(fg, PAGE_LIGHT), 4.5, f"{cls} light")
            self.assertGreaterEqual(contrast(colour(dark[cls]), page_dark), 4.5, f"{cls} dark")

    def test_grey_tag_uses_the_specified_colours(self):
        h = html_of(R.merge([repo("a", {"Published": ["p"]})], {}, assessed=False))
        self.assertRegex(h, r'background-color:#e8e6e3;color:#555555;[^"]*" class="tag-grey"')
        self.assertEqual(dark_css(h)["tag-grey"], "background-color:#3a3835 !important;color:#e0ddd8 !important")

    def test_product_heading_shows_the_escaped_icon(self):
        rep = R.merge([repo("a", {"Published": ["p"]})], {}, assessed=False, icons={"Lascade-Co/a": "🤖"})
        self.assertRegex(html_of(rep), r"<h2[^>]*>🤖 a</h2>")
        rep["repos"][0]["emoji"] = "<b>"
        self.assertRegex(html_of(rep), r"<h2[^>]*>&lt;b&gt; a</h2>")
        del rep["repos"][0]["emoji"]
        self.assertRegex(html_of(rep), r"<h2[^>]*>a</h2>")                 # no emoji, no stray space

    def test_large_report_renders_under_size_limit(self):
        h = html_of(self.fixtures()[0])
        self.assertLess(len(h.encode()), E.MAX_EMAIL_BYTES)

    def test_repeated_styles_live_on_the_list_not_on_every_item(self):
        r = repo("a", {"Published": ["p", "q"]})
        h = html_of(R.merge([r], codex_for("Lascade-Co/a", done=[{"text": "D", "from": ["R0.P1"]}])),
                    pins={"ada lovelace": 0})
        lists = re.findall(r'<ul role="list" style="([^"]*)" class="(text-main|text-muted)">', h)
        self.assertEqual([c for _, c in lists], ["text-main", "text-muted"])      # bullets, then the muted "Also" list
        for style, cls in lists:
            for prop in ("font-size:16px", "letter-spacing:0.01em"):
                self.assertIn(prop, style)
            self.assertIn("color:#222222" if cls == "text-main" else "color:#666666", style)
        # The font family is declared once, on the product's cell, and inherited by everything below the heading.
        self.assertRegex(h, r'<td style="padding:20px 40px 0 40px;font-family:[^;"]*;" class="body-cell"><h2')
        below_heading = h.split("</h2>")[1].split("</td></tr></table>")[0]
        self.assertIn("<ul", below_heading)
        self.assertNotIn("font-family", below_heading)
        for style in re.findall(r'<li style="([^"]*)"', h):
            for prop in ("font-family", "font-size", "letter-spacing", "color"):
                self.assertNotIn(prop, style)
        for style in re.findall(r'<span style="([^"]*)" class="tag-', h):
            self.assertNotIn("font-family", style)
            for prop in ("display:inline-block", "font-size:11px", "font-weight:500", "line-height:1.4",
                         "padding:2px 7px", "border-radius:4px", "background-color:", ";color:", "white-space:nowrap"):
                self.assertIn(prop, style)
        markers = re.findall(r'<span aria-hidden="true" style="([^"]*)" class="status-', h)
        self.assertEqual(len(markers), 2)
        for style in markers:
            for prop in ("display:inline-block", "width:22px", "margin-left:-22px", "color:#"):
                self.assertIn(prop, style)


def heaviest_day():
    """Synthetic replica of the heaviest real day: 15 products, 18 people, 63 work items
    spread across Done / Testing / In progress, three contributors per product, and
    a third of the items left uncited so they show as verbatim "Also" lines."""
    people = [f"Person{n:02d} Tester" for n in range(18)]
    statuses = ["Published", "Testing", "Work in Progress"]
    topics = ["checkout flow", "map export", "offline sync", "settings screen", "push reminders",
              "route sharing", "search results", "onboarding tour", "billing page", "photo upload"]
    active, item = [], 0
    for ri in range(15):
        devs = []
        for k in range(3):
            devs.append({"name": people[(ri * 3 + k) % 18], "login": f"login{(ri * 3 + k) % 18}",
                         "commit_count": 3 + k, "bullets": {}})
        for j in range(5 if ri < 3 else 4):
            d = devs[j % 3]
            topic = topics[item % len(topics)]
            filler = " and ".join(["tightened the retry handling"] * (1 + item % 3))
            text = f"🚀 Reworked the {topic} to {filler} across iOS and Android builds ({item})"
            d["bullets"].setdefault(statuses[item % 3], []).append(text)
            item += 1
        active.append({"repo": f"Lascade-Co/product-{ri:02d}", "developers": devs,
                       "prs": [{"number": n, "title": "t", "author": "x"} for n in range(ri % 4)],
                       "branches": ["feature/x"], "version": f"v4.0.{ri}" if ri % 2 else None})
    assert item == 63
    codex = {"headline": "A busy day across every product.", "decisions_needed": [], "repos": []}
    seen = 0
    for ri, r in enumerate(R.build_codex_payload("2026-09-18", active)["repos"]):
        entry = {"repo": r["repo"], "display_name": f"Product number {ri:02d}"}
        for label, key in (("Done", "done"), ("Testing", "testing"), ("In progress", "in_progress")):
            entry[key] = []
            for w in r["work"].get(label, []):
                seen += 1
                if seen % 3:                        # cite two of every three; the third becomes "Also"
                    entry[key].append({"text": f"Improved the {w['text'].split()[3]} experience for travellers",
                                       "from": [w["id"]]})
        codex["repos"].append(entry)
    return active, codex


class ScaleTests(unittest.TestCase):
    def test_heaviest_day_renders_under_the_email_size_limit(self):
        """A synthetic Sep-18-shaped day (15 products, 18 people, 63 items, a third as "Also")
        renders to 83,516 bytes against MAX_EMAIL_BYTES (90,000).

        A real day over about 102 KB would be clipped by Gmail behind "View entire message":
        the content is hidden, not deleted, and the renderer's ::warning:: flags it. Splitting
        into two emails is a follow-up only if that warning ever fires.
        """
        active, codex = heaviest_day()
        rep = R.merge(active, codex)
        self.assertEqual(sum(len(g["bullets"]) + len(g["also"]) for r in rep["repos"] for g in r["groups"]), 63)
        self.assertEqual(len({c["key"] for r in rep["repos"] for c in r["contributors"]}), 18)
        h = html_of(rep, pins={f"person{n:02d} tester": n for n in range(8)})
        size = len(h.encode("utf-8"))
        self.assertLess(size, E.MAX_EMAIL_BYTES, size)
        self.assertEqual(E.oversized_style_blocks(h), [])
        self.assertIn("Also (technical detail)", h)


if __name__ == "__main__":
    unittest.main()
