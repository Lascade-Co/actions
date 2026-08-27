# Release Blog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On every Android release, turn the release diff into an SEO-validated, human-sounding HTML draft in the site's WordPress CMS, without ever affecting whether the release ships.

**Architecture:** A reusable `workflow_call` workflow (`release-blog.yml`) that holds no business logic — it mints a token, checks out the released tag, installs Python and Codex, `curl`s the scripts, and makes one CLI call. All logic lives in five flat sibling modules under `scripts/seo/`, which reuse the existing Blog SEO Audit's parser, thresholds and rule functions rather than re-implementing them. The CLI always exits 0.

**Tech Stack:** Python 3.13 (stdlib + `requests`, `beautifulsoup4`, `lxml`), GitHub Actions reusable workflows, Codex CLI (`codex exec`), WordPress REST API v2, Infisical.

**Spec:** `docs/superpowers/specs/2026-08-27-release-blog-design.md` — read it before Task 1. The vocabulary section and ADR-0011/ADR-0012 explain *why* several things below look deliberately half-done (a draft that publishes with known warnings; a rule excluded from validation despite being runnable).

**Docs already landed:** `CONTEXT.md` (Language — Release Blog), `docs/adr/0011-release-blog-creates-drafts.md`, `docs/adr/0012-pre-publish-validation-allowlist.md` — committed in `e102433`. Do not rewrite them; do read them.

## Global Constraints

- **Vocabulary is enforced in code identifiers and prose.** This pipeline creates a **draft**; an **editor** publishes it and it becomes a **blog**. There is no noun "release blog". Links into existing blogs are **contextual internal links**; the pool is **link candidates**. Never `backlink`, `stand-in`, `WordPress` (in identifiers — use `CMS`), or `post` (except the literal REST path).
- **The CLI always exits 0.** Every failure path logs and returns cleanly. No exception may escape `main()`.
- **Python 3.13.** Pinned deps, matching the audit exactly: `requests==2.34.2 beautifulsoup4==4.15.0 lxml==6.1.1`.
- **Flat sibling imports only** — `from seo_model import ...`, never `from scripts.seo.seo_model import ...`. The workflow `curl`s every module into one working directory (scripts-refactor spec, constraint 1).
- **Latest action versions**, per `CLAUDE.md`: `actions/checkout@v6`, `actions/setup-python@v7`, `actions/upload-artifact@v7`, `actions/create-github-app-token@v3`, `Infisical/secrets-action@v1.0.15`.
- **Scripts are fetched by raw URL** including the domain subfolder: `https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/seo/<name>`.
- **Do not modify** `seo_parse.py`, `seo_rulekit.py`, `seo_fetch.py`, `seo_report.py`, `seo_blog_audit.py`, or `seo_checks_abc.py` / `seo_checks_ghi.py`. The only edits to existing audit code are `seo_model.py` (Task 1) and `seo_checks_def.py` (Task 2).
- **Thresholds are never hardcoded.** Read them from `site.threshold(...)`; `GENERIC_ANCHOR_TEXT` comes from `seo_model`.
- **Tests use `unittest`** (the repo's convention: `unittest.TestCase` classes, run under `pytest`).
- Run tests from inside `scripts/seo/`: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`. The flat imports require it.

## File Structure

**Create:**

| File | Responsibility |
|---|---|
| `scripts/seo/release_blog_digest.py` | git reading, patch filtering, 200 KB cap, `marketing_version_from_tag()` |
| `scripts/seo/release_blog_cms.py` | `LinkCandidate`, candidate fetch + sitemap fallback, marker search, draft write |
| `scripts/seo/release_blog_check.py` | synthetic page wrap, 18-rule allowlist, local checks, attempt scoring |
| `scripts/seo/release_blog_draft.py` | prompt assembly, `codex exec`, output parsing |
| `scripts/seo/release_blog.py` | CLI entry, orchestration, artifacts, exit-0 guarantee |
| `scripts/seo/test_release_blog.py` | all tests for the five modules above |
| `scripts/seo/fixtures/release_blog_good.html` | a fragment that violates nothing |
| `scripts/seo/fixtures/release_blog_bad.html` | a fragment that violates several rules at once |
| `scripts/seo/fixtures/release_blog_candidates.json` | a CMS posts response |
| `scripts/seo/fixtures/release_blog_sample.diff` | a diff with noise to be filtered |
| `data/RELEASE_BLOG.md` | the Codex prompt, fetched verbatim like `RELEASE_NOTES.md` |
| `.github/workflows/release-blog.yml` | the reusable workflow |

**Modify:**

| File | Change |
|---|---|
| `scripts/seo/seo_model.py:39-90` | `SiteConfig.repos`, parse it, add `resolve_site_for_repo()` |
| `scripts/seo/seo_checks_def.py:479-499` | `check_d9`, `D9`, register in `BLOG_RULES_DEF` |
| `scripts/seo/test_checks_def.py` | three `D9` tests |
| `scripts/seo/test_seo_audit.py:80` | rule count `46` → `47` |
| `data/seo_sites.json` | `repos` per site |
| `.github/workflows/android-build-release.yml:221-351` | `release` job outputs + `blog` job |

**Task order and parallelism:** Tasks 1 and 2 are independent — run them in parallel. Tasks 3, 4 and 5 each depend only on Task 1 and are independent of each other — run all three in parallel. Task 6 needs 3 and 4. Task 7 needs 3–6. Task 8 needs 7.

---

### Task 1: `repos` in site config, and repo → site resolution

**Files:**
- Modify: `scripts/seo/seo_model.py:39-90`
- Modify: `data/seo_sites.json`
- Test: `scripts/seo/test_release_blog.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `SiteConfig.repos: tuple[str, ...]` — lowercased `owner/name` strings.
  - `resolve_site_for_repo(path: str, repo: str) -> tuple[SiteConfig | None, str]` — returns `(site, "")` on exactly one match, `(None, reason)` otherwise, where `reason` is a human-readable sentence for the log. Never raises for a missing or ambiguous match; still raises `FileNotFoundError` / `json.JSONDecodeError` if the config file itself is unreadable.

- [ ] **Step 1: Write the failing tests**

Create `scripts/seo/test_release_blog.py`:

```python
"""Tests for the Release Blog pipeline. No network, no model, no git."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from seo_model import resolve_site_for_repo, site_config_from_dict

CONFIG = [
    {
        "name": "travelanimator",
        "label": "Travel Animator",
        "canonical_host": "www.travelanimator.com",
        "origin_host": "hub.travelanimator.com",
        "origin_asset_prefixes": ["/wp-content/uploads/"],
        "allowed_subdomains": ["support.travelanimator.com"],
        "listing_path": "/hub",
        "sitemap_url": "https://www.travelanimator.com/sitemap.xml",
        "blog_count": 10,
        "cms_api": True,
        "repos": ["Lascade-Co/travel-animator-android"],
        "suppress": [],
        "thresholds": {},
    },
    {
        "name": "marineradar",
        "label": "MarineRadar",
        "canonical_host": "www.marineradar.com",
        "origin_host": "hub.marineradar.com",
        "origin_asset_prefixes": ["/wp-content/uploads/"],
        "allowed_subdomains": [],
        "listing_path": "/hub",
        "sitemap_url": "https://www.marineradar.com/sitemap.xml",
        "blog_count": 10,
        "cms_api": True,
        "repos": ["Lascade-Co/marine-radar-android"],
        "suppress": [],
        "thresholds": {},
    },
]


def write_config(entries) -> str:
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(entries, handle)
    handle.close()
    return handle.name


class SiteResolutionTest(unittest.TestCase):
    def test_repos_defaults_to_empty_when_absent(self):
        raw = dict(CONFIG[0])
        del raw["repos"]
        self.assertEqual(site_config_from_dict(raw).repos, ())

    def test_repos_are_lowercased(self):
        raw = dict(CONFIG[0], repos=["Lascade-Co/Travel-Animator-Android"])
        self.assertEqual(site_config_from_dict(raw).repos, ("lascade-co/travel-animator-android",))

    def test_resolves_a_single_match(self):
        site, reason = resolve_site_for_repo(write_config(CONFIG), "Lascade-Co/travel-animator-android")
        self.assertEqual(site.name, "travelanimator")
        self.assertEqual(reason, "")

    def test_match_is_case_insensitive(self):
        site, _ = resolve_site_for_repo(write_config(CONFIG), "lascade-co/TRAVEL-ANIMATOR-ANDROID")
        self.assertEqual(site.name, "travelanimator")

    def test_no_match_returns_a_reason(self):
        site, reason = resolve_site_for_repo(write_config(CONFIG), "Lascade-Co/unknown-app")
        self.assertIsNone(site)
        self.assertIn("no site config", reason)
        self.assertIn("Lascade-Co/unknown-app", reason)

    def test_two_matches_is_a_config_error(self):
        entries = json.loads(json.dumps(CONFIG))
        entries[1]["repos"] = ["Lascade-Co/travel-animator-android"]
        site, reason = resolve_site_for_repo(write_config(entries), "Lascade-Co/travel-animator-android")
        self.assertIsNone(site)
        self.assertIn("two site configs", reason)
        self.assertIn("travelanimator", reason)
        self.assertIn("marineradar", reason)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: FAIL — `ImportError: cannot import name 'resolve_site_for_repo' from 'seo_model'`

- [ ] **Step 3: Add `repos` to `SiteConfig`**

In `scripts/seo/seo_model.py`, add the field to the `SiteConfig` dataclass after `suppress` (a trailing default keeps the single existing construction site valid):

```python
    suppress: frozenset[str]
    thresholds: dict
    repos: tuple[str, ...] = ()
```

And in `site_config_from_dict`, after `thresholds=thresholds,`:

```python
        thresholds=thresholds,
        repos=tuple(str(repo).lower() for repo in (raw.get("repos") or ())),
```

- [ ] **Step 4: Add the resolver**

In `scripts/seo/seo_model.py`, directly below `load_site_config`:

```python
def resolve_site_for_repo(path: str, repo: str) -> tuple[SiteConfig | None, str]:
    """The one site config whose `repos` names this repo.

    Returns (site, "") on exactly one match and (None, reason) otherwise. Two
    matches is a config error, not a preference: picking one would let file
    order decide which brand gets the blog, invisibly. Both misses are
    non-fatal — the Release Blog pipeline logs the reason and exits 0.
    """
    wanted = repo.lower()
    with open(path, encoding="utf-8") as fh:
        entries = json.load(fh)
    matches = [site_config_from_dict(raw) for raw in entries if wanted in site_config_from_dict(raw).repos]
    if len(matches) == 1:
        return matches[0], ""
    if not matches:
        known = ", ".join(sorted(e["name"] for e in entries))
        return None, f"no site config names {repo!r} in its repos list (known sites: {known})"
    names = ", ".join(sorted(site.name for site in matches))
    return None, f"{repo!r} is named by two site configs ({names}) — config error, refusing to pick one"
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: PASS (6 tests)

- [ ] **Step 6: Verify no existing audit test regressed**

Run: `cd scripts/seo && python3 -m pytest . -q`
Expected: PASS — every pre-existing test still green. `test_seo_fetch.py` builds a `SiteConfig` from a dict with no `repos` key, which is exactly why the field is defaulted.

- [ ] **Step 7: Add `repos` to the real site config**

In `data/seo_sites.json`, add `"repos"` immediately after `"cms_api"` in each entry. Only the Android repos are wired now; the iOS entries are listed because the mapping is a property of the site, not of this pipeline, and a repo that never dispatches simply never matches:

```json
    "cms_api": true,
    "repos": [
      "Lascade-Co/travel-animator-android",
      "Lascade-Co/travel-animator-ios"
    ],
```

```json
    "cms_api": true,
    "repos": [
      "Lascade-Co/marine-radar-android",
      "Lascade-Co/marine-radar-ios"
    ],
```

Verify the repo names against the org before committing: `gh repo list Lascade-Co --limit 200 | grep -Ei 'travel-animator|marine'`. Fix any that differ — a wrong name here is a silent skip, the hardest failure in this pipeline to notice.

- [ ] **Step 8: Verify the real config parses and resolves**

Run:
```bash
cd scripts/seo && python3 -c "
from seo_model import resolve_site_for_repo
site, reason = resolve_site_for_repo('../../data/seo_sites.json', 'Lascade-Co/travel-animator-android')
print(site.name if site else reason)
"
```
Expected: `travelanimator`

- [ ] **Step 9: Commit**

```bash
git add scripts/seo/seo_model.py scripts/seo/test_release_blog.py data/seo_sites.json
git commit -m "feat(seo): map repos to site configs

A repo named by no site config produces nothing; a repo named by two is a
config error and also produces nothing, rather than letting file order decide
which brand gets the blog."
```

---

### Task 2: Audit rule `D9` — placeholder media on a published blog

**Files:**
- Modify: `scripts/seo/seo_checks_def.py`
- Modify: `scripts/seo/test_checks_def.py`
- Modify: `scripts/seo/test_seo_audit.py:80`

**Interfaces:**
- Consumes: nothing.
- Produces: `D9` in `BLOG_RULES_DEF`, therefore in `seo_checks.BLOG_RULES` and `RULES_BY_ID`. Rule id `"D9"`, slug `"placeholder-media-published"`, group `"D"`, blog-scoped.

**Why this exists:** verified 2026-08-27 that a `placehold.co` image returns 200 with `image/png`, so `B3` passes it, and no rule anywhere constrains an image's host. Without `D9` the Release Blog pipeline's likeliest real-world failure — a draft published with its stand-in images still in place — has no detector at all. `D9` must **not** be added to the pre-publish allowlist in Task 5: placeholders are correct in a draft. See ADR-0011 and ADR-0012.

- [ ] **Step 1: Write the failing tests**

Append to the `GroupDTest` class in `scripts/seo/test_checks_def.py`:

```python
    def test_d9_fires_on_placeholder_media(self):
        page = make_page(
            raw_html='<article><img src="https://placehold.co/1600x900/png" '
            'width="1600" height="900" data-placeholder="true" alt="The route editor"></article>'
        )
        findings = run_rule("D9", page)
        self.assertEqual(findings[0].severity, SEVERITY_ERROR)
        self.assertIn("data-placeholder", findings[0].message)

    def test_d9_fires_on_an_unstripped_editor_checklist(self):
        page = make_page(
            raw_html='<article><div class="release-blog-checklist" '
            'data-strip-before-publish="true">Replace these</div></article>'
        )
        self.assertEqual(run_rule("D9", page)[0].severity, SEVERITY_ERROR)

    def test_d9_silent_on_clean_html(self):
        page = make_page(raw_html='<article><img src="/real.png" alt="A real screenshot"></article>')
        self.assertEqual(run_rule("D9", page), [])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_checks_def.py -k d9 -v`
Expected: FAIL with `KeyError: 'D9'` from `run_rule`'s `RULES[rule_id]` lookup.

- [ ] **Step 3: Implement the check**

In `scripts/seo/seo_checks_def.py`, add near the other module constants at the top:

```python
PLACEHOLDER_MARKERS = ("data-placeholder", "data-strip-before-publish")
```

Add the check function directly after `check_d8`:

```python
def check_d9(page, site, urls, ctx):
    """Draft scaffolding that reached a published blog.

    The Release Blog pipeline emits sized `placehold.co` stand-ins and an editor
    checklist, both correct in an unpublished draft and both wrong the moment a
    page is live. Nothing else catches this: a placeholder image answers 200 with
    `image/png`, so B3 passes it, and no rule constrains an image's host. Reads
    raw_html rather than page.images because the checklist is a div, not an image.
    """
    return [
        finding(
            D9,
            SEVERITY_ERROR,
            f"published blog still carries draft scaffolding ({marker})",
            blog_url=page.url,
        )
        for marker in PLACEHOLDER_MARKERS
        if marker in page.raw_html
    ]
```

- [ ] **Step 4: Register the rule**

In `scripts/seo/seo_checks_def.py`, add to the rule definitions after `D8`:

```python
D9 = Rule("D9", "placeholder-media-published", "D", check_d9)
```

And add `D9` to the list:

```python
BLOG_RULES_DEF = [D1, D2, D3, D4, D5, D6, D8, D9, E1, E2, E3, E4, E5, E6, F1, F2, F3, F4]
```

- [ ] **Step 5: Run the D9 tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_checks_def.py -k d9 -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Fix the rule-count assertion**

`scripts/seo/test_seo_audit.py:80` asserts the registry size. Change `46` to `47`:

```python
        self.assertEqual(len(ALL_RULES), 47)
```

- [ ] **Step 7: Run the full audit suite**

Run: `cd scripts/seo && python3 -m pytest . -q`
Expected: PASS. `seo_report.py` needs no change — it groups findings by the rule id's first letter and `"D"` is already in `GROUPS`.

- [ ] **Step 8: Commit**

```bash
git add scripts/seo/seo_checks_def.py scripts/seo/test_checks_def.py scripts/seo/test_seo_audit.py
git commit -m "feat(seo): add D9, placeholder media on a published blog

A placehold.co image answers 200 with image/png, so B3 passes it and no rule
constrained an image host — draft scaffolding could reach a live blog entirely
undetected. Deliberately excluded from the Release Blog pre-publish allowlist:
placeholders are correct in a draft."
```

---

### Task 3: The release digest

**Files:**
- Create: `scripts/seo/release_blog_digest.py`
- Create: `scripts/seo/fixtures/release_blog_sample.diff`
- Test: `scripts/seo/test_release_blog.py` (append)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `marketing_version_from_tag(tag: str) -> str`
  - `filter_patch(patch: str) -> str`
  - `truncate(text: str, limit: int = PATCH_BYTE_CAP) -> str`
  - `build_digest(repo_path: str, base: str, head: str, notes: str, *, run=subprocess.run) -> str`
  - `PATCH_BYTE_CAP = 200 * 1024`
  - `previous_tag(repo_path: str, head: str, *, run=subprocess.run) -> str | None`

- [ ] **Step 1: Write the fixture**

Create `scripts/seo/fixtures/release_blog_sample.diff` — a unified diff carrying one real change plus one of each noise class the filter must drop:

```diff
diff --git a/app/src/main/res/values/strings.xml b/app/src/main/res/values/strings.xml
--- a/app/src/main/res/values/strings.xml
+++ b/app/src/main/res/values/strings.xml
@@ -12,3 +12,5 @@
   <string name="route_editor_title">Route editor</string>
+  <string name="waypoint_drag_hint">Drag a waypoint to reshape your route</string>
+  <string name="waypoint_undo">Undo move</string>
diff --git a/app/src/main/res/values-de/strings.xml b/app/src/main/res/values-de/strings.xml
--- a/app/src/main/res/values-de/strings.xml
+++ b/app/src/main/res/values-de/strings.xml
@@ -12,3 +12,4 @@
   <string name="route_editor_title">Routeneditor</string>
+  <string name="waypoint_drag_hint">Ziehen Sie einen Wegpunkt</string>
diff --git a/gradle.lockfile b/gradle.lockfile
--- a/gradle.lockfile
+++ b/gradle.lockfile
@@ -1,2 +1,2 @@
-androidx.core:core:1.13.0
+androidx.core:core:1.14.0
diff --git a/app/src/main/java/com/lascade/ta/editor/WaypointDragHandler.kt b/app/src/main/java/com/lascade/ta/editor/WaypointDragHandler.kt
--- a/app/src/main/java/com/lascade/ta/editor/WaypointDragHandler.kt
+++ b/app/src/main/java/com/lascade/ta/editor/WaypointDragHandler.kt
@@ -1,3 +1,9 @@
 class WaypointDragHandler {
+    fun onDragEnd(waypoint: Waypoint, target: LatLng) {
+        route.move(waypoint, target)
+        elevation.recompute()
+    }
 }
diff --git a/app/src/main/assets/logo.png b/app/src/main/assets/logo.png
Binary files a/app/src/main/assets/logo.png and b/app/src/main/assets/logo.png differ
diff --git a/lib/generated/l10n.g.dart b/lib/generated/l10n.g.dart
--- a/lib/generated/l10n.g.dart
+++ b/lib/generated/l10n.g.dart
@@ -1,2 +1,3 @@
 // GENERATED — do not edit
+String get waypointDragHint => 'Drag a waypoint';
```

- [ ] **Step 2: Write the failing tests**

Append to `scripts/seo/test_release_blog.py` (add `from release_blog_digest import ...` to the imports at the top of the file):

```python
from release_blog_digest import (
    PATCH_BYTE_CAP,
    build_digest,
    filter_patch,
    marketing_version_from_tag,
    truncate,
)
from seo_testkit import fixture


class MarketingVersionTest(unittest.TestCase):
    def test_bare_version_passes_through(self):
        self.assertEqual(marketing_version_from_tag("3.9.3"), "3.9.3")

    def test_leading_v_is_stripped(self):
        self.assertEqual(marketing_version_from_tag("v3.9.3"), "3.9.3")

    def test_non_version_tag_falls_back_to_the_raw_tag(self):
        self.assertEqual(marketing_version_from_tag("release-candidate"), "release-candidate")

    def test_empty_tag_is_empty(self):
        self.assertEqual(marketing_version_from_tag(""), "")


class FilterPatchTest(unittest.TestCase):
    def setUp(self):
        self.filtered = filter_patch(fixture("release_blog_sample.diff"))

    def test_keeps_the_real_source_change(self):
        self.assertIn("WaypointDragHandler.kt", self.filtered)
        self.assertIn("elevation.recompute()", self.filtered)

    def test_keeps_default_strings_because_they_are_the_new_ui_copy(self):
        self.assertIn("values/strings.xml", self.filtered)
        self.assertIn("Drag a waypoint to reshape your route", self.filtered)

    def test_drops_translated_strings(self):
        self.assertNotIn("values-de", self.filtered)
        self.assertNotIn("Routeneditor", self.filtered)

    def test_drops_lockfiles(self):
        self.assertNotIn("gradle.lockfile", self.filtered)

    def test_drops_generated_code(self):
        self.assertNotIn("l10n.g.dart", self.filtered)

    def test_drops_binary_hunks(self):
        self.assertNotIn("logo.png", self.filtered)


class TruncateTest(unittest.TestCase):
    def test_short_text_is_untouched(self):
        self.assertEqual(truncate("short"), "short")

    def test_long_text_is_capped_and_says_so(self):
        result = truncate("x" * 300, limit=100)
        self.assertTrue(result.startswith("x" * 100))
        self.assertIn("diff truncated", result)
        self.assertIn("100 of 300 bytes", result)

    def test_default_cap_is_200kb(self):
        self.assertEqual(PATCH_BYTE_CAP, 200 * 1024)


class BuildDigestTest(unittest.TestCase):
    def test_digest_carries_all_four_sections_in_order(self):
        calls = []

        class FakeCompleted:
            def __init__(self, stdout):
                self.stdout = stdout
                self.returncode = 0

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if "log" in cmd:
                return FakeCompleted("abc1234 feat: drag waypoints\ndef5678 fix: elevation strip\n")
            if "--stat" in cmd:
                return FakeCompleted(" 4 files changed, 12 insertions(+)\n")
            return FakeCompleted(fixture("release_blog_sample.diff"))

        digest = build_digest(
            "/tmp/repo", "v3.9.2", "v3.9.3", "- Drag waypoints to reshape routes\n", run=fake_run
        )
        self.assertLess(digest.index("Release notes"), digest.index("Commits"))
        self.assertLess(digest.index("Commits"), digest.index("Changed files"))
        self.assertLess(digest.index("Changed files"), digest.index("Filtered diff"))
        self.assertIn("Drag waypoints to reshape routes", digest)
        self.assertIn("feat: drag waypoints", digest)
        self.assertIn("4 files changed", digest)
        self.assertIn("WaypointDragHandler.kt", digest)
        self.assertNotIn("values-de", digest)

    def test_git_failure_degrades_to_an_empty_section(self):
        class FakeCompleted:
            stdout = ""
            returncode = 128

        digest = build_digest("/tmp/repo", "v3.9.2", "v3.9.3", "notes\n", run=lambda cmd, **kw: FakeCompleted())
        self.assertIn("notes", digest)
        self.assertIn("unavailable", digest)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'release_blog_digest'`

- [ ] **Step 4: Implement the module**

Create `scripts/seo/release_blog_digest.py`:

```python
"""The release digest — what the writer is told about a release.

Four sections: the release notes, the commit subjects, the diffstat, and a
filtered patch. It is the outer bound of what a draft may claim, so it is
assembled deliberately rather than piped in raw: a release diff between two
tags of a mobile app is mostly lockfile churn, generated code and the same new
string repeated in thirty languages, and that noise crowds out the handful of
lines that actually describe the feature.
"""

from __future__ import annotations

import re
import subprocess

PATCH_BYTE_CAP = 200 * 1024

VERSION_SHAPE = re.compile(r"^\d+(\.\d+)*$")

# One entry per noise class. Matched against the b-side path of each `diff --git`
# header, so a rename out of a dropped path is still dropped.
DROP_PATH_PATTERNS = (
    re.compile(r"\.lock$|\.lockb$|lockfile$", re.IGNORECASE),
    re.compile(r"(^|/)(build|generated|node_modules|Pods)/"),
    re.compile(r"\.pb\.(go|py|dart|swift|kt)$|\.g\.dart$|\.freezed\.dart$|_pb2\.py$"),
    re.compile(r"(^|/)values-[a-z]{2}(-[A-Za-z0-9]+)?/strings\.xml$"),
)

FILE_HEADER = re.compile(r"^diff --git a/(?P<a>\S+) b/(?P<b>\S+)$")


def marketing_version_from_tag(tag: str) -> str:
    """The user-visible version, derived from the tag.

    Android tags bare (`3.9.3`), Flutter tags `v3.9.3`. Anything that is not
    version-shaped comes back untouched: this value reaches the writer as
    context only, and the body contract keeps the version out of the title, so a
    surprising tag is not worth failing over.
    """
    candidate = tag[1:] if tag[:1].lower() == "v" else tag
    return candidate if VERSION_SHAPE.match(candidate) else tag


def _is_dropped(path: str) -> bool:
    return any(pattern.search(path) for pattern in DROP_PATH_PATTERNS)


def _keep_section(path: str | None, section: list[str]) -> bool:
    if not section:
        return False
    if path is not None and _is_dropped(path):
        return False
    # A binary section says only that bytes changed. Drop it whole.
    return not any(line.startswith("Binary files ") for line in section)


def filter_patch(patch: str) -> str:
    """Drop whole file sections that cannot inform a feature story.

    `values/strings.xml` is kept on purpose — new default strings are the literal
    words on the new screen, and they are the single best source of concrete
    detail in an Android diff. Only `values-<lang>/` is dropped.

    One pass, buffering a section at a time. A regex spanning whole file sections
    is the obvious alternative and backtracks catastrophically on a real diff —
    verified while writing this plan, on this fixture.
    """
    kept: list[str] = []
    path: str | None = None
    section: list[str] = []
    for line in patch.splitlines():
        header = FILE_HEADER.match(line)
        if header:
            if _keep_section(path, section):
                kept.extend(section)
            path, section = header.group("b"), [line]
            continue
        section.append(line)
    if _keep_section(path, section):
        kept.extend(section)
    text = "\n".join(kept).strip()
    return text + "\n" if text else ""


def truncate(text: str, limit: int = PATCH_BYTE_CAP) -> str:
    """Cap the patch, and say so. Silent truncation reads as a complete diff."""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    head = encoded[:limit].decode("utf-8", errors="ignore")
    return f"{head}\n\n[diff truncated — {limit} of {len(encoded)} bytes shown]\n"


def _git(run, repo_path: str, *args: str) -> str:
    try:
        result = run(
            ["git", "-C", repo_path, *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    return result.stdout if result.returncode == 0 else ""


def previous_tag(repo_path: str, head: str, *, run=subprocess.run) -> str | None:
    """The tag before `head`, for a local run that did not pass --base."""
    out = _git(run, repo_path, "describe", "--tags", "--abbrev=0", f"{head}^")
    return out.strip() or None


def build_digest(repo_path: str, base: str, head: str, notes: str, *, run=subprocess.run) -> str:
    span = f"{base}..{head}"
    commits = _git(run, repo_path, "log", "--oneline", "--no-merges", span)
    stat = _git(run, repo_path, "diff", "--stat", span)
    patch = filter_patch(_git(run, repo_path, "diff", span))
    return "\n".join(
        (
            "## Release notes",
            "",
            (notes.strip() or "unavailable"),
            "",
            "## Commits",
            "",
            "```",
            (commits.strip() or "unavailable"),
            "```",
            "",
            "## Changed files",
            "",
            "```",
            (stat.strip() or "unavailable"),
            "```",
            "",
            "## Filtered diff",
            "",
            "```diff",
            truncate(patch).strip() or "unavailable",
            "```",
            "",
        )
    )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: PASS (all tests from Tasks 1 and 3)

- [ ] **Step 6: Commit**

```bash
git add scripts/seo/release_blog_digest.py scripts/seo/fixtures/release_blog_sample.diff scripts/seo/test_release_blog.py
git commit -m "feat(seo): build the release digest from a filtered diff

Notes, commit subjects, diffstat and a filtered patch capped at 200KB. Drops
lockfiles, generated code, binaries and translated strings; keeps
values/strings.xml, which is where new UI copy actually lives."
```

---

### Task 4: CMS reads and the draft write

**Files:**
- Create: `scripts/seo/release_blog_cms.py`
- Create: `scripts/seo/fixtures/release_blog_candidates.json`
- Test: `scripts/seo/test_release_blog.py` (append)

**Interfaces:**
- Consumes: `SiteConfig` (Task 1).
- Produces:
  - `LinkCandidate` — dataclass with `title: str`, `url: str`, `slug: str`, `excerpt: str`.
  - `release_marker(repo: str, tag: str) -> str`
  - `fetch_link_candidates(site, *, http) -> tuple[list[LinkCandidate], str]` — `(candidates, note)`; `note` is `""` on a clean CMS read, otherwise a sentence naming the fallback or failure.
  - `find_by_marker(site, marker, *, http, auth) -> dict | None` — the matching post as `{"id": int, "status": str, "slug": str}`, or `None`.
  - `build_payload(meta: dict, html: str) -> dict`
  - `write_draft(site, payload, *, http, auth, post_id=None) -> tuple[bool, str]`
  - `requests_http(method, url, *, headers=None, json_body=None, auth=None, timeout=20)` — the production transport, returning `(status: int, body: str)`.

- [ ] **Step 1: Write the fixture**

Create `scripts/seo/fixtures/release_blog_candidates.json` — a WordPress posts response shaped exactly as `_fields=title,excerpt,link,slug,date` returns it:

```json
[
  {
    "slug": "route-animation-guide",
    "link": "https://www.travelanimator.com/hub/route-animation-guide",
    "title": { "rendered": "How to animate a road trip route" },
    "excerpt": { "rendered": "<p>Build a route animation from a GPX file in five steps.</p>\n" },
    "date": "2026-08-20T10:00:00"
  },
  {
    "slug": "best-travel-maps",
    "link": "https://www.travelanimator.com/hub/best-travel-maps",
    "title": { "rendered": "The best map styles for travel videos" },
    "excerpt": { "rendered": "<p>Six map styles that read well on a phone screen.</p>\n" },
    "date": "2026-08-14T10:00:00"
  },
  {
    "slug": "export-for-instagram",
    "link": "https://www.travelanimator.com/hub/export-for-instagram",
    "title": { "rendered": "Exporting for Instagram Reels" },
    "excerpt": { "rendered": "<p>Aspect ratios, bitrates and the safe area.</p>\n" },
    "date": "2026-08-02T10:00:00"
  }
]
```

- [ ] **Step 2: Write the failing tests**

Append to `scripts/seo/test_release_blog.py`:

```python
from release_blog_cms import (
    LinkCandidate,
    build_payload,
    fetch_link_candidates,
    find_by_marker,
    release_marker,
    write_draft,
)
from seo_testkit import make_site

MARKER = "release-blog: Lascade-Co/travel-animator-android@3.9.3"


class FakeHttp:
    """Records calls and replays queued (status, body) pairs."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, *, headers=None, json_body=None, auth=None, timeout=20):
        self.calls.append({"method": method, "url": url, "json_body": json_body, "auth": auth})
        return self.responses.pop(0) if self.responses else (500, "no response queued")


class ReleaseMarkerTest(unittest.TestCase):
    def test_marker_names_the_repo_and_tag(self):
        self.assertEqual(
            release_marker("Lascade-Co/travel-animator-android", "3.9.3"),
            "release-blog: Lascade-Co/travel-animator-android@3.9.3",
        )


class LinkCandidateTest(unittest.TestCase):
    def test_reads_title_url_and_excerpt_from_the_cms(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        candidates, note = fetch_link_candidates(make_site(), http=http)
        self.assertEqual(note, "")
        self.assertEqual(len(candidates), 3)
        self.assertEqual(candidates[0].title, "How to animate a road trip route")
        self.assertEqual(candidates[0].url, "https://www.travelanimator.com/hub/route-animation-guide")
        self.assertEqual(candidates[0].slug, "route-animation-guide")
        self.assertIn("GPX", candidates[0].excerpt)

    def test_excerpt_html_is_stripped(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        candidates, _ = fetch_link_candidates(make_site(), http=http)
        self.assertNotIn("<p>", candidates[0].excerpt)

    def test_requests_only_published_posts(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        fetch_link_candidates(make_site(), http=http)
        self.assertIn("status=publish", http.calls[0]["url"])
        self.assertIn("per_page=100", http.calls[0]["url"])
        self.assertIn("hub.travelanimator.com", http.calls[0]["url"])

    def test_falls_back_to_the_sitemap_when_the_cms_fails(self):
        sitemap = (
            '<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            "<url><loc>https://www.travelanimator.com/hub/route-animation-guide</loc></url>"
            "<url><loc>https://www.travelanimator.com/pricing</loc></url>"
            "</urlset>"
        )
        http = FakeHttp((503, "service unavailable"), (200, sitemap))
        candidates, note = fetch_link_candidates(make_site(), http=http)
        self.assertIn("sitemap", note)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].slug, "route-animation-guide")
        self.assertEqual(candidates[0].title, "")

    def test_returns_empty_when_both_sources_fail(self):
        http = FakeHttp((503, ""), (503, ""))
        candidates, note = fetch_link_candidates(make_site(), http=http)
        self.assertEqual(candidates, [])
        self.assertIn("no link candidates", note)


class MarkerSearchTest(unittest.TestCase):
    def test_finds_a_draft_carrying_the_marker(self):
        body = json.dumps(
            [{"id": 41, "status": "draft", "slug": "drag-waypoints", "content": {"rendered": f"<p>x</p><!-- {MARKER} -->"}}]
        )
        http = FakeHttp((200, body))
        found = find_by_marker(make_site(), MARKER, http=http, auth=("u", "p"))
        self.assertEqual(found["id"], 41)
        self.assertEqual(found["status"], "draft")

    def test_ignores_a_fuzzy_search_hit_without_the_exact_marker(self):
        body = json.dumps(
            [{"id": 9, "status": "draft", "slug": "other", "content": {"rendered": "<!-- release-blog: other/repo@1.0.0 -->"}}]
        )
        http = FakeHttp((200, body))
        self.assertIsNone(find_by_marker(make_site(), MARKER, http=http, auth=("u", "p")))

    def test_search_is_authenticated_and_covers_drafts(self):
        http = FakeHttp((200, "[]"))
        find_by_marker(make_site(), MARKER, http=http, auth=("u", "p"))
        self.assertIn("status=draft%2Cpublish", http.calls[0]["url"])
        self.assertEqual(http.calls[0]["auth"], ("u", "p"))

    def test_a_failed_search_reads_as_not_found(self):
        http = FakeHttp((401, "unauthorised"))
        self.assertIsNone(find_by_marker(make_site(), MARKER, http=http, auth=("u", "p")))


class WriteDraftTest(unittest.TestCase):
    def test_payload_is_a_draft(self):
        payload = build_payload(
            {"title": "T", "slug": "s", "excerpt": "E"}, "<h2>Body</h2>"
        )
        self.assertEqual(payload["status"], "draft")
        self.assertEqual(payload["content"], "<h2>Body</h2>")
        self.assertNotIn("date", payload)

    def test_creates_a_new_post_when_no_id_is_given(self):
        http = FakeHttp((201, json.dumps({"id": 77})))
        ok, detail = write_draft(make_site(), {"status": "draft"}, http=http, auth=("u", "p"))
        self.assertTrue(ok)
        self.assertIn("77", detail)
        self.assertTrue(http.calls[0]["url"].endswith("/wp-json/wp/v2/posts"))

    def test_overwrites_the_given_post_id(self):
        http = FakeHttp((200, json.dumps({"id": 41})))
        ok, _ = write_draft(make_site(), {"status": "draft"}, http=http, auth=("u", "p"), post_id=41)
        self.assertTrue(ok)
        self.assertTrue(http.calls[0]["url"].endswith("/wp-json/wp/v2/posts/41"))

    def test_a_failed_write_reports_status_and_body(self):
        http = FakeHttp((403, "forbidden"))
        ok, detail = write_draft(make_site(), {"status": "draft"}, http=http, auth=("u", "p"))
        self.assertFalse(ok)
        self.assertIn("403", detail)
        self.assertIn("forbidden", detail)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'release_blog_cms'`

- [ ] **Step 4: Implement the module**

Create `scripts/seo/release_blog_cms.py`:

```python
"""CMS reads and the draft write.

Does its own HTTP rather than reusing `seo_fetch.Fetcher`: that transport takes
no request body, so it cannot POST. It does reuse `seo_parse.parse_sitemap` for
the fallback. Every function here degrades to a value the caller can carry on
with — this pipeline never fails a release.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import quote, urlparse

from seo_parse import parse_sitemap

CANDIDATE_LIMIT = 100
REQUEST_TIMEOUT = 20
TAG_STRIP = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class LinkCandidate:
    """One existing blog the writer may turn into a contextual internal link."""

    url: str
    slug: str
    title: str = ""
    excerpt: str = ""


def requests_http(method, url, *, headers=None, json_body=None, auth=None, timeout=REQUEST_TIMEOUT):
    """Production transport. Returns (status, body); never raises."""
    import requests

    try:
        response = requests.request(
            method,
            url,
            headers=headers or {},
            json=json_body,
            auth=auth,
            timeout=timeout,
        )
    except Exception as exc:  # network, DNS, TLS, timeout — all one outcome here
        return 0, f"{type(exc).__name__}: {exc}"
    return response.status_code, response.text


def release_marker(repo: str, tag: str) -> str:
    return f"release-blog: {repo}@{tag}"


def _plain(html: str) -> str:
    return " ".join(TAG_STRIP.sub(" ", html or "").split())


def _cms_url(site, path: str, query: str) -> str:
    return f"https://{site.origin_host}/wp-json/wp/v2/{path}?{query}"


def fetch_link_candidates(site, *, http=requests_http) -> tuple[list[LinkCandidate], str]:
    """Newest published blogs, richest source first.

    The CMS gives titles and excerpts, which is what lets the writer pick a
    genuinely relevant anchor. The sitemap gives URLs only, so relevance falls
    back to slug text — worse, but not nothing.
    """
    query = (
        f"per_page={CANDIDATE_LIMIT}&status=publish&orderby=date&order=desc"
        "&_fields=title,excerpt,link,slug,date"
    )
    status, body = http("GET", _cms_url(site, "posts", query))
    if status == 200:
        try:
            payload = json.loads(body)
            candidates = [
                LinkCandidate(
                    url=item.get("link", ""),
                    slug=item.get("slug", ""),
                    title=_plain((item.get("title") or {}).get("rendered", "")),
                    excerpt=_plain((item.get("excerpt") or {}).get("rendered", "")),
                )
                for item in payload
                if item.get("link")
            ]
            if candidates:
                return candidates, ""
        except (ValueError, TypeError, AttributeError) as exc:
            status, body = 0, f"unparseable CMS response: {exc}"

    note = f"CMS candidate read failed (HTTP {status}); fell back to the sitemap"
    sitemap_status, sitemap_body = http("GET", site.sitemap_url)
    if sitemap_status == 200:
        urls, _children = parse_sitemap(sitemap_body)
        prefix = site.listing_path.rstrip("/") + "/"
        blogs = sorted(url for url in urls if urlparse(url).path.startswith(prefix))
        if blogs:
            return [
                LinkCandidate(url=url, slug=urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])
                for url in blogs
            ], note
    return [], (
        f"no link candidates: CMS returned HTTP {status} and the sitemap returned "
        f"HTTP {sitemap_status}"
    )


def find_by_marker(site, marker: str, *, http=requests_http, auth) -> dict | None:
    """The post carrying this release's marker, draft or published.

    WordPress `search` is fuzzy, so the exact marker is confirmed against the
    rendered content in code. A failed search reads as "not found": a duplicate
    draft is recoverable, silently losing a blog is not.
    """
    query = (
        f"status=draft%2Cpublish&per_page=20&search={quote(marker)}"
        "&_fields=id,status,slug,content"
    )
    status, body = http("GET", _cms_url(site, "posts", query), auth=auth)
    if status != 200:
        return None
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return None
    for item in payload:
        rendered = (item.get("content") or {}).get("rendered", "")
        if marker in rendered:
            return {
                "id": item.get("id"),
                "status": item.get("status", ""),
                "slug": item.get("slug", ""),
            }
    return None


def build_payload(meta: dict, html: str) -> dict:
    """`date` is omitted deliberately so WordPress stamps it."""
    return {
        "status": "draft",
        "title": meta.get("title", ""),
        "slug": meta.get("slug", ""),
        "excerpt": meta.get("excerpt", ""),
        "content": html,
    }


def write_draft(site, payload: dict, *, http=requests_http, auth, post_id=None) -> tuple[bool, str]:
    path = f"posts/{post_id}" if post_id else "posts"
    url = f"https://{site.origin_host}/wp-json/wp/v2/{path}"
    status, body = http("POST", url, json_body=payload, auth=auth)
    if 200 <= status < 300:
        try:
            created = json.loads(body).get("id")
        except (ValueError, TypeError):
            created = None
        verb = "overwrote" if post_id else "created"
        return True, f"{verb} draft id {created}"
    return False, f"CMS write failed: HTTP {status} {body[:400]}"
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scripts/seo/release_blog_cms.py scripts/seo/fixtures/release_blog_candidates.json scripts/seo/test_release_blog.py
git commit -m "feat(seo): CMS link candidates, marker search and the draft write

Candidates come from the CMS with titles and excerpts, sitemap as fallback. The
marker search covers drafts and published posts and confirms the exact marker in
code, because WordPress search is fuzzy; a failed search reads as not-found so a
release can never silently lose its blog."
```

---

### Task 5: Pre-publish validation

**Files:**
- Create: `scripts/seo/release_blog_check.py`
- Create: `scripts/seo/fixtures/release_blog_good.html`
- Create: `scripts/seo/fixtures/release_blog_bad.html`
- Test: `scripts/seo/test_release_blog.py` (append)

**Interfaces:**
- Consumes: `SiteConfig` (Task 1), `LinkCandidate` (Task 4).
- Produces:
  - `PRE_PUBLISH_RULE_IDS: tuple[str, ...]` — the 18-rule allowlist.
  - `wrap_fragment(site, meta, html) -> str`
  - `build_page(site, meta, html)` → `BlogPage`
  - `run_rules(site, meta, html) -> list[Finding]`
  - `local_checks(site, meta, html, candidates) -> list[Finding]`
  - `validate(site, meta, html, candidates) -> Attempt`
  - `Attempt` — dataclass with `meta: dict`, `html: str`, `findings: list`, `errors: int`, `warns: int`, and `score` property returning `(errors, warns)`.
  - `better(first: Attempt, second: Attempt) -> Attempt` — lower score wins; ties keep `first`.
  - `render_report(attempts: list[Attempt], chosen: Attempt, notes: list[str]) -> str`

**Read ADR-0012 before starting.** The allowlist is exactly 18 ids and adding to it is how this design breaks: a rule reading `urls` sees an empty map and silently reports nothing.

- [ ] **Step 1: Write the fixtures**

Create `scripts/seo/fixtures/release_blog_good.html` — a fragment that violates nothing in the allowlist. It needs ≥300 words of article text, three distinct internal links with descriptive anchors, and one placeholder with descriptive alt:

```html
<div class="release-blog-checklist" data-strip-before-publish="true">
  <p><strong>Before publishing:</strong> replace the placeholder below and delete this block.</p>
  <ul><li>m1 — 1600×900 — the route editor with a waypoint dragged onto a coastal road</li></ul>
</div>
<p>Waypoints are draggable now. Open a route, press and hold any waypoint, and drag it somewhere else on the map. The line redraws under your finger and the elevation strip along the bottom recomputes as you go, so you can see what a detour costs before you commit to it.</p>
<h2>What changed in the route editor</h2>
<p>Before this release, moving a waypoint meant deleting it and adding a new one, which took four taps and lost the name you had given it. Now the waypoint keeps its name, its photo and its position in the sequence. Only the coordinates move.</p>
<p>The drag has a small amount of resistance built into it so a scroll gesture is not mistaken for a move. If you do move something by accident, the undo control in the top bar puts it back exactly where it was.</p>
<figure>
  <img src="https://placehold.co/1600x900/png?text=Route+editor" width="1600" height="900" loading="lazy" data-placeholder="true" data-media-id="m1" alt="The route editor with a waypoint handle dragged onto a coastal road, the elevation strip updating beneath the map">
  <figcaption>Drag any waypoint to reshape the route.</figcaption>
</figure>
<h2>Where this helps most</h2>
<p>Long multi-stop trips. A fortnight of driving might have thirty waypoints, and the ones that need adjusting are usually in the middle, where a road turned out to be closed or a detour turned out to be worth it. Rebuilding those by hand was the slowest part of putting a route together, and it is now a single gesture.</p>
<p>It also helps with coastal routes, where the road you actually drove is often a few hundred metres from the one the router picked. Dragging the line onto the right road takes a second and the animation follows it exactly.</p>
<h2>Getting a route in first</h2>
<p>If you are starting from a GPX file rather than building the route by hand, the <a href="https://www.travelanimator.com/hub/route-animation-guide">route animation guide</a> covers the import and the first few edits. It is worth reading before a long trip, because the order you add stops in decides the order the animation plays them.</p>
<h2>Making it look right</h2>
<p>Once the shape is correct, the map style does most of the work. The rundown of <a href="https://www.travelanimator.com/hub/best-travel-maps">map styles that read well on a phone screen</a> is the fastest way to pick one, and the differences matter more than they look on a desktop preview.</p>
<h2>Getting it out</h2>
<p>Exporting has not changed in this release, but if you are posting to Reels or Stories the notes on <a href="https://www.travelanimator.com/hub/export-for-instagram">exporting for Instagram</a> still apply: aspect ratio first, then bitrate, then check the safe area before you publish.</p>
<p>Update from the Play Store and the new drag behaviour is there the next time you open a route.</p>
<!-- release-blog: Lascade-Co/travel-animator-android@3.9.3 -->
```

Create `scripts/seo/fixtures/release_blog_bad.html` — one fragment breaking several allowlist rules at once (`D4` opens at `h3`, `B7` has one internal link, `G4` anchor is "read more", `D6` image has no alt, `A1` links to the origin host, `D5` is thin):

```html
<h3>New stuff</h3>
<p>We shipped some improvements to the editor. It is better now.</p>
<img src="https://placehold.co/1600x900/png" width="1600" height="900" data-placeholder="true" data-media-id="m1">
<p><a href="https://www.travelanimator.com/hub/route-animation-guide">read more</a></p>
<p><a href="https://hub.travelanimator.com/wp-json/wp/v2/posts">API</a></p>
<!-- release-blog: Lascade-Co/travel-animator-android@3.9.3 -->
```

- [ ] **Step 2: Write the failing tests**

Append to `scripts/seo/test_release_blog.py`:

```python
from release_blog_check import (
    PRE_PUBLISH_RULE_IDS,
    Attempt,
    better,
    local_checks,
    render_report,
    run_rules,
    validate,
)
from seo_model import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN

GOOD_META = {
    "title": "Reshape any route by dragging a waypoint",
    "slug": "drag-waypoints-to-reshape-routes",
    "excerpt": (
        "Waypoints are draggable now, so a route that took four taps to fix takes one. "
        "Here is how the new editor behaves on long multi-stop trips."
    ),
    "media": [
        {
            "id": "m1",
            "kind": "image",
            "width": 1600,
            "height": 900,
            "alt": "The route editor with a waypoint handle dragged onto a coastal road, the elevation strip updating beneath the map",
            "prompt": "Wide screenshot-style render of a mobile map editor. 16:9. No text overlays.",
        }
    ],
}

CANDIDATES = [
    LinkCandidate(url="https://www.travelanimator.com/hub/route-animation-guide", slug="route-animation-guide", title="Route guide"),
    LinkCandidate(url="https://www.travelanimator.com/hub/best-travel-maps", slug="best-travel-maps", title="Map styles"),
    LinkCandidate(url="https://www.travelanimator.com/hub/export-for-instagram", slug="export-for-instagram", title="Export"),
]


def ids(findings):
    return {f.rule for f in findings}


class AllowlistTest(unittest.TestCase):
    def test_the_allowlist_is_exactly_the_documented_set(self):
        # 18 distinct ids — verified against seo_checks.RULES_BY_ID while this
        # plan was written. Do not extend without re-reading ADR-0012.
        self.assertEqual(
            set(PRE_PUBLISH_RULE_IDS),
            {
                "A1", "A2", "A4", "A5",
                "B7",
                "C1", "C4",
                "D1", "D2", "D3", "D4", "D5", "D6",
                "E1", "E4",
                "G1", "G2", "G4",
            },
        )
        self.assertEqual(len(PRE_PUBLISH_RULE_IDS), 18)
        self.assertEqual(len(set(PRE_PUBLISH_RULE_IDS)), 18, "duplicate id in the allowlist")

    def test_d9_is_excluded_because_a_draft_may_hold_placeholders(self):
        self.assertNotIn("D9", PRE_PUBLISH_RULE_IDS)

    def test_no_allowlisted_rule_reads_the_network(self):
        # A rule that consults `urls` would see an empty map and silently pass.
        import inspect

        from seo_checks import RULES_BY_ID

        for rule_id in PRE_PUBLISH_RULE_IDS:
            source = inspect.getsource(RULES_BY_ID[rule_id].fn)
            body = source.split(":", 1)[1]
            self.assertNotIn("urls.get", body, f"{rule_id} reads the network")
            self.assertNotIn("urls[", body, f"{rule_id} reads the network")


class RunRulesTest(unittest.TestCase):
    def test_a_clean_fragment_produces_no_error_or_warn(self):
        findings = run_rules(make_site(), GOOD_META, fixture("release_blog_good.html"))
        bad = [f for f in findings if f.severity in (SEVERITY_ERROR, SEVERITY_WARN)]
        self.assertEqual(bad, [], f"unexpected findings: {[(f.rule, f.message) for f in bad]}")

    def test_a_bad_fragment_trips_the_expected_rules(self):
        findings = run_rules(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"))
        found = ids(findings)
        self.assertIn("D4", found)   # opens at h3
        self.assertIn("D5", found)   # thin
        self.assertIn("D6", found)   # image has no alt
        self.assertIn("B7", found)   # one internal link
        self.assertIn("G4", found)   # "read more"
        self.assertIn("A1", found)   # origin URL in an a[href]

    def test_a_short_title_trips_d1(self):
        findings = run_rules(make_site(), dict(GOOD_META, title="Waypoints"), fixture("release_blog_good.html"))
        self.assertIn("D1", ids(findings))

    def test_an_over_long_excerpt_trips_d2(self):
        findings = run_rules(make_site(), dict(GOOD_META, excerpt="x" * 200), fixture("release_blog_good.html"))
        self.assertIn("D2", ids(findings))

    def test_suppressed_rules_are_reported_at_info(self):
        site = make_site(suppress=["G4"])
        findings = run_rules(site, dict(GOOD_META), fixture("release_blog_bad.html"))
        g4 = [f for f in findings if f.rule == "G4"]
        self.assertTrue(g4)
        self.assertTrue(all(f.severity == SEVERITY_INFO for f in g4))


class LocalChecksTest(unittest.TestCase):
    def test_clean_draft_passes(self):
        findings = local_checks(make_site(), GOOD_META, fixture("release_blog_good.html"), CANDIDATES)
        self.assertEqual([f for f in findings if f.severity != SEVERITY_INFO], [])

    def test_a_fabricated_internal_link_is_an_error(self):
        html = fixture("release_blog_good.html").replace(
            "https://www.travelanimator.com/hub/best-travel-maps",
            "https://www.travelanimator.com/hub/best-travel-routes",
        )
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "not a link candidate" in f.message for f in findings))

    def test_a_slug_colliding_with_a_candidate_is_an_error(self):
        meta = dict(GOOD_META, slug="route-animation-guide")
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "slug" in f.message for f in findings))

    def test_script_in_the_fragment_is_an_error(self):
        html = fixture("release_blog_good.html") + "<script>alert(1)</script>"
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "script" in f.message for f in findings))

    def test_an_unresolved_media_id_is_an_error(self):
        html = fixture("release_blog_good.html").replace('data-media-id="m1"', 'data-media-id="m7"')
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "m7" in f.message for f in findings))

    def test_zero_placeholders_is_an_error(self):
        html = re.sub(r"<figure>.*?</figure>", "", fixture("release_blog_good.html"), flags=re.DOTALL)
        findings = local_checks(make_site(), dict(GOOD_META, media=[]), html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "placeholder" in f.message for f in findings))

    def test_more_than_six_placeholders_is_an_error(self):
        meta = dict(GOOD_META, media=[dict(GOOD_META["media"][0], id=f"m{n}") for n in range(7)])
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "placeholder" in f.message for f in findings))

    def test_a_missing_release_marker_is_an_error(self):
        html = fixture("release_blog_good.html").replace(
            "<!-- release-blog: Lascade-Co/travel-animator-android@3.9.3 -->", ""
        )
        findings = local_checks(make_site(), GOOD_META, html, CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_ERROR and "marker" in f.message for f in findings))

    def test_a_four_word_alt_is_a_warning(self):
        meta = dict(GOOD_META, media=[dict(GOOD_META["media"][0], alt="The route editor screen")])
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_WARN and "alt" in f.message for f in findings))

    def test_a_generic_alt_opener_is_a_warning(self):
        meta = dict(
            GOOD_META,
            media=[dict(GOOD_META["media"][0], alt="Screenshot of the app showing the editor screen")],
        )
        findings = local_checks(make_site(), meta, fixture("release_blog_good.html"), CANDIDATES)
        self.assertTrue(any(f.severity == SEVERITY_WARN and "alt" in f.message for f in findings))


class ScoringTest(unittest.TestCase):
    def attempt(self, errors, warns):
        return Attempt(meta={}, html="", findings=[], errors=errors, warns=warns)

    def test_fewer_errors_wins(self):
        first, second = self.attempt(2, 0), self.attempt(1, 9)
        self.assertIs(better(first, second), second)

    def test_equal_errors_fewer_warns_wins(self):
        first, second = self.attempt(1, 3), self.attempt(1, 1)
        self.assertIs(better(first, second), second)

    def test_a_full_tie_keeps_the_first(self):
        first, second = self.attempt(1, 1), self.attempt(1, 1)
        self.assertIs(better(first, second), first)


class ValidateTest(unittest.TestCase):
    def test_validate_counts_errors_and_warns(self):
        attempt = validate(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        self.assertGreater(attempt.errors, 0)
        self.assertEqual(attempt.score, (attempt.errors, attempt.warns))

    def test_a_clean_draft_scores_zero(self):
        attempt = validate(make_site(), GOOD_META, fixture("release_blog_good.html"), CANDIDATES)
        self.assertEqual(attempt.score, (0, 0), f"{[(f.rule, f.message) for f in attempt.findings]}")

    def test_report_names_every_attempt_and_the_choice(self):
        first = validate(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        second = validate(make_site(), GOOD_META, fixture("release_blog_good.html"), CANDIDATES)
        report = render_report([first, second], second, ["CMS read clean"])
        self.assertIn("attempt 1", report)
        self.assertIn("attempt 2", report)
        self.assertIn("chose attempt 2", report)
        self.assertIn("CMS read clean", report)
```


- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'release_blog_check'`

- [ ] **Step 4: Implement the module**

Create `scripts/seo/release_blog_check.py`:

```python
"""Pre-publish validation — the audit's own rules, against an allowlist.

See ADR-0012. The 18 rule ids below are the checks that read only the parsed
page AND can be answered about something nobody has published. Adding to this
list looks obviously correct and is how this breaks: a rule that consults `urls`
sees an empty map and silently reports nothing, and a rule about the document
head reports on the scaffolding `wrap_fragment` invented.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html import escape
from urllib.parse import urlparse

from seo_checks import RULES_BY_ID
from seo_model import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_ORDER,
    SEVERITY_WARN,
    Finding,
    Response,
    SiteContext,
)
from seo_parse import parse_blog

# Draft-answerable, page-only. Do not extend without re-reading ADR-0012.
PRE_PUBLISH_RULE_IDS = (
    "A1", "A2", "A4", "A5",   # origin hygiene: the writer must link to www
    "B7",                     # contextual internal link count
    "C1", "C4",               # stray noindex, thin body
    "D1", "D2", "D3", "D4", "D5", "D6",   # title, description, h1, order, length, alt
    "E1", "E4",               # fire only if JSON-LD / FAQ schema is present
    "G1", "G2", "G4",         # http:// links, fragment weight, anchor text
)

MIN_PLACEHOLDERS = 1
MAX_PLACEHOLDERS = 6
MIN_ALT_WORDS = 6
GENERIC_ALT_OPENERS = ("image of", "screenshot of", "photo of", "picture of", "img of")

FORBIDDEN_TAGS = ("script", "style", "html", "head", "body", "iframe")
MARKER_PATTERN = re.compile(r"<!--\s*release-blog:\s*\S+@\S+\s*-->")
MEDIA_ID_PATTERN = re.compile(r'data-media-id="([^"]+)"')


@dataclass
class Attempt:
    """One generated draft plus its verdict."""

    meta: dict
    html: str
    findings: list = field(default_factory=list)
    errors: int = 0
    warns: int = 0

    @property
    def score(self) -> tuple[int, int]:
        return self.errors, self.warns


def _finding(rule_id: str, slug: str, severity: str, message: str, url: str, evidence: str = "") -> Finding:
    return Finding(rule=rule_id, slug=slug, severity=severity, message=message, blog_url=url, evidence=evidence)


def draft_url(site, meta: dict) -> str:
    """Where the draft will live once published — the canonical it is checked against."""
    return f"https://{site.canonical_host}{site.listing_path.rstrip('/')}/{meta.get('slug', '')}"


def wrap_fragment(site, meta: dict, html: str) -> str:
    """A synthetic host page: only what the live site derives from these fields.

    OG tags, Twitter tags and Article schema are deliberately absent — the front
    end owns them, so inventing them here would mean validating this pipeline's
    own scaffolding. The fragment goes inside <article> because that is what
    seo_parse._article_root looks for, and therefore what makes B7's
    content_anchors populate.
    """
    url = draft_url(site, meta)
    return (
        '<!doctype html><html lang="en"><head>'
        f"<title>{escape(meta.get('title', ''))}</title>"
        f'<meta name="description" content="{escape(meta.get("excerpt", ""))}">'
        f'<link rel="canonical" href="{escape(url)}">'
        "</head><body><article>"
        f"<h1>{escape(meta.get('title', ''))}</h1>"
        f"{html}"
        "</article></body></html>"
    )


def build_page(site, meta: dict, html: str):
    url = draft_url(site, meta)
    page_html = wrap_fragment(site, meta, html)
    response = Response(
        url=url,
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=page_html,
        content=page_html.encode("utf-8"),
        ttfb_ms=0,
    )
    return parse_blog(url, meta.get("slug", ""), response)


def run_rules(site, meta: dict, html: str) -> list[Finding]:
    """The allowlist, with suppression downgrading to info.

    A suppressed rule still evaluates and still appears in the report, it just
    cannot trigger this pipeline's consequence — the retry and the scoring. That
    is the audit's rule generalised from delivery to whatever the consequence is.
    """
    page = build_page(site, meta, html)
    findings: list[Finding] = []
    for rule_id in PRE_PUBLISH_RULE_IDS:
        rule = RULES_BY_ID[rule_id]
        for item in rule.fn(page, site, {}, SiteContext()):
            if site.is_suppressed(rule_id):
                item = Finding(
                    rule=item.rule,
                    slug=item.slug,
                    severity=SEVERITY_INFO,
                    message=f"{item.message} (suppressed for {site.name})",
                    blog_url=item.blog_url,
                    evidence=item.evidence,
                )
            findings.append(item)
    return findings


def local_checks(site, meta: dict, html: str, candidates) -> list[Finding]:
    """What the rule engine cannot express about a draft."""
    url = draft_url(site, meta)
    out: list[Finding] = []

    def error(message, evidence=""):
        out.append(_finding("L1", "draft-shape", SEVERITY_ERROR, message, url, evidence))

    # 1. No fabricated internal links.
    allowed_paths = {urlparse(c.url).path.rstrip("/") for c in candidates}
    page = build_page(site, meta, html)
    for anchor in page.content_anchors:
        host = (urlparse(anchor.url).netloc or "").lower()
        if host != site.canonical_host:
            continue
        path = urlparse(anchor.url).path.rstrip("/")
        if path.startswith(site.listing_path.rstrip("/") + "/") and path not in allowed_paths:
            error(f"internal link is not a link candidate: {anchor.url}", anchor.url)

    # 2. No slug collision.
    if meta.get("slug") in {c.slug for c in candidates}:
        error(f"slug {meta.get('slug')!r} already belongs to a published blog")

    # 3. Shape.
    for field_name in ("title", "slug", "excerpt"):
        if not (meta.get(field_name) or "").strip():
            error(f"{field_name} is empty")
    for tag in FORBIDDEN_TAGS:
        if re.search(rf"<\s*{tag}\b", html, re.IGNORECASE):
            error(f"fragment contains a forbidden <{tag}> element")
    media = meta.get("media") or []
    if not MIN_PLACEHOLDERS <= len(media) <= MAX_PLACEHOLDERS:
        error(f"{len(media)} placeholders declared (expected {MIN_PLACEHOLDERS}–{MAX_PLACEHOLDERS})")
    declared = {str(item.get("id")) for item in media}
    for used in MEDIA_ID_PATTERN.findall(html):
        if used not in declared:
            error(f"data-media-id {used!r} has no entry in media[]", used)
    if not MARKER_PATTERN.search(html):
        error("fragment is missing its release marker comment")

    # 4. Alt descriptiveness — warn: an editor can fix wording, and a retry
    #    spent on phrasing is a retry not spent on a threshold miss.
    for item in media:
        alt = " ".join((item.get("alt") or "").split())
        lowered = alt.lower()
        if len(alt.split()) < MIN_ALT_WORDS:
            out.append(
                _finding(
                    "L2", "alt-not-descriptive", SEVERITY_WARN,
                    f"alt for {item.get('id')} is {len(alt.split())} words "
                    f"(minimum {MIN_ALT_WORDS}) — it must describe the image well enough to brief it",
                    url, alt,
                )
            )
        elif any(lowered.startswith(opener) for opener in GENERIC_ALT_OPENERS):
            out.append(
                _finding(
                    "L2", "alt-not-descriptive", SEVERITY_WARN,
                    f"alt for {item.get('id')} opens with a generic phrase — describe the subject instead",
                    url, alt,
                )
            )
    return out


def validate(site, meta: dict, html: str, candidates) -> Attempt:
    findings = run_rules(site, meta, html) + local_checks(site, meta, html, candidates)
    return Attempt(
        meta=meta,
        html=html,
        findings=findings,
        errors=sum(1 for f in findings if f.severity == SEVERITY_ERROR),
        warns=sum(1 for f in findings if f.severity == SEVERITY_WARN),
    )


def better(first: Attempt, second: Attempt) -> Attempt:
    """Lower score wins; a full tie keeps the first."""
    return second if second.score < first.score else first


def blocking(attempt: Attempt) -> bool:
    """Whether this attempt is worth spending the one retry on."""
    return attempt.errors > 0 or attempt.warns > 0


def render_report(attempts: list[Attempt], chosen: Attempt, notes: list[str]) -> str:
    lines: list[str] = []
    for note in notes:
        lines.append(f"note: {note}")
    if notes:
        lines.append("")
    for index, attempt in enumerate(attempts, start=1):
        lines.append(f"attempt {index}: {attempt.errors} errors, {attempt.warns} warns")
        for item in sorted(attempt.findings, key=lambda f: SEVERITY_ORDER[f.severity]):
            evidence = f" [{item.evidence}]" if item.evidence else ""
            lines.append(f"  {item.severity:<5} {item.rule:<3} {item.message}{evidence}")
        if not attempt.findings:
            lines.append("  clean")
        lines.append("")
    position = attempts.index(chosen) + 1 if chosen in attempts else 1
    lines.append(f"chose attempt {position} ({chosen.errors} errors, {chosen.warns} warns)")
    return "\n".join(lines) + "\n"
```

- [ ] **Step 5: Add the missing `re` import to the test file**

The local-checks tests use `re.sub`. Add `re` to the imports at the top of `scripts/seo/test_release_blog.py`:

```python
import json
import re
import tempfile
import unittest
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: PASS.

If `test_a_clean_fragment_produces_no_error_or_warn` fails, the fixture is at fault, not the code — read the reported rule ids and fix `release_blog_good.html`. Likely culprits: `D5` (needs ≥300 words of article text), `B7` (needs three *distinct* internal paths), `G2` (fragment weight), `D2` (excerpt must be 70–160 characters).

- [ ] **Step 7: Confirm the docs already agree**

The spec and ADR-0012 were corrected from 22 to 18 when this plan was written (the spec's prose table
had listed `D1 D2 D3` twice; the id lists were always right). Confirm nothing regressed:

```bash
grep -rn "22 rule\|22-rule\|22 named" docs/ ; echo "clean if nothing above"
grep -rn "18 named rules\|18 rules" docs/adr/0012-pre-publish-validation-allowlist.md
```
Expected: no `22` hits; ADR-0012 says 18.

- [ ] **Step 8: Commit**

```bash
git add scripts/seo/release_blog_check.py scripts/seo/fixtures/release_blog_good.html scripts/seo/fixtures/release_blog_bad.html scripts/seo/test_release_blog.py docs/
git commit -m "feat(seo): pre-publish validation against an 18-rule allowlist

Wraps the fragment in a synthetic host page and runs the audit's own rule
functions offline. Suppression downgrades to info rather than triggering the
retry. Local checks cover what the rule engine cannot: fabricated internal
links, slug collisions, fragment shape, and alt text descriptive enough to brief
the real image.

The allowlist is 18 rules; the spec and ADR-0012 already say so."
```

---

### Task 6: The prompt, Codex, and output parsing

**Files:**
- Create: `data/RELEASE_BLOG.md`
- Create: `scripts/seo/release_blog_draft.py`
- Test: `scripts/seo/test_release_blog.py` (append)

**Interfaces:**
- Consumes: `LinkCandidate` (Task 4), `Attempt` and `Finding` lists (Task 5), the digest string (Task 3).
- Produces:
  - `PROMPT_URL` — the raw URL of `data/RELEASE_BLOG.md`.
  - `load_prompt(path_or_url: str | None, *, http=None) -> str`
  - `build_prompt(base_prompt, site, marketing_version, marker, digest, candidates, *, previous=None, findings=None) -> str`
  - `run_codex(prompt_text: str, out_dir: str, *, run=subprocess.run) -> tuple[bool, str]`
  - `parse_output(out_dir: str) -> tuple[dict | None, str, str]` — `(meta, html, error)`; `error` is `""` on success.

- [ ] **Step 1: Write the prompt file**

Create `data/RELEASE_BLOG.md`. This is fetched verbatim by the CLI, exactly as `data/RELEASE_NOTES.md` is:

```markdown
# Codex Release-Blog Prompt

This file is fetched verbatim by `Lascade-Co/actions/scripts/seo/release_blog.py`, which appends the
release digest and the link candidates below it before invoking you with
`codex exec --sandbox workspace-write`.

## Role

You are a product writer for a consumer mobile app. You turn one release's changes into a single
blog post that a real person would choose to read. You are not writing release notes, a changelog,
or marketing copy.

## Output

Write exactly two files into the directory named in the RUN CONTEXT section below. Create or modify
nothing else. Do not run git, gradle, tests, or any build command.

**`blog.json`**

```json
{
  "title": "string, 15-60 characters",
  "slug": "kebab-case-url-slug",
  "excerpt": "string, 70-160 characters",
  "focus_keyword": "the search phrase this post should win",
  "media": [
    {
      "id": "m1",
      "kind": "image",
      "width": 1600,
      "height": 900,
      "alt": "one specific sentence: subject, on-screen state, setting",
      "prompt": "a generation brief: composition, aspect ratio, device framing, colour direction, and what text must NOT appear"
    }
  ]
}
```

**`blog.html`** — the post body only. No `<html>`, `<head>`, `<body>`, `<script>`, `<style>` or
`<iframe>`.

## Structure

- Open with the editor checklist block, verbatim in shape, one `<li>` per placeholder:

      <div class="release-blog-checklist" data-strip-before-publish="true">
        <p><strong>Before publishing:</strong> replace every placeholder below, then delete this block.</p>
        <ul><li>m1 — 1600×900 — {the alt text} — {the generation brief}</li></ul>
      </div>

- Then the post. **Start headings at `<h2>`.** The title is the page's only `<h1>` and the theme
  renders it — do not repeat it, and never open at `<h3>`.
- Close the file with the release marker line given in RUN CONTEXT, exactly as written.
- 900–1400 words of body text.
- 3–5 contextual internal links, each to a **different** URL, each copied **verbatim** from the
  LINK CANDIDATES list. Never invent a URL. Never link to `hub.` — always the `www.` host.
- Anchor text must describe the destination. Never `click here`, `read more`, `learn more`, `here`,
  `this link`, `link`, or `more`.
- 1–6 media placeholders, each as:

      <figure>
        <img src="https://placehold.co/{width}x{height}/png?text={short+label}"
             width="{width}" height="{height}" loading="lazy"
             data-placeholder="true" data-media-id="{id}"
             alt="{the alt text}">
        <figcaption>{a caption a reader benefits from}</figcaption>
      </figure>

  A video placeholder uses `<video width height controls>` with a `<p>` fallback carrying the same
  description.
- No JSON-LD, and no FAQ schema in particular. The site emits its own Article schema; a second copy
  conflicts with it.
- No "Conclusion" heading. End on what the reader can now do.

## Alt text and generation briefs

`alt` must be one specific sentence — subject, on-screen state, setting — descriptive enough that
someone could create the image from it alone and get the right picture. "Screenshot of the app" is a
failure. "The route editor with a waypoint handle dragged onto a coastal road, the elevation strip
updating beneath the map" is right. Six words minimum, and never open with "image of", "screenshot
of", "photo of" or "picture of".

`prompt` extends it with what belongs in a brief and would be noise in alt: composition, aspect
ratio, device framing, colour direction, and any text that must not appear in the image.

## Voice

Write like a person explaining something they built to someone who will use it.

- No scene-setting openers. Never "In today's fast-paced world", "Picture this", "We're excited to".
- Do not default to lists of three. Vary list lengths, and prefer prose where prose is clearer.
- Banned words: revolutionary, game-changing, seamlessly, effortlessly, unlock, elevate, empower,
  robust, cutting-edge, delve, leverage, harness, testament, landscape, realm.
- At most two em dashes in the entire post.
- Vary sentence length. Include several sentences under eight words.
- Second person, present tense. "You drag a waypoint", not "users can drag waypoints".
- Every claim must trace to something in the RELEASE DIGEST — a screen name, a gesture, a string, a
  number. Vague adjectives are what make writing read as machine-written; specifics are the fix.

## Hard constraints

- **Never describe a feature that is not in the RELEASE DIGEST.** If the digest is thin, write a
  shorter, more specific post about what is genuinely there. Inventing a feature is the single worst
  outcome of this task.
- The digest may be truncated. If it says so, do not speculate about what was cut.
- Do not create commits. Do not modify the repository.

## RUN CONTEXT

The workflow appends the run context, the release digest, and the link candidates below this line.
```

- [ ] **Step 2: Write the failing tests**

Append to `scripts/seo/test_release_blog.py`:

```python
from release_blog_draft import build_prompt, parse_output, run_codex

BASE_PROMPT = "# Codex Release-Blog Prompt\n\nWrite two files.\n"


class BuildPromptTest(unittest.TestCase):
    def prompt(self, **over):
        kwargs = {
            "base_prompt": BASE_PROMPT,
            "site": make_site(),
            "marketing_version": "3.9.3",
            "marker": MARKER,
            "digest": "## Release notes\n\n- Drag waypoints\n",
            "candidates": CANDIDATES,
            "out_dir": "/tmp/out",
        }
        kwargs.update(over)
        return build_prompt(**kwargs)

    def test_base_prompt_comes_first_and_verbatim(self):
        self.assertTrue(self.prompt().startswith(BASE_PROMPT))

    def test_run_context_names_the_output_dir_marker_and_host(self):
        text = self.prompt()
        self.assertIn("/tmp/out", text)
        self.assertIn(MARKER, text)
        self.assertIn("www.travelanimator.com", text)
        self.assertIn("3.9.3", text)

    def test_candidates_are_listed_with_titles_and_urls(self):
        text = self.prompt()
        self.assertIn("https://www.travelanimator.com/hub/route-animation-guide", text)
        self.assertIn("Route guide", text)

    def test_digest_is_included(self):
        self.assertIn("Drag waypoints", self.prompt())

    def test_retry_prompt_carries_the_previous_html_and_each_finding(self):
        previous = validate(make_site(), dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        text = self.prompt(previous=previous.html, findings=previous.findings)
        self.assertIn("previous attempt", text.lower())
        self.assertIn("New stuff", text)
        for item in previous.findings:
            if item.severity in (SEVERITY_ERROR, SEVERITY_WARN):
                self.assertIn(item.message[:40], text)

    def test_retry_prompt_omits_info_findings(self):
        site = make_site(suppress=["G4"])
        previous = validate(site, dict(GOOD_META), fixture("release_blog_bad.html"), CANDIDATES)
        text = self.prompt(previous=previous.html, findings=previous.findings)
        self.assertNotIn("suppressed for travelanimator", text)


class ParseOutputTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def write(self, name, content):
        Path(self.dir, name).write_text(content, encoding="utf-8")

    def test_reads_both_files(self):
        self.write("blog.json", json.dumps(GOOD_META))
        self.write("blog.html", "<h2>Body</h2>")
        meta, html, error = parse_output(self.dir)
        self.assertEqual(error, "")
        self.assertEqual(meta["slug"], GOOD_META["slug"])
        self.assertEqual(html, "<h2>Body</h2>")

    def test_missing_html_is_an_error(self):
        self.write("blog.json", json.dumps(GOOD_META))
        meta, html, error = parse_output(self.dir)
        self.assertIsNone(meta)
        self.assertIn("blog.html", error)

    def test_missing_json_is_an_error(self):
        self.write("blog.html", "<h2>Body</h2>")
        _meta, _html, error = parse_output(self.dir)
        self.assertIn("blog.json", error)

    def test_unparseable_json_is_an_error(self):
        self.write("blog.json", "{not json")
        self.write("blog.html", "<h2>Body</h2>")
        _meta, _html, error = parse_output(self.dir)
        self.assertIn("blog.json", error)

    def test_empty_html_is_an_error(self):
        self.write("blog.json", json.dumps(GOOD_META))
        self.write("blog.html", "   \n")
        _meta, _html, error = parse_output(self.dir)
        self.assertIn("empty", error)


class RunCodexTest(unittest.TestCase):
    def test_invokes_codex_with_workspace_write_and_the_prompt_on_stdin(self):
        seen = {}

        class Result:
            returncode = 0
            stdout = "done"
            stderr = ""

        def fake_run(cmd, **kwargs):
            seen["cmd"] = cmd
            seen["input"] = kwargs.get("input")
            return Result()

        ok, detail = run_codex("PROMPT TEXT", "/tmp/out", run=fake_run)
        self.assertTrue(ok)
        self.assertIn("codex", seen["cmd"][0])
        self.assertIn("--sandbox", seen["cmd"])
        self.assertIn("workspace-write", seen["cmd"])
        self.assertEqual(seen["input"], "PROMPT TEXT")

    def test_a_non_zero_exit_reports_the_stderr_tail(self):
        class Result:
            returncode = 1
            stdout = ""
            stderr = "not logged in"

        ok, detail = run_codex("PROMPT", "/tmp/out", run=lambda cmd, **kw: Result())
        self.assertFalse(ok)
        self.assertIn("not logged in", detail)

    def test_a_missing_codex_binary_is_not_an_exception(self):
        def fake_run(cmd, **kwargs):
            raise FileNotFoundError("codex")

        ok, detail = run_codex("PROMPT", "/tmp/out", run=fake_run)
        self.assertFalse(ok)
        self.assertIn("codex", detail)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'release_blog_draft'`

- [ ] **Step 4: Implement the module**

Create `scripts/seo/release_blog_draft.py`:

```python
"""Prompt assembly, Codex invocation, and reading back what it wrote."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from seo_model import SEVERITY_ERROR, SEVERITY_WARN

PROMPT_URL = "https://raw.githubusercontent.com/Lascade-Co/actions/main/data/RELEASE_BLOG.md"
CODEX_TIMEOUT = 900


def load_prompt(source: str | None = None, *, http=None) -> str:
    """The prompt text, from a local path when given and the raw URL otherwise."""
    if source and not source.startswith(("http://", "https://")):
        return Path(source).read_text(encoding="utf-8")
    from release_blog_cms import requests_http

    fetch = http or requests_http
    status, body = fetch("GET", source or PROMPT_URL)
    if status != 200:
        raise RuntimeError(f"could not fetch the prompt: HTTP {status}")
    return body


def _candidate_lines(candidates) -> str:
    lines = []
    for index, candidate in enumerate(candidates, start=1):
        title = candidate.title or f"(no title — slug: {candidate.slug})"
        excerpt = f" — {candidate.excerpt}" if candidate.excerpt else ""
        lines.append(f"{index}. {title} | {candidate.url}{excerpt}")
    return "\n".join(lines) if lines else "(none available)"


def build_prompt(
    *,
    base_prompt: str,
    site,
    marketing_version: str,
    marker: str,
    digest: str,
    candidates,
    out_dir: str,
    previous: str | None = None,
    findings=None,
) -> str:
    sections = [
        base_prompt.rstrip("\n"),
        "",
        "## RUN CONTEXT",
        "",
        f"- Site: {site.label} ({site.canonical_host})",
        f"- Blogs live under: https://{site.canonical_host}{site.listing_path}/<slug>",
        f"- Marketing version shipping now: {marketing_version}",
        f"- Write `blog.json` and `blog.html` into: {out_dir}",
        f"- Close `blog.html` with exactly this line:\n\n      <!-- {marker} -->",
        "",
        "## LINK CANDIDATES",
        "",
        "Use 3–5 of these, copied verbatim. Anything not on this list does not exist:",
        "",
        _candidate_lines(candidates),
        "",
        "## RELEASE DIGEST",
        "",
        digest.rstrip("\n"),
        "",
    ]

    actionable = [f for f in (findings or []) if f.severity in (SEVERITY_ERROR, SEVERITY_WARN)]
    if previous and actionable:
        sections += [
            "## YOUR PREVIOUS ATTEMPT FAILED VALIDATION",
            "",
            "Fix every point below. Keep what already worked — this is a revision, not a restart.",
            "",
            *(f"- [{item.severity}] {item.rule}: {item.message}" for item in actionable),
            "",
            "The HTML you produced last time:",
            "",
            "```html",
            previous.strip(),
            "```",
            "",
        ]
    return "\n".join(sections)


def run_codex(prompt_text: str, out_dir: str, *, run=subprocess.run) -> tuple[bool, str]:
    """One `codex exec`. Never raises — a missing or unauthenticated codex is a skip."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    try:
        result = run(
            ["codex", "exec", "--sandbox", "workspace-write", "-"],
            input=prompt_text,
            capture_output=True,
            text=True,
            timeout=CODEX_TIMEOUT,
            check=False,
        )
    except FileNotFoundError:
        return False, "codex is not installed or not on PATH"
    except subprocess.TimeoutExpired:
        return False, f"codex timed out after {CODEX_TIMEOUT}s"
    except OSError as exc:
        return False, f"codex could not be started: {exc}"
    if result.returncode != 0:
        tail = (result.stderr or result.stdout or "").strip()[-800:]
        return False, f"codex exited {result.returncode}: {tail}"
    return True, "codex completed"


def parse_output(out_dir: str) -> tuple[dict | None, str, str]:
    """Read `blog.json` and `blog.html`. Shape errors are a failed attempt, not a crash."""
    meta_path = Path(out_dir, "blog.json")
    html_path = Path(out_dir, "blog.html")
    if not meta_path.exists():
        return None, "", f"codex wrote no blog.json in {out_dir}"
    if not html_path.exists():
        return None, "", f"codex wrote no blog.html in {out_dir}"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        return None, "", f"blog.json is unparseable: {exc}"
    if not isinstance(meta, dict):
        return None, "", "blog.json is not a JSON object"
    html = html_path.read_text(encoding="utf-8")
    if not html.strip():
        return None, "", "blog.html is empty"
    return meta, html, ""
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add data/RELEASE_BLOG.md scripts/seo/release_blog_draft.py scripts/seo/test_release_blog.py
git commit -m "feat(seo): the release blog prompt, codex invocation and output parsing

Voice is specified as prohibitions rather than encouragement, because named
bans change model output and 'write naturally' does not. The retry prompt
carries the previous HTML plus only the actionable findings — info findings are
inert here, exactly as in the audit."
```

---

### Task 7: The CLI

**Files:**
- Create: `scripts/seo/release_blog.py`
- Test: `scripts/seo/test_release_blog.py` (append)

**Interfaces:**
- Consumes: everything from Tasks 1, 3, 4, 5, 6.
- Produces:
  - `main(argv=None, *, http=None, run=None) -> int` — always returns 0.
  - `build_parser() -> argparse.ArgumentParser`
  - Artifacts in `--out`: `blog.html`, `blog.json`, `wp-payload.json`, `validation.txt`, `prompt.md`, `prompt-retry.md`.

- [ ] **Step 1: Write the failing tests**

Append to `scripts/seo/test_release_blog.py`:

```python
import release_blog


class CliTest(unittest.TestCase):
    def setUp(self):
        self.out = tempfile.mkdtemp()
        self.config = write_config(CONFIG)

    def args(self, *extra):
        return [
            "--repo", "Lascade-Co/travel-animator-android",
            "--config", self.config,
            "--tag", "3.9.3",
            "--out", self.out,
            "--prompt", "../../data/RELEASE_BLOG.md",
            "--diff-file", str(Path("fixtures/release_blog_sample.diff")),
            "--notes-text", "- Drag waypoints to reshape routes",
            *extra,
        ]

    def fake_codex(self, meta=None, html=None):
        """A `run` double that writes the good fixture into --out, like codex would."""
        payload = json.dumps(meta or GOOD_META)
        body = html if html is not None else fixture("release_blog_good.html")

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            Path(self.out, "blog.json").write_text(payload, encoding="utf-8")
            Path(self.out, "blog.html").write_text(body, encoding="utf-8")
            return Result()

        return runner

    def test_dry_run_writes_artifacts_and_performs_no_write(self):
        http = FakeHttp(
            (200, fixture("release_blog_candidates.json")),  # candidates
        )
        code = release_blog.main(self.args("--dry-run"), http=http, run=self.fake_codex())
        self.assertEqual(code, 0)
        self.assertTrue(Path(self.out, "blog.html").exists())
        self.assertTrue(Path(self.out, "wp-payload.json").exists())
        self.assertTrue(Path(self.out, "validation.txt").exists())
        self.assertTrue(Path(self.out, "prompt.md").exists())
        self.assertEqual([c for c in http.calls if c["method"] == "POST"], [])

    def test_dry_run_payload_is_a_draft_with_the_generated_content(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        release_blog.main(self.args("--dry-run"), http=http, run=self.fake_codex())
        payload = json.loads(Path(self.out, "wp-payload.json").read_text())
        self.assertEqual(payload["body"]["status"], "draft")
        self.assertIn("waypoint", payload["body"]["content"].lower())
        self.assertIn("hub.travelanimator.com", payload["url"])

    def test_publish_posts_a_new_draft(self):
        http = FakeHttp(
            (200, "[]"),                                      # marker search: none
            (200, fixture("release_blog_candidates.json")),   # candidates
            (201, json.dumps({"id": 77})),                    # create
        )
        code = release_blog.main(
            self.args("--publish", "--cms-user", "u", "--cms-password", "p"),
            http=http,
            run=self.fake_codex(),
        )
        self.assertEqual(code, 0)
        posts = [c for c in http.calls if c["method"] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertTrue(posts[0]["url"].endswith("/wp-json/wp/v2/posts"))

    def test_publish_overwrites_an_existing_draft(self):
        existing = json.dumps(
            [{"id": 41, "status": "draft", "slug": "old", "content": {"rendered": f"<!-- {MARKER} -->"}}]
        )
        http = FakeHttp(
            (200, existing),
            (200, fixture("release_blog_candidates.json")),
            (200, json.dumps({"id": 41})),
        )
        release_blog.main(
            self.args("--publish", "--cms-user", "u", "--cms-password", "p"),
            http=http,
            run=self.fake_codex(),
        )
        posts = [c for c in http.calls if c["method"] == "POST"]
        self.assertTrue(posts[0]["url"].endswith("/posts/41"))

    def test_a_published_marker_stops_before_generating(self):
        existing = json.dumps(
            [{"id": 41, "status": "publish", "slug": "live", "content": {"rendered": f"<!-- {MARKER} -->"}}]
        )
        http = FakeHttp((200, existing))
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)
            raise AssertionError("codex must not run when the release is already published")

        code = release_blog.main(
            self.args("--publish", "--cms-user", "u", "--cms-password", "p"), http=http, run=runner
        )
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])
        self.assertEqual([c for c in http.calls if c["method"] == "POST"], [])

    def test_missing_credentials_generate_but_do_not_write(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        code = release_blog.main(self.args("--publish"), http=http, run=self.fake_codex())
        self.assertEqual(code, 0)
        self.assertTrue(Path(self.out, "blog.html").exists())
        self.assertEqual([c for c in http.calls if c["method"] == "POST"], [])
        self.assertIn("credential", Path(self.out, "validation.txt").read_text().lower())

    def test_an_unmapped_repo_exits_zero_with_no_artifacts(self):
        code = release_blog.main(
            [
                "--repo", "Lascade-Co/some-other-app",
                "--config", self.config,
                "--tag", "1.0.0",
                "--out", self.out,
                "--dry-run",
            ],
            http=FakeHttp(),
            run=self.fake_codex(),
        )
        self.assertEqual(code, 0)
        self.assertFalse(Path(self.out, "blog.html").exists())

    def test_an_ambiguous_repo_exits_zero_with_no_artifacts(self):
        entries = json.loads(json.dumps(CONFIG))
        entries[1]["repos"] = ["Lascade-Co/travel-animator-android"]
        code = release_blog.main(
            [
                "--repo", "Lascade-Co/travel-animator-android",
                "--config", write_config(entries),
                "--tag", "3.9.3",
                "--out", self.out,
                "--dry-run",
            ],
            http=FakeHttp(),
            run=self.fake_codex(),
        )
        self.assertEqual(code, 0)
        self.assertFalse(Path(self.out, "blog.html").exists())

    def test_codex_failure_exits_zero(self):
        class Failed:
            returncode = 1
            stdout = ""
            stderr = "not logged in"

        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        code = release_blog.main(self.args("--dry-run"), http=http, run=lambda cmd, **kw: Failed())
        self.assertEqual(code, 0)

    def test_no_candidates_skips_generation(self):
        http = FakeHttp((503, ""), (503, ""))
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)
            raise AssertionError("codex must not run without link candidates")

        code = release_blog.main(self.args("--dry-run"), http=http, run=runner)
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])

    def test_a_failing_first_attempt_triggers_one_retry(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        bodies = [fixture("release_blog_bad.html"), fixture("release_blog_good.html")]
        attempts = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            attempts.append(kwargs.get("input", ""))
            Path(self.out, "blog.json").write_text(json.dumps(GOOD_META), encoding="utf-8")
            Path(self.out, "blog.html").write_text(bodies[len(attempts) - 1], encoding="utf-8")
            return Result()

        release_blog.main(self.args("--dry-run"), http=http, run=runner)
        self.assertEqual(len(attempts), 2)
        self.assertIn("PREVIOUS ATTEMPT", attempts[1])
        self.assertTrue(Path(self.out, "prompt-retry.md").exists())
        report = Path(self.out, "validation.txt").read_text()
        self.assertIn("chose attempt 2", report)

    def test_no_retry_flag_stops_after_one_attempt(self):
        http = FakeHttp((200, fixture("release_blog_candidates.json")))
        attempts = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(cmd, **kwargs):
            attempts.append(1)
            Path(self.out, "blog.json").write_text(json.dumps(GOOD_META), encoding="utf-8")
            Path(self.out, "blog.html").write_text(fixture("release_blog_bad.html"), encoding="utf-8")
            return Result()

        release_blog.main(self.args("--dry-run", "--no-retry"), http=http, run=runner)
        self.assertEqual(len(attempts), 1)

    def test_html_flag_skips_codex_entirely(self):
        meta_path = Path(self.out, "hand.json")
        html_path = Path(self.out, "hand.html")
        meta_path.write_text(json.dumps(GOOD_META), encoding="utf-8")
        html_path.write_text(fixture("release_blog_good.html"), encoding="utf-8")
        http = FakeHttp((200, fixture("release_blog_candidates.json")))

        def runner(cmd, **kwargs):
            raise AssertionError("codex must not run with --html")

        code = release_blog.main(
            self.args("--dry-run", "--html", str(html_path), "--meta", str(meta_path)),
            http=http,
            run=runner,
        )
        self.assertEqual(code, 0)
        self.assertTrue(Path(self.out, "wp-payload.json").exists())
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'release_blog'`

- [ ] **Step 3: Implement the CLI**

Create `scripts/seo/release_blog.py`:

```python
#!/usr/bin/env python3
"""Turn one release into an SEO-validated draft in the site's CMS.

Always exits 0. A release must never go red because a blog could not be written
— see ADR-0011. Every failure path logs its reason, writes whatever artifacts it
has, and returns 0.

    python3 release_blog.py --repo Lascade-Co/travel-animator-android \\
        --config ../../data/seo_sites.json --tag 3.9.3 \\
        --repo-path ~/src/travel-animator-android --out ./out --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from release_blog_check import better, blocking, render_report, validate
from release_blog_cms import (
    build_payload,
    fetch_link_candidates,
    find_by_marker,
    release_marker,
    requests_http,
    write_draft,
)
from release_blog_digest import build_digest, marketing_version_from_tag, previous_tag
from release_blog_draft import build_prompt, load_prompt, parse_output, run_codex
from seo_model import load_site_config, resolve_site_for_repo


def log(message: str) -> None:
    print(f"release-blog: {message}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write a release blog draft into the site's CMS.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--site", help="site config name")
    target.add_argument("--repo", help="owner/name — resolved against each site's repos list")
    parser.add_argument("--config", default="seo_sites.json")
    parser.add_argument("--tag", required=True, help="the release tag; marketing version derived from it")
    parser.add_argument("--repo-path", default=".", help="checkout to read the diff from")
    parser.add_argument("--base", help="diff base (default: the tag before --head)")
    parser.add_argument("--head", help="diff head (default: --tag)")
    parser.add_argument("--out", default="./out")
    parser.add_argument("--prompt", help="path or URL of RELEASE_BLOG.md (default: the raw URL)")
    parser.add_argument("--notes-file", help="release notes (default: <repo-path>/releasenotes.txt)")
    parser.add_argument("--notes-text", help="release notes inline, for tests and local runs")
    parser.add_argument("--diff-file", help="skip git; use this patch")
    parser.add_argument("--candidates-file", help="skip the CMS; use this candidate JSON")
    parser.add_argument("--html", help="skip codex; validate this fragment")
    parser.add_argument("--meta", help="the blog.json to go with --html")
    parser.add_argument("--no-retry", action="store_true")
    parser.add_argument("--ignore-marker", action="store_true")
    parser.add_argument("--cms-user", default=os.environ.get("CMS_USER", ""))
    parser.add_argument("--cms-password", default=os.environ.get("CMS_APP_PASSWORD", ""))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="stop before writing to the CMS")
    mode.add_argument("--publish", action="store_true", help="write the draft to the CMS")
    return parser


def _read_notes(args) -> str:
    if args.notes_text is not None:
        return args.notes_text
    path = Path(args.notes_file) if args.notes_file else Path(args.repo_path, "releasenotes.txt")
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _load_candidates(args, site, http):
    if args.candidates_file:
        from release_blog_cms import LinkCandidate

        raw = json.loads(Path(args.candidates_file).read_text(encoding="utf-8"))
        return [LinkCandidate(**item) for item in raw], "candidates read from a file"
    return fetch_link_candidates(site, http=http)


def _digest(args, run) -> str:
    notes = _read_notes(args)
    if args.diff_file:
        from release_blog_digest import filter_patch, truncate

        patch = filter_patch(Path(args.diff_file).read_text(encoding="utf-8"))
        return "\n".join(
            (
                "## Release notes", "", notes.strip() or "unavailable", "",
                "## Filtered diff", "", "```diff", truncate(patch).strip() or "unavailable", "```", "",
            )
        )
    head = args.head or args.tag
    base = args.base or previous_tag(args.repo_path, head, run=run) or ""
    if not base:
        log("no diff base could be determined; the digest will carry notes only")
    return build_digest(args.repo_path, base, head, notes, run=run)


def main(argv=None, *, http=None, run=None) -> int:
    args = build_parser().parse_args(argv)
    http = http or requests_http
    run = run or subprocess.run
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    notes: list[str] = []

    # 1. Which site?
    try:
        if args.site:
            site = load_site_config(args.config, args.site)
        else:
            site, reason = resolve_site_for_repo(args.config, args.repo)
            if site is None:
                log(f"skipping: {reason}")
                return 0
    except (OSError, ValueError, KeyError) as exc:
        log(f"skipping: could not read the site config: {exc}")
        return 0
    log(f"site: {site.name} ({site.canonical_host})")

    repo = args.repo or site.repos[0] if site.repos else (args.repo or site.name)
    marker = release_marker(repo, args.tag)
    auth = (args.cms_user, args.cms_password) if args.cms_user and args.cms_password else None

    # 2. Has this release already been written?
    existing = None
    if auth and not args.ignore_marker:
        existing = find_by_marker(site, marker, http=http, auth=auth)
        if existing and existing.get("status") == "publish":
            log(f"skipping: this release is already a published blog (id {existing['id']}, slug {existing['slug']!r})")
            return 0
        if existing:
            log(f"an existing draft carries this marker (id {existing['id']}); it will be overwritten")
            notes.append(f"overwriting draft id {existing['id']}")

    # 3. Link candidates.
    try:
        candidates, note = _load_candidates(args, site, http)
    except (OSError, ValueError) as exc:
        candidates, note = [], f"candidate load failed: {exc}"
    if note:
        log(note)
        notes.append(note)
    if not candidates:
        log("skipping: without link candidates a draft cannot carry contextual internal links")
        return 0
    log(f"{len(candidates)} link candidates")

    # 4. Generate, validate, retry once.
    marketing_version = marketing_version_from_tag(args.tag)
    attempts = []
    if args.html:
        try:
            meta = json.loads(Path(args.meta).read_text(encoding="utf-8")) if args.meta else {}
            html = Path(args.html).read_text(encoding="utf-8")
        except (OSError, ValueError) as exc:
            log(f"skipping: could not read --html/--meta: {exc}")
            return 0
        attempts.append(validate(site, meta, html, candidates))
    else:
        try:
            base_prompt = load_prompt(args.prompt, http=http)
        except (OSError, RuntimeError) as exc:
            log(f"skipping: {exc}")
            return 0
        digest = _digest(args, run)
        previous_html = None
        previous_findings = None
        for index in range(1 if args.no_retry else 2):
            prompt = build_prompt(
                base_prompt=base_prompt,
                site=site,
                marketing_version=marketing_version,
                marker=marker,
                digest=digest,
                candidates=candidates,
                out_dir=str(out.resolve()),
                previous=previous_html,
                findings=previous_findings,
            )
            name = "prompt.md" if index == 0 else "prompt-retry.md"
            (out / name).write_text(prompt, encoding="utf-8")
            ok, detail = run_codex(prompt, str(out), run=run)
            log(f"attempt {index + 1}: {detail}")
            if not ok:
                notes.append(detail)
                break
            meta, html, error = parse_output(str(out))
            if error:
                log(f"attempt {index + 1}: {error}")
                notes.append(error)
                break
            attempt = validate(site, meta, html, candidates)
            attempts.append(attempt)
            log(f"attempt {index + 1}: {attempt.errors} errors, {attempt.warns} warns")
            if not blocking(attempt):
                break
            previous_html, previous_findings = html, attempt.findings

    if not attempts:
        (out / "validation.txt").write_text(
            "no attempt produced a draft\n" + "".join(f"note: {n}\n" for n in notes), encoding="utf-8"
        )
        log("skipping: no attempt produced a draft")
        return 0

    chosen = attempts[0]
    for attempt in attempts[1:]:
        chosen = better(chosen, attempt)
    (out / "validation.txt").write_text(render_report(attempts, chosen, notes), encoding="utf-8")
    (out / "blog.html").write_text(chosen.html, encoding="utf-8")
    (out / "blog.json").write_text(json.dumps(chosen.meta, indent=2), encoding="utf-8")

    # 5. Write, or say exactly what would have been written.
    payload = build_payload(chosen.meta, chosen.html)
    post_id = existing["id"] if existing else None
    path = f"posts/{post_id}" if post_id else "posts"
    target = f"https://{site.origin_host}/wp-json/wp/v2/{path}"
    (out / "wp-payload.json").write_text(
        json.dumps({"url": target, "method": "POST", "body": payload}, indent=2), encoding="utf-8"
    )

    if args.dry_run:
        log(f"dry run: would POST to {target}")
        return 0
    if not auth:
        message = "no CMS credentials (CMS_USER / CMS_APP_PASSWORD); draft not written"
        log(message)
        with (out / "validation.txt").open("a", encoding="utf-8") as handle:
            handle.write(f"note: {message}\n")
        return 0
    ok, detail = write_draft(site, payload, http=http, auth=auth, post_id=post_id)
    log(detail)
    with (out / "validation.txt").open("a", encoding="utf-8") as handle:
        handle.write(f"note: {detail}\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # the exit-0 guarantee of last resort
        print(f"release-blog: unexpected failure, exiting 0 anyway: {type(exc).__name__}: {exc}")
        sys.exit(0)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd scripts/seo && python3 -m pytest test_release_blog.py -v`
Expected: PASS

- [ ] **Step 5: Run the whole suite**

Run: `cd scripts/seo && python3 -m pytest . -q`
Expected: PASS — every audit test plus the new ones.

- [ ] **Step 6: Verify the exit-0 guarantee by hand**

Run:
```bash
cd scripts/seo && python3 release_blog.py --repo Lascade-Co/nonexistent \
  --config ../../data/seo_sites.json --tag 1.0.0 --out /tmp/rb-check --dry-run; echo "exit=$?"
```
Expected: a `skipping: no site config names ...` line and `exit=0`.

- [ ] **Step 7: Commit**

```bash
git add scripts/seo/release_blog.py scripts/seo/test_release_blog.py
git commit -m "feat(seo): the release blog CLI

One entry point, always exits 0. --dry-run stops before the CMS write and
records the exact payload; --html/--meta skip codex; --diff-file and
--candidates-file make each stage independently exercisable offline."
```

---

### Task 8: The reusable workflow and the Android caller

**Files:**
- Create: `.github/workflows/release-blog.yml`
- Modify: `.github/workflows/android-build-release.yml`

**Interfaces:**
- Consumes: `scripts/seo/release_blog.py` and its four sibling modules (Tasks 3–7), `data/RELEASE_BLOG.md` (Task 6), `data/seo_sites.json` (Task 1).
- Produces: a reusable workflow taking `repo`, `tag`, `project_slug`, optional `base`, optional `dry_run`.

**Do not add `continue-on-error` to the calling job.** GitHub restricts which keys a job using `uses:` may set, and a rejected key fails to parse the *whole* release workflow — the exact coupling ADR-0011 exists to prevent. Safety lives inside the reusable workflow, where it is certainly legal, plus the CLI's own exit-0 guarantee.

- [ ] **Step 1: Write the reusable workflow**

Create `.github/workflows/release-blog.yml`:

```yaml
name: Release Blog

# Turns one release into an SEO-validated draft in the site's CMS. Called by the
# central release runners; never called directly.
#
# It cannot fail a release. `release_blog.py` always exits 0, and every step that
# touches the network or a third-party tool is continue-on-error. Deliberately no
# continue-on-error on the *calling* job: GitHub restricts the keys a job using
# `uses:` may set, and a rejected key would fail to parse the caller.
#
# See docs/adr/0011-release-blog-creates-drafts.md and
# docs/superpowers/specs/2026-08-27-release-blog-design.md.

on:
  workflow_call:
    inputs:
      repo:
        description: owner/name of the repo that was released
        required: true
        type: string
      tag:
        description: The tag just created. Also the source of the marketing version (leading `v` stripped).
        required: true
        type: string
      project_slug:
        description: Infisical project slug — CMS_USER / CMS_APP_PASSWORD live in its prod /Build folder
        required: true
        type: string
      base:
        description: Diff base. Blank = the tag before `tag`.
        required: false
        type: string
      dry_run:
        description: Validate and upload artifacts without writing to the CMS
        required: false
        type: boolean
        default: false

permissions:
  contents: read

env:
  RAW: https://raw.githubusercontent.com/Lascade-Co/actions/main

jobs:
  draft:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: Mint GitHub App token
        id: app-token
        uses: actions/create-github-app-token@v3
        with:
          client-id: ${{ secrets.CI_APP_CLIENT_ID }}
          private-key: ${{ secrets.CI_APP_PRIVATE_KEY }}
          owner: ${{ github.repository_owner }}
          repositories: ${{ inputs.repo }}

      - name: Checkout the released tag
        uses: actions/checkout@v6
        with:
          repository: ${{ inputs.repo }}
          ref: ${{ inputs.tag }}
          token: ${{ steps.app-token.outputs.token }}
          fetch-depth: 0
          path: app

      - name: Set up Python
        uses: actions/setup-python@v7
        with:
          python-version: '3.13'

      - name: Install dependencies
        # Pinned identically to seo-blog-audit.yml: this shares that pipeline's
        # HTML parser and must not have it change underneath.
        run: pip install --quiet requests==2.34.2 beautifulsoup4==4.15.0 lxml==6.1.1

      - name: Fetch CMS credentials (prod)
        id: creds
        continue-on-error: true
        uses: Infisical/secrets-action@v1.0.15
        with:
          method: universal
          client-id: ${{ secrets.INFISICAL_CLIENT_ID }}
          client-secret: ${{ secrets.INFISICAL_CLIENT_SECRET }}
          project-slug: ${{ inputs.project_slug }}
          env-slug: "prod"
          domain: ${{ secrets.INFISICAL_DOMAIN }}
          secret-path: /Build
          export-type: env

      - name: Restore Codex auth
        continue-on-error: true
        shell: bash
        env:
          CODEX_AUTH_JSON_BASE_64: ${{ secrets.CODEX_AUTH_JSON_BASE_64 }}
        run: |
          mkdir -p "$HOME/.codex"
          printf '%s' "$CODEX_AUTH_JSON_BASE_64" | tr -d '\n\r ' | base64 -d > "$HOME/.codex/auth.json"

      - name: Install codex & enable user namespaces for its sandbox
        continue-on-error: true
        shell: bash
        run: |
          npm install -g @openai/codex

          current_userns="$(sysctl -n kernel.unprivileged_userns_clone 2>/dev/null || true)"
          if [ -n "$current_userns" ] && [ "$current_userns" != "1" ]; then
            sudo sysctl -w kernel.unprivileged_userns_clone=1
          fi
          current_apparmor="$(sysctl -n kernel.apparmor_restrict_unprivileged_userns 2>/dev/null || true)"
          if [ -n "$current_apparmor" ] && [ "$current_apparmor" != "0" ]; then
            sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
          fi

      - name: Download the pipeline scripts
        continue-on-error: true
        run: |
          for name in seo_model seo_parse seo_rulekit seo_checks_abc seo_checks_def seo_checks_ghi \
                      seo_checks seo_fetch release_blog_digest release_blog_cms release_blog_check \
                      release_blog_draft release_blog; do
            curl -fsSL --retry 3 --retry-delay 2 -o "$name.py" "$RAW/scripts/seo/$name.py"
          done
          curl -fsSL --retry 3 --retry-delay 2 -o seo_sites.json    "$RAW/data/seo_sites.json"
          curl -fsSL --retry 3 --retry-delay 2 -o RELEASE_BLOG.md   "$RAW/data/RELEASE_BLOG.md"

      - name: Write the draft
        id: draft
        continue-on-error: true
        shell: bash
        env:
          BASE: ${{ inputs.base }}
        run: |
          ARGS=(
            --repo "${{ inputs.repo }}"
            --config seo_sites.json
            --prompt RELEASE_BLOG.md
            --tag "${{ inputs.tag }}"
            --repo-path app
            --out out
          )
          if [ -n "$BASE" ]; then ARGS+=(--base "$BASE"); fi
          if [ "${{ inputs.dry_run }}" = "true" ]; then ARGS+=(--dry-run); else ARGS+=(--publish); fi
          python3 release_blog.py "${ARGS[@]}"

      - name: Upload artifacts
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: release-blog-${{ inputs.tag }}
          path: |
            out/blog.html
            out/blog.json
            out/wp-payload.json
            out/validation.txt
            out/prompt.md
            out/prompt-retry.md
          if-no-files-found: warn
          retention-days: 30
```

- [ ] **Step 2: Verify the workflow parses**

Run:
```bash
python3 -c "
import yaml
doc = yaml.safe_load(open('.github/workflows/release-blog.yml'))
assert 'workflow_call' in doc[True], doc.keys()
inputs = doc[True]['workflow_call']['inputs']
assert set(inputs) == {'repo', 'tag', 'project_slug', 'base', 'dry_run'}, set(inputs)
print('inputs ok:', sorted(inputs))
"
```
Expected: `inputs ok: ['base', 'dry_run', 'project_slug', 'repo', 'tag']`

(`doc[True]` is correct — PyYAML parses the bare key `on` as the boolean `True`.)

- [ ] **Step 3: Add the outputs to the Android `release` job**

In `.github/workflows/android-build-release.yml`, add an `outputs` block to the `release` job, directly under `runs-on: ubuntu-latest` (line 223):

```yaml
  release:
    needs: [prepare, build-aab, build-apk]
    runs-on: ubuntu-latest
    outputs:
      tag:  ${{ steps.increment_version.outputs.new_version }}
      base: ${{ steps.notes.outputs.base }}
```

Both values already exist in that job: `steps.increment_version.outputs.new_version` is used for `tag_name`, and the `Check if release notes are outdated` step is unconditional, so `steps.notes.outputs.base` is always set.

- [ ] **Step 4: Add the `blog` job**

Append to `.github/workflows/android-build-release.yml`, after the `release` job's final step:

```yaml
  # Turns the same diff the release notes came from into a CMS draft. Cannot
  # affect the release: it runs after it, and release_blog.py always exits 0.
  # See docs/adr/0011-release-blog-creates-drafts.md.
  blog:
    needs: [prepare, release]
    uses: ./.github/workflows/release-blog.yml
    with:
      repo:         ${{ github.event.client_payload.repo }}
      tag:          ${{ needs.release.outputs.tag }}
      project_slug: ${{ github.event.client_payload.project_slug }}
      base:         ${{ needs.release.outputs.base }}
    secrets: inherit
```

- [ ] **Step 5: Verify the caller parses and wires up correctly**

Run:
```bash
python3 -c "
import yaml
doc = yaml.safe_load(open('.github/workflows/android-build-release.yml'))
blog = doc['jobs']['blog']
assert blog['uses'] == './.github/workflows/release-blog.yml', blog['uses']
assert blog['needs'] == ['prepare', 'release'], blog['needs']
assert set(blog['with']) == {'repo', 'tag', 'project_slug', 'base'}, set(blog['with'])
assert 'continue-on-error' not in blog, 'not permitted on a job using uses:'
assert set(doc['jobs']['release']['outputs']) == {'tag', 'base'}
print('caller ok')
"
```
Expected: `caller ok`

- [ ] **Step 6: Lint both workflows with actionlint**

Run:
```bash
docker run --rm -v "$(pwd):/repo" --workdir /repo rhysd/actionlint:latest \
  -color .github/workflows/release-blog.yml .github/workflows/android-build-release.yml
```
Expected: no output (clean). If Docker is unavailable, `brew install actionlint && actionlint .github/workflows/release-blog.yml .github/workflows/android-build-release.yml`.

Fix anything it reports. Pay particular attention to any complaint about keys on the `blog` job — that is the failure mode this task is shaped to avoid.

- [ ] **Step 7: Commit**

```bash
git add .github/workflows/release-blog.yml .github/workflows/android-build-release.yml
git commit -m "feat(release): write a CMS draft from each Android release

A reusable workflow so Flutter can adopt it with one block. All safety lives
inside it — the CLI always exits 0 and every network-facing step is
continue-on-error — because GitHub restricts the keys a job using 'uses:' may
set, and a rejected key would fail to parse the whole release workflow."
```

---

### Task 9: End-to-end verification

**Files:** none — this task changes nothing. It is the gate before anyone trusts the pipeline.

- [ ] **Step 1: Full local dry run against a real checkout**

```bash
git clone --depth 200 https://github.com/Lascade-Co/travel-animator-android /tmp/ta-android
cd scripts/seo && python3 release_blog.py \
  --repo Lascade-Co/travel-animator-android \
  --config ../../data/seo_sites.json \
  --prompt ../../data/RELEASE_BLOG.md \
  --repo-path /tmp/ta-android \
  --tag "$(git -C /tmp/ta-android tag --sort=-creatordate | head -1)" \
  --out /tmp/rb-out --dry-run
```

Read all four outputs before continuing:
- `/tmp/rb-out/blog.html` — does it read like a person wrote it? Any banned word, any "In today's", any three-item list reflex? If the voice is wrong, the fix is `data/RELEASE_BLOG.md`, not the code.
- `/tmp/rb-out/validation.txt` — clean, or explained?
- `/tmp/rb-out/wp-payload.json` — `status: draft`, sane slug, 70–160-character excerpt.
- `/tmp/rb-out/prompt.md` — did the digest actually contain the feature, or was it drowned in noise? If drowned, extend `DROP_PATH_PATTERNS`.

- [ ] **Step 2: Confirm every internal link resolves**

```bash
python3 - <<'PY'
import re, urllib.request
html = open('/tmp/rb-out/blog.html').read()
for url in sorted(set(re.findall(r'href="(https://www\.[^"]+)"', html))):
    request = urllib.request.Request(url, method='HEAD', headers={'User-Agent': 'release-blog-check'})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            print(response.status, url)
    except Exception as exc:
        print('FAIL', url, exc)
PY
```
Expected: every line `200`. A non-200 means the anti-hallucination check has a hole — the URL came from the candidate list but does not resolve, so investigate `fetch_link_candidates`.

- [ ] **Step 3: Create the CMS account and secrets**

For each site, in WordPress: create a user with a **real display name** (e.g. "Travel Animator Team"), a filled-in bio and an avatar, role Author or Editor. This account becomes the public byline and `E2`'s schema `author` — see ADR-0011. Then generate an application password for it.

Store in Infisical, project = the app's `project_slug`, env `prod`, path `/Build`:
- `CMS_USER` — the account's username
- `CMS_APP_PASSWORD` — the application password, spaces included

- [ ] **Step 4: One real local publish**

```bash
cd scripts/seo && CMS_USER='…' CMS_APP_PASSWORD='…' python3 release_blog.py \
  --repo Lascade-Co/travel-animator-android \
  --config ../../data/seo_sites.json --prompt ../../data/RELEASE_BLOG.md \
  --repo-path /tmp/ta-android --tag "$(git -C /tmp/ta-android tag --sort=-creatordate | head -1)" \
  --out /tmp/rb-live --publish
```

In WordPress admin, confirm: the draft exists and is **unpublished**; the byline is the editorial account; the checklist block is at the top; the placeholder renders at the right aspect ratio; the marker comment is at the end of the content.

- [ ] **Step 5: Confirm idempotency**

Run the exact same command again. Expected: `an existing draft carries this marker (id N); it will be overwritten`, and WordPress still shows **one** draft, not two.

- [ ] **Step 6: Confirm the published-marker guard**

Publish that draft by hand, then run the command a third time. Expected: `skipping: this release is already a published blog`, no Codex call, and the live post untouched.

- [ ] **Step 7: Confirm the audit now sees it**

```bash
cd scripts/seo && python3 seo_blog_audit.py --site travelanimator --config ../../data/seo_sites.json --output /tmp/audit.html
```

Open `/tmp/audit.html`. The newly published blog must appear. If the placeholder was not swapped before publishing, **`D9` must fire `error`** against it — that is the whole point of Task 2, and this is the only place the pre-publish and post-publish halves get checked against each other. Then swap the image in WordPress and re-run: `D9` should go quiet.

- [ ] **Step 8: One real release run**

Trigger an Android release from an app repo as usual. Confirm: the `blog` job runs after `release`, the release itself is unaffected, the `release-blog-<tag>` artifact contains all six files, and a draft appears in WordPress.

- [ ] **Step 9: Record what the live run taught you**

Both `2026-08-03-seo-blog-audit-design.md` and `2026-08-10-marketing-net-design.md` carry a "corrections found by running it live" section, and both are the most useful part of those documents. Append the same to `docs/superpowers/specs/2026-08-27-release-blog-design.md` — every surprise, every prompt change, every filter pattern you had to add.

```bash
git add docs/superpowers/specs/2026-08-27-release-blog-design.md
git commit -m "docs(spec): corrections from the first live release blog run"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| Vocabulary (draft / link candidate / marker) | Global Constraints; enforced in code across 1, 4, 5 |
| `repos` mapping, no-match and ambiguous-match | 1 |
| `D9` | 2 |
| Release digest, filters, 200 KB cap, marketing version | 3 |
| Link candidates, CMS + sitemap fallback | 4 |
| Marker search, overwrite, stop-on-published | 4 (mechanics), 7 (orchestration), 9 (verification) |
| Draft write, payload shape, byline | 4, 9 Step 3 |
| Synthetic host page, allowlist, exclusions, suppression, `info` inert | 5 |
| Local checks, alt descriptiveness | 5 |
| Attempt scoring, one retry, `validation.txt` | 5 (mechanics), 7 (loop) |
| Body contract, humanised voice, placeholders, editor checklist | 6 (`data/RELEASE_BLOG.md`) |
| CLI flags and artifacts | 7 |
| Reusable workflow, caller wiring, secrets | 8 |
| Failure model — every row | 7 (`main`), tested in 7 Step 1 |
| Verification plan | 9 |
| Documentation (CONTEXT.md, ADRs) | already committed in `e102433` |

No gaps.

**Corrections made while reviewing:**

1. **The allowlist is 18 rules, not 22.** The spec's prose table double-counted `D1 D2 D3`. Corrected in the spec and ADR-0012 while writing this plan, after importing `PRE_PUBLISH_RULE_IDS` against `seo_checks.RULES_BY_ID` and confirming all 18 ids exist. Task 5 Step 7 is now just a regression check.
2. **`repo` for the marker.** `main` derives the marker from `--repo` when given and falls back to the site's first listed repo, so a `--site`-only local run still produces a stable marker.
3. **`--notes-text`** was added to the CLI beyond the spec's flag list — the tests need release notes without a checkout on disk, and it is the same affordance `--diff-file` provides for the patch.

**Type consistency:** checked. `Attempt.score` is `(errors, warns)` in Task 5 and consumed as such in Task 7. `fetch_link_candidates` returns `(list, str)` in Task 4 and is unpacked that way in Task 7. `parse_output` returns `(meta, html, error)` in Task 6 and is unpacked that way in Task 7. `find_by_marker` returns a dict with `id`/`status`/`slug` in Task 4 and only those keys are read in Task 7. `LinkCandidate` field order is `(url, slug, title, excerpt)` and every construction uses keywords. The `http` double's signature matches `requests_http` exactly, including the keyword-only `json_body` and `auth`.
