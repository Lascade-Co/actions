"""Sealing policy for every workflow and composite action in this repo.

The rule functions (`check_*`) return a list of violation strings. Real files must return
none; the synthetic files under testdata/ prove each rule can both pass and fail.
"""
import os
import posixpath
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HERE = Path(__file__).resolve().parent
CONTROL = HERE.parents[1]  # the checkout that holds this toolkit (this repo, or `control/` in a caller's run)
ROOT = Path(os.environ.get("SEAL_POLICY_ROOT") or CONTROL).resolve()
TESTDATA = HERE / "testdata"
DOCS = CONTROL / "docs" / "sealed-output.md"
TOOLKIT_COMPOSITES = {"seal", "seal-upload"}  # the toolkit itself: their install step must run under plain bash
KEY_FILE = Path(".github") / "seal" / "repo-key.pub"
KEY_RE = re.compile(r"^age1[02-9ac-hj-np-z]{58}$")


def workflows_of(root):
    d = Path(root) / ".github" / "workflows"
    return sorted([*d.glob("*.yml"), *d.glob("*.yaml")])


def composites_of(root):
    found = []
    for d in (Path(root) / "actions", Path(root) / ".github" / "actions"):
        found += [*d.glob("*/action.yml"), *d.glob("*/action.yaml")]
    return sorted(found)


def is_this_repo(root):
    """Lascade-Co/actions itself: the control checkout, or the reusable workflow said so via the environment.

    Never decided by the root's contents: a caller controls those.
    """
    return Path(root).resolve() == CONTROL or os.environ.get("SEAL_POLICY_THIS_REPO") == "true"


def is_toolkit_composite(root, path):
    return Path(path).parent.parent == Path(root) / "actions" and path.parent.name in TOOLKIT_COMPOSITES


THIS_REPO = is_this_repo(ROOT)
WORKFLOWS = workflows_of(ROOT)
COMPOSITES = composites_of(ROOT)

EXPECTED_EXEMPT = {"deploy-trigger.yml", "seal-policy.yml", "static-pages-deploy-trigger.yml",
                   "vinext-deploy-trigger.yml"}
SEALED_SHELLS = {"seal-bash {0}", "seal-bash-e {0}"}

SETUP_NAME = "Seal job output"
SETUP_SHELL = "bash"
SETUP_RUN = """\
u="https://raw.githubusercontent.com/Lascade-Co/actions/${{ github.workflow_sha }}/scripts/seal/install.sh"
curl -fsSL --retry 3 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 60 -o "$RUNNER_TEMP/seal-install.tmp" "$u"
test -s "$RUNNER_TEMP/seal-install.tmp" && mv "$RUNNER_TEMP/seal-install.tmp" "$RUNNER_TEMP/seal-install.sh"
SEAL_REF="${{ github.workflow_sha }}" bash "$RUNNER_TEMP/seal-install.sh"
"""
COMPOSITE_SETUP_USES = "Lascade-Co/actions/actions/seal@seal-v1"
COMPOSITE_UPLOAD_USES = "Lascade-Co/actions/actions/seal-upload@seal-v1"
UPLOAD_STEP = {
    "name": "Upload sealed log",
    "if": "always()",
    "uses": "actions/upload-artifact@v7",
    "with": {
        "name": "${{ env.SEAL_LOG_ARTIFACT }}",
        "path": "${{ runner.temp }}/seal/out/",
        "retention-days": 7,
        "if-no-files-found": "ignore",
    },
}


# ---------------------------------------------------------------- loading helpers

def load(path):
    return yaml.safe_load(Path(path).read_text())


def triggers(wf):
    on = wf.get("on", wf.get(True))  # PyYAML reads the bare key `on` as True
    if isinstance(on, str):
        return {on}
    if isinstance(on, list):
        return set(on)
    return set(on or {})


def jobs_with_steps(wf):
    """Jobs defined by steps. Reusable-workflow call jobs (`uses:`) have none and are skipped."""
    return {n: j for n, j in (wf.get("jobs") or {}).items() if "steps" in j}


def norm_run(text):
    return "\n".join(line.rstrip() for line in str(text).strip().splitlines())


def default_shell(wf, job):
    for scope in (job, wf):
        shell = ((scope.get("defaults") or {}).get("run") or {}).get("shell")
        if shell:
            return shell
    return None


def uses_of(step):
    return str(step.get("uses", "")).split("@")[0]


def exempt_names(workflows=None):
    """Only trigger is workflow_call, and no workflow here calls it via uses: ./.github/workflows/."""
    workflows = WORKFLOWS if workflows is None else workflows
    called = set()
    for p in workflows:
        for j in (load(p).get("jobs") or {}).values():
            uses = str(j.get("uses", "")) if isinstance(j, dict) else ""
            if uses.startswith("./.github/workflows/"):
                called.add(posixpath.basename(uses))
    return {p.name for p in workflows if triggers(load(p)) == {"workflow_call"} and p.name not in called}


# ---------------------------------------------------------------- rules (workflow files)

def check_default_shell_is_sealed(wf):
    return [f"job {n}: default shell {default_shell(wf, j)!r} is not sealed"
            for n, j in jobs_with_steps(wf).items() if default_shell(wf, j) not in SEALED_SHELLS]


def is_setup(step):
    extra = set(step) - {"name", "shell", "run"}
    if extra and not (extra == {"working-directory"} and step["working-directory"] == "${{ github.workspace }}"):
        return False  # the only allowed extra key, for jobs whose default directory does not exist yet
    return (step.keys() >= {"name", "shell", "run"} and step["name"] == SETUP_NAME
            and step["shell"] == SETUP_SHELL and norm_run(step["run"]) == norm_run(SETUP_RUN))


def is_composite_setup(step):
    return (set(step) <= {"name", "uses"} and step.get("uses") == COMPOSITE_SETUP_USES
            and isinstance(step.get("name", ""), str))


def is_composite_upload(step):
    return (set(step) <= {"name", "if", "uses"} and step.get("uses") == COMPOSITE_UPLOAD_USES
            and step.get("if") == "always()" and isinstance(step.get("name", ""), str))


def check_setup_step_first(wf):
    return [f"job {n}: first step is not the verbatim setup step or the seal@seal-v1 composite"
            for n, j in jobs_with_steps(wf).items()
            if not j["steps"] or not (is_setup(j["steps"][0]) or is_composite_setup(j["steps"][0]))]


def check_upload_step_last(wf):
    bad = []
    for n, j in jobs_with_steps(wf).items():
        steps = j["steps"]
        last = steps[-1] if steps else {}
        if steps and is_composite_setup(steps[0]):
            ok, what = is_composite_upload(last), "seal-upload@seal-v1 composite with if: always()"
        elif steps and is_setup(steps[0]):
            ok, what = last == UPLOAD_STEP, "verbatim sealed-log upload"
        else:
            ok, what = last == UPLOAD_STEP or is_composite_upload(last), "sealed-log upload"
        if not ok:
            bad.append(f"job {n}: last step is not the {what}")
    return bad


def check_no_unsealed_shell(wf):
    bad = []
    for n, j in jobs_with_steps(wf).items():
        for i, s in enumerate(j["steps"]):
            if "run" not in s or (i == 0 and is_setup(s)):
                continue
            shell = s.get("shell", default_shell(wf, j))
            if shell not in SEALED_SHELLS:
                bad.append(f"job {n} step {i + 1} ({s.get('name', s['run'][:30])!r}): shell {shell!r}")
    return bad


SEAL_PREFIX = "${{ runner.temp }}/seal/"
UPLOADS = ("actions/upload-artifact", "actions/upload-artifact/merge")


def runs_seal_pack(step):
    """The step's script invokes seal-pack as a command (first token of a line), not just mentions it."""
    return any(re.match(r"\s*(?:[\w./$\"{}-]*/)?seal-pack(\s|$)", line) for line in str(step.get("run", "")).splitlines())


def sealed_path(p):
    return p.startswith(SEAL_PREFIX) and posixpath.normpath(p).startswith(SEAL_PREFIX.rstrip("/") + "/")


def check_uploads_sealed(wf):
    bad = []
    for n, j in jobs_with_steps(wf).items():
        steps = j["steps"]
        for i, s in enumerate(steps):
            if uses_of(s) not in UPLOADS or s == UPLOAD_STEP:
                continue
            paths = [p.strip() for p in str((s.get("with") or {}).get("path", "")).splitlines() if p.strip()]
            prev = steps[i - 1] if i else {}
            if uses_of(s) == "actions/upload-artifact/merge":
                bad.append(f"job {n} step {i + 1}: upload-artifact/merge would republish artifacts unsealed")
            elif not paths or not all(sealed_path(p) for p in paths):
                bad.append(f"job {n} step {i + 1}: upload path is not under runner.temp/seal/")
            if uses_of(s) == "actions/upload-artifact" and not runs_seal_pack(prev):
                bad.append(f"job {n} step {i + 1}: previous step does not run seal-pack")
    return bad


def check_downloads_unsealed(wf):
    bad = []
    for n, j in jobs_with_steps(wf).items():
        steps = j["steps"]
        for i, s in enumerate(steps):
            if uses_of(s) != "actions/download-artifact":
                continue
            nxt = steps[i + 1] if i + 1 < len(steps) else {}
            if "seal-unpack" not in str(nxt.get("run", "")):
                bad.append(f"job {n} step {i + 1}: next step does not run seal-unpack")
            elif (nxt.get("env") or {}).get("REPO_AGE_KEY") != "${{ secrets.REPO_AGE_KEY }}":
                bad.append(f"job {n} step {i + 2}: seal-unpack lacks REPO_AGE_KEY from secrets")
    return bad


def check_retention_at_most_7(wf):
    """Retention is a ceiling: retention-days must be set and at most 7 (1-day values pass)."""
    bad = []
    for n, j in jobs_with_steps(wf).items():
        for i, s in enumerate(j["steps"]):
            if uses_of(s) not in (*UPLOADS, "actions/upload-pages-artifact"):
                continue
            days = (s.get("with") or {}).get("retention-days")
            if not isinstance(days, int) or not 1 <= days <= 7:
                bad.append(f"job {n} step {i + 1}: retention-days is {days!r}")
    return bad


def has_contents_read(perms):
    if isinstance(perms, str):
        return perms in {"read-all", "write-all"}
    return isinstance(perms, dict) and perms.get("contents") in {"read", "write"}


def check_permissions_allow_contents_read(wf):
    """The seal action reads repo-key.pub with the job token, so declared permissions must include contents."""
    bad = []
    for n, j in jobs_with_steps(wf).items():
        perms = j["permissions"] if "permissions" in j else wf.get("permissions")
        if perms is not None and not has_contents_read(perms):
            bad.append(f"job {n}: permissions {perms!r} lack contents: read")
    return bad


SEALED_RULES = [check_default_shell_is_sealed, check_setup_step_first, check_upload_step_last,
                check_no_unsealed_shell, check_uploads_sealed, check_downloads_unsealed,
                check_retention_at_most_7, check_permissions_allow_contents_read]


def check_no_pull_request_target(wf):
    return ["uses pull_request_target"] if "pull_request_target" in triggers(wf) else []


SECRET_REFS = [r"\bsecrets\s*[.\[]", r"secrets:\s*inherit", r"\$\{\{\s*secrets\s*\}\}", r"(?m)^\s*secrets:\s*$"]


def check_pull_request_no_secrets(wf, text):
    if "pull_request" not in triggers(wf):
        return []
    return [f"pull_request workflow references secrets ({r})" for r in SECRET_REFS if re.search(r, text)]


def check_repo_key(root):
    f = Path(root) / KEY_FILE
    if not f.is_file():
        return [f"{KEY_FILE} is missing"]
    lines = f.read_text().splitlines()
    return [] if len(lines) == 1 and KEY_RE.match(lines[0]) else [f"{KEY_FILE} is not a single age1... line"]


def check_composite_run_steps_sealed(action):
    if not isinstance(action, dict):
        return ["not a valid action file"]
    return [f"step {i + 1}: shell {s.get('shell')!r}" for i, s in enumerate((action.get("runs") or {}).get("steps", []))
            if "run" in s and s.get("shell") not in SEALED_SHELLS]


def violations(path):
    wf = load(path)
    return [f"{rule.__name__}: {m}" for rule in SEALED_RULES for m in rule(wf)]


def policy_violations(root, this_repo=None):
    """The whole policy as a function of a repo root (what the reusable seal-policy workflow runs on a caller).

    The workflow_call-only exemption and the toolkit-composite exemption apply only to this repo.
    """
    root = Path(root)
    this_repo = is_this_repo(root) if this_repo is None else this_repo
    bad = [] if this_repo else [f"repo-key: {m}" for m in check_repo_key(root)]
    wfs = workflows_of(root)
    exempt = exempt_names(wfs) if this_repo else set()
    for p in wfs:
        wf = load(p)
        if p.name not in exempt:
            bad += [f"{p.name}: {v}" for v in violations(p)]
        bad += [f"{p.name}: {m}" for m in check_no_pull_request_target(wf)]
        bad += [f"{p.name}: {m}" for m in check_pull_request_no_secrets(wf, p.read_text())]
    for p in composites_of(root):
        if this_repo and is_toolkit_composite(root, p):
            continue
        bad += [f"{p.parent.relative_to(root).as_posix()}: {m}" for m in check_composite_run_steps_sealed(load(p))]
    return bad


# ---------------------------------------------------------------- real files

NON_EXEMPT = [p for p in WORKFLOWS if not THIS_REPO or p.name not in exempt_names()]
REAL_COMPOSITES = [p for p in COMPOSITES if not (THIS_REPO and is_toolkit_composite(ROOT, p))]


@pytest.mark.parametrize("path", NON_EXEMPT, ids=lambda p: p.name)
@pytest.mark.parametrize("rule", SEALED_RULES, ids=lambda r: r.__name__.removeprefix("check_"))
def test_sealed_rules(path, rule):
    assert rule(load(path)) == []


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_no_pull_request_target(path):
    assert check_no_pull_request_target(load(path)) == []


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_pull_request_workflows_use_no_secrets(path):
    assert check_pull_request_no_secrets(load(path), path.read_text()) == []


@pytest.mark.skipif(not THIS_REPO, reason="repo-specific assertion")
def test_exempt_set_is_exactly():
    assert exempt_names() == EXPECTED_EXEMPT


@pytest.mark.skipif(not THIS_REPO, reason="repo-specific assertion")
@pytest.mark.parametrize("path", NON_EXEMPT, ids=lambda p: p.name)
def test_this_repo_uses_verbatim_steps_only(path):
    for j in jobs_with_steps(load(path)).values():
        assert is_setup(j["steps"][0]) and j["steps"][-1] == UPLOAD_STEP


@pytest.mark.skipif(THIS_REPO, reason="applies to other repos; this repo's key is scripts/seal/repo-key.pub")
def test_repo_key_file():
    assert check_repo_key(ROOT) == []


@pytest.mark.parametrize("path", REAL_COMPOSITES, ids=lambda p: p.parent.name)
def test_composite_run_steps_sealed(path):
    assert check_composite_run_steps_sealed(load(path)) == []


def test_all_workflows_classified():
    """One-line view of the remaining conversion work."""
    todo = [p.name for p in NON_EXEMPT if violations(p)]
    todo += [f"actions/{p.parent.name}" for p in REAL_COMPOSITES if check_composite_run_steps_sealed(load(p))]
    assert not todo, f"{len(todo)} unsealed: " + ", ".join(todo)


def test_missing_wrapper_fails_closed():
    r = subprocess.run([shutil.which("bash"), "-c", "command -v seal-bash"], env={"PATH": "/nonexistent"},
                       capture_output=True, text=True)
    assert r.returncode != 0
    assert "fails at its first step" in DOCS.read_text(), "docs must state that a job without setup fails closed"


# ---------------------------------------------------------------- synthetic fixtures

def fixture(name):
    return TESTDATA / name


def test_fixture_good_passes_every_rule():
    p = fixture("good.yml")
    assert violations(p) == []
    assert check_no_pull_request_target(load(p)) == []
    assert check_pull_request_no_secrets(load(p), p.read_text()) == []


def test_fixture_setup_working_directory():
    assert violations(fixture("good-setup-workdir.yml")) == []
    assert check_setup_step_first(load(fixture("bad-setup-workdir.yml")))
    assert check_setup_step_first(load(fixture("bad-setup-extra-key.yml")))


def test_fixture_good_job_default_and_reusable_call():
    # job-level shell default overriding the workflow's, plus a reusable-workflow call job
    assert violations(fixture("good-job-default.yml")) == []


BAD_CASES = [
    ("bad-default-shell.yml", check_default_shell_is_sealed),
    ("bad-setup-changed.yml", check_setup_step_first),
    ("bad-setup-not-first.yml", check_setup_step_first),
    ("bad-upload-not-last.yml", check_upload_step_last),
    ("bad-step-shell.yml", check_no_unsealed_shell),
    ("bad-upload-unsealed.yml", check_uploads_sealed),
    ("bad-download-unsealed.yml", check_downloads_unsealed),
    ("bad-retention.yml", check_retention_at_most_7),
]


@pytest.mark.parametrize("name,rule", BAD_CASES, ids=[c[0] for c in BAD_CASES])
def test_fixture_bad_trips_its_rule(name, rule):
    assert rule(load(fixture(name))), f"{rule.__name__} did not flag {name}"


def test_fixture_pull_request_target_flagged():
    assert check_no_pull_request_target(load(fixture("bad-prt.yml")))


@pytest.mark.parametrize("name", ["bad-pr-secrets.yml", "bad-pr-secrets-index.yml", "bad-pr-secrets-inherit.yml",
                                  "bad-pr-secrets-bare.yml", "bad-pr-secrets-key.yml"])
def test_fixture_pull_request_secrets_flagged(name):
    p = fixture(name)
    assert check_pull_request_no_secrets(load(p), p.read_text())


@pytest.mark.parametrize("name", ["bad-upload-traversal.yml", "bad-upload-pack-mention.yml", "bad-upload-merge.yml"])
def test_fixture_upload_hardening(name):
    assert check_uploads_sealed(load(fixture(name)))


def test_fixture_merge_retention_checked():
    assert check_retention_at_most_7(load(fixture("bad-upload-merge.yml")))


def test_fixture_one_day_retention_passes():
    assert check_retention_at_most_7(load(fixture("good.yml"))) == []


def test_fixture_composite():
    assert check_composite_run_steps_sealed(load(fixture("good-composite.yml"))) == []
    assert check_composite_run_steps_sealed(load(fixture("bad-composite.yml")))


# ---------------------------------------------------------------- whole-policy fixtures (mini repo roots)

COMPOSITE_ROOTS = TESTDATA / "composite"


def test_toolkit_composites_exempt_from_seal_bash_rule():
    action = {"runs": {"using": "composite", "steps": [{"run": "echo", "shell": "bash"}]}}
    assert check_composite_run_steps_sealed(action)  # the rule itself still flags plain bash
    assert {"seal", "seal-upload"} == TOOLKIT_COMPOSITES


def test_seal_policy_workflow_is_exempt_and_uses_composites():
    wf = CONTROL / ".github" / "workflows" / "seal-policy.yml"
    assert wf.name in exempt_names(workflows_of(CONTROL))
    steps = load(wf)["jobs"]["policy"]["steps"]
    assert is_composite_setup(steps[0]) and is_composite_upload(steps[-1])


def test_fixture_root_good():
    assert policy_violations(COMPOSITE_ROOTS / "good") == []


def test_fixture_root_good_without_permissions_key():
    assert policy_violations(COMPOSITE_ROOTS / "good-no-permissions") == []


@pytest.mark.parametrize("name,needle", [
    ("bad-upload-no-always", "last step"),
    ("bad-wrong-ref", "first step"),
    ("bad-upload-wrong-ref", "last step"),
    ("bad-composite-with", "first step"),
    ("bad-mixed-forms", "last step"),
    ("bad-permissions", "contents: read"),
    ("bad-workflow-permissions", "contents: read"),
    ("bad-no-key", "repo-key.pub is missing"),
    ("bad-key-malformed", "age1"),
])
def test_fixture_root_bad(name, needle):
    found = policy_violations(COMPOSITE_ROOTS / name)
    assert any(needle in v for v in found), found


def test_permissions_rule_values():
    wf = lambda perms: {"jobs": {"a": {"permissions": perms, "steps": []}}}
    for ok in ("read-all", "write-all", {"contents": "read"}, {"contents": "write", "issues": "read"}):
        assert check_permissions_allow_contents_read(wf(ok)) == []
    for bad in ({}, {"issues": "read"}, {"contents": "none"}, "none"):
        assert check_permissions_allow_contents_read(wf(bad))


# ---------------------------------------------------------------- review fixes

@pytest.mark.parametrize("name,needle", [
    ("bad-run-before-seal", "first step"),
    ("bad-empty-steps", "first step"),
    ("bad-empty-job-permissions", "contents: read"),
    ("bad-uncalled-reusable", "callee.yml"),  # no exemption outside this repo
    ("bad-dot-github-composite", ".github/actions/x"),
])
def test_fixture_root_bad_more(name, needle):
    found = policy_violations(COMPOSITE_ROOTS / name)
    assert any(needle in v for v in found), found


def test_fixture_root_inherits_workflow_permissions():
    assert policy_violations(COMPOSITE_ROOTS / "good-inherits-workflow-permissions") == []


def test_quoted_local_call_is_seen_as_called():
    root = COMPOSITE_ROOTS / "bad-quoted-local-call"
    assert exempt_names(workflows_of(root)) == set()
    assert any("callee.yaml" in v for v in policy_violations(root, this_repo=True))


def test_uncalled_reusable_exempt_only_in_this_repo():
    root = COMPOSITE_ROOTS / "bad-uncalled-reusable"
    assert not any("callee.yml" in v for v in policy_violations(root, this_repo=True))


def _spoof_root(tmp_path):
    shutil.copytree(COMPOSITE_ROOTS / "bad-uncalled-reusable", tmp_path / "r")
    root = tmp_path / "r"
    (root / "scripts" / "seal").mkdir(parents=True)
    (root / "scripts" / "seal" / "repo-key.pub").write_text("")
    (root / "actions" / "seal").mkdir(parents=True)
    (root / "actions" / "seal" / "action.yml").write_text("")
    return root


def test_spoofed_toolkit_files_do_not_make_a_caller_this_repo(tmp_path, monkeypatch):
    monkeypatch.delenv("SEAL_POLICY_THIS_REPO", raising=False)
    root = _spoof_root(tmp_path)
    assert not is_this_repo(root)
    assert any("callee.yml" in v for v in policy_violations(root))


def test_this_repo_env_flag_and_control_root(tmp_path, monkeypatch):
    root = _spoof_root(tmp_path)
    monkeypatch.setenv("SEAL_POLICY_THIS_REPO", "true")
    assert is_this_repo(root)
    assert not any("callee.yml" in v for v in policy_violations(root))
    monkeypatch.setenv("SEAL_POLICY_THIS_REPO", "false")
    assert not is_this_repo(root)
    assert is_this_repo(CONTROL)
