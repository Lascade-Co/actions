"""Sealing policy for every workflow and composite action in this repo.

The rule functions (`check_*`) return a list of violation strings. Real files must return
none; the synthetic files under testdata/ prove each rule can both pass and fail.
"""
import posixpath
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
WORKFLOWS = sorted([*(ROOT / ".github" / "workflows").glob("*.yml"), *(ROOT / ".github" / "workflows").glob("*.yaml")])
COMPOSITES = sorted([*(ROOT / "actions").glob("*/action.yml"), *(ROOT / "actions").glob("*/action.yaml")])
TESTDATA = HERE / "testdata"
DOCS = ROOT / "docs" / "sealed-output.md"

EXPECTED_EXEMPT = {"deploy-trigger.yml", "static-pages-deploy-trigger.yml", "vinext-deploy-trigger.yml"}
SEALED_SHELLS = {"seal-bash {0}", "seal-bash-e {0}"}

SETUP_NAME = "Seal job output"
SETUP_SHELL = "bash"
SETUP_RUN = """\
u="https://raw.githubusercontent.com/Lascade-Co/actions/${{ github.workflow_sha }}/scripts/seal/install.sh"
curl -fsSL --retry 3 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 60 -o "$RUNNER_TEMP/seal-install.tmp" "$u"
test -s "$RUNNER_TEMP/seal-install.tmp" && mv "$RUNNER_TEMP/seal-install.tmp" "$RUNNER_TEMP/seal-install.sh"
SEAL_REF="${{ github.workflow_sha }}" bash "$RUNNER_TEMP/seal-install.sh"
"""
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


def exempt_names():
    """Only trigger is workflow_call, and no workflow here calls it via uses: ./.github/workflows/."""
    called = set()
    for p in WORKFLOWS:
        called |= set(re.findall(r"uses:\s*\./\.github/workflows/([\w.-]+\.ya?ml)", p.read_text()))
    return {p.name for p in WORKFLOWS if triggers(load(p)) == {"workflow_call"} and p.name not in called}


# ---------------------------------------------------------------- rules (workflow files)

def check_default_shell_is_sealed(wf):
    return [f"job {n}: default shell {default_shell(wf, j)!r} is not sealed"
            for n, j in jobs_with_steps(wf).items() if default_shell(wf, j) not in SEALED_SHELLS]


def is_setup(step):
    return (set(step) == {"name", "shell", "run"} and step["name"] == SETUP_NAME
            and step["shell"] == SETUP_SHELL and norm_run(step["run"]) == norm_run(SETUP_RUN))


def check_setup_step_first(wf):
    return [f"job {n}: first step is not the verbatim setup step"
            for n, j in jobs_with_steps(wf).items() if not j["steps"] or not is_setup(j["steps"][0])]


def check_upload_step_last(wf):
    return [f"job {n}: last step is not the verbatim sealed-log upload"
            for n, j in jobs_with_steps(wf).items() if not j["steps"] or j["steps"][-1] != UPLOAD_STEP]


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


SEALED_RULES = [check_default_shell_is_sealed, check_setup_step_first, check_upload_step_last,
                check_no_unsealed_shell, check_uploads_sealed, check_downloads_unsealed,
                check_retention_at_most_7]


def check_no_pull_request_target(wf):
    return ["uses pull_request_target"] if "pull_request_target" in triggers(wf) else []


SECRET_REFS = [r"\bsecrets\s*[.\[]", r"secrets:\s*inherit", r"\$\{\{\s*secrets\s*\}\}", r"(?m)^\s*secrets:\s*$"]


def check_pull_request_no_secrets(wf, text):
    if "pull_request" not in triggers(wf):
        return []
    return [f"pull_request workflow references secrets ({r})" for r in SECRET_REFS if re.search(r, text)]


def check_composite_run_steps_sealed(action):
    return [f"step {i + 1}: shell {s.get('shell')!r}" for i, s in enumerate((action.get("runs") or {}).get("steps", []))
            if "run" in s and s.get("shell") not in SEALED_SHELLS]


def violations(path):
    wf = load(path)
    return [f"{rule.__name__}: {m}" for rule in SEALED_RULES for m in rule(wf)]


# ---------------------------------------------------------------- real files

NON_EXEMPT = [p for p in WORKFLOWS if p.name not in exempt_names()]


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


def test_exempt_set_is_exactly():
    assert exempt_names() == EXPECTED_EXEMPT


@pytest.mark.parametrize("path", COMPOSITES, ids=lambda p: p.parent.name)
def test_composite_run_steps_sealed(path):
    assert check_composite_run_steps_sealed(load(path)) == []


def test_all_workflows_classified():
    """One-line view of the remaining conversion work."""
    todo = [p.name for p in NON_EXEMPT if violations(p)]
    todo += [f"actions/{p.parent.name}" for p in COMPOSITES if check_composite_run_steps_sealed(load(p))]
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
