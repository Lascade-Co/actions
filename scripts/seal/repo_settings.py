#!/usr/bin/env python3
"""Repo settings for sealed repositories: Reviewed main ruleset, production environment, fork approval, teams.

Payload builders, `plan` and `check` are pure; they take bypass App ids already resolved from the
`bypass_apps` slugs in sealed_repos.json. Only the CLI layer shells out to `gh api`.
Usage: repo_settings.py {check|apply} <repo>... [--dry-run]
       repo_settings.py check <repo> --offline <checkout path>
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ORG = "Lascade-Co"
HERE = Path(__file__).resolve().parent
RULESET = "Reviewed main"
TAG_RULESET = "Protected seal tags"
CODEOWNERS_TEXT = "* @Lascade-Co/backend"
BRANCH_POLICY = {"protected_branches": False, "custom_branch_policies": True}
FORK_POLICY = "all_external_contributors"
KEY_RE = re.compile(r"^age1[02-9ac-hj-np-z]{58}$")
ADMIN_BYPASS = [{"actor_id": 1, "actor_type": "OrganizationAdmin", "bypass_mode": "always"}]


def load_config():
    return json.loads((HERE / "sealed_repos.json").read_text())


def _app_bypass(app_ids):
    return [{"actor_id": int(i), "actor_type": "Integration", "bypass_mode": "always"} for i in app_ids]


def ruleset_payload(required_checks, bypass_app_ids=()):
    return {
        "name": RULESET,
        "target": "branch",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
        "bypass_actors": [dict(a) for a in ADMIN_BYPASS] + _app_bypass(bypass_app_ids),
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {"type": "pull_request", "parameters": {
                "required_approving_review_count": 1,
                "require_code_owner_review": True,
                "dismiss_stale_reviews_on_push": True,
                "require_last_push_approval": True,
                "required_review_thread_resolution": False,
            }},
            {"type": "required_status_checks", "parameters": {
                "strict_required_status_checks_policy": False,
                "required_status_checks": [{"context": c} for c in required_checks],
            }},
        ],
    }


def tag_ruleset_payload(patterns):
    return {
        "name": TAG_RULESET,
        "target": "tag",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": list(patterns), "exclude": []}},
        "bypass_actors": [dict(a) for a in ADMIN_BYPASS],
        "rules": [{"type": "update"}, {"type": "deletion"}, {"type": "creation"}],
    }


def _env_path(repo, env):
    return f"/repos/{ORG}/{repo}/environments/{env}"


def environment_calls(repo, envs=("production",)):
    calls = []
    for env in envs:
        calls.append(("PUT", _env_path(repo, env), {"deployment_branch_policy": dict(BRANCH_POLICY)}))
        calls.append(("POST", _env_path(repo, env) + "/deployment-branch-policies", {"name": "main", "type": "branch"}))
    return calls


def _subset(want, have):
    """True if `want` is contained in `have` (dicts recursively, lists as unordered sets of matches)."""
    if isinstance(want, dict):
        if want.get("actor_type") == "OrganizationAdmin":
            # GitHub returns the org-admin bypass actor with actor_id null, though 1 is sent on write.
            want = {k: v for k, v in want.items() if k != "actor_id"}
        return isinstance(have, dict) and all(k in have and _subset(v, have[k]) for k, v in want.items())
    if isinstance(want, list):
        return (isinstance(have, list) and len(want) == len(have)
                and all(any(_subset(w, h) for h in have) for w in want))
    return want == have


def _ruleset_ok(want, have):
    return have is not None and _subset(want, have)


def _desired_rulesets(cfg, bypass_app_ids=()):
    out = [ruleset_payload(cfg["required_checks"], bypass_app_ids)]
    if cfg.get("protect_tags"):
        out.append(tag_ruleset_payload(cfg["protect_tags"]))
    return out


def _secret_envs(cfg, current):
    names = ["production", *cfg.get("extra_environments", [])]
    for name, env in current.get("environments", {}).items():
        if env.get("has_secrets") and name not in names:
            names.append(name)
    return names


def extra_policies(current, env):
    """Deployment branch policies other than the single `main` branch policy (reported, never deleted)."""
    have = current.get("environments", {}).get(env) or {}
    return [p for p in have.get("policies", []) if p != "main"]


def manual_problems(repo, current):
    cfg = load_config()[repo]
    return [f"environment '{e}' has extra deployment policies {extra_policies(current, e)}; remove them manually (only 'main' is allowed)"
            for e in _secret_envs(cfg, current) if extra_policies(current, e)]


def bypass_problems(repo, bypass_app_ids):
    """Reasons `Reviewed main` cannot be applied yet: a repo whose workflows push to main needs its App bypass."""
    cfg = load_config()[repo]
    slugs = cfg.get("bypass_apps", [])
    if cfg.get("requires_bypass_app") and not slugs:
        return ["Reviewed main needs the CI App slug in sealed_repos.json bypass_apps "
                "(this repo's workflows push to main); refusing until it is set"]
    if len(bypass_app_ids) != len(slugs):
        return [f"could not resolve bypass_apps {slugs} to App ids"]
    return []


def plan(repo, current, bypass_app_ids=()):
    cfg = load_config()[repo]
    base = f"/repos/{ORG}/{repo}"
    calls = []
    for want in _desired_rulesets(cfg, bypass_app_ids):
        have = current.get("rulesets", {}).get(want["name"])
        if have is None:
            calls.append(("POST", f"{base}/rulesets", want))
        elif not _ruleset_ok(want, have):
            calls.append(("PUT", f"{base}/rulesets/{have['id']}", want))
    for env in _secret_envs(cfg, current):
        have = current.get("environments", {}).get(env)
        if have is None or have.get("deployment_branch_policy") != BRANCH_POLICY:
            calls.append(("PUT", _env_path(repo, env), {"deployment_branch_policy": dict(BRANCH_POLICY)}))
            have_policies = [] if have is None else have.get("policies", [])
        else:
            have_policies = have.get("policies", [])
        if "main" not in have_policies:
            calls.append(("POST", _env_path(repo, env) + "/deployment-branch-policies", {"name": "main", "type": "branch"}))
    if current.get("fork_approval") != FORK_POLICY:
        calls.append(("PUT", f"{base}/actions/permissions/fork-pr-contributor-approval", {"approval_policy": FORK_POLICY}))
    for team, perm in cfg["teams"].items():
        if current.get("teams", {}).get(team) != perm:
            calls.append(("PUT", f"/orgs/{ORG}/teams/{team}/repos/{ORG}/{repo}", {"permission": perm}))
    return calls


def check(repo, current, bypass_app_ids=()):
    cfg = load_config()[repo]
    problems = bypass_problems(repo, bypass_app_ids)
    for want in _desired_rulesets(cfg, bypass_app_ids):
        have = current.get("rulesets", {}).get(want["name"])
        if have is None:
            problems.append(f"ruleset '{want['name']}' is missing")
        elif not _ruleset_ok(want, have):
            problems.append(f"ruleset '{want['name']}' differs from the required settings")
    for env in _secret_envs(cfg, current):
        have = current.get("environments", {}).get(env)
        if have is None:
            problems.append(f"environment '{env}' does not exist")
        elif have.get("deployment_branch_policy") != BRANCH_POLICY or "main" not in have.get("policies", []):
            problems.append(f"environment '{env}' does not restrict deployments to the main branch")
    problems += manual_problems(repo, current)
    if current.get("fork_approval") != FORK_POLICY:
        problems.append(f"fork PR approval is {current.get('fork_approval')!r}, want {FORK_POLICY!r}")
    for team, perm in cfg["teams"].items():
        if current.get("teams", {}).get(team) != perm:
            problems.append(f"team '{team}' lacks '{perm}' access")
    for name in current.get("repo_secrets", []):
        if name not in cfg["secret_exempt"]:
            problems.append(f"repo-level Actions secret '{name}' is not exempt (move it to the production environment)")
    problems += _codeowners_problems(current.get("codeowners"))
    return problems


def _codeowners_problems(text):
    if text is None:
        return [".github/CODEOWNERS is missing"]
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if lines != [CODEOWNERS_TEXT]:
        return [f".github/CODEOWNERS must contain exactly '{CODEOWNERS_TEXT}'"]
    return []


def check_offline(repo, root):
    root = Path(root)
    problems = []
    co = root / ".github" / "CODEOWNERS"
    problems += _codeowners_problems(co.read_text() if co.is_file() else None)
    ex = root / ".github" / "seal" / "env-exempt"
    if ex.is_file():
        for n, line in enumerate(ex.read_text().splitlines(), 1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if not re.match(r"^[^\s:#]+:[A-Za-z0-9_-]+\s+#\s*\S", line):
                problems.append(f".github/seal/env-exempt line {n} must be '<workflow file>:<job id>  # reason'")
    key = root / ".github" / "seal" / "repo-key.pub"
    if key.is_file():
        lines = key.read_text().splitlines()
        if len(lines) != 1 or not KEY_RE.match(lines[0].strip()):
            problems.append(".github/seal/repo-key.pub must be a single age1... line")
    elif repo != "actions":
        problems.append(".github/seal/repo-key.pub is missing")
    return problems


# ---- CLI layer: the only code that calls gh ----

def gh_api(method, path, body=None, raw=False, allow_404=False, accept=None):
    cmd = ["gh", "api", "-X", method, path]
    if raw:
        accept = "application/vnd.github.raw+json"
    if accept:
        cmd += ["-H", f"Accept: {accept}"]
    if body is not None:
        cmd += ["--input", "-"]
    r = subprocess.run(cmd, input=json.dumps(body) if body is not None else None,
                       capture_output=True, text=True)
    if r.returncode != 0:
        if allow_404 and "HTTP 404" in r.stderr + r.stdout:
            return None
        raise RuntimeError(f"gh api {method} {path} failed: {(r.stderr or r.stdout).strip()}")
    if raw:
        return r.stdout
    return json.loads(r.stdout) if r.stdout.strip() else {}


def resolve_bypass_app_ids(cfg):
    """App ids for the configured `bypass_apps` slugs (GET /apps/{slug} is public)."""
    return [gh_api("GET", f"/apps/{slug}")["id"] for slug in cfg.get("bypass_apps", [])]


ROLE_TO_PERMISSION = {"write": "push", "read": "pull"}


def team_permission(resp):
    """Map a team-repo response to the permission names the PUT uses (push/pull/maintain/triage/admin)."""
    role = resp.get("role_name")
    if isinstance(role, str):
        return ROLE_TO_PERMISSION.get(role, role)
    perms = resp.get("permissions") or {}
    for key, name in (("admin", "admin"), ("maintain", "maintain"), ("push", "push"), ("triage", "triage"), ("pull", "pull")):
        if perms.get(key):
            return name
    return None


def fetch_current(repo):
    base = f"/repos/{ORG}/{repo}"
    rulesets = {}
    for s in gh_api("GET", f"{base}/rulesets?per_page=100") or []:
        rulesets[s["name"]] = gh_api("GET", f"{base}/rulesets/{s['id']}")
    envs = {}
    for e in (gh_api("GET", f"{base}/environments?per_page=100") or {}).get("environments", []):
        n = e["name"]
        secrets = gh_api("GET", f"{base}/environments/{n}/secrets?per_page=100")["secrets"]
        pols = gh_api("GET", f"{base}/environments/{n}/deployment-branch-policies?per_page=100", allow_404=True) or {}
        envs[n] = {
            "has_secrets": bool(secrets),
            "deployment_branch_policy": e.get("deployment_branch_policy"),
            "policies": [p["name"] if p.get("type", "branch") == "branch" else f"{p['type']}:{p['name']}"
                         for p in pols.get("branch_policies", [])],
        }
    fork = gh_api("GET", f"{base}/actions/permissions/fork-pr-contributor-approval", allow_404=True)
    teams = {}
    for team in load_config()[repo]["teams"]:
        t = gh_api("GET", f"/orgs/{ORG}/teams/{team}/repos/{ORG}/{repo}", allow_404=True,
                   accept="application/vnd.github.v3.repository+json")
        if t:
            teams[team] = team_permission(t)
    return {
        "rulesets": rulesets,
        "environments": envs,
        "repo_secrets": [s["name"] for s in gh_api("GET", f"{base}/actions/secrets?per_page=100")["secrets"]],
        "fork_approval": (fork or {}).get("approval_policy"),
        "teams": teams,
        "codeowners": gh_api("GET", f"{base}/contents/.github/CODEOWNERS", raw=True, allow_404=True),
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["check", "apply"])
    ap.add_argument("repos", nargs="+")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--offline", metavar="PATH")
    a = ap.parse_args(argv)
    cfg = load_config()
    for r in a.repos:
        if r not in cfg:
            ap.error(f"unknown repo {r}; known: {', '.join(cfg)}")
    if a.offline and (a.command != "check" or len(a.repos) != 1):
        ap.error("--offline works only with `check <repo>`")
    rc = 0
    for repo in a.repos:
        try:
            if a.offline:
                problems = check_offline(repo, a.offline)
            else:
                app_ids = resolve_bypass_app_ids(cfg[repo])
                if a.command == "check":
                    problems = check(repo, fetch_current(repo), app_ids)
                else:
                    problems = None
        except RuntimeError as e:
            print(f"{repo}: ERROR {e}")
            rc = 1
            continue
        if problems is None:
            refused = bypass_problems(repo, app_ids)
            for msg in refused:
                print(f"{repo}: REFUSED {msg}")
            if refused:
                rc = 1
                continue
            try:
                cur = fetch_current(repo)
                for msg in manual_problems(repo, cur):
                    print(f"{repo}: MANUAL {msg}")
                    rc = 1
                for method, path, body in plan(repo, cur, app_ids):
                    print(f"{repo}: {method} {path} {json.dumps(body) if body is not None else ''}".rstrip())
                    if not a.dry_run:
                        gh_api(method, path, body)
            except RuntimeError as e:
                print(f"{repo}: ERROR {e}")
                rc = 1
            continue
        print(f"{repo}: {'ok' if not problems else f'{len(problems)} problem(s)'}")
        for p in problems:
            print(f"  - {p}")
        rc = rc or bool(problems)
    return int(rc)


if __name__ == "__main__":
    sys.exit(main())
