import json
import pytest
import repo_settings as rs

CODEOWNERS = "* @Lascade-Co/backend\n"


def cfg(repo):
    return rs.load_config()[repo]


def current_matching(repo):
    c = cfg(repo)
    rulesets = {"Reviewed main": {**rs.ruleset_payload(c["required_checks"]), "id": 1, "source": repo}}
    if c.get("protect_tags"):
        rulesets["Protected seal tags"] = {**rs.tag_ruleset_payload(c["protect_tags"]), "id": 2}
    return {
        "rulesets": rulesets,
        "environments": {"production": {"has_secrets": True, "deployment_branch_policy": {"protected_branches": False, "custom_branch_policies": True}, "policies": ["main"]}},
        "repo_secrets": [],
        "fork_approval": "all_external_contributors",
        "teams": dict(c["teams"]),
        "codeowners": CODEOWNERS,
    }


def test_ruleset_payload_exact():
    assert rs.ruleset_payload(["unit", "test"]) == {
        "name": "Reviewed main",
        "target": "branch",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
        "bypass_actors": [{"actor_id": 1, "actor_type": "OrganizationAdmin", "bypass_mode": "always"}],
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
                "required_status_checks": [{"context": "unit"}, {"context": "test"}],
            }},
        ],
    }


def test_tag_ruleset_only_for_actions():
    assert rs.load_config()["actions"]["protect_tags"] == ["refs/tags/seal-v*"]
    for r in ("connector", "infra-actions", "store-reviews", "product-actions"):
        assert "protect_tags" not in cfg(r)
        assert not any(c[2] and c[2].get("name") == "Protected seal tags" for c in rs.plan(r, {**current_matching(r), "rulesets": {}}))
    p = rs.tag_ruleset_payload(["refs/tags/seal-v*"])
    assert p["name"] == "Protected seal tags" and p["target"] == "tag"
    assert p["conditions"] == {"ref_name": {"include": ["refs/tags/seal-v*"], "exclude": []}}
    assert [r["type"] for r in p["rules"]] == ["update", "deletion", "creation"]
    assert p["bypass_actors"] == [{"actor_id": 1, "actor_type": "OrganizationAdmin", "bypass_mode": "always"}]
    calls = rs.plan("actions", {**current_matching("actions"), "rulesets": {}})
    assert any(c[2] and c[2].get("name") == "Protected seal tags" for c in calls)


@pytest.mark.parametrize("repo", ["actions", "connector", "infra-actions", "store-reviews", "product-actions"])
def test_plan_is_empty_when_current_matches(repo):
    assert rs.plan(repo, current_matching(repo)) == []


def test_plan_updates_ruleset_by_name():
    cur = current_matching("connector")
    cur["rulesets"]["Reviewed main"]["rules"][2]["parameters"]["required_approving_review_count"] = 0
    calls = rs.plan("connector", cur)
    assert [(m, p) for m, p, _ in calls] == [("PUT", "/repos/Lascade-Co/connector/rulesets/1")]
    assert calls[0][2] == rs.ruleset_payload(cfg("connector")["required_checks"])
    cur["rulesets"].pop("Reviewed main")
    calls = rs.plan("connector", cur)
    assert [(m, p) for m, p, _ in calls] == [("POST", "/repos/Lascade-Co/connector/rulesets")]


def test_plan_environment_policy_only_if_missing():
    cur = current_matching("connector")
    cur["environments"]["production"]["policies"] = []
    assert rs.plan("connector", cur) == [("POST", "/repos/Lascade-Co/connector/environments/production/deployment-branch-policies", {"name": "main", "type": "branch"})]
    cur["environments"] = {}
    calls = rs.plan("connector", cur)
    assert calls == rs.environment_calls("connector")
    assert calls[0] == ("PUT", "/repos/Lascade-Co/connector/environments/production", {"deployment_branch_policy": {"protected_branches": False, "custom_branch_policies": True}})


def test_plan_covers_other_secret_environments():
    cur = current_matching("actions")
    cur["environments"]["cloudflare-prod"] = {"has_secrets": True, "deployment_branch_policy": None, "policies": []}
    cur["environments"]["empty-env"] = {"has_secrets": False, "deployment_branch_policy": None, "policies": []}
    paths = [p for _, p, _ in rs.plan("actions", cur)]
    assert any("environments/cloudflare-prod" in p for p in paths)
    assert not any("empty-env" in p for p in paths)


def test_plan_fork_and_teams():
    cur = current_matching("product-actions")
    cur["fork_approval"] = "first_time_contributors"
    cur["teams"] = {"backend": "push"}
    calls = rs.plan("product-actions", cur)
    assert ("PUT", "/repos/Lascade-Co/product-actions/actions/permissions/fork-pr-contributor-approval", {"approval_policy": "all_external_contributors"}) in calls
    assert ("PUT", "/orgs/Lascade-Co/teams/product/repos/Lascade-Co/product-actions", {"permission": "push"}) in calls
    assert not any("/teams/backend/" in p for _, p, _ in calls)


def test_check_flags_repo_secret_not_exempt():
    cur = current_matching("store-reviews")
    cur["repo_secrets"] = ["CI_APP_CLIENT_ID", "CI_APP_PRIVATE_KEY", "OTHER"]
    probs = rs.check("store-reviews", cur)
    assert len(probs) == 1 and "OTHER" in probs[0]


def test_check_flags_missing_codeowners():
    cur = current_matching("connector")
    cur["codeowners"] = None
    assert any("CODEOWNERS" in p for p in rs.check("connector", cur))
    cur["codeowners"] = "* @someone\n"
    assert any("CODEOWNERS" in p for p in rs.check("connector", cur))
    cur["codeowners"] = CODEOWNERS
    assert rs.check("connector", cur) == []


def test_check_flags_settings_drift():
    cur = current_matching("connector")
    cur["rulesets"] = {}
    cur["fork_approval"] = None
    cur["environments"]["production"]["policies"] = []
    probs = " ".join(rs.check("connector", cur))
    assert "Reviewed main" in probs and "fork" in probs and "production" in probs


def test_product_team_only_on_product_actions():
    for r, c in rs.load_config().items():
        assert ("product" in c["teams"]) == (r == "product-actions")
        assert c["teams"]["backend"] == "push"
    assert rs.load_config()["store-reviews"]["secret_exempt"] == ["CI_APP_CLIENT_ID", "CI_APP_PRIVATE_KEY"]


KEY = "age1" + "q" * 58


def write(root, rel, text):
    f = root / rel
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(text)


def test_offline_ok_and_bad(tmp_path):
    write(tmp_path, ".github/CODEOWNERS", CODEOWNERS)
    write(tmp_path, ".github/seal/env-exempt", "# c\n\nci.yml:call  # cross-repo\n")
    write(tmp_path, ".github/seal/repo-key.pub", KEY + "\n")
    assert rs.check_offline("connector", tmp_path) == []
    write(tmp_path, ".github/CODEOWNERS", "* @x\n")
    write(tmp_path, ".github/seal/env-exempt", "badline\n")
    write(tmp_path, ".github/seal/repo-key.pub", "nope\n")
    probs = " ".join(rs.check_offline("connector", tmp_path))
    assert "CODEOWNERS" in probs and "env-exempt" in probs and "repo-key.pub" in probs


def test_cli_dry_run_makes_no_writes(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(rs, "gh_api", lambda *a, **k: calls.append(a))
    monkeypatch.setattr(rs, "fetch_current", lambda repo: {**current_matching(repo), "fork_approval": None})
    assert rs.main(["apply", "connector", "--dry-run"]) == 0
    assert calls == [] and "PUT /repos/Lascade-Co/connector/actions/permissions" in capsys.readouterr().out
