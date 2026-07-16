"""Summarise one repo's last-24h developer activity with Codex.

Run inside a matrix job AFTER Codex auth + sandbox are set up. For a single
repo it:

  1. Shallow-clones every branch within the look-back window.
  2. Collects non-merge commits, dropping bot authors.
  3. Classifies each commit's delivery status DETERMINISTICALLY from branch/PR
     state: on the default branch -> Published; on a branch with an open PR ->
     Testing; on a branch with no PR -> Work in Progress.
  4. Codex pass 1: classifies each commit message as descriptive or
     missing-info (writes classify.json).
  5. Pulls the git diff only for missing-info commits.
  6. Codex pass 2: writes emoji bullets per developer, grouped by the status
     from step 3 (repo-summary.json).
  7. Resolves each author's GitHub login and writes the per-repo artifact.
  8. Enriches with merged PRs, active branches, and the in-window version tag.

The published schema:
    {repo,
     developers: [{login, name, commit_count,
                   bullets: {"Published": [str], "Testing": [str],
                             "Work in Progress": [str]}}],
     prs: [{number, title, author}], branches: [str], version: str|null}
`bullets` keys only the statuses that have work. commit_count is authoritative
from git and the status split is deterministic — Codex is trusted only for the
bullet prose. Enrichment fields are best-effort: a lookup failure degrades to
[] / null without aborting.

Usage:
    GH_TOKEN=... python scripts/catchup/catchup_repo.py \
        --repo Lascade-Co/example \
        --classify-prompt CATCHUP_CLASSIFY.md \
        --summary-prompt CATCHUP_SUMMARY.md \
        --out summary-Lascade-Co__example.json

Requires: `git`, `gh`, and the `codex` CLI on PATH (Codex auth pre-restored).
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

FIELD = "\x1f"   # unit separator between log fields
RECORD = "\x1e"  # record separator between commits

BOT_NAMES = {"dependabot", "dependabot[bot]", "github-actions",
             "github-actions[bot]", "web-flow"}

MAX_DIFF_CHARS = 15000      # cap a single commit diff
MAX_TOTAL_DIFF_CHARS = 120000  # cap the combined diff payload to Codex
MAX_COMMITS_PER_DEV = 50    # cap commits sent to Codex (count stays accurate)

# Delivery status of a commit, decided deterministically from branch/PR state.
STATUS_PUBLISHED = "Published"          # reachable from the default branch
STATUS_TESTING = "Testing"              # on a branch with an open PR
STATUS_WIP = "Work in Progress"         # on a branch with no PR
STATUS_ORDER = [STATUS_PUBLISHED, STATUS_TESTING, STATUS_WIP]


def log(msg):
    print(msg, file=sys.stderr)


def run(cmd, **kw):
    # errors="replace": diffs can contain bytes that aren't valid UTF-8
    # (e.g. latin-1 source files git treats as text) — decode instead of crash.
    return subprocess.run(cmd, check=True, capture_output=True, text=True,
                          errors="replace", **kw)


def is_bot(name, email):
    low_name = name.strip().lower()
    if low_name.endswith("[bot]") or low_name in BOT_NAMES:
        return True
    low_email = email.lower()
    return "dependabot" in low_email or "github-actions" in low_email


def clone(repo, token, workdir, hours):
    url = f"https://x-access-token:{token}@github.com/{repo}.git"
    try:
        run(["git", "clone", "--no-single-branch",
             f"--shallow-since={hours} hours ago", "--filter=blob:none",
             url, workdir])
    except subprocess.CalledProcessError as exc:
        # --shallow-since can fail when the cutoff predates all history;
        # fall back to a normal blobless clone so git log can still filter.
        log(f"shallow-since clone failed, retrying full clone: {exc.stderr}")
        run(["git", "clone", "--no-single-branch", "--filter=blob:none",
             url, workdir])


def collect_commits(workdir, hours):
    """Return list of {sha, name, email, subject, body} for the window."""
    fmt = FIELD.join(["%H", "%an", "%ae", "%s", "%b"]) + RECORD
    out = run(["git", "-C", workdir, "log", "--all", "--no-merges",
               f"--since={hours} hours ago", f"--pretty=format:{fmt}"]).stdout
    commits = []
    seen = set()
    for record in out.split(RECORD):
        record = record.strip("\n")
        if not record.strip():
            continue
        parts = record.split(FIELD)
        if len(parts) < 5:
            continue
        sha, name, email, subject, body = parts[:5]
        if sha in seen:
            continue
        seen.add(sha)
        if is_bot(name, email):
            continue
        commits.append({"sha": sha, "name": name, "email": email,
                        "subject": subject, "body": body.strip()})
    return commits


def run_codex(prompt_template_path, payload_text, scratch, output_name):
    """Append payload to the prompt template, run Codex, return parsed JSON."""
    with open(prompt_template_path) as fh:
        prompt = fh.read()
    prompt_file = os.path.join(scratch, "prompt.md")
    with open(prompt_file, "w") as fh:
        fh.write(prompt + "\n\n" + payload_text + "\n")

    out_path = os.path.join(scratch, output_name)
    if os.path.exists(out_path):
        os.remove(out_path)

    with open(prompt_file) as stdin:
        subprocess.run(
            ["codex", "exec", "--sandbox", "workspace-write",
             "--skip-git-repo-check", "-"],
            stdin=stdin, cwd=scratch, check=True,
            capture_output=True, text=True,
        )
    with open(out_path) as fh:
        return json.load(fh)


def resolve_logins(repo, devs):
    """Map each developer's first commit SHA to a GitHub login (best effort)."""
    for dev in devs:
        dev["login"] = None
        try:
            out = run(["gh", "api", f"/repos/{repo}/commits/{dev['shas'][0]}",
                       "--jq", ".author.login"]).stdout.strip()
            if out and out != "null":
                dev["login"] = out
        except subprocess.CalledProcessError as exc:
            log(f"login lookup failed for {dev['name']}: {exc.stderr}")
    return devs


def group_by_author(commits):
    """Group commits by author email; merge groups sharing a resolved login."""
    by_email = {}
    for c in commits:
        dev = by_email.setdefault(c["email"], {
            "name": c["name"], "email": c["email"], "commits": [], "shas": [],
        })
        dev["commits"].append(c)
        dev["shas"].append(c["sha"])
    return list(by_email.values())


def merge_by_login(devs):
    """Collapse author groups that resolved to the same GitHub login."""
    merged = {}
    standalone = []
    for dev in devs:
        login = dev.get("login")
        if not login:
            standalone.append(dev)
            continue
        if login in merged:
            merged[login]["commits"].extend(dev["commits"])
        else:
            merged[login] = dev
    return list(merged.values()) + standalone


def get_diff(workdir, sha):
    out = run(["git", "-C", workdir, "show", "--no-color", "--format=",
               sha]).stdout
    if len(out) > MAX_DIFF_CHARS:
        out = out[:MAX_DIFF_CHARS] + "\n... [diff truncated] ...\n"
    return out


def rev_set(workdir, *args):
    """SHAs from `git rev-list <args>` as a set (empty on error)."""
    try:
        out = run(["git", "-C", workdir, "rev-list", *args]).stdout
        return {line.strip() for line in out.splitlines() if line.strip()}
    except subprocess.CalledProcessError:
        return set()


def open_pr_branches(repo):
    """Head branch names of the repo's currently-open PRs (best effort)."""
    try:
        out = run(["gh", "pr", "list", "--repo", repo, "--state", "open",
                   "--limit", "100", "--json", "headRefName"]).stdout
        return [pr.get("headRefName") for pr in json.loads(out)
                if pr.get("headRefName")]
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        log(f"{repo}: open-PR lookup failed: {exc}")
        return []


def classify_status(workdir, repo, hours, default_branch, shas):
    """Map each in-window sha -> Published / Testing / Work in Progress.

    Published        : reachable from the default branch (landed on main).
    Testing          : on a branch that has an OPEN PR but not yet on main.
    Work in Progress : on a branch with no PR.

    Precedence is Published > Testing > WIP, so a commit that has reached main
    counts as Published even if its branch still exists.
    """
    since = [f"--since={hours} hours ago", "--no-merges"]
    default = default_branch or "main"
    published = rev_set(workdir, f"origin/{default}", *since)
    testing = set()
    for branch in open_pr_branches(repo):
        testing |= rev_set(workdir, f"origin/{default}..origin/{branch}", *since)
    testing -= published

    status = {}
    for sha in shas:
        if sha in published:
            status[sha] = STATUS_PUBLISHED
        elif sha in testing:
            status[sha] = STATUS_TESTING
        else:
            status[sha] = STATUS_WIP
    return status


def build_summary_payload(repo, devs, missing_shas, status_by_sha, workdir):
    """Per developer, the window's commits grouped by delivery status."""
    total_diff = 0
    payload_devs = []
    for dev in devs:
        work = {}
        for c in dev["commits"][:MAX_COMMITS_PER_DEV]:
            entry = {"sha": c["sha"], "subject": c["subject"],
                     "body": c["body"]}
            if c["sha"] in missing_shas and total_diff < MAX_TOTAL_DIFF_CHARS:
                diff = get_diff(workdir, c["sha"])
                total_diff += len(diff)
                entry["diff"] = diff
            work.setdefault(status_by_sha.get(c["sha"], STATUS_WIP), []).append(entry)
        payload_devs.append({
            "login": dev.get("login"),
            "name": dev["name"],
            "commit_count": len(dev["commits"]),
            "work": {st: work[st] for st in STATUS_ORDER if st in work},
        })
    return {"repo": repo, "developers": payload_devs}


def fallback_bullets(dev, status_by_sha):
    """Degraded status-grouped bullets from commit subjects (no Codex)."""
    grouped, seen = {}, set()
    for c in dev["commits"]:
        subj = c["subject"].strip()
        if not subj or subj.lower() in seen:
            continue
        seen.add(subj.lower())
        st = status_by_sha.get(c["sha"], STATUS_WIP)
        bucket = grouped.setdefault(st, [])
        if len(bucket) < 5:
            bucket.append(f"• {subj}")
    return {st: grouped[st] for st in STATUS_ORDER if st in grouped}


# --- Enrichment (best-effort; every helper swallows its own errors so a
#     missing PR list / branch / tag never aborts the per-repo summary). ---

def gather_prs(repo, cutoff):
    """Merged PRs since exact UTC `cutoff` -> [{number, title, author}]."""
    cutoff_iso = cutoff.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        out = run(["gh", "pr", "list", "--repo", repo, "--state", "merged",
                   "--search", f"merged:>={cutoff_iso}",
                   "--limit", "50",
                   "--json", "number,title,author"]).stdout
        prs = []
        for pr in json.loads(out):
            author = (pr.get("author") or {})
            prs.append({"number": pr.get("number"),
                        "title": pr.get("title", ""),
                        "author": author.get("login") or author.get("name") or ""})
        return prs
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        log(f"{repo}: PR lookup failed: {exc}")
        return []


def gather_branches(workdir, cutoff, default_branch):
    """Remote branch names with a commit at/after `cutoff` (default excluded)."""
    try:
        out = run(["git", "-C", workdir, "for-each-ref",
                   "--format=%(refname:short) %(committerdate:unix)",
                   "refs/remotes/origin"]).stdout
        cutoff_unix = int(cutoff.timestamp())
        branches = []
        for line in out.splitlines():
            parts = line.rsplit(" ", 1)
            if len(parts) != 2 or not parts[1].isdigit():
                continue
            name, ts = parts[0], int(parts[1])
            name = name[len("origin/"):] if name.startswith("origin/") else name
            if name in ("HEAD", default_branch):
                continue
            if ts >= cutoff_unix:
                branches.append(name)
        return sorted(set(branches))
    except subprocess.CalledProcessError as exc:
        log(f"branch lookup failed: {exc}")
        return []


def gather_version(workdir, cutoff):
    """Newest tag created at/after `cutoff`, else None."""
    try:
        run(["git", "-C", workdir, "fetch", "--tags", "--quiet"])
    except subprocess.CalledProcessError as exc:
        log(f"tag fetch failed: {exc}")
    try:
        out = run(["git", "-C", workdir, "for-each-ref", "--sort=-creatordate",
                   "--format=%(refname:short) %(creatordate:unix)",
                   "refs/tags"]).stdout
        cutoff_unix = int(cutoff.timestamp())
        for line in out.splitlines():
            parts = line.rsplit(" ", 1)
            if len(parts) != 2 or not parts[1].isdigit():
                continue
            if int(parts[1]) >= cutoff_unix:
                return parts[0]
        return None
    except subprocess.CalledProcessError as exc:
        log(f"tag lookup failed: {exc}")
        return None


def default_branch_name(workdir):
    """Best-effort default branch (e.g. 'main'); '' if it can't be resolved."""
    try:
        ref = run(["git", "-C", workdir, "symbolic-ref", "--short",
                   "refs/remotes/origin/HEAD"]).stdout.strip()
        return ref[len("origin/"):] if ref.startswith("origin/") else ref
    except subprocess.CalledProcessError:
        return ""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--classify-prompt", required=True)
    parser.add_argument("--summary-prompt", required=True)
    parser.add_argument("--out", required=True, help="artifact JSON path")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--token", default=os.environ.get("GH_TOKEN", ""),
                        help="token for cloning (defaults to GH_TOKEN)")
    args = parser.parse_args()

    scratch = tempfile.mkdtemp(prefix="catchup-")
    workdir = os.path.join(scratch, "repo")

    clone(args.repo, args.token, workdir, args.hours)
    commits = collect_commits(workdir, args.hours)
    log(f"{args.repo}: {len(commits)} non-bot commit(s) in window")

    if not commits:
        with open(args.out, "w") as fh:
            json.dump({"repo": args.repo, "developers": []}, fh, indent=2)
        return

    devs = group_by_author(commits)
    devs = resolve_logins(args.repo, devs)
    devs = merge_by_login(devs)

    # Deterministic delivery status per commit (Published/Testing/WIP) from
    # branch + open-PR state — decided here, never by Codex.
    default_branch = default_branch_name(workdir)
    status_by_sha = classify_status(workdir, args.repo, args.hours,
                                     default_branch, [c["sha"] for c in commits])

    # Pass 1 — classify commit messages.
    classify_input = json.dumps([
        {"sha": c["sha"], "name": c["name"], "email": c["email"],
         "subject": c["subject"], "body": c["body"]} for c in commits
    ], indent=2)
    missing = set()
    try:
        result = run_codex(args.classify_prompt, classify_input, scratch,
                           "classify.json")
        missing = set(result.get("missing_info_shas", []))
        log(f"{args.repo}: {len(missing)} commit(s) flagged missing-info")
    except (subprocess.CalledProcessError, json.JSONDecodeError,
            FileNotFoundError) as exc:
        log(f"classify pass failed, treating all commits as missing-info: {exc}")
        missing = {c["sha"] for c in commits}

    # Pass 2 — per-developer bullets, grouped by the status above.
    payload = build_summary_payload(args.repo, devs, missing, status_by_sha,
                                    workdir)
    developers = []
    try:
        result = run_codex(args.summary_prompt, json.dumps(payload, indent=2),
                           scratch, "repo-summary.json")
        bullets_by_key = {}
        for d in result.get("developers", []):
            key = d.get("login") or d.get("name")
            b = d.get("bullets")
            bullets_by_key[key] = b if isinstance(b, dict) else None
        # Rebuild from authoritative dev list; trust Codex only for bullets.
        for dev in devs:
            key = dev.get("login") or dev["name"]
            developers.append({
                "login": dev.get("login"),
                "name": dev["name"],
                "commit_count": len(dev["commits"]),
                "bullets": bullets_by_key.get(key)
                or fallback_bullets(dev, status_by_sha),
            })
    except (subprocess.CalledProcessError, json.JSONDecodeError,
            FileNotFoundError) as exc:
        log(f"summary pass failed, using fallback bullets: {exc}")
        for dev in devs:
            developers.append({
                "login": dev.get("login"),
                "name": dev["name"],
                "commit_count": len(dev["commits"]),
                "bullets": fallback_bullets(dev, status_by_sha),
            })

    developers.sort(key=lambda d: d["commit_count"], reverse=True)

    # Enrichment: merged PRs, active branches, version tag.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    result = {
        "repo": args.repo,
        "developers": developers,
        "prs": gather_prs(args.repo, cutoff),
        "branches": gather_branches(workdir, cutoff, default_branch),
        "version": gather_version(workdir, cutoff),
    }
    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=2, ensure_ascii=False)
    log(f"{args.repo}: wrote {args.out} ({len(developers)} developer(s), "
        f"{len(result['prs'])} PR(s), {len(result['branches'])} branch(es), "
        f"version={result['version']})")


if __name__ == "__main__":
    main()
