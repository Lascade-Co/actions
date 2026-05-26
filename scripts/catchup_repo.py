"""Summarise one repo's last-24h developer activity with Codex.

Run inside a matrix job AFTER Codex auth + sandbox are set up. For a single
repo it:

  1. Shallow-clones every branch within the look-back window.
  2. Collects non-merge commits, dropping bot authors.
  3. Codex pass 1: classifies each commit message as descriptive or
     missing-info (writes classify.json).
  4. Pulls the git diff only for missing-info commits.
  5. Codex pass 2: writes 4-5 emoji bullets per developer (repo-summary.json),
     using clear messages directly and diffs for the vague ones.
  6. Resolves each author's GitHub login and writes the per-repo artifact.

The published schema (per developer): {login, name, commit_count, bullets}.
commit_count is authoritative from git, never from Codex.

Usage:
    GH_TOKEN=... python scripts/catchup_repo.py \
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

FIELD = "\x1f"   # unit separator between log fields
RECORD = "\x1e"  # record separator between commits

BOT_NAMES = {"dependabot", "dependabot[bot]", "github-actions",
             "github-actions[bot]", "web-flow"}

MAX_DIFF_CHARS = 15000      # cap a single commit diff
MAX_TOTAL_DIFF_CHARS = 120000  # cap the combined diff payload to Codex
MAX_COMMITS_PER_DEV = 50    # cap commits sent to Codex (count stays accurate)


def log(msg):
    print(msg, file=sys.stderr)


def run(cmd, **kw):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw)


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


def build_summary_payload(repo, devs, missing_shas, workdir):
    total_diff = 0
    payload_devs = []
    for dev in devs:
        commits_out = []
        for c in dev["commits"][:MAX_COMMITS_PER_DEV]:
            entry = {"sha": c["sha"], "subject": c["subject"],
                     "body": c["body"]}
            if c["sha"] in missing_shas and total_diff < MAX_TOTAL_DIFF_CHARS:
                diff = get_diff(workdir, c["sha"])
                total_diff += len(diff)
                entry["diff"] = diff
            commits_out.append(entry)
        payload_devs.append({
            "login": dev.get("login"),
            "name": dev["name"],
            "commit_count": len(dev["commits"]),
            "commits": commits_out,
        })
    return {"repo": repo, "developers": payload_devs}


def fallback_bullets(dev):
    """Degraded summary from commit subjects when Codex is unavailable."""
    seen, bullets = set(), []
    for c in dev["commits"]:
        subj = c["subject"].strip()
        if subj and subj.lower() not in seen:
            seen.add(subj.lower())
            bullets.append(f"• {subj}")
        if len(bullets) >= 5:
            break
    return bullets


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

    # Pass 2 — per-developer bullets.
    payload = build_summary_payload(args.repo, devs, missing, workdir)
    developers = []
    try:
        result = run_codex(args.summary_prompt, json.dumps(payload, indent=2),
                           scratch, "repo-summary.json")
        bullets_by_key = {}
        for d in result.get("developers", []):
            key = d.get("login") or d.get("name")
            bullets_by_key[key] = d.get("bullets", [])
        # Rebuild from authoritative dev list; trust Codex only for bullets.
        for dev in devs:
            key = dev.get("login") or dev["name"]
            developers.append({
                "login": dev.get("login"),
                "name": dev["name"],
                "commit_count": len(dev["commits"]),
                "bullets": bullets_by_key.get(key) or fallback_bullets(dev),
            })
    except (subprocess.CalledProcessError, json.JSONDecodeError,
            FileNotFoundError) as exc:
        log(f"summary pass failed, using fallback bullets: {exc}")
        for dev in devs:
            developers.append({
                "login": dev.get("login"),
                "name": dev["name"],
                "commit_count": len(dev["commits"]),
                "bullets": fallback_bullets(dev),
            })

    developers.sort(key=lambda d: d["commit_count"], reverse=True)
    with open(args.out, "w") as fh:
        json.dump({"repo": args.repo, "developers": developers}, fh, indent=2,
                  ensure_ascii=False)
    log(f"{args.repo}: wrote {args.out} ({len(developers)} developer(s))")


if __name__ == "__main__":
    main()
