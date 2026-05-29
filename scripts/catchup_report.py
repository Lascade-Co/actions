"""Build the daily email's report.json from the merged daily file.

Runs in the `email` job. Steps:

  1. Read the merged daily file and drop any repo named in the exclude list
     (data/catchup_exclude.txt). Everything else is emailed — a repo is included
     by default. If nothing is left, emit send=false and exit.
  2. Codex pass: hand the active repos to Codex with CATCHUP_REPORT.md, which
     returns PROSE ONLY — an executive summary, a display name + emoji per repo,
     and cross-repo patterns (report-codex.json).
  3. Build the report: the per-repo Published/Testing/Work-in-Progress sections
     come straight from the deterministic status split upstream; all numbers
     (commit counts, contributor list, PR count, version, branches, org stats)
     are computed here. Codex is trusted only for the prose in step 2.

If the Codex pass fails, the prose is dropped (empty summary/patterns, repo-name
display names) but the sections and numbers are intact, so the email still sends.

Usage:
    python scripts/catchup_report.py \
        --daily daily.json \
        --report-prompt CATCHUP_REPORT.md \
        --exclude catchup_exclude.txt \
        --out report.json

Requires: the `codex` CLI on PATH (Codex auth pre-restored).
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

DEFAULT_EMOJI = "📦"
# Section order in the email; matches the deterministic statuses set upstream
# in catchup_repo.py (Published / Testing / Work in Progress).
STATUS_ORDER = ["Published", "Testing", "Work in Progress"]


def log(msg):
    print(msg, file=sys.stderr)


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


def load_exclude(path):
    """owner/repo names to omit from the email (blank lines and # ignored)."""
    if not path:
        return set()
    with open(path) as fh:
        return {line.strip() for line in fh
                if line.strip() and not line.lstrip().startswith("#")}


def dev_key(dev):
    return dev.get("login") or dev.get("name")


def contributors_of(repo):
    """[{name, commits}] for one repo, highest commit count first."""
    devs = sorted(repo.get("developers", []),
                  key=lambda d: d.get("commit_count", 0), reverse=True)
    return [{"name": d.get("name", ""), "commits": d.get("commit_count", 0)}
            for d in devs]


def sections_from_repo(repo):
    """Deterministic Published/Testing/WIP sections from developer bullets.

    The status split was decided upstream (catchup_repo.py); here we just fan
    the per-developer, per-status bullets out into ordered email sections.
    """
    buckets = {st: [] for st in STATUS_ORDER}
    for dev in repo.get("developers", []):
        author = (dev.get("name") or "").split(" ")[0] or "Team"
        bullets = dev.get("bullets") or {}
        if not isinstance(bullets, dict):     # legacy flat-list safety
            bullets = {STATUS_ORDER[-1]: bullets}
        for status, items in bullets.items():
            for bullet in items:
                buckets.setdefault(status, []).append(
                    {"text": str(bullet).lstrip("•").strip(), "author": author})
    return [{"title": st, "items": buckets[st]}
            for st in STATUS_ORDER if buckets.get(st)]


def build_codex_payload(date, active):
    """Trim the active repos to just what Codex needs for prose."""
    repos = []
    for r in active:
        repos.append({
            "repo": r["repo"],
            "developers": [
                {"name": d.get("name"), "bullets": d.get("bullets", {})}
                for d in r.get("developers", [])
            ],
            "prs": r.get("prs", []),
            "branches": r.get("branches", []),
            "version": r.get("version"),
        })
    return {"date": date, "repos": repos}


def merge(active, codex):
    """Combine Codex prose with authoritative numbers and deterministic sections."""
    by_repo = {c.get("repo"): c for c in codex.get("repos", [])}

    repos_out = []
    for r in active:
        c = by_repo.get(r["repo"], {})
        commit_count = sum(d.get("commit_count", 0)
                           for d in r.get("developers", []))
        repos_out.append({
            "repo": r["repo"],
            "display_name": c.get("display_name") or r["repo"].split("/")[-1],
            "emoji": c.get("emoji") or DEFAULT_EMOJI,
            "commit_count": commit_count,
            "prs_merged": len(r.get("prs", [])),
            "version": r.get("version"),
            "contributors": contributors_of(r),
            "branches": r.get("branches", []),
            "sections": sections_from_repo(r),
        })

    org_contributors = set()
    for r in active:
        for d in r.get("developers", []):
            org_contributors.add(dev_key(d))

    return {
        "date": None,  # filled by caller
        "executive_summary": codex.get("executive_summary", ""),
        "stats": {
            "commits": sum(rp["commit_count"] for rp in repos_out),
            "repos_active": len(repos_out),
            "contributors": len(org_contributors),
        },
        "repos": repos_out,
        "patterns": codex.get("patterns", []),
    }


def emit_output(key, value):
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as fh:
            fh.write(f"{key}={value}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", required=True, help="merged daily file")
    parser.add_argument("--report-prompt", required=True,
                        help="CATCHUP_REPORT.md path")
    parser.add_argument("--exclude", help="repo exclude-list file (owner/repo per line)")
    parser.add_argument("--out", required=True, help="report.json output path")
    args = parser.parse_args()

    with open(args.daily) as fh:
        daily = json.load(fh)
    date = daily.get("date")

    excluded = load_exclude(args.exclude)
    repos = daily.get("repos", [])
    active = [r for r in repos if r.get("repo") not in excluded]
    log(f"{len(active)} repo(s) to email "
        f"({len(repos)} active, {len(repos) - len(active)} excluded).")
    if not active:
        log("No repos left after exclusions; skipping email.")
        emit_output("send", "false")
        return

    scratch = tempfile.mkdtemp(prefix="report-")
    payload = build_codex_payload(date, active)
    try:
        codex = run_codex(args.report_prompt, json.dumps(payload, indent=2),
                          scratch, "report-codex.json")
    except (subprocess.CalledProcessError, json.JSONDecodeError,
            FileNotFoundError) as exc:
        log(f"Codex prose pass failed; sections/numbers stand, prose dropped: {exc}")
        codex = {"executive_summary": "", "repos": [], "patterns": []}

    report = merge(active, codex)
    report["date"] = date

    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log(f"Wrote {args.out} ({len(report['repos'])} repo(s)).")
    emit_output("send", "true")


if __name__ == "__main__":
    main()
