"""Build the daily email's report.json from the merged daily file.

Runs in the `email` job. Steps:

  1. Read the merged daily file and keep only repos carrying the `catchup-mail`
     GitHub topic (the email's active set). If none, emit send=false and exit.
  2. Codex pass: hand the active repos to Codex with CATCHUP_REPORT.md, which
     returns prose + per-repo section structure (report-codex.json).
  3. Merge Codex's words with AUTHORITATIVE numbers computed here — commit
     counts, contributor list, PR count, version, branches, org stats — so the
     figures people read are never the model's invention (same principle as
     catchup_repo.py trusting Codex only for bullet prose).

If the Codex pass fails the report degrades to bullet-derived sections so the
email still sends.

Usage:
    python scripts/catchup_report.py \
        --daily daily.json \
        --report-prompt CATCHUP_REPORT.md \
        --out report.json

Requires: the `codex` CLI on PATH (Codex auth pre-restored).
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

TOPIC = "catchup-mail"          # repos carrying this GitHub topic are emailed
DEFAULT_EMOJI = "📦"


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


def dev_key(dev):
    return dev.get("login") or dev.get("name")


def contributors_of(repo):
    """[{name, commits}] for one repo, highest commit count first."""
    devs = sorted(repo.get("developers", []),
                  key=lambda d: d.get("commit_count", 0), reverse=True)
    return [{"name": d.get("name", ""), "commits": d.get("commit_count", 0)}
            for d in devs]


def fallback_sections(repo):
    """Degraded sections from developer bullets when Codex is unavailable."""
    items = []
    for dev in repo.get("developers", []):
        author = (dev.get("name") or "").split(" ")[0] or "Team"
        for bullet in dev.get("bullets", []):
            text = bullet.lstrip("•").strip()   # strip the existing list marker
            items.append({"text": text, "author": author})
    return [{"title": "Updates", "items": items}] if items else []


def build_codex_payload(date, active):
    """Trim the active repos to just what Codex needs (prose inputs)."""
    repos = []
    for r in active:
        repos.append({
            "repo": r["repo"],
            "developers": [
                {"login": d.get("login"), "name": d.get("name"),
                 "commit_count": d.get("commit_count", 0),
                 "bullets": d.get("bullets", [])}
                for d in r.get("developers", [])
            ],
            "prs": r.get("prs", []),
            "branches": r.get("branches", []),
            "version": r.get("version"),
        })
    return {"date": date, "repos": repos}


def merge(active, codex):
    """Combine Codex prose with authoritative per-repo and org numbers."""
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
            "sections": c.get("sections") or fallback_sections(r),
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
    parser.add_argument("--out", required=True, help="report.json output path")
    args = parser.parse_args()

    with open(args.daily) as fh:
        daily = json.load(fh)
    date = daily.get("date")

    active = [r for r in daily.get("repos", [])
              if TOPIC in (r.get("tags") or [])]
    log(f"{len(active)} repo(s) tagged '{TOPIC}' out of "
        f"{len(daily.get('repos', []))} active.")
    if not active:
        log("No tagged repos with activity; skipping email.")
        emit_output("send", "false")
        return

    scratch = tempfile.mkdtemp(prefix="report-")
    payload = build_codex_payload(date, active)
    try:
        codex = run_codex(args.report_prompt, json.dumps(payload, indent=2),
                          scratch, "report-codex.json")
    except (subprocess.CalledProcessError, json.JSONDecodeError,
            FileNotFoundError) as exc:
        log(f"Codex report pass failed, using fallback sections: {exc}")
        codex = {"executive_summary": "", "repos": [], "patterns": []}

    report = merge(active, codex)
    report["date"] = date

    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log(f"Wrote {args.out} ({len(report['repos'])} repo(s)).")
    emit_output("send", "true")


if __name__ == "__main__":
    main()
