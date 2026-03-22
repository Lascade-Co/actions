"""Process Crashlytics crash reports: create/update GitHub issues via git blame.

Reads the JSON output of crashlytics_report.py, searches for existing GitHub
issues (deduplication via HTML marker), assigns new issues to the developer
identified by git blame, and sends a Telegram summary.

Usage:
    python scripts/crashlytics_blame.py \
        --crashes crashes.json \
        --repo Lascade-Co/travel-animator-android \
        --telegram-chat-id "-100123456"

Environment variables:
    GH_TOKEN        — GitHub token for gh CLI
    TELEGRAM_TOKEN  — Telegram bot token for sending the report
"""

import argparse
from datetime import date
import glob
import html
import json
import os
import re
import subprocess
import sys
import urllib.request
import urllib.error

# Frames from these packages are skipped during git blame (library code).
SKIP_PREFIXES = (
    "java.", "javax.", "android.", "androidx.", "kotlin.", "kotlinx.",
    "okhttp3.", "okio.", "retrofit2.", "com.google.", "com.android.",
    "com.squareup.", "io.reactivex.", "org.reactivestreams.",
    "sun.", "dalvik.", "libcore.",
)

ISSUE_BODY_TEMPLATE = """\
<!-- crashlytics:{issue_id} -->
## {issue_title}

**Type:** {error_type} | **Sessions:** {affected_sessions} | **Version:** v{version} (build {build})

### Error
{issue_subtitle}

### Stack Trace
{frames_table}

[View in Crashlytics]({crashlytics_url})
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd, **kwargs):
    """Run a command and return stdout, or None on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def gh_json(cmd):
    """Run a gh command that returns JSON and parse it."""
    raw = run(cmd)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def frames_to_table(frames):
    """Format frames as a Markdown table."""
    lines = ["| # | File | Line |", "|---|------|------|"]
    for i, f in enumerate(frames, 1):
        lines.append(f"| {i} | {f.get('file', '?')} | {f.get('line', '?')} |")
    return "\n".join(lines)


def is_library_frame(frame):
    """Check if a frame belongs to a library (should be skipped for blame)."""
    file_name = frame.get("file", "")
    if not file_name or file_name == "?":
        return True
    if file_name.startswith(SKIP_PREFIXES):
        return True
    return False


def find_file_in_repo(file_name):
    """Find a source file in the current repo checkout."""
    matches = glob.glob(f"**/{file_name}", recursive=True)
    # Prefer src/ paths over build/ or generated paths
    src_matches = [m for m in matches if "/build/" not in m and "/.gradle/" not in m]
    return src_matches[0] if src_matches else (matches[0] if matches else None)


# ---------------------------------------------------------------------------
# Git blame → GitHub username
# ---------------------------------------------------------------------------

def git_blame_line(file_path, line):
    """Run git blame on a specific line and return the commit SHA."""
    line = int(line)
    if line <= 0:
        return None
    raw = run(["git", "blame", "-L", f"{line},{line}", "--porcelain", file_path])
    if not raw:
        return None
    # First line of porcelain output is the commit SHA
    first_line = raw.split("\n")[0]
    sha = first_line.split()[0] if first_line else None
    # Skip boundary commits (all zeros)
    if sha and re.match(r"^0+$", sha):
        return None
    return sha


def sha_to_github_user(sha, repo):
    """Resolve a commit SHA to a GitHub username via the API."""
    # gh --jq outputs plain text (not JSON), so use run() directly
    login = run(["gh", "api", f"/repos/{repo}/commits/{sha}", "--jq", ".author.login"])
    return login if login else None


def blame_frames(frames, repo):
    """Walk stack frames and return the GitHub username of the most likely author."""
    for frame in frames:
        if is_library_frame(frame):
            continue
        file_name = frame.get("file", "")
        line = frame.get("line", "0")

        path = find_file_in_repo(file_name)
        if not path:
            continue

        sha = git_blame_line(path, line)
        if not sha:
            continue

        user = sha_to_github_user(sha, repo)
        if user:
            return user

    return None


# ---------------------------------------------------------------------------
# GitHub issue operations
# ---------------------------------------------------------------------------

def find_existing_issue(issue_id, repo):
    """Search for a GitHub issue containing the crashlytics marker."""
    marker = f"<!-- crashlytics:{issue_id} -->"
    data = gh_json([
        "gh", "issue", "list",
        "--repo", repo,
        "--search", f"crashlytics:{issue_id} in:body",
        "--state", "all",
        "--json", "number,state,createdAt,assignees,url,body",
        "--limit", "5",
    ])
    if data and isinstance(data, list):
        for issue in data:
            if marker in issue.get("body", ""):
                return issue
    return None


def create_issue(crash, assignee, repo):
    """Create a new GitHub issue for a crash."""
    body = ISSUE_BODY_TEMPLATE.format(
        issue_id=crash["issue_id"],
        issue_title=crash["issue_title"],
        error_type=crash.get("error_type", "FATAL"),
        affected_sessions=crash.get("affected_sessions", "?"),
        version=crash.get("version", "?"),
        build=crash.get("build", "?"),
        issue_subtitle=crash.get("issue_subtitle", ""),
        frames_table=frames_to_table(crash.get("frames", [])),
        crashlytics_url=crash.get("crashlytics_url", ""),
    )
    title = f"[Crash] {crash['issue_title']}"
    # Truncate title to 256 chars (GitHub limit)
    if len(title) > 256:
        title = title[:253] + "..."

    cmd = [
        "gh", "issue", "create",
        "--repo", repo,
        "--title", title,
        "--body", body,
        "--label", "crashlytics",
    ]
    if assignee:
        cmd.extend(["--assignee", assignee])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Failed to create issue: {result.stderr.strip()}", file=sys.stderr)
        return None
    # gh issue create prints the URL
    url = result.stdout.strip()
    # Extract issue number from URL
    number = url.rstrip("/").split("/")[-1] if url else None
    return {"url": url, "number": number}


def reopen_issue(number, repo):
    """Reopen a closed GitHub issue."""
    run(["gh", "issue", "reopen", str(number), "--repo", repo])


def comment_issue(number, repo, message):
    """Post a comment on a GitHub issue."""
    run(["gh", "issue", "comment", str(number), "--repo", repo, "--body", message])


# ---------------------------------------------------------------------------
# Telegram report
# ---------------------------------------------------------------------------

TG_USER_MAP_URL = (
    "https://raw.githubusercontent.com/Lascade-Co/actions/main/data/tg_user.json"
)

STATUS_EMOJI = {
    "new": "\U0001F195",        # 🆕
    "regression": "\U0001F525",  # 🔥
    "active": "\U0001F504",      # 🔄
}

STATUS_LABEL = {
    "new": "New",
    "regression": "Regression",
    "active": "Active",
}


def fetch_tg_user_map():
    """Fetch the GitHub-to-Telegram username mapping."""
    try:
        req = urllib.request.Request(TG_USER_MAP_URL)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"Failed to fetch tg_user.json: {e}", file=sys.stderr)
        return {}


def send_telegram(chat_id, text):
    """Send an HTML message via Telegram Bot API."""
    token = os.environ.get("TELEGRAM_TOKEN", "")
    if not token:
        print("TELEGRAM_TOKEN not set, skipping Telegram report.", file=sys.stderr)
        return

    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode()

    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
    except urllib.error.URLError as e:
        print(f"Telegram send failed: {e}", file=sys.stderr)


def build_user_message(gh_user, tg_user_map, issues, version):
    """Build a Telegram message for a single user."""
    tg_user_id = tg_user_map.get(gh_user)
    if tg_user_id:
        greeting = f'<a href="tg://user?id={tg_user_id}">{html.escape(gh_user)}</a>'
    else:
        greeting = f"<b>{html.escape(gh_user)}</b>"

    lines = [
        f"\U0001F4CB <b>Crashlytics Report</b> \u2014 <i>v{html.escape(version)}</i>",
        f"\U0001F464 {greeting}",
        "",
    ]

    for i, r in enumerate(issues, 1):
        title = html.escape(r["title"])
        if len(title) > 60:
            title = title[:57] + "..."
        url = r.get("url", "")
        status = r.get("status", "active")
        sessions = r.get("sessions", "?")
        emoji = STATUS_EMOJI.get(status, "\u2022")
        label = STATUS_LABEL.get(status, status)

        if url:
            title_display = f'<a href="{html.escape(url)}">{title}</a>'
        else:
            title_display = title

        lines.append(f"{emoji} {title_display}")
        lines.append(f"      <i>{label}</i> \u00b7 {sessions} sessions")
        lines.append("")

    lines.append(f"\U0001F4CA <i>{len(issues)} issue(s) assigned to you</i>")
    return "\n".join(lines)


def send_telegram_reports(results, version, chat_id):
    """Group results by assignee and send one Telegram message per user."""
    tg_user_map = fetch_tg_user_map()

    # Deduplicate by URL (safety net against search false positives)
    seen_urls = set()
    unique_results = []
    for r in results:
        url = r.get("url", "")
        if url and url in seen_urls:
            continue
        seen_urls.add(url)
        unique_results.append(r)

    # Group by assignee
    by_user = {}
    for r in unique_results:
        assignee = r.get("assignee")
        if not assignee:
            continue
        by_user.setdefault(assignee, []).append(r)

    for gh_user, issues in by_user.items():
        msg = build_user_message(gh_user, tg_user_map, issues, version)
        print(f"  Sending to {gh_user} ({len(issues)} issues)")
        send_telegram(chat_id, msg)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_crash(crash, repo):
    """Process a single crash: find/create/update GitHub issue. Returns report row."""
    issue_id = crash["issue_id"]
    title = crash["issue_title"]
    version = crash.get("version", "?")
    sessions = crash.get("affected_sessions", "?")

    print(f"Processing: {title}")

    existing = find_existing_issue(issue_id, repo)

    if existing:
        number = existing["number"]
        state = existing.get("state", "OPEN")
        created = existing.get("createdAt", "")[:10]  # YYYY-MM-DD
        url = existing.get("url", "")
        assignees = existing.get("assignees", [])
        assignee = assignees[0]["login"] if assignees else None

        if not assignee:
            # Fall back to git blame when no GitHub assignee
            assignee = blame_frames(crash.get("frames", []), repo)

        if not assignee:
            print(f"  Skipping #{number} — no assignee")
            return None

        if state == "CLOSED":
            print(f"  Reopening #{number} (regression)")
            reopen_issue(number, repo)
            comment_issue(
                number, repo,
                f"Issue regressed with {sessions} affected sessions in v{version}",
            )
            status = "regression"
        else:
            print(f"  Commenting on #{number} (still active)")
            comment_issue(
                number, repo,
                f"Issue still active with {sessions} affected sessions in v{version}",
            )
            status = "active"

        return {
            "title": title,
            "assignee": assignee,
            "reported": created,
            "url": url,
            "status": status,
            "sessions": sessions,
        }

    # New issue — git blame to find assignee
    print("  Running git blame...")
    assignee = blame_frames(crash.get("frames", []), repo)
    if not assignee:
        print("  Skipping — could not determine assignee from blame")
        return None

    print(f"  Assigning to {assignee}")
    created = create_issue(crash, assignee, repo)
    if not created:
        return None

    return {
        "title": title,
        "assignee": assignee,
        "reported": date.today().isoformat(),
        "url": created["url"],
        "status": "new",
        "sessions": sessions,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Create/update GitHub issues from Crashlytics crash data"
    )
    parser.add_argument(
        "--crashes", required=True,
        help="Path to JSON file from crashlytics_report.py",
    )
    parser.add_argument(
        "--repo", required=True,
        help="GitHub repository (owner/repo)",
    )
    parser.add_argument(
        "--telegram-chat-id",
        help="Telegram chat ID for the summary report",
    )
    args = parser.parse_args()

    with open(args.crashes) as f:
        crashes = json.load(f)

    if not crashes:
        print("No crashes to process.")
        return

    version = crashes[0].get("version", "?")
    results = []

    for crash in crashes:
        row = process_crash(crash, args.repo)
        if row:
            results.append(row)

    # Telegram reports — one message per assignee
    if args.telegram_chat_id and results:
        print(f"\nSending Telegram reports to {args.telegram_chat_id}")
        send_telegram_reports(results, version, args.telegram_chat_id)

    print(f"\nDone. Processed {len(crashes)} crashes, {len(results)} issues updated/created.")


if __name__ == "__main__":
    main()
