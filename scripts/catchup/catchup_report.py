"""Build the Lascade Daily Report's report.json from the merged daily file.

Runs in the `email` job. Steps:

  1. Read the merged daily file and drop any repo named in the exclude list
     (data/catchup_exclude.txt). Everything else is emailed — a repo is included
     by default. If nothing is left, emit send=false and exit.
  2. People pass: resolve git identities through data/catchup_people.json. One
     person who commits under several names becomes one developer, and bots
     (Claude, Deploy) keep their work in the email but lose their author tag.
     This runs BEFORE the Codex payload is built, because merging duplicate
     entries renumbers the work ids Codex cites.
  3. Codex pass: hand the active repos to Codex with CATCHUP_REPORT.md, which
     returns PROSE ONLY — a headline, a display name per repo, plain-English
     bullets per status group that each cite the source bullet ids they cover,
     and decisions_needed (report-codex.json). Any source bullet no Codex bullet
     cites is shown verbatim, so nothing is silently dropped. Each repo's emoji
     comes from the icons file (data/catchup_icons.json), never from Codex.
  4. Build the report: the per-repo Published/Testing/Work-in-Progress sections
     come straight from the deterministic status split upstream; all numbers
     (commit counts, contributor list, PR count, version, branches, org stats)
     are computed here, and bots are left out of the counts. Codex is trusted only for the prose in step 2.

If the Codex pass fails, the prose is dropped (generic headline, repo-name display
names, assessed=false) but the sections and numbers are intact, so the email still sends.

Usage:
    python scripts/catchup/catchup_report.py \
        --daily daily.json \
        --report-prompt CATCHUP_REPORT.md \
        --exclude catchup_exclude.txt \
        --icons catchup_icons.json \
        --people catchup_people.json \
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
# Soft length target for a bullet. Longer bullets are logged, never cut: the
# prompt lets a bullet run long when that is the only way to keep a fact.
SOFT_MAX_WORDS = 20
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


def color_key(dev):
    """Who a colour belongs to: the lowercased full git name, else "team".

    Deliberately not dev_key(): the login is looked up best-effort, so a failed
    lookup would silently recolour someone. The name is always in the git data.
    """
    return (dev.get("name") or "").strip().lower() or "team"


def load_icons(path):
    """{owner/repo: emoji} from the icons file. A missing or badly edited file
    (or entry) is logged and ignored, so an icon typo can never stop the send."""
    if not path:
        return {}
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        log(f"Icons file {path} unusable ({exc}); using the default icon.")
        return {}
    if not isinstance(data, dict):
        log(f"Icons file {path} is not an object; using the default icon.")
        return {}
    return {k: v.strip() for k, v in data.items() if isinstance(v, str) and v.strip()}


def _strings(value):
    """The non-blank strings of a list; anything that is not a list gives []."""
    return [v.strip() for v in value if isinstance(v, str) and v.strip()] if isinstance(value, list) else []


def load_people(path):
    """(alias -> canonical name, bot aliases), keys lowercase, from the people file.

    An alias is a git name or a login. Each person's canonical name counts as an
    alias of itself. An alias listed under two people is logged and ignored. A
    missing or badly edited file (or entry) is logged and ignored, so a typo
    can never stop the send.
    """
    if not path:
        return {}, set()
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        log(f"People file {path} unusable ({exc}); names are left as they are.")
        return {}, set()
    if not isinstance(data, dict):
        log(f"People file {path} is not an object; names are left as they are.")
        return {}, set()
    owners = {}
    people = data.get("people")
    for entry in people if isinstance(people, list) else []:
        name = entry.get("name") if isinstance(entry, dict) else None
        if not isinstance(name, str) or not name.strip():
            log("People file: skipped an entry without a name.")
            continue
        name = name.strip()
        for alias in [name] + _strings(entry.get("aliases")):
            owners.setdefault(alias.lower(), set()).add(name)
    aliases = {}
    for alias, who in owners.items():
        if len(who) > 1:
            log(f"People file: alias {alias!r} is listed under {sorted(who)}; ignored.")
        else:
            aliases[alias] = next(iter(who))
    return aliases, {b.lower() for b in _strings(data.get("bots"))}


def _bullet_map(dev):
    """A developer's bullets as {status: items}, whatever shape the daily file used."""
    bullets = dev.get("bullets") or {}
    return bullets if isinstance(bullets, dict) else {STATUS_ORDER[-1]: bullets}


def _resolve(dev, aliases, bots):
    """("bot", None), ("person", canonical name) or (None, None): git name first, then login."""
    keys = [str(v).strip().lower() for v in (dev.get("name"), dev.get("login")) if v]
    if any(k in bots for k in keys):
        return "bot", None
    for k in keys:
        if k in aliases:
            return "person", aliases[k]
    return None, None


def apply_people(active, people):
    """Repos with every git identity resolved through the people map (new dicts).

    A mapped person gets their canonical name and the login "person:<name>", and
    duplicate entries in one repo merge into the first one's position: commits
    summed, bullets joined per status in order. A bot is flagged `bot` (its work
    stays, its tag and counts go). Anyone else is unchanged.

    Call it ONCE, before build_codex_payload(): merging renumbers the work ids.
    """
    aliases, bots = people
    out = []
    for r in active:
        devs, merged = [], {}
        for d in r.get("developers") or []:
            kind, canonical = _resolve(d, aliases, bots)
            if kind == "bot":
                devs.append({**d, "bot": True})
            elif kind == "person":
                login = f"person:{canonical}"
                if login not in merged:
                    merged[login] = {**d, "name": canonical, "login": login, "commit_count": 0, "bullets": {}}
                    devs.append(merged[login])
                one = merged[login]
                one["commit_count"] += d.get("commit_count") or 0
                for status, items in _bullet_map(d).items():
                    one["bullets"].setdefault(status, []).extend(_as_list(items))
            else:
                devs.append(dict(d))
        out.append({**r, "developers": devs})
    return out


def _is_mapped(dev):
    return str(dev.get("login") or "").startswith("person:")


def _short(dev):
    """A mapped person's canonical name as-is; anyone else's first name."""
    full = (dev.get("name") or "").strip()
    return full if _is_mapped(dev) else (full.split(" ")[0] if full else "")


def display_names(repo):
    """{dev_key: shown name} for one repo: the short name, or the full name when
    two different people share one (so two Alexes, or a canonical Rohit and an
    unmapped Rohit Sharma, never merge). Bots never make a clash."""
    devs = repo.get("developers", [])
    first = {}
    for d in devs:
        if not d.get("bot"):
            first.setdefault(_short(d), set()).add(dev_key(d))
    names = {}
    for d in devs:
        full = (d.get("name") or "").strip()
        short = _short(d)
        names[dev_key(d)] = (full if len(first.get(short, ())) > 1 else short) or "Team"
    return names


def contributors_of(repo):
    """[{name, commits, key}] for one repo, highest commit count first. Bots are left out."""
    names = display_names(repo)
    devs = sorted((d for d in repo.get("developers", []) if not d.get("bot")),
                  key=lambda d: d.get("commit_count", 0), reverse=True)
    return [{"name": names[dev_key(d)], "commits": d.get("commit_count", 0),
             "key": color_key(d)}
            for d in devs]


# Report group key, email label, and the id letter for each upstream status.
GROUPS = [("done", "Done", "Published", "P"),
          ("testing", "Testing", "Testing", "T"),
          ("in_progress", "In progress", "Work in Progress", "W")]


def _as_list(value):
    if isinstance(value, str):
        return [value]
    return value if isinstance(value, list) else []


def normalize_work(repo, ri):
    """Source bullets per group, each with a stable id, normalised ONCE.

    Returns {group_key: [{id, text, author, dev, who}]}: `author` is the shown
    name, `dev` the identity used to dedupe people, `who` the {name, key} the
    renderer colours (None for a bot, whose work is shown with no tag). Tolerates the legacy flat-list
    shape, string-valued or null groups and blank bullets. An unrecognised
    status is filed under In progress (and logged) rather than dropped.
    """
    known = {status: key for key, _, status, _ in GROUPS}
    letter = {key: ch for key, _, _, ch in GROUPS}
    work = {key: [] for key, *_ in GROUPS}
    names = display_names(repo)
    for dev in repo.get("developers", []):
        author = names[dev_key(dev)]
        who = None if dev.get("bot") else {"name": author, "key": color_key(dev)}
        bullets = dev.get("bullets") or {}
        if not isinstance(bullets, dict):     # legacy flat-list safety
            bullets = {STATUS_ORDER[-1]: bullets}
        for status, items in bullets.items():
            key = known.get(status)
            if key is None:
                log(f"Unknown status {status!r} in {repo.get('repo')}; filed as In progress.")
                key = "in_progress"
            for item in _as_list(items):
                text = str(item if item is not None else "").lstrip("\u2022").strip()
                if text:
                    work[key].append({"id": f"R{ri}.{letter[key]}{len(work[key]) + 1}",
                                      "text": text, "author": author,
                                      "dev": dev_key(dev), "who": who})
    return work


def build_codex_payload(date, active):
    """What Codex needs for prose: every source bullet, with its id."""
    repos = []
    for ri, r in enumerate(active):
        work = normalize_work(r, ri)
        repos.append({
            "repo": r["repo"],
            "work": {label: [{k: w[k] for k in ("id", "text", "author")} for w in work[key]]
                     for key, label, _, _ in GROUPS if work[key]},
            "prs": r.get("prs", []),
            "version": r.get("version"),
        })
    return {"date": date, "repos": repos}


def _text(value):
    return value.strip() if isinstance(value, str) else ""


def _codex_repos(codex):
    """{repo: entry}; non-dict entries, non-string ids and duplicates rejected."""
    entries = codex.get("repos")
    seen, dupes, out = set(), set(), {}
    for c in (entries if isinstance(entries, list) else []):
        rid = c.get("repo") if isinstance(c, dict) else None
        if not isinstance(rid, str):
            continue
        if rid in seen:
            dupes.add(rid)
        seen.add(rid)
        out[rid] = c
    for rid in dupes:
        log(f"Codex returned {rid} more than once; ignoring its prose.")
        del out[rid]
    return out


def warn_if_long(text, where):
    """Log (never alter) a bullet over SOFT_MAX_WORDS words. Returns True if logged.

    The log names the repo and group and the word count, never the bullet: this
    repo's workflow logs are public."""
    words = len(text.split())
    if words > SOFT_MAX_WORDS:
        log(f"Long bullet ({words} words) in {where}.")
        return True
    return False


def build_groups(work, entry, where=""):
    """Codex bullets ({text, authors:[{name, key}]}) that cite real source ids, plus every uncovered source bullet.

    A Codex bullet is kept only if at least one of the source ids it cites
    exists in that group (so invented work is dropped); source bullets that no
    kept bullet cites are returned verbatim under `also`, so nothing is silently
    lost. A group with no source work is never rendered.
    """
    groups = []
    for key, label, _, _ in GROUPS:
        source = work[key]
        if not source:
            continue
        valid = {w["id"] for w in source}
        covered, bullets = set(), []
        for b in _as_list(entry.get(key)):
            if not isinstance(b, dict):
                continue
            text = _text(b.get("text"))
            ids = {i for i in _as_list(b.get("from")) if isinstance(i, str)} & valid
            if text and ids:
                warn_if_long(text, f"{where} {label}".strip())
                # Authors come from the cited source items, never from Codex. Bots have no tag.
                cited = [w for w in source if w["id"] in ids]
                authors = list({w["dev"]: w["who"] for w in cited if w["who"]}.values())
                bullets.append({"text": text, "authors": authors})
                covered |= ids
        also = [{"text": w["text"], "author": w["who"]}
                for w in source if w["id"] not in covered]
        if also:
            log(f"{len(also)} of {len(source)} {label} bullet(s) uncovered; shown verbatim.")
        groups.append({"key": key, "label": label, "bullets": bullets, "also": also})
    return groups


def merge(active, codex, assessed=True, icons=None):
    """Combine Codex prose with authoritative numbers and deterministic work.

    `icons` ({repo: emoji}) supplies each repo's emoji; Codex's is ignored.

    Codex output is validated by type: wrong-shaped JSON degrades to defaults
    rather than aborting, and `assessed` is only true when it returned a real
    repos list and decisions list.
    """
    if not isinstance(codex, dict):
        codex, assessed = {}, False
    if not isinstance(codex.get("repos"), list) or not isinstance(
            codex.get("decisions_needed"), list):
        assessed = False
    by_repo = _codex_repos(codex)

    repos_out = []
    for ri, r in enumerate(active):
        c = by_repo.get(r["repo"], {})
        commit_count = sum(d.get("commit_count", 0)
                           for d in r.get("developers", []) if not d.get("bot"))
        repos_out.append({
            "repo": r["repo"],
            "display_name": _text(c.get("display_name")) or r["repo"].split("/")[-1],
            "emoji": (icons or {}).get(r["repo"]) or DEFAULT_EMOJI,
            "commit_count": commit_count,
            "prs_merged": len(r.get("prs", [])),
            "version": r.get("version"),
            "contributors": contributors_of(r),
            "branches": r.get("branches", []),
            "groups": build_groups(normalize_work(r, ri), c, r["repo"]),
        })

    org_contributors = set()
    for r in active:
        for d in r.get("developers", []):
            if not d.get("bot"):
                org_contributors.add(dev_key(d))

    raw_decisions = _as_list(codex.get("decisions_needed"))
    decisions_needed = [t for t in (_text(d) for d in raw_decisions) if t]
    if len(decisions_needed) != len(raw_decisions):
        assessed = False    # a blank or non-string decision is bad output, not a quiet day

    n = len(repos_out)
    headline = _text(codex.get("headline"))
    if not assessed or not headline:
        headline = f"Activity in {n} product{'s' if n != 1 else ''} today"

    return {
        "date": None,  # filled by caller
        "assessed": assessed,
        "headline": headline,
        "decisions_needed": decisions_needed,
        "stats": {
            "commits": sum(rp["commit_count"] for rp in repos_out),
            "repos_active": n,
            "contributors": len(org_contributors),
        },
        "repos": repos_out,
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
    parser.add_argument("--icons", help="repo -> emoji map (data/catchup_icons.json)")
    parser.add_argument("--people", help="git name/login -> person and bot map "
                                         "(data/catchup_people.json)")
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

    # Resolve people BEFORE the payload: merging duplicates renumbers work ids, and
    # Codex must cite the ids that merge() will see.
    active = apply_people(active, load_people(args.people))

    scratch = tempfile.mkdtemp(prefix="report-")
    payload = build_codex_payload(date, active)
    assessed = True
    try:
        codex = run_codex(args.report_prompt, json.dumps(payload, indent=2),
                          scratch, "report-codex.json")
    except (subprocess.CalledProcessError, json.JSONDecodeError,
            FileNotFoundError) as exc:
        log(f"Codex prose pass failed; sections/numbers stand, prose dropped: {exc}")
        codex, assessed = {}, False

    report = merge(active, codex, assessed, load_icons(args.icons))
    report["date"] = date

    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log(f"Wrote {args.out} ({len(report['repos'])} repo(s)).")
    emit_output("send", "true")


if __name__ == "__main__":
    main()
