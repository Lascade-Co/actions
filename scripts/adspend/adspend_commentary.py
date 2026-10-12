"""Prose written by Claude for the ad-spend email, none of which can state a number.

Two tasks share one table of the day's logged campaign changes:

* ``commentary``: the lead. One or two items, each an observed spend sentence and, when a
  logged change sits under that same line, a cause sentence naming the person.
* ``narrate``: one plain-English sentence per logged change, shown under its ledger line.

Claude is shown computed facts with ids. Numbers only appear as ``{id.field}``
placeholders, filled here from the model. Any literal digit, unknown placeholder,
contradicted direction, advice, unbacked name, campaign code or cause on the wrong line
rejects the answer with a one-word reason; the reason is fed back and Claude tries again,
three calls in all. Prose is decoration: every failure path returns nothing and the email
still sends.

Public repo: prompts, generated prose and API error bodies are never logged.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from decimal import Decimal, InvalidOperation

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ai"))
from claude_api import ClaudeError, generate_text

from pnl_money import format_usd
from adspend_model import CHANNELS, UNASSIGNED, app_of, os_of, round_pct

MAX_FACTS = 8
ATTEMPTS = 3                     # Claude calls per task, first try included
NARRATE_BATCH = 40
WHY_LIMIT = 400
_PLACEHOLDER = re.compile(r"\{(t|f\d+|c\d+)\.(\w+)\}")
_UP = {"up", "rose", "increased", "higher", "climbed", "grew", "jumped", "raised"}
_DOWN = {"down", "fell", "dropped", "lower", "cut", "declined", "decreased", "shrank", "reduced"}
_ADVICE = {"should", "recommend", "recommended", "consider", "must", "team", "decided", "budget"}
ALIASES = {"Apple Search Ads": {"Apple", "ASA"}, "Meta": {"Facebook", "Instagram"}, "Google": {"Google Ads"}}
# A daily budget as the ads team writes it: "45$ per day", "$45/day", "45.50 $ a day". One "$", nothing else.
BUDGET = re.compile(r"^\s*\$?\s*(\d+(?:\.\d{1,2})?)\s*\$?\s*(?:per\s+day|/\s*day|a\s+day|daily)\s*$", re.I)
# A campaign code: upper-case chunks chained by hyphens ("TA - WW EN", "ASA - Brand"). Mixed-case codes
# can slip past this pattern, so the logged campaign name itself is also checked verbatim.
_CODE = re.compile(r"\b[A-Z0-9]{2,}(?:\s*-\s*[A-Z0-9][A-Z0-9 ]*){1,}")
_DOWN_KINDS = ("paused", "stopped", "ended")
_UP_KINDS = ("launched", "started", "new creative", "resumed")

HINTS = {
    "shape": "Answer with exactly the JSON shape asked for; every text is one sentence ending in a full stop, "
             "with no other full stop, question mark or exclamation mark inside it",
    "digit": "Replace every number with a placeholder; do not spell numbers out either",
    "placeholder": "Use only the placeholders listed for the ids you reference",
    "advice": "Remove advice and words like should, must, consider, recommend, team, decided",
    "direction": "Use up/down words only when they match the fact's label, or in a cause what the person did",
    "name": "Mention only the apps, platforms and people of the facts or change you reference",
    "link": "Give a cause only in an item whose only ref is the line that change was logged under",
    "length": "Use fewer words",
    "code": "Describe the campaign in words instead of quoting its code",
}

PROMPT = """You are the VP of Performance Marketing writing the lead of a morning email for the CEO.
You are given FACTS about yesterday's ad spend and the CHANGES the ads team logged, as JSON.
Answer with 1 or 2 items about what stands out. Each item has:
- "observed": one sentence on what spend did. Observed spend only: never why, never a person or team, never advice.
- "refs": the fact ids that sentence uses.
- "cause" (optional): {"change": "c3", "text": "..."} only when a logged change sits under that same line,
  i.e. the change's "line_fact" is the item's only ref. One sentence, past tense, starting with the person's
  name exactly as logged, saying what they changed and why, in plain words a CEO uses. Never quote a campaign
  code (like "MR - iOS - ASA - BRAND"); describe the campaign instead.
Rules for all text:
- NEVER write a digit and never spell a number out. Use placeholders: {f3.d}, {f3.pct}, {t.d} in "observed"
  (only fields listed for that fact); {c3.budget} in a cause (only when that change lists "budget").
- A fact's "label" is its truth: do not say up for a down fact or the reverse. In a cause, direction words
  (raised, cut, ...) must match what the person did.
- No advice, no hype, no intensifiers. Never invent a cause that is not logged.
Prefer the biggest movers, and give the cause when a logged change explains one.
Answer with JSON only: {"items":[{"observed":"...","refs":["f3"],"cause":{"change":"c1","text":"..."}}]}

FACTS AND CHANGES:
"""

NARRATE_PROMPT = """You are writing for a CEO who is not an advertising specialist. Below are campaign changes
the ads team logged, as JSON. For each id write ONE plain-English sentence of at most 25 words, past tense,
starting with the person's name exactly as given in "author". Say what they did to which campaign and why.
Describe the campaign in words (app, platform, market, goal); never quote the campaign code. Translate ad jargon
(ROAS, CPA, UAC, tier one, broad match, ad set, creative) into everyday words. Use only what the log says; do not
guess outcomes. Never write a digit and never spell a number out; for the daily budget write {cN.budget} (only for
ids whose "fields" list "budget"). No advice. Mention no app, platform or person other than that change's own.
Answer with JSON only: {"c1": "...", "c2": "..."}

CHANGES:
"""

RETRY = "\n\nYour previous answer was rejected for: {word}. {hint}. Previous answer:\n{raw}\nAnswer again, JSON only."


class _Reject(Exception):
    """A validation failure carrying one safe word for the public log."""

    def __init__(self, word: str):
        self.word = word
        super().__init__(word)


def _n(v, spec="usd"):
    if v is None:
        return None
    if spec == "usd":
        return format_usd(v)
    if spec == "usd2":
        return f"${v.quantize(Decimal('0.01'))}"
    if spec == "pct":
        return f"{abs(round_pct(v))}%"
    return str(int(v.quantize(Decimal("1"))))


def build_facts(model: dict, variant: str) -> dict:
    """fid -> {"who": {...names}, "label": str, "show": {field: str}, "key"?: (project, channel, os)}"""
    t = model["total"]
    facts = {"t": {
        "who": {}, "label": "up" if t["d"] > t["d1"] else "down" if t["d"] < t["d1"] else "steady",
        "show": {k: v for k, v in {"d": _n(t["d"]), "d1": _n(t["d1"]), "avg7": _n(t["avg7"]),
                                   "pct": _n(t["pct_d1"], "pct") if t["pct_d1"] is not None else None}.items() if v},
    }}
    lines = [ln for p in model["projects"] for ln in p["lines"] if ln["label"] != "steady"]
    lines.sort(key=lambda ln: -abs(ln["d"] - ln["d1"]))
    for i, ln in enumerate(lines[:MAX_FACTS], 1):
        show = {"d": _n(ln["d"]), "d1": _n(ln["d1"]), "avg7": _n(ln["avg7"])}
        if ln["pct"] is not None:
            show["pct"] = _n(ln["pct"], "pct")
        if variant == "B" and ln["installs_d"] is not None:
            show["installs"] = _n(ln["installs_d"], "int")
            if ln["cpi_d"] is not None:
                show["cpi"] = _n(ln["cpi_d"], "usd2")
        facts[f"f{i}"] = {
            "who": {"project": ln["project"], "channel": ln["channel"],
                    **({} if ln["os"] == UNASSIGNED else {"os": ln["os"]})},
            "label": ln["label"], "show": show,
            "key": (ln["project"], ln["channel"], ln["os"]),   # never sent to Claude
        }
    return facts


def _names(facts: dict) -> set:
    out = set()
    for f in facts.values():
        out.update(v for v in f["who"].values())
    return out


# ---- the change table, shared by the lead and the narration

def parse_budget(value):
    """'45$ per day' -> Decimal('45'); anything that is not exactly a daily amount -> None."""
    s = str(value or "")
    m = BUDGET.match(s)
    if not m or s.count("$") != 1:
        return None
    try:
        return Decimal(m.group(1))
    except InvalidOperation:
        return None


def _budget_words(amount: Decimal) -> str:
    whole = amount == amount.to_integral_value()
    return f"${int(amount)} a day" if whole else f"${amount.quantize(Decimal('0.01'))} a day"


def _direction(kind: str, before: str, after: str):
    k = (kind or "").lower()
    if any(w in k for w in _DOWN_KINDS):
        return "down"
    if any(w in k for w in _UP_KINDS):
        return "up"
    b, a = parse_budget(before), parse_budget(after)
    if b is not None and a is not None and a != b:
        return "up" if a > b else "down"
    return None


def change_table(changes, model: dict, apps: dict) -> list:
    """The day's logged changes, ids c1..cN in reading order: ledger order, then the orphans."""
    if not changes or not changes.get("ok"):
        return []
    fact_of = {f["key"]: fid for fid, f in build_facts(model, "A").items() if "key" in f}
    ordered = []
    for p in model["projects"]:
        for ln in p["lines"]:
            k = (ln["project"], ln["channel"], ln["os"])
            ordered += [(c, k) for c in changes["by_line"].get(k, [])]
    ordered += [(c, None) for c in changes["orphans"]]
    table = []
    for i, (c, k) in enumerate(ordered, 1):
        if k is None:
            k = (app_of(c.entity, apps), c.channel,
                 "iOS" if c.channel == "Apple Search Ads" else os_of(c.entity))
        budget = parse_budget(c.after)
        row = {"project": k[0], "channel": k[1], "os": k[2], "author": c.author, "app": c.app,
               "kind": c.kind, "campaign": c.entity, "before": c.before, "after": c.after,
               "why": c.why[:WHY_LIMIT], "dir": _direction(c.kind, c.before, c.after)}
        row["key"] = hashlib.sha256(json.dumps({**row, "at": c.at.isoformat()}, sort_keys=True)
                                    .encode()).hexdigest()[:16]
        row.update(id=f"c{i}", change=c, line_fact=fact_of.get(k),
                   show={"budget": _budget_words(budget)} if budget is not None else {})
        table.append(row)
    return table


def _payload_row(row: dict) -> dict:
    out = {k: row[k] for k in ("id", "author", "channel", "project", "os", "app", "kind", "campaign",
                               "before", "after", "why", "line_fact")}
    if row["show"]:
        out["fields"] = sorted(row["show"])
    return out


def _proper(name: str) -> bool:
    """A logged app worth guarding as a name: 'MarineRadar', 'Plant the World'; not 'Other'."""
    return len(name) > 1 and (" " in name or any(ch.isupper() for ch in name[1:]))


def name_universe(apps: dict, table: list) -> set:
    """Every name a sentence could wrongly drop in. Blank values never enter: an empty name
    would match everywhere. Registry short codes are left out on purpose: 'GEO' would make
    'geo targeting' a name."""
    names = set(apps) | set(CHANNELS) | {"iOS", "Android"}
    for aliases in ALIASES.values():
        names |= aliases
    for row in table:
        names.add(row["author"])
        if row["app"] in apps or _proper(row["app"]):
            names.add(row["app"])
        names.add(row["project"])
    return {n.strip() for n in names if n and n.strip()}


def _backed(values) -> set:
    out = set()
    for v in values:
        if v and str(v).strip():
            out.add(str(v).strip().lower())
            out |= {a.lower() for a in ALIASES.get(str(v).strip(), ())}
    return out


def backed_names(row: dict) -> set:
    return _backed((row["project"], row["app"], row["channel"], row["os"], row["author"]))


# ---- validation

def _mentions(text: str, name: str) -> bool:
    return re.search(rf"(?<![a-z0-9]){re.escape(name.lower())}(?![a-z0-9])", text.lower()) is not None


def _check_names(text: str, names, backed: set) -> None:
    for name in names:
        if name and name.strip() and _mentions(text, name) and name.lower() not in backed:
            raise _Reject("name")


def _direction_ok(words: set, allowed: set) -> None:
    if words & _UP and not allowed & {"up", "started"}:
        raise _Reject("direction")
    if words & _DOWN and not allowed & {"down", "stopped"}:
        raise _Reject("direction")


def _parse(raw: str):
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"\A```\w*\s*\n(.*?)\n```\Z", r"\1", text, flags=re.S)
    try:
        return json.loads(text)
    except ValueError:
        raise _Reject("shape") from None


def _check_observed(sentence, refs, facts, names) -> str:
    if not isinstance(sentence, str) or not sentence.strip():
        raise _Reject("shape")
    bare = _PLACEHOLDER.sub("", sentence)
    if re.search(r"\d", bare):
        raise _Reject("digit")
    for fid, field in _PLACEHOLDER.findall(sentence):
        if fid not in refs or fid not in facts or field not in facts[fid]["show"]:
            raise _Reject("placeholder")
    if "{" in bare or "}" in bare:
        raise _Reject("placeholder")
    words = set(re.findall(r"[a-z]+", bare.lower()))
    if words & _ADVICE:
        raise _Reject("advice")
    _direction_ok(words, {facts[r]["label"] for r in refs})
    _check_names(bare, names, _backed(v for r in refs for v in facts[r]["who"].values()))
    return _PLACEHOLDER.sub(lambda m: facts[m.group(1)]["show"][m.group(2)], sentence).strip()


def _check_template(text, row, names, max_words, direction: bool) -> str:
    """One sentence about one logged change, checked as a template, returned filled.
    Placeholders are validated first and replaced by a neutral 'X' for the remaining checks,
    so '{c1.budget}.' cannot look like two sentences and '$45.50' cannot look like a digit."""
    if not isinstance(text, str) or not text.strip():
        raise _Reject("shape")
    for cid, field in _PLACEHOLDER.findall(text):
        if cid != row["id"] or field not in row["show"]:
            raise _Reject("placeholder")
    probe = _PLACEHOLDER.sub("X", text).strip()
    if "{" in probe or "}" in probe:
        raise _Reject("placeholder")
    if re.search(r"\d", probe):
        raise _Reject("digit")
    if row["author"] and not probe.startswith(row["author"]):
        raise _Reject("name")
    if not probe.endswith(".") or re.search(r"[.!?]", probe[:-1]):
        raise _Reject("shape")
    if len(probe.split()) > max_words:
        raise _Reject("length")
    if (row["campaign"] and row["campaign"].lower() in probe.lower()) or _CODE.search(probe):
        raise _Reject("code")
    words = set(re.findall(r"[a-z]+", probe.lower()))
    if words & (_ADVICE - {"budget"}):
        raise _Reject("advice")
    if direction:
        _direction_ok(words, {row["dir"]} if row["dir"] else set())
    _check_names(probe, names, backed_names(row))
    return _PLACEHOLDER.sub(lambda m: row["show"][m.group(2)], text).strip()


def _check(raw: str, facts: dict, table: list, names) -> list:
    """-> the filled lead sentences, or raise _Reject(word)."""
    try:
        doc = _parse(raw)
        items = doc.get("items") if isinstance(doc, dict) else None
        if not isinstance(items, list) or not 1 <= len(items) <= 2:
            raise _Reject("shape")
        rows = {r["id"]: r for r in table}
        out = []
        for it in items:
            if not isinstance(it, dict):
                raise _Reject("shape")
            refs = it.get("refs")
            if not isinstance(refs, list) or not refs or not all(isinstance(r, str) and r in facts for r in refs):
                raise _Reject("shape")
            sentence = _check_observed(it.get("observed"), refs, facts, names)
            cause = it.get("cause")
            if cause is not None:
                if not isinstance(cause, dict):
                    raise _Reject("shape")
                row = rows.get(cause.get("change"))
                if row is None or not row["line_fact"] or refs != [row["line_fact"]]:
                    raise _Reject("link")
                sentence += " " + _check_template(cause.get("text"), row, names, 30, direction=True)
            out.append(sentence)
        return out
    except _Reject:
        raise
    except Exception:
        raise _Reject("shape") from None


def validate(raw: str, facts: dict, all_names=(), table=()) -> list:
    """Return the filled sentences, or [] if anything is off."""
    try:
        return _check(raw, facts, list(table), set(all_names))
    except _Reject:
        return []


def _check_narration(text, row, names) -> str:
    return _check_template(text, row, names, 25, direction=False)


# ---- calling Claude

def _run_claude(prompt: str, scratch: str, max_tokens: int = 1024):
    return generate_text(prompt, max_tokens=max_tokens, timeout=90 if max_tokens <= 1024 else 180)


def _failure_reason(exc: Exception, scratch: str = "") -> str:
    """One safe reason for the public log; never inspect or echo API bodies."""
    return exc.reason if isinstance(exc, ClaudeError) else "claude-error"


def _call(run, prompt, scratch, max_tokens=None):
    # Runners with the old two-argument shape stay valid wherever no budget is needed.
    return run(prompt, scratch) if max_tokens is None else run(prompt, scratch, max_tokens=max_tokens)


def _attempt(task, run, scratch, prompt, check, max_tokens=None):
    """-> (result, "") or (None, last reject word). Exceptions from `run` are not retried."""
    ask, word = prompt, ""
    for n in range(ATTEMPTS):
        raw = _call(run, ask, scratch, max_tokens)
        try:
            return check(raw), ""
        except _Reject as exc:
            word = exc.word
        if n + 1 < ATTEMPTS:
            print(f"{task}: retry ({word})")
            ask = prompt + RETRY.format(word=word, hint=HINTS.get(word, "Follow every rule"), raw=raw)
    return None, word


def commentary(model: dict, variant: str, scratch: str, run=_run_claude, cache: bool = True,
               table=(), names=None) -> list:
    """Cached per (ad_date, variant, facts, changes) so a rerun on changed numbers never reuses
    stale prose. Only good prose is cached: an empty result must not block a rerun after a fix.
    Every skip is announced with a one-word reason."""
    facts = build_facts(model, variant)
    table = list(table or [])
    payload = {fid: {**f["who"], "label": f["label"], "values": f["show"], "fields": sorted(f["show"])}
               for fid, f in facts.items()}
    sent = {"facts": payload, "changes": [_payload_row(r) for r in table]}
    fingerprint = hashlib.sha256(json.dumps(sent, sort_keys=True, default=str).encode()).hexdigest()
    cache_path = os.path.join(scratch, f"commentary-{variant}.json")
    if cache and os.path.exists(cache_path):
        try:
            with open(cache_path) as fh:
                cached = json.load(fh)
            if cached.get("facts") == fingerprint and cached["sentences"]:
                return cached["sentences"]
        except Exception:
            pass
    names = set(names or ()) | _names(build_facts(model, "B")) | set(CHANNELS) | {"iOS", "Android"}
    for aliases in ALIASES.values():
        names |= aliases
    reason = ""
    try:
        result, word = _attempt("synopsis", run, scratch, PROMPT + json.dumps(sent, indent=1),
                                lambda raw: _check(raw, facts, table, names))
        if not result:
            result, reason = [], f"rejected:{word or 'shape'}"
    except Exception as exc:  # auth, timeout, API failure: no commentary, never a failed send
        result, reason = [], _failure_reason(exc, scratch)
    if reason:
        print(f"::warning title=Ad spend synopsis skipped::{reason}")
    elif cache:
        with open(cache_path, "w") as fh:
            json.dump({"facts": fingerprint, "sentences": result}, fh)
    return result


def _narrate_batch(batch, scratch, run, names, templates) -> None:
    """Fill `templates` {key: unfilled sentence} for the rows Claude got right within ATTEMPTS calls."""
    max_tokens = min(8192, 256 + 120 * len(batch))
    base = NARRATE_PROMPT + json.dumps([_payload_row(r) for r in batch], indent=1)
    pending, ask = list(batch), base
    for n in range(ATTEMPTS):
        raw = _call(run, ask, scratch, max_tokens)
        try:
            doc = _parse(raw)
            if not isinstance(doc, dict):
                raise _Reject("shape")
        except _Reject:
            doc = {}
        failed = []
        for row in pending:
            text = doc.get(row["id"])
            try:
                _check_narration(text, row, names)
                templates[row["key"]] = text
            except _Reject as exc:
                failed.append((row, exc.word))
        if not failed:
            return
        pending = [r for r, _ in failed]
        if n + 1 < ATTEMPTS:
            print(f"narration: retry ({failed[0][1]})")
            notes = "; ".join(f"{r['id']}: {w} ({HINTS.get(w, 'follow every rule')})" for r, w in failed)
            ask = (base + f"\n\nThese ids were rejected: {notes}. Your previous answer:\n{raw}\n"
                   "Answer again for those ids only, JSON only.")


def narrate(table: list, scratch: str, run=_run_claude, *, names, cache: bool = True) -> dict:
    """-> {row key: filled sentence} for every change Claude rendered well. Rows missing from
    the result get the rule-based wording at render time. The cache holds unfilled templates,
    re-validated on every read."""
    if not table:
        return {}
    cache_path = os.path.join(scratch, "narration.json")
    cached = {}
    if cache and os.path.exists(cache_path):
        try:
            with open(cache_path) as fh:
                cached = json.load(fh)
        except Exception:
            cached = {}
    templates, todo = {}, []
    for row in table:
        t = cached.get(row["key"]) if isinstance(cached, dict) else None
        try:
            _check_narration(t, row, names)
            templates[row["key"]] = t
        except _Reject:
            todo.append(row)
    reason = ""
    for i in range(0, len(todo), NARRATE_BATCH):
        try:
            _narrate_batch(todo[i:i + NARRATE_BATCH], scratch, run, names, templates)
        except Exception as exc:  # transport failure: keep what we have, fall back for the rest
            reason = _failure_reason(exc, scratch)
            break
    if cache and templates:
        with open(cache_path, "w") as fh:
            json.dump(templates, fh)
    print(f"narration: unavailable ({reason})" if reason else f"narration: ok {len(templates)} of {len(table)}")
    by_key = {r["key"]: r for r in table}
    return {k: _check_narration(t, by_key[k], names) for k, t in templates.items()}
