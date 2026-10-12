"""Prose written by Claude for the ad-spend email, none of which can state a number.

Two tasks share one table of the day's logged campaign changes:

* ``commentary``: the lead. One or two items, each an observed spend sentence and, when a
  logged change sits under that same line, a cause sentence (no person: the author is implicit).
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
CACHE_VERSION = 2
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
# An observation states what a line's spend did. Facts are per line, never per campaign, so it
# never needs campaign or action vocabulary; any reason belongs in the validated "cause".
_OBS_WORDS = {"because", "due", "thanks", "owing", "driven", "following", "caused", "causing", "reason",
              "reasons", "result", "resulting", "change", "changes", "stopping", "turning", "turned",
              "adding", "added", "removing", "removed", "decision"}
_OBS_STEMS = ("paus", "launch", "campaign", "creative", "closur", "keyword", "target", "bidding",
              "switch", "audience", "ad set", "adset")
# Words that start a reason; they belong in a cause's "reason", never in its "action".
_REASON_WORDS = {"because", "since", "due", "so", "thanks", "owing"}
_ABBREV = re.compile(r"\b(vs|approx|incl)\.", re.I)
_UP_KINDS = ("launched", "started", "new creative", "resumed")

HINTS = {
    "shape": "Answer with exactly the JSON shape asked for; every text is one sentence ending in a full stop, "
             "with no other full stop, question mark or exclamation mark inside it",
    "digit": "Replace every number with a placeholder; do not spell numbers out either",
    "placeholder": "Use only the placeholders listed for the ids you reference",
    "advice": "Remove advice and words like should, must, consider, recommend, team, decided",
    "direction": "Use up/down words only when they match the fact's label, or in a cause what was done",
    "name": "Name no person at all, and only the apps and platforms of the facts or change you reference",
    "link": "Give a cause only in an item whose only ref is the line that change was logged under",
    "cause": "Keep \"observed\" to what spend did, with no campaign, action or reason; put what was "
             "changed in cause.action and the why in cause.reason",
    "length": "Use fewer words: a cause's action and reason together at most 30 words, a change sentence at "
              "most 25",
    "code": "Describe the campaign in words instead of quoting its code",
}

PROMPT = """You are the VP of Performance Marketing writing the lead of a morning email for the CEO.
You are given FACTS about yesterday's ad spend and the CHANGES the ads team logged, as JSON.
Answer with 1 or 2 items about what stands out. Each item has:
- "observed": one sentence on what spend did. Observed spend only: never why, never a person or team, never advice.
- "refs": the fact ids that sentence uses.
  It never mentions a campaign, an action or a reason.
- "cause" (optional): {"change": "c3", "action": "...", "reason": "..."} only when a logged change sits under
  that same line, i.e. the change's "line_fact" is the item's only ref. "action": what was changed, past
  tense, with the campaign as the subject and no person ("The brand campaign on Apple was paused", "A new
  Pingee campaign went live on Google at {c3.budget}"). "reason": why, as a clause that completes the sentence
  ("because it was catching broad searches"). Plain words a CEO uses. Never quote a campaign code (like
  "MR - iOS - ASA - BRAND"); describe the campaign instead.
Rules for all text:
- NEVER write a digit and never spell a number out. Use placeholders: {f3.d}, {f3.pct}, {t.d} in "observed"
  (only fields listed for that fact); {c3.budget} in a cause (only when that change lists "budget"); it already
  reads like "$45 a day", so write "at {c3.budget}", never "{c3.budget} daily".
- A fact's "label" is its truth: do not say up for a down fact or the reverse. In a cause's "action",
  direction words (raised, cut, ...) must match what was done.
- No advice, no hype, no intensifiers. Never invent a cause that is not logged.
- Short: "observed" at most 25 words; a cause's action and reason together at most 30 words.
- Never name a person. Never name an app or platform that is not part of the item's own facts or change.
Prefer the biggest movers, and give the cause when a logged change explains one.
Answer with JSON only: {"items":[{"observed":"...","refs":["f3"],"cause":{"change":"c1","action":"...","reason":"..."}}]}

FACTS AND CHANGES:
"""

NARRATE_PROMPT = """You are writing for a CEO who is not an advertising specialist. Below are campaign changes
the ads team logged, as JSON. For each id write ONE plain-English sentence of at most 25 words, past tense,
starting with the verb, like a changelog entry ("Paused the Marine Radar brand campaign on Apple because ...").
Never name a person: who made the change is implicit. Say what was done to which campaign and why.
Describe the campaign in words (app, platform, market, goal); never quote the campaign code. Translate ad jargon
(ROAS, CPA, UAC, tier one, broad match, ad set, creative) into everyday words. Use only what the log says; do not
guess outcomes. Never write a digit and never spell a number out; for the daily budget write {cN.budget} (only for
ids whose "fields" list "budget"); it already reads like "$45 a day", so write "at {c1.budget}", never
"{c1.budget} daily". No advice. Mention no app or platform other than that change's own.
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
    # Ties broken by name, never by display order: the page's order must not change what Claude is told.
    lines.sort(key=lambda ln: (-abs(ln["d"] - ln["d1"]), ln["project"], ln["channel"], ln["os"]))
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
    """The day's logged changes, ids c1..cN: placed changes by line key (project, channel, os),
    newest first within a line, then the orphans. The order never follows the page's layout, so
    a new display order cannot renumber what Claude is told."""
    if not changes or not changes.get("ok"):
        return []
    fact_of = {f["key"]: fid for fid, f in build_facts(model, "A").items() if "key" in f}
    keys = {(ln["project"], ln["channel"], ln["os"]) for p in model["projects"] for ln in p["lines"]}
    ordered = []
    for k in sorted(keys):
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
    out = {k: row[k] for k in ("id", "channel", "project", "os", "app", "kind", "campaign",
                               "before", "after", "why", "line_fact")}
    if row["show"]:
        out["fields"] = sorted(row["show"])
    return out


def _proper(name: str) -> bool:
    """A name worth guarding case-insensitively: 'MarineRadar', 'Plant the World'; not 'Other',
    and not an all-caps code like 'TA' or 'GEO' (those are matched case-sensitively)."""
    return len(name) > 1 and not name.isupper() and (" " in name or any(ch.isupper() for ch in name[1:]))


class Names(set):
    """The names a sentence may not use unless its facts or change back them. `shorts` maps a
    registry short code ("MR", "GEO") to its project; codes match case-sensitively, so the
    word "geo" in "geo targeting" is not a name but "GEO" is."""

    def __init__(self, items=(), shorts=None, canon=None):
        super().__init__(items)
        self.shorts = dict(shorts or {})
        self.canon = dict(canon or {})     # squashed alias -> project ("geoanimator" -> "GeoAnimator")


def name_universe(apps: dict, table: list) -> set:
    """Every name a sentence could wrongly drop in. Blank values never enter: an empty name
    would match everywhere."""
    shorts = {spec["short"]: project for project, spec in apps.items()
              if spec.get("short") and spec["short"] != project}
    canon = {}
    for project, spec in apps.items():
        for alias in [project, *spec.get("prefixes", [])]:
            if alias == project or _proper(alias):     # "Geo Animator", not "TA" (a short code)
                canon[_squash(alias)] = project
            elif alias.isupper() and alias not in shorts:
                shorts[alias] = project                # "MT", "AR": case-sensitive like the short codes
    names = set(apps) | set(CHANNELS) | {"iOS", "Android"} | {a for p, sp in apps.items()
                                                              for a in sp.get("prefixes", []) if _proper(a)}
    for aliases in ALIASES.values():
        names |= aliases
    for row in table:
        names.add(row["author"])
        if row["app"] in apps or _proper(row["app"]):
            names.add(row["app"])
        names.add(row["project"])
    return Names({n.strip() for n in names if n and n.strip()}, shorts, canon)


def _backed(values) -> set:
    out = set()
    for v in values:
        if v and str(v).strip():
            out.add(str(v).strip().lower())
            out |= {a.lower() for a in ALIASES.get(str(v).strip(), ())}
    return out


def backed_names(row: dict) -> set:
    return _backed((row["project"], row["app"], row["channel"], row["os"]))     # never the author: no names


# ---- validation

def _mentions(text: str, name: str) -> bool:
    return re.search(rf"(?<![a-z0-9]){re.escape(name.lower())}(?![a-z0-9])", text.lower()) is not None


def _squash(name: str) -> str:
    """'TravelAnimator' and 'Travel Animator' are one name."""
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _check_names(text: str, names, backed: set) -> None:
    canon = getattr(names, "canon", {})

    def key(n):
        k = _squash(n)
        return _squash(canon.get(k, k))
    ok = {key(b) for b in backed}
    for name in names:
        if name and name.strip() and _mentions(text, name) and key(name) not in ok:
            raise _Reject("name")
    for short, project in getattr(names, "shorts", {}).items():
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(short)}(?![A-Za-z0-9])", text) and key(project) not in ok:
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
    probe = _ABBREV.sub(r"\1", _PLACEHOLDER.sub("X", sentence).strip())
    if not probe.endswith(".") or re.search(r"[.!?]", probe[:-1]):
        raise _Reject("shape")
    if _CODE.search(probe):
        raise _Reject("code")
    words = set(re.findall(r"[a-z]+", bare.lower()))
    if words & _ADVICE:
        raise _Reject("advice")
    low = bare.lower()
    if words & _OBS_WORDS or any(re.search(rf"\b{re.escape(st)}", low) for st in _OBS_STEMS):
        raise _Reject("cause")
    _direction_ok(words, {facts[r]["label"] for r in refs})
    _check_names(bare, names, _backed(v for r in refs for v in facts[r]["who"].values()))
    return _PLACEHOLDER.sub(lambda m: facts[m.group(1)]["show"][m.group(2)], sentence).strip()


def _check_template(text, row, names, max_words) -> str:
    """One sentence about one logged change, checked as a template, returned filled.
    Placeholders are validated first and replaced by a neutral 'X' for the remaining checks,
    so '{c1.budget}.' cannot look like two sentences and '$45.50' cannot look like a digit."""
    if not isinstance(text, str) or not text.strip():
        raise _Reject("shape")
    for cid, field in _PLACEHOLDER.findall(text):
        if cid != row["id"] or field not in row["show"]:
            raise _Reject("placeholder")
    probe = _ABBREV.sub(r"\1", _PLACEHOLDER.sub("X", text).strip())
    if "{" in probe or "}" in probe:
        raise _Reject("placeholder")
    if re.search(r"\d", probe):
        raise _Reject("digit")
    if not probe.endswith(".") or re.search(r"[.!?]", probe[:-1]):
        raise _Reject("shape")
    if len(probe.split()) > max_words:
        raise _Reject("length")
    if (row["campaign"] and row["campaign"].lower() in probe.lower()) or _CODE.search(probe):
        raise _Reject("code")
    words = set(re.findall(r"[a-z]+", probe.lower()))
    if words & (_ADVICE - {"budget"}):
        raise _Reject("advice")
    _check_names(probe, names, backed_names(row))
    return _tidy(_PLACEHOLDER.sub(lambda m: row["show"][m.group(2)], text).strip())


def _tidy(text: str) -> str:
    """'{c1.budget}' already reads '$45 a day'; drop the doubled unit a writer adds around it."""
    text = re.sub(r"(\$[\d.]+) a day(?: (?:daily|a day|per day|each day))", r"\1 a day", text)
    return re.sub(r"(daily budget of|budget of) (\$[\d.]+) a day", r"\1 \2 a day", text).replace(
        "daily budget of $", "budget of $")


def _item_observed(it, facts, names) -> str:
    if not isinstance(it, dict):
        raise _Reject("shape")
    refs = it.get("refs")
    if not isinstance(refs, list) or not refs or not all(isinstance(r, str) and r in facts for r in refs):
        raise _Reject("shape")
    return _check_observed(it.get("observed"), refs, facts, names)


def _item_cause(it, rows, names) -> str:
    cause = it.get("cause")
    if not isinstance(cause, dict):
        raise _Reject("shape")
    row = rows.get(cause.get("change"))
    if row is None or not row["line_fact"] or it.get("refs") != [row["line_fact"]]:
        raise _Reject("link")
    return _check_cause(cause, row, names)


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
            sentence = _item_observed(it, facts, names)
            if it.get("cause") is not None:
                sentence += " " + _item_cause(it, rows, names)
            out.append(sentence)
        return out
    except _Reject:
        raise
    except Exception:
        raise _Reject("shape") from None


def _salvage(raw: str, facts: dict, table: list, names) -> list:
    """The last resort after every attempt failed: keep each item that passes on its own,
    without its cause if only the cause fails. Every kept sentence passed the same rules."""
    try:
        doc = _parse(raw)
        items = doc.get("items") if isinstance(doc, dict) else None
        if not isinstance(items, list):
            return []
        rows = {r["id"]: r for r in table}
        out = []
        for it in items[:2]:
            try:
                sentence = _item_observed(it, facts, names)
            except _Reject:
                continue
            if it.get("cause") is not None:
                try:
                    sentence += " " + _item_cause(it, rows, names)
                except _Reject:
                    pass
            out.append(sentence)
        return out
    except Exception:
        return []


def _check_cause(cause: dict, row: dict, names) -> str:
    """A cause comes in two fields so that what was done and why are never confused:
    `action` ("The brand campaign was paused") carries the direction and is checked against
    the logged change; `reason` ("because it overspent") may say anything true about costs,
    up or down. Joined, the sentence passes every narration rule too."""
    action, reason = cause.get("action"), cause.get("reason")
    if not isinstance(action, str) or not action.strip() or (reason is not None and not isinstance(reason, str)):
        raise _Reject("shape")
    act = action.strip().rstrip(".").strip()
    words = set(re.findall(r"[a-z]+", _PLACEHOLDER.sub("X", act).lower()))
    if words & _REASON_WORDS:
        raise _Reject("cause")
    _direction_ok(words, {row["dir"]} if row["dir"] else set())
    why = (reason or "").strip().rstrip(".").strip()
    return _check_template(f"{act} {why}.".replace(" .", ".") if why else f"{act}.", row, names, 30)


def validate(raw: str, facts: dict, all_names=(), table=()) -> list:
    """Return the filled sentences, or [] if anything is off."""
    try:
        return _check(raw, facts, list(table), all_names if isinstance(all_names, Names) else set(all_names))
    except _Reject:
        return []


def _check_narration(text, row, names) -> str:
    return _check_template(text, row, names, 25)


# ---- calling Claude

# Sonnet thinks by default and its thinking counts toward max_tokens, so budgets leave it room.
# The call streams, so the timeout bounds a stall between chunks, not the whole reply.
def _run_claude(prompt: str, scratch: str, max_tokens: int = 4096):
    return generate_text(prompt, max_tokens=max_tokens, timeout=90)


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
    extra = set(_names(build_facts(model, "B"))) | set(CHANNELS) | {"iOS", "Android"}
    for aliases in ALIASES.values():
        extra |= aliases
    names = Names(set(names or ()) | extra, getattr(names, "shorts", {}), getattr(names, "canon", {}))
    cache_path = os.path.join(scratch, f"commentary-{variant}.json")
    if cache and os.path.exists(cache_path):
        try:
            with open(cache_path) as fh:
                cached = json.load(fh)
            # The answer itself is cached and re-validated, so a stricter rule or a new name
            # can never be bypassed by yesterday's prose.
            if cached.get("v") == CACHE_VERSION and cached.get("facts") == fingerprint:
                return _check(cached["raw"], facts, table, names)
        except Exception:
            pass
    reason, raw_ok, raws = "", None, []

    def check(raw):
        nonlocal raw_ok
        raws.append(raw)
        out = _check(raw, facts, table, names)
        raw_ok = raw
        return out
    try:
        result, word = _attempt("synopsis", run, scratch, PROMPT + json.dumps(sent, indent=1), check)
        failure = "" if result else f"rejected:{word or 'shape'}"
    except Exception as exc:  # auth, timeout, API failure: never a failed send
        result, word, failure = None, "", _failure_reason(exc, scratch)
    if not result:
        # Whatever ended the attempts (rejections, or a later call failing in transport), an
        # earlier answer may still hold items that pass on their own.
        for raw in reversed(raws):
            result = _salvage(raw, facts, table, names)
            if result:
                print(f"synopsis: partial ({word or failure})")
                raw_ok = None                         # never cache a salvage: a rerun may get a whole lead
                break
    if not result:
        result, reason = [], failure
    if reason:
        print(f"::warning title=Ad spend synopsis skipped::{reason}")
    elif cache and raw_ok:
        try:
            with open(cache_path, "w") as fh:
                json.dump({"v": CACHE_VERSION, "facts": fingerprint, "raw": raw_ok, "sentences": result}, fh)
        except OSError:
            pass                  # a cache is a convenience; the prose is already good
    return result


_SELF = "{self.budget}"


def _to_template(text: str) -> str:
    """Cache form: the row's own placeholder without its position-dependent id."""
    return re.sub(r"\{c\d+\.budget\}", _SELF, text)


def _for_row(template: str, row: dict) -> str:
    return template.replace(_SELF, "{%s.budget}" % row["id"])


def _narrate_batch(batch, scratch, run, names, templates) -> None:
    """Fill `templates` {key: unfilled sentence} for the rows Claude got right within ATTEMPTS calls."""
    max_tokens = min(16384, 4096 + 120 * len(batch))
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
                templates[row["key"]] = _to_template(text)
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
    templates, todo, queued = {}, [], set()
    for row in table:
        if row["key"] in templates or row["key"] in queued:      # an identical change: one sentence
            continue
        t = cached.get(row["key"]) if isinstance(cached, dict) else None
        try:
            _check_narration(_for_row(t, row) if isinstance(t, str) else t, row, names)
            templates[row["key"]] = t
        except _Reject:
            todo.append(row)
            queued.add(row["key"])
    reason = ""
    for i in range(0, len(todo), NARRATE_BATCH):
        try:
            _narrate_batch(todo[i:i + NARRATE_BATCH], scratch, run, names, templates)
        except Exception as exc:  # transport failure: keep what we have, fall back for the rest
            reason = _failure_reason(exc, scratch)
            break
    if cache and templates:
        try:
            with open(cache_path, "w") as fh:
                json.dump(templates, fh)
        except OSError:
            pass
    out = {}
    for row in table:
        t = templates.get(row["key"])
        if t is None or row["key"] in out:
            continue
        try:
            out[row["key"]] = _check_narration(_for_row(t, row), row, names)
        except _Reject:                       # never abort the email: the log's own words instead
            pass
    done = sum(1 for row in table if row["key"] in out)
    print(f"narration: unavailable ({reason})" if reason else f"narration: ok {done} of {len(table)}")
    return out
