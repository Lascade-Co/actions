"""One or two sentences of commentary, written by Codex, that cannot state a number.

Codex is shown computed facts with ids. It answers with sentences whose only
numbers are ``{fid.field}`` placeholders, and this module fills them from the
model. Any literal digit, unknown fact, contradicted direction, advice, or name
that is not backed by a referenced fact drops the whole commentary. Commentary
is decoration: every failure path returns ``[]`` and the email still sends.

Public repo: Codex stdout/stderr go to files and are never echoed.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from decimal import Decimal

from pnl_money import format_usd
from adspend_model import UNASSIGNED, round_pct

MAX_FACTS = 8
_PLACEHOLDER = re.compile(r"\{(t|f\d+)\.(\w+)\}")
_UP = {"up", "rose", "increased", "higher", "climbed", "grew", "jumped", "raised"}
_DOWN = {"down", "fell", "dropped", "lower", "cut", "declined", "decreased", "shrank", "reduced"}
_ADVICE = {"should", "recommend", "recommended", "consider", "must", "team", "decided", "budget"}

PROMPT = """You are the VP of Performance Marketing writing the lead of a morning email for the CEO.
You are given FACTS about yesterday's ad spend as JSON. Write at most 2 sentences that say
what stands out. Rules:
- Describe observed spend only. Never say why, never name a person or team, never give advice.
- NEVER write a digit. To use a number, write a placeholder like {f3.d} or {f3.pct} or {t.d}.
  Only fields listed under "fields" for that fact exist.
- Every sentence must list the fact ids it uses in "refs".
- A fact's "label" is its truth: do not say up for a down fact or the reverse.
- Plain words. No hype, no intensifiers.
Answer with JSON only: {"sentences":[{"text":"...","refs":["f3"]}]}

FACTS:
"""


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
    """fid -> {"who": {...names}, "label": str, "raw": {field: number}, "show": {field: str}}"""
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
        }
    return facts


def _names(facts: dict) -> set:
    out = set()
    for f in facts.values():
        out.update(v for v in f["who"].values())
    return out


def validate(raw: str, facts: dict, all_names=()) -> list:
    """Return the filled sentences, or [] if anything is off."""
    try:
        text = raw.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n|\n```$", "", text)
        doc = json.loads(text)
        items = doc["sentences"]
        assert isinstance(items, list) and 1 <= len(items) <= 2
        filled = []
        for it in items:
            sentence, refs = it["text"], it["refs"]
            assert isinstance(sentence, str) and isinstance(refs, list) and refs
            assert all(r in facts for r in refs)
            bare = _PLACEHOLDER.sub("", sentence)
            assert not re.search(r"\d", bare), "literal digit"
            used = set(_PLACEHOLDER.findall(sentence))
            for fid, field in used:
                assert fid in refs and field in facts[fid]["show"], "bad placeholder"
            assert "{" not in bare and "}" not in bare
            words = set(re.findall(r"[a-z]+", bare.lower()))
            assert not (words & _ADVICE), "advice or attribution"
            labels = {facts[r]["label"] for r in refs}
            if words & _UP:
                assert labels & {"up", "started"}, "direction contradicts facts"
            if words & _DOWN:
                assert labels & {"down", "stopped"}, "direction contradicts facts"
            backed = {v.lower() for r in refs for v in facts[r]["who"].values()}
            for name in all_names:
                if re.search(rf"\b{re.escape(name.lower())}\b", bare.lower()):
                    assert name.lower() in backed, "name not in referenced facts"
            filled.append(_PLACEHOLDER.sub(lambda m: facts[m.group(1)]["show"][m.group(2)], sentence).strip())
        return filled
    except Exception:
        return []


def _run_codex(prompt: str, scratch: str, timeout: int = 90):
    out = os.path.join(scratch, "codex-last.txt")
    if os.path.exists(out):
        os.remove(out)
    with open(os.path.join(scratch, "codex.log"), "w") as log:
        subprocess.run(["codex", "exec", "--sandbox", "read-only", "--skip-git-repo-check",
                        "-o", out, "-"], input=prompt, text=True, stdout=log, stderr=log,
                       timeout=timeout, check=True)
    with open(out) as fh:
        return fh.read()


def commentary(model: dict, variant: str, scratch: str, run=_run_codex) -> list:
    """Cached per (ad_date, variant) so a retry re-sends identical bytes."""
    cache = os.path.join(scratch, f"commentary-{variant}.json")
    if os.path.exists(cache):
        try:
            with open(cache) as fh:
                return json.load(fh)["sentences"]
        except Exception:
            pass
    facts = build_facts(model, variant)
    try:
        payload = {fid: {**f["who"], "label": f["label"], "values": f["show"], "fields": sorted(f["show"])}
                   for fid, f in facts.items()}
        raw = run(PROMPT + json.dumps(payload, indent=1), scratch)
        names = _names(build_facts(model, "B")) | {"Meta", "Google", "iOS", "Android"}
        result = validate(raw, facts, names)
    except Exception:  # missing binary, timeout, crash: no commentary, never a failed send
        result = []
    with open(cache, "w") as fh:
        json.dump({"sentences": result}, fh)
    return result
