"""Delivery settings shared by the workflow, builder, sender and watchdog.

Recipients are code-owned. No command-line argument can override them.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Edition:
    name: str
    timezone: str
    recipient: str
    sent_step: str
    send_crons: tuple[str, ...]
    prep_cron: str
    deliver_cron: str

    @property
    def zone(self):
        return ZoneInfo(self.timezone)

    @property
    def label(self):
        return self.name.upper()

    def at(self, date, hour, minute):
        return dt.datetime.combine(date, dt.time(hour, minute), self.zone).astimezone(dt.timezone.utc)

    def delivery_step(self, date):
        return f"{self.sent_step}{date}"

    def alert_step(self, date, phase):
        if phase not in ("prep", "deliver"):
            raise ValueError("unknown watchdog phase")
        return f"{'Prep alert' if phase == 'prep' else 'Alert'} {self.label} for {date}"

    def send_key(self, date, event, run_id):
        if event not in ("schedule", "workflow_dispatch"):
            raise ValueError("unsupported send event")
        suffix = "scheduled" if event == "schedule" else f"manual-{safe_suffix(run_id)}"
        return f"install-paid-{self.name}-{date}-{suffix}"

    def payload_name(self, date, run_id):
        return f"payload-{self.name}-{date}-{safe_suffix(run_id)}.json"

    def cache_name(self, cutoff):
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=dt.timezone.utc)
        stamp = cutoff.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        return f"window-{self.name}-{stamp}.local.json"

    def concurrency_group(self, event):
        return f"daily-install-paid-{self.name}-{event}"


def safe_suffix(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(value)):
        raise ValueError("invalid run identifier")
    return str(value)


EDITIONS = {
    "pt": Edition("pt", "America/Los_Angeles", "cherian@lascade.com", "Deliver for ",
                  tuple(f"41 {h} * * *" for h in (3, 6, 9)), "30 12,13 * * *", "15 13-19 * * *"),
    "ist": Edition("ist", "Asia/Kolkata", "fasna@lascade.com", "Deliver IST for ",
                   tuple(f"7 {h} * * *" for h in (13, 16, 19)), "0 0 * * *", "45 0-5 * * *"),
}


def get_edition(name="pt"):
    try:
        return EDITIONS[name]
    except KeyError:
        raise ValueError("unknown edition") from None


def resolve_daily_schedule(schedule):
    for name, edition in EDITIONS.items():
        if schedule in edition.send_crons:
            return name
    raise ValueError("unknown daily schedule")


def resolve_watchdog_schedule(schedule):
    for name, edition in EDITIONS.items():
        for phase, cron in (("prep", edition.prep_cron), ("deliver", edition.deliver_cron)):
            if schedule == cron:
                return name, phase
    raise ValueError("unknown watchdog schedule")
