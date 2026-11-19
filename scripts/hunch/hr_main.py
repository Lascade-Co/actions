"""Build the daily TreasureHunch email. Public logs contain status words only."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import hr_commentary as C
import hr_fetch as F
import hr_model as M
import hr_render_email as R

PT = ZoneInfo("America/Los_Angeles")
UTC = dt.timezone.utc


def utc_now():
    return dt.datetime.now(UTC)


def build_deadline(report_date, started_at):
    """Before 05:48 PT, finish by that time; later/historical runs are unbounded."""
    deadline = dt.datetime.combine(report_date, dt.time(5, 48), PT).astimezone(UTC)
    return deadline if started_at < deadline else None


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default error can echo arbitrary input into public logs.
        self.exit(2, "arguments-invalid\n")


def _check_deadline(deadline):
    if deadline is not None and utc_now() >= deadline:
        raise F.FetchError("build-deadline")


def _summary(facts, deadline):
    # The optional API stream gets an earlier absolute deadline. Its timeout
    # only bounds inactivity, so a streaming response can outlive that timeout.
    summary_deadline = deadline - dt.timedelta(seconds=15) if deadline else None
    if summary_deadline is not None and utc_now() >= summary_deadline:
        print("summary: fallback:deadline")
        return C.FALLBACK
    try:
        with F.absolute_deadline(summary_deadline):
            return C.summarize(facts, deadline=summary_deadline, clock=utc_now)
    except Exception:
        print("summary: fallback:deadline" if summary_deadline is not None
              and utc_now() >= summary_deadline else "summary: fallback:invalid")
        return C.FALLBACK


def main(argv=None):
    started_at = utc_now()
    parser = _Parser(description=__doc__)
    parser.add_argument("--report-date", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--out", type=Path, default=Path("out"))
    parser.add_argument("--dry-run", action="store_true", help="Build only; delivery is always a separate command")
    args = parser.parse_args(argv)
    if not os.environ.get("POSTHOG_PERSONAL_API_KEY", "").strip():
        print("posthog-key-missing", file=sys.stderr)
        return 2
    cutoff = dt.datetime.combine(args.report_date, dt.time(5), PT).astimezone(UTC)
    if started_at < cutoff:
        print("report-not-ready", file=sys.stderr)
        return 2
    deadline = build_deadline(args.report_date, started_at)
    try:
        raw = F.fetch_all(args.report_date, deadline=deadline)
        _check_deadline(deadline)
        # Fetch guards its own calls. Keep this separate to avoid nesting alarms.
        with F.absolute_deadline(deadline):
            model = M.build_model(args.report_date, **raw, test_number=os.environ.get("HR_TEST_NUMBER") or None)
        model["summary"] = _summary(model["facts"], deadline)
        _check_deadline(deadline)
        with F.absolute_deadline(deadline):
            subject, html, text = R.render(model)
            _check_deadline(deadline)
            args.out.mkdir(parents=True, exist_ok=True)
            (args.out / "email.json").write_text(json.dumps(
                {"subject": subject, "html": html, "text": text}, ensure_ascii=False, allow_nan=False
            ), encoding="utf-8")
            _check_deadline(deadline)
    except F.FetchError as error:
        print("deadline" if error.reason == "build-deadline" else error.reason, file=sys.stderr)
        return 3
    except Exception:
        print("build-failed", file=sys.stderr)
        return 1
    print("rendered")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
