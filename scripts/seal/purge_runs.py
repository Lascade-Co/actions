#!/usr/bin/env python3
"""Archive every completed GitHub Actions run's logs locally, then delete the run.

Usage: purge_runs.py --repo OWNER/REPO --archive DIR [--dry-run]
The token comes from `gh auth token`. Re-running is idempotent: a run already in
index.tsv whose zip exists is deleted without being downloaded again.
"""
import argparse
import http.client
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API = "https://api.github.com"
MAX_RETRIES = 5
TRANSPORT_ATTEMPTS = 3


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def urllib_http(method, url, headers=None):
    """Return (status, headers, body) without following redirects."""
    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with opener.open(req, timeout=120) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read()


def _header(headers, name):
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


class Stop(Exception):
    pass


class Purger:
    def __init__(self, repo, archive, token, http, sleep, now, out, dry_run, progress=None,
                 workers=1, delete_interval=0.0, expire_before=None):
        self.repo, self.archive, self.token = repo, Path(archive), token
        self.http, self.sleep, self.now, self.out, self.dry_run = http, sleep, now, out, dry_run
        self.progress = progress or (lambda line: None)
        self.workers, self.delete_interval = max(1, workers), delete_interval
        self.expire_before = expire_before  # ISO-8601 UTC; older runs' logs are past retention
        self.last_delete = None
        self.stats = {"archived": 0, "expired": 0, "deleted": 0, "skipped": 0,
                      "would_delete": 0, "bytes": 0, "stopped": False}
        self.processed = 0
        self.total = None
        self.started = now()

    def request(self, method, url, auth=True):
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if auth:
            headers["Authorization"] = f"Bearer {self.token}"
        transport_failures = 0
        for _ in range(MAX_RETRIES + TRANSPORT_ATTEMPTS):
            try:
                status, resp_headers, body = self.http(method, url, headers)
                if status >= 500:
                    raise http.client.HTTPException(f"server error {status}")
            except (OSError, http.client.HTTPException) as err:
                transport_failures += 1
                if transport_failures >= TRANSPORT_ATTEMPTS:
                    raise Stop(f"{method} {url}: {err!r} after {transport_failures} attempts")
                self.sleep(2 ** transport_failures)
                continue
            retry_after = _header(resp_headers, "Retry-After")
            if status in (403, 429) and retry_after is not None:
                self.sleep(float(retry_after) + 1)
                continue
            remaining = _header(resp_headers, "X-RateLimit-Remaining")
            reset = _header(resp_headers, "X-RateLimit-Reset")
            if remaining == "0" and reset is not None:
                wait = max(0.0, float(reset) - self.now()) + 1
                if status in (403, 429):
                    self.sleep(wait)
                    continue
                self.sleep(wait)
            return status, resp_headers, body
        raise Stop(f"{method} {url}: still rate limited after {MAX_RETRIES} retries")

    def load_index(self):
        index = self.archive / "index.tsv"
        done = set()
        if index.exists():
            for line in index.read_text().splitlines():
                parts = line.split("\t")
                if len(parts) >= 6:
                    done.add((parts[0], parts[5]))
        return done

    def download(self, run_id):
        """Return ('archived', size) or ('expired', 0); raise Stop on anything else."""
        url = f"{API}/repos/{self.repo}/actions/runs/{run_id}/logs"
        status, headers, body = self.request("GET", url)
        if status in (301, 302, 303, 307, 308):
            location = _header(headers, "Location")
            if not location:
                raise Stop(f"run {run_id}: redirect without Location")
            # Blob host: never forward the token.
            status, headers, body = self.request("GET", location, auth=False)
        elif status in (404, 410):
            return "expired", 0
        if status != 200:
            raise Stop(f"run {run_id}: logs request returned {status}")
        if not body:
            raise Stop(f"run {run_id}: empty log archive")
        if not zipfile.is_zipfile(io.BytesIO(body)):
            raise Stop(f"run {run_id}: logs body is not a zip archive")
        fd, tmp = tempfile.mkstemp(dir=self.archive, prefix=f".{run_id}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            if os.path.getsize(tmp) == 0:
                raise Stop(f"run {run_id}: empty log archive")
            os.replace(tmp, self.archive / f"{run_id}.zip")
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        return "archived", len(body)

    def already_archived(self, run, done):
        run_id = str(run["id"])
        zip_path = self.archive / f"{run_id}.zip"
        if (run_id, "archived") in done and zip_path.exists() and zip_path.stat().st_size > 0:
            return True
        return (run_id, "expired") in done

    def fetch_log(self, run):
        """Worker-safe: archive one run's logs. Returns (log_status, size); raises Stop."""
        created = run.get("created_at") or ""
        if self.expire_before and created and created < self.expire_before:
            return "expired", 0  # older than log retention: nothing to download
        return self.download(run["id"])

    def process_batch(self, runs, done):
        """Fetch logs (in parallel when workers > 1), then record and delete runs in order."""
        if self.dry_run:
            self.stats["would_delete"] += len(runs)
            return
        todo = [(r, self.already_archived(r, done)) for r in runs]
        if self.workers <= 1:
            for r, skip in todo:
                self.finish(r, None if skip else self.fetch_log(r), done)
            return
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [None if skip else pool.submit(self.fetch_log, r) for r, skip in todo]
            try:
                for (r, _), fut in zip(todo, futures):
                    self.finish(r, None if fut is None else fut.result(), done)
            except BaseException:
                for fut in futures:
                    if fut is not None:
                        fut.cancel()
                raise

    def finish(self, run, result, done):
        """Main thread only: index the fetched log, then delete the run (paced)."""
        run_id = str(run["id"])
        if result is not None:
            log_status, size = result
            self.stats[log_status] += 1
            self.stats["bytes"] += size
            row = [run_id, run.get("name") or "", run.get("event") or "",
                   run.get("created_at") or "", run.get("conclusion") or "", log_status]
            with open(self.archive / "index.tsv", "a") as fh:
                fh.write("\t".join(x.replace("\t", " ").replace("\n", " ") for x in row) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            done.add((run_id, log_status))
        if self.delete_interval > 0 and self.last_delete is not None:
            wait = self.delete_interval - (self.now() - self.last_delete)
            if wait > 0:
                self.sleep(wait)
        run_url = f"{API}/repos/{self.repo}/actions/runs/{run_id}"
        status, _, _ = self.request("DELETE", run_url)
        self.last_delete = self.now()
        if status == 404:
            # A retried DELETE whose first attempt succeeded answers 404; confirm it's gone.
            check, _, _ = self.request("GET", run_url)
            if check != 404:
                raise Stop(f"run {run_id}: delete returned 404 but the run still exists ({check})")
        elif status != 204:
            raise Stop(f"run {run_id}: delete returned {status}")
        self.stats["deleted"] += 1
        self.processed += 1
        self.progress(self.progress_line(run))

    def progress_line(self, run=None):
        return format_progress(self.processed, self.total, self.stats,
                               self.now() - self.started, run)

    def summary(self, label):
        s = self.stats
        return (f"{label}: archived={s['archived']} expired={s['expired']} deleted={s['deleted']} "
                f"skipped={s['skipped']} bytes={s['bytes']}"
                + (f" would_delete={s['would_delete']}" if self.dry_run else ""))

    def run(self):
        if not self.dry_run:
            self.archive.mkdir(parents=True, exist_ok=True)
        done = self.load_index() if not self.dry_run else set()
        skipped_ids = set()
        page = 1
        try:
            while True:
                url = f"{API}/repos/{self.repo}/actions/runs?per_page=100&page={page}"
                status, _, body = self.request("GET", url)
                if status != 200:
                    raise Stop(f"listing runs returned {status}")
                listing = json.loads(body)
                runs = listing.get("workflow_runs", [])
                if self.total is None:
                    self.total = listing.get("total_count")
                if not runs:
                    break
                completed = [r for r in runs if r.get("status") == "completed"]
                for r in runs:
                    if r.get("status") != "completed" and r["id"] not in skipped_ids:
                        skipped_ids.add(r["id"])
                        self.stats["skipped"] += 1
                self.process_batch(completed, done)
                # Deleting shifts later pages forward, so re-read the same page.
                if self.dry_run or not completed:
                    page += 1
        except Stop as err:
            self.stats["stopped"] = True
            self.progress(None)
            self.out(f"STOPPED: {err}")
        self.progress(None)
        self.out(self.summary("done"))
        return self.stats


def _duration(seconds):
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def _size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def format_progress(done, total, stats, elapsed, run=None):
    """One status line: done/total, percent, counts, size, rate, ETA and the current run."""
    total_txt = str(total) if total else "?"
    pct = f"{100 * done / total:5.1f}%" if total else "  ?  "
    rate = done / elapsed * 60 if elapsed > 0 else 0.0
    eta = _duration((total - done) / rate * 60) if total and rate > 0 and total >= done else "?"
    line = (f"[{done}/{total_txt} {pct}] archived {stats['archived']} expired {stats['expired']}"
            f" · {_size(stats['bytes'])} · {rate:.1f} runs/min · elapsed {_duration(elapsed)}"
            f" · ETA {eta}")
    if run:
        line += f" · {(run.get('created_at') or '')[:10]} {run.get('name') or ''}".rstrip()
    return line


def terminal_progress(stream=sys.stderr, every=50):
    """Redraw one line on a terminal; elsewhere print every `every`-th update."""
    state = {"n": 0, "width": 0}

    def report(line):
        if line is None:  # finish the line before other output
            if stream.isatty() and state["width"]:
                stream.write("\n")
                stream.flush()
                state["width"] = 0
            return
        state["n"] += 1
        if stream.isatty():
            cols = shutil.get_terminal_size((120, 20)).columns
            text = line[: cols - 1]
            stream.write("\r" + text + " " * max(0, state["width"] - len(text)))
            state["width"] = len(text)
            stream.flush()
        elif state["n"] % every == 0:
            stream.write(line + "\n")
            stream.flush()

    return report


def purge(repo, archive, token, http=urllib_http, sleep=time.sleep, now=time.time,
          out=print, dry_run=False, progress=None, workers=1, delete_interval=0.0,
          expire_before=None):
    return Purger(repo, archive, token, http, sleep, now, out, dry_run, progress,
                  workers, delete_interval, expire_before).run()


RETENTION_MARGIN_DAYS = 2


def expiry_cutoff(retention_days, now):
    """ISO timestamp before which a run's logs are past retention (with a safety margin)."""
    cutoff = now - (retention_days + RETENTION_MARGIN_DAYS) * 86400
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))


def repo_log_retention_days(repo, token, http=urllib_http, default=90):
    status, _, body = http("GET", f"{API}/repos/{repo}/actions/permissions/artifact-and-log-retention",
                           {"Accept": "application/vnd.github+json",
                            "Authorization": f"Bearer {token}"})
    if status == 200:
        try:
            return int(json.loads(body)["days"])
        except (ValueError, KeyError, TypeError):
            pass
    return default


def gh_token():
    return subprocess.run(["gh", "auth", "token"], check=True, capture_output=True,
                          text=True).stdout.strip()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=4,
                        help="parallel log downloads (deletes stay one at a time); default 4")
    parser.add_argument("--delete-interval", type=float, default=1.0,
                        help="minimum seconds between deletes (GitHub asks for >= 1); default 1")
    parser.add_argument("--log-retention-days", type=int, default=None,
                        help="skip the log download for runs older than this many days "
                             f"(+{RETENTION_MARGIN_DAYS} margin); default: the repo's setting")
    args = parser.parse_args(argv)
    token = gh_token()
    days = args.log_retention_days or repo_log_retention_days(args.repo, token)
    cutoff = expiry_cutoff(days, time.time())
    print(f"Runs created before {cutoff} ({days}-day log retention + {RETENTION_MARGIN_DAYS} days)"
          f" are recorded as expired without downloading. {args.workers} download workers,"
          f" deletes at most one per {args.delete_interval:g}s.")
    stats = purge(args.repo, Path(args.archive).expanduser(), token, dry_run=args.dry_run,
                  progress=terminal_progress(), workers=args.workers,
                  delete_interval=args.delete_interval, expire_before=cutoff)
    return 1 if stats["stopped"] else 0


if __name__ == "__main__":
    sys.exit(main())
