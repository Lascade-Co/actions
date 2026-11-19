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
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
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
    def __init__(self, repo, archive, token, http, sleep, now, out, dry_run):
        self.repo, self.archive, self.token = repo, Path(archive), token
        self.http, self.sleep, self.now, self.out, self.dry_run = http, sleep, now, out, dry_run
        self.stats = {"archived": 0, "expired": 0, "deleted": 0, "skipped": 0,
                      "would_delete": 0, "bytes": 0, "stopped": False}
        self.processed = 0

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

    def process(self, run, done):
        run_id = str(run["id"])
        if self.dry_run:
            self.stats["would_delete"] += 1
            return
        zip_path = self.archive / f"{run_id}.zip"
        if (run_id, "archived") in done and zip_path.exists() and zip_path.stat().st_size > 0:
            pass
        elif (run_id, "expired") in done:
            pass
        else:
            log_status, size = self.download(run["id"])
            self.stats[log_status] += 1
            self.stats["bytes"] += size
            row = [run_id, run.get("name") or "", run.get("event") or "",
                   run.get("created_at") or "", run.get("conclusion") or "", log_status]
            with open(self.archive / "index.tsv", "a") as fh:
                fh.write("\t".join(x.replace("\t", " ").replace("\n", " ") for x in row) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            done.add((run_id, log_status))
        status, _, _ = self.request("DELETE", f"{API}/repos/{self.repo}/actions/runs/{run_id}")
        if status != 204:
            raise Stop(f"run {run_id}: delete returned {status}")
        self.stats["deleted"] += 1
        self.processed += 1
        if self.processed % 50 == 0:
            self.out(self.summary("progress"))

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
                runs = json.loads(body).get("workflow_runs", [])
                if not runs:
                    break
                completed = [r for r in runs if r.get("status") == "completed"]
                for r in runs:
                    if r.get("status") != "completed" and r["id"] not in skipped_ids:
                        skipped_ids.add(r["id"])
                        self.stats["skipped"] += 1
                for r in completed:
                    self.process(r, done)
                # Deleting shifts later pages forward, so re-read the same page.
                if self.dry_run or not completed:
                    page += 1
        except Stop as err:
            self.stats["stopped"] = True
            self.out(f"STOPPED: {err}")
        self.out(self.summary("done"))
        return self.stats


def purge(repo, archive, token, http=urllib_http, sleep=time.sleep, now=time.time,
          out=print, dry_run=False):
    return Purger(repo, archive, token, http, sleep, now, out, dry_run).run()


def gh_token():
    return subprocess.run(["gh", "auth", "token"], check=True, capture_output=True,
                          text=True).stdout.strip()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    stats = purge(args.repo, Path(args.archive).expanduser(), gh_token(), dry_run=args.dry_run)
    return 1 if stats["stopped"] else 0


if __name__ == "__main__":
    sys.exit(main())
