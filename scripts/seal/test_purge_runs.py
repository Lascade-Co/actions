import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import purge_runs as pr

REPO = "o/r"
API = "https://api.github.com/repos/o/r/actions"


def run(id_, status="completed"):
    return {"id": id_, "name": "wf", "event": "push", "created_at": "2026-01-01T00:00:00Z",
            "conclusion": "success", "status": status}


class Fake:
    """Fake http client returning (status, headers, body). Deletions shift the run list."""

    def __init__(self, runs, logs=None, delete_status=204, headers=None):
        self.runs = list(runs)
        self.logs = logs or {}  # id -> (status, body)
        self.delete_status = delete_status
        self.calls = []
        self.extra_headers = headers or {}

    def __call__(self, method, url, headers=None):
        headers = headers or {}
        self.calls.append((method, url, dict(headers)))
        if method == "GET" and url.startswith(f"{API}/runs?"):
            page = int(url.split("&page=")[1])
            chunk = self.runs[(page - 1) * 100:page * 100]
            return 200, dict(self.extra_headers), json.dumps({"workflow_runs": chunk}).encode()
        if method == "GET" and url.endswith("/logs"):
            rid = int(url.split("/runs/")[1].split("/")[0])
            status, body = self.logs.get(rid, (200, b"zipdata"))
            if status == 200:
                return 302, {"Location": f"https://blob.example/{rid}"}, b""
            return status, {}, b""
        if method == "GET" and url.startswith("https://blob.example/"):
            rid = int(url.rsplit("/", 1)[1])
            return 200, {}, self.logs.get(rid, (200, b"zipdata"))[1]
        if method == "DELETE":
            rid = int(url.rsplit("/", 1)[1])
            if self.delete_status == 204:
                self.runs = [r for r in self.runs if r["id"] != rid]
            return self.delete_status, {}, b""
        raise AssertionError(url)

    def deleted(self):
        return [int(u.rsplit("/", 1)[1]) for m, u, _ in self.calls if m == "DELETE"]

    def log_fetches(self):
        return [u for m, u, _ in self.calls if u.endswith("/logs")]


def go(fake, tmp_path, **kw):
    sleeps = kw.pop("sleeps", [])
    return pr.purge(REPO, tmp_path / "arc", "tok", http=fake, sleep=sleeps.append,
                    out=lambda *_: None, **kw)


def test_archives_then_deletes(tmp_path):
    fake = Fake([run(1), run(2)], logs={1: (200, b"A"), 2: (200, b"B")})
    stats = go(fake, tmp_path)
    arc = tmp_path / "arc"
    assert (arc / "1.zip").read_bytes() == b"A"
    assert (arc / "2.zip").read_bytes() == b"B"
    assert fake.deleted() == [1, 2]
    lines = (arc / "index.tsv").read_text().splitlines()
    assert lines[0] == "1\twf\tpush\t2026-01-01T00:00:00Z\tsuccess\tarchived"
    assert stats["archived"] == 2 and stats["deleted"] == 2 and stats["bytes"] == 2
    blob = [h for m, u, h in fake.calls if u.startswith("https://blob.example")]
    assert blob and all("Authorization" not in h for h in blob)
    api = [h for m, u, h in fake.calls if u.startswith("https://api.github.com")]
    assert all(h.get("Authorization") == "Bearer tok" for h in api)


def test_expired_logs_still_deleted(tmp_path):
    fake = Fake([run(1), run(2)], logs={1: (410, b""), 2: (404, b"")})
    stats = go(fake, tmp_path)
    assert fake.deleted() == [1, 2]
    assert stats["expired"] == 2 and stats["archived"] == 0
    assert not (tmp_path / "arc" / "1.zip").exists()
    assert "\texpired\n" in (tmp_path / "arc" / "index.tsv").read_text()


def test_server_error_stops_without_delete(tmp_path):
    fake = Fake([run(1), run(2)], logs={1: (500, b"")})
    stats = go(fake, tmp_path)
    assert fake.deleted() == []
    assert stats["stopped"] is True
    index = tmp_path / "arc" / "index.tsv"
    assert not index.exists() or index.read_text() == ""


def test_skips_in_progress(tmp_path):
    fake = Fake([run(1, "in_progress"), run(2), run(3, "queued")])
    stats = go(fake, tmp_path)
    assert fake.deleted() == [2]
    assert stats["skipped"] == 2


def test_empty_zip_not_promoted(tmp_path):
    fake = Fake([run(1)], logs={1: (200, b"")})
    stats = go(fake, tmp_path)
    arc = tmp_path / "arc"
    assert fake.deleted() == []
    assert stats["stopped"] is True
    assert [p.name for p in arc.iterdir() if p.name != "index.tsv"] == []


def test_dry_run_deletes_nothing(tmp_path):
    fake = Fake([run(1), run(2)])
    stats = go(fake, tmp_path, dry_run=True)
    assert fake.deleted() == []
    assert fake.log_fetches() == []
    assert stats["deleted"] == 0 and stats["would_delete"] == 2
    assert not (tmp_path / "arc" / "1.zip").exists()


def test_resume_skips_redownload(tmp_path):
    arc = tmp_path / "arc"
    arc.mkdir()
    (arc / "1.zip").write_bytes(b"old")
    (arc / "index.tsv").write_text("1\twf\tpush\t2026-01-01T00:00:00Z\tsuccess\tarchived\n")
    fake = Fake([run(1)])
    go(fake, tmp_path)
    assert fake.log_fetches() == []
    assert fake.deleted() == [1]
    assert (arc / "1.zip").read_bytes() == b"old"
    assert (arc / "index.tsv").read_text().count("1\twf") == 1


def test_rate_limit_sleeps_until_reset(tmp_path):
    fake = Fake([run(1)], headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1030"})
    sleeps = []
    pr.purge(REPO, tmp_path / "arc", "tok", http=fake, sleep=sleeps.append,
             out=lambda *_: None, now=lambda: 1000.0)
    assert sleeps and sleeps[0] >= 30


def test_retry_after_sleeps_and_retries(tmp_path):
    base = Fake([run(1)])
    state = {"n": 0}

    def http(method, url, headers=None):
        if method == "DELETE" and state["n"] == 0:
            state["n"] = 1
            return 429, {"Retry-After": "7"}, b""
        return base(method, url, headers)

    sleeps = []
    pr.purge(REPO, tmp_path / "arc", "tok", http=http, sleep=sleeps.append, out=lambda *_: None)
    assert any(s >= 7 for s in sleeps)
    assert base.deleted() == [1]
