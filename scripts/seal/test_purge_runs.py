import io
import json
import sys
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import purge_runs as pr

REPO = "o/r"
API = "https://api.github.com/repos/o/r/actions"


def zb(content=b"x"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("log.txt", content)
    return buf.getvalue()


ZIP = zb()


def run(id_, status="completed"):
    return {"id": id_, "name": "wf", "event": "push", "created_at": "2026-01-01T00:00:00Z",
            "conclusion": "success", "status": status}


class Fake:
    """Fake http client returning (status, headers, body). Deletions shift the run list."""

    def __init__(self, runs, logs=None, delete_status=204, headers=None, gone_on_404=True):
        self.runs = list(runs)
        self.gone_on_404 = gone_on_404
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
            listing = {"total_count": len(self.runs), "workflow_runs": chunk}
            return 200, dict(self.extra_headers), json.dumps(listing).encode()
        if method == "GET" and url.endswith("/logs"):
            rid = int(url.split("/runs/")[1].split("/")[0])
            status, body = self.logs.get(rid, (200, ZIP))
            if status == 200:
                return 302, {"Location": f"https://blob.example/{rid}"}, b""
            return status, {}, b""
        if method == "GET" and url.startswith("https://blob.example/"):
            rid = int(url.rsplit("/", 1)[1])
            return 200, {}, self.logs.get(rid, (200, ZIP))[1]
        if method == "DELETE":
            rid = int(url.rsplit("/", 1)[1])
            if self.delete_status == 204 or (self.delete_status == 404 and self.gone_on_404):
                self.runs = [r for r in self.runs if r["id"] != rid]
            return self.delete_status, {}, b""
        if method == "GET" and "/runs/" in url and url.rsplit("/", 1)[1].isdigit():
            rid = int(url.rsplit("/", 1)[1])
            exists = any(r["id"] == rid for r in self.runs)
            return (200 if exists else 404), {}, b""
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
    fake = Fake([run(1), run(2)], logs={1: (200, zb(b"A")), 2: (200, zb(b"B"))})
    stats = go(fake, tmp_path)
    arc = tmp_path / "arc"
    assert (arc / "1.zip").read_bytes() == zb(b"A")
    assert (arc / "2.zip").read_bytes() == zb(b"B")
    assert fake.deleted() == [1, 2]
    lines = (arc / "index.tsv").read_text().splitlines()
    assert lines[0] == "1\twf\tpush\t2026-01-01T00:00:00Z\tsuccess\tarchived"
    assert stats["archived"] == 2 and stats["deleted"] == 2 and stats["bytes"] == len(zb(b"A")) + len(zb(b"B"))
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


def test_blob_404_stops_without_delete(tmp_path):
    fake = Fake([run(1)])
    inner = fake.__call__

    def http(method, url, headers=None):
        if url.startswith("https://blob.example/"):
            return 404, {}, b""
        return inner(method, url, headers)

    stats = pr.purge(REPO, tmp_path / "arc", "tok", http=http, sleep=lambda s: None, out=lambda *_: None)
    assert stats["stopped"] and fake.deleted() == [] and stats["expired"] == 0


def test_transport_error_retried_then_succeeds(tmp_path):
    base = Fake([run(1)])
    state = {"n": 0}

    def http(method, url, headers=None):
        if method == "DELETE" and state["n"] < 2:
            state["n"] += 1
            raise ConnectionResetError("boom")
        return base(method, url, headers)

    sleeps = []
    stats = pr.purge(REPO, tmp_path / "arc", "tok", http=http, sleep=sleeps.append, out=lambda *_: None)
    assert base.deleted() == [1] and not stats["stopped"] and len(sleeps) == 2


def test_transport_error_exhausted_stops(tmp_path):
    def http(method, url, headers=None):
        raise http_client_error()

    def http_client_error():
        import http.client
        return http.client.IncompleteRead(b"")

    out = []
    stats = pr.purge(REPO, tmp_path / "arc", "tok", http=http, sleep=lambda s: None, out=out.append)
    assert stats["stopped"] and any("STOPPED" in o for o in out)


def test_5xx_retried_then_stops(tmp_path):
    fake = Fake([run(1)], logs={1: (500, b"")})
    sleeps = []
    stats = pr.purge(REPO, tmp_path / "arc", "tok", http=fake, sleep=sleeps.append, out=lambda *_: None)
    assert stats["stopped"] and fake.deleted() == []
    assert len(fake.log_fetches()) == pr.TRANSPORT_ATTEMPTS and sleeps


def test_non_zip_body_stops_without_delete(tmp_path):
    fake = Fake([run(1)], logs={1: (200, b"<html>not a zip</html>")})
    stats = go(fake, tmp_path)
    assert stats["stopped"] and fake.deleted() == []
    assert not (tmp_path / "arc" / "1.zip").exists()


def test_resume_empty_zip_redownloads(tmp_path):
    arc = tmp_path / "arc"
    arc.mkdir()
    (arc / "1.zip").write_bytes(b"")
    (arc / "index.tsv").write_text("1\twf\tpush\tt\tsuccess\tarchived\n")
    fake = Fake([run(1)])
    go(fake, tmp_path)
    assert len(fake.log_fetches()) == 1 and (arc / "1.zip").read_bytes() == ZIP


def test_resume_missing_zip_redownloads(tmp_path):
    arc = tmp_path / "arc"
    arc.mkdir()
    (arc / "index.tsv").write_text("1\twf\tpush\tt\tsuccess\tarchived\n")
    fake = Fake([run(1)])
    go(fake, tmp_path)
    assert len(fake.log_fetches()) == 1 and fake.deleted() == [1]
    assert (arc / "1.zip").read_bytes() == ZIP


def test_resume_expired_row_deletes_without_download(tmp_path):
    arc = tmp_path / "arc"
    arc.mkdir()
    (arc / "index.tsv").write_text("1\twf\tpush\tt\tsuccess\texpired\n")
    fake = Fake([run(1)])
    go(fake, tmp_path)
    assert fake.log_fetches() == [] and fake.deleted() == [1]


def test_pagination_over_100_completed_with_deletion(tmp_path):
    fake = Fake([run(i) for i in range(1, 251)])
    stats = go(fake, tmp_path)
    assert stats["deleted"] == 250 and fake.runs == []
    assert sorted(fake.deleted()) == list(range(1, 251))


def test_urllib_http_does_not_follow_or_forward_auth():
    seen = {}

    class Second(BaseHTTPRequestHandler):
        def do_GET(self):
            seen["auth"] = self.headers.get("Authorization")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"blob")

        def log_message(self, *a):
            pass

    second = HTTPServer(("127.0.0.1", 0), Second)
    second_url = f"http://127.0.0.1:{second.server_port}/blob"

    class First(BaseHTTPRequestHandler):
        def do_GET(self):
            seen["first_auth"] = self.headers.get("Authorization")
            self.send_response(302)
            self.send_header("Location", second_url)
            self.end_headers()

        def log_message(self, *a):
            pass

    first = HTTPServer(("127.0.0.1", 0), First)
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (first, second)]
    for t in threads:
        t.start()
    try:
        status, headers, _ = pr.urllib_http(
            "GET", f"http://127.0.0.1:{first.server_port}/logs", {"Authorization": "Bearer tok"})
        assert status == 302 and headers["Location"] == second_url
        assert seen["first_auth"] == "Bearer tok" and "auth" not in seen
        status, _, body = pr.urllib_http("GET", second_url, {})
        assert status == 200 and body == b"blob" and seen["auth"] is None
    finally:
        for s in (first, second):
            s.shutdown()
            s.server_close()


def test_delete_404_for_a_run_that_is_gone_counts_as_deleted(tmp_path):
    fake = Fake([run(1), run(2)], delete_status=404, gone_on_404=True)
    stats = go(fake, tmp_path)
    assert not stats["stopped"]
    assert stats["deleted"] == 2 and stats["archived"] == 2
    assert [u for m, u, _ in fake.calls if m == "GET" and u.endswith("/runs/1")]


def test_delete_404_but_run_still_listed_stops(tmp_path):
    fake = Fake([run(1)], delete_status=404, gone_on_404=False)
    stats = go(fake, tmp_path)
    assert stats["stopped"] and stats["deleted"] == 0


def test_progress_reported_per_run_with_total(tmp_path):
    lines = []
    fake = Fake([run(1), run(2), run(3)])
    go(fake, tmp_path, progress=lines.append)
    shown = [l for l in lines if l]
    assert len(shown) == 3
    assert shown[0].startswith("[1/3 ") and shown[-1].startswith("[3/3 100.0%]")
    assert "2026-01-01 wf" in shown[-1]
    assert lines[-1] is None  # line finished before the summary


def test_format_progress_rate_eta_and_size():
    stats = {"archived": 30, "expired": 10, "bytes": 3 * 1024 * 1024}
    line = pr.format_progress(40, 100, stats, elapsed=120, run={"created_at": "2026-11-02T17:48:03Z", "name": "Vinext"})
    assert line.startswith("[40/100  40.0%] archived 30 expired 10")
    assert "3.0 MB" in line and "20.0 runs/min" in line and "elapsed 2m00s" in line
    assert "ETA 3m00s" in line and line.endswith("2026-11-02 Vinext")
    assert "ETA ?" in pr.format_progress(0, None, stats, elapsed=0)


def test_terminal_progress_prints_every_nth_line_when_not_a_tty():
    out = io.StringIO()
    report = pr.terminal_progress(stream=out, every=2)
    for i in range(5):
        report(f"line {i}")
    report(None)
    assert out.getvalue() == "line 1\nline 3\n"


def dated(id_, created):
    r = run(id_)
    r["created_at"] = created
    return r


def test_parallel_workers_archive_everything_and_delete_in_order(tmp_path):
    fake = Fake([run(i) for i in range(1, 11)], logs={i: (200, zb(bytes([65 + i]))) for i in range(1, 11)})
    stats = go(fake, tmp_path, workers=4)
    assert not stats["stopped"]
    assert fake.deleted() == list(range(1, 11))
    for i in range(1, 11):
        assert (tmp_path / "arc" / f"{i}.zip").read_bytes() == zb(bytes([65 + i]))
    ids = [line.split("\t")[0] for line in (tmp_path / "arc" / "index.tsv").read_text().splitlines()]
    assert ids == [str(i) for i in range(1, 11)]


def test_parallel_failed_download_stops_before_deleting_that_run(tmp_path):
    fake = Fake([run(1), run(2), run(3), run(4)], logs={3: (200, b"")})
    stats = go(fake, tmp_path, workers=4)
    assert stats["stopped"]
    assert fake.deleted() == [1, 2]
    assert 3 not in fake.deleted() and 4 not in fake.deleted()


def test_runs_older_than_retention_are_expired_without_download(tmp_path):
    fake = Fake([dated(1, "2026-01-01T00:00:00Z"), dated(2, "2026-11-01T00:00:00Z")])
    stats = go(fake, tmp_path, expire_before="2026-08-20T00:00:00Z")
    assert fake.deleted() == [1, 2]
    assert [u for u in fake.log_fetches() if "/runs/1/" in u] == []
    assert [u for u in fake.log_fetches() if "/runs/2/" in u]
    assert stats["expired"] == 1 and stats["archived"] == 1
    assert "1\twf\tpush\t2026-01-01T00:00:00Z\tsuccess\texpired" in (tmp_path / "arc" / "index.tsv").read_text()


def test_deletes_are_paced_by_delete_interval(tmp_path):
    clock = {"t": 1000.0}
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        clock["t"] += s

    fake = Fake([run(1), run(2), run(3)])
    pr.purge(REPO, tmp_path / "arc", "tok", http=fake, sleep=sleep, now=lambda: clock["t"],
             out=lambda *_: None, delete_interval=1.0)
    assert fake.deleted() == [1, 2, 3]
    assert sleeps == [1.0, 1.0]  # no wait before the first delete


def test_expiry_cutoff_adds_margin():
    now = 1795132800  # 2026-11-20T00:00:00Z
    assert pr.expiry_cutoff(90, now) == "2026-08-20T00:00:00Z"


def test_repo_log_retention_days_reads_setting_and_falls_back():
    ok = lambda m, u, h=None: (200, {}, b'{"days": 30, "maximum_allowed_days": 90}')
    bad = lambda m, u, h=None: (403, {}, b"{}")
    assert pr.repo_log_retention_days("o/r", "tok", http=ok) == 30
    assert pr.repo_log_retention_days("o/r", "tok", http=bad) == 90
