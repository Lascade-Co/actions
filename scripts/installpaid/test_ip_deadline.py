"""Absolute fetch deadlines, exercised against a local synthetic HTTP server."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import datetime as dt
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import signal
import threading
import time

import pytest


@contextmanager
def trickling_server(phase):
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, value, trickle=False):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                if trickle:
                    size = max(1, (len(body) + 19) // 20)
                    for start in range(0, len(body), size):
                        self.wfile.write(body[start:start + size])
                        self.wfile.flush()
                        time.sleep(0.04)
                else:
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            paths.append("submit")
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if phase == "submit":
                self.reply(200, {"columns": [], "results": [], "private": "synthetic private response" * 3}, True)
            else:
                self.reply(202, {"query_status": {"id": "synthetic"}})

        def do_GET(self):
            paths.append("poll")
            self.reply(200, {"query_status": {"complete": True, "results": {"columns": [], "results": []}},
                             "private": "synthetic private response" * 3}, True)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", paths
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


class TestAbsoluteDeadline:
    @pytest.mark.parametrize("phase", ["submit", "poll"])
    def test_trickling_response_cannot_extend_absolute_deadline(self, monkeypatch, capsys, phase):
        import ip_fetch as fetch
        before_handler = signal.getsignal(signal.SIGALRM)
        with trickling_server(phase) as (host, paths):
            monkeypatch.setattr(fetch, "HOST", host)
            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=0.2)
            started = time.monotonic()
            with pytest.raises(fetch.Unavailable) as caught:
                fetch.make_runner("synthetic", retries=1, poll_s=0, deadline=deadline)("synthetic query")
            elapsed = time.monotonic() - started
            assert elapsed < 0.65
            assert paths == (["submit"] if phase == "submit" else ["submit", "poll"])
        assert str(caught.value) == "absolute build deadline reached"
        captured = capsys.readouterr()
        assert "synthetic private response" not in captured.out + captured.err + str(caught.value)
        assert signal.getsignal(signal.SIGALRM) == before_handler
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)

    def test_success_restores_previous_alarm_handler_and_cancels_timer(self, monkeypatch):
        import ip_fetch as fetch
        before_handler = signal.getsignal(signal.SIGALRM)

        def previous_handler(*_):
            raise AssertionError("unexpected previous alarm")

        signal.signal(signal.SIGALRM, previous_handler)
        try:
            monkeypatch.setattr(fetch.requests, "post", lambda *a, **k: type("Response", (), {
                "status_code": 200, "json": lambda self: {"columns": [], "results": []}})())
            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=5)
            assert fetch.make_runner("synthetic", deadline=deadline)("synthetic query") == []
            assert signal.getsignal(signal.SIGALRM) is previous_handler
            assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, before_handler)

    def test_existing_alarm_is_not_replaced(self, monkeypatch):
        import ip_fetch as fetch
        before_handler = signal.getsignal(signal.SIGALRM)
        calls = []
        monkeypatch.setattr(fetch.requests, "post", lambda *a, **k: calls.append(True) or type("Response", (), {
            "status_code": 200, "json": lambda self: {"columns": [], "results": []}})())

        def previous_handler(*_):
            raise AssertionError("unexpected previous alarm")

        signal.signal(signal.SIGALRM, previous_handler)
        signal.setitimer(signal.ITIMER_REAL, 5)
        try:
            remaining = signal.getitimer(signal.ITIMER_REAL)[0]
            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1)
            with pytest.raises(fetch.Unavailable, match="cannot be enforced"):
                fetch.make_runner("synthetic", deadline=deadline)("synthetic query")
            assert calls == []
            assert signal.getsignal(signal.SIGALRM) is previous_handler
            assert remaining - 0.5 < signal.getitimer(signal.ITIMER_REAL)[0] <= remaining
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, before_handler)

    def test_deadline_in_worker_thread_fails_before_request(self, monkeypatch):
        import ip_fetch as fetch
        calls = []
        monkeypatch.setattr(fetch.requests, "post", lambda *a, **k: calls.append(True) or type("Response", (), {
            "status_code": 200, "json": lambda self: {"columns": [], "results": []}})())
        deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with pytest.raises(fetch.Unavailable, match="cannot be enforced"):
                workers.submit(fetch.make_runner("synthetic", deadline=deadline), "synthetic query").result(timeout=2)
        assert calls == []

    def test_no_deadline_still_works_in_worker_thread(self, monkeypatch):
        import ip_fetch as fetch
        monkeypatch.setattr(fetch.requests, "post", lambda *a, **k: type("Response", (), {
            "status_code": 200, "json": lambda self: {"columns": [], "results": []}})())
        with ThreadPoolExecutor(max_workers=1) as workers:
            assert workers.submit(fetch.make_runner("synthetic"), "synthetic query").result(timeout=2) == []

    def test_blocked_alarm_fails_before_request_and_preserves_signal_mask(self, monkeypatch):
        import ip_fetch as fetch
        calls = []
        monkeypatch.setattr(fetch.requests, "post", lambda *a, **k: calls.append(True) or type("Response", (), {
            "status_code": 200, "json": lambda self: {"columns": [], "results": []}})())
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        try:
            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1)
            with pytest.raises(fetch.Unavailable, match="cannot be enforced"):
                fetch.make_runner("synthetic", deadline=deadline)("synthetic query")
            assert calls == []
            assert signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, set())
            assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
