"""Exercise the real SDK against a local HTTP transport, without credentials or network."""

import json

import anthropic
import httpx2
import pytest

import claude_api as api


def response(text='{"ok":true}', stop="end_turn"):
    events = [
        {"type": "message_start", "message": {
            "id": "msg_test", "type": "message", "role": "assistant", "model": api.DEFAULT_MODEL,
            "content": [], "stop_reason": None, "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 0},
        }},
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}},
        {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None},
         "usage": {"output_tokens": 1}},
        {"type": "message_stop"},
    ]
    body = "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events)
    return httpx2.Response(200, headers={"content-type": "text/event-stream"}, text=body)


@pytest.fixture
def transport(monkeypatch):
    original = anthropic.Anthropic
    requests = []
    settings = []
    replies = [response()]

    def handle(request):
        requests.append(request)
        reply = replies.pop(0) if len(replies) > 1 else replies[0]
        if isinstance(reply, Exception):
            raise reply
        return reply

    def factory(**kwargs):
        settings.append(kwargs)
        return original(http_client=httpx2.Client(transport=httpx2.MockTransport(handle)), **kwargs)

    monkeypatch.setattr(anthropic, "Anthropic", factory)
    monkeypatch.setenv("CLAUDE_API_KEY", "synthetic-api-key")
    return requests, settings, replies


def test_key_authentication_and_only_explicit_content_sent(transport, monkeypatch):
    requests, settings, _ = transport
    monkeypatch.setenv("CMS_APP_PASSWORD", "unrelated-secret")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://unrelated.invalid")
    assert api.generate_json("Only this prompt", max_tokens=2048, timeout=90) == {"ok": True}
    request = requests[0]
    assert str(request.url) == "https://api.anthropic.com/v1/messages"
    assert request.headers["x-api-key"] == "synthetic-api-key"
    body = json.loads(request.content)
    assert body["model"] == api.DEFAULT_MODEL
    assert body["messages"] == [{"role": "user", "content": "Only this prompt"}]
    assert body["max_tokens"] == 2048 and body["stream"] is True
    assert "unrelated-secret" not in request.content.decode()
    assert settings[0]["timeout"] == 90 and settings[0]["max_retries"] == 2


def test_sonnet_model_ignores_ambient_override(transport, monkeypatch):
    monkeypatch.setenv("CLAUDE_MODEL", "test-model")
    api.generate_text("prompt")
    assert json.loads(transport[0][0].content)["model"] == "claude-sonnet-5-5"


@pytest.mark.parametrize("key", ["", " \n\t"])
def test_missing_key_never_sends_a_request(transport, monkeypatch, key):
    monkeypatch.setenv("CLAUDE_API_KEY", key)
    with pytest.raises(api.ClaudeError, match="auth"):
        api.generate_text("prompt")
    assert transport[0] == []


@pytest.mark.parametrize("status,reason", [(401, "auth"), (403, "auth"), (429, "rate-limit"), (503, "request-failed:http-503"), (404, "request-failed:http-404")])
def test_http_errors_do_not_expose_response_bodies(transport, status, reason, capsys):
    _, _, replies = transport
    replies[:] = [httpx2.Response(status, headers={"retry-after-ms": "1"},
                                 json={"error": {"type": "api_error", "message": "private prompt and key"}})]
    with pytest.raises(api.ClaudeError) as error:
        api.generate_text("prompt")
    assert error.value.reason == reason
    assert "private" not in str(error.value)
    assert capsys.readouterr() == ("", "")


def test_transient_rate_limit_is_retried(transport):
    requests, _, replies = transport
    replies[:] = [httpx2.Response(429, headers={"retry-after-ms": "1"}, json={"error": "busy"}), response("done")]
    assert api.generate_text("prompt") == "done"
    assert len(requests) == 2


def test_timeout_has_a_safe_reason(transport):
    transport[2][:] = [httpx2.ReadTimeout("private request details")]
    with pytest.raises(api.ClaudeError, match="timeout"):
        api.generate_text("prompt")


@pytest.mark.parametrize("stop", ["max_tokens", "refusal", "tool_use"])
def test_incomplete_or_refused_output_is_rejected(transport, stop):
    transport[2][:] = [response('{"partial":', stop)]
    with pytest.raises(api.ClaudeError, match=f"^Claude API: incomplete-response:{stop}$"):
        api.generate_text("prompt")


def test_unknown_stop_reason_is_not_logged_verbatim(transport):
    transport[2][:] = [response('{"partial":', "PRIVATE_PROSE")]
    with pytest.raises(api.ClaudeError, match="^Claude API: incomplete-response:other$"):
        api.generate_text("prompt")


def test_empty_output_is_rejected(transport):
    transport[2][:] = [response("  ")]
    with pytest.raises(api.ClaudeError, match="empty-response"):
        api.generate_text("prompt")


@pytest.mark.parametrize("text", ['{"ok":true}', '```json\n{"ok":true}\n```'])
def test_json_objects_and_single_fences(transport, text):
    transport[2][:] = [response(text)]
    assert api.generate_json("prompt") == {"ok": True}


@pytest.mark.parametrize("text", ["[]", "null", "bad", '{"truncated":', 'Here it is: {"ok":true}'])
def test_bad_json_and_wrong_shapes_are_rejected(transport, text):
    transport[2][:] = [response(text)]
    with pytest.raises(api.ClaudeError, match="invalid-json"):
        api.generate_json("prompt")



def test_cli_runner_sends_the_prompt_on_stdin_and_hides_stderr(monkeypatch, capsys):
    import subprocess
    seen = {}

    class Done:
        returncode, stdout, stderr = 0, " answer \n", "private stderr"

    def fake_run(argv, **kw):
        seen.update(argv=argv, **kw)
        return Done()
    monkeypatch.setattr(subprocess, "run", fake_run)
    assert api.generate_text_cli("the prompt", timeout=7) == "answer"
    assert seen["input"] == "the prompt" and seen["timeout"] == 7 and seen["capture_output"]
    argv = seen["argv"]
    assert argv[:2] == ["claude", "-p"] and "--safe-mode" in argv and "--bare" not in argv
    assert argv[argv.index("--tools") + 1] == "" and "--strict-mcp-config" in argv
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("outcome,reason", [("exit", "cli-failed"), ("timeout", "timeout"),
                                            ("missing", "cli-failed"), ("blank", "empty-response")])
def test_cli_runner_failures_have_safe_reasons(monkeypatch, outcome, reason):
    import subprocess

    class Done:
        returncode = 1 if outcome == "exit" else 0
        stdout = "  " if outcome == "blank" else "x"
        stderr = "private"

    def fake_run(argv, **kw):
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(argv, 1, output="private")
        if outcome == "missing":
            raise FileNotFoundError("private")
        return Done()
    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(api.ClaudeError) as error:
        api.generate_text_cli("prompt")
    assert error.value.reason == reason and "private" not in str(error.value)
