import asyncio
import json
from dataclasses import replace

import anyio
import httpx2
import pytest

from task3_gateway.mock_server import frame
from task3_gateway.protocol import DONE_EVENT, ERROR_EVENT, MAX_LINE_CHARS
from task3_gateway.server import Config, GatewayResponse, GenerationRequest, MAX_BODY_BYTES, create_app, load_config


CONFIG = Config("http://127.0.0.1:9999/v1/chat/completions")
PAYLOAD = {"model": "demo-model", "messages": [{"role": "user", "content": "hello"}]}
FINISH = frame(finish="stop") + b"data: [DONE]\n\n"


class TrackedStream(httpx2.AsyncByteStream):
    def __init__(self, chunks=(), *, error=None, stall=False):
        self.chunks = chunks
        self.error = error
        self.stall = stall
        self.closed = False
        self.close_calls = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.error is not None:
            raise self.error
        if self.stall:
            await anyio.sleep_forever()

    async def aclose(self):
        self.close_calls += 1
        await anyio.sleep(0)
        self.closed = True


class Provider(httpx2.AsyncBaseTransport):
    def __init__(self, chunks=(), *, status=200, headers=None, error=None, stall=False, stream_error=None):
        self.requests = []
        self.status = status
        self.headers = {"Content-Type": "text/event-stream", **(headers or {})}
        self.error = error
        self.stall = stall
        self.stream = TrackedStream(chunks, error=stream_error)
        self.closed = False

    async def handle_async_request(self, request):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        if self.stall:
            await anyio.sleep_forever()
        return httpx2.Response(self.status, stream=self.stream, headers=self.headers)

    async def aclose(self):
        await anyio.sleep(0)
        self.closed = True


def request(provider, payload=PAYLOAD, *, config=CONFIG, headers=None):
    async def run():
        app = create_app(config, transport=provider)
        outgoing_headers = {"Content-Type": "application/json", **(headers or {})}
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app), base_url="http://gateway") as client:
            return await client.post("/generate", content=raw, headers=outgoing_headers)
    return asyncio.run(run())


def text(response):
    event = None
    result = ""
    for line in response.text.splitlines():
        if line.startswith("event: "):
            event = line[7:]
        elif line.startswith("data: ") and event == "text":
            result += json.loads(line[6:])["text"]
    return result


def test_request_and_headers_are_owned_by_gateway():
    provider = Provider([frame("Hi alice@example.com! "), FINISH], headers={"Set-Cookie": "private=secret", "X-Private": "secret"})
    response = request(provider, config=replace(CONFIG, upstream_bearer="provider-only-secret"), headers={
        "Authorization": "Bearer caller-secret", "Cookie": "caller-cookie=secret", "X-Role": "admin",
    })
    assert response.status_code == 200
    assert text(response) == "Hi [REDACTED]! "
    assert response.content.endswith(DONE_EVENT)
    assert "secret" not in response.text and "metadata" not in response.text
    assert "set-cookie" not in response.headers and "x-private" not in response.headers
    sent = provider.requests[0]
    assert str(sent.url) == CONFIG.upstream_url
    assert json.loads(sent.content) == {**PAYLOAD, "stream": True, "n": 1}
    assert sent.headers["authorization"] == "Bearer provider-only-secret"
    assert sent.headers["accept-encoding"] == "identity"
    assert "cookie" not in sent.headers and "x-role" not in sent.headers
    assert provider.closed and provider.stream.closed


@pytest.mark.parametrize("secret", ["alice@example.com", "123-45-6789", "123456789", "4111 1111 1111 1111"])
def test_pii_every_content_delta_split_over_gateway(secret):
    for split in range(len(secret) + 1):
        provider = Provider([frame("Safe. " + secret[:split]), frame(secret[split:] + "! "), FINISH])
        response = request(provider)
        assert text(response) == "Safe. [REDACTED]! "
        assert response.content.endswith(DONE_EVENT)


def test_one_byte_network_and_one_character_content_deltas():
    source = "Café ☕ alice@example.com, 123-45-6789, 4111-1111-1111-1111."
    wire = b"".join(frame(char) for char in source) + FINISH
    provider = Provider(bytes([byte]) for byte in wire)
    response = request(provider)
    assert text(response) == "Café ☕ [REDACTED], [REDACTED], [REDACTED]."
    assert response.content.endswith(DONE_EVENT)


@pytest.mark.parametrize("ending", [
    b"", frame(finish="stop"), b"data: {private", b"data: \xff\n\n",
    b'data: {"choices":[],"choices":[]}\n\n', b"event: error\ndata: private\n\n",
    frame("", finish="tool_calls"),
    b'data: {"choices":[{"index":0,"delta":{"content":"private","tool_calls":[]}}]}\n\n',
    b"data: " + b"x" * MAX_LINE_CHARS,
])
def test_errors_discard_uncertain_tail_and_never_copy_upstream_payload(ending):
    provider = Provider([frame("Safe. alice@"), ending])
    response = request(provider)
    assert response.status_code == 200
    assert text(response) == "Safe. "
    assert response.content.endswith(ERROR_EVENT)
    assert "alice" not in response.text and "private" not in response.text
    assert DONE_EVENT not in response.content
    assert provider.stream.closed and provider.closed


@pytest.mark.parametrize("candidate", ["a" * 321, "a" * 65 + "@example.com", "a@" + "b" * 64 + ".com"])
def test_overlong_candidates_error_without_leaking_candidate(candidate):
    response = request(Provider([frame("Safe. " + candidate + ", "), FINISH]))
    assert text(response) in {"", "Safe. "}
    assert candidate not in response.text
    assert response.content.endswith(ERROR_EVENT)


@pytest.mark.parametrize("status", [201, 302, 400, 401, 429, 500])
def test_upstream_http_failures_and_redirects_are_generic_and_not_retried(status):
    provider = Provider([b"private-provider-error"], status=status, headers={"Location": "https://private.example/secret"})
    response = request(provider)
    assert response.status_code == 502 and response.content == ERROR_EVENT
    assert len(provider.requests) == 1 and provider.stream.closed and provider.closed


@pytest.mark.parametrize("headers", [{"Content-Type": "application/json"}, {"Content-Encoding": "gzip"}, {"Content-Encoding": "br"}])
def test_unexpected_response_type_or_compression_is_rejected_before_read(headers):
    provider = Provider([b"private"], headers=headers)
    response = request(provider)
    assert response.status_code == 502 and response.content == ERROR_EVENT
    assert provider.stream.closed


@pytest.mark.parametrize("error,status", [
    (httpx2.ConnectError("private-host secret"), 502),
    (httpx2.ConnectTimeout("private-token"), 504),
])
def test_before_start_network_errors_are_sanitized(error, status):
    provider = Provider(error=error)
    response = request(provider)
    assert response.status_code == status and response.content == ERROR_EVENT
    assert provider.closed and len(provider.requests) == 1


@pytest.mark.parametrize("error", [httpx2.ReadError("private-provider"), httpx2.ReadTimeout("private-token")])
def test_after_start_network_errors_preserve_only_safe_prefix(error):
    provider = Provider([frame("Safe. alice@")], stream_error=error)
    response = request(provider)
    assert response.status_code == 200 and text(response) == "Safe. "
    assert response.content.endswith(ERROR_EVENT)
    assert provider.stream.closed and provider.closed


@pytest.mark.parametrize("stage", ["headers", "before_text", "after_text"])
def test_idle_timeout_closes_before_and_after_response_start(stage):
    provider = Provider(stall=stage == "headers")
    if stage != "headers":
        provider.stream = TrackedStream([frame("Safe. alice@")] if stage == "after_text" else [], stall=True)
    response = request(provider, config=replace(CONFIG, idle_timeout_seconds=0.02))
    assert response.status_code == (504 if stage == "headers" else 200)
    assert text(response) == ("Safe. " if stage == "after_text" else "")
    assert response.content.endswith(ERROR_EVENT)
    assert provider.closed and (stage == "headers" or provider.stream.closed)


@pytest.mark.parametrize("payload,status", [
    ({}, 400), ({**PAYLOAD, "stream": False}, 400), ({**PAYLOAD, "n": 2}, 400),
    ({**PAYLOAD, "model": ""}, 400), ({**PAYLOAD, "model": True}, 400),
    ({**PAYLOAD, "messages": []}, 400), ({**PAYLOAD, "messages": [{"role": "tool", "content": "secret"}]}, 400),
    ({**PAYLOAD, "messages": [{"role": "user", "content": ["secret"]}]}, 400),
    ({**PAYLOAD, "messages": [{"role": "user", "content": "x" * 16385}]}, 400),
    ({**PAYLOAD, "messages": [{"role": "user", "content": "\ud800"}]}, 400),
    (b'{"model":"a","model":"b","messages":[]}', 400),
    (b'{"secret":1e999}', 400), (b'{"secret":NaN}', 400), (b'\xff', 400),
    (b"[1,2]", 400), (b"x" * (MAX_BODY_BYTES + 1), 413),
])
def test_bad_requests_never_contact_upstream(payload, status):
    provider = Provider()
    response = request(provider, payload)
    assert response.status_code == status
    assert provider.requests == []
    assert "secret" not in response.text


def test_content_type_and_body_limits():
    provider = Provider()
    assert request(provider, headers={"Content-Type": "text/plain"}).status_code == 415
    assert request(provider, b" " * MAX_BODY_BYTES).status_code == 400
    assert provider.requests == []


@pytest.mark.parametrize("url", [
    "http://example.com/v1", "ftp://127.0.0.1/a", "https://u:p@example.com/a", "https://example.com/a#secret",
    "https://example.com:0/a", "https://example.com:99999/a", "https://example.com/a\n", "not a url",
])
def test_unsafe_configuration_is_rejected(url):
    with pytest.raises(ValueError):
        Config(url)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True, "1"])
def test_positive_finite_timeout_required(timeout):
    with pytest.raises(ValueError):
        replace(CONFIG, idle_timeout_seconds=timeout)


def test_config_loading_and_optional_bearer():
    assert load_config({"LLM_UPSTREAM_URL": "https://api.example/v1/chat/completions"}).upstream_bearer is None
    assert Config("http://[::1]:8000/a").upstream_url.endswith("/a")
    with pytest.raises(ValueError, match="Missing"):
        load_config({})
    with pytest.raises(ValueError):
        replace(CONFIG, upstream_bearer="secret\r\nheader")


@pytest.mark.parametrize("trigger", ["send_start", "send_body", "disconnect_before_headers", "disconnect_after_text", "cancel_before_headers", "cancel_after_text"])
def test_response_owner_closes_upstream_and_client_on_send_failure_disconnect_and_cancellation(trigger):
    async def run():
        provider = Provider(stall=trigger.endswith("before_headers"))
        if not provider.stall:
            provider.stream = TrackedStream([frame("Safe. alice@")], stall=True)
        owner = GatewayResponse(replace(CONFIG, idle_timeout_seconds=30), GenerationRequest.model_validate(PAYLOAD), provider)
        disconnect = anyio.Event()
        reached = anyio.Event()

        async def receive():
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.start" and trigger == "send_start":
                raise OSError("client gone")
            if message["type"] == "http.response.body" and message.get("body", b"").startswith(b"event: text"):
                reached.set()
                if trigger == "send_body":
                    raise OSError("client gone")
                if trigger == "disconnect_after_text":
                    disconnect.set()

        async with anyio.create_task_group() as group:
            group.start_soon(owner, {"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
            if trigger.endswith("before_headers"):
                while not provider.requests:
                    await anyio.sleep(0)
                if trigger.startswith("disconnect"):
                    disconnect.set()
                else:
                    group.cancel_scope.cancel()
            elif trigger == "cancel_after_text":
                await reached.wait()
                group.cancel_scope.cancel()
        assert owner.client is not None and owner.client.is_closed
        assert provider.closed
        assert owner.redactor.retained_chars == 0
        if not provider.stall:
            assert owner.upstream.is_closed and provider.stream.closed
    asyncio.run(run())
