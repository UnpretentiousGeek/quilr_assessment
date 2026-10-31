import json
import threading
import time

import anyio
import httpx2
import pytest

from task4_router.limiter import LimiterUnavailable, RateLimited, SQLiteLimiter, key_bucket
from task4_router.router import CompletionRequest, GatewayError, MAX_PROVIDER_BYTES, Provider, Router, strict_json


BUCKET = key_bucket("a" * 32)
REQUEST = {"messages": [{"role": "user", "content": "hello"}], "max_tokens": 8}


def payload(*, prompt=12, completion=4, text="safe text"):
    return {
        "id": "private-id", "model": "private-model", "stack": "private metadata",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion},
    }


class Bytes(httpx2.AsyncByteStream):
    def __init__(self, data, *, on_body=None):
        self.data, self.on_body, self.closed = data, on_body, False

    async def __aiter__(self):
        if self.on_body is not None:
            self.on_body()
        yield self.data

    async def aclose(self):
        self.closed = True


def response(data=None, *, status=200, headers=None, stream=None):
    return httpx2.Response(status, stream=stream or Bytes(json.dumps(data if data is not None else payload()).encode()),
                           headers={"Content-Type": "application/json", **(headers or {})})


async def router_for(tmp_path, handler, *, limit=100, clock=None, limiter=None):
    limiter = limiter or SQLiteLimiter(tmp_path / "ledger.db")
    await limiter.initialize()
    router = Router(limiter, Provider("https://primary.test/complete", "fixed-primary", "primary-secret"),
                    Provider("https://secondary.test/complete", "fixed-secondary", "secondary-secret"),
                    transport=httpx2.MockTransport(handler), **({"monotonic": clock} if clock else {}))
    return router, limiter, CompletionRequest.model_validate(REQUEST)


def test_primary_success_reconciles_and_owns_wire_shape(tmp_path):
    async def check():
        requests = []
        def handler(request):
            requests.append(request)
            return response(headers={"X-Private": "secret", "Set-Cookie": "secret"})
        router, limiter, generation = await router_for(tmp_path, handler)
        result = await router.complete(BUCKET, 50, generation)
        assert result.payload() == {"text": "safe text", "usage": {"prompt_tokens": 12, "completion_tokens": 4, "total_tokens": 16}, "provider": "primary"}
        assert await limiter.used(BUCKET) == 16
        assert len(requests) == 1
        sent = requests[0]
        assert sent.headers["Authorization"] == "Bearer primary-secret"
        assert sent.headers["Accept-Encoding"] == "identity"
        assert sent.extensions["timeout"] == {"connect": None, "read": None, "write": None, "pool": None}
        assert json.loads(sent.content) == {**REQUEST, "model": "fixed-primary", "stream": False, "n": 1}
    anyio.run(check)


def test_exact_primary429_falls_back_and_charges_both_attempts(tmp_path):
    async def check():
        requests, streams = [], []
        def handler(request):
            if request.url.host == "secondary.test":
                assert streams[0].closed
            requests.append(request)
            stream = Bytes(json.dumps(payload()).encode())
            streams.append(stream)
            return response(status=429 if len(requests) == 1 else 200, stream=stream)
        router, limiter, generation = await router_for(tmp_path, handler)
        result = await router.complete(BUCKET, 100, generation)
        assert result.provider == "secondary"
        assert [item.url.host for item in requests] == ["primary.test", "secondary.test"]
        assert requests[1].headers["Authorization"] == "Bearer secondary-secret"
        assert await limiter.used(BUCKET) == generation.reservation() + 16
    anyio.run(check)


@pytest.mark.parametrize("status", [201, 301, 302, 400, 401, 403, 404, 408, 422, 500, 503, 504])
def test_other_primary_status_never_falls_back(tmp_path, status):
    async def check():
        calls = []
        def handler(request):
            calls.append(request)
            return response(status=status, headers={"Location": "https://secondary.test/complete"})
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 502
        assert len(calls) == 1
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


@pytest.mark.parametrize("exception", [httpx2.ConnectError, httpx2.ReadError, httpx2.ReadTimeout, TimeoutError])
def test_transport_failure_or_early_transport_timeout_never_falls_back(tmp_path, exception):
    async def check():
        calls = []
        def handler(request):
            calls.append(request)
            raise exception("private upstream secret")
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 502 and "private" not in caught.value.message
        assert len(calls) == 1
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


def test_local_rate_denial_and_backup_reservation_denial_make_no_extra_call(tmp_path):
    async def check():
        calls = []
        def handler(request):
            calls.append(request)
            return response(status=429)
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(RateLimited):
            await router.complete(BUCKET, 24, generation)
        assert not calls
        with pytest.raises(RateLimited):
            await router.complete(BUCKET, 40, generation)
        assert len(calls) == 1
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


@pytest.mark.parametrize("second_status", [429, 500, 302])
def test_backup_status_failure_has_no_retry(tmp_path, second_status):
    async def check():
        calls = []
        def handler(request):
            calls.append(request)
            return response(status=429 if len(calls) == 1 else second_status)
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 502
        assert len(calls) == 2
        assert await limiter.used(BUCKET) == generation.reservation() * 2
    anyio.run(check)


@pytest.mark.parametrize("finished,expected", [(2.999999, "primary"), (3.0, "secondary"), (3.000001, "secondary")])
def test_strict_deadline_before_at_and_after_body_completion(tmp_path, finished, expected):
    async def check():
        clock, calls = [0.0], []
        def handler(request):
            calls.append(request)
            if request.url.host == "primary.test":
                return response(stream=Bytes(json.dumps(payload()).encode(), on_body=lambda: clock.__setitem__(0, finished)))
            return response()
        router, limiter, generation = await router_for(tmp_path, handler, clock=lambda: clock[0])
        result = await router.complete(BUCKET, 100, generation)
        assert result.provider == expected
        assert len(calls) == (1 if expected == "primary" else 2)
        assert await limiter.used(BUCKET) == (16 if expected == "primary" else generation.reservation() + 16)
    anyio.run(check)


@pytest.mark.parametrize("headers", [{"Content-Type": "text/plain"}, {"Content-Encoding": "gzip"}])
def test_unsupported_response_headers_retain_reservation(tmp_path, headers):
    async def check():
        router, limiter, generation = await router_for(tmp_path, lambda request: response(headers=headers))
        with pytest.raises(GatewayError):
            await router.complete(BUCKET, 100, generation)
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


@pytest.mark.parametrize("raw", [b"not json", b'{"choices":[],"choices":[]}', b'{"n":NaN}', b'{"n":1e400}', b'"\\ud800"', b"\xff", b"[" * 2000])
def test_malformed_provider_json_is_sanitized_without_refund(tmp_path, raw):
    async def check():
        calls = []
        def handler(request):
            calls.append(request)
            return response(stream=Bytes(raw))
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 502 and len(calls) == 1
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


@pytest.mark.parametrize("usage", [None, {}, {"prompt_tokens": True, "completion_tokens": 1, "total_tokens": 2},
                                   {"prompt_tokens": -1, "completion_tokens": 1, "total_tokens": 0},
                                   {"prompt_tokens": 1, "completion_tokens": 9, "total_tokens": 10},
                                   {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 3},
                                   {"prompt_tokens": 1e100, "completion_tokens": 0, "total_tokens": 1e100},
                                   {"prompt_tokens": 10**40, "completion_tokens": 0, "total_tokens": 10**40}])
def test_invalid_usage_keeps_full_charge(tmp_path, usage):
    async def check():
        data = payload()
        data["usage"] = usage
        router, limiter, generation = await router_for(tmp_path, lambda request: response(data))
        with pytest.raises(GatewayError):
            await router.complete(BUCKET, 100, generation)
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


def test_overage_records_actual_debt_and_blocks_later_admission(tmp_path):
    async def check():
        clock, calls = [100_000], []
        limiter = SQLiteLimiter(tmp_path / "ledger.db", clock_ms=lambda: clock[0])
        def handler(request):
            calls.append(request)
            return response(payload(prompt=101, completion=4))
        router, limiter, generation = await router_for(tmp_path, handler, limiter=limiter)
        with pytest.raises(GatewayError):
            await router.complete(BUCKET, 100, generation)
        assert await limiter.used(BUCKET) == 105
        with pytest.raises(RateLimited):
            await router.complete(BUCKET, 100, generation)
        assert len(calls) == 1
        clock[0] += 60_000
        assert await limiter.used(BUCKET) == 0
    anyio.run(check)


def test_provider_body_is_bounded_and_closed(tmp_path):
    async def check():
        stream = Bytes(b"x" * (MAX_PROVIDER_BYTES + 1))
        router, limiter, generation = await router_for(tmp_path, lambda request: response(stream=stream))
        with pytest.raises(GatewayError):
            await router.complete(BUCKET, 100, generation)
        assert stream.closed
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


class Stalled(httpx2.AsyncByteStream):
    def __init__(self):
        self.started, self.closed, self.iteration_finished = anyio.Event(), False, False

    async def __aiter__(self):
        try:
            self.started.set()
            yield b"{"
            await anyio.sleep_forever()
        finally:
            self.iteration_finished = True

    async def aclose(self):
        await anyio.lowlevel.checkpoint()
        self.closed = True


def test_cancellation_closes_primary_stream_and_retains_charge_without_backup(tmp_path):
    async def check():
        stream, calls, scopes = Stalled(), [], []
        def handler(request):
            calls.append(request)
            return response(stream=stream)
        router, limiter, generation = await router_for(tmp_path, handler)
        async def run():
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await router.complete(BUCKET, 100, generation)
        async with anyio.create_task_group() as group:
            group.start_soon(run)
            await stream.started.wait()
            scopes[0].cancel()
        assert stream.closed and stream.iteration_finished
        assert len(calls) == 1
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


def test_real_three_second_body_timeout_then_backup(tmp_path):
    async def check():
        stream, calls = Stalled(), []
        def handler(request):
            calls.append(request)
            if request.url.host == "primary.test":
                return response(stream=stream)
            assert stream.closed and stream.iteration_finished
            return response()
        router, limiter, generation = await router_for(tmp_path, handler)
        started = time.monotonic()
        result = await router.complete(BUCKET, 100, generation)
        elapsed = time.monotonic() - started
        assert result.provider == "secondary" and 3.0 <= elapsed < 5
        assert len(calls) == 2
        assert await limiter.used(BUCKET) == generation.reservation() + 16
    anyio.run(check)


def test_real_three_second_secondary_timeout_is504_without_retry(tmp_path):
    async def check():
        stream, calls = Stalled(), []
        def handler(request):
            calls.append(request)
            return response(status=429) if len(calls) == 1 else response(stream=stream)
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 504
        assert len(calls) == 2 and stream.closed
        assert await limiter.used(BUCKET) == generation.reservation() * 2
    anyio.run(check)


@pytest.mark.parametrize("hang", [False, True])
def test_primary_cleanup_failure_suppresses_fallback(tmp_path, hang):
    async def check():
        calls = []
        class Broken(Bytes):
            async def aclose(self):
                if hang:
                    await anyio.sleep_forever()
                raise RuntimeError("private close detail")
        def handler(request):
            calls.append(request)
            return response(status=429, stream=Broken(b"private"))
        router, limiter, generation = await router_for(tmp_path, handler)
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 502 and "private" not in caught.value.message
        assert len(calls) == 1
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


def test_ledger_failure_makes_zero_network_calls(tmp_path):
    async def check():
        calls = []
        router, limiter, generation = await router_for(tmp_path, lambda request: calls.append(request))
        (tmp_path / "ledger.db").unlink()
        with pytest.raises(LimiterUnavailable):
            await router.complete(BUCKET, 100, generation)
        assert not calls
    anyio.run(check)


@pytest.mark.parametrize("url", ["http://example.com/complete", "file:///etc/passwd", "https://user:secret@host/", "https://host/#fragment", "https://host/\n"])
def test_provider_config_rejects_unsafe_url(url):
    with pytest.raises(ValueError):
        Provider(url, "model")


def test_utf8_reservation_counts_bytes_and_explicit_overhead():
    generation = CompletionRequest.model_validate({"messages": [{"role": "user", "content": "🙂"}], "max_tokens": 8})
    assert generation.reservation() == 4 + 8 + 4 + 8


def test_strict_json_rejects_surrogate_object_key():
    with pytest.raises(UnicodeError):
        strict_json(b'{"\\ud800":0}')


@pytest.mark.parametrize("finished,expected", [(2.999999, "error"), (3.0, "secondary"), (3.000001, "secondary")])
def test_total_deadline_precedes_malformed_primary_body(tmp_path, finished, expected):
    async def check():
        clock, calls = [0.0], []
        def handler(request):
            calls.append(request)
            if request.url.host == "primary.test":
                return response(stream=Bytes(b"malformed", on_body=lambda: clock.__setitem__(0, finished)))
            return response()
        router, limiter, generation = await router_for(tmp_path, handler, clock=lambda: clock[0])
        if expected == "error":
            with pytest.raises(GatewayError) as caught:
                await router.complete(BUCKET, 100, generation)
            assert caught.value.status == 502 and len(calls) == 1
            assert await limiter.used(BUCKET) == generation.reservation()
        else:
            result = await router.complete(BUCKET, 100, generation)
            assert result.provider == "secondary" and len(calls) == 2
            assert await limiter.used(BUCKET) == generation.reservation() + 16
    anyio.run(check)


def test_parse_failure_crossing_total_deadline_falls_back(tmp_path, monkeypatch):
    import task4_router.router as module
    async def check():
        clock, calls = [0.0], []
        original = module.parse_completion
        def parse(raw, generation, label):
            if label == "primary":
                clock[0] = 3.0
                raise ValueError("private parsing failure")
            return original(raw, generation, label)
        monkeypatch.setattr(module, "parse_completion", parse)
        def handler(request):
            calls.append(request)
            return response()
        router, limiter, generation = await router_for(tmp_path, handler, clock=lambda: clock[0])
        result = await router.complete(BUCKET, 100, generation)
        assert result.provider == "secondary" and len(calls) == 2
        assert await limiter.used(BUCKET) == generation.reservation() + 16
    anyio.run(check)


def test_secondary_deadline_precedes_malformed_body(tmp_path):
    async def check():
        clock, calls = [0.0], []
        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return response(status=429)
            return response(stream=Bytes(b"malformed", on_body=lambda: clock.__setitem__(0, 3.0)))
        router, limiter, generation = await router_for(tmp_path, handler, clock=lambda: clock[0])
        with pytest.raises(GatewayError) as caught:
            await router.complete(BUCKET, 100, generation)
        assert caught.value.status == 504 and len(calls) == 2
        assert await limiter.used(BUCKET) == generation.reservation() * 2
    anyio.run(check)


def test_cancelled_admission_cannot_dispatch_after_thread_commits(tmp_path, monkeypatch):
    async def check():
        calls = []

        def handler(request):
            calls.append(request)
            return response()

        router, limiter, generation = await router_for(tmp_path, handler)
        started, release = threading.Event(), threading.Event()
        original = limiter._admit

        def gated_admission(*arguments):
            started.set()
            assert release.wait(timeout=5)
            return original(*arguments)

        monkeypatch.setattr(limiter, "_admit", gated_admission)
        scopes = []

        async def request():
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await router.complete(BUCKET, 100, generation)

        with anyio.fail_after(5):
            async with anyio.create_task_group() as group:
                group.start_soon(request)
                while not started.is_set():
                    await anyio.sleep(0.001)
                scopes[0].cancel()
                release.set()
        assert not calls
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)


def test_cancellation_during_primary_cleanup_cannot_dispatch_backup(tmp_path):
    async def check():
        calls, scopes = [], []
        closing, release = anyio.Event(), anyio.Event()

        class GatedClose(Bytes):
            async def aclose(self):
                closing.set()
                await release.wait()
                self.closed = True

        stream = GatedClose(b"")

        def handler(request):
            calls.append(request)
            return response(status=429, stream=stream) if request.url.host == "primary.test" else response()

        router, limiter, generation = await router_for(tmp_path, handler)

        async def request():
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await router.complete(BUCKET, 100, generation)

        with anyio.fail_after(5):
            async with anyio.create_task_group() as group:
                group.start_soon(request)
                await closing.wait()
                scopes[0].cancel()
                release.set()
        assert stream.closed
        assert [call.url.host for call in calls] == ["primary.test"]
        assert await limiter.used(BUCKET) == generation.reservation()
    anyio.run(check)
