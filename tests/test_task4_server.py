import json
from contextlib import contextmanager

import anyio
import httpx2
import pytest
from starlette.testclient import TestClient

from task4_router.limiter import SQLiteLimiter, key_bucket
from task4_router.router import Provider
from task4_router.server import Config, Tenant, create_app, load_config
from test_task4_router import Bytes, REQUEST, Stalled, response


KEY = "a" * 40
OTHER_KEY = "b" * 40


def config_for(path, limit=100):
    return Config(Provider("https://primary.test/complete", "primary-model", "provider-primary-secret"),
                  Provider("https://secondary.test/complete", "secondary-model", "provider-secondary-secret"),
                  {KEY: Tenant("tenant", limit), OTHER_KEY: Tenant("tenant", limit)}, str(path))


@contextmanager
def client_for(tmp_path, handler=None, *, limit=100):
    calls = []
    def transport(request):
        calls.append(request)
        return handler(request) if handler else response(headers={"X-Private": "secret", "Set-Cookie": "secret"})
    app = create_app(config_for(tmp_path / "ledger.db", limit), transport=httpx2.MockTransport(transport))
    with TestClient(app) as client:
        yield client, app, calls


def auth(key=KEY):
    return {"Authorization": "Bearer " + key}


def test_owned_response_and_credentials_do_not_leak(tmp_path):
    with client_for(tmp_path) as (client, app, calls):
        result = client.post("/complete", json=REQUEST, headers={**auth(), "Cookie": "private-caller", "X-Role": "admin", "X-Tenant": "spoofed"})
        assert result.status_code == 200
        assert result.json() == {"text": "safe text", "usage": {"prompt_tokens": 12, "completion_tokens": 4, "total_tokens": 16}, "provider": "primary"}
        assert result.headers["Cache-Control"] == "no-store"
        assert "X-Private" not in result.headers and "Set-Cookie" not in result.headers
        assert calls[0].headers["Authorization"] == "Bearer provider-primary-secret"
        assert "cookie" not in calls[0].headers and "x-role" not in calls[0].headers and "x-tenant" not in calls[0].headers
        assert KEY not in result.text and "private" not in result.text


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer " + "unknown" * 6}, {"Authorization": "Basic " + KEY},
                                    {"Authorization": "Bearer  " + KEY}, {"Authorization": "Bearer " + KEY + " "},
                                    [("Authorization", "Bearer " + KEY), ("Authorization", "Bearer " + OTHER_KEY)]])
def test_invalid_credentials_are_sanitized_and_make_zero_calls(tmp_path, headers):
    with client_for(tmp_path) as (client, app, calls):
        result = client.post("/complete", json=REQUEST, headers=headers)
        assert result.status_code == 401 and result.headers["WWW-Authenticate"] == "Bearer"
        assert result.json() == {"error": {"code": "unauthorized", "message": "A valid API key is required."}}
        assert not calls
        assert anyio.run(app.state.limiter.used, key_bucket(KEY)) == 0


@pytest.mark.parametrize("extra", [{"usage": {"total_tokens": 0}}, {"tenant": "other"}, {"tenant_id": "other"},
                                  {"role": "admin"}, {"provider": "secondary"}, {"model": "caller-model"},
                                  {"stream": True}, {"max_completion_tokens": 20}])
def test_untrusted_routing_accounting_and_role_fields_are_forbidden(tmp_path, extra):
    with client_for(tmp_path) as (client, app, calls):
        result = client.post("/complete", json={**REQUEST, **extra}, headers=auth())
        assert result.status_code == 400 and result.json()["error"]["code"] == "invalid_request"
        assert not calls


@pytest.mark.parametrize("generation", [None, [], {}, {"messages": [], "max_tokens": 8},
                                      {"messages": [{"role": "tool", "content": "x"}], "max_tokens": 8},
                                      {"messages": [{"role": "user", "content": {"text": "x"}}], "max_tokens": 8},
                                      {"messages": [{"role": "user", "content": "x", "name": "spoof"}], "max_tokens": 8},
                                      {**REQUEST, "max_tokens": True}, {**REQUEST, "max_tokens": 0},
                                      {**REQUEST, "max_tokens": -1}, {**REQUEST, "max_tokens": 32769},
                                      {**REQUEST, "max_tokens": "8"}, {**REQUEST, "max_tokens": 8.0}])
def test_invalid_completion_models_make_zero_calls(tmp_path, generation):
    with client_for(tmp_path) as (client, app, calls):
        result = client.post("/complete", content=json.dumps(generation), headers={**auth(), "Content-Type": "application/json"})
        assert result.status_code == 400 and not calls


@pytest.mark.parametrize("raw", [b'{"messages":[],"messages":[],"max_tokens":1}', b'{"max_tokens":NaN}',
                                b'{"max_tokens":1e400}', b'{"messages":[{"role":"user","content":"\\ud800"}],"max_tokens":8}',
                                b"\xff", b"[" * 2000])
def test_malformed_json_and_unicode_make_zero_calls(tmp_path, raw):
    with client_for(tmp_path) as (client, app, calls):
        result = client.post("/complete", content=raw, headers={**auth(), "Content-Type": "application/json"})
        assert result.status_code == 400 and not calls
        assert result.json()["error"]["message"] == "The completion request is invalid."


def test_media_type_body_bounds_and_http_errors_are_owned(tmp_path):
    with client_for(tmp_path) as (client, app, calls):
        media = client.post("/complete", content="{}", headers=auth())
        assert media.status_code == 415
        large = client.post("/complete", content=b"x" * 65537, headers={**auth(), "Content-Type": "application/json"})
        assert large.status_code == 413
        streamed = client.post("/complete", content=iter([b"x" * 40000, b"x" * 40000]), headers={**auth(), "Content-Type": "application/json"})
        assert streamed.status_code == 413
        for path, method, status, code in [("/missing", "POST", 404, "not_found"), ("/complete/", "POST", 404, "not_found"), ("/complete", "GET", 405, "method_not_allowed")]:
            result = client.request(method, path, headers=auth())
            assert result.status_code == status and result.json()["error"]["code"] == code
        assert not calls


def test_key_buckets_are_isolated_even_when_trusted_tenant_labels_match(tmp_path):
    with client_for(tmp_path, limit=40) as (client, app, calls):
        assert client.post("/complete", json=REQUEST, headers=auth()).status_code == 200
        assert client.post("/complete", json=REQUEST, headers=auth()).status_code == 429
        assert client.post("/complete", json=REQUEST, headers=auth(OTHER_KEY)).status_code == 200
        assert len(calls) == 2
        assert anyio.run(app.state.limiter.used, key_bucket(KEY)) == 16
        assert anyio.run(app.state.limiter.used, key_bucket(OTHER_KEY)) == 16


def test_restart_preserves_charge(tmp_path):
    with client_for(tmp_path, limit=40) as (client, app, calls):
        assert client.post("/complete", json=REQUEST, headers=auth()).status_code == 200
    with client_for(tmp_path, limit=40) as (client, app, calls):
        assert client.post("/complete", json=REQUEST, headers=auth()).status_code == 429
        assert not calls


def test_disk_failure_is503_and_zero_network_calls(tmp_path):
    with client_for(tmp_path) as (client, app, calls):
        (tmp_path / "ledger.db").unlink()
        result = client.post("/complete", json=REQUEST, headers=auth())
        assert result.status_code == 503
        assert result.json() == {"error": {"code": "limiter_unavailable", "message": "The token budget service is unavailable."}}
        assert not calls and str(tmp_path) not in result.text


def test_malformed_backup_is_sanitized_and_both_reservations_remain(tmp_path):
    def handler(request):
        return response(status=429) if request.url.host == "primary.test" else response(stream=Bytes(b"private malformed traceback"))
    with client_for(tmp_path, handler) as (client, app, calls):
        result = client.post("/complete", json=REQUEST, headers=auth())
        assert result.status_code == 502 and "private" not in result.text and "traceback" not in result.text
        assert len(calls) == 2
        assert anyio.run(app.state.limiter.used, key_bucket(KEY)) == 50


def scope_for(raw):
    return {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1", "method": "POST",
            "scheme": "http", "path": "/complete", "raw_path": b"/complete", "query_string": b"",
            "headers": [(b"authorization", ("Bearer " + KEY).encode()), (b"content-type", b"application/json")],
            "client": ("127.0.0.1", 1234), "server": ("gateway", 80), "root_path": ""}


def test_disconnect_cancels_provider_and_closes_stream(tmp_path):
    async def check():
        stream, calls, sends = Stalled(), [], []
        def handler(request):
            calls.append(request)
            return response(stream=stream)
        limiter = SQLiteLimiter(tmp_path / "ledger.db")
        await limiter.initialize()
        app = create_app(config_for(tmp_path / "ledger.db"), limiter=limiter, transport=httpx2.MockTransport(handler))
        first = True
        async def receive():
            nonlocal first
            if first:
                first = False
                return {"type": "http.request", "body": json.dumps(REQUEST).encode(), "more_body": False}
            await stream.started.wait()
            return {"type": "http.disconnect"}
        async def send(message):
            sends.append(message)
        with anyio.fail_after(1):
            await app(scope_for(REQUEST), receive, send)
        assert stream.closed and stream.iteration_finished
        assert len(calls) == 1 and not sends
        assert await limiter.used(key_bucket(KEY)) == 25
    anyio.run(check)


def test_slow_request_body_returns_standard408(tmp_path, monkeypatch):
    import task4_router.server as server
    monkeypatch.setattr(server, "BODY_TIMEOUT_SECONDS", 0.01)
    async def check():
        sends, calls = [], []
        app = create_app(config_for(tmp_path / "ledger.db"), transport=httpx2.MockTransport(lambda request: calls.append(request)))
        async def receive():
            await anyio.sleep_forever()
        async def send(message):
            sends.append(message)
        await app(scope_for(REQUEST), receive, send)
        assert sends[0]["status"] == 408
        assert json.loads(sends[1]["body"])["error"]["code"] == "request_timeout"
        assert not calls
    anyio.run(check)


@pytest.mark.parametrize("settings", [
    {}, {"ROUTER_TENANTS_JSON": "{}"}, {"ROUTER_TENANTS_JSON": '{"short":{"tenant_id":"a"}}'},
    {"ROUTER_TENANTS_JSON": json.dumps({KEY: {"tenant_id": "a", "tokens_per_minute": True}})},
    {"ROUTER_TENANTS_JSON": json.dumps({KEY: {"tenant_id": "a", "unknown": 1}})},
    {"ROUTER_TENANTS_JSON": json.dumps({KEY: {"tenant_id": "a"}}), "ROUTER_DB_PATH": ":memory:"},
])
def test_startup_config_is_strict_and_sanitized(settings):
    with pytest.raises(ValueError, match="^Invalid router startup configuration$"):
        load_config(settings)


def test_startup_config_defaults_and_mapping_are_frozen(tmp_path):
    original = {KEY: Tenant("tenant")}
    config = Config(Provider("https://primary.test/complete", "primary"), Provider("https://secondary.test/complete", "secondary"), original, str(tmp_path / "ledger.db"))
    original.clear()
    assert KEY in config.tenants
    with pytest.raises(TypeError):
        config.tenants[OTHER_KEY] = Tenant("other")
    loaded = load_config({"ROUTER_PRIMARY_URL": "https://primary.test/complete", "ROUTER_SECONDARY_URL": "https://secondary.test/complete",
                          "ROUTER_DB_PATH": str(tmp_path / "ledger.db"), "ROUTER_TENANTS_JSON": json.dumps({KEY: {"tenant_id": "tenant"}})})
    assert loaded.primary.model == "demo-primary" and loaded.secondary.model == "demo-secondary"
    assert loaded.tenants[KEY].tokens_per_minute == 50000
