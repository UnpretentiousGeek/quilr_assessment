import asyncio
import json
from dataclasses import replace

import httpx2
import pytest

from task2_gateway.server import Config, MAX_BODY_BYTES, create_app, load_config

VIEWER = "v" * 43
ADMIN = "a" * 43
CONFIG = Config("http://127.0.0.1:9999/mcp", {VIEWER: "viewer", ADMIN: "admin"})
MISSING = object()


def message(method="tools/list", params=MISSING, identifier=1):
    value = {"jsonrpc": "2.0", "method": method}
    if identifier is not MISSING:
        value["id"] = identifier
    if params is not MISSING:
        value["params"] = params
    return value


class Recorder:
    def __init__(self, responder=None):
        self.requests = []
        self.responder = responder
        self.last_body = None

    def __call__(self, request):
        self.requests.append(request)
        if self.responder is not None:
            return self.responder(request)
        value = json.loads(request.content)
        if "id" not in value:
            return httpx2.Response(204)
        self.last_body = json.dumps({
            "jsonrpc": "2.0", "id": value["id"],
            "result": {"tools": [{"name": "public_echo"}, {"name": "admin_reset_key"}], "marker": "unchanged"},
        }, indent=2).encode() + b"\n"
        return httpx2.Response(200, content=self.last_body, headers={"Content-Type": "application/json"})


def request(recorder, payload, *, token=VIEWER, headers=None, config=CONFIG):
    async def run():
        app = create_app(config, transport=httpx2.MockTransport(recorder))
        outgoing_headers = [("Content-Type", "application/json")]
        if token is not None:
            outgoing_headers.append(("Authorization", f"Bearer {token}"))
        outgoing_headers.extend(headers or [])
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app), base_url="http://gateway") as client:
            return await client.post("/mcp", content=raw, headers=outgoing_headers)
    return asyncio.run(run())


def test_discovery_and_request_bytes_are_transparent():
    recorder = Recorder()
    raw = b'{ "jsonrpc":"2.0", "id":0, "method":"tools/list" }\n'
    response = request(recorder, raw)
    assert response.status_code == 200
    assert response.content == recorder.last_body
    assert recorder.requests[0].content == raw
    assert [tool["name"] for tool in response.json()["result"]["tools"]] == ["public_echo", "admin_reset_key"]


@pytest.mark.parametrize("token,name", [
    (VIEWER, "public_echo"), (ADMIN, "public_echo"), (ADMIN, "admin_reset_key"),
    (VIEWER, "Admin_reset_key"), (VIEWER, "admin"), (VIEWER, "xadmin_reset_key"),
])
def test_allowed_tool_calls_reach_upstream(token, name):
    recorder = Recorder()
    payload = message("tools/call", {"name": name, "arguments": {"text": "unchanged"}})
    response = request(recorder, payload, token=token)
    assert response.status_code == 200
    assert response.content == recorder.last_body
    assert len(recorder.requests) == 1
    assert json.loads(recorder.requests[0].content) == payload


@pytest.mark.parametrize("identifier", [0, "", -4, 3, "blocked"])
@pytest.mark.parametrize("name", ["admin_reset_key", "admin_"])
def test_denied_call_has_exact_error_and_no_upstream_contact(identifier, name):
    recorder = Recorder()
    response = request(recorder, message("tools/call", {"name": name}, identifier))
    assert response.status_code == 200
    assert response.json() == {
        "jsonrpc": "2.0", "id": identifier,
        "error": {"code": -32001, "message": "Unauthorized Tool Call"},
    }
    assert recorder.requests == []


@pytest.mark.parametrize("identifier", [0, "", -4, "call", 2**80])
def test_allowed_call_preserves_id_type_and_value(identifier):
    recorder = Recorder()
    response = request(recorder, message(identifier=identifier))
    assert response.json()["id"] == identifier
    assert type(response.json()["id"]) is type(identifier)


@pytest.mark.parametrize("identifier", [None, True, False, 1.0, [], {}])
def test_invalid_ids_fail_without_forwarding(identifier):
    recorder = Recorder()
    response = request(recorder, message(identifier=identifier))
    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32600
    assert response.json()["id"] is None
    assert recorder.requests == []


@pytest.mark.parametrize("token,headers", [
    (None, []), ("unknown" * 8, []), ("viewer", []), ("admin", []),
    (VIEWER[:-1] + "x", []),
    ("eyJhbGciOiJub25lIn0.eyJyb2xlIjoiYWRtaW4ifQ.", []),
    (None, [("X-Role", "admin"), ("Role", "admin")]),
    (None, [("Authorization", "Basic " + ADMIN)]),
    (None, [("Authorization", "Bearer " + ADMIN + " extra")]),
    (None, [("Authorization", "Bearer  " + ADMIN)]),
    (VIEWER, [("Authorization", "Bearer " + ADMIN)]),
])
def test_credential_failures_do_not_reach_upstream(token, headers):
    recorder = Recorder()
    response = request(recorder, message(), token=token, headers=headers)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json() == {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "Unauthorized"}}
    assert recorder.requests == []
    assert VIEWER not in response.text and ADMIN not in response.text


def test_role_spoofing_does_not_upgrade_viewer():
    recorder = Recorder()
    response = request(
        recorder, message("tools/call", {"name": "admin_reset_key"}),
        headers=[("X-Role", "admin"), ("X-User-Role", "admin")],
    )
    assert response.json()["error"] == {"code": -32001, "message": "Unauthorized Tool Call"}
    assert recorder.requests == []


@pytest.mark.parametrize("raw", [
    b"{", b"", b"\xff", b'{"jsonrpc":"2.0","id":1,"id":2,"method":"tools/list"}',
    b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"public_echo","name":"admin_reset_key"}}',
    b'{"jsonrpc":"2.0","id":NaN,"method":"tools/list"}',
    b'{"jsonrpc":"2.0","id":Infinity,"method":"tools/list"}',
    b'{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"value":1e999}}',
    b'{"jsonrpc":"2.0","id":"\\ud800","method":"tools/list"}',
    b"[" * 1500 + b"]" * 1500,
])
def test_malformed_json_and_duplicate_keys_are_parse_errors(raw):
    recorder = Recorder()
    response = request(recorder, raw)
    assert response.status_code == 400
    assert response.json() == {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
    assert recorder.requests == []


@pytest.mark.parametrize("payload", [
    [], [message("tools/call", {"name": "admin_reset_key"})], None, True, "request", 3,
    {}, {"jsonrpc": "1.0", "id": 1, "method": "tools/list"},
    {"jsonrpc": "2.0", "id": 1, "method": ""},
    {"jsonrpc": "2.0", "id": 1, "method": 1},
    message(params=None), message(params="wrong"), message(params=42),
    {**message(), "result": {}}, {**message(), "error": {}},
])
def test_invalid_envelopes_and_batches_do_not_reach_upstream(payload):
    recorder = Recorder()
    response = request(recorder, payload)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32600
    assert recorder.requests == []


@pytest.mark.parametrize("params", [MISSING, [], {}, {"name": ""}, {"name": 1}, {"name": None}, {"name": False}])
def test_invalid_tool_call_parameters(params):
    recorder = Recorder()
    response = request(recorder, message("tools/call", params, "bad-name"))
    assert response.json() == {
        "jsonrpc": "2.0", "id": "bad-name", "error": {"code": -32602, "message": "Invalid params"},
    }
    assert recorder.requests == []


@pytest.mark.parametrize("name,token,expected_status,expected_calls", [
    ("public_echo", VIEWER, 204, 1), ("admin_reset_key", ADMIN, 204, 1),
    ("admin_reset_key", VIEWER, 204, 0), ("", VIEWER, 204, 0),
    ("public_echo", None, 401, 0),
])
def test_notifications_have_no_json_rpc_response(name, token, expected_status, expected_calls):
    recorder = Recorder()
    response = request(recorder, message("tools/call", {"name": name}, MISSING), token=token)
    assert response.status_code == expected_status
    assert response.content == b""
    assert len(recorder.requests) == expected_calls


@pytest.mark.parametrize("method,params", [
    ("initialize", {"protocolVersion": "2025-11-25", "capabilities": {}}),
    ("unknown/method", [1, 2]), ("ping", MISSING),
])
def test_other_valid_methods_are_forwarded(method, params):
    recorder = Recorder()
    payload = message(method, params)
    response = request(recorder, payload)
    assert response.status_code == 200
    assert len(recorder.requests) == 1
    assert json.loads(recorder.requests[0].content) == payload


def test_downstream_json_rpc_error_and_status_pass_through():
    raw = b'{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"No such method","data":{"source":"mock"}}}\n'
    recorder = Recorder(lambda request: httpx2.Response(503, content=raw, headers={"Content-Type": "application/json; charset=utf-8"}))
    response = request(recorder, message("unknown/method"))
    assert response.status_code == 503
    assert response.content == raw
    assert response.headers["content-type"] == "application/json; charset=utf-8"


@pytest.mark.parametrize("error,status", [(httpx2.ReadTimeout, 504), (httpx2.ConnectError, 502), (httpx2.RemoteProtocolError, 502)])
@pytest.mark.parametrize("identifier", ["failure", MISSING])
def test_transport_failures_are_generic_and_never_retried(error, status, identifier):
    def fail(request):
        raise error("PRIVATE_UPSTREAM_SECRET", request=request)
    recorder = Recorder(fail)
    response = request(recorder, message(identifier=identifier))
    assert response.status_code == status
    assert len(recorder.requests) == 1
    if identifier is MISSING:
        assert response.content == b""
    else:
        assert response.json() == {
            "jsonrpc": "2.0", "id": identifier,
            "error": {"code": -32002, "message": "Upstream request failed"},
        }
    assert "PRIVATE_UPSTREAM_SECRET" not in response.text


def test_redirect_is_not_followed_or_returned():
    recorder = Recorder(lambda request: httpx2.Response(307, headers={"Location": "http://127.0.0.1:7777/private"}))
    response = request(recorder, message())
    assert response.status_code == 502
    assert len(recorder.requests) == 1
    assert "location" not in response.headers


@pytest.mark.parametrize("body,content_type", [
    (b"not json", "application/json"),
    (b'{"jsonrpc":"2.0","id":2,"result":{}}', "application/json"),
    (b'{"jsonrpc":"2.0","id":true,"result":{}}', "application/json"),
    (b'{"jsonrpc":"2.0","id":1,"result":{},"error":{}}', "application/json"),
    (b'{"jsonrpc":"2.0","id":1,"error":{"code":true,"message":"bad"}}', "application/json"),
    (b'{"jsonrpc":"2.0","id":1,"result":{},"id":1}', "application/json"),
    (b'{"jsonrpc":"2.0","id":1,"result":{}}', "text/event-stream"),
    (b"", "application/json"),
])
def test_invalid_upstream_protocol_is_rejected(body, content_type):
    recorder = Recorder(lambda request: httpx2.Response(200, content=body, headers={"Content-Type": content_type}))
    response = request(recorder, message())
    assert response.status_code == 502
    assert response.json()["error"] == {"code": -32002, "message": "Upstream request failed"}


def test_caller_credentials_headers_and_upstream_cookies_are_isolated():
    def respond(incoming):
        return httpx2.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {}}, headers={
            "Set-Cookie": "upstream_session=secret; Path=/", "X-Private": "secret", "Connection": "keep-alive",
        })
    recorder = Recorder(respond)
    config = replace(CONFIG, upstream_bearer="separate-upstream-secret")
    extra = [
        ("Cookie", "caller_session=private"), ("X-Role", "admin"),
        ("X-Forwarded-For", "203.0.113.2"), ("X-Custom-Secret", "private"),
        ("Host", "attacker.example"),
    ]
    async def run():
        app = create_app(config, transport=httpx2.MockTransport(recorder))
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app), base_url="http://gateway") as client:
            for token in [VIEWER, ADMIN]:
                response = await client.post("/mcp", json=message(), headers=[
                    ("Authorization", "Bearer " + token), *extra,
                ])
                assert response.status_code == 200
                assert "set-cookie" not in response.headers
                assert "x-private" not in response.headers
                assert "connection" not in response.headers
    asyncio.run(run())
    assert len(recorder.requests) == 2
    for incoming in recorder.requests:
        assert str(incoming.url) == CONFIG.upstream_url
        assert incoming.headers["authorization"] == "Bearer separate-upstream-secret"
        assert incoming.headers["host"] == "127.0.0.1:9999"
        assert incoming.headers["accept"] == "application/json"
        for header in ["cookie", "x-role", "x-forwarded-for", "x-custom-secret"]:
            assert header not in incoming.headers


def test_no_caller_authorization_is_forwarded_without_upstream_credential():
    recorder = Recorder()
    assert request(recorder, message()).status_code == 200
    assert "authorization" not in recorder.requests[0].headers


def test_proxy_environment_is_not_used(monkeypatch):
    captured = []
    original = httpx2.AsyncClient
    def observe(**kwargs):
        if "timeout" in kwargs:
            captured.append(kwargs)
        return original(**kwargs)
    monkeypatch.setattr(httpx2, "AsyncClient", observe)
    monkeypatch.setenv("HTTP_PROXY", "http://attacker.example:8080")
    assert request(Recorder(), message()).status_code == 200
    assert len(captured) == 1
    assert captured[0]["trust_env"] is False
    assert captured[0]["follow_redirects"] is False
    for kind in ["connect", "read", "write", "pool"]:
        assert getattr(captured[0]["timeout"], kind) == 5.0


def test_receive_time_body_limit_stops_before_reading_remaining_chunks():
    recorder = Recorder()
    app = create_app(CONFIG, transport=httpx2.MockTransport(recorder))
    received = 0
    sent = []
    chunks = [b" " * (MAX_BODY_BYTES // 2), b" " * (MAX_BODY_BYTES // 2), b"x", b"must-not-be-read"]
    async def receive():
        nonlocal received
        chunk = chunks[received]
        received += 1
        return {"type": "http.request", "body": chunk, "more_body": received < len(chunks)}
    async def send(event):
        sent.append(event)
    asyncio.run(app({
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "POST", "scheme": "http", "path": "/mcp", "raw_path": b"/mcp",
        "query_string": b"", "root_path": "", "server": ("gateway", 80), "client": ("127.0.0.1", 1234),
        "headers": [(b"content-type", b"application/json"), (b"authorization", f"Bearer {VIEWER}".encode())],
    }, receive, send))
    assert received == 3
    assert sent[0]["status"] == 413
    assert json.loads(sent[1]["body"])["error"]["message"] == "Request body too large"
    assert recorder.requests == []


def test_missing_content_type_is_rejected():
    async def run():
        recorder = Recorder()
        app = create_app(CONFIG, transport=httpx2.MockTransport(recorder))
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/mcp", content=b"{}")
        assert response.status_code == 415
        assert recorder.requests == []
    asyncio.run(run())


@pytest.mark.parametrize("environ", [
    {}, {"GATEWAY_UPSTREAM_URL": "http://127.0.0.1/mcp"},
    {"GATEWAY_UPSTREAM_URL": "http://127.0.0.1/mcp", "GATEWAY_TOKEN_ROLES": "{}"},
    {"GATEWAY_UPSTREAM_URL": "http://127.0.0.1/mcp", "GATEWAY_TOKEN_ROLES": "[]"},
    {"GATEWAY_UPSTREAM_URL": "http://127.0.0.1/mcp", "GATEWAY_TOKEN_ROLES": "not json"},
    {"GATEWAY_UPSTREAM_URL": "http://127.0.0.1/mcp", "GATEWAY_TOKEN_ROLES": '{"' + VIEWER + '":"viewer","' + VIEWER + '":"admin"}'},
])
def test_missing_and_invalid_configuration_fails_closed(environ):
    with pytest.raises(ValueError):
        load_config(environ)


@pytest.mark.parametrize("roles", [
    {"viewer": "viewer"}, {"": "admin"}, {VIEWER: "user"}, {VIEWER: "owner"},
    {VIEWER: None}, {VIEWER: []}, {VIEWER: {}}, {VIEWER: True}, {VIEWER: 1},
])
def test_invalid_token_configuration(roles):
    with pytest.raises(ValueError):
        Config(CONFIG.upstream_url, roles)


@pytest.mark.parametrize("url", [
    "https://example.com/mcp", "http://127.0.0.1:8002/mcp", "http://[::1]:8002/mcp", "http://localhost/mcp",
])
def test_allowed_upstream_configuration(url):
    assert Config(url, {VIEWER: "viewer"}).upstream_url == url


@pytest.mark.parametrize("url", [
    "", "ftp://example.com/mcp", "http://example.com/mcp", "https:///mcp", "https://host:bad/mcp",
    "https://user:secret@example.com/mcp", "https://@example.com/mcp", "https://example.com/mcp#fragment",
    "https://example.com/mcp#", "https://example.com/\nprivate", "https://example.com:0/mcp",
])
def test_unsafe_upstream_configuration_is_rejected(url):
    with pytest.raises(ValueError):
        Config(url, {VIEWER: "viewer"})


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True, "5"])
def test_invalid_timeout_configuration(timeout):
    with pytest.raises(ValueError):
        replace(CONFIG, timeout_seconds=timeout)


def test_configuration_is_an_immutable_snapshot():
    roles = {VIEWER: "viewer"}
    config = Config(CONFIG.upstream_url, roles)
    roles[VIEWER] = "admin"
    assert config.token_roles[VIEWER] == "viewer"
    with pytest.raises(TypeError):
        config.token_roles[VIEWER] = "admin"
