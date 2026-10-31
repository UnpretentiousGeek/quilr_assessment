import json
import os
import socket
import subprocess
import sys

import httpx2

from task2_gateway.demo import ROOT, read_records, running_server

VIEWER = "v" * 43
ADMIN = "a" * 43


def test_actual_http_discovery_authorization_and_recorder(tmp_path):
    recorder = tmp_path / "requests.jsonl"
    with running_server("task2_gateway.mock_server", arguments=["--record-file", str(recorder)]) as upstream:
        environment = {
            "GATEWAY_UPSTREAM_URL": upstream + "/mcp",
            "GATEWAY_TOKEN_ROLES": json.dumps({VIEWER: "viewer", ADMIN: "admin"}),
            "GATEWAY_UPSTREAM_BEARER": "upstream-only-secret",
        }
        with running_server("task2_gateway.server", environ=environment) as gateway:
            with httpx2.Client(trust_env=False, timeout=5) as client:
                payload = {"jsonrpc": "2.0", "id": "discovery", "method": "tools/list"}
                direct = client.post(upstream + "/mcp", json=payload)
                proxied = client.post(gateway + "/mcp", json=payload, headers={"Authorization": "Bearer " + VIEWER})
                assert proxied.status_code == 200
                assert proxied.content == direct.content
                assert [tool["name"] for tool in proxied.json()["result"]["tools"]] == ["public_echo", "admin_reset_key"]

                public = {"jsonrpc": "2.0", "id": 0, "method": "tools/call", "params": {"name": "public_echo", "arguments": {"text": "hello"}}}
                response = client.post(gateway + "/mcp", json=public, headers={"Authorization": "Bearer " + VIEWER, "Cookie": "caller=secret"})
                assert response.status_code == 200 and response.json()["id"] == 0
                assert "result" in response.json()

                admin = {"jsonrpc": "2.0", "id": "", "method": "tools/call", "params": {"name": "admin_reset_key", "arguments": {}}}
                before = len(read_records(recorder))
                denied = client.post(gateway + "/mcp", json=admin, headers={"Authorization": "Bearer " + VIEWER, "X-Role": "admin"})
                assert denied.status_code == 200
                assert denied.json() == {"jsonrpc": "2.0", "id": "", "error": {"code": -32001, "message": "Unauthorized Tool Call"}}
                assert len(read_records(recorder)) == before

                allowed = client.post(gateway + "/mcp", json=admin, headers={"Authorization": "Bearer " + ADMIN})
                assert allowed.status_code == 200 and "result" in allowed.json()
                assert len(read_records(recorder)) == before + 1

                before = len(read_records(recorder))
                for headers in [{}, {"Authorization": "Bearer admin"}]:
                    failure = client.post(gateway + "/mcp", json=payload, headers=headers)
                    assert failure.status_code == 401
                assert len(read_records(recorder)) == before

                notification = {"jsonrpc": "2.0", "method": "notifications/initialized"}
                response = client.post(gateway + "/mcp", json=notification, headers={"Authorization": "Bearer " + VIEWER})
                assert response.status_code == 204 and response.content == b""

                for record in read_records(recorder)[1:]:
                    assert record["headers"]["authorization"] == "Bearer upstream-only-secret"
                    assert "cookie" not in record["headers"]
                    assert VIEWER not in json.dumps(record) and ADMIN not in json.dumps(record)


def test_actual_unavailable_upstream_returns_sanitized_error():
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        environment = {
            "GATEWAY_UPSTREAM_URL": f"http://127.0.0.1:{port}/mcp",
            "GATEWAY_TOKEN_ROLES": json.dumps({VIEWER: "viewer"}),
            "GATEWAY_TIMEOUT_SECONDS": "0.2",
        }
        with running_server("task2_gateway.server", environ=environment) as gateway:
            response = httpx2.post(
                gateway + "/mcp", json={"jsonrpc": "2.0", "id": "connect", "method": "tools/list"},
                headers={"Authorization": "Bearer " + VIEWER}, trust_env=False, timeout=3,
            )
            assert response.status_code in {502, 504}
            assert response.json() == {
                "jsonrpc": "2.0", "id": "connect",
                "error": {"code": -32002, "message": "Upstream request failed"},
            }


def test_cli_missing_configuration_exits_before_serving():
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GATEWAY_")}
    result = subprocess.run(
        [sys.executable, "-m", "task2_gateway.server"], cwd=ROOT, env=environment,
        capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 2
    assert "Missing GATEWAY_UPSTREAM_URL" in result.stderr
    assert result.stdout == ""
