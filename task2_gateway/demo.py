import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import httpx2

ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def running_server(module: str, *, environ=None, arguments=()):
    process = None
    try:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            environment = dict(os.environ)
            environment.update(environ or {})
            process = subprocess.Popen(
                [sys.executable, "-m", module, "--fd", str(listener.fileno()), *arguments],
                cwd=ROOT, env=environment, pass_fds=[listener.fileno()],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            )
        base_url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 8
        with httpx2.Client(trust_env=False, timeout=0.2) as client:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"Local server exited: {process.stderr.read().decode('utf-8', errors='replace')}")
                try:
                    if client.get(base_url + "/").status_code == 404:
                        break
                except httpx2.HTTPError:
                    pass
                time.sleep(0.02)
            else:
                raise RuntimeError("Local server did not start")
        yield base_url
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            process.stderr.close()


def read_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def main():
    viewer, admin = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    with tempfile.TemporaryDirectory(prefix="task2-demo-") as directory:
        recorder = Path(directory) / "requests.jsonl"
        with running_server("task2_gateway.mock_server", arguments=["--record-file", str(recorder)]) as upstream:
            environment = {
                "GATEWAY_UPSTREAM_URL": upstream + "/mcp",
                "GATEWAY_TOKEN_ROLES": json.dumps({viewer: "viewer", admin: "admin"}),
            }
            with running_server("task2_gateway.server", environ=environment) as gateway:
                with httpx2.Client(trust_env=False, timeout=5) as client:
                    def send(token, identifier, method, params=None):
                        message = {"jsonrpc": "2.0", "id": identifier, "method": method}
                        if params is not None:
                            message["params"] = params
                        headers = {"Authorization": f"Bearer {token}"} if token else {}
                        return client.post(gateway + "/mcp", json=message, headers=headers)

                    discovery = send(viewer, "discovery", "tools/list")
                    names = [tool["name"] for tool in discovery.json()["result"]["tools"]]
                    assert discovery.status_code == 200
                    assert names == ["public_echo", "admin_reset_key"]
                    print("Discovery preserves both public_echo and admin_reset_key.")

                    public = send(viewer, 0, "tools/call", {"name": "public_echo", "arguments": {"hello": "world"}})
                    assert public.status_code == 200 and "result" in public.json()
                    print("Viewer public tool call succeeds and preserves request ID 0.")

                    before = len(read_records(recorder))
                    blocked = send(viewer, "blocked", "tools/call", {"name": "admin_reset_key", "arguments": {}})
                    assert blocked.json() == {
                        "jsonrpc": "2.0", "id": "blocked",
                        "error": {"code": -32001, "message": "Unauthorized Tool Call"},
                    }
                    assert len(read_records(recorder)) == before
                    print("Viewer admin call returns -32001 Unauthorized Tool Call; downstream request delta = 0.")

                    permitted = send(admin, "admin", "tools/call", {"name": "admin_reset_key", "arguments": {}})
                    assert permitted.status_code == 200 and "result" in permitted.json()
                    assert len(read_records(recorder)) == before + 1
                    print("Admin admin_reset_key call reaches the mock and succeeds.")

                    before = len(read_records(recorder))
                    unauthorized = send(None, "missing-token", "tools/list")
                    assert unauthorized.status_code == 401
                    assert unauthorized.headers["www-authenticate"] == "Bearer"
                    assert len(read_records(recorder)) == before
                    print("Missing credentials return HTTP 401; downstream request delta = 0.")
    print("Demo passed. Both local server processes stopped.")


if __name__ == "__main__":
    main()
