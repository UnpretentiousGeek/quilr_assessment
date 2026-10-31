import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager

import httpx2
import uvicorn

from task2_gateway.demo import ROOT, read_records, running_server
from task3_gateway.mock_server import EXPECTED, PREFIX
from task3_gateway.protocol import ERROR_EVENT, MAX_EVENT_CHARS, MAX_LINE_CHARS, MAX_NETWORK_CHUNK_BYTES
from task3_gateway.redaction import MAX_CANDIDATE_CHARS
from task3_gateway.server import Config, create_app


PAYLOAD = {"model": "local-demo", "messages": [{"role": "user", "content": "Show local fixture"}]}


def events(response):
    kind = None
    for line in response.iter_lines():
        if line.startswith("event: "):
            kind = line[7:]
        elif line.startswith("data: "):
            yield kind, json.loads(line[6:])


def wait_records(path, event):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        records = read_records(path)
        if any(record["event"] == event for record in records):
            return records
        time.sleep(0.01)
    raise AssertionError("Missing provider recorder event " + event)


def test_actual_http_first_safe_text_precedes_gated_upstream_completion(tmp_path):
    recorder, gate = tmp_path / "provider.jsonl", tmp_path / "continue"
    arguments = ["--record-file", str(recorder), "--gate-file", str(gate), "--chunk-chars", "1"]
    with running_server("task3_gateway.mock_server", arguments=arguments) as upstream:
        environment = {"LLM_UPSTREAM_URL": upstream + "/v1/chat/completions", "LLM_UPSTREAM_BEARER": "provider-only-secret"}
        with running_server("task3_gateway.server", environ=environment) as gateway:
            first_output, output, completed = None, "", False
            with httpx2.Client(trust_env=False, timeout=5) as client:
                with client.stream("POST", gateway + "/generate", json=PAYLOAD, headers={"Authorization": "Bearer caller-secret", "Cookie": "caller=secret"}) as response:
                    assert response.status_code == 200
                    assert "content-length" not in response.headers
                    for kind, data in events(response):
                        if kind == "text":
                            if first_output is None:
                                first_output = time.monotonic()
                                assert not gate.exists()
                                assert not any(record["event"] == "completed" for record in read_records(recorder))
                                gate.touch()
                            assert set(data) == {"text"}
                            output += data["text"]
                        elif kind == "done":
                            completed = True
                        else:
                            raise AssertionError("Unexpected stream error")
            assert completed and output == PREFIX + EXPECTED
            records = wait_records(recorder, "completed")
            assert first_output < next(record["time"] for record in records if record["event"] == "completed")
            sent = next(record for record in records if record["event"] == "request")
            assert sent["body"] == {**PAYLOAD, "stream": True, "n": 1}
            assert sent["headers"]["authorization"] == "Bearer provider-only-secret"
            assert "cookie" not in sent["headers"]
            assert "caller-secret" not in json.dumps(sent)


def test_actual_http_client_disconnect_closes_gated_provider_promptly(tmp_path):
    recorder, gate = tmp_path / "provider.jsonl", tmp_path / "continue"
    arguments = ["--record-file", str(recorder), "--gate-file", str(gate)]
    with running_server("task3_gateway.mock_server", arguments=arguments) as upstream:
        environment = {"LLM_UPSTREAM_URL": upstream + "/v1/chat/completions", "LLM_IDLE_TIMEOUT_SECONDS": "30"}
        with running_server("task3_gateway.server", environ=environment) as gateway:
            with httpx2.Client(trust_env=False, timeout=5) as client:
                with client.stream("POST", gateway + "/generate", json=PAYLOAD) as response:
                    assert response.status_code == 200
                    kind, data = next(events(response))
                    assert kind == "text" and data == {"text": PREFIX}
                    assert not gate.exists()
                    assert not any(record["event"] == "completed" for record in read_records(recorder))
                disconnected = time.monotonic()
                records = wait_records(recorder, "closed")
                assert next(record["time"] for record in records if record["event"] == "closed") - disconnected < 3
                assert not any(record["event"] == "completed" for record in records)
                assert not gate.exists()


@contextmanager
def running_app(app):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="error", lifespan="off", proxy_headers=False))
        worker = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        worker.start()
        try:
            deadline = time.monotonic() + 5
            while not server.started and worker.is_alive() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert server.started
            yield f"http://127.0.0.1:{port}"
        finally:
            server.should_exit = True
            worker.join(timeout=5)
            assert not worker.is_alive()


def test_actual_http_retained_state_is_bounded_as_response_grows():
    metrics = []
    for repeats in [20, 4000]:
        arguments = ["--repeat", str(repeats), "--chunk-chars", "256", "--delay-seconds", "0"]
        with running_server("task3_gateway.mock_server", arguments=arguments) as upstream:
            app = create_app(Config(upstream + "/v1/chat/completions"), metrics_sink=metrics.append)
            with running_app(app) as gateway:
                digest = hashlib.sha256()
                count, done = 0, False
                with httpx2.Client(trust_env=False, timeout=10) as client:
                    with client.stream("POST", gateway + "/generate", json=PAYLOAD) as response:
                        assert response.status_code == 200
                        for kind, data in events(response):
                            if kind == "text":
                                piece = data["text"]
                                assert "alice" not in piece and "example.com" not in piece
                                digest.update(piece.encode())
                                count += len(piece)
                            else:
                                assert kind == "done"
                                done = True
                expected = hashlib.sha256(PREFIX.encode())
                for _ in range(repeats):
                    expected.update(EXPECTED.encode())
                assert done and digest.digest() == expected.digest()
                assert count == len(PREFIX) + repeats * len(EXPECTED)
    assert len(metrics) == 2
    short, long = metrics
    assert long.upstream_bytes > 100 * short.upstream_bytes
    assert long.emitted_text_chars > 100 * short.emitted_text_chars
    assert long.max_candidate_chars == short.max_candidate_chars < MAX_CANDIDATE_CHARS
    assert long.max_line_chars == short.max_line_chars < MAX_LINE_CHARS
    assert long.max_event_chars == short.max_event_chars < MAX_EVENT_CHARS
    assert long.max_chunk_bytes <= MAX_NETWORK_CHUNK_BYTES


def test_actual_http_unavailable_provider_is_generic():
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        environment = {"LLM_UPSTREAM_URL": f"http://127.0.0.1:{port}/v1/chat/completions", "LLM_IDLE_TIMEOUT_SECONDS": "0.2"}
        with running_server("task3_gateway.server", environ=environment) as gateway:
            response = httpx2.post(gateway + "/generate", json=PAYLOAD, trust_env=False, timeout=5)
            assert response.status_code in {502, 504}
            assert response.content == ERROR_EVENT


def test_cli_missing_configuration_fails_before_serving():
    environment = {key: value for key, value in os.environ.items() if not key.startswith("LLM_")}
    result = subprocess.run([sys.executable, "-m", "task3_gateway.server"], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=5)
    assert result.returncode == 2 and "Missing LLM_UPSTREAM_URL" in result.stderr
    assert result.stdout == ""
