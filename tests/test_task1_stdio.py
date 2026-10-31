import json
import queue
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest
from mcp.client.session import LATEST_HANDSHAKE_VERSION
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
CUSTOMER = {"customer_id": "CUST-00001"}
REFUND = {**CUSTOMER, "amount": 12.5, "reason": "Duplicate charge"}


class WireClient:
    def __init__(self, command=None):
        self.process = subprocess.Popen(
            command or [sys.executable, "-m", "task1_mcp.server"],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.responses = queue.Queue()
        self.stdout_lines = []
        self.stderr_lines = []
        self.next_id = 0
        self.readers = [
            threading.Thread(target=self._read_stdout, daemon=True),
            threading.Thread(target=self._read_stderr, daemon=True),
        ]
        for reader in self.readers:
            reader.start()

    def _read_stdout(self):
        for line in self.process.stdout:
            self.stdout_lines.append(line)
            self.responses.put(line)

    def _read_stderr(self):
        self.stderr_lines.extend(self.process.stderr)

    def send(self, message, overflow=None):
        line = json.dumps(message, allow_nan=False)
        if overflow is not None:
            line = line.replace('"OVERFLOW_NUMBER"', overflow)
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def request(self, method, params=None, *, request_id=None, overflow=None):
        self.next_id += 1
        identifier = self.next_id if request_id is None else request_id
        message = {"jsonrpc": "2.0", "id": identifier, "method": method}
        if params is not None:
            message["params"] = params
        self.send(message, overflow)
        try:
            response = json.loads(self.responses.get(timeout=5))
        except queue.Empty:
            pytest.fail(f"No response for request {identifier}; stderr={self.stderr_lines!r}")
        assert response["jsonrpc"] == "2.0"
        assert response["id"] == identifier
        assert ("result" in response) != ("error" in response)
        return response

    def initialize(self):
        response = self.request(
            "initialize",
            {
                "protocolVersion": LATEST_HANDSHAKE_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "assessment-wire-tests", "version": "1"},
            },
        )
        assert response["result"]["protocolVersion"] == LATEST_HANDSHAKE_VERSION
        assert "tools" in response["result"]["capabilities"]
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def call(self, name, arguments, **kwargs):
        return self.request("tools/call", {"name": name, "arguments": arguments}, **kwargs)

    def close(self):
        if not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        for reader in self.readers:
            reader.join(timeout=2)
            assert not reader.is_alive()
        self.process.stdout.close()
        self.process.stderr.close()

    def assert_streams(self):
        assert self.stdout_lines
        for line in self.stdout_lines:
            message = json.loads(line)
            assert isinstance(message, dict)
            assert message["jsonrpc"] == "2.0"
            assert "method" in message or "id" in message
        assert "Starting mock customer and refund MCP server" in "".join(self.stderr_lines)


@contextmanager
def running_server(command=None):
    client = WireClient(command)
    try:
        client.initialize()
        yield client
    finally:
        client.close()
        client.assert_streams()


@pytest.fixture(scope="module")
def wire():
    with running_server() as client:
        yield client
    assert "Rejected invalid arguments" in "".join(client.stderr_lines)


def data(response):
    assert "error" not in response
    result = response["result"]
    payload = result["structuredContent"]
    assert json.loads(result["content"][0]["text"]) == payload
    return result, payload


def assert_invalid_and_recover(wire, name, arguments, **kwargs):
    response = wire.call(name, arguments, **kwargs)
    assert response["error"]["code"] == -32602
    assert "result" not in response
    assert set(response["error"]) <= {"code", "message", "data"}
    if "data" in response["error"]:
        for detail in response["error"]["data"]:
            assert set(detail) == {"path", "type", "message"}
    result, customer = data(wire.call("get_customer_record", CUSTOMER))
    assert result["isError"] is False
    assert customer["customer_id"] == "CUST-00001"
    return response


def test_discovery(wire):
    tools = wire.request("tools/list")["result"]["tools"]
    assert {tool["name"] for tool in tools} == {"get_customer_record", "trigger_refund"}
    assert len(tools) == 2
    schemas = {tool["name"]: tool["inputSchema"] for tool in tools}
    for schema in schemas.values():
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        customer_id = schema["properties"]["customer_id"]
        assert customer_id["type"] == "string"
        assert customer_id["minLength"] == customer_id["maxLength"] == 10
        assert customer_id["pattern"] == r"^CUST-[0-9]{5}$"
    assert schemas["get_customer_record"]["required"] == ["customer_id"]
    refund = schemas["trigger_refund"]
    assert set(refund["required"]) == {"customer_id", "amount", "reason"}
    assert refund["properties"]["amount"]["type"] == "number"
    assert refund["properties"]["amount"]["exclusiveMinimum"] == 0
    assert refund["properties"]["reason"]["type"] == "string"
    assert refund["properties"]["reason"]["minLength"] == 10


@pytest.mark.parametrize("amount", [1, 12.5])
@pytest.mark.parametrize("reason", ["1234567890", "          ", "Duplicate charge"])
def test_refund_is_mock_and_does_not_mutate_customer(wire, amount, reason):
    before = wire.call("get_customer_record", CUSTOMER)["result"]
    arguments = {**REFUND, "amount": amount, "reason": reason}
    first_result, receipt = data(wire.call("trigger_refund", arguments))
    assert first_result["isError"] is False
    assert receipt == {
        "customer_id": "CUST-00001", "amount": amount, "reason": reason,
        "status": "mock_refund_simulated", "mock": True, "payment_processed": False,
    }
    assert wire.call("trigger_refund", arguments)["result"] == first_result
    assert wire.call("get_customer_record", CUSTOMER)["result"] == before
    assert data({"result": before})[1]["mock"] is True


@pytest.mark.parametrize("name", ["get_customer_record", "trigger_refund"])
def test_unknown_customer_is_business_error(wire, name):
    arguments = dict(CUSTOMER if name == "get_customer_record" else REFUND)
    arguments["customer_id"] = "CUST-99999"
    result, failure = data(wire.call(name, arguments))
    assert result["isError"] is True
    assert failure["code"] == "CUSTOMER_NOT_FOUND"
    assert failure["mock"] is True


@pytest.mark.parametrize("name", ["get_customer_record", "trigger_refund"])
@pytest.mark.parametrize("customer_id", [
    "cust-00001", "CUST-0001", "CUST-000001", "CUST-１２３４５", "CUST-١٢٣٤٥",
    "CUST-00001\n", " CUST-00001", "CUST-0000A", "", None, 1, True, [], {},
])
def test_invalid_customer_id(wire, name, customer_id):
    arguments = dict(CUSTOMER if name == "get_customer_record" else REFUND)
    arguments["customer_id"] = customer_id
    assert_invalid_and_recover(wire, name, arguments)


@pytest.mark.parametrize("amount", [0, -1, -1.5, True, False, "12.5", None, [], {}])
def test_invalid_amount(wire, amount):
    assert_invalid_and_recover(wire, "trigger_refund", {**REFUND, "amount": amount})


@pytest.mark.parametrize("overflow", ["1e999", "-1e999"])
def test_overflow_json_number(wire, overflow):
    assert_invalid_and_recover(
        wire, "trigger_refund", {**REFUND, "amount": "OVERFLOW_NUMBER"}, overflow=overflow
    )


@pytest.mark.parametrize("amount", [float("inf"), float("-inf"), float("nan")])
def test_nonfinite_values_rejected_by_input_model(amount):
    from task1_mcp.server import RefundArgs
    with pytest.raises(ValidationError):
        RefundArgs.model_validate({**REFUND, "amount": amount})


@pytest.mark.parametrize("reason", ["123456789", "", None, 1234567890, True, [], {}])
def test_invalid_reason(wire, reason):
    assert_invalid_and_recover(wire, "trigger_refund", {**REFUND, "reason": reason})


@pytest.mark.parametrize("name", ["get_customer_record", "trigger_refund"])
def test_extra_fields_rejected_and_values_sanitized(wire, name):
    arguments = dict(CUSTOMER if name == "get_customer_record" else REFUND)
    arguments["secret"] = "DO_NOT_ECHO_PAYLOAD"
    response = assert_invalid_and_recover(wire, name, arguments)
    assert "DO_NOT_ECHO_PAYLOAD" not in json.dumps(response)


@pytest.mark.parametrize("name,field", [
    ("get_customer_record", "customer_id"),
    ("trigger_refund", "customer_id"), ("trigger_refund", "amount"), ("trigger_refund", "reason"),
])
def test_missing_required_field(wire, name, field):
    arguments = dict(CUSTOMER if name == "get_customer_record" else REFUND)
    del arguments[field]
    assert_invalid_and_recover(wire, name, arguments)


@pytest.mark.parametrize("name", ["get_customer_record", "trigger_refund"])
@pytest.mark.parametrize("arguments", [None, {}])
def test_null_and_empty_arguments(wire, name, arguments):
    assert_invalid_and_recover(wire, name, arguments)


@pytest.mark.parametrize("name", ["get_customer_record", "trigger_refund"])
def test_omitted_arguments(wire, name):
    response = wire.request("tools/call", {"name": name})
    assert response["error"]["code"] == -32602
    assert "error" not in wire.call("get_customer_record", CUSTOMER)


def test_unknown_tool_and_rpc_method(wire):
    assert_invalid_and_recover(wire, "not_a_tool", {})
    response = wire.request("not/a/method", {}, request_id="unknown-method")
    assert response["error"]["code"] == -32601
    assert "error" not in wire.call("get_customer_record", CUSTOMER)


@pytest.mark.parametrize("request_id", ["string-id", -14])
def test_error_preserves_request_id(wire, request_id):
    assert_invalid_and_recover(wire, "trigger_refund", {**REFUND, "amount": 0}, request_id=request_id)


@pytest.mark.parametrize("failure", [
    "raise RuntimeError('PRIVATE_EXECUTION_DETAIL')",
    "return server.CustomerRecord(customer_id='invalid', name='PRIVATE_EXECUTION_DETAIL')",
])
def test_execution_failures_are_internal_errors(failure):
    code = (
        "import asyncio\n"
        "from task1_mcp import server\n"
        f"def broken(args):\n    {failure}\n"
        "server.get_customer_record = broken\n"
        "asyncio.run(server.main())\n"
    )
    with running_server([sys.executable, "-c", code]) as client:
        response = client.call("get_customer_record", CUSTOMER, request_id="broken-output")
        assert response["error"] == {"code": -32603, "message": "Tool execution failed"}
        assert "PRIVATE_EXECUTION_DETAIL" not in json.dumps(response)
        result, receipt = data(client.call("trigger_refund", REFUND))
        assert result["isError"] is False
        assert receipt["payment_processed"] is False
    assert "Tool execution failed" in "".join(client.stderr_lines)
