import argparse
import json
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

TOOLS = [
    {
        "name": "public_echo",
        "description": "Return the supplied arguments as mock text.",
        "inputSchema": {"type": "object", "additionalProperties": True},
    },
    {
        "name": "admin_reset_key",
        "description": "Simulate an administrator key reset. No key is changed.",
        "inputSchema": {"type": "object", "additionalProperties": False},
    },
]


def create_app(record_file: Path | None = None) -> Starlette:
    async def rpc(request: Request) -> Response:
        raw = await request.body()
        if record_file is not None:
            record = {"body": raw.decode("utf-8"), "headers": dict(request.headers)}
            with record_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
        value = json.loads(raw)
        if "id" not in value:
            return Response(status_code=204)
        response = {"jsonrpc": "2.0", "id": value["id"]}
        method = value.get("method")
        if method == "tools/list":
            response["result"] = {"tools": TOOLS, "_meta": {"mock": True, "discovery_marker": "preserve me"}}
        elif method == "initialize":
            response["result"] = {
                "protocolVersion": value.get("params", {}).get("protocolVersion", "2025-11-25"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "task2-local-mock", "version": "1"},
            }
        elif method == "tools/call" and value.get("params", {}).get("name") in {tool["name"] for tool in TOOLS}:
            name = value["params"]["name"]
            payload = {"mock": True, "tool": name, "arguments": value["params"].get("arguments", {})}
            if name == "admin_reset_key":
                payload["key_changed"] = False
            response["result"] = {"content": [{"type": "text", "text": json.dumps(payload)}], "isError": False}
        elif method == "tools/call":
            response["error"] = {"code": -32602, "message": "Unknown tool"}
        else:
            response["error"] = {"code": -32601, "message": "Method not found"}
        return Response(json.dumps(response, indent=2) + "\n", media_type="application/json")

    return Starlette(routes=[Route("/mcp", rpc, methods=["POST"])])


def main():
    parser = argparse.ArgumentParser(description="Local recording JSON-RPC mock")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8002)
    parser.add_argument("--fd", type=int)
    parser.add_argument("--record-file", type=Path)
    args = parser.parse_args()
    uvicorn.run(
        create_app(args.record_file), host=args.host, port=args.port, fd=args.fd,
        access_log=False, proxy_headers=False,
    )


if __name__ == "__main__":
    main()
