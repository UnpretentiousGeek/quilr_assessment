import argparse
import json
from pathlib import Path

import anyio
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route


def create_app(mode: str = "success", record_file: Path | None = None) -> Starlette:
    async def complete(request: Request):
        body = await request.json()
        if record_file is not None:
            record = {"body": body, "authorization": request.headers.get("authorization"),
                      "cookie": request.headers.get("cookie"), "role": request.headers.get("x-role")}
            with record_file.open("a", encoding="utf-8") as recorder:
                recorder.write(json.dumps(record) + "\n")
        if mode == "429":
            return JSONResponse({"error": "private upstream throttling detail"}, status_code=429, headers={"X-Private": "secret"})
        if mode == "error":
            return JSONResponse({"stack": "private upstream stack"}, status_code=500)
        if mode == "malformed":
            return Response(b'{"usage":', media_type="application/json")
        if mode == "oversized":
            return Response(b"x" * 140_000, media_type="application/json")
        if mode == "timeout":
            await anyio.sleep(10)
        if mode == "body-timeout":
            async def stalled():
                yield b'{"choices":'
                await anyio.sleep(10)
                yield b"[]}"
            return StreamingResponse(stalled(), media_type="application/json")
        generated = min(4, body.get("max_tokens", 4))
        prompt = 100_000 if mode == "overage" else 12
        return JSONResponse({
            "id": "private-provider-id", "model": "private-model",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "Local completion."}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": prompt, "completion_tokens": generated, "total_tokens": prompt + generated},
        }, headers={"X-Private": "secret", "Set-Cookie": "upstream=private"})

    return Starlette(routes=[Route("/complete", complete, methods=["POST"])])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8006)
    parser.add_argument("--fd", type=int)
    parser.add_argument("--record-file", type=Path)
    parser.add_argument("--mode", choices=["success", "429", "timeout", "body-timeout", "error", "malformed", "oversized", "overage"], default="success")
    arguments = parser.parse_args()
    uvicorn.run(create_app(arguments.mode, arguments.record_file), host=arguments.host, port=arguments.port,
                fd=arguments.fd, log_level="warning", access_log=False, proxy_headers=False)


if __name__ == "__main__":
    main()
