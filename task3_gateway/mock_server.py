import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path

import anyio
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route


PREFIX = "Safe prefix. "
FIXTURE = "Email alice@example.com, SSNs 123-45-6789 and 987654321, card 4111 1111 1111 1111. Café ☕! "
EXPECTED = "Email [REDACTED], SSNs [REDACTED] and [REDACTED], card [REDACTED]. Café ☕! "


def frame(text: str = "", *, finish: str | None = None) -> bytes:
    value = {
        "id": "private-provider-metadata", "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": finish}],
    }
    return ("data: " + json.dumps(value, ensure_ascii=False) + "\n\n").encode("utf-8")


@dataclass(frozen=True)
class MockConfig:
    record_file: Path | None = None
    gate_file: Path | None = None
    delay_seconds: float = 0.15
    repeat: int = 1
    chunk_chars: int = 3


def create_app(config: MockConfig = MockConfig()) -> Starlette:
    def record(event: str, **fields) -> None:
        if config.record_file is not None:
            with config.record_file.open("a", encoding="utf-8") as output:
                output.write(json.dumps({"event": event, "time": time.monotonic(), **fields}) + "\n")

    async def completion(request: Request):
        value = await request.json()
        if value.get("stream") is not True or value.get("n") != 1:
            return JSONResponse({"error": "Expected streaming single choice"}, status_code=400)
        record("request", body=value, headers=dict(request.headers))

        async def stream():
            try:
                yield frame(PREFIX)
                record("first_frame")
                if config.gate_file is not None:
                    with anyio.fail_after(10):
                        while not config.gate_file.exists():
                            await anyio.sleep(0.01)
                else:
                    await anyio.sleep(config.delay_seconds)
                for _ in range(config.repeat):
                    for position in range(0, len(FIXTURE), config.chunk_chars):
                        yield frame(FIXTURE[position:position + config.chunk_chars])
                record("completed")
                yield frame(finish="stop")
                yield b"data: [DONE]\n\n"
            finally:
                record("closed")

        return StreamingResponse(stream(), media_type="text/event-stream")

    return Starlette(routes=[Route("/v1/chat/completions", completion, methods=["POST"])])


def main() -> None:
    parser = argparse.ArgumentParser(description="Loopback streaming provider fixture")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8004)
    parser.add_argument("--fd", type=int)
    parser.add_argument("--record-file", type=Path)
    parser.add_argument("--gate-file", type=Path)
    parser.add_argument("--delay-seconds", type=float, default=0.15)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--chunk-chars", type=int, default=3)
    args = parser.parse_args()
    if args.repeat < 1 or args.chunk_chars < 1 or args.delay_seconds < 0:
        parser.error("repeat/chunk-chars must be positive and delay-seconds nonnegative")
    config = MockConfig(args.record_file, args.gate_file, args.delay_seconds, args.repeat, args.chunk_chars)
    uvicorn.run(create_app(config), host=args.host, port=args.port, fd=args.fd, access_log=False, proxy_headers=False)


if __name__ == "__main__":
    main()
