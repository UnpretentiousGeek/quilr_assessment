import argparse
import ipaddress
import math
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from typing import Annotated, Literal
from urllib.parse import urlsplit

import anyio
import httpx2
import uvicorn
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.applications import Starlette
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .protocol import ChatStream, Completed, DONE_EVENT, ERROR_EVENT, SSEDecoder, StreamError, strict_json, text_event
from .redaction import RedactionError, StreamingRedactor


MAX_BODY_BYTES = 65536


@dataclass(frozen=True)
class Config:
    upstream_url: str
    upstream_bearer: str | None = None
    idle_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        try:
            url = urlsplit(self.upstream_url)
            parsed = httpx2.URL(self.upstream_url)
            host = url.hostname
            loopback = host == "localhost"
            if host and not loopback:
                try:
                    loopback = ipaddress.ip_address(host).is_loopback
                except ValueError:
                    pass
            valid = (
                url.scheme in {"http", "https"} and host and parsed.host
                and url.username is None and url.password is None and "@" not in url.netloc
                and "#" not in self.upstream_url
                and not any(char.isspace() or ord(char) < 32 for char in self.upstream_url)
                and (url.scheme == "https" or loopback)
                and (url.port is None or url.port > 0)
            )
        except (ValueError, TypeError, httpx2.InvalidURL):
            valid = False
        if not valid:
            raise ValueError("LLM_UPSTREAM_URL must be HTTPS or loopback HTTP without credentials or fragments")
        if self.upstream_bearer is not None and (
            not isinstance(self.upstream_bearer, str)
            or not 1 <= len(self.upstream_bearer) <= 4096
            or any(not 33 <= ord(char) <= 126 for char in self.upstream_bearer)
        ):
            raise ValueError("LLM_UPSTREAM_BEARER must contain visible ASCII characters")
        if (
            type(self.idle_timeout_seconds) not in {int, float}
            or not math.isfinite(self.idle_timeout_seconds)
            or self.idle_timeout_seconds <= 0
        ):
            raise ValueError("LLM_IDLE_TIMEOUT_SECONDS must be finite and positive")


def load_config(environ: Mapping[str, str]) -> Config:
    if "LLM_UPSTREAM_URL" not in environ:
        raise ValueError("Missing LLM_UPSTREAM_URL")
    try:
        timeout = float(environ.get("LLM_IDLE_TIMEOUT_SECONDS", "5"))
    except (ValueError, TypeError):
        raise ValueError("LLM_IDLE_TIMEOUT_SECONDS must be finite and positive") from None
    return Config(environ["LLM_UPSTREAM_URL"], environ.get("LLM_UPSTREAM_BEARER"), timeout)


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    role: Literal["system", "user", "assistant"]
    content: Annotated[str, Field(max_length=16384)]


class GenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    model: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")]
    messages: Annotated[list[Message], Field(min_length=1, max_length=128)]


@dataclass
class StreamMetrics:
    upstream_bytes: int = 0
    emitted_text_chars: int = 0
    max_candidate_chars: int = 0
    max_line_chars: int = 0
    max_event_chars: int = 0
    max_chunk_bytes: int = 0


class ResponseState(Enum):
    PREPARING = auto()
    STREAMING = auto()
    FINISHED = auto()


class GatewayResponse(Response):
    def __init__(
        self, config: Config, generation: GenerationRequest,
        transport: httpx2.AsyncBaseTransport | None = None,
        metrics_sink: Callable[[StreamMetrics], None] | None = None,
    ) -> None:
        super().__init__(headers={"Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-store", "X-Accel-Buffering": "no"})
        self.raw_headers = [(key, value) for key, value in self.raw_headers if key != b"content-length"]
        self.config = config
        self.generation = generation
        self.transport = transport
        self.metrics_sink = metrics_sink
        self.metrics = StreamMetrics()
        self.state = ResponseState.PREPARING
        self.client: httpx2.AsyncClient | None = None
        self.upstream: httpx2.Response | None = None
        self.chunks = None
        self.redactor = StreamingRedactor()
        self.decoder = SSEDecoder()

    async def _start(self, send, status: int = 200) -> None:
        await self._send(send, {"type": "http.response.start", "status": status, "headers": self.raw_headers})
        self.state = ResponseState.STREAMING

    @staticmethod
    async def _send(send, message) -> None:
        try:
            await send(message)
        except Exception:
            raise ClientDisconnect() from None

    async def _body(self, send, data: bytes) -> None:
        await self._send(send, {"type": "http.response.body", "body": data, "more_body": True})

    async def _text(self, send, text: str) -> None:
        if text:
            self.metrics.emitted_text_chars += len(text)
            await self._body(send, text_event(text))

    async def _produce(self, send) -> None:
        chat = ChatStream()
        try:
            self.client = httpx2.AsyncClient(
                timeout=httpx2.Timeout(self.config.idle_timeout_seconds),
                follow_redirects=False, trust_env=False, transport=self.transport,
            )
            headers = {"Content-Type": "application/json", "Accept": "text/event-stream", "Accept-Encoding": "identity"}
            if self.config.upstream_bearer is not None:
                headers["Authorization"] = "Bearer " + self.config.upstream_bearer
            outgoing = self.client.build_request(
                "POST", self.config.upstream_url, headers=headers,
                json={**self.generation.model_dump(), "stream": True, "n": 1},
            )
            with anyio.fail_after(self.config.idle_timeout_seconds):
                self.upstream = await self.client.send(outgoing, stream=True)
            if (
                self.upstream.status_code != 200
                or self.upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "text/event-stream"
                or self.upstream.headers.get("content-encoding", "identity").strip().lower() != "identity"
            ):
                raise StreamError("Unsupported upstream response")
            await self._start(send)
            self.chunks = self.upstream.aiter_raw().__aiter__()
            while True:
                try:
                    with anyio.fail_after(self.config.idle_timeout_seconds):
                        raw = await anext(self.chunks)
                except StopAsyncIteration:
                    self.decoder.eof()
                    raise StreamError("Missing completion marker") from None
                self.metrics.upstream_bytes += len(raw)
                self.metrics.max_chunk_bytes = max(self.metrics.max_chunk_bytes, len(raw))
                for data in self.decoder.feed(raw):
                    event = chat.consume(data)
                    if isinstance(event, Completed):
                        await self._text(send, self.redactor.finish())
                        await self._body(send, DONE_EVENT)
                        await self._send(send, {"type": "http.response.body", "body": b"", "more_body": False})
                        self.state = ResponseState.FINISHED
                        return
                    for text in self.redactor.feed(event.text):
                        await self._text(send, text)
        except ClientDisconnect:
            raise
        except Exception as error:
            self.redactor.discard()
            if self.state is ResponseState.PREPARING:
                await self._start(send, 504 if isinstance(error, (TimeoutError, httpx2.TimeoutException)) else 502)
            await self._body(send, ERROR_EVENT)
            await self._send(send, {"type": "http.response.body", "body": b"", "more_body": False})
            self.state = ResponseState.FINISHED

    async def __call__(self, scope, receive, send) -> None:
        try:
            async with anyio.create_task_group() as group:
                async def disconnected() -> None:
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            group.cancel_scope.cancel()
                            return

                group.start_soon(disconnected)
                try:
                    await self._produce(send)
                finally:
                    group.cancel_scope.cancel()
        except* ClientDisconnect:
            pass
        finally:
            self.metrics.max_candidate_chars = self.redactor.max_retained_chars
            self.metrics.max_line_chars = self.decoder.max_line_chars
            self.metrics.max_event_chars = self.decoder.max_event_chars
            self.redactor.discard()
            with anyio.CancelScope(shield=True):
                try:
                    try:
                        if self.chunks is not None:
                            await self.chunks.aclose()
                    finally:
                        if self.upstream is not None:
                            # Cancelled httpx2 iteration can mark a response closed before its stream finishes closing.
                            await self.upstream.stream.aclose()
                            await self.upstream.aclose()
                finally:
                    if self.client is not None:
                        await self.client.aclose()
            if self.metrics_sink is not None:
                self.metrics_sink(self.metrics)


def create_app(
    config: Config, *, transport: httpx2.AsyncBaseTransport | None = None,
    metrics_sink: Callable[[StreamMetrics], None] | None = None,
) -> Starlette:
    async def generate(request: Request) -> Response:
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            return JSONResponse({"error": "Content-Type must be application/json"}, status_code=415)
        raw = bytearray()
        try:
            stream = request.stream().__aiter__()
            while True:
                try:
                    with anyio.fail_after(config.idle_timeout_seconds):
                        chunk = await anext(stream)
                except StopAsyncIteration:
                    break
                if len(raw) + len(chunk) > MAX_BODY_BYTES:
                    return JSONResponse({"error": "Request body too large"}, status_code=413)
                raw.extend(chunk)
        except TimeoutError:
            return JSONResponse({"error": "Request timed out"}, status_code=408)
        except ClientDisconnect:
            return JSONResponse({"error": "Invalid request"}, status_code=400)
        try:
            generation = GenerationRequest.model_validate(strict_json(bytes(raw)))
            for message in generation.messages:
                message.content.encode("utf-8", errors="strict")
        except (ValidationError, StreamError, UnicodeError):
            return JSONResponse({"error": "Invalid request"}, status_code=400)
        return GatewayResponse(config, generation, transport, metrics_sink)

    app = Starlette(routes=[Route("/generate", generate, methods=["POST"])])
    app.router.redirect_slashes = False
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming text gateway with bounded PII redaction")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8003)
    parser.add_argument("--fd", type=int)
    args = parser.parse_args()
    try:
        config = load_config(os.environ)
    except ValueError as error:
        print(f"Invalid LLM gateway configuration: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    uvicorn.run(create_app(config), host=args.host, port=args.port, fd=args.fd, access_log=False, proxy_headers=False)


if __name__ == "__main__":
    main()
