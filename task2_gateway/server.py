import argparse
import hmac
import ipaddress
import json
import math
import os
import re
import sys
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, Mapping
from urllib.parse import urlsplit

import httpx2
import uvicorn
from starlette.applications import Starlette
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

Role = Literal["viewer", "admin"]
RpcId = str | int
MAX_BODY_BYTES = 65_536
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{32,256}\Z")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError("Non-finite JSON number")


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    return number


def _check_unicode(value):
    if isinstance(value, str):
        value.encode("utf-8")
    elif isinstance(value, dict):
        for key, item in value.items():
            _check_unicode(key)
            _check_unicode(item)
    elif isinstance(value, list):
        for item in value:
            _check_unicode(item)


def strict_json(raw: bytes) -> Any:
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
        parse_float=_finite_float,
    )
    _check_unicode(value)
    return value


@dataclass(frozen=True)
class Config:
    upstream_url: str
    token_roles: Mapping[str, Role]
    upstream_bearer: str | None = None
    timeout_seconds: float = 5.0

    def __post_init__(self):
        try:
            url = urlsplit(self.upstream_url)
            parsed_url = httpx2.URL(self.upstream_url)
            host = url.hostname
            port = url.port
            loopback = host == "localhost"
            if host and not loopback:
                try:
                    loopback = ipaddress.ip_address(host).is_loopback
                except ValueError:
                    pass
            valid = (
                url.scheme in {"http", "https"}
                and host
                and parsed_url.host
                and not url.username
                and url.password is None
                and "@" not in url.netloc
                and "#" not in self.upstream_url
                and not any(char.isspace() or ord(char) < 32 for char in self.upstream_url)
                and (url.scheme == "https" or loopback)
                and (port is None or port > 0)
            )
        except (TypeError, ValueError, httpx2.InvalidURL):
            valid = False
        if not valid:
            raise ValueError("GATEWAY_UPSTREAM_URL must be HTTPS or loopback HTTP without credentials or fragments")
        if not isinstance(self.token_roles, Mapping) or not self.token_roles:
            raise ValueError("GATEWAY_TOKEN_ROLES must be a nonempty token-to-role object")
        roles = dict(self.token_roles)
        for token, role in roles.items():
            if not isinstance(token, str) or TOKEN_PATTERN.fullmatch(token) is None:
                raise ValueError("Gateway tokens must have 32 to 256 URL-safe ASCII characters")
            if not isinstance(role, str) or role not in {"viewer", "admin"}:
                raise ValueError("Gateway roles must be viewer or admin")
        if self.upstream_bearer is not None and (
            not isinstance(self.upstream_bearer, str)
            or not self.upstream_bearer
            or any(ord(char) < 33 or ord(char) > 126 for char in self.upstream_bearer)
        ):
            raise ValueError("GATEWAY_UPSTREAM_BEARER must be a nonempty visible ASCII credential")
        if (
            type(self.timeout_seconds) not in {int, float}
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("GATEWAY_TIMEOUT_SECONDS must be finite and positive")
        object.__setattr__(self, "token_roles", MappingProxyType(roles))


def load_config(environ: Mapping[str, str]) -> Config:
    try:
        upstream = environ["GATEWAY_UPSTREAM_URL"]
        roles = strict_json(environ["GATEWAY_TOKEN_ROLES"].encode("utf-8"))
        timeout = float(environ.get("GATEWAY_TIMEOUT_SECONDS", "5"))
    except KeyError as error:
        raise ValueError(f"Missing {error.args[0]}") from None
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("Invalid gateway JSON or timeout configuration") from None
    return Config(upstream, roles, environ.get("GATEWAY_UPSTREAM_BEARER"), timeout)


@dataclass(frozen=True)
class RpcMessage:
    raw: bytes
    method: str
    params: dict[str, Any] | list[Any] | None
    has_id: bool
    request_id: RpcId | None


class ProtocolError(Exception):
    def __init__(self, code: int, message: str, request_id: RpcId | None = None):
        self.code = code
        self.message = message
        self.request_id = request_id


def parse_message(raw: bytes) -> RpcMessage:
    try:
        value = strict_json(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise ProtocolError(-32700, "Parse error") from None
    if not isinstance(value, dict):
        raise ProtocolError(-32600, "Invalid Request")
    has_id = "id" in value
    request_id = value.get("id")
    if has_id and type(request_id) not in {str, int}:
        raise ProtocolError(-32600, "Invalid Request")
    if (
        value.get("jsonrpc") != "2.0"
        or not isinstance(value.get("method"), str)
        or not value["method"]
        or ("params" in value and not isinstance(value["params"], (dict, list)))
        or "result" in value
        or "error" in value
    ):
        raise ProtocolError(-32600, "Invalid Request", request_id)
    return RpcMessage(raw, value["method"], value.get("params"), has_id, request_id)


def authenticate(request: Request, config: Config) -> Role | None:
    values = request.headers.getlist("authorization")
    if len(values) != 1:
        return None
    match = re.fullmatch(r"(?i:Bearer) ([A-Za-z0-9_-]{32,256})", values[0])
    if match is None:
        return None
    credential = match.group(1)
    role = None
    for token, candidate in config.token_roles.items():
        if hmac.compare_digest(token, credential):
            role = candidate
    return role


def authorize(message: RpcMessage, role: Role) -> None:
    if message.method == "tools/call":
        params = message.params
        if not isinstance(params, dict) or not isinstance(params.get("name"), str) or not params["name"]:
            raise ProtocolError(-32602, "Invalid params", message.request_id)
        if params["name"].startswith("admin_") and role != "admin":
            raise ProtocolError(-32001, "Unauthorized Tool Call", message.request_id)


def rpc_error(
    request_id: RpcId | None,
    code: int,
    message: str,
    status: int = 200,
    *,
    notification: bool = False,
    headers: dict[str, str] | None = None,
) -> Response:
    if notification:
        return Response(status_code=204 if status < 400 else status, headers=headers)
    return JSONResponse(
        {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}},
        status_code=status,
        headers=headers,
    )


def valid_response(raw: bytes, message: RpcMessage) -> bool:
    try:
        value = strict_json(raw)
    except (ValueError, UnicodeError, RecursionError):
        return False
    if (
        not isinstance(value, dict)
        or value.get("jsonrpc") != "2.0"
        or "id" not in value
        or type(value["id"]) is not type(message.request_id)
        or value["id"] != message.request_id
        or "method" in value
        or (("result" in value) == ("error" in value))
    ):
        return False
    if "error" in value:
        error = value["error"]
        return (
            isinstance(error, dict)
            and type(error.get("code")) is int
            and isinstance(error.get("message"), str)
        )
    return True


async def forward(
    message: RpcMessage, config: Config, transport: httpx2.AsyncBaseTransport | None
) -> Response:
    headers = {"Content-Type": "application/json", "Accept": "application/json", "Accept-Encoding": "identity"}
    if config.upstream_bearer is not None:
        headers["Authorization"] = f"Bearer {config.upstream_bearer}"
    status = 502
    try:
        async with httpx2.AsyncClient(
            timeout=httpx2.Timeout(config.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        ) as client:
            upstream = await client.post(config.upstream_url, content=message.raw, headers=headers)
        if 300 <= upstream.status_code < 400:
            raise ValueError("Upstream redirect")
        if not message.has_id:
            return Response(status_code=204 if upstream.status_code < 400 else upstream.status_code)
        if (
            upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json"
            or not valid_response(upstream.content, message)
        ):
            raise ValueError("Invalid upstream response")
        return Response(
            upstream.content,
            status_code=upstream.status_code,
            headers={"Content-Type": upstream.headers["content-type"]},
        )
    except httpx2.TimeoutException:
        status = 504
    except (httpx2.HTTPError, ValueError):
        pass
    return rpc_error(
        message.request_id, -32002, "Upstream request failed", status,
        notification=not message.has_id,
    )


def create_app(config: Config, *, transport: httpx2.AsyncBaseTransport | None = None) -> Starlette:
    async def proxy(request: Request) -> Response:
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            return rpc_error(None, -32600, "Content-Type must be application/json", 415)
        raw = bytearray()
        try:
            async for chunk in request.stream():
                if len(raw) + len(chunk) > MAX_BODY_BYTES:
                    return rpc_error(None, -32600, "Request body too large", 413)
                raw.extend(chunk)
        except ClientDisconnect:
            return rpc_error(None, -32600, "Invalid Request", 400)
        try:
            message = parse_message(bytes(raw))
        except ProtocolError as error:
            return rpc_error(error.request_id, error.code, error.message, 400)
        role = authenticate(request, config)
        if role is None:
            return rpc_error(
                message.request_id, -32000, "Unauthorized", 401,
                notification=not message.has_id, headers={"WWW-Authenticate": "Bearer"},
            )
        try:
            authorize(message, role)
        except ProtocolError as error:
            return rpc_error(
                message.request_id, error.code, error.message,
                notification=not message.has_id,
            )
        return await forward(message, config, transport)

    app = Starlette(routes=[Route("/mcp", proxy, methods=["POST"])])
    app.router.redirect_slashes = False
    return app


def main():
    parser = argparse.ArgumentParser(description="Single-message JSON-RPC security gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--fd", type=int)
    args = parser.parse_args()
    try:
        config = load_config(os.environ)
    except ValueError as error:
        print(f"Invalid gateway configuration: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    uvicorn.run(
        create_app(config), host=args.host, port=args.port, fd=args.fd,
        access_log=False, proxy_headers=False,
    )


if __name__ == "__main__":
    main()
