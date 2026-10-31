import argparse
import hmac
import os
import sys
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType

import anyio
import httpx2
import uvicorn
from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .limiter import MAX_ACCOUNTED_TOKENS, LimiterUnavailable, RateLimited, SQLiteLimiter, key_bucket
from .router import CompletionRequest, GatewayError, Provider, Router, strict_json


MAX_BODY_BYTES = 65_536
BODY_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class Tenant:
    tenant_id: str
    tokens_per_minute: int = 50_000

    def __post_init__(self):
        if not isinstance(self.tenant_id, str) or not 1 <= len(self.tenant_id) <= 128 or any(
            ord(char) < 32 or ord(char) == 127 for char in self.tenant_id
        ):
            raise ValueError("Invalid tenant identity")
        if type(self.tokens_per_minute) is not int or not 1 <= self.tokens_per_minute <= MAX_ACCOUNTED_TOKENS:
            raise ValueError("Invalid token limit")


@dataclass(frozen=True)
class Config:
    primary: Provider
    secondary: Provider
    tenants: Mapping[str, Tenant]
    db_path: str

    def __post_init__(self):
        if not isinstance(self.primary, Provider) or not isinstance(self.secondary, Provider):
            raise ValueError("Invalid provider configuration")
        if not isinstance(self.tenants, Mapping) or not self.tenants:
            raise ValueError("At least one tenant API key is required")
        for key, tenant in self.tenants.items():
            if (
                not isinstance(key, str) or not 32 <= len(key) <= 512
                or any(not 33 <= ord(char) <= 126 for char in key)
                or not isinstance(tenant, Tenant)
            ):
                raise ValueError("API keys must contain at least 32 visible ASCII characters")
        SQLiteLimiter(self.db_path)
        object.__setattr__(self, "tenants", MappingProxyType(dict(self.tenants)))


def load_config(environ: Mapping[str, str]) -> Config:
    try:
        raw = strict_json(environ["ROUTER_TENANTS_JSON"].encode("utf-8"))
        if not isinstance(raw, dict) or not raw:
            raise ValueError()
        tenants = {}
        for key, value in raw.items():
            if not isinstance(value, dict) or not {"tenant_id"} <= set(value) <= {"tenant_id", "tokens_per_minute"}:
                raise ValueError()
            tenants[key] = Tenant(**value)
        return Config(
            Provider(environ["ROUTER_PRIMARY_URL"], environ.get("ROUTER_PRIMARY_MODEL", "demo-primary"), environ.get("ROUTER_PRIMARY_BEARER")),
            Provider(environ["ROUTER_SECONDARY_URL"], environ.get("ROUTER_SECONDARY_MODEL", "demo-secondary"), environ.get("ROUTER_SECONDARY_BEARER")),
            tenants, environ["ROUTER_DB_PATH"],
        )
    except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
        raise ValueError("Invalid router startup configuration") from None


def error_response(error: GatewayError) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    if error.status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return JSONResponse({"error": {"code": error.code, "message": error.message}}, status_code=error.status, headers=headers)


def authenticate(request: Request, config: Config) -> tuple[str, Tenant]:
    headers = request.headers.getlist("authorization")
    if len(headers) != 1:
        raise GatewayError(401, "unauthorized", "A valid API key is required.")
    parts = headers[0].split(" ")
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise GatewayError(401, "unauthorized", "A valid API key is required.")
    supplied = parts[1]
    if not supplied.isascii():
        raise GatewayError(401, "unauthorized", "A valid API key is required.")
    found = None
    for key, tenant in config.tenants.items():
        if hmac.compare_digest(key, supplied):
            found = (key, tenant)
    if found is None:
        raise GatewayError(401, "unauthorized", "A valid API key is required.")
    return found


async def read_generation(request: Request) -> CompletionRequest:
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise GatewayError(415, "unsupported_media_type", "Use application/json.")
    lengths = request.headers.getlist("content-length")
    if len(lengths) > 1 or lengths and (not lengths[0].isascii() or not lengths[0].isdigit()):
        raise GatewayError(400, "invalid_request", "The completion request is invalid.")
    if lengths and (len(lengths[0]) > 10 or int(lengths[0]) > MAX_BODY_BYTES):
        raise GatewayError(413, "request_too_large", "The completion request is too large.")
    body = bytearray()
    try:
        with anyio.fail_after(BODY_TIMEOUT_SECONDS):
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    raise GatewayError(413, "request_too_large", "The completion request is too large.")
                body.extend(chunk)
    except TimeoutError:
        raise GatewayError(408, "request_timeout", "The completion request timed out.") from None
    try:
        generation = CompletionRequest.model_validate(strict_json(bytes(body)))
        generation.reservation()
        return generation
    except (ValueError, TypeError, ValidationError, UnicodeError, RecursionError):
        raise GatewayError(400, "invalid_request", "The completion request is invalid.") from None


class CompletionResponse(JSONResponse):
    def __init__(self, router: Router, bucket: str, limit: int, generation: CompletionRequest):
        super().__init__({})
        self.router, self.bucket, self.limit, self.generation = router, bucket, limit, generation

    async def __call__(self, scope, receive, send):
        disconnected = False

        async def watch_disconnect(cancel_scope):
            nonlocal disconnected
            while True:
                if (await receive())["type"] == "http.disconnect":
                    disconnected = True
                    cancel_scope.cancel()
                    return

        async with anyio.create_task_group() as group:
            group.start_soon(watch_disconnect, group.cancel_scope)
            try:
                completed = await self.router.complete(self.bucket, self.limit, self.generation)
                result = JSONResponse(completed.payload(), headers={"Cache-Control": "no-store"})
            except RateLimited:
                result = error_response(GatewayError(429, "rate_limited", "The token budget is exhausted."))
            except LimiterUnavailable:
                result = error_response(GatewayError(503, "limiter_unavailable", "The token budget service is unavailable."))
            except GatewayError as error:
                result = error_response(error)
            except Exception:
                result = error_response(GatewayError(502, "provider_error", "The provider request failed."))
            group.cancel_scope.cancel()
        if not disconnected:
            await result(scope, receive, send)


def create_app(config: Config, *, transport: httpx2.AsyncBaseTransport | None = None, limiter: SQLiteLimiter | None = None) -> Starlette:
    ledger = limiter or SQLiteLimiter(config.db_path)
    router = Router(ledger, config.primary, config.secondary, transport=transport)

    @asynccontextmanager
    async def lifespan(app):
        await ledger.initialize()
        yield

    async def complete(request: Request):
        try:
            key, tenant = authenticate(request, config)
            generation = await read_generation(request)
            return CompletionResponse(router, key_bucket(key), tenant.tokens_per_minute, generation)
        except GatewayError as error:
            return error_response(error)
        except ClientDisconnect:
            return error_response(GatewayError(400, "invalid_request", "The completion request is invalid."))

    async def http_error(request, error):
        if error.status_code == 405:
            return error_response(GatewayError(405, "method_not_allowed", "The request method is not supported."))
        return error_response(GatewayError(404, "not_found", "The requested endpoint does not exist."))

    async def unexpected_error(request, error):
        return error_response(GatewayError(500, "gateway_error", "The gateway request failed."))

    app = Starlette(routes=[Route("/complete", complete, methods=["POST"])], lifespan=lifespan, exception_handlers={HTTPException: http_error, Exception: unexpected_error})
    app.router.redirect_slashes = False
    app.state.limiter, app.state.router = ledger, router
    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8005)
    parser.add_argument("--fd", type=int)
    arguments = parser.parse_args()
    try:
        config = load_config(os.environ)
    except ValueError:
        print("Invalid router startup configuration.", file=sys.stderr)
        raise SystemExit(2) from None
    uvicorn.run(create_app(config), host=arguments.host, port=arguments.port, fd=arguments.fd, log_level="warning", access_log=False, proxy_headers=False)


if __name__ == "__main__":
    main()
