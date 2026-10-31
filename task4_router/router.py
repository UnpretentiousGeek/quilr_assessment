import ipaddress
import json
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Literal
from urllib.parse import urlsplit

import anyio
import httpx2
from pydantic import BaseModel, ConfigDict, Field

from .limiter import MAX_ACCOUNTED_TOKENS, SQLiteLimiter


PRIMARY_TIMEOUT_SECONDS = 3.0
SECONDARY_TIMEOUT_SECONDS = 3.0
CLEANUP_TIMEOUT_SECONDS = 0.5
MAX_PROVIDER_BYTES = 131_072


class GatewayError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(code)
        self.status, self.code, self.message = status, code, message


def provider_error() -> GatewayError:
    return GatewayError(502, "provider_error", "The provider returned an invalid response.")


def timeout_error() -> GatewayError:
    return GatewayError(504, "provider_timeout", "The provider timed out.")


def strict_json(raw: bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate key")
            result[key] = value
        return result

    def invalid(_):
        raise ValueError("Nonfinite number")

    def floating(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Nonfinite number")
        return parsed

    value = json.loads(
        raw.decode("utf-8", errors="strict"), object_pairs_hook=pairs,
        parse_constant=invalid, parse_float=floating,
    )
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            current.encode("utf-8", errors="strict")
        elif isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, dict):
            pending.extend(current)
            pending.extend(current.values())
    return value


@dataclass(frozen=True)
class Provider:
    url: str
    model: str
    bearer: str | None = None

    def __post_init__(self):
        try:
            url = urlsplit(self.url)
            parsed = httpx2.URL(self.url)
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
                and "#" not in self.url
                and not any(char.isspace() or ord(char) < 32 for char in self.url)
                and (url.scheme == "https" or loopback)
                and (url.port is None or url.port > 0)
            )
        except (ValueError, TypeError, httpx2.InvalidURL):
            valid = False
        if not valid:
            raise ValueError("Provider URL must be HTTPS or loopback HTTP without credentials or fragments")
        if not isinstance(self.model, str) or not 1 <= len(self.model) <= 128 or any(
            not 33 <= ord(char) <= 126 for char in self.model
        ):
            raise ValueError("Provider model must contain visible ASCII")
        if self.bearer is not None and (
            not isinstance(self.bearer, str) or not 1 <= len(self.bearer) <= 4096
            or any(not 33 <= ord(char) <= 126 for char in self.bearer)
        ):
            raise ValueError("Provider bearer must contain visible ASCII")


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    role: Literal["system", "user", "assistant"]
    content: Annotated[str, Field(max_length=16384)]


class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    messages: Annotated[list[Message], Field(min_length=1, max_length=128)]
    max_tokens: Annotated[int, Field(ge=1, le=32768)]

    def reservation(self) -> int:
        """Estimate with UTF-8 message bytes, 8 framing units/message, 4/request.

        This deliberately simple estimator is not a provider tokenizer. Usage
        over the reservation is recorded as debt and the response is rejected.
        """
        return 4 + sum(8 + len(message.content.encode("utf-8")) for message in self.messages) + self.max_tokens


@dataclass(frozen=True)
class Completion:
    text: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    provider: Literal["primary", "secondary"]

    def payload(self) -> dict:
        return {
            "text": self.text,
            "usage": {
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.total_tokens,
            },
            "provider": self.provider,
        }


def parse_completion(raw: bytes, generation: CompletionRequest, label) -> Completion:
    data = strict_json(raw)
    if not isinstance(data, dict):
        raise ValueError("Expected object")
    choices, usage = data.get("choices"), data.get("usage")
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError("Expected one text choice")
    choice = choices[0]
    if (
        not isinstance(choice, dict) or type(choice.get("index")) is not int or choice["index"] != 0
        or choice.get("finish_reason") not in {"stop", "length"}
        or not isinstance(choice.get("message"), dict)
    ):
        raise ValueError("Invalid choice")
    message = choice["message"]
    if set(message) != {"role", "content"} or message["role"] != "assistant" or not isinstance(message["content"], str):
        raise ValueError("Expected assistant text")
    if not isinstance(usage, dict) or set(usage) != {"prompt_tokens", "completion_tokens", "total_tokens"}:
        raise ValueError("Invalid usage")
    if any(type(value) is not int or not 0 <= value <= MAX_ACCOUNTED_TOKENS for value in usage.values()):
        raise ValueError("Invalid usage")
    prompt, generated, total = usage["prompt_tokens"], usage["completion_tokens"], usage["total_tokens"]
    if generated > generation.max_tokens or prompt + generated != total:
        raise ValueError("Invalid usage total")
    return Completion(message["content"], prompt, generated, total, label)


class Router:
    def __init__(
        self, limiter: SQLiteLimiter, primary: Provider, secondary: Provider, *,
        transport: httpx2.AsyncBaseTransport | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.limiter, self.primary, self.secondary = limiter, primary, secondary
        self.transport, self.monotonic = transport, monotonic

    async def complete(self, bucket: str, limit: int, generation: CompletionRequest) -> Completion:
        reserve = generation.reservation()
        primary = await self._attempt(bucket, limit, generation, reserve, self.primary, "primary")
        if primary is not None:
            return primary
        secondary = await self._attempt(bucket, limit, generation, reserve, self.secondary, "secondary")
        if secondary is None:
            raise GatewayError(502, "provider_error", "The provider request failed.")
        return secondary

    async def _attempt(self, bucket, limit, generation, reserve, provider, label):
        admission = await self.limiter.admit(bucket, reserve, limit)
        await anyio.lowlevel.checkpoint()
        client = httpx2.AsyncClient(
            timeout=None, follow_redirects=False,
            trust_env=False, transport=self.transport,
        )
        response = None
        result, failure, fallback = None, None, False
        duration = PRIMARY_TIMEOUT_SECONDS if label == "primary" else SECONDARY_TIMEOUT_SECONDS
        deadline = self.monotonic() + duration
        timeout_scope = None
        try:
            headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
            if provider.bearer is not None:
                headers["Authorization"] = "Bearer " + provider.bearer
            outgoing = client.build_request(
                "POST", provider.url, headers=headers,
                json={"model": provider.model, "messages": [item.model_dump() for item in generation.messages],
                      "max_tokens": generation.max_tokens, "stream": False, "n": 1},
            )
            with anyio.fail_after(max(0.0, deadline - self.monotonic())) as timeout_scope:
                if self.monotonic() >= deadline:
                    raise TimeoutError()
                response = await client.send(outgoing, stream=True)
                if self.monotonic() >= deadline:
                    raise TimeoutError()
                if response.status_code == 429:
                    if label == "primary":
                        fallback = True
                    else:
                        failure = GatewayError(502, "provider_error", "The provider request failed.")
                elif response.status_code != 200:
                    failure = GatewayError(502, "provider_error", "The provider request failed.")
                else:
                    if (
                        response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json"
                        or response.headers.get("content-encoding", "identity").strip().lower() != "identity"
                    ):
                        raise ValueError("Unsupported provider response")
                    body = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(body) + len(chunk) > MAX_PROVIDER_BYTES:
                            raise ValueError("Provider response too large")
                        body.extend(chunk)
                    result = parse_completion(bytes(body), generation, label)
                    if self.monotonic() >= deadline:
                        raise TimeoutError()
        except (TimeoutError, httpx2.HTTPError, ValueError, TypeError, UnicodeError, RecursionError):
            result = None
            expired = (timeout_scope is not None and timeout_scope.cancel_called) or self.monotonic() >= deadline
            if not expired:
                failure = provider_error()
            elif label == "primary":
                fallback = True
            else:
                failure = timeout_error()
        finally:
            with anyio.move_on_after(CLEANUP_TIMEOUT_SECONDS, shield=True) as cleanup:
                try:
                    try:
                        if response is not None:
                            try:
                                await response.aclose()
                            finally:
                                await response.stream.aclose()
                    finally:
                        await client.aclose()
                except Exception:
                    failure = provider_error()
                    fallback = False
            if cleanup.cancel_called:
                failure = provider_error()
                fallback = False
        if failure is not None:
            raise failure
        if fallback:
            return None
        if result is None:
            raise provider_error()
        with anyio.CancelScope(shield=True):
            await self.limiter.settle(admission, result.total_tokens)
        await anyio.lowlevel.checkpoint()
        if result.total_tokens > reserve:
            raise provider_error()
        return result
