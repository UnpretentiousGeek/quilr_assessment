import codecs
import json
import math
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum, auto


MAX_NETWORK_CHUNK_BYTES = 65536
MAX_LINE_CHARS = 16384
MAX_EVENT_CHARS = 32768
MAX_EVENT_LINES = 64


class StreamError(ValueError):
    pass


def strict_json(raw: str | bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise StreamError("Duplicate JSON key")
            result[key] = value
        return result

    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise StreamError("Nonfinite JSON number")
        return result

    def constant(value):
        raise StreamError("Invalid JSON constant")

    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="strict")
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise StreamError("Invalid JSON") from error


class SSEDecoder:
    def __init__(self) -> None:
        self.decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self.line = ""
        self.data: list[str] = []
        self.event_chars = 0
        self.event_lines = 0
        self.skip_lf = False
        self.first_char = True
        self.max_line_chars = 0
        self.max_event_chars = 0

    @property
    def retained_chars(self) -> int:
        return len(self.line) + sum(map(len, self.data))

    def _line(self) -> str | None:
        line, self.line = self.line, ""
        if not line:
            data = "\n".join(self.data) if self.data else None
            self.data.clear()
            self.event_chars = 0
            self.event_lines = 0
            return data
        self.event_lines += 1
        if self.event_lines > MAX_EVENT_LINES:
            raise StreamError("Too many SSE lines")
        if line.startswith(":"):
            return None
        field, separator, value = line.partition(":")
        if field != "data":
            raise StreamError("Unsupported SSE field")
        if value.startswith(" "):
            value = value[1:]
        self.data.append(value if separator else "")
        return None

    def feed(self, chunk: bytes) -> Iterator[str]:
        if len(chunk) > MAX_NETWORK_CHUNK_BYTES:
            raise StreamError("Network chunk limit exceeded")
        try:
            decoded = self.decoder.decode(chunk, final=False)
        except UnicodeError as error:
            raise StreamError("Invalid UTF-8") from error
        for char in decoded:
            if self.first_char:
                self.first_char = False
                if char == "\ufeff":
                    continue
            if self.skip_lf:
                self.skip_lf = False
                if char == "\n":
                    continue
            if char in "\r\n":
                self.skip_lf = char == "\r"
                event = self._line()
                if event is not None:
                    yield event
                continue
            if len(self.line) >= MAX_LINE_CHARS or self.event_chars >= MAX_EVENT_CHARS:
                raise StreamError("SSE size limit exceeded")
            self.line += char
            self.event_chars += 1
            self.max_line_chars = max(self.max_line_chars, len(self.line))
            self.max_event_chars = max(self.max_event_chars, self.event_chars)

    def eof(self) -> None:
        try:
            self.decoder.decode(b"", final=True)
        except UnicodeError as error:
            raise StreamError("Truncated UTF-8") from error
        if self.line or self.data or self.event_chars:
            raise StreamError("Truncated SSE event")


class CompletionState(Enum):
    STREAMING = auto()
    FINISHED = auto()
    DONE = auto()


@dataclass(frozen=True)
class TextDelta:
    text: str


@dataclass(frozen=True)
class Completed:
    pass


class ChatStream:
    def __init__(self) -> None:
        self.state = CompletionState.STREAMING

    def consume(self, data: str) -> TextDelta | Completed:
        if data.strip() == "[DONE]":
            if self.state is not CompletionState.FINISHED:
                raise StreamError("Completion marker without finish")
            self.state = CompletionState.DONE
            return Completed()
        if self.state is not CompletionState.STREAMING:
            raise StreamError("Data after finish")
        value = strict_json(data)
        allowed = {"id", "object", "created", "model", "choices", "system_fingerprint", "service_tier", "usage", "obfuscation"}
        if not isinstance(value, dict) or set(value) - allowed:
            raise StreamError("Unsupported completion envelope")
        if "object" in value and value["object"] != "chat.completion.chunk":
            raise StreamError("Unsupported completion object")
        if value.get("usage") is not None:
            raise StreamError("Unsupported usage frame")
        choices = value.get("choices")
        if not isinstance(choices, list) or len(choices) != 1:
            raise StreamError("Expected one choice")
        choice = choices[0]
        if (
            not isinstance(choice, dict)
            or set(choice) - {"index", "delta", "finish_reason", "logprobs"}
            or type(choice.get("index")) is not int
            or choice["index"] != 0
            or choice.get("logprobs") is not None
        ):
            raise StreamError("Unsupported choice")
        delta = choice.get("delta")
        if not isinstance(delta, dict) or set(delta) - {"role", "content"}:
            raise StreamError("Unsupported delta")
        if "role" in delta and delta["role"] != "assistant":
            raise StreamError("Unsupported role")
        text = delta.get("content")
        if text is None:
            text = ""
        if not isinstance(text, str):
            raise StreamError("Expected text")
        try:
            text.encode("utf-8", errors="strict")
        except UnicodeError as error:
            raise StreamError("Invalid text Unicode") from error
        reason = choice.get("finish_reason")
        if reason is not None:
            if reason not in ("stop", "length"):
                raise StreamError("Unsupported finish reason")
            self.state = CompletionState.FINISHED
        return TextDelta(text)


def text_event(text: str) -> bytes:
    return ("event: text\ndata: " + json.dumps({"text": text}, ensure_ascii=False) + "\n\n").encode("utf-8")


DONE_EVENT = b"event: done\ndata: {}\n\n"
ERROR_EVENT = b'event: error\ndata: {"error":"Generation failed"}\n\n'
