import re
from collections.abc import Iterator
from enum import Enum, auto


REPLACEMENT = "[REDACTED]"
MAX_CANDIDATE_CHARS = 320
MAX_OUTPUT_CHARS = 1024
ATOM = r"A-Za-z0-9!#$%&'*+/=?^_`{|}~\-"
TOKEN_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.!#$%&'*+/=?^_`{|}~@-")
EMAIL = re.compile(
    rf"(?<![{ATOM}.])[{ATOM}]+(?:\.[{ATOM}]+)*@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+"
    r"(?![A-Za-z0-9-])"
)
NUMERIC = re.compile(r"[0-9](?:[0-9 -]*[0-9])?")
SSN = re.compile(r"(?:[0-9]{9}|[0-9]{3}-[0-9]{2}-[0-9]{4})\Z")


class RedactionError(ValueError):
    pass


class CandidateState(Enum):
    EMPTY = auto()
    TOKEN = auto()
    NUMERIC_GAP = auto()
    CLOSED = auto()


def _numbers(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        value = match.group()
        count = sum("0" <= char <= "9" for char in value)
        return REPLACEMENT if SSN.fullmatch(value) or 13 <= count <= 19 else value

    return NUMERIC.sub(replace, text)


def redact_candidate(text: str) -> str:
    result = []
    position = 0
    for match in EMAIL.finditer(text):
        address = match.group()
        local, domain = address.rsplit("@", 1)
        if len(local) > 64 or len(address) > 254 or any(len(label) > 63 for label in domain.split(".")):
            raise RedactionError("Email size limit exceeded")
        result.extend((_numbers(text[position:match.start()]), REPLACEMENT))
        position = match.end()
    result.append(_numbers(text[position:]))
    return "".join(result)


class StreamingRedactor:
    def __init__(self) -> None:
        self.state = CandidateState.EMPTY
        self.pending = ""
        self.max_retained_chars = 0

    @property
    def retained_chars(self) -> int:
        return len(self.pending)

    def _append(self, char: str) -> None:
        if len(self.pending) >= MAX_CANDIDATE_CHARS:
            self.discard()
            raise RedactionError("Candidate limit exceeded")
        self.pending += char
        self.max_retained_chars = max(self.max_retained_chars, len(self.pending))

    def _release(self) -> str:
        try:
            text = redact_candidate(self.pending)
        except RedactionError:
            self.discard()
            raise
        self.pending = ""
        self.state = CandidateState.EMPTY
        return text

    def feed(self, text: str) -> Iterator[str]:
        if self.state is CandidateState.CLOSED:
            raise RedactionError("Redactor is closed")
        output = ""
        for char in text:
            if self.state is CandidateState.NUMERIC_GAP:
                if char in " -":
                    self._append(char)
                    continue
                if "0" <= char <= "9":
                    self._append(char)
                    self.state = CandidateState.TOKEN
                    continue
                prefix, space, suffix = self.pending.rpartition(" ")
                self.pending = prefix + space
                output += self._release()
                self.pending = suffix
            if char in TOKEN_CHARS:
                self._append(char)
                self.state = CandidateState.TOKEN
            elif char == " " and self.pending and re.search(r"[0-9][0-9-]*\Z", self.pending):
                self._append(char)
                self.state = CandidateState.NUMERIC_GAP
            else:
                output += self._release() + char
            if len(output) >= MAX_OUTPUT_CHARS:
                yield output
                output = ""
        if output:
            yield output

    def finish(self) -> str:
        if self.state is CandidateState.CLOSED:
            raise RedactionError("Redactor is closed")
        text = self._release()
        self.state = CandidateState.CLOSED
        return text

    def discard(self) -> None:
        self.pending = ""
        self.state = CandidateState.CLOSED
