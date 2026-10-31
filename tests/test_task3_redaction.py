import random

import pytest

from task3_gateway.redaction import (
    MAX_CANDIDATE_CHARS,
    RedactionError,
    StreamingRedactor,
    redact_candidate,
)


def redact(parts):
    redactor = StreamingRedactor()
    output = "".join(piece for part in parts for piece in redactor.feed(part))
    return output + redactor.finish()


@pytest.mark.parametrize("secret", [
    "alice@example.com", "foo+bar@example.co.uk", "123456789@example.com",
    "a" * 64 + "@" + "b" * 63 + ".example",
    "123-45-6789", "123456789", "000000000",
    *["1" * count for count in range(13, 20)],
    "4111 1111 1111 1111", "4111-1111-1111-1111", "4  111--1111 1111 1111",
])
def test_every_split_and_single_character_deltas(secret):
    text = "Before (" + secret + "), after."
    expected = "Before ([REDACTED]), after."
    for split in range(len(text) + 1):
        assert redact([text[:split], text[split:]]) == expected
    assert redact(text) == expected


@pytest.mark.parametrize("text,expected", [
    ("Hello, world! Café ☕\nNext\tline: [safe] {also safe}.", "Hello, world! Café ☕\nNext\tline: [safe] {also safe}."),
    ("foo+bar@example.co.uk.,", "[REDACTED].,"),
    ("123456789@example.com", "[REDACTED]"),
    ("123456789 123456789@example.com", "[REDACTED] [REDACTED]"),
    ("abc123456789def", "abc[REDACTED]def"),
    ("4111 1111 1111 1111 ordinary", "[REDACTED] ordinary"),
    ("1234567890 123", "[REDACTED]"),
    ("12345678", "12345678"), ("1234567890", "1234567890"),
    ("123456789012", "123456789012"), ("1" * 20, "1" * 20),
    ("123-45-6789-1", "123-45-6789-1"),
    ("2026-10-30", "2026-10-30"), ("price 12.50; a@localhost", "price 12.50; a@localhost"),
    ("a@b.co!x", "[REDACTED]!x"),
])
def test_policy_and_ordinary_text_preservation(text, expected):
    assert redact(text) == expected


@pytest.mark.parametrize("text", [
    "a" * 321, "1" * 321, "1" + " " * 320,
    "a" * 65 + "@example.com", "a@" + "b" * 64 + ".com",
    "a" * 64 + "@" + ".".join(["b" * 63, "c" * 63, "d" * 62]),
])
def test_overlong_candidates_never_release_the_candidate(text):
    redactor = StreamingRedactor()
    output = []
    with pytest.raises(RedactionError):
        for char in text + " ":
            output.extend(redactor.feed(char))
        output.append(redactor.finish())
    assert output == []
    assert redactor.retained_chars == 0
    assert redactor.max_retained_chars <= MAX_CANDIDATE_CHARS


def test_no_numeric_prefix_or_undecided_tail_is_released():
    redactor = StreamingRedactor()
    assert list(redactor.feed("123456789")) == []
    assert list(redactor.feed("0123 ")) == []
    assert list(redactor.feed("456")) == []
    assert list(redactor.feed(" ")) == []
    assert redactor.finish() == "[REDACTED] "
    redactor = StreamingRedactor()
    assert list(redactor.feed("Safe. alice@")) == ["Safe. "]
    redactor.discard()
    assert redactor.retained_chars == 0


@pytest.mark.parametrize("text,expected", [
    ("1 -a@b.co", "1 [REDACTED]"),
    ("1 ---secret@example.com", "1 [REDACTED]"),
    ("1 -.a@b.co", "1 [REDACTED]"),
    ("1 ---.secret@example.com", "1 [REDACTED]"),
    ("123456789 ---secret@example.com", "[REDACTED] [REDACTED]"),
    ("4111 1111 1111 1111 ---secret@example.com", "[REDACTED] [REDACTED]"),
    ("a@b.co1 2", "[REDACTED] 2"),
    ("a@b.co1 ---secret@example.com", "[REDACTED] [REDACTED]"),
    ("a@b.co1 123456789", "[REDACTED] [REDACTED]"),
    ("1 ---word", "1 ---word"),
    ("1 -.word", "1 -.word"),
    ("1234 ---5678 9012 3456", "[REDACTED]"),
    ("123-45-6789 --1", "123-45-6789 --1"),
    ("123456789 ---", "[REDACTED] ---"),
    ("1 -", "1 -"),
])
def test_numeric_gap_preserves_email_prefixes_and_numeric_runs(text, expected):
    assert redact_candidate(text) == expected
    for split in range(len(text) + 1):
        assert redact([text[:split], text[split:]]) == expected
    assert redact(text) == expected


@pytest.mark.parametrize("prefix", ["-a", "---secret", "-.a", "---.secret"])
def test_numeric_gap_does_not_emit_ambiguous_email_prefix(prefix):
    redactor = StreamingRedactor()
    assert list(redactor.feed("1 " + prefix)) == ["1 "]
    assert list(redactor.feed("@example.com")) == []
    assert redactor.finish() == "[REDACTED]"


def test_streaming_matches_whole_candidate_policy_across_generated_boundaries():
    rng = random.Random(241)
    chunk_rng = random.Random(242)
    units = [
        "a", ".", "@", "-", " ", "  ", "\n", "1", "123456789", "0123",
        "4111-1111", "☕", "é", "+", ":", "example.com", "a@b.co",
    ]
    checked = 0
    for _ in range(10000):
        text = "".join(rng.choice(units) for _ in range(rng.randrange(1, 25)))
        try:
            expected = redact_candidate(text)
        except RedactionError:
            continue
        assert redact(text) == expected, repr(text)
        parts = []
        position = 0
        while position < len(text):
            end = position + chunk_rng.randrange(1, 15)
            parts.append(text[position:end])
            position = end
        assert redact(parts) == expected, repr(text)
        checked += 1
    assert checked > 9900


def test_retained_state_does_not_grow_with_response_length():
    redactor = StreamingRedactor()
    emitted = 0
    for _ in range(20000):
        emitted += sum(map(len, redactor.feed("safe alice@example.com, 4111-1111-1111-1111. ")))
        assert redactor.retained_chars <= MAX_CANDIDATE_CHARS
    emitted += len(redactor.finish())
    assert emitted > 500000
    assert redactor.max_retained_chars < 64
