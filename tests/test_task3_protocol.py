import json

import pytest

from task3_gateway.mock_server import frame
from task3_gateway.protocol import (
    ChatStream, Completed, MAX_EVENT_CHARS, MAX_EVENT_LINES, MAX_LINE_CHARS,
    MAX_NETWORK_CHUNK_BYTES, SSEDecoder, StreamError, strict_json,
)


def decode(parts):
    parser, chat = SSEDecoder(), ChatStream()
    texts = []
    for part in parts:
        for data in parser.feed(part):
            event = chat.consume(data)
            if isinstance(event, Completed):
                return "".join(texts)
            texts.append(event.text)
    parser.eof()
    raise AssertionError("No completion")


def test_utf8_every_byte_split_and_one_byte_network_chunks():
    raw = b"\xef\xbb\xbf: heartbeat\r\n\r\n" + frame("Café ☕ 😊 ") + frame(finish="stop") + b"data: [DONE]\r\n\r\n"
    for split in range(len(raw) + 1):
        assert decode([raw[:split], raw[split:]]) == "Café ☕ 😊 "
    assert decode([bytes([char]) for char in raw]) == "Café ☕ 😊 "


def test_multiline_data_and_carriage_return_only_frames():
    raw = (
        b'data: {"choices":\rdata: [{"index":0,"delta":{"content":"ok"}}]}\r\r'
        b'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\r\r'
        b'data: [DONE]\r\r'
    )
    assert decode(bytes([char]) for char in raw) == "ok"


@pytest.mark.parametrize("raw", [b"\xff", b"\xc3", b"\xed\xa0\x80", b'data: {"choices":\n', b"data: [DONE]\n"])
def test_malformed_utf8_and_partial_frame(raw):
    parser = SSEDecoder()
    with pytest.raises(StreamError):
        list(parser.feed(raw))
        parser.eof()


@pytest.mark.parametrize("raw", [
    b"x" * (MAX_NETWORK_CHUNK_BYTES + 1),
    b"data: " + b"x" * MAX_LINE_CHARS,
    (b":" + b"x" * 1000 + b"\n") * (MAX_EVENT_CHARS // 1000 + 1),
    b":h\n" * (MAX_EVENT_LINES + 1),
    b"event: error\ndata: private\n\n", b"id: private\n\n", b"retry: 5\n\n",
])
def test_bounded_sse_and_unsupported_fields(raw):
    with pytest.raises(StreamError):
        list(SSEDecoder().feed(raw))


@pytest.mark.parametrize("raw", [
    '{"a":1,"a":2}', '{"a":{"x":1,"x":2}}', '{"a":NaN}', '{"a":1e999}', '[Infinity]',
])
def test_strict_json_rejects_duplicate_keys_and_nonfinite_numbers(raw):
    with pytest.raises(StreamError):
        strict_json(raw)


@pytest.mark.parametrize("change", [
    {"choices": []}, {"choices": [{"index": 0, "delta": {}}, {"index": 1, "delta": {}}]},
    {"choices": [{"index": True, "delta": {}}]},
    {"choices": [{"index": 1, "delta": {"content": "private"}}]},
    {"choices": [{"index": 0, "delta": {"tool_calls": [{"secret": "private"}]}}]},
    {"choices": [{"index": 0, "delta": {"refusal": "private"}}]},
    {"choices": [{"index": 0, "delta": {"content": ["private"]}}]},
    {"choices": [{"index": 0, "delta": {"content": "private"}, "finish_reason": "tool_calls"}]},
    {"choices": [{"index": 0, "delta": {"content": "private"}, "logprobs": {"private": 1}}]},
    {"choices": [{"index": 0, "delta": {"role": "user", "content": "private"}}]},
    {"choices": [{"index": 0, "delta": {"content": "\ud800"}}]},
    {"error": {"message": "private"}}, {"object": "response.delta"}, {"usage": {"tokens": 10}},
])
def test_unsupported_formats_are_rejected_before_content_is_returned(change):
    value = {"choices": [{"index": 0, "delta": {"content": "safe"}, "finish_reason": None}]}
    value.update(change)
    with pytest.raises(StreamError):
        ChatStream().consume(json.dumps(value))


def test_only_done_after_finish_is_trusted_and_metadata_is_dropped():
    with pytest.raises(StreamError):
        ChatStream().consume("[DONE]")
    chat = ChatStream()
    assert chat.consume('{"id":"secret","obfuscation":"secret","choices":[{"index":0,"delta":{"content":"ok"}}]}').text == "ok"
    chat.consume('{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}')
    assert isinstance(chat.consume("[DONE]"), Completed)
    with pytest.raises(StreamError):
        chat.consume("[DONE]")
    chat = ChatStream()
    chat.consume('{"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}')
    with pytest.raises(StreamError):
        chat.consume('{"choices":[{"index":0,"delta":{"content":"late"}}]}')
