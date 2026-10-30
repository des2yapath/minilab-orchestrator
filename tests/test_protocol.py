"""Tests for wire protocol framing, CRC, and serialization."""

from __future__ import annotations

import pytest

from minilab.errors import FrameError
from minilab.messages import Message, decode, encode


def test_roundtrip() -> None:
    """Verify encode -> decode roundtrip for all message types."""
    messages = [
        Message(type="cmd", msg_id=1, device="arm1", action="move_to", params={"x": 10, "y": 20}),
        Message(type="ack", msg_id=1, device="arm1"),
        Message(type="done", msg_id=1, device="arm1", result={"x": 10, "y": 20}),
        Message(type="error", msg_id=1, device="arm1", error="STALL"),
        Message(type="estop", msg_id=99, device="pump1"),
    ]
    for msg in messages:
        encoded = encode(msg)
        decoded = decode(encoded)
        assert decoded == msg


def test_crc_detects_single_char_corruption() -> None:
    """Verify single-character bit flips trigger FrameError."""
    msg = Message(type="cmd", msg_id=42, device="pump1", action="dispense", params={"ml": 5})
    encoded = encode(msg)
    parts = encoded.split("|", 1)
    body = parts[1]

    # Flip character in body
    corrupted_body = body[:5] + ("A" if body[5] != "A" else "B") + body[6:]
    corrupted_frame = f"{parts[0]}|{corrupted_body}"

    with pytest.raises(FrameError, match="CRC mismatch"):
        decode(corrupted_frame)


def test_decode_rejects_garbage() -> None:
    """Verify all invalid frame formats raise FrameError and never anything else."""
    garbage_cases = [
        "",
        "abc",
        "zzzzzzzz|{}",
        "1234567|{}",  # 7 hex chars instead of 8
        "deadbeef|",   # empty body
        "deadbeef|not_json",
        "00000000|[]", # list instead of dict
        encode(Message(type="cmd", msg_id=1, device="arm1"))[:-5],  # truncated
    ]
    for raw in garbage_cases:
        with pytest.raises(FrameError):
            decode(raw)

    # Unknown message type
    bad_type_body = '{"attempt":0,"device":"arm1","error":"","msg_id":1,"params":{},"result":{},"type":"invalid"}'
    import zlib
    crc = f"{zlib.crc32(bad_type_body.encode('utf-8')) & 0xFFFFFFFF:08x}"
    with pytest.raises(FrameError, match="Unknown message type"):
        decode(f"{crc}|{bad_type_body}")

