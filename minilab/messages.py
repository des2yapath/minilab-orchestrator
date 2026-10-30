"""Wire protocol message definitions, encoding, and CRC validation.

Provides serialization and framing for messages exchanged between the host
and simulated devices. Each frame is framed as '<crc32_hex>|<json_body>'.
"""

from __future__ import annotations

import json
import zlib
from dataclasses import asdict, dataclass, field
from typing import Any

from minilab.errors import FrameError

VALID_TYPES = frozenset({"cmd", "ack", "done", "error", "estop"})
MESSAGE_FIELDS = frozenset({
    "type",
    "msg_id",
    "device",
    "action",
    "params",
    "result",
    "error",
    "attempt",
})


@dataclass(frozen=True)
class Message:
    """Represents a discrete protocol message between host and device.

    Frozen to ensure immutability during retries, routing, and verification.
    """

    type: str  # "cmd" | "ack" | "done" | "error" | "estop"
    msg_id: int
    device: str
    action: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    attempt: int = 0


def ack_for(msg: Message) -> Message:
    """Build an immediate acknowledgement frame matching an incoming message."""
    return Message(type="ack", msg_id=msg.msg_id, device=msg.device, attempt=msg.attempt)


def encode(msg: Message) -> str:
    """Encode a Message into a CRC-prefixed string frame.

    Uses deterministic JSON (sorted keys, compact separators) and 8-character
    lowercase hexadecimal CRC32.
    """
    body = json.dumps(asdict(msg), separators=(",", ":"), sort_keys=True)
    crc = zlib.crc32(body.encode("utf-8")) & 0xFFFFFFFF
    return f"{crc:08x}|{body}"


def decode(raw: str) -> Message:
    """Decode a raw string frame into a Message object.

    Raises FrameError on any framing, CRC, parsing, or schema mismatch.
    Never raises any other exception type.
    """
    if not isinstance(raw, str) or "|" not in raw:
        raise FrameError("Invalid frame format: missing delimiter '|'")

    parts = raw.split("|", 1)
    crc_hex, body = parts[0], parts[1]

    if len(crc_hex) != 8:
        raise FrameError(f"Invalid CRC length: expected 8 hex digits, got {len(crc_hex)}")

    try:
        expected_crc = int(crc_hex, 16)
    except ValueError as e:
        raise FrameError(f"Invalid CRC hexadecimal value: {crc_hex}") from e

    computed_crc = zlib.crc32(body.encode("utf-8")) & 0xFFFFFFFF
    if computed_crc != expected_crc:
        raise FrameError(
            f"CRC mismatch: expected {expected_crc:08x}, computed {computed_crc:08x}"
        )

    try:
        data = json.loads(body)
    except Exception as e:
        raise FrameError(f"Malformed JSON in frame body: {e}") from e

    if not isinstance(data, dict):
        raise FrameError("Frame body is not a JSON object")

    extra_keys = set(data.keys()) - MESSAGE_FIELDS
    if extra_keys:
        raise FrameError(f"Unknown message fields: {sorted(extra_keys)}")

    msg_type = data.get("type")
    if msg_type not in VALID_TYPES:
        raise FrameError(f"Unknown message type: {msg_type!r}")

    if "msg_id" not in data or "device" not in data:
        raise FrameError("Message missing required fields ('msg_id', 'device')")

    try:
        return Message(**data)
    except Exception as e:
        raise FrameError(f"Failed to instantiate Message: {e}") from e

