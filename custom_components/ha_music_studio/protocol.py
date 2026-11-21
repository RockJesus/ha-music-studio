"""Wire protocol metadata shared by every WebSocket event."""

from __future__ import annotations

PROTOCOL_VERSION = 1


def envelope(payload: dict) -> dict:
    """Return the payload tagged with the current protocol version."""
    payload["protocol"] = PROTOCOL_VERSION
    return payload
