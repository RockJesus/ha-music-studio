"""Contract tests for the WebSocket protocol envelope."""

from __future__ import annotations

import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "custom_components" / "ha_music_studio" / "protocol.py"


def load_protocol():
    spec = importlib.util.spec_from_file_location("ha_music_studio_protocol", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_protocol_version_is_one():
    assert load_protocol().PROTOCOL_VERSION == 1


def test_envelope_adds_protocol_without_losing_payload():
    protocol = load_protocol()
    event = protocol.envelope({"type": "spectrum", "bands": [0.1, 0.9]})
    assert event == {"type": "spectrum", "bands": [0.1, 0.9], "protocol": 1}


def test_envelope_overwrites_stale_protocol():
    protocol = load_protocol()
    assert protocol.envelope({"protocol": 0, "type": "lyrics"})["protocol"] == 1
