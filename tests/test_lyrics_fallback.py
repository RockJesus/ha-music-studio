"""Unit tests for the relaxed LRCLIB candidate matcher."""

from __future__ import annotations

import importlib.util
from pathlib import Path

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "ha_music_studio"
    / "lyrics_fallback.py"
)


def load_fallback():
    spec = importlib.util.spec_from_file_location("ha_music_studio_lyrics_fallback", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_artist_tokens_split_multi_artist():
    fallback = load_fallback()
    assert fallback.artist_tokens("Marshmello/Jonas Brothers") == [
        "Marshmello",
        "Jonas Brothers",
    ]
    assert fallback.artist_tokens("A feat. B") == ["A", "B"]


def test_pick_prefers_synced_and_close_duration():
    fallback = load_fallback()
    candidates = [
        {
            "trackName": "Slow Motion",
            "artistName": "Marshmello",
            "duration": 152,
            "plainLyrics": "plain",
            "syncedLyrics": "",
        },
        {
            "trackName": "Slow Motion",
            "artistName": "Marshmello",
            "duration": 151.9,
            "plainLyrics": "plain",
            "syncedLyrics": "[00:00.00]line",
        },
        {
            "trackName": "Slow Motion",
            "artistName": "Other",
            "duration": 151.9,
            "syncedLyrics": "[00:00.00]other",
        },
    ]
    picked = fallback.pick_lrclib_candidate(
        candidates, "Slow Motion", "Marshmello/Jonas Brothers", 151.986
    )
    assert picked is not None
    assert picked["syncedLyrics"] == "[00:00.00]line"


def test_pick_rejects_wrong_title_and_far_duration():
    fallback = load_fallback()
    candidates = [
        {"trackName": "Other Song", "artistName": "Marshmello", "syncedLyrics": "x"},
        {
            "trackName": "Slow Motion",
            "artistName": "Marshmello",
            "duration": 300,
            "syncedLyrics": "x",
        },
    ]
    assert (
        fallback.pick_lrclib_candidate(
            candidates, "Slow Motion", "Marshmello", 151.986
        )
        is None
    )


def test_pick_accepts_plain_when_no_synced():
    fallback = load_fallback()
    candidates = [
        {
            "trackName": "Line",
            "artistName": "Little End",
            "duration": 325,
            "plainLyrics": "line one\nline two",
        }
    ]
    picked = fallback.pick_lrclib_candidate(candidates, "Line", "Little End", 325.062)
    assert picked is not None
    assert picked["plainLyrics"].startswith("line one")
