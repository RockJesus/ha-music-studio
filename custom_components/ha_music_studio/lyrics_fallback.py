"""Relaxed lyrics lookup used when Music Assistant's strict matcher finds nothing."""

from __future__ import annotations

import logging
import re
from typing import Any

LOGGER = logging.getLogger(__name__)

LRCLIB_SEARCH_URL = "https://lrclib.net/api/search"
LRCLIB_GET_URL = "https://lrclib.net/api/get"
USER_AGENT = "HA-Music-Studio/0.7.3 (https://github.com/RockJesus/ha-music-studio)"

_SPLIT_RE = re.compile(r"\s*(?:/|,|&|;|\bfeat\.?\s+|\bft\.?\s+|\bx\s+)\s*", re.IGNORECASE)
_BRACKET_RE = re.compile(r"\s*[\(\[].*?[\)\]]\s*")
_PUNCT_RE = re.compile(r"[^\w\u4e00-\u9fff]+", re.UNICODE)


def normalize_text(value: Any) -> str:
    """Case-fold, drop bracketed suffixes and punctuation for robust matching."""
    text = _BRACKET_RE.sub(" ", str(value or ""))
    text = _PUNCT_RE.sub(" ", text.casefold())
    return " ".join(text.split())


def artist_tokens(value: Any) -> list[str]:
    """Split a multi-artist string into searchable names."""
    parts = [part.strip() for part in _SPLIT_RE.split(str(value or "")) if part.strip()]
    return [part for part in parts if normalize_text(part)]


def _artist_matches(candidate: Any, tokens: list[str]) -> bool:
    haystack = normalize_text(candidate)
    if not haystack:
        return False
    hay_words = set(haystack.split())
    for token in tokens:
        wanted = normalize_text(token)
        if not wanted:
            continue
        if wanted == haystack:
            return True
        words = wanted.split()
        if len(words) == 1 and wanted in hay_words:
            return True
        if len(words) > 1 and f" {wanted} " in f" {haystack} ":
            return True
    return False


def _delta(candidate: Any, duration: float) -> float:
    try:
        value = float(candidate.get("duration") or 0)
    except (TypeError, ValueError):
        return 9999.0
    if not duration or not value:
        return 0.0
    return abs(value - duration)


def pick_lrclib_candidate(
    candidates: Any, title: str, artist: str, duration: float = 0
) -> dict[str, Any] | None:
    """Choose the closest exact-title candidate, preferring synced lyrics."""
    if not isinstance(candidates, list):
        return None
    wanted = normalize_text(title)
    if not wanted:
        return None
    tokens = artist_tokens(artist)
    ranked: list[tuple[int, int, float, dict[str, Any]]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if normalize_text(candidate.get("trackName")) != wanted:
            continue
        synced = str(candidate.get("syncedLyrics") or "").strip()
        plain = str(candidate.get("plainLyrics") or "").strip()
        if not synced and not plain:
            continue
        if tokens and not _artist_matches(candidate.get("artistName"), tokens):
            continue
        delta = _delta(candidate, duration)
        # 同名不同版本可能差几十秒；超过 8 秒基本是别的录音，宁可不用。
        if duration and delta > 8:
            continue
        ranked.append((0 if synced else 1, 0 if delta <= 2 else 1, delta, candidate))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[:3])
    return ranked[0][3]


def _lyrics_pair(candidate: dict[str, Any]) -> tuple[str, str]:
    return (
        str(candidate.get("plainLyrics") or ""),
        str(candidate.get("syncedLyrics") or ""),
    )


async def fetch_lrclib_lyrics(
    session: Any,
    title: str,
    artist: str,
    album: str = "",
    duration: float = 0,
    timeout: float = 12,
) -> tuple[str, str]:
    """Try an exact LRCLIB match, then a relaxed title + first-artist search."""
    if not title:
        return "", ""
    headers = {"User-Agent": USER_AGENT}
    params: dict[str, Any] = {"track_name": title}
    if artist:
        params["artist_name"] = artist
    if album:
        params["album_name"] = album
    if duration:
        params["duration"] = round(float(duration))
    try:
        async with session.get(
            LRCLIB_GET_URL, params=params, headers=headers, timeout=timeout
        ) as response:
            if response.status == 200:
                data = await response.json(content_type=None)
                if isinstance(data, dict):
                    pair = _lyrics_pair(data)
                    if pair[0] or pair[1]:
                        return pair
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("LRCLIB get failed for %s: %s", title, err)

    tokens = artist_tokens(artist)
    search_artists = [tokens[0]] if tokens else ([] if not artist else [artist])
    for search_artist in search_artists:
        query: dict[str, Any] = {"track_name": title}
        if search_artist:
            query["artist_name"] = search_artist
        try:
            async with session.get(
                LRCLIB_SEARCH_URL, params=query, headers=headers, timeout=timeout
            ) as response:
                if response.status != 200:
                    continue
                data = await response.json(content_type=None)
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("LRCLIB search failed for %s: %s", title, err)
            continue
        picked = pick_lrclib_candidate(data, title, artist, duration)
        if picked:
            return _lyrics_pair(picked)
    return "", ""
