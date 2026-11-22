"""Relaxed lyrics lookup used when Music Assistant's strict matcher finds nothing."""

from __future__ import annotations

import asyncio
import base64
import html
import json
import logging
import re
from typing import Any

LOGGER = logging.getLogger(__name__)

LRCLIB_SEARCH_URL = "https://lrclib.net/api/search"
LRCLIB_GET_URL = "https://lrclib.net/api/get"
USER_AGENT = "HA-Music-Studio/0.7.4 (https://github.com/RockJesus/ha-music-studio)"

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


NETEASE_SEARCH_URL = "https://music.163.com/api/cloudsearch/pc"
NETEASE_LYRIC_URL = "https://music.163.com/api/song/lyric"
QQ_SEARCH_URL = "https://c.y.qq.com/soso/fcgi-bin/client_search_cp"
QQ_LYRIC_URL = "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_new.fcg"
KUGOU_SEARCH_URL = "https://mobilecdn.kugou.com/api/v3/search/song"
KUGOU_MATCH_URL = "https://krcs.kugou.com/search"
KUGOU_LYRIC_URL = "https://lyrics.kugou.com/download"


def _decode_json(text: str) -> Any:
    """Parse JSON, tolerating JSONP wrappers used by QQ Music."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except (ValueError, TypeError):
            return None
    return None


def _duration_ok(value: Any, duration: float) -> bool:
    try:
        candidate = float(value or 0)
    except (TypeError, ValueError):
        return True
    if not candidate or not duration:
        return True
    return abs(candidate - float(duration)) <= 8


async def _fetch_netease(
    session: Any, title: str, artist: str, duration: float, timeout: float
) -> tuple[str, str, str]:
    """NetEase Cloud Music search + lyric API (no login required)."""
    headers = {"User-Agent": USER_AGENT, "Referer": "https://music.163.com/"}
    query = f"{title} {artist}".strip()
    try:
        async with session.post(
            NETEASE_SEARCH_URL,
            data={"s": query, "type": 1, "limit": 6, "offset": 0},
            headers=headers,
            timeout=timeout,
        ) as response:
            if response.status != 200:
                return "", "", ""
            payload = _decode_json(await response.text())
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("NetEase search failed for %s: %s", title, err)
        return "", "", ""
    songs = (payload or {}).get("result", {}).get("songs") or []
    tokens = artist_tokens(artist)
    for song in songs:
        if normalize_text(song.get("name")) != normalize_text(title):
            continue
        singers = " / ".join(
            str(item.get("name") or "") for item in (song.get("ar") or []) if item
        )
        if tokens and not _artist_matches(singers, tokens):
            continue
        if not _duration_ok((song.get("dt") or 0) / 1000, duration):
            continue
        song_id = song.get("id")
        if not song_id:
            continue
        try:
            async with session.get(
                NETEASE_LYRIC_URL,
                params={"id": song_id, "lv": 1, "kv": 1, "tv": -1},
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    continue
                lyric_payload = _decode_json(await response.text())
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("NetEase lyric failed for %s: %s", title, err)
            continue
        lrc = ((lyric_payload or {}).get("lrc") or {}).get("lyric") or ""
        if lrc.strip():
            return lrc, lrc, "网易云音乐"
    return "", "", ""


async def _fetch_qq(
    session: Any, title: str, artist: str, duration: float, timeout: float
) -> tuple[str, str, str]:
    """QQ Music search + lyric API."""
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": "https://y.qq.com/",
    }
    try:
        async with session.get(
            QQ_SEARCH_URL,
            params={"w": f"{title} {artist}".strip(), "format": "json", "n": 6, "p": 1},
            headers=headers,
            timeout=timeout,
        ) as response:
            if response.status != 200:
                return "", "", ""
            payload = _decode_json(await response.text())
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("QQ search failed for %s: %s", title, err)
        return "", "", ""
    songs = (((payload or {}).get("data") or {}).get("song") or {}).get("list") or []
    tokens = artist_tokens(artist)
    for song in songs:
        if normalize_text(song.get("songname")) != normalize_text(title):
            continue
        singers = " / ".join(
            str(item.get("name") or "") for item in (song.get("singer") or []) if item
        )
        if tokens and not _artist_matches(singers, tokens):
            continue
        if not _duration_ok(song.get("interval"), duration):
            continue
        song_mid = song.get("songmid")
        if not song_mid:
            continue
        try:
            async with session.get(
                QQ_LYRIC_URL,
                params={"songmid": song_mid, "format": "json", "nobase64": 1, "g_tk": 5381},
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    continue
                lyric_payload = _decode_json(await response.text())
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("QQ lyric failed for %s: %s", title, err)
            continue
        lrc = html.unescape(str((lyric_payload or {}).get("lyric") or ""))
        if lrc.strip():
            return lrc, lrc, "QQ 音乐"
    return "", "", ""


async def _fetch_kugou(
    session: Any, title: str, artist: str, duration: float, timeout: float
) -> tuple[str, str, str]:
    """Kugou search + KRC lyric download."""
    headers = {"User-Agent": USER_AGENT}
    try:
        async with session.get(
            KUGOU_SEARCH_URL,
            params={"keyword": f"{title} {artist}".strip(), "page": 1, "pagesize": 6},
            headers=headers,
            timeout=timeout,
        ) as response:
            if response.status != 200:
                return "", "", ""
            payload = _decode_json(await response.text())
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("Kugou search failed for %s: %s", title, err)
        return "", "", ""
    songs = ((payload or {}).get("data") or {}).get("info") or []
    tokens = artist_tokens(artist)
    for song in songs:
        if normalize_text(song.get("songname")) != normalize_text(title):
            continue
        if tokens and not _artist_matches(song.get("singername"), tokens):
            continue
        if not _duration_ok(song.get("duration"), duration):
            continue
        try:
            async with session.get(
                KUGOU_MATCH_URL,
                params={
                    "ver": 1,
                    "man": "yes",
                    "client": "mobi",
                    "keyword": f"{title} - {artist}".strip(" -"),
                    "duration": round(float(duration or 0)),
                },
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    continue
                match_payload = _decode_json(await response.text())
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("Kugou match failed for %s: %s", title, err)
            continue
        candidates = (match_payload or {}).get("candidates") or []
        if not candidates:
            continue
        candidate = candidates[0]
        try:
            async with session.get(
                KUGOU_LYRIC_URL,
                params={
                    "ver": 1,
                    "client": "pc",
                    "id": candidate.get("id"),
                    "accesskey": candidate.get("accesskey"),
                    "fmt": "lrc",
                    "charset": "utf8",
                },
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    continue
                lyric_payload = _decode_json(await response.text())
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("Kugou lyric failed for %s: %s", title, err)
            continue
        encoded = str((lyric_payload or {}).get("content") or "")
        try:
            lrc = base64.b64decode(encoded).decode("utf-8", "ignore")
        except (ValueError, TypeError):
            continue
        if lrc.strip():
            return lrc, lrc, "酷狗音乐"
    return "", "", ""


async def fetch_lyrics_multi(
    session: Any,
    title: str,
    artist: str,
    album: str = "",
    duration: float = 0,
    timeout: float = 9,
) -> tuple[str, str, str]:
    """Query all lyric sources concurrently and return the first usable hit."""
    if not title.strip():
        return "", "", ""
    tasks = [
        fetch_lrclib_lyrics(session, title, artist, album, duration, timeout),
        _fetch_netease(session, title, artist, duration, timeout),
        _fetch_qq(session, title, artist, duration, timeout),
        _fetch_kugou(session, title, artist, duration, timeout),
    ]
    results = await asyncio.gather(
        *(asyncio.wait_for(task, timeout + 2) for task in tasks),
        return_exceptions=True,
    )
    sources = ("LRCLIB", "网易云音乐", "QQ 音乐", "酷狗音乐")
    for index, result in enumerate(results):
        if isinstance(result, Exception) or not isinstance(result, tuple):
            continue
        plain, lrc = result[0], result[1]
        source = result[2] if len(result) > 2 else sources[index]
        if (plain or "").strip() or (lrc or "").strip():
            return plain, lrc, source
    return "", "", ""


async def fetch_lyrics_multi_plain(
    session: Any,
    title: str,
    artist: str,
    album: str = "",
    duration: float = 0,
    timeout: float = 9,
) -> tuple[str, str]:
    """Backward-compatible two-tuple wrapper for ``fetch_lyrics_multi``."""
    plain, lrc, _source = await fetch_lyrics_multi(
        session, title, artist, album, duration, timeout
    )
    return plain, lrc
