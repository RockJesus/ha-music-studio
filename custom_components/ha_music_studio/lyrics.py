"""Server-side lyrics lookup and playback-position sync for HA Music Studio."""

from __future__ import annotations

import asyncio
import bisect
import logging
import re
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import voluptuous as vol
from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import DOMAIN, WS_LYRICS_SUBSCRIBE
from .protocol import envelope
from .spectrum import _player_for_entity
from .token import async_get_viewer_token, entry_url, ma_entry

_LOGGER = logging.getLogger(__name__)

_TIME_RE = re.compile(r"\[(\d{1,3}):(\d{1,2})(?:[.:](\d{1,3}))?\]")
_OFFSET_RE = re.compile(r"\[offset:([+-]?\d+)\]", re.IGNORECASE)


def _artist_text(track: Any) -> str:
    artists = getattr(track, "artists", None)
    if artists:
        names = []
        for artist in artists:
            name = getattr(artist, "name", None)
            if name:
                names.append(str(name))
        if names:
            return " / ".join(names)
    for attr in ("artist_str", "artist", "album_artist"):
        value = getattr(track, attr, None)
        if isinstance(value, str) and value:
            return value
    return ""


def _track_key(track: Any) -> str:
    return "|".join(
        str(getattr(track, attr, None) or "")
        for attr in ("provider", "item_id", "uri", "name")
    )


def parse_lrc(text: str) -> list[dict[str, Any]]:
    """Parse LRC text into sorted, de-duplicated timestamped lines."""
    if not text:
        return []
    offset = 0.0
    for match in _OFFSET_RE.finditer(text):
        try:
            offset = int(match.group(1)) / 1000.0
        except ValueError:
            offset = 0.0
    rows: list[tuple[float, str]] = []
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        stamps = list(_TIME_RE.finditer(raw))
        if not stamps:
            continue
        line = _TIME_RE.sub("", raw).strip()
        if not line:
            continue
        for stamp in stamps:
            minutes = int(stamp.group(1))
            seconds = int(stamp.group(2))
            fraction = stamp.group(3) or ""
            millis = int(fraction.ljust(3, "0")[:3]) if fraction else 0
            rows.append((max(0.0, minutes * 60 + seconds + millis / 1000.0 + offset), line))
    if not rows:
        return []
    rows.sort(key=lambda item: item[0])
    merged: list[dict[str, Any]] = []
    for stamp, line in rows:
        if merged and abs(float(merged[-1]["time"]) - stamp) < 0.02 and merged[-1]["text"] == line:
            continue
        merged.append({"time": round(stamp, 2), "text": line})
    return merged


def _plain_lines(text: str) -> list[str]:
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def _build_entry(lyrics: str | None, lrc_lyrics: str | None) -> dict[str, Any]:
    timed = parse_lrc(lrc_lyrics or "") or parse_lrc(lyrics or "")
    if timed:
        return {"status": "ready", "lines": timed, "plain": [], "synced": True}
    plain = _plain_lines(lyrics or lrc_lyrics or "")
    if plain:
        return {"status": "ready", "lines": [], "plain": plain, "synced": False}
    return {"status": "none", "lines": [], "plain": [], "synced": False}


def _ma_ws_url(url: str) -> str:
    base = url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://") :]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://") :]
    elif not base.startswith(("ws://", "wss://")):
        base = "ws://" + base
    return base + "/ws"


async def _fetch_lyrics_ws(hass: HomeAssistant, entry: Any, track: Any) -> tuple[str | None, str | None]:
    """Call Music Assistant's metadata command directly over its WebSocket API."""
    token = await async_get_viewer_token(hass, entry)
    base = entry_url(entry)
    if not token or not base:
        raise RuntimeError("Music Assistant token or URL unavailable")
    if hasattr(track, "to_dict"):
        track_data = track.to_dict()
    elif isinstance(track, dict):
        track_data = track
    else:
        raise TypeError(f"Unsupported track payload: {type(track).__name__}")
    session = async_get_clientsession(hass, verify_ssl=False)
    async with session.ws_connect(_ma_ws_url(base), heartbeat=25, ssl=False, timeout=20) as ws:
        await ws.receive_json(timeout=10)  # server info
        auth_id = uuid4().hex
        await ws.send_json({"message_id": auth_id, "command": "auth", "args": {"token": token}})
        auth = await ws.receive_json(timeout=15)
        if not auth.get("success"):
            raise RuntimeError(str(auth.get("error") or "Music Assistant auth failed")[:160])
        command_id = uuid4().hex
        await ws.send_json(
            {
                "message_id": command_id,
                "command": "metadata/get_track_lyrics",
                "args": {"track": track_data},
            }
        )
        reply = await ws.receive_json(timeout=30)
        if not reply.get("success"):
            raise RuntimeError(str(reply.get("error") or "Music Assistant lyrics lookup failed")[:160])
        result = reply.get("result")
        if isinstance(result, (list, tuple)):
            lyrics = result[0] if len(result) > 0 else None
            lrc = result[1] if len(result) > 1 else None
            return lyrics, lrc
        if isinstance(result, dict):
            return result.get("lyrics"), result.get("lrc_lyrics")
    return None, None


async def _resolve_full_track(mass: Any, item: Any) -> Any:
    """Return a full Track with metadata, upgrading queue mappings when needed."""
    track = getattr(item, "media_item", None) or getattr(item, "track", None) or item
    if hasattr(track, "metadata"):
        return track
    provider = getattr(track, "provider", None) or getattr(item, "provider", None)
    item_id = getattr(track, "item_id", None) or getattr(item, "item_id", None)
    if provider and item_id:
        try:
            return await mass.music.get_track(str(item_id), str(provider))
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("get_track failed: %s", err)
    uri = getattr(item, "uri", None) or getattr(track, "uri", None)
    if uri:
        try:
            return await mass.music.get_item_by_uri(str(uri))
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("get_item_by_uri failed: %s", err)
    return track


class LyricsSession:
    """Push the current track's lyrics and active line over HA WebSocket."""

    def __init__(
        self,
        hass: HomeAssistant,
        entity_id: str | None,
        send: Callable,
        player: str | None = None,
        offset: float = 0.0,
    ) -> None:
        self.hass = hass
        self.entity_id = entity_id
        self.send = send
        self.requested_player = player
        self.offset = max(-5.0, min(5.0, float(offset or 0.0)))
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._track_key = ""
        self._entry: dict[str, Any] | None = None
        self._fetch_task: asyncio.Task | None = None
        self._meta: dict[str, Any] = {}

    async def start(self) -> None:
        self._task = self.hass.async_create_task(self._run())

    async def stop(self) -> None:
        self._stopped = True
        for task in (self._fetch_task, self._task):
            if task is not None:
                task.cancel()
        for task in (self._fetch_task, self._task):
            if task is None:
                continue
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._fetch_task = None
        self._task = None

    def _player(self) -> str | None:
        entry = ma_entry(self.hass)
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
        player = _player_for_entity(self.hass, self.entity_id) or self.requested_player
        if player or mass is None:
            return player
        try:
            for candidate in mass.players.players:
                state = getattr(candidate, "state", None)
                value = getattr(state, "value", state)
                if str(value).lower() in ("playing", "playerstate.playing"):
                    return candidate.player_id
        except Exception:  # noqa: BLE001
            return None
        return None

    async def _run(self) -> None:
        while not self._stopped:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Lyrics tick failed: %s", err.__class__.__name__)
                self.send({"type": "lyrics_error", "message": f"{err.__class__.__name__}: {err}"[:160]})
            await asyncio.sleep(1.0)

    async def _tick(self) -> None:
        entry = ma_entry(self.hass)
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
        if mass is None:
            self.send({"type": "lyrics", "status": "idle", "lines": [], "plain": []})
            return
        player = self._player()
        if not player:
            self.send({"type": "lyrics", "status": "idle", "lines": [], "plain": []})
            return
        queue = await mass.player_queues.get_active_queue(player)
        item = getattr(queue, "current_item", None) if queue else None
        if item is None:
            self._track_key = ""
            self._entry = None
            self.send({"type": "lyrics", "status": "idle", "lines": [], "plain": []})
            return
        track = await _resolve_full_track(mass, item)
        key = _track_key(track)
        title = str(getattr(track, "name", None) or getattr(item, "name", None) or "")
        artist = _artist_text(track)
        duration = float(
            getattr(item, "duration", None) or getattr(track, "duration", None) or 0
        )
        if key != self._track_key:
            self._track_key = key
            self._entry = None
            self._meta = {"track_id": key, "title": title, "artist": artist}
            self.send(
                {
                    "type": "lyrics",
                    "status": "loading",
                    "lines": [],
                    "plain": [],
                    "synced": False,
                    "title": title,
                    "artist": artist,
                    "track_id": key,
                }
            )
            if self._fetch_task is not None:
                self._fetch_task.cancel()
            self._fetch_task = self.hass.async_create_task(self._load(track, key, title, artist))
        if self._entry is None:
            return
        position = float(
            getattr(queue, "corrected_elapsed_time", None)
            or getattr(queue, "elapsed_time", None)
            or 0.0
        )
        lines = self._entry["lines"]
        index = -1
        progress = 0.0
        if lines:
            target = max(0.0, position + self.offset)
            times = [float(line["time"]) for line in lines]
            index = bisect.bisect_right(times, target) - 1
            if index >= len(lines) - 1 and duration and target > duration:
                index = len(lines) - 1
            if index >= 0:
                start = times[index]
                end = times[index + 1] if index + 1 < len(times) else duration
                if end > start:
                    progress = max(0.0, min(1.0, (target - start) / (end - start)))
        self.send(
            {
                "type": "lyrics",
                "status": self._entry["status"],
                "lines": lines,
                "plain": self._entry["plain"],
                "synced": self._entry["synced"],
                "instrumental": self._entry.get("instrumental", False),
                "title": title,
                "artist": artist,
                "track_id": key,
                "position": round(position, 2),
                "duration": round(duration, 2),
                "index": index,
                "progress": round(progress, 3),
                "offset": self.offset,
                "source": "Music Assistant",
                "errmsg": str(self._entry.get("error") or ""),
            }
        )

    async def _load(self, track: Any, key: str, title: str, artist: str) -> None:
        cache = self.hass.data.setdefault(DOMAIN, {}).setdefault("lyrics_cache", {})
        cached = cache.get(key)
        if cached and cached.get("status") in ("ready", "none"):
            self._entry = cached
            return
        entry = ma_entry(self.hass)
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
        if mass is None:
            return
        try:
            async with asyncio.timeout(30):
                result = await mass.send_command(
                    "metadata/get_track_lyrics", track=track
                )
            if isinstance(result, (list, tuple)):
                lyrics = result[0] if len(result) > 0 else None
                lrc_lyrics = result[1] if len(result) > 1 else None
            elif isinstance(result, dict):
                lyrics = result.get("lyrics")
                lrc_lyrics = result.get("lrc_lyrics")
            else:
                lyrics, lrc_lyrics = None, None
            parsed = _build_entry(lyrics, lrc_lyrics)
            parsed["title"] = title
            parsed["artist"] = artist
            parsed["fetched_at"] = time.time()
            if not parsed["lines"] and not parsed["plain"]:
                parsed["status"] = "none"
            cache[key] = parsed
            if len(cache) > 80:
                for old_key in list(cache)[: len(cache) - 80]:
                    cache.pop(old_key, None)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Lyrics lookup failed: %s", err)
            parsed = {
                "error": f"{err.__class__.__name__}: {err}"[:240],
                "status": "error",
                "lines": [],
                "plain": [],
                "synced": False,
                "title": title,
                "artist": artist,
                "fetched_at": time.time(),
            }
            cache[key] = parsed
        if key == self._track_key:
            self._entry = parsed


async def _async_subscribe(
    hass: HomeAssistant,
    entity_id: str | None,
    send: Callable,
    player: str | None = None,
    offset: float = 0.0,
) -> Callable:
    session = LyricsSession(hass, entity_id, send, player, offset)
    await session.start()

    @callback
    def unsubscribe() -> None:
        hass.async_create_task(session.stop())

    return unsubscribe


@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_LYRICS_SUBSCRIBE,
        vol.Optional("entity_id"): str,
        vol.Optional("player"): str,
        vol.Optional("offset"): vol.Coerce(float),
    }
)
@websocket_api.async_response
async def websocket_subscribe_lyrics(hass: HomeAssistant, connection, msg) -> None:
    """Subscribe to server-synced lyrics for the active track."""

    @callback
    def send(payload: dict) -> None:
        connection.send_message(websocket_api.event_message(msg["id"], envelope(payload)))

    try:
        unsubscribe = await _async_subscribe(
            hass,
            msg.get("entity_id"),
            send,
            msg.get("player"),
            msg.get("offset", 0.0),
        )
    except Exception as err:  # noqa: BLE001
        connection.send_error(msg["id"], "subscribe_failed", str(err)[:160])
        return
    connection.subscriptions[msg["id"]] = unsubscribe
    connection.send_result(msg["id"])


@callback
def async_register_lyrics_api(hass: HomeAssistant) -> None:
    """Register the lyrics websocket command once per instance."""
    if hass.data.get(f"{DOMAIN}_lyrics_ws"):
        return
    websocket_api.async_register_command(hass, websocket_subscribe_lyrics)
    hass.data[f"{DOMAIN}_lyrics_ws"] = True
