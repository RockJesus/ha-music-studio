"""Music Assistant queue gateway over Home Assistant WebSocket."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from typing import Any

import voluptuous as vol
from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant, callback

from .const import DOMAIN, WS_PLAYER_GROUP, WS_QUEUE_COMMAND, WS_QUEUE_SUBSCRIBE
from .protocol import envelope
from .token import ma_entry

_LOGGER = logging.getLogger(__name__)

QUEUE_WINDOW = 60
QUEUE_WINDOW_BEFORE = 15


def _resolve_queue_id(hass: HomeAssistant, entity_id: str | None, queue_id: str | None) -> str:
    if queue_id:
        return queue_id
    if not entity_id:
        return ""
    state = hass.states.get(entity_id)
    if state is None:
        return ""
    for key in ("active_queue", "queue_id", "mass_player_id", "player_id", "ma_player_id"):
        value = state.attributes.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _image_url(item: dict[str, Any]) -> str:
    media = item.get("media_item") or {}
    for candidate in (item.get("image"), item.get("media_image"), media.get("image")):
        if isinstance(candidate, str) and candidate:
            return candidate
        if isinstance(candidate, dict):
            for key in ("url", "path"):
                value = candidate.get(key)
                if isinstance(value, str) and value:
                    return value
    images = (media.get("metadata") or {}).get("images") or []
    for image in images:
        if isinstance(image, dict):
            for key in ("url", "path"):
                value = image.get(key)
                if isinstance(value, str) and value:
                    return value
    return ""


def _artist_text(item: dict[str, Any]) -> str:
    media = item.get("media_item") or {}
    artists = media.get("artists") or (media.get("metadata") or {}).get("artists") or []
    names = []
    for artist in artists:
        if isinstance(artist, str):
            names.append(artist)
        elif isinstance(artist, dict) and artist.get("name"):
            names.append(str(artist["name"]))
    return ", ".join(names)


def _queue_item(item: dict[str, Any], fallback_index: int) -> dict[str, Any]:
    media = item.get("media_item") or {}
    queue_item_id = str(item.get("queue_item_id") or "")
    return {
        "index": int(item.get("index") or fallback_index),
        "queueItemId": queue_item_id,
        "title": str(media.get("name") or item.get("name") or ""),
        "artist": _artist_text(item),
        "album": str((media.get("album") or {}).get("name") or ""),
        "duration": float(item.get("duration") or media.get("duration") or 0),
        "art": _image_url(item),
        "contentId": str(media.get("uri") or ""),
        "contentType": str(media.get("media_type") or "music"),
        "uri": str(media.get("uri") or ""),
        "available": bool(item.get("available", True)),
        "favorite": bool(media.get("favorite", False)),
        "mediaType": str(media.get("media_type") or ""),
        "itemId": str(media.get("item_id") or ""),
        "provider": str(media.get("provider") or ""),
    }


def _item_list(
    queue: dict[str, Any], items: list[dict[str, Any]], offset: int = 0
) -> list[dict[str, Any]]:
    current_id = str((queue.get("current_item") or {}).get("queue_item_id") or "")
    normalized = []
    for index, item in enumerate(items):
        entry = _queue_item(item, offset + index)
        entry["active"] = bool(entry["queueItemId"] and entry["queueItemId"] == current_id)
        normalized.append(entry)
    return normalized


def _queue_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, (list, tuple, dict)):
        return len(value)
    return 0


def _enum_value(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    return getattr(value, "value", value)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        return result if isinstance(result, dict) else {}
    return {}


async def _snapshot(mass: Any, queue_id: str) -> dict[str, Any]:
    # The client keeps queue state from subscriptions; no extra server round-trip is needed.
    queue = mass.player_queues.get(queue_id)
    if queue is None:
        return {"queue_id": queue_id, "items": [], "total": 0, "offset": 0}
    total = max(0, int(getattr(queue, "items", 0) or 0))
    current_index = getattr(queue, "current_index", None)
    current_index = current_index if isinstance(current_index, int) else -1
    start = 0
    if current_index >= 0:
        start = max(0, current_index - QUEUE_WINDOW_BEFORE)
    if total > 0:
        start = min(start, max(0, total - QUEUE_WINDOW))
    limit = min(QUEUE_WINDOW, max(0, total - start)) if total > 0 else 0
    items: list[dict[str, Any]] = []
    if limit:
        try:
            page = await mass.player_queues.get_queue_items(
                queue_id, limit=limit, offset=start
            )
        except Exception as err:
            raise RuntimeError(f"player_queues/items: {err}") from err
        items = [_as_dict(item) for item in page or []]
    current_item = _as_dict(getattr(queue, "current_item", None))
    normalized = _item_list({"current_item": current_item}, items, start)
    return {
        "queue_id": queue_id,
        "state": str(_enum_value(getattr(queue, "state", None), "unknown")),
        "items": normalized,
        "total": total or len(normalized),
        "offset": start,
        "limit": limit,
        "windowed": bool(total and len(normalized) < total),
        "current_index": current_index,
        "current_item_id": str(current_item.get("queue_item_id") or ""),
        "current_title": str(
            _as_dict(current_item.get("media_item")).get("name")
            or current_item.get("name")
            or ""
        ),
        "elapsed": round(float(getattr(queue, "corrected_elapsed_time", 0) or 0), 2),
        "duration": round(float(current_item.get("duration") or 0), 2),
        "playback_speed": float(getattr(queue, "playback_speed", 1) or 1),
        "shuffle": bool(getattr(queue, "shuffle_enabled", False)),
        "repeat": str(_enum_value(getattr(queue, "repeat_mode", None), "off")),
        "autoplay_enabled": bool(getattr(queue, "autoplay_enabled", False)),
        "crossfade_enabled": bool(getattr(queue, "crossfade_enabled", False)),
        "smart_fades_active": bool(getattr(queue, "smart_fades_active", False)),
        "smart_shuffle_active": bool(getattr(queue, "smart_shuffle_active", False)),
        "updated": round(time.time(), 3),
    }


class QueueSession:
    """Push a bounded queue window while a card drawer is open."""

    def __init__(self, hass: HomeAssistant, queue_id: str, send: Callable) -> None:
        self.hass = hass
        self.queue_id = queue_id
        self.send = send
        self._task: asyncio.Task | None = None
        self._last_signature: str | None = None

    async def start(self) -> None:
        await self._push()
        self._task = self.hass.async_create_task(self._run())

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(2)
            await self._push()

    async def _push(self) -> None:
        entry = ma_entry(self.hass)
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
        if mass is None:
            self.send({"type": "queue", "queue_id": self.queue_id, "items": [], "total": 0, "error": "Music Assistant unavailable"})
            return
        try:
            async with asyncio.timeout(12):
                snapshot = await _snapshot(mass, self.queue_id)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("queue snapshot failed: %s", err)
            self.send({"type": "queue", "queue_id": self.queue_id, "items": [], "total": 0, "error": str(err)[:160]})
            return
        signature = json.dumps(
            {key: value for key, value in snapshot.items() if key not in ("updated", "elapsed")},
            sort_keys=True,
            default=str,
        )
        # 队列只在内容变化时推送；播放进度由卡片本地时钟推进，避免每 2 秒重发整窗。
        if signature == self._last_signature:
            return
        self._last_signature = signature
        self.send({"type": "queue", **snapshot})

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None


async def async_register_queue_api(hass: HomeAssistant) -> None:
    """Register queue subscription and command once."""

    if hass.data.get(f"{DOMAIN}_queue_ws"):
        return

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_QUEUE_SUBSCRIBE,
            vol.Optional("entity_id"): str,
            vol.Optional("queue_id"): str,
        }
    )
    @websocket_api.async_response
    async def websocket_subscribe_queue(hass: HomeAssistant, connection, msg) -> None:
        queue_id = _resolve_queue_id(hass, msg.get("entity_id"), msg.get("queue_id"))
        if not queue_id:
            connection.send_error(msg["id"], "queue_not_found", "No Music Assistant queue for this entity")
            return

        @callback
        def send(payload: dict) -> None:
            connection.send_message(websocket_api.event_message(msg["id"], envelope(payload)))

        session = QueueSession(hass, queue_id, send)

        @callback
        def unsubscribe() -> None:
            hass.async_create_task(session.stop())

        connection.subscriptions[msg["id"]] = unsubscribe
        # 先回 subscribe 的 result，再推首帧；否则前端会在订阅 promise 完成前收到事件。
        connection.send_result(msg["id"])
        await session.start()

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_QUEUE_COMMAND,
            vol.Optional("entity_id"): str,
            vol.Optional("queue_id"): str,
            vol.Required("action"): str,
            vol.Optional("item_id"): str,
            vol.Optional("item_id_or_index"): vol.Any(str, int),
            vol.Optional("pos_shift"): int,
            vol.Optional("shuffle"): bool,
            vol.Optional("repeat"): str,
            vol.Optional("position"): vol.Coerce(float),
            vol.Optional("keep_playing"): bool,
            vol.Optional("autoplay"): bool,
            vol.Optional("crossfade"): bool,
            vol.Optional("favorite"): bool,
            vol.Optional("item_uri"): str,
        }
    )
    @websocket_api.async_response
    async def websocket_queue_command(hass: HomeAssistant, connection, msg) -> None:
        queue_id = _resolve_queue_id(hass, msg.get("entity_id"), msg.get("queue_id"))
        if not queue_id:
            connection.send_error(msg["id"], "queue_not_found", "No Music Assistant queue for this entity")
            return
        entry = ma_entry(hass)
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
        if mass is None:
            connection.send_error(msg["id"], "ma_unavailable", "Music Assistant client unavailable")
            return
        action = str(msg["action"])
        try:
            if action == "snapshot":
                result: Any = {"queue": await _snapshot(mass, queue_id)}
            elif action == "play_item":
                if not msg.get("item_id"):
                    raise ValueError("item_id is required")
                result = await mass.send_command(
                    "player_queues/play_index", queue_id=queue_id, index=msg["item_id"]
                )
            elif action == "move_item":
                result = await mass.send_command(
                    "player_queues/move_item",
                    queue_id=queue_id,
                    queue_item_id=msg.get("item_id", ""),
                    pos_shift=int(msg.get("pos_shift") or 0),
                )
            elif action == "remove_item":
                result = await mass.send_command(
                    "player_queues/delete_item",
                    queue_id=queue_id,
                    item_id_or_index=msg.get("item_id_or_index"),
                )
            elif action == "clear":
                result = await mass.send_command(
                    "player_queues/clear",
                    queue_id=queue_id,
                    skip_stop=bool(msg.get("keep_playing")),
                )
            elif action == "shuffle":
                result = await mass.send_command(
                    "player_queues/shuffle",
                    queue_id=queue_id,
                    shuffle_enabled=bool(msg.get("shuffle")),
                )
            elif action == "repeat":
                result = await mass.send_command(
                    "player_queues/repeat",
                    queue_id=queue_id,
                    repeat_mode=str(msg.get("repeat") or "off"),
                )
            elif action in ("play", "pause", "next", "previous", "play_pause", "stop"):
                result = await mass.send_command(
                    f"player_queues/{action}", queue_id=queue_id
                )
            elif action == "seek":
                result = await mass.send_command(
                    "player_queues/seek",
                    queue_id=queue_id,
                    position=float(msg.get("position") or 0),
                )
            elif action == "autoplay":
                result = await mass.send_command(
                    "player_queues/autoplay",
                    queue_id=queue_id,
                    autoplay_enabled=bool(msg.get("autoplay")),
                )
            elif action == "crossfade":
                result = await mass.send_command(
                    "player_queues/crossfade",
                    queue_id=queue_id,
                    crossfade_enabled=bool(msg.get("crossfade")),
                )
            elif action == "favorite":
                uri = str(msg.get("item_uri") or "")
                if not uri:
                    snapshot = await _snapshot(mass, queue_id)
                    active = next((i for i in snapshot.get("items", []) if i.get("active")), {})
                    uri = str(active.get("contentId") or "")
                if not uri:
                    raise ValueError("item_uri is required when no current track is available")
                favorite = bool(msg.get("favorite"))
                if favorite:
                    queue = mass.player_queues.get(queue_id)
                    current = _as_dict(getattr(queue, "current_item", None))
                    media_item = _as_dict(current.get("media_item"))
                    result = await mass.send_command(
                        "music/favorites/add_item",
                        item=media_item or uri,
                    )
                else:
                    snapshot = await _snapshot(mass, queue_id)
                    active = next(
                        (i for i in snapshot.get("items", []) if i.get("active")), {}
                    )
                    media_type = str(active.get("mediaType") or "track")
                    provider = str(active.get("provider") or "")
                    item_id = str(active.get("itemId") or "")
                    if not item_id or not provider:
                        raise ValueError("current track has no provider item id")
                    library_item = await mass.send_command(
                        "music/get_library_item",
                        media_type=media_type,
                        item_id=item_id,
                        provider_instance_id_or_domain=provider,
                    )
                    if not library_item:
                        result = {"ok": True, "favorite": False, "already_absent": True}
                    else:
                        result = await mass.send_command(
                            "music/favorites/remove_item",
                            media_type=media_type,
                            library_item_id=library_item.get("item_id"),
                        )
            elif action == "waveform":
                snapshot = await _snapshot(mass, queue_id)
                current = snapshot.get("items") or []
                active = next((i for i in current if i.get("active")), None)
                if not active:
                    raise ValueError("no active queue item")
                item_id = str(active.get("itemId") or "")
                provider = str(active.get("provider") or "")
                if not item_id or not provider:
                    raise ValueError("current track has no stream details")
                bins = await mass.send_command(
                    "audio_analysis/wave_form",
                    item_id=item_id,
                    provider_instance_id_or_domain=provider,
                )
                result = {"item_id": item_id, "bins": list(bins or [])}
                connection.send_result(msg["id"], {"ok": True, "action": action, "queue_id": queue_id, "result": result})
                return
            else:
                raise ValueError(f"unknown queue action: {action}")
            connection.send_result(msg["id"], {"ok": True, "action": action, "queue_id": queue_id, "result": result})
        except Exception as err:  # noqa: BLE001
            connection.send_error(msg["id"], "queue_command_failed", str(err)[:160])

    websocket_api.async_register_command(hass, websocket_subscribe_queue)
    websocket_api.async_register_command(hass, websocket_queue_command)

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_PLAYER_GROUP,
            vol.Required("action"): vol.In(["players", "set_members"]),
            vol.Optional("target_player"): str,
            vol.Optional("add"): [str],
            vol.Optional("remove"): [str],
        }
    )
    @websocket_api.async_response
    async def websocket_player_group(hass: HomeAssistant, connection, msg) -> None:
        entry = ma_entry(hass)
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
        if mass is None:
            connection.send_error(msg["id"], "ma_unavailable", "Music Assistant client unavailable")
            return
        action = str(msg["action"])
        try:
            if action == "players":
                players = []
                for player in mass.players.players:
                    players.append(
                        {
                            "player_id": str(getattr(player, "player_id", "")),
                            "name": str(getattr(player, "name", "") or getattr(player, "default_name", "")),
                            "available": bool(getattr(player, "available", True)),
                            "powered": getattr(player, "powered", None),
                            "state": str(_enum_value(getattr(player, "playback_state", None), "unknown")),
                            "volume_level": getattr(player, "volume_level", None),
                            "volume_muted": bool(getattr(player, "volume_muted", False)),
                            "group_members": list(getattr(player, "group_members", []) or []),
                            "can_group_with": list(getattr(player, "can_group_with", []) or []),
                            "active_group": getattr(player, "active_group", None),
                            "synced_to": getattr(player, "synced_to", None),
                            "active_source": getattr(player, "active_source", None),
                            "active_queue": getattr(player, "active_source", None),
                        }
                    )
                connection.send_result(msg["id"], {"ok": True, "action": action, "players": players})
                return
            if action == "set_members":
                target = str(msg.get("target_player") or "")
                if not target:
                    raise ValueError("target_player is required")
                await mass.players.set_members(
                    target,
                    player_ids_to_add=list(msg.get("add") or []),
                    player_ids_to_remove=list(msg.get("remove") or []),
                )
                connection.send_result(msg["id"], {"ok": True, "action": action, "target_player": target})
                return
            raise ValueError(f"unknown player action: {action}")
        except Exception as err:  # noqa: BLE001
            connection.send_error(msg["id"], "player_group_failed", str(err)[:160])

    websocket_api.async_register_command(hass, websocket_player_group)

    hass.data[f"{DOMAIN}_queue_ws"] = True
