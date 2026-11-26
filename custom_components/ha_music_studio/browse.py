"""Server-side media browsing + playlist list for the HA Music card.

卡片以前自己在浏览器里打 ``media_player/browse_media``：根目录、歌单文件夹、
子目录要分两次请求，切一层就重来一遍；App WebView 一挂起就整轮丢。这里把
「目录树 + 歌单文件夹定位」放服务端，做短时缓存，卡片只拿拍平后的条目渲染。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import voluptuous as vol
from homeassistant.components import media_player as mp_component
from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN, WS_BROWSE, WS_PLAYLISTS

_LOGGER = __import__("logging").getLogger(__name__)

BROWSE_TTL = 30.0
PLAYLIST_TTL = 120.0
BROWSE_TIMEOUT = 15.0
_CACHE_KEY = f"{DOMAIN}_browse_cache"


def _cache(hass: HomeAssistant) -> dict[str, tuple[float, dict[str, Any]]]:
    return hass.data.setdefault(_CACHE_KEY, {})


def _entity(hass: HomeAssistant, entity_id: str):
    """Look the media_player entity object up without going through a websocket."""
    components = hass.data.get("entity_components") or {}
    component = components.get(mp_component.DOMAIN)
    if component is None:
        return None
    return component.get_entity(entity_id)


async def _async_browse(
    hass: HomeAssistant, entity_id: str, content_id: str | None, content_type: str | None
) -> Any:
    entity = _entity(hass, entity_id)
    if entity is None or not hasattr(entity, "async_browse_media"):
        raise RuntimeError(f"{entity_id} cannot browse media")
    # MediaPlayerEntity.async_browse_media 形参顺序是 (media_content_type, media_content_id)，
    # 位置传参会把两个参数对调，MA 侧就报 Media not found。
    return await asyncio.wait_for(
        entity.async_browse_media(
            media_content_type=content_type or None,
            media_content_id=content_id or None,
        ),
        BROWSE_TIMEOUT,
    )


def _item(node: Any) -> dict[str, Any]:
    return {
        "title": getattr(node, "title", "") or "",
        "cls": getattr(node, "media_class", "") or "",
        "content_id": getattr(node, "media_content_id", "") or "",
        "content_type": getattr(node, "media_content_type", "") or "",
        "thumb": getattr(node, "thumbnail", "") or "",
        "can_play": bool(getattr(node, "can_play", False)),
        "can_expand": bool(getattr(node, "can_expand", False)),
    }


def _folder(node: Any) -> dict[str, Any]:
    return {"title": getattr(node, "title", "") or "", **_item(node)}


def _is_playlist_folder(node: Any) -> bool:
    cls = (getattr(node, "media_class", "") or "").lower()
    ctype = (getattr(node, "media_content_type", "") or "").lower()
    title = (getattr(node, "title", "") or "").lower()
    if "playlist" not in (cls + " " + ctype + " " + title):
        return False
    return bool(getattr(node, "can_expand", False))


def _is_playlist(node: Any) -> bool:
    return "playlist" in (getattr(node, "media_class", "") or "").lower()


@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_BROWSE,
        vol.Required("entity_id"): cv.entity_id,
        vol.Optional("media_content_id"): str,
        vol.Optional("media_content_type"): str,
        vol.Optional("refresh"): bool,
    }
)
@websocket_api.async_response
async def websocket_browse(hass: HomeAssistant, connection, msg) -> None:
    """Browse one level of a media_player library, cached briefly."""
    entity_id = msg["entity_id"]
    content_id = msg.get("media_content_id") or ""
    content_type = msg.get("media_content_type") or ""
    key = f"{entity_id}|{content_id}|{content_type}"
    cache = _cache(hass)
    hit = cache.get(key)
    if hit and not msg.get("refresh") and time.monotonic() - hit[0] < BROWSE_TTL:
        connection.send_result(msg["id"], {**hit[1], "cached": True})
        return
    try:
        root = await _async_browse(hass, entity_id, content_id, content_type)
    except Exception as err:  # noqa: BLE001
        connection.send_result(
            msg["id"],
            {"title": "", "items": [], "error": f"{str(err)[:120]}", "cached": False},
        )
        return
    payload = {
        "title": getattr(root, "title", "") or "",
        "items": [_item(child) for child in (getattr(root, "children", None) or [])],
        "error": "",
        "cached": False,
    }
    cache[key] = (time.monotonic(), payload)
    connection.send_result(msg["id"], payload)


async def _playlist_items(hass: HomeAssistant, entity_id: str) -> dict[str, Any]:
    root = await _async_browse(hass, entity_id, "", "")
    children = list(getattr(root, "children", None) or [])
    folder = next((child for child in children if _is_playlist_folder(child)), None)
    if folder is not None:
        listing = await _async_browse(
            hass,
            entity_id,
            getattr(folder, "media_content_id", "") or "",
            getattr(folder, "media_content_type", "") or "",
        )
        items = [_item(child) for child in (getattr(listing, "children", None) or [])]
    else:
        items = [_item(child) for child in children if _is_playlist(child)]
    items = [item for item in items if item["content_id"]]
    return {
        "title": getattr(folder, "title", "") or "",
        "folder": _folder(folder) if folder is not None else None,
        "items": items,
        "error": "",
    }


@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_PLAYLISTS,
        vol.Required("entity_id"): cv.entity_id,
        vol.Optional("refresh"): bool,
    }
)
@websocket_api.async_response
async def websocket_playlists(hass: HomeAssistant, connection, msg) -> None:
    """Return the playlist folder contents, hunting the folder server-side."""
    entity_id = msg["entity_id"]
    key = f"playlists|{entity_id}"
    cache = _cache(hass)
    hit = cache.get(key)
    if hit and not msg.get("refresh") and time.monotonic() - hit[0] < PLAYLIST_TTL:
        connection.send_result(msg["id"], {**hit[1], "cached": True})
        return
    try:
        payload = await _playlist_items(hass, entity_id)
    except Exception as err:  # noqa: BLE001
        connection.send_result(
            msg["id"],
            {"title": "", "folder": None, "items": [], "error": str(err)[:120], "cached": False},
        )
        return
    cache[key] = (time.monotonic(), payload)
    connection.send_result(msg["id"], {**payload, "cached": False})


async def async_register_browse_api(hass: HomeAssistant) -> None:
    """Register browse commands once per instance."""
    if hass.data.get(f"{DOMAIN}_browse_ws"):
        return
    websocket_api.async_register_command(hass, websocket_browse)
    websocket_api.async_register_command(hass, websocket_playlists)
    hass.data[f"{DOMAIN}_browse_ws"] = True
