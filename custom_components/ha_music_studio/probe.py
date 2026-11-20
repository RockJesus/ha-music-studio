"""Temporary raw relay probe for the MilkDrop stream."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import voluptuous as vol
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import DOMAIN
from .spectrum import _player_for_entity, _ws_url
from .token import async_get_viewer_token, entry_url, ma_entry


async def _queue_info(hass: HomeAssistant, player: str) -> dict[str, Any]:
    """Return MA queue internals for a player, defensively."""
    entry = ma_entry(hass)
    mass = getattr(getattr(entry, "runtime_data", None), "mass", None) if entry else None
    if mass is None:
        return {"error": "Music Assistant client unavailable"}
    try:
        queue = await mass.player_queues.get_active_queue(player)
    except Exception as err:  # noqa: BLE001
        return {"error": f"{err.__class__.__name__}: {err}"}
    if queue is None:
        return {"queue": None}
    item = queue.current_item
    details = getattr(item, "streamdetails", None) if item else None
    buffer = getattr(details, "buffer", None) if details else None
    return {
        "state": str(getattr(queue, "state", None)),
        "elapsed": getattr(queue, "corrected_elapsed_time", None),
        "playback_speed": getattr(queue, "playback_speed", None),
        "item": getattr(item, "name", None) or getattr(item, "uri", None),
        "item_id": getattr(item, "queue_item_id", None),
        "buffer": None
        if buffer is None
        else {
            "type": type(buffer).__name__,
            "first_chunk": getattr(buffer, "first_buffered_chunk", None),
            "seconds_available": getattr(buffer, "seconds_available", None),
            "pcm_format": str(getattr(buffer, "pcm_format", None)),
        },
    }


async def async_register_probe(hass: HomeAssistant) -> None:
    """Register the raw probe service once."""

    async def handle(call: ServiceCall) -> dict:
        player = call.data.get("player")
        seconds = int(call.data.get("seconds", 10))
        entity_id = call.data.get("entity_id")
        entry = ma_entry(hass)
        if entry is None:
            return {"error": "Music Assistant integration is not loaded"}
        token = await async_get_viewer_token(hass, entry)
        base = entry_url(entry)
        if not token or not base:
            return {"error": "Music Assistant token or URL unavailable"}
        target = player or _player_for_entity(hass, entity_id) or ""
        if not target:
            return {"error": "No player id resolved", "entity_id": entity_id}
        out: dict[str, Any] = {
            "player": target,
            "url": _ws_url(base, target),
            "queue_before": await _queue_info(hass, target),
        }
        counts: dict[str, int] = {}
        texts: list[str] = []
        first_binary: dict[str, Any] | None = None
        binary_bytes = 0
        started = time.monotonic()
        session = async_get_clientsession(hass, verify_ssl=False)
        try:
            async with session.ws_connect(
                _ws_url(base, target), heartbeat=25, ssl=False, timeout=20
            ) as ws:
                await ws.send_json({"type": "auth", "token": token})
                first = await ws.receive(timeout=15)
                out["auth"] = str(first.data or "")[:100]
                try:
                    async with asyncio.timeout(seconds):
                        async for msg in ws:
                            if msg.type.name == "BINARY":
                                data = msg.data or b""
                                tag = str(data[0]) if data else "-"
                                counts[tag] = counts.get(tag, 0) + 1
                                binary_bytes += len(data)
                                if first_binary is None:
                                    first_binary = {
                                        "tag": tag,
                                        "bytes": len(data),
                                        "head": bytes(data[:10]).hex(),
                                    }
                            elif msg.type.name == "TEXT":
                                text = str(msg.data or "")[:220]
                                texts.append(text)
                            if len(texts) + sum(counts.values()) <= 12:
                                # no-op, keep the stream fast
                                pass
                except TimeoutError:
                    pass
        except Exception as err:  # noqa: BLE001
            out["error"] = f"{err.__class__.__name__}: {err}"
        out.update(
            {
                "seconds": round(time.monotonic() - started, 2),
                "binary_counts": counts,
                "binary_bytes": binary_bytes,
                "first_binary": first_binary,
                "text_count": len(texts),
                "texts": texts[:12],
                "queue_after": await _queue_info(hass, target),
                "close_code": getattr(ws, "close_code", None) if "ws" in locals() else None,
            }
        )
        return out

    if not hass.services.has_service(DOMAIN, "probe"):
        hass.services.async_register(
            DOMAIN,
            "probe",
            handle,
            schema=vol.Schema(
                {
                    vol.Optional("player"): str,
                    vol.Optional("entity_id"): str,
                    vol.Optional("seconds", default=10): vol.All(int, vol.Range(min=3, max=60)),
                }
            ),
            supports_response=SupportsResponse.ONLY,
        )
