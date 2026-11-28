"""1 Hz playback progress + palette push.

The card used to run a requestAnimationFrame loop for the whole playback to keep
the seek bar moving, and sampled cover pixels on a canvas for the accent colour.
Both now live here: the progress frame carries elapsed/duration/state and, when
the track changes, the artwork URL plus its server-computed palette.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

import voluptuous as vol
from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant, callback

from .const import DOMAIN
from .mass_client import get_mass
from .palette import async_palette
from .protocol import envelope
from .queue import _as_dict, _enum_value, _image_url, _resolve_queue_id

_LOGGER = logging.getLogger(__name__)

WS_PROGRESS = "ha_music_studio/subscribe_progress"
FRAME_INTERVAL = 1.0


def _frame(mass: Any, queue_id: str) -> dict[str, Any] | None:
    queue = mass.player_queues.get(queue_id)
    if queue is None:
        return None
    item = _as_dict(getattr(queue, "current_item", None))
    state = str(_enum_value(getattr(queue, "state", None), "unknown"))
    duration = float(item.get("duration") or 0)
    elapsed = float(getattr(queue, "corrected_elapsed_time", 0) or 0)
    if duration > 0:
        elapsed = max(0.0, min(elapsed, duration))
    return {
        "type": "progress",
        "queue_id": queue_id,
        "state": state,
        "playing": state.lower().endswith("playing"),
        "elapsed": round(elapsed, 2),
        "duration": round(duration, 2),
        "speed": float(getattr(queue, "playback_speed", 1) or 1),
        "title": str(
            _as_dict(item.get("media_item")).get("name")
            or item.get("name")
            or ""
        ),
        "art": _image_url(item),
        "ts": round(time.time(), 3),
    }


class ProgressSession:
    """Push a tiny progress frame once a second for one queue."""

    def __init__(self, hass: HomeAssistant, queue_id: str, send: Callable) -> None:
        self.hass = hass
        self.queue_id = queue_id
        self.send = send
        self._task: asyncio.Task | None = None
        self._art: str | None = None
        self._palette: dict[str, Any] | None = None

    async def start(self) -> None:
        await self._push()
        self._task = self.hass.async_create_background_task(
            self._run(), "ha_music_studio progress session"
        )

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(FRAME_INTERVAL)
            await self._push()

    async def _push(self) -> None:
        entry_mass = get_mass(self.hass)
        if entry_mass is None:
            return
        try:
            frame = _frame(entry_mass, self.queue_id)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("progress frame failed: %s", str(err)[:160])
            return
        if frame is None:
            return
        art = frame.get("art") or ""
        if art and art != self._art:
            self._art = art
            self._palette = await async_palette(self.hass, art)
        elif not art:
            self._art = ""
            self._palette = None
        if self._palette:
            frame["palette"] = self._palette
        self.send(envelope(frame))

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None


async def async_register_progress_api(hass: HomeAssistant) -> None:
    """Register the progress subscription once per HA instance."""
    if hass.data.get(f"{DOMAIN}_progress_ws"):
        return

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_PROGRESS,
            vol.Optional("entity_id"): str,
            vol.Optional("queue_id"): str,
        }
    )
    @websocket_api.async_response
    async def websocket_subscribe_progress(hass: HomeAssistant, connection, msg) -> None:
        queue_id = _resolve_queue_id(hass, msg.get("entity_id"), msg.get("queue_id"))
        if not queue_id:
            connection.send_error(
                msg["id"], "queue_not_found", "No Music Assistant queue for this entity"
            )
            return

        session = ProgressSession(
            hass,
            queue_id,
            lambda payload: connection.send_message(
                websocket_api.event_message(msg["id"], payload)
            ),
        )

        @callback
        def unsubscribe() -> None:
            hass.async_create_task(session.stop())

        connection.subscriptions[msg["id"]] = unsubscribe
        connection.send_result(msg["id"])
        await session.start()

    websocket_api.async_register_command(hass, websocket_subscribe_progress)
    hass.data[f"{DOMAIN}_progress_ws"] = True
