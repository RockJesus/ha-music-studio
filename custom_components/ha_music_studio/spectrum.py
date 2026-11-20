"""Server-side MilkDrop PCM tap -> compact FFT band stream over HA WebSocket."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable
from typing import Any

import voluptuous as vol

from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .token import async_get_viewer_token, entry_url, ma_entry
from .const import (
    BAND_COUNT,
    DOMAIN,
    FFT_SIZE,
    FRAME_INTERVAL,
    MAX_FREQ,
    MIN_FREQ,
    SAMPLE_RATE,
    WS_SUBSCRIBE,
)

_LOGGER = logging.getLogger(__name__)

WAVE_TAG = 22


def _ws_url(url: str, player: str | None) -> str:
    base = url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://") :]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://") :]
    elif not base.startswith(("ws://", "wss://")):
        base = "ws://" + base
    result = base + "/milkdrop_visualizer"
    if player:
        result += "?player=" + player
    return result


def _player_for_entity(hass: HomeAssistant, entity_id: str | None) -> str | None:
    """Map a media_player entity to an MA player id."""
    if not entity_id:
        return None
    state = hass.states.get(entity_id)
    if state is None:
        return None
    for key in ("mass_player_id", "player_id", "ma_player_id"):
        value = state.attributes.get(key)
        if isinstance(value, str) and value:
            return value
    return None


class BandAnalyzer:
    """Turn 1024-sample PCM tails into 48 smoothed log bands."""

    def __init__(self) -> None:
        self._buf: Any = None
        self._levels = [0.0] * BAND_COUNT
        self._edges = None
        self._window = None
        try:
            import numpy as np

            self._np = np
            self._window = np.hanning(FFT_SIZE)
            freqs = np.fft.rfftfreq(FFT_SIZE, 1 / SAMPLE_RATE)
            self._freqs = freqs
            self._edges = np.geomspace(MIN_FREQ, MAX_FREQ, BAND_COUNT + 1)
        except Exception:  # pragma: no cover - numpy is present on HAOS
            self._np = None
            _LOGGER.warning("numpy unavailable; spectrum falls back to RMS bands")

    def buffer_size(self) -> int:
        return 0 if self._buf is None else int(self._buf.size)

    def feed(self, frame: bytes) -> None:
        if len(frame) < 10 + 1024:
            return
        samples = frame[10 : 10 + 1024]
        if self._np is None:
            return
        chunk = self._np.frombuffer(samples, dtype=self._np.uint8).astype(self._np.float32)
        chunk -= 128.0
        chunk /= 128.0
        if self._buf is None:
            self._buf = chunk
        else:
            self._buf = self._np.concatenate((self._buf, chunk))[-FFT_SIZE:]

    def compute(self) -> dict[str, Any] | None:
        if self._np is None:
            return self._fallback()
        if self._buf is None or self._buf.size < 256:
            return None
        np = self._np
        x = self._buf
        if x.size < FFT_SIZE:
            x = np.concatenate((np.zeros(FFT_SIZE - x.size, dtype=np.float32), x))
        spectrum = np.abs(np.fft.rfft(x * self._window))
        db = 20.0 * np.log10(spectrum + 1e-9)
        ref = max(float(db.max()), -50.0)
        values: list[float] = []
        for low, high in zip(self._edges[:-1], self._edges[1:]):
            mask = (self._freqs >= low) & (self._freqs < high)
            if not np.any(mask):
                values.append(0.0)
                continue
            level = float(db[mask].max())
            v = (level - (ref - 52.0)) / 52.0
            values.append(min(1.0, max(0.0, v)) ** 1.25)
        for i, v in enumerate(values):
            prior = self._levels[i]
            self._levels[i] = v if v >= prior else prior * 0.88 + v * 0.12
        rms = float(np.sqrt(np.mean(x * x)))
        return {
            "bands": [round(v, 3) for v in self._levels],
            "peak": round(max(self._levels), 3),
            "rms": round(min(1.0, rms * 2.2), 3),
        }

    def _fallback(self) -> dict[str, Any]:
        levels = self._levels
        return {"bands": levels, "peak": max(levels), "rms": 0.0}


class SpectrumSession:
    """One MA relay connection feeding one HA websocket subscriber."""

    def __init__(
        self, hass: HomeAssistant, entity_id: str | None, send, player: str | None = None
    ) -> None:
        self.hass = hass
        self.entity_id = entity_id
        self.send = send
        self.requested_player = player
        self._task: asyncio.Task | None = None
        self._stopped = False

    async def start(self) -> None:
        self._task = self.hass.async_create_task(self._run())

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopped:
            try:
                await self._connect_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                import traceback

                _LOGGER.exception("Spectrum session failed")
                self.send({"type": "error", "message": traceback.format_exc()[-450:]})
            if self._stopped:
                return
            await asyncio.sleep(backoff)
            backoff = min(10.0, backoff * 1.6)

    async def _connect_once(self) -> None:
        entry = ma_entry(self.hass)
        if entry is None:
            raise RuntimeError("Music Assistant integration is not loaded")
        token = await async_get_viewer_token(self.hass, entry)
        if not token:
            raise RuntimeError("No Music Assistant token available")
        base = entry_url(entry)
        if not base:
            raise RuntimeError("No Music Assistant URL available")
        player = _player_for_entity(self.hass, self.entity_id) or self.requested_player
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None)
        if not player and mass is not None:
            try:
                for candidate in mass.players.players:
                    state = getattr(candidate, "state", None)
                    value = getattr(state, "value", state)
                    if str(value).lower() in ("playing", "playerstate.playing"):
                        player = candidate.player_id
                        break
            except Exception:  # noqa: BLE001
                player = None
        session = async_get_clientsession(self.hass, verify_ssl=False)
        async with session.ws_connect(
            _ws_url(base, player),
            heartbeat=25,
            ssl=False,
            timeout=20,
        ) as ws:
            await ws.send_json({"type": "auth", "token": token})
            first = await ws.receive(timeout=15)
            if first.type.name == "CLOSE":
                code = first.data
                reason = getattr(first, "extra", "")
                raise RuntimeError(f"MA closed before auth: code={code} reason={reason}")
            if first.type.name != "TEXT" or "auth_ok" not in str(first.data or ""):
                raise RuntimeError(f"Music Assistant rejected the spectrum token: {str(first.data)[:80]}")
            self.send({"type": "ready", "player": player or "auto"})
            analyzer = BandAnalyzer()
            waves = 0
            texts: list[str] = []
            last_debug = 0.0
            stop = asyncio.Event()

            async def pump() -> None:
                nonlocal last_debug
                last_push = 0.0
                while not stop.is_set():
                    await asyncio.sleep(FRAME_INTERVAL)
                    now = time.monotonic()
                    if now - last_push < FRAME_INTERVAL:
                        continue
                    try:
                        payload = analyzer.compute()
                    except Exception as err:  # noqa: BLE001
                        import traceback
                        self.send({"type": "error", "message": traceback.format_exc()[-500:]})
                        _LOGGER.exception("Band computation failed")
                        return
                    if payload:
                        payload["type"] = "spectrum"
                        payload["ts"] = now
                        payload["frames"] = waves
                        self.send(payload)
                    last_push = now
                    if now - last_debug >= 5.0:
                        last_debug = now
                        self.send(
                            {
                                "type": "debug",
                                "message": f"waves={waves} buf={analyzer.buffer_size()}",
                            }
                        )

            pump_task = self.hass.async_create_task(pump())
            try:
                async for msg in ws:
                    if msg.type.name == "BINARY":
                        if msg.data and msg.data[0] == WAVE_TAG:
                            analyzer.feed(msg.data)
                            waves += 1
                            if waves == 1:
                                self.send({"type": "debug", "message": "first wave frame"})
                    elif msg.type.name == "TEXT":
                        text = str(msg.data)[:180]
                        texts.append(text)
                        self.send({"type": "ma_text", "message": text})
            finally:
                stop.set()
                pump_task.cancel()
                try:
                    await pump_task
                except asyncio.CancelledError:
                    pass
            raise RuntimeError(
                f"Music Assistant visualizer closed the stream code={ws.close_code} texts={texts[-3:]} waves={waves}"
            )


async def _async_subscribe(
    hass: HomeAssistant, entity_id: str | None, send: Callable, player: str | None = None
) -> Callable:
    session = SpectrumSession(hass, entity_id, send, player)
    await session.start()

    @callback
    def unsubscribe() -> None:
        hass.async_create_task(session.stop())

    return unsubscribe


@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_SUBSCRIBE,
        vol.Optional("entity_id"): str,
        vol.Optional("player"): str,
    }
)
@websocket_api.async_response
async def websocket_subscribe(hass: HomeAssistant, connection, msg) -> None:
    """Subscribe to a 30fps compact band stream."""
    entity_id = msg.get("entity_id")

    @callback
    def send(payload: dict) -> None:
        connection.send_message(websocket_api.event_message(msg["id"], payload))

    try:
        unsubscribe = await _async_subscribe(hass, entity_id, send, msg.get("player"))
    except Exception as err:  # noqa: BLE001
        connection.send_error(msg["id"], "subscribe_failed", str(err)[:160])
        return

    connection.subscriptions[msg["id"]] = unsubscribe
    connection.send_result(msg["id"])


@callback
def async_register_websocket_api(hass: HomeAssistant) -> None:
    """Register the command once per HA instance."""
    if hass.data.get(f"{DOMAIN}_ws"):
        return
    websocket_api.async_register_command(hass, websocket_subscribe)
    hass.data[f"{DOMAIN}_ws"] = True
