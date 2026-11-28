"""Server-side MilkDrop PCM tap -> compact FFT band stream over HA WebSocket."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

import voluptuous as vol
from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

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
from .mass_client import get_mass
from .protocol import envelope
from .token import async_get_viewer_token, entry_url

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
    # 实体不可用/未知时，属性里可能仍残留已删除的播放器 id，不能再拿它去连 MA。
    if str(state.state).lower() in ("unavailable", "unknown"):
        return None
    for key in ("mass_player_id", "player_id", "ma_player_id", "active_queue"):
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
        # 三频段分界：低 <250Hz，中 250-2000Hz，高 >2000Hz；按实际 log 频段
        # 取整，避免前端按“柱数三等分”把中低频算进高频。
        self._zone_split = (BAND_COUNT // 3, (BAND_COUNT * 2) // 3)
        try:
            import numpy as np

            self._np = np
            self._window = np.hanning(FFT_SIZE)
            freqs = np.fft.rfftfreq(FFT_SIZE, 1 / SAMPLE_RATE)
            self._freqs = freqs
            self._edges = np.geomspace(MIN_FREQ, MAX_FREQ, BAND_COUNT + 1)
            low_end = int(np.searchsorted(self._edges, 250.0))
            mid_end = int(np.searchsorted(self._edges, 2000.0))
            low_end = min(max(1, low_end), BAND_COUNT - 2)
            mid_end = min(max(low_end + 1, mid_end), BAND_COUNT - 1)
            self._zone_split = (low_end, mid_end)
        except Exception:  # noqa: BLE001  # pragma: no cover - numpy is present on HAOS
            self._np = None
            _LOGGER.warning("numpy unavailable; spectrum falls back to RMS bands")

    def buffer_size(self) -> int:
        return 0 if self._buf is None else int(self._buf.size)

    def feed(self, frame: bytes) -> None:
        if len(frame) < 9 + 1024:
            return
        samples = frame[9 : 9 + 1024]
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
            "zones": self._zone_levels(self._levels),
            "zone_split": list(self._zone_split),
            "peak": round(max(self._levels), 3),
            "rms": round(min(1.0, rms * 2.2), 3),
        }

    def _zone_levels(self, levels: list[float]) -> dict[str, float]:
        low_end, mid_end = self._zone_split

        def average(values: list[float]) -> float:
            return round(sum(values) / len(values), 3) if values else 0.0

        return {
            "low": average(levels[:low_end]),
            "mid": average(levels[low_end:mid_end]),
            "high": average(levels[mid_end:]),
        }

    def _fallback(self) -> dict[str, Any]:
        levels = self._levels
        return {
            "bands": levels,
            "zones": self._zone_levels(levels),
            "zone_split": list(self._zone_split),
            "peak": max(levels),
            "rms": 0.0,
        }


class _NoSpectrumPlayer(Exception):
    """Raised when the selected entity has no live Music Assistant player."""


class SpectrumSession:
    """One MA relay connection feeding one HA websocket subscriber."""

    def __init__(
        self, hass: HomeAssistant, entity_id: str | None, send, player: str | None = None
    ) -> None:
        self.hass = hass
        self.entity_id = entity_id
        self.send = send
        self.requested_player = player
        self._force_auto = False
        self._failure_count = 0
        self._task: asyncio.Task | None = None
        self._stopped = False

    async def start(self) -> None:
        self._task = self.hass.async_create_background_task(
            self._run(), "ha_music_studio spectrum session"
        )

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
                self._failure_count = 0
            except asyncio.CancelledError:
                raise
            except _NoSpectrumPlayer:
                # 当前实体不是存活的 MA 播放器（HASS.Agent、已删除的 Web 播放器、
                # 电视等）：频谱没有数据源，安静等待，不连 MA、不打日志。
                self._failure_count = 0
                backoff = 5.0
            except Exception as err:  # noqa: BLE001
                self._failure_count += 1
                message = str(err)
                # 旧 Web 播放器被删除后，原连接会一直拿死 id 重试，MA 只会回
                # "no such player"。第一次遇到就永久退回自动选择当前播放器。
                if (
                    "no such player" in message
                    and not self._force_auto
                    and (self.requested_player or self.entity_id)
                ):
                    self._force_auto = True
                    _LOGGER.warning(
                        "Spectrum player for %s no longer exists; falling back to auto",
                        self.entity_id or self.requested_player,
                    )
                    backoff = 1.0
                elif self._failure_count <= 3:
                    _LOGGER.warning("Spectrum session failed: %s", message[-220:])
                else:
                    _LOGGER.debug("Spectrum session still failing: %s", message[-220:])
                if self._failure_count <= 3:
                    self.send({"type": "error", "message": message[-450:]})
            if self._stopped:
                return
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.6)

    async def _connect_once(self) -> None:
        token = await async_get_viewer_token(self.hass)
        if not token:
            raise RuntimeError("No Music Assistant token available")
        base = entry_url(self.hass)
        if not base:
            raise RuntimeError("No Music Assistant URL available")
        mass = get_mass(self.hass)
        player = (
            None
            if self._force_auto
            else (_player_for_entity(self.hass, self.entity_id) or self.requested_player)
        )
        if player and mass is not None:
            try:
                live_ids = {item.player_id for item in mass.players.players}
                if player not in live_ids:
                    player = None
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Spectrum player list unavailable: %s", err)
        if not player:
            # 实体存在但映射不到存活的 MA 播放器（HASS.Agent、电视、已删除的 Web
            # 播放器）时，频谱没有数据源：安静退出，不借用别的设备，也不连 MA。
            if self.entity_id:
                raise _NoSpectrumPlayer()
            if mass is not None:
                try:
                    for candidate in mass.players.players:
                        state = getattr(candidate, "state", None)
                        value = getattr(state, "value", state)
                        if str(value).lower() in ("playing", "playerstate.playing"):
                            player = candidate.player_id
                            break
                except Exception:  # noqa: BLE001
                    player = None
        if not player:
            raise _NoSpectrumPlayer()
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
                    except Exception:
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

            pump_task = self.hass.async_create_background_task(
                pump(), "ha_music_studio spectrum pump"
            )
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
        connection.send_message(websocket_api.event_message(msg["id"], envelope(payload)))

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
