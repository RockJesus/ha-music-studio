"""Music Assistant DSP sound-effect gateway over Home Assistant WebSocket."""

from __future__ import annotations

import copy
import logging
from typing import Any

import voluptuous as vol
from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant

from .const import DOMAIN, WS_SOUND_EFFECT
from .mass_client import get_mass

_LOGGER = logging.getLogger(__name__)

EFFECTS: dict[str, dict[str, Any]] = {
    "hifi": {
        "filters": [
            {"type": "stereo_width", "enabled": True, "width": 1.55},
            {"type": "tone_control", "enabled": True, "bass_level": 2.0, "mid_level": 0.0, "treble_level": 1.0},
        ]
    },
    "vocal": {
        "filters": [
            {"type": "tone_control", "enabled": True, "bass_level": -2.0, "mid_level": 3.0, "treble_level": 1.0},
            {"type": "stereo_width", "enabled": True, "width": 1.15},
        ]
    },
    "bass": {
        "filters": [
            {"type": "tone_control", "enabled": True, "bass_level": 5.0, "mid_level": 0.0, "treble_level": 0.0},
            {"type": "stereo_width", "enabled": True, "width": 1.15},
        ]
    },
}


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return copy.deepcopy(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        return copy.deepcopy(result) if isinstance(result, dict) else {}
    return {}


def _player_id(hass: HomeAssistant, explicit: str | None, entity_id: str | None) -> str:
    if explicit:
        return explicit
    if not entity_id:
        return ""
    state = hass.states.get(entity_id)
    if state is None:
        return ""
    for key in ("mass_player_id", "player_id", "ma_player_id", "active_queue", "active_source"):
        value = state.attributes.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _is_same_filter(existing: dict[str, Any], wanted: dict[str, Any]) -> bool:
    if str(existing.get("type")) != str(wanted.get("type")):
        return False
    keys = [key for key in wanted if key not in ("type", "enabled")]
    return all(existing.get(key) == wanted.get(key) for key in keys)


def _detect_effect(config: dict[str, Any]) -> str:
    if not config.get("enabled"):
        return "off"
    filters = [f for f in config.get("filters") or [] if isinstance(f, dict) and f.get("enabled", True)]
    for effect, preset in EFFECTS.items():
        if all(any(_is_same_filter(f, wanted) for f in filters) for wanted in preset["filters"]):
            return effect
    return "custom"


async def _save(mass: Any, player_id: str, config: dict[str, Any]) -> dict[str, Any]:
    result = await mass.send_command(
        "config/players/dsp/save", player_id=player_id, config=config
    )
    return _as_dict(result)


async def async_register_effects_api(hass: HomeAssistant) -> None:
    """Register the DSP sound-effect command."""
    hass.data.setdefault(DOMAIN, {})

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_SOUND_EFFECT,
            vol.Required("action"): vol.In(["get", "set"]),
            vol.Optional("effect"): vol.In(["off", *EFFECTS.keys()]),
            vol.Optional("player_id"): str,
            vol.Optional("entity_id"): str,
        }
    )
    @websocket_api.async_response
    async def websocket_sound_effect(hass: HomeAssistant, connection, msg) -> None:
        mass = get_mass(hass)
        if mass is None:
            connection.send_error(msg["id"], "ma_unavailable", "Music Assistant client unavailable")
            return
        player_id = _player_id(hass, msg.get("player_id"), msg.get("entity_id"))
        if not player_id:
            connection.send_error(msg["id"], "player_not_found", "No Music Assistant player for this entity")
            return
        baselines = hass.data[DOMAIN].setdefault("dsp_baseline", {})
        try:
            current = _as_dict(
                await mass.send_command("config/players/dsp/get", player_id=player_id)
            )
            action = str(msg["action"])
            if action == "get":
                connection.send_result(
                    msg["id"],
                    {
                        "ok": True,
                        "player_id": player_id,
                        "effect": _detect_effect(current),
                        "config": current,
                    },
                )
                return
            effect = str(msg.get("effect") or "off")
            if effect == "off":
                baseline = baselines.pop(player_id, None)
                if baseline:
                    config = baseline
                else:
                    config = current
                    config["enabled"] = False
            else:
                if player_id not in baselines:
                    baselines[player_id] = current
                config = copy.deepcopy(current)
                config["enabled"] = True
                config["preset_id"] = None
                filters = [
                    f for f in config.get("filters") or [] if isinstance(f, dict)
                ]
                for wanted in EFFECTS[effect]["filters"]:
                    if not any(_is_same_filter(f, wanted) for f in filters):
                        filters.append(copy.deepcopy(wanted))
                if not any(str(f.get("type")) == "safety_limiter" for f in filters):
                    filters.append(
                        {"type": "safety_limiter", "enabled": True, "ceiling": -2.0}
                    )
                config["filters"] = filters
            saved = await _save(mass, player_id, config)
            connection.send_result(
                msg["id"],
                {
                    "ok": True,
                    "player_id": player_id,
                    "effect": _detect_effect(saved or config),
                    "config": saved or config,
                },
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("sound effect failed: %s", err)
            connection.send_error(msg["id"], "sound_effect_failed", str(err)[:200])

    websocket_api.async_register_command(hass, websocket_sound_effect)
