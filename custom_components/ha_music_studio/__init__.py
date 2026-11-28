"""HA Music Studio: server-side realtime spectrum for the HA Music card."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .browse import async_register_browse_api
from .const import DOMAIN
from .effects import async_register_effects_api
from .frontend import async_register_card
from .lyrics import async_register_lyrics_api
from .progress import async_register_progress_api
from .queue import async_register_queue_api
from .spectrum import async_register_websocket_api
from .webplayer import async_register_web_player_api

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = []


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up a config entry."""
    hass.data.setdefault(DOMAIN, {})
    await async_register_card(hass)
    async_register_websocket_api(hass)
    async_register_lyrics_api(hass)
    await async_register_queue_api(hass)
    await async_register_progress_api(hass)
    await async_register_effects_api(hass)
    await async_register_web_player_api(hass, entry)
    await async_register_browse_api(hass)
    _LOGGER.info("HA Music Studio started")
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    return True
