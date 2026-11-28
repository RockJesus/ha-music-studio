"""HA Music Studio: server-side realtime spectrum for the HA Music card."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_TOKEN, CONF_URL
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .browse import async_register_browse_api
from .const import DOMAIN
from .effects import async_register_effects_api
from .frontend import async_register_card
from .lyrics import async_register_lyrics_api
from .mass_client import DATA_CLIENT, async_open_client
from .progress import async_register_progress_api
from .queue import async_register_queue_api
from .spectrum import async_register_websocket_api
from .webplayer import async_register_web_player_api

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = []


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up a config entry."""
    data = hass.data.setdefault(DOMAIN, {})
    url = str(entry.data.get(CONF_URL) or "")
    token = str(entry.data.get(CONF_TOKEN) or "")
    if not url or not token:
        raise ConfigEntryNotReady("Music Assistant URL and token are required")

    if data.get(DATA_CLIENT) is None:
        try:
            data[DATA_CLIENT] = await async_open_client(hass, url, token)
        except Exception as err:
            raise ConfigEntryNotReady(f"Music Assistant at {url} is not reachable") from err
    data[CONF_URL] = url
    data[CONF_TOKEN] = token

    await async_register_card(hass)
    if not data.get("ws_registered"):
        async_register_websocket_api(hass)
        async_register_lyrics_api(hass)
        await async_register_queue_api(hass)
        await async_register_progress_api(hass)
        await async_register_effects_api(hass)
        await async_register_web_player_api(hass)
        await async_register_browse_api(hass)
        data["ws_registered"] = True
    _LOGGER.info("HA Music Studio started")
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload the entry, close the connection and forget its credentials."""
    data = hass.data.get(DOMAIN, {})
    client = data.pop(DATA_CLIENT, None)
    data.pop(CONF_URL, None)
    data.pop(CONF_TOKEN, None)
    if client is not None:
        await client.disconnect()
    return True
