"""HA Music Studio: server-side realtime spectrum for the HA Music card."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.helpers import config_validation as cv

from .browse import async_register_browse_api
from .const import DOMAIN
from .diagnostics import async_register_diagnostics
from .effects import async_register_effects_api
from .frontend import async_register_card
from .lyrics import async_register_lyrics_api
from .probe import async_register_probe
from .progress import async_register_progress_api
from .queue import async_register_queue_api
from .spectrum import async_register_websocket_api
from .webplayer import async_register_web_player_api

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = []

SERVICE_CLEANUP_LEGACY = "cleanup_legacy"
LEGACY_DOMAIN = "ha_music_analysis"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up a config entry."""
    hass.data.setdefault(DOMAIN, {})
    await async_register_card(hass)
    async_register_websocket_api(hass)
    async_register_lyrics_api(hass)
    await async_register_queue_api(hass)
    await async_register_progress_api(hass)
    await async_register_effects_api(hass)
    await async_register_diagnostics(hass)
    await async_register_probe(hass)
    await async_register_web_player_api(hass, entry)
    await async_register_browse_api(hass)
    _register_cleanup_service(hass)
    _LOGGER.info("HA Music Studio started")
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    return True


def _register_cleanup_service(hass: HomeAssistant) -> None:
    """Register a one-shot service that removes the retired analysis component."""
    if hass.services.has_service(DOMAIN, SERVICE_CLEANUP_LEGACY):
        return

    async def handle(call: ServiceCall) -> dict[str, object]:
        if not call.data.get("confirm"):
            return {"removed": False, "error": "confirm must be true"}
        target = Path(__file__).resolve().parent.parent / LEGACY_DOMAIN
        if not target.is_dir():
            return {"removed": False, "path": str(target), "error": "not found"}
        await hass.async_add_executor_job(shutil.rmtree, target, True)
        _LOGGER.warning("Removed retired component directory %s", target)
        return {"removed": True, "path": str(target)}

    hass.services.async_register(
        DOMAIN,
        SERVICE_CLEANUP_LEGACY,
        handle,
        schema=vol.Schema({vol.Required("confirm"): cv.boolean}),
        supports_response=SupportsResponse.OPTIONAL,
    )
