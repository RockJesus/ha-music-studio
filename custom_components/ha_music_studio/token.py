"""Return a token accepted by the MA webserver visualizer relay."""

from __future__ import annotations

import logging

from homeassistant.const import CONF_TOKEN, CONF_URL
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


def ma_entry(hass: HomeAssistant):
    """Return the loaded Music Assistant config entry, if any."""
    for entry in hass.config_entries.async_entries("music_assistant"):
        if entry.state.name == "LOADED":
            return entry
    return None


def _connection_token(entry) -> str | None:
    mass = getattr(getattr(entry, "runtime_data", None), "mass", None)
    conn = getattr(mass, "connection", None)
    value = getattr(conn, "auth_token", None)
    if isinstance(value, str) and value:
        return value
    for obj in (mass, getattr(mass, "auth", None)):
        for attr in ("token", "_token"):
            value = getattr(obj, attr, None)
            if isinstance(value, str) and value:
                return value
    return None


async def async_get_viewer_token(hass: HomeAssistant, entry) -> str | None:
    """Mint one viewer token once and reuse it for every spectrum session."""
    cache = hass.data.setdefault(DOMAIN, {})
    if token := cache.get("viewer_token"):
        return token
    mass = getattr(getattr(entry, "runtime_data", None), "mass", None)
    if mass is not None:
        try:
            token = await mass.auth.create_token("HA Music Studio")
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Could not mint MA viewer token: %s", err.__class__.__name__)
        else:
            if token:
                cache["viewer_token"] = token
                return token
    token = _connection_token(entry) or entry.data.get(CONF_TOKEN)
    if token:
        cache["viewer_token"] = token
    return token


def entry_url(entry) -> str | None:
    """Return the MA webserver URL.

    The config entry points at the Supervisor add-on port (8094); the public
    webserver inside the add-on listens on 8095.
    """
    url = entry.data.get(CONF_URL)
    if not url:
        return None
    return url.replace(":8094", ":8095")
