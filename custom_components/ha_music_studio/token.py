"""Music Assistant connection details owned by this integration."""

from __future__ import annotations

from homeassistant.const import CONF_TOKEN, CONF_URL
from homeassistant.core import HomeAssistant

from .const import DOMAIN


def _data(hass: HomeAssistant) -> dict:
    return hass.data.get(DOMAIN, {})


async def async_get_viewer_token(hass: HomeAssistant) -> str | None:
    """Return the Music Assistant token the user entered in the config flow."""
    token = _data(hass).get(CONF_TOKEN)
    return token if isinstance(token, str) and token else None


def entry_url(hass: HomeAssistant) -> str | None:
    """Return the Music Assistant webserver URL from our own entry."""
    url = _data(hass).get(CONF_URL)
    if not isinstance(url, str) or not url:
        return None
    return url.replace(":8094", ":8095")
