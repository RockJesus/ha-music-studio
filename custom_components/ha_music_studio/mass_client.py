"""Own Music Assistant client for HA Music Studio.

The integration opens its own connection with the URL and token the user
enters in the config flow; it never touches the Music Assistant integration's
config entry or its runtime client.
"""

from __future__ import annotations

from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from music_assistant_client import MusicAssistantClient

from .const import DOMAIN

DATA_CLIENT = "mass_client"


def get_mass(hass: HomeAssistant) -> MusicAssistantClient | None:
    """Return this integration's Music Assistant client, if connected."""
    return hass.data.get(DOMAIN, {}).get(DATA_CLIENT)


async def async_open_client(
    hass: HomeAssistant, url: str, token: str
) -> MusicAssistantClient:
    """Open and connect our own Music Assistant client."""
    client = MusicAssistantClient(
        url.rstrip("/"), async_get_clientsession(hass), token
    )
    await client.connect()
    return client
