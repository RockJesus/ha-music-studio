"""Serve and register the bundled HA Music card.

The card is a 1:1 companion of this integration: it talks to this integration's
websocket API for queue state, progress frames, palette and spectrum bands, and
it renders nothing useful without it. HACS policy therefore wants the card
inside the integration repository instead of a separate plugin entry, so the
built bundle ships in ``www/`` and is registered as a frontend module here.
"""

from __future__ import annotations

import logging
from pathlib import Path

from homeassistant.components.frontend import add_extra_js_url
from homeassistant.components.http import StaticPathConfig
from homeassistant.core import HomeAssistant
from homeassistant.loader import async_get_integration

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

URL_BASE = f"/{DOMAIN}_files"
CARD_FILE = "ha-music.js"
FRONTEND_KEY = "frontend_url"


def _resource_urls(hass: HomeAssistant) -> list[str]:
    """Return the dashboard resource URLs, whichever resource mode is in use."""
    lovelace = hass.data.get("lovelace")
    collection = getattr(lovelace, "resources", None)
    if collection is None:
        return []
    try:
        items = collection.async_items()
    except Exception:  # noqa: BLE001 - resource collections differ by mode
        return []
    return [str(item.get("url", "")) for item in items if isinstance(item, dict)]


def _legacy_card_resource(hass: HomeAssistant) -> str | None:
    """Find a card copy registered outside this integration (HACS plugin or /local)."""
    for url in _resource_urls(hass):
        if CARD_FILE in url and URL_BASE not in url:
            return url
    return None


async def async_register_card(hass: HomeAssistant) -> None:
    """Expose ``www/ha-music.js`` and load it as a frontend module, once."""
    store = hass.data.setdefault(DOMAIN, {})
    if store.get(FRONTEND_KEY):
        return

    www = Path(__file__).with_name("www")
    if not www.is_dir():
        _LOGGER.warning("HA Music card bundle is missing at %s", www)
        return

    await hass.http.async_register_static_paths(
        [StaticPathConfig(URL_BASE, str(www), False)]
    )

    integration = await async_get_integration(hass, DOMAIN)
    version = integration.version or "0"
    url = f"{URL_BASE}/{CARD_FILE}?v={version}"

    legacy = _legacy_card_resource(hass)
    if legacy:
        # Loading both copies would define the same custom elements twice and
        # throw, so the bundled one stays out until the old resource is removed.
        _LOGGER.warning(
            "HA Music card is already loaded from %s; keeping the bundled copy off "
            "to avoid defining the custom elements twice. Remove that dashboard "
            "resource to switch to the bundled card",
            legacy,
        )
        return

    add_extra_js_url(hass, url)
    store[FRONTEND_KEY] = url
    _LOGGER.debug("Registered bundled HA Music card at %s", url)
