"""Config flow for HA Music Studio."""

from __future__ import annotations

import logging

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_TOKEN, CONF_URL
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import DOMAIN
from .mass_client import async_open_client

_LOGGER = logging.getLogger(__name__)

DEFAULT_URL = "http://homeassistant.local:8095"


class HaMusicStudioConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Ask for the Music Assistant URL and a token created by the user."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        """Create the single service entry."""
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        errors: dict[str, str] = {}
        if user_input is not None:
            url = str(user_input[CONF_URL]).strip()
            token = str(user_input[CONF_TOKEN]).strip()
            try:
                client = await async_open_client(self.hass, url, token)
            except Exception:  # noqa: BLE001 - any failure means "cannot connect"
                _LOGGER.warning("Could not connect to Music Assistant at %s", url)
                errors["base"] = "cannot_connect"
            else:
                await client.disconnect()
                return self.async_create_entry(
                    title="HA Music Studio",
                    data={CONF_URL: url, CONF_TOKEN: token},
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_URL, default=DEFAULT_URL): str,
                    vol.Required(CONF_TOKEN): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    ),
                }
            ),
            errors=errors,
        )
