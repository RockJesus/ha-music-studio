"""Config flow for HA Music Studio."""

from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries

from .const import DOMAIN


class HaMusicStudioConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for HA Music Studio."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        """Create the single service entry."""
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if user_input is not None:
            return self.async_create_entry(title="HA Music Studio", data={})
        return self.async_show_form(step_id="user", data_schema=vol.Schema({}))
