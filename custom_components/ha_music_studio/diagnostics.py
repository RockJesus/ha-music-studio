"""Temporary diagnostics for the spectrum bridge."""

from __future__ import annotations

import traceback

from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse

from .const import DOMAIN
from .token import ma_entry


async def async_register_diagnostics(hass: HomeAssistant) -> None:
    """Register the diagnose service once."""

    async def handle(call: ServiceCall) -> dict:
        entry = ma_entry(hass)
        if entry is None:
            return {"entry": None}
        mass = getattr(getattr(entry, "runtime_data", None), "mass", None)
        conn = getattr(mass, "connection", None)
        out: dict = {
            "entry_keys": sorted(entry.data),
            "url": entry.data.get("url"),
            "entry_token_type": type(entry.data.get("token")).__name__,
            "entry_token_prefix": str(entry.data.get("token"))[:12],
            "mass_type": type(mass).__name__ if mass else None,
            "conn_token_type": type(getattr(conn, "auth_token", None)).__name__,
            "conn_token_prefix": str(getattr(conn, "auth_token", None))[:12],
        }
        try:
            out["players"] = [
                {
                    "id": p.player_id,
                    "name": p.display_name,
                    "state": str(getattr(p, "state", None)),
                    "value": str(getattr(getattr(p, "state", None), "value", None)),
                }
                for p in mass.players.players
            ]
        except Exception as err:  # noqa: BLE001
            out["players_error"] = f"{err.__class__.__name__}: {err}"
        try:
            token = await mass.auth.create_token("HA Music Studio Diag")
            out["created_type"] = type(token).__name__
            out["created_len"] = len(token) if hasattr(token, "__len__") else None
            out["created_prefix"] = str(token)[:12]
        except Exception as err:  # noqa: BLE001
            out["create_error"] = f"{err.__class__.__name__}: {err}"
            out["create_tb"] = traceback.format_exc()[-800:]
        return out

    if not hass.services.has_service(DOMAIN, "diagnose"):
        hass.services.async_register(
            DOMAIN, "diagnose", handle, supports_response=SupportsResponse.ONLY
        )
