"""Local browser Sendspin player bridge for the HA Music card."""

from __future__ import annotations

import asyncio
import secrets
import time
from typing import Any
from urllib.parse import urlparse

import voluptuous as vol
from aiohttp import WSMsgType, web
from homeassistant.components import websocket_api
from homeassistant.components.http import HomeAssistantView
from homeassistant.const import CONF_URL
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import DOMAIN, WS_WEB_PLAYER_TICKET

TICKET_TTL = 300
SENDSPIN_PORT = 8927
PROXY_PATH = "/api/ha_music_studio/sendspin"


def _sendspin_server(entry: Any) -> str:
    """Return the Sendspin endpoint of the Music Assistant server."""
    raw = str(entry.data.get(CONF_URL) or "") if entry else ""
    parsed = urlparse(raw)
    host = parsed.hostname or "d5369777-music-assistant"
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{host}:{SENDSPIN_PORT}/sendspin"


class SendspinProxyView(HomeAssistantView):
    """Proxy the browser Sendspin socket to Music Assistant.

    Music Assistant only accepts custom Sendspin clients on its dedicated
    port.  HA is normally the only public HTTPS entry point, so the card
    connects to this same-origin endpoint and we bridge the raw frames.
    """

    url = PROXY_PATH
    name = "api:ha_music_studio:sendspin"
    requires_auth = False

    def __init__(self, hass: HomeAssistant, target: str) -> None:
        self.hass = hass
        self.target = target

    async def get(self, request: web.Request) -> web.StreamResponse:
        ticket = str(request.query.get("ticket") or "")
        tickets: dict[str, float] = self.hass.data[DOMAIN].setdefault(
            "web_player_tickets", {}
        )
        now = time.monotonic()
        for key, created in list(tickets.items()):
            if now - created > TICKET_TTL:
                tickets.pop(key, None)
        created = tickets.pop(ticket, None)
        if created is None or now - created > TICKET_TTL:
            return web.Response(status=403, text="invalid or expired ticket")

        client = web.WebSocketResponse(
            heartbeat=30, autoping=True, compress=False, max_msg_size=0
        )
        await client.prepare(request)

        session = async_get_clientsession(self.hass, verify_ssl=False)
        try:
            upstream = await session.ws_connect(
                self.target,
                heartbeat=30,
                autoping=True,
                compress=False,
                max_msg_size=0,
                ssl=False,
                timeout=15,
            )
        except Exception:  # noqa: BLE001
            await client.close(code=1011, message=b"sendspin upstream unavailable")
            return client

        async def client_to_upstream() -> None:
            async for message in client:
                if message.type == WSMsgType.TEXT:
                    await upstream.send_str(message.data)
                elif message.type == WSMsgType.BINARY:
                    await upstream.send_bytes(message.data)
                elif message.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
            if not upstream.closed:
                await upstream.close()

        async def upstream_to_client() -> None:
            async for message in upstream:
                if message.type == WSMsgType.TEXT:
                    await client.send_str(message.data)
                elif message.type == WSMsgType.BINARY:
                    await client.send_bytes(message.data)
                elif message.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
            if not client.closed:
                await client.close()

        tasks = [
            asyncio.create_task(client_to_upstream()),
            asyncio.create_task(upstream_to_client()),
        ]
        try:
            _done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        finally:
            for stream in (client, upstream):
                if not stream.closed:
                    await stream.close()
        return client


async def async_register_web_player_api(hass: HomeAssistant, entry: Any) -> None:
    """Register the ticket command and the same-origin Sendspin relay."""
    cache = hass.data.setdefault(DOMAIN, {})
    if cache.get("web_player_registered"):
        return
    cache["web_player_registered"] = True
    cache["web_player_target"] = _sendspin_server(entry)

    @websocket_api.websocket_command({vol.Required("type"): WS_WEB_PLAYER_TICKET})
    @callback
    def websocket_web_player_ticket(hass: HomeAssistant, connection, msg) -> None:
        tickets: dict[str, float] = hass.data[DOMAIN].setdefault(
            "web_player_tickets", {}
        )
        now = time.monotonic()
        for key, created in list(tickets.items()):
            if now - created > TICKET_TTL:
                tickets.pop(key, None)
        ticket = secrets.token_urlsafe(32)
        tickets[ticket] = now
        connection.send_result(
            msg["id"],
            {
                "ticket": ticket,
                "url": PROXY_PATH,
                "ttl": TICKET_TTL,
                "target": hass.data[DOMAIN].get("web_player_target", ""),
            },
        )

    websocket_api.async_register_command(hass, websocket_web_player_ticket)
    hass.http.register_view(SendspinProxyView(hass, cache["web_player_target"]))
