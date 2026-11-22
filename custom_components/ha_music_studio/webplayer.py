"""Local browser Sendspin player bridge for the HA Music card."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from typing import Any
from urllib.parse import urlparse

import voluptuous as vol
from aiohttp import WSMsgType, web
from homeassistant.components import websocket_api
from homeassistant.components.http import HomeAssistantView
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import DOMAIN, WS_WEB_PLAYER_EXPOSE, WS_WEB_PLAYER_PAIR, WS_WEB_PLAYER_TICKET

_LOGGER = logging.getLogger(__name__)

TICKET_TTL = 300
PROXY_PATH = "/api/ha_music_studio/sendspin"
MA_DOMAIN = "music_assistant"
MA_PAIR_COMMAND = "sendspin/pair_web_player"


def _music_assistant_api_url(target: str) -> str:
    """Return the Music Assistant JSON-RPC API URL for a sendspin target."""
    parsed = urlparse(target)
    scheme = "https" if parsed.scheme == "wss" else "http"
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    return f"{scheme}://{netloc}/api"


async def _music_assistant_api_command(
    hass: HomeAssistant, command: str, args: dict[str, Any], message_id: str
) -> None:
    """Run one authenticated command on the local Music Assistant API."""
    target, token = _music_assistant_connection(hass)
    if not target or not token:
        raise RuntimeError("Music Assistant connection is unavailable")
    payload = {"message_id": message_id, "command": command, "args": args}
    session = async_get_clientsession(hass, verify_ssl=False)
    async with session.post(
        _music_assistant_api_url(target),
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        ssl=False,
        timeout=40,
    ) as response:
        body = await response.text()
        if response.status != 200:
            raise RuntimeError(
                f"Music Assistant {command} failed ({response.status}): {body[:200]}"
            )


async def _pair_web_player(hass: HomeAssistant, pairing_token: str) -> None:
    """Ask Music Assistant to pair the built-in web player.

    The pairing token names the Sendspin client, so the server can approve the
    browser player without an operator step.  The token only travels to the
    local Music Assistant API; it is never sent back to the browser.
    """
    await _music_assistant_api_command(
        hass, MA_PAIR_COMMAND, {"pairing_token": pairing_token}, "ha-music-pair"
    )


async def _expose_web_player_to_ha(hass: HomeAssistant, client_id: str) -> None:
    """Make the browser player visible to Home Assistant.

    Music Assistant hides its built-in web players by default, but this card
    plays through the Home Assistant media_player entity, so the player must be
    exposed there.  This also keeps it visible in Music Assistant's own list.
    """
    await _music_assistant_api_command(
        hass,
        "config/players/save",
        {
            "player_id": client_id,
            "values": {"expose_to_ha": True, "hide_in_ui": False},
        },
        "ha-music-expose",
    )



def _music_assistant_connection(hass: HomeAssistant) -> tuple[str, str]:
    """Return the Music Assistant proxy URL and its access token.

    Music Assistant exposes an authenticated ``/sendspin`` websocket on its
    main webserver port.  That endpoint auto-approves the browser web player,
    unlike the dedicated 8927 port which requires separate player setup.
    """
    for entry in hass.config_entries.async_entries(MA_DOMAIN):
        data = entry.data
        raw = str(data.get("url") or "")
        token = str(data.get("token") or "")
        if not raw or not token:
            continue
        parsed = urlparse(raw)
        if not parsed.hostname:
            continue
        scheme = "wss" if parsed.scheme == "https" else "ws"
        netloc = parsed.hostname
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return f"{scheme}://{netloc}/sendspin", token
    return "", ""


class SendspinProxyView(HomeAssistantView):
    """Proxy the browser Sendspin socket to Music Assistant.

    The browser must not receive the Music Assistant token.  This view
    authenticates the upstream Music Assistant proxy itself and then relays
    raw Sendspin frames in both directions.
    """

    url = PROXY_PATH
    name = "api:ha_music_studio:sendspin"
    requires_auth = False

    def __init__(self, hass: HomeAssistant, target: str, token: str) -> None:
        self.hass = hass
        self.target = target
        self.token = token

    async def get(self, request: web.Request) -> web.StreamResponse:
        ticket = str(request.query.get("ticket") or "")
        tickets: dict[str, tuple[float, str]] = self.hass.data[DOMAIN].setdefault(
            "web_player_tickets", {}
        )
        now = time.monotonic()
        for key, (created, _client_id) in list(tickets.items()):
            if now - created > TICKET_TTL:
                tickets.pop(key, None)
        record = tickets.pop(ticket, None)
        if record is None or now - record[0] > TICKET_TTL:
            return web.Response(status=403, text="invalid or expired ticket")
        client_id = record[1]

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
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Sendspin upstream unavailable: %s", err)
            await client.close(code=1011, message=b"sendspin upstream unavailable")
            return client

        if self.token:
            try:
                await upstream.send_str(
                    json.dumps(
                        {
                            "type": "auth",
                            "token": self.token,
                            "client_id": client_id,
                        }
                    )
                )
                auth = await asyncio.wait_for(upstream.receive(), timeout=10)
                payload = json.loads(auth.data) if auth.type == WSMsgType.TEXT else {}
                if payload.get("type") != "auth_ok":
                    _LOGGER.warning("Sendspin proxy auth rejected: %s", payload)
                    await upstream.close()
                    await client.close(code=1011, message=b"sendspin auth failed")
                    return client
                # 把 auth_ok 转发给浏览器端 Sendspin 客户端；代理吞掉这条
                # 会令客户端一直停在握手阶段，MA 注册后很快又标记 unavailable。
                await client.send_str(auth.data)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("Sendspin proxy auth failed: %s", err)
                await upstream.close()
                await client.close(code=1011, message=b"sendspin auth failed")
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
    target, token = _music_assistant_connection(hass)
    cache["web_player_registered"] = True
    cache["web_player_target"] = target
    cache["web_player_token"] = token

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_WEB_PLAYER_TICKET,
            vol.Optional("client_id"): str,
        }
    )
    @callback
    def websocket_web_player_ticket(hass: HomeAssistant, connection, msg) -> None:
        if not target or not token:
            connection.send_error(
                msg["id"],
                "web_player_unavailable",
                "Music Assistant URL or token is unavailable",
            )
            return
        tickets: dict[str, tuple[float, str]] = hass.data[DOMAIN].setdefault(
            "web_player_tickets", {}
        )
        now = time.monotonic()
        for key, (created, _client_id) in list(tickets.items()):
            if now - created > TICKET_TTL:
                tickets.pop(key, None)
        ticket = secrets.token_urlsafe(32)
        tickets[ticket] = (now, str(msg.get("client_id") or ""))
        connection.send_result(
            msg["id"],
            {
                "ticket": ticket,
                "url": PROXY_PATH,
                "ttl": TICKET_TTL,
                "target": target,
            },
        )

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_WEB_PLAYER_PAIR,
            vol.Required("pairing_token"): str,
            vol.Optional("client_id"): str,
        }
    )
    @websocket_api.async_response
    async def websocket_web_player_pair(hass: HomeAssistant, connection, msg) -> None:
        pairing_token = str(msg.get("pairing_token") or "").strip()
        client_id = str(msg.get("client_id") or "").strip()
        if not pairing_token or len(pairing_token) > 2048:
            connection.send_error(
                msg["id"], "invalid_pairing_token", "Invalid pairing token"
            )
            return
        try:
            await _pair_web_player(hass, pairing_token)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Sendspin web player pairing failed: %s", err)
            connection.send_error(msg["id"], "pairing_failed", str(err)[:200])
            return
        # 暴露给 HA 会触发 MA 重注册播放器，不要在配对后立刻执行；
        # 交给 web_player_expose 在会话稳定后单独调用。
        connection.send_result(msg["id"], {"paired": True})

    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_WEB_PLAYER_EXPOSE,
            vol.Required("client_id"): str,
        }
    )
    @websocket_api.async_response
    async def websocket_web_player_expose(hass: HomeAssistant, connection, msg) -> None:
        """Expose a stable web player session to Home Assistant.

        Pairing and exposing are deliberately separate: the browser calls this
        only after the Sendspin session has stayed connected, so Music
        Assistant's config save cannot tear down the freshly paired socket.
        """
        client_id = str(msg.get("client_id") or "").strip()
        if not client_id:
            connection.send_error(msg["id"], "invalid_client_id", "Invalid client id")
            return
        try:
            await _expose_web_player_to_ha(hass, client_id)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Sendspin web player expose failed: %s", err)
            connection.send_error(msg["id"], "expose_failed", str(err)[:200])
            return
        connection.send_result(msg["id"], {"exposed": True})

    websocket_api.async_register_command(hass, websocket_web_player_ticket)
    websocket_api.async_register_command(hass, websocket_web_player_pair)
    websocket_api.async_register_command(hass, websocket_web_player_expose)
    hass.http.register_view(SendspinProxyView(hass, target, token))
