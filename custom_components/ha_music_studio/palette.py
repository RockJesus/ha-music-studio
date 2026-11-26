"""Cover-art palette computed server-side so the card never samples pixels.

The rule mirrors what the card used to do on a 36x36 canvas: drop near-black,
near-white and grey pixels, bucket the rest by 18-degree hue bands, then pick the
highest-scoring bucket as primary and the strongest non-adjacent bucket as
secondary. Only the two values the card actually uses are returned
(``h2/s2/l2`` for the secondary mix and ``rgb1`` for the glow colour).
"""

from __future__ import annotations

import colorsys
import io
import logging
from collections import OrderedDict
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

_LOGGER = logging.getLogger(__name__)

_MAX_ENTRIES = 40
_SAMPLE = 36
_CACHE: OrderedDict[str, dict[str, Any] | None] = OrderedDict()


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _hsl_to_rgb(h: float, s: float, l: float) -> str:
    r, g, b = colorsys.hls_to_rgb((h % 360) / 360.0, l / 100.0, s / 100.0)
    return f"{round(r * 255)},{round(g * 255)},{round(b * 255)}"


def _analyze(image: Any) -> dict[str, Any] | None:
    """Return the compact palette dict for a PIL image."""
    img = image.convert("RGB").resize((_SAMPLE, _SAMPLE))
    pixels = list(img.getdata())
    buckets: dict[int, dict[str, float]] = {}
    for r, g, b in pixels:
        top = max(r, g, b)
        low = min(r, g, b)
        light = (top + low) / 510
        sat = 0.0 if top == 0 else (top - low) / top
        if light < 0.12 or light > 0.9 or sat < 0.14:
            continue
        delta = top - low
        if delta == 0:
            hue = 0.0
        elif top == r:
            hue = ((g - b) / delta) % 6
        elif top == g:
            hue = (b - r) / delta + 2
        else:
            hue = (r - g) / delta + 4
        hue = (hue * 60 + 360) % 360
        key = int(hue // 18)
        entry = buckets.setdefault(key, {"count": 0.0, "sat": 0.0, "light": 0.0})
        entry["count"] += 1
        entry["sat"] += sat
        entry["light"] += light

    if not buckets:
        return None

    scored = []
    for key, entry in buckets.items():
        count = entry["count"]
        scored.append(
            {
                "hue": key * 18 + 9,
                "score": count * (0.45 + entry["sat"] / count),
                "sat": entry["sat"] / count,
                "light": entry["light"] / count,
            }
        )
    scored.sort(key=lambda item: item["score"], reverse=True)
    primary = scored[0]
    secondary = next(
        (
            item
            for item in scored
            if min(abs(item["hue"] - primary["hue"]), 360 - abs(item["hue"] - primary["hue"]))
            > 28
        ),
        {
            "hue": (primary["hue"] + 44) % 360,
            "sat": primary["sat"],
            "light": primary["light"],
        },
    )

    def sat_of(value: float) -> int:
        return round(_clamp(value * 100, 42, 88))

    hue1 = round(primary["hue"])
    hue2 = round(secondary["hue"])
    s1 = sat_of(primary["sat"] + 0.08)
    s2 = sat_of(secondary["sat"] + 0.05)
    l1 = round(_clamp(round(primary["light"] * 100) + 12, 46, 66))
    l2 = round(_clamp(round(secondary["light"] * 100) + 8, 40, 60))

    return {
        "a1": f"hsl({hue1} {s1}% {l1}%)",
        "a2": f"hsl({hue2} {s2}% {l2}%)",
        "rgb1": _hsl_to_rgb(hue1, s1, l1),
        "rgb2": _hsl_to_rgb(hue2, s2, l2),
        "h1": hue1,
        "s1": s1,
        "l1": l1,
        "h2": hue2,
        "s2": s2,
        "l2": l2,
    }


async def _async_compute(hass: HomeAssistant, url: str) -> dict[str, Any] | None:
    try:
        from PIL import Image  # 可选依赖，缺失就交给卡片本地兜底
    except Exception:  # noqa: BLE001
        return None
    session = async_get_clientsession(hass, verify_ssl=False)
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.read()
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("palette fetch failed: %s", str(err)[:120])
        return None
    if not data or len(data) > 12 * 1024 * 1024:
        return None
    try:
        image = Image.open(io.BytesIO(data))
        return _analyze(image)
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("palette decode failed: %s", str(err)[:120])
        return None


async def async_palette(hass: HomeAssistant, url: str | None) -> dict[str, Any] | None:
    """Cached palette for one artwork URL; None means "card should fall back"."""
    if not url:
        return None
    if url in _CACHE:
        _CACHE.move_to_end(url)
        return _CACHE[url]
    result = await _async_compute(hass, url)
    _CACHE[url] = result
    while len(_CACHE) > _MAX_ENTRIES:
        _CACHE.popitem(last=False)
    return result
