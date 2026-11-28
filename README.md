# HA Music Studio

Home Assistant custom integration that taps Music Assistant's real-time PCM waveform and emits a compact 48-band FFT stream for the `ha-music` card.

## What it does

- Connects to the local Music Assistant MilkDrop visualizer relay.
- Reuses the Music Assistant token already stored by Home Assistant; no token is written to this repository.
- Collapses the 200–300 fps PCM feed into **30 fps, 48 log-spaced bands**.
- Pushes the bands over a Home Assistant WebSocket command: `ha_music_studio/subscribe`.
- Fetches the current track's lyrics through Music Assistant's native `metadata/get_track_lyrics`, parses LRC, and pushes synchronized lines plus the active line index over `ha_music_studio/subscribe_lyrics`.
- Exposes a same-origin Sendspin WebSocket relay for the card's **This device** browser player, with one-time tickets; the browser then plays audio locally through Music Assistant.
- Owns the Music Assistant queue: bounded queue windows over `ha_music_studio/subscribe_queue`, and queue control (play item, move, remove, clear, shuffle, repeat, transport, seek) over `ha_music_studio/queue_command`. Cards no longer need Supervisor ingress, `mass_queue`, or a local MA websocket.

## Compatibility

| HA Music Studio | ha-music card | Protocol |
|---|---|---|
| `v0.7.0` and newer | `v2.17.0` and newer | `1` |

Upgrade the integration first, then the card. Every WebSocket event carries a `protocol` field; clients must reject a different version instead of rendering stale data.

## Requirements

- Home Assistant with the Music Assistant integration configured.
- Music Assistant 2.10+ with the MilkDrop visualizer provider available.
- A player that is actually playing; when no player is active the stream stays idle.

## Install

Install through Marketplace as an `integration`, restart Home Assistant, then add **HA Music Studio** in Settings → Devices & Services.

The `ha-music` card is bundled with this integration (`custom_components/ha_music_studio/www/ha-music.js`) and is registered as a frontend module on startup, so installing the integration is all you need — there is no separate card install step.

### The card is part of this integration

The card talks only to this integration's WebSocket API (queue, progress, palette, spectrum) and is useless without it, so it ships here instead of as a separate HACS plugin. The bundle is served from `/ha_music_studio_files/ha-music.js` with the integration version as cache buster.

If you previously installed the card as a HACS plugin or dropped it into `www/`, remove that dashboard resource: two copies would define the same custom elements twice. The integration detects the old resource, logs a warning, and keeps the bundled copy off until it is gone.

If an older release left `custom_components/ha_music_analysis` behind, delete that folder yourself (or through Marketplace); this integration only talks to Music Assistant and never removes other integrations' folders.

Upgrade the integration first, then hard-refresh the dashboard once so the browser picks up the new bundle.
