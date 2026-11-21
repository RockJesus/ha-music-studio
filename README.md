# HA Music Studio

Home Assistant custom integration that taps Music Assistant's real-time PCM waveform and emits a compact 48-band FFT stream for the `ha-music` card.

## What it does

- Connects to the local Music Assistant MilkDrop visualizer relay.
- Reuses the Music Assistant token already stored by Home Assistant; no token is written to this repository.
- Collapses the 200–300 fps PCM feed into **30 fps, 48 log-spaced bands**.
- Pushes the bands over a Home Assistant WebSocket command: `ha_music_studio/subscribe`.
- Fetches the current track's lyrics through Music Assistant's native `metadata/get_track_lyrics`, parses LRC, and pushes synchronized lines plus the active line index over `ha_music_studio/subscribe_lyrics`.
- Owns the Music Assistant queue: bounded queue windows over `ha_music_studio/subscribe_queue`, and queue control (play item, move, remove, clear, shuffle, repeat, transport, seek) over `ha_music_studio/queue_command`. Cards no longer need Supervisor ingress, `mass_queue`, or a local MA websocket.

## Compatibility

| HA Music Studio | ha-music card | Protocol |
|---|---|---|
| `v0.4.0` and newer | `v2.14.0` and newer | `1` |

Upgrade the integration first, then the card. Every WebSocket event carries a `protocol` field; clients must reject a different version instead of rendering stale data.

## Requirements

- Home Assistant with the Music Assistant integration configured.
- Music Assistant 2.10+ with the MilkDrop visualizer provider available.
- A player that is actually playing; when no player is active the stream stays idle.

## Install

Install through Marketplace as an `integration`, restart Home Assistant, then add **HA Music Studio** in Settings → Devices & Services.

The `ha-music` card subscribes automatically when this integration is loaded. Without it, the card keeps playback controls but shows no live spectrum or backend lyrics.
