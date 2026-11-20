# HA Music Studio

Home Assistant custom integration that taps Music Assistant's real-time PCM waveform and emits a compact 48-band FFT stream for the `ha-music` card.

## What it does

- Connects to the local Music Assistant MilkDrop visualizer relay.
- Reuses the Music Assistant token already stored by Home Assistant; no token is written to this repository.
- Collapses the 200–300 fps PCM feed into **30 fps, 48 log-spaced bands**.
- Pushes the bands over a Home Assistant WebSocket command: `ha_music_studio/subscribe`.

## Requirements

- Home Assistant with the Music Assistant integration configured.
- Music Assistant 2.10+ with the MilkDrop visualizer provider available.
- A player that is actually playing; when no player is active the stream stays idle.

## Install

Install through Marketplace as an `integration`, restart Home Assistant, then add **HA Music Studio** in Settings → Devices & Services.

The frontend card subscribes automatically when this integration is loaded. If it is missing, the card falls back to its built-in visualizer.
