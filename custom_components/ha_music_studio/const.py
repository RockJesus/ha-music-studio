"""Constants for HA Music Studio."""

DOMAIN = "ha_music_studio"

WS_SUBSCRIBE = "ha_music_studio/subscribe"
WS_LYRICS_SUBSCRIBE = "ha_music_studio/subscribe_lyrics"
WS_QUEUE_SUBSCRIBE = "ha_music_studio/subscribe_queue"
WS_QUEUE_COMMAND = "ha_music_studio/queue_command"
WS_PLAYER_GROUP = "ha_music_studio/player_group"
WS_SOUND_EFFECT = "ha_music_studio/sound_effect"
WS_WEB_PLAYER_TICKET = "ha_music_studio/web_player_ticket"
WS_WEB_PLAYER_PAIR = "ha_music_studio/web_player_pair"
WS_WEB_PLAYER_EXPOSE = "ha_music_studio/web_player_expose"
WS_WEB_PLAYER_REGISTER = "ha_music_studio/web_player_register"

# Target output rate. The MA relay can emit 200-300 waveform frames per second;
# we deliberately collapse them to a small, steady stream for the frontend.
FRAME_INTERVAL = 1 / 30
BAND_COUNT = 48
SAMPLE_RATE = 44100
FFT_SIZE = 2048
MIN_FREQ = 60.0
MAX_FREQ = 12000.0
