#
# Copyright (c) 2024-2026, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""FrameSerializer for FreeSWITCH's ``mod_audio_stream`` WebSocket protocol.

FreeSWITCH (with amigniter's ``mod_audio_stream``) forks a call's audio to a
WebSocket and plays audio it receives back to the caller. The wire protocol is
deliberately tiny:

- **Inbound** (FreeSWITCH → us): raw **L16** PCM (16-bit signed, little-endian),
  mono, at the streaming sample rate chosen in the dialplan (``8k`` or ``16k``).
  Frames arrive as binary WebSocket messages. The optional ``metadata`` string is
  delivered once as a leading *text* message — we ignore it (the agent id comes
  from the WS URL path instead).

- **Outbound** (us → FreeSWITCH): a JSON *text* message::

      {"type": "streamAudio",
       "data": {"audioDataType": "raw", "sampleRate": 8000, "audioData": "<base64 L16>"}}

This serializer plugs into ``FastAPIWebsocketTransport`` exactly like Pipecat's
built-in ``TwilioFrameSerializer`` does — the difference is mod_audio_stream uses
raw L16 in/JSON-base64-L16 out instead of Twilio's base64-μ-law-in-JSON both ways.

Barge-in note: ``mod_audio_stream`` does not document a WebSocket message to flush
audio already handed to FreeSWITCH for playback, so true server-initiated barge-in
is best-effort. We stop emitting audio on interruption (the pipeline stops calling
``serialize`` with new ``AudioRawFrame``s); any already-buffered audio plays out.
Keep TTS chunks small and VAD ``stop_secs`` low to minimise the tail.
"""

import base64
import json

from loguru import logger
from pipecat.audio.utils import create_stream_resampler
from pipecat.frames.frames import (
    AudioRawFrame,
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputTransportMessageFrame,
    OutputTransportMessageUrgentFrame,
    StartFrame,
)
from pipecat.serializers.base_serializer import FrameSerializer


class ModAudioStreamSerializer(FrameSerializer):
    """Serializer for the FreeSWITCH ``mod_audio_stream`` WebSocket protocol."""

    class InputParams(FrameSerializer.InputParams):
        """Configuration parameters for ModAudioStreamSerializer.

        Parameters:
            stream_sample_rate: Sample rate of the audio on the FreeSWITCH side, i.e.
                the ``<sampling-rate>`` given to ``uuid_audio_stream start`` (8000 or
                16000). Defaults to 8000 (FreeSWITCH default / telephony).
            sample_rate: Optional override for the pipeline input sample rate. When
                None it is taken from the StartFrame.
        """

        stream_sample_rate: int = 8000
        sample_rate: int | None = None

    def __init__(self, params: InputParams | None = None):
        """Initialize the serializer.

        Args:
            params: Configuration parameters.
        """
        params = params or ModAudioStreamSerializer.InputParams()
        super().__init__(params)
        self._params: ModAudioStreamSerializer.InputParams = params

        self._stream_sample_rate = self._params.stream_sample_rate
        self._sample_rate = 0  # Pipeline input rate, set in setup()

        self._input_resampler = create_stream_resampler()
        self._output_resampler = create_stream_resampler()

    async def setup(self, frame: StartFrame):
        """Capture the pipeline input sample rate from the StartFrame."""
        self._sample_rate = self._params.sample_rate or frame.audio_in_sample_rate

    async def serialize(self, frame: Frame) -> str | bytes | None:
        """Convert a Pipecat frame to a mod_audio_stream WebSocket message.

        ``AudioRawFrame`` → a ``streamAudio`` JSON text message (base64 L16 at the
        FreeSWITCH stream rate). Other frames are ignored.
        """
        if isinstance(frame, InterruptionFrame):
            # mod_audio_stream exposes no documented "clear playback" WS message;
            # interruption is handled implicitly by the pipeline ceasing to emit
            # audio frames. Nothing to send here.
            return None
        elif isinstance(frame, AudioRawFrame):
            data = frame.audio

            # Resample pipeline PCM → FreeSWITCH stream rate (still L16 PCM).
            resampled = await self._output_resampler.resample(
                data, frame.sample_rate, self._stream_sample_rate
            )
            if not resampled:
                return None

            payload = base64.b64encode(resampled).decode("utf-8")
            message = {
                "type": "streamAudio",
                "data": {
                    "audioDataType": "raw",
                    "sampleRate": self._stream_sample_rate,
                    "audioData": payload,
                },
            }
            return json.dumps(message)
        elif isinstance(frame, (OutputTransportMessageFrame, OutputTransportMessageUrgentFrame)):
            if self.should_ignore_frame(frame):
                return None
            return json.dumps(frame.message)

        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        """Convert an inbound mod_audio_stream WebSocket message to a Pipecat frame.

        Binary messages are raw L16 PCM audio. The single leading text message is
        the dialplan ``metadata`` string, which we ignore.
        """
        if isinstance(data, str):
            # Leading metadata (or any unexpected text frame) — not audio.
            logger.debug(f"mod_audio_stream metadata/text frame ignored: {data[:120]!r}")
            return None

        # Binary frame: raw L16 PCM at the FreeSWITCH stream rate.
        resampled = await self._input_resampler.resample(
            data, self._stream_sample_rate, self._sample_rate
        )
        if not resampled:
            return None

        return InputAudioRawFrame(
            audio=resampled, num_channels=1, sample_rate=self._sample_rate
        )
