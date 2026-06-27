"""Simulate FreeSWITCH mod_audio_stream against the Pipecat SIP gateway.

Connects to ws://host/ws/<agent_id>, sends the leading metadata text frame, then
streams 20ms frames of raw L16 PCM (silence by default) at the stream sample rate,
while listening for `streamAudio` JSON messages (the bot's greeting / replies).

Usage:
    uv run python scripts/ws_test_client.py [ws://127.0.0.1:8088/ws/<agent_id>]

A non-zero `audio_bytes` result means the full STT→LLM→TTS pipeline works end to end
without any telephony — the bot should greet you on connect.
"""
import asyncio
import base64
import json
import sys
import time

import websockets

URL = sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:8088/ws/c703612b"
STREAM_RATE = 8000
FRAME_MS = 20
SAMPLES = STREAM_RATE * FRAME_MS // 1000  # 160
SILENCE = (b"\x00\x00") * SAMPLES          # 20ms of 8k L16 silence
LISTEN_SECS = 12


async def main():
    print(f"connecting {URL}")
    async with websockets.connect(URL, max_size=None) as ws:
        # mod_audio_stream sends the dialplan metadata string first (text frame).
        await ws.send("agent_test_metadata")

        recv_audio_msgs = 0
        recv_audio_bytes = 0
        other_msgs = 0

        async def sender():
            end = time.time() + LISTEN_SECS
            while time.time() < end:
                await ws.send(SILENCE)
                await asyncio.sleep(FRAME_MS / 1000)

        async def receiver():
            nonlocal recv_audio_msgs, recv_audio_bytes, other_msgs
            end = time.time() + LISTEN_SECS
            while time.time() < end:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    break
                if isinstance(msg, (bytes, bytearray)):
                    other_msgs += 1
                    continue
                try:
                    data = json.loads(msg)
                except Exception:
                    other_msgs += 1
                    continue
                if data.get("type") == "streamAudio":
                    recv_audio_msgs += 1
                    recv_audio_bytes += len(base64.b64decode(data["data"]["audioData"]))
                    if recv_audio_msgs == 1:
                        print(f"  first streamAudio: sr={data['data']['sampleRate']} "
                              f"type={data['data']['audioDataType']}")
                else:
                    other_msgs += 1

        await asyncio.gather(sender(), receiver())

        print(f"RESULT: streamAudio_msgs={recv_audio_msgs} "
              f"audio_bytes={recv_audio_bytes} other_msgs={other_msgs}")
        if recv_audio_bytes > 0:
            secs = recv_audio_bytes / 2 / STREAM_RATE
            print(f"  ~{secs:.2f}s of bot audio received -> end-to-end pipeline WORKS")
        else:
            print("  no bot audio received (check gateway log / agent API keys)")


asyncio.run(main())
