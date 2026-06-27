#
# Copyright (c) 2024-2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""FreeSWITCH/SIP → Pipecat WebSocket gateway.

A single FastAPI/uvicorn process that serves **all** agents over WebSockets, so an
inbound phone call (PSTN → SIP trunk → FreeSWITCH) reaches the same STT→LLM→TTS +
FlowManager pipeline the browser/WebRTC client uses. No conversation logic changes —
this is purely a new transport edge.

Flow:
    FreeSWITCH dialplan: DID → agent_id → `uuid_audio_stream start ws://host:8088/ws/<agent_id> mono 8k <meta>`
      → this gateway's `/ws/{agent_id}` endpoint
        → ModAudioStreamSerializer (raw L16 in ↔ base64 "streamAudio" JSON out)
        → FastAPIWebsocketTransport
        → session.run_voice_session(...)  (same pipeline as bot.py)

Per call we load that agent's `agents/<agent_id>/config.json` and `flow.py` fresh, then
run one `run_voice_session` with the agent's config passed as `env_overrides` so several
agents can be served concurrently from this one process without clobbering each other's
API keys (see session._build_services).

Run:
    uv run uvicorn sip_gateway:app --host 0.0.0.0 --port 8088
    # or:  uv run python sip_gateway.py
"""

import importlib.util
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from loguru import logger

# Base process env (Postgres creds for stats, any shared API keys). Per-agent keys come
# from each agent's config.json and are layered on top per call (env_overrides).
load_dotenv()

from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from session import run_voice_session
from transports.mod_audio_stream_serializer import ModAudioStreamSerializer

# Pipeline sample rate. Telephony audio is natively 8 kHz; we run the pipeline at 16 kHz
# (better for STT) and let the serializer resample to/from the 8 kHz FreeSWITCH stream.
PIPELINE_SAMPLE_RATE = int(os.getenv("SIP_PIPELINE_SAMPLE_RATE", "16000"))
# Sample rate negotiated with FreeSWITCH (must match the dialplan `<sampling-rate>`).
STREAM_SAMPLE_RATE = int(os.getenv("SIP_STREAM_SAMPLE_RATE", "8000"))
# VAD stop window — how long of silence ends a user turn. Matches bot.py.
VAD_STOP_SECS = float(os.getenv("SIP_VAD_STOP_SECS", "0.2"))

AGENTS_DIR = Path(__file__).parent / "agents"

app = FastAPI(title="Pipecat SIP gateway")


def _load_flow_module(path: Path):
    spec = importlib.util.spec_from_file_location("flow_module", str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_agent(agent_id: str) -> tuple[dict, str | None, object]:
    """Load an agent's config dict, display name, and freshly-imported flow module.

    Returns ``(config, name, flow_module)``. Raises FileNotFoundError if the agent
    directory or its flow.py is missing.
    """
    agent_dir = AGENTS_DIR / agent_id
    if not agent_dir.is_dir():
        raise FileNotFoundError(f"unknown agent_id '{agent_id}' ({agent_dir} not found)")

    config: dict = {}
    name: str | None = None
    config_path = agent_dir / "config.json"
    if config_path.exists():
        with open(config_path) as fh:
            data = json.load(fh) or {}
        config = data.get("config") or {}
        name = data.get("name")

    flow_path = agent_dir / "flow.py"
    if not flow_path.exists():
        raise FileNotFoundError(f"agent '{agent_id}' has no flow.py at {flow_path}")
    flow_module = _load_flow_module(flow_path)

    return config, name, flow_module


@app.get("/health")
async def health():
    """Liveness probe and a quick list of known agents."""
    agents = sorted(p.name for p in AGENTS_DIR.iterdir() if p.is_dir()) if AGENTS_DIR.exists() else []
    return {"status": "ok", "agents": agents}


@app.websocket("/ws/{agent_id}")
async def audio_stream(websocket: WebSocket, agent_id: str):
    """Handle one FreeSWITCH ``mod_audio_stream`` call for ``agent_id``."""
    await websocket.accept()
    logger.info(f"SIP call connected for agent {agent_id}")

    try:
        config, agent_name, flow_module = _load_agent(agent_id)
    except Exception as exc:
        logger.error(f"Rejecting SIP call: {exc}")
        await websocket.close(code=1011)
        return

    serializer = ModAudioStreamSerializer(
        params=ModAudioStreamSerializer.InputParams(stream_sample_rate=STREAM_SAMPLE_RATE)
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=PIPELINE_SAMPLE_RATE,
            audio_out_sample_rate=PIPELINE_SAMPLE_RATE,
            add_wav_header=False,
            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=VAD_STOP_SECS)),
            turn_analyzer=LocalSmartTurnAnalyzerV3(),
            serializer=serializer,
        ),
    )

    try:
        await run_voice_session(
            transport,
            initial_node_factory=flow_module.create_initial_node,
            agent_id=agent_id,
            agent_name=agent_name,
            handle_sigint=False,
            env_overrides=config,
        )
    except WebSocketDisconnect:
        logger.info(f"SIP call disconnected for agent {agent_id}")
    except Exception as exc:
        logger.exception(f"SIP session error for agent {agent_id}: {exc}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("SIP_GATEWAY_HOST", "0.0.0.0"),
        port=int(os.getenv("SIP_GATEWAY_PORT", "8088")),
    )
