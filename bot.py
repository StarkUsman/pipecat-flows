#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Pipecat pipeline for the lead-qualification voice bot.

This module owns the runtime side of the bot:

- Loading models / service imports
- Building the STT, LLM, TTS services and the context aggregators
- Assembling and running the Pipeline + PipelineTask
- Creating the FlowManager and kicking off the conversation

The conversation logic (nodes + handlers) lives in ``flow.py``. This file
imports the flow's entry node from there; the dependency only points one way
(pipeline -> flow), so there is no circular import.

Run the bot using:

    uv run bot.py

Required AI services:
- Deepgram (Speech-to-Text)
- OpenAI (LLM)
- Cartesia (Text-to-Speech)
- HeyGen (Avatar Video)
"""

import asyncio
import importlib
import importlib.util
import json
import os
import uuid
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from loguru import logger

import db

print("🚀 Starting Pipecat bot...")
print("⏳ Loading models and imports (20 seconds, first run only)\n")

# Audio processing imports
logger.info("Loading Local Smart Turn Analyzer V3...")
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3

logger.info("✅ Local Smart Turn Analyzer V3 loaded")

logger.info("Loading Silero VAD model...")
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams

logger.info("✅ Silero VAD model loaded")

# Pipeline and core imports
logger.info("Loading pipeline components...")
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.daily.transport import DailyParams

# The transport-agnostic conversation runner (pipeline assembly, FlowManager, stats)
# is shared with the SIP gateway — see session.py.
from session import run_voice_session

# Conversation flow (nodes + handlers) lives in its own module.
# Loaded dynamically so FLOW_PATH env var can point to any agent-specific file.
# from flow import create_greeting_node

logger.info("✅ All components loaded successfully!")

load_dotenv(override=True)

_FLOW_PATH = os.environ.get(
    "FLOW_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow.py"),
)
# Sibling of flow.py — re-read on each connection so config hot-updates take
# effect on the NEXT call, mirroring the flow.py hot-reload (see run_bot).
_CONFIG_PATH = os.path.join(os.path.dirname(_FLOW_PATH), "config.json")
_applied_config_keys: set[str] = set()
_api_server_started = False


def _read_config_keys() -> dict:
    """Return the 'config' dict from config.json (empty on any failure)."""
    try:
        with open(_CONFIG_PATH) as fh:
            return (json.load(fh) or {}).get("config") or {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _reload_agent_config() -> None:
    """Re-read config.json into os.environ (full replace) for the next call.

    Applies every current config key and removes keys that were applied on the
    previous reload but are now absent. Manager-owned env (FLOW_PATH,
    FLOW_API_PORT, AGENT_ID, AGENT_NAME) and unrelated base-process env are left
    untouched — only keys sourced from config.json are managed here.
    """
    global _applied_config_keys
    cfg = _read_config_keys()
    for stale in _applied_config_keys - set(cfg):
        os.environ.pop(stale, None)
    for key, value in cfg.items():
        os.environ[key] = str(value)
    _applied_config_keys = set(cfg)


# Seed from the spawn-time config so the first reload's removal diff is correct.
_applied_config_keys = set(_read_config_keys())


def _load_flow_module(path: str):
    spec = importlib.util.spec_from_file_location("flow_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


flow_module = _load_flow_module(_FLOW_PATH)
_api_server_thread_started = False


def _cors_headers(request: web.Request) -> dict[str, str]:
    request_headers = request.headers.get("Access-Control-Request-Headers")
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": request_headers or "*",
        "Access-Control-Max-Age": "86400",
    }


@web.middleware
async def _cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=_cors_headers(request))

    response = await handler(request)
    response.headers.update(_cors_headers(request))
    return response


async def _start_api_server():
    global _api_server_started
    if _api_server_started:
        return
    _api_server_started = True

    _TYPING_NAMES = {"Any", "Optional", "Union", "List", "Dict", "Tuple", "Set", "Type", "Callable"}
    _TYPING_HEADER = "from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union\n"

    async def deploy_new_flow(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            code = body.get("code")
            if not code:
                return web.json_response({"error": "Missing 'code' field in request body"}, status=400)

            # Inject typing imports if the code uses typing names without importing them
            needs_typing = any(name in code for name in _TYPING_NAMES)
            has_typing_import = "from typing import" in code or "import typing" in code
            if needs_typing and not has_typing_import:
                code = _TYPING_HEADER + code

            try:
                compile(code, "flow.py", "exec")
            except SyntaxError as exc:
                return web.json_response({"error": f"Syntax error in submitted code: {exc}"}, status=400)

            with open(_FLOW_PATH, "w") as fh:
                fh.write(code)

            logger.info("flow.py updated via /deployNewFlow")
            return web.json_response({"status": "ok", "message": "flow.py updated — takes effect on next client connection"})

        except Exception as exc:
            logger.error(f"/deployNewFlow error: {exc}")
            return web.json_response({"error": str(exc)}, status=500)

    app = web.Application(middlewares=[_cors_middleware])
    app.router.add_post("/deployNewFlow", deploy_new_flow)
    app.router.add_options("/deployNewFlow", lambda request: web.Response(status=204))

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("FLOW_API_PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Flow API server listening on :{port} — POST /deployNewFlow")


async def _serve_api_server_forever():
    await _start_api_server()
    await asyncio.Event().wait()


def _ensure_api_server_thread():
    global _api_server_thread_started
    if _api_server_thread_started:
        return

    import threading

    _api_server_thread_started = True
    thread = threading.Thread(
        target=lambda: asyncio.run(_serve_api_server_forever()),
        daemon=True,
        name="flow-api-server",
    )
    thread.start()


async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
    """Run one WebRTC/Daily voice session for this agent subprocess.

    The pipeline assembly, FlowManager and stats persistence are shared with the SIP
    gateway in :func:`session.run_voice_session`. Here we only wire in this agent's
    per-process config/flow hot-reload behaviour:

    - ``_reload_agent_config`` re-reads config.json into os.environ before the session
      builds services, so config edits apply to the next call.
    - the ``initial_node_factory`` reloads flow.py on connect, so visual-editor deploys
      hot-apply on the next call.
    """
    try:
        _reload_agent_config()
    except Exception as exc:
        logger.error(f"config.json reload failed, using last good config: {exc}")

    def _initial_node():
        global flow_module
        try:
            flow_module = _load_flow_module(_FLOW_PATH)
        except Exception as exc:
            logger.error(f"flow.py reload failed, using last good version: {exc}")
        return flow_module.create_initial_node()

    await run_voice_session(
        transport,
        initial_node_factory=_initial_node,
        agent_id=os.getenv("AGENT_ID", "unknown"),
        agent_name=os.getenv("AGENT_NAME"),
        handle_sigint=runner_args.handle_sigint,
    )


async def bot(runner_args: RunnerArguments):
    """Main bot entry point for the bot starter."""

    transport_params = {
        "daily": lambda: DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=True,

            video_out_enabled=True,
            video_out_is_live=True,
            video_out_width=1280,
            video_out_height=720,
            video_out_bitrate=2_000_000,  # 2MBps

            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.2)),
            turn_analyzer=LocalSmartTurnAnalyzerV3(),
        ),
        "webrtc": lambda: TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,

            video_out_enabled=True,
            video_out_is_live=True,
            video_out_width=1280,
            video_out_height=720,

            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.2)),
            turn_analyzer=LocalSmartTurnAnalyzerV3(),
        ),
    }

    transport = await create_transport(runner_args, transport_params)

    await run_bot(transport, runner_args)


if __name__ == "__main__":
    from pipecat.runner.run import main

    _ensure_api_server_thread()
    main()