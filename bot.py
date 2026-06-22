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
from pipecat.frames.frames import EndFrame, LLMFullResponseEndFrame, MetricsFrame
from pipecat.metrics.metrics import (
    LLMUsageMetricsData,
    TTFBMetricsData,
    TTSUsageMetricsData,
)
from pipecat.observers.base_observer import BaseObserver
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frameworks.rtvi import RTVIObserver, RTVIProcessor
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.daily.transport import DailyParams

# Service imports
from pipecat.services.heygen.api_interactive_avatar import AvatarQuality, NewSessionRequest
#from pipecat.services.heygen.video import HeyGenVideoService
from pipecat.services.tavus.video import TavusVideoService

# Per-agent STT / LLM / TTS selection (config-driven, with graceful fallback).
import services

# Pipecat Flows imports
from pipecat_flows import FlowManager

# Conversation flow (nodes + handlers) lives in its own module.
# Loaded dynamically so FLOW_PATH env var can point to any agent-specific file.
# from flow import create_greeting_node

logger.info("✅ All components loaded successfully!")

load_dotenv(override=True)

_FLOW_PATH = os.environ.get(
    "FLOW_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow.py"),
)
_api_server_started = False


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


class StatsCollector(BaseObserver):
    """Accumulates per-conversation metrics from frames flowing through the pipeline.

    Pipecat already emits these because the PipelineTask runs with
    enable_metrics=True / enable_usage_metrics=True. We just tally them and expose a
    summary that bot.py persists to agent_stats when the session ends.
    """

    def __init__(self):
        super().__init__()
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.total_tokens = 0
        self.tts_characters = 0
        self.turns = 0
        self.completed = False
        self._llm_ttfb: list[float] = []
        self._tts_ttfb: list[float] = []

    async def on_push_frame(self, *args, **kwargs):
        # Be tolerant of Pipecat's evolving observer signature: newer versions pass a
        # single FramePushed object, older ones pass (src, dst, frame, ...).
        frame = None
        if args:
            first = args[0]
            frame = getattr(first, "frame", None)
            if frame is None and len(args) >= 3:
                frame = args[2]
        if frame is None:
            frame = kwargs.get("frame")
        if frame is not None:
            self._handle_frame(frame)

    def _handle_frame(self, frame):
        if isinstance(frame, MetricsFrame):
            for item in getattr(frame, "data", []) or []:
                self._handle_metric(item)
        elif isinstance(frame, LLMFullResponseEndFrame):
            self.turns += 1
        elif isinstance(frame, EndFrame):
            # Flow reached its end node (end_conversation post-action).
            self.completed = True

    def _handle_metric(self, item):
        if isinstance(item, LLMUsageMetricsData):
            usage = getattr(item, "value", None)
            if usage is not None:
                self.prompt_tokens += getattr(usage, "prompt_tokens", 0) or 0
                self.completion_tokens += getattr(usage, "completion_tokens", 0) or 0
                self.total_tokens += getattr(usage, "total_tokens", 0) or 0
        elif isinstance(item, TTSUsageMetricsData):
            self.tts_characters += getattr(item, "value", 0) or 0
        elif isinstance(item, TTFBMetricsData):
            value = getattr(item, "value", None)
            if value is None:
                return
            processor = (getattr(item, "processor", "") or "").lower()
            if "tts" in processor:
                self._tts_ttfb.append(value)
            else:
                # Anything that isn't TTS (LLM, STT, …) counts toward response latency.
                self._llm_ttfb.append(value)

    @staticmethod
    def _avg_ms(values: list[float]) -> float | None:
        return (sum(values) / len(values)) * 1000 if values else None

    def summary(self) -> dict:
        return {
            "turns": self.turns,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "tts_characters": self.tts_characters,
            "avg_llm_ttfb_ms": self._avg_ms(self._llm_ttfb),
            "avg_tts_ttfb_ms": self._avg_ms(self._tts_ttfb),
        }


async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
    async with aiohttp.ClientSession() as session:
        logger.info(f"Starting bot")

        agent_id = os.getenv("AGENT_ID", "unknown")
        agent_name = os.getenv("AGENT_NAME")
        try:
            await db.init_db()
        except Exception as exc:
            logger.warning(f"Stats DB unavailable, conversation stats disabled: {exc}")

        # STT / LLM / TTS are selected per agent from its config (env) — see services.py.
        stt = services.build_stt_service()
        llm = services.build_llm_service()
        tts = services.build_tts_service()

        # tavus = TavusVideoService(
        #     api_key=os.getenv("TAVUS_API_KEY"),
        #     replica_id=os.getenv("TAVUS_REPLICA_ID"),
        #     session=session,
        # )

#        heygen = HeyGenVideoService(
#            api_key=os.getenv("HEYGEN_API_KEY"),
#            session=session,
#            session_request=NewSessionRequest(
#                avatar_id="Marianne_Black_Suit_public",
#                version="v2",
#                quality=AvatarQuality.high
#            ),
#        )

        context = LLMContext()
        context_aggregator = LLMContextAggregatorPair(context)

        rtvi = RTVIProcessor()
        collector = StatsCollector()

        pipeline = Pipeline(
            [
                transport.input(),  # Transport user input
                rtvi,  # RTVI processor
                stt,
                context_aggregator.user(),  # User responses
                llm,  # LLM
                tts,  # TTS
                # tavus,
                #                heygen,  # HeyGen video service
                transport.output(),  # Transport bot output
                context_aggregator.assistant(),  # Assistant spoken responses
            ]
        )

        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                enable_metrics=True,
                enable_usage_metrics=True,
                allow_interruptions=True,
            ),
            observers=[RTVIObserver(rtvi), collector],
        )

        # Initialize flow manager in dynamic mode
        flow_manager = FlowManager(
            task=task,
            llm=llm,
            context_aggregator=context_aggregator,
            transport=transport,
        )

        # Per-session stats state (mutable so the nested handlers can update it).
        session = {"id": None, "started_at": None, "persisted": False}

        async def _persist_stats(status: str, error: str | None = None):
            if session["persisted"] or session["started_at"] is None:
                return
            session["persisted"] = True
            ended_at = datetime.now(timezone.utc)
            last_node = getattr(flow_manager, "current_node", None)
            if last_node is not None and not isinstance(last_node, str):
                last_node = getattr(last_node, "name", str(last_node))
            row = {
                "session_id": session["id"],
                "agent_id": agent_id,
                "agent_name": agent_name,
                "started_at": session["started_at"],
                "ended_at": ended_at,
                "duration_seconds": (ended_at - session["started_at"]).total_seconds(),
                "status": "completed" if collector.completed else status,
                "last_node": last_node,
                "error": error,
                **collector.summary(),
            }
            try:
                await db.insert_stats(row)
                logger.info(f"Saved stats for session {session['id']} (status={row['status']})")
            except Exception as exc:
                logger.warning(f"Failed to persist conversation stats: {exc}")

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport, client):
            logger.info(f"Client connected")
            session["id"] = uuid.uuid4().hex
            session["started_at"] = datetime.now(timezone.utc)
            session["persisted"] = False
            global flow_module
            try:
                flow_module = _load_flow_module(_FLOW_PATH)
            except Exception as exc:
                logger.error(f"flow.py reload failed, using last good version: {exc}")
            # await flow_manager.initialize(flow_module.create_greeting_node())
            await flow_manager.initialize(flow_module.create_initial_node())

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            logger.info(f"Client disconnected")
            await _persist_stats("disconnected")
            await task.cancel()

        runner = PipelineRunner(handle_sigint=runner_args.handle_sigint)

        try:
            await runner.run(task)
        except Exception as exc:
            # Pipeline error mid-conversation — record it as a failed session.
            await _persist_stats("failed", error=str(exc))
            raise
        else:
            # Normal end without an explicit disconnect event (e.g. flow ended itself).
            await _persist_stats("disconnected")


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