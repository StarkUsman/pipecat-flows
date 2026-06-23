#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Pipecat pipeline for a speech-to-speech (S2S) voice bot.

This is the realtime sibling of ``bot.py``. Instead of a STT → LLM → TTS chain,
a single realtime multimodal LLM ingests the user's audio and emits audio back
directly, so there is no STT or TTS stage in the pipeline:

    transport.input() -> rtvi -> context.user() -> llm -> transport.output() -> context.assistant()

The conversation logic (nodes + handlers) still lives in a per-agent ``flow.py``
and is driven by ``FlowManager`` exactly as in ``bot.py`` — pipecat-flows works
with realtime LLMs unchanged. The S2S provider is selected from the agent config
(``S2S_PROVIDER``) by ``services.build_s2s_service()``.

Run the bot using:

    uv run sts_bot.py
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

print("🚀 Starting Pipecat speech-to-speech bot...")
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

# Per-agent realtime S2S service selection (config-driven, with graceful fallback).
import services

# Pipecat Flows imports
from pipecat_flows import FlowManager

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


class StatsCollector(BaseObserver):
    """Accumulates per-conversation metrics for a speech-to-speech session.

    Same idea as bot.py's collector, but there is no TTS stage, so it only tracks
    LLM token usage, turn count, and response latency (TTFB). The summary feeds
    sts_agent_stats.
    """

    def __init__(self):
        super().__init__()
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.total_tokens = 0
        self.turns = 0
        self.completed = False
        self._llm_ttfb: list[float] = []

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
        elif isinstance(item, TTFBMetricsData):
            value = getattr(item, "value", None)
            if value is not None:
                # The realtime model's time-to-first-audio counts as response latency.
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
            "avg_llm_ttfb_ms": self._avg_ms(self._llm_ttfb),
        }


async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
    async with aiohttp.ClientSession():
        logger.info("Starting speech-to-speech bot")

        agent_id = os.getenv("AGENT_ID", "unknown")
        agent_name = os.getenv("AGENT_NAME")
        try:
            await db.init_db()
        except Exception as exc:
            logger.warning(f"Stats DB unavailable, conversation stats disabled: {exc}")

        # Re-read config.json into os.environ so config hot-updates apply to this
        # (new) connection; in-progress calls keep their already-built service.
        try:
            _reload_agent_config()
        except Exception as exc:
            logger.error(f"config.json reload failed, using last good config: {exc}")

        # Single realtime LLM (audio in → audio out) selected per agent — see services.py.
        llm = services.build_s2s_service()

        context = LLMContext()
        context_aggregator = LLMContextAggregatorPair(context)

        rtvi = RTVIProcessor()
        collector = StatsCollector()

        pipeline = Pipeline(
            [
                transport.input(),  # Transport user input
                rtvi,  # RTVI processor
                context_aggregator.user(),  # User responses
                llm,  # Realtime speech-to-speech LLM (no separate STT/TTS)
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
                await db.insert_sts_stats(row)
                logger.info(f"Saved S2S stats for session {session['id']} (status={row['status']})")
            except Exception as exc:
                logger.warning(f"Failed to persist conversation stats: {exc}")

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport, client):
            logger.info("Client connected")
            session["id"] = uuid.uuid4().hex
            session["started_at"] = datetime.now(timezone.utc)
            session["persisted"] = False
            global flow_module
            try:
                flow_module = _load_flow_module(_FLOW_PATH)
            except Exception as exc:
                logger.error(f"flow.py reload failed, using last good version: {exc}")
            await flow_manager.initialize(flow_module.create_initial_node())

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            logger.info("Client disconnected")
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
            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.2)),
            turn_analyzer=LocalSmartTurnAnalyzerV3(),
        ),
        "webrtc": lambda: TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
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
