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
import os

import aiohttp
from aiohttp import web

from dotenv import load_dotenv
from loguru import logger

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
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.deepgram.tts import DeepgramTTSService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.heygen.api_interactive_avatar import AvatarQuality, NewSessionRequest
#from pipecat.services.heygen.video import HeyGenVideoService
from pipecat.services.tavus.video import TavusVideoService

# Pipecat Flows imports
from pipecat_flows import FlowManager

# Conversation flow (nodes + handlers) lives in its own module.
# Imported as a module so it can be hot-reloaded between sessions.
# from flow import create_greeting_node
import flow as flow_module

logger.info("✅ All components loaded successfully!")

load_dotenv(override=True)

_FLOW_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow.py")
_api_server_started = False
_api_server_thread_started = False


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

    app = web.Application()
    app.router.add_post("/deployNewFlow", deploy_new_flow)

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


def _build_tts_service():
    provider = os.getenv("TTS_PROVIDER", "deepgram").strip().lower()

    def _has_real_value(value: str | None) -> bool:
        return bool(value and value.strip() and not value.strip().lower().startswith("your_"))

    if provider == "deepgram":
        logger.warning("Using Deepgram TTS because TTS_PROVIDER=deepgram")
        return DeepgramTTSService(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            settings=DeepgramTTSService.Settings(
                voice=os.getenv("DEEPGRAM_TTS_VOICE", "aura-2-helena-en"),
            ),
        )

    if not _has_real_value(os.getenv("CARTESIA_API_KEY")):
        logger.warning(
            "CARTESIA_API_KEY is missing or still a placeholder; falling back to Deepgram TTS"
        )
        return DeepgramTTSService(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            settings=DeepgramTTSService.Settings(
                voice=os.getenv("DEEPGRAM_TTS_VOICE", "aura-2-helena-en"),
            ),
        )

    try:
        return CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            settings=CartesiaTTSService.Settings(
                voice=os.getenv("CARTESIA_TTS_VOICE", "e07c00bc-4134-4eae-9ea4-1a55fb45746b"),
            ),
        )
    except Exception as error:
        logger.warning(f"Cartesia TTS failed to initialize, falling back to Deepgram TTS: {error}")
        return DeepgramTTSService(
            api_key=os.getenv("DEEPGRAM_API_KEY"),
            settings=DeepgramTTSService.Settings(
                voice=os.getenv("DEEPGRAM_TTS_VOICE", "aura-2-helena-en"),
            ),
        )


async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
    async with aiohttp.ClientSession() as session:
        logger.info(f"Starting bot")

        stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
        tts = _build_tts_service()

        #llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"))
        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL"),
            model=os.getenv("OPENAI_MODEL"),
        )

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
            observers=[RTVIObserver(rtvi)],
        )

        # Initialize flow manager in dynamic mode
        flow_manager = FlowManager(
            task=task,
            llm=llm,
            context_aggregator=context_aggregator,
            transport=transport,
        )

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport, client):
            logger.info(f"Client connected")
            try:
                importlib.reload(flow_module)
            except Exception as exc:
                logger.error(f"flow.py reload failed, using last good version: {exc}")
            # await flow_manager.initialize(flow_module.create_greeting_node())
            await flow_manager.initialize(flow_module.create_initial_node())

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            logger.info(f"Client disconnected")
            await task.cancel()

        runner = PipelineRunner(handle_sigint=runner_args.handle_sigint)

        await runner.run(task)


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