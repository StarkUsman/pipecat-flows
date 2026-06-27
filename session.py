#
# Copyright (c) 2024-2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Transport-agnostic voice session runner shared by the WebRTC bot and the SIP gateway.

[bot.py](bot.py) runs one of these per browser/WebRTC connection; [sip_gateway.py](sip_gateway.py)
runs one per inbound phone call (FreeSWITCH WebSocket). The body is identical to what
used to live in ``bot.run_bot`` — assemble STT→LLM→TTS + FlowManager, run the
PipelineTask, and persist per-session stats — but it takes the agent's identity, the
initial-node factory, and (optionally) a config dict explicitly instead of reading
module globals, so several agents can be served from one process (the gateway) without
clobbering each other's environment.
"""

import asyncio
import os
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Callable

from loguru import logger
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
from pipecat.transports.base_transport import BaseTransport
from pipecat_flows import FlowManager

import db
import services

# Serializes the brief, synchronous "apply config → build services → restore env"
# window when a caller passes `env_overrides` (the SIP gateway, which serves many
# agents from one process). Service constructors read api keys/models from os.environ
# in __init__ only, so holding this lock just around construction keeps per-agent
# config from leaking across concurrent connections without a deep services.py refactor.
_env_lock = asyncio.Lock()


class StatsCollector(BaseObserver):
    """Accumulates per-conversation metrics from frames flowing through the pipeline.

    Pipecat already emits these because the PipelineTask runs with
    enable_metrics=True / enable_usage_metrics=True. We just tally them and expose a
    summary that the session runner persists to agent_stats when the session ends.
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


async def _build_services(env_overrides: Mapping[str, str] | None):
    """Build STT/LLM/TTS, optionally from a per-agent config applied to os.environ.

    ``services.build_*`` read os.environ. When the caller (gateway) provides
    ``env_overrides`` we temporarily apply them under ``_env_lock`` for the synchronous
    construction window, then restore — so concurrent agents in one process never see
    each other's keys. When ``env_overrides`` is None (the per-agent bot.py subprocess,
    whose env is already correct) we build directly.
    """
    if not env_overrides:
        return services.build_stt_service(), services.build_llm_service(), services.build_tts_service()

    async with _env_lock:
        saved: dict[str, str | None] = {k: os.environ.get(k) for k in env_overrides}
        try:
            for key, value in env_overrides.items():
                os.environ[key] = str(value)
            return (
                services.build_stt_service(),
                services.build_llm_service(),
                services.build_tts_service(),
            )
        finally:
            for key, old in saved.items():
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old


async def run_voice_session(
    transport: BaseTransport,
    *,
    initial_node_factory: Callable[[], object],
    agent_id: str = "unknown",
    agent_name: str | None = None,
    handle_sigint: bool = False,
    env_overrides: Mapping[str, str] | None = None,
):
    """Assemble and run a single voice conversation on ``transport``.

    Args:
        transport: Any Pipecat transport (WebRTC for the browser, FastAPI WebSocket
            for FreeSWITCH/SIP). Must emit ``on_client_connected`` / ``on_client_disconnected``.
        initial_node_factory: Called when the client connects to produce the flow's
            initial ``NodeConfig``. Invoked at connect time so flow.py edits hot-reload.
        agent_id: Agent id, recorded with stats.
        agent_name: Agent display name, recorded with stats.
        handle_sigint: Forwarded to ``PipelineRunner`` (True for the standalone bot,
            False when embedded in a server like the gateway).
        env_overrides: Optional per-agent config to apply while building services
            (see ``_build_services``).
    """
    logger.info(f"Starting voice session for agent {agent_id}")

    try:
        await db.init_db()
    except Exception as exc:
        logger.warning(f"Stats DB unavailable, conversation stats disabled: {exc}")

    stt, llm, tts = await _build_services(env_overrides)

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
        logger.info("Client connected")
        session["id"] = uuid.uuid4().hex
        session["started_at"] = datetime.now(timezone.utc)
        session["persisted"] = False
        try:
            initial_node = initial_node_factory()
        except Exception as exc:
            logger.error(f"initial node creation failed: {exc}")
            raise
        await flow_manager.initialize(initial_node)

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Client disconnected")
        await _persist_stats("disconnected")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=handle_sigint)

    try:
        await runner.run(task)
    except Exception as exc:
        # Pipeline error mid-conversation — record it as a failed session.
        await _persist_stats("failed", error=str(exc))
        raise
    else:
        # Normal end without an explicit disconnect event (e.g. flow ended itself).
        await _persist_stats("disconnected")
