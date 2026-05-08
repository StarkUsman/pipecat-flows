#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Pipecat Flows Agent with HeyGen Avatar.

A voice AI bot with avatar video generation that qualifies leads through
a conversational flow. Features real-time video avatars powered by HeyGen.

Required AI services:
- Deepgram (Speech-to-Text)
- OpenAI (LLM)
- Cartesia (Text-to-Speech)
- HeyGen (Avatar Video)

Run the bot using:

    uv run bot.py
"""

import os
import aiohttp

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
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.heygen.api_interactive_avatar import AvatarQuality, NewSessionRequest
from pipecat.services.heygen.video import HeyGenVideoService

# Pipecat Flows imports
from pipecat_flows import FlowManager, FlowsFunctionSchema, FlowArgs, NodeConfig


logger.info("✅ All components loaded successfully!")

load_dotenv(override=True)

SYSTEM_PROMPT = "You are a friendly and professional AI assistant for a creative agency. Your goal is to qualify new leads by asking a few simple questions. You must ALWAYS use the provided functions to progress the conversation. This is a voice conversation, so keep your responses natural and concise. Do not use emojis or markdown."
ROLE_MESSAGES = [{"role": "system", "content": SYSTEM_PROMPT}]

def create_greeting_node() -> NodeConfig:
    return NodeConfig(
        name="greeting",
        role_messages=ROLE_MESSAGES,
        task_messages=[
                    {
                        "role": "system",
                        "content": "Greet the user warmly, introduce yourself, and ask for their name. Wait for their response.",
                    }
                ],
        functions = [
            FlowsFunctionSchema(
                name="record_name",
                handler=handle_name,
                description="Call this function when the user provides their name.",
                properties={"name": {"type": "string"}},
                required=["name"],

            )
        ]
    )


def create_get_budget_node(flow_manager: FlowManager) -> NodeConfig:

    name = flow_manager.state.get("name")

    return NodeConfig(
        name="get_budget",
        role_messages=ROLE_MESSAGES,
        task_messages=[{
            "role": "system",
            "content": f"Thank you, {name}. To help us find the best fit for you, what is your approximate project budget? Wait for their response.",
        }],
        functions = [
            FlowsFunctionSchema(
                name="record_budget",
                handler=handle_budget,
                description="Call this function when the user provides their project budget.",
                properties={"budget": {"type": "number", "description": "The user's approximate project budget in dollars."}},
                required=["budget"]
            )
        ]
    )

def create_get_timeline_node() -> NodeConfig:

    return NodeConfig(
        name="get_timeline",
        role_messages=ROLE_MESSAGES,
        task_messages=[{"role": "system", "content": "Got it. And what is your ideal timeline for this project? (e.g., 'within 3 months', 'asap', '6 weeks'). Wait for their response."}],
        functions = [
            FlowsFunctionSchema(
                name="record_timeline",
                handler=handle_timeline,
                description="Call this function when the user provides their project timeline.",
                properties={"timeline": {"type": "string"}},
                required=["timeline"]
            )
        ]
    )

def create_get_service_node() -> NodeConfig:

    return NodeConfig(
        name="get_services",
        role_messages=ROLE_MESSAGES,
        task_messages=[
            {
                "role": "system",
                "content": "Great. And finally, what specific service are you looking for? (e.g., 'a custom AI avatar', 'automating my business', 'a new website'). Wait for their response.",
            }
        ],
        functions = [
            FlowsFunctionSchema(
                name="record_service_and_qualify",
                handler=handle_service_and_qualify,
                description="Call this function when the user describes the service they need. This is the final step before qualification.",
                properties={"service_needed": {"type": "string"}},
                required=["service_needed"],
            )
        ]
    )

def create_qualify_node() -> NodeConfig:

    return NodeConfig(
        name="qualify_lead",
        role_messages=ROLE_MESSAGES,
        task_messages=[
            {
                "role": "system",
                "content": "That sounds like a perfect fit for our team. The last step is to get you booked in with a specialist. I can do that for you now. What is a good email address to send the calendar invite to?",
            }
        ],
        functions = [
            FlowsFunctionSchema(
                name="book_meeting",
                handler=handle_booking, # Link to the booking handler
                description="Call this function when the user provides their email address to book the meeting.",
                properties={"email": {"type": "string"}},
                required=["email"],
            )
        ]
    )

def create_unqualified_node() -> NodeConfig:
    return NodeConfig(
        name="not_qualified_lead",
        role_messages=ROLE_MESSAGES,
        task_messages=[
            {
                "role": "system",
                "content": "Thank you for all that information. Based on your needs, it sounds like you might not be a perfect fit for our core services right now. I really appreciate you reaching out. Have a great day!",
            }
        ],
        functions = [
            FlowsFunctionSchema(
                name="end_conversation",
                handler=handle_end_conversation, # Link to the end handler
                description="Call this function to acknowledge the user and end the conversation politely.",
                properties={},
                required=[],
            )
        ]
    )

def create_end_node() -> NodeConfig:
    return NodeConfig(
        name="end",
        role_messages=ROLE_MESSAGES,
        task_messages=[
            {
                "role": "system",
                "content": "Thank the user for their time and say a polite and professional goodbye. This is the final step.",
            }
        ],
        functions = []
    )

### handler
async def handle_name(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info(f"User's name is {args['name']}")
    flow_manager.state.update({"name": args["name"]})

    return (None, create_get_budget_node(flow_manager))

async def handle_budget(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    budget = args["budget"]
    logger.info(f"User's budget is: {budget}")
    flow_manager.state.update({"budget": budget})
    return (None, create_get_timeline_node())

async def handle_timeline(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    """Saves the timeline and transitions to the service node."""
    logger.info(f"User's timeline is: {args['timeline']}")
    flow_manager.state.update({"timeline": args["timeline"]})
    return (None, create_get_service_node())

async def handle_service_and_qualify(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info(f"User needs service: {args['service_needed']}")
    flow_manager.state.update({"service": args["service_needed"]})

    # --- Qualification logic ---
    budget = flow_manager.state.get("budget", 0)

    if budget >= 5000:
        logger.info("User is qualified")
        return (None, create_qualify_node())
    else:
        logger.info("User is not qualified")
        return (None, create_unqualified_node())

async def handle_booking(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    email = args['email']
    logger.info(f"============================ SAVING LEAD ============================")
    logger.info(f"Name: {flow_manager.state.get('name')}")
    logger.info(f"Budget: {flow_manager.state.get('budget')}")
    logger.info(f"Timeline: {flow_manager.state.get('timeline')}")
    logger.info(f"Service: {flow_manager.state.get('service')}") # Corrected state key
    logger.info(f"Email: {email}")
    logger.info(f"============================ END LEAD ============================")
    # In a real app, you would save this info to a CRM or database here!
    # Return the END node
    return (None, create_end_node())

async def handle_end_conversation(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:

    logger.info("Conversation ended")
    return (None, create_end_node())

async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):

    async with aiohttp.ClientSession() as session:
        logger.info(f"Starting bot")

        stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))

        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            voice_id="e07c00bc-4134-4eae-9ea4-1a55fb45746b",  
        )

        #llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"))
        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL"),
            model=os.getenv("OPENAI_MODEL"),
        )

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
            # Kick off the conversation.
            await flow_manager.initialize(create_greeting_node())

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

    main()
