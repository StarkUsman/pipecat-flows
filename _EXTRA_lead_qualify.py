#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Conversation flow for the lead-qualification voice bot.

This module contains everything related to Pipecat Flows and nothing else:

- The system prompt / role messages shared by every node
- Node factory functions (greeting, budget, timeline, service, qualify, ...)
- The handler functions that transition the conversation between nodes
- Small helpers used by the handlers (e.g. budget normalization)

It deliberately knows nothing about the transport, AI services, or pipeline.
The pipeline module (``bot.py``) imports :func:`create_greeting_node` from here
and feeds it to the ``FlowManager`` at runtime. Keeping the dependency arrow
pointing one way (pipeline -> flow) avoids a circular import.
"""

import re

from loguru import logger

from pipecat_flows import FlowManager, FlowsFunctionSchema, FlowArgs, NodeConfig


SYSTEM_PROMPT = """
You are a friendly and professional AI assistant for a creative agency.

Your goal is to qualify new leads by asking a few simple questions.

Only call a function when the user has clearly provided the required information.

If information is missing or unclear, ask follow-up questions.

This is a voice conversation, so keep responses natural and concise.

Do not use emojis or markdown.
"""
ROLE_MESSAGES = [{"role": "system", "content": SYSTEM_PROMPT}]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_budget_value(value: object) -> int:
    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    text = str(value)
    digits = re.sub(r"[^0-9]", "", text)
    return int(digits) if digits else 0


# ---------------------------------------------------------------------------
# Node factories
# ---------------------------------------------------------------------------
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
        functions=[
            FlowsFunctionSchema(
                name="record_name",
                handler=handle_name,
                description="Call this function when the user provides their name.",
                properties={"name": {"type": "string"}},
                required=["name"],
            )
        ],
    )


def create_get_budget_node(flow_manager: FlowManager) -> NodeConfig:
    name = flow_manager.state.get("name")

    return NodeConfig(
        name="get_budget",
        role_messages=ROLE_MESSAGES,
        task_messages=[
            {
                "role": "system",
                "content": f"Thank you, {name}. To help us find the best fit for you, what is your approximate project budget? Wait for their response.",
            }
        ],
        functions=[
            FlowsFunctionSchema(
                name="record_budget",
                handler=handle_budget,
                description="Call this function when the user provides their project budget.",
                properties={"budget": {"type": "string", "description": "The user's approximate project budget in dollars."}},
                required=["budget"],
            )
        ],
    )


def create_get_timeline_node() -> NodeConfig:
    return NodeConfig(
        name="get_timeline",
        role_messages=ROLE_MESSAGES,
        task_messages=[{"role": "system", "content": "Got it. And what is your ideal timeline for this project? (e.g., 'within 3 months', 'asap', '6 weeks'). Wait for their response."}],
        functions=[
            FlowsFunctionSchema(
                name="record_timeline",
                handler=handle_timeline,
                description="Call this function when the user provides their project timeline.",
                properties={"timeline": {"type": "string"}},
                required=["timeline"],
            )
        ],
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
        functions=[
            FlowsFunctionSchema(
                name="record_service_and_qualify",
                handler=handle_service_and_qualify,
                description="Call this function when the user describes the service they need. This is the final step before qualification.",
                properties={"service_needed": {"type": "string"}},
                required=["service_needed"],
            )
        ],
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
        functions=[
            FlowsFunctionSchema(
                name="book_meeting",
                handler=handle_booking,  # Link to the booking handler
                description="Call this function when the user provides their email address to book the meeting.",
                properties={"email": {"type": "string", "description": "Valid email address of the user"}},
                required=["email"],
            )
        ],
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
        functions=[
            FlowsFunctionSchema(
                name="end_conversation",
                handler=handle_end_conversation,  # Link to the end handler
                description="Call this function to acknowledge the user and end the conversation politely.",
                properties={},
                required=[],
            )
        ],
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
        functions=[],
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
async def handle_name(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info(f"User's name is {args['name']}")
    flow_manager.state.update({"name": args["name"]})

    return (None, create_get_budget_node(flow_manager))


async def handle_budget(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    budget = _normalize_budget_value(args["budget"])
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
    budget = _normalize_budget_value(flow_manager.state.get("budget", 0))

    if budget >= 5000:
        logger.info("User is qualified")
        return (None, create_qualify_node())
    else:
        logger.info("User is not qualified")
        logger.info("#############################################################")
        logger.info(f"Name: {flow_manager.state.get('name')}")
        logger.info("#############################################################")
        return (None, create_unqualified_node())


async def handle_booking(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    email = args["email"]
    logger.info(f"============================ SAVING LEAD ============================")
    logger.info(f"Name: {flow_manager.state.get('name')}")
    logger.info(f"Budget: {flow_manager.state.get('budget')}")
    logger.info(f"Timeline: {flow_manager.state.get('timeline')}")
    logger.info(f"Service: {flow_manager.state.get('service')}")  # Corrected state key
    logger.info(f"Email: {email}")
    logger.info(f"============================ END LEAD ============================")
    # In a real app, you would save this info to a CRM or database here!
    # Return the END node
    return (None, create_end_node())


async def handle_end_conversation(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
    logger.info("Conversation ended")
    return (None, create_end_node())