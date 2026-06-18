from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union
# pipecat-flow-json:eyIkaWQiOiJodHRwczovL2Zsb3dzLnBpcGVjYXQuYWkvc2NoZW1hL2Zsb3cuanNvbiIsIm1ldGEiOnsibmFtZSI6IlVudGl0bGVkIiwidmVyc2lvbiI6IjAuMS4wIn0sIm5vZGVzIjpbeyJpZCI6ImluaXRpYWwiLCJ0eXBlIjoiaW5pdGlhbCIsInBvc2l0aW9uIjp7IngiOjQwMCwieSI6MH0sImRhdGEiOnsibGFiZWwiOiJJbml0aWFsIiwicm9sZV9tZXNzYWdlcyI6W3sicm9sZSI6InN5c3RlbSIsImNvbnRlbnQiOiJZb3UgYXJlIGEgd2FybSwgZW5nYWdpbmcgcG9kY2FzdCBob3N0IHdpdGggYSBuYXR1cmFsIGNvbnZlcnNhdGlvbmFsIHN0eWxlLiBZb3UncmUgZ2VudWluZWx5IGN1cmlvdXMgYWJvdXQgeW91ciBndWVzdHMgYW5kIHNraWxsZWQgYXQgbWFraW5nIHRoZW0gZmVlbCBjb21mb3J0YWJsZSB3aGlsZSBkcmF3aW5nIG91dCBpbnRlcmVzdGluZyBpbnNpZ2h0cy4gWW91ciBxdWVzdGlvbnMgZmxvdyBuYXR1cmFsbHksIGFuZCB5b3UgbGlzdGVuIGFjdGl2ZWx5LCBidWlsZGluZyBvbiB3aGF0IHlvdXIgZ3Vlc3Qgc2hhcmVzLiJ9XSwidGFza19tZXNzYWdlcyI6W3sicm9sZSI6InN5c3RlbSIsImNvbnRlbnQiOiJXZWxjb21lIHRoZSBndWVzdCB3YXJtbHkgYW5kIGVudGh1c2lhc3RpY2FsbHkuIEZvY3VzIHRoaXMgZXhjaGFuZ2Ugb24gZ2V0dGluZyB0byBrbm93IHdobyB0aGV5IGFyZS4gSW52aXRlIHRoZW0gdG8gYnJpZWZseSBpbnRyb2R1Y2UgdGhlbXNlbHZlc+KAlG5hbWUsIHJvbGUsIGN1cnJlbnQgZm9jdXMsIG9yIGFueXRoaW5nIGZ1biB0aGV5J2QgbGlrZSB0byBzaGFyZS4gQXNrIG9uZSBmb2xsb3ctdXAgcXVlc3Rpb24gaWYgaXQgaGVscHMgY2xhcmlmeSBvciBoaWdobGlnaHQgc29tZXRoaW5nIGludGVyZXN0aW5nIGFib3V0IHRoZW0uIE9uY2UgeW91IGZlZWwgeW91IGhhdmUgYSBjbGVhciBpbnRyb2R1Y3Rpb24sIHVzZSB0aGUgcHJvY2VlZF90b190b3BpYyBmdW5jdGlvbiB0byBtb3ZlIGludG8gdG9waWMgc2VsZWN0aW9uLiJ9XSwiZnVuY3Rpb25zIjpbeyJuYW1lIjoicHJvY2VlZF90b190b3BpYyIsImRlc2NyaXB0aW9uIjoiVXNlIGFmdGVyIHRoZSBndWVzdCBoYXMgaW50cm9kdWNlZCB0aGVtc2VsdmVzLiIsInByb3BlcnRpZXMiOnsiZ3Vlc3Rfc3VtbWFyeSI6eyJ0eXBlIjoic3RyaW5nIiwiZGVzY3JpcHRpb24iOiJBIHF1aWNrIHN1bW1hcnkgb2Ygd2hvIHRoZSBndWVzdCBpcyAobmFtZSwgcm9sZSwgYXJlYSBvZiBleHBlcnRpc2UsIGV0Yy4pIn19LCJyZXF1aXJlZCI6WyJndWVzdF9zdW1tYXJ5Il0sIm5leHRfbm9kZV9pZCI6InRvcGljIn1dLCJ0eXBlIjoiaW5pdGlhbCJ9fSx7ImlkIjoidG9waWMiLCJ0eXBlIjoibm9kZSIsInBvc2l0aW9uIjp7IngiOjQwMCwieSI6ODB9LCJkYXRhIjp7ImxhYmVsIjoiVG9waWMgU2VsZWN0aW9uIiwidGFza19tZXNzYWdlcyI6W3sicm9sZSI6InN5c3RlbSIsImNvbnRlbnQiOiJOb3cgdGhhdCB5b3Uga25vdyB3aG8gdGhlIGd1ZXN0IGlzLCBoZWxwIHRoZW0gY2hvb3NlIHRoZSB0b3BpYyB0aGV5J2QgbGlrZSB0byBleHBsb3JlLiBSZWZlciBiYWNrIHRvIHRoZWlyIGludHJvZHVjdGlvbiB0byBwZXJzb25hbGl6ZSB0aGUgdHJhbnNpdGlvbi4gQXNrIHdoYXQgdG9waWMsIHN0b3J5LCBvciBjaGFsbGVuZ2UgdGhleSdyZSBleGNpdGVkIHRvIGRpc2N1c3MgdG9kYXkuIFNob3cgZ2VudWluZSBpbnRlcmVzdCBhbmQsIGlmIG5lZWRlZCwgYXNrIGEgY2xhcmlmeWluZyBxdWVzdGlvbiB0byBtYWtlIHN1cmUgeW91IHVuZGVyc3RhbmQgdGhlIGFuZ2xlIHRoZXkgd2FudCB0byB0YWtlLiBPbmNlIHRoZSB0b3BpYyBmZWVscyBjbGVhciBhbmQgc3BlY2lmaWMgZW5vdWdoIHRvIGRpdmUgaW50bywgdXNlIHRoZSBzdGFydF9pbnRlcnZpZXcgZnVuY3Rpb24uIn1dLCJmdW5jdGlvbnMiOlt7Im5hbWUiOiJzdGFydF9pbnRlcnZpZXciLCJkZXNjcmlwdGlvbiI6IlVzZSB0aGlzIHdoZW4gdGhlIGd1ZXN0IGhhcyBzaGFyZWQgYSBjbGVhciB0b3BpYyB0aGV5IHdhbnQgdG8gZXhwbG9yZS4iLCJwcm9wZXJ0aWVzIjp7InRvcGljIjp7InR5cGUiOiJzdHJpbmciLCJkZXNjcmlwdGlvbiI6IlRoZSB0b3BpYyB0aGUgZ3Vlc3Qgd2FudHMgdG8gZGlzY3VzcyJ9fSwicmVxdWlyZWQiOlsidG9waWMiXSwibmV4dF9ub2RlX2lkIjoiaW50ZXJ2aWV3In1dLCJ0eXBlIjoibm9kZSJ9fSx7ImlkIjoiaW50ZXJ2aWV3IiwidHlwZSI6Im5vZGUiLCJwb3NpdGlvbiI6eyJ4Ijo0MjAsInkiOjE2MH0sImRhdGEiOnsibGFiZWwiOiJJbnRlcnZpZXciLCJ0YXNrX21lc3NhZ2VzIjpbeyJyb2xlIjoic3lzdGVtIiwiY29udGVudCI6IllvdSdyZSBub3cgaW4gdGhlIGhlYXJ0IG9mIHRoZSBpbnRlcnZpZXcuIFN0YXJ0IGJ5IGludHJvZHVjaW5nIHRoZSB0b3BpYyB3aXRoIGVudGh1c2lhc20sIHRoZW4gZGl2ZSBkZWVwIGludG8gb25lIGtleSBhc3BlY3QgYXQgYSB0aW1lLiBBc2sgb3Blbi1lbmRlZCwgdGhvdWdodGZ1bCBxdWVzdGlvbnMgdGhhdCBpbnZpdGUgc3Rvcnl0ZWxsaW5nIGFuZCBwZXJzb25hbCBpbnNpZ2h0cy4gTGlzdGVuIGFjdGl2ZWx5IHRvIHJlc3BvbnNlcyBhbmQgYXNrIG5hdHVyYWwgZm9sbG93LXVwIHF1ZXN0aW9ucyB0aGF0IGJ1aWxkIG9uIHdoYXQgeW91ciBndWVzdCBzaGFyZXPigJRkaWcgZGVlcGVyIGludG8gaW50ZXJlc3RpbmcgcG9pbnRzLCBhc2sgZm9yIGV4YW1wbGVzLCBvciBleHBsb3JlIHRoZSAnd2h5JyBiZWhpbmQgdGhlaXIgYW5zd2Vycy4gS2VlcCB0aGUgY29udmVyc2F0aW9uIGZsb3dpbmcgbmF0dXJhbGx5LCBsaWtlIGEgZ2VudWluZSBkaWFsb2d1ZSBiZXR3ZWVuIGZyaWVuZHMuIE9uY2UgeW91J3ZlIHRob3JvdWdobHkgZXhwbG9yZWQgYW4gYXNwZWN0ICh0eXBpY2FsbHkgYWZ0ZXIgMy01IGV4Y2hhbmdlcyksIHVzZSB0aGUgbmV4dF9xdWVzdGlvbiBmdW5jdGlvbiB0byBzbW9vdGhseSB0cmFuc2l0aW9uIHRvIHRoZSBuZXh0IGtleSBhc3BlY3QuIEFmdGVyIGNvdmVyaW5nIDMga2V5IGFzcGVjdHMgb2YgdGhlIHRvcGljLCB1c2UgdGhlIHdyYXBfdXAgZnVuY3Rpb24gdG8gY29uY2x1ZGUgdGhlIGludGVydmlldy4ifV0sImZ1bmN0aW9ucyI6W3sibmFtZSI6Im5leHRfcXVlc3Rpb24iLCJkZXNjcmlwdGlvbiI6IlVzZSB0aGlzIGFmdGVyIHlvdSd2ZSB0aG9yb3VnaGx5IGV4cGxvcmVkIHRoZSBjdXJyZW50IGFzcGVjdCB3aXRoIG11bHRpcGxlIHF1ZXN0aW9ucyBhbmQgZm9sbG93LXVwcy4iLCJuZXh0X25vZGVfaWQiOiJpbnRlcnZpZXcifSx7Im5hbWUiOiJ3cmFwX3VwIiwiZGVzY3JpcHRpb24iOiJVc2UgdGhpcyB3aGVuIHlvdSd2ZSBnYXRoZXJlZCBzdWJzdGFudGlhbCBpbnNpZ2h0cyBhbmQgYXJlIHJlYWR5IHRvIHdyYXAgdXAuIiwibmV4dF9ub2RlX2lkIjoiY29uY2x1c2lvbiJ9XSwidHlwZSI6Im5vZGUifX0seyJpZCI6ImNvbmNsdXNpb24iLCJ0eXBlIjoibm9kZSIsInBvc2l0aW9uIjp7IngiOjQyMCwieSI6MjQwfSwiZGF0YSI6eyJsYWJlbCI6IkNvbmNsdXNpb24iLCJ0YXNrX21lc3NhZ2VzIjpbeyJyb2xlIjoic3lzdGVtIiwiY29udGVudCI6IkV4cHJlc3MgZ2VudWluZSBhcHByZWNpYXRpb24gZm9yIHRoZSBjb252ZXJzYXRpb24gYW5kIHRoZSBpbnNpZ2h0cyB5b3VyIGd1ZXN0IHNoYXJlZC4gU3VtbWFyaXplIDItMyBrZXkgdGFrZWF3YXlzIG9yIG1lbW9yYWJsZSBwb2ludHMgZnJvbSB5b3VyIGRpc2N1c3Npb24gaW4gYSB3YXJtLCBjb252ZXJzYXRpb25hbCB3YXnigJR0aGlzIGhlbHBzIHJlaW5mb3JjZSB0aGUgdmFsdWUgb2YgdGhlIGNvbnZlcnNhdGlvbi4gVGhlbiwgYXNrIHlvdXIgZ3Vlc3QgaWYgdGhleSBoYXZlIGFueSBmaW5hbCB0aG91Z2h0cywgYSBsYXN0IHdvcmQsIG9yIGFueXRoaW5nIGVsc2UgdGhleSdkIGxpa2UgdG8gYWRkLiBXYWl0IGZvciB0aGVpciByZXNwb25zZSwgdGhlbiB1c2UgdGhlIGVuZF9pbnRlcnZpZXcgZnVuY3Rpb24gdG8gd3JhcCB1cC4ifV0sImZ1bmN0aW9ucyI6W3sibmFtZSI6ImVuZF9pbnRlcnZpZXciLCJkZXNjcmlwdGlvbiI6IlVzZSB0aGlzIGFmdGVyIHRoZSBndWVzdCBoYXMgc2hhcmVkIHRoZWlyIGZpbmFsIHRob3VnaHRzLiIsIm5leHRfbm9kZV9pZCI6ImZpbmFsIn1dLCJ0eXBlIjoibm9kZSJ9fSx7ImlkIjoiZmluYWwiLCJ0eXBlIjoiZW5kIiwicG9zaXRpb24iOnsieCI6NDQwLCJ5IjozMjB9LCJkYXRhIjp7ImxhYmVsIjoiRmluYWwiLCJ0YXNrX21lc3NhZ2VzIjpbeyJyb2xlIjoic3lzdGVtIiwiY29udGVudCI6IlRoYW5rIHRoZSBndWVzdCBvbmUgZmluYWwgdGltZSBmb3Igam9pbmluZyB5b3UgYW5kIGZvciBzaGFyaW5nIHRoZWlyIGluc2lnaHRzLiBFbmQgdGhlIGNvbnZlcnNhdGlvbiBvbiBhIHBvc2l0aXZlLCB3YXJtIG5vdGUuIn1dLCJwb3N0X2FjdGlvbnMiOlt7InR5cGUiOiJlbmRfY29udmVyc2F0aW9uIn1dLCJ0eXBlIjoiZW5kIn19XSwiZWRnZXMiOlt7ImlkIjoiZnVuYy1pbml0aWFsLXByb2NlZWRfdG9fdG9waWMtdG9waWMiLCJzb3VyY2UiOiJpbml0aWFsIiwidGFyZ2V0IjoidG9waWMiLCJsYWJlbCI6InByb2NlZWRfdG9fdG9waWMifSx7ImlkIjoiZnVuYy10b3BpYy1zdGFydF9pbnRlcnZpZXctaW50ZXJ2aWV3Iiwic291cmNlIjoidG9waWMiLCJ0YXJnZXQiOiJpbnRlcnZpZXciLCJsYWJlbCI6InN0YXJ0X2ludGVydmlldyJ9LHsiaWQiOiJmdW5jLWludGVydmlldy1uZXh0X3F1ZXN0aW9uLWludGVydmlldyIsInNvdXJjZSI6ImludGVydmlldyIsInRhcmdldCI6ImludGVydmlldyIsImxhYmVsIjoibmV4dF9xdWVzdGlvbiJ9LHsiaWQiOiJmdW5jLWludGVydmlldy13cmFwX3VwLWNvbmNsdXNpb24iLCJzb3VyY2UiOiJpbnRlcnZpZXciLCJ0YXJnZXQiOiJjb25jbHVzaW9uIiwibGFiZWwiOiJ3cmFwX3VwIn0seyJpZCI6ImZ1bmMtY29uY2x1c2lvbi1lbmRfaW50ZXJ2aWV3LWZpbmFsIiwic291cmNlIjoiY29uY2x1c2lvbiIsInRhcmdldCI6ImZpbmFsIiwibGFiZWwiOiJlbmRfaW50ZXJ2aWV3In1dfQ==
"""Generated Pipecat Flow: Untitled

This file was generated from the visual flow editor.
Customize the function handlers to implement your flow logic.
"""

from pipecat_flows import (
    FlowArgs,
    FlowManager,
    FlowResult,
    FlowsFunctionSchema,
    NodeConfig,
)

# Type definitions
class ProceedToTopicResult(FlowResult):
    """Result type for proceed_to_topic function"""
    guest_summary: str

class StartInterviewResult(FlowResult):
    """Result type for start_interview function"""
    topic: str




# Node creation functions
def create_initial_node() -> NodeConfig:
    """Create the Initial node."""

    async def handle_proceed_to_topic(args: FlowArgs, flow_manager: FlowManager) -> tuple[ProceedToTopicResult | None, NodeConfig]:
        """Handler for proceed_to_topic function"""
        guest_summary: str = args.get("guest_summary", "")
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return ProceedToTopicResult(guest_summary=guest_summary), create_topic_node()

    proceed_to_topic_func = FlowsFunctionSchema(
        name="proceed_to_topic",
        handler=handle_proceed_to_topic,
        description="Use after the guest has introduced themselves.",
        properties={
            "guest_summary": {
                "type": "string",
                "description": "A quick summary of who the guest is (name, role, area of expertise, etc.)"
            }
        },
        required=["guest_summary"]
    )
    return NodeConfig(
        name="initial",
        role_messages=[
            {
                "role": "system",
                "content": "You are a warm, engaging podcast host with a natural conversational style. You're genuinely curious about your guests and skilled at making them feel comfortable while drawing out interesting insights. Your questions flow naturally, and you listen actively, building on what your guest shares."
            }
        ],
        task_messages=[
            {
                "role": "system",
                "content": "Welcome the guest warmly and enthusiastically. Focus this exchange on getting to know who they are. Invite them to briefly introduce themselves—name, role, current focus, or anything fun they'd like to share. Ask one follow-up question if it helps clarify or highlight something interesting about them. Once you feel you have a clear introduction, use the proceed_to_topic function to move into topic selection."
            }
        ],
        functions=[proceed_to_topic_func],
    )

def create_topic_node() -> NodeConfig:
    """Create the Topic Selection node."""

    async def handle_start_interview(args: FlowArgs, flow_manager: FlowManager) -> tuple[StartInterviewResult | None, NodeConfig]:
        """Handler for start_interview function"""
        topic: str = args.get("topic", "")
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return StartInterviewResult(topic=topic), create_interview_node()

    start_interview_func = FlowsFunctionSchema(
        name="start_interview",
        handler=handle_start_interview,
        description="Use this when the guest has shared a clear topic they want to explore.",
        properties={
            "topic": {
                "type": "string",
                "description": "The topic the guest wants to discuss"
            }
        },
        required=["topic"]
    )
    return NodeConfig(
        name="topic",
        task_messages=[
            {
                "role": "system",
                "content": "Now that you know who the guest is, help them choose the topic they'd like to explore. Refer back to their introduction to personalize the transition. Ask what topic, story, or challenge they're excited to discuss today. Show genuine interest and, if needed, ask a clarifying question to make sure you understand the angle they want to take. Once the topic feels clear and specific enough to dive into, use the start_interview function."
            }
        ],
        functions=[start_interview_func],
    )

def create_interview_node() -> NodeConfig:
    """Create the Interview node."""

    async def handle_next_question(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for next_question function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_interview_node()


    async def handle_wrap_up(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for wrap_up function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_conclusion_node()

    next_question_func = FlowsFunctionSchema(
        name="next_question",
        handler=handle_next_question,
        description="Use this after you've thoroughly explored the current aspect with multiple questions and follow-ups.",
        properties={},
        required=[]
    )
    wrap_up_func = FlowsFunctionSchema(
        name="wrap_up",
        handler=handle_wrap_up,
        description="Use this when you've gathered substantial insights and are ready to wrap up.",
        properties={},
        required=[]
    )
    return NodeConfig(
        name="interview",
        task_messages=[
            {
                "role": "system",
                "content": "You're now in the heart of the interview. Start by introducing the topic with enthusiasm, then dive deep into one key aspect at a time. Ask open-ended, thoughtful questions that invite storytelling and personal insights. Listen actively to responses and ask natural follow-up questions that build on what your guest shares—dig deeper into interesting points, ask for examples, or explore the 'why' behind their answers. Keep the conversation flowing naturally, like a genuine dialogue between friends. Once you've thoroughly explored an aspect (typically after 3-5 exchanges), use the next_question function to smoothly transition to the next key aspect. After covering 3 key aspects of the topic, use the wrap_up function to conclude the interview."
            }
        ],
        functions=[next_question_func, wrap_up_func],
    )

def create_conclusion_node() -> NodeConfig:
    """Create the Conclusion node."""

    async def handle_end_interview(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for end_interview function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_final_node()

    end_interview_func = FlowsFunctionSchema(
        name="end_interview",
        handler=handle_end_interview,
        description="Use this after the guest has shared their final thoughts.",
        properties={},
        required=[]
    )
    return NodeConfig(
        name="conclusion",
        task_messages=[
            {
                "role": "system",
                "content": "Express genuine appreciation for the conversation and the insights your guest shared. Summarize 2-3 key takeaways or memorable points from your discussion in a warm, conversational way—this helps reinforce the value of the conversation. Then, ask your guest if they have any final thoughts, a last word, or anything else they'd like to add. Wait for their response, then use the end_interview function to wrap up."
            }
        ],
        functions=[end_interview_func],
    )

def create_final_node() -> NodeConfig:
    """Create the Final node."""
    return NodeConfig(
        name="final",
        task_messages=[
            {
                "role": "system",
                "content": "Thank the guest one final time for joining you and for sharing their insights. End the conversation on a positive, warm note."
            }
        ],
        post_actions=[
            {"type": "end_conversation"}
        ],
    )

# FlowManager Setup
# 
# Initialize the FlowManager in your bot setup:
#
# async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
#     stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
#     tts = CartesiaTTSService(api_key=os.getenv("CARTESIA_API_KEY"))
#     llm = create_llm()  # Your LLM service
#     
#     context = LLMContext()
#     context_aggregator = LLMContextAggregatorPair(context)
#     
#     pipeline = Pipeline([
#         transport.input(),
#         stt,
#         context_aggregator.user(),
#         llm,
#         tts,
#         transport.output(),
#         context_aggregator.assistant(),
#     ])
#     
#     task = PipelineTask(pipeline, params=PipelineParams(allow_interruptions=True))
#     
#     # Initialize FlowManager
#     flow_manager = FlowManager(
#         task=task,
#         llm=llm,
#         context_aggregator=context_aggregator,
#         transport=transport,
#         # global_functions=[],
#     )
#     
#     @transport.event_handler("on_client_connected")
#     async def on_client_connected(transport, client):
#         logger.info("Client connected")
#         # Start the flow with the initial node
#         await flow_manager.initialize(create_initial_node())