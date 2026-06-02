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
class SelectPizzaOrderResult(FlowResult):
    """Result type for select_pizza_order function"""
    size: str
    type: str

class SelectSushiOrderResult(FlowResult):
    """Result type for select_sushi_order function"""
    count: int
    type: str




# Node creation functions
def create_initial_node() -> NodeConfig:
    """Create the Initial node."""

    async def handle_choose_pizza(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for choose_pizza function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_pizza_task_node()


    async def handle_choose_sushi(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for choose_sushi function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_sushi_task_node()

    choose_pizza_func = FlowsFunctionSchema(
        name="choose_pizza",
        handler=handle_choose_pizza,
        description="User wants to order pizza",
        properties={},
        required=[]
    )
    choose_sushi_func = FlowsFunctionSchema(
        name="choose_sushi",
        handler=handle_choose_sushi,
        description="User wants to order sushi",
        properties={},
        required=[]
    )
    return NodeConfig(
        name="initial",
        role_messages=[
            {
                "role": "system",
                "content": "You are a helpful food ordering assistant. You must ALWAYS use the available functions to progress the conversation."
            }
        ],
        task_messages=[
            {
                "role": "system",
                "content": "Welcome! Greet the user warmly and ask if they would like to order pizza or sushi today."
            }
        ],
        functions=[choose_pizza_func, choose_sushi_func],
    )

def create_pizza_task_node() -> NodeConfig:
    """Create the Pizza Task node."""

    async def handle_select_pizza_order(args: FlowArgs, flow_manager: FlowManager) -> tuple[SelectPizzaOrderResult | None, NodeConfig]:
        """Handler for select_pizza_order function"""
        size: str = args.get("size", "")
        type: str = args.get("type", "")
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return SelectPizzaOrderResult(size=size, type=type), create_confirm_node()

    select_pizza_order_func = FlowsFunctionSchema(
        name="select_pizza_order",
        handler=handle_select_pizza_order,
        description="Record pizza order details",
        properties={
            "size": {
                "type": "string",
                "description": "Pizza size",
                "enum": ["small","medium","large"]
            },
            "type": {
                "type": "string",
                "description": "Pizza type",
                "enum": ["pepperoni","cheese","supreme","vegetarian"]
            }
        },
        required=["size","type"]
    )
    return NodeConfig(
        name="pizza_task",
        task_messages=[
            {
                "role": "system",
                "content": "Ask the user what size and type of pizza they want. Use the select_pizza_order function when they provide both size AND type. Pricing: Small $10, Medium $15, Large $20."
            }
        ],
        functions=[select_pizza_order_func],
    )

def create_sushi_task_node() -> NodeConfig:
    """Create the Sushi Task node."""

    async def handle_select_sushi_order(args: FlowArgs, flow_manager: FlowManager) -> tuple[SelectSushiOrderResult | None, NodeConfig]:
        """Handler for select_sushi_order function"""
        count: int = args.get("count", 0)
        type: str = args.get("type", "")
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return SelectSushiOrderResult(count=count, type=type), create_confirm_node()

    select_sushi_order_func = FlowsFunctionSchema(
        name="select_sushi_order",
        handler=handle_select_sushi_order,
        description="Record sushi order details",
        properties={
            "count": {
                "type": "integer",
                "description": "Number of rolls",
                "minimum": 1,
                "maximum": 10
            },
            "type": {
                "type": "string",
                "description": "Sushi type",
                "enum": ["california","spicy tuna","rainbow","dragon"]
            }
        },
        required=["count","type"]
    )
    return NodeConfig(
        name="sushi_task",
        task_messages=[
            {
                "role": "system",
                "content": "Ask the user how many rolls and what type of sushi they want. Use the select_sushi_order function when they provide both count AND type. Pricing: $8 per roll."
            }
        ],
        functions=[select_sushi_order_func],
    )

def create_confirm_node() -> NodeConfig:
    """Create the Confirm node."""

    async def handle_complete_order(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for complete_order function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_end_node()


    async def handle_revise_order(args: FlowArgs, flow_manager: FlowManager) -> tuple[None, NodeConfig]:
        """Handler for revise_order function"""
        # TODO: Implement function logic
        # Update flow_manager.state as needed
        return None, create_initial_node()

    complete_order_func = FlowsFunctionSchema(
        name="complete_order",
        handler=handle_complete_order,
        description="User confirms the order is correct",
        properties={},
        required=[]
    )
    revise_order_func = FlowsFunctionSchema(
        name="revise_order",
        handler=handle_revise_order,
        description="User wants to change the order",
        properties={},
        required=[]
    )
    return NodeConfig(
        name="confirm",
        task_messages=[
            {
                "role": "system",
                "content": "Read back the order details to the user. Use complete_order if they confirm it's correct, or revise_order if they want to make changes (which will bring them back to choose pizza or sushi again)."
            }
        ],
        functions=[complete_order_func, revise_order_func],
    )

def create_end_node() -> NodeConfig:
    """Create the End node."""
    return NodeConfig(
        name="end",
        task_messages=[
            {
                "role": "system",
                "content": "Thank the user for their order and end the conversation politely."
            }
        ],
        post_actions=[
            {"type": "end_conversation"}
        ],
    )