"""Control flow node implementations."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from budflow.workflows.models import NodeType
from .base import ControlNode, BaseNode, NodeDefinition, NodeCategory, NodeParameter, ParameterType


class StopWorkflowException(Exception):
    """Exception to stop workflow execution."""
    
    def __init__(self, success: bool = True, message: str = "", output_data: List[Dict[str, Any]] = None):
        self.success = success
        self.message = message
        self.output_data = output_data or []
        super().__init__(message)


class IfNode(ControlNode):
    """IF conditional node."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute IF condition."""
        condition = self.get_parameter("condition", "true")
        output_true = self.get_parameter("output_true", "true")
        output_false = self.get_parameter("output_false", "false")
        
        results = []
        
        for item in self.context.input_data or [{}]:
            # Evaluate condition
            try:
                result = self.evaluate_expression(condition, item)
                
                # Determine output port
                output_port = output_true if result else output_false
                
                # Add output port to data
                output_item = item.copy()
                output_item["_output"] = output_port
                
                results.append(output_item)
                
            except Exception as e:
                self.logger.error("Failed to evaluate condition", error=str(e))
                # Default to false branch on error
                output_item = item.copy()
                output_item["_output"] = output_false
                output_item["_error"] = str(e)
                results.append(output_item)
        
        return results
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="IF",
            type=NodeType.CONDITION,
            category=NodeCategory.FLOW,
            description="Branch execution based on condition",
            icon="git-branch",
            color="#F59F00",
            parameters=[
                NodeParameter(
                    name="condition",
                    type=ParameterType.EXPRESSION,
                    required=True,
                    description="Condition to evaluate",
                    placeholder="{{value > 10}}"
                ),
                NodeParameter(
                    name="output_true",
                    type=ParameterType.STRING,
                    default="true",
                    description="Output port name for true condition"
                ),
                NodeParameter(
                    name="output_false",
                    type=ParameterType.STRING,
                    default="false",
                    description="Output port name for false condition"
                ),
            ],
            inputs=["main"],
            outputs=["true", "false"]
        )


class LoopNode(ControlNode):
    """LOOP node for iterating over data."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute loop."""
        mode = self.get_parameter("mode", "items")
        items_expr = self.get_parameter("items", "{{$input}}")
        batch_size = self.get_parameter("batch_size", 1)
        
        results = []
        
        if mode == "items":
            # Loop over items in array
            for item in self.context.input_data or [{}]:
                # Get items to loop over
                try:
                    items = self.evaluate_expression(items_expr, item)
                    
                    if not isinstance(items, list):
                        items = [items]
                    
                    # Process in batches
                    for i in range(0, len(items), batch_size):
                        batch = items[i:i + batch_size]
                        
                        output_item = item.copy()
                        output_item["_batch"] = batch
                        output_item["_batch_index"] = i // batch_size
                        output_item["_batch_size"] = len(batch)
                        
                        results.append(output_item)
                        
                except Exception as e:
                    self.logger.error("Failed to process loop items", error=str(e))
                    output_item = item.copy()
                    output_item["_error"] = str(e)
                    results.append(output_item)
        
        elif mode == "count":
            # Loop a fixed number of times
            count = self.get_parameter("count", 1)
            
            for item in self.context.input_data or [{}]:
                for i in range(count):
                    output_item = item.copy()
                    output_item["_iteration"] = i
                    output_item["_total_iterations"] = count
                    results.append(output_item)
        
        return results
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Loop",
            type=NodeType.LOOP,
            category=NodeCategory.FLOW,
            description="Loop over items or repeat execution",
            icon="repeat",
            color="#7950F2",
            parameters=[
                NodeParameter(
                    name="mode",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="items",
                    options=["items", "count"],
                    description="Loop mode"
                ),
                NodeParameter(
                    name="items",
                    type=ParameterType.EXPRESSION,
                    description="Items to loop over (for items mode)",
                    placeholder="{{items}}"
                ),
                NodeParameter(
                    name="count",
                    type=ParameterType.NUMBER,
                    default=1,
                    min_value=1,
                    max_value=1000,
                    description="Number of iterations (for count mode)"
                ),
                NodeParameter(
                    name="batch_size",
                    type=ParameterType.NUMBER,
                    default=1,
                    min_value=1,
                    max_value=100,
                    description="Batch size for items mode"
                ),
            ],
            inputs=["main"],
            outputs=["main", "done"]
        )


class WaitNode(BaseNode):
    """WAIT node to pause execution."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute wait."""
        duration = self.get_parameter("duration", 1000)
        unit = self.get_parameter("unit", "milliseconds")
        
        # Convert to seconds
        if unit == "milliseconds":
            wait_seconds = duration / 1000.0
        elif unit == "seconds":
            wait_seconds = float(duration)
        elif unit == "minutes":
            wait_seconds = duration * 60.0
        elif unit == "hours":
            wait_seconds = duration * 3600.0
        else:
            wait_seconds = 1.0
        
        # Wait
        await asyncio.sleep(wait_seconds)
        
        # Pass through input data
        return self.context.input_data or []
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Wait",
            type=NodeType.ACTION,
            category=NodeCategory.FLOW,
            description="Pause execution for specified duration",
            icon="clock",
            color="#868E96",
            parameters=[
                NodeParameter(
                    name="duration",
                    type=ParameterType.NUMBER,
                    required=True,
                    default=1000,
                    min_value=1,
                    description="Wait duration"
                ),
                NodeParameter(
                    name="unit",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="milliseconds",
                    options=["milliseconds", "seconds", "minutes", "hours"],
                    description="Time unit"
                ),
            ],
            inputs=["main"],
            outputs=["main"]
        )


class StopNode(BaseNode):
    """STOP node to end workflow execution."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute stop."""
        success = self.get_parameter("success", True)
        message = self.get_parameter("message", "Workflow stopped")
        
        # Evaluate message with data
        if self.context.input_data:
            message = self.evaluate_expression(message, self.context.input_data[0])
        
        # Raise stop exception
        raise StopWorkflowException(
            success=success,
            message=message,
            output_data=self.context.input_data or []
        )
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Stop",
            type=NodeType.ACTION,
            category=NodeCategory.FLOW,
            description="Stop workflow execution",
            icon="stop-circle",
            color="#FA5252",
            parameters=[
                NodeParameter(
                    name="success",
                    type=ParameterType.BOOLEAN,
                    default=True,
                    description="Mark execution as successful"
                ),
                NodeParameter(
                    name="message",
                    type=ParameterType.STRING,
                    description="Stop message",
                    placeholder="Workflow completed"
                ),
            ],
            inputs=["main"],
            outputs=[]
        )


# Additional control nodes can be added here:
# - SwitchNode (multi-branch based on value)
# - TryCatchNode (error handling)
# - ParallelNode (parallel execution)
# - etc.