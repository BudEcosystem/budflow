"""Base node classes and definitions."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import structlog
from pydantic import BaseModel, Field, field_validator

from budflow.executor.context import NodeExecutionContext
from budflow.workflows.models import NodeType

logger = structlog.get_logger()


class NodeCategory(str, Enum):
    """Node category enumeration."""
    CORE = "core"
    TRIGGER = "trigger"
    ACTION = "action"
    TRANSFORM = "transform"
    FLOW = "flow"
    NETWORK = "network"
    DATABASE = "database"
    COMMUNICATION = "communication"
    UTILITY = "utility"
    CUSTOM = "custom"


class ParameterType(str, Enum):
    """Parameter type enumeration."""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    JSON = "json"
    OPTIONS = "options"
    ARRAY = "array"
    DATE = "date"
    FILE = "file"
    CREDENTIAL = "credential"
    EXPRESSION = "expression"


class NodeParameter(BaseModel):
    """Node parameter definition."""
    name: str = Field(..., description="Parameter name")
    display_name: Optional[str] = Field(None, description="Parameter display name")
    type: ParameterType = Field(..., description="Parameter type")
    required: bool = Field(default=False, description="Is parameter required")
    default: Any = Field(default=None, description="Default value")
    description: Optional[str] = Field(None, description="Parameter description")
    placeholder: Optional[str] = Field(None, description="Placeholder text")
    
    # Type-specific attributes
    options: Optional[List[str]] = Field(None, description="Options for OPTIONS type")
    min_value: Optional[Union[int, float]] = Field(None, description="Minimum value for NUMBER type")
    max_value: Optional[Union[int, float]] = Field(None, description="Maximum value for NUMBER type")
    multiline: bool = Field(default=False, description="Multiline for STRING type")
    
    def validate(self, value: Any) -> bool:
        """Validate parameter value."""
        if value is None:
            return not self.required
        
        if self.type == ParameterType.STRING:
            return isinstance(value, str)
        
        elif self.type == ParameterType.NUMBER:
            if not isinstance(value, (int, float)):
                return False
            if self.min_value is not None and value < self.min_value:
                return False
            if self.max_value is not None and value > self.max_value:
                return False
            return True
        
        elif self.type == ParameterType.BOOLEAN:
            return isinstance(value, bool)
        
        elif self.type == ParameterType.JSON:
            return isinstance(value, (dict, list))
        
        elif self.type == ParameterType.OPTIONS:
            return value in (self.options or [])
        
        elif self.type == ParameterType.ARRAY:
            return isinstance(value, list)
        
        elif self.type == ParameterType.DATE:
            return isinstance(value, (str, datetime))
        
        elif self.type == ParameterType.EXPRESSION:
            return isinstance(value, str)
        
        return True


class NodeDefinition(BaseModel):
    """Node type definition."""
    name: str = Field(..., description="Node display name")
    type: NodeType = Field(..., description="Node type")
    category: NodeCategory = Field(..., description="Node category")
    description: str = Field(..., description="Node description")
    icon: Optional[str] = Field(None, description="Node icon")
    color: Optional[str] = Field(None, description="Node color")
    
    parameters: List[NodeParameter] = Field(default_factory=list, description="Node parameters")
    inputs: List[str] = Field(default_factory=lambda: ["main"], description="Input ports")
    outputs: List[str] = Field(default_factory=lambda: ["main"], description="Output ports")
    
    version: str = Field(default="1.0", description="Node version")
    deprecated: bool = Field(default=False, description="Is node deprecated")
    
    def get_parameter(self, name: str) -> Optional[NodeParameter]:
        """Get parameter by name."""
        for param in self.parameters:
            if param.name == name:
                return param
        return None


class BaseNode(ABC):
    """Base class for all nodes."""
    
    def __init__(self, context: NodeExecutionContext):
        self.context = context
        self.logger = logger.bind(
            node_id=context.node.id,
            node_name=context.node.name,
            node_type=context.node.type.value
        )
        self._expression_evaluator = None
    
    @property
    def name(self) -> str:
        """Get node name."""
        return self.context.node.name
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """Get node parameters."""
        return self.context.parameters
    
    def get_parameter(self, key: str, default: Any = None) -> Any:
        """Get parameter value."""
        return self.context.get_parameter(key, default)
    
    @property
    def expression_evaluator(self):
        """Get expression evaluator (lazy loading)."""
        if self._expression_evaluator is None:
            from .expression import ExpressionEvaluator
            self._expression_evaluator = ExpressionEvaluator()
        return self._expression_evaluator
    
    def evaluate_expression(
        self,
        expression: str,
        data: Optional[Dict[str, Any]] = None,
        use_context: bool = False
    ) -> Any:
        """Evaluate an expression."""
        if data is None:
            # Use first input item if available
            data = self.context.input_data[0] if self.context.input_data else {}
        
        # Add context data if requested
        if use_context:
            eval_data = {
                **data,
                **self.context.execution_data.context_data
            }
        else:
            eval_data = data
        
        return self.expression_evaluator.evaluate(expression, eval_data)
    
    async def run(self) -> List[Dict[str, Any]]:
        """Run the node with lifecycle hooks."""
        try:
            # Pre-execution hook
            await self.pre_execute()
            
            # Execute node
            result = await self.execute()
            
            # Post-execution hook
            await self.post_execute(result)
            
            return result
            
        except Exception as e:
            # Error hook
            await self.on_error(e)
            raise
    
    async def pre_execute(self) -> None:
        """Hook called before execution."""
        pass
    
    @abstractmethod
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute the node. Must be implemented by subclasses."""
        raise NotImplementedError("Node execution not implemented")
    
    async def post_execute(self, result: List[Dict[str, Any]]) -> None:
        """Hook called after successful execution."""
        pass
    
    async def on_error(self, error: Exception) -> None:
        """Hook called on execution error."""
        pass
    
    @classmethod
    @abstractmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        raise NotImplementedError("Node definition not implemented")


class TriggerNode(BaseNode):
    """Base class for trigger nodes."""
    
    def __init__(self, context: NodeExecutionContext):
        super().__init__(context)
        # Allow specific trigger types as well as generic TRIGGER
        valid_trigger_types = {NodeType.TRIGGER, NodeType.MANUAL, NodeType.WEBHOOK, NodeType.SCHEDULE}
        if context.node.type not in valid_trigger_types:
            raise ValueError(f"Invalid node type for trigger: {context.node.type}")
    
    async def is_triggered(self) -> bool:
        """Check if trigger conditions are met."""
        return True
    
    async def get_trigger_data(self) -> Dict[str, Any]:
        """Get data from trigger source."""
        return {}


class ActionNode(BaseNode):
    """Base class for action nodes."""
    
    def __init__(self, context: NodeExecutionContext):
        super().__init__(context)
        if context.node.type != NodeType.ACTION:
            raise ValueError(f"Invalid node type for action: {context.node.type}")


class ControlNode(BaseNode):
    """Base class for control flow nodes."""
    
    def __init__(self, context: NodeExecutionContext):
        super().__init__(context)
        # Allow various control flow types
        valid_control_types = {NodeType.CONDITION, NodeType.LOOP}
        if context.node.type not in valid_control_types:
            raise ValueError(f"Invalid node type for control: {context.node.type}")
    
    def get_output_port(self, condition: Any) -> str:
        """Determine output port based on condition."""
        return "main"