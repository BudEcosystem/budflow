"""Node runner for executing individual nodes."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

import structlog

from budflow.workflows.models import NodeType, NodeExecutionStatus
from .context import NodeExecutionContext
from .errors import NodeExecutionError, DataValidationError, ExecutionTimeoutError

logger = structlog.get_logger()


class BaseNode:
    """Base class for all node types."""
    
    def __init__(self, context: NodeExecutionContext):
        self.context = context
        self.logger = logger.bind(
            node_id=context.node.id,
            node_name=context.node.name,
            node_type=context.node.type.value
        )
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute the node. Must be implemented by subclasses."""
        raise NotImplementedError("Node execution not implemented")
    
    def validate_parameters(self) -> None:
        """Validate node parameters. Can be overridden by subclasses."""
        pass
    
    def get_parameter(self, key: str, default: Any = None, required: bool = False) -> Any:
        """Get a parameter value with optional validation."""
        value = self.context.get_parameter(key, default)
        if required and value is None:
            raise DataValidationError(
                f"Required parameter '{key}' is missing",
                field=key
            )
        return value


class SetNode(BaseNode):
    """Node for setting values in the execution context."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute set node."""
        values = self.get_parameter("values", {})
        keep_only_set = self.get_parameter("keepOnlySet", False)
        
        output_items = []
        
        # Process each input item
        for item in self.context.input_data:
            if keep_only_set:
                # Only output the set values
                output_item = values.copy()
            else:
                # Merge with input data
                output_item = item.copy()
                output_item.update(values)
            
            output_items.append(output_item)
        
        # If no input items, create one output item
        if not output_items:
            output_items.append(values.copy())
        
        return output_items


class FunctionNode(BaseNode):
    """Node for executing custom functions."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute function node."""
        # For now, just pass through the input data
        # In the future, this will execute custom JavaScript/Python functions
        self.logger.warning("Function node execution not fully implemented, passing through data")
        return self.context.input_data


class MergeNode(BaseNode):
    """Node for merging multiple inputs."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute merge node."""
        mode = self.get_parameter("mode", "append")
        
        if mode == "append":
            # Simply return all input items
            return self.context.input_data
        
        elif mode == "merge":
            # Merge all items into one
            if not self.context.input_data:
                return []
            
            merged_item = {}
            for item in self.context.input_data:
                merged_item.update(item)
            
            return [merged_item]
        
        elif mode == "multiplex":
            # Create cartesian product of inputs
            # For simplicity, just append for now
            self.logger.warning("Multiplex mode not fully implemented, using append")
            return self.context.input_data
        
        else:
            raise DataValidationError(
                f"Invalid merge mode: {mode}",
                field="mode",
                expected_type="append|merge|multiplex"
            )


class NodeRunner:
    """Runs individual nodes within their execution context."""
    
    # Registry of node type implementations
    NODE_TYPES: Dict[NodeType, Type[BaseNode]] = {
        NodeType.SET: SetNode,
        NodeType.FUNCTION: FunctionNode,
        NodeType.MERGE: MergeNode,
    }
    
    def __init__(self):
        self.logger = logger.bind(component="node_runner")
    
    async def run_node(
        self,
        context: NodeExecutionContext,
        timeout_seconds: Optional[int] = None
    ) -> NodeExecutionContext:
        """Run a single node with its context."""
        node = context.node
        node_logger = self.logger.bind(
            node_id=node.id,
            node_name=node.name,
            node_type=node.type.value,
            try_number=context.current_try
        )
        
        # Mark execution as started
        context.started_at = datetime.now(timezone.utc)
        context.status = NodeExecutionStatus.RUNNING
        context.node_execution.status = NodeExecutionStatus.RUNNING
        context.node_execution.started_at = context.started_at
        
        try:
            node_logger.info("Starting node execution")
            
            # Use node factory to create node instance
            from budflow.nodes import NodeFactory
            factory = NodeFactory()
            node_instance = factory.create_node(context)
            
            if not node_instance:
                # Get node implementation from legacy registry
                node_class = self.NODE_TYPES.get(node.type)
                if not node_class:
                    # For unimplemented node types, pass through data
                    node_logger.warning(f"Node type {node.type.value} not implemented, passing through data")
                    context.output_data = context.input_data
                else:
                    # Create and execute node
                    node_instance = node_class(context)
            
            if node_instance:
                # Execute with timeout if specified
                if timeout_seconds:
                    try:
                        context.output_data = await asyncio.wait_for(
                            node_instance.run(),
                            timeout=timeout_seconds
                        )
                    except asyncio.TimeoutError:
                        raise ExecutionTimeoutError(
                            f"Node execution timed out after {timeout_seconds} seconds",
                            timeout_seconds=timeout_seconds
                        )
                else:
                    context.output_data = await node_instance.run()
            
            # Mark as successful
            context.status = NodeExecutionStatus.SUCCESS
            context.error = None
            
            # Store output in execution data
            context.execution_data.set_node_output(
                str(node.id),
                context.output_data,
                "main"
            )
            
            node_logger.info(
                "Node execution completed",
                output_items=len(context.output_data)
            )
            
        except Exception as e:
            # Mark as failed
            context.status = NodeExecutionStatus.ERROR
            context.error = e
            
            node_logger.error(
                "Node execution failed",
                error=str(e),
                error_type=type(e).__name__
            )
            
            # Check if should retry
            if context.should_retry:
                context.current_try += 1
                node_logger.info(
                    "Will retry node execution",
                    next_try=context.current_try,
                    max_tries=context.max_tries
                )
                
                # Wait before retry
                if node.wait_between_tries > 0:
                    await asyncio.sleep(node.wait_between_tries / 1000.0)
                
                # Reset and retry
                context.status = NodeExecutionStatus.NEW
                context.error = None
                return await self.run_node(context, timeout_seconds)
            
            # No more retries, propagate error
            if not isinstance(e, NodeExecutionError):
                # Wrap in NodeExecutionError
                raise NodeExecutionError(
                    f"Node execution failed: {str(e)}",
                    node_id=node.id,
                    node_name=node.name,
                    node_type=node.type.value,
                    details={"original_error": str(e)}
                )
            else:
                raise
        
        finally:
            # Mark execution as finished
            context.finished_at = datetime.now(timezone.utc)
            
            # Update node execution record
            context.node_execution.status = context.status
            context.node_execution.finished_at = context.finished_at
            context.node_execution.duration_ms = context.execution_time_ms
            context.node_execution.try_number = context.current_try
            
            if context.status == NodeExecutionStatus.SUCCESS:
                context.node_execution.output_data = {
                    "items": context.output_data,
                    "count": len(context.output_data)
                }
            elif context.error:
                context.node_execution.error = {
                    "message": str(context.error),
                    "type": type(context.error).__name__,
                    "try_number": context.current_try
                }
        
        return context
    
    def register_node_type(self, node_type: NodeType, node_class: Type[BaseNode]) -> None:
        """Register a custom node type implementation."""
        self.NODE_TYPES[node_type] = node_class
    
    def get_supported_node_types(self) -> List[NodeType]:
        """Get list of supported node types."""
        return list(self.NODE_TYPES.keys())