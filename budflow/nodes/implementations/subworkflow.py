"""SubWorkflow node implementation for workflow composition."""

import asyncio
from typing import Any, Dict, List, Optional
import json
from jsonpath_ng import parse as jsonpath_parse

import structlog

from budflow.workflows.models import NodeType, ExecutionStatus
from budflow.nodes.base import BaseNode, NodeDefinition, NodeCategory, NodeParameter, ParameterType
from budflow.executor.errors import CircularWorkflowError, WorkflowValidationError


logger = structlog.get_logger()


class SubWorkflowNode(BaseNode):
    """Node that executes another workflow as a sub-workflow."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute the sub-workflow."""
        workflow_id = self.get_parameter("workflowId")
        if not workflow_id:
            raise ValueError("workflowId parameter is required")
        
        input_mapping = self.get_parameter("inputMapping", {})
        output_mapping = self.get_parameter("outputMapping", {})
        wait_for_completion = self.get_parameter("waitForCompletion", True)
        max_depth = self.get_parameter("maxDepth", 5)
        error_handling = self.get_parameter("errorHandling", "fail")
        
        # Check maximum depth
        current_depth = getattr(self.context.workflow_execution, 'execution_depth', 0)
        if current_depth >= max_depth:
            raise WorkflowValidationError(
                f"Maximum workflow nesting depth ({max_depth}) exceeded"
            )
        
        # Check for circular dependencies
        execution_stack = getattr(self.context.workflow_execution, 'execution_stack', [])
        if int(workflow_id) in execution_stack:
            raise CircularWorkflowError(
                f"Circular workflow dependency detected: {' -> '.join(map(str, execution_stack + [workflow_id]))}"
            )
        
        # Map input data
        mapped_input = []
        for item in self.context.input_data:
            mapped_item = self._map_input_data(item, input_mapping)
            mapped_input.append(mapped_item)
        
        try:
            if wait_for_completion:
                # Execute sub-workflow and wait for completion
                sub_execution = await self._execute_subworkflow(
                    workflow_id=workflow_id,
                    input_data=mapped_input,
                    parent_execution=self.context.workflow_execution,
                    execution_depth=current_depth + 1,
                    execution_stack=execution_stack + [self.context.workflow_execution.workflow_id]
                )
                
                # Map output data
                if sub_execution.status == ExecutionStatus.SUCCESS:
                    output_data = sub_execution.output_data or {}
                    mapped_output = self._map_output_data(output_data, output_mapping)
                    
                    # Ensure output is a list
                    if isinstance(mapped_output, dict):
                        return [mapped_output]
                    elif isinstance(mapped_output, list):
                        return mapped_output
                    else:
                        return [{"result": mapped_output}]
                else:
                    if error_handling == "continue":
                        return [{"error": f"Sub-workflow failed: {sub_execution.error}"}]
                    else:
                        raise RuntimeError(f"Sub-workflow execution failed: {sub_execution.error}")
            else:
                # Start sub-workflow asynchronously
                execution_id = await self._start_subworkflow_async(
                    workflow_id=workflow_id,
                    input_data=mapped_input,
                    parent_execution=self.context.workflow_execution,
                    execution_depth=current_depth + 1
                )
                
                return [{"executionId": execution_id, "status": "started"}]
        
        except Exception as e:
            self.logger.error(
                "Failed to execute sub-workflow",
                workflow_id=workflow_id,
                error=str(e)
            )
            
            if error_handling == "continue":
                return [{"error": str(e)}]
            else:
                raise
    
    def _map_input_data(self, data: Dict[str, Any], mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Map input data using JSONPath or static values."""
        if not mapping:
            return data
        
        result = {}
        
        for target_key, source in mapping.items():
            if isinstance(source, str) and source.startswith("$"):
                # JSONPath expression
                try:
                    expr = jsonpath_parse(source)
                    matches = [match.value for match in expr.find(data)]
                    if matches:
                        result[target_key] = matches[0] if len(matches) == 1 else matches
                except Exception as e:
                    self.logger.warning(f"Failed to evaluate JSONPath {source}: {e}")
                    result[target_key] = None
            elif isinstance(source, dict):
                # Nested mapping
                result[target_key] = self._map_input_data(data, source)
            else:
                # Static value
                result[target_key] = source
        
        return result
    
    def _map_output_data(self, data: Any, mapping: Dict[str, Any]) -> Any:
        """Map output data using JSONPath or static values."""
        if not mapping:
            return data
        
        # If data is not a dict, wrap it
        if not isinstance(data, dict):
            data = {"value": data}
        
        result = {}
        
        for target_key, source in mapping.items():
            if isinstance(source, str) and source.startswith("$"):
                # JSONPath expression
                try:
                    # Special handling for array length
                    if source.endswith(".length"):
                        base_path = source[:-7]  # Remove .length
                        expr = jsonpath_parse(base_path)
                        matches = [match.value for match in expr.find(data)]
                        if matches and isinstance(matches[0], list):
                            result[target_key] = len(matches[0])
                        else:
                            result[target_key] = 0
                    else:
                        expr = jsonpath_parse(source)
                        matches = [match.value for match in expr.find(data)]
                        if matches:
                            result[target_key] = matches[0] if len(matches) == 1 else matches
                except Exception as e:
                    self.logger.warning(f"Failed to evaluate JSONPath {source}: {e}")
                    result[target_key] = None
            else:
                # Static value
                result[target_key] = source
        
        return result
    
    async def _execute_subworkflow(
        self,
        workflow_id: str,
        input_data: List[Dict[str, Any]],
        parent_execution: Any,
        execution_depth: int,
        execution_stack: List[int]
    ) -> Any:
        """Execute a sub-workflow and wait for completion."""
        # Get the execution engine from context
        from budflow.executor.engine import WorkflowExecutionEngine
        from budflow.workflows.models import ExecutionMode
        
        # Create a new engine instance with the same DB session
        engine = WorkflowExecutionEngine(self.context.execution_context.db_session)
        
        # Execute the sub-workflow
        sub_execution = await engine.execute_workflow(
            workflow_id=int(workflow_id),
            mode=ExecutionMode.MANUAL,
            input_data={"items": input_data},
            user_id=parent_execution.user_id if parent_execution else None,
            parent_execution=parent_execution,
            execution_depth=execution_depth,
            execution_stack=execution_stack
        )
        
        # Wait for completion
        max_wait_time = 300  # 5 minutes max
        check_interval = 1  # Check every second
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            # Refresh execution status
            await self.context.execution_context.db_session.refresh(sub_execution)
            
            if sub_execution.is_finished:
                break
                
            await asyncio.sleep(check_interval)
            elapsed_time += check_interval
        
        if not sub_execution.is_finished:
            raise TimeoutError(f"Sub-workflow {workflow_id} did not complete within {max_wait_time} seconds")
        
        # Get the output data from the last node execution
        output_data = sub_execution.data or {}
        if sub_execution.node_executions:
            last_node = sorted(sub_execution.node_executions, key=lambda x: x.finished_at or x.started_at)[-1]
            if last_node.output_data:
                output_data = last_node.output_data
        
        return sub_execution
    
    async def _start_subworkflow_async(
        self,
        workflow_id: str,
        input_data: List[Dict[str, Any]],
        parent_execution: Any,
        execution_depth: int
    ) -> str:
        """Start a sub-workflow asynchronously."""
        # This will be implemented by the execution engine
        # For now, return a mock execution ID
        return f"async-exec-{workflow_id}"
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Sub-Workflow",
            type=NodeType.SUBWORKFLOW,
            category=NodeCategory.FLOW,
            description="Execute another workflow as part of this workflow",
            icon="workflow",
            color="#9B59B6",
            parameters=[
                NodeParameter(
                    name="workflowId",
                    display_name="Workflow",
                    type=ParameterType.STRING,
                    required=True,
                    description="ID of the workflow to execute",
                    placeholder="Select or enter workflow ID"
                ),
                NodeParameter(
                    name="inputMapping",
                    display_name="Input Mapping",
                    type=ParameterType.JSON,
                    required=False,
                    default={},
                    description="Map current data to sub-workflow inputs using JSONPath",
                    placeholder='{"userId": "$.user.id", "items": "$.data"}'
                ),
                NodeParameter(
                    name="outputMapping",
                    display_name="Output Mapping",
                    type=ParameterType.JSON,
                    required=False,
                    default={},
                    description="Map sub-workflow outputs to current data using JSONPath",
                    placeholder='{"results": "$.output", "count": "$.items.length"}'
                ),
                NodeParameter(
                    name="waitForCompletion",
                    display_name="Wait for Completion",
                    type=ParameterType.BOOLEAN,
                    required=False,
                    default=True,
                    description="Wait for sub-workflow to complete before continuing"
                ),
                NodeParameter(
                    name="maxDepth",
                    display_name="Max Recursion Depth",
                    type=ParameterType.NUMBER,
                    required=False,
                    default=5,
                    min_value=1,
                    max_value=20,
                    description="Maximum depth for nested workflows to prevent infinite recursion"
                ),
                NodeParameter(
                    name="errorHandling",
                    display_name="Error Handling",
                    type=ParameterType.OPTIONS,
                    required=False,
                    default="fail",
                    options=["fail", "continue"],
                    description="How to handle sub-workflow errors"
                )
            ],
            inputs=["main"],
            outputs=["main"]
        )