"""Test sub-workflow execution functionality."""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from budflow.executions.subworkflow import (
    SubWorkflowEngine,
    SubWorkflowExecution,
    SubWorkflowNode,
    SubWorkflowNotFoundError,
    SubWorkflowCircularReferenceError,
    SubWorkflowDepthLimitError,
    SubWorkflowParameterError,
    ParameterMapping,
    SubWorkflowResult
)
from budflow.workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode


@pytest.fixture
def subworkflow_engine():
    """Create a SubWorkflowEngine for testing."""
    mock_db_session = AsyncMock()
    return SubWorkflowEngine(mock_db_session)


@pytest.fixture
def parent_workflow():
    """Create a parent workflow with sub-workflow node."""
    return Workflow(
        id=1,
        name="Parent Workflow",
        nodes=[
            {"id": "start", "name": "Start Node", "type": "manual.trigger", "position": [100, 100]},
            {"id": "subworkflow1", "name": "Process Data", "type": "subworkflow", "position": [200, 100], 
             "parameters": {
                 "subworkflow_id": 2,
                 "parameter_mapping": {
                     "input_data": "$.start[0].data",
                     "config": {"retry_count": 3}
                 },
                 "output_mapping": {
                     "result": "$.output[0].processed_user_id",
                     "metadata": "$.output[0].status"
                 }
             }},
            {"id": "end", "name": "End Node", "type": "set", "position": [300, 100]}
        ],
        connections=[
            {"source": "start", "target": "subworkflow1"},
            {"source": "subworkflow1", "target": "end"}
        ]
    )


@pytest.fixture
def child_workflow():
    """Create a child workflow to be executed as sub-workflow."""
    return Workflow(
        id=2,
        name="Child Workflow",
        nodes=[
            {"id": "input", "name": "Input Node", "type": "manual.trigger", "position": [100, 100]},
            {"id": "process", "name": "Process Node", "type": "transform", "position": [200, 100]},
            {"id": "output", "name": "Output Node", "type": "set", "position": [300, 100]}
        ],
        connections=[
            {"source": "input", "target": "process"},
            {"source": "process", "target": "output"}
        ]
    )


@pytest.fixture
def parent_execution():
    """Create a parent execution for testing."""
    return WorkflowExecution(
        id=1,
        workflow_id=1,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        data={"start": [{"data": "test input", "id": 123}]}
    )


class TestSubWorkflowParameterMapping:
    """Test parameter mapping for sub-workflows."""

    async def test_map_input_parameters_simple(self, subworkflow_engine):
        """Test simple parameter mapping from parent to child."""
        parent_data = {
            "start": [{"name": "John", "age": 30, "city": "New York"}],
            "config": [{"timeout": 60, "retries": 3}]
        }
        
        parameter_mapping = {
            "user_name": "$.start[0].name",
            "user_age": "$.start[0].age",
            "timeout_setting": "$.config[0].timeout"
        }
        
        mapped_params = await subworkflow_engine.map_input_parameters(parent_data, parameter_mapping)
        
        assert mapped_params["user_name"] == "John"
        assert mapped_params["user_age"] == 30
        assert mapped_params["timeout_setting"] == 60

    async def test_map_input_parameters_complex_nested(self, subworkflow_engine):
        """Test complex nested parameter mapping."""
        parent_data = {
            "api_response": [{
                "users": [
                    {"id": 1, "profile": {"name": "Alice", "settings": {"theme": "dark"}}},
                    {"id": 2, "profile": {"name": "Bob", "settings": {"theme": "light"}}}
                ],
                "metadata": {"total": 2, "page": 1}
            }]
        }
        
        parameter_mapping = {
            "first_user": "$.api_response[0].users[0]",
            "user_names": "$.api_response[0].users[*].profile.name",
            "total_count": "$.api_response[0].metadata.total",
            "themes": "$.api_response[0].users[*].profile.settings.theme"
        }
        
        mapped_params = await subworkflow_engine.map_input_parameters(parent_data, parameter_mapping)
        
        assert mapped_params["first_user"]["id"] == 1
        assert mapped_params["user_names"] == ["Alice", "Bob"]
        assert mapped_params["total_count"] == 2
        assert mapped_params["themes"] == ["dark", "light"]

    async def test_map_input_parameters_static_values(self, subworkflow_engine):
        """Test parameter mapping with static values."""
        parent_data = {"start": [{"id": 123}]}
        
        parameter_mapping = {
            "dynamic_id": "$.start[0].id",
            "static_config": {"environment": "production", "debug": False},
            "static_list": [1, 2, 3],
            "static_string": "fixed_value"
        }
        
        mapped_params = await subworkflow_engine.map_input_parameters(parent_data, parameter_mapping)
        
        assert mapped_params["dynamic_id"] == 123
        assert mapped_params["static_config"]["environment"] == "production"
        assert mapped_params["static_list"] == [1, 2, 3]
        assert mapped_params["static_string"] == "fixed_value"

    async def test_map_output_parameters(self, subworkflow_engine):
        """Test mapping sub-workflow output back to parent."""
        subworkflow_output = {
            "processed_data": [{"result": "success", "count": 42}],
            "metadata": [{"execution_time": 1.5, "nodes_executed": 3}],
            "logs": [{"level": "info", "message": "Processing complete"}]
        }
        
        output_mapping = {
            "result": "$.processed_data[0].result",
            "item_count": "$.processed_data[0].count",
            "execution_info": "$.metadata[0]",
            "all_logs": "$.logs"
        }
        
        mapped_output = await subworkflow_engine.map_output_parameters(subworkflow_output, output_mapping)
        
        assert mapped_output["result"] == "success"
        assert mapped_output["item_count"] == 42
        assert mapped_output["execution_info"]["execution_time"] == 1.5
        assert len(mapped_output["all_logs"]) == 1

    async def test_parameter_mapping_error_handling(self, subworkflow_engine):
        """Test error handling in parameter mapping."""
        parent_data = {"start": [{"name": "test"}]}
        
        # Invalid JSONPath expression
        invalid_mapping = {
            "result": "$.nonexistent.path"
        }
        
        with pytest.raises(SubWorkflowParameterError) as exc_info:
            await subworkflow_engine.map_input_parameters(parent_data, invalid_mapping)
        
        assert "Parameter mapping failed" in str(exc_info.value)


class TestSubWorkflowValidation:
    """Test sub-workflow validation and safety checks."""

    async def test_validate_subworkflow_exists(self, subworkflow_engine, child_workflow):
        """Test validation that sub-workflow exists."""
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        
        # Should not raise exception
        await subworkflow_engine.validate_subworkflow_exists(2)
        
        subworkflow_engine.workflow_service.get_workflow.assert_called_once_with(2)

    async def test_validate_subworkflow_not_found(self, subworkflow_engine):
        """Test error when sub-workflow doesn't exist."""
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = None
        
        with pytest.raises(SubWorkflowNotFoundError) as exc_info:
            await subworkflow_engine.validate_subworkflow_exists(999)
        
        assert "Sub-workflow 999 not found" in str(exc_info.value)

    async def test_detect_circular_reference_direct(self, subworkflow_engine):
        """Test detection of direct circular reference."""
        # Workflow 1 calls workflow 1 (itself)
        execution_chain = [1]
        
        with pytest.raises(SubWorkflowCircularReferenceError) as exc_info:
            await subworkflow_engine.detect_circular_references(1, execution_chain)
        
        assert "Circular reference detected" in str(exc_info.value)

    async def test_detect_circular_reference_indirect(self, subworkflow_engine):
        """Test detection of indirect circular reference."""
        # Workflow 1 -> 2 -> 3 -> 1 (circular)
        execution_chain = [1, 2, 3]
        
        with pytest.raises(SubWorkflowCircularReferenceError) as exc_info:
            await subworkflow_engine.detect_circular_references(1, execution_chain)
        
        assert "Circular reference detected" in str(exc_info.value)

    async def test_detect_circular_reference_none(self, subworkflow_engine):
        """Test no circular reference in valid chain."""
        # Workflow 1 -> 2 -> 3 -> 4 (no circular)
        execution_chain = [1, 2, 3]
        
        # Should not raise exception
        await subworkflow_engine.detect_circular_references(4, execution_chain)

    async def test_validate_depth_limit_exceeded(self, subworkflow_engine):
        """Test depth limit validation."""
        # Simulate deep nesting beyond limit
        execution_chain = list(range(1, 12))  # 11 levels deep (limit is usually 10)
        
        with pytest.raises(SubWorkflowDepthLimitError) as exc_info:
            await subworkflow_engine.validate_depth_limit(execution_chain)
        
        assert "Sub-workflow depth limit exceeded" in str(exc_info.value)

    async def test_validate_depth_limit_within_bounds(self, subworkflow_engine):
        """Test depth validation within acceptable limits."""
        execution_chain = [1, 2, 3, 4, 5]  # 5 levels deep (within limit)
        
        # Should not raise exception
        await subworkflow_engine.validate_depth_limit(execution_chain)


class TestSubWorkflowExecution:
    """Test sub-workflow execution functionality."""

    async def test_execute_subworkflow_simple(self, subworkflow_engine, parent_workflow, child_workflow, parent_execution):
        """Test simple sub-workflow execution."""
        subworkflow_node_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {
                "input_data": "$.start[0].data"
            },
            "output_mapping": {
                "result": "$.output[0].result"
            }
        }
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock child execution result
        child_execution = WorkflowExecution(
            id=2,
            workflow_id=2,
            mode=ExecutionMode.MANUAL,
            status=ExecutionStatus.SUCCESS,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            data={"output": [{"result": "processed successfully"}]}
        )
        subworkflow_engine.execution_service.execute_workflow.return_value = child_execution
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_node_config,
            execution_chain=[1]
        )
        
        assert result.status == ExecutionStatus.SUCCESS
        assert result.output_data["result"] == "processed successfully"
        assert result.child_execution_id == 2
        assert result.parent_execution_id == 1

    async def test_execute_subworkflow_with_complex_mapping(self, subworkflow_engine, parent_workflow, child_workflow):
        """Test sub-workflow execution with complex parameter mapping."""
        parent_execution = WorkflowExecution(
            id=1,
            workflow_id=1,
            status=ExecutionStatus.RUNNING,
            data={
                "start": [{"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}],
                "config": [{"batch_size": 10, "timeout": 300}]
            }
        )
        
        subworkflow_node_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {
                "user_list": "$.start[0].users",
                "batch_config": "$.config[0]",
                "static_env": "production"
            },
            "output_mapping": {
                "processed_users": "$.result[0].users",
                "summary": "$.result[0].summary"
            }
        }
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock child execution with mapped parameters
        def mock_execute_workflow(workflow_id, input_data, **kwargs):
            # Verify mapped parameters were passed correctly
            assert input_data["user_list"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            assert input_data["batch_config"]["batch_size"] == 10
            assert input_data["static_env"] == "production"
            
            return WorkflowExecution(
                id=3,
                workflow_id=2,
                status=ExecutionStatus.SUCCESS,
                data={"result": [{"users": [{"id": 1, "processed": True}], "summary": {"total": 1}}]}
            )
        
        subworkflow_engine.execution_service.execute_workflow.side_effect = mock_execute_workflow
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_node_config,
            execution_chain=[1]
        )
        
        assert result.status == ExecutionStatus.SUCCESS
        assert result.output_data["processed_users"] == [{"id": 1, "processed": True}]
        assert result.output_data["summary"]["total"] == 1

    async def test_execute_subworkflow_failure_handling(self, subworkflow_engine, child_workflow, parent_execution):
        """Test handling of sub-workflow execution failures."""
        subworkflow_node_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {"input": "$.start[0].data"},
            "output_mapping": {"result": "$.output[0].result"}
        }
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock failed child execution
        failed_execution = WorkflowExecution(
            id=4,
            workflow_id=2,
            status=ExecutionStatus.ERROR,
            error={"message": "Node processing failed", "type": "ProcessingError"}
        )
        subworkflow_engine.execution_service.execute_workflow.return_value = failed_execution
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_node_config,
            execution_chain=[1]
        )
        
        assert result.status == ExecutionStatus.ERROR
        assert result.error_message == "Node processing failed"
        assert result.child_execution_id == 4

    async def test_execute_subworkflow_timeout_handling(self, subworkflow_engine, child_workflow, parent_execution):
        """Test timeout handling for sub-workflow execution."""
        subworkflow_node_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {"input": "$.start[0].data"},
            "timeout": 5  # 5 second timeout
        }
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock long-running execution that would timeout
        async def slow_execution(*args, **kwargs):
            await asyncio.sleep(10)  # Longer than timeout
            return WorkflowExecution(id=5, workflow_id=2, status=ExecutionStatus.SUCCESS)
        
        subworkflow_engine.execution_service.execute_workflow.side_effect = slow_execution
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_node_config,
            execution_chain=[1]
        )
        
        assert result.status == ExecutionStatus.ERROR
        assert "timed out" in result.error_message.lower()


class TestSubWorkflowNodeImplementation:
    """Test the SubWorkflow node implementation."""

    async def test_subworkflow_node_execute(self, subworkflow_engine):
        """Test SubWorkflow node execution."""
        # Create SubWorkflow node
        subworkflow_node = SubWorkflowNode(
            node_id="subworkflow1",
            subworkflow_id=2,
            parameter_mapping={
                "input_data": "$.input[0].data",
                "config": {"environment": "test"}
            },
            output_mapping={
                "result": "$.output[0].result"
            }
        )
        
        input_data = [{"input": [{"data": "test input"}]}]
        
        # Mock the engine
        subworkflow_node.engine = subworkflow_engine
        subworkflow_engine.execute_subworkflow = AsyncMock(return_value=SubWorkflowResult(
            status=ExecutionStatus.SUCCESS,
            output_data={"result": "processed"},
            child_execution_id=10,
            parent_execution_id=1
        ))
        
        result = await subworkflow_node.execute(input_data)
        
        assert len(result) == 1
        assert result[0]["result"] == "processed"
        assert result[0]["_subworkflow_execution_id"] == 10

    async def test_subworkflow_node_configuration_validation(self):
        """Test validation of SubWorkflow node configuration."""
        # Valid configuration
        valid_node = SubWorkflowNode(
            node_id="subworkflow1",
            subworkflow_id=2,
            parameter_mapping={"input": "$.data"},
            output_mapping={"result": "$.output"}
        )
        assert valid_node.subworkflow_id == 2
        
        # Invalid configuration - missing subworkflow_id
        with pytest.raises(SubWorkflowParameterError):
            SubWorkflowNode(
                node_id="subworkflow1",
                subworkflow_id=None,
                parameter_mapping={},
                output_mapping={}
            )


class TestSubWorkflowResourceManagement:
    """Test resource management for sub-workflows."""

    async def test_track_subworkflow_resources(self, subworkflow_engine, parent_execution):
        """Test tracking of sub-workflow resource usage."""
        subworkflow_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {"input": "$.start[0].data"},
            "resource_limits": {
                "max_memory_mb": 512,
                "max_execution_time_seconds": 300,
                "max_concurrent_nodes": 10
            }
        }
        
        # Mock resource tracking
        mock_resource_tracker = AsyncMock()
        subworkflow_engine.resource_tracker = mock_resource_tracker
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = Mock()  # Mock workflow exists
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock successful execution with resource usage
        mock_execution = WorkflowExecution(
            id=5,
            workflow_id=2,
            status=ExecutionStatus.SUCCESS,
            data={"result": [{"processed": True}]}
        )
        subworkflow_engine.execution_service.execute_workflow.return_value = mock_execution
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_config,
            execution_chain=[1]
        )
        
        # Verify resource tracking was called
        mock_resource_tracker.track_execution.assert_called_once()

    async def test_resource_limit_enforcement(self, subworkflow_engine):
        """Test enforcement of resource limits for sub-workflows."""
        # Test memory limit validation
        resource_limits = {
            "max_memory_mb": 1024,
            "max_execution_time_seconds": 600
        }
        
        # Should pass validation
        await subworkflow_engine.validate_resource_limits(resource_limits)
        
        # Test excessive limits
        excessive_limits = {
            "max_memory_mb": 10000,  # 10GB - too high
            "max_execution_time_seconds": 3600  # 1 hour - too long
        }
        
        with pytest.raises(SubWorkflowParameterError) as exc_info:
            await subworkflow_engine.validate_resource_limits(excessive_limits)
        
        assert "Resource limit exceeded" in str(exc_info.value)


class TestSubWorkflowConcurrency:
    """Test concurrent sub-workflow execution."""

    async def test_concurrent_subworkflow_executions(self, subworkflow_engine):
        """Test handling of concurrent sub-workflow executions."""
        # Create multiple parent executions
        parent_executions = []
        for i in range(3):
            parent_executions.append(WorkflowExecution(
                id=i+1,
                workflow_id=1,
                status=ExecutionStatus.RUNNING,
                data={"start": [{"data": f"test_{i}"}]}
            ))
        
        subworkflow_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {"input": "$.start[0].data"},
            "output_mapping": {"result": "$.output[0].result"}
        }
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock different execution results for each
        execution_results = [
            WorkflowExecution(id=10+i, workflow_id=2, status=ExecutionStatus.SUCCESS, 
                            data={"output": [{"result": f"result_{i}"}]})
            for i in range(3)
        ]
        subworkflow_engine.execution_service.execute_workflow.side_effect = execution_results
        
        # Execute all sub-workflows concurrently
        tasks = []
        for parent_exec in parent_executions:
            task = subworkflow_engine.execute_subworkflow(
                parent_execution=parent_exec,
                subworkflow_config=subworkflow_config,
                execution_chain=[parent_exec.workflow_id]
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # Verify all executions completed successfully
        assert len(results) == 3
        for i, result in enumerate(results):
            assert result.status == ExecutionStatus.SUCCESS
            assert result.output_data["result"] == f"result_{i}"

    async def test_concurrent_execution_limits(self, subworkflow_engine):
        """Test limits on concurrent sub-workflow executions."""
        # Mock too many concurrent executions
        subworkflow_engine.max_concurrent_subworkflows = 2
        subworkflow_engine.active_subworkflow_count = 2
        
        parent_execution = WorkflowExecution(id=1, workflow_id=1, status=ExecutionStatus.RUNNING)
        
        with pytest.raises(SubWorkflowParameterError) as exc_info:
            await subworkflow_engine.validate_concurrency_limits()
        
        assert "Maximum concurrent sub-workflows exceeded" in str(exc_info.value)


class TestSubWorkflowIntegration:
    """Test integration with main workflow execution."""

    async def test_subworkflow_in_main_execution_flow(self, subworkflow_engine, parent_workflow, child_workflow):
        """Test sub-workflow execution as part of main workflow."""
        # Create main execution with sub-workflow node
        main_execution = WorkflowExecution(
            id=1,
            workflow_id=1,
            status=ExecutionStatus.RUNNING,
            data={"start": [{"data": {"user_id": 123, "action": "process"}}]}
        )
        
        # Mock execution flow
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.side_effect = [parent_workflow, child_workflow]
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock sub-workflow execution result
        subworkflow_result = WorkflowExecution(
            id=2,
            workflow_id=2,
            status=ExecutionStatus.SUCCESS,
            data={"output": [{"processed_user_id": 123, "status": "completed"}]}
        )
        subworkflow_engine.execution_service.execute_workflow.return_value = subworkflow_result
        
        # Execute the sub-workflow node
        result = await subworkflow_engine.execute_subworkflow_node(
            main_execution=main_execution,
            node_config=parent_workflow.nodes[1]["parameters"],  # subworkflow1 node
            execution_chain=[]
        )
        
        assert result.status == ExecutionStatus.SUCCESS
        assert result.output_data["result"] == 123

    async def test_subworkflow_error_propagation(self, subworkflow_engine, parent_workflow, child_workflow):
        """Test error propagation from sub-workflow to parent."""
        main_execution = WorkflowExecution(
            id=1,
            workflow_id=1,
            status=ExecutionStatus.RUNNING,
            data={"start": [{"invalid": "data"}]}
        )
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock sub-workflow failure
        failed_result = WorkflowExecution(
            id=3,
            workflow_id=2,
            status=ExecutionStatus.ERROR,
            error={"message": "Invalid input data", "type": "ValidationError", "node": "process"}
        )
        subworkflow_engine.execution_service.execute_workflow.return_value = failed_result
        
        subworkflow_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {"input": "$.start[0].invalid"},
            "error_handling": "propagate"  # Propagate errors to parent
        }
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=main_execution,
            subworkflow_config=subworkflow_config,
            execution_chain=[1]
        )
        
        assert result.status == ExecutionStatus.ERROR
        assert "Invalid input data" in result.error_message
        assert result.error_details["type"] == "ValidationError"


class TestSubWorkflowPerformance:
    """Test performance aspects of sub-workflow execution."""

    async def test_subworkflow_execution_timing(self, subworkflow_engine, child_workflow, parent_execution):
        """Test timing measurement for sub-workflow execution."""
        subworkflow_config = {
            "subworkflow_id": 2,
            "parameter_mapping": {"input": "$.start[0].data"},
            "measure_performance": True
        }
        
        # Mock dependencies
        subworkflow_engine.workflow_service = AsyncMock()
        subworkflow_engine.workflow_service.get_workflow.return_value = child_workflow
        subworkflow_engine.execution_service = AsyncMock()
        
        # Mock execution with timing
        async def timed_execution(*args, **kwargs):
            await asyncio.sleep(0.1)  # Simulate execution time
            return WorkflowExecution(
                id=6,
                workflow_id=2,
                status=ExecutionStatus.SUCCESS,
                started_at=datetime.now(timezone.utc) - timedelta(milliseconds=100),
                finished_at=datetime.now(timezone.utc),
                data={"result": [{"processed": True}]}
            )
        
        subworkflow_engine.execution_service.execute_workflow.side_effect = timed_execution
        
        result = await subworkflow_engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_config,
            execution_chain=[1]
        )
        
        assert result.status == ExecutionStatus.SUCCESS
        assert result.execution_time_ms is not None
        assert result.execution_time_ms > 0

    async def test_large_parameter_mapping_performance(self, subworkflow_engine):
        """Test performance with large parameter mappings."""
        # Create large parent data
        large_data = {
            "items": [{"id": i, "data": f"item_{i}" * 100} for i in range(1000)]
        }
        
        parameter_mapping = {
            "all_items": "$.items",
            "first_100": "$.items[:100]",
            "item_ids": "$.items[*].id"
        }
        
        start_time = datetime.now()
        mapped_params = await subworkflow_engine.map_input_parameters(large_data, parameter_mapping)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete within reasonable time
        assert execution_time < 1.0
        assert len(mapped_params["all_items"]) == 1000
        assert len(mapped_params["first_100"]) == 100
        assert len(mapped_params["item_ids"]) == 1000