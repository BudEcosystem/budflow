"""Test partial execution functionality."""

import pytest
from datetime import datetime, timezone
from typing import Dict, Any, List
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from budflow.executions.partial import (
    PartialExecutionEngine,
    DependencyGraph,
    NodeNotFoundError,
    MaxExecutionDepthError,
    CircularDependencyError
)
from budflow.workflows.models import Workflow, WorkflowNode, WorkflowConnection
from budflow.workflows.models import WorkflowExecution, ExecutionStatus, ExecutionMode
from budflow.executions.schemas import PartialExecutionRequest


@pytest.fixture
def sample_workflow():
    """Create a sample workflow for testing."""
    workflow = Workflow(
        id=1,
        name="Test Workflow",
        nodes=[
            {"id": "node1", "name": "Start Node", "type": "manual.trigger", "position": [100, 100]},
            {"id": "node2", "name": "Process Node", "type": "http.request", "position": [200, 100]},
            {"id": "node3", "name": "Transform Node", "type": "code.javascript", "position": [300, 100]},
            {"id": "node4", "name": "End Node", "type": "webhook.response", "position": [400, 100]}
        ],
        connections=[
            {"source": "node1", "target": "node2", "sourceOutput": "main", "targetInput": "main"},
            {"source": "node2", "target": "node3", "sourceOutput": "main", "targetInput": "main"},
            {"source": "node3", "target": "node4", "sourceOutput": "main", "targetInput": "main"}
        ]
    )
    return workflow


@pytest.fixture
def complex_workflow():
    """Create a complex workflow with branching for testing."""
    workflow = Workflow(
        id=2,
        name="Complex Workflow",
        nodes=[
            {"id": "start", "name": "Start", "type": "manual.trigger", "position": [100, 100]},
            {"id": "branch1", "name": "Branch 1", "type": "http.request", "position": [200, 50]},
            {"id": "branch2", "name": "Branch 2", "type": "database.query", "position": [200, 150]},
            {"id": "merge", "name": "Merge", "type": "code.javascript", "position": [300, 100]},
            {"id": "final", "name": "Final", "type": "email.send", "position": [400, 100]}
        ],
        connections=[
            {"source": "start", "target": "branch1", "sourceOutput": "main", "targetInput": "main"},
            {"source": "start", "target": "branch2", "sourceOutput": "main", "targetInput": "main"},
            {"source": "branch1", "target": "merge", "sourceOutput": "main", "targetInput": "main"},
            {"source": "branch2", "target": "merge", "sourceOutput": "main", "targetInput": "main"},
            {"source": "merge", "target": "final", "sourceOutput": "main", "targetInput": "main"}
        ]
    )
    return workflow


@pytest.fixture
def mock_db_session():
    """Create mock database session."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.add = Mock()
    return session


@pytest.fixture
def partial_execution_engine(mock_db_session):
    """Create partial execution engine with mocked dependencies."""
    engine = PartialExecutionEngine(mock_db_session)
    engine.execution_service = AsyncMock()
    engine.workflow_service = AsyncMock()
    return engine


class TestDependencyGraph:
    """Test dependency graph construction and analysis."""
    
    def test_build_dependency_graph_linear(self, sample_workflow):
        """Test building dependency graph for linear workflow."""
        engine = PartialExecutionEngine(None)
        graph = engine.build_dependency_graph(sample_workflow)
        
        expected = {
            "node1": ["node2"],
            "node2": ["node3"],
            "node3": ["node4"],
            "node4": []
        }
        assert graph.forward_dependencies == expected
        
        expected_reverse = {
            "node1": [],
            "node2": ["node1"],
            "node3": ["node2"],
            "node4": ["node3"]
        }
        assert graph.reverse_dependencies == expected_reverse
    
    def test_build_dependency_graph_branching(self, complex_workflow):
        """Test building dependency graph for branching workflow."""
        engine = PartialExecutionEngine(None)
        graph = engine.build_dependency_graph(complex_workflow)
        
        expected_forward = {
            "start": ["branch1", "branch2"],
            "branch1": ["merge"],
            "branch2": ["merge"],
            "merge": ["final"],
            "final": []
        }
        assert graph.forward_dependencies == expected_forward
        
        expected_reverse = {
            "start": [],
            "branch1": ["start"],
            "branch2": ["start"],
            "merge": ["branch1", "branch2"],
            "final": ["merge"]
        }
        assert graph.reverse_dependencies == expected_reverse
    
    def test_get_nodes_to_execute_from_start(self, sample_workflow):
        """Test getting nodes to execute starting from first node."""
        engine = PartialExecutionEngine(None)
        graph = engine.build_dependency_graph(sample_workflow)
        
        nodes_to_execute = engine.get_nodes_to_execute(graph, "node1")
        expected = {"node1", "node2", "node3", "node4"}
        assert nodes_to_execute == expected
    
    def test_get_nodes_to_execute_from_middle(self, sample_workflow):
        """Test getting nodes to execute starting from middle node."""
        engine = PartialExecutionEngine(None)
        graph = engine.build_dependency_graph(sample_workflow)
        
        nodes_to_execute = engine.get_nodes_to_execute(graph, "node3")
        expected = {"node3", "node4"}
        assert nodes_to_execute == expected
    
    def test_get_nodes_to_execute_from_end(self, sample_workflow):
        """Test getting nodes to execute starting from last node."""
        engine = PartialExecutionEngine(None)
        graph = engine.build_dependency_graph(sample_workflow)
        
        nodes_to_execute = engine.get_nodes_to_execute(graph, "node4")
        expected = {"node4"}
        assert nodes_to_execute == expected
    
    def test_get_nodes_to_execute_branching(self, complex_workflow):
        """Test getting nodes to execute in branching workflow."""
        engine = PartialExecutionEngine(None)
        graph = engine.build_dependency_graph(complex_workflow)
        
        # Starting from branch1 should include merge and final
        nodes_to_execute = engine.get_nodes_to_execute(graph, "branch1")
        expected = {"branch1", "merge", "final"}
        assert nodes_to_execute == expected
        
        # Starting from merge should include final
        nodes_to_execute = engine.get_nodes_to_execute(graph, "merge")
        expected = {"merge", "final"}
        assert nodes_to_execute == expected


class TestPartialExecutionValidation:
    """Test validation for partial execution requests."""
    
    async def test_validate_start_node_exists(self, partial_execution_engine, sample_workflow):
        """Test validation passes when start node exists."""
        engine = partial_execution_engine
        
        # Mock workflow service
        engine.workflow_service.get_workflow.return_value = sample_workflow
        
        # Should not raise
        await engine.validate_start_node("1", "node2")
    
    async def test_validate_start_node_not_found(self, partial_execution_engine, sample_workflow):
        """Test validation fails when start node doesn't exist."""
        engine = partial_execution_engine
        
        # Mock workflow service
        engine.workflow_service.get_workflow.return_value = sample_workflow
        
        with pytest.raises(NodeNotFoundError) as exc_info:
            await engine.validate_start_node("1", "nonexistent_node")
        
        assert "not found" in str(exc_info.value).lower()
    
    async def test_validate_circular_dependency_detection(self, partial_execution_engine):
        """Test detection of circular dependencies."""
        engine = partial_execution_engine
        
        # Create workflow with circular dependency
        circular_workflow = Workflow(
            id=3,
            name="Circular Workflow",
            nodes=[
                {"id": "node1", "name": "Node 1", "type": "manual.trigger"},
                {"id": "node2", "name": "Node 2", "type": "http.request"},
                {"id": "node3", "name": "Node 3", "type": "code.javascript"}
            ],
            connections=[
                {"source": "node1", "target": "node2"},
                {"source": "node2", "target": "node3"},
                {"source": "node3", "target": "node1"}  # Creates cycle
            ]
        )
        
        with pytest.raises(CircularDependencyError):
            engine.build_dependency_graph(circular_workflow)


class TestPartialExecutionDataPreparation:
    """Test data preparation for partial execution."""
    
    async def test_prepare_execution_data_with_input(self, partial_execution_engine):
        """Test data preparation with provided input data."""
        engine = partial_execution_engine
        
        input_data = {"key": "value", "number": 42}
        
        result = await engine.prepare_execution_data(
            workflow_id="1",
            start_node="node2",
            input_data=input_data,
            previous_execution_id=None
        )
        
        assert result["node2"] == input_data
    
    async def test_prepare_execution_data_from_previous_execution(self, partial_execution_engine):
        """Test data preparation from previous execution."""
        engine = partial_execution_engine
        
        # Mock previous execution data
        previous_data = {
            "node1": [{"output": "data from node1"}],
            "node2": [{"output": "data from node2"}]
        }
        engine.get_execution_results = AsyncMock(return_value=previous_data)
        
        result = await engine.prepare_execution_data(
            workflow_id="1",
            start_node="node2",
            input_data=None,
            previous_execution_id="prev-exec-123"
        )
        
        # Should extract data relevant to node2
        assert "node1" in result  # Previous node data available
        engine.get_execution_results.assert_called_once_with("prev-exec-123")
    
    async def test_prepare_execution_data_empty_input(self, partial_execution_engine):
        """Test data preparation with no input data."""
        engine = partial_execution_engine
        
        result = await engine.prepare_execution_data(
            workflow_id="1",
            start_node="node2",
            input_data=None,
            previous_execution_id=None
        )
        
        # Should create empty input for start node
        assert result.get("node2") is None or result.get("node2") == []


class TestPartialExecutionIntegration:
    """Integration tests for partial execution."""
    
    async def test_execute_partial_simple_workflow(self, partial_execution_engine, sample_workflow):
        """Test partial execution on simple workflow."""
        engine = partial_execution_engine
        
        # Mock dependencies
        engine.workflow_service.get_workflow.return_value = sample_workflow
        engine.execution_service.create_execution.return_value = Mock(id="exec-123")
        engine.execution_service.execute_nodes.return_value = {
            "node3": [{"result": "success"}],
            "node4": [{"result": "completed"}]
        }
        
        input_data = {"test": "data"}
        
        result = await engine.execute_partial(
            workflow_id="1",
            start_node="node3",
            input_data=input_data
        )
        
        # Verify execution was created
        engine.execution_service.create_execution.assert_called_once()
        
        # Verify correct nodes were executed
        call_args = engine.execution_service.execute_nodes.call_args
        executed_nodes = call_args[1]["nodes_to_execute"]
        assert "node3" in executed_nodes
        assert "node4" in executed_nodes
        assert "node1" not in executed_nodes  # Should be skipped
        assert "node2" not in executed_nodes  # Should be skipped
    
    async def test_execute_partial_with_dependencies(self, partial_execution_engine, complex_workflow):
        """Test partial execution with complex dependencies."""
        engine = partial_execution_engine
        
        # Mock dependencies
        engine.workflow_service.get_workflow.return_value = complex_workflow
        engine.execution_service.create_execution.return_value = Mock(id="exec-456")
        engine.execution_service.execute_nodes.return_value = {
            "merge": [{"merged": "data"}],
            "final": [{"sent": "email"}]
        }
        
        result = await engine.execute_partial(
            workflow_id="2",
            start_node="merge",
            input_data={"branch1_data": "value1", "branch2_data": "value2"}
        )
        
        # Verify only merge and final nodes were executed
        call_args = engine.execution_service.execute_nodes.call_args
        executed_nodes = call_args[1]["nodes_to_execute"]
        assert executed_nodes == {"merge", "final"}
    
    async def test_execute_partial_with_previous_execution_data(self, partial_execution_engine, sample_workflow):
        """Test partial execution using previous execution data."""
        engine = partial_execution_engine
        
        # Mock dependencies
        engine.workflow_service.get_workflow.return_value = sample_workflow
        engine.execution_service.create_execution.return_value = Mock(id="exec-789")
        engine.get_execution_results.return_value = {
            "node1": [{"initial": "data"}],
            "node2": [{"processed": "data"}]
        }
        
        result = await engine.execute_partial(
            workflow_id="1",
            start_node="node3",
            input_data=None,
            previous_execution_id="prev-exec-456"
        )
        
        # Verify previous execution data was retrieved
        engine.get_execution_results.assert_called_once_with("prev-exec-456")


class TestPartialExecutionErrorHandling:
    """Test error handling in partial execution."""
    
    async def test_partial_execution_invalid_workflow(self, partial_execution_engine):
        """Test error when workflow doesn't exist."""
        engine = partial_execution_engine
        
        engine.workflow_service.get_workflow.return_value = None
        
        with pytest.raises(Exception) as exc_info:
            await engine.execute_partial(
                workflow_id="nonexistent",
                start_node="node1"
            )
        
        assert "not found" in str(exc_info.value).lower()
    
    async def test_partial_execution_node_failure(self, partial_execution_engine, sample_workflow):
        """Test handling of node execution failure."""
        engine = partial_execution_engine
        
        # Mock dependencies
        engine.workflow_service.get_workflow.return_value = sample_workflow
        engine.execution_service.create_execution.return_value = Mock(id="exec-fail")
        engine.execution_service.execute_nodes.side_effect = Exception("Node execution failed")
        
        with pytest.raises(Exception) as exc_info:
            await engine.execute_partial(
                workflow_id="1",
                start_node="node2"
            )
        
        assert "execution failed" in str(exc_info.value).lower()


class TestPartialExecutionAPI:
    """Test API integration for partial execution."""
    
    async def test_partial_execution_request_schema(self):
        """Test partial execution request schema validation."""
        # Valid request
        request = PartialExecutionRequest(
            start_node="node2",
            input_data={"key": "value"},
            previous_execution_id="exec-123"
        )
        
        assert request.start_node == "node2"
        assert request.input_data == {"key": "value"}
        assert request.previous_execution_id == "exec-123"
        
        # Minimal request
        minimal_request = PartialExecutionRequest(start_node="node1")
        assert minimal_request.start_node == "node1"
        assert minimal_request.input_data is None
        assert minimal_request.previous_execution_id is None


class TestPartialExecutionPerformance:
    """Test performance aspects of partial execution."""
    
    async def test_partial_execution_large_workflow(self, partial_execution_engine):
        """Test partial execution performance on large workflow."""
        # Create large workflow with many nodes
        large_workflow = Workflow(
            id=4,
            name="Large Workflow",
            nodes=[
                {"id": f"node{i}", "name": f"Node {i}", "type": "http.request"}
                for i in range(100)
            ],
            connections=[
                {"source": f"node{i}", "target": f"node{i+1}"}
                for i in range(99)
            ]
        )
        
        engine = partial_execution_engine
        engine.workflow_service.get_workflow.return_value = large_workflow
        
        # Test dependency graph building performance
        import time
        start_time = time.time()
        graph = engine.build_dependency_graph(large_workflow)
        build_time = time.time() - start_time
        
        # Should build graph quickly (under 1 second for 100 nodes)
        assert build_time < 1.0
        
        # Test node selection performance
        start_time = time.time()
        nodes_to_execute = engine.get_nodes_to_execute(graph, "node50")
        selection_time = time.time() - start_time
        
        # Should select nodes quickly
        assert selection_time < 0.1
        assert len(nodes_to_execute) == 50  # node50 through node99
    
    async def test_concurrent_partial_executions(self, partial_execution_engine, sample_workflow):
        """Test concurrent partial executions."""
        import asyncio
        
        engine = partial_execution_engine
        engine.workflow_service.get_workflow.return_value = sample_workflow
        engine.execution_service.create_execution.return_value = Mock(id="concurrent-exec")
        engine.execution_service.execute_nodes.return_value = {"node2": [{"result": "success"}]}
        
        # Create multiple concurrent partial executions
        tasks = []
        for i in range(5):
            task = engine.execute_partial(
                workflow_id="1",
                start_node="node2",
                input_data={"batch": i}
            )
            tasks.append(task)
        
        # Execute all concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should complete successfully
        assert len(results) == 5
        for result in results:
            assert not isinstance(result, Exception)