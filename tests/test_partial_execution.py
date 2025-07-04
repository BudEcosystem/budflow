"""
Comprehensive tests for partial execution functionality.

Partial execution allows starting workflow execution from a specific node
rather than from the beginning, which is critical for debugging and
iterative workflow development.
"""

import pytest
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from budflow.executor.engine import WorkflowExecutionEngine
from budflow.executor.errors import (
    WorkflowExecutionError,
    CircularDependencyError,
    InvalidStartNodeError,
)
from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, ExecutionStatus, NodeExecutionStatus,
    ExecutionMode, NodeType
)
from budflow.nodes.base import BaseNode, NodeDefinition
from budflow.nodes.registry import NodeRegistry


class MockNode(BaseNode):
    """Mock node for testing."""
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        return NodeDefinition(
            name="MockNode",
            type=NodeType.ACTION,
            description="Mock node for testing",
            inputs=["main"],
            outputs=["main"]
        )
    
    async def execute(self) -> List[Dict[str, Any]]:
        # Return input data with node name appended
        input_data = self.get_input_data()
        return [{
            **item,
            "processed_by": self.node_name
        } for item in input_data]


class TestPartialExecution:
    """Test suite for partial execution functionality."""
    
    @pytest.fixture
    async def db_session(self):
        """Mock database session."""
        session = AsyncMock(spec=AsyncSession)
        return session
    
    @pytest.fixture
    def node_registry(self):
        """Mock node registry."""
        registry = MagicMock(spec=NodeRegistry)
        registry.get_node_class.return_value = MockNode
        return registry
    
    @pytest.fixture
    def sample_workflow(self):
        """Create a sample workflow for testing."""
        workflow = Workflow(
            id=1,
            name="Test Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1
        )
        workflow.uuid = str(uuid4())
        
        # Create nodes: Start -> Process1 -> Process2 -> Process3 -> End
        nodes = []
        for i, (name, x) in enumerate([("Start", 0), ("Process1", 100), ("Process2", 200), ("Process3", 300), ("End", 400)], 1):
            node = WorkflowNode(
                id=i,
                workflow_id=1,
                name=name,
                type=NodeType.ACTION,
                position={"x": x, "y": 0},
                parameters={}
            )
            node.uuid = str(uuid4())
            nodes.append(node)
        
        # Create linear connections
        connections = [
            WorkflowConnection(
                id=1,
                workflow_id=1,
                source_node_id=1,
                source_output="main",
                target_node_id=2,
                target_input="main"
            ),
            WorkflowConnection(
                id=2,
                workflow_id=1,
                source_node_id=2,
                source_output="main",
                target_node_id=3,
                target_input="main"
            ),
            WorkflowConnection(
                id=3,
                workflow_id=1,
                source_node_id=3,
                source_output="main",
                target_node_id=4,
                target_input="main"
            ),
            WorkflowConnection(
                id=4,
                workflow_id=1,
                source_node_id=4,
                source_output="main",
                target_node_id=5,
                target_input="main"
            )
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow
    
    @pytest.fixture
    def branching_workflow(self):
        """Create a workflow with branching for testing."""
        workflow = Workflow(
            id=2,
            name="Branching Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1
        )
        workflow.uuid = str(uuid4())
        
        # Create nodes with branching:
        #     -> Branch1 -> Merge
        # Start            
        #     -> Branch2 -> Merge -> End
        nodes = []
        node_specs = [
            (1, "Start", 0, 0),
            (2, "Branch1", 100, -50),
            (3, "Branch2", 100, 50),
            (4, "Merge", 200, 0),
            (5, "End", 300, 0)
        ]
        for node_id, name, x, y in node_specs:
            node = WorkflowNode(
                id=node_id,
                workflow_id=2,
                name=name,
                type=NodeType.ACTION,
                position={"x": x, "y": y}
            )
            node.uuid = str(uuid4())
            nodes.append(node)
        
        connections = [
            WorkflowConnection(id=1, workflow_id=2, source_node_id=1, source_output="main", target_node_id=2, target_input="main"),
            WorkflowConnection(id=2, workflow_id=2, source_node_id=1, source_output="main", target_node_id=3, target_input="main"),
            WorkflowConnection(id=3, workflow_id=2, source_node_id=2, source_output="main", target_node_id=4, target_input="main"),
            WorkflowConnection(id=4, workflow_id=2, source_node_id=3, source_output="main", target_node_id=4, target_input="main"),
            WorkflowConnection(id=5, workflow_id=2, source_node_id=4, source_output="main", target_node_id=5, target_input="main")
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow
    
    @pytest.mark.asyncio
    async def test_partial_execution_from_middle_node(self, db_session, sample_workflow):
        """Test starting execution from a middle node."""
        engine = WorkflowExecutionEngine(db_session)
        
        # Mock data that would have been produced by previous nodes
        mock_previous_data = {
            "Process1": [{"data": "from_start", "processed_by": "Start"}]
        }
        
        # Execute starting from Process2
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            with patch.object(engine, '_create_partial_workflow_execution') as mock_create:
                mock_execution = MagicMock()
                mock_execution.id = 1
                mock_execution.uuid = str(uuid4())
                mock_create.return_value = mock_execution
                
                result = await engine.execute_partial_workflow(
                    workflow_id=1,
                    start_node_id=3,  # Process2
                    previous_node_data=mock_previous_data,
                    mode=ExecutionMode.MANUAL
                )
                
                # Verify execution was created with partial flag
                mock_create.assert_called_once()
                call_args = mock_create.call_args[1]
                assert call_args['execution_type'] == 'partial'
                assert call_args['start_node'] == 'Process2'
    
    @pytest.mark.asyncio
    async def test_partial_execution_validates_start_node(self, db_session, sample_workflow):
        """Test that partial execution validates the start node exists."""
        engine = WorkflowExecutionEngine(db_session)
        
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            # Try to start from non-existent node
            with pytest.raises(InvalidStartNodeError) as exc_info:
                await engine.execute_partial_workflow(
                    workflow_id=1,
                    start_node_id=999,  # Non-existent
                    previous_node_data={},
                    mode=ExecutionMode.MANUAL
                )
            
            assert "Node 999 not found in workflow" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_partial_execution_requires_previous_data(self, db_session, sample_workflow):
        """Test that partial execution requires data from previous nodes."""
        engine = WorkflowExecutionEngine(db_session)
        
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            # Try to start from Process3 without data from Process2
            with pytest.raises(WorkflowExecutionError) as exc_info:
                await engine.execute_partial_workflow(
                    workflow_id=1,
                    start_node_id=4,  # Process3
                    previous_node_data={},  # Missing required data
                    mode=ExecutionMode.MANUAL
                )
            
            assert "Missing required input data" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_partial_execution_with_branching(self, db_session, branching_workflow):
        """Test partial execution in a branching workflow."""
        engine = WorkflowExecutionEngine(db_session)
        
        # Mock data from both branches
        mock_previous_data = {
            "Branch1": [{"data": "from_branch1", "processed_by": "Branch1"}],
            "Branch2": [{"data": "from_branch2", "processed_by": "Branch2"}]
        }
        
        with patch.object(engine, '_load_workflow', return_value=branching_workflow):
            with patch.object(engine, '_create_workflow_execution') as mock_create:
                mock_execution = MagicMock()
                mock_execution.id = 1
                mock_execution.uuid = uuid4()
                mock_create.return_value = mock_execution
                
                # Start from Merge node
                with patch.object(engine, '_create_partial_workflow_execution') as mock_create_exec:
                    mock_exec = MagicMock()
                    mock_exec.id = 1
                    mock_exec.uuid = str(uuid4())
                    mock_create_exec.return_value = mock_exec
                    
                    result = await engine.execute_partial_workflow(
                        workflow_id=2,
                        start_node_id=4,  # Merge
                        previous_node_data=mock_previous_data,
                        mode=ExecutionMode.MANUAL
                    )
                    
                    # Verify merge node receives data from both branches
                    assert mock_create_exec.called
    
    @pytest.mark.asyncio
    async def test_partial_execution_creates_dependency_graph(self, db_session, sample_workflow):
        """Test that partial execution correctly builds dependency graph."""
        engine = WorkflowExecutionEngine(db_session)
        
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            # Build dependency graph starting from Process2
            graph = await engine._build_partial_execution_graph(
                workflow=sample_workflow,
                start_node_id=3  # Process2
            )
            
            # Verify graph includes only Process2 and subsequent nodes
            assert 3 in graph.nodes  # Process2
            assert 4 in graph.nodes  # Process3
            assert 5 in graph.nodes  # End
            
            # Verify earlier nodes are excluded
            assert 1 not in graph.nodes  # Start
            assert 2 not in graph.nodes  # Process1
    
    @pytest.mark.asyncio
    async def test_partial_execution_handles_circular_dependencies(self, db_session):
        """Test that partial execution detects circular dependencies."""
        # Create workflow with circular dependency
        workflow = Workflow(
            id=3,
            name="Circular Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1
        )
        workflow.uuid = str(uuid4())
        
        nodes = []
        for i, name in enumerate(["Node1", "Node2", "Node3"], 1):
            node = WorkflowNode(
                id=i,
                workflow_id=3,
                name=name,
                type=NodeType.ACTION
            )
            node.uuid = str(uuid4())
            nodes.append(node)
        
        # Create circular connections: 1 -> 2 -> 3 -> 1
        connections = [
            WorkflowConnection(id=1, workflow_id=3, source_node_id=1, target_node_id=2),
            WorkflowConnection(id=2, workflow_id=3, source_node_id=2, target_node_id=3),
            WorkflowConnection(id=3, workflow_id=3, source_node_id=3, target_node_id=1)  # Circular
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        engine = WorkflowExecutionEngine(db_session)
        
        with patch.object(engine, '_load_workflow', return_value=workflow):
            with pytest.raises(CircularDependencyError):
                await engine.execute_partial_workflow(
                    workflow_id=3,
                    start_node_id=2,
                    previous_node_data={"Node1": [{"data": "test"}]},
                    mode=ExecutionMode.MANUAL
                )
    
    @pytest.mark.asyncio
    async def test_partial_execution_preserves_execution_history(self, db_session, sample_workflow):
        """Test that partial execution preserves execution history."""
        engine = WorkflowExecutionEngine(db_session)
        
        mock_previous_data = {
            "Process1": [{"data": "historical", "processed_by": "Start"}]
        }
        
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            with patch.object(engine, '_create_workflow_execution') as mock_create:
                with patch.object(engine, '_save_node_execution') as mock_save_node:
                    mock_execution = MagicMock()
                    mock_execution.id = 1
                    mock_execution.uuid = uuid4()
                    mock_create.return_value = mock_execution
                    
                    # Track saved node executions
                    saved_executions = []
                    mock_save_node.side_effect = lambda x: saved_executions.append(x)
                    
                    await engine.execute_partial_workflow(
                        workflow_id=1,
                        start_node_id=3,  # Process2
                        previous_node_data=mock_previous_data,
                        mode=ExecutionMode.MANUAL
                    )
                    
                    # Verify historical data is preserved
                    assert any(
                        exec.is_historical == True 
                        for exec in saved_executions 
                        if exec.node_id == 2  # Process1
                    )
    
    @pytest.mark.asyncio
    async def test_partial_execution_with_run_data_override(self, db_session, sample_workflow):
        """Test partial execution with custom run data for debugging."""
        engine = WorkflowExecutionEngine(db_session)
        
        # Custom data to inject at specific node
        run_data_override = {
            "Process2": [{"custom": "debug_data", "override": True}]
        }
        
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            with patch.object(engine, '_execute_node') as mock_execute:
                mock_execute.return_value = [{"processed": "data"}]
                
                await engine.execute_partial_workflow(
                    workflow_id=1,
                    start_node_id=3,  # Process2
                    previous_node_data={},
                    run_data_override=run_data_override,
                    mode=ExecutionMode.MANUAL
                )
                
                # Verify Process2 uses override data instead of executing
                assert not any(
                    call[0][0].id == 3  # Process2
                    for call in mock_execute.call_args_list
                )
    
    @pytest.mark.asyncio
    async def test_partial_execution_skip_nodes_until_start(self, db_session, sample_workflow):
        """Test that nodes before start node are skipped during partial execution."""
        engine = WorkflowExecutionEngine(db_session)
        
        with patch.object(engine, '_load_workflow', return_value=sample_workflow):
            with patch.object(engine, '_execute_node') as mock_execute:
                mock_execute.return_value = [{"processed": "data"}]
                
                await engine.execute_partial_workflow(
                    workflow_id=1,
                    start_node_id=3,  # Process2
                    previous_node_data={"Process1": [{"data": "test"}]},
                    mode=ExecutionMode.MANUAL
                )
                
                # Verify only nodes from Process2 onwards are executed
                executed_node_ids = [call[0][0].id for call in mock_execute.call_args_list]
                assert 1 not in executed_node_ids  # Start not executed
                assert 2 not in executed_node_ids  # Process1 not executed
                assert 3 in executed_node_ids      # Process2 executed
                assert 4 in executed_node_ids      # Process3 executed
                assert 5 in executed_node_ids      # End executed


class TestPartialExecutionAPI:
    """Test partial execution API endpoints."""
    
    @pytest.mark.asyncio
    async def test_api_partial_execution_endpoint(self, client, sample_workflow):
        """Test the partial execution API endpoint."""
        request_data = {
            "workflow_id": 1,
            "start_node_id": 3,
            "previous_node_data": {
                "Process1": [{"data": "test"}]
            }
        }
        
        response = await client.post(
            "/api/v1/executions/partial",
            json=request_data
        )
        
        assert response.status_code == 201
        assert response.json()["execution_type"] == "partial"
        assert response.json()["start_node"] == "Process2"
    
    @pytest.mark.asyncio
    async def test_api_partial_execution_validation(self, client):
        """Test API validation for partial execution."""
        # Missing required fields
        response = await client.post(
            "/api/v1/executions/partial",
            json={"workflow_id": 1}
        )
        
        assert response.status_code == 422
        assert "start_node_id" in response.json()["detail"][0]["loc"]
    
    @pytest.mark.asyncio
    async def test_api_get_node_dependencies(self, client, sample_workflow):
        """Test getting node dependencies for partial execution."""
        response = await client.get(
            f"/api/v1/workflows/1/nodes/3/dependencies"
        )
        
        assert response.status_code == 200
        dependencies = response.json()
        assert "required_nodes" in dependencies
        assert 2 in dependencies["required_nodes"]  # Process1 is required
        assert "execution_order" in dependencies