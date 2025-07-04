"""
Comprehensive tests for directed graph execution algorithm.

The directed graph execution algorithm is critical for optimal node execution
order in complex workflows, supporting topological sorting, dependency resolution,
and execution ordering based on N8N's approach.
"""

import pytest
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession
import networkx as nx

from budflow.executor.engine import WorkflowExecutionEngine
from budflow.executor.directed_graph import DirectedGraphExecutor
from budflow.executor.errors import (
    WorkflowExecutionError,
    CircularDependencyError,
    InvalidWorkflowStructureError,
)
from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, ExecutionStatus, NodeExecutionStatus,
    ExecutionMode, NodeType
)


class TestDirectedGraphExecution:
    """Test suite for directed graph execution algorithm."""

    @pytest.fixture
    async def db_session(self):
        """Mock database session."""
        session = AsyncMock(spec=AsyncSession)
        return session

    @pytest.fixture
    def complex_workflow(self):
        """Create a complex workflow with multiple paths for testing."""
        workflow = Workflow(
            id=1,
            name="Complex Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1
        )
        workflow.uuid = str(uuid4())
        
        # Complex workflow structure:
        #     Start1
        #       |
        #    Process1 -> Process2 -> Merge -> End
        #       |          |         |
        #    Process3 -> Process4 ----+
        #       |
        #    Start2 -> Process5 -------+
        
        nodes = []
        node_specs = [
            (1, "Start1", 0, 0),
            (2, "Process1", 100, 0),
            (3, "Process2", 200, -50),
            (4, "Process3", 200, 50),
            (5, "Process4", 300, 0),
            (6, "Start2", 100, 100),
            (7, "Process5", 200, 100),
            (8, "Merge", 400, 50),
            (9, "End", 500, 50)
        ]
        
        for node_id, name, x, y in node_specs:
            node = WorkflowNode(
                id=node_id,
                workflow_id=1,
                name=name,
                type=NodeType.ACTION,
                position={"x": x, "y": y},
                parameters={}
            )
            node.uuid = str(uuid4())
            nodes.append(node)
        
        # Create connections
        connections = [
            WorkflowConnection(id=1, workflow_id=1, source_node_id=1, source_output="main", target_node_id=2, target_input="main"),  # Start1 -> Process1
            WorkflowConnection(id=2, workflow_id=1, source_node_id=2, source_output="main", target_node_id=3, target_input="main"),  # Process1 -> Process2
            WorkflowConnection(id=3, workflow_id=1, source_node_id=2, source_output="main", target_node_id=4, target_input="main"),  # Process1 -> Process3
            WorkflowConnection(id=4, workflow_id=1, source_node_id=4, source_output="main", target_node_id=5, target_input="main"),  # Process3 -> Process4
            WorkflowConnection(id=5, workflow_id=1, source_node_id=6, source_output="main", target_node_id=7, target_input="main"),  # Start2 -> Process5
            WorkflowConnection(id=6, workflow_id=1, source_node_id=3, source_output="main", target_node_id=8, target_input="main"),  # Process2 -> Merge
            WorkflowConnection(id=7, workflow_id=1, source_node_id=5, source_output="main", target_node_id=8, target_input="main"),  # Process4 -> Merge
            WorkflowConnection(id=8, workflow_id=1, source_node_id=7, source_output="main", target_node_id=8, target_input="main"),  # Process5 -> Merge
            WorkflowConnection(id=9, workflow_id=1, source_node_id=8, source_output="main", target_node_id=9, target_input="main"),  # Merge -> End
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow

    @pytest.fixture
    def diamond_workflow(self):
        """Create a diamond-shaped workflow for testing parallel paths."""
        workflow = Workflow(
            id=2,
            name="Diamond Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1
        )
        workflow.uuid = str(uuid4())
        
        # Diamond structure:
        #      Start
        #      /   \
        #   Left   Right
        #      \   /
        #      Merge
        
        nodes = []
        node_specs = [
            (1, "Start", 0, 0),
            (2, "Left", 100, -50),
            (3, "Right", 100, 50),
            (4, "Merge", 200, 0)
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
            WorkflowConnection(id=1, workflow_id=2, source_node_id=1, target_node_id=2),  # Start -> Left
            WorkflowConnection(id=2, workflow_id=2, source_node_id=1, target_node_id=3),  # Start -> Right
            WorkflowConnection(id=3, workflow_id=2, source_node_id=2, target_node_id=4),  # Left -> Merge
            WorkflowConnection(id=4, workflow_id=2, source_node_id=3, target_node_id=4),  # Right -> Merge
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow

    @pytest.mark.asyncio
    async def test_topological_sort_basic(self, db_session, complex_workflow):
        """Test basic topological sorting of workflow nodes."""
        executor = DirectedGraphExecutor(db_session)
        
        # Build graph and get topological order
        graph = await executor.build_execution_graph(complex_workflow)
        execution_order = await executor.get_topological_execution_order(graph)
        
        # Verify that dependencies are respected
        node_positions = {node_id: idx for idx, node_id in enumerate(execution_order)}
        
        # Start1 should come before Process1
        assert node_positions[1] < node_positions[2]
        
        # Process1 should come before Process2 and Process3
        assert node_positions[2] < node_positions[3]
        assert node_positions[2] < node_positions[4]
        
        # Process3 should come before Process4
        assert node_positions[4] < node_positions[5]
        
        # All predecessors of Merge should come before it
        merge_pos = node_positions[8]
        assert node_positions[3] < merge_pos  # Process2
        assert node_positions[5] < merge_pos  # Process4
        assert node_positions[7] < merge_pos  # Process5
        
        # Merge should come before End
        assert node_positions[8] < node_positions[9]

    @pytest.mark.asyncio
    async def test_parallel_execution_detection(self, db_session, diamond_workflow):
        """Test detection of nodes that can execute in parallel."""
        executor = DirectedGraphExecutor(db_session)
        
        # Build graph
        graph = await executor.build_execution_graph(diamond_workflow)
        
        # Get parallel execution groups
        parallel_groups = await executor.get_parallel_execution_groups(graph)
        
        # Verify parallel groups
        assert len(parallel_groups) >= 3  # Start group, parallel group (Left+Right), merge group
        
        # Find the parallel group containing Left and Right
        parallel_group = None
        for group in parallel_groups:
            if 2 in group and 3 in group:  # Left and Right nodes
                parallel_group = group
                break
        
        assert parallel_group is not None
        assert 2 in parallel_group  # Left
        assert 3 in parallel_group  # Right

    @pytest.mark.asyncio
    async def test_execution_levels(self, db_session, complex_workflow):
        """Test calculating execution levels for optimal scheduling."""
        executor = DirectedGraphExecutor(db_session)
        
        # Build graph
        graph = await executor.build_execution_graph(complex_workflow)
        
        # Calculate execution levels
        levels = await executor.calculate_execution_levels(graph)
        
        # Verify levels are correct
        assert levels[1] == 0  # Start1 is level 0
        assert levels[6] == 0  # Start2 is level 0
        assert levels[2] == 1  # Process1 is level 1 (after Start1)
        assert levels[7] == 1  # Process5 is level 1 (after Start2)
        assert levels[3] >= 2  # Process2 is at least level 2
        assert levels[4] >= 2  # Process3 is at least level 2
        assert levels[8] >= 3  # Merge is at least level 3 (after all predecessors)
        assert levels[9] >= 4  # End is at least level 4 (after Merge)

    @pytest.mark.asyncio
    async def test_execution_order_optimization(self, db_session, complex_workflow):
        """Test execution order optimization for performance."""
        executor = DirectedGraphExecutor(db_session)
        
        # Get optimized execution order
        execution_plan = await executor.create_execution_plan(complex_workflow)
        
        # Verify plan structure
        assert hasattr(execution_plan, 'execution_order')
        assert hasattr(execution_plan, 'parallel_groups')
        assert hasattr(execution_plan, 'critical_path')
        assert hasattr(execution_plan, 'levels')
        
        # Verify critical path
        critical_path = execution_plan.critical_path
        assert len(critical_path) > 0
        assert critical_path[0] in [1, 6]  # Should start with one of the start nodes
        assert critical_path[-1] == 9  # Should end with End node

    @pytest.mark.asyncio
    async def test_dependency_validation(self, db_session, complex_workflow):
        """Test comprehensive dependency validation."""
        executor = DirectedGraphExecutor(db_session)
        
        # Build graph
        graph = await executor.build_execution_graph(complex_workflow)
        
        # Validate all dependencies
        validation_result = await executor.validate_dependencies(graph)
        
        assert validation_result["is_valid"] is True
        assert "circular_dependencies" in validation_result
        assert "disconnected_components" in validation_result
        assert "unreachable_nodes" in validation_result
        
        # Should have no circular dependencies
        assert len(validation_result["circular_dependencies"]) == 0

    @pytest.mark.asyncio
    async def test_circular_dependency_detection(self, db_session):
        """Test detection of circular dependencies."""
        # Create workflow with circular dependency
        workflow = Workflow(
            id=3,
            name="Circular Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1
        )
        workflow.uuid = str(uuid4())
        
        nodes = []
        for i in range(1, 4):
            node = WorkflowNode(
                id=i,
                workflow_id=3,
                name=f"Node{i}",
                type=NodeType.ACTION
            )
            node.uuid = str(uuid4())
            nodes.append(node)
        
        # Create circular connections: 1 -> 2 -> 3 -> 1
        connections = [
            WorkflowConnection(id=1, workflow_id=3, source_node_id=1, target_node_id=2),
            WorkflowConnection(id=2, workflow_id=3, source_node_id=2, target_node_id=3),
            WorkflowConnection(id=3, workflow_id=3, source_node_id=3, target_node_id=1)  # Creates cycle
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        executor = DirectedGraphExecutor(db_session)
        
        # Should detect circular dependency
        with pytest.raises(CircularDependencyError) as exc_info:
            await executor.create_execution_plan(workflow)
        
        assert "circular" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_node_readiness_calculation(self, db_session, complex_workflow):
        """Test calculation of node readiness based on dependencies."""
        executor = DirectedGraphExecutor(db_session)
        
        # Build graph
        graph = await executor.build_execution_graph(complex_workflow)
        
        # Initially, only start nodes should be ready
        executed_nodes = set()
        ready_nodes = await executor.get_ready_nodes(graph, executed_nodes)
        
        # Should include both start nodes
        assert 1 in ready_nodes  # Start1
        assert 6 in ready_nodes  # Start2
        assert len(ready_nodes) == 2
        
        # After executing Start1, Process1 should be ready
        executed_nodes.add(1)
        ready_nodes = await executor.get_ready_nodes(graph, executed_nodes)
        assert 2 in ready_nodes  # Process1
        assert 6 in ready_nodes  # Start2 still ready
        
        # After executing Process1, Process2 and Process3 should be ready
        executed_nodes.add(2)
        ready_nodes = await executor.get_ready_nodes(graph, executed_nodes)
        assert 3 in ready_nodes  # Process2
        assert 4 in ready_nodes  # Process3

    @pytest.mark.asyncio
    async def test_execution_path_analysis(self, db_session, complex_workflow):
        """Test analysis of execution paths and bottlenecks."""
        executor = DirectedGraphExecutor(db_session)
        
        # Analyze execution paths
        path_analysis = await executor.analyze_execution_paths(complex_workflow)
        
        assert "all_paths" in path_analysis
        assert "critical_path" in path_analysis
        assert "bottlenecks" in path_analysis
        assert "parallelizable_segments" in path_analysis
        
        # Should identify merge node as a potential bottleneck
        bottlenecks = path_analysis["bottlenecks"]
        merge_node_id = 8
        assert any(bottleneck["node_id"] == merge_node_id for bottleneck in bottlenecks)

    @pytest.mark.asyncio
    async def test_subgraph_execution(self, db_session, complex_workflow):
        """Test execution of workflow subgraphs."""
        executor = DirectedGraphExecutor(db_session)
        
        # Create subgraph starting from Process1
        subgraph_plan = await executor.create_subgraph_execution_plan(
            workflow=complex_workflow,
            start_nodes=[2],  # Process1
            end_nodes=[8]     # Merge
        )
        
        assert "execution_order" in subgraph_plan
        assert "included_nodes" in subgraph_plan
        
        # Should include nodes 2, 3, 4, 5, 8 but not 1, 6, 7, 9
        included = set(subgraph_plan["included_nodes"])
        assert 2 in included  # Process1
        assert 3 in included  # Process2
        assert 4 in included  # Process3
        assert 5 in included  # Process4
        assert 8 in included  # Merge
        assert 1 not in included  # Start1
        assert 6 not in included  # Start2
        assert 7 not in included  # Process5
        assert 9 not in included  # End

    @pytest.mark.asyncio
    async def test_weighted_execution_planning(self, db_session, complex_workflow):
        """Test execution planning with node weights/costs."""
        executor = DirectedGraphExecutor(db_session)
        
        # Assign weights to nodes (execution time estimates)
        node_weights = {
            1: 1,   # Start1 - fast
            2: 5,   # Process1 - medium
            3: 10,  # Process2 - slow
            4: 3,   # Process3 - fast
            5: 7,   # Process4 - medium
            6: 1,   # Start2 - fast
            7: 15,  # Process5 - very slow
            8: 8,   # Merge - medium
            9: 2    # End - fast
        }
        
        # Create weighted execution plan
        weighted_plan = await executor.create_weighted_execution_plan(
            complex_workflow, node_weights
        )
        
        assert "critical_path" in weighted_plan
        assert "total_weight" in weighted_plan
        assert "parallelization_opportunities" in weighted_plan
        
        # Critical path should prefer the path with highest total weight
        critical_path = weighted_plan["critical_path"]
        assert len(critical_path) > 0

    @pytest.mark.asyncio
    async def test_execution_plan_serialization(self, db_session, complex_workflow):
        """Test serialization and deserialization of execution plans."""
        executor = DirectedGraphExecutor(db_session)
        
        # Create execution plan
        original_plan = await executor.create_execution_plan(complex_workflow)
        
        # Serialize to JSON
        serialized = await executor.serialize_execution_plan(original_plan)
        assert isinstance(serialized, str)
        
        # Deserialize
        deserialized_plan = await executor.deserialize_execution_plan(serialized)
        
        # Verify key fields are preserved
        assert deserialized_plan["execution_order"] == original_plan.execution_order
        assert deserialized_plan["levels"] == original_plan.levels
        assert deserialized_plan["critical_path"] == original_plan.critical_path

    @pytest.mark.asyncio
    async def test_dynamic_execution_reordering(self, db_session, complex_workflow):
        """Test dynamic reordering based on runtime conditions."""
        executor = DirectedGraphExecutor(db_session)
        
        # Simulate runtime conditions (node failures, delays)
        runtime_conditions = {
            "failed_nodes": [3],  # Process2 failed
            "delayed_nodes": [7], # Process5 is delayed
            "completed_nodes": [1, 2, 4, 6]  # These are done
        }
        
        # Get updated execution plan
        updated_plan = await executor.update_execution_plan(
            complex_workflow, runtime_conditions
        )
        
        assert "remaining_nodes" in updated_plan
        assert "alternative_paths" in updated_plan
        assert "recovery_options" in updated_plan
        
        # Should suggest alternative paths avoiding failed nodes
        remaining = updated_plan["remaining_nodes"]
        assert 3 not in remaining  # Failed node should be excluded


class TestDirectedGraphIntegration:
    """Integration tests for directed graph execution with main engine."""
    
    @pytest.fixture
    async def db_session(self):
        """Mock database session."""
        session = AsyncMock(spec=AsyncSession)
        return session
    
    @pytest.fixture
    def complex_workflow(self):
        """Create a complex workflow with multiple paths for testing."""
        workflow = Workflow(
            id=1,
            name="Complex Workflow",
            status=WorkflowStatus.ACTIVE,
            user_id=1,
            total_executions=0,
            successful_executions=0,
            failed_executions=0
        )
        workflow.uuid = str(uuid4())
        
        # Complex workflow structure
        nodes = []
        node_specs = [
            (1, "Start1", 0, 0),
            (2, "Process1", 100, 0),
            (3, "Process2", 200, -50),
            (4, "Process3", 200, 50),
            (5, "Process4", 300, 0),
            (6, "Start2", 100, 100),
            (7, "Process5", 200, 100),
            (8, "Merge", 400, 50),
            (9, "End", 500, 50)
        ]
        
        for node_id, name, x, y in node_specs:
            node = WorkflowNode(
                id=node_id,
                workflow_id=1,
                name=name,
                type=NodeType.ACTION,
                position={"x": x, "y": y},
                parameters={}
            )
            node.uuid = str(uuid4())
            nodes.append(node)
        
        # Create connections
        connections = [
            WorkflowConnection(id=1, workflow_id=1, source_node_id=1, source_output="main", target_node_id=2, target_input="main"),
            WorkflowConnection(id=2, workflow_id=1, source_node_id=2, source_output="main", target_node_id=3, target_input="main"),
            WorkflowConnection(id=3, workflow_id=1, source_node_id=2, source_output="main", target_node_id=4, target_input="main"),
            WorkflowConnection(id=4, workflow_id=1, source_node_id=4, source_output="main", target_node_id=5, target_input="main"),
            WorkflowConnection(id=5, workflow_id=1, source_node_id=6, source_output="main", target_node_id=7, target_input="main"),
            WorkflowConnection(id=6, workflow_id=1, source_node_id=3, source_output="main", target_node_id=8, target_input="main"),
            WorkflowConnection(id=7, workflow_id=1, source_node_id=5, source_output="main", target_node_id=8, target_input="main"),
            WorkflowConnection(id=8, workflow_id=1, source_node_id=7, source_output="main", target_node_id=8, target_input="main"),
            WorkflowConnection(id=9, workflow_id=1, source_node_id=8, source_output="main", target_node_id=9, target_input="main"),
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow
    
    @pytest.mark.asyncio
    async def test_engine_uses_directed_graph(self, db_session, complex_workflow):
        """Test that main engine uses directed graph execution."""
        engine = WorkflowExecutionEngine(db_session)
        
        with patch.object(engine, '_load_workflow', return_value=complex_workflow):
            with patch.object(engine, '_create_workflow_execution') as mock_create:
                with patch.object(engine.directed_graph_executor, 'create_execution_plan') as mock_plan_method:
                    mock_execution = MagicMock()
                    mock_execution.id = 1
                    mock_execution.uuid = str(uuid4())
                    mock_create.return_value = mock_execution
                    
                    # Mock execution plan
                    from budflow.executor.directed_graph import ExecutionPlan
                    mock_plan = ExecutionPlan(
                        execution_order=[1, 6, 2, 7, 3, 4, 5, 8, 9],
                        parallel_groups=[[1, 6], [3, 4, 7]],
                        critical_path=[1, 2, 3, 8, 9],
                        levels={1: 0, 6: 0, 2: 1, 7: 1, 3: 2, 4: 2, 5: 3, 8: 4, 9: 5}
                    )
                    mock_plan_method.return_value = mock_plan
                    
                    # Execute workflow
                    result = await engine.execute_workflow(
                        workflow_id=1,
                        mode=ExecutionMode.MANUAL
                    )
                    
                    # Verify directed graph executor was used
                    assert mock_plan_method.called

    @pytest.mark.asyncio
    async def test_partial_execution_with_directed_graph(self, db_session, complex_workflow):
        """Test partial execution using directed graph algorithm."""
        engine = WorkflowExecutionEngine(db_session)
        
        # Mock previous data
        mock_previous_data = {
            "Process1": [{"data": "from_start"}]
        }
        
        with patch.object(engine, '_load_workflow', return_value=complex_workflow):
            with patch.object(engine, '_create_partial_workflow_execution') as mock_create:
                with patch.object(engine.directed_graph_executor, 'create_subgraph_execution_plan') as mock_subgraph:
                    mock_execution = MagicMock()
                    mock_execution.id = 1
                    mock_execution.uuid = str(uuid4())
                    mock_create.return_value = mock_execution
                    
                    # Mock subgraph execution plan
                    mock_subgraph_plan = {
                        "execution_order": [3, 4, 5, 8, 9],
                        "parallel_groups": [[3, 4], [5], [8], [9]],
                        "levels": {3: 0, 4: 0, 5: 1, 8: 2, 9: 3},
                        "included_nodes": [3, 4, 5, 8, 9]
                    }
                    mock_subgraph.return_value = mock_subgraph_plan
                    
                    # Execute partial workflow starting from Process2
                    result = await engine.execute_partial_workflow(
                        workflow_id=1,
                        start_node_id=3,  # Process2
                        previous_node_data=mock_previous_data,
                        mode=ExecutionMode.MANUAL
                    )
                    
                    # Verify execution plan was optimized for partial execution
                    assert mock_create.called
                    assert mock_subgraph.called