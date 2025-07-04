"""
Comprehensive tests for parallel node execution functionality.

Parallel execution allows multiple independent nodes to execute simultaneously,
significantly improving workflow performance for non-dependent operations.
"""

import pytest
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch, call
from uuid import uuid4
from collections import defaultdict

from sqlalchemy.ext.asyncio import AsyncSession
import networkx as nx

from budflow.executor.engine import WorkflowExecutionEngine
from budflow.executor.parallel import (
    ParallelExecutionManager,
    ExecutionGraph,
    NodeDependencyResolver,
    ExecutionScheduler,
    ParallelExecutionStrategy,
    ResourcePool,
    ExecutionSlot
)
from budflow.executor.errors import (
    WorkflowExecutionError,
    CircularDependencyError,
    ResourceExhaustedError
)
from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, ExecutionStatus, NodeExecutionStatus,
    ExecutionMode, NodeType
)
from budflow.nodes.base import BaseNode, NodeDefinition
from budflow.config import settings


class SlowMockNode(BaseNode):
    """Mock node that simulates slow execution."""
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        return NodeDefinition(
            name="SlowMockNode",
            type=NodeType.ACTION,
            description="Slow mock node for testing",
            inputs=["main"],
            outputs=["main"]
        )
    
    async def execute(self) -> List[Dict[str, Any]]:
        # Simulate slow processing
        await asyncio.sleep(0.1)
        input_data = self.get_input_data()
        return [{**item, "processed_by": self.node_name} for item in input_data]


class FastMockNode(BaseNode):
    """Mock node that executes quickly."""
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        return NodeDefinition(
            name="FastMockNode",
            type=NodeType.ACTION,
            description="Fast mock node for testing",
            inputs=["main"],
            outputs=["main"]
        )
    
    async def execute(self) -> List[Dict[str, Any]]:
        # Fast execution
        await asyncio.sleep(0.01)
        input_data = self.get_input_data()
        return [{**item, "processed_by": self.node_name} for item in input_data]


class TestParallelExecutionCore:
    """Test core parallel execution functionality."""
    
    @pytest.fixture
    def parallel_manager(self):
        """Create parallel execution manager."""
        return ParallelExecutionManager(max_parallel_nodes=3)
    
    @pytest.fixture
    def dependency_resolver(self):
        """Create dependency resolver."""
        return NodeDependencyResolver()
    
    @pytest.fixture
    def execution_scheduler(self):
        """Create execution scheduler."""
        return ExecutionScheduler()
    
    @pytest.fixture
    def resource_pool(self):
        """Create resource pool for limiting parallelism."""
        return ResourcePool(max_slots=3)
    
    @pytest.fixture
    def parallel_workflow(self):
        """Create a workflow suitable for parallel execution."""
        workflow = Workflow(
            id=1,
            uuid=uuid4(),
            name="Parallel Test Workflow",
            status=WorkflowStatus.ACTIVE
        )
        
        # Create nodes that can run in parallel:
        #         -> Process1 ->
        # Start ->              -> Merge -> End
        #         -> Process2 ->
        #         -> Process3 ->
        nodes = [
            WorkflowNode(id=1, name="Start", type="FastMockNode"),
            WorkflowNode(id=2, name="Process1", type="SlowMockNode"),
            WorkflowNode(id=3, name="Process2", type="SlowMockNode"),
            WorkflowNode(id=4, name="Process3", type="SlowMockNode"),
            WorkflowNode(id=5, name="Merge", type="FastMockNode"),
            WorkflowNode(id=6, name="End", type="FastMockNode")
        ]
        
        connections = [
            # Start to all process nodes
            WorkflowConnection(source_node_id=1, target_node_id=2),
            WorkflowConnection(source_node_id=1, target_node_id=3),
            WorkflowConnection(source_node_id=1, target_node_id=4),
            # All process nodes to merge
            WorkflowConnection(source_node_id=2, target_node_id=5),
            WorkflowConnection(source_node_id=3, target_node_id=5),
            WorkflowConnection(source_node_id=4, target_node_id=5),
            # Merge to end
            WorkflowConnection(source_node_id=5, target_node_id=6)
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow
    
    @pytest.mark.asyncio
    async def test_dependency_graph_creation(self, dependency_resolver, parallel_workflow):
        """Test creation of dependency graph from workflow."""
        graph = await dependency_resolver.create_dependency_graph(parallel_workflow)
        
        # Verify graph structure
        assert graph.number_of_nodes() == 6
        assert graph.number_of_edges() == 7
        
        # Check dependencies
        assert list(graph.predecessors(2)) == [1]  # Process1 depends on Start
        assert list(graph.predecessors(3)) == [1]  # Process2 depends on Start
        assert list(graph.predecessors(4)) == [1]  # Process3 depends on Start
        assert set(graph.predecessors(5)) == {2, 3, 4}  # Merge depends on all processes
    
    @pytest.mark.asyncio
    async def test_parallel_node_identification(self, dependency_resolver, parallel_workflow):
        """Test identification of nodes that can run in parallel."""
        graph = await dependency_resolver.create_dependency_graph(parallel_workflow)
        
        # After Start completes, find parallelizable nodes
        completed_nodes = {1}  # Start is done
        parallel_nodes = await dependency_resolver.get_executable_nodes(
            graph, completed_nodes
        )
        
        # Process1, Process2, Process3 should all be executable
        assert parallel_nodes == {2, 3, 4}
        
        # After some processes complete
        completed_nodes = {1, 2, 3}  # Start, Process1, Process2 done
        parallel_nodes = await dependency_resolver.get_executable_nodes(
            graph, completed_nodes
        )
        
        # Only Process3 should be executable (Merge needs all three)
        assert parallel_nodes == {4}
    
    @pytest.mark.asyncio
    async def test_execution_scheduling(self, execution_scheduler, parallel_workflow):
        """Test scheduling of node execution."""
        graph = nx.DiGraph()
        graph.add_edges_from([
            (1, 2), (1, 3), (1, 4),
            (2, 5), (3, 5), (4, 5),
            (5, 6)
        ])
        
        # Create execution schedule
        schedule = await execution_scheduler.create_execution_schedule(graph)
        
        # Verify schedule levels
        assert len(schedule) == 4  # 4 levels of execution
        assert schedule[0] == [1]  # Level 0: Start
        assert set(schedule[1]) == {2, 3, 4}  # Level 1: Parallel processes
        assert schedule[2] == [5]  # Level 2: Merge
        assert schedule[3] == [6]  # Level 3: End
    
    @pytest.mark.asyncio
    async def test_resource_pool_management(self, resource_pool):
        """Test resource pool slot management."""
        # Acquire slots
        slot1 = await resource_pool.acquire_slot()
        slot2 = await resource_pool.acquire_slot()
        slot3 = await resource_pool.acquire_slot()
        
        assert resource_pool.available_slots == 0
        
        # Try to acquire when exhausted
        with pytest.raises(ResourceExhaustedError):
            await resource_pool.acquire_slot(timeout=0.1)
        
        # Release a slot
        await resource_pool.release_slot(slot1)
        assert resource_pool.available_slots == 1
        
        # Can acquire again
        slot4 = await resource_pool.acquire_slot()
        assert slot4 is not None
    
    @pytest.mark.asyncio
    async def test_parallel_execution_with_limited_resources(self, parallel_manager, resource_pool):
        """Test parallel execution with resource constraints."""
        # Set up limited resources (only 2 parallel)
        parallel_manager.resource_pool = ResourcePool(max_slots=2)
        
        # Track execution order
        execution_order = []
        
        async def mock_execute_node(node_id: int):
            execution_order.append(f"start_{node_id}")
            await asyncio.sleep(0.1)  # Simulate work
            execution_order.append(f"end_{node_id}")
            return [{"data": f"result_{node_id}"}]
        
        # Execute 3 nodes in parallel (but only 2 slots)
        tasks = [
            parallel_manager.execute_node_async(1, mock_execute_node),
            parallel_manager.execute_node_async(2, mock_execute_node),
            parallel_manager.execute_node_async(3, mock_execute_node)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Verify results
        assert len(results) == 3
        
        # Check that at most 2 were running simultaneously
        parallel_count = 0
        max_parallel = 0
        for event in execution_order:
            if event.startswith("start_"):
                parallel_count += 1
                max_parallel = max(max_parallel, parallel_count)
            else:  # end_
                parallel_count -= 1
        
        assert max_parallel <= 2
    
    @pytest.mark.asyncio
    async def test_parallel_execution_error_handling(self, parallel_manager):
        """Test error handling in parallel execution."""
        async def failing_node(node_id: int):
            if node_id == 2:
                raise Exception(f"Node {node_id} failed")
            return [{"data": f"result_{node_id}"}]
        
        # Execute nodes where one fails
        tasks = [
            parallel_manager.execute_node_async(1, failing_node),
            parallel_manager.execute_node_async(2, failing_node),  # This will fail
            parallel_manager.execute_node_async(3, failing_node)
        ]
        
        # Gather with return_exceptions to handle failures
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check results
        assert isinstance(results[0], list)  # Node 1 succeeded
        assert isinstance(results[1], Exception)  # Node 2 failed
        assert isinstance(results[2], list)  # Node 3 succeeded
        assert str(results[1]) == "Node 2 failed"
    
    @pytest.mark.asyncio
    async def test_circular_dependency_detection(self, dependency_resolver):
        """Test detection of circular dependencies."""
        # Create workflow with circular dependency
        workflow = Workflow(id=1, name="Circular")
        
        nodes = [
            WorkflowNode(id=1, name="A"),
            WorkflowNode(id=2, name="B"),
            WorkflowNode(id=3, name="C")
        ]
        
        # A -> B -> C -> A (circular)
        connections = [
            WorkflowConnection(source_node_id=1, target_node_id=2),
            WorkflowConnection(source_node_id=2, target_node_id=3),
            WorkflowConnection(source_node_id=3, target_node_id=1)
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        # Should raise error when creating graph
        with pytest.raises(CircularDependencyError):
            await dependency_resolver.create_dependency_graph(workflow)


class TestParallelExecutionStrategies:
    """Test different parallel execution strategies."""
    
    @pytest.mark.asyncio
    async def test_breadth_first_strategy(self, execution_scheduler):
        """Test breadth-first execution strategy."""
        graph = nx.DiGraph()
        graph.add_edges_from([
            (1, 2), (1, 3),
            (2, 4), (2, 5),
            (3, 6), (3, 7)
        ])
        
        strategy = ParallelExecutionStrategy.BREADTH_FIRST
        schedule = await execution_scheduler.create_execution_schedule(
            graph, strategy=strategy
        )
        
        # Breadth-first: execute all nodes at same depth level
        assert schedule[0] == [1]
        assert set(schedule[1]) == {2, 3}
        assert set(schedule[2]) == {4, 5, 6, 7}
    
    @pytest.mark.asyncio
    async def test_priority_based_strategy(self, execution_scheduler):
        """Test priority-based execution strategy."""
        graph = nx.DiGraph()
        graph.add_edges_from([(1, 2), (1, 3), (1, 4)])
        
        # Add priorities to nodes
        nx.set_node_attributes(graph, {
            1: {"priority": 1},
            2: {"priority": 3},  # Highest priority
            3: {"priority": 2},
            4: {"priority": 1}
        }, "priority")
        
        strategy = ParallelExecutionStrategy.PRIORITY_BASED
        schedule = await execution_scheduler.create_execution_schedule(
            graph, strategy=strategy
        )
        
        # Should execute in priority order when possible
        assert schedule[1][0] == 2  # Highest priority first
    
    @pytest.mark.asyncio
    async def test_resource_aware_strategy(self, execution_scheduler):
        """Test resource-aware execution strategy."""
        graph = nx.DiGraph()
        graph.add_edges_from([(1, 2), (1, 3), (1, 4)])
        
        # Add resource requirements
        nx.set_node_attributes(graph, {
            1: {"cpu": 1, "memory": 100},
            2: {"cpu": 2, "memory": 500},  # Heavy node
            3: {"cpu": 1, "memory": 200},
            4: {"cpu": 1, "memory": 150}
        })
        
        strategy = ParallelExecutionStrategy.RESOURCE_AWARE
        available_resources = {"cpu": 3, "memory": 600}
        
        schedule = await execution_scheduler.create_resource_aware_schedule(
            graph, strategy=strategy, resources=available_resources
        )
        
        # Should schedule nodes that fit within resources
        # Node 2 (heavy) might run alone, while 3 and 4 run together
        assert len(schedule) > 0


class TestParallelExecutionIntegration:
    """Test parallel execution integration with workflow engine."""
    
    @pytest.fixture
    async def parallel_engine(self, db_session):
        """Create execution engine with parallel support."""
        engine = WorkflowExecutionEngine(db_session)
        engine.parallel_manager = ParallelExecutionManager(max_parallel_nodes=3)
        return engine
    
    @pytest.mark.asyncio
    async def test_workflow_parallel_execution(self, parallel_engine, parallel_workflow):
        """Test complete workflow execution with parallelism."""
        engine = parallel_engine
        
        # Track execution times
        node_execution_times = {}
        
        async def track_execution(node, *args, **kwargs):
            start = time.time()
            result = await node.execute(*args, **kwargs)
            node_execution_times[node.node_name] = time.time() - start
            return result
        
        with patch.object(engine, '_load_workflow', return_value=parallel_workflow):
            with patch('budflow.nodes.base.BaseNode.execute', side_effect=track_execution):
                start_time = time.time()
                
                result = await engine.execute_workflow_parallel(
                    workflow_id=1,
                    mode=ExecutionMode.MANUAL
                )
                
                total_time = time.time() - start_time
        
        # Verify parallel execution saved time
        # If run sequentially, would take sum of all execution times
        sequential_time = sum(node_execution_times.values())
        
        # Parallel execution should be significantly faster
        assert total_time < sequential_time * 0.7  # At least 30% faster
    
    @pytest.mark.asyncio
    async def test_parallel_execution_with_failures(self, parallel_engine, parallel_workflow):
        """Test parallel execution handling node failures."""
        engine = parallel_engine
        
        # Make Process2 fail
        async def mock_execute_node(node):
            if node.name == "Process2":
                raise Exception("Process2 failed")
            return [{"data": f"result_{node.name}"}]
        
        with patch.object(engine, '_load_workflow', return_value=parallel_workflow):
            with patch.object(engine, '_execute_node', side_effect=mock_execute_node):
                with pytest.raises(WorkflowExecutionError) as exc_info:
                    await engine.execute_workflow_parallel(
                        workflow_id=1,
                        mode=ExecutionMode.MANUAL
                    )
        
        # Should indicate which node failed
        assert "Process2" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_parallel_execution_progress_tracking(self, parallel_engine, parallel_workflow):
        """Test progress tracking during parallel execution."""
        engine = parallel_engine
        progress_updates = []
        
        async def track_progress(execution_id, node_id, status):
            progress_updates.append({
                "node_id": node_id,
                "status": status,
                "timestamp": time.time()
            })
        
        engine.parallel_manager.on_node_status_change = track_progress
        
        with patch.object(engine, '_load_workflow', return_value=parallel_workflow):
            await engine.execute_workflow_parallel(
                workflow_id=1,
                mode=ExecutionMode.MANUAL
            )
        
        # Verify progress updates
        assert len(progress_updates) > 0
        
        # Check that parallel nodes started close together
        parallel_starts = [
            u for u in progress_updates
            if u["node_id"] in [2, 3, 4] and u["status"] == "started"
        ]
        
        if len(parallel_starts) >= 2:
            time_diff = max(u["timestamp"] for u in parallel_starts) - \
                       min(u["timestamp"] for u in parallel_starts)
            assert time_diff < 0.1  # Started within 100ms
    
    @pytest.mark.asyncio
    async def test_parallel_execution_memory_efficiency(self, parallel_engine):
        """Test memory efficiency of parallel execution."""
        # Create workflow with many parallel branches
        workflow = Workflow(id=1, name="Memory Test")
        
        # Start node + 10 parallel nodes + end node
        nodes = [WorkflowNode(id=1, name="Start", type="FastMockNode")]
        for i in range(2, 12):
            nodes.append(WorkflowNode(id=i, name=f"Process{i}", type="FastMockNode"))
        nodes.append(WorkflowNode(id=12, name="End", type="FastMockNode"))
        
        connections = []
        # Connect start to all process nodes
        for i in range(2, 12):
            connections.append(WorkflowConnection(source_node_id=1, target_node_id=i))
        # Connect all process nodes to end
        for i in range(2, 12):
            connections.append(WorkflowConnection(source_node_id=i, target_node_id=12))
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        # Limit parallel execution to control memory
        parallel_engine.parallel_manager.max_parallel_nodes = 3
        
        with patch.object(parallel_engine, '_load_workflow', return_value=workflow):
            # Should complete without memory issues
            result = await parallel_engine.execute_workflow_parallel(
                workflow_id=1,
                mode=ExecutionMode.MANUAL
            )
            
            assert result.status == ExecutionStatus.SUCCESS


class TestParallelExecutionOptimizations:
    """Test optimizations for parallel execution."""
    
    @pytest.mark.asyncio
    async def test_node_batching_optimization(self, parallel_manager):
        """Test batching of small nodes for efficiency."""
        # Many small nodes that could be batched
        small_nodes = []
        for i in range(10):
            node = MagicMock()
            node.estimated_duration = 0.01  # Very fast
            small_nodes.append(node)
        
        # Should batch small nodes together
        batches = await parallel_manager.create_node_batches(
            small_nodes,
            batch_threshold=0.1  # Batch if total < 100ms
        )
        
        # Should create fewer batches than nodes
        assert len(batches) < len(small_nodes)
        assert len(batches) <= 3  # With our batch threshold
    
    @pytest.mark.asyncio
    async def test_dynamic_parallelism_adjustment(self, parallel_manager):
        """Test dynamic adjustment of parallelism based on load."""
        # Simulate system load monitoring
        async def get_system_load():
            return {
                "cpu": 75,  # 75% CPU usage
                "memory": 60,  # 60% memory usage
                "io_wait": 20  # 20% I/O wait
            }
        
        parallel_manager.get_system_load = get_system_load
        
        # Should reduce parallelism under high load
        original_max = parallel_manager.max_parallel_nodes
        await parallel_manager.adjust_parallelism_for_load()
        
        assert parallel_manager.max_parallel_nodes < original_max
    
    @pytest.mark.asyncio
    async def test_predictive_scheduling(self, execution_scheduler):
        """Test predictive scheduling based on historical data."""
        # Historical execution times
        historical_data = {
            "Process1": {"avg_duration": 1.0, "variance": 0.1},
            "Process2": {"avg_duration": 2.0, "variance": 0.2},
            "Process3": {"avg_duration": 0.5, "variance": 0.05}
        }
        
        # Create optimal schedule based on predictions
        graph = nx.DiGraph()
        graph.add_edges_from([(1, 2), (1, 3), (1, 4)])
        
        schedule = await execution_scheduler.create_predictive_schedule(
            graph,
            historical_data=historical_data,
            optimization_goal="minimize_total_time"
        )
        
        # Should prioritize longer-running nodes
        # to maximize parallelism efficiency
        assert schedule is not None