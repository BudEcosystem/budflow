"""Test workflow execution engine."""

import pytest
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, NodeType, ExecutionMode,
    ExecutionStatus, NodeExecutionStatus
)
from budflow.executor import (
    WorkflowExecutionEngine,
    ExecutionContext,
    NodeExecutionContext,
    ExecutionData,
    NodeOutputData,
    NodeRunner,
    ExecutionError,
    WorkflowExecutionError,
    CircularDependencyError,
)
from budflow.executor.runner import BaseNode, SetNode, FunctionNode, MergeNode
from budflow.auth.models import User
from budflow.auth.schemas import UserCreate
from budflow.auth.service import UserService


@pytest.mark.unit
def test_execution_data():
    """Test ExecutionData functionality."""
    # Create execution data
    exec_data = ExecutionData(
        execution_id="test-exec-123",
        input_data={"test": "input"}
    )
    
    # Test setting node output
    exec_data.set_node_output("node1", [{"result": "data"}])
    assert exec_data.get_node_output_items("node1") == [{"result": "data"}]
    
    # Test setting output with different types
    exec_data.set_node_output("node2", {"single": "item"})
    assert exec_data.get_node_output_items("node2") == [{"single": "item"}]
    
    # Test NodeOutputData directly
    node_output = NodeOutputData(items=[{"item": 1}, {"item": 2}])
    exec_data.set_node_output("node3", node_output)
    assert exec_data.get_node_output_items("node3") == [{"item": 1}, {"item": 2}]
    
    # Test context data
    exec_data.set_context("key", "value")
    assert exec_data.get_context("key") == "value"
    assert exec_data.get_context("missing", "default") == "default"
    
    # Test JSON serialization
    json_str = exec_data.to_json()
    restored = ExecutionData.from_json(json_str)
    assert restored.execution_id == exec_data.execution_id
    assert restored.get_node_output_items("node1") == [{"result": "data"}]


@pytest.mark.unit
def test_node_output_data():
    """Test NodeOutputData functionality."""
    output = NodeOutputData()
    
    # Test empty output
    assert output.is_empty()
    
    # Test adding items
    output.add_item({"key": "value"})
    assert not output.is_empty()
    assert len(output.items) == 1
    
    # Test extending items
    output.extend_items([{"key2": "value2"}, {"key3": "value3"}])
    assert len(output.items) == 3
    
    # Test JSON output
    json_data = output.get_json()
    assert json_data == [{"key": "value"}, {"key2": "value2"}, {"key3": "value3"}]


@pytest.mark.unit
def test_execution_errors():
    """Test execution error classes."""
    # Test base ExecutionError
    error = ExecutionError("Test error", error_code="TEST", details={"key": "value"})
    assert error.message == "Test error"
    assert error.error_code == "TEST"
    assert error.details["key"] == "value"
    
    error_dict = error.to_dict()
    assert error_dict["error"] == "ExecutionError"
    assert error_dict["message"] == "Test error"
    
    # Test WorkflowExecutionError
    workflow_error = WorkflowExecutionError(
        "Workflow failed",
        workflow_id=1,
        execution_id=2
    )
    assert workflow_error.workflow_id == 1
    assert workflow_error.execution_id == 2
    
    # Test CircularDependencyError
    cycle_error = CircularDependencyError(
        "Cycle detected",
        cycle_path=[1, 2, 3, 1]
    )
    assert cycle_error.cycle_path == [1, 2, 3, 1]


@pytest.mark.integration
async def test_set_node_execution(test_session: AsyncSession):
    """Test SetNode execution."""
    # Create a simple workflow with a set node
    user = await _create_test_user(test_session)
    workflow = await _create_test_workflow(test_session, user.id, "Set Node Test")
    
    # Add a set node
    set_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Set Values",
        type=NodeType.SET,
        parameters={
            "values": {
                "key1": "value1",
                "key2": "value2"
            },
            "keepOnlySet": False
        }
    )
    test_session.add(set_node)
    await test_session.commit()
    await test_session.refresh(set_node)
    
    # Create execution context
    exec_data = ExecutionData(input_data={"existing": "data"})
    node_execution = NodeExecution(
        workflow_execution_id=1,  # Mock ID
        node_id=set_node.id,
        status=NodeExecutionStatus.NEW
    )
    
    context = NodeExecutionContext(
        node=set_node,
        node_execution=node_execution,
        input_data=[{"existing": "data"}],
        execution_data=exec_data,
        parameters=set_node.parameters
    )
    
    # Execute the node
    node_runner = NodeRunner()
    result_context = await node_runner.run_node(context)
    
    # Check results
    assert result_context.status == NodeExecutionStatus.SUCCESS
    assert result_context.output_data == [
        {
            "existing": "data",
            "key1": "value1",
            "key2": "value2"
        }
    ]


@pytest.mark.integration
async def test_merge_node_execution(test_session: AsyncSession):
    """Test MergeNode execution."""
    # Create a simple workflow with a merge node
    user = await _create_test_user(test_session)
    workflow = await _create_test_workflow(test_session, user.id, "Merge Node Test")
    
    # Add a merge node
    merge_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Merge Data",
        type=NodeType.MERGE,
        parameters={"mode": "merge"}
    )
    test_session.add(merge_node)
    await test_session.commit()
    await test_session.refresh(merge_node)
    
    # Create execution context with multiple inputs
    exec_data = ExecutionData()
    node_execution = NodeExecution(
        workflow_execution_id=1,
        node_id=merge_node.id,
        status=NodeExecutionStatus.NEW
    )
    
    input_data = [
        {"key1": "value1", "common": "first"},
        {"key2": "value2", "common": "second"}
    ]
    
    context = NodeExecutionContext(
        node=merge_node,
        node_execution=node_execution,
        input_data=input_data,
        execution_data=exec_data,
        parameters=merge_node.parameters
    )
    
    # Execute the node
    node_runner = NodeRunner()
    result_context = await node_runner.run_node(context)
    
    # Check results - should merge into single item
    assert result_context.status == NodeExecutionStatus.SUCCESS
    assert len(result_context.output_data) == 1
    assert result_context.output_data[0] == {
        "key1": "value1",
        "key2": "value2",
        "common": "second"  # Later value overwrites
    }


@pytest.mark.integration
async def test_simple_workflow_execution(test_session: AsyncSession):
    """Test execution of a simple sequential workflow."""
    # Create user and workflow
    user = await _create_test_user(test_session)
    workflow = await _create_test_workflow(test_session, user.id, "Simple Workflow")
    
    # Create nodes: Manual Trigger -> Set -> Function
    trigger_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Manual Trigger",
        type=NodeType.MANUAL,
        position={"x": 100, "y": 100}
    )
    
    set_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Set Data",
        type=NodeType.SET,
        position={"x": 300, "y": 100},
        parameters={
            "values": {"processed": True, "timestamp": "2024-01-01"}
        }
    )
    
    function_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Process Data",
        type=NodeType.FUNCTION,
        position={"x": 500, "y": 100}
    )
    
    test_session.add_all([trigger_node, set_node, function_node])
    await test_session.commit()
    await test_session.refresh(trigger_node)
    await test_session.refresh(set_node)
    await test_session.refresh(function_node)
    
    # Create connections
    conn1 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=trigger_node.id,
        target_node_id=set_node.id
    )
    
    conn2 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=set_node.id,
        target_node_id=function_node.id
    )
    
    test_session.add_all([conn1, conn2])
    await test_session.commit()
    
    # Execute workflow
    engine = WorkflowExecutionEngine(test_session)
    execution = await engine.execute_workflow(
        workflow_id=workflow.id,
        mode=ExecutionMode.MANUAL,
        input_data={"initial": "data"},
        user_id=user.id
    )
    
    # Check execution results
    assert execution.status == ExecutionStatus.SUCCESS
    assert execution.workflow_id == workflow.id
    assert execution.mode == ExecutionMode.MANUAL
    
    # Check node executions
    await test_session.refresh(execution)
    # Note: Need to explicitly load node_executions
    # For now, just verify the execution completed


@pytest.mark.integration
async def test_workflow_with_disabled_node(test_session: AsyncSession):
    """Test workflow execution with disabled node."""
    # Create user and workflow
    user = await _create_test_user(test_session)
    workflow = await _create_test_workflow(test_session, user.id, "Disabled Node Test")
    
    # Create nodes with one disabled
    node1 = WorkflowNode(
        workflow_id=workflow.id,
        name="Node 1",
        type=NodeType.SET,
        parameters={"values": {"step": 1}}
    )
    
    node2 = WorkflowNode(
        workflow_id=workflow.id,
        name="Node 2 (Disabled)",
        type=NodeType.SET,
        disabled=True,  # This node is disabled
        parameters={"values": {"step": 2}}
    )
    
    node3 = WorkflowNode(
        workflow_id=workflow.id,
        name="Node 3",
        type=NodeType.SET,
        parameters={"values": {"step": 3}}
    )
    
    test_session.add_all([node1, node2, node3])
    await test_session.commit()
    await test_session.refresh(node1)
    await test_session.refresh(node2)
    await test_session.refresh(node3)
    
    # Create connections: 1 -> 2 -> 3
    conn1 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=node1.id,
        target_node_id=node2.id
    )
    
    conn2 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=node2.id,
        target_node_id=node3.id
    )
    
    test_session.add_all([conn1, conn2])
    await test_session.commit()
    
    # Execute workflow
    engine = WorkflowExecutionEngine(test_session)
    execution = await engine.execute_workflow(workflow_id=workflow.id)
    
    # Should complete successfully, skipping disabled node
    assert execution.status == ExecutionStatus.SUCCESS


@pytest.mark.integration
async def test_workflow_cycle_detection(test_session: AsyncSession):
    """Test detection of cycles in workflow."""
    # Create user and workflow
    user = await _create_test_user(test_session)
    workflow = await _create_test_workflow(test_session, user.id, "Cycle Test")
    
    # Create nodes that form a cycle
    node1 = WorkflowNode(
        workflow_id=workflow.id,
        name="Node 1",
        type=NodeType.SET
    )
    
    node2 = WorkflowNode(
        workflow_id=workflow.id,
        name="Node 2",
        type=NodeType.SET
    )
    
    node3 = WorkflowNode(
        workflow_id=workflow.id,
        name="Node 3",
        type=NodeType.SET
    )
    
    test_session.add_all([node1, node2, node3])
    await test_session.commit()
    await test_session.refresh(node1)
    await test_session.refresh(node2)
    await test_session.refresh(node3)
    
    # Create connections that form a cycle: 1 -> 2 -> 3 -> 1
    conn1 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=node1.id,
        target_node_id=node2.id
    )
    
    conn2 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=node2.id,
        target_node_id=node3.id
    )
    
    conn3 = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=node3.id,
        target_node_id=node1.id  # Creates cycle
    )
    
    test_session.add_all([conn1, conn2, conn3])
    await test_session.commit()
    
    # Try to execute workflow - should fail with cycle error
    engine = WorkflowExecutionEngine(test_session)
    
    # Execute workflow - it will fail but error is caught
    execution = await engine.execute_workflow(workflow_id=workflow.id)
    
    # Check that execution failed with error status
    assert execution.status == ExecutionStatus.ERROR
    assert execution.error is not None
    assert "circular dependencies" in execution.error["message"].lower()


@pytest.mark.integration
async def test_workflow_with_error_handling(test_session: AsyncSession):
    """Test workflow execution with node that always outputs data on error."""
    # Create user and workflow
    user = await _create_test_user(test_session)
    workflow = await _create_test_workflow(test_session, user.id, "Error Handling Test")
    
    # Create a node that will fail but has always_output_data=True
    error_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Error Node",
        type=NodeType.FUNCTION,  # Will fail as not fully implemented
        always_output_data=True,  # Continue on error
        parameters={"undefined_param": "value"}
    )
    
    next_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Next Node",
        type=NodeType.SET,
        parameters={"values": {"continued": True}}
    )
    
    test_session.add_all([error_node, next_node])
    await test_session.commit()
    await test_session.refresh(error_node)
    await test_session.refresh(next_node)
    
    # Connect nodes
    connection = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=error_node.id,
        target_node_id=next_node.id
    )
    
    test_session.add(connection)
    await test_session.commit()
    
    # Execute workflow - should continue despite error
    engine = WorkflowExecutionEngine(test_session)
    execution = await engine.execute_workflow(workflow_id=workflow.id)
    
    # Should complete (function node just passes through data)
    assert execution.status == ExecutionStatus.SUCCESS


@pytest.mark.unit
def test_execution_context_node_tracking():
    """Test ExecutionContext node tracking functionality."""
    # Create mock workflow and execution
    workflow = Workflow(id=1, name="Test", user_id=1)
    workflow_execution = WorkflowExecution(
        id=1,
        uuid="test-123",
        workflow_id=1,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.NEW
    )
    
    # Create nodes
    node1 = WorkflowNode(id=1, workflow_id=1, name="Node 1", type=NodeType.SET)
    node2 = WorkflowNode(id=2, workflow_id=1, name="Node 2", type=NodeType.SET)
    workflow.nodes = [node1, node2]
    
    # Create connection
    conn = WorkflowConnection(
        id=1,
        workflow_id=1,
        source_node_id=1,
        target_node_id=2
    )
    workflow.connections = [conn]
    
    # Create context (mock session)
    context = ExecutionContext(
        workflow=workflow,
        workflow_execution=workflow_execution,
        db_session=None  # Would be mocked in real test
    )
    
    # Initialize context to build node indices
    # Can't use await in non-async test, so manually build indices
    context.nodes_by_id = {node.id: node for node in workflow.nodes}
    for connection in workflow.connections:
        if connection.target_node_id not in context.connections_by_target:
            context.connections_by_target[connection.target_node_id] = []
        context.connections_by_target[connection.target_node_id].append(connection)
        
        if connection.source_node_id not in context.connections_by_source:
            context.connections_by_source[connection.source_node_id] = []
        context.connections_by_source[connection.source_node_id].append(connection)
    
    # Test node tracking
    assert not context.is_node_ready(2)  # Node 2 depends on Node 1
    
    context.mark_node_executed(1, success=True)
    assert 1 in context.executed_nodes
    assert context.is_node_ready(2)  # Now Node 2 is ready
    
    # Test getting nodes
    start_nodes = context.get_start_nodes()
    assert len(start_nodes) == 1
    assert start_nodes[0].id == 1
    
    next_nodes = context.get_next_nodes(1)
    assert len(next_nodes) == 1
    assert next_nodes[0].id == 2


# Helper functions
async def _create_test_user(session: AsyncSession) -> User:
    """Create a test user."""
    user_service = UserService(session)
    user_data = UserCreate(
        email=f"test_{datetime.now().timestamp()}@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Test",
        last_name="User"
    )
    return await user_service.create_user(user_data)


async def _create_test_workflow(
    session: AsyncSession,
    user_id: int,
    name: str
) -> Workflow:
    """Create a test workflow."""
    workflow = Workflow(
        name=name,
        user_id=user_id,
        status=WorkflowStatus.ACTIVE,
        settings={},
        tags=["test"]
    )
    session.add(workflow)
    await session.commit()
    await session.refresh(workflow)
    return workflow