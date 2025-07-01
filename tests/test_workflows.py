"""Test workflow models and schemas."""

import json
import pytest
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection, WorkflowExecution, NodeExecution,
    WorkflowStatus, NodeType, ExecutionMode, ExecutionStatus, NodeExecutionStatus
)
from budflow.workflows.schemas import (
    WorkflowCreate, WorkflowNodeCreate, WorkflowConnectionCreate,
    WorkflowExecutionCreate, WorkflowDefinition
)
from budflow.auth.models import User
from budflow.auth.schemas import UserCreate
from budflow.auth.service import UserService


@pytest.mark.integration
async def test_workflow_model_creation(test_session: AsyncSession):
    """Test workflow model creation and relationships."""
    # Create a user first
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="workflow@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Workflow",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    # Create workflow
    workflow = Workflow(
        name="Test Workflow",
        description="A test workflow for automation",
        user_id=user.id,
        status=WorkflowStatus.DRAFT,
        settings={"timeout": 300, "retry_failed": True},
        tags=["test", "automation"],
        timezone="UTC"
    )
    
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    assert workflow.id is not None
    assert workflow.name == "Test Workflow"
    assert workflow.user_id == user.id
    assert workflow.status == WorkflowStatus.DRAFT
    assert workflow.settings["timeout"] == 300
    assert "test" in workflow.tags
    assert workflow.is_active is False
    assert workflow.success_rate == 0.0
    
    # Test workflow representation
    repr_str = repr(workflow)
    assert "Test Workflow" in repr_str
    assert "draft" in repr_str


@pytest.mark.integration
async def test_workflow_node_creation(test_session: AsyncSession):
    """Test workflow node creation and relationships."""
    # Create user and workflow
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="node@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Node",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(
        name="Node Test Workflow",
        user_id=user.id,
        status=WorkflowStatus.DRAFT
    )
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    # Create workflow nodes
    trigger_node = WorkflowNode(
        workflow_id=workflow.id,
        name="HTTP Trigger",
        type=NodeType.TRIGGER,
        parameters={"method": "POST", "path": "/webhook"},
        position={"x": 100, "y": 100},
        notes="Webhook trigger node"
    )
    
    action_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Send Email",
        type=NodeType.ACTION,
        parameters={"to": "user@example.com", "subject": "Test"},
        credentials={"smtp_host": "smtp.gmail.com"},
        position={"x": 300, "y": 100},
        retry_on_fail=True,
        max_tries=3
    )
    
    test_session.add_all([trigger_node, action_node])
    await test_session.commit()
    await test_session.refresh(trigger_node)
    await test_session.refresh(action_node)
    
    assert trigger_node.uuid is not None
    assert trigger_node.is_trigger_node is True
    assert trigger_node.has_credentials is False
    assert action_node.has_credentials is True
    assert action_node.retry_on_fail is True
    
    # Test node representation
    repr_str = repr(trigger_node)
    assert "HTTP Trigger" in repr_str
    assert "trigger" in repr_str


@pytest.mark.integration
async def test_workflow_connection_creation(test_session: AsyncSession):
    """Test workflow connection creation."""
    # Create user, workflow, and nodes
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="connection@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Connection",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(name="Connection Test", user_id=user.id)
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    source_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Source",
        type=NodeType.TRIGGER
    )
    target_node = WorkflowNode(
        workflow_id=workflow.id,
        name="Target",
        type=NodeType.ACTION
    )
    
    test_session.add_all([source_node, target_node])
    await test_session.commit()
    await test_session.refresh(source_node)
    await test_session.refresh(target_node)
    
    # Create connection
    connection = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=source_node.id,
        target_node_id=target_node.id,
        source_output="main",
        target_input="main",
        type="main"
    )
    
    test_session.add(connection)
    await test_session.commit()
    await test_session.refresh(connection)
    
    assert connection.id is not None
    assert connection.source_node_id == source_node.id
    assert connection.target_node_id == target_node.id
    
    # Test connection representation
    repr_str = repr(connection)
    assert f"{source_node.id}->{target_node.id}" in repr_str


@pytest.mark.integration
async def test_workflow_execution_creation(test_session: AsyncSession):
    """Test workflow execution creation."""
    # Create user and workflow
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="execution@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Execution",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(name="Execution Test", user_id=user.id)
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    # Create execution
    execution = WorkflowExecution(
        workflow_id=workflow.id,
        user_id=user.id,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.NEW,
        data={"input": "test data"}
    )
    
    test_session.add(execution)
    await test_session.commit()
    await test_session.refresh(execution)
    
    assert execution.uuid is not None
    assert execution.is_running is False
    assert execution.is_finished is False
    assert execution.success_rate == 0.0
    
    # Test execution representation
    repr_str = repr(execution)
    assert execution.uuid in repr_str
    assert "new" in repr_str


@pytest.mark.integration
async def test_node_execution_creation(test_session: AsyncSession):
    """Test node execution creation."""
    # Create user, workflow, node, and workflow execution
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="nodeexec@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="NodeExec",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(name="Node Exec Test", user_id=user.id)
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    node = WorkflowNode(
        workflow_id=workflow.id,
        name="Test Node",
        type=NodeType.ACTION
    )
    test_session.add(node)
    await test_session.commit()
    await test_session.refresh(node)
    
    workflow_execution = WorkflowExecution(
        workflow_id=workflow.id,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.RUNNING
    )
    test_session.add(workflow_execution)
    await test_session.commit()
    await test_session.refresh(workflow_execution)
    
    # Create node execution
    node_execution = NodeExecution(
        workflow_execution_id=workflow_execution.id,
        node_id=node.id,
        status=NodeExecutionStatus.NEW,
        input_data={"param": "value"},
        try_number=1
    )
    
    test_session.add(node_execution)
    await test_session.commit()
    await test_session.refresh(node_execution)
    
    assert node_execution.id is not None
    assert node_execution.is_running is False
    assert node_execution.is_finished is False
    
    # Test node execution representation
    repr_str = repr(node_execution)
    assert str(node.id) in repr_str
    assert "new" in repr_str


@pytest.mark.integration
async def test_workflow_relationships(test_session: AsyncSession):
    """Test workflow model relationships."""
    # Create user
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="relationships@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Rel",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    # Create workflow with nodes and connections
    workflow = Workflow(name="Relationship Test", user_id=user.id)
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    # Add nodes
    node1 = WorkflowNode(workflow_id=workflow.id, name="Node 1", type=NodeType.TRIGGER)
    node2 = WorkflowNode(workflow_id=workflow.id, name="Node 2", type=NodeType.ACTION)
    test_session.add_all([node1, node2])
    await test_session.commit()
    await test_session.refresh(node1)
    await test_session.refresh(node2)
    
    # Add connection
    connection = WorkflowConnection(
        workflow_id=workflow.id,
        source_node_id=node1.id,
        target_node_id=node2.id
    )
    test_session.add(connection)
    await test_session.commit()
    
    # Test relationships
    await test_session.refresh(workflow)
    assert len(workflow.nodes) == 2
    assert len(workflow.connections) == 1
    assert workflow.nodes[0].name in ["Node 1", "Node 2"]
    assert workflow.connections[0].source_node_id == node1.id


@pytest.mark.unit
def test_workflow_schemas():
    """Test workflow Pydantic schemas."""
    # Test WorkflowCreate schema
    workflow_data = WorkflowCreate(
        name="Test Workflow",
        description="Test description",
        settings={"timeout": 300},
        tags=["test"],
        timezone="America/New_York"
    )
    
    assert workflow_data.name == "Test Workflow"
    assert workflow_data.status == WorkflowStatus.DRAFT
    assert workflow_data.settings["timeout"] == 300
    
    # Test WorkflowNodeCreate schema
    node_data = WorkflowNodeCreate(
        name="Test Node",
        type=NodeType.ACTION,
        parameters={"key": "value"},
        position={"x": 100, "y": 200},
        retry_on_fail=True,
        max_tries=5
    )
    
    assert node_data.name == "Test Node"
    assert node_data.type == NodeType.ACTION
    assert node_data.max_tries == 5
    
    # Test WorkflowConnectionCreate schema
    connection_data = WorkflowConnectionCreate(
        source_node_uuid="source-uuid",
        target_node_uuid="target-uuid",
        source_output="success",
        target_input="input"
    )
    
    assert connection_data.source_node_uuid == "source-uuid"
    assert connection_data.target_node_uuid == "target-uuid"


@pytest.mark.unit
def test_workflow_definition_validation():
    """Test workflow definition schema validation."""
    # Test valid workflow definition
    workflow_def = WorkflowDefinition(
        workflow=WorkflowCreate(name="Test Workflow"),
        nodes=[
            WorkflowNodeCreate(
                uuid="node1",
                name="Trigger",
                type=NodeType.TRIGGER
            ),
            WorkflowNodeCreate(
                uuid="node2", 
                name="Action",
                type=NodeType.ACTION
            )
        ],
        connections=[
            WorkflowConnectionCreate(
                source_node_uuid="node1",
                target_node_uuid="node2"
            )
        ]
    )
    
    assert len(workflow_def.nodes) == 2
    assert len(workflow_def.connections) == 1
    
    # Test invalid connection reference
    with pytest.raises(ValueError, match="Source node.*not found"):
        WorkflowDefinition(
            workflow=WorkflowCreate(name="Invalid Workflow"),
            nodes=[
                WorkflowNodeCreate(uuid="node1", name="Node", type=NodeType.ACTION)
            ],
            connections=[
                WorkflowConnectionCreate(
                    source_node_uuid="nonexistent",
                    target_node_uuid="node1"
                )
            ]
        )


@pytest.mark.integration
async def test_workflow_statistics(test_session: AsyncSession):
    """Test workflow statistics calculation."""
    # Create user and workflow
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="stats@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Stats",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(
        name="Stats Test",
        user_id=user.id,
        total_executions=10,
        successful_executions=7,
        failed_executions=3
    )
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    # Test success rate calculation
    assert workflow.success_rate == 70.0
    
    # Test with zero executions
    workflow2 = Workflow(
        name="Zero Stats",
        user_id=user.id,
        total_executions=0
    )
    test_session.add(workflow2)
    await test_session.commit()
    await test_session.refresh(workflow2)
    
    assert workflow2.success_rate == 0.0


@pytest.mark.integration
async def test_workflow_to_dict(test_session: AsyncSession):
    """Test workflow model to_dict method."""
    # Create user and workflow
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="dict@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Dict",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(
        name="Dict Test",
        description="Test description",
        user_id=user.id,
        status=WorkflowStatus.ACTIVE,
        settings={"key": "value"},
        tags=["test", "dict"],
        timezone="UTC",
        version=2,
        hash="abc123",
        total_executions=5,
        successful_executions=4,
        failed_executions=1
    )
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    # Convert to dict
    workflow_dict = workflow.to_dict()
    
    assert workflow_dict["id"] == workflow.id
    assert workflow_dict["name"] == "Dict Test"
    assert workflow_dict["status"] == "active"
    assert workflow_dict["settings"]["key"] == "value"
    assert "test" in workflow_dict["tags"]
    assert workflow_dict["success_rate"] == 80.0
    assert workflow_dict["version"] == 2


@pytest.mark.integration
async def test_workflow_cascade_deletion(test_session: AsyncSession):
    """Test cascade deletion of workflow components."""
    # Create user and workflow
    user_service = UserService(test_session)
    user_data = UserCreate(
        email="cascade@example.com",
        password="TestP@ssw0rd123",
        confirm_password="TestP@ssw0rd123",
        first_name="Cascade",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    workflow = Workflow(name="Cascade Test", user_id=user.id)
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)
    
    # Add node
    node = WorkflowNode(
        workflow_id=workflow.id,
        name="Test Node",
        type=NodeType.ACTION
    )
    test_session.add(node)
    await test_session.commit()
    await test_session.refresh(node)
    
    # Add execution
    execution = WorkflowExecution(
        workflow_id=workflow.id,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.SUCCESS
    )
    test_session.add(execution)
    await test_session.commit()
    await test_session.refresh(execution)
    
    # Add node execution
    node_execution = NodeExecution(
        workflow_execution_id=execution.id,
        node_id=node.id,
        status=NodeExecutionStatus.SUCCESS
    )
    test_session.add(node_execution)
    await test_session.commit()
    
    # Verify components exist
    node_count = await test_session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_id == workflow.id)
    )
    execution_count = await test_session.scalar(
        select(WorkflowExecution).where(WorkflowExecution.workflow_id == workflow.id)
    )
    assert node_count is not None
    assert execution_count is not None
    
    # Delete workflow
    await test_session.delete(workflow)
    await test_session.commit()
    
    # Verify cascade deletion
    remaining_nodes = await test_session.scalar(
        select(WorkflowNode).where(WorkflowNode.workflow_id == workflow.id)
    )
    remaining_executions = await test_session.scalar(
        select(WorkflowExecution).where(WorkflowExecution.workflow_id == workflow.id)
    )
    assert remaining_nodes is None
    assert remaining_executions is None