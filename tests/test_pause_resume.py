"""Test pause/resume execution functionality."""

import pytest
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from budflow.executions.pause_resume import (
    PauseResumeEngine,
    PauseInfo,
    PauseType,
    ExecutionState,
    ExecutionPauseData,
    ExecutionNotPausedError,
    PauseTimeoutError
)
from budflow.workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode
from budflow.executions.schemas import ExecutionResume


@pytest.fixture
def pause_resume_engine():
    """Create a PauseResumeEngine for testing."""
    mock_db_session = AsyncMock()
    return PauseResumeEngine(mock_db_session)


@pytest.fixture
def sample_workflow_with_wait_node():
    """Create a workflow with wait node for testing."""
    workflow = Workflow(
        id=1,
        name="Workflow with Wait Node",
        nodes=[
            {"id": "start", "name": "Start Node", "type": "manual.trigger", "position": [100, 100]},
            {"id": "wait1", "name": "Wait Node", "type": "wait", "position": [200, 100], "parameters": {
                "wait_config": {"type": "webhook", "timeout": 3600}
            }},
            {"id": "end", "name": "End Node", "type": "http.request", "position": [300, 100]}
        ],
        connections=[
            {"source": "start", "target": "wait1"},
            {"source": "wait1", "target": "end"}
        ]
    )
    return workflow


@pytest.fixture
def sample_execution():
    """Create a sample execution for testing."""
    return WorkflowExecution(
        id=1,
        workflow_id=1,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        data={"start": [{"test": "data"}]}
    )


@pytest.fixture
def sample_paused_execution():
    """Create a sample paused execution for testing."""
    return WorkflowExecution(
        id=2,
        workflow_id=1,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.WAITING,
        started_at=datetime.now(timezone.utc),
        data={"start": [{"test": "data"}]}
    )


class TestPauseConditionChecking:
    """Test pause condition detection and evaluation."""

    async def test_check_pause_conditions_wait_node_webhook(self, pause_resume_engine, sample_workflow_with_wait_node):
        """Test pause condition detection for webhook wait node."""
        node = WorkflowNode(
            name="wait1",
            type="wait",
            parameters={
                "wait_config": {"type": "webhook", "timeout": 3600}
            }
        )
        
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=sample_workflow_with_wait_node,
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        pause_info = await pause_resume_engine.check_pause_conditions(node, execution_state)
        
        assert pause_info is not None
        assert pause_info.type == PauseType.WEBHOOK
        assert pause_info.timeout == 3600
        assert "webhook" in pause_info.webhook_url

    async def test_check_pause_conditions_wait_node_manual(self, pause_resume_engine, sample_workflow_with_wait_node):
        """Test pause condition detection for manual wait node."""
        node = WorkflowNode(
            name="wait1",
            type="wait",
            parameters={
                "wait_config": {"type": "manual", "message": "Please approve", "timeout": 7200}
            }
        )
        
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=sample_workflow_with_wait_node,
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        pause_info = await pause_resume_engine.check_pause_conditions(node, execution_state)
        
        assert pause_info is not None
        assert pause_info.type == PauseType.MANUAL
        assert pause_info.message == "Please approve"
        assert pause_info.timeout == 7200

    async def test_check_pause_conditions_wait_node_time(self, pause_resume_engine, sample_workflow_with_wait_node):
        """Test pause condition detection for time-based wait node."""
        node = WorkflowNode(
            name="wait1",
            type="wait",
            parameters={
                "wait_config": {"type": "time", "seconds": 300}
            }
        )
        
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=sample_workflow_with_wait_node,
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        pause_info = await pause_resume_engine.check_pause_conditions(node, execution_state)
        
        assert pause_info is not None
        assert pause_info.type == PauseType.TIMER
        assert pause_info.resume_at > datetime.now(timezone.utc)

    async def test_check_pause_conditions_approval_node(self, pause_resume_engine, sample_workflow_with_wait_node):
        """Test pause condition detection for approval-required node."""
        node = WorkflowNode(
            name="approval1",
            type="http.request",
            parameters={
                "require_approval": True,
                "approver": "admin@example.com",
                "approval_message": "Please approve this HTTP request"
            }
        )
        
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=sample_workflow_with_wait_node,
            current_node="approval1",
            completed_nodes=set(),
            node_data={},
            pause_data=None
        )
        
        pause_info = await pause_resume_engine.check_pause_conditions(node, execution_state)
        
        assert pause_info is not None
        assert pause_info.type == PauseType.APPROVAL
        assert pause_info.approver == "admin@example.com"
        assert pause_info.message == "Please approve this HTTP request"

    async def test_check_pause_conditions_no_pause(self, pause_resume_engine, sample_workflow_with_wait_node):
        """Test no pause condition for regular node."""
        node = WorkflowNode(
            name="regular1",
            type="http.request",
            parameters={"url": "https://api.example.com"}
        )
        
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=sample_workflow_with_wait_node,
            current_node="regular1",
            completed_nodes=set(),
            node_data={},
            pause_data=None
        )
        
        pause_info = await pause_resume_engine.check_pause_conditions(node, execution_state)
        
        assert pause_info is None


class TestPauseExecution:
    """Test execution pausing functionality."""

    async def test_pause_execution_webhook_wait(self, pause_resume_engine):
        """Test pausing execution for webhook wait."""
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=Mock(),
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        pause_info = PauseInfo(
            type=PauseType.WEBHOOK,
            webhook_url="https://test.com/webhook/resume/test-exec-123",
            timeout=3600,
            expected_data_schema={"type": "object"}
        )
        
        # Mock database operations
        pause_resume_engine.serialize_execution_state = AsyncMock(return_value={"serialized": "state"})
        pause_resume_engine.store_pause_data = AsyncMock()
        pause_resume_engine.setup_resume_triggers = AsyncMock()
        pause_resume_engine.update_execution_status = AsyncMock()
        
        await pause_resume_engine.pause_execution(execution_state, pause_info)
        
        # Verify pause data was stored
        pause_resume_engine.store_pause_data.assert_called_once()
        stored_data = pause_resume_engine.store_pause_data.call_args[0][0]
        assert stored_data.execution_id == "test-exec-123"
        assert stored_data.pause_type == PauseType.WEBHOOK
        assert stored_data.pause_node == "wait1"
        
        # Verify resume triggers were set up
        pause_resume_engine.setup_resume_triggers.assert_called_once_with(
            pause_info, "test-exec-123"
        )
        
        # Verify execution status was updated
        pause_resume_engine.update_execution_status.assert_called_once_with(
            "test-exec-123", ExecutionStatus.WAITING, pause_info=pause_info
        )

    async def test_pause_execution_manual_approval(self, pause_resume_engine):
        """Test pausing execution for manual approval."""
        execution_state = ExecutionState(
            execution_id="test-exec-456",
            workflow=Mock(),
            current_node="approval1",
            completed_nodes={"start", "process"},
            node_data={"start": [{"id": 1}], "process": [{"processed": True}]},
            pause_data=None
        )
        
        pause_info = PauseInfo(
            type=PauseType.APPROVAL,
            approver="admin@example.com",
            message="Please approve this action",
            timeout=7200
        )
        
        # Mock database operations
        pause_resume_engine.serialize_execution_state = AsyncMock(return_value={"serialized": "state"})
        pause_resume_engine.store_pause_data = AsyncMock()
        pause_resume_engine.setup_resume_triggers = AsyncMock()
        pause_resume_engine.update_execution_status = AsyncMock()
        
        await pause_resume_engine.pause_execution(execution_state, pause_info)
        
        # Verify pause data was stored with correct approver
        pause_resume_engine.store_pause_data.assert_called_once()
        stored_data = pause_resume_engine.store_pause_data.call_args[0][0]
        assert stored_data.pause_metadata["approver"] == "admin@example.com"
        assert stored_data.pause_metadata["message"] == "Please approve this action"

    async def test_pause_execution_with_timeout(self, pause_resume_engine):
        """Test pausing execution with timeout calculation."""
        execution_state = ExecutionState(
            execution_id="test-exec-789",
            workflow=Mock(),
            current_node="wait1",
            completed_nodes=set(),
            node_data={},
            pause_data=None
        )
        
        pause_info = PauseInfo(
            type=PauseType.MANUAL,
            message="Manual intervention required",
            timeout=1800  # 30 minutes
        )
        
        # Mock current time
        mock_now = datetime.now()
        with patch('budflow.executions.pause_resume.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_now
            
            # Mock database operations
            pause_resume_engine.serialize_execution_state = AsyncMock(return_value={"serialized": "state"})
            pause_resume_engine.store_pause_data = AsyncMock()
            pause_resume_engine.setup_resume_triggers = AsyncMock()
            pause_resume_engine.update_execution_status = AsyncMock()
            
            await pause_resume_engine.pause_execution(execution_state, pause_info)
            
            # Verify timeout was calculated correctly
            stored_data = pause_resume_engine.store_pause_data.call_args[0][0]
            expected_expires_at = mock_now + timedelta(seconds=1800)
            assert stored_data.expires_at == expected_expires_at


class TestResumeExecution:
    """Test execution resuming functionality."""

    async def test_resume_execution_simple(self, pause_resume_engine):
        """Test resuming a simple paused execution."""
        execution_id = "test-exec-123"
        resume_data = {"webhook_data": {"result": "approved"}}
        
        # Mock pause data
        mock_pause_data = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.WEBHOOK,
            pause_node="wait1",
            serialized_state={"current_node": "wait1", "completed_nodes": ["start"]},
            pause_metadata={"webhook_url": "https://test.com/webhook"},
            created_at=datetime.now(),
            expires_at=None
        )
        
        # Mock execution state
        mock_execution_state = ExecutionState(
            execution_id=execution_id,
            workflow=Mock(),
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        # Mock methods
        pause_resume_engine.get_pause_data = AsyncMock(return_value=mock_pause_data)
        pause_resume_engine.validate_resume_conditions = AsyncMock()
        pause_resume_engine.deserialize_execution_state = AsyncMock(return_value=mock_execution_state)
        pause_resume_engine.merge_resume_data = AsyncMock(return_value=mock_execution_state)
        pause_resume_engine.cleanup_pause_data = AsyncMock()
        pause_resume_engine.continue_execution_from_state = AsyncMock(return_value=Mock(status=ExecutionStatus.SUCCESS))
        
        result = await pause_resume_engine.resume_execution(execution_id, resume_data)
        
        # Verify all steps were called
        pause_resume_engine.get_pause_data.assert_called_once_with(execution_id)
        pause_resume_engine.validate_resume_conditions.assert_called_once_with(mock_pause_data, resume_data)
        pause_resume_engine.deserialize_execution_state.assert_called_once()
        pause_resume_engine.merge_resume_data.assert_called_once_with(
            mock_execution_state, resume_data, "wait1"
        )
        pause_resume_engine.cleanup_pause_data.assert_called_once_with(execution_id)
        pause_resume_engine.continue_execution_from_state.assert_called_once_with(mock_execution_state)

    async def test_resume_execution_not_paused_error(self, pause_resume_engine):
        """Test error when trying to resume non-paused execution."""
        execution_id = "test-exec-999"
        
        # Mock no pause data found
        pause_resume_engine.get_pause_data = AsyncMock(return_value=None)
        
        with pytest.raises(ExecutionNotPausedError) as exc_info:
            await pause_resume_engine.resume_execution(execution_id)
        
        assert f"Execution {execution_id} is not paused" in str(exc_info.value)

    async def test_resume_execution_with_data_merging(self, pause_resume_engine):
        """Test resuming execution with data merging."""
        execution_id = "test-exec-456"
        resume_data = {
            "approval_result": "approved",
            "additional_data": {"user_id": 123, "notes": "Approved by admin"}
        }
        
        # Mock pause data
        mock_pause_data = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.APPROVAL,
            pause_node="approval1",
            serialized_state={"current_node": "approval1"},
            pause_metadata={"approver": "admin@example.com"},
            created_at=datetime.now(),
            expires_at=None
        )
        
        # Mock execution state
        mock_execution_state = ExecutionState(
            execution_id=execution_id,
            workflow=Mock(),
            current_node="approval1",
            completed_nodes={"start"},
            node_data={"start": [{"id": 1}]},
            pause_data=None
        )
        
        # Mock methods
        pause_resume_engine.get_pause_data = AsyncMock(return_value=mock_pause_data)
        pause_resume_engine.validate_resume_conditions = AsyncMock()
        pause_resume_engine.deserialize_execution_state = AsyncMock(return_value=mock_execution_state)
        pause_resume_engine.cleanup_pause_data = AsyncMock()
        pause_resume_engine.continue_execution_from_state = AsyncMock(return_value=Mock())
        
        # Mock merge_resume_data to verify data merging
        async def mock_merge_resume_data(execution_state, resume_data, pause_node):
            execution_state.node_data[pause_node] = [resume_data]
            return execution_state
        
        pause_resume_engine.merge_resume_data = AsyncMock(side_effect=mock_merge_resume_data)
        
        await pause_resume_engine.resume_execution(execution_id, resume_data)
        
        # Verify data was merged correctly
        pause_resume_engine.merge_resume_data.assert_called_once_with(
            mock_execution_state, resume_data, "approval1"
        )


class TestExecutionStateSerialization:
    """Test execution state serialization and deserialization."""

    async def test_serialize_execution_state(self, pause_resume_engine):
        """Test serialization of execution state."""
        execution_state = ExecutionState(
            execution_id="test-exec-123",
            workflow=Mock(id=1, name="Test Workflow"),
            current_node="wait1",
            completed_nodes={"start", "process"},
            node_data={
                "start": [{"id": 1, "data": "test"}],
                "process": [{"processed": True, "result": 42}]
            },
            pause_data=None
        )
        
        # Mock serialization
        pause_resume_engine.serialize_workflow = Mock(return_value={"id": 1, "name": "Test Workflow"})
        
        serialized = await pause_resume_engine.serialize_execution_state(execution_state)
        
        assert serialized["execution_id"] == "test-exec-123"
        assert serialized["current_node"] == "wait1"
        assert set(serialized["completed_nodes"]) == {"start", "process"}
        assert serialized["node_data"]["start"] == [{"id": 1, "data": "test"}]
        assert serialized["node_data"]["process"] == [{"processed": True, "result": 42}]

    async def test_deserialize_execution_state(self, pause_resume_engine):
        """Test deserialization of execution state."""
        serialized_state = {
            "execution_id": "test-exec-456",
            "current_node": "approval1",
            "completed_nodes": ["start", "transform"],
            "node_data": {
                "start": [{"input": "data"}],
                "transform": [{"transformed": True}]
            },
            "workflow": {"id": 2, "name": "Approval Workflow"}
        }
        
        # Mock deserialization
        mock_workflow = Mock(id=2, name="Approval Workflow")
        pause_resume_engine.deserialize_workflow = Mock(return_value=mock_workflow)
        
        execution_state = await pause_resume_engine.deserialize_execution_state(serialized_state)
        
        assert execution_state.execution_id == "test-exec-456"
        assert execution_state.current_node == "approval1"
        assert execution_state.completed_nodes == {"start", "transform"}
        assert execution_state.node_data["start"] == [{"input": "data"}]
        assert execution_state.node_data["transform"] == [{"transformed": True}]
        assert execution_state.workflow == mock_workflow


class TestResumeWebhooks:
    """Test webhook-based resume functionality."""

    async def test_generate_resume_webhook_url(self, pause_resume_engine):
        """Test generation of unique resume webhook URLs."""
        execution_id = "test-exec-123"
        
        webhook_url = pause_resume_engine.generate_resume_webhook_url(execution_id)
        
        assert execution_id in webhook_url
        assert "webhook" in webhook_url
        assert "resume" in webhook_url

    async def test_setup_resume_webhook_trigger(self, pause_resume_engine):
        """Test setup of webhook resume trigger."""
        execution_id = "test-exec-456"
        pause_info = PauseInfo(
            type=PauseType.WEBHOOK,
            webhook_url="https://test.com/webhook/resume/test-exec-456",
            timeout=3600,
            expected_data_schema={"type": "object", "properties": {"result": {"type": "string"}}}
        )
        
        # Mock webhook setup
        pause_resume_engine.store_resume_webhook = AsyncMock()
        
        await pause_resume_engine.setup_resume_triggers(pause_info, execution_id)
        
        # Verify webhook was stored
        pause_resume_engine.store_resume_webhook.assert_called_once()
        webhook_data = pause_resume_engine.store_resume_webhook.call_args[0][0]
        assert webhook_data.execution_id == execution_id
        assert webhook_data.webhook_path in pause_info.webhook_url
        assert webhook_data.expected_data_schema == pause_info.expected_data_schema


class TestPauseResumeIntegration:
    """Test complete pause/resume workflow integration."""

    async def test_complete_pause_resume_cycle(self, pause_resume_engine, sample_workflow_with_wait_node):
        """Test complete pause and resume cycle."""
        execution_id = "test-exec-integration-123"
        
        # Create execution state
        execution_state = ExecutionState(
            execution_id=execution_id,
            workflow=sample_workflow_with_wait_node,
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"initial": "data"}]},
            pause_data=None
        )
        
        # Create pause info
        pause_info = PauseInfo(
            type=PauseType.WEBHOOK,
            webhook_url=f"https://test.com/webhook/resume/{execution_id}",
            timeout=3600
        )
        
        # Mock all database operations
        pause_resume_engine.serialize_execution_state = AsyncMock(return_value={"serialized": "state"})
        pause_resume_engine.store_pause_data = AsyncMock()
        pause_resume_engine.setup_resume_triggers = AsyncMock()
        pause_resume_engine.update_execution_status = AsyncMock()
        pause_resume_engine.get_pause_data = AsyncMock()
        pause_resume_engine.validate_resume_conditions = AsyncMock()
        pause_resume_engine.deserialize_execution_state = AsyncMock(return_value=execution_state)
        pause_resume_engine.merge_resume_data = AsyncMock(return_value=execution_state)
        pause_resume_engine.cleanup_pause_data = AsyncMock()
        pause_resume_engine.continue_execution_from_state = AsyncMock(
            return_value=Mock(status=ExecutionStatus.SUCCESS)
        )
        
        # Test pause
        await pause_resume_engine.pause_execution(execution_state, pause_info)
        
        # Verify pause operations
        pause_resume_engine.store_pause_data.assert_called_once()
        pause_resume_engine.setup_resume_triggers.assert_called_once()
        pause_resume_engine.update_execution_status.assert_called_once()
        
        # Test resume
        resume_data = {"webhook_data": {"result": "approved"}}
        
        # Mock pause data for resume
        mock_pause_data = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.WEBHOOK,
            pause_node="wait1",
            serialized_state={"serialized": "state"},
            pause_metadata={"webhook_url": pause_info.webhook_url},
            created_at=datetime.now(),
            expires_at=None
        )
        pause_resume_engine.get_pause_data.return_value = mock_pause_data
        
        result = await pause_resume_engine.resume_execution(execution_id, resume_data)
        
        # Verify resume operations
        pause_resume_engine.get_pause_data.assert_called_with(execution_id)
        pause_resume_engine.validate_resume_conditions.assert_called_once()
        pause_resume_engine.deserialize_execution_state.assert_called_once()
        pause_resume_engine.merge_resume_data.assert_called_once()
        pause_resume_engine.cleanup_pause_data.assert_called_once()
        pause_resume_engine.continue_execution_from_state.assert_called_once()

    async def test_pause_resume_multiple_cycles(self, pause_resume_engine):
        """Test multiple pause/resume cycles in single execution."""
        execution_id = "test-exec-multi-123"
        
        # Mock initial execution state
        execution_state = ExecutionState(
            execution_id=execution_id,
            workflow=Mock(),
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"step": 1}]},
            pause_data=None
        )
        
        # Mock all operations
        pause_resume_engine.serialize_execution_state = AsyncMock(return_value={"step": 1})
        pause_resume_engine.store_pause_data = AsyncMock()
        pause_resume_engine.setup_resume_triggers = AsyncMock()
        pause_resume_engine.update_execution_status = AsyncMock()
        pause_resume_engine.get_pause_data = AsyncMock()
        pause_resume_engine.validate_resume_conditions = AsyncMock()
        pause_resume_engine.deserialize_execution_state = AsyncMock(return_value=execution_state)
        pause_resume_engine.merge_resume_data = AsyncMock(return_value=execution_state)
        pause_resume_engine.cleanup_pause_data = AsyncMock()
        pause_resume_engine.continue_execution_from_state = AsyncMock(
            return_value=Mock(status=ExecutionStatus.SUCCESS)
        )
        
        # First pause/resume cycle
        pause_info_1 = PauseInfo(type=PauseType.WEBHOOK, webhook_url="url1", timeout=3600)
        await pause_resume_engine.pause_execution(execution_state, pause_info_1)
        
        mock_pause_data_1 = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.WEBHOOK,
            pause_node="wait1",
            serialized_state={"step": 1},
            pause_metadata={},
            created_at=datetime.now(),
            expires_at=None
        )
        pause_resume_engine.get_pause_data.return_value = mock_pause_data_1
        
        await pause_resume_engine.resume_execution(execution_id, {"step1": "complete"})
        
        # Update state for second cycle
        execution_state.current_node = "wait2"
        execution_state.completed_nodes.add("wait1")
        execution_state.node_data["wait1"] = [{"step1": "complete"}]
        
        # Second pause/resume cycle
        pause_info_2 = PauseInfo(type=PauseType.MANUAL, message="Second approval", timeout=7200)
        await pause_resume_engine.pause_execution(execution_state, pause_info_2)
        
        mock_pause_data_2 = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.MANUAL,
            pause_node="wait2",
            serialized_state={"step": 2},
            pause_metadata={},
            created_at=datetime.now(),
            expires_at=None
        )
        pause_resume_engine.get_pause_data.return_value = mock_pause_data_2
        
        await pause_resume_engine.resume_execution(execution_id, {"step2": "approved"})
        
        # Verify both cycles completed
        assert pause_resume_engine.store_pause_data.call_count == 2
        assert pause_resume_engine.cleanup_pause_data.call_count == 2


class TestPauseResumeErrorHandling:
    """Test error handling in pause/resume functionality."""

    async def test_resume_execution_timeout_error(self, pause_resume_engine):
        """Test error when trying to resume expired pause."""
        execution_id = "test-exec-expired-123"
        
        # Mock expired pause data
        expired_pause_data = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.WEBHOOK,
            pause_node="wait1",
            serialized_state={"serialized": "state"},
            pause_metadata={},
            created_at=datetime.now() - timedelta(hours=2),
            expires_at=datetime.now() - timedelta(hours=1)  # Expired 1 hour ago
        )
        
        pause_resume_engine.get_pause_data = AsyncMock(return_value=expired_pause_data)
        
        async def mock_validate_resume_conditions(pause_data, resume_data):
            if pause_data.expires_at and pause_data.expires_at < datetime.now():
                raise PauseTimeoutError("Pause has expired")
        
        pause_resume_engine.validate_resume_conditions = AsyncMock(side_effect=mock_validate_resume_conditions)
        
        with pytest.raises(PauseTimeoutError) as exc_info:
            await pause_resume_engine.resume_execution(execution_id, {"test": "data"})
        
        assert "Pause has expired" in str(exc_info.value)

    async def test_pause_execution_state_corruption_handling(self, pause_resume_engine):
        """Test handling of corrupted execution state during pause."""
        execution_state = ExecutionState(
            execution_id="test-exec-corrupt-123",
            workflow=None,  # Corrupted: missing workflow
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        pause_info = PauseInfo(
            type=PauseType.WEBHOOK,
            webhook_url="https://test.com/webhook",
            timeout=3600
        )
        
        # Mock serialization to fail due to corrupted state
        pause_resume_engine.serialize_execution_state = AsyncMock(
            side_effect=Exception("Cannot serialize corrupted state")
        )
        
        with pytest.raises(Exception) as exc_info:
            await pause_resume_engine.pause_execution(execution_state, pause_info)
        
        assert "Cannot serialize corrupted state" in str(exc_info.value)

    async def test_concurrent_resume_attempts(self, pause_resume_engine):
        """Test handling of concurrent resume attempts."""
        execution_id = "test-exec-concurrent-123"
        
        # Mock pause data
        mock_pause_data = ExecutionPauseData(
            execution_id=execution_id,
            pause_type=PauseType.WEBHOOK,
            pause_node="wait1",
            serialized_state={"serialized": "state"},
            pause_metadata={},
            created_at=datetime.now(),
            expires_at=None
        )
        
        # First call returns pause data, second call returns None (already resumed)
        pause_resume_engine.get_pause_data = AsyncMock(side_effect=[mock_pause_data, None])
        pause_resume_engine.validate_resume_conditions = AsyncMock()
        pause_resume_engine.deserialize_execution_state = AsyncMock()
        pause_resume_engine.merge_resume_data = AsyncMock()
        pause_resume_engine.cleanup_pause_data = AsyncMock()
        pause_resume_engine.continue_execution_from_state = AsyncMock()
        
        # First resume attempt succeeds
        await pause_resume_engine.resume_execution(execution_id, {"first": "resume"})
        
        # Second concurrent resume attempt should fail
        with pytest.raises(ExecutionNotPausedError):
            await pause_resume_engine.resume_execution(execution_id, {"second": "resume"})


class TestPauseResumePerformance:
    """Test performance aspects of pause/resume functionality."""

    async def test_pause_state_size_optimization(self, pause_resume_engine):
        """Test optimization of pause state size for large executions."""
        # Create large execution state
        large_node_data = {}
        for i in range(100):  # 100 nodes with data
            large_node_data[f"node_{i}"] = [{"large_data": "x" * 1000}] * 10  # 10KB per node
        
        execution_state = ExecutionState(
            execution_id="test-exec-large-123",
            workflow=Mock(),
            current_node="wait1",
            completed_nodes=set(f"node_{i}" for i in range(50)),
            node_data=large_node_data,
            pause_data=None
        )
        
        # Mock optimized serialization
        async def mock_serialize_optimized(state):
            # Simulate compression/optimization
            return {"optimized": True, "size_reduced": True}
        
        pause_resume_engine.serialize_execution_state = AsyncMock(side_effect=mock_serialize_optimized)
        pause_resume_engine.store_pause_data = AsyncMock()
        pause_resume_engine.setup_resume_triggers = AsyncMock()
        pause_resume_engine.update_execution_status = AsyncMock()
        
        pause_info = PauseInfo(type=PauseType.MANUAL, message="Test", timeout=3600)
        
        await pause_resume_engine.pause_execution(execution_state, pause_info)
        
        # Verify serialization was called (optimization would happen there)
        pause_resume_engine.serialize_execution_state.assert_called_once_with(execution_state)

    async def test_pause_resume_performance_measurement(self, pause_resume_engine):
        """Test performance measurement for pause/resume operations."""
        execution_id = "test-exec-perf-123"
        
        # Mock timing operations
        start_time = datetime.now()
        
        # Mock all operations with small delays to simulate real performance
        async def mock_with_delay(delay_ms=10):
            await asyncio.sleep(delay_ms / 1000)
            return Mock()
        
        pause_resume_engine.serialize_execution_state = AsyncMock(side_effect=lambda x: mock_with_delay(50))
        pause_resume_engine.store_pause_data = AsyncMock(side_effect=lambda x: mock_with_delay(20))
        pause_resume_engine.setup_resume_triggers = AsyncMock(side_effect=lambda x, y: mock_with_delay(10))
        pause_resume_engine.update_execution_status = AsyncMock(side_effect=lambda x, y, **z: mock_with_delay(30))
        
        execution_state = ExecutionState(
            execution_id=execution_id,
            workflow=Mock(),
            current_node="wait1",
            completed_nodes={"start"},
            node_data={"start": [{"test": "data"}]},
            pause_data=None
        )
        
        pause_info = PauseInfo(type=PauseType.WEBHOOK, webhook_url="url", timeout=3600)
        
        await pause_resume_engine.pause_execution(execution_state, pause_info)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Verify pause operation completed within reasonable time (< 1 second for test)
        assert execution_time < 1.0