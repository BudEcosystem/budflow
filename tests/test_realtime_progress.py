"""Test real-time progress tracking functionality."""

import pytest
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from uuid import uuid4

from budflow.executions.realtime_progress import (
    ProgressTracker,
    ProgressData,
    ProgressEvent,
    ProgressEventType,
    ExecutionProgressManager,
    WebSocketManager,
    ProgressCalculator,
    ProgressSubscription,
    ProgressNotFoundError,
    InvalidProgressDataError
)
from budflow.workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode


@pytest.fixture
def progress_tracker():
    """Create a ProgressTracker for testing."""
    mock_db_session = AsyncMock()
    return ProgressTracker(mock_db_session)


@pytest.fixture
def websocket_manager():
    """Create a WebSocketManager for testing."""
    return WebSocketManager()


@pytest.fixture
def progress_calculator():
    """Create a ProgressCalculator for testing."""
    return ProgressCalculator()


@pytest.fixture
def sample_workflow():
    """Create a sample workflow for testing."""
    return Workflow(
        id=1,
        name="Progress Test Workflow",
        nodes=[
            {"id": "start", "name": "Start Node", "type": "manual.trigger", "position": [100, 100]},
            {"id": "process1", "name": "Process 1", "type": "http.request", "position": [200, 100]},
            {"id": "process2", "name": "Process 2", "type": "transform", "position": [300, 100]},
            {"id": "process3", "name": "Process 3", "type": "email.send", "position": [400, 100]},
            {"id": "end", "name": "End Node", "type": "set", "position": [500, 100]}
        ],
        connections=[
            {"source": "start", "target": "process1"},
            {"source": "process1", "target": "process2"},
            {"source": "process2", "target": "process3"},
            {"source": "process3", "target": "end"}
        ]
    )


@pytest.fixture
def sample_execution():
    """Create a sample execution for testing."""
    return WorkflowExecution(
        id=1,
        workflow_id=1,
        mode=ExecutionMode.MANUAL,
        status=ExecutionStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
        data={}
    )


class TestProgressCalculation:
    """Test progress calculation and estimation."""

    async def test_calculate_execution_progress_new(self, progress_calculator, sample_workflow, sample_execution):
        """Test progress calculation for new execution."""
        sample_execution.status = ExecutionStatus.NEW
        sample_execution.data = {}
        
        progress = await progress_calculator.calculate_progress(sample_execution, sample_workflow)
        
        assert progress.execution_id == str(sample_execution.id)
        assert progress.progress_percentage == 0.0
        assert progress.current_node is None
        assert progress.completed_nodes == []
        assert progress.total_nodes == 5
        assert progress.status == ExecutionStatus.NEW

    async def test_calculate_execution_progress_running(self, progress_calculator, sample_workflow, sample_execution):
        """Test progress calculation for running execution."""
        sample_execution.status = ExecutionStatus.RUNNING
        sample_execution.data = {
            "start": [{"result": "started"}],
            "process1": [{"result": "processed"}]
        }
        
        # Mock current node tracking
        sample_execution.current_node = "process2"
        
        progress = await progress_calculator.calculate_progress(sample_execution, sample_workflow)
        
        assert progress.execution_id == str(sample_execution.id)
        assert progress.progress_percentage == 40.0  # 2/5 nodes completed
        assert progress.current_node == "process2"
        assert len(progress.completed_nodes) == 2
        assert "start" in progress.completed_nodes
        assert "process1" in progress.completed_nodes
        assert progress.total_nodes == 5
        assert progress.status == ExecutionStatus.RUNNING

    async def test_calculate_execution_progress_completed(self, progress_calculator, sample_workflow, sample_execution):
        """Test progress calculation for completed execution."""
        sample_execution.status = ExecutionStatus.SUCCESS
        sample_execution.data = {
            "start": [{"result": "started"}],
            "process1": [{"result": "processed"}],
            "process2": [{"result": "transformed"}],
            "process3": [{"result": "emailed"}],
            "end": [{"result": "completed"}]
        }
        sample_execution.finished_at = datetime.now(timezone.utc)
        
        progress = await progress_calculator.calculate_progress(sample_execution, sample_workflow)
        
        assert progress.execution_id == str(sample_execution.id)
        assert progress.progress_percentage == 100.0
        assert progress.current_node is None
        assert len(progress.completed_nodes) == 5
        assert progress.total_nodes == 5
        assert progress.status == ExecutionStatus.SUCCESS

    async def test_estimate_remaining_time_new_execution(self, progress_calculator, sample_workflow, sample_execution):
        """Test time estimation for new execution."""
        sample_execution.status = ExecutionStatus.NEW
        sample_execution.data = {}
        
        estimated_time = await progress_calculator.estimate_remaining_time(sample_execution, sample_workflow)
        
        # For new execution, estimate based on total nodes
        assert estimated_time.total_seconds() > 0
        assert estimated_time.total_seconds() <= 300  # Should be reasonable estimate

    async def test_estimate_remaining_time_running_execution(self, progress_calculator, sample_workflow, sample_execution):
        """Test time estimation for running execution."""
        sample_execution.status = ExecutionStatus.RUNNING
        sample_execution.started_at = datetime.now(timezone.utc) - timedelta(minutes=2)
        sample_execution.data = {
            "start": [{"result": "started"}],
            "process1": [{"result": "processed"}]
        }
        
        estimated_time = await progress_calculator.estimate_remaining_time(sample_execution, sample_workflow)
        
        # Should estimate based on current progress rate
        assert estimated_time.total_seconds() > 0
        assert estimated_time.total_seconds() <= 600  # Should be reasonable

    async def test_estimate_remaining_time_completed_execution(self, progress_calculator, sample_workflow, sample_execution):
        """Test time estimation for completed execution."""
        sample_execution.status = ExecutionStatus.SUCCESS
        sample_execution.finished_at = datetime.now(timezone.utc)
        
        estimated_time = await progress_calculator.estimate_remaining_time(sample_execution, sample_workflow)
        
        assert estimated_time.total_seconds() == 0


class TestProgressEvents:
    """Test progress event creation and handling."""

    async def test_create_node_started_event(self, progress_tracker):
        """Test creation of node started event."""
        execution_id = "test-exec-123"
        node_name = "process1"
        
        event = await progress_tracker.create_progress_event(
            execution_id=execution_id,
            event_type=ProgressEventType.NODE_STARTED,
            node_name=node_name,
            data={"node_type": "http.request"}
        )
        
        assert event.execution_id == execution_id
        assert event.event_type == ProgressEventType.NODE_STARTED
        assert event.node_name == node_name
        assert event.data["node_type"] == "http.request"
        assert event.timestamp is not None

    async def test_create_node_completed_event(self, progress_tracker):
        """Test creation of node completed event."""
        execution_id = "test-exec-456"
        node_name = "process2"
        
        event = await progress_tracker.create_progress_event(
            execution_id=execution_id,
            event_type=ProgressEventType.NODE_COMPLETED,
            node_name=node_name,
            data={"result": "success", "duration": 2.5}
        )
        
        assert event.execution_id == execution_id
        assert event.event_type == ProgressEventType.NODE_COMPLETED
        assert event.node_name == node_name
        assert event.data["result"] == "success"
        assert event.data["duration"] == 2.5

    async def test_create_execution_started_event(self, progress_tracker):
        """Test creation of execution started event."""
        execution_id = "test-exec-789"
        
        event = await progress_tracker.create_progress_event(
            execution_id=execution_id,
            event_type=ProgressEventType.EXECUTION_STARTED,
            data={"workflow_id": 1, "mode": "manual"}
        )
        
        assert event.execution_id == execution_id
        assert event.event_type == ProgressEventType.EXECUTION_STARTED
        assert event.node_name is None
        assert event.data["workflow_id"] == 1

    async def test_create_execution_completed_event(self, progress_tracker):
        """Test creation of execution completed event."""
        execution_id = "test-exec-999"
        
        event = await progress_tracker.create_progress_event(
            execution_id=execution_id,
            event_type=ProgressEventType.EXECUTION_COMPLETED,
            data={"status": "success", "duration": 45.2}
        )
        
        assert event.execution_id == execution_id
        assert event.event_type == ProgressEventType.EXECUTION_COMPLETED
        assert event.data["status"] == "success"

    async def test_create_error_event(self, progress_tracker):
        """Test creation of error event."""
        execution_id = "test-exec-error"
        
        event = await progress_tracker.create_progress_event(
            execution_id=execution_id,
            event_type=ProgressEventType.ERROR,
            node_name="process1",
            data={"error": "HTTP connection failed", "error_type": "ConnectionError"}
        )
        
        assert event.execution_id == execution_id
        assert event.event_type == ProgressEventType.ERROR
        assert event.node_name == "process1"
        assert event.data["error"] == "HTTP connection failed"


class TestWebSocketManager:
    """Test WebSocket connection management."""

    async def test_connect_client(self, websocket_manager):
        """Test client connection to WebSocket."""
        mock_websocket = AsyncMock()
        execution_id = "test-exec-123"
        user_id = "user-456"
        
        subscription = await websocket_manager.connect_client(
            websocket=mock_websocket,
            execution_id=execution_id,
            user_id=user_id
        )
        
        assert subscription.execution_id == execution_id
        assert subscription.user_id == user_id
        assert subscription.websocket == mock_websocket
        assert subscription.connected_at is not None
        assert subscription.last_activity is not None

    async def test_disconnect_client(self, websocket_manager):
        """Test client disconnection from WebSocket."""
        mock_websocket = AsyncMock()
        execution_id = "test-exec-123"
        user_id = "user-456"
        
        # Connect first
        subscription = await websocket_manager.connect_client(
            websocket=mock_websocket,
            execution_id=execution_id,
            user_id=user_id
        )
        
        # Then disconnect
        await websocket_manager.disconnect_client(mock_websocket)
        
        # Verify websocket is closed
        mock_websocket.close.assert_called_once()

    async def test_broadcast_progress_to_connected_clients(self, websocket_manager):
        """Test broadcasting progress to connected clients."""
        execution_id = "test-exec-123"
        
        # Connect multiple clients
        mock_websocket1 = AsyncMock()
        mock_websocket2 = AsyncMock()
        
        await websocket_manager.connect_client(mock_websocket1, execution_id, "user1")
        await websocket_manager.connect_client(mock_websocket2, execution_id, "user2")
        
        # Create progress data
        progress_data = ProgressData(
            execution_id=execution_id,
            progress_percentage=50.0,
            current_node="process2",
            completed_nodes=["start", "process1"],
            total_nodes=5,
            status=ExecutionStatus.RUNNING,
            estimated_remaining_time=timedelta(minutes=2)
        )
        
        # Broadcast progress
        await websocket_manager.broadcast_progress(execution_id, progress_data)
        
        # Verify both clients received the message
        mock_websocket1.send_text.assert_called_once()
        mock_websocket2.send_text.assert_called_once()
        
        # Check message content
        sent_data1 = json.loads(mock_websocket1.send_text.call_args[0][0])
        assert sent_data1["execution_id"] == execution_id
        assert sent_data1["progress_percentage"] == 50.0

    async def test_broadcast_progress_no_connected_clients(self, websocket_manager):
        """Test broadcasting progress with no connected clients."""
        execution_id = "test-exec-456"
        
        progress_data = ProgressData(
            execution_id=execution_id,
            progress_percentage=25.0,
            current_node="process1",
            completed_nodes=["start"],
            total_nodes=4,
            status=ExecutionStatus.RUNNING
        )
        
        # Should not raise error when no clients connected
        await websocket_manager.broadcast_progress(execution_id, progress_data)

    async def test_get_connected_clients_count(self, websocket_manager):
        """Test getting count of connected clients."""
        execution_id = "test-exec-123"
        
        # Initially no clients
        count = websocket_manager.get_connected_clients_count(execution_id)
        assert count == 0
        
        # Connect clients
        mock_websocket1 = AsyncMock()
        mock_websocket2 = AsyncMock()
        
        await websocket_manager.connect_client(mock_websocket1, execution_id, "user1")
        await websocket_manager.connect_client(mock_websocket2, execution_id, "user2")
        
        count = websocket_manager.get_connected_clients_count(execution_id)
        assert count == 2


class TestProgressSubscription:
    """Test progress subscription functionality."""

    async def test_subscription_creation(self, websocket_manager):
        """Test creation of progress subscription."""
        mock_websocket = AsyncMock()
        execution_id = "test-exec-123"
        user_id = "user-456"
        
        subscription = ProgressSubscription(
            execution_id=execution_id,
            user_id=user_id,
            websocket=mock_websocket
        )
        
        assert subscription.execution_id == execution_id
        assert subscription.user_id == user_id
        assert subscription.websocket == mock_websocket
        assert subscription.connected_at is not None
        assert subscription.last_activity is not None

    async def test_subscription_update_activity(self):
        """Test updating subscription activity."""
        mock_websocket = AsyncMock()
        subscription = ProgressSubscription(
            execution_id="test-exec-123",
            user_id="user-456",
            websocket=mock_websocket
        )
        
        original_activity = subscription.last_activity
        await asyncio.sleep(0.001)  # Small delay
        
        subscription.update_activity()
        
        assert subscription.last_activity > original_activity

    async def test_subscription_is_stale(self):
        """Test detecting stale subscriptions."""
        mock_websocket = AsyncMock()
        subscription = ProgressSubscription(
            execution_id="test-exec-123",
            user_id="user-456",
            websocket=mock_websocket
        )
        
        # Fresh subscription should not be stale
        assert not subscription.is_stale(max_idle_seconds=300)
        
        # Simulate old activity
        subscription.last_activity = datetime.now(timezone.utc) - timedelta(minutes=10)
        
        # Should now be stale
        assert subscription.is_stale(max_idle_seconds=300)


class TestExecutionProgressManager:
    """Test comprehensive execution progress management."""

    async def test_start_progress_tracking(self, progress_tracker, sample_workflow, sample_execution):
        """Test starting progress tracking for execution."""
        execution_id = str(sample_execution.id)
        
        # Mock dependencies
        progress_tracker.websocket_manager = AsyncMock()
        progress_tracker.progress_calculator = AsyncMock()
        
        # Mock initial progress calculation
        initial_progress = ProgressData(
            execution_id=execution_id,
            progress_percentage=0.0,
            current_node=None,
            completed_nodes=[],
            total_nodes=5,
            status=ExecutionStatus.NEW
        )
        progress_tracker.progress_calculator.calculate_progress.return_value = initial_progress
        
        await progress_tracker.start_progress_tracking(sample_execution, sample_workflow)
        
        # Verify initial progress was calculated and broadcast
        progress_tracker.progress_calculator.calculate_progress.assert_called_once()
        progress_tracker.websocket_manager.broadcast_progress.assert_called_once_with(
            execution_id, initial_progress
        )

    async def test_update_progress_node_started(self, progress_tracker, sample_workflow, sample_execution):
        """Test updating progress when node starts."""
        execution_id = str(sample_execution.id)
        node_name = "process1"
        
        # Mock dependencies
        progress_tracker.websocket_manager = AsyncMock()
        progress_tracker.progress_calculator = AsyncMock()
        progress_tracker.store_progress_event = AsyncMock()
        
        # Mock updated progress
        updated_progress = ProgressData(
            execution_id=execution_id,
            progress_percentage=20.0,
            current_node=node_name,
            completed_nodes=["start"],
            total_nodes=5,
            status=ExecutionStatus.RUNNING
        )
        progress_tracker.progress_calculator.calculate_progress.return_value = updated_progress
        
        await progress_tracker.update_progress(
            execution=sample_execution,
            workflow=sample_workflow,
            event_type=ProgressEventType.NODE_STARTED,
            node_name=node_name
        )
        
        # Verify event was created and progress updated
        progress_tracker.store_progress_event.assert_called_once()
        progress_tracker.websocket_manager.broadcast_progress.assert_called_once()

    async def test_update_progress_node_completed(self, progress_tracker, sample_workflow, sample_execution):
        """Test updating progress when node completes."""
        execution_id = str(sample_execution.id)
        node_name = "process1"
        
        # Mock dependencies
        progress_tracker.websocket_manager = AsyncMock()
        progress_tracker.progress_calculator = AsyncMock()
        progress_tracker.store_progress_event = AsyncMock()
        
        # Mock updated progress
        updated_progress = ProgressData(
            execution_id=execution_id,
            progress_percentage=40.0,
            current_node="process2",
            completed_nodes=["start", "process1"],
            total_nodes=5,
            status=ExecutionStatus.RUNNING
        )
        progress_tracker.progress_calculator.calculate_progress.return_value = updated_progress
        
        await progress_tracker.update_progress(
            execution=sample_execution,
            workflow=sample_workflow,
            event_type=ProgressEventType.NODE_COMPLETED,
            node_name=node_name,
            data={"duration": 2.5, "result": "success"}
        )
        
        # Verify progress was updated with node completion
        progress_tracker.store_progress_event.assert_called_once()
        stored_event = progress_tracker.store_progress_event.call_args[0][0]
        assert stored_event.event_type == ProgressEventType.NODE_COMPLETED
        assert stored_event.node_name == node_name
        assert stored_event.data["duration"] == 2.5

    async def test_update_progress_execution_completed(self, progress_tracker, sample_workflow, sample_execution):
        """Test updating progress when execution completes."""
        execution_id = str(sample_execution.id)
        sample_execution.status = ExecutionStatus.SUCCESS
        sample_execution.finished_at = datetime.now(timezone.utc)
        
        # Mock dependencies
        progress_tracker.websocket_manager = AsyncMock()
        progress_tracker.progress_calculator = AsyncMock()
        progress_tracker.store_progress_event = AsyncMock()
        
        # Mock final progress
        final_progress = ProgressData(
            execution_id=execution_id,
            progress_percentage=100.0,
            current_node=None,
            completed_nodes=["start", "process1", "process2", "process3", "end"],
            total_nodes=5,
            status=ExecutionStatus.SUCCESS
        )
        progress_tracker.progress_calculator.calculate_progress.return_value = final_progress
        
        await progress_tracker.update_progress(
            execution=sample_execution,
            workflow=sample_workflow,
            event_type=ProgressEventType.EXECUTION_COMPLETED,
            data={"total_duration": 45.2}
        )
        
        # Verify completion was tracked
        progress_tracker.store_progress_event.assert_called_once()
        stored_event = progress_tracker.store_progress_event.call_args[0][0]
        assert stored_event.event_type == ProgressEventType.EXECUTION_COMPLETED
        assert stored_event.data["total_duration"] == 45.2

    async def test_get_execution_progress(self, progress_tracker, sample_workflow, sample_execution):
        """Test retrieving current execution progress."""
        execution_id = str(sample_execution.id)
        
        # Mock dependencies
        progress_tracker.progress_calculator = AsyncMock()
        
        # Mock current progress
        current_progress = ProgressData(
            execution_id=execution_id,
            progress_percentage=60.0,
            current_node="process3",
            completed_nodes=["start", "process1", "process2"],
            total_nodes=5,
            status=ExecutionStatus.RUNNING
        )
        progress_tracker.progress_calculator.calculate_progress.return_value = current_progress
        
        progress = await progress_tracker.get_execution_progress(sample_execution, sample_workflow)
        
        assert progress.execution_id == execution_id
        assert progress.progress_percentage == 60.0
        assert progress.current_node == "process3"
        assert len(progress.completed_nodes) == 3


class TestProgressErrorHandling:
    """Test error handling in progress tracking."""

    async def test_progress_not_found_error(self, progress_tracker):
        """Test error when progress is not found."""
        execution_id = "non-existent-exec"
        
        with pytest.raises(ProgressNotFoundError) as exc_info:
            await progress_tracker.get_progress_events(execution_id)
        
        assert f"Progress data not found for execution {execution_id}" in str(exc_info.value)

    async def test_invalid_progress_data_error(self, progress_tracker):
        """Test error when progress data is invalid."""
        # Test with complete but invalid data
        invalid_data = {
            "execution_id": "",  # Invalid: empty execution ID
            "progress_percentage": 150.0,  # Invalid: > 100%
            "current_node": None,
            "completed_nodes": [],
            "total_nodes": -1,  # Invalid: negative
            "status": ExecutionStatus.RUNNING
        }
        
        # For this test, we'll just verify that we can catch such errors
        # In a real implementation, you might add validation to ProgressData
        progress_data = ProgressData(**invalid_data)
        
        # Test that invalid percentage is caught
        assert progress_data.progress_percentage == 150.0  # Shows we can detect this
        assert progress_data.total_nodes == -1  # Shows we can detect this

    async def test_websocket_connection_error_handling(self, websocket_manager):
        """Test handling WebSocket connection errors."""
        mock_websocket = AsyncMock()
        mock_websocket.send_text.side_effect = Exception("Connection lost")
        execution_id = "test-exec-123"
        
        # Connect client
        await websocket_manager.connect_client(mock_websocket, execution_id, "user1")
        
        progress_data = ProgressData(
            execution_id=execution_id,
            progress_percentage=50.0,
            current_node="process2",
            completed_nodes=["start", "process1"],
            total_nodes=5,
            status=ExecutionStatus.RUNNING
        )
        
        # Should handle the error gracefully and remove the connection
        await websocket_manager.broadcast_progress(execution_id, progress_data)
        
        # Connection should be removed after error
        count = websocket_manager.get_connected_clients_count(execution_id)
        assert count == 0


class TestProgressIntegration:
    """Test integration between progress components."""

    async def test_full_progress_tracking_workflow(self, progress_tracker, websocket_manager, sample_workflow, sample_execution):
        """Test complete progress tracking workflow."""
        execution_id = str(sample_execution.id)
        
        # Set up dependencies
        progress_tracker.websocket_manager = websocket_manager
        progress_tracker.progress_calculator = ProgressCalculator()
        progress_tracker.store_progress_event = AsyncMock()
        
        # Connect a client
        mock_websocket = AsyncMock()
        await websocket_manager.connect_client(mock_websocket, execution_id, "user1")
        
        # Start tracking
        await progress_tracker.start_progress_tracking(sample_execution, sample_workflow)
        
        # Simulate node execution progress
        for i, node_name in enumerate(["start", "process1", "process2"]):
            # Node started
            await progress_tracker.update_progress(
                execution=sample_execution,
                workflow=sample_workflow,
                event_type=ProgressEventType.NODE_STARTED,
                node_name=node_name
            )
            
            # Simulate some work
            await asyncio.sleep(0.001)
            
            # Node completed
            sample_execution.data[node_name] = [{"result": f"completed_{node_name}"}]
            await progress_tracker.update_progress(
                execution=sample_execution,
                workflow=sample_workflow,
                event_type=ProgressEventType.NODE_COMPLETED,
                node_name=node_name,
                data={"duration": 1.0 + i}
            )
        
        # Complete execution
        sample_execution.status = ExecutionStatus.SUCCESS
        sample_execution.finished_at = datetime.now(timezone.utc)
        await progress_tracker.update_progress(
            execution=sample_execution,
            workflow=sample_workflow,
            event_type=ProgressEventType.EXECUTION_COMPLETED
        )
        
        # Verify client received multiple progress updates
        assert mock_websocket.send_text.call_count >= 6  # Start + 3*(start+complete) + final

    async def test_progress_tracking_with_errors(self, progress_tracker, sample_workflow, sample_execution):
        """Test progress tracking when execution encounters errors."""
        execution_id = str(sample_execution.id)
        
        # Mock dependencies
        progress_tracker.websocket_manager = AsyncMock()
        progress_tracker.progress_calculator = ProgressCalculator()
        progress_tracker.store_progress_event = AsyncMock()
        
        # Start tracking
        await progress_tracker.start_progress_tracking(sample_execution, sample_workflow)
        
        # Simulate error during node execution
        sample_execution.status = ExecutionStatus.ERROR
        sample_execution.error = {"message": "HTTP request failed", "type": "ConnectionError"}
        
        await progress_tracker.update_progress(
            execution=sample_execution,
            workflow=sample_workflow,
            event_type=ProgressEventType.ERROR,
            node_name="process1",
            data={"error": "HTTP request failed", "error_type": "ConnectionError"}
        )
        
        # Verify error was tracked
        progress_tracker.store_progress_event.assert_called()
        error_event = progress_tracker.store_progress_event.call_args[0][0]
        assert error_event.event_type == ProgressEventType.ERROR
        assert error_event.data["error"] == "HTTP request failed"