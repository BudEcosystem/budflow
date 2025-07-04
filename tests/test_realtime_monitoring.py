"""
Comprehensive tests for real-time execution monitoring via WebSockets.

Real-time monitoring allows users to see live updates of workflow execution
progress, including node status changes, data flow, and error reporting.
"""

import pytest
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call
from uuid import uuid4

from fastapi import WebSocket
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.executor.engine import WorkflowExecutionEngine
from budflow.executor.realtime import (
    RealtimeMonitor,
    ExecutionEvent,
    EventType,
    NodeProgress,
    ExecutionProgress,
    WebSocketManager
)
from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, ExecutionStatus, NodeExecutionStatus,
    ExecutionMode
)


class TestRealtimeMonitoringCore:
    """Test the core real-time monitoring functionality."""
    
    @pytest.fixture
    def realtime_monitor(self):
        """Create a real-time monitor instance."""
        return RealtimeMonitor()
    
    @pytest.fixture
    def websocket_manager(self):
        """Create a WebSocket manager instance."""
        return WebSocketManager()
    
    @pytest.fixture
    def mock_websocket(self):
        """Create a mock WebSocket connection."""
        ws = AsyncMock(spec=WebSocket)
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()
        ws.receive_json = AsyncMock()
        ws.close = AsyncMock()
        return ws
    
    @pytest.mark.asyncio
    async def test_websocket_connection_lifecycle(self, websocket_manager, mock_websocket):
        """Test WebSocket connection and disconnection."""
        execution_id = uuid4()
        
        # Connect
        await websocket_manager.connect(execution_id, mock_websocket)
        assert execution_id in websocket_manager.active_connections
        assert mock_websocket in websocket_manager.active_connections[execution_id]
        
        # Disconnect
        await websocket_manager.disconnect(execution_id, mock_websocket)
        assert mock_websocket not in websocket_manager.active_connections.get(execution_id, [])
    
    @pytest.mark.asyncio
    async def test_broadcast_to_execution_subscribers(self, websocket_manager, mock_websocket):
        """Test broadcasting messages to all subscribers of an execution."""
        execution_id = uuid4()
        ws1 = mock_websocket
        ws2 = AsyncMock(spec=WebSocket)
        ws2.send_json = AsyncMock()
        
        # Connect multiple clients
        await websocket_manager.connect(execution_id, ws1)
        await websocket_manager.connect(execution_id, ws2)
        
        # Broadcast message
        message = {"type": "node_started", "nodeId": 1}
        await websocket_manager.broadcast_to_execution(execution_id, message)
        
        # Verify both clients received the message
        ws1.send_json.assert_called_once_with(message)
        ws2.send_json.assert_called_once_with(message)
    
    @pytest.mark.asyncio
    async def test_execution_started_event(self, realtime_monitor):
        """Test execution started event generation."""
        execution = MagicMock()
        execution.uuid = uuid4()
        execution.workflow_id = 1
        execution.started_at = datetime.now(timezone.utc)
        
        event = await realtime_monitor.create_execution_started_event(execution)
        
        assert event.type == EventType.EXECUTION_STARTED
        assert event.execution_id == execution.uuid
        assert event.data["workflow_id"] == 1
        assert event.data["started_at"] is not None
    
    @pytest.mark.asyncio
    async def test_node_execution_events(self, realtime_monitor):
        """Test node execution lifecycle events."""
        node_execution = MagicMock()
        node_execution.id = 1
        node_execution.node_id = 2
        node_execution.node.name = "ProcessNode"
        node_execution.status = NodeExecutionStatus.RUNNING
        
        # Node started event
        started_event = await realtime_monitor.create_node_started_event(node_execution)
        assert started_event.type == EventType.NODE_STARTED
        assert started_event.data["node_id"] == 2
        assert started_event.data["node_name"] == "ProcessNode"
        
        # Node progress event
        progress_event = await realtime_monitor.create_node_progress_event(
            node_execution,
            progress=0.5,
            message="Processing items"
        )
        assert progress_event.type == EventType.NODE_PROGRESS
        assert progress_event.data["progress"] == 0.5
        assert progress_event.data["message"] == "Processing items"
        
        # Node completed event
        node_execution.status = NodeExecutionStatus.SUCCESS
        completed_event = await realtime_monitor.create_node_completed_event(node_execution)
        assert completed_event.type == EventType.NODE_COMPLETED
        assert completed_event.data["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_error_event_creation(self, realtime_monitor):
        """Test error event creation with details."""
        error = Exception("Node execution failed")
        node_id = 3
        
        error_event = await realtime_monitor.create_error_event(
            execution_id=uuid4(),
            error=error,
            node_id=node_id
        )
        
        assert error_event.type == EventType.ERROR
        assert error_event.data["error_message"] == "Node execution failed"
        assert error_event.data["node_id"] == node_id
        assert error_event.data["error_type"] == "Exception"
    
    @pytest.mark.asyncio
    async def test_execution_progress_tracking(self, realtime_monitor):
        """Test overall execution progress tracking."""
        execution_id = uuid4()
        
        # Initialize progress
        await realtime_monitor.initialize_execution_progress(
            execution_id=execution_id,
            total_nodes=5
        )
        
        # Update node progress
        await realtime_monitor.update_node_progress(
            execution_id=execution_id,
            node_id=1,
            status=NodeExecutionStatus.SUCCESS
        )
        
        # Get overall progress
        progress = await realtime_monitor.get_execution_progress(execution_id)
        assert progress.total_nodes == 5
        assert progress.completed_nodes == 1
        assert progress.progress_percentage == 20.0
    
    @pytest.mark.asyncio
    async def test_concurrent_websocket_connections(self, websocket_manager):
        """Test handling multiple concurrent WebSocket connections."""
        execution_ids = [uuid4() for _ in range(3)]
        websockets = [AsyncMock(spec=WebSocket) for _ in range(5)]
        
        # Connect multiple websockets to different executions
        await websocket_manager.connect(execution_ids[0], websockets[0])
        await websocket_manager.connect(execution_ids[0], websockets[1])
        await websocket_manager.connect(execution_ids[1], websockets[2])
        await websocket_manager.connect(execution_ids[2], websockets[3])
        await websocket_manager.connect(execution_ids[2], websockets[4])
        
        # Broadcast to specific execution
        await websocket_manager.broadcast_to_execution(
            execution_ids[0],
            {"type": "test"}
        )
        
        # Verify only correct websockets received message
        websockets[0].send_json.assert_called_once()
        websockets[1].send_json.assert_called_once()
        websockets[2].send_json.assert_not_called()
        websockets[3].send_json.assert_not_called()
        websockets[4].send_json.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_websocket_error_handling(self, websocket_manager, mock_websocket):
        """Test error handling when WebSocket send fails."""
        execution_id = uuid4()
        mock_websocket.send_json.side_effect = Exception("Connection closed")
        
        await websocket_manager.connect(execution_id, mock_websocket)
        
        # Should handle error gracefully and remove failed connection
        await websocket_manager.broadcast_to_execution(
            execution_id,
            {"type": "test"}
        )
        
        # Verify connection was removed after error
        assert mock_websocket not in websocket_manager.active_connections.get(execution_id, [])


class TestRealtimeMonitoringIntegration:
    """Test real-time monitoring integration with execution engine."""
    
    @pytest.fixture
    async def execution_engine_with_monitoring(self, db_session):
        """Create execution engine with real-time monitoring enabled."""
        engine = WorkflowExecutionEngine(db_session)
        engine.realtime_monitor = RealtimeMonitor()
        engine.websocket_manager = WebSocketManager()
        return engine
    
    @pytest.fixture
    def sample_workflow_for_monitoring(self):
        """Create a workflow for monitoring tests."""
        workflow = Workflow(
            id=1,
            uuid=uuid4(),
            name="Monitor Test Workflow",
            status=WorkflowStatus.ACTIVE
        )
        
        nodes = [
            WorkflowNode(id=1, name="Start", type="Start"),
            WorkflowNode(id=2, name="Process", type="Process"),
            WorkflowNode(id=3, name="End", type="End")
        ]
        
        connections = [
            WorkflowConnection(source_node_id=1, target_node_id=2),
            WorkflowConnection(source_node_id=2, target_node_id=3)
        ]
        
        workflow.workflow_nodes = nodes
        workflow.workflow_connections = connections
        
        return workflow
    
    @pytest.mark.asyncio
    async def test_execution_with_realtime_monitoring(
        self,
        execution_engine_with_monitoring,
        sample_workflow_for_monitoring,
        mock_websocket
    ):
        """Test that execution emits real-time events."""
        engine = execution_engine_with_monitoring
        execution_id = uuid4()
        
        # Connect WebSocket
        await engine.websocket_manager.connect(execution_id, mock_websocket)
        
        # Mock execution methods
        with patch.object(engine, '_load_workflow', return_value=sample_workflow_for_monitoring):
            with patch.object(engine, '_execute_node', return_value=[{"data": "test"}]):
                with patch.object(engine, '_create_workflow_execution') as mock_create:
                    mock_execution = MagicMock()
                    mock_execution.uuid = execution_id
                    mock_create.return_value = mock_execution
                    
                    # Execute workflow
                    await engine.execute_workflow(
                        workflow_id=1,
                        mode=ExecutionMode.MANUAL
                    )
        
        # Verify events were sent
        calls = mock_websocket.send_json.call_args_list
        event_types = [call[0][0]["type"] for call in calls]
        
        assert EventType.EXECUTION_STARTED.value in event_types
        assert EventType.NODE_STARTED.value in event_types
        assert EventType.NODE_COMPLETED.value in event_types
        assert EventType.EXECUTION_COMPLETED.value in event_types
    
    @pytest.mark.asyncio
    async def test_error_reporting_via_websocket(
        self,
        execution_engine_with_monitoring,
        sample_workflow_for_monitoring,
        mock_websocket
    ):
        """Test that errors are reported via WebSocket."""
        engine = execution_engine_with_monitoring
        execution_id = uuid4()
        
        await engine.websocket_manager.connect(execution_id, mock_websocket)
        
        # Mock execution to fail
        with patch.object(engine, '_load_workflow', return_value=sample_workflow_for_monitoring):
            with patch.object(engine, '_execute_node', side_effect=Exception("Node failed")):
                with patch.object(engine, '_create_workflow_execution') as mock_create:
                    mock_execution = MagicMock()
                    mock_execution.uuid = execution_id
                    mock_create.return_value = mock_execution
                    
                    # Execute workflow (should fail)
                    with pytest.raises(Exception):
                        await engine.execute_workflow(
                            workflow_id=1,
                            mode=ExecutionMode.MANUAL
                        )
        
        # Verify error event was sent
        calls = mock_websocket.send_json.call_args_list
        error_events = [
            call[0][0] for call in calls
            if call[0][0]["type"] == EventType.ERROR.value
        ]
        
        assert len(error_events) > 0
        assert "Node failed" in error_events[0]["data"]["error_message"]
    
    @pytest.mark.asyncio
    async def test_progress_updates_during_execution(
        self,
        execution_engine_with_monitoring,
        mock_websocket
    ):
        """Test progress updates are sent during execution."""
        engine = execution_engine_with_monitoring
        execution_id = uuid4()
        
        await engine.websocket_manager.connect(execution_id, mock_websocket)
        
        # Simulate progress updates
        await engine.realtime_monitor.send_progress_update(
            execution_id=execution_id,
            node_id=1,
            progress=0.5,
            message="Processing 50% complete"
        )
        
        # Verify progress event
        mock_websocket.send_json.assert_called()
        progress_event = mock_websocket.send_json.call_args[0][0]
        assert progress_event["type"] == EventType.NODE_PROGRESS.value
        assert progress_event["data"]["progress"] == 0.5
        assert progress_event["data"]["message"] == "Processing 50% complete"


class TestWebSocketAPI:
    """Test WebSocket API endpoints."""
    
    @pytest.mark.asyncio
    async def test_websocket_endpoint(self, test_client):
        """Test WebSocket connection endpoint."""
        execution_id = str(uuid4())
        
        with test_client.websocket_connect(f"/ws/executions/{execution_id}") as websocket:
            # Send subscription message
            websocket.send_json({
                "action": "subscribe",
                "execution_id": execution_id
            })
            
            # Receive confirmation
            data = websocket.receive_json()
            assert data["type"] == "subscription_confirmed"
            assert data["execution_id"] == execution_id
    
    @pytest.mark.asyncio
    async def test_websocket_authentication(self, test_client):
        """Test WebSocket requires authentication."""
        execution_id = str(uuid4())
        
        # Try to connect without auth token
        with pytest.raises(Exception) as exc_info:
            with test_client.websocket_connect(
                f"/ws/executions/{execution_id}",
                headers={}
            ):
                pass
        
        assert "403" in str(exc_info.value) or "Forbidden" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_multiple_execution_subscriptions(self, test_client, auth_headers):
        """Test subscribing to multiple executions via WebSocket."""
        execution_ids = [str(uuid4()) for _ in range(3)]
        
        with test_client.websocket_connect(
            "/ws/executions",
            headers=auth_headers
        ) as websocket:
            # Subscribe to multiple executions
            for exec_id in execution_ids:
                websocket.send_json({
                    "action": "subscribe",
                    "execution_id": exec_id
                })
                
                # Receive confirmation
                data = websocket.receive_json()
                assert data["type"] == "subscription_confirmed"
                assert data["execution_id"] == exec_id
            
            # Unsubscribe from one
            websocket.send_json({
                "action": "unsubscribe",
                "execution_id": execution_ids[0]
            })
            
            data = websocket.receive_json()
            assert data["type"] == "unsubscribed"
            assert data["execution_id"] == execution_ids[0]
    
    @pytest.mark.asyncio
    async def test_websocket_heartbeat(self, test_client, auth_headers):
        """Test WebSocket heartbeat/ping-pong."""
        with test_client.websocket_connect(
            "/ws/executions",
            headers=auth_headers
        ) as websocket:
            # Send ping
            websocket.send_json({"action": "ping"})
            
            # Receive pong
            data = websocket.receive_json()
            assert data["type"] == "pong"
            assert "timestamp" in data


class TestRealtimeDataFlow:
    """Test real-time data flow monitoring."""
    
    @pytest.mark.asyncio
    async def test_node_input_output_monitoring(self, realtime_monitor):
        """Test monitoring of node input/output data."""
        execution_id = uuid4()
        node_id = 1
        
        # Monitor input data
        input_data = [{"id": 1, "value": "test"}]
        await realtime_monitor.report_node_input(
            execution_id=execution_id,
            node_id=node_id,
            data=input_data
        )
        
        # Monitor output data
        output_data = [{"id": 1, "value": "processed"}]
        await realtime_monitor.report_node_output(
            execution_id=execution_id,
            node_id=node_id,
            data=output_data
        )
        
        # Verify data flow tracking
        data_flow = await realtime_monitor.get_node_data_flow(
            execution_id=execution_id,
            node_id=node_id
        )
        
        assert data_flow["input"] == input_data
        assert data_flow["output"] == output_data
    
    @pytest.mark.asyncio
    async def test_execution_timeline(self, realtime_monitor):
        """Test execution timeline tracking."""
        execution_id = uuid4()
        
        # Record timeline events
        await realtime_monitor.record_timeline_event(
            execution_id=execution_id,
            event_type="execution_started",
            timestamp=datetime.now(timezone.utc)
        )
        
        await asyncio.sleep(0.1)  # Simulate processing time
        
        await realtime_monitor.record_timeline_event(
            execution_id=execution_id,
            event_type="node_1_started",
            timestamp=datetime.now(timezone.utc),
            node_id=1
        )
        
        # Get timeline
        timeline = await realtime_monitor.get_execution_timeline(execution_id)
        
        assert len(timeline) == 2
        assert timeline[0]["event_type"] == "execution_started"
        assert timeline[1]["event_type"] == "node_1_started"
        assert timeline[1]["timestamp"] > timeline[0]["timestamp"]
    
    @pytest.mark.asyncio
    async def test_performance_metrics_reporting(self, realtime_monitor):
        """Test real-time performance metrics."""
        execution_id = uuid4()
        node_id = 1
        
        # Report performance metrics
        await realtime_monitor.report_node_metrics(
            execution_id=execution_id,
            node_id=node_id,
            metrics={
                "execution_time_ms": 150,
                "memory_usage_mb": 32,
                "items_processed": 100,
                "items_per_second": 667
            }
        )
        
        # Get metrics
        metrics = await realtime_monitor.get_node_metrics(
            execution_id=execution_id,
            node_id=node_id
        )
        
        assert metrics["execution_time_ms"] == 150
        assert metrics["items_per_second"] == 667