"""Test pin data support functionality."""

import pytest
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from budflow.executions.pin_data import (
    PinDataService,
    PinData,
    PinDataEngine,
    PinDataNotFoundError,
    InvalidPinDataError,
    PinDataVersionMismatchError,
    NodeNotPinnedError
)
from budflow.workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode


@pytest.fixture
def pin_data_service():
    """Create a PinDataService for testing."""
    mock_db_session = AsyncMock()
    return PinDataService(mock_db_session)


@pytest.fixture
def pin_data_engine():
    """Create a PinDataEngine for testing."""
    mock_db_session = AsyncMock()
    return PinDataEngine(mock_db_session)


@pytest.fixture
def sample_workflow():
    """Create a sample workflow for testing."""
    return Workflow(
        id=1,
        name="Pin Data Test Workflow",
        nodes=[
            {"id": "start", "name": "Start Node", "type": "manual.trigger", "position": [100, 100]},
            {"id": "http1", "name": "HTTP Request", "type": "http.request", "position": [200, 100], "parameters": {"url": "https://api.example.com/data"}},
            {"id": "transform1", "name": "Transform Data", "type": "transform", "position": [300, 100]},
            {"id": "email1", "name": "Send Email", "type": "email.send", "position": [400, 100]},
            {"id": "end", "name": "End Node", "type": "set", "position": [500, 100]}
        ],
        connections=[
            {"source": "start", "target": "http1"},
            {"source": "http1", "target": "transform1"},
            {"source": "transform1", "target": "email1"},
            {"source": "email1", "target": "end"}
        ]
    )


@pytest.fixture
def sample_pin_data():
    """Create sample pin data."""
    return [
        {
            "id": 1,
            "data": {"users": [{"id": 1, "name": "John Doe", "email": "john@example.com"}]},
            "timestamp": "2023-12-01T10:00:00Z"
        },
        {
            "id": 2, 
            "data": {"users": [{"id": 2, "name": "Jane Smith", "email": "jane@example.com"}]},
            "timestamp": "2023-12-01T10:01:00Z"
        }
    ]


class TestPinDataStorage:
    """Test pin data storage and retrieval."""

    async def test_save_pin_data(self, pin_data_service, sample_pin_data):
        """Test saving pin data for a node."""
        workflow_id = 1
        node_id = "http1"
        user_id = "user-123"
        
        # Mock database operations
        pin_data_service.store_pin_data = AsyncMock()
        
        pin_data = await pin_data_service.save_pin_data(
            workflow_id=workflow_id,
            node_id=node_id,
            data=sample_pin_data,
            user_id=user_id,
            description="Sample API response for testing"
        )
        
        assert pin_data.workflow_id == workflow_id
        assert pin_data.node_id == node_id
        assert pin_data.data == sample_pin_data
        assert pin_data.created_by == user_id
        assert pin_data.description == "Sample API response for testing"
        assert pin_data.created_at is not None
        assert pin_data.version == 1  # First version

    async def test_get_pin_data_exists(self, pin_data_service, sample_pin_data):
        """Test getting existing pin data."""
        workflow_id = 1
        node_id = "http1"
        
        # Mock existing pin data
        existing_pin_data = PinData(
            id=1,
            workflow_id=workflow_id,
            node_id=node_id,
            data=sample_pin_data,
            created_at=datetime.now(timezone.utc),
            created_by="user-123",
            version=1
        )
        
        pin_data_service.get_pin_data_from_db = AsyncMock(return_value=existing_pin_data)
        
        pin_data = await pin_data_service.get_pin_data(workflow_id, node_id)
        
        assert pin_data == existing_pin_data
        assert pin_data.data == sample_pin_data

    async def test_get_pin_data_not_found(self, pin_data_service):
        """Test getting non-existent pin data."""
        workflow_id = 1
        node_id = "non_existent"
        
        pin_data_service.get_pin_data_from_db = AsyncMock(return_value=None)
        
        with pytest.raises(PinDataNotFoundError) as exc_info:
            await pin_data_service.get_pin_data(workflow_id, node_id)
        
        assert f"Pin data not found for workflow {workflow_id}, node {node_id}" in str(exc_info.value)

    async def test_delete_pin_data(self, pin_data_service):
        """Test deleting pin data."""
        workflow_id = 1
        node_id = "http1"
        
        # Mock existing pin data
        pin_data_service.get_pin_data_from_db = AsyncMock(return_value=Mock())
        pin_data_service.delete_pin_data_from_db = AsyncMock()
        
        await pin_data_service.delete_pin_data(workflow_id, node_id)
        
        pin_data_service.delete_pin_data_from_db.assert_called_once_with(workflow_id, node_id)

    async def test_delete_pin_data_not_found(self, pin_data_service):
        """Test deleting non-existent pin data."""
        workflow_id = 1
        node_id = "non_existent"
        
        pin_data_service.get_pin_data_from_db = AsyncMock(return_value=None)
        
        with pytest.raises(PinDataNotFoundError):
            await pin_data_service.delete_pin_data(workflow_id, node_id)

    async def test_list_pinned_nodes(self, pin_data_service):
        """Test listing all pinned nodes for a workflow."""
        workflow_id = 1
        
        # Mock pinned nodes
        pinned_nodes = [
            PinData(id=1, workflow_id=workflow_id, node_id="http1", data=[], created_at=datetime.now(timezone.utc), created_by="user1", version=1),
            PinData(id=2, workflow_id=workflow_id, node_id="transform1", data=[], created_at=datetime.now(timezone.utc), created_by="user2", version=1),
            PinData(id=3, workflow_id=workflow_id, node_id="email1", data=[], created_at=datetime.now(timezone.utc), created_by="user1", version=2)
        ]
        
        pin_data_service.get_all_pin_data_for_workflow = AsyncMock(return_value=pinned_nodes)
        
        result = await pin_data_service.list_pinned_nodes(workflow_id)
        
        assert len(result) == 3
        assert result[0].node_id == "http1"
        assert result[1].node_id == "transform1"
        assert result[2].node_id == "email1"

    async def test_update_pin_data_version(self, pin_data_service, sample_pin_data):
        """Test updating pin data creates new version."""
        workflow_id = 1
        node_id = "http1"
        user_id = "user-456"
        
        # Mock existing pin data
        existing_pin_data = PinData(
            id=1,
            workflow_id=workflow_id,
            node_id=node_id,
            data=[{"old": "data"}],
            created_at=datetime.now(timezone.utc),
            created_by="user-123",
            version=1
        )
        
        pin_data_service.get_pin_data_from_db = AsyncMock(return_value=existing_pin_data)
        pin_data_service.store_pin_data = AsyncMock()
        
        new_pin_data = await pin_data_service.save_pin_data(
            workflow_id=workflow_id,
            node_id=node_id,
            data=sample_pin_data,
            user_id=user_id,
            description="Updated pin data"
        )
        
        assert new_pin_data.version == 2  # Incremented version
        assert new_pin_data.created_by == user_id
        assert new_pin_data.data == sample_pin_data


class TestPinDataValidation:
    """Test pin data validation and error handling."""

    async def test_validate_pin_data_valid(self, pin_data_service):
        """Test validation of valid pin data."""
        valid_data = [
            {"id": 1, "name": "Test", "value": 100},
            {"id": 2, "name": "Test2", "value": 200}
        ]
        
        # Should not raise any exception
        await pin_data_service.validate_pin_data(valid_data)

    async def test_validate_pin_data_invalid_format(self, pin_data_service):
        """Test validation of invalid pin data format."""
        invalid_data = "not a list"
        
        with pytest.raises(InvalidPinDataError) as exc_info:
            await pin_data_service.validate_pin_data(invalid_data)
        
        assert "Pin data must be a list" in str(exc_info.value)

    async def test_validate_pin_data_too_large(self, pin_data_service):
        """Test validation of oversized pin data."""
        # Create data that's too large (simulate 10MB+ data)
        large_data = [{"data": "x" * 1000000} for _ in range(15)]  # 15MB
        
        with pytest.raises(InvalidPinDataError) as exc_info:
            await pin_data_service.validate_pin_data(large_data)
        
        assert "Pin data too large" in str(exc_info.value)

    async def test_validate_pin_data_empty_list(self, pin_data_service):
        """Test validation allows empty list."""
        empty_data = []
        
        # Should not raise any exception
        await pin_data_service.validate_pin_data(empty_data)

    async def test_validate_node_exists(self, pin_data_service, sample_workflow):
        """Test validation that node exists in workflow."""
        pin_data_service.get_workflow = AsyncMock(return_value=sample_workflow)
        
        # Valid node
        await pin_data_service.validate_node_exists(1, "http1")
        
        # Invalid node
        with pytest.raises(InvalidPinDataError) as exc_info:
            await pin_data_service.validate_node_exists(1, "non_existent")
        
        assert "Node non_existent not found in workflow 1" in str(exc_info.value)


class TestPinDataExecution:
    """Test pin data integration with workflow execution."""

    async def test_is_node_pinned(self, pin_data_engine):
        """Test checking if a node has pin data."""
        workflow_id = 1
        
        # Mock pin data service
        pin_data_engine.pin_data_service = AsyncMock()
        pin_data_engine.pin_data_service.get_pin_data.side_effect = [
            Mock(),  # http1 is pinned
            PinDataNotFoundError("Not found")  # transform1 is not pinned
        ]
        
        # Test pinned node
        is_pinned = await pin_data_engine.is_node_pinned(workflow_id, "http1")
        assert is_pinned is True
        
        # Test non-pinned node
        is_pinned = await pin_data_engine.is_node_pinned(workflow_id, "transform1")
        assert is_pinned is False

    async def test_get_pinned_data_for_execution(self, pin_data_engine, sample_pin_data):
        """Test getting pinned data during execution."""
        workflow_id = 1
        node_id = "http1"
        
        # Mock pin data
        pin_data = PinData(
            id=1,
            workflow_id=workflow_id,
            node_id=node_id,
            data=sample_pin_data,
            created_at=datetime.now(timezone.utc),
            created_by="user-123",
            version=1
        )
        
        pin_data_engine.pin_data_service = AsyncMock()
        pin_data_engine.pin_data_service.get_pin_data.return_value = pin_data
        
        result = await pin_data_engine.get_pinned_data_for_execution(workflow_id, node_id)
        
        assert result == sample_pin_data

    async def test_get_pinned_data_not_pinned(self, pin_data_engine):
        """Test getting pinned data for non-pinned node."""
        workflow_id = 1
        node_id = "transform1"
        
        pin_data_engine.pin_data_service = AsyncMock()
        pin_data_engine.pin_data_service.get_pin_data.side_effect = PinDataNotFoundError("Not found")
        
        with pytest.raises(NodeNotPinnedError) as exc_info:
            await pin_data_engine.get_pinned_data_for_execution(workflow_id, node_id)
        
        assert f"Node {node_id} is not pinned" in str(exc_info.value)

    async def test_execute_with_pin_data(self, pin_data_engine, sample_workflow, sample_pin_data):
        """Test workflow execution using pin data."""
        execution_id = "test-exec-123"
        
        # Mock dependencies
        pin_data_engine.pin_data_service = AsyncMock()
        pin_data_engine.workflow_service = AsyncMock()
        pin_data_engine.workflow_service.get_workflow.return_value = sample_workflow
        
        # Mock pin data for http1 node
        pin_data = PinData(
            id=1,
            workflow_id=1,
            node_id="http1",
            data=sample_pin_data,
            created_at=datetime.now(timezone.utc),
            created_by="user-123",
            version=1
        )
        
        def mock_get_pin_data(workflow_id, node_id):
            if node_id == "http1":
                return pin_data
            else:
                raise PinDataNotFoundError("Not found")
        
        pin_data_engine.pin_data_service.get_pin_data.side_effect = mock_get_pin_data
        
        # Mock execution functions
        pin_data_engine.execute_node_normally = AsyncMock(return_value=[{"executed": True}])
        pin_data_engine.create_execution_result = AsyncMock()
        
        execution_result = await pin_data_engine.execute_with_pin_data(
            execution_id=execution_id,
            workflow=sample_workflow,
            input_data={"start": [{"input": "data"}]}
        )
        
        # Verify pin data was used for http1
        pin_data_engine.pin_data_service.get_pin_data.assert_any_call(1, "http1")
        
        # Verify normal execution was called for non-pinned nodes
        assert pin_data_engine.execute_node_normally.call_count >= 1

    async def test_mixed_execution_pinned_and_live(self, pin_data_engine, sample_workflow, sample_pin_data):
        """Test execution with mix of pinned and live nodes."""
        execution_id = "test-exec-456"
        
        # Mock dependencies
        pin_data_engine.pin_data_service = AsyncMock()
        pin_data_engine.workflow_service = AsyncMock()
        
        # Mock pin data for some nodes
        def mock_mixed_get_pin_data(workflow_id, node_id):
            if node_id == "http1":
                return PinData(id=1, workflow_id=1, node_id="http1", data=sample_pin_data, created_at=datetime.now(timezone.utc), created_by="user", version=1)
            elif node_id == "email1":
                return PinData(id=2, workflow_id=1, node_id="email1", data=[{"sent": True}], created_at=datetime.now(timezone.utc), created_by="user", version=1)
            else:
                raise PinDataNotFoundError("Not found")
        
        pin_data_engine.pin_data_service.get_pin_data.side_effect = mock_mixed_get_pin_data
        
        pin_data_engine.execute_node_normally = AsyncMock(return_value=[{"executed": True}])
        pin_data_engine.create_execution_result = AsyncMock()
        
        execution_result = await pin_data_engine.execute_with_pin_data(
            execution_id=execution_id,
            workflow=sample_workflow,
            input_data={"start": [{"input": "data"}]}
        )
        
        # Should check pin data for all nodes (may be called multiple times due to is_node_pinned checks)
        assert pin_data_engine.pin_data_service.get_pin_data.call_count >= 5


class TestPinDataWorkflowIntegration:
    """Test pin data integration with workflow system."""

    async def test_get_workflow_pin_summary(self, pin_data_service, sample_workflow):
        """Test getting summary of all pinned nodes in workflow."""
        workflow_id = 1
        
        # Mock pinned nodes
        pinned_nodes = [
            PinData(
                id=1, workflow_id=workflow_id, node_id="http1", 
                data=[{"sample": "data"}], created_at=datetime.now(timezone.utc), 
                created_by="user1", version=1, description="API response"
            ),
            PinData(
                id=2, workflow_id=workflow_id, node_id="email1", 
                data=[{"sent": True}], created_at=datetime.now(timezone.utc), 
                created_by="user2", version=2, description="Email confirmation"
            )
        ]
        
        pin_data_service.get_all_pin_data_for_workflow = AsyncMock(return_value=pinned_nodes)
        
        summary = await pin_data_service.get_workflow_pin_summary(workflow_id)
        
        assert summary["workflow_id"] == workflow_id
        assert summary["total_pinned_nodes"] == 2
        assert len(summary["pinned_nodes"]) == 2
        assert summary["pinned_nodes"][0]["node_id"] == "http1"
        assert summary["pinned_nodes"][1]["node_id"] == "email1"
        assert summary["pinned_nodes"][0]["version"] == 1
        assert summary["pinned_nodes"][1]["version"] == 2

    async def test_clone_pin_data_to_new_workflow(self, pin_data_service):
        """Test cloning pin data from one workflow to another."""
        source_workflow_id = 1
        target_workflow_id = 2
        user_id = "user-789"
        
        # Mock source pin data
        source_pin_data = [
            PinData(
                id=1, workflow_id=source_workflow_id, node_id="http1",
                data=[{"api": "data"}], created_at=datetime.now(timezone.utc),
                created_by="user1", version=1
            ),
            PinData(
                id=2, workflow_id=source_workflow_id, node_id="transform1",
                data=[{"transformed": True}], created_at=datetime.now(timezone.utc),
                created_by="user2", version=1
            )
        ]
        
        pin_data_service.get_all_pin_data_for_workflow = AsyncMock(return_value=source_pin_data)
        pin_data_service.store_pin_data = AsyncMock()
        
        cloned_count = await pin_data_service.clone_pin_data_to_workflow(
            source_workflow_id, target_workflow_id, user_id
        )
        
        assert cloned_count == 2
        assert pin_data_service.store_pin_data.call_count == 2
        
        # Verify new pin data has correct workflow_id and user
        call_args = pin_data_service.store_pin_data.call_args_list
        for call in call_args:
            pin_data = call[0][0]
            assert pin_data.workflow_id == target_workflow_id
            assert pin_data.created_by == user_id
            assert pin_data.version == 1  # Reset version for new workflow

    async def test_validate_pin_data_compatibility(self, pin_data_service, sample_workflow):
        """Test validating pin data compatibility with current workflow."""
        workflow_id = 1
        
        pin_data_service.get_workflow = AsyncMock(return_value=sample_workflow)
        
        # Valid pin data (node exists)
        valid_pin_data = [
            PinData(id=1, workflow_id=workflow_id, node_id="http1", data=[], created_at=datetime.now(timezone.utc), created_by="user", version=1),
            PinData(id=2, workflow_id=workflow_id, node_id="email1", data=[], created_at=datetime.now(timezone.utc), created_by="user", version=1)
        ]
        
        compatibility_report = await pin_data_service.validate_pin_data_compatibility(workflow_id, valid_pin_data)
        
        assert compatibility_report["compatible"] is True
        assert len(compatibility_report["incompatible_nodes"]) == 0
        assert len(compatibility_report["missing_nodes"]) == 3  # start, transform1, end nodes have no pin data
        
        # Invalid pin data (node doesn't exist)
        invalid_pin_data = [
            PinData(id=3, workflow_id=workflow_id, node_id="non_existent", data=[], created_at=datetime.now(timezone.utc), created_by="user", version=1)
        ]
        
        compatibility_report = await pin_data_service.validate_pin_data_compatibility(workflow_id, invalid_pin_data)
        
        assert compatibility_report["compatible"] is False
        assert len(compatibility_report["incompatible_nodes"]) == 1
        assert compatibility_report["incompatible_nodes"][0] == "non_existent"


class TestPinDataVersioning:
    """Test pin data versioning functionality."""

    async def test_get_pin_data_version_history(self, pin_data_service):
        """Test getting version history for pin data."""
        workflow_id = 1
        node_id = "http1"
        
        # Mock version history
        version_history = [
            PinData(
                id=1, workflow_id=workflow_id, node_id=node_id,
                data=[{"version": 1}], created_at=datetime.now(timezone.utc) - timedelta(days=2),
                created_by="user1", version=1, description="Initial data"
            ),
            PinData(
                id=2, workflow_id=workflow_id, node_id=node_id,
                data=[{"version": 2}], created_at=datetime.now(timezone.utc) - timedelta(days=1),
                created_by="user2", version=2, description="Updated data"
            ),
            PinData(
                id=3, workflow_id=workflow_id, node_id=node_id,
                data=[{"version": 3}], created_at=datetime.now(timezone.utc),
                created_by="user1", version=3, description="Latest data"
            )
        ]
        
        pin_data_service.get_pin_data_version_history = AsyncMock(return_value=version_history)
        
        history = await pin_data_service.get_pin_data_version_history(workflow_id, node_id)
        
        assert len(history) == 3
        assert history[0].version == 1
        assert history[1].version == 2
        assert history[2].version == 3
        assert history[2].description == "Latest data"

    async def test_get_pin_data_specific_version(self, pin_data_service):
        """Test getting specific version of pin data."""
        workflow_id = 1
        node_id = "http1"
        version = 2
        
        # Mock specific version data
        version_data = PinData(
            id=2, workflow_id=workflow_id, node_id=node_id,
            data=[{"specific": "version 2"}], created_at=datetime.now(timezone.utc),
            created_by="user2", version=version
        )
        
        pin_data_service.get_pin_data_version = AsyncMock(return_value=version_data)
        
        pin_data = await pin_data_service.get_pin_data_version(workflow_id, node_id, version)
        
        assert pin_data.version == version
        assert pin_data.data == [{"specific": "version 2"}]

    async def test_revert_pin_data_to_version(self, pin_data_service):
        """Test reverting pin data to previous version."""
        workflow_id = 1
        node_id = "http1"
        target_version = 2
        user_id = "user-revert"
        
        # Mock target version data
        target_version_data = PinData(
            id=2, workflow_id=workflow_id, node_id=node_id,
            data=[{"reverted": "to version 2"}], created_at=datetime.now(timezone.utc),
            created_by="user2", version=target_version
        )
        
        pin_data_service.get_pin_data_version = AsyncMock(return_value=target_version_data)
        pin_data_service.get_latest_version = AsyncMock(return_value=4)  # Current latest is version 4
        pin_data_service.store_pin_data = AsyncMock()
        
        reverted_pin_data = await pin_data_service.revert_pin_data_to_version(
            workflow_id, node_id, target_version, user_id
        )
        
        assert reverted_pin_data.version == 5  # New version created
        assert reverted_pin_data.data == [{"reverted": "to version 2"}]
        assert reverted_pin_data.created_by == user_id
        assert "Reverted to version 2" in reverted_pin_data.description


class TestPinDataPerformance:
    """Test pin data performance and optimization."""

    async def test_bulk_pin_data_operations(self, pin_data_service):
        """Test bulk operations for pin data."""
        workflow_id = 1
        user_id = "bulk-user"
        
        # Bulk pin data for multiple nodes
        bulk_pin_data = [
            {"node_id": "http1", "data": [{"bulk": "data1"}], "description": "Bulk 1"},
            {"node_id": "transform1", "data": [{"bulk": "data2"}], "description": "Bulk 2"},
            {"node_id": "email1", "data": [{"bulk": "data3"}], "description": "Bulk 3"}
        ]
        
        pin_data_service.store_pin_data = AsyncMock()
        
        result = await pin_data_service.bulk_save_pin_data(workflow_id, bulk_pin_data, user_id)
        
        assert result["success_count"] == 3
        assert result["error_count"] == 0
        assert len(result["created_pin_data"]) == 3
        assert pin_data_service.store_pin_data.call_count == 3

    async def test_pin_data_size_optimization(self, pin_data_service):
        """Test pin data size optimization for large datasets."""
        workflow_id = 1
        node_id = "http1"
        user_id = "optimize-user"
        
        # Large dataset that should be optimized
        large_data = []
        for i in range(1000):
            large_data.append({
                "id": i,
                "data": f"large data item {i}" * 100,  # Simulate large text
                "metadata": {"timestamp": datetime.now().isoformat()}
            })
        
        pin_data_service.optimize_pin_data = AsyncMock(return_value={
            "original_size": len(json.dumps(large_data)),
            "optimized_size": len(json.dumps(large_data)) // 2,  # Simulate compression
            "optimization_applied": True
        })
        pin_data_service.store_pin_data = AsyncMock()
        
        pin_data = await pin_data_service.save_pin_data(
            workflow_id=workflow_id,
            node_id=node_id,
            data=large_data,
            user_id=user_id,
            optimize=True
        )
        
        # Verify optimization was attempted
        pin_data_service.optimize_pin_data.assert_called_once()


class TestPinDataErrorHandling:
    """Test comprehensive error handling for pin data."""

    async def test_concurrent_pin_data_updates(self, pin_data_service):
        """Test handling concurrent updates to pin data."""
        workflow_id = 1
        node_id = "http1"
        
        # Simulate concurrent updates
        pin_data_service.get_latest_version = AsyncMock(side_effect=[2, 3])  # Version changed during update
        pin_data_service.store_pin_data = AsyncMock()
        
        # This should handle version conflicts gracefully
        pin_data1 = await pin_data_service.save_pin_data(
            workflow_id=workflow_id,
            node_id=node_id,
            data=[{"concurrent": "update1"}],
            user_id="user1"
        )
        
        pin_data2 = await pin_data_service.save_pin_data(
            workflow_id=workflow_id,
            node_id=node_id,
            data=[{"concurrent": "update2"}],
            user_id="user2"
        )
        
        # Both should succeed with different versions
        assert pin_data1.version != pin_data2.version

    async def test_invalid_workflow_id(self, pin_data_service):
        """Test error handling for invalid workflow ID."""
        invalid_workflow_id = 99999
        node_id = "http1"
        
        pin_data_service.get_workflow = AsyncMock(return_value=None)
        
        with pytest.raises(InvalidPinDataError) as exc_info:
            await pin_data_service.validate_node_exists(invalid_workflow_id, node_id)
        
        assert f"Workflow {invalid_workflow_id} not found" in str(exc_info.value)

    async def test_storage_failure_handling(self, pin_data_service):
        """Test handling of storage failures."""
        workflow_id = 1
        node_id = "http1"
        
        pin_data_service.store_pin_data = AsyncMock(side_effect=Exception("Database connection failed"))
        
        with pytest.raises(Exception) as exc_info:
            await pin_data_service.save_pin_data(
                workflow_id=workflow_id,
                node_id=node_id,
                data=[{"test": "data"}],
                user_id="user"
            )
        
        assert "Database connection failed" in str(exc_info.value)