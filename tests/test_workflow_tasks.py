"""Tests for workflow execution tasks."""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone
from uuid import UUID, uuid4

from celery import states
from celery.exceptions import Retry, MaxRetriesExceededError

from budflow.workflows.tasks import (
    execute_workflow_task,
    execute_node_task,
    cleanup_execution_task,
    retry_failed_execution_task,
    schedule_workflow_task,
    cancel_workflow_task,
    WorkflowTaskError,
    NodeTaskError,
)


@pytest.fixture
def mock_workflow_service():
    """Mock workflow service."""
    service = AsyncMock()
    service.get_workflow = AsyncMock()
    service.create_execution = AsyncMock()
    service.update_execution = AsyncMock()
    return service


@pytest.fixture
def mock_execution_service():
    """Mock execution service."""
    service = AsyncMock()
    service.get_execution = AsyncMock()
    service.update_execution = AsyncMock()
    service.cancel_execution = AsyncMock()
    return service


@pytest.fixture
def mock_executor():
    """Mock workflow executor."""
    executor = AsyncMock()
    executor.execute_workflow = AsyncMock()
    executor.execute_node = AsyncMock()
    return executor


@pytest.fixture
def sample_workflow_data():
    """Sample workflow data."""
    return {
        "workflow_id": str(uuid4()),
        "trigger_data": {"source": "manual", "user_id": str(uuid4())},
        "initial_data": {"key": "value"},
        "execution_mode": "manual"
    }


@pytest.fixture
def sample_node_data():
    """Sample node execution data."""
    return {
        "execution_id": str(uuid4()),
        "node_id": "http_request_1",
        "input_data": {"url": "https://api.example.com"},
        "retry_count": 0
    }


class TestWorkflowTasks:
    """Test workflow execution tasks."""
    
    @pytest.mark.asyncio
    async def test_execute_workflow_task_success(
        self,
        mock_workflow_service,
        mock_execution_service,
        mock_executor,
        sample_workflow_data
    ):
        """Test successful workflow execution."""
        # Setup mocks
        workflow = Mock(id=sample_workflow_data["workflow_id"], name="Test Workflow")
        execution = Mock(id=str(uuid4()), status="completed")
        
        mock_workflow_service.get_workflow.return_value = workflow
        mock_execution_service.create_execution.return_value = execution
        mock_executor.execute_workflow.return_value = execution
        
        with patch("budflow.workflows.tasks.get_workflow_service", return_value=mock_workflow_service):
            with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
                with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                    # Execute task
                    result = await execute_workflow_task.apply_async(
                        kwargs=sample_workflow_data
                    ).get()
                    
                    # Verify
                    assert result["status"] == "completed"
                    assert "execution_id" in result
                    mock_workflow_service.get_workflow.assert_called_once()
                    mock_executor.execute_workflow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_workflow_task_not_found(
        self,
        mock_workflow_service,
        sample_workflow_data
    ):
        """Test workflow execution with non-existent workflow."""
        mock_workflow_service.get_workflow.return_value = None
        
        with patch("budflow.workflows.tasks.get_workflow_service", return_value=mock_workflow_service):
            with pytest.raises(WorkflowTaskError) as exc_info:
                await execute_workflow_task.apply_async(
                    kwargs=sample_workflow_data
                ).get()
            
            assert "not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_execute_workflow_task_retry_on_failure(
        self,
        mock_workflow_service,
        mock_executor,
        sample_workflow_data
    ):
        """Test workflow execution retry on failure."""
        workflow = Mock(id=sample_workflow_data["workflow_id"])
        mock_workflow_service.get_workflow.return_value = workflow
        mock_executor.execute_workflow.side_effect = Exception("Temporary error")
        
        with patch("budflow.workflows.tasks.get_workflow_service", return_value=mock_workflow_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                with pytest.raises(Retry):
                    await execute_workflow_task.apply_async(
                        kwargs=sample_workflow_data
                    ).get()
    
    @pytest.mark.asyncio
    async def test_execute_node_task_success(
        self,
        mock_execution_service,
        mock_executor,
        sample_node_data
    ):
        """Test successful node execution."""
        execution = Mock(id=sample_node_data["execution_id"], status="running")
        node_result = {"output": {"result": "success"}, "status": "completed"}
        
        mock_execution_service.get_execution.return_value = execution
        mock_executor.execute_node.return_value = node_result
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                result = await execute_node_task.apply_async(
                    kwargs=sample_node_data
                ).get()
                
                assert result["status"] == "completed"
                assert "output" in result
                mock_executor.execute_node.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_node_task_with_retry(
        self,
        mock_execution_service,
        mock_executor,
        sample_node_data
    ):
        """Test node execution with retry."""
        execution = Mock(id=sample_node_data["execution_id"], status="running")
        mock_execution_service.get_execution.return_value = execution
        
        # First call fails, second succeeds
        mock_executor.execute_node.side_effect = [
            Exception("Network error"),
            {"output": {"result": "success"}, "status": "completed"}
        ]
        
        sample_node_data["retry_count"] = 1
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                result = await execute_node_task.apply_async(
                    kwargs=sample_node_data
                ).get()
                
                assert result["status"] == "completed"
    
    @pytest.mark.asyncio
    async def test_cleanup_execution_task(
        self,
        mock_execution_service
    ):
        """Test execution cleanup task."""
        old_executions = [
            Mock(id=str(uuid4()), created_at=datetime.now(timezone.utc)),
            Mock(id=str(uuid4()), created_at=datetime.now(timezone.utc))
        ]
        
        mock_execution_service.cleanup_old_executions.return_value = len(old_executions)
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            result = await cleanup_execution_task.apply_async(
                kwargs={"days": 30}
            ).get()
            
            assert result["cleaned_count"] == 2
            mock_execution_service.cleanup_old_executions.assert_called_once_with(days=30)
    
    @pytest.mark.asyncio
    async def test_retry_failed_execution_task(
        self,
        mock_execution_service,
        mock_executor
    ):
        """Test retry failed execution task."""
        execution_id = str(uuid4())
        execution = Mock(id=execution_id, status="failed", retry_count=1)
        
        mock_execution_service.get_execution.return_value = execution
        mock_executor.retry_execution.return_value = Mock(status="completed")
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                result = await retry_failed_execution_task.apply_async(
                    kwargs={"execution_id": execution_id}
                ).get()
                
                assert result["status"] == "completed"
                assert result["retried"] is True
    
    @pytest.mark.asyncio
    async def test_schedule_workflow_task(
        self,
        mock_workflow_service,
        sample_workflow_data
    ):
        """Test workflow scheduling task."""
        workflow = Mock(id=sample_workflow_data["workflow_id"], schedule_config={
            "cron": "0 * * * *",
            "timezone": "UTC"
        })
        
        mock_workflow_service.get_workflow.return_value = workflow
        
        with patch("budflow.workflows.tasks.get_workflow_service", return_value=mock_workflow_service):
            with patch("budflow.workflows.tasks.execute_workflow_task.apply_async") as mock_execute:
                result = await schedule_workflow_task.apply_async(
                    kwargs={"workflow_id": sample_workflow_data["workflow_id"]}
                ).get()
                
                assert result["scheduled"] is True
                mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cancel_workflow_task(
        self,
        mock_execution_service
    ):
        """Test workflow cancellation task."""
        execution_id = str(uuid4())
        execution = Mock(id=execution_id, status="running")
        
        mock_execution_service.get_execution.return_value = execution
        mock_execution_service.cancel_execution.return_value = Mock(status="cancelled")
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            result = await cancel_workflow_task.apply_async(
                kwargs={"execution_id": execution_id}
            ).get()
            
            assert result["status"] == "cancelled"
            mock_execution_service.cancel_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_task_timeout_handling(
        self,
        mock_workflow_service,
        mock_executor,
        sample_workflow_data
    ):
        """Test task timeout handling."""
        workflow = Mock(id=sample_workflow_data["workflow_id"])
        mock_workflow_service.get_workflow.return_value = workflow
        
        # Simulate timeout
        mock_executor.execute_workflow.side_effect = TimeoutError("Task timed out")
        
        with patch("budflow.workflows.tasks.get_workflow_service", return_value=mock_workflow_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                with pytest.raises(WorkflowTaskError) as exc_info:
                    await execute_workflow_task.apply_async(
                        kwargs=sample_workflow_data,
                        time_limit=60
                    ).get()
                
                assert "timed out" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_task_rate_limiting(
        self,
        mock_workflow_service,
        sample_workflow_data
    ):
        """Test task rate limiting."""
        # Test that tasks respect rate limits
        with patch("budflow.workflows.tasks.RATE_LIMIT_KEY", "test_rate_limit"):
            with patch("budflow.workflows.tasks.check_rate_limit") as mock_check:
                mock_check.return_value = False  # Rate limit exceeded
                
                with pytest.raises(Retry) as exc_info:
                    await execute_workflow_task.apply_async(
                        kwargs=sample_workflow_data
                    ).get()
                
                assert "rate limit" in str(exc_info.value).lower()


class TestNodeTasks:
    """Test node execution tasks."""
    
    @pytest.mark.asyncio
    async def test_parallel_node_execution(
        self,
        mock_execution_service,
        mock_executor
    ):
        """Test parallel node execution."""
        execution_id = str(uuid4())
        node_ids = ["node1", "node2", "node3"]
        
        # Create tasks for parallel execution
        tasks = []
        for node_id in node_ids:
            task_data = {
                "execution_id": execution_id,
                "node_id": node_id,
                "input_data": {"value": node_id}
            }
            tasks.append(execute_node_task.s(**task_data))
        
        # Test group execution
        from celery import group
        job = group(tasks)
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                mock_executor.execute_node.return_value = {"status": "completed"}
                
                results = job.apply_async().get()
                assert len(results) == 3
                assert all(r["status"] == "completed" for r in results)
    
    @pytest.mark.asyncio
    async def test_node_error_propagation(
        self,
        mock_execution_service,
        mock_executor,
        sample_node_data
    ):
        """Test error propagation from node execution."""
        execution = Mock(id=sample_node_data["execution_id"])
        mock_execution_service.get_execution.return_value = execution
        
        # Node raises specific error
        mock_executor.execute_node.side_effect = NodeTaskError("Invalid node configuration")
        
        with patch("budflow.workflows.tasks.get_execution_service", return_value=mock_execution_service):
            with patch("budflow.workflows.tasks.get_executor", return_value=mock_executor):
                with pytest.raises(NodeTaskError) as exc_info:
                    await execute_node_task.apply_async(
                        kwargs=sample_node_data
                    ).get()
                
                assert "Invalid node configuration" in str(exc_info.value)