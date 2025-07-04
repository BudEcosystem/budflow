"""Test workflow Celery tasks."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from uuid import UUID, uuid4

import pytest
from celery import states
from celery.exceptions import Retry

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
from budflow.workflows.models import ExecutionStatus, ExecutionMode
from budflow.executions.schemas import ExecutionCreate


@pytest.fixture
def mock_db_session():
    """Create mock database session."""
    session = AsyncMock()
    session.close = AsyncMock()
    return session


@pytest.fixture
def mock_workflow():
    """Create mock workflow."""
    workflow = Mock()
    workflow.id = uuid4()
    workflow.name = "Test Workflow"
    workflow.active = True
    workflow.nodes = [
        Mock(
            node_id="node_1",
            type="http.request",
            name="HTTP Request",
            parameters={"url": "https://api.example.com"},
            position={"x": 100, "y": 100}
        ),
        Mock(
            node_id="node_2",
            type="code",
            name="Process Data",
            parameters={"code": "return {processed: true}"},
            position={"x": 300, "y": 100}
        )
    ]
    workflow.connections = {
        "node_1": {
            "main": [[{"node": "node_2", "type": "main", "index": 0}]]
        }
    }
    workflow.settings = {"rate_limit": 100}
    return workflow


@pytest.fixture
def mock_execution():
    """Create mock execution."""
    execution = Mock()
    execution.id = uuid4()
    execution.workflow_id = uuid4()
    execution.status = ExecutionStatus.SUCCESS
    execution.mode = ExecutionMode.MANUAL
    execution.data = {"result": "success"}
    execution.error = None
    execution.finished_at = datetime.now(timezone.utc)
    return execution


@pytest.fixture
def mock_workflow_service():
    """Create mock workflow service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_execution_service():
    """Create mock execution service."""
    service = AsyncMock()
    return service


@pytest.mark.unit
class TestExecuteWorkflowTask:
    """Test execute_workflow_task."""
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.WorkflowService')
    @patch('budflow.workflows.tasks.ExecutionService')
    @patch('budflow.workflows.tasks.check_rate_limit')
    def test_execute_workflow_success(
        self,
        mock_rate_limit,
        mock_execution_service_class,
        mock_workflow_service_class,
        mock_get_db,
        mock_db_session,
        mock_workflow,
        mock_execution
    ):
        """Test successful workflow execution."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        mock_rate_limit.return_value = True
        
        workflow_service = mock_workflow_service_class.return_value
        workflow_service.get_workflow = AsyncMock(return_value=mock_workflow)
        
        execution_service = mock_execution_service_class.return_value
        execution_service.create_execution = AsyncMock(return_value=mock_execution)
        
        # Execute task
        result = execute_workflow_task(
            workflow_id=str(mock_workflow.id),
            trigger_data={"type": "manual"},
            initial_data={"input": "data"},
            execution_mode="manual"
        )
        
        # Verify result
        assert result["execution_id"] == str(mock_execution.id)
        assert result["status"] == "success"
        assert result["output_data"] == {"result": "success"}
        assert result["error"] is None
        
        # Verify service calls
        workflow_service.get_workflow.assert_called_once()
        execution_service.create_execution.assert_called_once()
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.WorkflowService')
    def test_execute_workflow_not_found(
        self,
        mock_workflow_service_class,
        mock_get_db,
        mock_db_session
    ):
        """Test workflow not found error."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        workflow_service = mock_workflow_service_class.return_value
        workflow_service.get_workflow = AsyncMock(return_value=None)
        
        # Execute task and expect error
        with pytest.raises(WorkflowTaskError) as exc_info:
            execute_workflow_task(
                workflow_id="999",
                trigger_data={"type": "manual"}
            )
        
        assert "Workflow 999 not found" in str(exc_info.value)
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.WorkflowService')
    @patch('budflow.workflows.tasks.check_rate_limit')
    @patch('budflow.workflows.tasks.execute_workflow_task')
    def test_execute_workflow_rate_limit(
        self,
        mock_task,
        mock_rate_limit,
        mock_workflow_service_class,
        mock_get_db,
        mock_db_session,
        mock_workflow
    ):
        """Test workflow rate limit handling."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        mock_rate_limit.return_value = False  # Rate limit exceeded
        
        workflow_service = mock_workflow_service_class.return_value
        workflow_service.get_workflow = AsyncMock(return_value=mock_workflow)
        
        # Setup retry mock
        mock_retry = Mock()
        mock_task.retry = mock_retry
        mock_retry.side_effect = Retry("Rate limit retry")
        
        # Execute task and expect retry
        with pytest.raises(Retry):
            execute_workflow_task(
                workflow_id=str(mock_workflow.id),
                trigger_data={"type": "manual"}
            )
        
        # Verify retry was called
        mock_retry.assert_called_once()


@pytest.mark.unit
class TestExecuteNodeTask:
    """Test execute_node_task."""
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.ExecutionService')
    @patch('budflow.workflows.tasks.WorkflowExecutionEngine')
    def test_execute_node_success(
        self,
        mock_engine_class,
        mock_execution_service_class,
        mock_get_db,
        mock_db_session,
        mock_execution,
        mock_workflow
    ):
        """Test successful node execution."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        mock_execution.workflow = mock_workflow
        execution_service = mock_execution_service_class.return_value
        execution_service.get_execution = AsyncMock(return_value=mock_execution)
        
        node_execution = Mock()
        node_execution.status = ExecutionStatus.SUCCESS
        node_execution.output_data = {"processed": True}
        node_execution.error = None
        node_execution.execution_time_ms = 100
        execution_service.get_node_execution = AsyncMock(return_value=node_execution)
        
        engine = mock_engine_class.return_value
        engine._execute_node = AsyncMock()
        
        # Execute task
        result = execute_node_task(
            execution_id=str(mock_execution.id),
            node_id="node_1",
            input_data={"input": "data"}
        )
        
        # Verify result
        assert result["node_id"] == "node_1"
        assert result["status"] == "success"
        assert result["output"] == {"processed": True}
        assert result["error"] is None
        assert result["execution_time"] == 100
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.ExecutionService')
    def test_execute_node_not_found(
        self,
        mock_execution_service_class,
        mock_get_db,
        mock_db_session,
        mock_execution,
        mock_workflow
    ):
        """Test node not found error."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        mock_execution.workflow = mock_workflow
        execution_service = mock_execution_service_class.return_value
        execution_service.get_execution = AsyncMock(return_value=mock_execution)
        
        # Execute task with non-existent node
        with pytest.raises(NodeTaskError) as exc_info:
            execute_node_task(
                execution_id=str(mock_execution.id),
                node_id="non_existent",
                input_data={}
            )
        
        assert "Node non_existent not found" in str(exc_info.value)


@pytest.mark.unit
class TestCleanupExecutionTask:
    """Test cleanup_execution_task."""
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.ExecutionService')
    def test_cleanup_executions_success(
        self,
        mock_execution_service_class,
        mock_get_db,
        mock_db_session
    ):
        """Test successful execution cleanup."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        execution_service = mock_execution_service_class.return_value
        execution_service.cleanup_old_executions = AsyncMock(return_value=42)
        
        # Execute task
        result = cleanup_execution_task(days=30)
        
        # Verify result
        assert result["cleaned_count"] == 42
        assert result["cutoff_days"] == 30
        
        # Verify service call
        execution_service.cleanup_old_executions.assert_called_once()


@pytest.mark.unit
class TestRetryFailedExecutionTask:
    """Test retry_failed_execution_task."""
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.ExecutionService')
    def test_retry_failed_execution_success(
        self,
        mock_execution_service_class,
        mock_get_db,
        mock_db_session,
        mock_execution
    ):
        """Test successful execution retry."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        # Set execution to failed state
        mock_execution.status = ExecutionStatus.ERROR
        
        new_execution = Mock()
        new_execution.id = uuid4()
        new_execution.status = ExecutionStatus.SUCCESS
        
        execution_service = mock_execution_service_class.return_value
        execution_service.get_execution = AsyncMock(return_value=mock_execution)
        execution_service.create_execution = AsyncMock(return_value=new_execution)
        
        # Execute task
        result = retry_failed_execution_task(execution_id=str(mock_execution.id))
        
        # Verify result
        assert result["execution_id"] == str(new_execution.id)
        assert result["status"] == "success"
        assert result["retried"] is True
        assert result["original_execution_id"] == str(mock_execution.id)
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.ExecutionService')
    def test_retry_non_failed_execution(
        self,
        mock_execution_service_class,
        mock_get_db,
        mock_db_session,
        mock_execution
    ):
        """Test retry of non-failed execution."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        # Set execution to success state
        mock_execution.status = ExecutionStatus.SUCCESS
        
        execution_service = mock_execution_service_class.return_value
        execution_service.get_execution = AsyncMock(return_value=mock_execution)
        
        # Execute task and expect error
        with pytest.raises(WorkflowTaskError) as exc_info:
            retry_failed_execution_task(execution_id=str(mock_execution.id))
        
        assert "is not in failed state" in str(exc_info.value)


@pytest.mark.unit
class TestScheduleWorkflowTask:
    """Test schedule_workflow_task."""
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.WorkflowService')
    @patch('budflow.workflows.tasks.execute_workflow_task')
    def test_schedule_workflow_success(
        self,
        mock_execute_task,
        mock_workflow_service_class,
        mock_get_db,
        mock_db_session,
        mock_workflow
    ):
        """Test successful workflow scheduling."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        # Add schedule to workflow
        mock_workflow.settings["schedule"] = {"cron": "0 0 * * *"}
        
        workflow_service = mock_workflow_service_class.return_value
        workflow_service.get_workflow = AsyncMock(return_value=mock_workflow)
        
        mock_execute_task.apply_async = Mock()
        
        # Execute task
        result = schedule_workflow_task(workflow_id=str(mock_workflow.id))
        
        # Verify result
        assert result["workflow_id"] == str(mock_workflow.id)
        assert result["scheduled"] is True
        assert result["schedule_config"] == {"cron": "0 0 * * *"}
        
        # Verify task was scheduled
        mock_execute_task.apply_async.assert_called_once()


@pytest.mark.unit
class TestCancelWorkflowTask:
    """Test cancel_workflow_task."""
    
    @patch('budflow.workflows.tasks.get_db')
    @patch('budflow.workflows.tasks.ExecutionService')
    def test_cancel_workflow_success(
        self,
        mock_execution_service_class,
        mock_get_db,
        mock_db_session,
        mock_execution
    ):
        """Test successful workflow cancellation."""
        # Setup mocks
        mock_get_db.return_value.__aiter__.return_value = [mock_db_session]
        
        # Set execution to running state
        mock_execution.status = ExecutionStatus.RUNNING
        
        execution_service = mock_execution_service_class.return_value
        execution_service.get_execution = AsyncMock(return_value=mock_execution)
        
        # Execute task
        result = cancel_workflow_task(execution_id=str(mock_execution.id))
        
        # Verify result
        assert result["execution_id"] == str(mock_execution.id)
        assert result["status"] == "cancelled"
        assert result["cancelled"] is True
        
        # Verify execution was updated
        assert mock_execution.status == ExecutionStatus.CANCELED
        assert mock_execution.finished_at is not None


@pytest.mark.integration
class TestWorkflowTasksIntegration:
    """Integration tests for workflow tasks."""
    
    @pytest.mark.asyncio
    @patch('budflow.workflows.tasks.get_db')
    async def test_workflow_execution_flow(
        self,
        mock_get_db,
        mock_db_session,
        mock_workflow,
        mock_execution
    ):
        """Test complete workflow execution flow."""
        # This would test the full integration with real services
        # For now, just verify the task structure is correct
        
        # Verify task registration
        from budflow.worker import celery_app
        
        assert 'budflow.execute_workflow' in celery_app.tasks
        assert 'budflow.execute_node' in celery_app.tasks
        assert 'budflow.cleanup_executions' in celery_app.tasks
        assert 'budflow.retry_failed_execution' in celery_app.tasks
        assert 'budflow.schedule_workflow' in celery_app.tasks
        assert 'budflow.cancel_workflow' in celery_app.tasks
        
        # Verify task routing
        assert celery_app.conf.task_routes['budflow.execute_workflow']['queue'] == 'workflows'
        assert celery_app.conf.task_routes['budflow.cleanup_executions']['queue'] == 'maintenance'
        assert celery_app.conf.task_routes['budflow.schedule_workflow']['queue'] == 'scheduler'