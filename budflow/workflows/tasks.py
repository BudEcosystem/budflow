"""Celery tasks for workflow execution.

This module contains all Celery tasks related to workflow execution,
including workflow execution, node execution, cleanup, and retry tasks.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List
from uuid import UUID

import structlog
from celery import Task, states
from celery.exceptions import Retry, MaxRetriesExceededError
from celery.utils.log import get_task_logger

from budflow.config import settings
from budflow.database import get_db
from budflow.workflows.service import WorkflowService
from budflow.executions.service import ExecutionService
from budflow.executions.schemas import ExecutionCreate
from budflow.executor.engine import WorkflowExecutionEngine
from budflow.workflows.models import ExecutionStatus, ExecutionMode
from budflow.worker import celery_app
from budflow.metrics import metrics
from budflow.utils.rate_limit import check_rate_limit

logger = structlog.get_logger()
task_logger = get_task_logger(__name__)


class WorkflowTaskError(Exception):
    """Base exception for workflow task errors."""
    pass


class NodeTaskError(Exception):
    """Base exception for node task errors."""
    pass


class WorkflowTask(Task):
    """Base class for workflow tasks with common functionality."""
    
    autoretry_for = (Exception,)
    retry_kwargs = {'max_retries': 3, 'countdown': 5}
    retry_backoff = True
    retry_backoff_max = 300
    retry_jitter = True
    
    def before_start(self, task_id, args, kwargs):
        """Called before task execution starts."""
        logger.info(
            "Starting workflow task",
            task_id=task_id,
            task_name=self.name,
            kwargs=kwargs
        )
        metrics.increment("workflow_tasks_started", tags={"task": self.name})
    
    def on_success(self, retval, task_id, args, kwargs):
        """Called on successful task completion."""
        logger.info(
            "Workflow task completed successfully",
            task_id=task_id,
            task_name=self.name,
            result=retval
        )
        metrics.increment("workflow_tasks_completed", tags={"task": self.name})
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called on task failure."""
        logger.error(
            "Workflow task failed",
            task_id=task_id,
            task_name=self.name,
            error=str(exc),
            exc_info=einfo
        )
        metrics.increment("workflow_tasks_failed", tags={"task": self.name})
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Called when task is retried."""
        logger.warning(
            "Retrying workflow task",
            task_id=task_id,
            task_name=self.name,
            error=str(exc),
            retry_count=self.request.retries
        )
        metrics.increment("workflow_tasks_retried", tags={"task": self.name})


@celery_app.task(
    base=WorkflowTask,
    name="budflow.execute_workflow",
    queue="workflows",
    time_limit=settings.max_execution_time,
    soft_time_limit=settings.max_execution_time - 60
)
def execute_workflow_task(
    workflow_id: str,
    trigger_data: Dict[str, Any],
    initial_data: Optional[Dict[str, Any]] = None,
    execution_mode: str = "manual",
    parent_execution_id: Optional[str] = None
) -> Dict[str, Any]:
    """Execute a workflow.
    
    Args:
        workflow_id: ID of the workflow to execute
        trigger_data: Data from the trigger that started the workflow
        initial_data: Initial data to pass to the workflow
        execution_mode: Execution mode (manual, trigger, schedule, etc.)
        parent_execution_id: Parent execution ID for sub-workflows
    
    Returns:
        Execution result with status and data
    """
    try:
        # Run async code in sync context
        return asyncio.run(_execute_workflow(
            workflow_id,
            trigger_data,
            initial_data,
            execution_mode,
            parent_execution_id
        ))
    except Exception as e:
        # Check if we should retry
        if execute_workflow_task.request.retries < execute_workflow_task.max_retries:
            logger.warning(f"Workflow execution failed, retrying: {str(e)}")
            raise execute_workflow_task.retry(exc=e)
        else:
            logger.error(f"Workflow execution failed after all retries: {str(e)}")
            raise WorkflowTaskError(f"Workflow execution failed: {str(e)}")


async def _execute_workflow(
    workflow_id: str,
    trigger_data: Dict[str, Any],
    initial_data: Optional[Dict[str, Any]],
    execution_mode: str,
    parent_execution_id: Optional[str]
) -> Dict[str, Any]:
    """Async implementation of workflow execution."""
    async for db_session in get_db():
        try:
            workflow_service = WorkflowService(db_session)
            execution_service = ExecutionService(db_session)
            
            # Get workflow
            workflow = await workflow_service.get_workflow(int(workflow_id))
            if not workflow:
                raise WorkflowTaskError(f"Workflow {workflow_id} not found")
            
            # Check rate limits
            rate_limit_key = f"workflow:{workflow_id}:executions"
            if not check_rate_limit(rate_limit_key, workflow.settings.get("rate_limit", 100)):
                raise execute_workflow_task.retry(
                    exc=WorkflowTaskError("Rate limit exceeded"),
                    countdown=60
                )
            
            # Create execution record
            execution_create = ExecutionCreate(
                workflow_id=UUID(workflow_id),
                mode=execution_mode,
                data={
                    **initial_data or {},
                    "_trigger": trigger_data,
                    "_parent_execution_id": parent_execution_id
                }
            )
            
            # Create and execute workflow
            execution = await execution_service.create_execution(
                execution_data=execution_create,
                trigger_node_id=trigger_data.get("node_id"),
                queue_mode=False  # Execute directly, not queue again
            )
            
            return {
                "execution_id": str(execution.id),
                "status": execution.status.value,
                "output_data": execution.data,
                "error": execution.error,
                "finished_at": execution.finished_at.isoformat() if execution.finished_at else None
            }
            
        finally:
            await db_session.close()


@celery_app.task(
    base=WorkflowTask,
    name="budflow.execute_node",
    queue="workflows",
    time_limit=300,
    soft_time_limit=240
)
def execute_node_task(
    execution_id: str,
    node_id: str,
    input_data: Dict[str, Any],
    retry_count: int = 0,
    timeout: Optional[int] = None
) -> Dict[str, Any]:
    """Execute a single node within a workflow.
    
    Args:
        execution_id: ID of the workflow execution
        node_id: ID of the node to execute
        input_data: Input data for the node
        retry_count: Number of retries attempted
        timeout: Optional timeout in seconds
    
    Returns:
        Node execution result
    """
    try:
        return asyncio.run(_execute_node(
            execution_id,
            node_id,
            input_data,
            retry_count,
            timeout
        ))
    except Exception as e:
        if retry_count < 3:
            # Retry with exponential backoff
            countdown = (2 ** retry_count) * 5
            logger.warning(f"Node execution failed, retrying in {countdown}s: {str(e)}")
            raise execute_node_task.retry(
                kwargs={
                    "execution_id": execution_id,
                    "node_id": node_id,
                    "input_data": input_data,
                    "retry_count": retry_count + 1,
                    "timeout": timeout
                },
                countdown=countdown
            )
        else:
            raise NodeTaskError(f"Node execution failed after {retry_count} retries: {str(e)}")


async def _execute_node(
    execution_id: str,
    node_id: str,
    input_data: Dict[str, Any],
    retry_count: int,
    timeout: Optional[int]
) -> Dict[str, Any]:
    """Async implementation of node execution."""
    async for db_session in get_db():
        try:
            execution_service = ExecutionService(db_session)
            engine = WorkflowExecutionEngine(db_session)
            
            # Get execution
            execution = await execution_service.get_execution(UUID(execution_id))
            if not execution:
                raise NodeTaskError(f"Execution {execution_id} not found")
            
            # Get workflow
            workflow = execution.workflow
            
            # Find node
            node = None
            for wf_node in workflow.nodes:
                if wf_node.node_id == node_id:
                    node = wf_node
                    break
            
            if not node:
                raise NodeTaskError(f"Node {node_id} not found in workflow")
            
            # Execute node
            await engine._execute_node(
                workflow=workflow,
                node=node,
                context=None,  # Would need proper context
                input_data=input_data
            )
            
            # Get node execution result
            node_execution = await execution_service.get_node_execution(
                execution_id=UUID(execution_id),
                node_id=node_id
            )
            
            return {
                "node_id": node_id,
                "status": node_execution.status.value if node_execution else "error",
                "output": node_execution.output_data if node_execution else {},
                "error": node_execution.error if node_execution else None,
                "execution_time": node_execution.execution_time_ms if node_execution else None
            }
            
        finally:
            await db_session.close()


@celery_app.task(
    name="budflow.cleanup_executions",
    queue="maintenance"
)
def cleanup_execution_task(days: int = 30) -> Dict[str, Any]:
    """Clean up old workflow executions.
    
    Args:
        days: Number of days to keep executions
    
    Returns:
        Cleanup result with count of cleaned executions
    """
    return asyncio.run(_cleanup_executions(days))


async def _cleanup_executions(days: int) -> Dict[str, Any]:
    """Async implementation of execution cleanup."""
    async for db_session in get_db():
        try:
            execution_service = ExecutionService(db_session)
            
            # Clean up old executions
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            cleaned_count = await execution_service.cleanup_old_executions(
                before_date=cutoff_date
            )
            
            logger.info(f"Cleaned up {cleaned_count} old executions")
            metrics.gauge("executions_cleaned", cleaned_count)
            
            return {
                "cleaned_count": cleaned_count,
                "cutoff_days": days
            }
            
        finally:
            await db_session.close()


@celery_app.task(
    base=WorkflowTask,
    name="budflow.retry_failed_execution",
    queue="workflows"
)
def retry_failed_execution_task(execution_id: str) -> Dict[str, Any]:
    """Retry a failed workflow execution.
    
    Args:
        execution_id: ID of the execution to retry
    
    Returns:
        Retry result
    """
    return asyncio.run(_retry_failed_execution(execution_id))


async def _retry_failed_execution(execution_id: str) -> Dict[str, Any]:
    """Async implementation of execution retry."""
    async for db_session in get_db():
        try:
            execution_service = ExecutionService(db_session)
            
            # Get execution
            execution = await execution_service.get_execution(UUID(execution_id))
            if not execution:
                raise WorkflowTaskError(f"Execution {execution_id} not found")
            
            if execution.status != ExecutionStatus.ERROR:
                raise WorkflowTaskError(f"Execution {execution_id} is not in failed state")
            
            # Create new execution with same data
            execution_create = ExecutionCreate(
                workflow_id=execution.workflow_id,
                mode=execution.mode,
                data=execution.data
            )
            
            # Create and execute workflow
            new_execution = await execution_service.create_execution(
                execution_data=execution_create,
                queue_mode=False
            )
            
            return {
                "execution_id": str(new_execution.id),
                "status": new_execution.status.value,
                "retried": True,
                "original_execution_id": str(execution.id)
            }
            
        finally:
            await db_session.close()


@celery_app.task(
    name="budflow.schedule_workflow",
    queue="scheduler"
)
def schedule_workflow_task(workflow_id: str) -> Dict[str, Any]:
    """Schedule a workflow for execution based on its schedule configuration.
    
    Args:
        workflow_id: ID of the workflow to schedule
    
    Returns:
        Scheduling result
    """
    return asyncio.run(_schedule_workflow(workflow_id))


async def _schedule_workflow(workflow_id: str) -> Dict[str, Any]:
    """Async implementation of workflow scheduling."""
    async for db_session in get_db():
        try:
            workflow_service = WorkflowService(db_session)
            
            # Get workflow
            workflow = await workflow_service.get_workflow(int(workflow_id))
            if not workflow:
                raise WorkflowTaskError(f"Workflow {workflow_id} not found")
            
            if not workflow.settings.get("schedule"):
                raise WorkflowTaskError(f"Workflow {workflow_id} has no schedule configuration")
            
            # Schedule execution
            execute_workflow_task.apply_async(
                kwargs={
                    "workflow_id": workflow_id,
                    "trigger_data": {
                        "type": ExecutionMode.SCHEDULE,
                        "schedule": workflow.settings.get("schedule")
                    },
                    "execution_mode": "schedule"
                }
            )
            
            return {
                "workflow_id": workflow_id,
                "scheduled": True,
                "schedule_config": workflow.settings.get("schedule")
            }
            
        finally:
            await db_session.close()


@celery_app.task(
    name="budflow.cancel_workflow",
    queue="workflows"
)
def cancel_workflow_task(execution_id: str) -> Dict[str, Any]:
    """Cancel a running workflow execution.
    
    Args:
        execution_id: ID of the execution to cancel
    
    Returns:
        Cancellation result
    """
    return asyncio.run(_cancel_workflow(execution_id))


async def _cancel_workflow(execution_id: str) -> Dict[str, Any]:
    """Async implementation of workflow cancellation."""
    async for db_session in get_db():
        try:
            execution_service = ExecutionService(db_session)
            
            # Get execution
            execution = await execution_service.get_execution(UUID(execution_id))
            if not execution:
                raise WorkflowTaskError(f"Execution {execution_id} not found")
            
            # Update status to cancelled
            execution.status = ExecutionStatus.CANCELED
            execution.finished_at = datetime.now(timezone.utc)
            await db_session.commit()
            
            # TODO: Send cancellation signal to worker if running
            
            return {
                "execution_id": str(execution.id),
                "status": execution.status.value,
                "cancelled": True
            }
            
        finally:
            await db_session.close()


# Beat schedule for periodic tasks
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    'cleanup-old-executions': {
        'task': 'budflow.cleanup_executions',
        'schedule': crontab(hour=2, minute=0),  # Run daily at 2 AM
        'kwargs': {'days': settings.execution_retention_days}
    },
}


# Task routing
celery_app.conf.task_routes = {
    'budflow.execute_workflow': {'queue': 'workflows'},
    'budflow.execute_node': {'queue': 'workflows'},
    'budflow.retry_failed_execution': {'queue': 'workflows'},
    'budflow.cancel_workflow': {'queue': 'workflows'},
    'budflow.schedule_workflow': {'queue': 'scheduler'},
    'budflow.cleanup_executions': {'queue': 'maintenance'},
}