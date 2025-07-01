"""Execution service for business logic."""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

import structlog
from sqlalchemy import String, and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from budflow.auth.models import User
from budflow.executor.engine import WorkflowExecutionEngine
from budflow.nodes.registry import NodeRegistry
from budflow.workflows.models import (
    Execution,
    ExecutionStatus,
    NodeExecution,
    SharedWorkflow,
    Workflow,
    WorkflowPermission,
    WorkflowStatus,
)
from budflow.executions.schemas import (
    ExecutionBulkDelete,
    ExecutionCreate,
    ExecutionMetrics,
    ExecutionResume,
)
from budflow.executions.exceptions import (
    ExecutionNotFoundError,
    ExecutionPermissionError,
    ExecutionStateError,
)

logger = structlog.get_logger()


class ExecutionService:
    """Service for managing workflow executions."""

    def __init__(self, db: AsyncSession, node_registry: NodeRegistry):
        """Initialize execution service."""
        self.db = db
        self.node_registry = node_registry
        self.engine = WorkflowExecutionEngine(node_registry)

    async def create_execution(
        self,
        execution_data: ExecutionCreate,
        user: Optional[User] = None,
        trigger_node_id: Optional[str] = None,
        queue_mode: bool = False,
        queue_manager: Optional[Any] = None,
    ) -> Execution:
        """Create and optionally execute a workflow execution."""
        # Check workflow permissions if user is provided
        if user:
            # Check permissions
            result = await self.db.execute(
                select(SharedWorkflow).where(
                    and_(
                        SharedWorkflow.workflow_id == execution_data.workflow_id,
                        SharedWorkflow.user_id == user.id,
                        SharedWorkflow.permission.in_([
                            WorkflowPermission.EXECUTE,
                            WorkflowPermission.UPDATE,
                            WorkflowPermission.OWNER,
                        ])
                    )
                )
            )
            if not result.scalar_one_or_none():
                raise ExecutionPermissionError(
                    f"No execute permission for workflow {execution_data.workflow_id}"
                )
        
        # Get workflow
        workflow = await self.db.get(Workflow, execution_data.workflow_id)
        if not workflow:
            raise ExecutionNotFoundError(f"Workflow {execution_data.workflow_id} not found")
        
        # Create execution record
        execution = Execution(
            workflow_id=workflow.id,
            user_id=user.id if user else None,
            mode=execution_data.mode,
            start_node=execution_data.start_node or trigger_node_id,
            status=ExecutionStatus.NEW,
            data=execution_data.data or {},
        )
        
        self.db.add(execution)
        await self.db.commit()
        await self.db.refresh(execution)
        
        # Check if we should use queue mode
        if queue_mode and queue_manager:
            # Import here to avoid circular dependency
            from budflow.queue import JobData, JobOptions
            
            # Create job data
            job_data = JobData(
                workflow_id=str(workflow.id),
                execution_id=str(execution.id),
                load_static_data=True,
            )
            
            # Set job options
            options = JobOptions(priority=1000)
            
            # Enqueue job
            try:
                job_id = await queue_manager.enqueue_job(job_data, options)
                
                # Update execution with job ID
                execution.job_id = job_id
                execution.status = ExecutionStatus.WAITING
                await self.db.commit()
                
                logger.info(
                    "Execution queued",
                    workflow_id=workflow.id,
                    execution_id=execution.id,
                    job_id=job_id,
                )
                
            except Exception as e:
                execution.status = ExecutionStatus.ERROR
                execution.error = {"message": f"Failed to queue job: {str(e)}", "type": type(e).__name__}
                await self.db.commit()
                raise
        else:
            # Execute workflow directly
            try:
                execution = await self.engine.execute(
                    workflow=workflow,
                    execution=execution,
                    input_data=execution_data.data,
                    start_node=execution_data.start_node or trigger_node_id,
                )
            except Exception as e:
                execution.status = ExecutionStatus.ERROR
                execution.error = {"message": str(e), "type": type(e).__name__}
                execution.finished_at = datetime.now(timezone.utc)
                if execution.started_at:
                    execution.execution_time_ms = int(
                        (execution.finished_at - execution.started_at).total_seconds() * 1000
                    )
            
            await self.db.commit()
            await self.db.refresh(execution)
            
            logger.info(
                "Execution completed",
                workflow_id=workflow.id,
                execution_id=execution.id,
                status=execution.status,
            )
        
        return execution

    async def list_executions(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        workflow_id: Optional[UUID] = None,
        status: Optional[ExecutionStatus] = None,
        mode: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """List executions accessible to user."""
        # Base query - join with workflows to check permissions
        query = select(Execution).join(
            Workflow, Execution.workflow_id == Workflow.id
        ).join(
            SharedWorkflow, SharedWorkflow.workflow_id == Workflow.id
        ).where(
            SharedWorkflow.user_id == user.id
        )
        
        # Apply filters
        if workflow_id:
            query = query.where(Execution.workflow_id == workflow_id)
        
        if status:
            query = query.where(Execution.status == status)
        
        if mode:
            query = query.where(Execution.mode == mode)
        
        if start_date:
            query = query.where(Execution.started_at >= start_date)
        
        if end_date:
            query = query.where(Execution.started_at <= end_date)
        
        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total = await self.db.scalar(count_query)
        
        # Apply pagination and ordering
        query = query.options(selectinload(Execution.workflow))
        query = query.order_by(Execution.created_at.desc())
        query = query.limit(limit).offset(offset)
        
        # Execute query
        result = await self.db.execute(query)
        executions = result.scalars().unique().all()
        
        return {
            "items": executions,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    async def get_execution(self, execution_id: UUID, user: User) -> Execution:
        """Get execution by ID."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # Load relationships
        await self.db.refresh(execution, ["workflow", "node_executions"])
        
        return execution

    async def cancel_execution(self, execution_id: UUID, user: User) -> Execution:
        """Cancel a running execution."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # Check if execution can be cancelled
        if execution.status not in (ExecutionStatus.NEW, ExecutionStatus.RUNNING, ExecutionStatus.WAITING):
            raise ExecutionStateError(
                f"Cannot cancel execution in {execution.status} state"
            )
        
        # Update status
        execution.status = ExecutionStatus.CANCELED
        execution.finished_at = datetime.now(timezone.utc)
        
        if execution.started_at:
            execution.execution_time_ms = int(
                (execution.finished_at - execution.started_at).total_seconds() * 1000
            )
        
        await self.db.commit()
        await self.db.refresh(execution)
        
        # TODO: Send cancellation signal to worker if running
        
        logger.info(
            "Execution cancelled",
            execution_id=execution_id,
            user_id=user.id,
        )
        
        return execution

    async def retry_execution(self, execution_id: UUID, user: User) -> Execution:
        """Retry a failed execution."""
        original_execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # Check if execution can be retried
        if original_execution.status not in (
            ExecutionStatus.ERROR,
            ExecutionStatus.CANCELED,
            ExecutionStatus.CRASHED,
        ):
            raise ExecutionStateError(
                f"Cannot retry execution in {original_execution.status} state"
            )
        
        # Create new execution as retry
        new_execution = Execution(
            workflow_id=original_execution.workflow_id,
            user_id=user.id,
            mode=original_execution.mode,
            status=ExecutionStatus.NEW,
            data=original_execution.data,
            retry_of=original_execution.id,
            retry_count=original_execution.retry_count + 1,
        )
        
        self.db.add(new_execution)
        await self.db.commit()
        await self.db.refresh(new_execution)
        
        # Execute workflow
        # In production, this would be sent to a queue
        workflow = await self.db.get(Workflow, original_execution.workflow_id)
        new_execution = await self.engine.execute(
            workflow=workflow,
            execution=new_execution,
            input_data=original_execution.data,
        )
        
        await self.db.commit()
        await self.db.refresh(new_execution)
        
        logger.info(
            "Execution retried",
            original_id=execution_id,
            new_id=new_execution.id,
            user_id=user.id,
        )
        
        return new_execution

    async def delete_execution(
        self, execution_id: UUID, user: User, force: bool = False
    ) -> None:
        """Delete an execution."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # Check if execution can be deleted
        if not force and execution.status == ExecutionStatus.RUNNING:
            raise ExecutionStateError("Cannot delete running execution")
        
        # Delete execution (cascade will handle related records)
        await self.db.delete(execution)
        await self.db.commit()
        
        logger.info(
            "Execution deleted",
            execution_id=execution_id,
            user_id=user.id,
        )

    async def bulk_delete_executions(
        self, user: User, execution_ids: List[UUID]
    ) -> Dict[str, int]:
        """Bulk delete executions."""
        # Get accessible execution IDs
        result = await self.db.execute(
            select(Execution.id)
            .join(Workflow)
            .join(SharedWorkflow)
            .where(
                and_(
                    Execution.id.in_(execution_ids),
                    SharedWorkflow.user_id == user.id,
                    Execution.status != ExecutionStatus.RUNNING,
                )
            )
        )
        
        deletable_ids = [row[0] for row in result]
        
        if deletable_ids:
            # Delete executions
            await self.db.execute(
                Execution.__table__.delete().where(
                    Execution.id.in_(deletable_ids)
                )
            )
            await self.db.commit()
        
        logger.info(
            "Bulk execution delete",
            requested=len(execution_ids),
            deleted=len(deletable_ids),
            user_id=user.id,
        )
        
        return {"deleted": len(deletable_ids)}

    async def get_execution_data(
        self, execution_id: UUID, user: User
    ) -> Dict[str, Any]:
        """Get execution input/output data."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # Get node outputs
        node_outputs = {}
        if execution.node_executions:
            for node_exec in execution.node_executions:
                if node_exec.output_data:
                    node_outputs[node_exec.node.uuid] = node_exec.output_data
        
        return {
            "input": execution.data,
            "output": node_outputs,
            "error": execution.error,
        }

    async def get_execution_logs(
        self, execution_id: UUID, user: User
    ) -> Dict[str, Any]:
        """Get execution logs."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # TODO: Implement actual log retrieval
        # This would fetch from a logging service or database
        
        logs = []
        
        # Add execution start log
        if execution.started_at:
            logs.append({
                "timestamp": execution.started_at.isoformat(),
                "level": "info",
                "message": f"Execution started in {execution.mode} mode",
            })
        
        # Add node execution logs
        if execution.node_executions:
            for node_exec in execution.node_executions:
                if node_exec.started_at:
                    logs.append({
                        "timestamp": node_exec.started_at.isoformat(),
                        "level": "info",
                        "message": f"Node '{node_exec.node.name}' started",
                        "nodeId": node_exec.node.uuid,
                    })
                
                if node_exec.error:
                    logs.append({
                        "timestamp": node_exec.finished_at.isoformat() if node_exec.finished_at else None,
                        "level": "error",
                        "message": node_exec.error.get("message", "Unknown error"),
                        "nodeId": node_exec.node.uuid,
                    })
                elif node_exec.finished_at:
                    logs.append({
                        "timestamp": node_exec.finished_at.isoformat(),
                        "level": "info",
                        "message": f"Node '{node_exec.node.name}' completed",
                        "nodeId": node_exec.node.uuid,
                    })
        
        # Add execution end log
        if execution.finished_at:
            logs.append({
                "timestamp": execution.finished_at.isoformat(),
                "level": "info" if execution.status == ExecutionStatus.SUCCESS else "error",
                "message": f"Execution {execution.status.value}",
            })
        
        return {"logs": sorted(logs, key=lambda x: x["timestamp"] or "")}

    async def get_execution_node_outputs(
        self, execution_id: UUID, user: User
    ) -> Dict[str, Any]:
        """Get individual node outputs from execution."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        nodes = []
        if execution.node_executions:
            for node_exec in execution.node_executions:
                nodes.append({
                    "nodeId": node_exec.node.uuid,
                    "nodeName": node_exec.node.name,
                    "status": node_exec.status.value,
                    "inputData": node_exec.input_data,
                    "outputData": node_exec.output_data,
                    "error": node_exec.error,
                    "startedAt": node_exec.started_at.isoformat() if node_exec.started_at else None,
                    "finishedAt": node_exec.finished_at.isoformat() if node_exec.finished_at else None,
                    "executionTime": node_exec.execution_time_ms,
                })
        
        return {"nodes": nodes}

    async def search_executions(
        self,
        user: User,
        query: str,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Search executions by content."""
        # Search in execution data and error messages
        search_term = f"%{query}%"
        
        # Use JSON operators for PostgreSQL
        # This is a simplified search - in production, use full-text search
        search_query = select(Execution).join(
            Workflow
        ).join(
            SharedWorkflow
        ).where(
            and_(
                SharedWorkflow.user_id == user.id,
                or_(
                    Execution.data.cast(String).ilike(search_term),
                    Execution.error.cast(String).ilike(search_term),
                )
            )
        )
        
        # Get total count
        count_query = select(func.count()).select_from(search_query.subquery())
        total = await self.db.scalar(count_query)
        
        # Apply pagination
        search_query = search_query.limit(limit).offset(offset)
        
        result = await self.db.execute(search_query)
        executions = result.scalars().unique().all()
        
        return {
            "items": executions,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    async def resume_execution(
        self,
        execution_id: UUID,
        user: User,
        resume_data: ExecutionResume,
    ) -> Execution:
        """Resume a waiting execution."""
        execution = await self._get_execution_with_permission_check(
            execution_id, user
        )
        
        # Check if execution is waiting
        if execution.status != ExecutionStatus.WAITING:
            raise ExecutionStateError(
                f"Cannot resume execution in {execution.status} state"
            )
        
        # Update execution data with resume data
        if resume_data.data:
            execution.data = {**execution.data, **resume_data.data}
        
        execution.status = ExecutionStatus.RUNNING
        
        await self.db.commit()
        
        # Resume execution
        # In production, this would signal the waiting execution
        workflow = await self.db.get(Workflow, execution.workflow_id)
        execution = await self.engine.resume(
            workflow=workflow,
            execution=execution,
            resume_data=resume_data.data,
        )
        
        await self.db.commit()
        await self.db.refresh(execution)
        
        logger.info(
            "Execution resumed",
            execution_id=execution_id,
            user_id=user.id,
        )
        
        return execution

    async def get_execution_metrics(
        self,
        user: User,
        start_date: datetime,
        end_date: datetime,
        workflow_id: Optional[UUID] = None,
    ) -> ExecutionMetrics:
        """Get execution metrics for date range."""
        # Base query with permission check
        base_query = select(Execution).join(
            Workflow
        ).join(
            SharedWorkflow
        ).where(
            and_(
                SharedWorkflow.user_id == user.id,
                Execution.started_at >= start_date,
                Execution.started_at <= end_date,
            )
        )
        
        if workflow_id:
            base_query = base_query.where(Execution.workflow_id == workflow_id)
        
        # Get metrics
        metrics_result = await self.db.execute(
            select(
                func.count(Execution.id).label("total"),
                func.count(
                    Execution.id
                ).filter(
                    Execution.status == ExecutionStatus.SUCCESS
                ).label("successful"),
                func.count(
                    Execution.id
                ).filter(
                    Execution.status == ExecutionStatus.ERROR
                ).label("failed"),
                func.avg(Execution.execution_time_ms).label("avg_time"),
                func.min(Execution.execution_time_ms).label("min_time"),
                func.max(Execution.execution_time_ms).label("max_time"),
            ).select_from(base_query.subquery())
        )
        
        metrics = metrics_result.one()
        
        # Calculate success rate
        success_rate = 0.0
        if metrics.total > 0:
            success_rate = (metrics.successful / metrics.total) * 100
        
        return ExecutionMetrics(
            totalExecutions=metrics.total,
            successfulExecutions=metrics.successful,
            failedExecutions=metrics.failed,
            successRate=success_rate,
            averageExecutionTime=float(metrics.avg_time) if metrics.avg_time else 0,
            minExecutionTime=metrics.min_time or 0,
            maxExecutionTime=metrics.max_time or 0,
            startDate=start_date,
            endDate=end_date,
        )

    async def get_workflow_executions(
        self,
        workflow_id: UUID,
        user: User,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get executions for a specific workflow."""
        # Check workflow access
        workflow_access = await self.db.execute(
            select(SharedWorkflow).where(
                and_(
                    SharedWorkflow.workflow_id == workflow_id,
                    SharedWorkflow.user_id == user.id,
                )
            )
        )
        
        if not workflow_access.scalar_one_or_none():
            raise ExecutionPermissionError(
                f"No access to workflow {workflow_id}"
            )
        
        # Get executions
        query = select(Execution).where(
            Execution.workflow_id == workflow_id
        ).order_by(Execution.created_at.desc())
        
        # Get total count
        total = await self.db.scalar(
            select(func.count()).select_from(
                select(Execution).where(
                    Execution.workflow_id == workflow_id
                ).subquery()
            )
        )
        
        # Apply pagination
        query = query.limit(limit).offset(offset)
        
        result = await self.db.execute(query)
        executions = result.scalars().all()
        
        return {
            "items": executions,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    # Private helper methods

    async def _get_execution_with_permission_check(
        self,
        execution_id: UUID,
        user: User,
    ) -> Execution:
        """Get execution and check user permission."""
        result = await self.db.execute(
            select(Execution, SharedWorkflow.permission)
            .join(Workflow, Execution.workflow_id == Workflow.id)
            .join(SharedWorkflow, SharedWorkflow.workflow_id == Workflow.id)
            .where(
                and_(
                    Execution.id == execution_id,
                    SharedWorkflow.user_id == user.id,
                )
            )
        )
        
        row = result.one_or_none()
        if not row:
            raise ExecutionNotFoundError(f"Execution {execution_id} not found")
        
        execution, permission = row
        
        # For executions, read permission is sufficient
        if permission not in (
            WorkflowPermission.READ,
            WorkflowPermission.EXECUTE,
            WorkflowPermission.UPDATE,
            WorkflowPermission.DELETE,
            WorkflowPermission.SHARE,
            WorkflowPermission.OWNER,
        ):
            raise ExecutionPermissionError(
                f"Insufficient permissions for execution {execution_id}"
            )
        
        return execution