"""Queue recovery service implementation."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from .config import QueueConfig
from .manager import QueueManager
from .models import JobData
from .exceptions import QueueRecoveryError
# Import at function level to avoid circular imports


logger = logging.getLogger(__name__)


class QueueRecoveryService:
    """Service for recovering crashed executions."""
    
    def __init__(
        self,
        queue_config: QueueConfig,
        db_session_factory: Callable[[], AsyncSession],
        is_leader_func: Callable[[], bool]
    ):
        """
        Initialize recovery service.
        
        Args:
            queue_config: Queue configuration
            db_session_factory: Factory function to create DB sessions
            is_leader_func: Function to check if current instance is leader
        """
        self.config = queue_config
        self.db_session_factory = db_session_factory
        self.is_leader_func = is_leader_func
        self.queue_manager: Optional[QueueManager] = None
        self._recovery_task: Optional[asyncio.Task] = None
        self._last_full_batch = False
        self._current_interval = self.config.recovery_interval
        self.is_running = False
    
    async def start(self) -> None:
        """Start recovery service."""
        if not self.config.recovery_enabled:
            logger.info("Queue recovery is disabled")
            return
        
        try:
            # Initialize queue manager
            self.queue_manager = QueueManager(self.config)
            await self.queue_manager.initialize()
            
            # Start recovery task
            self._recovery_task = asyncio.create_task(self._recovery_loop())
            self.is_running = True
            
            logger.info("Queue recovery service started")
            
        except Exception as e:
            raise QueueRecoveryError(f"Failed to start recovery service: {e}")
    
    async def stop(self) -> None:
        """Stop recovery service."""
        self.is_running = False
        
        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
        
        if self.queue_manager:
            await self.queue_manager.close()
        
        logger.info("Queue recovery service stopped")
    
    async def _recovery_loop(self) -> None:
        """Main recovery loop."""
        while self.is_running:
            try:
                # Only run on leader
                if not self.is_leader_func():
                    await asyncio.sleep(self._current_interval)
                    continue
                
                # Perform recovery
                recovered_count = await self._perform_recovery()
                
                # Adjust interval based on results
                self._adjust_interval(recovered_count)
                
                # Wait for next iteration
                await asyncio.sleep(self._current_interval)
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in recovery loop: {e}", exc_info=True)
                await asyncio.sleep(self._current_interval)
    
    async def _perform_recovery(self) -> int:
        """
        Perform recovery of crashed executions.
        
        Returns:
            Number of executions recovered
        """
        logger.debug("Starting recovery check...")
        recovered = 0
        
        async with self.db_session_factory() as session:
            try:
                # Import locally to avoid circular import
                from ..workflows.models import WorkflowExecution, ExecutionStatus
                
                # Find running executions
                stmt = select(WorkflowExecution).where(
                    WorkflowExecution.status == ExecutionStatus.RUNNING
                ).limit(self.config.recovery_batch_size)
                
                result = await session.execute(stmt)
                executions = result.scalars().all()
                
                if not executions:
                    logger.debug("No running executions found")
                    return 0
                
                logger.info(f"Checking {len(executions)} running executions")
                
                for execution in executions:
                    try:
                        # Check if job exists in queue
                        job_exists = await self._check_job_exists(execution)
                        
                        if not job_exists:
                            # Mark as crashed
                            await self._mark_as_crashed(session, execution)
                            recovered += 1
                            
                    except Exception as e:
                        logger.error(
                            f"Error checking execution {execution.id}: {e}"
                        )
                
                if recovered > 0:
                    await session.commit()
                    logger.info(f"Recovered {recovered} crashed executions")
                
                # Check if we processed a full batch
                self._last_full_batch = len(executions) == self.config.recovery_batch_size
                
                return recovered
                
            except Exception as e:
                await session.rollback()
                raise QueueRecoveryError(f"Recovery failed: {e}")
    
    async def _check_job_exists(self, execution: "WorkflowExecution") -> bool:
        """Check if a job exists for the execution."""
        # In n8n, this checks if the job exists in Bull queue
        # We'll check if there's an active job for this execution
        
        # First check if execution has a job_id stored
        if not hasattr(execution, 'job_id') or not execution.job_id:
            # No job ID means it was never queued
            return False
        
        # Check if job exists in queue
        return await self.queue_manager.job_exists(execution.job_id)
    
    async def _mark_as_crashed(
        self,
        session: AsyncSession,
        execution: "WorkflowExecution"
    ) -> None:
        """Mark an execution as crashed."""
        logger.warning(
            f"Marking execution {execution.id} (workflow {execution.workflow_id}) "
            f"as crashed"
        )
        
        # Import locally to avoid circular import
        from ..workflows.models import ExecutionStatus
        
        # Update execution status
        execution.status = ExecutionStatus.CRASHED
        execution.finished_at = datetime.now(timezone.utc)
        
        if not execution.data:
            execution.data = {}
        
        execution.data["resultData"] = {
            "error": {
                "message": "Execution was recovered and set to crashed status",
                "name": "RecoveryError",
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "runData": execution.data.get("runData", {}),
            "lastNodeExecuted": execution.data.get("lastNodeExecuted")
        }
        
        # Save changes (commit will be done by caller)
        session.add(execution)
    
    def _adjust_interval(self, recovered_count: int) -> None:
        """Adjust recovery interval based on results."""
        if self._last_full_batch and recovered_count > 0:
            # Speed up if we're finding crashed executions
            self._current_interval = max(
                10,  # Minimum 10 seconds
                self._current_interval // 2
            )
            logger.debug(f"Speeding up recovery interval to {self._current_interval}s")
        else:
            # Slow down back to normal
            self._current_interval = min(
                self.config.recovery_interval,
                self._current_interval * 1.5
            )
    
    async def recover_execution(self, execution_id: str) -> bool:
        """
        Manually recover a specific execution.
        
        Args:
            execution_id: ID of execution to recover
            
        Returns:
            True if execution was recovered
        """
        async with self.db_session_factory() as session:
            try:
                # Import locally to avoid circular import
                from ..workflows.models import WorkflowExecution, ExecutionStatus
                
                # Get execution
                execution = await session.get(WorkflowExecution, execution_id)
                
                if not execution:
                    logger.error(f"Execution {execution_id} not found")
                    return False
                
                if execution.status != ExecutionStatus.RUNNING:
                    logger.info(
                        f"Execution {execution_id} is not running "
                        f"(status: {execution.status})"
                    )
                    return False
                
                # Check if job exists
                job_exists = await self._check_job_exists(execution)
                
                if job_exists:
                    logger.info(f"Execution {execution_id} has active job")
                    return False
                
                # Mark as crashed
                await self._mark_as_crashed(session, execution)
                await session.commit()
                
                logger.info(f"Successfully recovered execution {execution_id}")
                return True
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Failed to recover execution {execution_id}: {e}")
                return False
    
    async def get_recovery_stats(self) -> dict:
        """Get recovery statistics."""
        async with self.db_session_factory() as session:
            try:
                # Import locally to avoid circular import
                from ..workflows.models import WorkflowExecution, ExecutionStatus
                
                # Count crashed executions in last 24 hours
                since = datetime.now(timezone.utc) - timedelta(days=1)
                
                stmt = select(WorkflowExecution).where(
                    and_(
                        WorkflowExecution.status == ExecutionStatus.CRASHED,
                        WorkflowExecution.finished_at >= since
                    )
                )
                
                result = await session.execute(stmt)
                crashed_count = len(result.scalars().all())
                
                # Count currently running
                stmt = select(WorkflowExecution).where(
                    WorkflowExecution.status == ExecutionStatus.RUNNING
                )
                
                result = await session.execute(stmt)
                running_count = len(result.scalars().all())
                
                return {
                    "crashed_last_24h": crashed_count,
                    "currently_running": running_count,
                    "recovery_interval": self._current_interval,
                    "is_leader": self.is_leader_func(),
                    "is_running": self.is_running
                }
                
            except Exception as e:
                logger.error(f"Failed to get recovery stats: {e}")
                return {}