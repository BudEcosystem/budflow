"""Execution recovery service for BudFlow.

This module provides robust execution recovery capabilities including:
- Failed execution detection and recovery
- Crashed worker detection
- Execution state restoration
- Automatic retry mechanisms
- Recovery policies and strategies
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models.executions import Execution, ExecutionStatus
from ..models.workflows import Workflow
from ..core.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class RecoveryStrategy(str, Enum):
    """Recovery strategies for failed executions."""
    
    RESTART = "restart"
    RESUME = "resume"
    ABORT = "abort"
    MANUAL = "manual"


class RecoveryPolicy(BaseModel):
    """Recovery policy configuration."""
    
    max_retries: int = 3
    retry_delay_seconds: int = 60
    strategy: RecoveryStrategy = RecoveryStrategy.RESTART
    exponential_backoff: bool = True
    max_retry_delay: int = 3600  # 1 hour
    recovery_timeout: int = 86400  # 24 hours
    notify_on_failure: bool = True
    
    model_config = ConfigDict(from_attributes=True)


class ExecutionRecoveryInfo(BaseModel):
    """Information about execution recovery."""
    
    execution_id: UUID
    workflow_id: UUID
    recovery_attempts: int = 0
    last_recovery_at: Optional[datetime] = None
    recovery_strategy: RecoveryStrategy
    recovery_reason: str
    worker_id: Optional[str] = None
    node_position: Optional[str] = None
    context_snapshot: Dict[str, Any] = Field(default_factory=dict)
    error_details: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)


class WorkerHealthStatus(BaseModel):
    """Worker health status information."""
    
    worker_id: str
    last_heartbeat: datetime
    is_healthy: bool
    current_executions: List[UUID] = Field(default_factory=list)
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    active_since: datetime
    
    model_config = ConfigDict(from_attributes=True)


class ExecutionRecoveryService:
    """Service for recovering failed executions and handling worker crashes."""
    
    def __init__(self, redis_client=None, db: Optional[AsyncSession] = None):
        self.redis = redis_client or get_redis_client()
        self.db = db
        self.recovery_policies: Dict[str, RecoveryPolicy] = {}
        self.is_running = False
        self.recovery_tasks: Set[asyncio.Task] = set()
        
        # Redis keys
        self.worker_heartbeat_key = "budflow:worker_heartbeats"
        self.execution_recovery_key = "budflow:execution_recovery"
        self.failed_executions_key = "budflow:failed_executions"
        
        # Default recovery policy
        self.default_policy = RecoveryPolicy()
        
    def set_recovery_policy(self, workflow_id: str, policy: RecoveryPolicy) -> None:
        """Set recovery policy for a specific workflow."""
        self.recovery_policies[workflow_id] = policy
        
    def get_recovery_policy(self, workflow_id: str) -> RecoveryPolicy:
        """Get recovery policy for a workflow."""
        return self.recovery_policies.get(workflow_id, self.default_policy)
    
    async def start(self) -> None:
        """Start the recovery service."""
        if self.is_running:
            return
            
        self.is_running = True
        logger.info("Starting Execution Recovery Service")
        
        # Start background recovery tasks
        recovery_task = asyncio.create_task(self._recovery_loop())
        self.recovery_tasks.add(recovery_task)
        
        health_check_task = asyncio.create_task(self._worker_health_check_loop())
        self.recovery_tasks.add(health_check_task)
        
        cleanup_task = asyncio.create_task(self._cleanup_loop())
        self.recovery_tasks.add(cleanup_task)
        
    async def stop(self) -> None:
        """Stop the recovery service."""
        logger.info("Stopping Execution Recovery Service")
        self.is_running = False
        
        # Cancel all recovery tasks
        for task in self.recovery_tasks:
            if not task.done():
                task.cancel()
        
        if self.recovery_tasks:
            await asyncio.gather(*self.recovery_tasks, return_exceptions=True)
        
        self.recovery_tasks.clear()
        
    async def register_worker_heartbeat(
        self, 
        worker_id: str, 
        executions: List[UUID],
        metrics: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register worker heartbeat."""
        try:
            heartbeat_data = {
                "worker_id": worker_id,
                "timestamp": time.time(),
                "executions": [str(exec_id) for exec_id in executions],
                "metrics": metrics or {},
            }
            
            # Store in Redis with TTL
            await self.redis.setex(
                f"{self.worker_heartbeat_key}:{worker_id}",
                120,  # 2 minute TTL
                json.dumps(heartbeat_data)
            )
            
        except Exception as e:
            logger.error(f"Failed to register worker heartbeat: {e}")
    
    async def detect_failed_executions(self) -> List[ExecutionRecoveryInfo]:
        """Detect executions that need recovery."""
        failed_executions = []
        
        try:
            if not self.db:
                async with get_session() as session:
                    return await self._detect_failed_executions_impl(session)
            else:
                return await self._detect_failed_executions_impl(self.db)
                
        except Exception as e:
            logger.error(f"Failed to detect failed executions: {e}")
            return []
    
    async def _detect_failed_executions_impl(self, session: AsyncSession) -> List[ExecutionRecoveryInfo]:
        """Implementation of failed execution detection."""
        failed_executions = []
        
        # Find stuck executions (running for too long)
        stuck_threshold = datetime.now(timezone.utc) - timedelta(hours=2)
        
        result = await session.execute(
            select(Execution, Workflow).
            join(Workflow, Execution.workflow_id == Workflow.id).
            where(
                and_(
                    Execution.status == ExecutionStatus.RUNNING,
                    Execution.started_at < stuck_threshold
                )
            )
        )
        
        for execution, workflow in result:
            recovery_info = ExecutionRecoveryInfo(
                execution_id=execution.id,
                workflow_id=execution.workflow_id,
                recovery_strategy=RecoveryStrategy.RESTART,
                recovery_reason="Execution stuck - running too long",
                context_snapshot=execution.context or {},
            )
            failed_executions.append(recovery_info)
        
        # Find executions with crashed workers
        crashed_workers = await self._detect_crashed_workers()
        
        for worker_id in crashed_workers:
            worker_executions = await self._get_worker_executions(worker_id)
            
            for exec_id in worker_executions:
                # Get execution details
                result = await session.execute(
                    select(Execution).where(Execution.id == exec_id)
                )
                execution = result.scalar_one_or_none()
                
                if execution and execution.status == ExecutionStatus.RUNNING:
                    recovery_info = ExecutionRecoveryInfo(
                        execution_id=execution.id,
                        workflow_id=execution.workflow_id,
                        recovery_strategy=RecoveryStrategy.RESUME,
                        recovery_reason=f"Worker {worker_id} crashed",
                        worker_id=worker_id,
                        context_snapshot=execution.context or {},
                    )
                    failed_executions.append(recovery_info)
        
        return failed_executions
    
    async def _detect_crashed_workers(self) -> List[str]:
        """Detect workers that have crashed (no heartbeat)."""
        crashed_workers = []
        
        try:
            # Get all worker heartbeat keys
            pattern = f"{self.worker_heartbeat_key}:*"
            keys = await self.redis.keys(pattern)
            
            current_time = time.time()
            
            for key in keys:
                worker_id = key.decode().split(":")[-1]
                
                # Check if heartbeat exists (TTL will auto-expire stale ones)
                heartbeat_data = await self.redis.get(key)
                
                if not heartbeat_data:
                    crashed_workers.append(worker_id)
                else:
                    try:
                        data = json.loads(heartbeat_data)
                        last_heartbeat = data.get("timestamp", 0)
                        
                        # Consider crashed if no heartbeat for 3 minutes
                        if current_time - last_heartbeat > 180:
                            crashed_workers.append(worker_id)
                    except json.JSONDecodeError:
                        crashed_workers.append(worker_id)
            
        except Exception as e:
            logger.error(f"Failed to detect crashed workers: {e}")
        
        return crashed_workers
    
    async def _get_worker_executions(self, worker_id: str) -> List[UUID]:
        """Get executions assigned to a worker."""
        try:
            heartbeat_key = f"{self.worker_heartbeat_key}:{worker_id}"
            heartbeat_data = await self.redis.get(heartbeat_key)
            
            if heartbeat_data:
                data = json.loads(heartbeat_data)
                execution_ids = data.get("executions", [])
                return [UUID(exec_id) for exec_id in execution_ids]
        
        except Exception as e:
            logger.error(f"Failed to get worker executions: {e}")
        
        return []
    
    async def recover_execution(self, recovery_info: ExecutionRecoveryInfo) -> bool:
        """Recover a failed execution."""
        try:
            policy = self.get_recovery_policy(str(recovery_info.workflow_id))
            
            # Check if we've exceeded max retries
            if recovery_info.recovery_attempts >= policy.max_retries:
                logger.warning(
                    f"Execution {recovery_info.execution_id} exceeded max retries, "
                    f"marking as failed"
                )
                await self._mark_execution_failed(recovery_info)
                return False
            
            # Apply recovery strategy
            if policy.strategy == RecoveryStrategy.RESTART:
                return await self._restart_execution(recovery_info, policy)
            elif policy.strategy == RecoveryStrategy.RESUME:
                return await self._resume_execution(recovery_info, policy)
            elif policy.strategy == RecoveryStrategy.ABORT:
                return await self._abort_execution(recovery_info)
            else:  # MANUAL
                await self._queue_for_manual_recovery(recovery_info)
                return True
                
        except Exception as e:
            logger.error(f"Failed to recover execution {recovery_info.execution_id}: {e}")
            return False
    
    async def _restart_execution(
        self, 
        recovery_info: ExecutionRecoveryInfo, 
        policy: RecoveryPolicy
    ) -> bool:
        """Restart execution from the beginning."""
        try:
            if not self.db:
                async with get_session() as session:
                    return await self._restart_execution_impl(session, recovery_info, policy)
            else:
                return await self._restart_execution_impl(self.db, recovery_info, policy)
                
        except Exception as e:
            logger.error(f"Failed to restart execution: {e}")
            return False
    
    async def _restart_execution_impl(
        self,
        session: AsyncSession,
        recovery_info: ExecutionRecoveryInfo,
        policy: RecoveryPolicy
    ) -> bool:
        """Implementation of execution restart."""
        # Reset execution state
        await session.execute(
            update(Execution).
            where(Execution.id == recovery_info.execution_id).
            values(
                status=ExecutionStatus.PENDING,
                started_at=None,
                finished_at=None,
                error=None,
                context={},  # Reset context
                current_node_id=None,
            )
        )
        
        # Update recovery info
        recovery_info.recovery_attempts += 1
        recovery_info.last_recovery_at = datetime.now(timezone.utc)
        
        # Store recovery info
        await self._store_recovery_info(recovery_info)
        
        # Schedule execution restart with delay
        delay = self._calculate_retry_delay(recovery_info.recovery_attempts, policy)
        await self._schedule_execution_restart(recovery_info.execution_id, delay)
        
        await session.commit()
        
        logger.info(
            f"Restarted execution {recovery_info.execution_id} "
            f"(attempt {recovery_info.recovery_attempts})"
        )
        
        return True
    
    async def _resume_execution(
        self, 
        recovery_info: ExecutionRecoveryInfo, 
        policy: RecoveryPolicy
    ) -> bool:
        """Resume execution from last known state."""
        try:
            if not self.db:
                async with get_session() as session:
                    return await self._resume_execution_impl(session, recovery_info, policy)
            else:
                return await self._resume_execution_impl(self.db, recovery_info, policy)
                
        except Exception as e:
            logger.error(f"Failed to resume execution: {e}")
            return False
    
    async def _resume_execution_impl(
        self,
        session: AsyncSession,
        recovery_info: ExecutionRecoveryInfo,
        policy: RecoveryPolicy
    ) -> bool:
        """Implementation of execution resume."""
        # Reset execution to pending but keep context
        await session.execute(
            update(Execution).
            where(Execution.id == recovery_info.execution_id).
            values(
                status=ExecutionStatus.PENDING,
                error=None,
            )
        )
        
        # Update recovery info
        recovery_info.recovery_attempts += 1
        recovery_info.last_recovery_at = datetime.now(timezone.utc)
        
        # Store recovery info
        await self._store_recovery_info(recovery_info)
        
        # Schedule execution resume with delay
        delay = self._calculate_retry_delay(recovery_info.recovery_attempts, policy)
        await self._schedule_execution_resume(recovery_info.execution_id, delay)
        
        await session.commit()
        
        logger.info(
            f"Resumed execution {recovery_info.execution_id} "
            f"(attempt {recovery_info.recovery_attempts})"
        )
        
        return True
    
    async def _abort_execution(self, recovery_info: ExecutionRecoveryInfo) -> bool:
        """Abort failed execution."""
        try:
            if not self.db:
                async with get_session() as session:
                    return await self._abort_execution_impl(session, recovery_info)
            else:
                return await self._abort_execution_impl(self.db, recovery_info)
                
        except Exception as e:
            logger.error(f"Failed to abort execution: {e}")
            return False
    
    async def _abort_execution_impl(
        self,
        session: AsyncSession,
        recovery_info: ExecutionRecoveryInfo
    ) -> bool:
        """Implementation of execution abort."""
        await session.execute(
            update(Execution).
            where(Execution.id == recovery_info.execution_id).
            values(
                status=ExecutionStatus.FAILED,
                finished_at=datetime.now(timezone.utc),
                error=f"Aborted due to recovery policy: {recovery_info.recovery_reason}",
            )
        )
        
        await session.commit()
        
        logger.info(f"Aborted execution {recovery_info.execution_id}")
        return True
    
    async def _mark_execution_failed(self, recovery_info: ExecutionRecoveryInfo) -> None:
        """Mark execution as failed after max retries."""
        try:
            if not self.db:
                async with get_session() as session:
                    await self._mark_execution_failed_impl(session, recovery_info)
            else:
                await self._mark_execution_failed_impl(self.db, recovery_info)
                
        except Exception as e:
            logger.error(f"Failed to mark execution as failed: {e}")
    
    async def _mark_execution_failed_impl(
        self,
        session: AsyncSession,
        recovery_info: ExecutionRecoveryInfo
    ) -> None:
        """Implementation of marking execution as failed."""
        await session.execute(
            update(Execution).
            where(Execution.id == recovery_info.execution_id).
            values(
                status=ExecutionStatus.FAILED,
                finished_at=datetime.now(timezone.utc),
                error=f"Max retries exceeded: {recovery_info.recovery_reason}",
            )
        )
        
        await session.commit()
    
    def _calculate_retry_delay(self, attempt: int, policy: RecoveryPolicy) -> int:
        """Calculate retry delay with optional exponential backoff."""
        base_delay = policy.retry_delay_seconds
        
        if policy.exponential_backoff:
            delay = base_delay * (2 ** (attempt - 1))
            return min(delay, policy.max_retry_delay)
        
        return base_delay
    
    async def _store_recovery_info(self, recovery_info: ExecutionRecoveryInfo) -> None:
        """Store recovery information in Redis."""
        try:
            key = f"{self.execution_recovery_key}:{recovery_info.execution_id}"
            data = recovery_info.model_dump_json()
            
            # Store with 7 day TTL
            await self.redis.setex(key, 604800, data)
            
        except Exception as e:
            logger.error(f"Failed to store recovery info: {e}")
    
    async def _queue_for_manual_recovery(self, recovery_info: ExecutionRecoveryInfo) -> None:
        """Queue execution for manual recovery."""
        try:
            # Add to manual recovery queue
            await self.redis.lpush(
                "budflow:manual_recovery",
                recovery_info.model_dump_json()
            )
            
            logger.info(f"Queued execution {recovery_info.execution_id} for manual recovery")
            
        except Exception as e:
            logger.error(f"Failed to queue for manual recovery: {e}")
    
    async def _schedule_execution_restart(self, execution_id: UUID, delay: int) -> None:
        """Schedule execution restart after delay."""
        try:
            # Use Redis delayed queue or Celery for scheduling
            schedule_time = time.time() + delay
            
            schedule_data = {
                "execution_id": str(execution_id),
                "action": "restart",
                "scheduled_at": schedule_time,
            }
            
            # Add to delayed execution queue
            await self.redis.zadd(
                "budflow:delayed_executions",
                {json.dumps(schedule_data): schedule_time}
            )
            
        except Exception as e:
            logger.error(f"Failed to schedule execution restart: {e}")
    
    async def _schedule_execution_resume(self, execution_id: UUID, delay: int) -> None:
        """Schedule execution resume after delay."""
        try:
            schedule_time = time.time() + delay
            
            schedule_data = {
                "execution_id": str(execution_id),
                "action": "resume",
                "scheduled_at": schedule_time,
            }
            
            # Add to delayed execution queue
            await self.redis.zadd(
                "budflow:delayed_executions",
                {json.dumps(schedule_data): schedule_time}
            )
            
        except Exception as e:
            logger.error(f"Failed to schedule execution resume: {e}")
    
    async def _recovery_loop(self) -> None:
        """Main recovery loop."""
        try:
            while self.is_running:
                try:
                    # Detect failed executions
                    failed_executions = await self.detect_failed_executions()
                    
                    # Process each failed execution
                    for recovery_info in failed_executions:
                        await self.recover_execution(recovery_info)
                    
                    # Process delayed executions
                    await self._process_delayed_executions()
                    
                    # Sleep before next check
                    await asyncio.sleep(30)  # Check every 30 seconds
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in recovery loop: {e}")
                    await asyncio.sleep(10)
                    
        except asyncio.CancelledError:
            pass
    
    async def _worker_health_check_loop(self) -> None:
        """Worker health check loop."""
        try:
            while self.is_running:
                try:
                    # Check worker health and cleanup stale workers
                    await self._cleanup_stale_workers()
                    
                    # Sleep before next check
                    await asyncio.sleep(60)  # Check every minute
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in worker health check: {e}")
                    await asyncio.sleep(30)
                    
        except asyncio.CancelledError:
            pass
    
    async def _cleanup_loop(self) -> None:
        """Cleanup loop for old recovery data."""
        try:
            while self.is_running:
                try:
                    # Cleanup old recovery data
                    await self._cleanup_old_recovery_data()
                    
                    # Sleep before next cleanup
                    await asyncio.sleep(3600)  # Cleanup every hour
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in cleanup loop: {e}")
                    await asyncio.sleep(300)
                    
        except asyncio.CancelledError:
            pass
    
    async def _process_delayed_executions(self) -> None:
        """Process delayed executions that are ready."""
        try:
            current_time = time.time()
            
            # Get ready executions
            ready_executions = await self.redis.zrangebyscore(
                "budflow:delayed_executions",
                0,
                current_time,
                withscores=True
            )
            
            for execution_data, score in ready_executions:
                try:
                    data = json.loads(execution_data)
                    execution_id = UUID(data["execution_id"])
                    action = data["action"]
                    
                    # Process the execution
                    if action == "restart":
                        await self._trigger_execution_restart(execution_id)
                    elif action == "resume":
                        await self._trigger_execution_resume(execution_id)
                    
                    # Remove from delayed queue
                    await self.redis.zrem("budflow:delayed_executions", execution_data)
                    
                except Exception as e:
                    logger.error(f"Failed to process delayed execution: {e}")
                    # Remove invalid entry
                    await self.redis.zrem("budflow:delayed_executions", execution_data)
                    
        except Exception as e:
            logger.error(f"Failed to process delayed executions: {e}")
    
    async def _trigger_execution_restart(self, execution_id: UUID) -> None:
        """Trigger execution restart."""
        try:
            # Add to execution queue for processing
            await self.redis.lpush(
                "budflow:execution_queue",
                json.dumps({"execution_id": str(execution_id), "action": "restart"})
            )
            
            logger.info(f"Triggered restart for execution {execution_id}")
            
        except Exception as e:
            logger.error(f"Failed to trigger execution restart: {e}")
    
    async def _trigger_execution_resume(self, execution_id: UUID) -> None:
        """Trigger execution resume."""
        try:
            # Add to execution queue for processing
            await self.redis.lpush(
                "budflow:execution_queue",
                json.dumps({"execution_id": str(execution_id), "action": "resume"})
            )
            
            logger.info(f"Triggered resume for execution {execution_id}")
            
        except Exception as e:
            logger.error(f"Failed to trigger execution resume: {e}")
    
    async def _cleanup_stale_workers(self) -> None:
        """Cleanup stale worker heartbeats."""
        try:
            # Redis TTL will handle most cleanup, but we can do additional cleanup here
            crashed_workers = await self._detect_crashed_workers()
            
            for worker_id in crashed_workers:
                # Remove worker heartbeat
                await self.redis.delete(f"{self.worker_heartbeat_key}:{worker_id}")
                
                logger.info(f"Cleaned up stale worker: {worker_id}")
                
        except Exception as e:
            logger.error(f"Failed to cleanup stale workers: {e}")
    
    async def _cleanup_old_recovery_data(self) -> None:
        """Cleanup old recovery data."""
        try:
            # Cleanup old recovery info (older than 7 days)
            pattern = f"{self.execution_recovery_key}:*"
            keys = await self.redis.keys(pattern)
            
            for key in keys:
                ttl = await self.redis.ttl(key)
                if ttl == -1:  # No TTL set, delete
                    await self.redis.delete(key)
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old recovery data: {e}")
    
    async def get_recovery_status(self, execution_id: UUID) -> Optional[ExecutionRecoveryInfo]:
        """Get recovery status for an execution."""
        try:
            key = f"{self.execution_recovery_key}:{execution_id}"
            data = await self.redis.get(key)
            
            if data:
                return ExecutionRecoveryInfo.model_validate_json(data)
                
        except Exception as e:
            logger.error(f"Failed to get recovery status: {e}")
        
        return None
    
    async def get_worker_health(self) -> List[WorkerHealthStatus]:
        """Get health status of all workers."""
        workers = []
        
        try:
            pattern = f"{self.worker_heartbeat_key}:*"
            keys = await self.redis.keys(pattern)
            
            for key in keys:
                worker_id = key.decode().split(":")[-1]
                
                try:
                    heartbeat_data = await self.redis.get(key)
                    if heartbeat_data:
                        data = json.loads(heartbeat_data)
                        
                        last_heartbeat = datetime.fromtimestamp(
                            data["timestamp"], tz=timezone.utc
                        )
                        
                        # Worker is healthy if heartbeat is recent
                        is_healthy = (
                            datetime.now(timezone.utc) - last_heartbeat
                        ).total_seconds() < 120
                        
                        metrics = data.get("metrics", {})
                        executions = [UUID(eid) for eid in data.get("executions", [])]
                        
                        worker_status = WorkerHealthStatus(
                            worker_id=worker_id,
                            last_heartbeat=last_heartbeat,
                            is_healthy=is_healthy,
                            current_executions=executions,
                            cpu_percent=metrics.get("cpu_percent", 0.0),
                            memory_percent=metrics.get("memory_percent", 0.0),
                            active_since=last_heartbeat,  # Simplified
                        )
                        
                        workers.append(worker_status)
                        
                except Exception as e:
                    logger.error(f"Failed to parse worker data for {worker_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to get worker health: {e}")
        
        return workers
    
    async def force_execution_recovery(
        self, 
        execution_id: UUID, 
        strategy: RecoveryStrategy,
        reason: str = "Manual recovery"
    ) -> bool:
        """Force recovery of a specific execution."""
        try:
            if not self.db:
                async with get_session() as session:
                    return await self._force_execution_recovery_impl(
                        session, execution_id, strategy, reason
                    )
            else:
                return await self._force_execution_recovery_impl(
                    self.db, execution_id, strategy, reason
                )
                
        except Exception as e:
            logger.error(f"Failed to force execution recovery: {e}")
            return False
    
    async def _force_execution_recovery_impl(
        self,
        session: AsyncSession,
        execution_id: UUID,
        strategy: RecoveryStrategy,
        reason: str
    ) -> bool:
        """Implementation of forced execution recovery."""
        # Get execution details
        result = await session.execute(
            select(Execution).where(Execution.id == execution_id)
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            logger.error(f"Execution {execution_id} not found")
            return False
        
        # Create recovery info
        recovery_info = ExecutionRecoveryInfo(
            execution_id=execution_id,
            workflow_id=execution.workflow_id,
            recovery_strategy=strategy,
            recovery_reason=reason,
            context_snapshot=execution.context or {},
        )
        
        # Perform recovery
        return await self.recover_execution(recovery_info)


# Global recovery service instance
_recovery_service: Optional[ExecutionRecoveryService] = None


def get_recovery_service() -> ExecutionRecoveryService:
    """Get global recovery service instance."""
    global _recovery_service
    if _recovery_service is None:
        _recovery_service = ExecutionRecoveryService()
    return _recovery_service


async def start_recovery_service() -> None:
    """Start the global recovery service."""
    service = get_recovery_service()
    await service.start()


async def stop_recovery_service() -> None:
    """Stop the global recovery service."""
    global _recovery_service
    if _recovery_service:
        await _recovery_service.stop()
        _recovery_service = None