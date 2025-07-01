"""Queue manager implementation."""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Callable
from uuid import uuid4

from celery import Celery, Task
from celery.result import AsyncResult
from redis import Redis
from redis.exceptions import RedisError

from .config import QueueConfig
from .models import (
    JobData, JobOptions, JobStatus, JobMessage,
    QueueEvents, QueueMetrics, JobHeartbeat,
    MessageType
)
from .exceptions import (
    QueueConnectionError, QueueInitializationError,
    StalledJobError, MaxStalledCountError
)


logger = logging.getLogger(__name__)


class EventEmitter:
    """Simple event emitter for queue events."""
    
    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
    
    def on(self, event: str, handler: Callable) -> None:
        """Register an event handler."""
        if event not in self._handlers:
            self._handlers[event] = []
        self._handlers[event].append(handler)
    
    def emit(self, event: str, *args, **kwargs) -> None:
        """Emit an event to all handlers."""
        if event in self._handlers:
            for handler in self._handlers[event]:
                try:
                    handler(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in event handler for {event}: {e}")
    
    def off(self, event: str, handler: Callable) -> None:
        """Remove an event handler."""
        if event in self._handlers and handler in self._handlers[event]:
            self._handlers[event].remove(handler)


class QueueTask(Task):
    """Custom Celery task class for queue jobs."""
    
    def __init__(self):
        super().__init__()
        self.queue_manager: Optional["QueueManager"] = None
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure."""
        if self.queue_manager:
            self.queue_manager.events.emit(
                QueueEvents.JOB_FAILED,
                task_id,
                exc,
                einfo
            )
    
    def on_success(self, retval, task_id, args, kwargs):
        """Handle task success."""
        if self.queue_manager:
            self.queue_manager.events.emit(
                QueueEvents.JOB_FINISHED,
                task_id,
                retval
            )


class QueueManager:
    """Main queue manager for job processing."""
    
    def __init__(self, config: QueueConfig):
        self.config = config
        self.celery_app: Optional[Celery] = None
        self.redis_client: Optional[Redis] = None
        self.events = EventEmitter()
        self.is_initialized = False
        self._metrics_task: Optional[asyncio.Task] = None
        self._latest_metrics: Optional[QueueMetrics] = None
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}
    
    async def initialize(self) -> None:
        """Initialize queue manager."""
        try:
            # Initialize Celery app
            self.celery_app = Celery(
                "budflow",
                broker=self.config.get_redis_url(),
                backend=self.config.get_redis_url()
            )
            
            # Configure Celery
            self.celery_app.conf.update(**self.config.get_celery_config())
            
            # Register custom task class
            self.celery_app.Task = QueueTask
            self.celery_app.Task.queue_manager = self
            
            # Initialize Redis client
            self.redis_client = Redis.from_url(
                self.config.get_redis_url(),
                decode_responses=True
            )
            
            # Test Redis connection
            await asyncio.to_thread(self.redis_client.ping)
            
            self.is_initialized = True
            logger.info("Queue manager initialized successfully")
            
        except Exception as e:
            raise QueueInitializationError(f"Failed to initialize queue: {e}")
    
    async def close(self) -> None:
        """Close queue connections."""
        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
        
        for task in self._heartbeat_tasks.values():
            task.cancel()
        
        if self.redis_client:
            await asyncio.to_thread(self.redis_client.close)
        
        self.is_initialized = False
    
    async def enqueue_job(
        self,
        job_data: JobData,
        options: Optional[JobOptions] = None
    ) -> str:
        """Enqueue a job for processing."""
        if not self.is_initialized:
            raise QueueInitializationError("Queue manager not initialized")
        
        options = options or JobOptions()
        job_id = str(uuid4())
        
        # Prepare task kwargs
        task_kwargs = {
            "job_id": job_id,
            "job_data": job_data.to_dict(),
        }
        
        # Get Celery options
        celery_options = options.to_celery_options()
        
        # Send task to Celery
        result = self.celery_app.send_task(
            f"{self.config.job_type}.process",
            kwargs=task_kwargs,
            queue=self.config.queue_name,
            task_id=job_id,
            **celery_options
        )
        
        # Emit job enqueued event
        self.events.emit(
            QueueEvents.JOB_ENQUEUED,
            job_id,
            job_data,
            options
        )
        
        # Start heartbeat for this job
        await self._start_heartbeat(job_id)
        
        logger.info(f"Enqueued job {job_id} for workflow {job_data.workflow_id}")
        return job_id
    
    async def abort_job(self, job_id: str, reason: str = "User requested") -> bool:
        """Abort a running job."""
        if not self.is_initialized:
            raise QueueInitializationError("Queue manager not initialized")
        
        try:
            # Revoke the task
            self.celery_app.control.revoke(job_id, terminate=True)
            
            # Stop heartbeat
            await self._stop_heartbeat(job_id)
            
            logger.info(f"Aborted job {job_id}: {reason}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to abort job {job_id}: {e}")
            return False
    
    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get current job status."""
        if not self.is_initialized:
            raise QueueInitializationError("Queue manager not initialized")
        
        result = AsyncResult(job_id, app=self.celery_app)
        
        if result.state == "PENDING":
            return JobStatus.PENDING
        elif result.state == "STARTED":
            return JobStatus.ACTIVE
        elif result.state == "SUCCESS":
            return JobStatus.COMPLETED
        elif result.state == "FAILURE":
            return JobStatus.FAILED
        elif result.state == "REVOKED":
            return JobStatus.CANCELLED
        else:
            return None
    
    async def job_exists(self, job_id: str) -> bool:
        """Check if a job exists in the queue."""
        status = await self.get_job_status(job_id)
        return status is not None
    
    async def send_job_message(self, job_id: str, message: JobMessage) -> None:
        """Send a message to a job (for progress updates)."""
        if not self.is_initialized:
            raise QueueInitializationError("Queue manager not initialized")
        
        # Store message in Redis for job to pick up
        key = f"{self.config.redis_key_prefix}:job:message:{job_id}"
        await asyncio.to_thread(
            self.redis_client.lpush,
            key,
            message.to_json()
        )
        
        # Set expiration
        await asyncio.to_thread(
            self.redis_client.expire,
            key,
            3600  # 1 hour
        )
    
    async def get_metrics(self) -> QueueMetrics:
        """Get current queue metrics."""
        if not self.is_initialized:
            raise QueueInitializationError("Queue manager not initialized")
        
        try:
            # Get active and waiting jobs from Celery inspect
            inspect = self.celery_app.control.inspect()
            
            active = await asyncio.to_thread(inspect.active)
            reserved = await asyncio.to_thread(inspect.reserved)
            
            active_count = sum(len(tasks) for tasks in (active or {}).values())
            waiting_count = sum(len(tasks) for tasks in (reserved or {}).values())
            
            # Get completed and failed counts from Redis counters
            completed_key = f"{self.config.redis_key_prefix}:metrics:completed"
            failed_key = f"{self.config.redis_key_prefix}:metrics:failed"
            
            completed_count = int(await asyncio.to_thread(
                self.redis_client.get, completed_key
            ) or 0)
            
            failed_count = int(await asyncio.to_thread(
                self.redis_client.get, failed_key
            ) or 0)
            
            return QueueMetrics(
                active_count=active_count,
                waiting_count=waiting_count,
                completed_count=completed_count,
                failed_count=failed_count
            )
            
        except Exception as e:
            logger.error(f"Failed to get queue metrics: {e}")
            return QueueMetrics()
    
    async def check_stalled_jobs(self) -> List[str]:
        """Check for stalled jobs."""
        if not self.is_initialized:
            raise QueueInitializationError("Queue manager not initialized")
        
        stalled_jobs = []
        pattern = f"{self.config.redis_key_prefix}:heartbeat:*"
        
        try:
            # Get all heartbeat keys
            cursor = 0
            while True:
                cursor, keys = await asyncio.to_thread(
                    self.redis_client.scan,
                    cursor,
                    match=pattern,
                    count=100
                )
                
                for key in keys:
                    heartbeat_data = await asyncio.to_thread(
                        self.redis_client.get, key
                    )
                    
                    if heartbeat_data:
                        try:
                            heartbeat = JobHeartbeat.from_json(heartbeat_data)
                            
                            # Check if stalled
                            time_since_heartbeat = (
                                datetime.now(timezone.utc) - heartbeat.last_heartbeat
                            ).total_seconds()
                            
                            if time_since_heartbeat > self.config.stalled_interval:
                                stalled_jobs.append(heartbeat.job_id)
                                
                        except Exception as e:
                            logger.error(f"Failed to parse heartbeat: {e}")
                
                if cursor == 0:
                    break
            
        except Exception as e:
            logger.error(f"Failed to check stalled jobs: {e}")
        
        return stalled_jobs
    
    async def handle_stalled_job(self, job_id: str) -> None:
        """Handle a stalled job."""
        heartbeat_key = f"{self.config.redis_key_prefix}:heartbeat:{job_id}"
        
        try:
            # Get current heartbeat data
            heartbeat_data = await asyncio.to_thread(
                self.redis_client.get, heartbeat_key
            )
            
            if not heartbeat_data:
                return
            
            heartbeat = JobHeartbeat.from_json(heartbeat_data)
            
            # Check if exceeded max stalled count
            if heartbeat.stalled_count >= self.config.max_stalled_count:
                # Mark as permanently failed
                await self._stop_heartbeat(job_id)
                raise MaxStalledCountError(job_id, self.config.max_stalled_count)
            
            # Increment stalled count
            heartbeat.stalled_count += 1
            heartbeat.last_heartbeat = datetime.now(timezone.utc)
            
            await asyncio.to_thread(
                self.redis_client.set,
                heartbeat_key,
                heartbeat.to_json(),
                ex=self.config.lock_duration * 2
            )
            
            # Emit stalled event
            self.events.emit(QueueEvents.JOB_STALLED, job_id, heartbeat.stalled_count)
            
            raise StalledJobError(
                f"Job {job_id} is stalled",
                job_id,
                heartbeat.stalled_count
            )
            
        except MaxStalledCountError:
            raise
        except Exception as e:
            logger.error(f"Failed to handle stalled job {job_id}: {e}")
    
    async def _start_heartbeat(self, job_id: str) -> None:
        """Start heartbeat for a job."""
        async def heartbeat_task():
            heartbeat = JobHeartbeat(
                job_id=job_id,
                worker_id=self.config.worker_id or "main"
            )
            heartbeat_key = f"{self.config.redis_key_prefix}:heartbeat:{job_id}"
            
            try:
                while True:
                    # Update heartbeat
                    heartbeat.last_heartbeat = datetime.now(timezone.utc)
                    
                    await asyncio.to_thread(
                        self.redis_client.set,
                        heartbeat_key,
                        heartbeat.to_json(),
                        ex=self.config.lock_duration * 2
                    )
                    
                    # Wait for next heartbeat
                    await asyncio.sleep(self.config.lock_renew_time)
                    
            except asyncio.CancelledError:
                # Clean up heartbeat key
                await asyncio.to_thread(
                    self.redis_client.delete,
                    heartbeat_key
                )
                raise
        
        self._heartbeat_tasks[job_id] = asyncio.create_task(heartbeat_task())
    
    async def _stop_heartbeat(self, job_id: str) -> None:
        """Stop heartbeat for a job."""
        if job_id in self._heartbeat_tasks:
            self._heartbeat_tasks[job_id].cancel()
            try:
                await self._heartbeat_tasks[job_id]
            except asyncio.CancelledError:
                pass
            del self._heartbeat_tasks[job_id]
    
    async def start_metrics_collection(self) -> None:
        """Start periodic metrics collection."""
        if not self.config.enable_metrics:
            return
        
        async def collect_metrics():
            try:
                while True:
                    self._latest_metrics = await self.get_metrics()
                    await asyncio.sleep(self.config.metrics_interval)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Metrics collection error: {e}")
        
        self._metrics_task = asyncio.create_task(collect_metrics())
    
    def get_latest_metrics(self) -> Optional[QueueMetrics]:
        """Get latest collected metrics."""
        return self._latest_metrics
    
    async def export_metrics_to_prometheus(self, metrics: QueueMetrics) -> None:
        """Export metrics in Prometheus format."""
        if not self.config.prometheus_enabled:
            return
        
        # In a real implementation, this would update Prometheus metrics
        # For now, we just log the metrics
        logger.debug(f"Queue metrics: {metrics.to_dict()}")
    
    async def is_healthy(self) -> bool:
        """Check if queue system is healthy."""
        try:
            # Check Redis connection
            await asyncio.to_thread(self.redis_client.ping)
            
            # Check Celery connection
            inspect = self.celery_app.control.inspect()
            stats = await asyncio.to_thread(inspect.stats)
            
            return stats is not None
            
        except Exception as e:
            logger.error(f"Queue health check failed: {e}")
            return False