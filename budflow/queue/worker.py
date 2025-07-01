"""Queue worker implementation."""

import asyncio
import logging
import signal
import socket
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Set
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from .config import QueueConfig
from .models import (
    JobData, JobStatus, JobMessage,
    JobFinishedMessage, JobFailedMessage,
    MessageType, QueueEvents
)
from .manager import QueueManager
from .exceptions import (
    JobProcessingError, WorkerShutdownError
)
from ..database import DatabaseManager
from ..executor import WorkflowExecutionEngine
from ..workflows.models import WorkflowExecution


logger = logging.getLogger(__name__)


class WorkerManager:
    """Worker process manager for job execution."""
    
    def __init__(self, config: QueueConfig):
        self.config = config
        self.worker_id = config.worker_id or f"worker-{socket.gethostname()}-{uuid4().hex[:8]}"
        self.queue_manager: Optional[QueueManager] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.active_jobs: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self.is_initialized = False
        self.app: Optional[FastAPI] = None
    
    async def initialize(self) -> None:
        """Initialize worker components."""
        try:
            # Initialize queue manager
            self.queue_manager = QueueManager(self.config)
            await self.queue_manager.initialize()
            
            # Initialize database manager
            self.db_manager = DatabaseManager()
            await self.db_manager.initialize()
            
            # Setup signal handlers
            self._setup_signal_handlers()
            
            # Create FastAPI app for health checks
            self._create_health_check_app()
            
            self.is_initialized = True
            logger.info(f"Worker {self.worker_id} initialized successfully")
            
        except Exception as e:
            raise WorkerShutdownError(f"Failed to initialize worker: {e}")
    
    async def start(self) -> None:
        """Start worker process."""
        if not self.is_initialized:
            await self.initialize()
        
        logger.info(f"Worker {self.worker_id} starting...")
        
        # Start health check server
        health_task = asyncio.create_task(self._run_health_server())
        
        # Start processing jobs
        try:
            await self._process_jobs()
        finally:
            health_task.cancel()
            try:
                await health_task
            except asyncio.CancelledError:
                pass
    
    async def _process_jobs(self) -> None:
        """Main job processing loop."""
        # Register job processing task with Celery
        @self.queue_manager.celery_app.task(
            name=f"{self.config.job_type}.process",
            bind=True
        )
        def process_job_task(task_self, job_id: str, job_data: Dict[str, Any]):
            """Celery task for processing jobs."""
            # Run async job processing in sync context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                job = JobData.from_dict(job_data)
                result = loop.run_until_complete(
                    self.process_job(job_id, job)
                )
                return result
            finally:
                loop.close()
        
        # Wait for shutdown
        await self.shutdown_event.wait()
    
    async def process_job(self, job_id: str, job_data: JobData) -> Dict[str, Any]:
        """Process a single job."""
        logger.info(f"Processing job {job_id} for workflow {job_data.workflow_id}")
        
        # Emit job dequeued event
        self.queue_manager.events.emit(
            QueueEvents.JOB_DEQUEUED,
            job_id,
            job_data
        )
        
        # Create job task
        task = asyncio.create_task(self._execute_job(job_id, job_data))
        self.active_jobs[job_id] = task
        
        try:
            result = await task
            return result
        finally:
            self.active_jobs.pop(job_id, None)
    
    async def _execute_job(self, job_id: str, job_data: JobData) -> Dict[str, Any]:
        """Execute workflow for a job."""
        start_time = datetime.now(timezone.utc)
        
        async with self.db_manager.get_postgres_session() as session:
            try:
                # Get execution
                execution = await session.get(
                    WorkflowExecution,
                    job_data.execution_id
                )
                
                if not execution:
                    raise JobProcessingError(
                        f"Execution {job_data.execution_id} not found",
                        job_id
                    )
                
                # Check if already completed/failed
                if execution.status in ["success", "failed", "crashed"]:
                    logger.warning(
                        f"Execution {job_data.execution_id} already finished "
                        f"with status {execution.status}"
                    )
                    return {"status": execution.status, "skipped": True}
                
                # Create execution engine
                engine = WorkflowExecutionEngine(session)
                
                # Load static data if requested
                if job_data.load_static_data:
                    # In n8n, this loads pinned data and other static info
                    # For now, we'll just log it
                    logger.debug(f"Loading static data for workflow {job_data.workflow_id}")
                
                # Execute workflow
                result = await engine.resume_execution(
                    execution_id=job_data.execution_id,
                    push_ref=job_data.push_ref
                )
                
                # Calculate execution time
                execution_time_ms = int(
                    (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                )
                
                # Send success message
                success_msg = JobFinishedMessage(
                    execution_id=job_data.execution_id,
                    success=True,
                    result_data=result,
                    execution_time_ms=execution_time_ms
                )
                
                await self.queue_manager.send_job_message(job_id, success_msg)
                
                # Update metrics
                await self._update_metrics("completed", execution_time_ms)
                
                return {
                    "status": "success",
                    "execution_id": job_data.execution_id,
                    "execution_time_ms": execution_time_ms,
                    "result": result
                }
                
            except asyncio.CancelledError:
                # Job was cancelled
                logger.info(f"Job {job_id} was cancelled")
                raise
                
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}", exc_info=True)
                
                # Send failure message
                fail_msg = JobFailedMessage(
                    execution_id=job_data.execution_id,
                    error_message=str(e),
                    error_details={
                        "job_id": job_id,
                        "workflow_id": job_data.workflow_id
                    },
                    stack_trace=None  # Could add traceback here
                )
                
                await self.queue_manager.send_job_message(job_id, fail_msg)
                
                # Update metrics
                await self._update_metrics("failed")
                
                raise JobProcessingError(
                    f"Job execution failed: {e}",
                    job_id,
                    {"execution_id": job_data.execution_id}
                )
    
    async def _update_metrics(self, metric_type: str, execution_time_ms: Optional[int] = None) -> None:
        """Update job metrics in Redis."""
        try:
            if metric_type == "completed":
                key = f"{self.config.redis_key_prefix}:metrics:completed"
                await asyncio.to_thread(
                    self.queue_manager.redis_client.incr,
                    key
                )
                
                if execution_time_ms is not None:
                    # Store execution time for average calculation
                    time_key = f"{self.config.redis_key_prefix}:metrics:exec_times"
                    await asyncio.to_thread(
                        self.queue_manager.redis_client.lpush,
                        time_key,
                        execution_time_ms
                    )
                    # Keep only last 1000 times
                    await asyncio.to_thread(
                        self.queue_manager.redis_client.ltrim,
                        time_key,
                        0,
                        999
                    )
                    
            elif metric_type == "failed":
                key = f"{self.config.redis_key_prefix}:metrics:failed"
                await asyncio.to_thread(
                    self.queue_manager.redis_client.incr,
                    key
                )
        except Exception as e:
            logger.error(f"Failed to update metrics: {e}")
    
    async def shutdown(self, timeout: Optional[int] = None) -> None:
        """Gracefully shutdown worker."""
        timeout = timeout or self.config.shutdown_timeout
        logger.info(f"Worker {self.worker_id} shutting down...")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Stop accepting new jobs
        if self.queue_manager:
            # In Celery, this would be done via control commands
            pass
        
        # Wait for active jobs to complete
        if self.active_jobs:
            logger.info(f"Waiting for {len(self.active_jobs)} active jobs to complete...")
            
            try:
                await asyncio.wait_for(
                    self._wait_for_active_jobs(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for jobs, cancelling {len(self.active_jobs)} jobs")
                for task in self.active_jobs.values():
                    task.cancel()
        
        # Close connections
        if self.queue_manager:
            await self.queue_manager.close()
        
        if self.db_manager:
            await self.db_manager.close()
        
        logger.info(f"Worker {self.worker_id} shutdown complete")
    
    async def _wait_for_active_jobs(self) -> None:
        """Wait for all active jobs to complete."""
        while self.active_jobs:
            await asyncio.sleep(0.1)
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    def _create_health_check_app(self) -> None:
        """Create FastAPI app for health checks."""
        self.app = FastAPI(title="BudFlow Worker", version="1.0.0")
        
        @self.app.get("/healthz")
        async def health_check():
            """Basic health check."""
            health = await self.health_check()
            if health["status"] != "healthy":
                raise HTTPException(status_code=503, detail=health)
            return health
        
        @self.app.get("/healthz/readiness")
        async def readiness_check():
            """Readiness check."""
            ready = await self.readiness_check()
            if not ready["ready"]:
                raise HTTPException(status_code=503, detail=ready)
            return ready
    
    async def _run_health_server(self) -> None:
        """Run health check HTTP server."""
        if not self.app:
            return
        
        import uvicorn
        
        config = uvicorn.Config(
            self.app,
            host=self.config.worker_server_host,
            port=self.config.worker_server_port,
            log_level="warning"
        )
        server = uvicorn.Server(config)
        
        await server.serve()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        return {
            "status": "healthy",
            "worker_id": self.worker_id,
            "active_jobs": len(self.active_jobs),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    async def readiness_check(self) -> Dict[str, Any]:
        """Perform readiness check."""
        ready = True
        details = {
            "worker_id": self.worker_id,
            "ready": True,
            "redis": False,
            "database": False
        }
        
        # Check Redis
        try:
            if self.queue_manager and await self.queue_manager.is_healthy():
                details["redis"] = True
            else:
                ready = False
        except Exception:
            ready = False
        
        # Check database
        try:
            if self.db_manager:
                health = await self.db_manager.health_check()
                if health["postgres"]["status"] == "healthy":
                    details["database"] = True
                else:
                    ready = False
            else:
                ready = False
        except Exception:
            ready = False
        
        details["ready"] = ready
        return details