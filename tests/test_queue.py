"""Test queue system implementation."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from unittest.mock import Mock, patch, AsyncMock, MagicMock

import pytest
from celery import Celery
from celery.result import AsyncResult
from redis import Redis

from budflow.queue import (
    QueueManager,
    JobData,
    JobOptions,
    JobStatus,
    JobMessage,
    MessageType,
    QueueEvents,
    QueueConfig,
    QueueMetrics,
    StalledJobError,
    MaxStalledCountError,
    QueueRecoveryService,
    WorkerManager,
    JobProgressMessage,
    RespondToWebhookMessage,
    JobFinishedMessage,
    JobFailedMessage,
    AbortJobMessage,
    SendChunkMessage,
)
from budflow.config import settings


@pytest.fixture
def queue_config():
    """Create test queue configuration."""
    return QueueConfig(
        redis_url="redis://localhost:6379/0",
        queue_name="test-jobs",
        worker_concurrency=2,
        lock_duration=30,
        lock_renew_time=15,
        stalled_interval=30,
        max_stalled_count=1,
        recovery_interval=60,
        recovery_batch_size=100,
        metrics_interval=30,
        enable_metrics=True,
    )


@pytest.fixture
def mock_celery_app():
    """Create mock Celery app."""
    app = Mock(spec=Celery)
    app.send_task = Mock(return_value=AsyncResult("test-task-id"))
    app.control = Mock()
    app.events = Mock()
    return app


@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    redis = Mock(spec=Redis)
    redis.ping = Mock(return_value=True)
    redis.set = Mock(return_value=True)
    redis.get = Mock(return_value=None)
    redis.delete = Mock(return_value=1)
    redis.expire = Mock(return_value=True)
    redis.close = Mock()  # Mock close method
    return redis


@pytest.mark.unit
class TestJobData:
    """Test JobData model."""
    
    def test_job_data_creation(self):
        """Test creating job data."""
        job_data = JobData(
            workflow_id="workflow-123",
            execution_id="exec-456",
            load_static_data=True,
            push_ref="push-789"
        )
        
        assert job_data.workflow_id == "workflow-123"
        assert job_data.execution_id == "exec-456"
        assert job_data.load_static_data is True
        assert job_data.push_ref == "push-789"
    
    def test_job_data_serialization(self):
        """Test job data serialization."""
        job_data = JobData(
            workflow_id="workflow-123",
            execution_id="exec-456",
            load_static_data=False
        )
        
        serialized = job_data.to_dict()
        assert serialized["workflow_id"] == "workflow-123"
        assert serialized["execution_id"] == "exec-456"
        assert serialized["load_static_data"] is False
        assert serialized["push_ref"] is None
        
        # Test deserialization
        deserialized = JobData.from_dict(serialized)
        assert deserialized.workflow_id == job_data.workflow_id
        assert deserialized.execution_id == job_data.execution_id


@pytest.mark.unit
class TestJobOptions:
    """Test JobOptions model."""
    
    def test_job_options_defaults(self):
        """Test job options with defaults."""
        options = JobOptions()
        
        assert options.priority == 1000  # Default priority
        assert options.remove_on_complete is True
        assert options.remove_on_fail is True
        assert options.delay is None
    
    def test_job_options_custom(self):
        """Test job options with custom values."""
        options = JobOptions(
            priority=1,  # Highest priority
            delay=timedelta(minutes=5)
        )
        
        assert options.priority == 1
        assert options.delay == timedelta(minutes=5)
        assert options.remove_on_complete is True
        
    def test_job_options_to_celery_options(self):
        """Test converting to Celery options."""
        options = JobOptions(
            priority=10,
            delay=timedelta(seconds=30)
        )
        
        celery_opts = options.to_celery_options()
        assert celery_opts["priority"] == 10
        assert "eta" in celery_opts  # ETA should be set for delayed jobs


@pytest.mark.unit
class TestQueueManager:
    """Test QueueManager functionality."""
    
    @pytest.mark.asyncio
    async def test_queue_manager_initialization(self, queue_config, mock_celery_app, mock_redis):
        """Test queue manager initialization."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                manager = QueueManager(queue_config)
                await manager.initialize()
                
                assert manager.is_initialized
                assert manager.celery_app == mock_celery_app
                assert manager.redis_client == mock_redis
                mock_redis.ping.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enqueue_job(self, queue_config, mock_celery_app, mock_redis):
        """Test enqueueing a job."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                with patch('budflow.queue.manager.EventEmitter') as MockEventEmitter:
                    mock_emitter = Mock()
                    mock_emitter.emit = Mock()
                    MockEventEmitter.return_value = mock_emitter
                    
                    manager = QueueManager(queue_config)
                    await manager.initialize()
                    
                    try:
                        job_data = JobData(
                            workflow_id="wf-1",
                            execution_id="exec-1",
                            load_static_data=True
                        )
                        
                        job_id = await manager.enqueue_job(job_data)
                        
                        # Verify job_id is a valid UUID
                        assert isinstance(job_id, str)
                        assert len(job_id) == 36  # UUID format
                        mock_celery_app.send_task.assert_called_once()
                        
                        # Verify job-enqueued event
                        mock_emitter.emit.assert_called()
                        emit_call = mock_emitter.emit.call_args
                        assert emit_call[0][0] == QueueEvents.JOB_ENQUEUED
                    finally:
                        # Clean up heartbeat tasks
                        await manager.close()
    
    @pytest.mark.asyncio
    async def test_enqueue_job_with_options(self, queue_config, mock_celery_app, mock_redis):
        """Test enqueueing a job with custom options."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                manager = QueueManager(queue_config)
                await manager.initialize()
                
                try:
                    job_data = JobData(
                        workflow_id="wf-1",
                        execution_id="exec-1"
                    )
                    options = JobOptions(
                        priority=1,  # Highest priority
                        delay=timedelta(minutes=1)
                    )
                    
                    job_id = await manager.enqueue_job(job_data, options)
                    
                    # Verify job_id is a valid UUID
                    assert isinstance(job_id, str)
                    assert len(job_id) == 36  # UUID format
                    send_call = mock_celery_app.send_task.call_args
                    assert send_call[1]["priority"] == 1
                    assert "eta" in send_call[1]
                finally:
                    # Clean up heartbeat tasks
                    await manager.close()
    
    @pytest.mark.asyncio
    async def test_get_queue_metrics(self, queue_config, mock_celery_app, mock_redis):
        """Test getting queue metrics."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                # Mock inspect methods
                inspect = Mock()
                inspect.active.return_value = {"worker1": [{"id": "1"}]}
                inspect.reserved.return_value = {"worker1": [{"id": "2"}, {"id": "3"}]}
                mock_celery_app.control.inspect.return_value = inspect
                
                manager = QueueManager(queue_config)
                await manager.initialize()
                
                metrics = await manager.get_metrics()
                
                assert isinstance(metrics, QueueMetrics)
                assert metrics.active_count == 1
                assert metrics.waiting_count == 2
                assert metrics.timestamp is not None


@pytest.mark.unit
class TestJobMessages:
    """Test job message types."""
    
    def test_respond_to_webhook_message(self):
        """Test webhook response message."""
        msg = RespondToWebhookMessage(
            webhook_id="webhook-123",
            response_data={"status": "ok"},
            response_code=200,
            response_headers={"Content-Type": "application/json"}
        )
        
        assert msg.type == MessageType.RESPOND_TO_WEBHOOK
        assert msg.webhook_id == "webhook-123"
        assert msg.response_code == 200
        
        serialized = msg.to_dict()
        assert serialized["type"] == MessageType.RESPOND_TO_WEBHOOK.value
        assert serialized["webhook_id"] == "webhook-123"
    
    def test_job_finished_message(self):
        """Test job finished message."""
        msg = JobFinishedMessage(
            execution_id="exec-123",
            success=True,
            result_data={"output": "data"}
        )
        
        assert msg.type == MessageType.JOB_FINISHED
        assert msg.execution_id == "exec-123"
        assert msg.success is True
    
    def test_job_failed_message(self):
        """Test job failed message."""
        msg = JobFailedMessage(
            execution_id="exec-123",
            error_message="Something went wrong",
            error_details={"code": "ERR001"},
            stack_trace="Traceback..."
        )
        
        assert msg.type == MessageType.JOB_FAILED
        assert msg.execution_id == "exec-123"
        assert msg.error_message == "Something went wrong"
    
    def test_abort_job_message(self):
        """Test abort job message."""
        msg = AbortJobMessage(
            job_id="job-123",
            reason="User requested cancellation"
        )
        
        assert msg.type == MessageType.ABORT_JOB
        assert msg.job_id == "job-123"
        assert msg.reason == "User requested cancellation"
    
    def test_send_chunk_message(self):
        """Test send chunk message for streaming."""
        msg = SendChunkMessage(
            execution_id="exec-123",
            chunk_data={"node": "node1", "output": [1, 2, 3]},
            chunk_index=0
        )
        
        assert msg.type == MessageType.SEND_CHUNK
        assert msg.execution_id == "exec-123"
        assert msg.chunk_index == 0


@pytest.mark.unit
class TestStalledJobRecovery:
    """Test stalled job recovery mechanism."""
    
    @pytest.mark.asyncio
    async def test_stalled_job_detection(self, queue_config, mock_celery_app, mock_redis):
        """Test detecting stalled jobs."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                # Mock Redis scan to return a heartbeat key
                mock_redis.scan.return_value = (0, [b"budflow:heartbeat:job-123"])
                
                # Mock stalled job heartbeat data
                heartbeat_data = {
                    "job_id": "job-123",
                    "worker_id": "worker-1",
                    "stalled_count": 0,
                    "last_heartbeat": (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
                }
                mock_redis.get.return_value = json.dumps(heartbeat_data)
                
                manager = QueueManager(queue_config)
                await manager.initialize()
                
                stalled_jobs = await manager.check_stalled_jobs()
                
                assert len(stalled_jobs) > 0
                assert "job-123" in stalled_jobs
                mock_redis.scan.assert_called()
                mock_redis.get.assert_called()
    
    @pytest.mark.asyncio
    async def test_max_stalled_count_error(self, queue_config, mock_celery_app, mock_redis):
        """Test max stalled count error."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                # Mock job that has been stalled too many times
                heartbeat_data = {
                    "job_id": "job-123",
                    "worker_id": "worker-1",
                    "stalled_count": 1,  # Already stalled once
                    "last_heartbeat": (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
                }
                mock_redis.get.return_value = json.dumps(heartbeat_data)
                
                manager = QueueManager(queue_config)
                await manager.initialize()
                
                with pytest.raises(MaxStalledCountError):
                    await manager.handle_stalled_job("job-123")


@pytest.mark.unit
class TestQueueRecoveryService:
    """Test queue recovery service."""
    
    @pytest.mark.asyncio
    async def test_recovery_service_initialization(self, queue_config, mock_celery_app, mock_redis):
        """Test recovery service initialization."""
        mock_db_session = AsyncMock()
        
        with patch('budflow.queue.recovery.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            MockQueueManager.return_value = mock_queue_manager
            
            service = QueueRecoveryService(
                queue_config=queue_config,
                db_session_factory=mock_db_session,
                is_leader_func=lambda: True
            )
            
            await service.start()
            
            assert service.is_running
            assert service._recovery_task is not None
            
            # Clean up the recovery task
            await service.stop()
    
    @pytest.mark.asyncio
    async def test_recovery_process(self, queue_config):
        """Test recovery process for crashed executions."""
        mock_execution = Mock()
        mock_execution.id = "exec-123"
        mock_execution.workflow_id = "wf-123"
        mock_execution.status = "running"
        mock_execution.data = {}
        
        # Mock the database session
        mock_db_session = AsyncMock()
        
        # Mock finding running executions using SQLAlchemy 2.0 style
        mock_scalars = Mock()
        mock_scalars.all.return_value = [mock_execution]
        
        mock_result = Mock()
        mock_result.scalars.return_value = mock_scalars
        
        mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_session.__aexit__ = AsyncMock(return_value=None)
        mock_db_session.execute = AsyncMock(return_value=mock_result)
        mock_db_session.commit = AsyncMock()
        mock_db_session.rollback = AsyncMock()
        mock_db_session.add = Mock()
        
        # Create a session factory that returns the mock session
        def session_factory():
            return mock_db_session
        
        with patch('budflow.queue.recovery.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            mock_queue_manager.job_exists = AsyncMock(return_value=False)
            MockQueueManager.return_value = mock_queue_manager
            
            service = QueueRecoveryService(
                queue_config=queue_config,
                db_session_factory=session_factory,
                is_leader_func=lambda: True
            )
            
            # Set the queue manager directly since we're not calling start()
            service.queue_manager = mock_queue_manager
            
            recovered = await service._perform_recovery()
            
            assert recovered == 1
            # Check that the execution was modified and session was used correctly
            mock_db_session.add.assert_called_with(mock_execution)
            mock_db_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_recovery_only_on_leader(self, queue_config):
        """Test recovery only runs on leader node."""
        mock_db_session = AsyncMock()
        
        with patch('budflow.queue.recovery.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            MockQueueManager.return_value = mock_queue_manager
            
            service = QueueRecoveryService(
                queue_config=queue_config,
                db_session_factory=mock_db_session,
                is_leader_func=lambda: False  # Not leader
            )
            
            await service.start()
            await asyncio.sleep(0.1)  # Let the task run
            
            # Should not perform recovery (check execute wasn't called)
            mock_db_session.execute.assert_not_called()
            
            # Clean up the recovery task
            await service.stop()


@pytest.mark.unit
class TestWorkerManager:
    """Test WorkerManager functionality."""
    
    @pytest.mark.asyncio
    async def test_worker_initialization(self, queue_config):
        """Test worker manager initialization."""
        with patch('budflow.queue.worker.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            MockQueueManager.return_value = mock_queue_manager
            
            # Also patch DatabaseManager
            with patch('budflow.queue.worker.DatabaseManager') as MockDBManager:
                mock_db_manager = Mock()
                mock_db_manager.initialize = AsyncMock()
                mock_db_manager.close = AsyncMock()
                MockDBManager.return_value = mock_db_manager
                
                worker = WorkerManager(queue_config)
                await worker.initialize()
                
                assert worker.is_initialized
                assert worker.active_jobs == {}
                assert worker.shutdown_event is not None
    
    @pytest.mark.asyncio
    async def test_process_job(self, queue_config):
        """Test processing a job."""
        with patch('budflow.queue.worker.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            mock_queue_manager.events = Mock()
            mock_queue_manager.events.emit = Mock()
            mock_queue_manager.send_job_message = AsyncMock()
            MockQueueManager.return_value = mock_queue_manager
            
            with patch('budflow.queue.worker.DatabaseManager') as MockDBManager:
                # Mock the database session and execution
                mock_execution = Mock()
                mock_execution.status = "running"
                
                mock_db_session = AsyncMock()
                mock_db_session.get = AsyncMock(return_value=mock_execution)
                mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
                mock_db_session.__aexit__ = AsyncMock(return_value=None)
                
                mock_db_manager = Mock()
                mock_db_manager.initialize = AsyncMock()
                mock_db_manager.close = AsyncMock()
                mock_db_manager.get_postgres_session = Mock(return_value=mock_db_session)
                MockDBManager.return_value = mock_db_manager
                
                with patch('budflow.queue.worker.WorkflowExecutionEngine') as MockEngine:
                    mock_engine = Mock()
                    mock_engine.resume_execution = AsyncMock(return_value={"success": True})
                    MockEngine.return_value = mock_engine
                    
                    worker = WorkerManager(queue_config)
                    await worker.initialize()
                    
                    job_data = JobData(
                        workflow_id="wf-1",
                        execution_id="exec-1",
                        load_static_data=True
                    )
                    
                    result = await worker.process_job("job-123", job_data)
                    
                    assert result["status"] == "success"
                    assert "job-123" not in worker.active_jobs
                    mock_engine.resume_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, queue_config):
        """Test graceful worker shutdown."""
        with patch('budflow.queue.worker.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            MockQueueManager.return_value = mock_queue_manager
            
            with patch('budflow.queue.worker.DatabaseManager') as MockDBManager:
                mock_db_manager = Mock()
                mock_db_manager.initialize = AsyncMock()
                mock_db_manager.close = AsyncMock()
                MockDBManager.return_value = mock_db_manager
                
                worker = WorkerManager(queue_config)
                await worker.initialize()
                
                # Add mock active job
                worker.active_jobs["job-123"] = asyncio.create_task(asyncio.sleep(1))
                
                # Start shutdown
                shutdown_task = asyncio.create_task(worker.shutdown(timeout=5))
                
                # Should wait for active jobs
                await asyncio.sleep(0.1)
                assert worker.shutdown_event.is_set()
                
                # Complete the job
                worker.active_jobs.clear()
                
                await shutdown_task
                assert len(worker.active_jobs) == 0
    
    @pytest.mark.asyncio
    async def test_health_check_endpoints(self, queue_config):
        """Test worker health check endpoints."""
        with patch('budflow.queue.worker.QueueManager') as MockQueueManager:
            mock_queue_manager = Mock()
            mock_queue_manager.initialize = AsyncMock()
            mock_queue_manager.close = AsyncMock()
            mock_queue_manager.is_healthy = AsyncMock(return_value=True)
            MockQueueManager.return_value = mock_queue_manager
            
            with patch('budflow.queue.worker.DatabaseManager') as MockDBManager:
                mock_health = {
                    "postgres": {"status": "healthy", "error": None}
                }
                mock_db_manager = Mock()
                mock_db_manager.initialize = AsyncMock()
                mock_db_manager.close = AsyncMock()
                mock_db_manager.health_check = AsyncMock(return_value=mock_health)
                MockDBManager.return_value = mock_db_manager
                
                worker = WorkerManager(queue_config)
                await worker.initialize()
                
                # Test basic health check
                health = await worker.health_check()
                assert health["status"] == "healthy"
                assert health["worker_id"] is not None
                
                # Test readiness check
                ready = await worker.readiness_check()
                assert ready["ready"] is True
                assert ready["redis"] is True
                assert ready["database"] is True


@pytest.mark.unit
class TestQueueMetrics:
    """Test queue metrics collection."""
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self, queue_config, mock_celery_app, mock_redis):
        """Test collecting queue metrics."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                manager = QueueManager(queue_config)
                await manager.initialize()
                
                # Start metrics collection
                await manager.start_metrics_collection()
                
                # Wait for metrics to be collected
                await asyncio.sleep(0.1)
                
                metrics = manager.get_latest_metrics()
                assert metrics is not None
                assert hasattr(metrics, "active_count")
                assert hasattr(metrics, "waiting_count")
                assert hasattr(metrics, "completed_count")
                assert hasattr(metrics, "failed_count")
    
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="export_metrics_to_prometheus not yet implemented")
    async def test_prometheus_metrics_export(self, queue_config, mock_celery_app, mock_redis):
        """Test exporting metrics to Prometheus."""
        with patch('budflow.queue.manager.Celery', return_value=mock_celery_app):
            with patch('budflow.queue.manager.Redis.from_url', return_value=mock_redis):
                with patch('budflow.queue.metrics.prometheus_client') as mock_prom:
                    manager = QueueManager(queue_config)
                    await manager.initialize()
                    
                    metrics = QueueMetrics(
                        active_count=5,
                        waiting_count=10,
                        completed_count=100,
                        failed_count=2
                    )
                    
                    await manager.export_metrics_to_prometheus(metrics)
                    
                    # Verify Prometheus gauges were set
                    assert mock_prom.Gauge.called


@pytest.mark.integration
class TestQueueIntegration:
    """Integration tests for queue system."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_job_processing(self, test_db_session):
        """Test complete job processing flow."""
        # This test requires Redis and database to be running
        config = QueueConfig(
            redis_url=settings.redis_url,
            queue_name="test-integration-jobs"
        )
        
        # Initialize queue manager
        queue_manager = QueueManager(config)
        await queue_manager.initialize()
        
        # Initialize worker
        worker = WorkerManager(config)
        await worker.initialize()
        
        # Create test workflow and execution
        from budflow.workflows.models import Workflow, WorkflowExecution
        
        workflow = Workflow(
            name="Test Workflow",
            user_id=1,
            nodes=[],
            connections=[]
        )
        test_db_session.add(workflow)
        await test_db_session.commit()
        
        execution = WorkflowExecution(
            workflow_id=workflow.id,
            status="new"
        )
        test_db_session.add(execution)
        await test_db_session.commit()
        
        # Enqueue job
        job_data = JobData(
            workflow_id=str(workflow.id),
            execution_id=str(execution.id),
            load_static_data=True
        )
        
        job_id = await queue_manager.enqueue_job(job_data)
        assert job_id is not None
        
        # Process job (in real scenario, this would be done by worker process)
        result = await worker.process_job(job_id, job_data)
        assert result is not None
        
        # Verify execution was updated
        await test_db_session.refresh(execution)
        assert execution.status in ["success", "failed"]
        
        # Cleanup
        await queue_manager.close()
        await worker.shutdown()