"""Test Execution Recovery Service."""

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest

from budflow.core.recovery import (
    ExecutionRecoveryService,
    RecoveryStrategy,
    RecoveryPolicy,
    ExecutionRecoveryInfo,
    WorkerHealthStatus,
)
from budflow.models.executions import ExecutionStatus


@pytest.fixture
def recovery_service():
    """Create recovery service with mocks."""
    redis_client = AsyncMock()
    db_session = AsyncMock()
    
    service = ExecutionRecoveryService(redis_client=redis_client, db=db_session)
    return service


@pytest.fixture
def recovery_policy():
    """Create test recovery policy."""
    return RecoveryPolicy(
        max_retries=3,
        retry_delay_seconds=60,
        strategy=RecoveryStrategy.RESTART,
        exponential_backoff=True,
    )


@pytest.fixture
def recovery_info():
    """Create test recovery info."""
    return ExecutionRecoveryInfo(
        execution_id=uuid4(),
        workflow_id=uuid4(),
        recovery_strategy=RecoveryStrategy.RESTART,
        recovery_reason="Test failure",
        context_snapshot={"key": "value"},
    )


@pytest.mark.unit
class TestRecoveryPolicy:
    """Test recovery policy configuration."""
    
    def test_default_policy(self):
        """Test default recovery policy."""
        policy = RecoveryPolicy()
        
        assert policy.max_retries == 3
        assert policy.retry_delay_seconds == 60
        assert policy.strategy == RecoveryStrategy.RESTART
        assert policy.exponential_backoff is True
        assert policy.max_retry_delay == 3600
        assert policy.recovery_timeout == 86400
        assert policy.notify_on_failure is True
    
    def test_custom_policy(self):
        """Test custom recovery policy."""
        policy = RecoveryPolicy(
            max_retries=5,
            retry_delay_seconds=30,
            strategy=RecoveryStrategy.RESUME,
            exponential_backoff=False,
        )
        
        assert policy.max_retries == 5
        assert policy.retry_delay_seconds == 30
        assert policy.strategy == RecoveryStrategy.RESUME
        assert policy.exponential_backoff is False


@pytest.mark.unit
class TestExecutionRecoveryInfo:
    """Test execution recovery info model."""
    
    def test_recovery_info_creation(self, recovery_info):
        """Test recovery info creation."""
        assert recovery_info.recovery_attempts == 0
        assert recovery_info.last_recovery_at is None
        assert recovery_info.recovery_strategy == RecoveryStrategy.RESTART
        assert recovery_info.recovery_reason == "Test failure"
        assert recovery_info.context_snapshot == {"key": "value"}
    
    def test_recovery_info_serialization(self, recovery_info):
        """Test recovery info serialization."""
        data = recovery_info.model_dump()
        
        assert "execution_id" in data
        assert "workflow_id" in data
        assert "recovery_strategy" in data
        assert "recovery_reason" in data
        assert "context_snapshot" in data
        
        # Test deserialization
        restored = ExecutionRecoveryInfo.model_validate(data)
        assert restored.execution_id == recovery_info.execution_id
        assert restored.workflow_id == recovery_info.workflow_id


@pytest.mark.unit
class TestExecutionRecoveryService:
    """Test execution recovery service."""
    
    def test_service_initialization(self):
        """Test service initialization."""
        redis_client = AsyncMock()
        service = ExecutionRecoveryService(redis_client=redis_client)
        
        assert service.redis == redis_client
        assert service.is_running is False
        assert len(service.recovery_tasks) == 0
        assert service.default_policy.max_retries == 3
    
    def test_recovery_policy_management(self, recovery_service, recovery_policy):
        """Test recovery policy management."""
        workflow_id = str(uuid4())
        
        # Set policy
        recovery_service.set_recovery_policy(workflow_id, recovery_policy)
        
        # Get policy
        retrieved_policy = recovery_service.get_recovery_policy(workflow_id)
        assert retrieved_policy == recovery_policy
        
        # Get default policy for unknown workflow
        unknown_policy = recovery_service.get_recovery_policy("unknown")
        assert unknown_policy == recovery_service.default_policy
    
    @pytest.mark.asyncio
    async def test_worker_heartbeat_registration(self, recovery_service):
        """Test worker heartbeat registration."""
        worker_id = "worker-1"
        executions = [uuid4(), uuid4()]
        metrics = {"cpu_percent": 45.0, "memory_percent": 60.0}
        
        await recovery_service.register_worker_heartbeat(worker_id, executions, metrics)
        
        # Verify Redis call
        recovery_service.redis.setex.assert_called_once()
        call_args = recovery_service.redis.setex.call_args
        
        assert call_args[0][0] == f"budflow:worker_heartbeats:{worker_id}"
        assert call_args[0][1] == 120  # TTL
        
        # Verify data structure
        stored_data = json.loads(call_args[0][2])
        assert stored_data["worker_id"] == worker_id
        assert len(stored_data["executions"]) == 2
        assert stored_data["metrics"] == metrics
    
    @pytest.mark.asyncio
    async def test_detect_crashed_workers(self, recovery_service):
        """Test crashed worker detection."""
        # Mock Redis keys response
        recovery_service.redis.keys.return_value = [
            b"budflow:worker_heartbeats:worker-1",
            b"budflow:worker_heartbeats:worker-2",
            b"budflow:worker_heartbeats:worker-3",
        ]
        
        # Mock heartbeat data
        current_time = time.time()
        old_time = current_time - 300  # 5 minutes ago
        
        def mock_get(key):
            if b"worker-1" in key:
                return json.dumps({"timestamp": current_time})  # Healthy
            elif b"worker-2" in key:
                return json.dumps({"timestamp": old_time})  # Crashed
            else:
                return None  # No data (crashed)
        
        recovery_service.redis.get.side_effect = mock_get
        
        crashed_workers = await recovery_service._detect_crashed_workers()
        
        assert "worker-2" in crashed_workers
        assert "worker-3" in crashed_workers
        assert "worker-1" not in crashed_workers
    
    @pytest.mark.asyncio
    async def test_get_worker_executions(self, recovery_service):
        """Test getting worker executions."""
        worker_id = "worker-1"
        execution_ids = [str(uuid4()), str(uuid4())]
        
        heartbeat_data = {
            "executions": execution_ids,
            "timestamp": time.time(),
        }
        
        recovery_service.redis.get.return_value = json.dumps(heartbeat_data)
        
        executions = await recovery_service._get_worker_executions(worker_id)
        
        assert len(executions) == 2
        assert all(str(exec_id) in execution_ids for exec_id in executions)
    
    @pytest.mark.asyncio
    async def test_calculate_retry_delay(self, recovery_service):
        """Test retry delay calculation."""
        policy = RecoveryPolicy(
            retry_delay_seconds=60,
            exponential_backoff=True,
            max_retry_delay=3600,
        )
        
        # Test exponential backoff
        delay1 = recovery_service._calculate_retry_delay(1, policy)
        delay2 = recovery_service._calculate_retry_delay(2, policy)
        delay3 = recovery_service._calculate_retry_delay(3, policy)
        
        assert delay1 == 60   # 60 * 2^0
        assert delay2 == 120  # 60 * 2^1
        assert delay3 == 240  # 60 * 2^2
        
        # Test max delay cap
        delay_large = recovery_service._calculate_retry_delay(10, policy)
        assert delay_large == 3600  # Capped at max
        
        # Test linear backoff
        policy.exponential_backoff = False
        delay_linear = recovery_service._calculate_retry_delay(5, policy)
        assert delay_linear == 60  # Always base delay
    
    @pytest.mark.asyncio
    async def test_store_and_get_recovery_info(self, recovery_service, recovery_info):
        """Test storing and retrieving recovery info."""
        # Test store
        await recovery_service._store_recovery_info(recovery_info)
        
        key = f"budflow:execution_recovery:{recovery_info.execution_id}"
        recovery_service.redis.setex.assert_called_once_with(
            key, 604800, recovery_info.model_dump_json()
        )
        
        # Test get
        recovery_service.redis.get.return_value = recovery_info.model_dump_json()
        
        retrieved_info = await recovery_service.get_recovery_status(recovery_info.execution_id)
        
        assert retrieved_info is not None
        assert retrieved_info.execution_id == recovery_info.execution_id
        assert retrieved_info.workflow_id == recovery_info.workflow_id
    
    @pytest.mark.asyncio
    async def test_schedule_execution_restart(self, recovery_service):
        """Test scheduling execution restart."""
        execution_id = uuid4()
        delay = 60
        
        await recovery_service._schedule_execution_restart(execution_id, delay)
        
        # Verify Redis call
        recovery_service.redis.zadd.assert_called_once()
        call_args = recovery_service.redis.zadd.call_args
        
        assert call_args[0][0] == "budflow:delayed_executions"
        
        # Verify schedule data
        schedule_data = list(call_args[0][1].keys())[0]
        data = json.loads(schedule_data)
        
        assert data["execution_id"] == str(execution_id)
        assert data["action"] == "restart"
        assert "scheduled_at" in data
    
    @pytest.mark.asyncio
    async def test_process_delayed_executions(self, recovery_service):
        """Test processing delayed executions."""
        execution_id = uuid4()
        current_time = time.time()
        
        # Mock ready execution
        schedule_data = {
            "execution_id": str(execution_id),
            "action": "restart",
            "scheduled_at": current_time - 10,  # Ready
        }
        
        recovery_service.redis.zrangebyscore.return_value = [
            (json.dumps(schedule_data), current_time - 10)
        ]
        
        await recovery_service._process_delayed_executions()
        
        # Verify execution was triggered
        recovery_service.redis.lpush.assert_called_once_with(
            "budflow:execution_queue",
            json.dumps({"execution_id": str(execution_id), "action": "restart"})
        )
        
        # Verify removal from delayed queue
        recovery_service.redis.zrem.assert_called_once_with(
            "budflow:delayed_executions",
            json.dumps(schedule_data)
        )
    
    @pytest.mark.asyncio
    async def test_get_worker_health(self, recovery_service):
        """Test getting worker health status."""
        current_time = time.time()
        execution_ids = [str(uuid4()), str(uuid4())]
        
        # Mock worker heartbeats
        recovery_service.redis.keys.return_value = [
            b"budflow:worker_heartbeats:worker-1",
            b"budflow:worker_heartbeats:worker-2",
        ]
        
        def mock_get(key):
            if b"worker-1" in key:
                return json.dumps({
                    "timestamp": current_time - 30,  # Healthy
                    "executions": execution_ids,
                    "metrics": {"cpu_percent": 45.0, "memory_percent": 60.0},
                })
            elif b"worker-2" in key:
                return json.dumps({
                    "timestamp": current_time - 150,  # Unhealthy
                    "executions": [],
                    "metrics": {"cpu_percent": 80.0, "memory_percent": 90.0},
                })
            return None
        
        recovery_service.redis.get.side_effect = mock_get
        
        workers = await recovery_service.get_worker_health()
        
        assert len(workers) == 2
        
        # Find worker-1 and worker-2
        worker1 = next(w for w in workers if w.worker_id == "worker-1")
        worker2 = next(w for w in workers if w.worker_id == "worker-2")
        
        assert worker1.is_healthy is True
        assert worker1.cpu_percent == 45.0
        assert len(worker1.current_executions) == 2
        
        assert worker2.is_healthy is False
        assert worker2.cpu_percent == 80.0
        assert len(worker2.current_executions) == 0
    
    @pytest.mark.asyncio
    async def test_queue_for_manual_recovery(self, recovery_service, recovery_info):
        """Test queueing for manual recovery."""
        await recovery_service._queue_for_manual_recovery(recovery_info)
        
        recovery_service.redis.lpush.assert_called_once_with(
            "budflow:manual_recovery",
            recovery_info.model_dump_json()
        )


@pytest.mark.unit
class TestRecoveryStrategies:
    """Test different recovery strategies."""
    
    @pytest.mark.asyncio
    async def test_restart_execution(self, recovery_service, recovery_info, recovery_policy):
        """Test restart recovery strategy."""
        # Mock database operations
        recovery_service.db.execute = AsyncMock()
        recovery_service.db.commit = AsyncMock()
        
        result = await recovery_service._restart_execution_impl(
            recovery_service.db, recovery_info, recovery_policy
        )
        
        assert result is True
        assert recovery_info.recovery_attempts == 1
        assert recovery_info.last_recovery_at is not None
        
        # Verify database update call
        recovery_service.db.execute.assert_called()
        recovery_service.db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_resume_execution(self, recovery_service, recovery_info, recovery_policy):
        """Test resume recovery strategy."""
        # Mock database operations
        recovery_service.db.execute = AsyncMock()
        recovery_service.db.commit = AsyncMock()
        
        result = await recovery_service._resume_execution_impl(
            recovery_service.db, recovery_info, recovery_policy
        )
        
        assert result is True
        assert recovery_info.recovery_attempts == 1
        assert recovery_info.last_recovery_at is not None
        
        # Verify database update call
        recovery_service.db.execute.assert_called()
        recovery_service.db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_abort_execution(self, recovery_service, recovery_info):
        """Test abort recovery strategy."""
        # Mock database operations
        recovery_service.db.execute = AsyncMock()
        recovery_service.db.commit = AsyncMock()
        
        result = await recovery_service._abort_execution_impl(
            recovery_service.db, recovery_info
        )
        
        assert result is True
        
        # Verify database update call
        recovery_service.db.execute.assert_called()
        recovery_service.db.commit.assert_called()
    
    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self, recovery_service, recovery_info):
        """Test behavior when max retries exceeded."""
        # Set recovery attempts to exceed max
        recovery_info.recovery_attempts = 5
        
        policy = RecoveryPolicy(max_retries=3)
        recovery_service.set_recovery_policy(str(recovery_info.workflow_id), policy)
        
        # Mock mark_execution_failed
        recovery_service._mark_execution_failed = AsyncMock()
        
        result = await recovery_service.recover_execution(recovery_info)
        
        assert result is False
        recovery_service._mark_execution_failed.assert_called_once_with(recovery_info)


@pytest.mark.integration
class TestRecoveryServiceIntegration:
    """Integration tests for recovery service."""
    
    @pytest.mark.asyncio
    async def test_service_start_stop(self, recovery_service):
        """Test service start and stop."""
        assert recovery_service.is_running is False
        
        # Start service
        await recovery_service.start()
        assert recovery_service.is_running is True
        assert len(recovery_service.recovery_tasks) == 3  # 3 background tasks
        
        # Stop service
        await recovery_service.stop()
        assert recovery_service.is_running is False
        assert len(recovery_service.recovery_tasks) == 0
    
    @pytest.mark.asyncio
    async def test_force_execution_recovery(self, recovery_service):
        """Test forcing execution recovery."""
        execution_id = uuid4()
        
        # Mock database operations
        mock_execution = Mock()
        mock_execution.id = execution_id
        mock_execution.workflow_id = uuid4()
        mock_execution.context = {"test": "data"}
        
        recovery_service.db.execute = AsyncMock()
        recovery_service.db.execute.return_value.scalar_one_or_none.return_value = mock_execution
        
        # Mock recovery
        recovery_service.recover_execution = AsyncMock(return_value=True)
        
        result = await recovery_service._force_execution_recovery_impl(
            recovery_service.db,
            execution_id,
            RecoveryStrategy.RESTART,
            "Manual recovery"
        )
        
        assert result is True
        recovery_service.recover_execution.assert_called_once()
        
        # Verify recovery info
        call_args = recovery_service.recover_execution.call_args[0][0]
        assert call_args.execution_id == execution_id
        assert call_args.recovery_strategy == RecoveryStrategy.RESTART
        assert call_args.recovery_reason == "Manual recovery"


@pytest.mark.performance
class TestRecoveryPerformance:
    """Performance tests for recovery service."""
    
    @pytest.mark.asyncio
    async def test_bulk_worker_health_check(self, recovery_service):
        """Test performance with many workers."""
        # Simulate 100 workers
        worker_keys = [f"budflow:worker_heartbeats:worker-{i}".encode() for i in range(100)]
        recovery_service.redis.keys.return_value = worker_keys
        
        current_time = time.time()
        
        def mock_get(key):
            return json.dumps({
                "timestamp": current_time - 30,
                "executions": [str(uuid4())],
                "metrics": {"cpu_percent": 50.0, "memory_percent": 60.0},
            })
        
        recovery_service.redis.get.side_effect = mock_get
        
        start_time = time.time()
        workers = await recovery_service.get_worker_health()
        end_time = time.time()
        
        # Should complete quickly
        assert end_time - start_time < 2.0  # 2 seconds
        assert len(workers) == 100
    
    @pytest.mark.asyncio
    async def test_bulk_delayed_execution_processing(self, recovery_service):
        """Test processing many delayed executions."""
        current_time = time.time()
        
        # Simulate 50 ready executions
        ready_executions = []
        for i in range(50):
            schedule_data = {
                "execution_id": str(uuid4()),
                "action": "restart",
                "scheduled_at": current_time - 10,
            }
            ready_executions.append((json.dumps(schedule_data), current_time - 10))
        
        recovery_service.redis.zrangebyscore.return_value = ready_executions
        
        start_time = time.time()
        await recovery_service._process_delayed_executions()
        end_time = time.time()
        
        # Should complete quickly
        assert end_time - start_time < 1.0  # 1 second
        
        # Verify all executions were processed
        assert recovery_service.redis.lpush.call_count == 50
        assert recovery_service.redis.zrem.call_count == 50