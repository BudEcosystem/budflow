"""Test task runner implementation."""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from unittest.mock import Mock, patch, AsyncMock, MagicMock

import pytest

from budflow.executor.task_runner import (
    TaskRunner,
    TaskRunnerConfig,
    TaskRunnerMode,
    TaskRequest,
    TaskResult,
    TaskStatus,
    TaskOffer,
    TaskRunnerProcess,
    TaskBroker,
    ResourceLimits,
    SecurityConfig,
    TaskRunnerError,
    TaskTimeoutError,
    ResourceLimitError,
    SecurityViolationError,
    SecuritySandbox,
    CodeAnalyzer,
    ResourceLimitEnforcer,
)


@pytest.fixture
def task_runner_config():
    """Create test task runner configuration."""
    return TaskRunnerConfig(
        mode=TaskRunnerMode.INTERNAL,
        max_concurrency=2,
        task_timeout=30,  # seconds
        heartbeat_interval=5,
        max_memory_mb=512,
        max_cpu_percent=50,
        max_payload_size_mb=100,
        allowed_modules=["json", "datetime", "math"],
        enable_sandboxing=True,
        enable_prototype_pollution_prevention=True,
    )


@pytest.fixture
def resource_limits():
    """Create test resource limits."""
    return ResourceLimits(
        max_memory_mb=512,
        max_cpu_percent=50,
        max_execution_time_seconds=30,
        max_payload_size_mb=100,
    )


@pytest.fixture
def security_config():
    """Create test security configuration."""
    return SecurityConfig(
        enable_sandboxing=True,
        allowed_builtin_modules=["json", "datetime", "math"],
        allowed_external_modules=[],
        enable_prototype_pollution_prevention=True,
        enable_code_analysis=True,
        disallow_code_generation=True,
    )


@pytest.mark.unit
class TestTaskRequest:
    """Test TaskRequest model."""
    
    def test_task_request_creation(self):
        """Test creating a task request."""
        request = TaskRequest(
            task_id="task-123",
            code="return 2 + 2",
            context={"items": [1, 2, 3]},
            required_modules=["json"],
            timeout=10,
        )
        
        assert request.task_id == "task-123"
        assert request.code == "return 2 + 2"
        assert request.context == {"items": [1, 2, 3]}
        assert request.required_modules == ["json"]
        assert request.timeout == 10
    
    def test_task_request_validation(self):
        """Test task request validation."""
        # Test invalid timeout
        with pytest.raises(ValueError):
            TaskRequest(
                task_id="task-123",
                code="return 1",
                timeout=-1,
            )
        
        # Test oversized payload
        with pytest.raises(ValueError):
            large_context = {"data": "x" * (1024 * 1024 * 1024)}  # 1GB
            TaskRequest(
                task_id="task-123",
                code="return 1",
                context=large_context,
            )


@pytest.mark.unit
class TestTaskResult:
    """Test TaskResult model."""
    
    def test_task_result_success(self):
        """Test successful task result."""
        result = TaskResult(
            task_id="task-123",
            status=TaskStatus.COMPLETED,
            result={"output": 42},
            execution_time_ms=100,
        )
        
        assert result.task_id == "task-123"
        assert result.status == TaskStatus.COMPLETED
        assert result.result == {"output": 42}
        assert result.error is None
        assert result.execution_time_ms == 100
    
    def test_task_result_failure(self):
        """Test failed task result."""
        result = TaskResult(
            task_id="task-123",
            status=TaskStatus.FAILED,
            error="Division by zero",
            error_details={"type": "ZeroDivisionError"},
            execution_time_ms=50,
        )
        
        assert result.task_id == "task-123"
        assert result.status == TaskStatus.FAILED
        assert result.error == "Division by zero"
        assert result.error_details == {"type": "ZeroDivisionError"}
        assert result.result is None


@pytest.mark.unit
class TestTaskOffer:
    """Test TaskOffer model."""
    
    def test_task_offer_creation(self):
        """Test creating a task offer."""
        offer = TaskOffer(
            offer_id="offer-123",
            runner_id="runner-1",
            valid_until=datetime.now(timezone.utc) + timedelta(seconds=30),
            capabilities=["python", "restricted"],
        )
        
        assert offer.offer_id == "offer-123"
        assert offer.runner_id == "runner-1"
        assert offer.is_valid()
        assert "python" in offer.capabilities
    
    def test_task_offer_expiration(self):
        """Test task offer expiration."""
        # Create expired offer
        offer = TaskOffer(
            offer_id="offer-123",
            runner_id="runner-1",
            valid_until=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        
        assert not offer.is_valid()


@pytest.mark.unit
class TestResourceLimits:
    """Test resource limit enforcement."""
    
    @pytest.mark.asyncio
    async def test_memory_limit_enforcement(self, resource_limits):
        """Test memory limit enforcement."""
        with patch('budflow.executor.task_runner.resource_limiter.psutil') as mock_psutil, \
             patch('budflow.executor.task_runner.resource_limiter.PSUTIL_AVAILABLE', True):
            # Mock memory usage
            mock_process = Mock()
            mock_process.memory_info.return_value.rss = 600 * 1024 * 1024  # 600MB
            mock_psutil.Process.return_value = mock_process
            # Mock NoSuchProcess exception
            mock_psutil.NoSuchProcess = Exception
            
            enforcer = ResourceLimitEnforcer(resource_limits)
            
            with pytest.raises(ResourceLimitError):
                await enforcer.check_memory_usage()
    
    @pytest.mark.asyncio
    async def test_cpu_limit_enforcement(self, resource_limits):
        """Test CPU limit enforcement."""
        with patch('budflow.executor.task_runner.resource_limiter.psutil') as mock_psutil, \
             patch('budflow.executor.task_runner.resource_limiter.PSUTIL_AVAILABLE', True):
            # Mock CPU usage
            mock_process = Mock()
            mock_process.cpu_percent.return_value = 75.0  # 75%
            mock_psutil.Process.return_value = mock_process
            # Mock NoSuchProcess exception
            mock_psutil.NoSuchProcess = Exception
            
            enforcer = ResourceLimitEnforcer(resource_limits)
            
            with pytest.raises(ResourceLimitError):
                await enforcer.check_cpu_usage()
    
    def test_execution_time_limit(self, resource_limits):
        """Test execution time limit."""
        enforcer = ResourceLimitEnforcer(resource_limits)
        
        # Start timer
        enforcer.start_execution()
        
        # Mock time passage
        with patch('time.time', return_value=time.time() + 31):
            with pytest.raises(TaskTimeoutError):
                enforcer.check_execution_time()


@pytest.mark.unit
class TestSecuritySandbox:
    """Test security sandboxing."""
    
    def test_restricted_python_sandbox(self, security_config):
        """Test RestrictedPython sandbox."""
        sandbox = SecuritySandbox(security_config)
        
        # Test safe code
        safe_code = "result = 2 + 2"
        compiled = sandbox.compile_restricted(safe_code)
        assert compiled is not None
        
        # Test unsafe code (should raise)
        unsafe_code = "__import__('os').system('ls')"
        with pytest.raises(SecurityViolationError):
            sandbox.compile_restricted(unsafe_code)
    
    def test_module_access_control(self, security_config):
        """Test module access control."""
        sandbox = SecuritySandbox(security_config)
        
        # Test allowed module
        assert sandbox.is_module_allowed("json")
        assert sandbox.is_module_allowed("datetime")
        
        # Test disallowed module
        assert not sandbox.is_module_allowed("os")
        assert not sandbox.is_module_allowed("subprocess")
    
    def test_code_analysis(self, security_config):
        """Test static code analysis."""
        analyzer = CodeAnalyzer(security_config)
        
        # Analyze code with imports
        code = """
import json
from datetime import datetime

data = json.dumps({"time": str(datetime.now())})
result = len(data)
        """
        
        analysis = analyzer.analyze(code)
        assert "json" in analysis.imports
        assert "datetime" in analysis.imports
        assert len(analysis.accessed_variables) > 0


@pytest.mark.unit
class TestTaskRunnerProcess:
    """Test TaskRunnerProcess functionality."""
    
    @pytest.mark.asyncio
    async def test_process_initialization(self, task_runner_config):
        """Test task runner process initialization."""
        with patch('budflow.executor.task_runner.process.Process') as MockProcess:
            mock_process = Mock()
            MockProcess.return_value = mock_process
            
            runner = TaskRunnerProcess(
                runner_id="runner-1",
                config=task_runner_config,
            )
            
            await runner.start()
            
            assert runner.is_running
            MockProcess.assert_called_once()
            mock_process.start.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_task_execution(self, task_runner_config):
        """Test task execution in runner process."""
        runner = TaskRunnerProcess(
            runner_id="runner-1",
            config=task_runner_config,
        )
        
        # Mock IPC
        mock_queue = AsyncMock()
        runner._task_queue = mock_queue
        runner._result_queue = mock_queue
        
        # Test simple task
        request = TaskRequest(
            task_id="task-123",
            code="result = 2 + 2",
            context={},
        )
        
        # Mock execution
        with patch.object(runner, '_execute_code') as mock_execute:
            mock_execute.return_value = 4
            
            result = await runner.execute_task(request)
            
            assert result.status == TaskStatus.COMPLETED
            assert result.result == 4
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_task_timeout(self, task_runner_config):
        """Test task timeout handling."""
        runner = TaskRunnerProcess(
            runner_id="runner-1",
            config=task_runner_config,
        )
        
        request = TaskRequest(
            task_id="task-123",
            code="import time; time.sleep(5); result = 1",
            context={},
            timeout=1,  # 1 second timeout
        )
        
        # Mock slow execution
        with patch.object(runner, '_execute_code') as mock_execute:
            async def slow_execution(*args):
                await asyncio.sleep(2)
                return 1
            
            mock_execute.side_effect = slow_execution
            
            result = await runner.execute_task(request)
            
            assert result.status == TaskStatus.TIMEOUT
            assert result.error is not None
    
    @pytest.mark.asyncio
    async def test_heartbeat_monitoring(self, task_runner_config):
        """Test heartbeat monitoring."""
        runner = TaskRunnerProcess(
            runner_id="runner-1",
            config=task_runner_config,
        )
        
        # Start heartbeat
        heartbeat_task = asyncio.create_task(runner._heartbeat_loop())
        
        # Check heartbeat updates
        initial_heartbeat = runner.last_heartbeat
        await asyncio.sleep(0.1)
        
        assert runner.last_heartbeat > initial_heartbeat
        
        # Cleanup
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, task_runner_config):
        """Test graceful shutdown."""
        with patch('budflow.executor.task_runner.process.Process') as MockProcess:
            mock_process = Mock()
            mock_process.is_alive.return_value = True
            MockProcess.return_value = mock_process
            
            runner = TaskRunnerProcess(
                runner_id="runner-1",
                config=task_runner_config,
            )
            
            await runner.start()
            
            # Add mock running task
            runner._running_tasks.add("task-123")
            
            # Shutdown
            await runner.shutdown(timeout=5)
            
            assert not runner.is_running
            mock_process.terminate.assert_called()


@pytest.mark.unit
class TestTaskBroker:
    """Test TaskBroker functionality."""
    
    @pytest.mark.asyncio
    async def test_broker_initialization(self, task_runner_config):
        """Test task broker initialization."""
        broker = TaskBroker(config=task_runner_config)
        
        await broker.start()
        
        assert broker.is_running
        assert len(broker._runners) == 0
        assert len(broker._pending_requests) == 0
        assert len(broker._active_offers) == 0
    
    @pytest.mark.asyncio
    async def test_runner_registration(self, task_runner_config):
        """Test runner registration with broker."""
        broker = TaskBroker(config=task_runner_config)
        await broker.start()
        
        # Register runner
        runner_id = await broker.register_runner()
        
        assert runner_id is not None
        assert runner_id in broker._runners
        assert broker._runners[runner_id].is_running
    
    @pytest.mark.asyncio
    async def test_task_offer_matching(self, task_runner_config):
        """Test task offer and request matching."""
        broker = TaskBroker(config=task_runner_config)
        await broker.start()
        
        # Register runner and create offer
        runner_id = await broker.register_runner()
        offer = TaskOffer(
            offer_id="offer-123",
            runner_id=runner_id,
            valid_until=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        
        await broker.submit_offer(offer)
        
        # Submit task request
        request = TaskRequest(
            task_id="task-123",
            code="result = 2 + 2",
            context={},
        )
        
        # Should match immediately
        assigned = await broker.submit_task(request)
        
        assert assigned
        assert offer.offer_id not in broker._active_offers
        assert request.task_id not in broker._pending_requests
    
    @pytest.mark.asyncio
    async def test_load_balancing(self, task_runner_config):
        """Test load balancing across multiple runners."""
        broker = TaskBroker(config=task_runner_config)
        await broker.start()
        
        # Register multiple runners
        runner1 = await broker.register_runner()
        runner2 = await broker.register_runner()
        
        # Track task assignments
        assignments = {runner1: 0, runner2: 0}
        
        # Submit multiple tasks
        for i in range(10):
            request = TaskRequest(
                task_id=f"task-{i}",
                code=f"result = {i}",
                context={},
            )
            
            # Mock offer creation for each runner
            for runner_id in [runner1, runner2]:
                offer = TaskOffer(
                    offer_id=f"offer-{runner_id}-{i}",
                    runner_id=runner_id,
                    valid_until=datetime.now(timezone.utc) + timedelta(seconds=30),
                )
                await broker.submit_offer(offer)
            
            await broker.submit_task(request)
        
        # Check distribution (should be roughly balanced)
        # Note: Actual implementation would track assignments
        # This is a simplified test
        assert len(broker._pending_requests) == 0
    
    @pytest.mark.asyncio
    async def test_runner_failure_recovery(self, task_runner_config):
        """Test recovery from runner failure."""
        broker = TaskBroker(config=task_runner_config)
        await broker.start()
        
        # Register runner
        runner_id = await broker.register_runner()
        runner = broker._runners[runner_id]
        
        # Simulate runner crash
        runner.is_running = False
        runner.last_heartbeat = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        # Health check should detect and restart
        await broker._health_check_loop_once()
        
        # Runner should be restarted
        assert broker._runners[runner_id].is_running


@pytest.mark.unit
class TestTaskRunner:
    """Test main TaskRunner interface."""
    
    @pytest.mark.asyncio
    async def test_task_runner_initialization(self, task_runner_config):
        """Test task runner initialization."""
        runner = TaskRunner(config=task_runner_config)
        
        await runner.start()
        
        assert runner.is_running
        assert runner.broker is not None
        assert runner.broker.is_running
    
    @pytest.mark.asyncio
    async def test_execute_code(self, task_runner_config):
        """Test code execution through task runner."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Execute simple code
        result = await runner.execute(
            code="result = 2 + 2",
            context={},
            timeout=10,
        )
        
        assert result["success"] is True
        assert result["result"] == 4
        assert result["error"] is None
    
    @pytest.mark.asyncio
    async def test_execute_with_context(self, task_runner_config):
        """Test code execution with context."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Execute code with context
        result = await runner.execute(
            code="""
items = context['items']
result = sum(items) * multiplier
            """,
            context={
                "items": [1, 2, 3, 4, 5],
                "multiplier": 2,
            },
            timeout=10,
        )
        
        assert result["success"] is True
        assert result["result"] == 30  # sum([1,2,3,4,5]) * 2
    
    @pytest.mark.asyncio
    async def test_execute_with_imports(self, task_runner_config):
        """Test code execution with allowed imports."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Execute code with imports
        result = await runner.execute(
            code="""
import json
import datetime

data = {"timestamp": str(datetime.datetime.now())}
result = json.dumps(data)
            """,
            context={},
            timeout=10,
        )
        
        assert result["success"] is True
        assert isinstance(result["result"], str)
        assert "timestamp" in json.loads(result["result"])
    
    @pytest.mark.asyncio
    async def test_execute_with_error(self, task_runner_config):
        """Test code execution with error."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Execute code that raises error
        result = await runner.execute(
            code="result = 1 / 0",
            context={},
            timeout=10,
        )
        
        assert result["success"] is False
        assert result["error"] is not None
        assert "ZeroDivisionError" in result["error"]
    
    @pytest.mark.asyncio
    async def test_execute_with_security_violation(self, task_runner_config):
        """Test code execution with security violation."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Try to execute unsafe code
        result = await runner.execute(
            code="import os; result = os.listdir('/')",
            context={},
            timeout=10,
        )
        
        assert result["success"] is False
        assert result["error"] is not None
        assert "security" in result["error"].lower() or "not allowed" in result["error"].lower()
    
    @pytest.mark.asyncio
    async def test_concurrent_execution(self, task_runner_config):
        """Test concurrent task execution."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Execute multiple tasks concurrently
        tasks = []
        for i in range(5):
            task = runner.execute(
                code=f"result = {i} * {i}",
                context={},
                timeout=10,
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # All should succeed
        for i, result in enumerate(results):
            assert result["success"] is True
            assert result["result"] == i * i
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, task_runner_config):
        """Test graceful shutdown of task runner."""
        runner = TaskRunner(config=task_runner_config)
        await runner.start()
        
        # Submit a long-running task
        task = asyncio.create_task(
            runner.execute(
                code="import time; time.sleep(2); result = 42",
                context={},
                timeout=10,
            )
        )
        
        # Give it time to start
        await asyncio.sleep(0.1)
        
        # Shutdown runner
        await runner.shutdown(timeout=5)
        
        assert not runner.is_running
        
        # Task should be cancelled or completed
        if not task.done():
            task.cancel()


@pytest.mark.integration
class TestTaskRunnerIntegration:
    """Integration tests for task runner."""
    
    @pytest.mark.asyncio
    async def test_real_process_execution(self):
        """Test real process execution (requires working environment)."""
        config = TaskRunnerConfig(
            mode=TaskRunnerMode.INTERNAL,
            max_concurrency=1,
            task_timeout=10,
            enable_sandboxing=True,
        )
        
        runner = TaskRunner(config=config)
        await runner.start()
        
        try:
            # Test mathematical computation
            result = await runner.execute(
                code="""
# Calculate fibonacci
def fib(n):
    if n <= 1:
        return n
    return fib(n-1) + fib(n-2)

result = [fib(i) for i in range(10)]
                """,
                context={},
                timeout=5,
            )
            
            assert result["success"] is True
            assert result["result"] == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
            
        finally:
            await runner.shutdown()
    
    @pytest.mark.asyncio
    async def test_resource_limit_enforcement(self):
        """Test resource limit enforcement in real execution."""
        config = TaskRunnerConfig(
            mode=TaskRunnerMode.INTERNAL,
            max_memory_mb=128,  # Low memory limit
            task_timeout=5,
            enable_sandboxing=True,
        )
        
        runner = TaskRunner(config=config)
        await runner.start()
        
        try:
            # Try to allocate too much memory
            result = await runner.execute(
                code="""
# Try to allocate large list
large_list = []
for i in range(10000000):
    large_list.append("x" * 1000)
result = len(large_list)
                """,
                context={},
                timeout=5,
            )
            
            # Should fail due to memory limit
            assert result["success"] is False
            assert "memory" in result["error"].lower() or "resource" in result["error"].lower()
            
        finally:
            await runner.shutdown()