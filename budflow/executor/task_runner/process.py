"""Task runner process implementation."""

import asyncio
import io
import logging
import multiprocessing
import os
import queue
import signal
import sys
import time
import traceback
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
from multiprocessing import Process, Queue, Event
from typing import Optional, Dict, Any, Set, Callable

from .config import TaskRunnerConfig, SecurityConfig
from .models import (
    TaskRequest, TaskResult, TaskStatus, TaskOffer,
    HeartbeatMessage, TaskAcceptedMessage, TaskDoneMessage,
    TaskErrorMessage, TaskProgressMessage,
)
from .exceptions import (
    TaskRunnerError, TaskTimeoutError, ResourceLimitError,
    SecurityViolationError, ProcessCommunicationError,
)
from .sandbox import SecuritySandbox
from .resource_limiter import ResourceLimitEnforcer, PayloadSizeChecker

logger = logging.getLogger(__name__)


class TaskRunnerProcess:
    """Isolated process for task execution."""
    
    def __init__(
        self,
        runner_id: str,
        config: TaskRunnerConfig,
        task_queue: Optional[Queue] = None,
        result_queue: Optional[Queue] = None,
        control_queue: Optional[Queue] = None,
    ):
        self.runner_id = runner_id
        self.config = config
        self._task_queue = task_queue or Queue()
        self._result_queue = result_queue or Queue()
        self._control_queue = control_queue or Queue()
        
        self._process: Optional[Process] = None
        self._running_tasks: Set[str] = set()
        self._shutdown_event = Event()
        self.is_running = False
        self.last_heartbeat = datetime.now(timezone.utc)
        
        # Create security sandbox
        self._sandbox = SecuritySandbox(config.get_security_config())
        
        # Resource enforcement
        self._resource_limiter: Optional[ResourceLimitEnforcer] = None
    
    async def start(self) -> None:
        """Start the runner process."""
        if self.is_running:
            return
        
        # Create process with security flags
        process_args = []
        if self.config.get_security_config().disallow_code_generation:
            # These flags would be passed to the Python interpreter
            # In practice, we'd need to spawn a subprocess with these flags
            process_args.extend([
                "--disallow-code-generation-from-strings",
            ])
        
        # Start the process
        self._process = Process(
            target=self._run_process,
            args=(
                self.runner_id,
                self.config,
                self._task_queue,
                self._result_queue,
                self._control_queue,
                self._shutdown_event,
            ),
            daemon=True,
        )
        self._process.start()
        self.is_running = True
        
        # Start monitoring tasks
        asyncio.create_task(self._monitor_process())
        asyncio.create_task(self._heartbeat_loop())
    
    async def shutdown(self, timeout: int = 30) -> None:
        """Gracefully shutdown the runner."""
        if not self.is_running:
            return
        
        logger.info(f"Shutting down runner {self.runner_id}")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Wait for running tasks to complete
        start_time = time.time()
        while self._running_tasks and (time.time() - start_time) < timeout:
            await asyncio.sleep(0.1)
        
        # Force terminate if still running
        if self._process and self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=5)
            
            if self._process.is_alive():
                self._process.kill()
                self._process.join(timeout=5)
        
        self.is_running = False
    
    async def execute_task(self, request: TaskRequest) -> TaskResult:
        """Execute a task in the runner process."""
        if not self.is_running:
            raise TaskRunnerError(f"Runner {self.runner_id} is not running")
        
        # Check payload size
        PayloadSizeChecker.check_size(
            request.context,
            self.config.max_payload_size_mb
        )
        
        # Add to running tasks
        self._running_tasks.add(request.task_id)
        
        try:
            # Send task to process
            self._task_queue.put(request.to_dict(), timeout=self.config.ipc_timeout)
            
            # Wait for result
            start_time = time.time()
            timeout = request.timeout or self.config.task_timeout
            
            while True:
                try:
                    result_data = self._result_queue.get(timeout=1)
                    if result_data["task_id"] == request.task_id:
                        return TaskResult.from_dict(result_data)
                except queue.Empty:
                    # Check timeout
                    if time.time() - start_time > timeout:
                        raise TaskTimeoutError(
                            f"Task {request.task_id} timed out",
                            task_id=request.task_id,
                            timeout_seconds=timeout
                        )
                
        finally:
            self._running_tasks.discard(request.task_id)
    
    async def create_offer(self) -> TaskOffer:
        """Create a task offer."""
        # Get current resource usage
        if self._resource_limiter:
            usage = self._resource_limiter.get_current_usage()
        else:
            usage = {}
        
        # Calculate available resources
        available_memory = None
        if usage.get("memory_mb") is not None:
            available_memory = self.config.max_memory_mb - usage["memory_mb"]
        
        available_cpu = None
        if usage.get("cpu_percent") is not None:
            available_cpu = self.config.max_cpu_percent - usage["cpu_percent"]
        
        return TaskOffer(
            runner_id=self.runner_id,
            capabilities=["python", "restricted"] if self.config.enable_sandboxing else ["python"],
            available_memory_mb=available_memory,
            available_cpu_percent=available_cpu,
        )
    
    async def _monitor_process(self) -> None:
        """Monitor the process health."""
        while self.is_running:
            if self._process and not self._process.is_alive():
                logger.error(f"Runner process {self.runner_id} died unexpectedly")
                self.is_running = False
                
                # Check exit code
                if self._process.exitcode != 0:
                    logger.error(f"Process exited with code {self._process.exitcode}")
                
                # Restart if configured
                if self.config.restart_on_crash:
                    await self._restart_process()
            
            await asyncio.sleep(1)
    
    async def _restart_process(self) -> None:
        """Restart the process after crash."""
        logger.info(f"Attempting to restart runner {self.runner_id}")
        
        # Clear state
        self._running_tasks.clear()
        self._shutdown_event.clear()
        
        # Wait before restart
        await asyncio.sleep(self.config.restart_delay_seconds)
        
        # Start again
        await self.start()
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self.is_running:
            self.last_heartbeat = datetime.now(timezone.utc)
            
            # Send heartbeat message
            heartbeat = HeartbeatMessage(
                runner_id=self.runner_id,
                running_tasks=list(self._running_tasks),
                resource_usage=self._get_resource_usage(),
            )
            
            try:
                self._control_queue.put(heartbeat.to_dict(), timeout=1)
            except queue.Full:
                logger.warning(f"Control queue full for runner {self.runner_id}")
            
            await asyncio.sleep(self.config.heartbeat_interval)
    
    def _get_resource_usage(self) -> Dict[str, Any]:
        """Get current resource usage."""
        if self._resource_limiter:
            return self._resource_limiter.get_current_usage()
        return {}
    
    @staticmethod
    def _run_process(
        runner_id: str,
        config: TaskRunnerConfig,
        task_queue: Queue,
        result_queue: Queue,
        control_queue: Queue,
        shutdown_event: Event,
    ) -> None:
        """Main process loop."""
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format=f"[{runner_id}] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        logger = logging.getLogger(__name__)
        logger.info(f"Runner process {runner_id} started")
        
        # Create sandbox and resource limiter
        sandbox = SecuritySandbox(config.get_security_config())
        resource_limiter = ResourceLimitEnforcer(config.get_resource_limits())
        
        # Apply system resource limits
        resource_limiter.apply_system_limits()
        
        # Signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}")
            shutdown_event.set()
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Main loop
        while not shutdown_event.is_set():
            try:
                # Get task with timeout
                task_data = task_queue.get(timeout=1)
                request = TaskRequest.from_dict(task_data)
                
                # Execute task
                result = _execute_task_in_process(
                    request,
                    sandbox,
                    resource_limiter,
                    config
                )
                
                # Send result
                result_queue.put(result.to_dict())
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in process loop: {e}", exc_info=True)
        
        # Restore resource limits
        resource_limiter.restore_system_limits()
        
        logger.info(f"Runner process {runner_id} shutting down")


def _execute_task_in_process(
    request: TaskRequest,
    sandbox: SecuritySandbox,
    resource_limiter: ResourceLimitEnforcer,
    config: TaskRunnerConfig
) -> TaskResult:
    """Execute a task in the current process."""
    start_time = time.time()
    resource_limiter.start_execution()
    
    # Capture output
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    logs = []
    
    # Custom print function that captures output
    def captured_print(*args, **kwargs):
        output = io.StringIO()
        print(*args, **kwargs, file=output)
        message = output.getvalue()
        stdout_buffer.write(message)
        logs.append(message.rstrip())
    
    try:
        # Compile code with restrictions
        compiled_code = sandbox.compile_restricted(request.code)
        
        # Create execution context
        context = request.context.copy()
        context["print"] = captured_print
        context["result"] = None  # This will hold the result
        
        # Create safe globals
        safe_globals = sandbox.create_safe_globals(context)
        
        # Set up timeout
        timeout_triggered = False
        
        def timeout_handler():
            nonlocal timeout_triggered
            timeout_triggered = True
            raise TaskTimeoutError(
                f"Task execution timed out",
                task_id=request.task_id,
                timeout_seconds=request.timeout or config.task_timeout
            )
        
        # Use signal-based timeout on Unix
        if hasattr(signal, 'SIGALRM'):
            old_handler = signal.signal(signal.SIGALRM, lambda s, f: timeout_handler())
            signal.alarm(request.timeout or config.task_timeout)
        
        # Execute code
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exec(compiled_code, safe_globals, safe_globals)
        
        # Clear timeout
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
        
        # Get result
        result_value = safe_globals.get("result")
        
        # Calculate execution time
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Get resource usage
        resource_usage = resource_limiter.get_current_usage()
        
        return TaskResult(
            task_id=request.task_id,
            status=TaskStatus.COMPLETED,
            result=result_value,
            execution_time_ms=execution_time_ms,
            resource_usage=resource_usage,
            logs=logs,
        )
        
    except TaskTimeoutError as e:
        return TaskResult(
            task_id=request.task_id,
            status=TaskStatus.TIMEOUT,
            error=str(e),
            error_details={"timeout_seconds": e.timeout_seconds},
            logs=logs,
        )
        
    except ResourceLimitError as e:
        return TaskResult(
            task_id=request.task_id,
            status=TaskStatus.RESOURCE_LIMIT,
            error=str(e),
            error_details=e.details,
            logs=logs,
        )
        
    except SecurityViolationError as e:
        return TaskResult(
            task_id=request.task_id,
            status=TaskStatus.SECURITY_VIOLATION,
            error=str(e),
            error_details=e.details,
            logs=logs,
        )
        
    except Exception as e:
        # Get traceback
        tb = traceback.format_exc()
        
        return TaskResult(
            task_id=request.task_id,
            status=TaskStatus.FAILED,
            error=str(e),
            error_details={
                "type": type(e).__name__,
                "traceback": tb,
            },
            logs=logs,
        )
        
    finally:
        # Clear any remaining timeouts
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)