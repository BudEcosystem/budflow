"""Main task runner interface."""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from uuid import uuid4

from .config import TaskRunnerConfig, TaskRunnerMode
from .models import TaskRequest, TaskResult, TaskStatus
from .broker import TaskBroker
from .exceptions import TaskRunnerError

logger = logging.getLogger(__name__)


class TaskRunner:
    """Main interface for task execution."""
    
    def __init__(self, config: Optional[TaskRunnerConfig] = None):
        self.config = config or TaskRunnerConfig()
        self.config.validate()
        
        self.broker: Optional[TaskBroker] = None
        self.is_running = False
        self._runner_ids: List[str] = []
    
    async def start(self) -> None:
        """Start the task runner."""
        if self.is_running:
            return
        
        logger.info("Starting task runner")
        
        # Create and start broker
        self.broker = TaskBroker(self.config)
        await self.broker.start()
        
        # Start runners based on mode
        if self.config.mode == TaskRunnerMode.INTERNAL:
            # Start internal runner processes
            for i in range(min(self.config.max_runners, 4)):  # Limit for safety
                runner_id = await self.broker.register_runner()
                self._runner_ids.append(runner_id)
                logger.info(f"Started internal runner {runner_id}")
        
        elif self.config.mode == TaskRunnerMode.EXTERNAL:
            # External runners will register themselves
            logger.info("Waiting for external runners to register")
        
        elif self.config.mode == TaskRunnerMode.DOCKER:
            # Docker mode would spawn containers
            logger.warning("Docker mode not yet implemented")
            # TODO: Implement Docker-based runners
        
        self.is_running = True
        logger.info("Task runner started")
    
    async def shutdown(self, timeout: int = 30) -> None:
        """Shutdown the task runner."""
        if not self.is_running:
            return
        
        logger.info("Shutting down task runner")
        
        # Stop broker
        if self.broker:
            await self.broker.stop()
        
        self.is_running = False
        self._runner_ids.clear()
        
        logger.info("Task runner shutdown complete")
    
    async def execute(
        self,
        code: str,
        context: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        required_modules: Optional[List[str]] = None,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute code in an isolated environment.
        
        Args:
            code: Python code to execute
            context: Variables to make available in execution context
            timeout: Execution timeout in seconds
            required_modules: List of required modules
            task_id: Optional task ID
            
        Returns:
            Dictionary with execution results:
                - success: Whether execution succeeded
                - result: The value of 'result' variable if success
                - error: Error message if failed
                - logs: List of print outputs
                - execution_time_ms: Execution time in milliseconds
                - resource_usage: Resource usage statistics
        """
        if not self.is_running:
            raise TaskRunnerError("Task runner is not running")
        
        if not self.broker:
            raise TaskRunnerError("Broker not initialized")
        
        # Create task request
        request = TaskRequest(
            task_id=task_id or str(uuid4()),
            code=code,
            context=context or {},
            required_modules=required_modules or [],
            timeout=timeout,
        )
        
        try:
            # Submit task
            submitted = await self.broker.submit_task(request)
            
            if not submitted:
                logger.debug(f"Task {request.task_id} queued for execution")
            
            # Wait for result
            result = await self.broker.wait_for_task(
                request.task_id,
                timeout=timeout or self.config.task_timeout
            )
            
            # Convert to response format
            return self._format_result(result)
            
        except Exception as e:
            logger.error(f"Error executing task {request.task_id}: {e}")
            
            # Return error response
            return {
                "success": False,
                "result": None,
                "error": str(e),
                "error_type": type(e).__name__,
                "logs": [],
                "execution_time_ms": None,
                "resource_usage": None,
            }
    
    async def execute_batch(
        self,
        tasks: List[Dict[str, Any]],
        max_concurrent: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple tasks concurrently.
        
        Args:
            tasks: List of task dictionaries with code, context, etc.
            max_concurrent: Maximum concurrent executions
            
        Returns:
            List of results in same order as tasks
        """
        if not tasks:
            return []
        
        max_concurrent = max_concurrent or self.config.max_concurrency
        
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def execute_with_semaphore(task_dict: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                return await self.execute(**task_dict)
        
        # Execute all tasks
        results = await asyncio.gather(
            *[execute_with_semaphore(task) for task in tasks],
            return_exceptions=True
        )
        
        # Handle exceptions in results
        formatted_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                formatted_results.append({
                    "success": False,
                    "result": None,
                    "error": str(result),
                    "error_type": type(result).__name__,
                    "logs": [],
                    "execution_time_ms": None,
                    "resource_usage": None,
                })
            else:
                formatted_results.append(result)
        
        return formatted_results
    
    def _format_result(self, result: TaskResult) -> Dict[str, Any]:
        """Format task result for response."""
        return {
            "success": result.is_success,
            "result": result.result,
            "error": result.error,
            "error_type": result.error_details.get("type") if result.error_details else None,
            "logs": result.logs,
            "execution_time_ms": result.execution_time_ms,
            "resource_usage": result.resource_usage,
            "status": result.status.value,
        }
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get task runner statistics."""
        stats = {
            "is_running": self.is_running,
            "mode": self.config.mode.value,
            "internal_runners": len(self._runner_ids),
        }
        
        if self.broker:
            stats["broker"] = self.broker.get_stats()
        
        return stats
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()


# Convenience function for simple code execution
async def execute_code(
    code: str,
    context: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = None,
    config: Optional[TaskRunnerConfig] = None,
) -> Dict[str, Any]:
    """
    Execute code in an isolated environment.
    
    This is a convenience function that creates a temporary task runner,
    executes the code, and cleans up.
    
    Args:
        code: Python code to execute
        context: Variables to make available in execution context
        timeout: Execution timeout in seconds
        config: Optional task runner configuration
        
    Returns:
        Execution result dictionary
    """
    async with TaskRunner(config) as runner:
        return await runner.execute(code, context, timeout)