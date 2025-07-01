"""Task broker for managing task distribution."""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from uuid import uuid4

from .config import TaskRunnerConfig
from .models import (
    TaskRequest, TaskResult, TaskStatus, TaskOffer,
    TaskOfferAcceptMessage, TaskAcceptedMessage,
    TaskDoneMessage, MessageType,
)
from .exceptions import RunnerNotAvailableError, TaskRunnerError
from .process import TaskRunnerProcess

logger = logging.getLogger(__name__)


class PendingRequest:
    """Pending task request waiting for a runner."""
    
    def __init__(self, request: TaskRequest, future: asyncio.Future):
        self.request = request
        self.future = future
        self.created_at = datetime.now(timezone.utc)
        self.assigned_runner: Optional[str] = None
        self.assigned_offer: Optional[str] = None


class TaskBroker:
    """Broker for managing task distribution between runners."""
    
    def __init__(self, config: TaskRunnerConfig):
        self.config = config
        self._runners: Dict[str, TaskRunnerProcess] = {}
        self._active_offers: Dict[str, TaskOffer] = {}
        self._pending_requests: Dict[str, PendingRequest] = {}
        self._task_assignments: Dict[str, str] = {}  # task_id -> runner_id
        self._runner_load: Dict[str, int] = {}  # runner_id -> active task count
        
        self.is_running = False
        self._match_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._health_check_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the broker."""
        if self.is_running:
            return
        
        self.is_running = True
        
        # Start background tasks
        self._match_task = asyncio.create_task(self._match_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info("Task broker started")
    
    async def stop(self) -> None:
        """Stop the broker."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Cancel background tasks
        for task in [self._match_task, self._cleanup_task, self._health_check_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Shutdown all runners
        shutdown_tasks = []
        for runner in self._runners.values():
            shutdown_tasks.append(runner.shutdown(timeout=self.config.shutdown_timeout))
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        # Fail pending requests
        for pending in self._pending_requests.values():
            if not pending.future.done():
                pending.future.set_exception(
                    TaskRunnerError("Broker shutdown", task_id=pending.request.task_id)
                )
        
        self._runners.clear()
        self._pending_requests.clear()
        self._active_offers.clear()
        
        logger.info("Task broker stopped")
    
    async def register_runner(self, runner_id: Optional[str] = None) -> str:
        """Register a new runner."""
        if not runner_id:
            runner_id = f"runner-{uuid4().hex[:8]}"
        
        if runner_id in self._runners:
            raise TaskRunnerError(f"Runner {runner_id} already registered")
        
        # Create runner process
        runner = TaskRunnerProcess(runner_id, self.config)
        await runner.start()
        
        self._runners[runner_id] = runner
        self._runner_load[runner_id] = 0
        
        logger.info(f"Registered runner {runner_id}")
        
        # Start creating offers for this runner
        asyncio.create_task(self._create_offers_for_runner(runner_id))
        
        return runner_id
    
    async def unregister_runner(self, runner_id: str) -> None:
        """Unregister a runner."""
        if runner_id not in self._runners:
            return
        
        runner = self._runners[runner_id]
        await runner.shutdown(timeout=self.config.shutdown_timeout)
        
        # Remove runner
        del self._runners[runner_id]
        self._runner_load.pop(runner_id, None)
        
        # Remove offers from this runner
        offers_to_remove = [
            offer_id for offer_id, offer in self._active_offers.items()
            if offer.runner_id == runner_id
        ]
        for offer_id in offers_to_remove:
            del self._active_offers[offer_id]
        
        # Fail tasks assigned to this runner
        tasks_to_fail = [
            task_id for task_id, assigned_runner in self._task_assignments.items()
            if assigned_runner == runner_id
        ]
        for task_id in tasks_to_fail:
            if task_id in self._pending_requests:
                pending = self._pending_requests[task_id]
                if not pending.future.done():
                    pending.future.set_exception(
                        TaskRunnerError(f"Runner {runner_id} unregistered", task_id=task_id)
                    )
        
        logger.info(f"Unregistered runner {runner_id}")
    
    async def submit_task(self, request: TaskRequest) -> bool:
        """Submit a task for execution."""
        # Create pending request
        future = asyncio.Future()
        pending = PendingRequest(request, future)
        self._pending_requests[request.task_id] = pending
        
        # Try to match immediately
        matched = await self._try_match_request(pending)
        
        if not matched:
            logger.debug(f"No immediate match for task {request.task_id}, queued")
        
        return matched
    
    async def wait_for_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult:
        """Wait for a task to complete."""
        if task_id not in self._pending_requests:
            raise TaskRunnerError(f"Unknown task {task_id}")
        
        pending = self._pending_requests[task_id]
        
        try:
            # Wait for result
            if timeout:
                result = await asyncio.wait_for(pending.future, timeout=timeout)
            else:
                result = await pending.future
            
            return result
            
        finally:
            # Cleanup
            self._pending_requests.pop(task_id, None)
            self._task_assignments.pop(task_id, None)
            
            # Update runner load
            if pending.assigned_runner:
                self._runner_load[pending.assigned_runner] = max(
                    0, self._runner_load.get(pending.assigned_runner, 0) - 1
                )
    
    async def submit_offer(self, offer: TaskOffer) -> None:
        """Submit a task offer from a runner."""
        if offer.runner_id not in self._runners:
            logger.warning(f"Offer from unknown runner {offer.runner_id}")
            return
        
        if not offer.is_valid():
            logger.debug(f"Expired offer {offer.offer_id}")
            return
        
        self._active_offers[offer.offer_id] = offer
        
        # Try to match with pending requests
        await self._try_match_offer(offer)
    
    async def _create_offers_for_runner(self, runner_id: str) -> None:
        """Continuously create offers for a runner."""
        while self.is_running and runner_id in self._runners:
            runner = self._runners[runner_id]
            
            if not runner.is_running:
                await asyncio.sleep(1)
                continue
            
            # Calculate how many offers to create
            current_load = self._runner_load.get(runner_id, 0)
            active_offers = sum(
                1 for offer in self._active_offers.values()
                if offer.runner_id == runner_id and offer.is_valid()
            )
            
            offers_to_create = self.config.max_concurrency - current_load - active_offers
            
            # Create offers
            for _ in range(offers_to_create):
                try:
                    offer = await runner.create_offer()
                    await self.submit_offer(offer)
                except Exception as e:
                    logger.error(f"Error creating offer for runner {runner_id}: {e}")
            
            await asyncio.sleep(1)
    
    async def _try_match_request(self, pending: PendingRequest) -> bool:
        """Try to match a request with available offers."""
        # Find suitable offers
        suitable_offers = []
        
        for offer_id, offer in self._active_offers.items():
            if not offer.is_valid():
                continue
            
            # Check if runner is available
            runner = self._runners.get(offer.runner_id)
            if not runner or not runner.is_running:
                continue
            
            # Check runner load
            current_load = self._runner_load.get(offer.runner_id, 0)
            if current_load >= self.config.max_concurrency:
                continue
            
            suitable_offers.append((offer_id, offer))
        
        if not suitable_offers:
            return False
        
        # Select best offer (load balancing)
        best_offer_id, best_offer = min(
            suitable_offers,
            key=lambda x: self._runner_load.get(x[1].runner_id, 0)
        )
        
        # Assign task
        return await self._assign_task(pending, best_offer_id, best_offer)
    
    async def _try_match_offer(self, offer: TaskOffer) -> bool:
        """Try to match an offer with pending requests."""
        # Find suitable requests
        for task_id, pending in list(self._pending_requests.items()):
            if pending.assigned_runner:
                continue
            
            # Try to assign
            if await self._assign_task(pending, offer.offer_id, offer):
                return True
        
        return False
    
    async def _assign_task(
        self,
        pending: PendingRequest,
        offer_id: str,
        offer: TaskOffer
    ) -> bool:
        """Assign a task to a runner."""
        runner = self._runners.get(offer.runner_id)
        if not runner:
            return False
        
        try:
            # Remove offer
            self._active_offers.pop(offer_id, None)
            
            # Update assignment
            pending.assigned_runner = offer.runner_id
            pending.assigned_offer = offer_id
            self._task_assignments[pending.request.task_id] = offer.runner_id
            self._runner_load[offer.runner_id] = self._runner_load.get(offer.runner_id, 0) + 1
            
            # Execute task
            logger.debug(f"Assigning task {pending.request.task_id} to runner {offer.runner_id}")
            
            # Create task to execute
            asyncio.create_task(self._execute_assigned_task(pending, runner))
            
            return True
            
        except Exception as e:
            logger.error(f"Error assigning task: {e}")
            
            # Rollback
            pending.assigned_runner = None
            pending.assigned_offer = None
            self._task_assignments.pop(pending.request.task_id, None)
            self._runner_load[offer.runner_id] = max(
                0, self._runner_load.get(offer.runner_id, 0) - 1
            )
            
            return False
    
    async def _execute_assigned_task(
        self,
        pending: PendingRequest,
        runner: TaskRunnerProcess
    ) -> None:
        """Execute an assigned task."""
        try:
            result = await runner.execute_task(pending.request)
            if not pending.future.done():
                pending.future.set_result(result)
        except Exception as e:
            if not pending.future.done():
                pending.future.set_exception(e)
    
    async def _match_loop(self) -> None:
        """Background task to match requests with offers."""
        while self.is_running:
            try:
                # Try to match pending requests
                for task_id, pending in list(self._pending_requests.items()):
                    if pending.assigned_runner:
                        continue
                    
                    await self._try_match_request(pending)
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error in match loop: {e}")
                await asyncio.sleep(1)
    
    async def _cleanup_loop(self) -> None:
        """Background task to cleanup expired offers and requests."""
        while self.is_running:
            try:
                now = datetime.now(timezone.utc)
                
                # Remove expired offers
                expired_offers = [
                    offer_id for offer_id, offer in self._active_offers.items()
                    if not offer.is_valid()
                ]
                for offer_id in expired_offers:
                    del self._active_offers[offer_id]
                
                # Timeout old pending requests
                timeout_threshold = now - timedelta(seconds=self.config.task_timeout * 2)
                
                for task_id, pending in list(self._pending_requests.items()):
                    if pending.created_at < timeout_threshold and not pending.assigned_runner:
                        if not pending.future.done():
                            pending.future.set_exception(
                                RunnerNotAvailableError(
                                    f"No runner available for task {task_id}",
                                    task_id=task_id,
                                    wait_time=(now - pending.created_at).total_seconds()
                                )
                            )
                        del self._pending_requests[task_id]
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(10)
    
    async def _health_check_loop(self) -> None:
        """Background task to check runner health."""
        while self.is_running:
            try:
                await self._health_check_loop_once()
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(self.config.heartbeat_interval)
    
    async def _health_check_loop_once(self) -> None:
        """Perform one health check iteration."""
        now = datetime.now(timezone.utc)
        timeout_threshold = now - timedelta(seconds=self.config.heartbeat_interval * 2)
        
        for runner_id, runner in list(self._runners.items()):
            # Check if runner is responsive
            if runner.last_heartbeat < timeout_threshold:
                logger.warning(f"Runner {runner_id} is unresponsive")
                
                # Restart if configured
                if self.config.restart_on_crash and runner.is_running:
                    logger.info(f"Restarting unresponsive runner {runner_id}")
                    await runner.shutdown(timeout=5)
                    await runner.start()
                elif not runner.is_running:
                    # Remove dead runner
                    await self.unregister_runner(runner_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get broker statistics."""
        return {
            "runners": len(self._runners),
            "active_runners": sum(1 for r in self._runners.values() if r.is_running),
            "active_offers": len(self._active_offers),
            "pending_requests": len(self._pending_requests),
            "total_load": sum(self._runner_load.values()),
            "runner_load": dict(self._runner_load),
        }