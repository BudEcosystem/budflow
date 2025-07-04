"""Real-time progress tracking for BudFlow workflow executions."""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List, Set, Optional
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload

from ..workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode


class ProgressEventType(str, Enum):
    """Types of progress events."""
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"
    NODE_STARTED = "node_started"
    NODE_COMPLETED = "node_completed"
    ERROR = "error"
    PROGRESS_UPDATE = "progress_update"


@dataclass
class ProgressData:
    """Represents current execution progress data."""
    execution_id: str
    progress_percentage: float
    current_node: Optional[str]
    completed_nodes: List[str]
    total_nodes: int
    status: ExecutionStatus
    estimated_remaining_time: Optional[timedelta] = None
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "execution_id": self.execution_id,
            "progress_percentage": self.progress_percentage,
            "current_node": self.current_node,
            "completed_nodes": self.completed_nodes,
            "total_nodes": self.total_nodes,
            "status": self.status.value if isinstance(self.status, ExecutionStatus) else self.status,
            "estimated_remaining_time": self.estimated_remaining_time.total_seconds() if self.estimated_remaining_time else None,
            "last_updated": self.last_updated.isoformat()
        }


@dataclass
class ProgressEvent:
    """Represents a progress event during execution."""
    execution_id: str
    event_type: ProgressEventType
    timestamp: datetime
    node_name: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid4()))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "event_id": self.event_id,
            "execution_id": self.execution_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "node_name": self.node_name,
            "data": self.data
        }


@dataclass
class ProgressSubscription:
    """Represents a WebSocket subscription to progress updates."""
    execution_id: str
    user_id: str
    websocket: Any  # WebSocket connection
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    subscription_id: str = field(default_factory=lambda: str(uuid4()))
    
    def update_activity(self):
        """Update last activity timestamp."""
        self.last_activity = datetime.now(timezone.utc)
    
    def is_stale(self, max_idle_seconds: int = 300) -> bool:
        """Check if subscription is stale (no activity for too long)."""
        idle_time = datetime.now(timezone.utc) - self.last_activity
        return idle_time.total_seconds() > max_idle_seconds


class ProgressNotFoundError(Exception):
    """Raised when progress data is not found."""
    pass


class InvalidProgressDataError(Exception):
    """Raised when progress data is invalid."""
    pass


class ProgressCalculator:
    """Calculates execution progress and estimates remaining time."""
    
    async def calculate_progress(
        self, 
        execution: WorkflowExecution, 
        workflow: Workflow
    ) -> ProgressData:
        """Calculate current execution progress."""
        total_nodes = len(workflow.nodes) if workflow.nodes else 0
        completed_nodes = list(execution.data.keys()) if execution.data else []
        progress_percentage = (len(completed_nodes) / total_nodes * 100) if total_nodes > 0 else 0.0
        
        # Determine current node
        current_node = None
        if execution.status == ExecutionStatus.RUNNING:
            # For simplicity, assume current_node is stored on execution
            current_node = getattr(execution, 'current_node', None)
        
        # Calculate estimated remaining time
        estimated_time = await self.estimate_remaining_time(execution, workflow)
        
        return ProgressData(
            execution_id=str(execution.id),
            progress_percentage=progress_percentage,
            current_node=current_node,
            completed_nodes=completed_nodes,
            total_nodes=total_nodes,
            status=execution.status,
            estimated_remaining_time=estimated_time
        )
    
    async def estimate_remaining_time(
        self, 
        execution: WorkflowExecution, 
        workflow: Workflow
    ) -> timedelta:
        """Estimate remaining execution time."""
        if execution.status in (ExecutionStatus.SUCCESS, ExecutionStatus.ERROR):
            return timedelta(0)
        
        total_nodes = len(workflow.nodes) if workflow.nodes else 1
        completed_nodes = len(execution.data.keys()) if execution.data else 0
        
        if execution.status == ExecutionStatus.NEW:
            # Estimate based on average node execution time (assume 30 seconds per node)
            return timedelta(seconds=total_nodes * 30)
        
        if execution.started_at and completed_nodes > 0:
            # Calculate based on current progress rate
            elapsed_time = datetime.now(timezone.utc) - execution.started_at
            avg_time_per_node = elapsed_time / completed_nodes
            remaining_nodes = total_nodes - completed_nodes
            return avg_time_per_node * remaining_nodes
        
        # Fallback estimate
        remaining_nodes = total_nodes - completed_nodes
        return timedelta(seconds=remaining_nodes * 30)


class WebSocketManager:
    """Manages WebSocket connections for real-time progress updates."""
    
    def __init__(self):
        self._subscriptions: Dict[str, List[ProgressSubscription]] = {}
        self._websocket_to_subscription: Dict[Any, ProgressSubscription] = {}
    
    async def connect_client(
        self, 
        websocket: Any, 
        execution_id: str, 
        user_id: str
    ) -> ProgressSubscription:
        """Connect a client to receive progress updates."""
        subscription = ProgressSubscription(
            execution_id=execution_id,
            user_id=user_id,
            websocket=websocket
        )
        
        # Add to subscriptions
        if execution_id not in self._subscriptions:
            self._subscriptions[execution_id] = []
        self._subscriptions[execution_id].append(subscription)
        
        # Track websocket mapping
        self._websocket_to_subscription[websocket] = subscription
        
        return subscription
    
    async def disconnect_client(self, websocket: Any):
        """Disconnect a client from progress updates."""
        if websocket in self._websocket_to_subscription:
            subscription = self._websocket_to_subscription[websocket]
            execution_id = subscription.execution_id
            
            # Remove from subscriptions
            if execution_id in self._subscriptions:
                self._subscriptions[execution_id] = [
                    sub for sub in self._subscriptions[execution_id] 
                    if sub.websocket != websocket
                ]
                
                # Clean up empty subscription lists
                if not self._subscriptions[execution_id]:
                    del self._subscriptions[execution_id]
            
            # Remove websocket mapping
            del self._websocket_to_subscription[websocket]
            
            # Close websocket
            try:
                await websocket.close()
            except:
                pass  # Ignore errors during close
    
    async def broadcast_progress(self, execution_id: str, progress_data: ProgressData):
        """Broadcast progress update to all connected clients for execution."""
        if execution_id not in self._subscriptions:
            return
        
        message = json.dumps(progress_data.to_dict())
        disconnected_websockets = []
        
        for subscription in self._subscriptions[execution_id]:
            try:
                await subscription.websocket.send_text(message)
                subscription.update_activity()
            except Exception:
                # Connection is broken, mark for removal
                disconnected_websockets.append(subscription.websocket)
        
        # Clean up broken connections
        for websocket in disconnected_websockets:
            await self.disconnect_client(websocket)
    
    def get_connected_clients_count(self, execution_id: str) -> int:
        """Get number of connected clients for execution."""
        if execution_id not in self._subscriptions:
            return 0
        return len(self._subscriptions[execution_id])
    
    async def cleanup_stale_connections(self, max_idle_seconds: int = 300):
        """Clean up stale WebSocket connections."""
        stale_websockets = []
        
        for subscription in self._websocket_to_subscription.values():
            if subscription.is_stale(max_idle_seconds):
                stale_websockets.append(subscription.websocket)
        
        for websocket in stale_websockets:
            await self.disconnect_client(websocket)


class ExecutionProgressManager:
    """Manages progress tracking for a single execution."""
    
    def __init__(self, execution_id: str, websocket_manager: WebSocketManager):
        self.execution_id = execution_id
        self.websocket_manager = websocket_manager
        self.progress_events: List[ProgressEvent] = []
        self.current_progress: Optional[ProgressData] = None
    
    async def add_event(self, event: ProgressEvent):
        """Add a progress event."""
        self.progress_events.append(event)
        
        # Broadcast event to connected clients
        event_message = json.dumps(event.to_dict())
        if self.execution_id in self.websocket_manager._subscriptions:
            for subscription in self.websocket_manager._subscriptions[self.execution_id]:
                try:
                    await subscription.websocket.send_text(event_message)
                    subscription.update_activity()
                except Exception:
                    # Will be cleaned up by broadcast_progress
                    pass
    
    async def update_progress(self, progress_data: ProgressData):
        """Update current progress and broadcast to clients."""
        self.current_progress = progress_data
        await self.websocket_manager.broadcast_progress(self.execution_id, progress_data)


class ProgressTracker:
    """Main progress tracking engine."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.websocket_manager = WebSocketManager()
        self.progress_calculator = ProgressCalculator()
        self.execution_managers: Dict[str, ExecutionProgressManager] = {}
        
        # In-memory storage for testing (use database in production)
        self._progress_events: Dict[str, List[ProgressEvent]] = {}
        self._current_progress: Dict[str, ProgressData] = {}
    
    async def start_progress_tracking(
        self, 
        execution: WorkflowExecution, 
        workflow: Workflow
    ):
        """Start tracking progress for an execution."""
        execution_id = str(execution.id)
        
        # Create execution manager
        manager = ExecutionProgressManager(execution_id, self.websocket_manager)
        self.execution_managers[execution_id] = manager
        
        # Calculate initial progress
        initial_progress = await self.progress_calculator.calculate_progress(execution, workflow)
        self._current_progress[execution_id] = initial_progress
        
        # Create and store initial event
        start_event = await self.create_progress_event(
            execution_id=execution_id,
            event_type=ProgressEventType.EXECUTION_STARTED,
            data={
                "workflow_id": execution.workflow_id,
                "mode": execution.mode.value,
                "total_nodes": initial_progress.total_nodes
            }
        )
        await self.store_progress_event(start_event)
        
        # Broadcast initial progress
        await self.websocket_manager.broadcast_progress(execution_id, initial_progress)
    
    async def update_progress(
        self,
        execution: WorkflowExecution,
        workflow: Workflow,
        event_type: ProgressEventType,
        node_name: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None
    ):
        """Update progress for an execution."""
        execution_id = str(execution.id)
        
        # Create progress event
        event = await self.create_progress_event(
            execution_id=execution_id,
            event_type=event_type,
            node_name=node_name,
            data=data or {}
        )
        await self.store_progress_event(event)
        
        # Recalculate progress
        updated_progress = await self.progress_calculator.calculate_progress(execution, workflow)
        self._current_progress[execution_id] = updated_progress
        
        # Broadcast updated progress
        await self.websocket_manager.broadcast_progress(execution_id, updated_progress)
        
        # Add event to execution manager if exists
        if execution_id in self.execution_managers:
            await self.execution_managers[execution_id].add_event(event)
    
    async def get_execution_progress(
        self, 
        execution: WorkflowExecution, 
        workflow: Workflow
    ) -> ProgressData:
        """Get current progress for an execution."""
        execution_id = str(execution.id)
        
        # Return cached progress if available
        if execution_id in self._current_progress:
            return self._current_progress[execution_id]
        
        # Calculate fresh progress
        progress = await self.progress_calculator.calculate_progress(execution, workflow)
        self._current_progress[execution_id] = progress
        return progress
    
    async def create_progress_event(
        self,
        execution_id: str,
        event_type: ProgressEventType,
        node_name: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> ProgressEvent:
        """Create a progress event."""
        return ProgressEvent(
            execution_id=execution_id,
            event_type=event_type,
            timestamp=datetime.now(timezone.utc),
            node_name=node_name,
            data=data or {}
        )
    
    async def store_progress_event(self, event: ProgressEvent):
        """Store a progress event (in-memory for testing)."""
        execution_id = event.execution_id
        if execution_id not in self._progress_events:
            self._progress_events[execution_id] = []
        self._progress_events[execution_id].append(event)
    
    async def get_progress_events(self, execution_id: str) -> List[ProgressEvent]:
        """Get all progress events for an execution."""
        if execution_id not in self._progress_events:
            raise ProgressNotFoundError(f"Progress data not found for execution {execution_id}")
        return self._progress_events[execution_id]
    
    async def connect_websocket(
        self, 
        websocket: Any, 
        execution_id: str, 
        user_id: str
    ) -> ProgressSubscription:
        """Connect a WebSocket client to receive progress updates."""
        subscription = await self.websocket_manager.connect_client(websocket, execution_id, user_id)
        
        # Send current progress if available
        if execution_id in self._current_progress:
            current_progress = self._current_progress[execution_id]
            message = json.dumps(current_progress.to_dict())
            try:
                await websocket.send_text(message)
            except Exception:
                # Connection failed, remove it
                await self.websocket_manager.disconnect_client(websocket)
                raise
        
        return subscription
    
    async def disconnect_websocket(self, websocket: Any):
        """Disconnect a WebSocket client."""
        await self.websocket_manager.disconnect_client(websocket)
    
    async def cleanup_execution_tracking(self, execution_id: str):
        """Clean up tracking data for completed execution."""
        # Keep events but remove active tracking
        if execution_id in self.execution_managers:
            del self.execution_managers[execution_id]
        
        # Optionally clean up old progress data
        # In production, you might want to keep this for historical purposes
    
    async def get_active_executions(self) -> List[str]:
        """Get list of actively tracked execution IDs."""
        return list(self.execution_managers.keys())
    
    async def get_connected_clients_stats(self) -> Dict[str, int]:
        """Get statistics about connected clients per execution."""
        stats = {}
        for execution_id in self._current_progress.keys():
            stats[execution_id] = self.websocket_manager.get_connected_clients_count(execution_id)
        return stats