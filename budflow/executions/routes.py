"""API routes for execution management."""

from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..database_deps import get_db
from ..workflows.models import ExecutionStatus
from .partial import PartialExecutionEngine, PartialExecutionError, NodeNotFoundError, CircularDependencyError
from .pause_resume import PauseResumeEngine, ExecutionNotPausedError, PauseTimeoutError, InvalidResumeDataError
from .realtime_progress import ProgressTracker, ProgressNotFoundError, ProgressEventType
from .pin_data import PinDataService, PinDataEngine, PinDataNotFoundError, InvalidPinDataError, NodeNotPinnedError
from ..nodes.registry import NodeRegistry
from .schemas import (
    PartialExecutionRequest,
    PartialExecutionResponse,
    ExecutionSummary,
    DependencyGraphResponse,
    NodeExecutionPlan,
    PartialExecutionPreview
)

router = APIRouter(prefix="/executions", tags=["Executions"])


@router.post("/workflows/{workflow_id}/execute-partial", response_model=PartialExecutionResponse)
async def execute_partial_workflow(
    workflow_id: int,
    request: PartialExecutionRequest,
    db: AsyncSession = Depends(get_db)
):
    """Execute workflow starting from specific node."""
    try:
        node_registry = NodeRegistry()
        engine = PartialExecutionEngine(db, node_registry)
        execution = await engine.execute_partial(
            workflow_id=str(workflow_id),
            start_node=request.start_node,
            input_data=request.input_data,
            previous_execution_id=request.previous_execution_id,
            execution_mode=request.execution_mode
        )
        
        # Calculate duration if finished
        duration = None
        if execution.finished_at and execution.started_at:
            duration = (execution.finished_at - execution.started_at).total_seconds()
        
        # Get nodes that were executed from result data
        nodes_executed = list(execution.data.keys()) if execution.data else []
        
        return PartialExecutionResponse(
            execution_id=str(execution.id),
            status=execution.status,
            start_node=execution.start_node,
            execution_type=execution.execution_type,
            nodes_executed=nodes_executed,
            started_at=execution.started_at,
            finished_at=execution.finished_at,
            data=execution.data,
            error=execution.error
        )
        
    except NodeNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except CircularDependencyError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PartialExecutionError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/dependency-graph", response_model=DependencyGraphResponse)
async def get_workflow_dependency_graph(
    workflow_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get workflow dependency graph analysis."""
    try:
        node_registry = NodeRegistry()
        engine = PartialExecutionEngine(db, node_registry)
        workflow = await engine.workflow_service.get_workflow(workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        # Build dependency graph
        graph = engine.build_dependency_graph(workflow)
        
        # Get all nodes
        nodes = list(graph.forward_dependencies.keys())
        
        return DependencyGraphResponse(
            workflow_id=workflow_id,
            nodes=nodes,
            dependencies=graph.forward_dependencies,
            reverse_dependencies=graph.reverse_dependencies,
            has_cycles=graph.detect_cycles()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/execution-plan/{start_node}", response_model=NodeExecutionPlan)
async def get_execution_plan(
    workflow_id: int,
    start_node: str,
    db: AsyncSession = Depends(get_db)
):
    """Get execution plan for starting from specific node."""
    try:
        node_registry = NodeRegistry()
        engine = PartialExecutionEngine(db, node_registry)
        
        # Validate start node
        await engine.validate_start_node(str(workflow_id), start_node)
        
        # Get workflow and build dependency graph
        workflow = await engine.workflow_service.get_workflow(workflow_id)
        graph = engine.build_dependency_graph(workflow)
        
        # Get nodes to execute
        nodes_to_execute = engine.get_nodes_to_execute(graph, start_node)
        
        # Create execution order (simple topological sort)
        execution_order = list(nodes_to_execute)
        execution_order.sort()  # Simple ordering for now
        
        return NodeExecutionPlan(
            start_node=start_node,
            nodes_to_execute=execution_order,
            execution_order=execution_order,
            estimated_duration=len(execution_order) * 2.0  # Rough estimate: 2 seconds per node
        )
        
    except NodeNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/partial-execution-preview/{start_node}", response_model=PartialExecutionPreview)
async def preview_partial_execution(
    workflow_id: int,
    start_node: str,
    db: AsyncSession = Depends(get_db)
):
    """Preview what would happen in a partial execution."""
    try:
        node_registry = NodeRegistry()
        engine = PartialExecutionEngine(db, node_registry)
        
        # Get dependency graph
        workflow = await engine.workflow_service.get_workflow(workflow_id)
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        graph = engine.build_dependency_graph(workflow)
        
        # Get execution plan
        nodes_to_execute = engine.get_nodes_to_execute(graph, start_node)
        execution_order = list(nodes_to_execute)
        execution_order.sort()
        
        execution_plan = NodeExecutionPlan(
            start_node=start_node,
            nodes_to_execute=execution_order,
            execution_order=execution_order,
            estimated_duration=len(execution_order) * 2.0
        )
        
        # Get dependency graph response
        dependency_graph = DependencyGraphResponse(
            workflow_id=workflow_id,
            nodes=list(graph.forward_dependencies.keys()),
            dependencies=graph.forward_dependencies,
            reverse_dependencies=graph.reverse_dependencies,
            has_cycles=graph.detect_cycles()
        )
        
        # Generate warnings
        warnings = []
        if dependency_graph.has_cycles:
            warnings.append("Workflow contains circular dependencies")
        
        if len(execution_order) == 1:
            warnings.append("Only one node will be executed")
        elif len(execution_order) > 10:
            warnings.append(f"Large number of nodes will be executed ({len(execution_order)})")
        
        return PartialExecutionPreview(
            workflow_id=workflow_id,
            start_node=start_node,
            execution_plan=execution_plan,
            dependency_graph=dependency_graph,
            warnings=warnings
        )
        
    except NodeNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/executions", response_model=List[ExecutionSummary])
async def list_workflow_executions(
    workflow_id: int,
    execution_type: Optional[str] = Query(None, description="Filter by execution type (full/partial)"),
    status: Optional[ExecutionStatus] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=1000, description="Number of executions to return"),
    offset: int = Query(0, ge=0, description="Number of executions to skip"),
    db: AsyncSession = Depends(get_db)
):
    """List workflow executions with optional filtering."""
    try:
        node_registry = NodeRegistry()
        engine = PartialExecutionEngine(db, node_registry)
        
        # For now, return empty list as we need to implement the query
        # In a real implementation, this would query the database
        return []
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/{execution_id}", response_model=PartialExecutionResponse)
async def get_execution(
    execution_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get details of a specific execution."""
    try:
        node_registry = NodeRegistry()
        engine = PartialExecutionEngine(db, node_registry)
        
        # Query execution from database
        from sqlalchemy import select
        from ..workflows.models import WorkflowExecution
        
        result = await db.execute(
            select(WorkflowExecution).where(WorkflowExecution.id == int(execution_id))
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        # Get nodes that were executed from result data
        nodes_executed = list(execution.data.keys()) if execution.data else []
        
        return PartialExecutionResponse(
            execution_id=str(execution.id),
            status=execution.status,
            start_node=execution.start_node or "",
            execution_type=execution.execution_type,
            nodes_executed=nodes_executed,
            started_at=execution.started_at,
            finished_at=execution.finished_at,
            data=execution.data,
            error=execution.error
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Pause/Resume API Endpoints

@router.post("/executions/{execution_id}/pause")
async def pause_execution(
    execution_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Pause a running execution."""
    try:
        node_registry = NodeRegistry()
        pause_resume_engine = PauseResumeEngine(db, node_registry)
        
        # Check if execution exists and is running
        from sqlalchemy import select
        from ..workflows.models import WorkflowExecution
        
        result = await db.execute(
            select(WorkflowExecution).where(WorkflowExecution.id == int(execution_id))
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        if execution.status != ExecutionStatus.RUNNING:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot pause execution in {execution.status} state"
            )
        
        # Update execution status to WAITING (paused)
        execution.status = ExecutionStatus.WAITING
        await db.commit()
        
        return {
            "message": f"Execution {execution_id} has been paused",
            "execution_id": execution_id,
            "status": execution.status.value,
            "paused_at": execution.started_at.isoformat() if execution.started_at else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str,
    resume_data: Optional[Dict[str, Any]] = None,
    db: AsyncSession = Depends(get_db)
):
    """Resume a paused execution."""
    try:
        node_registry = NodeRegistry()
        pause_resume_engine = PauseResumeEngine(db, node_registry)
        
        execution = await pause_resume_engine.resume_execution(execution_id, resume_data)
        
        return {
            "message": f"Execution {execution_id} has been resumed",
            "execution_id": execution_id,
            "status": execution.status.value,
            "resumed_at": execution.finished_at.isoformat() if execution.finished_at else None
        }
        
    except ExecutionNotPausedError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PauseTimeoutError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except InvalidResumeDataError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/paused")
async def list_paused_executions(
    limit: int = Query(50, ge=1, le=1000, description="Number of executions to return"),
    offset: int = Query(0, ge=0, description="Number of executions to skip"),
    db: AsyncSession = Depends(get_db)
):
    """List all paused executions."""
    try:
        from sqlalchemy import select
        from ..workflows.models import WorkflowExecution
        
        # Query paused executions
        result = await db.execute(
            select(WorkflowExecution)
            .where(WorkflowExecution.status == ExecutionStatus.WAITING)
            .order_by(WorkflowExecution.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        
        executions = result.scalars().all()
        
        paused_executions = []
        for execution in executions:
            paused_executions.append({
                "execution_id": str(execution.id),
                "workflow_id": execution.workflow_id,
                "mode": execution.mode.value,
                "status": execution.status.value,
                "paused_at": execution.started_at.isoformat() if execution.started_at else None,
                "execution_type": getattr(execution, 'execution_type', 'full')
            })
        
        return {
            "paused_executions": paused_executions,
            "total": len(paused_executions),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/{execution_id}/pause-info")
async def get_execution_pause_info(
    execution_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get pause information for a paused execution."""
    try:
        node_registry = NodeRegistry()
        pause_resume_engine = PauseResumeEngine(db, node_registry)
        
        pause_data = await pause_resume_engine.get_pause_data(execution_id)
        
        if not pause_data:
            raise HTTPException(status_code=404, detail=f"No pause data found for execution {execution_id}")
        
        return {
            "execution_id": execution_id,
            "pause_type": pause_data.pause_type.value,
            "pause_node": pause_data.pause_node,
            "pause_metadata": pause_data.pause_metadata,
            "created_at": pause_data.created_at.isoformat(),
            "expires_at": pause_data.expires_at.isoformat() if pause_data.expires_at else None,
            "is_expired": pause_data.expires_at and pause_data.expires_at < datetime.now(timezone.utc) if pause_data.expires_at else False
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Real-time Progress Tracking API Endpoints

@router.get("/executions/{execution_id}/progress")
async def get_execution_progress(
    execution_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get current progress for an execution."""
    try:
        progress_tracker = ProgressTracker(db)
        
        # Get execution from database
        from sqlalchemy import select
        from ..workflows.models import WorkflowExecution
        
        result = await db.execute(
            select(WorkflowExecution).where(WorkflowExecution.id == int(execution_id))
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        # Get workflow
        from ..workflows.service import WorkflowService
        node_registry = NodeRegistry()
        workflow_service = WorkflowService(db, node_registry)
        workflow = await workflow_service.get_workflow(execution.workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {execution.workflow_id} not found")
        
        # Get progress
        progress = await progress_tracker.get_execution_progress(execution, workflow)
        
        return progress.to_dict()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/{execution_id}/progress/events")
async def get_execution_progress_events(
    execution_id: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of events to return"),
    offset: int = Query(0, ge=0, description="Number of events to skip"),
    db: AsyncSession = Depends(get_db)
):
    """Get progress events for an execution."""
    try:
        progress_tracker = ProgressTracker(db)
        
        events = await progress_tracker.get_progress_events(execution_id)
        
        # Apply pagination
        total_events = len(events)
        paginated_events = events[offset:offset + limit]
        
        return {
            "execution_id": execution_id,
            "events": [event.to_dict() for event in paginated_events],
            "total": total_events,
            "limit": limit,
            "offset": offset
        }
        
    except ProgressNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/executions/{execution_id}/progress/start")
async def start_progress_tracking(
    execution_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Start progress tracking for an execution."""
    try:
        progress_tracker = ProgressTracker(db)
        
        # Get execution from database
        from sqlalchemy import select
        from ..workflows.models import WorkflowExecution
        
        result = await db.execute(
            select(WorkflowExecution).where(WorkflowExecution.id == int(execution_id))
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        # Get workflow
        from ..workflows.service import WorkflowService
        node_registry = NodeRegistry()
        workflow_service = WorkflowService(db, node_registry)
        workflow = await workflow_service.get_workflow(execution.workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {execution.workflow_id} not found")
        
        # Start tracking
        await progress_tracker.start_progress_tracking(execution, workflow)
        
        return {
            "message": f"Progress tracking started for execution {execution_id}",
            "execution_id": execution_id,
            "started_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/executions/{execution_id}/progress/update")
async def update_execution_progress(
    execution_id: str,
    event_type: ProgressEventType,
    node_name: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    db: AsyncSession = Depends(get_db)
):
    """Update progress for an execution (for testing/manual updates)."""
    try:
        progress_tracker = ProgressTracker(db)
        
        # Get execution from database
        from sqlalchemy import select
        from ..workflows.models import WorkflowExecution
        
        result = await db.execute(
            select(WorkflowExecution).where(WorkflowExecution.id == int(execution_id))
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        # Get workflow
        from ..workflows.service import WorkflowService
        node_registry = NodeRegistry()
        workflow_service = WorkflowService(db, node_registry)
        workflow = await workflow_service.get_workflow(execution.workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {execution.workflow_id} not found")
        
        # Update progress
        await progress_tracker.update_progress(
            execution=execution,
            workflow=workflow,
            event_type=event_type,
            node_name=node_name,
            data=data
        )
        
        return {
            "message": f"Progress updated for execution {execution_id}",
            "execution_id": execution_id,
            "event_type": event_type.value,
            "node_name": node_name,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/progress/active-executions")
async def get_active_executions(
    db: AsyncSession = Depends(get_db)
):
    """Get list of actively tracked executions."""
    try:
        progress_tracker = ProgressTracker(db)
        
        active_executions = await progress_tracker.get_active_executions()
        client_stats = await progress_tracker.get_connected_clients_stats()
        
        return {
            "active_executions": active_executions,
            "total_active": len(active_executions),
            "client_connections": client_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/executions/{execution_id}/progress")
async def cleanup_execution_progress(
    execution_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Clean up progress tracking data for an execution."""
    try:
        progress_tracker = ProgressTracker(db)
        
        await progress_tracker.cleanup_execution_tracking(execution_id)
        
        return {
            "message": f"Progress tracking cleaned up for execution {execution_id}",
            "execution_id": execution_id,
            "cleaned_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Pin Data Support API Endpoints

@router.post("/workflows/{workflow_id}/nodes/{node_id}/pin-data")
async def save_pin_data(
    workflow_id: int,
    node_id: str,
    data: List[Dict[str, Any]],
    description: Optional[str] = None,
    optimize: bool = False,
    user_id: str = "default-user",  # In production, get from auth
    db: AsyncSession = Depends(get_db)
):
    """Save pin data for a workflow node."""
    try:
        pin_data_service = PinDataService(db)
        
        pin_data = await pin_data_service.save_pin_data(
            workflow_id=workflow_id,
            node_id=node_id,
            data=data,
            user_id=user_id,
            description=description,
            optimize=optimize
        )
        
        return {
            "message": f"Pin data saved for node {node_id}",
            "pin_data": pin_data.to_dict()
        }
        
    except InvalidPinDataError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/nodes/{node_id}/pin-data")
async def get_pin_data(
    workflow_id: int,
    node_id: str,
    version: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get pin data for a workflow node."""
    try:
        pin_data_service = PinDataService(db)
        
        if version:
            pin_data = await pin_data_service.get_pin_data_version(workflow_id, node_id, version)
        else:
            pin_data = await pin_data_service.get_pin_data(workflow_id, node_id)
        
        return pin_data.to_dict()
        
    except PinDataNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/workflows/{workflow_id}/nodes/{node_id}/pin-data")
async def delete_pin_data(
    workflow_id: int,
    node_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Delete pin data for a workflow node."""
    try:
        pin_data_service = PinDataService(db)
        
        await pin_data_service.delete_pin_data(workflow_id, node_id)
        
        return {
            "message": f"Pin data deleted for node {node_id}",
            "workflow_id": workflow_id,
            "node_id": node_id,
            "deleted_at": datetime.now(timezone.utc).isoformat()
        }
        
    except PinDataNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/pin-data")
async def list_workflow_pin_data(
    workflow_id: int,
    summary: bool = Query(False, description="Return summary instead of full data"),
    db: AsyncSession = Depends(get_db)
):
    """List all pin data for a workflow."""
    try:
        pin_data_service = PinDataService(db)
        
        if summary:
            result = await pin_data_service.get_workflow_pin_summary(workflow_id)
        else:
            pinned_nodes = await pin_data_service.list_pinned_nodes(workflow_id)
            result = {
                "workflow_id": workflow_id,
                "pinned_nodes": [pin_data.to_dict() for pin_data in pinned_nodes],
                "total_pinned_nodes": len(pinned_nodes)
            }
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/nodes/{node_id}/pin-data/versions")
async def get_pin_data_version_history(
    workflow_id: int,
    node_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get version history for pin data."""
    try:
        pin_data_service = PinDataService(db)
        
        version_history = await pin_data_service.get_pin_data_version_history(workflow_id, node_id)
        
        return {
            "workflow_id": workflow_id,
            "node_id": node_id,
            "versions": [pin_data.to_dict() for pin_data in version_history],
            "total_versions": len(version_history)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/{workflow_id}/nodes/{node_id}/pin-data/revert")
async def revert_pin_data_to_version(
    workflow_id: int,
    node_id: str,
    target_version: int,
    user_id: str = "default-user",  # In production, get from auth
    db: AsyncSession = Depends(get_db)
):
    """Revert pin data to a previous version."""
    try:
        pin_data_service = PinDataService(db)
        
        reverted_pin_data = await pin_data_service.revert_pin_data_to_version(
            workflow_id, node_id, target_version, user_id
        )
        
        return {
            "message": f"Pin data reverted to version {target_version}",
            "reverted_pin_data": reverted_pin_data.to_dict()
        }
        
    except PinDataNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/{workflow_id}/pin-data/bulk")
async def bulk_save_pin_data(
    workflow_id: int,
    bulk_pin_data: List[Dict[str, Any]],
    user_id: str = "default-user",  # In production, get from auth
    db: AsyncSession = Depends(get_db)
):
    """Save multiple pin data entries in bulk."""
    try:
        pin_data_service = PinDataService(db)
        
        result = await pin_data_service.bulk_save_pin_data(
            workflow_id, bulk_pin_data, user_id
        )
        
        return {
            "message": f"Bulk pin data operation completed",
            "workflow_id": workflow_id,
            "result": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/{workflow_id}/pin-data/clone")
async def clone_pin_data_to_workflow(
    workflow_id: int,
    target_workflow_id: int,
    user_id: str = "default-user",  # In production, get from auth
    db: AsyncSession = Depends(get_db)
):
    """Clone pin data from this workflow to another workflow."""
    try:
        pin_data_service = PinDataService(db)
        
        cloned_count = await pin_data_service.clone_pin_data_to_workflow(
            workflow_id, target_workflow_id, user_id
        )
        
        return {
            "message": f"Pin data cloned to workflow {target_workflow_id}",
            "source_workflow_id": workflow_id,
            "target_workflow_id": target_workflow_id,
            "cloned_count": cloned_count,
            "cloned_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/execution-plan/pin-data")
async def get_execution_plan_with_pin_data(
    workflow_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get execution plan showing which nodes will use pin data."""
    try:
        pin_data_engine = PinDataEngine(db)
        
        # Get workflow
        from ..workflows.service import WorkflowService
        node_registry = NodeRegistry()
        workflow_service = WorkflowService(db, node_registry)
        workflow = await workflow_service.get_workflow(workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        execution_plan = await pin_data_engine.get_execution_plan_with_pin_data(workflow)
        
        return execution_plan
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/{workflow_id}/execute-with-pin-data")
async def execute_workflow_with_pin_data(
    workflow_id: int,
    input_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    db: AsyncSession = Depends(get_db)
):
    """Execute workflow using pin data where available."""
    try:
        pin_data_engine = PinDataEngine(db)
        
        # Get workflow
        from ..workflows.service import WorkflowService
        node_registry = NodeRegistry()
        workflow_service = WorkflowService(db, node_registry)
        workflow = await workflow_service.get_workflow(workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        execution_id = f"pin-exec-{workflow_id}-{int(datetime.now().timestamp())}"
        
        execution_result = await pin_data_engine.execute_with_pin_data(
            execution_id=execution_id,
            workflow=workflow,
            input_data=input_data or {}
        )
        
        return execution_result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}/pin-data/validate")
async def validate_pin_data_compatibility(
    workflow_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Validate pin data compatibility with current workflow."""
    try:
        pin_data_engine = PinDataEngine(db)
        
        # Get workflow
        from ..workflows.service import WorkflowService
        node_registry = NodeRegistry()
        workflow_service = WorkflowService(db, node_registry)
        workflow = await workflow_service.get_workflow(workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        validation_result = await pin_data_engine.validate_pin_data_for_execution(workflow)
        
        return validation_result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))