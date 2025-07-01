"""Execution API routes."""

from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.dependencies import get_current_user
from budflow.auth.models import User
from budflow.database_deps import get_db
from budflow.nodes.registry import NodeRegistry
from budflow.nodes.dependencies import get_node_registry
from budflow.workflows.models import ExecutionStatus
from budflow.executions.exceptions import (
    ExecutionNotFoundError,
    ExecutionPermissionError,
    ExecutionStateError,
)
from budflow.executions.schemas import (
    ExecutionBulkDelete,
    ExecutionDataResponse,
    ExecutionListResponse,
    ExecutionLogsResponse,
    ExecutionMetrics,
    ExecutionNodesResponse,
    ExecutionResponse,
    ExecutionResume,
    ExecutionSearchResponse,
)
from budflow.executions.service import ExecutionService

router = APIRouter(prefix="/executions", tags=["Executions"])


@router.get("", response_model=ExecutionListResponse)
async def list_executions(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    workflow_id: Optional[UUID] = Query(None, alias="workflowId"),
    status: Optional[ExecutionStatus] = Query(None),
    mode: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None, alias="startDate"),
    end_date: Optional[datetime] = Query(None, alias="endDate"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionListResponse:
    """List executions accessible to current user."""
    service = ExecutionService(db, node_registry)
    
    result = await service.list_executions(
        user=current_user,
        limit=limit,
        offset=offset,
        workflow_id=workflow_id,
        status=status,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
    )
    
    return ExecutionListResponse(
        items=[ExecutionResponse.from_orm(e) for e in result["items"]],
        total=result["total"],
        limit=limit,
        offset=offset,
    )


@router.get("/metrics", response_model=ExecutionMetrics)
async def get_execution_metrics(
    start_date: datetime = Query(..., alias="startDate"),
    end_date: datetime = Query(..., alias="endDate"),
    workflow_id: Optional[UUID] = Query(None, alias="workflowId"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionMetrics:
    """Get execution metrics for date range."""
    service = ExecutionService(db, node_registry)
    
    return await service.get_execution_metrics(
        user=current_user,
        start_date=start_date,
        end_date=end_date,
        workflow_id=workflow_id,
    )


@router.get("/search", response_model=ExecutionSearchResponse)
async def search_executions(
    q: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionSearchResponse:
    """Search executions by content."""
    service = ExecutionService(db, node_registry)
    
    result = await service.search_executions(
        user=current_user,
        query=q,
        limit=limit,
        offset=offset,
    )
    
    return ExecutionSearchResponse(
        items=[ExecutionResponse.from_orm(e) for e in result["items"]],
        total=result["total"],
        limit=limit,
        offset=offset,
        query=q,
    )


@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution(
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionResponse:
    """Get a specific execution."""
    service = ExecutionService(db, node_registry)
    
    try:
        execution = await service.get_execution(execution_id, current_user)
        return ExecutionResponse.from_orm(execution)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.delete("/{execution_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_execution(
    execution_id: UUID,
    force: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> Response:
    """Delete an execution."""
    service = ExecutionService(db, node_registry)
    
    try:
        await service.delete_execution(
            execution_id=execution_id,
            user=current_user,
            force=force,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except ExecutionStateError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{execution_id}/cancel", response_model=ExecutionResponse)
async def cancel_execution(
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionResponse:
    """Cancel a running execution."""
    service = ExecutionService(db, node_registry)
    
    try:
        execution = await service.cancel_execution(execution_id, current_user)
        return ExecutionResponse.from_orm(execution)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except ExecutionStateError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{execution_id}/retry", response_model=ExecutionResponse)
async def retry_execution(
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionResponse:
    """Retry a failed execution."""
    service = ExecutionService(db, node_registry)
    
    try:
        execution = await service.retry_execution(execution_id, current_user)
        return ExecutionResponse.from_orm(execution)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except ExecutionStateError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{execution_id}/resume", response_model=ExecutionResponse)
async def resume_execution(
    execution_id: UUID,
    resume_data: ExecutionResume,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionResponse:
    """Resume a waiting execution."""
    service = ExecutionService(db, node_registry)
    
    try:
        execution = await service.resume_execution(
            execution_id=execution_id,
            user=current_user,
            resume_data=resume_data,
        )
        return ExecutionResponse.from_orm(execution)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except ExecutionStateError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/{execution_id}/data", response_model=ExecutionDataResponse)
async def get_execution_data(
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionDataResponse:
    """Get execution input/output data."""
    service = ExecutionService(db, node_registry)
    
    try:
        data = await service.get_execution_data(execution_id, current_user)
        return ExecutionDataResponse(**data)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{execution_id}/logs", response_model=ExecutionLogsResponse)
async def get_execution_logs(
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionLogsResponse:
    """Get execution logs."""
    service = ExecutionService(db, node_registry)
    
    try:
        logs = await service.get_execution_logs(execution_id, current_user)
        return ExecutionLogsResponse(**logs)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{execution_id}/nodes", response_model=ExecutionNodesResponse)
async def get_execution_nodes(
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionNodesResponse:
    """Get execution node outputs."""
    service = ExecutionService(db, node_registry)
    
    try:
        nodes = await service.get_execution_node_outputs(execution_id, current_user)
        return ExecutionNodesResponse(**nodes)
    except ExecutionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution not found",
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.post("/bulk/delete")
async def bulk_delete_executions(
    bulk_data: ExecutionBulkDelete,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> Dict[str, int]:
    """Bulk delete executions."""
    service = ExecutionService(db, node_registry)
    
    return await service.bulk_delete_executions(
        user=current_user,
        execution_ids=bulk_data.execution_ids,
    )


# This endpoint is in the workflows router but included here for completeness
async def get_workflow_executions(
    workflow_id: UUID,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> ExecutionListResponse:
    """Get executions for a specific workflow."""
    service = ExecutionService(db, node_registry)
    
    try:
        result = await service.get_workflow_executions(
            workflow_id=workflow_id,
            user=current_user,
            limit=limit,
            offset=offset,
        )
        
        return ExecutionListResponse(
            items=[ExecutionResponse.from_orm(e) for e in result["items"]],
            total=result["total"],
            limit=limit,
            offset=offset,
        )
    except ExecutionPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )