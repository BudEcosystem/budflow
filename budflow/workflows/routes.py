"""Workflow API routes."""

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.dependencies import get_current_user
from budflow.auth.models import User
from budflow.database_deps import get_db
from budflow.nodes.registry import NodeRegistry
from budflow.nodes.dependencies import get_node_registry
from budflow.workflows.exceptions import (
    WorkflowNotFoundError,
    WorkflowPermissionError,
    WorkflowValidationError,
    WorkflowVersionConflictError,
)
from budflow.workflows.schemas import (
    WorkflowCreate,
    WorkflowListResponse,
    WorkflowResponse,
    WorkflowRun,
    WorkflowShare,
    WorkflowStatistics,
    WorkflowUpdate,
)
from budflow.workflows.service import WorkflowService

router = APIRouter(prefix="/workflows", tags=["Workflows"])


@router.post("", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
async def create_workflow(
    workflow_data: WorkflowCreate,
    project_id: Optional[UUID] = Query(None),
    folder_id: Optional[UUID] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Create a new workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.create_workflow(
            user=current_user,
            workflow_data=workflow_data,
            project_id=project_id,
            folder_id=folder_id,
        )
        return WorkflowResponse.from_orm(workflow)
    except WorkflowValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create workflow",
        )


@router.get("", response_model=WorkflowListResponse)
async def list_workflows(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    active: Optional[bool] = Query(None),
    archived: Optional[bool] = Query(None),
    tags: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    project_id: Optional[UUID] = Query(None),
    folder_id: Optional[UUID] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowListResponse:
    """List workflows accessible to current user."""
    service = WorkflowService(db, node_registry)
    
    # Parse tags
    tag_list = tags.split(",") if tags else None
    
    result = await service.list_workflows(
        user=current_user,
        limit=limit,
        offset=offset,
        active=active,
        archived=archived,
        tags=tag_list,
        search=search,
        project_id=project_id,
        folder_id=folder_id,
    )
    
    return WorkflowListResponse(
        items=[WorkflowResponse.from_orm(w) for w in result["items"]],
        total=result["total"],
        limit=limit,
        offset=offset,
    )


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Get a specific workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.get_workflow(workflow_id, current_user)
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.patch("/{workflow_id}", response_model=WorkflowResponse)
async def update_workflow(
    workflow_id: UUID,
    update_data: WorkflowUpdate,
    force_save: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Update a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.update_workflow(
            workflow_id=workflow_id,
            user=current_user,
            update_data=update_data,
            force_save=force_save,
        )
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except WorkflowVersionConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Version conflict - workflow has been modified",
        )
    except WorkflowValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{workflow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_workflow(
    workflow_id: UUID,
    force: bool = Query(False),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> Response:
    """Delete a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        await service.delete_workflow(
            workflow_id=workflow_id,
            user=current_user,
            force=force,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except WorkflowValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{workflow_id}/archive", response_model=WorkflowResponse)
async def archive_workflow(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Archive a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.archive_workflow(workflow_id, current_user)
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.post("/{workflow_id}/unarchive", response_model=WorkflowResponse)
async def unarchive_workflow(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Unarchive a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.unarchive_workflow(workflow_id, current_user)
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.post("/{workflow_id}/activate", response_model=WorkflowResponse)
async def activate_workflow(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Activate a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.activate_workflow(workflow_id, current_user)
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except WorkflowValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{workflow_id}/deactivate", response_model=WorkflowResponse)
async def deactivate_workflow(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Deactivate a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.deactivate_workflow(workflow_id, current_user)
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.post("/{workflow_id}/run")
async def run_workflow(
    workflow_id: UUID,
    run_data: WorkflowRun,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Manually run a workflow."""
    from budflow.executions.schemas import ExecutionResponse
    
    service = WorkflowService(db, node_registry)
    
    try:
        execution = await service.run_workflow(
            workflow_id=workflow_id,
            user=current_user,
            run_data=run_data,
        )
        return ExecutionResponse.from_orm(execution)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Execution failed: {str(e)}",
        )


@router.post("/{workflow_id}/duplicate", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
async def duplicate_workflow(
    workflow_id: UUID,
    name: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Duplicate a workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.duplicate_workflow(
            workflow_id=workflow_id,
            user=current_user,
            new_name=name,
        )
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )
    except WorkflowValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put("/{workflow_id}/share", response_model=WorkflowResponse)
async def share_workflow(
    workflow_id: UUID,
    share_data: WorkflowShare,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Share a workflow with other users."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.share_workflow(
            workflow_id=workflow_id,
            owner=current_user,
            share_data=share_data,
        )
        return WorkflowResponse.from_orm(workflow)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{workflow_id}/statistics", response_model=WorkflowStatistics)
async def get_workflow_statistics(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowStatistics:
    """Get workflow execution statistics."""
    service = WorkflowService(db, node_registry)
    
    try:
        stats = await service.get_workflow_statistics(workflow_id, current_user)
        return WorkflowStatistics(**stats)
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.post("/import", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
async def import_workflow(
    workflow_import: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> WorkflowResponse:
    """Import a workflow from JSON."""
    service = WorkflowService(db, node_registry)
    
    try:
        # Extract workflow data
        workflow_data = workflow_import.get("workflow", {})
        
        # Create workflow from import
        create_data = WorkflowCreate(
            name=workflow_data.get("name", "Imported Workflow"),
            description=workflow_data.get("description"),
            nodes=workflow_data.get("nodes", []),
            connections=workflow_data.get("connections", []),
            settings=workflow_data.get("settings", {}),
            tags=workflow_data.get("tags"),
        )
        
        workflow = await service.create_workflow(
            user=current_user,
            workflow_data=create_data,
        )
        return WorkflowResponse.from_orm(workflow)
    except WorkflowValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to import workflow",
        )


@router.get("/{workflow_id}/export")
async def export_workflow(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
) -> Dict[str, Any]:
    """Export a workflow as JSON."""
    service = WorkflowService(db, node_registry)
    
    try:
        workflow = await service.get_workflow(workflow_id, current_user)
        
        # Export workflow data (excluding sensitive info)
        return {
            "name": workflow.name,
            "description": workflow.description,
            "nodes": workflow.nodes,
            "connections": workflow.connections,
            "settings": workflow.settings,
            "tags": [tag.tag for tag in workflow.tags] if workflow.tags else [],
        }
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{workflow_id}/executions")
async def get_workflow_executions(
    workflow_id: UUID,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Get executions for a specific workflow."""
    # Import here to avoid circular dependency
    from budflow.executions.service import ExecutionService
    from budflow.executions.schemas import ExecutionListResponse, ExecutionResponse
    
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
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


# Workflow composition endpoints

@router.post("/{workflow_id}/validate-composition")
async def validate_workflow_composition(
    workflow_id: UUID,
    max_depth: int = Query(10, ge=1, le=20, description="Maximum nesting depth"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Validate workflow composition for circular dependencies."""
    service = WorkflowService(db, node_registry)
    
    try:
        result = await service.validate_workflow_composition(
            workflow_id, current_user, max_depth
        )
        return result
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{workflow_id}/dependencies")
async def get_workflow_dependencies(
    workflow_id: UUID,
    recursive: bool = Query(True, description="Include recursive dependencies"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Get workflows that this workflow depends on."""
    service = WorkflowService(db, node_registry)
    
    try:
        dependencies = await service.get_workflow_dependencies(
            workflow_id, current_user, recursive
        )
        return dependencies
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{workflow_id}/dependents")
async def get_workflow_dependents(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Get workflows that depend on this workflow."""
    service = WorkflowService(db, node_registry)
    
    try:
        dependents = await service.get_workflow_dependents(
            workflow_id, current_user
        )
        return dependents
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{workflow_id}/composition-graph")
async def get_workflow_composition_graph(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Get the workflow composition graph."""
    service = WorkflowService(db, node_registry)
    
    try:
        graph = await service.get_workflow_composition_graph(
            workflow_id, current_user
        )
        return graph
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.get("/{workflow_id}/composition-analysis")
async def analyze_workflow_composition(
    workflow_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Analyze workflow composition for optimization opportunities."""
    service = WorkflowService(db, node_registry)
    
    try:
        analysis = await service.analyze_workflow_composition(
            workflow_id, current_user
        )
        return analysis
    except WorkflowNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found",
        )
    except WorkflowPermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )


@router.post("/validate-compositions")
async def bulk_validate_compositions(
    workflow_ids: List[UUID],
    max_depth: int = Query(10, ge=1, le=20, description="Maximum nesting depth"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    node_registry: NodeRegistry = Depends(get_node_registry),
):
    """Bulk validate multiple workflow compositions."""
    service = WorkflowService(db, node_registry)
    
    results = []
    for workflow_id in workflow_ids:
        try:
            result = await service.validate_workflow_composition(
                workflow_id, current_user, max_depth
            )
            results.append(result)
        except Exception as e:
            results.append({
                "workflow_id": str(workflow_id),
                "valid": False,
                "error": str(e)
            })
    
    return results