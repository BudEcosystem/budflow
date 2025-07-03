"""Workflow service for business logic."""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Union
from uuid import UUID, uuid4

import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from budflow.auth.models import User
from budflow.executor.engine import WorkflowExecutionEngine
from budflow.nodes.registry import NodeRegistry
from budflow.workflows.models import (
    Execution,
    ExecutionStatus,
    SharedWorkflow,
    Workflow,
    WorkflowPermission,
    WorkflowStatus,
    WorkflowTag,
)
from budflow.workflows.schemas import (
    WorkflowCreate,
    WorkflowUpdate,
    WorkflowShare,
    WorkflowRun,
)
from budflow.workflows.exceptions import (
    WorkflowNotFoundError,
    WorkflowValidationError,
    WorkflowPermissionError,
    WorkflowVersionConflictError,
)

logger = structlog.get_logger()


class WorkflowService:
    """Service for managing workflows."""

    def __init__(self, db: AsyncSession, node_registry: NodeRegistry):
        """Initialize workflow service."""
        self.db = db
        self.node_registry = node_registry
        self.engine = WorkflowExecutionEngine(node_registry)

    async def create_workflow(
        self,
        user: User,
        workflow_data: WorkflowCreate,
        project_id: Optional[UUID] = None,
        folder_id: Optional[UUID] = None,
    ) -> Workflow:
        """Create a new workflow."""
        # Validate workflow structure
        await self._validate_workflow_structure(workflow_data)
        
        # Validate user has access to all credentials
        await self._validate_credential_access(user, workflow_data)
        
        # Generate version ID
        version_id = str(uuid4())
        
        # Add node IDs if not present
        nodes = self._ensure_node_ids(workflow_data.nodes)
        
        # Create workflow
        workflow = Workflow(
            name=workflow_data.name,
            description=workflow_data.description,
            nodes=nodes,
            connections=workflow_data.connections,
            settings=workflow_data.settings or {},
            version_id=version_id,
            user_id=user.id,
            project_id=project_id or user.personal_project_id,
            folder_id=folder_id,
            status=WorkflowStatus.DRAFT,
            active=False,
        )
        
        self.db.add(workflow)
        
        # Create owner share
        shared_workflow = SharedWorkflow(
            workflow_id=workflow.id,
            user_id=user.id,
            permission=WorkflowPermission.OWNER,
        )
        self.db.add(shared_workflow)
        
        # Add tags if provided
        if workflow_data.tags:
            await self._add_tags_to_workflow(workflow, workflow_data.tags)
        
        await self.db.commit()
        await self.db.refresh(workflow)
        
        logger.info(
            "Workflow created",
            workflow_id=workflow.id,
            user_id=user.id,
            name=workflow.name,
        )
        
        return workflow

    async def get_workflow(self, workflow_id: UUID, user: User) -> Workflow:
        """Get a workflow by ID."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        # Load relationships
        await self.db.refresh(workflow, ["workflow_tags", "shared_with"])
        
        return workflow

    async def list_workflows(
        self,
        user: User,
        limit: int = 50,
        offset: int = 0,
        active: Optional[bool] = None,
        archived: Optional[bool] = None,
        tags: Optional[List[str]] = None,
        search: Optional[str] = None,
        project_id: Optional[UUID] = None,
        folder_id: Optional[UUID] = None,
    ) -> Dict[str, Any]:
        """List workflows accessible to user."""
        # Base query for accessible workflows
        query = select(Workflow).join(
            SharedWorkflow,
            SharedWorkflow.workflow_id == Workflow.id
        ).where(
            SharedWorkflow.user_id == user.id
        )
        
        # Apply filters
        if active is not None:
            query = query.where(Workflow.active == active)
        
        if archived is not None:
            query = query.where(Workflow.archived == archived)
        
        if project_id:
            query = query.where(Workflow.project_id == project_id)
        
        if folder_id:
            query = query.where(Workflow.folder_id == folder_id)
        
        if tags:
            query = query.join(WorkflowTag).where(WorkflowTag.tag.in_(tags))
        
        if search:
            search_term = f"%{search}%"
            query = query.where(
                or_(
                    Workflow.name.ilike(search_term),
                    Workflow.description.ilike(search_term),
                )
            )
        
        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total = await self.db.scalar(count_query)
        
        # Apply pagination
        query = query.options(selectinload(Workflow.workflow_tags))
        query = query.order_by(Workflow.updated_at.desc())
        query = query.limit(limit).offset(offset)
        
        # Execute query
        result = await self.db.execute(query)
        workflows = result.scalars().unique().all()
        
        return {
            "items": workflows,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    async def update_workflow(
        self,
        workflow_id: UUID,
        user: User,
        update_data: WorkflowUpdate,
        force_save: bool = False,
    ) -> Workflow:
        """Update a workflow."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.UPDATE
        )
        
        # Check version conflict
        if not force_save and update_data.version_id != workflow.version_id:
            raise WorkflowVersionConflictError(
                "Workflow has been modified by another user"
            )
        
        # Validate updates if nodes or connections changed
        if update_data.nodes is not None or update_data.connections is not None:
            await self._validate_workflow_structure(update_data)
            await self._validate_credential_access(user, update_data)
        
        # Deactivate if active and structure changed
        was_active = workflow.active
        if was_active and (update_data.nodes or update_data.connections):
            await self._deactivate_workflow(workflow)
        
        # Update fields
        update_fields = update_data.dict(exclude_unset=True, exclude={"version_id"})
        
        # Generate new version ID if content changed
        if any(field in update_fields for field in ["nodes", "connections", "settings"]):
            workflow.version_id = str(uuid4())
        
        # Update workflow
        for field, value in update_fields.items():
            if field == "tags":
                await self._update_workflow_tags(workflow, value)
            elif field == "nodes" and value is not None:
                value = self._ensure_node_ids(value)
                setattr(workflow, field, value)
            else:
                setattr(workflow, field, value)
        
        await self.db.commit()
        await self.db.refresh(workflow)
        
        # Reactivate if was active
        if was_active and workflow.active is False:
            await self._activate_workflow(workflow)
        
        logger.info(
            "Workflow updated",
            workflow_id=workflow.id,
            user_id=user.id,
            version_id=workflow.version_id,
        )
        
        return workflow

    async def delete_workflow(
        self, workflow_id: UUID, user: User, force: bool = False
    ) -> None:
        """Delete a workflow."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.DELETE
        )
        
        # Check if archived (unless forced)
        if not force and not workflow.archived:
            raise WorkflowValidationError(
                "Workflow must be archived before deletion"
            )
        
        # Deactivate if active
        if workflow.active:
            await self._deactivate_workflow(workflow)
        
        # Delete workflow (cascade will handle related records)
        await self.db.delete(workflow)
        await self.db.commit()
        
        logger.info(
            "Workflow deleted",
            workflow_id=workflow_id,
            user_id=user.id,
        )

    async def archive_workflow(self, workflow_id: UUID, user: User) -> Workflow:
        """Archive a workflow."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.UPDATE
        )
        
        workflow.archived = True
        await self.db.commit()
        await self.db.refresh(workflow)
        
        logger.info(
            "Workflow archived",
            workflow_id=workflow.id,
            user_id=user.id,
        )
        
        return workflow

    async def unarchive_workflow(self, workflow_id: UUID, user: User) -> Workflow:
        """Unarchive a workflow."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.UPDATE
        )
        
        workflow.archived = False
        await self.db.commit()
        await self.db.refresh(workflow)
        
        logger.info(
            "Workflow unarchived",
            workflow_id=workflow.id,
            user_id=user.id,
        )
        
        return workflow

    async def activate_workflow(self, workflow_id: UUID, user: User) -> Workflow:
        """Activate a workflow."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.UPDATE
        )
        
        await self._activate_workflow(workflow)
        await self.db.commit()
        await self.db.refresh(workflow)
        
        return workflow

    async def deactivate_workflow(self, workflow_id: UUID, user: User) -> Workflow:
        """Deactivate a workflow."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.UPDATE
        )
        
        await self._deactivate_workflow(workflow)
        await self.db.commit()
        await self.db.refresh(workflow)
        
        return workflow

    async def run_workflow(
        self,
        workflow_id: UUID,
        user: User,
        run_data: WorkflowRun,
        queue_mode: bool = False,
        queue_manager: Optional[Any] = None,
    ) -> Execution:
        """Run a workflow manually."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.EXECUTE
        )
        
        # Create execution record
        execution = Execution(
            workflow_id=workflow.id,
            user_id=user.id,
            mode="manual",
            start_node=run_data.start_node,
            status=ExecutionStatus.NEW,
            data={"input": run_data.input} if run_data.input else {},
        )
        
        self.db.add(execution)
        await self.db.commit()
        await self.db.refresh(execution)
        
        # Check if we should use queue mode
        if queue_mode and queue_manager:
            # Import here to avoid circular dependency
            from budflow.queue import JobData, JobOptions
            
            # Create job data
            job_data = JobData(
                workflow_id=str(workflow.id),
                execution_id=str(execution.uuid),
                load_static_data=True,
                push_ref=run_data.push_ref if hasattr(run_data, 'push_ref') else None
            )
            
            # Set job options
            options = JobOptions(
                priority=getattr(run_data, 'priority', 1000),
            )
            
            # Enqueue job
            try:
                job_id = await queue_manager.enqueue_job(job_data, options)
                
                # Update execution with job ID
                execution.job_id = job_id
                execution.status = ExecutionStatus.WAITING
                await self.db.commit()
                
                logger.info(
                    "Workflow queued for execution",
                    workflow_id=workflow.id,
                    execution_id=execution.id,
                    job_id=job_id,
                )
                
            except Exception as e:
                execution.status = ExecutionStatus.ERROR
                execution.error = {"message": f"Failed to queue job: {str(e)}", "type": type(e).__name__}
                await self.db.commit()
                raise
                
        else:
            # Execute workflow directly (non-queue mode)
            try:
                execution = await self.engine.execute(
                    workflow=workflow,
                    execution=execution,
                    input_data=run_data.input,
                    start_node=run_data.start_node,
                )
            except Exception as e:
                execution.status = ExecutionStatus.ERROR
                execution.error = {"message": str(e), "type": type(e).__name__}
                execution.finished_at = datetime.now(timezone.utc)
                if execution.started_at:
                    execution.execution_time_ms = int(
                        (execution.finished_at - execution.started_at).total_seconds() * 1000
                    )
            
            await self.db.commit()
            await self.db.refresh(execution)
            
            logger.info(
                "Workflow executed",
                workflow_id=workflow.id,
                execution_id=execution.id,
                status=execution.status,
            )
        
        return execution

    async def duplicate_workflow(
        self,
        workflow_id: UUID,
        user: User,
        new_name: Optional[str] = None,
    ) -> Workflow:
        """Duplicate a workflow."""
        source_workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        # Create duplicate
        duplicate_data = WorkflowCreate(
            name=new_name or f"{source_workflow.name} (copy)",
            description=source_workflow.description,
            nodes=source_workflow.nodes,
            connections=source_workflow.connections,
            settings=source_workflow.settings,
            tags=[tag.tag for tag in source_workflow.workflow_tags] if source_workflow.workflow_tags else None,
        )
        
        return await self.create_workflow(
            user=user,
            workflow_data=duplicate_data,
            project_id=source_workflow.project_id,
            folder_id=source_workflow.folder_id,
        )

    async def share_workflow(
        self,
        workflow_id: UUID,
        owner: User,
        share_data: WorkflowShare,
    ) -> Workflow:
        """Share a workflow with other users."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, owner, WorkflowPermission.SHARE
        )
        
        # Remove existing shares for these users
        await self.db.execute(
            SharedWorkflow.__table__.delete().where(
                and_(
                    SharedWorkflow.workflow_id == workflow_id,
                    SharedWorkflow.user_id.in_(share_data.user_ids),
                    SharedWorkflow.user_id != owner.id,  # Don't remove owner
                )
            )
        )
        
        # Add new shares
        for user_id in share_data.user_ids:
            if user_id == owner.id:
                continue  # Skip owner
            
            shared = SharedWorkflow(
                workflow_id=workflow_id,
                user_id=user_id,
                permission=share_data.permission,
            )
            self.db.add(shared)
        
        await self.db.commit()
        await self.db.refresh(workflow, ["shared_with"])
        
        logger.info(
            "Workflow shared",
            workflow_id=workflow_id,
            owner_id=owner.id,
            shared_with=share_data.user_ids,
        )
        
        return workflow

    async def get_workflow_statistics(
        self, workflow_id: UUID, user: User
    ) -> Dict[str, Any]:
        """Get workflow execution statistics."""
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        # Get execution statistics
        stats = await self.db.execute(
            select(
                func.count(Execution.id).label("total"),
                func.count(
                    Execution.id
                ).filter(
                    Execution.status == ExecutionStatus.SUCCESS
                ).label("successful"),
                func.count(
                    Execution.id
                ).filter(
                    Execution.status == ExecutionStatus.ERROR
                ).label("failed"),
                func.avg(Execution.execution_time_ms).label("avg_time"),
                func.max(Execution.finished_at).label("last_execution"),
            ).where(Execution.workflow_id == workflow_id)
        )
        
        result = stats.one()
        
        return {
            "totalExecutions": result.total,
            "successfulExecutions": result.successful,
            "failedExecutions": result.failed,
            "averageExecutionTime": float(result.avg_time) if result.avg_time else 0,
            "lastExecutionDate": result.last_execution,
        }

    # Private helper methods

    async def _get_workflow_with_permission_check(
        self,
        workflow_id: UUID,
        user: User,
        required_permission: WorkflowPermission,
    ) -> Workflow:
        """Get workflow and check user permission."""
        result = await self.db.execute(
            select(Workflow, SharedWorkflow.permission)
            .join(SharedWorkflow)
            .where(
                and_(
                    Workflow.id == workflow_id,
                    SharedWorkflow.user_id == user.id,
                )
            )
        )
        
        row = result.one_or_none()
        if not row:
            raise WorkflowNotFoundError(f"Workflow {workflow_id} not found")
        
        workflow, permission = row
        
        # Check permission
        if not self._has_permission(permission, required_permission):
            raise WorkflowPermissionError(
                f"Insufficient permissions for workflow {workflow_id}"
            )
        
        return workflow

    def _has_permission(
        self,
        user_permission: WorkflowPermission,
        required_permission: WorkflowPermission,
    ) -> bool:
        """Check if user permission satisfies required permission."""
        permission_hierarchy = {
            WorkflowPermission.OWNER: 5,
            WorkflowPermission.SHARE: 4,
            WorkflowPermission.DELETE: 3,
            WorkflowPermission.UPDATE: 2,
            WorkflowPermission.EXECUTE: 1,
            WorkflowPermission.READ: 0,
        }
        
        return (
            permission_hierarchy[user_permission]
            >= permission_hierarchy[required_permission]
        )

    async def _validate_workflow_structure(
        self, workflow_data: Union[WorkflowCreate, WorkflowUpdate]
    ) -> None:
        """Validate workflow structure."""
        if not workflow_data.nodes:
            return
        
        # Validate node types
        for node in workflow_data.nodes:
            if not self.node_registry.is_registered(node["type"]):
                raise WorkflowValidationError(
                    f"Invalid node type: {node['type']}"
                )
            
            # Validate node has required fields
            if "id" not in node or "type" not in node:
                raise WorkflowValidationError(
                    "Node must have 'id' and 'type' fields"
                )
            
            if "position" not in node:
                raise WorkflowValidationError(
                    f"Node {node['id']} missing position"
                )
        
        # Validate connections
        if workflow_data.connections:
            node_ids = {node["id"] for node in workflow_data.nodes}
            
            for connection in workflow_data.connections:
                from_node = connection.get("from", {}).get("node")
                to_node = connection.get("to", {}).get("node")
                
                if from_node not in node_ids:
                    raise WorkflowValidationError(
                        f"Invalid connection: node {from_node} not found"
                    )
                
                if to_node not in node_ids:
                    raise WorkflowValidationError(
                        f"Invalid connection: node {to_node} not found"
                    )

    async def _validate_credential_access(
        self, user: User, workflow_data: Union[WorkflowCreate, WorkflowUpdate]
    ) -> None:
        """Validate user has access to all credentials in workflow."""
        if not workflow_data.nodes:
            return
        
        credential_ids = set()
        for node in workflow_data.nodes:
            if "credentials" in node.get("parameters", {}):
                creds = node["parameters"]["credentials"]
                if isinstance(creds, dict):
                    credential_ids.add(creds.get("id"))
                elif isinstance(creds, list):
                    credential_ids.update(c.get("id") for c in creds)
        
        # TODO: Implement credential access check
        # This would check against the credentials service
        pass

    def _ensure_node_ids(self, nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Ensure all nodes have IDs."""
        for node in nodes:
            if "id" not in node:
                node["id"] = f"node_{uuid4().hex[:8]}"
        return nodes

    async def _add_tags_to_workflow(
        self, workflow: Workflow, tags: List[str]
    ) -> None:
        """Add tags to workflow."""
        for tag_name in tags:
            tag = WorkflowTag(workflow_id=workflow.id, tag=tag_name)
            self.db.add(tag)

    async def _update_workflow_tags(
        self, workflow: Workflow, tags: List[str]
    ) -> None:
        """Update workflow tags."""
        # Remove existing tags
        await self.db.execute(
            WorkflowTag.__table__.delete().where(
                WorkflowTag.workflow_id == workflow.id
            )
        )
        
        # Add new tags
        await self._add_tags_to_workflow(workflow, tags)

    async def _activate_workflow(self, workflow: Workflow) -> None:
        """Activate a workflow."""
        # Validate workflow can be activated
        if not workflow.nodes:
            raise WorkflowValidationError("Cannot activate empty workflow")
        
        # Check for trigger nodes
        has_trigger = any(
            node["type"].startswith("trigger.") for node in workflow.nodes
        )
        
        if not has_trigger:
            raise WorkflowValidationError(
                "Workflow must have at least one trigger node"
            )
        
        workflow.active = True
        workflow.status = WorkflowStatus.ACTIVE
        
        # TODO: Register triggers with webhook/schedule services
        
        logger.info(
            "Workflow activated",
            workflow_id=workflow.id,
        )

    async def _deactivate_workflow(self, workflow: Workflow) -> None:
        """Deactivate a workflow."""
        workflow.active = False
        workflow.status = WorkflowStatus.INACTIVE
        
        # TODO: Unregister triggers from webhook/schedule services
        
        logger.info(
            "Workflow deactivated",
            workflow_id=workflow.id,
        )
    
    # Workflow composition methods
    
    async def validate_workflow_composition(
        self,
        workflow_id: UUID,
        user: User,
        max_depth: int = 10
    ) -> Dict[str, Any]:
        """Validate workflow composition for circular dependencies."""
        from .composition import WorkflowCompositionService
        
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        composition_service = WorkflowCompositionService(self.db)
        return await composition_service.validate_workflow_composition(
            workflow.id, max_depth
        )
    
    async def get_workflow_dependencies(
        self,
        workflow_id: UUID,
        user: User,
        recursive: bool = True
    ) -> List[Dict[str, Any]]:
        """Get workflows that this workflow depends on."""
        from .composition import WorkflowCompositionService
        
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        composition_service = WorkflowCompositionService(self.db)
        return await composition_service.get_workflow_dependencies(
            workflow.id, recursive
        )
    
    async def get_workflow_dependents(
        self,
        workflow_id: UUID,
        user: User
    ) -> List[Dict[str, Any]]:
        """Get workflows that depend on this workflow."""
        from .composition import WorkflowCompositionService
        
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        composition_service = WorkflowCompositionService(self.db)
        return await composition_service.get_workflow_dependents(workflow.id)
    
    async def get_workflow_composition_graph(
        self,
        workflow_id: UUID,
        user: User
    ) -> Dict[str, Any]:
        """Get the workflow composition graph."""
        from .composition import WorkflowCompositionService
        
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        composition_service = WorkflowCompositionService(self.db)
        return await composition_service.get_workflow_composition_graph(workflow.id)
    
    async def analyze_workflow_composition(
        self,
        workflow_id: UUID,
        user: User
    ) -> Dict[str, Any]:
        """Analyze workflow composition for optimization opportunities."""
        from .composition import WorkflowCompositionService
        
        workflow = await self._get_workflow_with_permission_check(
            workflow_id, user, WorkflowPermission.READ
        )
        
        composition_service = WorkflowCompositionService(self.db)
        return await composition_service.analyze_workflow_composition(workflow.id)
    
    async def get_child_executions(
        self,
        execution_id: int,
        user: User
    ) -> List[Any]:
        """Get child executions of a workflow execution."""
        from .composition import WorkflowCompositionService
        
        # TODO: Add permission check for execution
        
        composition_service = WorkflowCompositionService(self.db)
        return await composition_service.get_child_executions(execution_id)