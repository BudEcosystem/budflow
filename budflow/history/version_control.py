"""Workflow History and Version Control system for BudFlow.

This module provides comprehensive version control capabilities for workflows,
including versioning, branching, tagging, diffing, and history tracking.
"""

import asyncio
import hashlib
import json
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Set, Tuple
from uuid import UUID, uuid4

import structlog
from pydantic import BaseModel, Field, ConfigDict, field_validator
from sqlalchemy import select, and_, or_, desc, asc
from sqlalchemy.orm import selectinload

logger = structlog.get_logger()


class ChangeType(str, Enum):
    """Types of changes in workflow diffs."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    MOVED = "moved"


class MergeStrategy(str, Enum):
    """Strategies for merging conflicting changes."""
    MANUAL = "manual"
    TAKE_SOURCE = "take_source"
    TAKE_TARGET = "take_target"
    THREE_WAY = "three_way"


class ConflictResolution(BaseModel):
    """Resolution for merge conflicts."""
    
    path: List[str] = Field(..., description="Path to conflicting element")
    strategy: MergeStrategy = Field(..., description="Resolution strategy")
    resolved_value: Any = Field(default=None, description="Manually resolved value")
    comment: Optional[str] = Field(default=None, description="Resolution comment")
    
    model_config = ConfigDict(from_attributes=True)


class VersionControlError(Exception):
    """Base exception for version control operations."""
    pass


class WorkflowVersion(BaseModel):
    """Workflow version model."""
    
    id: UUID = Field(default_factory=uuid4, description="Version ID")
    workflow_id: UUID = Field(..., description="Workflow ID")
    version_number: str = Field(..., description="Semantic version number")
    content_hash: str = Field(..., description="Hash of workflow content")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: UUID = Field(..., description="User who created this version")
    message: str = Field(..., description="Version commit message")
    branch: str = Field(default="main", description="Branch name")
    tag: Optional[str] = Field(default=None, description="Version tag")
    parent_version_id: Optional[UUID] = Field(default=None, description="Parent version ID")
    is_snapshot: bool = Field(default=False, description="Whether this is a snapshot")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(from_attributes=True)
    
    @field_validator('version_number')
    @classmethod
    def validate_version_number(cls, v):
        """Validate semantic version number format."""
        if not re.match(r'^\d+\.\d+\.\d+$', v):
            raise ValueError("Version number must be in semantic version format (x.y.z)")
        return v
    
    @field_validator('branch')
    @classmethod
    def validate_branch(cls, v):
        """Validate branch name format."""
        if not v or not v.strip():
            raise ValueError("Branch name cannot be empty")
        if not re.match(r'^[a-zA-Z0-9\-_/.]+$', v.strip()):
            raise ValueError("Branch name contains invalid characters")
        return v.strip()
    
    def is_initial_version(self) -> bool:
        """Check if this is an initial version (no parent)."""
        return self.parent_version_id is None
    
    def get_version_parts(self) -> Tuple[int, int, int]:
        """Get version parts as tuple (major, minor, patch)."""
        parts = self.version_number.split('.')
        return int(parts[0]), int(parts[1]), int(parts[2])
    
    def increment_version(self, level: str = "patch") -> str:
        """Increment version number."""
        major, minor, patch = self.get_version_parts()
        
        if level == "major":
            return f"{major + 1}.0.0"
        elif level == "minor":
            return f"{major}.{minor + 1}.0"
        else:  # patch
            return f"{major}.{minor}.{patch + 1}"


class WorkflowRevision(BaseModel):
    """Workflow revision storing actual workflow data."""
    
    id: UUID = Field(default_factory=uuid4, description="Revision ID")
    workflow_id: UUID = Field(..., description="Workflow ID")
    revision_number: int = Field(..., description="Sequential revision number")
    workflow_data: Dict[str, Any] = Field(..., description="Complete workflow data")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: UUID = Field(..., description="User who created this revision")
    message: str = Field(..., description="Revision message")
    compressed: bool = Field(default=False, description="Whether data is compressed")
    
    model_config = ConfigDict(from_attributes=True)
    
    def calculate_content_hash(self) -> str:
        """Calculate SHA-256 hash of workflow content."""
        # Create deterministic JSON representation
        content_json = json.dumps(self.workflow_data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(content_json.encode('utf-8')).hexdigest()


class WorkflowDiff(BaseModel):
    """Workflow difference between two versions."""
    
    version_from: str = Field(..., description="Source version")
    version_to: str = Field(..., description="Target version")
    changes: List[Dict[str, Any]] = Field(..., description="List of changes")
    summary: str = Field(..., description="Human-readable summary")
    created_at: datetime = Field(..., description="When diff was calculated")
    stats: Dict[str, int] = Field(default_factory=dict, description="Change statistics")
    
    model_config = ConfigDict(from_attributes=True)
    
    def get_changes_by_type(self, change_type: ChangeType) -> List[Dict[str, Any]]:
        """Get changes of specific type."""
        return [change for change in self.changes if change.get("type") == change_type]
    
    def get_affected_paths(self) -> List[List[str]]:
        """Get all paths affected by changes."""
        return [change["path"] for change in self.changes]
    
    def has_breaking_changes(self) -> bool:
        """Check if diff contains breaking changes."""
        # Breaking changes include removing nodes or changing node types
        for change in self.changes:
            if change.get("type") == ChangeType.REMOVED:
                path = change.get("path", [])
                if len(path) >= 2 and path[0] == "nodes":
                    return True
            elif change.get("type") == ChangeType.MODIFIED:
                path = change.get("path", [])
                if len(path) >= 3 and path[0] == "nodes" and path[2] == "type":
                    return True
        return False


class WorkflowHistoryEntry(BaseModel):
    """Workflow history entry."""
    
    id: UUID = Field(default_factory=uuid4, description="Entry ID")
    workflow_id: UUID = Field(..., description="Workflow ID")
    version_id: Optional[UUID] = Field(default=None, description="Version ID")
    action: str = Field(..., description="Action performed")
    timestamp: datetime = Field(..., description="When action occurred")
    user_id: UUID = Field(..., description="User who performed action")
    changes_summary: str = Field(..., description="Summary of changes")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(from_attributes=True)


class VersionBranch(BaseModel):
    """Version branch model."""
    
    name: str = Field(..., description="Branch name")
    workflow_id: UUID = Field(..., description="Workflow ID")
    created_from_version: str = Field(..., description="Version this branch was created from")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: UUID = Field(..., description="User who created branch")
    description: Optional[str] = Field(default=None, description="Branch description")
    is_active: bool = Field(default=True, description="Whether branch is active")
    merged_to: Optional[str] = Field(default=None, description="Branch this was merged to")
    merged_at: Optional[datetime] = Field(default=None, description="When branch was merged")
    
    model_config = ConfigDict(from_attributes=True)
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        """Validate branch name."""
        if not v or not v.strip():
            raise ValueError("Branch name cannot be empty")
        name = v.strip()
        
        # Check if name differs from trimmed version (has leading/trailing spaces)
        if v != name:
            raise ValueError("Branch name cannot start/end with / or spaces")
        
        # Check for spaces in the middle
        if ' ' in name:
            raise ValueError("Branch name contains invalid characters")
            
        if not re.match(r'^[a-zA-Z0-9\-_/.]+$', name):
            raise ValueError("Branch name contains invalid characters")
        
        if name.startswith('/') or name.endswith('/'):
            raise ValueError("Branch name cannot start/end with / or spaces")
        
        return name


class VersionTag(BaseModel):
    """Version tag model."""
    
    name: str = Field(..., description="Tag name")
    version_id: UUID = Field(..., description="Version ID")
    workflow_id: UUID = Field(..., description="Workflow ID")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: UUID = Field(..., description="User who created tag")
    message: Optional[str] = Field(default=None, description="Tag message")
    is_release: bool = Field(default=False, description="Whether this is a release tag")
    
    model_config = ConfigDict(from_attributes=True)


class WorkflowSnapshot(BaseModel):
    """Workflow snapshot for quick access."""
    
    id: UUID = Field(default_factory=uuid4, description="Snapshot ID")
    workflow_id: UUID = Field(..., description="Workflow ID")
    name: str = Field(..., description="Snapshot name")
    description: Optional[str] = Field(default=None, description="Snapshot description")
    workflow_data: Dict[str, Any] = Field(..., description="Workflow data")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: UUID = Field(..., description="User who created snapshot")
    auto_generated: bool = Field(default=False, description="Whether auto-generated")
    
    model_config = ConfigDict(from_attributes=True)


class HistoryFilter(BaseModel):
    """Filter for workflow history queries."""
    
    user_id: Optional[UUID] = Field(default=None, description="Filter by user")
    action: Optional[str] = Field(default=None, description="Filter by action")
    date_from: Optional[datetime] = Field(default=None, description="Filter from date")
    date_to: Optional[datetime] = Field(default=None, description="Filter to date")
    branch: Optional[str] = Field(default=None, description="Filter by branch")
    limit: int = Field(default=50, description="Maximum results")
    offset: int = Field(default=0, description="Results offset")
    
    model_config = ConfigDict(from_attributes=True)


class ComparisonResult(BaseModel):
    """Result of comparing two workflow versions."""
    
    version_from: str = Field(..., description="Source version")
    version_to: str = Field(..., description="Target version")
    diff: WorkflowDiff = Field(..., description="Calculated diff")
    is_compatible: bool = Field(..., description="Whether versions are compatible")
    migration_required: bool = Field(..., description="Whether migration is needed")
    recommendations: List[str] = Field(default_factory=list, description="Upgrade recommendations")
    
    model_config = ConfigDict(from_attributes=True)


class RestorePoint(BaseModel):
    """Restore point for workflow recovery."""
    
    id: UUID = Field(default_factory=uuid4, description="Restore point ID")
    workflow_id: UUID = Field(..., description="Workflow ID")
    version_id: UUID = Field(..., description="Version ID")
    name: str = Field(..., description="Restore point name")
    description: Optional[str] = Field(default=None, description="Description")
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: UUID = Field(..., description="User who created restore point")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(from_attributes=True)


class WorkflowVersionManager:
    """Manager for workflow version control operations."""
    
    def __init__(self):
        self.logger = logger.bind(component="workflow_version_manager")
    
    async def create_version(
        self,
        workflow_id: UUID,
        workflow_data: Dict[str, Any],
        user_id: UUID,
        message: str,
        branch: str = "main",
        parent_version_id: Optional[UUID] = None,
        version_override: Optional[str] = None
    ) -> WorkflowVersion:
        """Create a new workflow version."""
        try:
            self.logger.info("Creating workflow version", 
                           workflow_id=str(workflow_id), 
                           branch=branch, 
                           message=message)
            
            # Calculate content hash
            content_json = json.dumps(workflow_data, sort_keys=True, separators=(',', ':'))
            content_hash = hashlib.sha256(content_json.encode('utf-8')).hexdigest()
            
            # Determine version number
            if version_override:
                version_number = version_override
            else:
                if parent_version_id:
                    parent_version = await self._get_version_by_id(parent_version_id)
                    version_number = parent_version.increment_version("minor")
                else:
                    # Check if there's already a version for this workflow
                    latest = await self._get_latest_version(workflow_id, branch)
                    if latest:
                        version_number = latest.increment_version("minor")
                    else:
                        version_number = "1.0.0"
            
            # Create version
            version = WorkflowVersion(
                workflow_id=workflow_id,
                version_number=version_number,
                content_hash=content_hash,
                created_at=datetime.now(timezone.utc),
                created_by=user_id,
                message=message,
                branch=branch,
                parent_version_id=parent_version_id
            )
            
            # Create revision
            revision_number = await self._get_next_revision_number(workflow_id)
            revision = WorkflowRevision(
                workflow_id=workflow_id,
                revision_number=revision_number,
                workflow_data=workflow_data,
                created_at=datetime.now(timezone.utc),
                created_by=user_id,
                message=message
            )
            
            # Save version and revision
            await self._save_version(version)
            await self._save_revision(revision)
            
            # Create history entry
            await self._create_history_entry(
                workflow_id=workflow_id,
                version_id=version.id,
                action="version_created",
                user_id=user_id,
                changes_summary=f"Created version {version_number}: {message}"
            )
            
            self.logger.info("Workflow version created", 
                           version_id=str(version.id),
                           version_number=version_number)
            
            return version
            
        except Exception as e:
            self.logger.error("Failed to create workflow version", 
                            workflow_id=str(workflow_id), 
                            error=str(e))
            raise VersionControlError(f"Failed to create version: {str(e)}")
    
    async def get_version(
        self,
        workflow_id: UUID,
        version_number: str,
        branch: str = "main"
    ) -> Optional[WorkflowVersion]:
        """Get specific workflow version."""
        try:
            return await self._load_version(workflow_id, version_number, branch)
        except Exception as e:
            self.logger.error("Failed to get workflow version", 
                            workflow_id=str(workflow_id), 
                            version_number=version_number,
                            error=str(e))
            raise VersionControlError(f"Failed to get version: {str(e)}")
    
    async def list_versions(
        self,
        workflow_id: UUID,
        branch: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[WorkflowVersion]:
        """List workflow versions."""
        try:
            return await self._load_versions(workflow_id, branch, limit, offset)
        except Exception as e:
            self.logger.error("Failed to list workflow versions", 
                            workflow_id=str(workflow_id), 
                            error=str(e))
            raise VersionControlError(f"Failed to list versions: {str(e)}")
    
    async def get_revision(
        self,
        workflow_id: UUID,
        version_number: str,
        branch: str = "main"
    ) -> Optional[WorkflowRevision]:
        """Get workflow revision data."""
        try:
            version = await self._load_version(workflow_id, version_number, branch)
            if not version:
                return None
            return await self._load_revision_by_version(version.id)
        except Exception as e:
            self.logger.error("Failed to get workflow revision", 
                            workflow_id=str(workflow_id), 
                            version_number=version_number,
                            error=str(e))
            raise VersionControlError(f"Failed to get revision: {str(e)}")
    
    async def compare_versions(
        self,
        workflow_id: UUID,
        version_from: str,
        version_to: str,
        branch: str = "main"
    ) -> WorkflowDiff:
        """Compare two workflow versions."""
        try:
            self.logger.info("Comparing workflow versions", 
                           workflow_id=str(workflow_id),
                           version_from=version_from, 
                           version_to=version_to)
            
            # Load versions
            from_version = await self._load_version(workflow_id, version_from, branch)
            to_version = await self._load_version(workflow_id, version_to, branch)
            
            if not from_version or not to_version:
                raise VersionControlError("One or both versions not found")
            
            # Load revisions
            from_revision = await self._load_revision_by_version(from_version.id)
            to_revision = await self._load_revision_by_version(to_version.id)
            
            if not from_revision or not to_revision:
                raise VersionControlError("Revision data not found")
            
            # Calculate diff
            diff = await self._calculate_diff(
                from_revision.workflow_data,
                to_revision.workflow_data
            )
            
            diff.version_from = version_from
            diff.version_to = version_to
            
            return diff
            
        except Exception as e:
            self.logger.error("Failed to compare workflow versions", 
                            workflow_id=str(workflow_id), 
                            error=str(e))
            raise VersionControlError(f"Failed to compare versions: {str(e)}")
    
    async def create_branch(
        self,
        workflow_id: UUID,
        branch_name: str,
        from_version: str,
        user_id: UUID,
        description: Optional[str] = None
    ) -> VersionBranch:
        """Create a new version branch."""
        try:
            self.logger.info("Creating version branch", 
                           workflow_id=str(workflow_id),
                           branch_name=branch_name, 
                           from_version=from_version)
            
            branch = VersionBranch(
                name=branch_name,
                workflow_id=workflow_id,
                created_from_version=from_version,
                created_at=datetime.now(timezone.utc),
                created_by=user_id,
                description=description
            )
            
            await self._save_branch(branch)
            
            self.logger.info("Version branch created", 
                           branch_name=branch_name)
            
            return branch
            
        except Exception as e:
            self.logger.error("Failed to create version branch", 
                            workflow_id=str(workflow_id), 
                            branch_name=branch_name,
                            error=str(e))
            raise VersionControlError(f"Failed to create branch: {str(e)}")
    
    async def create_tag(
        self,
        version_id: UUID,
        workflow_id: UUID,
        tag_name: str,
        user_id: UUID,
        message: Optional[str] = None,
        is_release: bool = False
    ) -> VersionTag:
        """Create a version tag."""
        try:
            self.logger.info("Creating version tag", 
                           version_id=str(version_id),
                           tag_name=tag_name)
            
            tag = VersionTag(
                name=tag_name,
                version_id=version_id,
                workflow_id=workflow_id,
                created_at=datetime.now(timezone.utc),
                created_by=user_id,
                message=message,
                is_release=is_release
            )
            
            await self._save_tag(tag)
            
            self.logger.info("Version tag created", 
                           tag_name=tag_name)
            
            return tag
            
        except Exception as e:
            self.logger.error("Failed to create version tag", 
                            version_id=str(version_id), 
                            tag_name=tag_name,
                            error=str(e))
            raise VersionControlError(f"Failed to create tag: {str(e)}")
    
    async def restore_version(
        self,
        workflow_id: UUID,
        target_version: str,
        user_id: UUID,
        branch: str = "main"
    ) -> WorkflowVersion:
        """Restore workflow to a specific version."""
        try:
            self.logger.info("Restoring workflow version", 
                           workflow_id=str(workflow_id),
                           target_version=target_version)
            
            # Load target version
            target = await self._load_version(workflow_id, target_version, branch)
            if not target:
                raise VersionControlError(f"Target version {target_version} not found")
            
            # Load revision data
            target_revision = await self._load_revision_by_version(target.id)
            if not target_revision:
                raise VersionControlError("Target revision data not found")
            
            # Create new version with restored data
            new_version = await self.create_version(
                workflow_id=workflow_id,
                workflow_data=target_revision.workflow_data,
                user_id=user_id,
                message=f"Restored from version {target_version}",
                branch=branch
            )
            
            self.logger.info("Workflow version restored", 
                           new_version_id=str(new_version.id),
                           restored_from=target_version)
            
            return new_version
            
        except Exception as e:
            self.logger.error("Failed to restore workflow version", 
                            workflow_id=str(workflow_id), 
                            target_version=target_version,
                            error=str(e))
            raise VersionControlError(f"Failed to restore version: {str(e)}")
    
    async def get_workflow_history(
        self,
        workflow_id: UUID,
        filter_params: Optional[HistoryFilter] = None,
        limit: int = 50
    ) -> List[WorkflowHistoryEntry]:
        """Get workflow history."""
        try:
            if not filter_params:
                filter_params = HistoryFilter(limit=limit)
            
            return await self._load_history(workflow_id, filter_params)
            
        except Exception as e:
            self.logger.error("Failed to get workflow history", 
                            workflow_id=str(workflow_id), 
                            error=str(e))
            raise VersionControlError(f"Failed to get history: {str(e)}")
    
    async def detect_merge_conflicts(
        self,
        workflow_id: UUID,
        source_branch: str,
        target_branch: str
    ) -> List[str]:
        """Detect merge conflicts between branches."""
        try:
            # This is a simplified implementation
            # In a real system, you'd implement proper 3-way merge conflict detection
            conflicts = await self._detect_conflicts(workflow_id, source_branch, target_branch)
            return conflicts
            
        except Exception as e:
            self.logger.error("Failed to detect merge conflicts", 
                            workflow_id=str(workflow_id), 
                            error=str(e))
            raise VersionControlError(f"Failed to detect conflicts: {str(e)}")
    
    # Private helper methods
    
    async def _save_version(self, version: WorkflowVersion) -> None:
        """Save version to database."""
        # In a real implementation, this would save to database
        self.logger.debug("Saving version", version_id=str(version.id))
    
    async def _save_revision(self, revision: WorkflowRevision) -> None:
        """Save revision to database."""
        # In a real implementation, this would save to database
        self.logger.debug("Saving revision", revision_id=str(revision.id))
    
    async def _save_branch(self, branch: VersionBranch) -> None:
        """Save branch to database."""
        # In a real implementation, this would save to database
        self.logger.debug("Saving branch", branch_name=branch.name)
    
    async def _save_tag(self, tag: VersionTag) -> None:
        """Save tag to database."""
        # In a real implementation, this would save to database
        self.logger.debug("Saving tag", tag_name=tag.name)
    
    async def _load_version(
        self,
        workflow_id: UUID,
        version_number: str,
        branch: str = "main"
    ) -> Optional[WorkflowVersion]:
        """Load version from database."""
        # In a real implementation, this would load from database
        self.logger.debug("Loading version", 
                         workflow_id=str(workflow_id), 
                         version_number=version_number)
        return None
    
    async def _load_versions(
        self,
        workflow_id: UUID,
        branch: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[WorkflowVersion]:
        """Load versions from database."""
        # In a real implementation, this would load from database
        self.logger.debug("Loading versions", workflow_id=str(workflow_id))
        return []
    
    async def _load_revision_by_version(self, version_id: UUID) -> Optional[WorkflowRevision]:
        """Load revision by version ID."""
        # In a real implementation, this would load from database
        self.logger.debug("Loading revision", version_id=str(version_id))
        return None
    
    async def _get_latest_version(
        self,
        workflow_id: UUID,
        branch: str = "main"
    ) -> Optional[WorkflowVersion]:
        """Get latest version for workflow and branch."""
        # In a real implementation, this would query database
        self.logger.debug("Getting latest version", 
                         workflow_id=str(workflow_id), 
                         branch=branch)
        return None
    
    async def _get_version_by_id(self, version_id: UUID) -> Optional[WorkflowVersion]:
        """Get version by ID."""
        # In a real implementation, this would query database
        self.logger.debug("Getting version by ID", version_id=str(version_id))
        return None
    
    async def _get_next_revision_number(self, workflow_id: UUID) -> int:
        """Get next revision number for workflow."""
        # In a real implementation, this would query database
        return 1
    
    async def _create_history_entry(
        self,
        workflow_id: UUID,
        version_id: Optional[UUID],
        action: str,
        user_id: UUID,
        changes_summary: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create workflow history entry."""
        entry = WorkflowHistoryEntry(
            workflow_id=workflow_id,
            version_id=version_id,
            action=action,
            timestamp=datetime.now(timezone.utc),
            user_id=user_id,
            changes_summary=changes_summary,
            metadata=metadata or {}
        )
        
        # In a real implementation, this would save to database
        self.logger.debug("Creating history entry", 
                         workflow_id=str(workflow_id), 
                         action=action)
    
    async def _load_history(
        self,
        workflow_id: UUID,
        filter_params: HistoryFilter
    ) -> List[WorkflowHistoryEntry]:
        """Load workflow history."""
        # In a real implementation, this would load from database
        self.logger.debug("Loading history", workflow_id=str(workflow_id))
        return []
    
    async def _calculate_diff(
        self,
        data_from: Dict[str, Any],
        data_to: Dict[str, Any]
    ) -> WorkflowDiff:
        """Calculate diff between two workflow data structures."""
        changes = []
        
        # Simplified diff calculation
        # In a real implementation, you'd use a more sophisticated algorithm
        changes = self._deep_diff(data_from, data_to, [])
        
        # Calculate summary
        added = len([c for c in changes if c.get("type") == ChangeType.ADDED])
        removed = len([c for c in changes if c.get("type") == ChangeType.REMOVED])
        modified = len([c for c in changes if c.get("type") == ChangeType.MODIFIED])
        
        summary = f"{added} added, {modified} modified, {removed} removed"
        
        return WorkflowDiff(
            version_from="",  # Will be set by caller
            version_to="",    # Will be set by caller
            changes=changes,
            summary=summary,
            created_at=datetime.now(timezone.utc),
            stats={"added": added, "modified": modified, "removed": removed}
        )
    
    def _deep_diff(
        self,
        obj1: Any,
        obj2: Any,
        path: List[str]
    ) -> List[Dict[str, Any]]:
        """Recursively calculate deep differences."""
        changes = []
        
        if isinstance(obj1, dict) and isinstance(obj2, dict):
            # Handle dictionaries
            all_keys = set(obj1.keys()) | set(obj2.keys())
            
            for key in all_keys:
                current_path = path + [key]
                
                if key not in obj1:
                    changes.append({
                        "type": ChangeType.ADDED,
                        "path": current_path,
                        "old_value": None,
                        "new_value": obj2[key]
                    })
                elif key not in obj2:
                    changes.append({
                        "type": ChangeType.REMOVED,
                        "path": current_path,
                        "old_value": obj1[key],
                        "new_value": None
                    })
                elif obj1[key] != obj2[key]:
                    if isinstance(obj1[key], (dict, list)) and isinstance(obj2[key], (dict, list)):
                        changes.extend(self._deep_diff(obj1[key], obj2[key], current_path))
                    else:
                        changes.append({
                            "type": ChangeType.MODIFIED,
                            "path": current_path,
                            "old_value": obj1[key],
                            "new_value": obj2[key]
                        })
        
        elif isinstance(obj1, list) and isinstance(obj2, list):
            # Handle lists
            max_len = max(len(obj1), len(obj2))
            
            for i in range(max_len):
                current_path = path + [i]
                
                if i >= len(obj1):
                    changes.append({
                        "type": ChangeType.ADDED,
                        "path": current_path,
                        "old_value": None,
                        "new_value": obj2[i]
                    })
                elif i >= len(obj2):
                    changes.append({
                        "type": ChangeType.REMOVED,
                        "path": current_path,
                        "old_value": obj1[i],
                        "new_value": None
                    })
                elif obj1[i] != obj2[i]:
                    if isinstance(obj1[i], (dict, list)) and isinstance(obj2[i], (dict, list)):
                        changes.extend(self._deep_diff(obj1[i], obj2[i], current_path))
                    else:
                        changes.append({
                            "type": ChangeType.MODIFIED,
                            "path": current_path,
                            "old_value": obj1[i],
                            "new_value": obj2[i]
                        })
        
        elif obj1 != obj2:
            changes.append({
                "type": ChangeType.MODIFIED,
                "path": path,
                "old_value": obj1,
                "new_value": obj2
            })
        
        return changes
    
    async def _detect_conflicts(
        self,
        workflow_id: UUID,
        source_branch: str,
        target_branch: str
    ) -> List[str]:
        """Detect conflicts between branches."""
        # Simplified conflict detection
        # In a real implementation, you'd do proper 3-way merge analysis
        return []