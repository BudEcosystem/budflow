"""Test Workflow History and Version Control system."""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from uuid import UUID, uuid4

from budflow.history.version_control import (
    WorkflowVersionManager,
    WorkflowVersion,
    WorkflowRevision,
    WorkflowDiff,
    WorkflowHistoryEntry,
    VersionControlError,
    ConflictResolution,
    MergeStrategy,
    ChangeType,
    VersionBranch,
    VersionTag,
    WorkflowSnapshot,
    HistoryFilter,
    ComparisonResult,
    RestorePoint,
)


@pytest.fixture
def sample_workflow_data():
    """Create sample workflow data for testing."""
    return {
        "name": "Test Workflow",
        "description": "A test workflow",
        "active": True,
        "nodes": [
            {
                "id": "node1",
                "type": "trigger.manual",
                "position": {"x": 100, "y": 100},
                "parameters": {}
            },
            {
                "id": "node2", 
                "type": "action.http",
                "position": {"x": 300, "y": 100},
                "parameters": {
                    "url": "https://api.example.com",
                    "method": "GET"
                }
            }
        ],
        "connections": [
            {
                "source": "node1",
                "target": "node2",
                "source_output": "main",
                "target_input": "main"
            }
        ],
        "settings": {
            "timezone": "UTC",
            "error_workflow": None
        }
    }


@pytest.fixture
def modified_workflow_data(sample_workflow_data):
    """Create modified workflow data for testing."""
    data = sample_workflow_data.copy()
    data["description"] = "Modified test workflow"
    data["nodes"][1]["parameters"]["method"] = "POST"
    data["nodes"].append({
        "id": "node3",
        "type": "action.email",
        "position": {"x": 500, "y": 100},
        "parameters": {
            "to": "test@example.com",
            "subject": "Test"
        }
    })
    return data


@pytest.fixture
def workflow_version_manager():
    """Create WorkflowVersionManager for testing."""
    return WorkflowVersionManager()


@pytest.fixture
def sample_workflow_version():
    """Create sample workflow version for testing."""
    return WorkflowVersion(
        id=uuid4(),
        workflow_id=uuid4(),
        version_number="1.0.0",
        content_hash="abc123def456",
        created_at=datetime.now(timezone.utc),
        created_by=uuid4(),
        message="Initial version",
        branch="main",
        tag=None,
        parent_version_id=None
    )


@pytest.fixture
def sample_workflow_diff():
    """Create sample workflow diff for testing."""
    return WorkflowDiff(
        version_from="1.0.0",
        version_to="1.1.0",
        changes=[
            {
                "type": ChangeType.MODIFIED,
                "path": ["nodes", 1, "parameters", "method"],
                "old_value": "GET",
                "new_value": "POST"
            },
            {
                "type": ChangeType.ADDED,
                "path": ["nodes", 2],
                "old_value": None,
                "new_value": {
                    "id": "node3",
                    "type": "action.email",
                    "position": {"x": 500, "y": 100},
                    "parameters": {
                        "to": "test@example.com",
                        "subject": "Test"
                    }
                }
            }
        ],
        summary="Added email node and changed HTTP method",
        created_at=datetime.now(timezone.utc)
    )


@pytest.mark.unit
class TestWorkflowVersion:
    """Test WorkflowVersion model."""
    
    def test_workflow_version_creation(self, sample_workflow_version):
        """Test creating workflow version."""
        assert sample_workflow_version.version_number == "1.0.0"
        assert sample_workflow_version.branch == "main"
        assert sample_workflow_version.message == "Initial version"
        assert sample_workflow_version.tag is None
        assert sample_workflow_version.parent_version_id is None
    
    def test_workflow_version_serialization(self, sample_workflow_version):
        """Test workflow version serialization."""
        data = sample_workflow_version.model_dump()
        
        assert "id" in data
        assert "workflow_id" in data
        assert "version_number" in data
        assert "content_hash" in data
        assert "created_at" in data
        
        # Test deserialization
        restored = WorkflowVersion.model_validate(data)
        assert restored.version_number == sample_workflow_version.version_number
        assert restored.content_hash == sample_workflow_version.content_hash
    
    def test_version_number_validation(self):
        """Test version number validation."""
        # Valid semantic versions
        valid_versions = ["1.0.0", "2.1.3", "0.0.1", "10.20.30"]
        for version in valid_versions:
            wv = WorkflowVersion(
                id=uuid4(),
                workflow_id=uuid4(),
                version_number=version,
                content_hash="test",
                created_at=datetime.now(timezone.utc),
                created_by=uuid4(),
                message="Test"
            )
            assert wv.version_number == version
        
        # Invalid versions should raise ValueError
        invalid_versions = ["1.0", "1", "v1.0.0", "1.0.0-alpha", ""]
        for version in invalid_versions:
            with pytest.raises(ValueError):
                WorkflowVersion(
                    id=uuid4(),
                    workflow_id=uuid4(),
                    version_number=version,
                    content_hash="test",
                    created_at=datetime.now(timezone.utc),
                    created_by=uuid4(),
                    message="Test"
                )
    
    def test_is_initial_version(self, sample_workflow_version):
        """Test checking if version is initial."""
        # Version with no parent is initial
        assert sample_workflow_version.is_initial_version()
        
        # Version with parent is not initial
        sample_workflow_version.parent_version_id = uuid4()
        assert not sample_workflow_version.is_initial_version()
    
    def test_get_version_parts(self, sample_workflow_version):
        """Test getting version parts."""
        major, minor, patch = sample_workflow_version.get_version_parts()
        assert major == 1
        assert minor == 0
        assert patch == 0


@pytest.mark.unit
class TestWorkflowRevision:
    """Test WorkflowRevision model."""
    
    def test_workflow_revision_creation(self):
        """Test creating workflow revision."""
        revision = WorkflowRevision(
            id=uuid4(),
            workflow_id=uuid4(),
            revision_number=1,
            workflow_data={"test": "data"},
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Test revision"
        )
        
        assert revision.revision_number == 1
        assert revision.workflow_data == {"test": "data"}
        assert revision.message == "Test revision"
    
    def test_calculate_content_hash(self):
        """Test calculating content hash."""
        revision = WorkflowRevision(
            id=uuid4(),
            workflow_id=uuid4(),
            revision_number=1,
            workflow_data={"nodes": [], "connections": []},
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Test"
        )
        
        hash1 = revision.calculate_content_hash()
        assert len(hash1) == 64  # SHA-256 hex digest
        
        # Same data should produce same hash
        hash2 = revision.calculate_content_hash()
        assert hash1 == hash2
        
        # Different data should produce different hash
        revision.workflow_data = {"nodes": [{"id": "test"}], "connections": []}
        hash3 = revision.calculate_content_hash()
        assert hash1 != hash3


@pytest.mark.unit
class TestWorkflowDiff:
    """Test WorkflowDiff model."""
    
    def test_workflow_diff_creation(self, sample_workflow_diff):
        """Test creating workflow diff."""
        assert sample_workflow_diff.version_from == "1.0.0"
        assert sample_workflow_diff.version_to == "1.1.0"
        assert len(sample_workflow_diff.changes) == 2
        assert sample_workflow_diff.summary == "Added email node and changed HTTP method"
    
    def test_get_changes_by_type(self, sample_workflow_diff):
        """Test getting changes by type."""
        added_changes = sample_workflow_diff.get_changes_by_type(ChangeType.ADDED)
        modified_changes = sample_workflow_diff.get_changes_by_type(ChangeType.MODIFIED)
        
        assert len(added_changes) == 1
        assert len(modified_changes) == 1
        assert added_changes[0]["path"] == ["nodes", 2]
        assert modified_changes[0]["path"] == ["nodes", 1, "parameters", "method"]
    
    def test_get_affected_paths(self, sample_workflow_diff):
        """Test getting affected paths."""
        paths = sample_workflow_diff.get_affected_paths()
        assert ["nodes", 1, "parameters", "method"] in paths
        assert ["nodes", 2] in paths
    
    def test_has_breaking_changes(self, sample_workflow_diff):
        """Test checking for breaking changes."""
        # By default, no breaking changes
        assert not sample_workflow_diff.has_breaking_changes()
        
        # Add a breaking change (removing a node)
        sample_workflow_diff.changes.append({
            "type": ChangeType.REMOVED,
            "path": ["nodes", 0],
            "old_value": {"id": "node1", "type": "trigger.manual"},
            "new_value": None
        })
        
        assert sample_workflow_diff.has_breaking_changes()


@pytest.mark.unit  
class TestWorkflowHistoryEntry:
    """Test WorkflowHistoryEntry model."""
    
    def test_history_entry_creation(self):
        """Test creating history entry."""
        entry = WorkflowHistoryEntry(
            id=uuid4(),
            workflow_id=uuid4(),
            version_id=uuid4(),
            action="created",
            timestamp=datetime.now(timezone.utc),
            user_id=uuid4(),
            changes_summary="Initial workflow creation",
            metadata={"source": "web_editor"}
        )
        
        assert entry.action == "created"
        assert entry.changes_summary == "Initial workflow creation"
        assert entry.metadata["source"] == "web_editor"


@pytest.mark.unit
class TestVersionBranch:
    """Test VersionBranch model."""
    
    def test_branch_creation(self):
        """Test creating version branch."""
        branch = VersionBranch(
            name="feature/new-nodes",
            workflow_id=uuid4(),
            created_from_version="1.0.0",
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            description="Adding new node types",
            is_active=True
        )
        
        assert branch.name == "feature/new-nodes"
        assert branch.created_from_version == "1.0.0"
        assert branch.is_active is True
        assert branch.description == "Adding new node types"
    
    def test_branch_name_validation(self):
        """Test branch name validation."""
        # Valid branch names
        valid_names = ["main", "develop", "feature/new-feature", "hotfix/bug-123", "release/v1.0"]
        for name in valid_names:
            branch = VersionBranch(
                name=name,
                workflow_id=uuid4(),
                created_from_version="1.0.0",
                created_at=datetime.now(timezone.utc),
                created_by=uuid4()
            )
            assert branch.name == name
        
        # Invalid branch names
        invalid_names = ["", " ", "main/", "/feature", "main ", " main", "ma in"]
        for name in invalid_names:
            with pytest.raises(ValueError):
                VersionBranch(
                    name=name,
                    workflow_id=uuid4(),
                    created_from_version="1.0.0",
                    created_at=datetime.now(timezone.utc),
                    created_by=uuid4()
                )


@pytest.mark.unit
class TestVersionTag:
    """Test VersionTag model."""
    
    def test_tag_creation(self):
        """Test creating version tag."""
        tag = VersionTag(
            name="v1.0.0",
            version_id=uuid4(),
            workflow_id=uuid4(),
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Release version 1.0.0",
            is_release=True
        )
        
        assert tag.name == "v1.0.0"
        assert tag.message == "Release version 1.0.0"
        assert tag.is_release is True


@pytest.mark.unit
class TestWorkflowVersionManager:
    """Test WorkflowVersionManager."""
    
    def test_manager_initialization(self, workflow_version_manager):
        """Test manager initialization."""
        assert workflow_version_manager is not None
        assert hasattr(workflow_version_manager, 'create_version')
        assert hasattr(workflow_version_manager, 'get_version')
        assert hasattr(workflow_version_manager, 'list_versions')
    
    @pytest.mark.asyncio
    async def test_create_initial_version(self, workflow_version_manager, sample_workflow_data):
        """Test creating initial workflow version."""
        workflow_id = uuid4()
        user_id = uuid4()
        
        with patch.object(workflow_version_manager, '_get_next_revision_number') as mock_get_next_rev:
            with patch.object(workflow_version_manager, '_save_version') as mock_save:
                with patch.object(workflow_version_manager, '_save_revision') as mock_save_revision:
                    mock_get_next_rev.return_value = 1
                    mock_save.return_value = None
                    mock_save_revision.return_value = None
                
                    version = await workflow_version_manager.create_version(
                        workflow_id=workflow_id,
                        workflow_data=sample_workflow_data,
                        user_id=user_id,
                        message="Initial version"
                    )
                    
                    assert version.version_number == "1.0.0"
                    assert version.workflow_id == workflow_id
                    assert version.message == "Initial version"
                    assert version.parent_version_id is None
                    assert version.branch == "main"
                    
                    mock_save.assert_called_once()
                    mock_save_revision.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_subsequent_version(self, workflow_version_manager, sample_workflow_data, modified_workflow_data):
        """Test creating subsequent workflow version."""
        workflow_id = uuid4()
        user_id = uuid4()
        parent_version_id = uuid4()
        
        # Mock existing parent version
        parent_version = WorkflowVersion(
            id=parent_version_id,
            workflow_id=workflow_id,
            version_number="1.0.0",
            content_hash="old_hash",
            created_at=datetime.now(timezone.utc),
            created_by=user_id,
            message="Initial version",
            branch="main"
        )
        
        with patch.object(workflow_version_manager, '_get_latest_version') as mock_get_latest:
            with patch.object(workflow_version_manager, '_get_version_by_id') as mock_get_by_id:
                with patch.object(workflow_version_manager, '_get_next_revision_number') as mock_get_next_rev:
                    with patch.object(workflow_version_manager, '_save_version') as mock_save:
                        with patch.object(workflow_version_manager, '_save_revision') as mock_save_revision:
                            mock_get_latest.return_value = parent_version
                            mock_get_by_id.return_value = parent_version
                            mock_get_next_rev.return_value = 2
                            mock_save.return_value = None
                            mock_save_revision.return_value = None
                        
                            version = await workflow_version_manager.create_version(
                                workflow_id=workflow_id,
                                workflow_data=modified_workflow_data,
                                user_id=user_id,
                                message="Added email node",
                                parent_version_id=parent_version_id
                            )
                            
                            assert version.version_number == "1.1.0"  # Minor version bump
                            assert version.parent_version_id == parent_version_id
                            assert version.message == "Added email node"
                            
                            mock_save.assert_called_once()
                            mock_save_revision.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_version(self, workflow_version_manager, sample_workflow_version):
        """Test getting workflow version."""
        with patch.object(workflow_version_manager, '_load_version') as mock_load:
            mock_load.return_value = sample_workflow_version
            
            result = await workflow_version_manager.get_version(
                workflow_id=sample_workflow_version.workflow_id,
                version_number="1.0.0"
            )
            
            assert result == sample_workflow_version
            mock_load.assert_called_once_with(sample_workflow_version.workflow_id, "1.0.0", "main")
    
    @pytest.mark.asyncio
    async def test_list_versions(self, workflow_version_manager):
        """Test listing workflow versions."""
        workflow_id = uuid4()
        versions = [
            WorkflowVersion(
                id=uuid4(),
                workflow_id=workflow_id,
                version_number="1.0.0",
                content_hash="hash1",
                created_at=datetime.now(timezone.utc),
                created_by=uuid4(),
                message="Initial"
            ),
            WorkflowVersion(
                id=uuid4(),
                workflow_id=workflow_id,
                version_number="1.1.0",
                content_hash="hash2",
                created_at=datetime.now(timezone.utc),
                created_by=uuid4(),
                message="Update"
            )
        ]
        
        with patch.object(workflow_version_manager, '_load_versions') as mock_load:
            mock_load.return_value = versions
            
            result = await workflow_version_manager.list_versions(workflow_id)
            
            assert len(result) == 2
            assert result[0].version_number == "1.0.0"
            assert result[1].version_number == "1.1.0"
            mock_load.assert_called_once_with(workflow_id, None, None, None)
    
    @pytest.mark.asyncio
    async def test_compare_versions(self, workflow_version_manager, sample_workflow_data, modified_workflow_data):
        """Test comparing workflow versions."""
        workflow_id = uuid4()
        
        version1 = WorkflowVersion(
            id=uuid4(),
            workflow_id=workflow_id,
            version_number="1.0.0",
            content_hash="hash1",
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Initial"
        )
        
        version2 = WorkflowVersion(
            id=uuid4(),
            workflow_id=workflow_id,
            version_number="1.1.0",
            content_hash="hash2",
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Updated"
        )
        
        revision1 = WorkflowRevision(
            id=uuid4(),
            workflow_id=workflow_id,
            revision_number=1,
            workflow_data=sample_workflow_data,
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Initial"
        )
        
        revision2 = WorkflowRevision(
            id=uuid4(),
            workflow_id=workflow_id,
            revision_number=2,
            workflow_data=modified_workflow_data,
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            message="Updated"
        )
        
        with patch.object(workflow_version_manager, '_load_version') as mock_load_version:
            with patch.object(workflow_version_manager, '_load_revision_by_version') as mock_load_revision:
                mock_load_version.side_effect = [version1, version2]
                mock_load_revision.side_effect = [revision1, revision2]
                
                diff = await workflow_version_manager.compare_versions(
                    workflow_id=workflow_id,
                    version_from="1.0.0",
                    version_to="1.1.0"
                )
                
                assert diff.version_from == "1.0.0"
                assert diff.version_to == "1.1.0"
                assert len(diff.changes) > 0
    
    @pytest.mark.asyncio
    async def test_create_branch(self, workflow_version_manager):
        """Test creating version branch."""
        workflow_id = uuid4()
        user_id = uuid4()
        
        with patch.object(workflow_version_manager, '_save_branch') as mock_save:
            mock_save.return_value = None
            
            branch = await workflow_version_manager.create_branch(
                workflow_id=workflow_id,
                branch_name="feature/new-feature",
                from_version="1.0.0",
                user_id=user_id,
                description="Adding new feature"
            )
            
            assert branch.name == "feature/new-feature"
            assert branch.created_from_version == "1.0.0"
            assert branch.workflow_id == workflow_id
            assert branch.created_by == user_id
            
            mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_tag(self, workflow_version_manager):
        """Test creating version tag."""
        version_id = uuid4()
        workflow_id = uuid4()
        user_id = uuid4()
        
        with patch.object(workflow_version_manager, '_save_tag') as mock_save:
            mock_save.return_value = None
            
            tag = await workflow_version_manager.create_tag(
                version_id=version_id,
                workflow_id=workflow_id,
                tag_name="v1.0.0",
                user_id=user_id,
                message="Release version 1.0.0",
                is_release=True
            )
            
            assert tag.name == "v1.0.0"
            assert tag.version_id == version_id
            assert tag.workflow_id == workflow_id
            assert tag.message == "Release version 1.0.0"
            assert tag.is_release is True
            
            mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_restore_version(self, workflow_version_manager, sample_workflow_data):
        """Test restoring workflow to specific version."""
        workflow_id = uuid4()
        user_id = uuid4()
        
        # Mock the version to restore
        version_to_restore = WorkflowVersion(
            id=uuid4(),
            workflow_id=workflow_id,
            version_number="1.0.0",
            content_hash="hash1",
            created_at=datetime.now(timezone.utc),
            created_by=user_id,
            message="Version to restore"
        )
        
        revision_to_restore = WorkflowRevision(
            id=uuid4(),
            workflow_id=workflow_id,
            revision_number=1,
            workflow_data=sample_workflow_data,
            created_at=datetime.now(timezone.utc),
            created_by=user_id,
            message="Revision to restore"
        )
        
        with patch.object(workflow_version_manager, '_load_version') as mock_load_version:
            with patch.object(workflow_version_manager, '_load_revision_by_version') as mock_load_revision:
                with patch.object(workflow_version_manager, 'create_version') as mock_create:
                    mock_load_version.return_value = version_to_restore
                    mock_load_revision.return_value = revision_to_restore
                    mock_create.return_value = WorkflowVersion(
                        id=uuid4(),
                        workflow_id=workflow_id,
                        version_number="2.0.0",
                        content_hash="new_hash",
                        created_at=datetime.now(timezone.utc),
                        created_by=user_id,
                        message=f"Restored from version 1.0.0"
                    )
                    
                    new_version = await workflow_version_manager.restore_version(
                        workflow_id=workflow_id,
                        target_version="1.0.0",
                        user_id=user_id
                    )
                    
                    assert "Restored from version 1.0.0" in new_version.message
                    mock_load_version.assert_called_once()
                    mock_load_revision.assert_called_once()
                    mock_create.assert_called_once()


@pytest.mark.integration
class TestVersionControlIntegration:
    """Integration tests for version control."""
    
    @pytest.mark.asyncio
    async def test_full_version_lifecycle(self, workflow_version_manager, sample_workflow_data, modified_workflow_data):
        """Test complete version control lifecycle."""
        workflow_id = uuid4()
        user_id = uuid4()
        
        with patch.object(workflow_version_manager, '_save_version'):
            with patch.object(workflow_version_manager, '_save_revision'):
                with patch.object(workflow_version_manager, '_save_branch'):
                    with patch.object(workflow_version_manager, '_save_tag'):
                        with patch.object(workflow_version_manager, '_get_next_revision_number') as mock_get_next_rev:
                            mock_get_next_rev.return_value = 1
                            
                            # 1. Create initial version
                            v1 = await workflow_version_manager.create_version(
                                workflow_id=workflow_id,
                                workflow_data=sample_workflow_data,
                                user_id=user_id,
                                message="Initial version"
                            )
                            
                            assert v1.version_number == "1.0.0"
                            assert v1.is_initial_version()
                            
                            # 2. Create a branch
                            branch = await workflow_version_manager.create_branch(
                                workflow_id=workflow_id,
                                branch_name="feature/improvements",
                                from_version="1.0.0",
                                user_id=user_id
                            )
                            
                            assert branch.name == "feature/improvements"
                            
                            # 3. Create version on branch
                            mock_get_next_rev.return_value = 2
                            with patch.object(workflow_version_manager, '_get_latest_version') as mock_latest:
                                with patch.object(workflow_version_manager, '_get_version_by_id') as mock_get_by_id:
                                    mock_latest.return_value = v1
                                    mock_get_by_id.return_value = v1
                                    
                                    v2 = await workflow_version_manager.create_version(
                                        workflow_id=workflow_id,
                                        workflow_data=modified_workflow_data,
                                        user_id=user_id,
                                        message="Added improvements",
                                        branch="feature/improvements",
                                        parent_version_id=v1.id
                                    )
                                    
                                    assert v2.version_number == "1.1.0"
                                    assert v2.branch == "feature/improvements"
                                    assert v2.parent_version_id == v1.id
                            
                            # 4. Create tag
                            tag = await workflow_version_manager.create_tag(
                                version_id=v2.id,
                                workflow_id=workflow_id,
                                tag_name="v1.1.0-beta",
                                user_id=user_id,
                                message="Beta release"
                            )
                            
                            assert tag.name == "v1.1.0-beta"
                            assert tag.version_id == v2.id
    
    @pytest.mark.asyncio
    async def test_conflict_detection_and_resolution(self, workflow_version_manager):
        """Test conflict detection and resolution."""
        workflow_id = uuid4()
        user_id = uuid4()
        
        # Mock conflicting changes
        base_data = {
            "nodes": [{"id": "node1", "type": "trigger", "value": "original"}],
            "connections": []
        }
        
        branch1_data = {
            "nodes": [{"id": "node1", "type": "trigger", "value": "changed_by_user1"}],
            "connections": []
        }
        
        branch2_data = {
            "nodes": [{"id": "node1", "type": "trigger", "value": "changed_by_user2"}],
            "connections": []
        }
        
        with patch.object(workflow_version_manager, '_detect_conflicts') as mock_detect:
            mock_detect.return_value = ["nodes.0.value"]
            
            conflicts = await workflow_version_manager.detect_merge_conflicts(
                workflow_id=workflow_id,
                source_branch="feature/user1",
                target_branch="feature/user2"
            )
            
            assert len(conflicts) == 1
            assert "nodes.0.value" in conflicts
    
    @pytest.mark.asyncio
    async def test_workflow_history_tracking(self, workflow_version_manager):
        """Test workflow history tracking."""
        workflow_id = uuid4()
        
        history_entries = [
            WorkflowHistoryEntry(
                id=uuid4(),
                workflow_id=workflow_id,
                version_id=uuid4(),
                action="created",
                timestamp=datetime.now(timezone.utc) - timedelta(days=2),
                user_id=uuid4(),
                changes_summary="Workflow created"
            ),
            WorkflowHistoryEntry(
                id=uuid4(),
                workflow_id=workflow_id,
                version_id=uuid4(),
                action="modified",
                timestamp=datetime.now(timezone.utc) - timedelta(days=1),
                user_id=uuid4(),
                changes_summary="Added new nodes"
            )
        ]
        
        with patch.object(workflow_version_manager, '_load_history') as mock_load:
            mock_load.return_value = history_entries
            
            history = await workflow_version_manager.get_workflow_history(
                workflow_id=workflow_id,
                limit=10
            )
            
            assert len(history) == 2
            assert history[0].action == "created"
            assert history[1].action == "modified"


@pytest.mark.performance
class TestVersionControlPerformance:
    """Performance tests for version control."""
    
    @pytest.mark.asyncio
    async def test_large_workflow_versioning(self, workflow_version_manager):
        """Test versioning performance with large workflows."""
        # Create large workflow data
        large_workflow_data = {
            "nodes": [
                {
                    "id": f"node_{i}",
                    "type": "action.http",
                    "position": {"x": i * 100, "y": i * 50},
                    "parameters": {"url": f"https://api{i}.example.com"}
                }
                for i in range(1000)  # 1000 nodes
            ],
            "connections": [
                {
                    "source": f"node_{i}",
                    "target": f"node_{i+1}",
                    "source_output": "main",
                    "target_input": "main"
                }
                for i in range(999)  # 999 connections
            ]
        }
        
        workflow_id = uuid4()
        user_id = uuid4()
        
        with patch.object(workflow_version_manager, '_save_version'):
            with patch.object(workflow_version_manager, '_save_revision'):
                # Measure time for creating version
                import time
                start_time = time.time()
                
                version = await workflow_version_manager.create_version(
                    workflow_id=workflow_id,
                    workflow_data=large_workflow_data,
                    user_id=user_id,
                    message="Large workflow test"
                )
                
                end_time = time.time()
                duration = end_time - start_time
                
                # Should complete within reasonable time (< 1 second)
                assert duration < 1.0
                assert version.version_number == "1.0.0"
    
    @pytest.mark.asyncio
    async def test_version_comparison_performance(self, workflow_version_manager):
        """Test performance of version comparison."""
        workflow_id = uuid4()
        
        # Create two large, similar workflow datasets
        base_data = {
            "nodes": [{"id": f"node_{i}", "value": i} for i in range(500)],
            "connections": []
        }
        
        modified_data = base_data.copy()
        modified_data["nodes"][250]["value"] = 9999  # Single change
        
        with patch.object(workflow_version_manager, '_calculate_diff') as mock_diff:
            mock_diff.return_value = WorkflowDiff(
                version_from="1.0.0",
                version_to="1.1.0",
                changes=[{
                    "type": ChangeType.MODIFIED,
                    "path": ["nodes", 250, "value"],
                    "old_value": 250,
                    "new_value": 9999
                }],
                summary="Single node value change",
                created_at=datetime.now(timezone.utc)
            )
            
            import time
            start_time = time.time()
            
            diff = await workflow_version_manager._calculate_diff(base_data, modified_data)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Diff calculation should be fast
            assert duration < 0.5
            assert len(diff.changes) == 1