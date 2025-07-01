"""Test Collaborative Features (Real-time editing) system."""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set
from uuid import UUID, uuid4

from budflow.collaboration.realtime import (
    CollaborationManager,
    RealTimeSession,
    WorkflowSession,
    UserSession,
    SessionManager,
    OperationSync,
    ConflictResolver,
    PresenceManager,
    LiveCursor,
    UserPresence,
    EditOperation,
    OperationType,
    OperationResult,
    CollaborationEvent,
    EventType,
    WebSocketManager,
    SyncStrategy,
    ConflictResolution,
    MergeStrategy,
    LockManager,
    SharedLock,
    LockType,
    LockScope,
    PermissionManager,
    CollaborationPermission,
    AccessLevel,
    SessionState,
    SyncState,
    CollaborationError,
    VersionVector,
    OperationalTransform,
    TextOperation,
    NodeOperation,
    WorkflowChange,
    ChangeType,
    DiffGenerator,
    PatchApplier,
    CommentSystem,
    WorkflowComment,
    CommentThread,
    NotificationManager,
    CollaborationNotification,
    AwarenessInfo,
    ViewportInfo,
    SelectionInfo,
    ActivityTracker,
    UserActivity,
    ActivityType,
    SessionMetrics,
    CollaborationAnalytics,
)


@pytest.fixture
def collaboration_manager():
    """Create CollaborationManager for testing."""
    return CollaborationManager()


@pytest.fixture
def session_manager():
    """Create SessionManager for testing."""
    return SessionManager()


@pytest.fixture
def sample_user_session():
    """Create sample user session."""
    return UserSession(
        session_id=uuid4(),
        user_id=uuid4(),
        username="test_user",
        email="test@example.com",
        connected_at=datetime.now(timezone.utc),
        last_activity=datetime.now(timezone.utc),
        is_active=True,
        permissions=set([CollaborationPermission.READ, CollaborationPermission.WRITE]),
        metadata={}
    )


@pytest.fixture
def sample_workflow_session():
    """Create sample workflow session."""
    return WorkflowSession(
        session_id=uuid4(),
        workflow_id=uuid4(),
        owner_id=uuid4(),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        participants=[],
        current_version=1,
        sync_state=SyncState.SYNCHRONIZED,
        locks={},
        metadata={}
    )


@pytest.fixture
def sample_edit_operation():
    """Create sample edit operation."""
    return EditOperation(
        operation_id=uuid4(),
        session_id=uuid4(),
        user_id=uuid4(),
        operation_type=OperationType.NODE_UPDATE,
        target_path=["nodes", "node1", "parameters", "url"],
        old_value="https://old.example.com",
        new_value="https://new.example.com",
        timestamp=datetime.now(timezone.utc),
        version_vector={"user1": 1, "user2": 0},
        context={}
    )


@pytest.fixture
def sample_user_presence():
    """Create sample user presence."""
    return UserPresence(
        user_id=uuid4(),
        session_id=uuid4(),
        username="test_user",
        avatar_url=None,
        cursor_position=LiveCursor(
            node_id="node1",
            x=100,
            y=200,
            color="#FF0000"
        ),
        viewport=ViewportInfo(
            center_x=500,
            center_y=300,
            zoom_level=1.0
        ),
        selection=SelectionInfo(
            selected_nodes=["node1", "node2"],
            selected_connections=[]
        ),
        status="online",
        last_seen=datetime.now(timezone.utc)
    )


@pytest.fixture
def sample_workflow_comment():
    """Create sample workflow comment."""
    return WorkflowComment(
        comment_id=uuid4(),
        thread_id=uuid4(),
        workflow_id=uuid4(),
        user_id=uuid4(),
        content="This needs to be updated",
        created_at=datetime.now(timezone.utc),
        updated_at=None,
        is_resolved=False,
        parent_comment_id=None,
        attachments=[],
        mentions=[],
        reactions={}
    )


@pytest.mark.unit
class TestUserSession:
    """Test UserSession model."""
    
    def test_user_session_creation(self, sample_user_session):
        """Test creating user session."""
        assert sample_user_session.username == "test_user"
        assert sample_user_session.email == "test@example.com"
        assert sample_user_session.is_active is True
        assert CollaborationPermission.READ in sample_user_session.permissions
        assert CollaborationPermission.WRITE in sample_user_session.permissions
    
    def test_user_session_permissions(self, sample_user_session):
        """Test user session permission checking."""
        assert sample_user_session.has_permission(CollaborationPermission.READ)
        assert sample_user_session.has_permission(CollaborationPermission.WRITE)
        assert not sample_user_session.has_permission(CollaborationPermission.ADMIN)
    
    def test_user_session_activity_update(self, sample_user_session):
        """Test updating user session activity."""
        old_activity = sample_user_session.last_activity
        sample_user_session.update_activity()
        assert sample_user_session.last_activity > old_activity
    
    def test_user_session_timeout_check(self, sample_user_session):
        """Test checking if user session has timed out."""
        # Active session should not be timed out
        assert not sample_user_session.is_timed_out(timeout_minutes=30)
        
        # Mock old last_activity
        sample_user_session.last_activity = datetime.now(timezone.utc) - timedelta(minutes=35)
        assert sample_user_session.is_timed_out(timeout_minutes=30)


@pytest.mark.unit
class TestWorkflowSession:
    """Test WorkflowSession model."""
    
    def test_workflow_session_creation(self, sample_workflow_session):
        """Test creating workflow session."""
        assert sample_workflow_session.is_active is True
        assert sample_workflow_session.current_version == 1
        assert sample_workflow_session.sync_state == SyncState.SYNCHRONIZED
        assert len(sample_workflow_session.participants) == 0
    
    def test_workflow_session_participant_management(self, sample_workflow_session, sample_user_session):
        """Test managing session participants."""
        # Add participant
        sample_workflow_session.add_participant(sample_user_session)
        assert len(sample_workflow_session.participants) == 1
        assert sample_user_session.user_id in [p.user_id for p in sample_workflow_session.participants]
        
        # Remove participant
        sample_workflow_session.remove_participant(sample_user_session.user_id)
        assert len(sample_workflow_session.participants) == 0
    
    def test_workflow_session_version_management(self, sample_workflow_session):
        """Test workflow session version management."""
        initial_version = sample_workflow_session.current_version
        
        sample_workflow_session.increment_version()
        assert sample_workflow_session.current_version == initial_version + 1
    
    def test_workflow_session_lock_management(self, sample_workflow_session):
        """Test workflow session lock management."""
        lock = SharedLock(
            lock_id=uuid4(),
            user_id=uuid4(),
            lock_type=LockType.EXCLUSIVE,
            scope=LockScope.NODE,
            target_path=["nodes", "node1"],
            acquired_at=datetime.now(timezone.utc)
        )
        
        # Add lock
        sample_workflow_session.add_lock(lock)
        assert len(sample_workflow_session.locks) == 1
        
        # Remove lock
        sample_workflow_session.remove_lock(lock.lock_id)
        assert len(sample_workflow_session.locks) == 0


@pytest.mark.unit
class TestEditOperation:
    """Test EditOperation model."""
    
    def test_edit_operation_creation(self, sample_edit_operation):
        """Test creating edit operation."""
        assert sample_edit_operation.operation_type == OperationType.NODE_UPDATE
        assert sample_edit_operation.target_path == ["nodes", "node1", "parameters", "url"]
        assert sample_edit_operation.old_value == "https://old.example.com"
        assert sample_edit_operation.new_value == "https://new.example.com"
    
    def test_edit_operation_serialization(self, sample_edit_operation):
        """Test edit operation serialization."""
        data = sample_edit_operation.model_dump()
        
        assert "operation_id" in data
        assert "operation_type" in data
        assert "target_path" in data
        assert "old_value" in data
        assert "new_value" in data
        
        # Test deserialization
        restored = EditOperation.model_validate(data)
        assert restored.operation_type == sample_edit_operation.operation_type
        assert restored.target_path == sample_edit_operation.target_path
    
    def test_edit_operation_conflict_detection(self, sample_edit_operation):
        """Test detecting conflicts between operations."""
        # Create conflicting operation
        conflicting_op = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "parameters", "url"],  # Same path
            old_value="https://another.example.com",
            new_value="https://different.example.com",
            timestamp=datetime.now(timezone.utc),
            version_vector={"user1": 1, "user2": 1}
        )
        
        assert sample_edit_operation.conflicts_with(conflicting_op)
    
    def test_edit_operation_path_matching(self, sample_edit_operation):
        """Test operation path matching."""
        assert sample_edit_operation.affects_path(["nodes", "node1"])
        assert sample_edit_operation.affects_path(["nodes", "node1", "parameters"])
        assert not sample_edit_operation.affects_path(["nodes", "node2"])


@pytest.mark.unit
class TestUserPresence:
    """Test UserPresence model."""
    
    def test_user_presence_creation(self, sample_user_presence):
        """Test creating user presence."""
        assert sample_user_presence.username == "test_user"
        assert sample_user_presence.status == "online"
        assert sample_user_presence.cursor_position.node_id == "node1"
        assert sample_user_presence.viewport.zoom_level == 1.0
        assert "node1" in sample_user_presence.selection.selected_nodes
    
    def test_user_presence_cursor_update(self, sample_user_presence):
        """Test updating user cursor position."""
        new_cursor = LiveCursor(
            node_id="node2",
            x=300,
            y=400,
            color="#00FF00"
        )
        
        sample_user_presence.update_cursor(new_cursor)
        assert sample_user_presence.cursor_position.node_id == "node2"
        assert sample_user_presence.cursor_position.x == 300
    
    def test_user_presence_viewport_update(self, sample_user_presence):
        """Test updating user viewport."""
        new_viewport = ViewportInfo(
            center_x=800,
            center_y=600,
            zoom_level=1.5
        )
        
        sample_user_presence.update_viewport(new_viewport)
        assert sample_user_presence.viewport.center_x == 800
        assert sample_user_presence.viewport.zoom_level == 1.5
    
    def test_user_presence_selection_update(self, sample_user_presence):
        """Test updating user selection."""
        new_selection = SelectionInfo(
            selected_nodes=["node3", "node4"],
            selected_connections=["conn1"]
        )
        
        sample_user_presence.update_selection(new_selection)
        assert "node3" in sample_user_presence.selection.selected_nodes
        assert "conn1" in sample_user_presence.selection.selected_connections


@pytest.mark.unit
class TestSessionManager:
    """Test SessionManager."""
    
    def test_session_manager_initialization(self, session_manager):
        """Test session manager initialization."""
        assert session_manager is not None
        assert hasattr(session_manager, 'create_workflow_session')
        assert hasattr(session_manager, 'join_session')
        assert hasattr(session_manager, 'leave_session')
    
    @pytest.mark.asyncio
    async def test_create_workflow_session(self, session_manager):
        """Test creating workflow session."""
        workflow_id = uuid4()
        owner_id = uuid4()
        
        session = await session_manager.create_workflow_session(
            workflow_id=workflow_id,
            owner_id=owner_id
        )
        
        assert session.workflow_id == workflow_id
        assert session.owner_id == owner_id
        assert session.is_active is True
        assert session.current_version == 1
    
    @pytest.mark.asyncio
    async def test_join_session(self, session_manager, sample_user_session):
        """Test user joining session."""
        workflow_id = uuid4()
        
        with patch.object(session_manager, '_get_workflow_session') as mock_get:
            with patch.object(session_manager, '_check_permissions') as mock_check:
                with patch.object(session_manager, '_notify_participants') as mock_notify:
                    mock_session = Mock()
                    mock_session.add_participant = Mock()
                    mock_get.return_value = mock_session
                    mock_check.return_value = True
                    mock_notify.return_value = None
                    
                    result = await session_manager.join_session(
                        workflow_id=workflow_id,
                        user_session=sample_user_session
                    )
                    
                    assert result is True
                    mock_get.assert_called_once_with(workflow_id)
                    mock_check.assert_called_once()
                    mock_session.add_participant.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_leave_session(self, session_manager, sample_user_session):
        """Test user leaving session."""
        workflow_id = uuid4()
        
        with patch.object(session_manager, '_get_workflow_session') as mock_get:
            with patch.object(session_manager, '_notify_participants') as mock_notify:
                mock_session = Mock()
                mock_session.remove_participant = Mock()
                mock_get.return_value = mock_session
                mock_notify.return_value = None
                
                result = await session_manager.leave_session(
                    workflow_id=workflow_id,
                    user_id=sample_user_session.user_id
                )
                
                assert result is True
                mock_session.remove_participant.assert_called_once_with(sample_user_session.user_id)
    
    @pytest.mark.asyncio
    async def test_get_active_sessions(self, session_manager):
        """Test getting active sessions."""
        user_id = uuid4()
        
        mock_sessions = [
            Mock(workflow_id=uuid4(), is_active=True),
            Mock(workflow_id=uuid4(), is_active=True)
        ]
        
        with patch.object(session_manager, '_load_user_sessions') as mock_load:
            mock_load.return_value = mock_sessions
            
            sessions = await session_manager.get_active_sessions(user_id)
            
            assert len(sessions) == 2
            mock_load.assert_called_once_with(user_id)


@pytest.mark.unit
class TestOperationSync:
    """Test OperationSync."""
    
    def test_operation_sync_initialization(self):
        """Test operation sync initialization."""
        sync = OperationSync()
        assert sync is not None
        assert hasattr(sync, 'apply_operation')
        assert hasattr(sync, 'sync_operations')
    
    @pytest.mark.asyncio
    async def test_apply_operation(self, sample_edit_operation):
        """Test applying operation to workflow."""
        sync = OperationSync()
        workflow_data = {
            "nodes": {
                "node1": {
                    "parameters": {
                        "url": "https://old.example.com"
                    }
                }
            }
        }
        
        result = await sync.apply_operation(sample_edit_operation, workflow_data)
        
        assert result.success is True
        assert workflow_data["nodes"]["node1"]["parameters"]["url"] == "https://new.example.com"
    
    @pytest.mark.asyncio
    async def test_sync_operations_batch(self):
        """Test syncing batch of operations."""
        sync = OperationSync()
        
        operations = [
            EditOperation(
                operation_id=uuid4(),
                session_id=uuid4(),
                user_id=uuid4(),
                operation_type=OperationType.NODE_CREATE,
                target_path=["nodes", "new_node"],
                old_value=None,
                new_value={"type": "http", "parameters": {}},
                timestamp=datetime.now(timezone.utc),
                version_vector={"user1": 1}
            ),
            EditOperation(
                operation_id=uuid4(),
                session_id=uuid4(),
                user_id=uuid4(),
                operation_type=OperationType.CONNECTION_CREATE,
                target_path=["connections"],
                old_value=None,
                new_value={"source": "node1", "target": "new_node"},
                timestamp=datetime.now(timezone.utc),
                version_vector={"user1": 2}
            )
        ]
        
        workflow_data = {"nodes": {}, "connections": []}
        
        results = await sync.sync_operations(operations, workflow_data)
        
        assert len(results) == 2
        assert all(result.success for result in results)
        assert "new_node" in workflow_data["nodes"]
        assert len(workflow_data["connections"]) == 1


@pytest.mark.unit
class TestConflictResolver:
    """Test ConflictResolver."""
    
    def test_conflict_resolver_initialization(self):
        """Test conflict resolver initialization."""
        resolver = ConflictResolver()
        assert resolver is not None
        assert hasattr(resolver, 'resolve_conflict')
        assert hasattr(resolver, 'detect_conflicts')
    
    @pytest.mark.asyncio
    async def test_detect_conflicts(self):
        """Test detecting conflicts between operations."""
        resolver = ConflictResolver()
        
        op1 = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "parameters", "url"],
            old_value="https://original.com",
            new_value="https://user1.com",
            timestamp=datetime.now(timezone.utc),
            version_vector={"user1": 1, "user2": 0}
        )
        
        op2 = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "parameters", "url"],
            old_value="https://original.com",
            new_value="https://user2.com",
            timestamp=datetime.now(timezone.utc),
            version_vector={"user1": 0, "user2": 1}
        )
        
        conflicts = await resolver.detect_conflicts([op1, op2])
        
        assert len(conflicts) == 1
        conflict = conflicts[0]
        assert op1.operation_id in [conflict.operation1_id, conflict.operation2_id]
        assert op2.operation_id in [conflict.operation1_id, conflict.operation2_id]
    
    @pytest.mark.asyncio
    async def test_resolve_conflict_last_writer_wins(self):
        """Test resolving conflict with last writer wins strategy."""
        resolver = ConflictResolver()
        
        earlier_op = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "name"],
            old_value="Original",
            new_value="Earlier Change",
            timestamp=datetime.now(timezone.utc) - timedelta(seconds=10),
            version_vector={"user1": 1}
        )
        
        later_op = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "name"],
            old_value="Original",
            new_value="Later Change",
            timestamp=datetime.now(timezone.utc),
            version_vector={"user2": 1}
        )
        
        resolution = await resolver.resolve_conflict(
            operations=[earlier_op, later_op],
            strategy=MergeStrategy.LAST_WRITER_WINS
        )
        
        assert resolution.winning_operation_id == later_op.operation_id
        assert resolution.strategy == MergeStrategy.LAST_WRITER_WINS


@pytest.mark.unit
class TestPresenceManager:
    """Test PresenceManager."""
    
    def test_presence_manager_initialization(self):
        """Test presence manager initialization."""
        manager = PresenceManager()
        assert manager is not None
        assert hasattr(manager, 'update_presence')
        assert hasattr(manager, 'get_participants')
    
    @pytest.mark.asyncio
    async def test_update_presence(self, sample_user_presence):
        """Test updating user presence."""
        manager = PresenceManager()
        
        result = await manager.update_presence(sample_user_presence)
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_get_participants(self):
        """Test getting session participants."""
        manager = PresenceManager()
        workflow_id = uuid4()
        
        mock_participants = [
            UserPresence(
                user_id=uuid4(),
                session_id=uuid4(),
                username="user1",
                status="online",
                last_seen=datetime.now(timezone.utc)
            ),
            UserPresence(
                user_id=uuid4(),
                session_id=uuid4(),
                username="user2",
                status="online",
                last_seen=datetime.now(timezone.utc)
            )
        ]
        
        with patch.object(manager, '_load_participants') as mock_load:
            mock_load.return_value = mock_participants
            
            participants = await manager.get_participants(workflow_id)
            
            assert len(participants) == 2
            assert participants[0].username == "user1"
            assert participants[1].username == "user2"
    
    @pytest.mark.asyncio
    async def test_remove_inactive_users(self):
        """Test removing inactive users from presence."""
        manager = PresenceManager()
        workflow_id = uuid4()
        
        with patch.object(manager, '_cleanup_inactive_users') as mock_cleanup:
            mock_cleanup.return_value = 2  # 2 users removed
            
            removed_count = await manager.remove_inactive_users(
                workflow_id=workflow_id,
                timeout_minutes=30
            )
            
            assert removed_count == 2
            mock_cleanup.assert_called_once_with(workflow_id, 30)


@pytest.mark.unit
class TestLockManager:
    """Test LockManager."""
    
    def test_lock_manager_initialization(self):
        """Test lock manager initialization."""
        manager = LockManager()
        assert manager is not None
        assert hasattr(manager, 'acquire_lock')
        assert hasattr(manager, 'release_lock')
    
    @pytest.mark.asyncio
    async def test_acquire_exclusive_lock(self):
        """Test acquiring exclusive lock."""
        manager = LockManager()
        
        lock_request = {
            "user_id": uuid4(),
            "lock_type": LockType.EXCLUSIVE,
            "scope": LockScope.NODE,
            "target_path": ["nodes", "node1"]
        }
        
        with patch.object(manager, '_check_lock_conflicts') as mock_check:
            with patch.object(manager, '_create_lock') as mock_create:
                mock_check.return_value = []  # No conflicts
                mock_lock = SharedLock(
                    lock_id=uuid4(),
                    user_id=lock_request["user_id"],
                    lock_type=lock_request["lock_type"],
                    scope=lock_request["scope"],
                    target_path=lock_request["target_path"],
                    acquired_at=datetime.now(timezone.utc)
                )
                mock_create.return_value = mock_lock
                
                result = await manager.acquire_lock(**lock_request)
                
                assert result.success is True
                assert result.lock_id == mock_lock.lock_id
    
    @pytest.mark.asyncio
    async def test_acquire_lock_with_conflict(self):
        """Test acquiring lock with existing conflict."""
        manager = LockManager()
        
        lock_request = {
            "user_id": uuid4(),
            "lock_type": LockType.EXCLUSIVE,
            "scope": LockScope.NODE,
            "target_path": ["nodes", "node1"]
        }
        
        # Mock existing conflicting lock
        existing_lock = SharedLock(
            lock_id=uuid4(),
            user_id=uuid4(),
            lock_type=LockType.EXCLUSIVE,
            scope=LockScope.NODE,
            target_path=["nodes", "node1"],
            acquired_at=datetime.now(timezone.utc)
        )
        
        with patch.object(manager, '_check_lock_conflicts') as mock_check:
            mock_check.return_value = [existing_lock]
            
            result = await manager.acquire_lock(**lock_request)
            
            assert result.success is False
            assert "conflict" in result.message.lower()
    
    @pytest.mark.asyncio
    async def test_release_lock(self):
        """Test releasing lock."""
        manager = LockManager()
        
        lock_id = uuid4()
        user_id = uuid4()
        
        with patch.object(manager, '_remove_lock') as mock_remove:
            mock_remove.return_value = True
            
            result = await manager.release_lock(lock_id, user_id)
            
            assert result is True
            mock_remove.assert_called_once_with(lock_id, user_id)


@pytest.mark.unit
class TestCommentSystem:
    """Test CommentSystem."""
    
    def test_comment_system_initialization(self):
        """Test comment system initialization."""
        system = CommentSystem()
        assert system is not None
        assert hasattr(system, 'create_comment')
        assert hasattr(system, 'get_comments')
    
    @pytest.mark.asyncio
    async def test_create_comment(self, sample_workflow_comment):
        """Test creating workflow comment."""
        system = CommentSystem()
        
        with patch.object(system, '_save_comment') as mock_save:
            with patch.object(system, '_notify_mentions') as mock_notify:
                mock_save.return_value = sample_workflow_comment.comment_id
                mock_notify.return_value = None
                
                result = await system.create_comment(
                    workflow_id=sample_workflow_comment.workflow_id,
                    user_id=sample_workflow_comment.user_id,
                    content=sample_workflow_comment.content,
                    thread_id=sample_workflow_comment.thread_id
                )
                
                assert result.success is True
                assert result.comment_id == sample_workflow_comment.comment_id
                mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_comments(self):
        """Test getting workflow comments."""
        system = CommentSystem()
        workflow_id = uuid4()
        
        mock_comments = [
            WorkflowComment(
                comment_id=uuid4(),
                thread_id=uuid4(),
                workflow_id=workflow_id,
                user_id=uuid4(),
                content="First comment",
                created_at=datetime.now(timezone.utc)
            ),
            WorkflowComment(
                comment_id=uuid4(),
                thread_id=uuid4(),
                workflow_id=workflow_id,
                user_id=uuid4(),
                content="Second comment",
                created_at=datetime.now(timezone.utc)
            )
        ]
        
        with patch.object(system, '_load_comments') as mock_load:
            mock_load.return_value = mock_comments
            
            comments = await system.get_comments(workflow_id)
            
            assert len(comments) == 2
            assert comments[0].content == "First comment"
            assert comments[1].content == "Second comment"
    
    @pytest.mark.asyncio
    async def test_resolve_comment_thread(self):
        """Test resolving comment thread."""
        system = CommentSystem()
        thread_id = uuid4()
        user_id = uuid4()
        
        with patch.object(system, '_update_thread_status') as mock_update:
            mock_update.return_value = True
            
            result = await system.resolve_thread(thread_id, user_id)
            
            assert result is True
            mock_update.assert_called_once_with(thread_id, is_resolved=True, resolved_by=user_id)


@pytest.mark.unit
class TestWebSocketManager:
    """Test WebSocketManager."""
    
    def test_websocket_manager_initialization(self):
        """Test WebSocket manager initialization."""
        manager = WebSocketManager()
        assert manager is not None
        assert hasattr(manager, 'connect_user')
        assert hasattr(manager, 'disconnect_user')
        assert hasattr(manager, 'broadcast_to_session')
    
    @pytest.mark.asyncio
    async def test_connect_user(self):
        """Test connecting user via WebSocket."""
        manager = WebSocketManager()
        
        user_id = uuid4()
        session_id = uuid4()
        mock_websocket = Mock()
        
        with patch.object(manager, '_add_connection') as mock_add:
            mock_add.return_value = True
            
            result = await manager.connect_user(user_id, session_id, mock_websocket)
            
            assert result is True
            mock_add.assert_called_once_with(user_id, session_id, mock_websocket)
    
    @pytest.mark.asyncio
    async def test_disconnect_user(self):
        """Test disconnecting user from WebSocket."""
        manager = WebSocketManager()
        
        user_id = uuid4()
        session_id = uuid4()
        
        with patch.object(manager, '_remove_connection') as mock_remove:
            mock_remove.return_value = True
            
            result = await manager.disconnect_user(user_id, session_id)
            
            assert result is True
            mock_remove.assert_called_once_with(user_id, session_id)
    
    @pytest.mark.asyncio
    async def test_broadcast_to_session(self):
        """Test broadcasting message to session participants."""
        manager = WebSocketManager()
        
        session_id = uuid4()
        message = {
            "type": "operation_applied",
            "operation_id": str(uuid4()),
            "data": {"nodes": {"node1": {"updated": True}}}
        }
        
        with patch.object(manager, '_send_to_session_participants') as mock_send:
            mock_send.return_value = 3  # 3 users received the message
            
            sent_count = await manager.broadcast_to_session(session_id, message)
            
            assert sent_count == 3
            mock_send.assert_called_once_with(session_id, message, None)


@pytest.mark.integration
class TestCollaborationIntegration:
    """Integration tests for collaboration features."""
    
    @pytest.mark.asyncio
    async def test_full_collaboration_workflow(self, collaboration_manager):
        """Test complete collaboration workflow."""
        workflow_id = uuid4()
        user1_id = uuid4()
        user2_id = uuid4()
        
        # Mock dependencies
        with patch.object(collaboration_manager, 'session_manager') as mock_session:
            with patch.object(collaboration_manager, 'operation_sync') as mock_sync:
                with patch.object(collaboration_manager, 'presence_manager') as mock_presence:
                    with patch.object(collaboration_manager, 'websocket_manager') as mock_ws:
                        
                        # 1. Create workflow session
                        mock_session_obj = Mock(session_id=uuid4())
                        mock_session.create_workflow_session = AsyncMock(return_value=mock_session_obj)
                        session = await collaboration_manager.create_session(workflow_id, user1_id)
                        assert session is not None
                        
                        # 2. User 2 joins session
                        user2_session = UserSession(
                            session_id=uuid4(),
                            user_id=user2_id,
                            username="user2",
                            email="user2@example.com",
                            connected_at=datetime.now(timezone.utc),
                            last_activity=datetime.now(timezone.utc),
                            is_active=True,
                            permissions=set([CollaborationPermission.READ, CollaborationPermission.WRITE])
                        )
                        
                        mock_session.join_session = AsyncMock(return_value=True)
                        join_result = await collaboration_manager.join_session(workflow_id, user2_session)
                        assert join_result is True
                        
                        # 3. Apply collaborative operation
                        operation = EditOperation(
                            operation_id=uuid4(),
                            session_id=session.session_id,
                            user_id=user2_id,
                            operation_type=OperationType.NODE_UPDATE,
                            target_path=["nodes", "node1", "name"],
                            old_value="Old Name",
                            new_value="New Name",
                            timestamp=datetime.now(timezone.utc),
                            version_vector={"user2": 1}
                        )
                        
                        mock_sync.apply_operation = AsyncMock(return_value=Mock(success=True))
                        mock_ws.broadcast_to_session = AsyncMock(return_value=2)
                        
                        result = await collaboration_manager.apply_operation(workflow_id, operation)
                        assert result.success is True
                        
                        # 4. Update presence
                        presence = UserPresence(
                            user_id=user2_id,
                            session_id=user2_session.session_id,
                            username="user2",
                            status="online",
                            last_seen=datetime.now(timezone.utc)
                        )
                        
                        mock_presence.update_presence = AsyncMock(return_value=True)
                        presence_result = await collaboration_manager.update_presence(presence)
                        assert presence_result is True
    
    @pytest.mark.asyncio
    async def test_conflict_resolution_workflow(self):
        """Test conflict resolution workflow."""
        resolver = ConflictResolver()
        sync = OperationSync()
        
        # Create conflicting operations
        op1 = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "parameters", "timeout"],
            old_value=30,
            new_value=60,
            timestamp=datetime.now(timezone.utc) - timedelta(seconds=5),
            version_vector={"user1": 1, "user2": 0}
        )
        
        op2 = EditOperation(
            operation_id=uuid4(),
            session_id=uuid4(),
            user_id=uuid4(),
            operation_type=OperationType.NODE_UPDATE,
            target_path=["nodes", "node1", "parameters", "timeout"],
            old_value=30,
            new_value=45,
            timestamp=datetime.now(timezone.utc),
            version_vector={"user1": 0, "user2": 1}
        )
        
        workflow_data = {
            "nodes": {
                "node1": {
                    "parameters": {
                        "timeout": 30
                    }
                }
            }
        }
        
        # Detect conflicts
        conflicts = await resolver.detect_conflicts([op1, op2])
        assert len(conflicts) == 1
        
        # Resolve conflict using last writer wins
        resolution = await resolver.resolve_conflict(
            operations=[op1, op2],
            strategy=MergeStrategy.LAST_WRITER_WINS
        )
        
        # Apply winning operation
        winning_op = op2 if resolution.winning_operation_id == op2.operation_id else op1
        result = await sync.apply_operation(winning_op, workflow_data)
        
        assert result.success is True
        assert workflow_data["nodes"]["node1"]["parameters"]["timeout"] == winning_op.new_value
    
    @pytest.mark.asyncio
    async def test_session_lifecycle_management(self, session_manager):
        """Test complete session lifecycle management."""
        workflow_id = uuid4()
        owner_id = uuid4()
        participant_id = uuid4()
        
        with patch.object(session_manager, '_save_session') as mock_save:
            with patch.object(session_manager, '_get_workflow_session') as mock_get:
                with patch.object(session_manager, '_check_permissions') as mock_check:
                    with patch.object(session_manager, '_notify_participants') as mock_notify:
                        
                        # 1. Create session
                        mock_save.return_value = None
                        session = await session_manager.create_workflow_session(workflow_id, owner_id)
                        assert session.workflow_id == workflow_id
                        assert session.owner_id == owner_id
                        
                        # 2. User joins
                        mock_session = Mock()
                        mock_session.add_participant = Mock()
                        mock_get.return_value = mock_session
                        mock_check.return_value = True
                        mock_notify.return_value = None
                        
                        participant_session = UserSession(
                            session_id=uuid4(),
                            user_id=participant_id,
                            username="participant",
                            email="participant@example.com",
                            connected_at=datetime.now(timezone.utc),
                            last_activity=datetime.now(timezone.utc),
                            is_active=True,
                            permissions=set([CollaborationPermission.READ])
                        )
                        
                        join_result = await session_manager.join_session(workflow_id, participant_session)
                        assert join_result is True
                        
                        # 3. User leaves
                        mock_session.remove_participant = Mock()
                        leave_result = await session_manager.leave_session(workflow_id, participant_id)
                        assert leave_result is True
                        
                        # 4. Close session
                        with patch.object(session_manager, '_close_session') as mock_close:
                            mock_close.return_value = True
                            close_result = await session_manager.close_session(workflow_id)
                            assert close_result is True


@pytest.mark.performance
class TestCollaborationPerformance:
    """Performance tests for collaboration features."""
    
    @pytest.mark.asyncio
    async def test_high_volume_operations(self):
        """Test handling high volume of operations."""
        sync = OperationSync()
        
        # Create 1000 operations
        operations = []
        for i in range(1000):
            op = EditOperation(
                operation_id=uuid4(),
                session_id=uuid4(),
                user_id=uuid4(),
                operation_type=OperationType.NODE_UPDATE,
                target_path=["nodes", f"node_{i}", "name"],
                old_value=f"Old Name {i}",
                new_value=f"New Name {i}",
                timestamp=datetime.now(timezone.utc),
                version_vector={f"user_{i}": 1}
            )
            operations.append(op)
        
        workflow_data = {"nodes": {f"node_{i}": {"name": f"Old Name {i}"} for i in range(1000)}}
        
        import time
        start_time = time.time()
        
        results = await sync.sync_operations(operations, workflow_data)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should handle 1000 operations within reasonable time (< 5 seconds)
        assert duration < 5.0
        assert len(results) == 1000
        assert all(result.success for result in results)
    
    @pytest.mark.asyncio
    async def test_concurrent_session_management(self, session_manager):
        """Test concurrent session management."""
        # Simulate 50 concurrent users joining/leaving sessions
        workflow_id = uuid4()
        user_ids = [uuid4() for _ in range(50)]
        
        async def user_session_lifecycle(user_id):
            """Simulate user session lifecycle."""
            user_session = UserSession(
                session_id=uuid4(),
                user_id=user_id,
                username=f"user_{str(user_id)[:8]}",
                email=f"user_{str(user_id)[:8]}@example.com",
                connected_at=datetime.now(timezone.utc),
                last_activity=datetime.now(timezone.utc),
                is_active=True,
                permissions=set([CollaborationPermission.READ, CollaborationPermission.WRITE])
            )
            
            # Mock session manager methods
            with patch.object(session_manager, '_get_workflow_session') as mock_get:
                with patch.object(session_manager, '_check_permissions') as mock_check:
                    with patch.object(session_manager, '_notify_participants') as mock_notify:
                        mock_session = Mock()
                        mock_session.add_participant = Mock()
                        mock_session.remove_participant = Mock()
                        mock_get.return_value = mock_session
                        mock_check.return_value = True
                        mock_notify.return_value = None
                        
                        # Join session
                        await session_manager.join_session(workflow_id, user_session)
                        
                        # Simulate some activity
                        await asyncio.sleep(0.01)
                        
                        # Leave session
                        await session_manager.leave_session(workflow_id, user_id)
        
        import time
        start_time = time.time()
        
        # Run all user sessions concurrently
        await asyncio.gather(*[user_session_lifecycle(user_id) for user_id in user_ids])
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should handle 50 concurrent users within reasonable time (< 3 seconds)
        assert duration < 3.0
    
    @pytest.mark.asyncio
    async def test_presence_update_performance(self):
        """Test presence update performance."""
        manager = PresenceManager()
        
        # Create 100 presence updates
        presence_updates = []
        for i in range(100):
            presence = UserPresence(
                user_id=uuid4(),
                session_id=uuid4(),
                username=f"user_{i}",
                cursor_position=LiveCursor(
                    node_id=f"node_{i % 10}",
                    x=i * 10,
                    y=i * 5,
                    color=f"#{i:06x}"
                ),
                status="online",
                last_seen=datetime.now(timezone.utc)
            )
            presence_updates.append(presence)
        
        with patch.object(manager, '_save_presence') as mock_save:
            mock_save.return_value = True
            
            import time
            start_time = time.time()
            
            # Update all presences
            for presence in presence_updates:
                await manager.update_presence(presence)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should update 100 presences quickly (< 1 second)
            assert duration < 1.0
            assert mock_save.call_count == 100