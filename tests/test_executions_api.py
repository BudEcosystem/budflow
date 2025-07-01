"""Comprehensive test suite for executions API."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import uuid4

import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.models import User
from budflow.workflows.models import (
    Execution,
    ExecutionStatus,
    Workflow,
    WorkflowStatus,
)


class TestExecutionsAPI:
    """Test cases for execution REST API endpoints."""

    @pytest.fixture
    async def active_workflow(self, db_session: AsyncSession, test_user: User) -> Workflow:
        """Create an active workflow for testing."""
        workflow = Workflow(
            name="Test Active Workflow",
            user_id=test_user.id,
            status=WorkflowStatus.ACTIVE,
            active=True,
            nodes=[
                {
                    "id": "trigger_1",
                    "type": "trigger.manual",
                    "name": "Manual Trigger",
                    "position": [100, 100],
                    "parameters": {}
                },
                {
                    "id": "action_1",
                    "type": "action.code",
                    "name": "Process Data",
                    "position": [300, 100],
                    "parameters": {
                        "code": "return { result: $input.value * 2 };"
                    }
                }
            ],
            connections=[
                {
                    "from": {"node": "trigger_1", "output": "main"},
                    "to": {"node": "action_1", "input": "main"}
                }
            ],
            settings={
                "executionOrder": "v1",
                "saveDataErrorExecution": "all",
                "saveDataSuccessExecution": "all"
            }
        )
        db_session.add(workflow)
        await db_session.commit()
        await db_session.refresh(workflow)
        return workflow

    @pytest.fixture
    async def test_execution(
        self,
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ) -> Execution:
        """Create a test execution."""
        execution = Execution(
            workflow_id=active_workflow.id,
            user_id=test_user.id,
            mode="manual",
            status=ExecutionStatus.SUCCESS,
            data={"input": {"value": 42}},
            started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            finished_at=datetime.now(timezone.utc) - timedelta(minutes=4),
            execution_time_ms=60000
        )
        db_session.add(execution)
        await db_session.commit()
        await db_session.refresh(execution)
        return execution

    @pytest.mark.asyncio
    async def test_list_executions(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test listing executions with filtering."""
        # Create test executions
        executions = []
        for i in range(5):
            execution = Execution(
                workflow_id=active_workflow.id,
                user_id=test_user.id,
                mode="manual",
                status=ExecutionStatus.SUCCESS if i % 2 == 0 else ExecutionStatus.ERROR,
                data={"input": {"value": i}},
                started_at=datetime.now(timezone.utc) - timedelta(hours=i),
                finished_at=datetime.now(timezone.utc) - timedelta(hours=i) + timedelta(minutes=5),
                execution_time_ms=300000  # 5 minutes
            )
            db_session.add(execution)
            executions.append(execution)
        await db_session.commit()

        # Test basic listing
        response = await async_client.get(
            "/api/v1/executions",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 5
        assert len(data["items"]) >= 5

        # Test filtering by workflow
        response = await async_client.get(
            f"/api/v1/executions?workflowId={active_workflow.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert all(item["workflowId"] == str(active_workflow.id) for item in data["items"])

        # Test filtering by status
        response = await async_client.get(
            "/api/v1/executions?status=success",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert all(item["status"] == "success" for item in data["items"])

        # Test pagination
        response = await async_client.get(
            "/api/v1/executions?limit=2&offset=2",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data["items"]) <= 2

    @pytest.mark.asyncio
    async def test_get_execution_by_id(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test getting a specific execution."""
        response = await async_client.get(
            f"/api/v1/executions/{test_execution.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["id"] == str(test_execution.id)
        assert data["status"] == test_execution.status.value
        assert data["data"] == test_execution.data

    @pytest.mark.asyncio
    async def test_get_execution_not_found(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test getting a non-existent execution."""
        response = await async_client.get(
            f"/api/v1/executions/{uuid4()}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_cancel_execution(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test canceling a running execution."""
        # Create a running execution
        execution = Execution(
            workflow_id=active_workflow.id,
            user_id=test_user.id,
            mode="manual",
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(timezone.utc)
        )
        db_session.add(execution)
        await db_session.commit()
        await db_session.refresh(execution)

        response = await async_client.post(
            f"/api/v1/executions/{execution.id}/cancel",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == ExecutionStatus.CANCELED.value

    @pytest.mark.asyncio
    async def test_cancel_completed_execution(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test canceling an already completed execution."""
        response = await async_client.post(
            f"/api/v1/executions/{test_execution.id}/cancel",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Cannot cancel" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_retry_execution(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test retrying a failed execution."""
        # Create a failed execution
        failed_execution = Execution(
            workflow_id=active_workflow.id,
            user_id=test_user.id,
            mode="manual",
            status=ExecutionStatus.ERROR,
            data={"input": {"value": 42}},
            error={"message": "Test error", "stack": "..."},
            started_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            finished_at=datetime.now(timezone.utc) - timedelta(minutes=9)
        )
        db_session.add(failed_execution)
        await db_session.commit()
        await db_session.refresh(failed_execution)

        response = await async_client.post(
            f"/api/v1/executions/{failed_execution.id}/retry",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] in [ExecutionStatus.NEW.value, ExecutionStatus.RUNNING.value]
        assert data["retryOf"] == str(failed_execution.id)

    @pytest.mark.asyncio
    async def test_retry_successful_execution(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test retrying a successful execution (should fail)."""
        response = await async_client.post(
            f"/api/v1/executions/{test_execution.id}/retry",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Cannot retry" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_delete_execution(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test deleting an execution."""
        response = await async_client.delete(
            f"/api/v1/executions/{test_execution.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_204_NO_CONTENT

        # Verify execution is deleted
        response = await async_client.get(
            f"/api/v1/executions/{test_execution.id}",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_running_execution(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test deleting a running execution (should fail)."""
        # Create a running execution
        execution = Execution(
            workflow_id=active_workflow.id,
            user_id=test_user.id,
            mode="manual",
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(timezone.utc)
        )
        db_session.add(execution)
        await db_session.commit()
        await db_session.refresh(execution)

        response = await async_client.delete(
            f"/api/v1/executions/{execution.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Cannot delete running" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_get_execution_data(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test getting execution data (input/output)."""
        response = await async_client.get(
            f"/api/v1/executions/{test_execution.id}/data",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "input" in data
        assert data["input"] == test_execution.data

    @pytest.mark.asyncio
    async def test_workflow_execution_history(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        active_workflow: Workflow,
        db_session: AsyncSession,
        test_user: User
    ):
        """Test getting execution history for a workflow."""
        # Create executions
        for i in range(3):
            execution = Execution(
                workflow_id=active_workflow.id,
                user_id=test_user.id,
                mode="manual",
                status=ExecutionStatus.SUCCESS,
                started_at=datetime.now(timezone.utc) - timedelta(days=i),
                finished_at=datetime.now(timezone.utc) - timedelta(days=i) + timedelta(minutes=5)
            )
            db_session.add(execution)
        await db_session.commit()

        response = await async_client.get(
            f"/api/v1/workflows/{active_workflow.id}/executions",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 3
        assert all(item["workflowId"] == str(active_workflow.id) for item in data["items"])

    @pytest.mark.asyncio
    async def test_execution_metrics(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test getting execution metrics."""
        # Create executions with different statuses
        for i in range(10):
            execution = Execution(
                workflow_id=active_workflow.id,
                user_id=test_user.id,
                mode="manual",
                status=ExecutionStatus.SUCCESS if i < 7 else ExecutionStatus.ERROR,
                execution_time_ms=(i + 1) * 1000,  # 1-10 seconds
                started_at=datetime.now(timezone.utc) - timedelta(hours=i),
                finished_at=datetime.now(timezone.utc) - timedelta(hours=i) + timedelta(seconds=i+1)
            )
            db_session.add(execution)
        await db_session.commit()

        response = await async_client.get(
            "/api/v1/executions/metrics",
            params={
                "startDate": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                "endDate": datetime.now(timezone.utc).isoformat()
            },
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "totalExecutions" in data
        assert "successRate" in data
        assert "averageExecutionTime" in data
        assert data["totalExecutions"] >= 10
        assert 60 <= data["successRate"] <= 80  # 7 out of 10 successful

    @pytest.mark.asyncio
    async def test_execution_logs(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test getting execution logs."""
        response = await async_client.get(
            f"/api/v1/executions/{test_execution.id}/logs",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "logs" in data
        assert isinstance(data["logs"], list)

    @pytest.mark.asyncio
    async def test_execution_node_outputs(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_execution: Execution
    ):
        """Test getting individual node outputs from execution."""
        response = await async_client.get(
            f"/api/v1/executions/{test_execution.id}/nodes",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "nodes" in data
        assert isinstance(data["nodes"], list)

    @pytest.mark.asyncio
    async def test_execution_permissions(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        auth_headers_user2: Dict[str, str],
        test_execution: Execution
    ):
        """Test execution permission enforcement."""
        # User 2 should not have access to user 1's execution
        response = await async_client.get(
            f"/api/v1/executions/{test_execution.id}",
            headers=auth_headers_user2
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_bulk_delete_executions(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test bulk deletion of executions."""
        # Create old executions
        execution_ids = []
        for i in range(5):
            execution = Execution(
                workflow_id=active_workflow.id,
                user_id=test_user.id,
                mode="manual",
                status=ExecutionStatus.SUCCESS,
                started_at=datetime.now(timezone.utc) - timedelta(days=30+i),
                finished_at=datetime.now(timezone.utc) - timedelta(days=30+i) + timedelta(minutes=5)
            )
            db_session.add(execution)
            await db_session.flush()
            execution_ids.append(str(execution.id))
        await db_session.commit()

        response = await async_client.post(
            "/api/v1/executions/bulk/delete",
            json={"executionIds": execution_ids},
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["deleted"] == len(execution_ids)

    @pytest.mark.asyncio
    async def test_execution_search(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test searching executions."""
        # Create executions with specific data
        execution = Execution(
            workflow_id=active_workflow.id,
            user_id=test_user.id,
            mode="manual",
            status=ExecutionStatus.ERROR,
            data={"customer": "john.doe@example.com"},
            error={"message": "Customer not found"},
            started_at=datetime.now(timezone.utc) - timedelta(hours=1),
            finished_at=datetime.now(timezone.utc) - timedelta(minutes=50)
        )
        db_session.add(execution)
        await db_session.commit()

        # Search by error message
        response = await async_client.get(
            "/api/v1/executions/search?q=Customer not found",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 1
        assert any("Customer not found" in item.get("error", {}).get("message", "") 
                  for item in data["items"])

    @pytest.mark.asyncio
    async def test_execution_wait_resume(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        active_workflow: Workflow,
        test_user: User
    ):
        """Test resuming a waiting execution."""
        # Create a waiting execution
        waiting_execution = Execution(
            workflow_id=active_workflow.id,
            user_id=test_user.id,
            mode="manual",
            status=ExecutionStatus.WAITING,
            data={"waitingFor": "webhook"},
            started_at=datetime.now(timezone.utc) - timedelta(minutes=10)
        )
        db_session.add(waiting_execution)
        await db_session.commit()
        await db_session.refresh(waiting_execution)

        response = await async_client.post(
            f"/api/v1/executions/{waiting_execution.id}/resume",
            json={"data": {"webhookData": {"status": "approved"}}},
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] in [ExecutionStatus.RUNNING.value, ExecutionStatus.SUCCESS.value]