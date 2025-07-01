"""Comprehensive test suite for workflows API."""

import json
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.models import User
from budflow.workflows.models import Workflow, WorkflowStatus


class TestWorkflowsAPI:
    """Test cases for workflow REST API endpoints."""

    @pytest.fixture
    async def test_workflow_data(self) -> Dict[str, Any]:
        """Sample workflow data for testing."""
        return {
            "name": "Test Workflow",
            "description": "A test workflow for API testing",
            "nodes": [
                {
                    "id": "node_1",
                    "type": "trigger.http",
                    "name": "HTTP Trigger",
                    "position": [100, 100],
                    "parameters": {
                        "path": "/webhook/test",
                        "method": "POST"
                    }
                },
                {
                    "id": "node_2",
                    "type": "action.code",
                    "name": "Transform Data",
                    "position": [300, 100],
                    "parameters": {
                        "code": "return { result: data.value * 2 };"
                    }
                }
            ],
            "connections": [
                {
                    "from": {"node": "node_1", "output": "main"},
                    "to": {"node": "node_2", "input": "main"}
                }
            ],
            "settings": {
                "executionOrder": "v1",
                "saveDataErrorExecution": "all",
                "saveDataSuccessExecution": "all",
                "saveManualExecutions": True,
                "timezone": "UTC"
            },
            "tags": ["test", "api"]
        }

    @pytest.mark.asyncio
    async def test_create_workflow_success(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow_data: Dict[str, Any]
    ):
        """Test successful workflow creation."""
        response = await async_client.post(
            "/api/v1/workflows",
            json=test_workflow_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        
        assert data["name"] == test_workflow_data["name"]
        assert data["description"] == test_workflow_data["description"]
        assert "id" in data
        assert "versionId" in data
        assert data["active"] is False
        assert data["status"] == WorkflowStatus.DRAFT
        assert len(data["nodes"]) == 2
        assert len(data["connections"]) == 1

    @pytest.mark.asyncio
    async def test_create_workflow_invalid_nodes(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test workflow creation with invalid nodes."""
        invalid_workflow = {
            "name": "Invalid Workflow",
            "nodes": [
                {
                    "id": "node_1",
                    "type": "invalid.node.type",
                    "name": "Invalid Node"
                }
            ],
            "connections": []
        }
        
        response = await async_client.post(
            "/api/v1/workflows",
            json=invalid_workflow,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid node type" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_create_workflow_invalid_connections(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test workflow creation with invalid connections."""
        invalid_workflow = {
            "name": "Invalid Connections",
            "nodes": [
                {
                    "id": "node_1",
                    "type": "trigger.http",
                    "name": "HTTP Trigger"
                }
            ],
            "connections": [
                {
                    "from": {"node": "node_1", "output": "main"},
                    "to": {"node": "non_existent", "input": "main"}
                }
            ]
        }
        
        response = await async_client.post(
            "/api/v1/workflows",
            json=invalid_workflow,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid connection" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_list_workflows(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        test_user: User
    ):
        """Test listing workflows with pagination and filtering."""
        # Create test workflows
        for i in range(5):
            workflow = Workflow(
                name=f"Test Workflow {i}",
                user_id=test_user.id,
                nodes=[],
                connections=[],
                active=i % 2 == 0,
                tags=["even"] if i % 2 == 0 else ["odd"]
            )
            db_session.add(workflow)
        await db_session.commit()
        
        # Test basic listing
        response = await async_client.get(
            "/api/v1/workflows",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 5
        
        # Test pagination
        response = await async_client.get(
            "/api/v1/workflows?limit=2&offset=2",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        
        # Test filtering by active status
        response = await async_client.get(
            "/api/v1/workflows?active=true",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 3
        assert all(w["active"] for w in data["items"])
        
        # Test filtering by tags
        response = await async_client.get(
            "/api/v1/workflows?tags=even",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 3

    @pytest.mark.asyncio
    async def test_get_workflow_by_id(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test getting a specific workflow by ID."""
        response = await async_client.get(
            f"/api/v1/workflows/{test_workflow.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["id"] == str(test_workflow.id)
        assert data["name"] == test_workflow.name

    @pytest.mark.asyncio
    async def test_get_workflow_not_found(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test getting a non-existent workflow."""
        response = await async_client.get(
            f"/api/v1/workflows/{uuid4()}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_update_workflow_success(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test successful workflow update."""
        update_data = {
            "name": "Updated Workflow",
            "description": "Updated description",
            "versionId": test_workflow.version_id
        }
        
        response = await async_client.patch(
            f"/api/v1/workflows/{test_workflow.id}",
            json=update_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == update_data["name"]
        assert data["description"] == update_data["description"]
        assert data["versionId"] != test_workflow.version_id  # Version should change

    @pytest.mark.asyncio
    async def test_update_workflow_version_conflict(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test workflow update with version conflict."""
        update_data = {
            "name": "Updated Workflow",
            "versionId": "incorrect-version-id"
        }
        
        response = await async_client.patch(
            f"/api/v1/workflows/{test_workflow.id}",
            json=update_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_409_CONFLICT
        assert "Version conflict" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_update_workflow_force_save(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test workflow update with force save."""
        update_data = {
            "name": "Force Updated Workflow",
            "versionId": "incorrect-version-id"
        }
        
        response = await async_client.patch(
            f"/api/v1/workflows/{test_workflow.id}?forceSave=true",
            json=update_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == update_data["name"]

    @pytest.mark.asyncio
    async def test_delete_workflow_success(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test successful workflow deletion."""
        # First archive the workflow
        test_workflow.archived = True
        await test_workflow.save()
        
        response = await async_client.delete(
            f"/api/v1/workflows/{test_workflow.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        # Verify workflow is deleted
        response = await async_client.get(
            f"/api/v1/workflows/{test_workflow.id}",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_workflow_not_archived(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test deleting a non-archived workflow."""
        response = await async_client.delete(
            f"/api/v1/workflows/{test_workflow.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "must be archived" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_archive_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test archiving a workflow."""
        response = await async_client.post(
            f"/api/v1/workflows/{test_workflow.id}/archive",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["archived"] is True

    @pytest.mark.asyncio
    async def test_unarchive_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test unarchiving a workflow."""
        test_workflow.archived = True
        await test_workflow.save()
        
        response = await async_client.post(
            f"/api/v1/workflows/{test_workflow.id}/unarchive",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["archived"] is False

    @pytest.mark.asyncio
    async def test_activate_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test activating a workflow."""
        response = await async_client.post(
            f"/api/v1/workflows/{test_workflow.id}/activate",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["active"] is True
        assert data["status"] == WorkflowStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_deactivate_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test deactivating a workflow."""
        test_workflow.active = True
        test_workflow.status = WorkflowStatus.ACTIVE
        await test_workflow.save()
        
        response = await async_client.post(
            f"/api/v1/workflows/{test_workflow.id}/deactivate",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["active"] is False
        assert data["status"] == WorkflowStatus.INACTIVE

    @pytest.mark.asyncio
    async def test_run_workflow_manual(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test manual workflow execution."""
        run_data = {
            "startNode": "node_1",
            "input": {
                "value": 42
            }
        }
        
        response = await async_client.post(
            f"/api/v1/workflows/{test_workflow.id}/run",
            json=run_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "executionId" in data
        assert data["status"] in ["running", "success"]

    @pytest.mark.asyncio
    async def test_duplicate_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test duplicating a workflow."""
        response = await async_client.post(
            f"/api/v1/workflows/{test_workflow.id}/duplicate",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["name"] == f"{test_workflow.name} (copy)"
        assert data["id"] != str(test_workflow.id)
        assert data["nodes"] == test_workflow.nodes

    @pytest.mark.asyncio
    async def test_share_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow,
        test_user2: User
    ):
        """Test sharing a workflow with another user."""
        share_data = {
            "userIds": [str(test_user2.id)],
            "permissions": ["read", "execute"]
        }
        
        response = await async_client.put(
            f"/api/v1/workflows/{test_workflow.id}/share",
            json=share_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data["sharedWith"]) == 1
        assert data["sharedWith"][0]["userId"] == str(test_user2.id)

    @pytest.mark.asyncio
    async def test_import_workflow_from_json(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow_data: Dict[str, Any]
    ):
        """Test importing a workflow from JSON."""
        response = await async_client.post(
            "/api/v1/workflows/import",
            json={"workflow": test_workflow_data},
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["name"] == test_workflow_data["name"]
        assert len(data["nodes"]) == len(test_workflow_data["nodes"])

    @pytest.mark.asyncio
    async def test_export_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test exporting a workflow."""
        response = await async_client.get(
            f"/api/v1/workflows/{test_workflow.id}/export",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == test_workflow.name
        assert "nodes" in data
        assert "connections" in data
        assert "id" not in data  # ID should be excluded from export

    @pytest.mark.asyncio
    async def test_workflow_permissions(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        auth_headers_user2: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test workflow permission enforcement."""
        # User 2 should not have access
        response = await async_client.get(
            f"/api/v1/workflows/{test_workflow.id}",
            headers=auth_headers_user2
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_workflow_validation_node_position(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow_data: Dict[str, Any]
    ):
        """Test workflow validation for node positions."""
        # Remove position from a node
        test_workflow_data["nodes"][0].pop("position", None)
        
        response = await async_client.post(
            "/api/v1/workflows",
            json=test_workflow_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "position" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_workflow_search(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        db_session: AsyncSession,
        test_user: User
    ):
        """Test searching workflows."""
        # Create test workflows
        workflows = [
            Workflow(
                name="Email Automation",
                description="Automates email sending",
                user_id=test_user.id,
                nodes=[],
                connections=[],
                tags=["email", "automation"]
            ),
            Workflow(
                name="Data Processing",
                description="Processes incoming data",
                user_id=test_user.id,
                nodes=[],
                connections=[],
                tags=["data", "processing"]
            ),
            Workflow(
                name="Webhook Handler",
                description="Handles webhook requests",
                user_id=test_user.id,
                nodes=[],
                connections=[],
                tags=["webhook", "api"]
            )
        ]
        
        for workflow in workflows:
            db_session.add(workflow)
        await db_session.commit()
        
        # Search by name
        response = await async_client.get(
            "/api/v1/workflows?search=email",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert "Email" in data["items"][0]["name"]
        
        # Search by description
        response = await async_client.get(
            "/api/v1/workflows?search=data",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        
        # Search by tag
        response = await async_client.get(
            "/api/v1/workflows?search=webhook",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1

    @pytest.mark.asyncio
    async def test_workflow_statistics(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_workflow: Workflow
    ):
        """Test getting workflow statistics."""
        response = await async_client.get(
            f"/api/v1/workflows/{test_workflow.id}/statistics",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "totalExecutions" in data
        assert "successfulExecutions" in data
        assert "failedExecutions" in data
        assert "averageExecutionTime" in data
        assert "lastExecutionDate" in data