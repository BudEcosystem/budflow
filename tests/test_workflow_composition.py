"""Comprehensive test suite for workflow composition (DAG) functionality."""

import pytest
from datetime import datetime
from typing import Dict, List, Any
from unittest.mock import AsyncMock, MagicMock, patch
import json

from budflow.workflows.models import Workflow, WorkflowExecution, NodeType, ExecutionStatus
from budflow.nodes.base import NodeDefinition, NodeCategory, NodeParameter, ParameterType
from budflow.executor.context import NodeExecutionContext, ExecutionData
from budflow.executor.errors import CircularWorkflowError, WorkflowValidationError


@pytest.fixture
def parent_workflow():
    """Create a parent workflow that includes sub-workflows."""
    return {
        "id": 1,
        "name": "Parent Workflow",
        "nodes": [
            {
                "id": "node1",
                "name": "Start",
                "type": "manual",
                "parameters": {}
            },
            {
                "id": "node2", 
                "name": "Process Data",
                "type": "function",
                "parameters": {
                    "code": "return [{'processed': true, 'value': item['value'] * 2} for item in items]"
                }
            },
            {
                "id": "node3",
                "name": "Execute Sub-Workflow",
                "type": "subworkflow",
                "parameters": {
                    "workflowId": "2",
                    "inputMapping": {
                        "data": "$.processed_items"
                    },
                    "outputMapping": {
                        "results": "$.sub_results"
                    },
                    "waitForCompletion": True,
                    "maxDepth": 5
                }
            },
            {
                "id": "node4",
                "name": "Final Processing",
                "type": "function",
                "parameters": {
                    "code": "return [{'final': true, 'results': item['results']} for item in items]"
                }
            }
        ],
        "connections": [
            {"source": "node1", "target": "node2"},
            {"source": "node2", "target": "node3"},
            {"source": "node3", "target": "node4"}
        ]
    }


@pytest.fixture
def child_workflow():
    """Create a child workflow to be executed as sub-workflow."""
    return {
        "id": 2,
        "name": "Child Workflow",
        "nodes": [
            {
                "id": "child1",
                "name": "Receive Data",
                "type": "manual",
                "parameters": {}
            },
            {
                "id": "child2",
                "name": "Transform Data",
                "type": "function", 
                "parameters": {
                    "code": "return [{'transformed': true, 'original': item} for item in items]"
                }
            },
            {
                "id": "child3",
                "name": "Return Results",
                "type": "set",
                "parameters": {
                    "values": {
                        "sub_results": "{{$json}}"
                    }
                }
            }
        ],
        "connections": [
            {"source": "child1", "target": "child2"},
            {"source": "child2", "target": "child3"}
        ]
    }


@pytest.fixture
def recursive_workflow():
    """Create a workflow that references itself (for testing circular detection)."""
    return {
        "id": 3,
        "name": "Recursive Workflow",
        "nodes": [
            {
                "id": "rec1",
                "name": "Start",
                "type": "manual",
                "parameters": {}
            },
            {
                "id": "rec2",
                "name": "Call Self",
                "type": "subworkflow",
                "parameters": {
                    "workflowId": "3",  # References itself
                    "maxDepth": 2
                }
            }
        ],
        "connections": [
            {"source": "rec1", "target": "rec2"}
        ]
    }


@pytest.fixture
def deeply_nested_workflows():
    """Create workflows with deep nesting for testing depth limits."""
    workflows = []
    for i in range(10):
        workflow = {
            "id": i + 10,
            "name": f"Nested Workflow Level {i}",
            "nodes": [
                {
                    "id": f"nested{i}_1",
                    "name": "Process",
                    "type": "function",
                    "parameters": {
                        "code": f"return [{{'level': {i}, 'data': item}} for item in items]"
                    }
                }
            ],
            "connections": []
        }
        
        # Add sub-workflow call to next level (except last)
        if i < 9:
            workflow["nodes"].append({
                "id": f"nested{i}_2",
                "name": f"Call Level {i+1}",
                "type": "subworkflow",
                "parameters": {
                    "workflowId": str(i + 11),
                    "maxDepth": 10
                }
            })
            workflow["connections"].append({
                "source": f"nested{i}_1",
                "target": f"nested{i}_2"
            })
        
        workflows.append(workflow)
    
    return workflows


class TestSubWorkflowNode:
    """Test SubWorkflowNode implementation."""
    
    @pytest.mark.asyncio
    async def test_subworkflow_node_definition(self):
        """Test SubWorkflowNode has correct definition."""
        from budflow.nodes.implementations.subworkflow import SubWorkflowNode
        
        definition = SubWorkflowNode.get_definition()
        
        assert definition.name == "Sub-Workflow"
        assert definition.type == NodeType.SUBWORKFLOW
        assert definition.category == NodeCategory.FLOW
        assert definition.description == "Execute another workflow as part of this workflow"
        
        # Check parameters
        param_names = [p.name for p in definition.parameters]
        assert "workflowId" in param_names
        assert "inputMapping" in param_names
        assert "outputMapping" in param_names
        assert "waitForCompletion" in param_names
        assert "maxDepth" in param_names
        
        # Verify required parameters
        workflow_param = next(p for p in definition.parameters if p.name == "workflowId")
        assert workflow_param.required == True
        assert workflow_param.type == ParameterType.STRING
    
    @pytest.mark.asyncio
    async def test_subworkflow_node_execution(self, mock_context):
        """Test SubWorkflowNode executes child workflow correctly."""
        from budflow.nodes.implementations.subworkflow import SubWorkflowNode
        
        # Setup context
        mock_context.parameters = {
            "workflowId": "2",
            "inputMapping": {"data": "$.items"},
            "outputMapping": {"results": "$.output"},
            "waitForCompletion": True
        }
        mock_context.input_data = [{"items": [{"value": 1}, {"value": 2}]}]
        
        # Mock workflow execution
        mock_execution = MagicMock()
        mock_execution.output_data = {"output": [{"result": "success"}]}
        mock_execution.status = ExecutionStatus.SUCCESS
        
        node = SubWorkflowNode(mock_context)
        
        with patch.object(node, '_execute_subworkflow', return_value=mock_execution) as mock_exec:
            result = await node.execute()
            
            mock_exec.assert_called_once()
            assert result == [{"results": [{"result": "success"}]}]
    
    @pytest.mark.asyncio
    async def test_subworkflow_input_mapping(self, mock_context):
        """Test input data mapping to sub-workflow."""
        from budflow.nodes.implementations.subworkflow import SubWorkflowNode
        
        mock_context.parameters = {
            "workflowId": "2",
            "inputMapping": {
                "userId": "$.user.id",
                "items": "$.data.items",
                "config": {"static": "value", "dynamic": "$.settings"}
            }
        }
        mock_context.input_data = [{
            "user": {"id": 123, "name": "Test"},
            "data": {"items": ["a", "b", "c"]},
            "settings": {"option": "enabled"}
        }]
        
        node = SubWorkflowNode(mock_context)
        mapped_input = node._map_input_data(mock_context.input_data[0], mock_context.parameters["inputMapping"])
        
        assert mapped_input == {
            "userId": 123,
            "items": ["a", "b", "c"],
            "config": {"static": "value", "dynamic": {"option": "enabled"}}
        }
    
    @pytest.mark.asyncio
    async def test_subworkflow_output_mapping(self, mock_context):
        """Test output data mapping from sub-workflow."""
        from budflow.nodes.implementations.subworkflow import SubWorkflowNode
        
        mock_context.parameters = {
            "workflowId": "2",
            "outputMapping": {
                "processed": "$.results",
                "status": "$.execution.status",
                "count": "$.results.length"
            }
        }
        
        sub_output = {
            "results": [{"id": 1}, {"id": 2}],
            "execution": {"status": "completed", "time": 100}
        }
        
        node = SubWorkflowNode(mock_context)
        mapped_output = node._map_output_data(sub_output, mock_context.parameters["outputMapping"])
        
        assert mapped_output == {
            "processed": [{"id": 1}, {"id": 2}],
            "status": "completed",
            "count": 2
        }
    
    @pytest.mark.asyncio
    async def test_subworkflow_async_execution(self, mock_context):
        """Test sub-workflow can execute asynchronously."""
        from budflow.nodes.implementations.subworkflow import SubWorkflowNode
        
        mock_context.parameters = {
            "workflowId": "2",
            "waitForCompletion": False  # Async execution
        }
        
        node = SubWorkflowNode(mock_context)
        
        with patch.object(node, '_start_subworkflow_async', return_value={"executionId": "exec123"}) as mock_async:
            result = await node.execute()
            
            mock_async.assert_called_once()
            assert result == [{"executionId": "exec123", "status": "started"}]


class TestWorkflowComposition:
    """Test workflow composition and DAG functionality."""
    
    @pytest.mark.asyncio
    async def test_simple_workflow_composition(self, workflow_service, parent_workflow, child_workflow):
        """Test executing a workflow that contains a sub-workflow."""
        # Setup workflows
        await workflow_service.create_workflow(parent_workflow)
        await workflow_service.create_workflow(child_workflow)
        
        # Execute parent workflow
        execution = await workflow_service.execute_workflow(
            workflow_id=1,
            input_data=[{"value": 10}]
        )
        
        # Verify execution
        assert execution.status == ExecutionStatus.SUCCESS
        assert execution.execution_depth == 0
        
        # Check sub-workflow was executed
        child_executions = await workflow_service.get_child_executions(execution.id)
        assert len(child_executions) == 1
        assert child_executions[0].parent_execution_id == execution.id
        assert child_executions[0].execution_depth == 1
    
    @pytest.mark.asyncio
    async def test_circular_workflow_detection(self, workflow_service, recursive_workflow):
        """Test detection of circular workflow dependencies."""
        await workflow_service.create_workflow(recursive_workflow)
        
        with pytest.raises(CircularWorkflowError) as exc_info:
            await workflow_service.execute_workflow(workflow_id=3)
        
        assert "Circular workflow dependency detected" in str(exc_info.value)
    
    @pytest.mark.asyncio 
    async def test_max_depth_limit(self, workflow_service, deeply_nested_workflows):
        """Test maximum workflow nesting depth is enforced."""
        # Create all nested workflows
        for workflow in deeply_nested_workflows:
            await workflow_service.create_workflow(workflow)
        
        # Execute with depth limit
        with pytest.raises(WorkflowValidationError) as exc_info:
            await workflow_service.execute_workflow(
                workflow_id=10,
                max_depth=5  # Limit to 5 levels
            )
        
        assert "Maximum workflow nesting depth" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_workflow_dependency_tracking(self, workflow_service, parent_workflow, child_workflow):
        """Test tracking of workflow dependencies."""
        await workflow_service.create_workflow(parent_workflow)
        await workflow_service.create_workflow(child_workflow)
        
        # Get dependencies
        dependencies = await workflow_service.get_workflow_dependencies(1)
        assert len(dependencies) == 1
        assert dependencies[0]["id"] == 2
        assert dependencies[0]["name"] == "Child Workflow"
        
        # Get dependents
        dependents = await workflow_service.get_workflow_dependents(2)
        assert len(dependents) == 1
        assert dependents[0]["id"] == 1
        assert dependents[0]["name"] == "Parent Workflow"
    
    @pytest.mark.asyncio
    async def test_data_flow_between_workflows(self, workflow_service, parent_workflow, child_workflow):
        """Test data flows correctly between parent and child workflows."""
        await workflow_service.create_workflow(parent_workflow)
        await workflow_service.create_workflow(child_workflow)
        
        input_data = [{"value": 5}]
        execution = await workflow_service.execute_workflow(
            workflow_id=1,
            input_data=input_data
        )
        
        # Check final output includes data from sub-workflow
        output = execution.output_data
        assert output[0]["final"] == True
        assert "results" in output[0]
        assert output[0]["results"]["transformed"] == True
    
    @pytest.mark.asyncio
    async def test_parallel_subworkflow_execution(self, workflow_service):
        """Test parallel execution of multiple sub-workflows."""
        # Create workflow with parallel sub-workflows
        parallel_workflow = {
            "id": 100,
            "name": "Parallel Sub-Workflows",
            "nodes": [
                {
                    "id": "start",
                    "type": "manual",
                    "parameters": {}
                },
                {
                    "id": "split",
                    "type": "split",
                    "parameters": {"branches": 3}
                },
                {
                    "id": "sub1",
                    "type": "subworkflow",
                    "parameters": {"workflowId": "101"}
                },
                {
                    "id": "sub2", 
                    "type": "subworkflow",
                    "parameters": {"workflowId": "102"}
                },
                {
                    "id": "sub3",
                    "type": "subworkflow",
                    "parameters": {"workflowId": "103"}
                },
                {
                    "id": "merge",
                    "type": "merge",
                    "parameters": {}
                }
            ],
            "connections": [
                {"source": "start", "target": "split"},
                {"source": "split", "target": "sub1", "port": 0},
                {"source": "split", "target": "sub2", "port": 1},
                {"source": "split", "target": "sub3", "port": 2},
                {"source": "sub1", "target": "merge"},
                {"source": "sub2", "target": "merge"},
                {"source": "sub3", "target": "merge"}
            ]
        }
        
        await workflow_service.create_workflow(parallel_workflow)
        
        # Create sub-workflows
        for i in range(101, 104):
            await workflow_service.create_workflow({
                "id": i,
                "name": f"Sub {i}",
                "nodes": [{"id": "n1", "type": "function", "parameters": {"code": f"return [{{'sub': {i}}}]"}}],
                "connections": []
            })
        
        execution = await workflow_service.execute_workflow(workflow_id=100)
        
        # Verify all sub-workflows executed
        child_executions = await workflow_service.get_child_executions(execution.id)
        assert len(child_executions) == 3
        assert all(ce.status == ExecutionStatus.SUCCESS for ce in child_executions)
    
    @pytest.mark.asyncio
    async def test_conditional_subworkflow_execution(self, workflow_service):
        """Test conditional execution of sub-workflows."""
        conditional_workflow = {
            "id": 200,
            "name": "Conditional Sub-Workflow",
            "nodes": [
                {
                    "id": "start",
                    "type": "manual",
                    "parameters": {}
                },
                {
                    "id": "condition",
                    "type": "if",
                    "parameters": {
                        "conditions": [{"field": "$.execute_sub", "operator": "equals", "value": True}]
                    }
                },
                {
                    "id": "sub",
                    "type": "subworkflow",
                    "parameters": {"workflowId": "201"}
                },
                {
                    "id": "skip",
                    "type": "set",
                    "parameters": {"values": {"skipped": True}}
                }
            ],
            "connections": [
                {"source": "start", "target": "condition"},
                {"source": "condition", "target": "sub", "port": "true"},
                {"source": "condition", "target": "skip", "port": "false"}
            ]
        }
        
        await workflow_service.create_workflow(conditional_workflow)
        await workflow_service.create_workflow({
            "id": 201,
            "name": "Conditional Sub",
            "nodes": [{"id": "n1", "type": "set", "parameters": {"values": {"executed": True}}}],
            "connections": []
        })
        
        # Test with condition true
        execution1 = await workflow_service.execute_workflow(
            workflow_id=200,
            input_data=[{"execute_sub": True}]
        )
        child_execs1 = await workflow_service.get_child_executions(execution1.id)
        assert len(child_execs1) == 1
        
        # Test with condition false
        execution2 = await workflow_service.execute_workflow(
            workflow_id=200,
            input_data=[{"execute_sub": False}]
        )
        child_execs2 = await workflow_service.get_child_executions(execution2.id) 
        assert len(child_execs2) == 0
    
    @pytest.mark.asyncio
    async def test_error_handling_in_subworkflows(self, workflow_service):
        """Test error handling when sub-workflow fails."""
        error_workflow = {
            "id": 300,
            "name": "Error Workflow",
            "nodes": [
                {
                    "id": "start",
                    "type": "manual",
                    "parameters": {}
                },
                {
                    "id": "sub",
                    "type": "subworkflow",
                    "parameters": {
                        "workflowId": "301",
                        "errorHandling": "continue"  # Continue on error
                    }
                },
                {
                    "id": "cleanup",
                    "type": "function",
                    "parameters": {"code": "return [{'cleaned': true}]"}
                }
            ],
            "connections": [
                {"source": "start", "target": "sub"},
                {"source": "sub", "target": "cleanup"}
            ]
        }
        
        failing_sub = {
            "id": 301,
            "name": "Failing Sub",
            "nodes": [
                {
                    "id": "fail",
                    "type": "function",
                    "parameters": {"code": "raise Exception('Intentional failure')"}
                }
            ],
            "connections": []
        }
        
        await workflow_service.create_workflow(error_workflow)
        await workflow_service.create_workflow(failing_sub)
        
        execution = await workflow_service.execute_workflow(workflow_id=300)
        
        # Parent should continue despite sub-workflow error
        assert execution.status == ExecutionStatus.SUCCESS
        assert execution.output_data[0]["cleaned"] == True
        
        # Check sub-workflow failed
        child_execs = await workflow_service.get_child_executions(execution.id)
        assert child_execs[0].status == ExecutionStatus.ERROR


class TestWorkflowCompositionAPI:
    """Test API endpoints for workflow composition."""
    
    @pytest.mark.asyncio
    async def test_get_workflow_dependencies_endpoint(self, client, auth_headers, parent_workflow):
        """Test GET /workflows/{id}/dependencies endpoint."""
        # Create workflow
        response = await client.post(
            "/api/v1/workflows",
            json=parent_workflow,
            headers=auth_headers
        )
        workflow_id = response.json()["id"]
        
        # Get dependencies
        response = await client.get(
            f"/api/v1/workflows/{workflow_id}/dependencies",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        dependencies = response.json()
        assert len(dependencies) == 1
        assert dependencies[0]["workflow_id"] == "2"
    
    @pytest.mark.asyncio
    async def test_get_workflow_dependents_endpoint(self, client, auth_headers, child_workflow):
        """Test GET /workflows/{id}/dependents endpoint."""
        # Create workflow
        response = await client.post(
            "/api/v1/workflows",
            json=child_workflow,
            headers=auth_headers
        )
        workflow_id = response.json()["id"]
        
        # Get dependents
        response = await client.get(
            f"/api/v1/workflows/{workflow_id}/dependents",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        dependents = response.json()
        assert isinstance(dependents, list)
    
    @pytest.mark.asyncio
    async def test_validate_workflow_composition_endpoint(self, client, auth_headers, recursive_workflow):
        """Test POST /workflows/{id}/validate-composition endpoint."""
        # Create workflow
        response = await client.post(
            "/api/v1/workflows",
            json=recursive_workflow,
            headers=auth_headers
        )
        workflow_id = response.json()["id"]
        
        # Validate composition
        response = await client.post(
            f"/api/v1/workflows/{workflow_id}/validate-composition",
            headers=auth_headers
        )
        
        assert response.status_code == 400
        error = response.json()
        assert "circular" in error["detail"].lower()
    
    @pytest.mark.asyncio
    async def test_workflow_composition_graph_endpoint(self, client, auth_headers):
        """Test GET /workflows/{id}/composition-graph endpoint."""
        # Create workflows with dependencies
        parent = await client.post("/api/v1/workflows", json=parent_workflow, headers=auth_headers)
        parent_id = parent.json()["id"]
        
        # Get composition graph
        response = await client.get(
            f"/api/v1/workflows/{parent_id}/composition-graph",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        graph = response.json()
        assert "nodes" in graph
        assert "edges" in graph
        assert len(graph["nodes"]) >= 1
    
    @pytest.mark.asyncio
    async def test_bulk_validate_compositions_endpoint(self, client, auth_headers):
        """Test POST /workflows/validate-compositions endpoint."""
        # Create multiple workflows
        workflow_ids = []
        for i in range(3):
            response = await client.post(
                "/api/v1/workflows",
                json={"name": f"Test {i}", "nodes": [], "connections": []},
                headers=auth_headers
            )
            workflow_ids.append(response.json()["id"])
        
        # Bulk validate
        response = await client.post(
            "/api/v1/workflows/validate-compositions",
            json={"workflow_ids": workflow_ids},
            headers=auth_headers
        )
        
        assert response.status_code == 200
        results = response.json()
        assert len(results) == 3
        assert all(r["valid"] for r in results)


class TestWorkflowCompositionOptimization:
    """Test graph optimization for workflow compositions."""
    
    @pytest.mark.asyncio
    async def test_workflow_composition_analysis(self, workflow_service):
        """Test analyzing workflow composition for optimization."""
        # Create complex workflow graph
        workflows = create_complex_workflow_graph()
        for workflow in workflows:
            await workflow_service.create_workflow(workflow)
        
        # Analyze composition
        analysis = await workflow_service.analyze_workflow_composition(workflows[0]["id"])
        
        assert "total_nodes" in analysis
        assert "total_workflows" in analysis
        assert "max_depth" in analysis
        assert "critical_path" in analysis
        assert "parallelization_opportunities" in analysis
    
    @pytest.mark.asyncio
    async def test_workflow_composition_optimization(self, workflow_service):
        """Test optimizing workflow composition."""
        # Create inefficient workflow
        inefficient = create_inefficient_workflow_composition()
        await workflow_service.create_workflow(inefficient)
        
        # Optimize
        optimized = await workflow_service.optimize_workflow_composition(inefficient["id"])
        
        assert optimized["optimization_applied"] == True
        assert optimized["improvements"]["reduced_depth"] > 0
        assert optimized["improvements"]["parallelized_nodes"] > 0
    
    @pytest.mark.asyncio
    async def test_cycle_detection_in_complex_graph(self, workflow_service):
        """Test cycle detection in complex workflow graphs."""
        # Create workflow graph with hidden cycle
        workflows = create_workflow_graph_with_cycle()
        
        for workflow in workflows:
            await workflow_service.create_workflow(workflow)
        
        # Detect cycles
        cycles = await workflow_service.detect_workflow_cycles()
        
        assert len(cycles) > 0
        assert all("cycle_path" in cycle for cycle in cycles)


# Helper functions
def create_complex_workflow_graph():
    """Create a complex workflow graph for testing."""
    return [
        {
            "id": 1000,
            "name": "Main Process",
            "nodes": [
                {"id": "m1", "type": "manual", "parameters": {}},
                {"id": "m2", "type": "subworkflow", "parameters": {"workflowId": "1001"}},
                {"id": "m3", "type": "subworkflow", "parameters": {"workflowId": "1002"}},
                {"id": "m4", "type": "merge", "parameters": {}}
            ],
            "connections": [
                {"source": "m1", "target": "m2"},
                {"source": "m1", "target": "m3"},
                {"source": "m2", "target": "m4"},
                {"source": "m3", "target": "m4"}
            ]
        },
        {
            "id": 1001,
            "name": "Sub Process 1",
            "nodes": [
                {"id": "s1_1", "type": "function", "parameters": {"code": "return items"}},
                {"id": "s1_2", "type": "subworkflow", "parameters": {"workflowId": "1003"}}
            ],
            "connections": [{"source": "s1_1", "target": "s1_2"}]
        },
        {
            "id": 1002,
            "name": "Sub Process 2",
            "nodes": [
                {"id": "s2_1", "type": "function", "parameters": {"code": "return items"}},
                {"id": "s2_2", "type": "subworkflow", "parameters": {"workflowId": "1003"}}
            ],
            "connections": [{"source": "s2_1", "target": "s2_2"}]
        },
        {
            "id": 1003,
            "name": "Shared Sub Process",
            "nodes": [
                {"id": "sh1", "type": "function", "parameters": {"code": "return items"}}
            ],
            "connections": []
        }
    ]


def create_inefficient_workflow_composition():
    """Create an inefficient workflow that can be optimized."""
    return {
        "id": 2000,
        "name": "Inefficient Workflow",
        "nodes": [
            {"id": "i1", "type": "manual", "parameters": {}},
            # Sequential sub-workflows that could be parallelized
            {"id": "i2", "type": "subworkflow", "parameters": {"workflowId": "2001"}},
            {"id": "i3", "type": "subworkflow", "parameters": {"workflowId": "2002"}},
            {"id": "i4", "type": "subworkflow", "parameters": {"workflowId": "2003"}},
            # Redundant processing
            {"id": "i5", "type": "function", "parameters": {"code": "return items"}},
            {"id": "i6", "type": "function", "parameters": {"code": "return items"}},
            {"id": "i7", "type": "merge", "parameters": {}}
        ],
        "connections": [
            {"source": "i1", "target": "i2"},
            {"source": "i2", "target": "i3"},
            {"source": "i3", "target": "i4"},
            {"source": "i4", "target": "i5"},
            {"source": "i5", "target": "i6"},
            {"source": "i6", "target": "i7"}
        ]
    }


def create_workflow_graph_with_cycle():
    """Create workflow graph with a hidden cycle."""
    return [
        {
            "id": 3000,
            "name": "Workflow A",
            "nodes": [
                {"id": "a1", "type": "manual", "parameters": {}},
                {"id": "a2", "type": "subworkflow", "parameters": {"workflowId": "3001"}}
            ],
            "connections": [{"source": "a1", "target": "a2"}]
        },
        {
            "id": 3001,
            "name": "Workflow B",
            "nodes": [
                {"id": "b1", "type": "function", "parameters": {"code": "return items"}},
                {"id": "b2", "type": "subworkflow", "parameters": {"workflowId": "3002"}}
            ],
            "connections": [{"source": "b1", "target": "b2"}]
        },
        {
            "id": 3002,
            "name": "Workflow C",
            "nodes": [
                {"id": "c1", "type": "function", "parameters": {"code": "return items"}},
                {"id": "c2", "type": "subworkflow", "parameters": {"workflowId": "3000"}}  # Cycle back to A
            ],
            "connections": [{"source": "c1", "target": "c2"}]
        }
    ]


# Fixtures
@pytest.fixture
def mock_context():
    """Create mock node execution context."""
    context = MagicMock(spec=NodeExecutionContext)
    context.node = MagicMock()
    context.node.id = "test_node"
    context.execution_data = ExecutionData()
    context.workflow_execution = MagicMock()
    context.execution_depth = 0
    return context


@pytest.fixture
def workflow_service():
    """Create mock workflow service."""
    service = AsyncMock()
    service.workflows = {}
    service.executions = []
    
    async def create_workflow(workflow_data):
        service.workflows[workflow_data["id"]] = workflow_data
        return workflow_data
    
    async def execute_workflow(workflow_id, input_data=None, max_depth=10):
        if workflow_id not in service.workflows:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        execution = MagicMock()
        execution.id = len(service.executions) + 1
        execution.workflow_id = workflow_id
        execution.status = ExecutionStatus.SUCCESS
        execution.input_data = input_data or []
        execution.output_data = [{"executed": True}]
        execution.execution_depth = 0
        execution.parent_execution_id = None
        
        service.executions.append(execution)
        return execution
    
    async def get_child_executions(parent_id):
        return [e for e in service.executions if getattr(e, 'parent_execution_id', None) == parent_id]
    
    service.create_workflow = create_workflow
    service.execute_workflow = execute_workflow
    service.get_child_executions = get_child_executions
    
    return service