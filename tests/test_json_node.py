"""
Test JSON/Set node implementation.
Following extreme TDD methodology - tests first, implementation second.
"""

import pytest
import json
from typing import Dict, Any, List
from unittest.mock import Mock

from budflow.nodes.base import NodeDefinition, NodeCategory, NodeParameter, ParameterType
from budflow.nodes.actions.json_node import JsonNode
from budflow.executor.context import NodeExecutionContext
from budflow.workflows.models import NodeType


def create_json_node_context(parameters: Dict[str, Any], input_data: List[Any]) -> Mock:
    """Helper to create a properly mocked context for JSON node tests."""
    context = Mock(spec=NodeExecutionContext)
    context.node = Mock(
        type=NodeType.ACTION,
        parameters=parameters
    )
    context.input_data = input_data
    context.get_parameter = lambda key, default=None: parameters.get(key, default)
    return context


class TestJsonNodeDefinition:
    """Test JSON node definition and metadata."""
    
    def test_json_node_definition(self):
        """Test JSON node definition matches N8N specification."""
        definition = JsonNode.get_definition()
        
        assert definition.name == "Edit Fields"
        assert definition.type == NodeType.ACTION
        assert definition.category == NodeCategory.TRANSFORM
        assert definition.description == "Add, remove, select and update JSON fields"
        assert definition.version == "1.0"
        
        # Check parameters match N8N JSON node
        params = {param.name: param for param in definition.parameters}
        
        # Operations parameter
        assert "operation" in params
        assert params["operation"].type == ParameterType.OPTIONS
        assert params["operation"].required is True
        assert params["operation"].default == "set"
        assert set(params["operation"].options) == {
            "set", "unset", "select", "append", "prepend", "push", "shift"
        }
        
        # Fields parameter for set operation
        assert "fields" in params
        assert params["fields"].type == ParameterType.ARRAY
        
        # Keep only set parameter
        assert "keep_only_set" in params
        assert params["keep_only_set"].type == ParameterType.BOOLEAN
        assert params["keep_only_set"].default is False


class TestJsonNodeSetOperation:
    """Test JSON node SET operation (most common)."""
    
    @pytest.mark.asyncio
    async def test_set_single_field(self):
        """Test setting a single field."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [
                    {
                        "name": "status",
                        "value": "active"
                    }
                ],
                "keep_only_set": False
            },
            input_data=[
                {"id": 1, "name": "John"},
                {"id": 2, "name": "Jane"}
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "John", "status": "active"}
        assert result[1] == {"id": 2, "name": "Jane", "status": "active"}
    
    @pytest.mark.asyncio
    async def test_set_multiple_fields(self):
        """Test setting multiple fields."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [
                    {
                        "name": "status",
                        "value": "active"
                    },
                    {
                        "name": "priority",
                        "value": "high"
                    },
                    {
                        "name": "count",
                        "value": 42
                    }
                ],
                "keep_only_set": False
            },
            input_data=[{"id": 1, "name": "John"}]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "name": "John",
            "status": "active",
            "priority": "high",
            "count": 42
        }
        assert result[0] == expected
    
    @pytest.mark.asyncio
    async def test_set_with_expressions(self):
        """Test setting fields with expressions."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [
                    {
                        "name": "full_name",
                        "value": "{{first_name}} {{last_name}}"
                    },
                    {
                        "name": "age_next_year",
                        "value": "={{age + 1}}"
                    },
                    {
                        "name": "uppercase_email",
                        "value": "={{email.upper()}}"
                    }
                ],
                "keep_only_set": False
            },
            input_data=[
                {
                    "first_name": "John",
                    "last_name": "Doe",
                    "age": 30,
                    "email": "john@example.com"
                }
            ]
        )
        
        node = JsonNode(context)
        
        # Mock expression evaluation
        def mock_evaluate(expr, data):
            if expr == "{{first_name}} {{last_name}}":
                return f"{data['first_name']} {data['last_name']}"
            elif expr == "{{age + 1}}":
                return data["age"] + 1
            elif expr == "{{email.upper()}}":
                return data["email"].upper()
            return expr
        
        node.evaluate_expression = mock_evaluate
        
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["full_name"] == "John Doe"
        assert result[0]["age_next_year"] == 31
        assert result[0]["uppercase_email"] == "JOHN@EXAMPLE.COM"
    
    @pytest.mark.asyncio
    async def test_set_nested_fields(self):
        """Test setting nested object fields."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [
                    {
                        "name": "user.profile.avatar",
                        "value": "https://example.com/avatar.jpg"
                    },
                    {
                        "name": "metadata.created_at",
                        "value": "2024-01-01T00:00:00Z"
                    }
                ],
                "keep_only_set": False
            },
            input_data=[
                {
                    "id": 1,
                    "user": {"name": "John"},
                    "metadata": {"updated_at": "2024-01-02T00:00:00Z"}
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "user": {
                "name": "John",
                "profile": {
                    "avatar": "https://example.com/avatar.jpg"
                }
            },
            "metadata": {
                "updated_at": "2024-01-02T00:00:00Z",
                "created_at": "2024-01-01T00:00:00Z"
            }
        }
        assert result[0] == expected
    
    @pytest.mark.asyncio
    async def test_set_keep_only_set(self):
        """Test setting fields with keep_only_set=True."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [
                    {
                        "name": "id",
                        "value": "={{id}}"
                    },
                    {
                        "name": "processed_name",
                        "value": "={{name.upper()}}"
                    }
                ],
                "keep_only_set": True
            },
            input_data=[
                {
                    "id": 1,
                    "name": "John",
                    "email": "john@example.com",
                    "age": 30
                }
            ]
        )
        
        node = JsonNode(context)
        
        # Mock expression evaluation
        def mock_evaluate(expr, data):
            if expr == "{{id}}":
                return data["id"]
            elif expr == "{{name.upper()}}":
                return data["name"].upper()
            return expr
        
        node.evaluate_expression = mock_evaluate
        
        result = await node.execute()
        
        assert len(result) == 1
        # Should only contain the set fields
        expected = {
            "id": 1,
            "processed_name": "JOHN"
        }
        assert result[0] == expected


class TestJsonNodeUnsetOperation:
    """Test JSON node UNSET operation."""
    
    @pytest.mark.asyncio
    async def test_unset_single_field(self):
        """Test removing a single field."""
        context = create_json_node_context(
            parameters={
                "operation": "unset",
                "fields_to_remove": ["email"]
            },
            input_data=[
                {
                    "id": 1,
                    "name": "John",
                    "email": "john@example.com",
                    "age": 30
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "name": "John",
            "age": 30
        }
        assert result[0] == expected
    
    @pytest.mark.asyncio
    async def test_unset_multiple_fields(self):
        """Test removing multiple fields."""
        context = create_json_node_context(
            parameters={
                "operation": "unset",
                "fields_to_remove": ["email", "age", "temp_field"]
            },
            input_data=[
                {
                    "id": 1,
                    "name": "John",
                    "email": "john@example.com",
                    "age": 30,
                    "active": True
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "name": "John",
            "active": True
        }
        assert result[0] == expected
    
    @pytest.mark.asyncio
    async def test_unset_nested_fields(self):
        """Test removing nested fields."""
        context = create_json_node_context(
            parameters={
                "operation": "unset",
                "fields_to_remove": ["user.email", "metadata.temp"]
            },
            input_data=[
                {
                    "id": 1,
                    "user": {
                        "name": "John",
                        "email": "john@example.com",
                        "age": 30
                    },
                    "metadata": {
                        "created_at": "2024-01-01",
                        "temp": "remove_me"
                    }
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "user": {
                "name": "John",
                "age": 30
            },
            "metadata": {
                "created_at": "2024-01-01"
            }
        }
        assert result[0] == expected


class TestJsonNodeSelectOperation:
    """Test JSON node SELECT operation."""
    
    @pytest.mark.asyncio
    async def test_select_single_field(self):
        """Test selecting a single field."""
        context = create_json_node_context(
            parameters={
                "operation": "select",
                "fields_to_select": ["name"]
            },
            input_data=[
                {
                    "id": 1,
                    "name": "John",
                    "email": "john@example.com",
                    "age": 30
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {"name": "John"}
        assert result[0] == expected
    
    @pytest.mark.asyncio
    async def test_select_multiple_fields(self):
        """Test selecting multiple fields."""
        context = create_json_node_context(
            parameters={
                "operation": "select",
                "fields_to_select": ["id", "name", "active"]
            },
            input_data=[
                {
                    "id": 1,
                    "name": "John",
                    "email": "john@example.com",
                    "age": 30,
                    "active": True,
                    "temp": "data"
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "name": "John",
            "active": True
        }
        assert result[0] == expected


class TestJsonNodeArrayOperations:
    """Test JSON node array operations (append, prepend, push, shift)."""
    
    @pytest.mark.asyncio
    async def test_append_operation(self):
        """Test appending to arrays."""
        context = create_json_node_context(
            parameters={
                "operation": "append",
                "fields": [
                    {
                        "name": "tags",
                        "value": "new-tag"
                    },
                    {
                        "name": "scores",
                        "value": 95
                    }
                ]
            },
            input_data=[
                {
                    "id": 1,
                    "tags": ["tag1", "tag2"],
                    "scores": [80, 85, 90]
                }
            ]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        expected = {
            "id": 1,
            "tags": ["tag1", "tag2", "new-tag"],
            "scores": [80, 85, 90, 95]
        }
        assert result[0] == expected


class TestJsonNodeEdgeCases:
    """Test JSON node edge cases and error handling."""
    
    @pytest.mark.asyncio
    async def test_empty_input_data(self):
        """Test handling empty input data."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [{"name": "status", "value": "active"}]
            },
            input_data=[]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_invalid_operation(self):
        """Test handling invalid operation."""
        context = create_json_node_context(
            parameters={
                "operation": "invalid_op",
                "fields": []
            },
            input_data=[{"id": 1}]
        )
        
        node = JsonNode(context)
        
        with pytest.raises(ValueError, match="Unsupported operation"):
            await node.execute()
    
    @pytest.mark.asyncio
    async def test_non_dict_input(self):
        """Test handling non-dictionary input."""
        context = create_json_node_context(
            parameters={
                "operation": "set",
                "fields": [{"name": "new_field", "value": "value"}]
            },
            input_data=["string", 123, True, None]
        )
        
        node = JsonNode(context)
        result = await node.execute()
        
        # Should convert non-dict inputs to dicts
        assert len(result) == 4
        assert result[0] == {"_original": "string", "new_field": "value"}
        assert result[1] == {"_original": 123, "new_field": "value"}
        assert result[2] == {"_original": True, "new_field": "value"}
        assert result[3] == {"_original": None, "new_field": "value"}