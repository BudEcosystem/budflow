"""
Test Code node implementation.
Following extreme TDD methodology - tests first, implementation second.
"""

import pytest
import json
from typing import Dict, Any, List
from unittest.mock import Mock, patch

from budflow.nodes.base import NodeDefinition, NodeCategory, NodeParameter, ParameterType
from budflow.nodes.actions.code_node import CodeNode
from budflow.executor.context import NodeExecutionContext
from budflow.workflows.models import NodeType


def create_code_node_context(parameters: Dict[str, Any], input_data: List[Any]) -> Mock:
    """Helper to create a properly mocked context for Code node tests."""
    context = Mock(spec=NodeExecutionContext)
    context.node = Mock(
        type=NodeType.ACTION,
        parameters=parameters
    )
    context.input_data = input_data
    context.get_parameter = lambda key, default=None: parameters.get(key, default)
    return context


class TestCodeNodeDefinition:
    """Test Code node definition and metadata."""
    
    def test_code_node_definition(self):
        """Test Code node definition matches N8N specification."""
        definition = CodeNode.get_definition()
        
        assert definition.name == "Code"
        assert definition.type == NodeType.ACTION
        assert definition.category == NodeCategory.UTILITY
        assert definition.description == "Execute JavaScript or Python code"
        assert definition.version == "1.0"
        
        # Check parameters match N8N Code node
        params = {param.name: param for param in definition.parameters}
        
        # Language parameter
        assert "language" in params
        assert params["language"].type == ParameterType.OPTIONS
        assert params["language"].required is True
        assert params["language"].default == "python"
        assert set(params["language"].options) == {"python", "javascript"}
        
        # Code parameter
        assert "code" in params
        assert params["code"].type == ParameterType.STRING
        assert params["code"].required is True
        assert params["code"].multiline is True
        
        # Mode parameter
        assert "mode" in params
        assert params["mode"].type == ParameterType.OPTIONS
        assert params["mode"].default == "runOnceForAllItems"
        assert set(params["mode"].options) == {"runOnceForAllItems", "runOnceForEachItem"}
        
        # Continue on fail parameter
        assert "continue_on_fail" in params
        assert params["continue_on_fail"].type == ParameterType.BOOLEAN
        assert params["continue_on_fail"].default is False


class TestCodeNodePythonExecution:
    """Test Code node Python execution."""
    
    @pytest.mark.asyncio
    async def test_python_simple_calculation(self):
        """Test simple Python calculation."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Simple calculation
result = 2 + 2
return [{"result": result, "message": "Simple calculation"}]
"""
            },
            input_data=[{"input": "data"}]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["result"] == 4
        assert result[0]["message"] == "Simple calculation"
    
    @pytest.mark.asyncio
    async def test_python_access_input_data(self):
        """Test Python code accessing input data."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Access input data
total = 0
for item in $input.all():
    total += item["value"]

return [{"total": total, "count": len($input.all())}]
"""
            },
            input_data=[
                {"value": 10},
                {"value": 20},
                {"value": 30}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["total"] == 60
        assert result[0]["count"] == 3
    
    @pytest.mark.asyncio
    async def test_python_run_once_per_item(self):
        """Test Python code running once for each item."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForEachItem",
                "code": """
# Process each item
item = $input.first()
processed_value = item["value"] * 2
return [{"original": item["value"], "processed": processed_value}]
"""
            },
            input_data=[
                {"value": 5},
                {"value": 10},
                {"value": 15}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 3
        assert result[0] == {"original": 5, "processed": 10}
        assert result[1] == {"original": 10, "processed": 20}
        assert result[2] == {"original": 15, "processed": 30}
    
    @pytest.mark.asyncio
    async def test_python_import_libraries(self):
        """Test Python code importing and using libraries."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
import json
import datetime
from collections import Counter

# Use imported libraries
data = $input.all()
names = [item["name"] for item in data]
name_counts = Counter(names)

current_time = datetime.datetime.now().isoformat()

return [{
    "processed_at": current_time,
    "name_counts": dict(name_counts),
    "total_items": len(data)
}]
"""
            },
            input_data=[
                {"name": "John"},
                {"name": "Jane"},
                {"name": "John"},
                {"name": "Bob"}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert "processed_at" in result[0]
        assert result[0]["name_counts"] == {"John": 2, "Jane": 1, "Bob": 1}
        assert result[0]["total_items"] == 4
    
    @pytest.mark.asyncio
    async def test_python_error_handling(self):
        """Test Python code error handling."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Code with intentional error
result = 1 / 0  # This will cause a ZeroDivisionError
return [{"result": result}]
""",
                "continue_on_fail": False
            },
            input_data=[{"input": "data"}]
        )
        
        node = CodeNode(context)
        
        with pytest.raises(Exception):
            await node.execute()
    
    @pytest.mark.asyncio
    async def test_python_continue_on_fail(self):
        """Test Python code continuing on failure."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForEachItem",
                "code": """
item = $input.first()
if item["value"] == 0:
    result = 1 / item["value"]  # This will fail for value=0
else:
    result = 100 / item["value"]

return [{"input": item["value"], "result": result}]
""",
                "continue_on_fail": True
            },
            input_data=[
                {"value": 10},
                {"value": 0},  # This will cause error
                {"value": 5}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        # Should have results for items that didn't fail
        assert len(result) >= 2
        # Check successful executions
        success_results = [r for r in result if "result" in r]
        assert len(success_results) == 2
        assert {"input": 10, "result": 10.0} in success_results
        assert {"input": 5, "result": 20.0} in success_results
    
    @pytest.mark.asyncio
    async def test_python_complex_data_processing(self):
        """Test complex data processing with Python."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
import statistics

# Complex data processing
data = $input.all()

# Extract numeric values
values = [item["score"] for item in data if "score" in item]

# Calculate statistics
stats = {
    "mean": statistics.mean(values),
    "median": statistics.median(values),
    "stdev": statistics.stdev(values) if len(values) > 1 else 0,
    "min": min(values),
    "max": max(values)
}

# Group by category
categories = {}
for item in data:
    category = item.get("category", "unknown")
    if category not in categories:
        categories[category] = []
    categories[category].append(item["score"])

# Calculate category averages
category_stats = {}
for cat, scores in categories.items():
    category_stats[cat] = {
        "count": len(scores),
        "average": sum(scores) / len(scores)
    }

return [{
    "overall_stats": stats,
    "category_stats": category_stats,
    "total_processed": len(data)
}]
"""
            },
            input_data=[
                {"score": 85, "category": "A"},
                {"score": 92, "category": "A"},
                {"score": 78, "category": "B"},
                {"score": 88, "category": "B"},
                {"score": 95, "category": "A"},
                {"score": 82, "category": "B"}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        
        stats = result[0]["overall_stats"]
        assert stats["min"] == 78
        assert stats["max"] == 95
        assert stats["mean"] == pytest.approx(86.67, abs=0.1)
        
        category_stats = result[0]["category_stats"]
        assert category_stats["A"]["count"] == 3
        assert category_stats["A"]["average"] == pytest.approx(90.67, abs=0.1)
        assert category_stats["B"]["count"] == 3
        assert category_stats["B"]["average"] == pytest.approx(82.67, abs=0.1)


class TestCodeNodeJavaScriptExecution:
    """Test Code node JavaScript execution."""
    
    @pytest.mark.asyncio
    async def test_javascript_simple_calculation(self):
        """Test simple JavaScript calculation."""
        context = create_code_node_context(
            parameters={
                "language": "javascript",
                "mode": "runOnceForAllItems",
                "code": """
// Simple calculation
const result = 2 + 2;
return [{"result": result, "message": "Simple calculation"}];
"""
            },
            input_data=[{"input": "data"}]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["result"] == 4
        assert result[0]["message"] == "Simple calculation"
    
    @pytest.mark.asyncio
    async def test_javascript_access_input_data(self):
        """Test JavaScript code accessing input data."""
        context = create_code_node_context(
            parameters={
                "language": "javascript",
                "mode": "runOnceForAllItems",
                "code": """
// Access input data
const items = $input.all();
let total = 0;
for (const item of items) {
    total += item.value;
}

return [{"total": total, "count": items.length}];
"""
            },
            input_data=[
                {"value": 10},
                {"value": 20},
                {"value": 30}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["total"] == 60
        assert result[0]["count"] == 3
    
    @pytest.mark.asyncio
    async def test_javascript_run_once_per_item(self):
        """Test JavaScript code running once for each item."""
        context = create_code_node_context(
            parameters={
                "language": "javascript",
                "mode": "runOnceForEachItem",
                "code": """
// Process each item
const item = $input.first();
const processedValue = item.value * 2;
return [{"original": item.value, "processed": processedValue}];
"""
            },
            input_data=[
                {"value": 5},
                {"value": 10},
                {"value": 15}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 3
        assert result[0] == {"original": 5, "processed": 10}
        assert result[1] == {"original": 10, "processed": 20}
        assert result[2] == {"original": 15, "processed": 30}
    
    @pytest.mark.asyncio
    async def test_javascript_array_methods(self):
        """Test JavaScript array manipulation methods."""
        context = create_code_node_context(
            parameters={
                "language": "javascript",
                "mode": "runOnceForAllItems",
                "code": """
// Use array methods
const items = $input.all();
const numbers = items.map(item => item.value);

const filtered = numbers.filter(num => num > 15);
const sum = numbers.reduce((acc, num) => acc + num, 0);
const doubled = numbers.map(num => num * 2);

return [{
    "original": numbers,
    "filtered": filtered,
    "sum": sum,
    "doubled": doubled,
    "average": sum / numbers.length
}];
"""
            },
            input_data=[
                {"value": 10},
                {"value": 20},
                {"value": 30},
                {"value": 5}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["original"] == [10, 20, 30, 5]
        assert result[0]["filtered"] == [20, 30]
        assert result[0]["sum"] == 65
        assert result[0]["doubled"] == [20, 40, 60, 10]
        assert result[0]["average"] == 16.25


class TestCodeNodeSecurityAndSandboxing:
    """Test Code node security and sandboxing features."""
    
    @pytest.mark.asyncio
    async def test_python_restricted_imports(self):
        """Test that dangerous Python imports are restricted."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Try to import restricted modules
import os
import subprocess
import sys

# Try to execute system command
os.system("ls")
subprocess.run(["ls"])

return [{"message": "This should not work"}]
"""
            },
            input_data=[{"input": "data"}]
        )
        
        node = CodeNode(context)
        
        with pytest.raises(Exception, match="Import not allowed|Security violation"):
            await node.execute()
    
    @pytest.mark.asyncio
    async def test_javascript_restricted_globals(self):
        """Test that dangerous JavaScript globals are restricted."""
        context = create_code_node_context(
            parameters={
                "language": "javascript",
                "mode": "runOnceForAllItems",
                "code": """
// Try to access restricted globals
const fs = require('fs');
const process = global.process;
const child_process = require('child_process');

return [{"message": "This should not work"}];
"""
            },
            input_data=[{"input": "data"}]
        )
        
        node = CodeNode(context)
        
        with pytest.raises(Exception, match="require is not defined|Security violation"):
            await node.execute()
    
    @pytest.mark.asyncio
    async def test_execution_timeout(self):
        """Test that code execution times out for infinite loops."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Infinite loop that should timeout
while True:
    pass

return [{"message": "This should timeout"}]
""",
                "timeout": 2  # 2 second timeout
            },
            input_data=[{"input": "data"}]
        )
        
        node = CodeNode(context)
        
        with pytest.raises(Exception, match="timeout|execution time exceeded"):
            await node.execute()


class TestCodeNodeBuiltinFunctions:
    """Test Code node built-in functions and helpers."""
    
    @pytest.mark.asyncio
    async def test_input_helper_functions(self):
        """Test $input helper functions."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Test input helper functions
all_items = $input.all()
first_item = $input.first()
last_item = $input.last()
item_by_index = $input.item(1)

return [{
    "all_count": len(all_items),
    "first": first_item,
    "last": last_item,
    "second": item_by_index
}]
"""
            },
            input_data=[
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"},
                {"id": 3, "name": "third"}
            ]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["all_count"] == 3
        assert result[0]["first"] == {"id": 1, "name": "first"}
        assert result[0]["last"] == {"id": 3, "name": "third"}
        assert result[0]["second"] == {"id": 2, "name": "second"}
    
    @pytest.mark.asyncio
    async def test_workflow_context_access(self):
        """Test access to workflow context variables."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Access workflow context
workflow_id = $workflow.id
execution_id = $execution.id
node_name = $node.name

return [{
    "workflow_id": workflow_id,
    "execution_id": execution_id,
    "node_name": node_name,
    "timestamp": $now
}]
"""
            },
            input_data=[{"input": "data"}]
        )
        
        # Mock workflow context
        context.workflow_id = "test-workflow-123"
        context.execution_id = "exec-456"
        context.node_name = "Code Node"
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["workflow_id"] == "test-workflow-123"
        assert result[0]["execution_id"] == "exec-456"
        assert result[0]["node_name"] == "Code Node"
        assert "timestamp" in result[0]


class TestCodeNodePerformance:
    """Test Code node performance with large datasets."""
    
    @pytest.mark.asyncio
    async def test_large_dataset_processing_python(self):
        """Test processing large datasets efficiently with Python."""
        # Create 1000 items
        large_dataset = [
            {"id": i, "value": i * 2, "category": f"cat_{i % 5}"}
            for i in range(1000)
        ]
        
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Process large dataset efficiently
data = $input.all()

# Group by category and calculate statistics
category_stats = {}
for item in data:
    cat = item["category"]
    if cat not in category_stats:
        category_stats[cat] = {"count": 0, "sum": 0}
    category_stats[cat]["count"] += 1
    category_stats[cat]["sum"] += item["value"]

# Calculate averages
for cat in category_stats:
    category_stats[cat]["average"] = category_stats[cat]["sum"] / category_stats[cat]["count"]

return [{
    "total_processed": len(data),
    "categories": len(category_stats),
    "category_stats": category_stats
}]
"""
            },
            input_data=large_dataset
        )
        
        node = CodeNode(context)
        
        import time
        start_time = time.time()
        result = await node.execute()
        end_time = time.time()
        
        # Should complete within reasonable time (< 2 seconds)
        assert (end_time - start_time) < 2.0
        assert len(result) == 1
        assert result[0]["total_processed"] == 1000
        assert result[0]["categories"] == 5
        
        # Verify category statistics
        category_stats = result[0]["category_stats"]
        for cat in category_stats:
            assert category_stats[cat]["count"] == 200  # 1000 / 5 categories
    
    @pytest.mark.asyncio
    async def test_memory_efficiency(self):
        """Test memory efficiency with large data processing."""
        context = create_code_node_context(
            parameters={
                "language": "python",
                "mode": "runOnceForAllItems",
                "code": """
# Memory efficient processing - use generators
def process_items():
    for item in $input.all():
        if item["value"] % 2 == 0:
            yield {"id": item["id"], "processed_value": item["value"] * 2}

# Convert generator to list for return
processed = list(process_items())

return [{
    "processed_count": len(processed),
    "sample": processed[:5] if processed else []
}]
"""
            },
            input_data=[{"id": i, "value": i} for i in range(100)]
        )
        
        node = CodeNode(context)
        result = await node.execute()
        
        assert len(result) == 1
        assert result[0]["processed_count"] == 50  # Only even numbers
        assert len(result[0]["sample"]) == 5