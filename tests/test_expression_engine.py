"""
Comprehensive tests for advanced expression engine with N8N variables and Jinja templates.

The expression engine supports:
- N8N variables: $node, $input, $json, $now, $workflow, $execution
- Jinja2 templates with extensive filters and functions
- JavaScript-style expressions for N8N compatibility
- Data transformation and manipulation
- Date/time operations
- String processing
- Array/object operations
"""

import pytest
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from budflow.executor.expression_engine import (
    ExpressionEngine,
    ExpressionContext,
    ExpressionError,
    JinjaExpressionEngine,
    N8NExpressionEngine,
    JavaScriptExpressionEngine
)
from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowExecution, NodeExecution,
    ExecutionStatus, NodeExecutionStatus, ExecutionMode, NodeType
)


class TestExpressionEngine:
    """Test suite for the main expression engine."""

    @pytest.fixture
    def sample_workflow(self):
        """Create a sample workflow for testing."""
        workflow = Workflow(
            id=1,
            name="Test Workflow",
            user_id=1
        )
        workflow.uuid = str(uuid4())
        return workflow

    @pytest.fixture
    def sample_execution(self, sample_workflow):
        """Create a sample execution for testing."""
        execution = WorkflowExecution(
            id=1,
            workflow_id=1,
            mode=ExecutionMode.MANUAL,
            status=ExecutionStatus.RUNNING,
            data={"test": "data"}
        )
        execution.uuid = str(uuid4())
        execution.workflow = sample_workflow
        return execution

    @pytest.fixture
    def expression_context(self, sample_workflow, sample_execution):
        """Create expression context for testing."""
        context = ExpressionContext(
            workflow=sample_workflow,
            execution=sample_execution,
            current_node_id=1,
            current_item_index=0
        )
        
        # Add sample node data
        # Use node ID "1" to match current_node_id=1
        context.add_node_data("1", [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ])
        
        # Also add with name for $node.Start access
        context.add_node_data("Start", [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ])
        
        context.add_node_data("Process", [
            {"processed": True, "result": "success", "timestamp": "2024-01-01T12:00:00Z"}
        ])
        
        return context

    @pytest.mark.asyncio
    async def test_expression_engine_initialization(self):
        """Test expression engine initialization."""
        engine = ExpressionEngine()
        
        assert engine.jinja_engine is not None
        assert engine.n8n_engine is not None
        assert engine.js_engine is not None

    @pytest.mark.asyncio
    async def test_detect_expression_type(self):
        """Test detection of different expression types."""
        engine = ExpressionEngine()
        
        # Jinja expressions
        assert engine.detect_expression_type("{{ variable }}") == "jinja"
        assert engine.detect_expression_type("{% if condition %}text{% endif %}") == "jinja"
        assert engine.detect_expression_type("{# comment #}") == "jinja"
        
        # N8N expressions
        assert engine.detect_expression_type("{{ $json.field }}") == "n8n"
        assert engine.detect_expression_type("{{ $node.Start.json }}") == "n8n"
        assert engine.detect_expression_type("{{ $input.all() }}") == "n8n"
        
        # JavaScript expressions
        assert engine.detect_expression_type("$json.field") == "javascript"
        assert engine.detect_expression_type("$node('Start').json") == "javascript"
        assert engine.detect_expression_type("new Date()") == "javascript"
        
        # Plain text
        assert engine.detect_expression_type("plain text") == "text"

    @pytest.mark.asyncio
    async def test_evaluate_simple_expressions(self, expression_context):
        """Test evaluation of simple expressions."""
        engine = ExpressionEngine()
        
        # Simple variable access
        result = await engine.evaluate("{{ name }}", expression_context, {"name": "test"})
        assert result == "test"
        
        # Math operations
        result = await engine.evaluate("{{ 5 + 3 }}", expression_context)
        assert result == 8
        
        # String operations
        result = await engine.evaluate("{{ 'hello' | upper }}", expression_context)
        assert result == "HELLO"

    @pytest.mark.asyncio
    async def test_evaluate_n8n_variables(self, expression_context):
        """Test evaluation of N8N-style variables."""
        engine = ExpressionEngine()
        
        # $json access
        result = await engine.evaluate("{{ $json.name }}", expression_context)
        assert result == "John"  # First item from Start node
        
        # $node access - accessing first item since there are multiple
        result = await engine.evaluate("{{ $node.Start.json[0].email }}", expression_context)
        assert result == "john@example.com"
        
        # $input access
        result = await engine.evaluate("{{ $input.all() | length }}", expression_context)
        assert result == 2

    @pytest.mark.asyncio
    async def test_javascript_expressions(self, expression_context):
        """Test JavaScript-style expressions."""
        engine = ExpressionEngine()
        
        # Simple property access
        result = await engine.evaluate("$json.name", expression_context)
        assert result == "John"
        
        # Function calls - accessing first item since there are multiple
        result = await engine.evaluate("$node('Start').json[0].name", expression_context)
        assert result == "John"
        
        # Array operations
        result = await engine.evaluate("$input.all().length", expression_context)
        assert result == 2


class TestJinjaExpressionEngine:
    """Test suite for Jinja expression engine."""

    @pytest.fixture
    def jinja_engine(self):
        """Create Jinja expression engine."""
        return JinjaExpressionEngine()

    @pytest.fixture
    def sample_context(self):
        """Create sample context data."""
        return {
            "user": {"name": "John", "age": 30, "email": "john@example.com"},
            "items": [
                {"id": 1, "title": "Item 1", "price": 10.50},
                {"id": 2, "title": "Item 2", "price": 25.00}
            ],
            "timestamp": "2024-01-01T12:00:00Z",
            "settings": {"theme": "dark", "notifications": True}
        }

    @pytest.mark.asyncio
    async def test_basic_jinja_templates(self, jinja_engine, sample_context):
        """Test basic Jinja template functionality."""
        # Variable substitution
        result = await jinja_engine.evaluate("Hello {{ user.name }}!", sample_context)
        assert result == "Hello John!"
        
        # Conditional rendering
        template = "{% if user.age >= 18 %}Adult{% else %}Minor{% endif %}"
        result = await jinja_engine.evaluate(template, sample_context)
        assert result == "Adult"
        
        # Loop rendering
        template = "{% for item in items %}{{ item.title }}{% if not loop.last %}, {% endif %}{% endfor %}"
        result = await jinja_engine.evaluate(template, sample_context)
        assert result == "Item 1, Item 2"

    @pytest.mark.asyncio
    async def test_jinja_filters(self, jinja_engine, sample_context):
        """Test Jinja built-in and custom filters."""
        # String filters
        result = await jinja_engine.evaluate("{{ user.name | upper }}", sample_context)
        assert result == "JOHN"
        
        result = await jinja_engine.evaluate("{{ user.email | replace('@', ' at ') }}", sample_context)
        assert result == "john at example.com"
        
        # Number filters
        result = await jinja_engine.evaluate("{{ items | sum(attribute='price') }}", sample_context)
        assert result == 35.50
        
        # List filters - use as_object to return actual list
        result = await jinja_engine.evaluate("{{ items | map(attribute='title') | list | as_object }}", sample_context)
        assert result == ["Item 1", "Item 2"]

    @pytest.mark.asyncio
    async def test_custom_jinja_filters(self, jinja_engine, sample_context):
        """Test custom Jinja filters for workflow processing."""
        # Date formatting filter
        result = await jinja_engine.evaluate("{{ timestamp | dateformat('%Y-%m-%d') }}", sample_context)
        assert result == "2024-01-01"
        
        # JSON parsing filter
        json_string = '{"key": "value"}'
        result = await jinja_engine.evaluate("{{ json_str | fromjson }}", {"json_str": json_string})
        assert result == {"key": "value"}
        
        # Base64 encoding filter
        result = await jinja_engine.evaluate("{{ 'hello' | b64encode }}", sample_context)
        assert result == "aGVsbG8="
        
        # URL encoding filter
        result = await jinja_engine.evaluate("{{ 'hello world' | urlencode }}", sample_context)
        assert result == "hello%20world"

    @pytest.mark.asyncio
    async def test_jinja_functions(self, jinja_engine, sample_context):
        """Test custom Jinja global functions."""
        # Random functions
        result = await jinja_engine.evaluate("{{ random_int(1, 10) }}", sample_context)
        assert 1 <= result <= 10
        
        result = await jinja_engine.evaluate("{{ random_choice(['a', 'b', 'c']) }}", sample_context)
        assert result in ['a', 'b', 'c']
        
        # UUID generation
        result = await jinja_engine.evaluate("{{ uuid4() }}", sample_context)
        assert len(result) == 36  # UUID string length
        
        # Hash functions
        result = await jinja_engine.evaluate("{{ md5('test') }}", sample_context)
        assert result == "098f6bcd4621d373cade4e832627b4f6"

    @pytest.mark.asyncio
    async def test_advanced_jinja_features(self, jinja_engine, sample_context):
        """Test advanced Jinja features."""
        # Macros
        template = """
        {% macro render_item(item) -%}
        {{ item.title }} - ${{ item.price }}
        {%- endmacro %}
        {% for item in items %}{{ render_item(item) }}{% if not loop.last %}
        {% endif %}{% endfor %}
        """
        result = await jinja_engine.evaluate(template, sample_context)
        assert "Item 1 - $10.5" in result
        assert "Item 2 - $25.0" in result
        
        # Block assignments
        template = """
        {% set total_price = items | sum(attribute='price') %}
        Total: ${{ total_price }}
        """
        result = await jinja_engine.evaluate(template, sample_context)
        assert "Total: $35.5" in result

    @pytest.mark.asyncio
    async def test_jinja_error_handling(self, jinja_engine):
        """Test Jinja error handling."""
        # Undefined variable
        with pytest.raises(ExpressionError) as exc_info:
            await jinja_engine.evaluate("{{ undefined_var }}", {})
        assert "undefined" in str(exc_info.value).lower()
        
        # Syntax error
        with pytest.raises(ExpressionError):
            await jinja_engine.evaluate("{{ unclosed", {})
        
        # Invalid filter
        with pytest.raises(ExpressionError):
            await jinja_engine.evaluate("{{ 'test' | nonexistent_filter }}", {})


class TestN8NExpressionEngine:
    """Test suite for N8N-compatible expression engine."""

    @pytest.fixture
    def n8n_engine(self):
        """Create N8N expression engine."""
        return N8NExpressionEngine()

    @pytest.fixture
    def n8n_context(self):
        """Create N8N-style context."""
        # Create a context similar to expression_context
        context = ExpressionContext(
            workflow=Workflow(id=1, name="Test Workflow", user_id=1),
            execution=WorkflowExecution(
                id=1,
                workflow_id=1,
                mode=ExecutionMode.MANUAL,
                status=ExecutionStatus.RUNNING,
                data={"test": "data"}
            ),
            current_node_id=1,
            current_item_index=0
        )
        
        # Add sample node data
        context.add_node_data("1", [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ])
        
        context.add_node_data("Start", [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ])
        
        return context

    @pytest.mark.asyncio
    async def test_n8n_json_variable(self, n8n_engine, n8n_context):
        """Test $json variable access."""
        # Current item access
        result = await n8n_engine.evaluate("{{ $json.name }}", n8n_context)
        assert result == "John"
        
        result = await n8n_engine.evaluate("{{ $json.email }}", n8n_context)
        assert result == "john@example.com"
        
        # Nested property access
        n8n_context.add_node_data("Test", [{"user": {"profile": {"name": "Test User"}}}])
        n8n_context.current_node_id = "Test"
        result = await n8n_engine.evaluate("{{ $json.user.profile.name }}", n8n_context)
        assert result == "Test User"

    @pytest.mark.asyncio
    async def test_n8n_node_variable(self, n8n_engine, n8n_context):
        """Test $node variable access."""
        # Access specific node data - since there are multiple items, we need to use index
        result = await n8n_engine.evaluate("{{ $node.Start.json[0].name }}", n8n_context)
        assert result == "John"
        
        # Access node with index
        result = await n8n_engine.evaluate("{{ $node.Start.json[1].name }}", n8n_context)
        assert result == "Jane"
        
        # Access all items from node
        result = await n8n_engine.evaluate("{{ $node.Start.json | length }}", n8n_context)
        assert result == 2

    @pytest.mark.asyncio
    async def test_n8n_input_variable(self, n8n_engine, n8n_context):
        """Test $input variable access."""
        # Get all input items
        result = await n8n_engine.evaluate("{{ $input.all() | length }}", n8n_context)
        assert result == 2
        
        # Get first input item
        result = await n8n_engine.evaluate("{{ $input.first().name }}", n8n_context)
        assert result == "John"
        
        # Get last input item
        result = await n8n_engine.evaluate("{{ $input.last().name }}", n8n_context)
        assert result == "Jane"
        
        # Get specific input item
        result = await n8n_engine.evaluate("{{ $input.item(1).name }}", n8n_context)
        assert result == "Jane"

    @pytest.mark.asyncio
    async def test_n8n_workflow_variables(self, n8n_engine, n8n_context):
        """Test $workflow and $execution variables."""
        # Workflow properties
        result = await n8n_engine.evaluate("{{ $workflow.id }}", n8n_context)
        assert result == 1
        
        result = await n8n_engine.evaluate("{{ $workflow.name }}", n8n_context)
        assert result == "Test Workflow"
        
        # Execution properties
        result = await n8n_engine.evaluate("{{ $execution.id }}", n8n_context)
        assert result == 1
        
        result = await n8n_engine.evaluate("{{ $execution.mode }}", n8n_context)
        assert result == "manual"

    @pytest.mark.asyncio
    async def test_n8n_now_variable(self, n8n_engine, n8n_context):
        """Test $now variable for date/time operations."""
        # Current timestamp - $now outputs an ISO string
        result = await n8n_engine.evaluate("{{ $now }}", n8n_context)
        assert isinstance(result, str)  # DateTimeHelper.__str__ returns ISO string
        assert 'T' in result  # ISO format check
        
        # Date formatting
        result = await n8n_engine.evaluate("{{ $now.format('YYYY-MM-DD') }}", n8n_context)
        assert len(result) == 10  # YYYY-MM-DD format
        assert result.count('-') == 2  # Date format check
        
        # Date arithmetic
        result = await n8n_engine.evaluate("{{ $now.plus(1, 'day').format('YYYY-MM-DD') }}", n8n_context)
        assert isinstance(result, str)
        assert len(result) == 10  # YYYY-MM-DD format

    @pytest.mark.asyncio
    async def test_n8n_utility_functions(self, n8n_engine, n8n_context):
        """Test N8N utility functions."""
        # String functions - split returns a list
        result = await n8n_engine.evaluate("{{ 'hello world'.split(' ') | as_object }}", n8n_context)
        assert result == ["hello", "world"]
        
        # Array functions - use Jinja2 select filter
        data = [1, 2, 3, 4, 5]
        result = await n8n_engine.evaluate("{{ data | select('>', 3) | list | as_object }}", 
                                         n8n_context, {"data": data})
        assert result == [4, 5]
        
        # Object functions - use dict methods
        obj = {"a": 1, "b": 2, "c": 3}
        result = await n8n_engine.evaluate("{{ obj.keys() | list | as_object }}", 
                                         n8n_context, {"obj": obj})
        assert result == ["a", "b", "c"]


class TestJavaScriptExpressionEngine:
    """Test suite for JavaScript-compatible expression engine."""

    @pytest.fixture
    def js_engine(self):
        """Create JavaScript expression engine."""
        return JavaScriptExpressionEngine()

    @pytest.fixture
    def js_context(self):
        """Create JavaScript execution context."""
        # Create a context similar to expression_context
        context = ExpressionContext(
            workflow=Workflow(id=1, name="Test Workflow", user_id=1),
            execution=WorkflowExecution(
                id=1,
                workflow_id=1,
                mode=ExecutionMode.MANUAL,
                status=ExecutionStatus.RUNNING,
                data={"test": "data"}
            ),
            current_node_id=1,
            current_item_index=0
        )
        
        # Add sample node data
        context.add_node_data("1", [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ])
        
        context.add_node_data("Start", [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ])
        
        return context

    @pytest.mark.asyncio
    async def test_javascript_syntax(self, js_engine, js_context):
        """Test JavaScript syntax support."""
        # Property access
        result = await js_engine.evaluate("$json.name", js_context)
        assert result == "John"
        
        # Method calls
        result = await js_engine.evaluate("$input.all().length", js_context)
        assert result == 2
        
        # Array access
        result = await js_engine.evaluate("$node('Start').json[0].name", js_context)
        assert result == "John"

    @pytest.mark.asyncio
    async def test_javascript_functions(self, js_engine, js_context):
        """Test JavaScript function support."""
        # Array methods
        result = await js_engine.evaluate(
            "$input.all().map(item => item.name)", 
            js_context
        )
        assert result == ["John", "Jane"]
        
        # Filter methods
        result = await js_engine.evaluate(
            "$input.all().filter(item => item.id > 1)", 
            js_context
        )
        assert len(result) == 1
        assert result[0]["name"] == "Jane"
        
        # Reduce methods
        result = await js_engine.evaluate(
            "$input.all().reduce((acc, item) => acc + item.id, 0)", 
            js_context
        )
        assert result == 3  # 1 + 2

    @pytest.mark.asyncio
    async def test_javascript_date_operations(self, js_engine, js_context):
        """Test JavaScript date operations."""
        # Date creation
        result = await js_engine.evaluate("new Date().getFullYear()", js_context)
        assert isinstance(result, int)
        assert result >= 2024
        
        # Date formatting - use Jinja filter for split
        result = await js_engine.evaluate(
            "new Date('2024-01-01').toISOString() | js_split('T') | first", 
            js_context
        )
        assert result == "2024-01-01"

    @pytest.mark.asyncio
    async def test_javascript_math_operations(self, js_engine, js_context):
        """Test JavaScript math operations."""
        # Basic math
        result = await js_engine.evaluate("Math.max(1, 2, 3)", js_context)
        assert result == 3
        
        # Complex calculations
        result = await js_engine.evaluate(
            "Math.round($input.all().reduce((acc, item) => acc + item.id, 0) / $input.all().length)", 
            js_context
        )
        assert result == 2  # Average of 1 and 2, rounded


class TestExpressionSecurity:
    """Test suite for expression security and sandboxing."""

    @pytest.fixture
    def secure_engine(self):
        """Create expression engine with security enabled."""
        return ExpressionEngine(enable_security=True)

    @pytest.mark.asyncio
    async def test_prevent_dangerous_operations(self, secure_engine):
        """Test prevention of dangerous operations."""
        context = {}
        
        # File system access should be blocked
        with pytest.raises(ExpressionError):
            await secure_engine.evaluate("{{ open('/etc/passwd') }}", context)
        
        # Import statements should be blocked
        with pytest.raises(ExpressionError):
            await secure_engine.evaluate("{{ __import__('os') }}", context)
        
        # Subprocess execution should be blocked
        with pytest.raises(ExpressionError):
            await secure_engine.evaluate("{{ subprocess.call(['ls']) }}", context)

    @pytest.mark.asyncio
    async def test_resource_limits(self, secure_engine):
        """Test resource limits for expressions."""
        context = {}
        
        # Long-running loops should timeout or error
        with pytest.raises(ExpressionError):
            await secure_engine.evaluate("{% for i in range_list(1000000) %}{{ i }}{% endfor %}", context)
        
        # Large memory allocation should be limited by range_list
        with pytest.raises(ExpressionError):
            await secure_engine.evaluate("{{ range_list(1000000) }}", context)

    @pytest.mark.asyncio
    async def test_safe_operations_allowed(self, secure_engine):
        """Test that safe operations are still allowed."""
        context = {"data": [1, 2, 3]}
        
        # Normal template operations should work
        result = await secure_engine.evaluate("{{ data | sum }}", context)
        assert result == 6
        
        # String operations should work
        result = await secure_engine.evaluate("{{ 'hello' | upper }}", context)
        assert result == "HELLO"


class TestExpressionPerformance:
    """Test suite for expression performance optimization."""

    @pytest.fixture
    def performance_engine(self):
        """Create expression engine with performance optimization."""
        return ExpressionEngine(enable_caching=True, optimize_performance=True)

    @pytest.mark.asyncio
    async def test_expression_caching(self, performance_engine):
        """Test expression caching for performance."""
        context = {"value": 42}
        
        # First evaluation should compile and cache
        result1 = await performance_engine.evaluate("{{ value * 2 }}", context)
        assert result1 == 84
        
        # Second evaluation should use cache
        result2 = await performance_engine.evaluate("{{ value * 2 }}", context)
        assert result2 == 84
        
        # Verify cache is being used
        assert performance_engine.total_cache_size > 0

    @pytest.mark.asyncio
    async def test_batch_evaluation(self, performance_engine):
        """Test batch evaluation for multiple expressions."""
        expressions = [
            "{{ value + 1 }}",
            "{{ value * 2 }}",
            "{{ value / 2 }}"
        ]
        context = {"value": 10}
        
        results = await performance_engine.evaluate_batch(expressions, context)
        assert results == [11, 20, 5]

    @pytest.mark.asyncio
    async def test_async_evaluation(self, performance_engine):
        """Test asynchronous expression evaluation."""
        import asyncio
        
        # Test that expressions are evaluated asynchronously
        context = {"value": 42}
        
        # Multiple evaluations should work concurrently
        tasks = [
            performance_engine.evaluate("{{ value + 1 }}", context),
            performance_engine.evaluate("{{ value + 2 }}", context),
            performance_engine.evaluate("{{ value + 3 }}", context),
        ]
        
        results = await asyncio.gather(*tasks)
        assert results == [43, 44, 45]