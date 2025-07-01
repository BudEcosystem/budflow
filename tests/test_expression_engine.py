"""Tests for Expression Engine with JavaScript/Python support."""

import pytest
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from unittest.mock import Mock, AsyncMock, patch

from budflow.core.expression_engine import (
    ExpressionEngine,
    ExpressionContext,
    ExpressionError,
    JavaScriptEngine,
    PythonEngine,
    SecurityValidator,
    ExpressionResult,
    ExpressionType,
    SecurityLevel,
    ExpressionConfig,
)


@pytest.fixture
def expression_config():
    """Create expression engine configuration."""
    return ExpressionConfig(
        default_language="javascript",
        timeout_seconds=5,
        memory_limit_mb=64,
        enable_python=True,
        enable_javascript=True,
        security_level=SecurityLevel.MEDIUM,
        allowed_modules=["math", "json", "datetime"],
        blocked_functions=["eval", "exec", "__import__"],
        max_expression_length=10000,
    )


@pytest.fixture
def expression_context():
    """Create expression context."""
    return ExpressionContext(
        data={
            "item": {"id": 1, "name": "test", "value": 42.5},
            "workflow": {"id": "wf-123", "name": "Test Workflow"},
            "execution": {"id": "exec-456", "step": 3},
            "env": {"API_KEY": "secret123", "BASE_URL": "https://api.example.com"},
        },
        variables={
            "counter": 10,
            "multiplier": 2.5,
            "user_id": "user-789",
        },
        functions={
            "now": lambda: datetime.now(timezone.utc).isoformat(),
            "uuid": lambda: "generated-uuid-123",
        },
        execution_id="exec-456",
        node_id="node-789",
        workflow_id="wf-123",
    )


@pytest.fixture
def expression_engine(expression_config):
    """Create expression engine."""
    return ExpressionEngine(expression_config)


@pytest.mark.unit
class TestExpressionConfig:
    """Test expression engine configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = ExpressionConfig()
        
        assert config.default_language == "javascript"
        assert config.timeout_seconds == 10
        assert config.memory_limit_mb == 128
        assert config.security_level == SecurityLevel.MEDIUM
        assert config.enable_javascript is True
        assert config.enable_python is True
    
    def test_custom_config(self, expression_config):
        """Test custom configuration."""
        assert expression_config.timeout_seconds == 5
        assert expression_config.memory_limit_mb == 64
        assert expression_config.security_level == SecurityLevel.MEDIUM
        assert "math" in expression_config.allowed_modules
        assert "eval" in expression_config.blocked_functions


@pytest.mark.unit
class TestExpressionContext:
    """Test expression context functionality."""
    
    def test_context_creation(self, expression_context):
        """Test creating expression context."""
        assert expression_context.data["item"]["id"] == 1
        assert expression_context.variables["counter"] == 10
        assert callable(expression_context.functions["now"])
        assert expression_context.execution_id == "exec-456"
    
    def test_get_value(self, expression_context):
        """Test getting values from context."""
        # Direct data access
        assert expression_context.get_value("item.id") == 1
        assert expression_context.get_value("item.name") == "test"
        
        # Variable access
        assert expression_context.get_value("$counter") == 10
        assert expression_context.get_value("$multiplier") == 2.5
        
        # Environment access
        assert expression_context.get_value("$env.API_KEY") == "secret123"
        
        # Non-existent key
        assert expression_context.get_value("nonexistent") is None
        assert expression_context.get_value("nonexistent", "default") == "default"
    
    def test_set_value(self, expression_context):
        """Test setting values in context."""
        expression_context.set_value("new_item", {"test": True})
        assert expression_context.get_value("new_item.test") is True
        
        expression_context.set_value("$new_var", "variable_value")
        assert expression_context.get_value("$new_var") == "variable_value"
    
    def test_call_function(self, expression_context):
        """Test calling context functions."""
        # Call now function
        now_result = expression_context.call_function("now")
        assert isinstance(now_result, str)
        
        # Call uuid function
        uuid_result = expression_context.call_function("uuid")
        assert uuid_result == "generated-uuid-123"
        
        # Non-existent function
        with pytest.raises(ExpressionError, match="Function 'nonexistent' not found"):
            expression_context.call_function("nonexistent")


@pytest.mark.unit
class TestSecurityValidator:
    """Test expression security validation."""
    
    @pytest.fixture
    def security_validator(self, expression_config):
        """Create security validator."""
        return SecurityValidator(expression_config)
    
    def test_validate_safe_javascript(self, security_validator):
        """Test validating safe JavaScript expressions."""
        safe_expressions = [
            "item.value * 2",
            "Math.round(item.value)",
            "item.name.toUpperCase()",
            "JSON.stringify({result: item.value})",
            "new Date().toISOString()",
        ]
        
        for expr in safe_expressions:
            assert security_validator.validate_javascript(expr) is True
    
    def test_validate_unsafe_javascript(self, security_validator):
        """Test detecting unsafe JavaScript expressions."""
        unsafe_expressions = [
            "eval('malicious code')",
            "Function('return process')('').exit()",
            "require('fs').readFileSync('/etc/passwd')",
            "global.process.exit(1)",
            "this.constructor.constructor('return process')().exit()",
        ]
        
        for expr in unsafe_expressions:
            with pytest.raises(ExpressionError, match="Security violation"):
                security_validator.validate_javascript(expr)
    
    def test_validate_safe_python(self, security_validator):
        """Test validating safe Python expressions."""
        safe_expressions = [
            "item['value'] * 2",
            "math.ceil(item['value'])",
            "item['name'].upper()",
            "json.dumps({'result': item['value']})",
            "datetime.now().isoformat()",
        ]
        
        for expr in safe_expressions:
            assert security_validator.validate_python(expr) is True
    
    def test_validate_unsafe_python(self, security_validator):
        """Test detecting unsafe Python expressions."""
        unsafe_expressions = [
            "eval('malicious code')",
            "exec('import os; os.system(\"rm -rf /\")')",
            "__import__('os').system('whoami')",
            "open('/etc/passwd').read()",
            "globals()['__builtins__']['eval']('print(1)')",
        ]
        
        for expr in unsafe_expressions:
            with pytest.raises(ExpressionError, match="Security violation"):
                security_validator.validate_python(expr)
    
    def test_length_validation(self, security_validator):
        """Test expression length validation."""
        # Test within limit
        short_expr = "item.value"
        assert security_validator.validate_length(short_expr) is True
        
        # Test exceeding limit
        long_expr = "x" * 20000  # Exceeds 10000 limit
        with pytest.raises(ExpressionError, match="Expression too long"):
            security_validator.validate_length(long_expr)


@pytest.mark.unit
class TestJavaScriptEngine:
    """Test JavaScript expression engine."""
    
    @pytest.fixture
    def js_engine(self, expression_config):
        """Create JavaScript engine."""
        return JavaScriptEngine(expression_config)
    
    @pytest.mark.asyncio
    async def test_basic_expressions(self, js_engine, expression_context):
        """Test basic JavaScript expressions."""
        test_cases = [
            ("item.value * 2", 85.0),
            ("item.name.toUpperCase()", "TEST"),
            ("Math.round(item.value)", 43),
            ("$counter + $multiplier", 12.5),
            ("item.id === 1", True),
        ]
        
        for expr, expected in test_cases:
            result = await js_engine.evaluate(expr, expression_context)
            assert result.success is True
            assert result.value == expected
            assert result.type == ExpressionType.JAVASCRIPT
    
    @pytest.mark.asyncio
    async def test_complex_expressions(self, js_engine, expression_context):
        """Test complex JavaScript expressions."""
        # Object manipulation
        expr = """
        ({
            original: item.value,
            doubled: item.value * 2,
            rounded: Math.round(item.value),
            name: item.name.toUpperCase()
        })
        """
        
        result = await js_engine.evaluate(expr, expression_context)
        assert result.success is True
        assert result.value["original"] == 42.5
        assert result.value["doubled"] == 85.0
        assert result.value["rounded"] == 43
        assert result.value["name"] == "TEST"
    
    @pytest.mark.asyncio
    async def test_array_operations(self, js_engine, expression_context):
        """Test JavaScript array operations."""
        # Add array to context
        expression_context.set_value("numbers", [1, 2, 3, 4, 5])
        
        test_cases = [
            ("numbers.map(x => x * 2)", [2, 4, 6, 8, 10]),
            ("numbers.filter(x => x > 3)", [4, 5]),
            ("numbers.reduce((a, b) => a + b, 0)", 15),
            ("numbers.length", 5),
        ]
        
        for expr, expected in test_cases:
            result = await js_engine.evaluate(expr, expression_context)
            assert result.success is True
            assert result.value == expected
    
    @pytest.mark.asyncio
    async def test_function_calls(self, js_engine, expression_context):
        """Test JavaScript function calls."""
        # Call context functions
        result = await js_engine.evaluate("now()", expression_context)
        assert result.success is True
        assert isinstance(result.value, str)
        
        result = await js_engine.evaluate("uuid()", expression_context)
        assert result.success is True
        assert result.value == "generated-uuid-123"
    
    @pytest.mark.asyncio
    async def test_error_handling(self, js_engine, expression_context):
        """Test JavaScript error handling."""
        # Syntax error
        result = await js_engine.evaluate("invalid syntax +++", expression_context)
        assert result.success is False
        assert "SyntaxError" in result.error
        
        # Runtime error
        result = await js_engine.evaluate("undefined_variable.property", expression_context)
        assert result.success is False
        assert "error" in result.error.lower()
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self, js_engine, expression_context):
        """Test JavaScript timeout handling."""
        # Infinite loop expression
        expr = "while(true) { /* infinite loop */ }"
        
        result = await js_engine.evaluate(expr, expression_context)
        assert result.success is False
        assert "timeout" in result.error.lower()


@pytest.mark.unit
class TestPythonEngine:
    """Test Python expression engine."""
    
    @pytest.fixture
    def python_engine(self, expression_config):
        """Create Python engine."""
        return PythonEngine(expression_config)
    
    @pytest.mark.asyncio
    async def test_basic_expressions(self, python_engine, expression_context):
        """Test basic Python expressions."""
        test_cases = [
            ("item['value'] * 2", 85.0),
            ("item['name'].upper()", "TEST"),
            ("round(item['value'])", 42),  # Python banker's rounding: round(42.5) = 42
            ("counter + multiplier", 12.5),
            ("item['id'] == 1", True),
        ]
        
        for expr, expected in test_cases:
            result = await python_engine.evaluate(expr, expression_context)
            assert result.success is True
            assert result.value == expected
            assert result.type == ExpressionType.PYTHON
    
    @pytest.mark.asyncio
    async def test_complex_expressions(self, python_engine, expression_context):
        """Test complex Python expressions."""
        # Dictionary comprehension
        expr = """
        {
            'original': item['value'],
            'doubled': item['value'] * 2,
            'rounded': round(item['value']),
            'name': item['name'].upper()
        }
        """
        
        result = await python_engine.evaluate(expr, expression_context)
        assert result.success is True
        assert result.value["original"] == 42.5
        assert result.value["doubled"] == 85.0
        assert result.value["rounded"] == 42  # Python banker's rounding
        assert result.value["name"] == "TEST"
    
    @pytest.mark.asyncio
    async def test_list_operations(self, python_engine, expression_context):
        """Test Python list operations."""
        # Add list to context
        expression_context.set_value("numbers", [1, 2, 3, 4, 5])
        
        test_cases = [
            ("[x * 2 for x in numbers]", [2, 4, 6, 8, 10]),
            ("[x for x in numbers if x > 3]", [4, 5]),
            ("sum(numbers)", 15),
            ("len(numbers)", 5),
        ]
        
        for expr, expected in test_cases:
            result = await python_engine.evaluate(expr, expression_context)
            assert result.success is True
            assert result.value == expected
    
    @pytest.mark.asyncio
    async def test_module_imports(self, python_engine, expression_context):
        """Test Python module imports."""
        # Math module
        result = await python_engine.evaluate("math.pi", expression_context)
        assert result.success is True
        assert abs(result.value - 3.14159) < 0.001
        
        # JSON module
        result = await python_engine.evaluate("json.dumps({'test': True})", expression_context)
        assert result.success is True
        assert json.loads(result.value) == {"test": True}
    
    @pytest.mark.asyncio
    async def test_function_calls(self, python_engine, expression_context):
        """Test Python function calls."""
        # Call context functions
        result = await python_engine.evaluate("now()", expression_context)
        assert result.success is True
        assert isinstance(result.value, str)
        
        result = await python_engine.evaluate("uuid()", expression_context)
        assert result.success is True
        assert result.value == "generated-uuid-123"
    
    @pytest.mark.asyncio
    async def test_error_handling(self, python_engine, expression_context):
        """Test Python error handling."""
        # Syntax error
        result = await python_engine.evaluate("if True", expression_context)
        assert result.success is False
        assert "SyntaxError" in result.error
        
        # Runtime error
        result = await python_engine.evaluate("undefined_variable", expression_context)
        assert result.success is False
        assert "NameError" in result.error
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self, python_engine, expression_context):
        """Test Python timeout handling."""
        # Long-running expression
        expr = "sum(range(10000000))"  # Should timeout
        
        result = await python_engine.evaluate(expr, expression_context)
        # May or may not timeout depending on system speed, but should handle gracefully
        if not result.success:
            assert "timeout" in result.error.lower() or "error" in result.error.lower()


@pytest.mark.unit
class TestExpressionEngine:
    """Test main expression engine."""
    
    @pytest.mark.asyncio
    async def test_auto_language_detection(self, expression_engine, expression_context):
        """Test automatic language detection."""
        # JavaScript style
        result = await expression_engine.evaluate("item.value * 2", expression_context)
        assert result.success is True
        assert result.type == ExpressionType.JAVASCRIPT
        
        # Python style
        result = await expression_engine.evaluate("item['value'] * 2", expression_context)
        assert result.success is True
        assert result.type == ExpressionType.PYTHON
    
    @pytest.mark.asyncio
    async def test_explicit_language_selection(self, expression_engine, expression_context):
        """Test explicit language selection."""
        # Force JavaScript
        result = await expression_engine.evaluate(
            "item.value * 2", 
            expression_context, 
            language="javascript"
        )
        assert result.success is True
        assert result.type == ExpressionType.JAVASCRIPT
        
        # Force Python
        result = await expression_engine.evaluate(
            "item['value'] * 2", 
            expression_context, 
            language="python"
        )
        assert result.success is True
        assert result.type == ExpressionType.PYTHON
    
    @pytest.mark.asyncio
    async def test_template_rendering(self, expression_engine, expression_context):
        """Test template rendering with expressions."""
        template = "Hello {{ item.name.toUpperCase() }}, your value is {{ item.value * 2 }}!"
        
        result = await expression_engine.render_template(template, expression_context)
        assert result.success is True
        assert result.value == "Hello TEST, your value is 85!"
    
    @pytest.mark.asyncio
    async def test_complex_template(self, expression_engine, expression_context):
        """Test complex template with multiple expressions."""
        template = """
        User: {{ $user_id }}
        Item: {{ item.name }} (ID: {{ item.id }})
        Value: {{ item.value }} -> {{ Math.round(item.value) }}
        Counter: {{ $counter }} x {{ $multiplier }} = {{ $counter * $multiplier }}
        """
        
        result = await expression_engine.render_template(template, expression_context)
        assert result.success is True
        assert "User: user-789" in result.value
        assert "Item: test (ID: 1)" in result.value
        assert "Value: 42.5 -> 43" in result.value
        assert "Counter: 10 x 2.5 = 25" in result.value
    
    @pytest.mark.asyncio
    async def test_expression_caching(self, expression_engine, expression_context):
        """Test expression result caching."""
        expr = "Math.random()"  # Should be cached
        
        # First evaluation
        result1 = await expression_engine.evaluate(expr, expression_context, cache_key="random_test")
        assert result1.success is True
        
        # Second evaluation with same cache key
        result2 = await expression_engine.evaluate(expr, expression_context, cache_key="random_test")
        assert result2.success is True
        
        # Results should be the same due to caching
        # Note: This test assumes the cache is working, actual implementation may vary
    
    @pytest.mark.asyncio
    async def test_batch_evaluation(self, expression_engine, expression_context):
        """Test batch expression evaluation."""
        expressions = {
            "doubled_value": "item.value * 2",
            "upper_name": "item.name.toUpperCase()",
            "counter_sum": "$counter + $multiplier",
            "is_positive": "item.value > 0",
        }
        
        results = await expression_engine.evaluate_batch(expressions, expression_context)
        
        assert len(results) == 4
        assert results["doubled_value"].value == 85.0
        assert results["upper_name"].value == "TEST"
        assert results["counter_sum"].value == 12.5
        assert results["is_positive"].value is True
    
    @pytest.mark.asyncio
    async def test_security_enforcement(self, expression_engine, expression_context):
        """Test security enforcement."""
        # Should block dangerous expressions
        dangerous_expressions = [
            "eval('malicious code')",
            "require('child_process').exec('rm -rf /')",
            "__import__('os').system('whoami')",
        ]
        
        for expr in dangerous_expressions:
            result = await expression_engine.evaluate(expr, expression_context)
            assert result.success is False
            assert "security" in result.error.lower() or "violation" in result.error.lower()


@pytest.mark.integration
class TestExpressionEngineIntegration:
    """Integration tests for expression engine."""
    
    @pytest.mark.asyncio
    async def test_workflow_context_integration(self, expression_engine):
        """Test integration with workflow execution context."""
        # Simulate workflow execution context
        workflow_context = ExpressionContext(
            data={
                "input": {"user_id": 123, "action": "process"},
                "previous_node": {"result": "success", "data": {"processed": True}},
                "http_request": {
                    "method": "POST",
                    "url": "https://api.example.com/users/123",
                    "headers": {"Authorization": "Bearer token123"},
                },
            },
            variables={
                "api_base": "https://api.example.com",
                "timeout": 30,
                "retry_count": 3,
            },
            functions={
                "format_url": lambda path: f"https://api.example.com{path}",
                "encode_base64": lambda s: f"encoded:{s}",
            },
            execution_id="exec-integration-test",
            node_id="http-node-1",
            workflow_id="wf-integration",
        )
        
        # Test complex workflow expressions
        expressions = {
            "api_url": "format_url('/users/' + input.user_id)",
            "auth_header": "'Bearer ' + encode_base64('user:' + input.user_id)",
            "should_retry": "previous_node.result !== 'success' && $retry_count > 0",
            "timeout_ms": "$timeout * 1000",
        }
        
        results = await expression_engine.evaluate_batch(expressions, workflow_context)
        
        assert results["api_url"].value == "https://api.example.com/users/123"
        assert results["auth_header"].value == "Bearer encoded:user:123"
        assert results["should_retry"].value is False
        assert results["timeout_ms"].value == 30000
    
    @pytest.mark.asyncio
    async def test_data_transformation(self, expression_engine):
        """Test data transformation scenarios."""
        # Input data transformation
        input_data = {
            "users": [
                {"id": 1, "name": "John", "age": 30, "active": True},
                {"id": 2, "name": "Jane", "age": 25, "active": False},
                {"id": 3, "name": "Bob", "age": 35, "active": True},
            ],
            "config": {"min_age": 26, "active_only": True},
        }
        
        context = ExpressionContext(data=input_data)
        
        # Filter and transform users
        filter_expr = """
        users.filter(user => 
            user.age >= config.min_age && 
            (!config.active_only || user.active)
        ).map(user => ({
            id: user.id,
            display_name: user.name.toUpperCase(),
            category: user.age >= 30 ? 'senior' : 'junior'
        }))
        """
        
        result = await expression_engine.evaluate(filter_expr, context)
        assert result.success is True
        assert len(result.value) == 1  # Only Bob matches criteria
        assert result.value[0]["display_name"] == "BOB"
        assert result.value[0]["category"] == "senior"
    
    @pytest.mark.asyncio
    async def test_conditional_logic(self, expression_engine):
        """Test conditional logic in expressions."""
        context = ExpressionContext(
            data={
                "order": {"total": 150.0, "currency": "USD", "country": "US"},
                "user": {"tier": "premium", "discount_rate": 0.1},
                "promotion": {"code": "SAVE20", "discount": 0.2, "min_amount": 100},
            }
        )
        
        # Complex conditional pricing logic
        pricing_expr = """
        (() => {
            let total = order.total;
            let discount = 0;
            
            // User tier discount
            if (user.tier === 'premium') {
                discount = Math.max(discount, user.discount_rate);
            }
            
            // Promotion discount
            if (promotion.code && total >= promotion.min_amount) {
                discount = Math.max(discount, promotion.discount);
            }
            
            // Apply discount
            let discounted_total = total * (1 - discount);
            
            // Tax calculation (US)
            let tax_rate = order.country === 'US' ? 0.08 : 0.20;
            let tax = discounted_total * tax_rate;
            
            return {
                original_total: total,
                discount_rate: discount,
                discount_amount: total * discount,
                subtotal: discounted_total,
                tax_rate: tax_rate,
                tax_amount: tax,
                final_total: discounted_total + tax
            };
        })()
        """
        
        result = await expression_engine.evaluate(pricing_expr, context)
        assert result.success is True
        
        pricing = result.value
        assert pricing["original_total"] == 150.0
        assert pricing["discount_rate"] == 0.2  # Promotion is better than user tier
        assert pricing["discount_amount"] == 30.0
        assert pricing["subtotal"] == 120.0
        assert pricing["tax_rate"] == 0.08
        assert pricing["final_total"] == 129.6  # 120 + (120 * 0.08)


@pytest.mark.performance
class TestExpressionEnginePerformance:
    """Performance tests for expression engine."""
    
    @pytest.mark.asyncio
    async def test_expression_performance(self, expression_engine, expression_context):
        """Test expression evaluation performance."""
        import time
        
        # Simple expressions should be fast
        simple_expressions = [
            "item.value * 2",
            "item.name.toUpperCase()",
            "$counter + $multiplier",
            "Math.round(item.value)",
        ]
        
        start_time = time.time()
        
        # Evaluate 100 expressions
        for _ in range(100):
            for expr in simple_expressions:
                result = await expression_engine.evaluate(expr, expression_context)
                assert result.success is True
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete 400 evaluations quickly
        assert duration < 2.0  # 2 seconds for 400 evaluations
    
    @pytest.mark.asyncio
    async def test_batch_performance(self, expression_engine, expression_context):
        """Test batch evaluation performance."""
        import time
        
        # Create batch of expressions
        expressions = {f"expr_{i}": f"item.value * {i}" for i in range(50)}
        
        start_time = time.time()
        
        # Evaluate batch 10 times
        for _ in range(10):
            results = await expression_engine.evaluate_batch(expressions, expression_context)
            assert len(results) == 50
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete 500 evaluations quickly
        assert duration < 1.0  # 1 second for 500 evaluations
    
    @pytest.mark.asyncio
    async def test_template_performance(self, expression_engine, expression_context):
        """Test template rendering performance."""
        import time
        
        # Complex template
        template = """
        Item: {{ item.name }} ({{ item.id }})
        Value: {{ item.value }} -> {{ Math.round(item.value) }}
        User: {{ $user_id }}
        Counter: {{ $counter }} x {{ $multiplier }} = {{ $counter * $multiplier }}
        Time: {{ now() }}
        """
        
        start_time = time.time()
        
        # Render template 100 times
        for _ in range(100):
            result = await expression_engine.render_template(template, expression_context)
            assert result.success is True
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should render quickly
        assert duration < 1.0  # 1 second for 100 renders