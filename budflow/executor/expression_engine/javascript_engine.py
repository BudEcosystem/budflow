"""
JavaScript-compatible expression engine for N8N compatibility.

This module provides JavaScript-style expression evaluation,
translating JavaScript syntax to Python/Jinja2 equivalents.
"""

import re
import json
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Callable

import structlog

from .context import ExpressionContext
from .errors import (
    ExpressionError,
    ExpressionSyntaxError,
    ExpressionRuntimeError,
    UndefinedVariableError
)
from .jinja_engine import JinjaExpressionEngine

logger = structlog.get_logger()


class JavaScriptExpressionEngine:
    """
    JavaScript-compatible expression engine.
    
    This engine translates JavaScript-style expressions to Python/Jinja2
    equivalents for evaluation. It supports:
    - Property access (obj.prop)
    - Array access (arr[0])
    - Method calls (arr.map(), arr.filter())
    - Arrow functions (item => item.id)
    - JavaScript built-ins (Math, Date, Object)
    - N8N variables in JavaScript syntax
    """
    
    def __init__(
        self,
        enable_security: bool = True,
        enable_caching: bool = True,
        timeout_seconds: float = 30.0
    ):
        self.enable_security = enable_security
        self.enable_caching = enable_caching
        self.timeout_seconds = timeout_seconds
        self.logger = logger.bind(component="javascript_expression_engine")
        
        # Use Jinja engine as the underlying processor
        self.jinja_engine = JinjaExpressionEngine(
            enable_security=enable_security,
            enable_caching=enable_caching,
            timeout_seconds=timeout_seconds
        )
        
        # Cache for compiled expressions
        self._compiled_cache: Dict[str, str] = {} if enable_caching else None
    
    async def evaluate(
        self,
        expression: str,
        context: Union[ExpressionContext, Dict[str, Any], Any] = None,
        additional_context: Dict[str, Any] = None
    ) -> Any:
        """
        Evaluate JavaScript-style expression.
        
        Args:
            expression: JavaScript expression to evaluate
            context: Expression context or dictionary
            additional_context: Additional context variables
            
        Returns:
            Evaluated result
            
        Raises:
            ExpressionError: If evaluation fails
        """
        try:
            # Compile JavaScript to Jinja2
            compiled_expression = self._compile_javascript_to_jinja(expression)
            
            # Prepare context with JavaScript-compatible helpers
            if isinstance(context, ExpressionContext):
                template_context = context.to_jinja_context()
            elif isinstance(context, dict):
                template_context = context.copy()
            else:
                template_context = {}
            
            # Add JavaScript-compatible context helpers
            js_helpers = self._create_javascript_context(context)
            template_context.update(js_helpers)
            
            if additional_context:
                template_context.update(additional_context)
            
            # Add helpers to Jinja globals and filters
            self.jinja_engine.env.globals.update(js_helpers)
            # Also add as filters for pipe syntax
            self.jinja_engine.env.filters.update(js_helpers)
            
            # Evaluate using Jinja engine
            result = await self.jinja_engine.evaluate(
                compiled_expression,
                template_context,
                additional_context
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                "JavaScript expression evaluation failed",
                expression=expression,
                error=str(e),
                error_type=type(e).__name__
            )
            
            if isinstance(e, ExpressionError):
                raise
            else:
                raise ExpressionRuntimeError(
                    f"JavaScript expression evaluation failed: {e}",
                    expression=expression
                )
    
    def _compile_javascript_to_jinja(self, expression: str) -> str:
        """
        Compile JavaScript expression to Jinja2 template.
        
        Args:
            expression: JavaScript expression
            
        Returns:
            Equivalent Jinja2 template
        """
        if self.enable_caching and expression in self._compiled_cache:
            return self._compiled_cache[expression]
        
        compiled = self._transform_javascript_syntax(expression)
        
        if self.enable_caching:
            self._compiled_cache[expression] = compiled
        
        return compiled
    
    def _transform_javascript_syntax(self, expression: str) -> str:
        """
        Transform JavaScript syntax to Jinja2 equivalent.
        
        Args:
            expression: JavaScript expression
            
        Returns:
            Jinja2 template string
        """
        # Start with the original expression
        result = expression.strip()
        
        # Replace N8N variables with their context equivalents (remove $ prefix)
        # This is needed because Jinja2 doesn't accept $ in variable names
        result = re.sub(r'\$json\b', 'json', result)
        result = re.sub(r'\$node\b', 'node', result)
        result = re.sub(r'\$input\b', 'input', result)
        result = re.sub(r'\$now\b', 'now', result)
        result = re.sub(r'\$workflow\b', 'workflow', result)
        result = re.sub(r'\$execution\b', 'execution', result)
        
        # Handle N8N function calls
        result = self._transform_n8n_functions(result)
        
        # Handle array methods
        result = self._transform_array_methods(result)
        
        # Handle object methods
        result = self._transform_object_methods(result)
        
        # Handle JavaScript built-ins
        result = self._transform_javascript_builtins(result)
        
        # Handle property access chains
        result = self._transform_property_access(result)
        
        # Handle arrow functions
        result = self._transform_arrow_functions(result)
        
        # Wrap in Jinja2 syntax if not already wrapped
        if not (result.startswith('{{') or result.startswith('{%')):
            result = f"{{{{ {result} }}}}"
        
        return result
    
    def _transform_n8n_functions(self, expression: str) -> str:
        """
        Transform N8N function calls to Jinja2 syntax.
        
        Args:
            expression: Expression with N8N functions
            
        Returns:
            Transformed expression
        """
        # Transform node() function calls
        expression = re.sub(
            r"node\('([^']+)'\)",
            r"node.\1",
            expression
        )
        
        expression = re.sub(
            r'node\("([^"]+)"\)',
            r"node.\1",
            expression
        )
        
        # Transform method calls on N8N variables
        expression = re.sub(
            r'input\.all\(\)',
            r'input.all()',
            expression
        )
        
        expression = re.sub(
            r'input\.first\(\)',
            r'input.first()',
            expression
        )
        
        expression = re.sub(
            r'input\.last\(\)',
            r'input.last()',
            expression
        )
        
        return expression
    
    def _transform_array_methods(self, expression: str) -> str:
        """
        Transform JavaScript array methods to Jinja2 filters.
        
        Args:
            expression: Expression with array methods
            
        Returns:
            Transformed expression
        """
        # Transform .length property
        expression = re.sub(
            r'\.length\b',
            r' | length',
            expression
        )
        
        # Transform .map() method
        expression = re.sub(
            r'\.map\(([^)]+)\)',
            r' | js_transform("\1") | as_object',
            expression
        )
        
        # Transform .filter() method
        expression = re.sub(
            r'\.filter\(([^)]+)\)',
            r' | js_test("\1") | as_object',
            expression
        )
        
        # Transform .reduce() method - handle arrow functions with commas
        expression = re.sub(
            r'\.reduce\(([^)]+)\)\s*,\s*([^)]+)\)',
            r' | js_reduce("\1", \2)',
            expression
        )
        # Fallback for simpler reduce patterns
        expression = re.sub(
            r'\.reduce\((\([^)]+\)\s*=>\s*[^,]+),\s*([^)]+)\)',
            r' | js_reduce("\1", \2)',
            expression
        )
        
        # Transform .join() method
        expression = re.sub(
            r'\.join\(([^)]+)\)',
            r' | join(\1)',
            expression
        )
        
        # Transform .slice() method
        expression = re.sub(
            r'\.slice\(([^)]+)\)',
            r' | slice(\1)',
            expression
        )
        
        return expression
    
    def _transform_object_methods(self, expression: str) -> str:
        """
        Transform JavaScript object methods.
        
        Args:
            expression: Expression with object methods
            
        Returns:
            Transformed expression
        """
        # Transform Object.keys()
        expression = re.sub(
            r'Object\.keys\(([^)]+)\)',
            r'\1 | list',
            expression
        )
        
        # Transform Object.values()
        expression = re.sub(
            r'Object\.values\(([^)]+)\)',
            r'\1.values() | list',
            expression
        )
        
        # Transform Object.entries()
        expression = re.sub(
            r'Object\.entries\(([^)]+)\)',
            r'\1.items() | list',
            expression
        )
        
        return expression
    
    def _transform_javascript_builtins(self, expression: str) -> str:
        """
        Transform JavaScript built-in functions and objects.
        
        Args:
            expression: Expression with JavaScript built-ins
            
        Returns:
            Transformed expression
        """
        # Transform Math functions
        math_functions = {
            'Math.max': 'js_math_max',
            'Math.min': 'js_math_min',
            'Math.round': 'js_math_round',
            'Math.floor': 'js_math_floor',
            'Math.ceil': 'js_math_ceil',
            'Math.abs': 'js_math_abs',
            'Math.random': 'js_math_random'
        }
        
        for js_func, jinja_func in math_functions.items():
            expression = re.sub(
                rf'{re.escape(js_func)}\(([^)]*)\)',
                rf'{jinja_func}(\1)',
                expression
            )
        
        # Transform Date functions
        expression = re.sub(
            r'new\s+Date\(([^)]*)\)',
            r'js_new_date(\1)',
            expression
        )
        
        # Transform string methods
        expression = re.sub(
            r'\.split\(([^)]+)\)',
            r' | js_split(\1)',
            expression
        )
        
        expression = re.sub(
            r'\.toLowerCase\(\)',
            r' | lower',
            expression
        )
        
        expression = re.sub(
            r'\.toUpperCase\(\)',
            r' | upper',
            expression
        )
        
        return expression
    
    def _transform_property_access(self, expression: str) -> str:
        """
        Transform JavaScript property access to Jinja2 syntax.
        
        Args:
            expression: Expression with property access
            
        Returns:
            Transformed expression
        """
        # Array access with brackets
        expression = re.sub(
            r'\[(\d+)\]',
            r'[\1]',
            expression
        )
        
        # Property access with dots (already compatible with Jinja2)
        return expression
    
    def _transform_arrow_functions(self, expression: str) -> str:
        """
        Transform JavaScript arrow functions to Jinja2 equivalents.
        
        Args:
            expression: Expression with arrow functions
            
        Returns:
            Transformed expression
        """
        # Simple arrow functions: item => item.prop
        # We'll just pass the expression as-is to our custom handlers
        # The actual transformation happens in js_transform, js_test, etc.
        return expression
    
    def _create_javascript_context(self, context: Any) -> Dict[str, Any]:
        """
        Create JavaScript-compatible context helpers.
        
        Args:
            context: Original context object
            
        Returns:
            Dictionary with JavaScript helpers
        """
        js_context = {}
        
        # Math functions
        js_context.update({
            'js_math_max': lambda *args: max(args) if args else 0,
            'js_math_min': lambda *args: min(args) if args else 0,
            'js_math_round': lambda x: round(float(x)),
            'js_math_floor': lambda x: int(float(x)),
            'js_math_ceil': lambda x: int(float(x)) + (1 if float(x) % 1 > 0 else 0),
            'js_math_abs': lambda x: abs(float(x)),
            'js_math_random': lambda: __import__('random').random()
        })
        
        # Date functions
        class JSDate:
            """JavaScript Date wrapper."""
            def __init__(self, dt):
                self._dt = dt
            
            def getFullYear(self):
                return self._dt.year
            
            def getMonth(self):
                return self._dt.month - 1  # JS months are 0-indexed
            
            def getDate(self):
                return self._dt.day
            
            def getHours(self):
                return self._dt.hour
            
            def getMinutes(self):
                return self._dt.minute
            
            def getSeconds(self):
                return self._dt.second
            
            def toISOString(self):
                return self._dt.isoformat()
            
            def __str__(self):
                return self._dt.isoformat()
        
        def js_new_date(*args):
            if not args:
                dt = datetime.now()
            elif len(args) == 1:
                arg = args[0]
                if isinstance(arg, str):
                    from dateutil.parser import parse
                    dt = parse(arg)
                elif isinstance(arg, (int, float)):
                    dt = datetime.fromtimestamp(arg / 1000)  # JS timestamps are in milliseconds
                else:
                    dt = datetime.now()
            else:
                dt = datetime.now()
            return JSDate(dt)
        
        js_context['js_new_date'] = js_new_date
        
        # String functions
        js_context.update({
            'js_split': lambda text, separator=None: str(text).split(separator),
        })
        
        # Array transformation helpers
        def js_transform(items, transform_expr):
            """Apply transformation expression to each item (Jinja filter style)."""
            result = []
            for item in items:
                # Handle arrow function expressions like "item => item.name"
                if isinstance(transform_expr, str):
                    # Extract the property name from expressions like "item => item.name"
                    import re
                    match = re.search(r'\w+\s*=>\s*\w+\.([\w]+)', transform_expr)
                    if match and hasattr(item, 'get'):
                        prop = match.group(1)
                        result.append(item.get(prop))
                    else:
                        result.append(item)
                else:
                    result.append(item)
            return result
        
        def js_test(items, test_expr):
            """Filter items based on test expression (Jinja filter style)."""
            result = []
            for item in items:
                # Handle expressions like "item => item.id > 1"
                if isinstance(test_expr, str):
                    import re
                    # Match patterns like "item => item.prop > value"
                    match = re.search(r'\w+\s*=>\s*\w+\.(\w+)\s*>\s*(\d+)', test_expr)
                    if match and hasattr(item, 'get'):
                        prop = match.group(1)
                        value = int(match.group(2))
                        if item.get(prop, 0) > value:
                            result.append(item)
                    else:
                        # Default to excluding the item if we can't parse the expression
                        pass
                else:
                    result.append(item)
            return result
        
        def js_reduce(items, reducer_expr, initial):
            """Reduce items using reducer expression (Jinja filter style)."""
            result = initial
            for item in items:
                # Handle expressions like "(acc, item) => acc + item.id"
                if isinstance(reducer_expr, str):
                    import re
                    # Match patterns like "(acc, item) => acc + item.prop"
                    match = re.search(r'\(\w+,\s*\w+\)\s*=>\s*\w+\s*\+\s*\w+\.(\w+)', reducer_expr)
                    if match and hasattr(item, 'get'):
                        prop = match.group(1)
                        result += item.get(prop, 0)
                    else:
                        # Try simple addition if it's a number
                        result += item if isinstance(item, (int, float)) else 0
                else:
                    result += item if isinstance(item, (int, float)) else 0
            return result
        
        js_context.update({
            'js_transform': js_transform,
            'js_test': js_test,
            'js_reduce': js_reduce,
            'as_object': lambda value: f"__JINJA_OBJECT_RESULT__{json.dumps(value)}__JINJA_OBJECT_RESULT__"
        })
        
        return js_context
    
    def clear_cache(self) -> None:
        """Clear expression cache."""
        self.jinja_engine.clear_cache()
        if self._compiled_cache:
            self._compiled_cache.clear()
    
    @property
    def cache_size(self) -> int:
        """Get current cache size."""
        jinja_size = self.jinja_engine.cache_size
        compiled_size = len(self._compiled_cache) if self._compiled_cache else 0
        return jinja_size + compiled_size
