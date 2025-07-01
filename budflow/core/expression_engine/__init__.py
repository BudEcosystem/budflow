"""Expression Engine with JavaScript and Python support."""

import asyncio
import ast
import json
import re
import time
import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
import subprocess
import tempfile
from pathlib import Path

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger()


class ExpressionType(str, Enum):
    """Expression type enumeration."""
    JAVASCRIPT = "javascript"
    PYTHON = "python"
    TEMPLATE = "template"


class SecurityLevel(str, Enum):
    """Security level enumeration."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    STRICT = "strict"


class ExpressionConfig(BaseModel):
    """Configuration for expression engine."""
    default_language: str = "javascript"
    timeout_seconds: int = 10
    memory_limit_mb: int = 128
    enable_python: bool = True
    enable_javascript: bool = True
    security_level: SecurityLevel = SecurityLevel.MEDIUM
    allowed_modules: List[str] = Field(default_factory=lambda: ["math", "json", "datetime"])
    blocked_functions: List[str] = Field(default_factory=lambda: ["eval", "exec", "__import__"])
    max_expression_length: int = 50000
    cache_enabled: bool = True
    cache_ttl_seconds: int = 300


class ExpressionError(Exception):
    """Expression evaluation error."""
    pass


@dataclass
class ExpressionResult:
    """Result of expression evaluation."""
    success: bool
    value: Any = None
    error: Optional[str] = None
    type: ExpressionType = ExpressionType.JAVASCRIPT
    execution_time_ms: int = 0
    cached: bool = False


class ExpressionContext:
    """Context for expression evaluation."""
    
    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        functions: Optional[Dict[str, Callable]] = None,
        execution_id: Optional[str] = None,
        node_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ):
        self.data = data or {}
        self.variables = variables or {}
        self.functions = functions or {}
        self.execution_id = execution_id
        self.node_id = node_id
        self.workflow_id = workflow_id
    
    def get_value(self, path: str, default: Any = None) -> Any:
        """Get value from context using dot notation."""
        try:
            if path.startswith("$"):
                # Variable or environment access
                if path.startswith("$env."):
                    env_key = path[5:]  # Remove "$env."
                    return self.data.get("env", {}).get(env_key, default)
                else:
                    var_key = path[1:]  # Remove "$"
                    return self.variables.get(var_key, default)
            else:
                # Data access with dot notation
                current = self.data
                for part in path.split("."):
                    if isinstance(current, dict):
                        current = current.get(part)
                    elif isinstance(current, list) and part.isdigit():
                        idx = int(part)
                        if 0 <= idx < len(current):
                            current = current[idx]
                        else:
                            current = None
                    else:
                        current = None
                    
                    if current is None:
                        return default
                
                return current
        except Exception:
            return default
    
    def set_value(self, path: str, value: Any):
        """Set value in context."""
        if path.startswith("$"):
            # Set variable
            var_key = path[1:]
            self.variables[var_key] = value
        else:
            # Set data
            parts = path.split(".")
            current = self.data
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
    
    def call_function(self, name: str, *args, **kwargs) -> Any:
        """Call a context function."""
        if name not in self.functions:
            raise ExpressionError(f"Function '{name}' not found")
        
        return self.functions[name](*args, **kwargs)
    
    def to_js_context(self) -> Dict[str, Any]:
        """Convert to JavaScript context."""
        js_context = {}
        
        # Add data
        js_context.update(self.data)
        
        # Add variables with $ prefix removed
        for key, value in self.variables.items():
            js_context[key] = value
        
        # Add functions
        for key, func in self.functions.items():
            js_context[key] = func
        
        return js_context
    
    def to_python_context(self) -> Dict[str, Any]:
        """Convert to Python context."""
        python_context = {}
        
        # Add data
        python_context.update(self.data)
        
        # Add variables
        for key, value in self.variables.items():
            python_context[key] = value
        
        # Add functions
        for key, func in self.functions.items():
            python_context[key] = func
        
        # Add safe built-ins
        python_context.update({
            "abs": abs,
            "all": all,
            "any": any,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "filter": filter,
            "float": float,
            "int": int,
            "len": len,
            "list": list,
            "map": map,
            "max": max,
            "min": min,
            "range": range,
            "round": round,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
            "zip": zip,
        })
        
        return python_context


class SecurityValidator:
    """Validates expressions for security issues."""
    
    def __init__(self, config: ExpressionConfig):
        self.config = config
        
        # Define security patterns
        self.js_dangerous_patterns = [
            r'\beval\s*\(',
            r'\bFunction\s*\(',
            r'\brequire\s*\(',
            r'\bprocess\s*\.',
            r'\bglobal\s*\.',
            r'\bthis\s*\.\s*constructor',
            r'__proto__',
            r'prototype\s*\.\s*constructor',
        ]
        
        self.python_dangerous_patterns = [
            r'\beval\s*\(',
            r'\bexec\s*\(',
            r'\b__import__\s*\(',
            r'\bopen\s*\(',
            r'\bfile\s*\(',
            r'\binput\s*\(',
            r'\bglobals\s*\(',
            r'\blocals\s*\(',
            r'\bvars\s*\(',
            r'\bdir\s*\(',
            r'__.*__',  # Dunder methods
        ]
    
    def validate_length(self, expression: str) -> bool:
        """Validate expression length."""
        if len(expression) > self.config.max_expression_length:
            raise ExpressionError(
                f"Expression too long: {len(expression)} > {self.config.max_expression_length}"
            )
        return True
    
    def validate_javascript(self, expression: str) -> bool:
        """Validate JavaScript expression for security."""
        self.validate_length(expression)
        
        if self.config.security_level in [SecurityLevel.MEDIUM, SecurityLevel.HIGH, SecurityLevel.STRICT]:
            for pattern in self.js_dangerous_patterns:
                if re.search(pattern, expression, re.IGNORECASE):
                    raise ExpressionError(f"Security violation: dangerous pattern detected")
        
        return True
    
    def validate_python(self, expression: str) -> bool:
        """Validate Python expression for security."""
        self.validate_length(expression)
        
        if self.config.security_level in [SecurityLevel.MEDIUM, SecurityLevel.HIGH, SecurityLevel.STRICT]:
            for pattern in self.python_dangerous_patterns:
                if re.search(pattern, expression, re.IGNORECASE):
                    raise ExpressionError(f"Security violation: dangerous pattern detected")
        
        # Check for blocked functions
        for blocked in self.config.blocked_functions:
            if blocked in expression:
                raise ExpressionError(f"Security violation: blocked function '{blocked}'")
        
        return True


class JavaScriptEngine:
    """JavaScript expression engine using Node.js."""
    
    def __init__(self, config: ExpressionConfig):
        self.config = config
        self.validator = SecurityValidator(config)
    
    async def evaluate(self, expression: str, context: ExpressionContext) -> ExpressionResult:
        """Evaluate JavaScript expression."""
        start_time = time.time()
        
        try:
            # Validate expression
            self.validator.validate_javascript(expression)
            
            # Prepare context
            js_context = context.to_js_context()
            
            # Create JavaScript code
            js_code = self._wrap_expression(expression, js_context)
            
            # Execute in Node.js
            result = await self._execute_nodejs(js_code)
            
            execution_time = int((time.time() - start_time) * 1000)
            
            return ExpressionResult(
                success=True,
                value=result,
                type=ExpressionType.JAVASCRIPT,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            return ExpressionResult(
                success=False,
                error=str(e),
                type=ExpressionType.JAVASCRIPT,
                execution_time_ms=execution_time
            )
    
    def _wrap_expression(self, expression: str, context: Dict[str, Any]) -> str:
        """Wrap expression in JavaScript code."""
        # Serialize context safely
        context_json = json.dumps(context, default=str)
        
        return f"""
const context = {context_json};

// Make context variables available globally
for (const [key, value] of Object.entries(context)) {{
    if (typeof value === 'function') {{
        global[key] = value;
    }} else {{
        global[key] = value;
    }}
}}

// Execute expression
try {{
    const result = ({expression});
    console.log(JSON.stringify({{success: true, result: result}}));
}} catch (error) {{
    console.log(JSON.stringify({{success: false, error: error.message}}));
}}
"""
    
    async def _execute_nodejs(self, js_code: str) -> Any:
        """Execute JavaScript code in Node.js."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
            f.write(js_code)
            js_file = f.name
        
        try:
            # Execute with timeout
            process = await asyncio.create_subprocess_exec(
                'node',
                js_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.config.timeout_seconds
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise ExpressionError("Expression evaluation timed out")
            
            # Parse result
            output = stdout.decode().strip()
            if output:
                result_data = json.loads(output)
                if result_data.get("success"):
                    return result_data.get("result")
                else:
                    raise ExpressionError(result_data.get("error", "Unknown error"))
            else:
                error_output = stderr.decode().strip()
                raise ExpressionError(f"JavaScript execution error: {error_output}")
                
        finally:
            # Clean up temp file
            try:
                Path(js_file).unlink()
            except:
                pass


class PythonEngine:
    """Python expression engine with restricted execution."""
    
    def __init__(self, config: ExpressionConfig):
        self.config = config
        self.validator = SecurityValidator(config)
    
    async def evaluate(self, expression: str, context: ExpressionContext) -> ExpressionResult:
        """Evaluate Python expression."""
        start_time = time.time()
        
        try:
            # Validate expression
            self.validator.validate_python(expression)
            
            # Prepare context
            python_context = context.to_python_context()
            
            # Add allowed modules
            for module_name in self.config.allowed_modules:
                try:
                    module = __import__(module_name)
                    python_context[module_name] = module
                except ImportError:
                    pass
            
            # Create restricted globals
            restricted_globals = {
                "__builtins__": {
                    "__import__": self._restricted_import,
                },
            }
            restricted_globals.update(python_context)
            
            # Execute expression
            result = await self._execute_python(expression, restricted_globals)
            
            execution_time = int((time.time() - start_time) * 1000)
            
            return ExpressionResult(
                success=True,
                value=result,
                type=ExpressionType.PYTHON,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            return ExpressionResult(
                success=False,
                error=f"{type(e).__name__}: {str(e)}",
                type=ExpressionType.PYTHON,
                execution_time_ms=execution_time
            )
    
    def _restricted_import(self, name, *args, **kwargs):
        """Restricted import function."""
        if name not in self.config.allowed_modules:
            raise ImportError(f"Import of '{name}' is not allowed")
        return __import__(name, *args, **kwargs)
    
    async def _execute_python(self, expression: str, globals_dict: Dict[str, Any]) -> Any:
        """Execute Python expression with timeout."""
        # Use asyncio to run with timeout
        try:
            # Clean up the expression
            cleaned_expr = expression.strip()
            
            # For simple expressions, use eval
            # For complex expressions, might need to compile and exec
            needs_exec = any(keyword in cleaned_expr for keyword in ['def ', 'class ', 'for ', 'while ', 'if ', 'try:', 'with ', 'import '])
            
            if needs_exec:
                # Complex statement - use exec
                # Try to dedent the expression in case it has leading whitespace
                import textwrap
                dedented_expr = textwrap.dedent(cleaned_expr)
                
                local_vars = {}
                exec(dedented_expr, globals_dict, local_vars)
                # Return the last assigned variable or None
                return local_vars.get('result', None)
            else:
                # Expression that can be evaluated - use eval (handles multi-line dict/list literals)
                import textwrap
                dedented_expr = textwrap.dedent(cleaned_expr)
                return eval(dedented_expr, globals_dict)
                
        except Exception as e:
            raise e


class ExpressionEngine:
    """Main expression engine that coordinates JavaScript and Python engines."""
    
    def __init__(self, config: ExpressionConfig):
        self.config = config
        self.js_engine = JavaScriptEngine(config) if config.enable_javascript else None
        self.python_engine = PythonEngine(config) if config.enable_python else None
        self.cache: Dict[str, ExpressionResult] = {}
        self.cache_timestamps: Dict[str, float] = {}
    
    async def evaluate(
        self,
        expression: str,
        context: ExpressionContext,
        language: Optional[str] = None,
        cache_key: Optional[str] = None,
    ) -> ExpressionResult:
        """Evaluate expression in specified or auto-detected language."""
        
        # Check cache first
        if cache_key and self.config.cache_enabled:
            cached_result = self._get_cached_result(cache_key)
            if cached_result:
                cached_result.cached = True
                return cached_result
        
        # Determine language
        if language is None:
            language = self._detect_language(expression)
        
        # Evaluate expression
        if language == "javascript" and self.js_engine:
            result = await self.js_engine.evaluate(expression, context)
        elif language == "python" and self.python_engine:
            result = await self.python_engine.evaluate(expression, context)
        else:
            return ExpressionResult(
                success=False,
                error=f"Language '{language}' not supported or not enabled"
            )
        
        # Cache result
        if cache_key and self.config.cache_enabled and result.success:
            self._cache_result(cache_key, result)
        
        return result
    
    async def evaluate_batch(
        self,
        expressions: Dict[str, str],
        context: ExpressionContext,
        language: Optional[str] = None,
    ) -> Dict[str, ExpressionResult]:
        """Evaluate multiple expressions in batch."""
        results = {}
        
        # Create tasks for concurrent evaluation
        tasks = []
        for key, expr in expressions.items():
            task = asyncio.create_task(
                self.evaluate(expr, context, language, cache_key=f"batch_{key}")
            )
            tasks.append((key, task))
        
        # Wait for all tasks to complete
        for key, task in tasks:
            try:
                result = await task
                results[key] = result
            except Exception as e:
                results[key] = ExpressionResult(
                    success=False,
                    error=str(e)
                )
        
        return results
    
    async def render_template(
        self,
        template: str,
        context: ExpressionContext,
        language: Optional[str] = None,
    ) -> ExpressionResult:
        """Render template with embedded expressions."""
        start_time = time.time()
        
        try:
            # Find all expressions in template
            pattern = r'\{\{\s*(.+?)\s*\}\}'
            expressions = re.findall(pattern, template)
            
            # Evaluate each expression
            rendered_template = template
            for expr in expressions:
                result = await self.evaluate(expr.strip(), context, language)
                if result.success:
                    # Convert result to string
                    value_str = str(result.value) if result.value is not None else ""
                    # Replace in template
                    rendered_template = rendered_template.replace(f"{{ {expr} }}", value_str)
                    rendered_template = rendered_template.replace(f"{{  {expr}  }}", value_str)
                    rendered_template = rendered_template.replace(f"{{{{{expr}}}}}", value_str)
                else:
                    # Replace with error placeholder
                    rendered_template = rendered_template.replace(f"{{ {expr} }}", f"[ERROR: {result.error}]")
            
            execution_time = int((time.time() - start_time) * 1000)
            
            return ExpressionResult(
                success=True,
                value=rendered_template,
                type=ExpressionType.TEMPLATE,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            return ExpressionResult(
                success=False,
                error=str(e),
                type=ExpressionType.TEMPLATE,
                execution_time_ms=execution_time
            )
    
    def _detect_language(self, expression: str) -> str:
        """Auto-detect expression language."""
        # Simple heuristics for language detection
        python_indicators = [
            "['",  # Dictionary access with quotes
            '["',  # Dictionary access with quotes
            "True", "False", "None",  # Python booleans/None
            " and ", " or ", " not ",  # Python logical operators
            "import ",  # Python imports
        ]
        
        javascript_indicators = [
            ".",  # Object property access (common in JS)
            "=>",  # Arrow functions
            "===", "!==",  # Strict equality
            "true", "false", "null",  # JavaScript booleans/null
            "&&", "||", "!",  # JavaScript logical operators
            "new Date()", "Math.",  # Common JavaScript patterns
        ]
        
        python_score = sum(1 for indicator in python_indicators if indicator in expression)
        js_score = sum(1 for indicator in javascript_indicators if indicator in expression)
        
        if python_score > js_score:
            return "python"
        else:
            return self.config.default_language
    
    def _get_cached_result(self, cache_key: str) -> Optional[ExpressionResult]:
        """Get cached result if still valid."""
        if cache_key in self.cache:
            timestamp = self.cache_timestamps.get(cache_key, 0)
            if time.time() - timestamp < self.config.cache_ttl_seconds:
                return self.cache[cache_key]
            else:
                # Remove expired cache entry
                del self.cache[cache_key]
                del self.cache_timestamps[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, result: ExpressionResult):
        """Cache evaluation result."""
        self.cache[cache_key] = result
        self.cache_timestamps[cache_key] = time.time()
        
        # Clean up old cache entries (simple LRU)
        if len(self.cache) > 1000:  # Limit cache size
            oldest_key = min(self.cache_timestamps.keys(), key=lambda k: self.cache_timestamps[k])
            del self.cache[oldest_key]
            del self.cache_timestamps[oldest_key]


# Export all classes
__all__ = [
    'ExpressionEngine',
    'ExpressionContext',
    'ExpressionConfig',
    'ExpressionResult',
    'ExpressionError',
    'ExpressionType',
    'SecurityLevel',
    'SecurityValidator',
    'JavaScriptEngine',
    'PythonEngine',
]