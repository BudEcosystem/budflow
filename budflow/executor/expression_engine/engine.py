"""
Main expression engine that orchestrates different expression types.

This module provides the primary interface for expression evaluation,
automatically detecting expression types and delegating to appropriate engines.
"""

import re
import asyncio
from typing import Any, Dict, List, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor

import structlog

from .context import ExpressionContext
from .errors import (
    ExpressionError,
    ExpressionRuntimeError,
    ExpressionTimeoutError
)
from .jinja_engine import JinjaExpressionEngine
from .n8n_engine import N8NExpressionEngine
from .javascript_engine import JavaScriptExpressionEngine

logger = structlog.get_logger()


class ExpressionEngine:
    """
    Main expression engine that handles multiple expression types.
    
    This engine automatically detects expression types and delegates
    evaluation to the appropriate specialized engine.
    
    Supported expression types:
    - Jinja2 templates ({{ }}, {% %}, {# #})
    - N8N variables ($json, $node, $input, etc.)
    - JavaScript-style expressions
    - Plain text
    """
    
    def __init__(
        self,
        enable_security: bool = True,
        enable_caching: bool = True,
        optimize_performance: bool = False,
        timeout_seconds: float = 30.0,
        max_workers: int = 4
    ):
        self.enable_security = enable_security
        self.enable_caching = enable_caching
        self.optimize_performance = optimize_performance
        self.timeout_seconds = timeout_seconds
        self.max_workers = max_workers
        
        self.logger = logger.bind(component="expression_engine")
        
        # Initialize specialized engines
        self.jinja_engine = JinjaExpressionEngine(
            enable_security=enable_security,
            enable_caching=enable_caching,
            timeout_seconds=timeout_seconds
        )
        
        self.n8n_engine = N8NExpressionEngine(
            enable_security=enable_security,
            enable_caching=enable_caching,
            timeout_seconds=timeout_seconds
        )
        
        self.js_engine = JavaScriptExpressionEngine(
            enable_security=enable_security,
            enable_caching=enable_caching,
            timeout_seconds=timeout_seconds
        )
        
        # Performance optimization
        self._executor = None
        if optimize_performance:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Cache statistics
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "total_evaluations": 0
        }
    
    def detect_expression_type(self, expression: str) -> str:
        """
        Detect the type of expression.
        
        Args:
            expression: Expression string to analyze
            
        Returns:
            Expression type: 'jinja', 'n8n', 'javascript', or 'text'
        """
        if not expression or not isinstance(expression, str):
            return "text"
        
        expression = expression.strip()
        
        # Check for Jinja2 templates
        if (re.search(r'{{.*?}}', expression) or 
            re.search(r'{%.*?%}', expression) or
            re.search(r'{#.*?#}', expression)):
            
            # Check if it contains N8N variables within Jinja syntax
            if re.search(r'{{\s*\$(?:json|node|input|now|workflow|execution)', expression):
                return "n8n"
            
            return "jinja"
        
        # Check for JavaScript-style expressions
        js_patterns = [
            r'\$json\.',
            r'\$node\(',
            r'\$input\.',
            r'\$workflow\.',
            r'\$execution\.',
            r'new\s+\w+\(',
            r'\w+\.\w+\s*\(',
            r'=>\s*',  # Arrow functions
            r'\[\s*\d+\s*\]',  # Array access
        ]
        
        for pattern in js_patterns:
            if re.search(pattern, expression):
                return "javascript"
        
        # Check for standalone N8N variable access
        if re.search(r'^\$(?:json|node|input|now|workflow|execution)', expression):
            return "javascript"  # Treat as JS for property access
        
        return "text"
    
    async def evaluate(
        self,
        expression: str,
        context: Union[ExpressionContext, Dict[str, Any], Any] = None,
        additional_context: Dict[str, Any] = None
    ) -> Any:
        """
        Evaluate expression using appropriate engine.
        
        Args:
            expression: Expression to evaluate
            context: Evaluation context
            additional_context: Additional context variables
            
        Returns:
            Evaluation result
            
        Raises:
            ExpressionError: If evaluation fails
        """
        if not expression:
            return ""
        
        self.cache_stats["total_evaluations"] += 1
        
        try:
            # Detect expression type
            expr_type = self.detect_expression_type(expression)
            
            self.logger.debug(
                "Evaluating expression",
                expression=expression[:100] + "..." if len(expression) > 100 else expression,
                type=expr_type
            )
            
            # Delegate to appropriate engine
            if expr_type == "jinja":
                return await self.jinja_engine.evaluate(expression, context, additional_context)
            elif expr_type == "n8n":
                return await self.n8n_engine.evaluate(expression, context, additional_context)
            elif expr_type == "javascript":
                return await self.js_engine.evaluate(expression, context, additional_context)
            else:
                # Plain text - return as is, but apply context substitution if it's a simple variable
                return self._evaluate_plain_text(expression, context, additional_context)
                
        except Exception as e:
            self.logger.error(
                "Expression evaluation failed",
                expression=expression,
                error=str(e),
                error_type=type(e).__name__
            )
            
            if isinstance(e, ExpressionError):
                raise
            else:
                raise ExpressionRuntimeError(
                    f"Expression evaluation failed: {e}",
                    expression=expression
                )
    
    def _evaluate_plain_text(
        self,
        expression: str,
        context: Union[ExpressionContext, Dict[str, Any], Any] = None,
        additional_context: Dict[str, Any] = None
    ) -> str:
        """
        Handle plain text expressions with simple variable substitution.
        
        Args:
            expression: Plain text expression
            context: Evaluation context
            additional_context: Additional context variables
            
        Returns:
            Processed text
        """
        # If it's just a simple variable name, try to resolve it
        if (expression.isidentifier() and 
            additional_context and 
            expression in additional_context):
            return additional_context[expression]
        
        # Otherwise return as-is
        return expression
    
    async def evaluate_batch(
        self,
        expressions: List[str],
        context: Union[ExpressionContext, Dict[str, Any], Any] = None,
        additional_context: Dict[str, Any] = None
    ) -> List[Any]:
        """
        Evaluate multiple expressions in batch for better performance.
        
        Args:
            expressions: List of expressions to evaluate
            context: Evaluation context
            additional_context: Additional context variables
            
        Returns:
            List of evaluation results
        """
        if self.optimize_performance and len(expressions) > 1:
            # Evaluate in parallel
            tasks = [
                self.evaluate(expr, context, additional_context)
                for expr in expressions
            ]
            return await asyncio.gather(*tasks)
        else:
            # Evaluate sequentially
            results = []
            for expr in expressions:
                result = await self.evaluate(expr, context, additional_context)
                results.append(result)
            return results
    
    def clear_cache(self) -> None:
        """Clear all engine caches."""
        self.jinja_engine.clear_cache()
        self.n8n_engine.clear_cache()
        self.js_engine.clear_cache()
        
        # Reset cache statistics
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "total_evaluations": 0
        }
    
    @property
    def total_cache_size(self) -> int:
        """Get total cache size across all engines."""
        return (
            self.jinja_engine.cache_size +
            self.n8n_engine.cache_size +
            self.js_engine.cache_size
        )
    
    def get_engine_stats(self) -> Dict[str, Any]:
        """Get statistics about engine usage and performance."""
        return {
            "cache_stats": self.cache_stats,
            "total_cache_size": self.total_cache_size,
            "engine_cache_sizes": {
                "jinja": self.jinja_engine.cache_size,
                "n8n": self.n8n_engine.cache_size,
                "javascript": self.js_engine.cache_size
            },
            "configuration": {
                "security_enabled": self.enable_security,
                "caching_enabled": self.enable_caching,
                "performance_optimized": self.optimize_performance,
                "timeout_seconds": self.timeout_seconds
            }
        }
    
    def __del__(self):
        """Cleanup resources."""
        if self._executor:
            self._executor.shutdown(wait=False)
