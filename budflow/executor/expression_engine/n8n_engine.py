"""
N8N-compatible expression engine for evaluating N8N-style variables.

This module provides compatibility with N8N's expression system,
supporting variables like $json, $node, $input, $now, $workflow, and $execution.
"""

import re
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

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


class N8NExpressionEngine:
    """
    N8N-compatible expression engine.
    
    This engine evaluates expressions containing N8N-specific variables:
    - $json: Current item data
    - $node: Access to other nodes' data
    - $input: Input data helpers
    - $now: Date/time operations
    - $workflow: Workflow metadata
    - $execution: Execution metadata
    
    The engine wraps these variables in Jinja2 templates for evaluation.
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
        self.logger = logger.bind(component="n8n_expression_engine")
        
        # Use Jinja engine as the underlying processor
        self.jinja_engine = JinjaExpressionEngine(
            enable_security=enable_security,
            enable_caching=enable_caching,
            timeout_seconds=timeout_seconds
        )
    
    async def evaluate(
        self,
        expression: str,
        context: Union[ExpressionContext, Dict[str, Any], Any] = None,
        additional_context: Dict[str, Any] = None
    ) -> Any:
        """
        Evaluate N8N expression.
        
        Args:
            expression: N8N expression to evaluate
            context: Expression context or dictionary
            additional_context: Additional context variables
            
        Returns:
            Evaluated result
            
        Raises:
            ExpressionError: If evaluation fails
        """
        try:
            # Ensure expression is wrapped in Jinja syntax if needed
            processed_expression = self._process_n8n_expression(expression)
            
            # Prepare context with N8N variables
            if isinstance(context, ExpressionContext):
                template_context = context.to_jinja_context()
            elif isinstance(context, dict):
                template_context = context.copy()
            else:
                template_context = {}
            
            # Add N8N-specific context helpers
            template_context.update(self._create_n8n_context(context))
            
            if additional_context:
                template_context.update(additional_context)
            
            # Evaluate using Jinja engine
            result = await self.jinja_engine.evaluate(
                processed_expression,
                template_context,
                additional_context
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                "N8N expression evaluation failed",
                expression=expression,
                error=str(e),
                error_type=type(e).__name__
            )
            
            if isinstance(e, ExpressionError):
                raise
            else:
                raise ExpressionRuntimeError(
                    f"N8N expression evaluation failed: {e}",
                    expression=expression
                )
    
    def _process_n8n_expression(self, expression: str) -> str:
        """
        Process N8N expression to ensure proper Jinja2 formatting.
        
        Args:
            expression: Original N8N expression
            
        Returns:
            Processed expression ready for Jinja2 evaluation
        """
        # Process the expression content
        processed = expression
        
        # Replace N8N variables with their context equivalents (remove $ prefix)
        # This is needed because Jinja2 doesn't accept $ in variable names
        # The context already provides these variables without the $ prefix
        processed = re.sub(r'\$json\b', 'json', processed)
        processed = re.sub(r'\$node\b', 'node', processed)
        processed = re.sub(r'\$input\b', 'input', processed)
        processed = re.sub(r'\$now\b', 'now', processed)
        processed = re.sub(r'\$workflow\b', 'workflow', processed)
        processed = re.sub(r'\$execution\b', 'execution', processed)
        
        # Transform node() function calls to attribute access
        processed = re.sub(
            r"node\('([^']+)'\)\.json", 
            r"node.\1.json", 
            processed
        )
        
        processed = re.sub(
            r'node\("([^"]+)"\)\.json', 
            r"node.\1.json", 
            processed
        )
        
        # Transform array access from JS-style to Jinja-style
        processed = re.sub(
            r'\.(\w+)\[(\d+)\]', 
            r'.\1[\2]',
            processed
        )
        
        # Transform method calls to function calls where needed
        processed = re.sub(
            r'input\.all\(\)\.length', 
            r'input.all() | length',
            processed
        )
        
        processed = re.sub(
            r'input\.all\(\)\.map\(([^)]+)\)', 
            r'input.all() | map(\1) | list',
            processed
        )
        
        processed = re.sub(
            r'input\.all\(\)\.filter\(([^)]+)\)', 
            r'input.all() | selectattr(\1) | list',
            processed
        )
        
        # Wrap in Jinja syntax if not already wrapped
        if not (processed.strip().startswith('{{') or processed.strip().startswith('{%')):
            processed = f"{{{{ {processed} }}}}"
        
        return processed
    
    def _create_n8n_context(self, context: Any) -> Dict[str, Any]:
        """
        Create N8N-specific context variables.
        
        Args:
            context: Original context object
            
        Returns:
            Dictionary with N8N context helpers
        """
        n8n_context = {}
        
        if isinstance(context, ExpressionContext):
            # Already has N8N variables in to_jinja_context()
            return {}
        
        # For non-ExpressionContext, create basic N8N-like variables
        if isinstance(context, dict):
            # Create a simplified $json from context
            n8n_context['$json'] = context.get('json', context)
            
            # Create simplified helpers
            n8n_context['$now'] = datetime.now()
            
            # Create simplified input helper
            input_data = context.get('input', [])
            if isinstance(input_data, list):
                n8n_context['$input'] = {
                    'all': lambda: input_data,
                    'first': lambda: input_data[0] if input_data else {},
                    'last': lambda: input_data[-1] if input_data else {},
                    'item': lambda idx: input_data[idx] if 0 <= idx < len(input_data) else {},
                    'length': len(input_data)
                }
            
            # Create simplified node helper
            node_data = context.get('node_data', {})
            if isinstance(node_data, dict):
                class SimpleNodeHelper:
                    def __init__(self, data):
                        self._data = data
                    
                    def __getattr__(self, name):
                        node_items = self._data.get(name, [])
                        if len(node_items) == 1:
                            return {'json': node_items[0]}
                        return {'json': node_items}
                
                n8n_context['$node'] = SimpleNodeHelper(node_data)
        
        return n8n_context
    
    def clear_cache(self) -> None:
        """Clear expression cache."""
        self.jinja_engine.clear_cache()
    
    @property
    def cache_size(self) -> int:
        """Get current cache size."""
        return self.jinja_engine.cache_size
