"""
Advanced expression engine with N8N variables and Jinja templates.

This module provides comprehensive expression evaluation capabilities including:
- N8N-compatible variables ($node, $input, $json, $now, $workflow, $execution)
- Jinja2 templates with extensive filters and functions
- JavaScript-style expressions for compatibility
- Security sandboxing
- Performance optimization
- Async evaluation support
"""

from .engine import ExpressionEngine
from .context import ExpressionContext
from .errors import ExpressionError
from .jinja_engine import JinjaExpressionEngine
from .n8n_engine import N8NExpressionEngine
from .javascript_engine import JavaScriptExpressionEngine

__all__ = [
    "ExpressionEngine",
    "ExpressionContext", 
    "ExpressionError",
    "JinjaExpressionEngine",
    "N8NExpressionEngine",
    "JavaScriptExpressionEngine",
]