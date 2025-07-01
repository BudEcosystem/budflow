"""Expression evaluation system."""

import re
import ast
from typing import Any, Dict, Optional, Set
import operator

import structlog

logger = structlog.get_logger()


class SafeEvalVisitor(ast.NodeVisitor):
    """AST visitor for safe expression evaluation."""
    
    # Allowed names
    ALLOWED_NAMES = {
        'None', 'True', 'False',
        'int', 'float', 'str', 'bool', 'list', 'dict', 'tuple', 'set',
        'len', 'sum', 'min', 'max', 'abs', 'round',
        'any', 'all', 'sorted', 'reversed',
    }
    
    # Allowed operators
    ALLOWED_OPS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.And: lambda x, y: x and y,
        ast.Or: lambda x, y: x or y,
        ast.Not: operator.not_,
        ast.In: lambda x, y: x in y,
        ast.NotIn: lambda x, y: x not in y,
        ast.Is: operator.is_,
        ast.IsNot: operator.is_not,
    }
    
    def __init__(self, names: Dict[str, Any]):
        self.names = names
        self.unsafe = False
    
    def visit_Name(self, node):
        if node.id not in self.ALLOWED_NAMES and node.id not in self.names:
            self.unsafe = True
        self.generic_visit(node)
    
    def visit_Call(self, node):
        # Only allow certain function calls
        if isinstance(node.func, ast.Name):
            if node.func.id not in self.ALLOWED_NAMES:
                self.unsafe = True
            # Explicitly block dangerous functions
            if node.func.id in ('__import__', 'eval', 'exec', 'compile', 'open'):
                self.unsafe = True
        elif isinstance(node.func, ast.Attribute):
            # Allow method calls on known objects
            if isinstance(node.func.value, ast.Name):
                if node.func.value.id not in self.names:
                    self.unsafe = True
        else:
            self.unsafe = True
        self.generic_visit(node)
    
    def visit_Import(self, node):
        self.unsafe = True
    
    def visit_ImportFrom(self, node):
        self.unsafe = True
    
    def visit_FunctionDef(self, node):
        self.unsafe = True
    
    def visit_ClassDef(self, node):
        self.unsafe = True
    
    def visit_Delete(self, node):
        self.unsafe = True
    
    def visit_Exec(self, node):
        self.unsafe = True


class ExpressionEvaluator:
    """Safe expression evaluator for workflow nodes."""
    
    # Regex to find expressions in strings
    EXPRESSION_PATTERN = re.compile(r'\{\{(.+?)\}\}')
    
    def __init__(self):
        self.logger = logger.bind(component="expression_evaluator")
    
    def evaluate(self, expression: str, data: Dict[str, Any]) -> Any:
        """Evaluate an expression with the given data context."""
        # Check if it's a template string with expressions
        if '{{' in expression and '}}' in expression:
            return self._evaluate_template(expression, data)
        
        # Otherwise treat as pure expression
        return self._evaluate_expression(expression, data)
    
    def _evaluate_template(self, template: str, data: Dict[str, Any]) -> str:
        """Evaluate a template string with embedded expressions."""
        def replace_expr(match):
            expr = match.group(1).strip()
            try:
                result = self._evaluate_expression(expr, data)
                return str(result)
            except Exception as e:
                self.logger.error("Expression evaluation failed", expression=expr, error=str(e))
                return match.group(0)  # Return original on error
        
        # Replace all expressions in the template
        result = self.EXPRESSION_PATTERN.sub(replace_expr, template)
        
        # If the entire string was a single expression, return the actual value
        if result == template:
            # No substitution happened
            return result
        
        # Check if the entire string was a single expression
        match = self.EXPRESSION_PATTERN.fullmatch(template)
        if match:
            expr = match.group(1).strip()
            try:
                return self._evaluate_expression(expr, data)
            except Exception:
                pass
        
        return result
    
    def _evaluate_expression(self, expr: str, data: Dict[str, Any]) -> Any:
        """Evaluate a single expression safely."""
        try:
            # Parse the expression
            tree = ast.parse(expr, mode='eval')
            
            # Check for unsafe operations
            visitor = SafeEvalVisitor(data)
            visitor.visit(tree)
            
            if visitor.unsafe:
                self.logger.error(
                    "Expression evaluation failed",
                    expression=expr,
                    error=f"Unsafe expression: {expr}",
                    error_type="ValueError"
                )
                return None
            
            # Compile and evaluate
            code = compile(tree, '<expression>', 'eval')
            
            # Create evaluation namespace
            namespace = {
                '__builtins__': {},  # Empty builtins for safety
                'len': len,
                'sum': sum,
                'min': min,
                'max': max,
                'abs': abs,
                'round': round,
                'any': any,
                'all': all,
                'sorted': sorted,
                'reversed': list,  # Convert to list for reversed
                'str': str,
                'int': int,
                'float': float,
                'bool': bool,
                'list': list,
                'dict': dict,
                'tuple': tuple,
                'set': set,
            }
            
            # Add data to namespace
            namespace.update(data)
            
            # Evaluate
            result = eval(code, namespace)
            
            return result
            
        except Exception as e:
            self.logger.error(
                "Expression evaluation failed",
                expression=expr,
                error=str(e),
                error_type=type(e).__name__
            )
            return None
    
    def extract_variables(self, expression: str) -> Set[str]:
        """Extract variable names from an expression."""
        variables = set()
        
        # Find all expressions
        for match in self.EXPRESSION_PATTERN.finditer(expression):
            expr = match.group(1).strip()
            
            try:
                # Parse expression
                tree = ast.parse(expr, mode='eval')
                
                # Extract variable names
                for node in ast.walk(tree):
                    if isinstance(node, ast.Name):
                        variables.add(node.id)
                        
            except Exception:
                pass
        
        return variables
    
    def validate_expression(self, expression: str) -> bool:
        """Validate that an expression is safe and syntactically correct."""
        try:
            # Check template expressions
            if '{{' in expression and '}}' in expression:
                for match in self.EXPRESSION_PATTERN.finditer(expression):
                    expr = match.group(1).strip()
                    if not self._validate_single_expression(expr):
                        return False
                return True
            
            # Check single expression
            return self._validate_single_expression(expression)
            
        except Exception:
            return False
    
    def _validate_single_expression(self, expr: str) -> bool:
        """Validate a single expression."""
        try:
            # Parse the expression
            tree = ast.parse(expr, mode='eval')
            
            # Check for unsafe operations
            visitor = SafeEvalVisitor({})
            visitor.visit(tree)
            
            return not visitor.unsafe
            
        except Exception:
            return False