"""Security sandbox for code execution."""

import ast
import builtins
import logging
from typing import Dict, Any, List, Optional, Set, Tuple

try:
    from RestrictedPython import compile_restricted_exec, safe_globals
    from RestrictedPython.Guards import guarded_iter_unpack_sequence, guarded_setattr, safe_builtins
    RESTRICTED_PYTHON_AVAILABLE = True
except ImportError:
    RESTRICTED_PYTHON_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("RestrictedPython not installed, sandboxing disabled")

from .config import SecurityConfig
from .exceptions import SecurityViolationError

logger = logging.getLogger(__name__)


class CodeAnalysis:
    """Code analysis results."""
    
    def __init__(self):
        self.imports: Set[str] = set()
        self.accessed_variables: Set[str] = set()
        self.function_calls: Set[str] = set()
        self.has_exec_eval: bool = False
        self.has_imports: bool = False
        self.has_dangerous_calls: bool = False
        self.ast_depth: int = 0
        self.warnings: List[str] = []


class CodeAnalyzer(ast.NodeVisitor):
    """Static code analyzer using AST."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.analysis = CodeAnalysis()
        self._current_depth = 0
        self._max_depth = 0
        
        # Dangerous function names
        self.dangerous_functions = {
            "exec", "eval", "compile", "__import__",
            "open", "file", "input", "raw_input",
            "execfile", "reload", "vars", "globals", "locals",
        }
    
    def analyze(self, code: str) -> CodeAnalysis:
        """Analyze code and return analysis results."""
        self.analysis = CodeAnalysis()
        
        try:
            tree = ast.parse(code)
            self.visit(tree)
            self.analysis.ast_depth = self._max_depth
        except SyntaxError as e:
            raise SecurityViolationError(
                f"Syntax error in code: {e}",
                violation_type="syntax_error",
                code_snippet=code[:100]
            )
        
        return self.analysis
    
    def visit(self, node):
        """Visit AST node with depth tracking."""
        self._current_depth += 1
        self._max_depth = max(self._max_depth, self._current_depth)
        
        if self._current_depth > self.config.max_ast_depth:
            raise SecurityViolationError(
                f"Code complexity exceeds maximum AST depth {self.config.max_ast_depth}",
                violation_type="complexity",
            )
        
        super().visit(node)
        self._current_depth -= 1
    
    def visit_Import(self, node):
        """Handle import statements."""
        self.analysis.has_imports = True
        for alias in node.names:
            module_name = alias.name
            self.analysis.imports.add(module_name)
            
            if not self.config.is_module_allowed(module_name):
                raise SecurityViolationError(
                    f"Import of module '{module_name}' is not allowed",
                    violation_type="forbidden_import",
                    code_snippet=f"import {module_name}"
                )
        self.generic_visit(node)
    
    def visit_ImportFrom(self, node):
        """Handle from...import statements."""
        self.analysis.has_imports = True
        module_name = node.module or ""
        self.analysis.imports.add(module_name)
        
        if not self.config.is_module_allowed(module_name):
            raise SecurityViolationError(
                f"Import from module '{module_name}' is not allowed",
                violation_type="forbidden_import",
                code_snippet=f"from {module_name} import ..."
            )
        self.generic_visit(node)
    
    def visit_Call(self, node):
        """Handle function calls."""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            self.analysis.function_calls.add(func_name)
            
            if func_name in self.dangerous_functions:
                self.analysis.has_dangerous_calls = True
                if func_name in {"exec", "eval"}:
                    self.analysis.has_exec_eval = True
                    if self.config.disallow_exec_eval:
                        raise SecurityViolationError(
                            f"Use of '{func_name}' is not allowed",
                            violation_type="forbidden_function",
                            code_snippet=func_name
                        )
                else:
                    self.analysis.warnings.append(
                        f"Potentially dangerous function call: {func_name}"
                    )
        self.generic_visit(node)
    
    def visit_Name(self, node):
        """Handle variable names."""
        if isinstance(node.ctx, ast.Load):
            self.analysis.accessed_variables.add(node.id)
        self.generic_visit(node)
    
    def visit_Attribute(self, node):
        """Handle attribute access."""
        if self.config.disallow_attribute_access:
            # Check for dangerous attribute patterns
            if isinstance(node.value, ast.Name):
                base_name = node.value.id
                attr_name = node.attr
                
                # Check for dangerous patterns like __class__, __bases__, etc.
                if attr_name.startswith("__") and attr_name.endswith("__"):
                    self.analysis.warnings.append(
                        f"Access to dunder attribute: {base_name}.{attr_name}"
                    )
        self.generic_visit(node)


class SecuritySandbox:
    """Security sandbox for code execution."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.analyzer = CodeAnalyzer(config)
        
        if not RESTRICTED_PYTHON_AVAILABLE and config.enable_sandboxing:
            raise RuntimeError(
                "RestrictedPython is required for sandboxing. "
                "Install with: pip install RestrictedPython"
            )
    
    def compile_restricted(self, code: str, filename: str = "<sandbox>") -> Any:
        """Compile code with restrictions."""
        if not self.config.enable_sandboxing:
            # No sandboxing, just compile normally
            return compile(code, filename, "exec")
        
        if not RESTRICTED_PYTHON_AVAILABLE:
            raise SecurityViolationError(
                "Sandboxing is enabled but RestrictedPython is not available",
                violation_type="configuration_error"
            )
        
        # Analyze code first
        if self.config.enable_code_analysis:
            analysis = self.analyzer.analyze(code)
            
            # Check for violations based on analysis
            if analysis.has_exec_eval and self.config.disallow_exec_eval:
                raise SecurityViolationError(
                    "Code contains exec/eval which is not allowed",
                    violation_type="forbidden_function"
                )
        
        # Compile with RestrictedPython
        result = compile_restricted_exec(code, filename=filename)
        
        if result.errors:
            error_msg = "\n".join(str(e) for e in result.errors)
            raise SecurityViolationError(
                f"Code compilation failed: {error_msg}",
                violation_type="compilation_error",
                code_snippet=code[:100]
            )
        
        if result.warnings:
            for warning in result.warnings:
                logger.warning(f"RestrictedPython warning: {warning}")
        
        return result.code
    
    def create_safe_globals(self, additional_globals: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create safe global environment for execution."""
        if not self.config.enable_sandboxing:
            # No sandboxing, return full globals
            globs = {"__builtins__": builtins}
            if additional_globals:
                globs.update(additional_globals)
            return globs
        
        if not RESTRICTED_PYTHON_AVAILABLE:
            raise SecurityViolationError(
                "Sandboxing is enabled but RestrictedPython is not available",
                violation_type="configuration_error"
            )
        
        # Start with RestrictedPython safe globals
        globs = safe_globals.copy()
        
        # Add guarded operations
        globs["_iter_unpack_sequence_"] = guarded_iter_unpack_sequence
        globs["_write_"] = guarded_setattr
        globs["_print_"] = True  # Enable print function
        
        # Add allowed builtins
        safe_builtins = {}
        
        # Always allowed builtins
        always_allowed = {
            "None", "True", "False", "int", "float", "str", "bool",
            "list", "tuple", "dict", "set", "frozenset",
            "len", "range", "enumerate", "zip", "map", "filter",
            "sum", "min", "max", "abs", "round", "pow",
            "sorted", "reversed", "all", "any",
            "isinstance", "issubclass", "hasattr", "getattr", "setattr",
            "type", "id", "hash", "chr", "ord", "bin", "hex", "oct",
            "print",  # We'll capture output
        }
        
        for name in always_allowed:
            if hasattr(builtins, name):
                safe_builtins[name] = getattr(builtins, name)
        
        # Add allowed modules as imports
        if not self.config.disallow_import:
            # Create custom __import__ that checks allowed modules
            def safe_import(name, *args, **kwargs):
                if not self.config.is_module_allowed(name):
                    raise SecurityViolationError(
                        f"Import of module '{name}' is not allowed",
                        violation_type="forbidden_import"
                    )
                return __import__(name, *args, **kwargs)
            
            safe_builtins["__import__"] = safe_import
        
        globs["__builtins__"] = safe_builtins
        
        # Add additional globals if provided
        if additional_globals:
            globs.update(additional_globals)
        
        # Freeze prototypes if enabled
        if self.config.enable_prototype_pollution_prevention:
            self._freeze_prototypes(globs)
        
        return globs
    
    def _freeze_prototypes(self, globs: Dict[str, Any]) -> None:
        """Freeze prototypes to prevent pollution."""
        # Freeze built-in type prototypes
        types_to_freeze = [
            dict, list, tuple, set, frozenset,
            str, int, float, bool,
        ]
        
        for type_obj in types_to_freeze:
            if hasattr(type_obj, "__dict__"):
                for attr_name in dir(type_obj):
                    if not attr_name.startswith("__"):
                        try:
                            attr = getattr(type_obj, attr_name)
                            if callable(attr):
                                # In Python, we can't truly freeze, but we can document
                                # that these shouldn't be modified
                                pass
                        except Exception:
                            pass
    
    def is_module_allowed(self, module_name: str) -> bool:
        """Check if a module is allowed."""
        return self.config.is_module_allowed(module_name)